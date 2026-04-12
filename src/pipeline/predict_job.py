import sys
import os
import io
import json
import joblib
import pandas as pd
import numpy as np
import holidays
import requests
from datetime import datetime, timedelta, timezone
from sqlalchemy import text 

# --- PATH FIX ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.database.connection import get_db_connection

# ==========================================================
# 🔄 TOGGLE SWITCH: Change to "TODAY" or "TOMORROW"
MODE = "TOMORROW" 
# ==========================================================

# 🕰️ DANISH TIMEZONE ALIGNMENT
# We use Pandas to lock to exactly Midnight in Copenhagen, regardless of DST.
cph_now = pd.Timestamp.now(tz='Europe/Copenhagen')

if MODE == "TODAY":
    TARGET_DATE_CPH = cph_now.normalize() # Snaps to 00:00:00 CEST
else:
    TARGET_DATE_CPH = cph_now.normalize() + pd.Timedelta(days=1)

print(f"🎯 MODE: {MODE} | Danish Day: {TARGET_DATE_CPH.strftime('%Y-%m-%d')}")

def get_dynamic_thresholds(area_name, engine):
    query = f"""
        SELECT spot_price_dkk_kwh, co2_emissions_g_kwh 
        FROM processed_features 
        WHERE price_area = '{area_name.upper()}' 
        AND is_forecast = FALSE
        AND datetime_utc >= CURRENT_DATE - INTERVAL '2 years'
    """
    try:
        df = pd.read_sql(query, engine)
        if df.empty: return {'p33_price': 0.5, 'p83_price': 2.0, 'p33_co2': 50, 'p83_co2': 150}
        return {
            'p33_price': float(df['spot_price_dkk_kwh'].quantile(0.33)),
            'p83_price': float(df['spot_price_dkk_kwh'].quantile(0.83)),
            'p33_co2': float(df['co2_emissions_g_kwh'].quantile(0.33)),
            'p83_co2': float(df['co2_emissions_g_kwh'].quantile(0.83))
        }
    except: return {'p33_price': 0.5, 'p83_price': 2.0, 'p33_co2': 50, 'p83_co2': 150}

def download_model_from_neon(area_name):
    engine = get_db_connection()
    query = text("SELECT model_binary, model_version FROM model_registry WHERE model_name = :name AND is_active = TRUE ORDER BY created_at DESC LIMIT 1")
    with engine.connect() as conn:
        result = conn.execute(query, {"name": f"co2_{area_name.lower()}"}).fetchone()
    if not result: return None, None, None
    buffer = io.BytesIO(result[0])
    payload = joblib.load(buffer)
    return payload['model'], payload['features'], result[1]

def get_future_prices(area_name, target_cph):
    url = "https://api.energidataservice.dk/dataset/DayAheadPrices"
    params = {
        "filter": json.dumps({"PriceArea": [area_name.upper()]}),
        "sort": "TimeUTC DESC",
        "limit": 400 
    }
    
    try:
        print(f"📡 Requesting recent prices exclusively for {area_name}...")
        res = requests.get(url, params=params, timeout=15)
        if res.status_code == 200:
            records = res.json().get('records', [])
            if not records: return None
            
            df = pd.DataFrame(records)
            df['datetime_utc'] = pd.to_datetime(df['TimeUTC'], utc=True)
            df['price_kwh'] = pd.to_numeric(df['DayAheadPriceDKK']) / 1000
            
            # 🛡️ THE EXACT UTC BOUNDS OF THE DANISH DAY
            start_utc = target_cph.tz_convert('UTC')
            end_utc = start_utc + pd.Timedelta(days=1)
            
            mask = (df['datetime_utc'] >= start_utc) & (df['datetime_utc'] < end_utc)
            df_filtered = df[mask]
            
            if df_filtered.empty: 
                print(f"⚠️ Data downloaded, but {target_cph.date()} is not available yet.")
                return None
                
            return df_filtered.set_index('datetime_utc').resample('h').mean(numeric_only=True)['price_kwh'].to_dict()
        else:
            print(f"❌ API HTTP Error: {res.status_code}")
    except Exception as e:
        print(f"❌ Price API Error: {e}")
    return None

def generate_full_day_forecast(area_name, engine, target_cph):
    model, feature_names, db_version = download_model_from_neon(area_name)
    if model is None: return None
    
    t = get_dynamic_thresholds(area_name, engine)
    prices = get_future_prices(area_name, target_cph)
    
    if not prices: 
        print(f"⚠️ Missing price data. Skipping {area_name}.")
        return None

    query = f"SELECT datetime_utc, co2_emissions_g_kwh, wind_speed, solar_radiation FROM processed_features WHERE price_area='{area_name.upper()}' AND is_forecast=FALSE ORDER BY datetime_utc DESC LIMIT 169"
    recent = pd.read_sql(query, engine)
    history = recent['co2_emissions_g_kwh'].tolist()[::-1]
    last_weather = recent.iloc[0]
    dk_holidays = holidays.Denmark()

    preds = []
    # Loop over the 24 hours of the DANISH day
    for h in range(24):
        # 1. Get the local Danish Time
        time_local = target_cph + pd.Timedelta(hours=h)
        # 2. Convert to UTC to fetch prices and save to database safely
        time_utc = time_local.tz_convert('UTC')
        
        # Now every hour matches perfectly without hitting tomorrow!
        p = prices.get(time_utc, 0)
        
        feats = {
            'spot_price_dkk_kwh': p, 'wind_speed': last_weather['wind_speed'],
            'solar_radiation': last_weather['solar_radiation'], 'hour': time_local.hour,
            'day_of_week': time_local.weekday(), 'month': time_local.month,
            'hour_sin': np.sin(2*np.pi*time_local.hour/24), 'hour_cos': np.cos(2*np.pi*time_local.hour/24),
            'is_holiday': 1 if time_local.date() in dk_holidays else 0, 
            'is_weekend': 1 if time_local.weekday()>=5 else 0,
            'co2_lag_1h': history[-1], 'co2_lag_2h': history[-2], 
            'co2_lag_24h': history[-24], 'co2_lag_168h': history[-168]
        }
        
        co2_pred = model.predict(pd.DataFrame([feats])[feature_names])[0]
        
        # Avoid 17:00 to 21:00 Local Danish Time
        if (17 <= time_local.hour <= 21) or (p > t['p83_price']) or (co2_pred > t['p83_co2']):
            status = "AVOID"
        elif (p < t['p33_price']) and (co2_pred < t['p33_co2']):
            status = "BEST"
        else:
            status = "CAUTION"

        preds.append({
            'datetime_utc': time_utc.to_pydatetime(), # Clean DB insert
            'price_area': area_name, 
            'model_version': db_version, 
            'predicted_co2': float(co2_pred), 
            'market_price_dkk_kwh': p, 
            'recommendation_status': status
        })
        history.append(co2_pred)

    df = pd.DataFrame(preds)
    df['should_charge'] = df['predicted_co2'] <= df['predicted_co2'].nsmallest(6).max()
    return df

def run_job():
    engine = get_db_connection()
    for area in ['DK1', 'DK2']:
        df = generate_full_day_forecast(area, engine, TARGET_DATE_CPH)
        if df is not None:
            df.to_sql(f'temp_{area.lower()}', engine, if_exists='replace', index=False)
            upsert = text(f"""
                INSERT INTO ai_forecasts (datetime_utc, price_area, model_version, predicted_co2, market_price_dkk_kwh, should_charge, recommendation_status)
                SELECT datetime_utc, price_area, model_version, predicted_co2, market_price_dkk_kwh, should_charge, recommendation_status FROM temp_{area.lower()}
                ON CONFLICT (datetime_utc, price_area, model_version) DO UPDATE SET 
                predicted_co2=EXCLUDED.predicted_co2, market_price_dkk_kwh=EXCLUDED.market_price_dkk_kwh, 
                should_charge=EXCLUDED.should_charge, recommendation_status=EXCLUDED.recommendation_status, prediction_timestamp=CURRENT_TIMESTAMP;
            """)
            with engine.begin() as conn:
                conn.execute(upsert)
                conn.execute(text(f"DROP TABLE IF EXISTS temp_{area.lower()}"))
            print(f"✅ {area} synced exactly 24 hours ({MODE})")

if __name__ == "__main__":
    run_job()
