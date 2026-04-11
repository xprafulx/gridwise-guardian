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
MODE = "TODAY" 
# ==========================================================

now_utc = datetime.now(timezone.utc)

if MODE == "TODAY":
    # Snaps to 00:00 of the current Danish Calendar Day
    TARGET_DATE = (now_utc + timedelta(hours=2)).replace(hour=0, minute=0, second=0, microsecond=0)
else:
    # Snaps to 00:00 of Tomorrow
    TARGET_DATE = (now_utc + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

print(f"🎯 MODE: {MODE} | Predicting for: {TARGET_DATE.date()}")

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
    query = text("""
        SELECT model_binary, model_version FROM model_registry 
        WHERE model_name = :name AND is_active = TRUE 
        ORDER BY created_at DESC LIMIT 1
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"name": f"co2_{area_name.lower()}"}).fetchone()
    if not result: return None, None, None
    buffer = io.BytesIO(result[0])
    payload = joblib.load(buffer)
    return payload['model'], payload['features'], result[1]

def get_future_prices(area_name, date):
    url = "https://api.energidataservice.dk/dataset/DayAheadPrices"
    start_str = date.strftime('%Y-%m-%dT00:00Z')
    end_str = (date + timedelta(days=1)).strftime('%Y-%m-%dT00:00Z')
    
    params = {
        "filter": json.dumps({"PriceArea": [area_name.upper()]}),
        "start": start_str,
        "end": end_str,
        "limit": 100
    }
    
    try:
        print(f"📡 API: Requesting {area_name} from {start_str} to {end_str}")
        res = requests.get(url, params=params, timeout=15)
        if res.status_code == 200:
            records = res.json().get('records', [])
            if not records: return None
            df = pd.DataFrame(records)
            time_col = next((c for c in ['HourUTC', 'Minutes15UTC', 'ds'] if c in df.columns), None)
            df['datetime_utc'] = pd.to_datetime(df[time_col], utc=True)
            p_col = 'SpotPriceDKK' if 'SpotPriceDKK' in df.columns else 'DayAheadPriceDKK'
            df['price_kwh'] = pd.to_numeric(df[p_col]) / 1000
            return df.set_index('datetime_utc').resample('h').mean(numeric_only=True)['price_kwh'].to_dict()
    except Exception as e:
        print(f"❌ Price API Error: {e}")
    return None

def generate_full_day_forecast(area_name, engine, target_date):
    model, feature_names, db_version = download_model_from_neon(area_name)
    if model is None: return None
    t = get_dynamic_thresholds(area_name, engine)
    prices = get_future_prices(area_name, target_date)
    
    if not prices: 
        print(f"⚠️ No prices found for {area_name} on {target_date.date()}.")
        return None

    query = f"SELECT datetime_utc, co2_emissions_g_kwh, wind_speed, solar_radiation FROM processed_features WHERE price_area='{area_name.upper()}' AND is_forecast=FALSE ORDER BY datetime_utc DESC LIMIT 169"
    recent = pd.read_sql(query, engine)
    history = recent['co2_emissions_g_kwh'].tolist()[::-1]
    last_weather = recent.iloc[0]
    dk_holidays = holidays.Denmark()

    preds = []
    for h in range(24):
        time_utc = pd.to_datetime(target_date.replace(hour=h), utc=True)
        p = prices.get(time_utc, list(prices.values())[0] if prices else 0)
        
        feats = {
            'spot_price_dkk_kwh': p, 'wind_speed': last_weather['wind_speed'],
            'solar_radiation': last_weather['solar_radiation'], 'hour': h,
            'day_of_week': time_utc.weekday(), 'month': time_utc.month,
            'hour_sin': np.sin(2*np.pi*h/24), 'hour_cos': np.cos(2*np.pi*h/24),
            'is_holiday': 1 if time_utc.date() in dk_holidays else 0, 
            'is_weekend': 1 if time_utc.weekday()>=5 else 0,
            'co2_lag_1h': history[-1], 'co2_lag_2h': history[-2], 
            'co2_lag_24h': history[-24], 'co2_lag_168h': history[-168]
        }
        
        co2_pred = model.predict(pd.DataFrame([feats])[feature_names])[0]
        
        if (17 <= h <= 21) or (p > t['p83_price']) or (co2_pred > t['p83_co2']):
            status = "AVOID"
        elif (p < t['p33_price']) and (co2_pred < t['p33_co2']):
            status = "BEST"
        else:
            status = "CAUTION"

        preds.append({
            'datetime_utc': time_utc, 'price_area': area_name, 
            'model_version': db_version, 'predicted_co2': float(co2_pred), 
            'market_price_dkk_kwh': p, 'recommendation_status': status
        })
        history.append(co2_pred)

    df = pd.DataFrame(preds)
    df['should_charge'] = df['predicted_co2'] <= df['predicted_co2'].nsmallest(6).max()
    return df

def run_job():
    engine = get_db_connection()
    for area in ['DK1', 'DK2']:
        df = generate_full_day_forecast(area, engine, TARGET_DATE)
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
            print(f"✅ {area} synced ({MODE})")

if __name__ == "__main__":
    run_job()
