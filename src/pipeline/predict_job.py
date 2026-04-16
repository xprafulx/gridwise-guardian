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
cph_now = pd.Timestamp.now(tz='Europe/Copenhagen')
cph_now_floor = cph_now.floor('h') 

if MODE == "TODAY":
    TARGET_DATE_CPH = cph_now.normalize() 
else:
    TARGET_DATE_CPH = cph_now.normalize() + pd.Timedelta(days=1)

print(f"🎯 MODE: {MODE} | Danish Day: {TARGET_DATE_CPH.strftime('%Y-%m-%d')}")
print(f"🕒 Current Local Time: {cph_now_floor}")

def get_dynamic_thresholds(area_name, engine):
    query = text("""
        SELECT spot_price_dkk_kwh, co2_emissions_g_kwh 
        FROM processed_features 
        WHERE price_area = :area 
        AND is_forecast = FALSE
        AND datetime_utc >= CURRENT_DATE - INTERVAL '2 years'
    """)
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"area": area_name.upper()})
            
        if df.empty: 
            print(f"⚠️ Warning: DB empty. Using fallback thresholds for {area_name}")
            return {'p33_price': 0.5, 'p83_price': 2.0, 'p33_co2': 50, 'p83_co2': 150}
            
        return {
            'p33_price': float(df['spot_price_dkk_kwh'].quantile(0.33)),
            'p83_price': float(df['spot_price_dkk_kwh'].quantile(0.83)),
            'p33_co2': float(df['co2_emissions_g_kwh'].quantile(0.33)),
            'p83_co2': float(df['co2_emissions_g_kwh'].quantile(0.83))
        }
    except Exception as e: 
        print(f"⚠️ Threshold Error ({e}). Using fallbacks.")
        return {'p33_price': 0.5, 'p83_price': 2.0, 'p33_co2': 50, 'p83_co2': 150}

def download_model_from_neon(area_name):
    engine = get_db_connection()
    query = text("SELECT model_binary, model_version FROM model_registry WHERE model_name = :name AND is_active = TRUE ORDER BY created_at DESC LIMIT 1")
    with engine.connect() as conn:
        result = conn.execute(query, {"name": f"co2_{area_name.lower()}"}).fetchone()
    if not result: return None, None, None, None
    buffer = io.BytesIO(result[0])
    payload = joblib.load(buffer)
    
    # --- 🛠️ THE SCALER FIX 🛠️ ---
    return payload['model'], payload['scaler'], payload['features'], result[1]

def fetch_realtime_co2_api(area_name, end_time_cph):
    print(f"📡 Fetching live CO2 history from API for {area_name} up to {end_time_cph.time()}...")
    url = "https://api.energidataservice.dk/dataset/CO2Emis"
    start_time_cph = end_time_cph - pd.Timedelta(days=8)
    params = {
        "filter": json.dumps({"PriceArea": [area_name.upper()]}),
        "start": start_time_cph.tz_convert('UTC').strftime('%Y-%m-%dT%H:%M'),
        "end": (end_time_cph + pd.Timedelta(hours=1)).tz_convert('UTC').strftime('%Y-%m-%dT%H:%M'),
        "sort": "Minutes5UTC DESC",
        "limit": 10000
    }
    try:
        res = requests.get(url, params=params, timeout=15)
        if res.status_code == 200:
            records = res.json().get('records', [])
            if not records: return None
            df = pd.DataFrame(records)
            df['datetime_utc'] = pd.to_datetime(df['Minutes5UTC'], utc=True)
            hourly = df.set_index('datetime_utc').resample('h').mean(numeric_only=True)['CO2Emission']
            end_time_utc = end_time_cph.tz_convert('UTC')
            hourly = hourly[hourly.index <= end_time_utc]
            hourly = hourly.ffill().bfill() 
            history_list = hourly.tail(168).tolist()
            if len(history_list) == 168:
                return history_list
    except Exception as e:
        print(f"❌ Real-time API Error: {e}")
    return None

def get_future_prices(area_name, start_cph, target_cph):
    url = "https://api.energidataservice.dk/dataset/DayAheadPrices"
    params = {
        "filter": json.dumps({"PriceArea": [area_name.upper()]}),
        "sort": "TimeUTC DESC",
        "limit": 400 
    }
    try:
        res = requests.get(url, params=params, timeout=15)
        if res.status_code == 200:
            df = pd.DataFrame(res.json().get('records', []))
            df['datetime_utc'] = pd.to_datetime(df['TimeUTC'], utc=True)
            df['price_kwh'] = pd.to_numeric(df['DayAheadPriceDKK']) / 1000
            start_utc = start_cph.tz_convert('UTC')
            end_utc = target_cph.tz_convert('UTC') + pd.Timedelta(days=1)
            mask = (df['datetime_utc'] >= start_utc) & (df['datetime_utc'] < end_utc)
            df_filtered = df[mask]
            if df_filtered.empty: return None
            return df_filtered.set_index('datetime_utc').resample('h').mean(numeric_only=True)['price_kwh'].to_dict()
    except Exception as e:
        print(f"❌ Price API Error: {e}")
    return None

# --- 🌦️ THE REAL WEATHER FIX 🌦️ ---
def get_future_weather(area_name):
    print(f"🌤️ Fetching real weather forecast for {area_name}...")
    lat = 56.15 if area_name.upper() == 'DK1' else 55.67
    lon = 10.20 if area_name.upper() == 'DK1' else 12.56
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&hourly=wind_speed_10m,direct_radiation&timezone=Europe%2FCopenhagen"
    
    try:
        res = requests.get(url, timeout=15)
        if res.status_code == 200:
            data = res.json()
            df = pd.DataFrame({
                'datetime_local': pd.to_datetime(data['hourly']['time']),
                'wind_speed': data['hourly']['wind_speed_10m'],
                'solar_radiation': data['hourly']['direct_radiation']
            })
            df['datetime_utc'] = df['datetime_local'].dt.tz_localize('Europe/Copenhagen', ambiguous='NaT', nonexistent='shift_forward').dt.tz_convert('UTC')
            return df.set_index('datetime_utc')[['wind_speed', 'solar_radiation']].to_dict('index')
    except Exception as e:
        print(f"❌ Weather API Error: {e}")
    return None

def generate_full_day_forecast(area_name, engine, target_cph):
    model, scaler, feature_names, db_version = download_model_from_neon(area_name)
    if model is None: return None
    t = get_dynamic_thresholds(area_name, engine)
    
    history_end_cph = cph_now_floor - pd.Timedelta(hours=1)
    history = fetch_realtime_co2_api(area_name, history_end_cph)
    
    if not history:
        print("⚠️ API failed, falling back to database history...")
        query = f"SELECT datetime_utc, co2_emissions_g_kwh FROM processed_features WHERE price_area='{area_name.upper()}' AND is_forecast=FALSE ORDER BY datetime_utc DESC LIMIT 168"
        history = pd.read_sql(query, engine)['co2_emissions_g_kwh'].tolist()[::-1]

    prices = get_future_prices(area_name, cph_now_floor, target_cph)
    if not prices: 
        print(f"⚠️ Missing price data. Skipping {area_name}.")
        return None

    # Fetch the REAL weather for tomorrow!
    future_weather = get_future_weather(area_name)
    
    # Fallback just in case Open-Meteo goes offline
    weather_query = f"SELECT wind_speed, solar_radiation FROM processed_features WHERE price_area='{area_name.upper()}' AND is_forecast=FALSE ORDER BY datetime_utc DESC LIMIT 1"
    last_weather = pd.read_sql(weather_query, engine).iloc[0]
    
    dk_holidays = holidays.Denmark()
    preds = []

    end_of_tomorrow = target_cph + pd.Timedelta(hours=23)
    hours_to_predict = int((end_of_tomorrow - cph_now_floor).total_seconds() / 3600) + 1
    print(f"🔄 Running Recursive Forecast for {hours_to_predict} hours...")

    for h in range(hours_to_predict):
        time_local = cph_now_floor + pd.Timedelta(hours=h)
        time_utc = time_local.tz_convert('UTC')
        
        p = prices.get(time_utc, None)
        if p is None:
            p = (t['p33_price'] + t['p83_price']) / 2 
            
        # Get REAL weather for this exact hour
        if future_weather and time_utc in future_weather:
            current_weather = future_weather[time_utc]
        else:
            current_weather = {'wind_speed': last_weather['wind_speed'], 'solar_radiation': last_weather['solar_radiation']}
        
        feats = {
            'spot_price_dkk_kwh': p, 
            'wind_speed': current_weather['wind_speed'], 
            'solar_radiation': current_weather['solar_radiation'], 
            'hour': time_local.hour,
            'day_of_week': time_local.weekday(), 'month': time_local.month,
            'hour_sin': np.sin(2*np.pi*time_local.hour/24), 'hour_cos': np.cos(2*np.pi*time_local.hour/24),
            'is_holiday': 1 if time_local.date() in dk_holidays else 0, 
            'is_weekend': 1 if time_local.weekday()>=5 else 0,
            'co2_lag_1h': history[-1], 'co2_lag_2h': history[-2], 
            'co2_lag_24h': history[-24], 'co2_lag_168h': history[-168]
        }
        
        # --- 🛠️ APPLYING THE SCALER BEFORE PREDICTING 🛠️ ---
        raw_df = pd.DataFrame([feats])[feature_names] 
        scaled_array = scaler.transform(raw_df)       
        
        co2_pred = model.predict(scaled_array)[0]     
        co2_pred = max(0, co2_pred) 
        
        if time_local.date() == target_cph.date():
            price_norm = (p - t['p33_price']) / (t['p83_price'] - t['p33_price'] + 1e-6)
            co2_norm = (co2_pred - t['p33_co2']) / (t['p83_co2'] - t['p33_co2'] + 1e-6)

            price_norm = np.clip(price_norm, 0, 1.5)
            co2_norm = np.clip(co2_norm, 0, 1.5)

            score = 0.3 * price_norm + 0.7 * co2_norm
            
            if 17 <= time_local.hour <= 21:
                score += 0.2

            if score > 0.75:
                status = "AVOID"
            elif score < 0.3:
                status = "BEST"
            else:
                status = "CAUTION"

            preds.append({
                'datetime_utc': time_utc.to_pydatetime(), 
                'price_area': area_name, 
                'model_version': db_version, 
                'predicted_co2': float(co2_pred), 
                'market_price_dkk_kwh': p, 
                'recommendation_status': status
            })
            
        anchored_pred = 0.8 * co2_pred + 0.2 * np.mean(history[-24:])
        history.append(anchored_pred)

    df = pd.DataFrame(preds)
    
    if not df.empty:
        df['should_charge'] = False
        best_indices = df.nsmallest(6, 'predicted_co2').index
        df.loc[best_indices, 'should_charge'] = True
        
    return df

def run_job():
    engine = get_db_connection()
    for area in ['DK1', 'DK2']:
        df = generate_full_day_forecast(area, engine, TARGET_DATE_CPH)
        if df is not None and not df.empty:
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
            print(f"✅ {area} synced exactly 24 hours for {TARGET_DATE_CPH.date()}")

if __name__ == "__main__":
    run_job()
