import sys
import os
import io
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

# --- CONFIG ---
TARGET_DATE = (datetime.now(timezone.utc) + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

def get_historical_thresholds(area_name, engine):
    """Calculates 33% and 83% quantiles from 2 years of history."""
    query = f"""
        SELECT co2_emissions_g_kwh 
        FROM processed_features 
        WHERE price_area = '{area_name.upper()}' 
        AND is_forecast = FALSE
        AND datetime_utc >= CURRENT_DATE - INTERVAL '2 years'
    """
    try:
        hist_df = pd.read_sql(query, engine)
        if hist_df.empty:
            return 50.0, 150.0
        return float(hist_df['co2_emissions_g_kwh'].quantile(0.33)), float(hist_df['co2_emissions_g_kwh'].quantile(0.83))
    except:
        return 50.0, 150.0

def download_model_from_neon(area_name):
    engine = get_db_connection()
    area_key = f"co2_{area_name.lower()}"
    query = text("""
        SELECT model_binary, model_version FROM model_registry 
        WHERE model_name = :name AND is_active = TRUE 
        ORDER BY created_at DESC LIMIT 1
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"name": area_key}).fetchone()
    if not result: return None, None, None
    buffer = io.BytesIO(result[0])
    payload = joblib.load(buffer)
    return payload['model'], payload['features'], result[1]

def get_future_prices(area_name, date):
    url = f"https://api.energidataservice.dk/dataset/DayAheadPrices?filter={{\"PriceArea\":[\"{area_name}\"]}}&start={date.strftime('%Y-%m-%dT00:00')}&end={date.strftime('%Y-%m-%dT23:59')}"
    try:
        res = requests.get(url, timeout=10)
        if res.status_code == 200:
            df = pd.DataFrame(res.json().get('records', []))
            time_col = next((c for c in ['HourUTC', 'HourDK', 'ds'] if c in df.columns), None)
            df['datetime_utc'] = pd.to_datetime(df[time_col], utc=True)
            p_col = 'SpotPriceDKK' if 'SpotPriceDKK' in df.columns else 'DayAheadPriceDKK'
            df['price_kwh'] = pd.to_numeric(df[p_col]) / 1000
            return df.set_index('datetime_utc').resample('h').mean(numeric_only=True)['price_kwh'].to_dict()
    except: return None

def generate_full_day_forecast(area_name, engine, target_date):
    model, feature_names, db_version = download_model_from_neon(area_name)
    if model is None: return None
    low_t, high_t = get_historical_thresholds(area_name, engine)
    prices = get_future_prices(area_name, target_date)
    if not prices: return None

    query = f"SELECT datetime_utc, co2_emissions_g_kwh, wind_speed, solar_radiation FROM processed_features WHERE price_area='{area_name.upper()}' AND is_forecast=FALSE ORDER BY datetime_utc DESC LIMIT 169"
    recent = pd.read_sql(query, engine)
    history = recent['co2_emissions_g_kwh'].tolist()[::-1]
    last_weather = recent.iloc[0]
    dk_holidays = holidays.Denmark()

    preds = []
    for h in range(24):
        t = pd.to_datetime(target_date.replace(hour=h))
        p = prices.get(t, list(prices.values())[0])
        feats = {
            'spot_price_dkk_kwh': p, 'wind_speed': last_weather['wind_speed'],
            'solar_radiation': last_weather['solar_radiation'], 'hour': h,
            'day_of_week': t.weekday(), 'month': t.month,
            'hour_sin': np.sin(2*np.pi*h/24), 'hour_cos': np.cos(2*np.pi*h/24),
            'is_holiday': 1 if t.date() in dk_holidays else 0, 'is_weekend': 1 if t.weekday()>=5 else 0,
            'co2_lag_1h': history[-1], 'co2_lag_2h': history[-2], 'co2_lag_24h': history[-24], 'co2_lag_168h': history[-168]
        }
        pred = model.predict(pd.DataFrame([feats])[feature_names])[0]
        preds.append({'datetime_utc': t, 'price_area': area_name, 'model_version': db_version, 'predicted_co2': float(pred), 'market_price_dkk_kwh': p})
        history.append(pred)

    df = pd.DataFrame(preds)
    df['recommendation_status'] = df['predicted_co2'].apply(lambda x: "GO" if x<=low_t else ("AVOID" if x>=high_t else "CAUTION"))
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
            print(f"✅ {area} synced (Model: {df['model_version'].iloc[0]})")

if __name__ == "__main__":
    run_job()
