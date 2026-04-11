import sys
import os
import io
import joblib
import pandas as pd
import numpy as np
import holidays
import requests
from datetime import datetime, timedelta, timezone
from src.database.connection import get_db_connection
from sqlalchemy import text 

# --- PATH FIX ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# --- CONFIG ---
# Snaps to the start of the current UTC day
TARGET_DATE = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
MODEL_VERSION = "xgb_v1_baseline"

def download_model_from_neon(area_name):
    """
    Retrieves the active model binary from Neon Model Registry.
    This allows the script to run without local .pkl files.
    """
    engine = get_db_connection()
    area_key = f"co2_{area_name.lower()}"
    
    print(f"📡 Downloading active model for {area_name} from Neon...")
    
    query = text("""
        SELECT model_binary 
        FROM model_registry 
        WHERE model_name = :name AND is_active = TRUE
        ORDER BY created_at DESC LIMIT 1
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query, {"name": area_key}).fetchone()
        
    if not result:
        print(f"❌ No active model found in Neon for {area_name}!")
        return None, None

    # Load the binary 'blob' into joblib via a buffer
    model_bytes = result[0]
    buffer = io.BytesIO(model_bytes)
    payload = joblib.load(buffer)
    
    print(f"✅ Model {area_name} loaded successfully.")
    return payload['model'], payload['features']

def find_time_column(df):
    """Safely finds a timestamp column regardless of API naming quirks."""
    candidates = ['HourUTC', 'Minutes5UTC', 'HourDK', 'Minutes5DK', 'TimeDK', 'ds']
    for col in candidates:
        if col in df.columns: return col
    return None

def get_future_prices(area_name, date):
    """Fetches Day-Ahead prices for the target date."""
    start_str = date.strftime('%Y-%m-%dT00:00')
    end_str = date.strftime('%Y-%m-%dT23:59')
    url = f"https://api.energidataservice.dk/dataset/DayAheadPrices?filter={{\"PriceArea\":[\"{area_name}\"]}}&start={start_str}&end={end_str}"
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            records = response.json().get('records', [])
            if not records: return None
            
            df = pd.DataFrame(records)
            time_col = find_time_column(df)
            if not time_col: return None
            
            df['datetime_utc'] = pd.to_datetime(df[time_col], utc=True)
            price_col = 'SpotPriceDKK' if 'SpotPriceDKK' in df.columns else 'DayAheadPriceDKK'
            df['price_kwh'] = pd.to_numeric(df[price_col], errors='coerce') / 1000
            
            return df.set_index('datetime_utc').resample('h').mean(numeric_only=True)['price_kwh'].to_dict()
    except Exception as e:
        print(f"❌ Price API Error: {e}")
    return None

def generate_full_day_forecast(area_name, engine, target_date):
    area_name = area_name.upper()
    
    # 📥 MLOps Fetch: Pull model from DB instead of local file
    model, feature_names = download_model_from_neon(area_name)
    
    if model is None: return None

    dk_holidays = holidays.Denmark()
    prices = get_future_prices(area_name, target_date)
    if not prices: return None

    # Pull history for Lag features from Silver Layer
    query = f"""
        SELECT datetime_utc, co2_emissions_g_kwh, wind_speed, solar_radiation 
        FROM processed_features 
        WHERE price_area = '{area_name}' AND is_forecast = FALSE
        ORDER BY datetime_utc DESC LIMIT 169
    """
    recent_data = pd.read_sql(query, engine)
    recent_data['datetime_utc'] = pd.to_datetime(recent_data['datetime_utc'], utc=True)
    history = recent_data['co2_emissions_g_kwh'].tolist()[::-1]
    last_weather = recent_data.iloc[0]

    predictions = []

    for hour in range(24):
        current_time = pd.to_datetime(target_date.replace(hour=hour))
        
        # Safely get price
        price_val = prices.get(current_time, 0)
        if price_val == 0 and prices:
            price_val = list(prices.values())[0]

        feats = {
            'spot_price_dkk_kwh': price_val, 
            'wind_speed': last_weather['wind_speed'],
            'solar_radiation': last_weather['solar_radiation'],
            'hour': hour,
            'day_of_week': current_time.weekday(),
            'month': current_time.month,
            'hour_sin': np.sin(2 * np.pi * hour / 24),
            'hour_cos': np.cos(2 * np.pi * hour / 24),
            'is_holiday': 1 if current_time.date() in dk_holidays else 0,
            'is_weekend': 1 if current_time.weekday() >= 5 else 0,
            'co2_lag_1h': history[-1],
            'co2_lag_2h': history[-2],
            'co2_lag_24h': history[-24],
            'co2_lag_168h': history[-168]
        }

        X = pd.DataFrame([feats])[feature_names]
        pred_co2 = model.predict(X)[0]
        
        predictions.append({
            'datetime_utc': current_time,
            'price_area': area_name,
            'model_version': MODEL_VERSION,
            'predicted_co2': float(pred_co2),
            'predicted_price_dkk_kwh': feats['spot_price_dkk_kwh']
        })
        history.append(pred_co2)

    df_preds = pd.DataFrame(predictions)
    # Smart logic: Set 'should_charge' for the 6 cleanest hours
    threshold = df_preds['predicted_co2'].nsmallest(6).max()
    df_preds['should_charge'] = df_preds['predicted_co2'] <= threshold
    
    return df_preds

def run_job():
    engine = get_db_connection()
    print(f"🚀 GENERATING SMART FORECASTS FOR: {TARGET_DATE.date()}")
    
    for area in ['DK1', 'DK2']:
        forecast_df = generate_full_day_forecast(area, engine, TARGET_DATE)
        
        if forecast_df is not None:
            # Sync to Gold Layer
            forecast_df.to_sql(f'temp_{area.lower()}', engine, if_exists='replace', index=False)
            upsert_query = text(f"""
                INSERT INTO ai_forecasts (datetime_utc, price_area, model_version, predicted_co2, predicted_price_dkk_kwh, should_charge)
                SELECT datetime_utc, price_area, model_version, predicted_co2, predicted_price_dkk_kwh, should_charge 
                FROM temp_{area.lower()}
                ON CONFLICT (datetime_utc, price_area, model_version) 
                DO UPDATE SET 
                    predicted_co2 = EXCLUDED.predicted_co2,
                    predicted_price_dkk_kwh = EXCLUDED.predicted_price_dkk_kwh,
                    should_charge = EXCLUDED.should_charge,
                    prediction_timestamp = CURRENT_TIMESTAMP;
            """)
            with engine.begin() as conn:
                conn.execute(upsert_query)
                conn.execute(text(f"DROP TABLE IF EXISTS temp_{area.lower()};"))
            print(f"✅ {area} forecasts synced to Neon.")

if __name__ == "__main__":
    run_job()