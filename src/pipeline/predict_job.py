import sys
import os
# --- PATH FIX: Ensures it finds the 'src' folder from the root ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import joblib
import pandas as pd
import numpy as np
import holidays
import requests
from datetime import datetime, timedelta
from src.database.connection import get_db_connection
from sqlalchemy import text 

# --- DYNAMIC CONFIGURATION ---
# Set days=0 for DEMO, days=1 for PRODUCTION automation
# Change days=0 to days=1
TARGET_DATE = (datetime.now() + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

def get_future_prices(area_name, date):
    """Fetches Day-Ahead prices using 2026 standards (15-min resampled to 1-hour)."""
    print(f"🌐 Fetching API prices for {area_name} on {date.date()}...")
    
    start_str = date.strftime('%Y-%m-%dT00:00')
    end_str = date.strftime('%Y-%m-%dT23:59')
    
    url = f"https://api.energidataservice.dk/dataset/DayAheadPrices?filter={{\"PriceArea\":[\"{area_name}\"]}}&start={start_str}&end={end_str}&sort=TimeDK ASC"
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            records = response.json().get('records', [])
            if not records:
                print(f"⚠️ No records found for {date.date()}. (Market results release at 13:00).")
                return None
            
            df = pd.DataFrame(records)
            df['TimeDK'] = pd.to_datetime(df['TimeDK'])
            df['DayAheadPriceDKK'] = pd.to_numeric(df['DayAheadPriceDKK'], errors='coerce')
            
            df = df.set_index('TimeDK').resample('h').mean(numeric_only=True).reset_index()
            df = df[df['TimeDK'].dt.date == date.date()]
            
            if len(df) < 24:
                print(f"⚠️ Only {len(df)}/24 hourly slots found.")
                return None
            
            return {r.TimeDK.hour: r.DayAheadPriceDKK/1000 for r in df.itertuples()}
    except Exception as e:
        print(f"❌ API Error: {str(e)}")
    return None

def get_season(month):
    return (month % 12 // 3)

def generate_full_day_forecast(area_name, engine, target_date):
    area_name = area_name.upper()
    
    # --- UPDATED: LOADING FROM THE 'LATEST' VAULT ---
    model_path = f"models/latest/{area_name.lower()}/model.pkl"
    feat_path = f"models/latest/{area_name.lower()}/features.pkl"
    
    if not os.path.exists(model_path):
        print(f"❌ Error: No model found at {model_path}. Run train_job.py first!")
        return None

    model = joblib.load(model_path)
    feature_names = joblib.load(feat_path)
    dk_holidays = holidays.Denmark()

    # 1. Inject API Prices
    prices = get_future_prices(area_name, target_date)
    if not prices: return None

    # 2. Fetch seed data for Lags (CO2 history)
    query = f"""
        SELECT co2_emissions_g_kwh, wind_speed, solar_radiation 
        FROM historical_training_data 
        WHERE price_area = '{area_name}' 
        AND ds < '{target_date.strftime('%Y-%m-%d %H:%M:%S')}'
        ORDER BY ds DESC LIMIT 169
    """
    recent_data = pd.read_sql(query, engine)
    history = recent_data['co2_emissions_g_kwh'].tolist()[::-1]
    last_weather = recent_data.iloc[0]

    predictions = []

    # 3. Forecasting Loop: 00:00 to 23:00
    for hour in range(24):
        current_time = target_date.replace(hour=hour)
        
        feats = {
            'spot_price_dkk_kwh': prices[hour], 
            'wind_speed': last_weather['wind_speed'],
            'solar_radiation': last_weather['solar_radiation'],
            'hour': hour,
            'day_of_week': current_time.weekday(),
            'month': current_time.month,
            'season': get_season(current_time.month),
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
            'forecast_time': current_time,
            'predicted_co2': float(pred_co2),
            'spot_price_dkk_kwh': prices[hour],
            'price_area': area_name,
            'generated_at': datetime.now()
        })
        history.append(pred_co2)

    return pd.DataFrame(predictions)

def run_job():
    engine = get_db_connection()
    print(f"\n🚀 GRIDWISE MASTER PLAN FOR: {TARGET_DATE.date()}")
    
    dk1 = generate_full_day_forecast('DK1', engine, TARGET_DATE)
    dk2 = generate_full_day_forecast('DK2', engine, TARGET_DATE)

    if dk1 is not None and dk2 is not None:
        for area_df, table_name in [(dk1, 'temp_dk1'), (dk2, 'temp_dk2')]:
            area_df.to_sql(table_name, engine, if_exists='replace', index=False)
            
            upsert_query = text(f"""
                INSERT INTO forecast_results (forecast_time, predicted_co2, spot_price_dkk_kwh, price_area, generated_at)
                SELECT forecast_time, predicted_co2, spot_price_dkk_kwh, price_area, generated_at FROM {table_name}
                ON CONFLICT (forecast_time, price_area) 
                DO UPDATE SET 
                    predicted_co2 = EXCLUDED.predicted_co2,
                    spot_price_dkk_kwh = EXCLUDED.spot_price_dkk_kwh,
                    generated_at = EXCLUDED.generated_at;
            """)
            with engine.begin() as conn:
                conn.execute(upsert_query)
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name};"))

        print("\n" + "="*65)
        print(f"{'TIME':<5} | {'DK1 CO2':<10} | {'DK1 PRICE':<10} | {'DK2 CO2':<10} | {'DK2 PRICE'}")
        print("-" * 65)
        for i in range(24):
            t = dk1.iloc[i]['forecast_time'].strftime('%H:%M')
            c1, p1 = dk1.iloc[i]['predicted_co2'], dk1.iloc[i]['spot_price_dkk_kwh']
            c2, p2 = dk2.iloc[i]['predicted_co2'], dk2.iloc[i]['spot_price_dkk_kwh']
            print(f"{t:<5} | {c1:>7.1f}g | {p1:>8.2f}kr | {c2:>7.1f}g | {p2:>8.2f}kr")
        print("="*65)
        print("💾 Success! Predictions synced with latest models.")

if __name__ == "__main__":
    run_job()