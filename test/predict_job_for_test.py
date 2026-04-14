import os
import joblib
import pandas as pd
import numpy as np
import holidays
import requests
from datetime import datetime, timedelta
from src.database.connection import get_db_connection

# --- CONFIGURATION ---
# Simulation mode: April 8 predicting for April 9
SIMULATED_TODAY = datetime(2026, 4, 8) 
TARGET_DATE = SIMULATED_TODAY + timedelta(days=1) 

def get_future_prices(area_name, date):
    """Fetches Day-Ahead prices from the 2026 Energinet DataHub."""
    print(f"🌐 Fetching API prices for {area_name} on {date.date()}...")
    start_str = date.strftime('%Y-%m-%dT00:00')
    end_str = date.strftime('%Y-%m-%dT23:59')
    
    url = f"https://api.energidataservice.dk/dataset/DayAheadPrices?filter={{\"PriceArea\":[\"{area_name}\"]}}&start={start_str}&end={end_str}&sort=TimeDK ASC"
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            records = response.json().get('records', [])
            if not records: return None
            
            df = pd.DataFrame(records)
            df['TimeDK'] = pd.to_datetime(df['TimeDK'])
            df['DayAheadPriceDKK'] = pd.to_numeric(df['DayAheadPriceDKK'], errors='coerce')
            
            # Resample 15-min to 1-hour
            df = df.set_index('TimeDK').resample('h').mean(numeric_only=True).reset_index()
            df = df[df['TimeDK'].dt.date == date.date()]
            
            if len(df) < 24: return None
            return {r.TimeDK.hour: r.DayAheadPriceDKK/1000 for r in df.itertuples()}
    except Exception as e:
        print(f"❌ API Error: {str(e)}")
    return None

def get_season(month):
    return (month % 12 // 3)

def generate_full_day_forecast(area_name, engine, target_date):
    area_name = area_name.upper()
    model = joblib.load(f'models/xgb_co2_{area_name.lower()}_tuned.pkl')
    feature_names = joblib.load(f'models/features_{area_name.lower()}.pkl')
    dk_holidays = holidays.Denmark()

    prices = get_future_prices(area_name, target_date)
    if not prices: return None

    # Fetch seed data for Lags (most recent readings before target date)
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
    for hour in range(24):
        current_time = target_date.replace(hour=hour)
        feats = {
            'spot_price_dkk_kwh': prices[hour], 
            'wind_speed': last_weather['wind_speed'],
            'solar_radiation': last_weather['solar_radiation'],
            'hour': hour, 'day_of_week': current_time.weekday(),
            'month': current_time.month, 'season': get_season(current_time.month),
            'hour_sin': np.sin(2 * np.pi * hour / 24), 'hour_cos': np.cos(2 * np.pi * hour / 24),
            'is_holiday': 1 if current_time.date() in dk_holidays else 0,
            'is_weekend': 1 if current_time.weekday() >= 5 else 0,
            'co2_lag_1h': history[-1], 'co2_lag_2h': history[-2],
            'co2_lag_24h': history[-24], 'co2_lag_168h': history[-168]
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
        # Save results to Database
        # Note: Using 'replace' once to clean up the old schema (removing trigger columns)
        dk1.to_sql('forecast_results', engine, if_exists='replace', index=False)
        dk2.to_sql('forecast_results', engine, if_exists='append', index=False)

        # UI Table Output (Cleaned of Trigger logic)
        print("\n" + "="*65)
        print(f"{'TIME':<5} | {'DK1 CO2':<10} | {'DK1 PRICE':<10} | {'DK2 CO2':<10} | {'DK2 PRICE'}")
        print("-" * 65)
        for i in range(24):
            t = dk1.iloc[i]['forecast_time'].strftime('%H:%M')
            c1, p1 = dk1.iloc[i]['predicted_co2'], dk1.iloc[i]['spot_price_dkk_kwh']
            c2, p2 = dk2.iloc[i]['predicted_co2'], dk2.iloc[i]['spot_price_dkk_kwh']
            
            print(f"{t:<5} | {c1:>7.1f}g | {p1:>8.2f}kr | {c2:>7.1f}g | {p2:>8.2f}kr")
        print("="*65)
        print("💾 Success! Full 24-hour forecast logged to 'forecast_results' table.")

if __name__ == "__main__":
    run_job()