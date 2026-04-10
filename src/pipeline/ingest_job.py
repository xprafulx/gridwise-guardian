import pandas as pd
import requests
import numpy as np
from datetime import datetime, timedelta
from src.database.connection import get_db_connection
from sqlalchemy import text  

# --- COORDINATES FOR WEATHER ---
LOCATIONS = {
    'DK1': {'lat': 57.0488, 'lon': 9.9187},  # Aalborg
    'DK2': {'lat': 55.6761, 'lon': 12.5683}  # Copenhagen
}

def get_weather_data(lat, lon, days_back=3):
    """Fetches actual/recent weather from Open-Meteo forecast API."""
    url = (
        f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}"
        f"&hourly=wind_speed_10m,shortwave_radiation&past_days={days_back}"
        f"&timezone=Europe%2FCopenhagen"
    )
    
    try:
        res = requests.get(url, timeout=10)
        
        if res.status_code != 200:
            print(f"⚠️ Weather API Status {res.status_code}: {res.text[:100]}")
            return pd.DataFrame()
            
        data = res.json()
        df = pd.DataFrame({
            'ds': pd.to_datetime(data['hourly']['time']),
            'wind_speed': data['hourly']['wind_speed_10m'],
            'solar_radiation': data['hourly']['shortwave_radiation']
        })
        return df
    except Exception as e:
        print(f"❌ Weather API Error: {e}")
        return pd.DataFrame()

def ingest_job():
    engine = get_db_connection()
    
    # --- TIME LOCK ---
    # Define EXACT window: 3 days ago up to exactly 23:59 yesterday
    now = datetime.now()
    start_date = (now - timedelta(days=3)).strftime('%Y-%m-%dT%H:%M')
    end_date = (now - timedelta(days=1)).strftime('%Y-%m-%dT23:59') # Locks to yesterday 11:59 PM
    
    print(f"📥 Automated Sync: Fetching records from {start_date} to {end_date}...")

    try:
        # --- 1. FETCH CO2 (Using CO2Emis and locked dates) ---
        co2_base_url = "https://api.energidataservice.dk/dataset/CO2Emis"
        co2_params = {
            'start': start_date,
            'end': end_date,
            'filter': '{"PriceArea":["DK1","DK2"]}'
        }
        co2_res = requests.get(co2_base_url, params=co2_params, timeout=15)
        
        if co2_res.status_code != 200:
            raise ValueError(f"CO2 API failed (Status {co2_res.status_code}): {co2_res.text[:100]}")
            
        co2_data = co2_res.json().get('records', [])
        df_co2 = pd.DataFrame(co2_data)
        
        if df_co2.empty:
            print("⚠️ CO2 data is empty right now. Skipping insertion.")
            return

        df_co2['ds'] = pd.to_datetime(df_co2['Minutes5DK'])
        df_co2 = df_co2.set_index('ds').groupby('PriceArea').resample('h').mean(numeric_only=True).reset_index()


        # --- 2. FETCH PRICES (Using DayAheadPrices and locked dates) ---
        price_base_url = "https://api.energidataservice.dk/dataset/DayAheadPrices"
        price_params = {
            'start': start_date,
            'end': end_date,
            'filter': '{"PriceArea":["DK1","DK2"]}'
        }
        price_res = requests.get(price_base_url, params=price_params, timeout=15)
        
        if price_res.status_code != 200:
            raise ValueError(f"Price API failed (Status {price_res.status_code}): {price_res.text[:100]}")
            
        price_data = price_res.json().get('records', [])
        df_price = pd.DataFrame(price_data)
        
        if df_price.empty:
            print("⚠️ Price data is empty right now. Skipping insertion.")
            return

        df_price['ds'] = pd.to_datetime(df_price['TimeDK'])
        df_price['DayAheadPriceDKK'] = pd.to_numeric(df_price['DayAheadPriceDKK'], errors='coerce')
        df_price = df_price.set_index('ds').groupby('PriceArea').resample('h').mean(numeric_only=True).reset_index()


        # --- 3. MERGE CORE DATA ---
        merged_core = pd.merge(df_co2, df_price, on=['ds', 'PriceArea'])


        # --- 4. ATTACH WEATHER DATA PER AREA ---
        final_list = []
        for area in ['DK1', 'DK2']:
            area_data = merged_core[merged_core['PriceArea'] == area].copy()
            weather_df = get_weather_data(LOCATIONS[area]['lat'], LOCATIONS[area]['lon'])
            
            if not weather_df.empty:
                area_combined = pd.merge(area_data, weather_df, on='ds', how='left')
                final_list.append(area_combined)
        
        if not final_list:
             print("⚠️ Missing weather data. Skipping database upload.")
             return
             
        final_df = pd.concat(final_list)


        # --- 5. FORMAT TO MATCH SCHEMA ---
        final_df = final_df.rename(columns={
            'PriceArea': 'price_area',
            'CO2Emission': 'co2_emissions_g_kwh',
            'DayAheadPriceDKK': 'spot_price_dkk_kwh'
        })[['ds', 'price_area', 'spot_price_dkk_kwh', 'co2_emissions_g_kwh', 'wind_speed', 'solar_radiation']]


        # --- 6. UPSERT INTO DATABASE ---
        final_df.to_sql('temp_ingest', engine, if_exists='replace', index=False)
        
        upsert_query = text("""
            INSERT INTO historical_training_data (ds, price_area, spot_price_dkk_kwh, co2_emissions_g_kwh, wind_speed, solar_radiation)
            SELECT ds, price_area, spot_price_dkk_kwh, co2_emissions_g_kwh, wind_speed, solar_radiation FROM temp_ingest
            ON CONFLICT (ds, price_area) DO NOTHING;
        """)
        
        with engine.begin() as conn:
            conn.execute(upsert_query)
            conn.execute(text("DROP TABLE IF EXISTS temp_ingest;"))
            
        print(f"✅ Success! Ingested {len(final_df)} hours of clean, locked historical data.")

    except Exception as e:
        print(f"❌ Ingestion Error: {str(e)}")

if __name__ == "__main__":
    ingest_job()