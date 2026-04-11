import pandas as pd
import requests
from datetime import datetime, timedelta, timezone
from src.database.connection import get_db_connection
from sqlalchemy import text  

# --- CONFIG ---
LOCATIONS = {
    'DK1': {'lat': 57.0488, 'lon': 9.9187},
    'DK2': {'lat': 55.6761, 'lon': 12.5683}
}

def get_weather_data(lat, lon, days_back=3):
    url = (f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}"
           f"&hourly=wind_speed_10m,shortwave_radiation&past_days={days_back}&timezone=UTC")
    try:
        res = requests.get(url, timeout=10)
        if res.status_code != 200: return pd.DataFrame()
        data = res.json()
        return pd.DataFrame({
            'datetime_utc': pd.to_datetime(data['hourly']['time'], utc=True),
            'wind_speed': data['hourly']['wind_speed_10m'],
            'solar_radiation': data['hourly']['shortwave_radiation']
        })
    except: return pd.DataFrame()

def find_time_column(df):
    """Safely finds a timestamp column regardless of API naming quirks."""
    candidates = ['HourUTC', 'Minutes5UTC', 'HourDK', 'Minutes5DK', 'TimeDK', 'ds']
    for col in candidates:
        if col in df.columns: return col
    return None

def ingest_job():
    engine = get_db_connection()
    
    # --- TIME LOCK (Modern UTC) ---
    now = datetime.now(timezone.utc) 
    start_date = (now - timedelta(days=3)).strftime('%Y-%m-%dT%H:%M')
    end_date = (now - timedelta(days=1)).strftime('%Y-%m-%dT23:59')
    
    print(f"📥 Automated Sync: Fetching records from {start_date} to {end_date}...")

    try:
        # --- 1. FETCH CO2 ---
        co2_res = requests.get("https://api.energidataservice.dk/dataset/CO2Emis", 
                               params={'start': start_date, 'end': end_date, 'filter': '{"PriceArea":["DK1","DK2"]}'})
        df_co2 = pd.DataFrame(co2_res.json().get('records', []))
        if df_co2.empty: 
            print("⚠️ No CO2 records found.")
            return
        
        time_col = find_time_column(df_co2)
        df_co2['datetime_utc'] = pd.to_datetime(df_co2[time_col], utc=True)
        df_co2 = df_co2.set_index('datetime_utc').groupby('PriceArea').resample('h').mean(numeric_only=True).reset_index()

        # --- 2. FETCH PRICES ---
        price_res = requests.get("https://api.energidataservice.dk/dataset/DayAheadPrices", 
                                 params={'start': start_date, 'end': end_date, 'filter': '{"PriceArea":["DK1","DK2"]}'})
        df_price = pd.DataFrame(price_res.json().get('records', []))
        if df_price.empty:
            print("⚠️ No Price records found.")
            return

        time_col_p = find_time_column(df_price)
        df_price['datetime_utc'] = pd.to_datetime(df_price[time_col_p], utc=True)
        # Handle Potential naming differences for Price column
        price_col = 'SpotPriceDKK' if 'SpotPriceDKK' in df_price.columns else 'DayAheadPriceDKK'
        df_price = df_price.set_index('datetime_utc').groupby('PriceArea').resample('h').mean(numeric_only=True).reset_index()

        # --- 3. MERGE & WEATHER ---
        merged_core = pd.merge(df_co2, df_price, on=['datetime_utc', 'PriceArea'])
        
        final_list = []
        for area in ['DK1', 'DK2']:
            area_data = merged_core[merged_core['PriceArea'] == area].copy()
            weather_df = get_weather_data(LOCATIONS[area]['lat'], LOCATIONS[area]['lon'])
            if not weather_df.empty:
                final_list.append(pd.merge(area_data, weather_df, on='datetime_utc', how='left'))
        
        if not final_list: return
        final_df = pd.concat(final_list)

        # --- 4. FINAL FORMATTING ---
        final_df = final_df.rename(columns={
            'PriceArea': 'price_area',
            'CO2Emission': 'co2_emissions_g_kwh',
            'DayAheadPriceDKK': 'spot_price_dkk_kwh',
            'SpotPriceDKK': 'spot_price_dkk_kwh' # Catch both naming styles
        })
        final_df['is_forecast'] = False 

        # Keep only what matches our schema
        cols = ['datetime_utc', 'price_area', 'co2_emissions_g_kwh', 'spot_price_dkk_kwh', 'wind_speed', 'solar_radiation', 'is_forecast']
        final_df = final_df[[c for c in cols if c in final_df.columns]]

        # --- 5. UPSERT INTO NEON ---
        final_df.to_sql('temp_ingest', engine, if_exists='replace', index=False)
        upsert_query = text("""
            INSERT INTO processed_features (datetime_utc, price_area, co2_emissions_g_kwh, spot_price_dkk_kwh, wind_speed, solar_radiation, is_forecast)
            SELECT datetime_utc, price_area, co2_emissions_g_kwh, spot_price_dkk_kwh, wind_speed, solar_radiation, is_forecast FROM temp_ingest
            ON CONFLICT (datetime_utc, price_area) DO UPDATE SET 
                co2_emissions_g_kwh = EXCLUDED.co2_emissions_g_kwh,
                spot_price_dkk_kwh = EXCLUDED.spot_price_dkk_kwh,
                wind_speed = EXCLUDED.wind_speed,
                solar_radiation = EXCLUDED.solar_radiation;
        """)
        
        with engine.begin() as conn:
            conn.execute(upsert_query)
            conn.execute(text("DROP TABLE IF EXISTS temp_ingest;"))
            
        print(f"✅ Success! {len(final_df)} hours synced to Neon.")

    except Exception as e:
        print(f"❌ Ingestion Error: {str(e)}")

if __name__ == "__main__":
    ingest_job()