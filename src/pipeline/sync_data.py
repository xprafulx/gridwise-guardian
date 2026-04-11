import pandas as pd
from sqlalchemy import text
from src.database.connection import get_db_connection

def sync_huggingface_to_postgres():
    # 1. The Raw URL from Hugging Face
    hf_url = "https://huggingface.co/datasets/appleballcay/denmark-grid-co2-2021-2026/raw/main/final_master_dataset.csv"
    
    print(f"📥 Downloading master dataset from Hugging Face...")
    try:
        df = pd.read_csv(hf_url)
        
        # --- FIX 1: Align columns with your Medallion Schema ---
        # Map 'ds' to 'datetime_utc' and ensure UTC timezone awareness
        df = df.rename(columns={'ds': 'datetime_utc'})
        df['datetime_utc'] = pd.to_datetime(df['datetime_utc'], utc=True)
        
        # Add the MLOps flag (Historical data is not a forecast)
        df['is_forecast'] = False
        
        print(f"✅ Downloaded {len(df)} rows. Standardizing for Neon...")

        # 2. Connect to Neon
        engine = get_db_connection()
        
        # --- FIX 2: Use the 'Temp Table' Strategy to preserve Schema ---
        # We push to a temporary table first
        df.to_sql('temp_master_import', engine, if_exists='replace', index=False)
        
        # Now we move data into the official table using a proper SQL UPSERT
        # This keeps our PRIMARY KEY (datetime_utc, price_area) and TIMESTAMPTZ intact
        upsert_query = text("""
            INSERT INTO processed_features (
                datetime_utc, price_area, co2_emissions_g_kwh, 
                spot_price_dkk_kwh, wind_speed, solar_radiation, is_forecast
            )
            SELECT 
                datetime_utc, price_area, co2_emissions_g_kwh, 
                spot_price_dkk_kwh, wind_speed, solar_radiation, is_forecast 
            FROM temp_master_import
            ON CONFLICT (datetime_utc, price_area) DO NOTHING;
        """)
        
        print("📤 Syncing to 'processed_features' (Silver Layer)...")
        with engine.begin() as conn:
            conn.execute(upsert_query)
            conn.execute(text("DROP TABLE IF EXISTS temp_master_import;"))
        
        # 3. Final verification
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM processed_features"))
            count = result.scalar()
            
        print(f"🎉 SUCCESS! Neon now contains {count} rows of clean, historical grid data.")
        
    except Exception as e:
        print(f"❌ Error during sync: {e}")

if __name__ == "__main__":
    sync_huggingface_to_postgres()