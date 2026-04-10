import pandas as pd
from sqlalchemy import text
from src.database.connection import get_db_connection

def sync_huggingface_to_postgres():
    # 1. The Raw URL from your Hugging Face repo
    hf_url = "https://huggingface.co/datasets/appleballcay/denmark-grid-co2-2021-2026/raw/main/final_master_dataset.csv"
    
    print(f"📥 Downloading master dataset from Hugging Face...")
    try:
        # Read directly from the URL into a DataFrame
        df = pd.read_csv(hf_url)
        
        # Ensure the time column is in the correct datetime format for Postgres
        df['ds'] = pd.to_datetime(df['ds'])
        
        print(f"✅ Successfully downloaded {len(df)} rows.")
        
        # 2. Connect to your local PostgreSQL
        print("🚀 Connecting to database...")
        engine = get_db_connection()
        
        # 3. Push to the historical_training_data table
        # We use 'replace' to ensure the table matches your clean master file perfectly
        print("📤 Pushing data to 'historical_training_data' table...")
        df.to_sql('historical_training_data', engine, if_exists='replace', index=False)
        
        # 4. Final verification
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM historical_training_data"))
            count = result.scalar()
            
        print(f"🎉 SUCCESS! Database now contains {count} rows of clean grid data.")
        
    except Exception as e:
        print(f"❌ Error during sync: {e}")

if __name__ == "__main__":
    sync_huggingface_to_postgres()