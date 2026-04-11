import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load variables from the .env file for local development
load_dotenv()

def get_db_connection():
    """
    Creates a SQLAlchemy connection engine to the Neon PostgreSQL database.
    """
    # 1. Grab the master string from GitHub Secrets (or local .env)
    db_url = os.environ.get("DATABASE_URL")
    
    # 2. Hard stop if it's missing
    if not db_url:
        raise ValueError("❌ DATABASE_URL is completely missing from the environment!")
        
    # 3. SQLAlchemy strict formatting fix 
    # (Sometimes Neon gives 'postgres://' but SQLAlchemy demands 'postgresql://')
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
        
    # 4. Create the engine with a pool_pre_ping to keep connections healthy
    engine = create_engine(db_url, pool_pre_ping=True)
    return engine

if __name__ == "__main__":
    # Test script to verify connection
    try:
        engine = get_db_connection()
        with engine.connect() as conn:
            print("Successfully connected to the GreenHour database on Neon! 🟢")
    except Exception as e:
        print(f"❌ Connection failed: {e}")
