import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load variables from the .env file for local development
load_dotenv()

def get_db_connection():
    """
    Creates a SQLAlchemy connection engine to the Neon PostgreSQL database.
    """
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB")

    # Neon requires ?sslmode=require for secure cloud connections
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{db}?sslmode=require"
    
    # Create the engine with a pool_pre_ping to keep connections healthy
    engine = create_engine(connection_string, pool_pre_ping=True)
    return engine

if __name__ == "__main__":
    # Test script to verify connection
    try:
        engine = get_db_connection()
        with engine.connect() as conn:
            print("Successfully connected to the GreenHour database on Neon! 🟢")
    except Exception as e:
        print(f"❌ Connection failed: {e}")