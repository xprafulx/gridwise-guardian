import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load variables from the .env file
load_dotenv()

def get_db_connection():
    """
    Creates a connection engine to the PostgreSQL database.
    """
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    db = os.getenv("POSTGRES_DB")

    # The Connection String (The 'Address' of your DB)
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    
    # Create the engine
    engine = create_engine(connection_string)
    return engine

if __name__ == "__main__":
    # Quick test to see if it works
    try:
        engine = get_db_connection()
        with engine.connect() as conn:
            print("Successfully connected to the GreenHour database! 🟢")
    except Exception as e:
        print(f"Connection failed: {e}")