from src.database.connection import get_db_connection
from sqlalchemy import text

def lock_forecast_db():
    engine = get_db_connection()
    print("🔧 Fixing PostgreSQL constraints for Forecasts...")
    
    try:
        with engine.begin() as conn:
            # 1. Keep only the absolute newest prediction for each hour/area
            print("🧹 Cleaning up old prediction duplicates...")
            conn.execute(text("""
                DELETE FROM forecast_results
                WHERE (forecast_time, price_area, generated_at) NOT IN (
                    SELECT forecast_time, price_area, MAX(generated_at)
                    FROM forecast_results
                    GROUP BY forecast_time, price_area
                );
            """))
            
            # 2. Add the strict Unique Constraint
            print("🔒 Locking in the Unique Constraint...")
            conn.execute(text("""
                ALTER TABLE forecast_results 
                ADD CONSTRAINT forecast_time_area_unique UNIQUE (forecast_time, price_area);
            """))
            
        print("✅ Success! Forecast database is now secured.")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    lock_forecast_db()