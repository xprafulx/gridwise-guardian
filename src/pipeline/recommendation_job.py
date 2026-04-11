import sys
import os
import pandas as pd
from sqlalchemy import text
from src.database.connection import get_db_connection

# --- PATH FIX ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

def get_dynamic_thresholds(engine, area, years_back=2):
    """Calculates statistical benchmarks from the Silver Layer."""
    query = f"""
        SELECT spot_price_dkk_kwh, co2_emissions_g_kwh 
        FROM processed_features 
        WHERE price_area = '{area}' AND is_forecast = FALSE
        AND datetime_utc >= NOW() - INTERVAL '{years_back} YEARS'
    """
    df = pd.read_sql(query, engine)
    if df.empty:
        return {'q1_price': 0.5, 'q3_price': 2.0, 'q1_co2': 50, 'q3_co2': 150}
    
    return {
        'q1_price': df['spot_price_dkk_kwh'].quantile(0.33),
        'q3_price': df['spot_price_dkk_kwh'].quantile(0.83),
        'q1_co2': df['co2_emissions_g_kwh'].quantile(0.33),
        'q3_co2': df['co2_emissions_g_kwh'].quantile(0.83)
    }

def apply_regional_logic(row, thresholds_dk1, thresholds_dk2):
    """🟢 BEST | 🟡 CAUTION | 🔴 AVOID"""
    area = row['price_area']
    hour = pd.to_datetime(row['datetime_utc']).hour
    price = row['predicted_price_dkk_kwh']
    co2 = row['predicted_co2']

    t = thresholds_dk1 if area == 'DK1' else thresholds_dk2

    if (17 <= hour <= 21) or (price > t['q3_price']) or (co2 > t['q3_co2']): 
        return "AVOID"
    elif (price < t['q1_price']) and (co2 < t['q1_co2']): 
        return "BEST"
    else: 
        return "CAUTION"

def run_recommendation_engine():
    engine = get_db_connection()
    
    # 1. Get Benchmarks
    dk1_t = get_dynamic_thresholds(engine, 'DK1')
    dk2_t = get_dynamic_thresholds(engine, 'DK2')
    
    # 2. Get the latest forecasts (Gold Layer)
    query = "SELECT * FROM ai_forecasts ORDER BY datetime_utc ASC"
    df = pd.read_sql(query, engine)
    
    if df.empty:
        print("⚠️ No forecasts found.")
        return

    # 3. Calculate Recommendations
    df['recommendation_status'] = df.apply(lambda row: apply_regional_logic(row, dk1_t, dk2_t), axis=1)

    # 4. 💾 UPSERT recommendations back to Neon
    print("📤 Saving recommendations to Gold Layer...")
    
    # We use a temp table for the update
    df[['datetime_utc', 'price_area', 'model_version', 'recommendation_status']].to_sql('temp_recs', engine, if_exists='replace', index=False)
    
    update_query = text("""
        UPDATE ai_forecasts f
        SET recommendation_status = t.recommendation_status
        FROM temp_recs t
        WHERE f.datetime_utc = t.datetime_utc 
        AND f.price_area = t.price_area 
        AND f.model_version = t.model_version;
    """)
    
    with engine.begin() as conn:
        conn.execute(update_query)
        conn.execute(text("DROP TABLE IF EXISTS temp_recs;"))

    print("✅ Recommendations synced. The 'Guardian' has spoken.")

if __name__ == "__main__":
    run_recommendation_engine()