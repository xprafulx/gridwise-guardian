import sys
import os
import numpy as np
import pandas as pd
from sqlalchemy import text
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from src.database.connection import get_db_connection
from src.utils.logger import setup_artifact_logger

# --- PATH FIX ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

def run_evaluation(area_name):
    print(f"\n🧪 EVALUATING PERFORMANCE FOR: {area_name}")
    engine = get_db_connection()
    
    # 🕰️ DANISH TIMEZONE ALIGNMENT (The Perfect Yesterday Fix)
    # 1. Get right now in Copenhagen
    cph_now = pd.Timestamp.now(tz='Europe/Copenhagen')
    
    # 2. Step back 1 day, and snap to Midnight (00:00:00)
    yesterday_cph = (cph_now - pd.Timedelta(days=1)).normalize()
    eval_date_str = yesterday_cph.strftime('%Y-%m-%d')
    
    # 3. Convert to UTC to safely query the database
    start_utc = yesterday_cph.tz_convert('UTC')
    end_utc = start_utc + pd.Timedelta(days=1) # Exactly 24 hours later
    
    print(f"📅 Target Date: {eval_date_str} | UTC Window: {start_utc.strftime('%H:%M')} to {end_utc.strftime('%H:%M')}")

    # Fetch exactly the 24 hours of "Yesterday"
    query = text("""
        SELECT 
            f.datetime_utc,
            f.predicted_co2,
            s.co2_emissions_g_kwh AS actual_co2,
            f.model_version
        FROM ai_forecasts f
        JOIN processed_features s 
            ON f.datetime_utc = s.datetime_utc 
            AND f.price_area = s.price_area
        WHERE f.price_area = :area
        AND f.datetime_utc >= :start_time
        AND f.datetime_utc < :end_time
        AND s.is_forecast = FALSE
    """)
    
    df = pd.read_sql(query, engine, params={
        "area": area_name,
        "start_time": start_utc.to_pydatetime(),
        "end_time": end_utc.to_pydatetime()
    })

    if df.empty or len(df) < 12:
        print(f"⚠️ Not enough data to evaluate {area_name} yet. (Waiting for actuals to sync)")
        return

    # 2. Calculate the "Evaluation Artifacts"
    mae = mean_absolute_error(df['actual_co2'], df['predicted_co2'])
    rmse = np.sqrt(mean_squared_error(df['actual_co2'], df['predicted_co2']))
    r2 = r2_score(df['actual_co2'], df['predicted_co2'])
    
    # Accuracy Percentage (Standardizing the miss relative to the mean)
    mean_actual = df['actual_co2'].mean()
    accuracy_pct = 100 - (mae / mean_actual * 100) if mean_actual != 0 else 0

    print(f"   - MAE  (Avg Miss): {mae:.2f} g/kWh")
    print(f"   - RMSE (Outliers): {rmse:.2f} g/kWh")
    print(f"   - R²   (Fit Score): {r2:.4f}")
    print(f"   - Accuracy Score: {accuracy_pct:.2f}%")

    # 3. Save the Artifacts to Neon
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO model_performance_history (eval_date, price_area, model_version, mae, rmse, r2)
            VALUES (:eval_date, :area, :version, :mae, :rmse, :r2)
            ON CONFLICT (eval_date, price_area, model_version) 
            DO UPDATE SET
                mae = EXCLUDED.mae, 
                rmse = EXCLUDED.rmse, 
                r2 = EXCLUDED.r2,
                eval_timestamp = CURRENT_TIMESTAMP;
        """), {
            "eval_date": eval_date_str, # Using our locked Python date, not DB time
            "area": area_name, 
            "version": df['model_version'].iloc[0],
            "mae": float(mae), 
            "rmse": float(rmse), 
            "r2": float(r2)
        })
    
    print(f"✅ Evaluation artifacts for {area_name} pushed to Neon.")
    
    # 4. DRIFT TRIGGER (Your MLOps logic)
    if accuracy_pct < 70:
        print(f"🚨 ALERT: Low accuracy detected for {area_name}! Consider manual review or retraining.")

if __name__ == "__main__":
    setup_artifact_logger("evaluate")
    
    for area in ['DK1', 'DK2']:
        try:
            run_evaluation(area)
        except Exception as e:
            print(f"❌ Critical error evaluating {area}: {e}")
