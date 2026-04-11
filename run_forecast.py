# run_forecast.py
import sys
import os
import json
import pandas as pd
from datetime import datetime, timezone
# --- PATH FIX ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.pipeline.predict_job import run_job as run_prediction
from src.database.connection import get_db_connection

def export_static_api():
    """Generates the Static JSON API for the GitHub Pages UI"""
    print("\n" + "-"*40)
    print("🌐 Step 2: Building Static API for UI Sync...")
    
    engine = get_db_connection()

    # Fetch the fresh forecast - includes the GO/AVOID status we just generated
    forecast_query = """
        SELECT datetime_utc, market_price_dkk_kwh, predicted_co2, price_area, recommendation_status
        FROM ai_forecasts 
        WHERE DATE(datetime_utc) >= CURRENT_DATE
        ORDER BY datetime_utc ASC
    """
    forecast_df = pd.read_sql(forecast_query, engine)

    if forecast_df.empty:
        print("⚠️ No data in ai_forecasts to export.")
        return

    # Format for the JS frontend
    forecast_list = []
    for _, row in forecast_df.iterrows():
        forecast_list.append({
            "time": pd.to_datetime(row['datetime_utc']).strftime('%H:00'),
            "price": round(float(row['market_price_dkk_kwh']), 3),
            "co2": round(float(row['predicted_co2']), 1),
            "region": row['price_area'],
            "status": row['recommendation_status'] 
        })

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "forecast": forecast_list
    }

    # Save to docs folder
    os.makedirs('docs', exist_ok=True)
    file_path = 'docs/latest_forecast.json'
    with open(file_path, 'w') as f:
        json.dump(payload, f, indent=4)

    print(f"✅ Static API JSON dumped successfully at: {file_path}")


def main():
    print("⚡️ GREENHOUR DAILY PIPELINE STARTED")
    print("="*40)
    
    # STEP 1: Predict & Recommend (Done together in predict_job)
    try:
        run_prediction()
    except Exception as e:
        print(f"❌ Pipeline Failed at Prediction/Strategy: {e}")
        return

    # STEP 2: Export
    try:
        export_static_api()
    except Exception as e:
        print(f"❌ Pipeline Failed at JSON Export: {e}")
        return

    print("\n" + "="*40)
    print("✅ SUCCESS: The Guardian has updated the grid strategy!")

if __name__ == "__main__":
    main()
