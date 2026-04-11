import sys
import os
import json
import pandas as pd
from datetime import datetime, timezone

# --- PATH FIX ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from src.pipeline.predict_job import run_job as run_prediction
from src.database.connection import get_db_connection

def export_static_api():
    print("\n" + "-"*40)
    print("🌐 Step 2: Building Static API for UI Sync...")
    
    engine = get_db_connection()

    # Grab the last 12 hours + future to handle the midnight rollover
    forecast_query = """
        SELECT datetime_utc, market_price_dkk_kwh, predicted_co2, price_area, recommendation_status
        FROM ai_forecasts 
        WHERE datetime_utc >= NOW() - INTERVAL '12 hours'
        ORDER BY datetime_utc ASC
    """
    
    try:
        forecast_df = pd.read_sql(forecast_query, engine)
    except Exception as e:
        print(f"❌ Database Query Failed: {e}")
        return

    if forecast_df.empty:
        print("⚠️ No data in ai_forecasts to export.")
        return

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

    os.makedirs('docs', exist_ok=True)
    file_path = 'docs/latest_forecast.json'
    
    with open(file_path, 'w') as f:
        json.dump(payload, f, indent=4)

    print(f"✅ Static API JSON dumped successfully at: {file_path}")

def main():
    print("⚡️ GREENHOUR DAILY PIPELINE STARTED")
    print("="*40)
    
    try:
        run_prediction()
        export_static_api()
    except Exception as e:
        print(f"❌ Pipeline Failed: {e}")
        return

    print("\n" + "="*40)
    print("✅ SUCCESS: The Guardian strategy is synced to Neon and GitHub!")

if __name__ == "__main__":
    main()    file_path = os.path.join(docs_dir, 'latest_forecast.json')
    with open(file_path, 'w') as f:
        json.dump(payload, f, indent=4)

    print(f"✅ Static API JSON dumped successfully at: {file_path}")


def main():
    print("⚡️ GREENHOUR DAILY PIPELINE STARTED")
    print("="*40)
    
    # STEP 1: Predict & Recommend (Using your Q1/Q3 regional logic)
    try:
        run_prediction()
    except Exception as e:
        print(f"❌ Pipeline Failed at Prediction Step: {e}")
        return

    # STEP 2: Export JSON for GitHub Pages
    try:
        export_static_api()
    except Exception as e:
        print(f"❌ Pipeline Failed at JSON Export: {e}")
        return

    print("\n" + "="*40)
    print("✅ SUCCESS: The Guardian strategy is synced to Neon and GitHub!")

if __name__ == "__main__":
    main()
