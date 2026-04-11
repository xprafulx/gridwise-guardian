import os
import json
import pandas as pd
from datetime import datetime, timezone

from src.pipeline.predict_job import run_job as run_prediction
# We are removing the old recommendation_job import because predict_job now handles it!
from src.database.connection import get_db_connection

def export_static_api():
    """Generates the Static JSON API for the GitHub Pages UI"""
    print("\n" + "-"*40)
    print("🌐 Step 2: Building Static API for UI Sync...")
    
    engine = get_db_connection()

    # 1. Fetch the fresh forecast that Step 1 just generated
    # We include 'market_price_dkk_kwh' and 'recommendation_status'
    forecast_query = """
        SELECT datetime_utc, market_price_dkk_kwh, predicted_co2, price_area, recommendation_status
        FROM ai_forecasts 
        WHERE DATE(datetime_utc) >= CURRENT_DATE
        ORDER BY datetime_utc ASC
    """
    forecast_df = pd.read_sql(forecast_query, engine)

    if forecast_df.empty:
        print("⚠️ No data in ai_forecasts to export. Did the prediction fail?")
        return

    # 2. Format the data for the JavaScript frontend
    forecast_list = []
    for _, row in forecast_df.iterrows():
        forecast_list.append({
            "time": pd.to_datetime(row['datetime_utc']).strftime('%H:00'),
            "price": round(float(row['market_price_dkk_kwh']), 3),
            "co2": round(float(row['predicted_co2']), 1),
            "region": row['price_area'],
            "status": row['recommendation_status'] # Pushes 'GO', 'CAUTION', or 'AVOID' to your UI
        })

    # 3. Prepare the final payload
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "forecast": forecast_list
    }

    # 4. Save to docs/latest_forecast.json (Ensures path works in GitHub Actions)
    # This points to the docs folder in your root directory
    file_path = 'docs/latest_forecast.json'
    os.makedirs('docs', exist_ok=True)
    
    with open(file_path, 'w') as f:
        json.dump(payload, f, indent=4)

    print(f"✅ Static API JSON dumped successfully at: {file_path}")


def main():
    print("⚡️ GREENHOUR DAILY PIPELINE STARTED")
    print("="*40)
    
    # Step 1: Generate the numbers AND the advice (predict_job does both now)
    try:
        run_prediction()
    except Exception as e:
        print(f"❌ Prediction & Strategy Step Failed: {e}")
        return

    # Step 2: Export to JSON for GitHub Pages
    try:
        export_static_api()
    except Exception as e:
        print(f"❌ Static API Export Failed: {e}")
        return

    print("\n" + "="*40)
    print("✅ SUCCESS: Tomorrow's grid strategy is ready in Neon AND GitHub Docs!")

if __name__ == "__main__":
    main()
