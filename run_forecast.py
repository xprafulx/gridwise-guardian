# run_forecast.py
import os
import json
import pandas as pd
from datetime import datetime

from src.pipeline.predict_job import run_job as run_prediction
from src.pipeline.recommendation_job import run_recommendation_engine as run_recommendation
from src.database.connection import get_db_connection

def export_static_api():
    """Generates the Static JSON API for the GitHub Pages UI"""
    print("\n" + "-"*40)
    print("🌐 Step 3: Building Static API for UI Sync...")
    
    engine = get_db_connection()

    # 1. Fetch 2 years of history for strict 33/83% Quartiles
    history_query = """
        SELECT price_area, predicted_co2 
        FROM ai_forecasts 
        WHERE datetime_utc >= NOW() - INTERVAL '2 years'
    """
    history_df = pd.read_sql(history_query, engine)

    thresholds = {}
    for area in ['DK1', 'DK2']:
        area_data = history_df[history_df['price_area'] == area]['predicted_co2']
        if not area_data.empty:
            thresholds[area] = {
                "p33": round(float(area_data.quantile(0.33)), 1),
                "p83": round(float(area_data.quantile(0.83)), 1)
            }
        else:
            # Safe fallback if DB history is missing
            thresholds[area] = {"p33": 35.0, "p83": 75.0}

    # 2. Fetch the fresh forecast that Step 1 & 2 just generated
    forecast_query = """
        SELECT datetime_utc, market_price_dkk_kwh, predicted_co2, price_area 
        FROM ai_forecasts 
        WHERE DATE(datetime_utc) >= CURRENT_DATE
        ORDER BY datetime_utc ASC
    """
    forecast_df = pd.read_sql(forecast_query, engine)

    # 3. Format the data for the JavaScript frontend
    forecast_list = []
    for _, row in forecast_df.iterrows():
        forecast_list.append({
            "time": pd.to_datetime(row['datetime_utc']).strftime('%H:00'),
            "price": round(float(row['market_price_dkk_kwh']), 3),
            "co2": round(float(row['predicted_co2']), 1),
            "region": row['price_area']
        })

    payload = {
        "generated_at": datetime.utcnow().isoformat(),
        "thresholds": thresholds,
        "forecast": forecast_list
    }

    # 4. Save to docs/latest_forecast.json
    base_dir = os.path.dirname(os.path.abspath(__file__))
    docs_dir = os.path.join(base_dir, 'docs')
    os.makedirs(docs_dir, exist_ok=True)
    
    file_path = os.path.join(docs_dir, 'latest_forecast.json')
    with open(file_path, 'w') as f:
        json.dump(payload, f, indent=4)

    print(f"✅ Static API JSON dumped successfully at: docs/latest_forecast.json")


def main():
    print("⚡️ GREENHOUR DAILY PIPELINE STARTED")
    print("="*40)
    
    # Step 1: Generate the numbers
    try:
        run_prediction()
    except Exception as e:
        print(f"❌ Prediction Step Failed: {e}")
        return

    print("\n" + "-"*40)
    
    # Step 2: Generate the advice
    try:
        run_recommendation()
    except Exception as e:
        print(f"❌ Recommendation Step Failed: {e}")
        return

    # Step 3: Export to JSON for GitHub Pages
    try:
        export_static_api()
    except Exception as e:
        print(f"❌ Static API Export Failed: {e}")
        return

    print("\n" + "="*40)
    print("✅ SUCCESS: Tomorrow's grid strategy is ready in Neon AND GitHub Docs!")

if __name__ == "__main__":
    main()
