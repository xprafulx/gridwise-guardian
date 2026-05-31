import sys
import os

# --- PATH FIX ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.pipeline.predict_job import run_job as run_prediction
from src.pipeline.recommendation_job import run_recommendation_engine


def main():
    print("⚡ GREENHOUR DAILY PIPELINE STARTED")
    print("=" * 40)

    try:
        print("\n🔮 Step 1: Running prediction job...")
        run_prediction()

        print("\n🧮 Step 2: Running recommendation job...")
        run_recommendation_engine()

    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        return

    print("\n" + "=" * 40)
    print("✅ SUCCESS: Prediction and recommendation completed.")
    print("✅ Prediction saved in ai_forecasts")
    print("✅ CO₂-aware price signal saved in co2_aware_price_signals")


if __name__ == "__main__":
    main()