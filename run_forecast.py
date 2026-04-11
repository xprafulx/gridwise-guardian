# run_forecast.py
from src.pipeline.predict_job import run_job as run_prediction
from src.pipeline.recommendation_job import run_recommendation_engine as run_recommendation

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

    print("\n" + "="*40)
    print("✅ SUCCESS: Tomorrow's grid strategy is ready in Neon!")

if __name__ == "__main__":
    main()