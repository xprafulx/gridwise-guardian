import sys
import time

# --- FIX THESE IMPORTS TO MATCH YOUR EXACT FUNCTION NAMES ---
from src.pipeline.ingest_job import ingest_job 
from src.pipeline.predict_job import run_job 
from src.pipeline.recommendation_job import run_recommendation_engine

def run_gridwise_pipeline():
    print("\n" + "🚀 " * 15)
    print("      GRIDWISE: END-TO-END MLOPS PIPELINE")
    print("🚀 " * 15)
    
    start_time = time.time()

    try:
        # --- PHASE 1: INGESTION ---
        print("\n[STEP 1/3] 📥 Syncing live grid data from Energinet & Weather APIs...")
        ingest_job()  # <--- Make sure this matches the import above!
        print("✅ Data Ingestion Successful.")

        # --- PHASE 2: PREDICTION ---
        print("\n[STEP 2/3] 🔮 Running ML Forecast models for DK1 & DK2...")
        run_job()
        print("✅ Machine Learning Forecasts Generated.")

        # --- PHASE 3: RECOMMENDATION ---
        print("\n[STEP 3/3] 🌍 Synthesizing the Guardian's Manifesto...")
        run_recommendation_engine()
        
        duration = round(time.time() - start_time, 2)
        print(f"\n✨ Pipeline executed successfully in {duration} seconds.")
        print("The Danish grid is guarded. / Det danske elnet er beskyttet.")

    except Exception as e:
        print(f"\n❌ PIPELINE CRITICAL FAILURE: {e}")
        print("Manual intervention required. Check logs.")
        sys.exit(1)

if __name__ == "__main__":
    run_gridwise_pipeline()