# Pseudo-code for run_evening_shift.py
from src.pipeline.predict_job import run_job
from src.pipeline.recommendation_job import run_recommendation_engine

print("🌆 Starting Evening Shift: Forecasting Tomorrow...")
run_job()
run_recommendation_engine()
print("✅ Evening Shift Complete. The Guardian's Manifesto is ready.")