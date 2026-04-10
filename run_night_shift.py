# Pseudo-code for run_night_shift.py
from src.pipeline.ingest_job import ingest_job
# from src.pipeline.train_job import check_drift_and_train

print("🌙 Starting Night Shift: Ingestion & Monitoring...")
ingest_job()
# check_drift_and_train() 
print("✅ Night Shift Complete. Data warehouse is updated.")