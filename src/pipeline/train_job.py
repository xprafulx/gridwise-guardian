import sys
import os
import io
import json  # <--- NEW: Required to convert hyperparams to JSON for the database
import joblib
import numpy as np
import pandas as pd
import optuna
import xgboost as xgb
import holidays
from datetime import datetime
from sqlalchemy import text
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from src.database.connection import get_db_connection
from src.utils.logger import setup_artifact_logger

# --- PATH FIX: Ensures it finds the 'src' folder from the root ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

def upload_model_to_neon(model, feature_list, area_name, metrics, hyperparams, start_date, end_date, git_hash):
    """
    Serializes the winning model and pushes ALL artifacts to the Neon Model Registry.
    """
    engine = get_db_connection()
    area_key = area_name.lower()
    
    # 1. Serialize model and features to a binary buffer
    buffer = io.BytesIO()
    joblib.dump({'model': model, 'features': feature_list}, buffer)
    model_binary = buffer.getvalue()

    print(f"📦 Serialized model size: {len(model_binary) / 1024:.2f} KB")

    # 2. SQL to save the model and swap 'is_active' status
    with engine.begin() as conn:
        # Deactivate previous active model for this area
        conn.execute(text("""
            UPDATE model_registry 
            SET is_active = FALSE 
            WHERE model_name = :name AND is_active = TRUE
        """), {"name": f"co2_{area_key}"})
        
        # Insert the new active model WITH all our new tracking columns!
        query = text("""
            INSERT INTO model_registry (
                model_name, model_version, model_binary, 
                mae, rmse, r2, is_active,
                hyperparameters, training_start_date, training_end_date, git_commit_hash
            )
            VALUES (
                :name, :version, :binary, 
                :mae, :rmse, :r2, TRUE,
                :hyperparams, :start_date, :end_date, :git_hash
            )
        """)
        
        # We explicitly cast metrics to float() and use json.dumps() for the hyperparams
        conn.execute(query, {
            "name": f"co2_{area_key}",
            "version": datetime.now().strftime("%Y%m%d_%H%M"),
            "binary": model_binary,
            "mae": float(metrics['mae']),
            "rmse": float(metrics['rmse']),
            "r2": float(metrics['r2']),
            "hyperparams": json.dumps(hyperparams),
            "start_date": start_date,
            "end_date": end_date,
            "git_hash": git_hash
        })
    print(f"🚀 Model '{area_name}' and ALL artifacts successfully pushed to Neon Model Registry!")

def create_features(df):
    """Generates time-series and cyclical features for the model."""
    df = df.copy()
    df['ds'] = pd.to_datetime(df['datetime_utc'], utc=True)
    df = df.sort_values(['price_area', 'ds'])
    
    # Lag Features (The Grid's Memory)
    for i in [1, 2, 24, 168]:
        df[f'co2_lag_{i}h'] = df.groupby('price_area')['co2_emissions_g_kwh'].shift(i)
    
    df = df.set_index('ds')
    df['hour'] = df.index.hour
    df['day_of_week'] = df.index.dayofweek
    df['month'] = df.index.month
    
    # Cyclical Time (Helps the model understand the 24h and 12-month loops)
    df['hour_sin'] = np.sin(2 * np.pi * df['hour']/24)
    df['hour_cos'] = np.cos(2 * np.pi * df['hour']/24)
    
    # Danish Holiday and Weekend Logic
    dk_holidays = holidays.Denmark()
    df['is_holiday'] = [1 if d.date() in dk_holidays else 0 for d in df.index]
    df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
    
    return df.drop(columns=['datetime_utc']).dropna()

def train_area_model(area_name):
    print(f"\n🚀 STARTING TRAINING PIPELINE FOR: {area_name}")
    engine = get_db_connection()
    
    # Load training data from the Silver layer
    query = f"""
        SELECT datetime_utc, price_area, spot_price_dkk_kwh, co2_emissions_g_kwh, wind_speed, solar_radiation 
        FROM processed_features 
        WHERE price_area = '{area_name}' 
        AND is_forecast = FALSE
        AND datetime_utc >= '2022-01-01' 
        ORDER BY datetime_utc ASC
    """
    raw_df = pd.read_sql(query, engine)
    if raw_df.empty:
        print(f"⚠️ No data found for {area_name}. Run ingest first!")
        return

    df = create_features(raw_df)
    
    # --- DATA LINEAGE FIX ---
    # We grab the exact start and end timestamps from the data index we are about to train on
    training_start_date = df.index.min().to_pydatetime()
    training_end_date = df.index.max().to_pydatetime()

    X = df.drop(columns=['co2_emissions_g_kwh', 'price_area'])
    y = df['co2_emissions_g_kwh']

    # Time-series split (no shuffling to prevent data leakage)
    split_idx = int(len(X) * 0.8)
    X_train, y_train = X.iloc[:split_idx], y.iloc[:split_idx]
    X_test, y_test = X.iloc[split_idx:], y.iloc[split_idx:]

    # Optuna Hyperparameter Tuning
    def objective(trial):
        param = {
            'max_depth': trial.suggest_int('max_depth', 3, 10),
            'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.1, log=True),
            'n_estimators': 500,
            'early_stopping_rounds': 50,
            'random_state': 42,
            'tree_method': 'hist'
        }
        model = xgb.XGBRegressor(**param)
        model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)
        return mean_absolute_error(y_test, model.predict(X_test))

    print(f"🔎 Tuning {area_name} with Optuna Bayesian Optimization...")
    study = optuna.create_study(direction='minimize')
    study.optimize(objective, n_trials=10)

    # Train final model with the BEST parameters found
    # (This guarantees we ONLY store the winning model's settings)
    best_model = xgb.XGBRegressor(**study.best_params, n_estimators=1000, early_stopping_rounds=50)
    best_model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

    # Calculate Diagnostics
    preds = best_model.predict(X_test)
    metrics = {
        'mae': float(mean_absolute_error(y_test, preds)),
        'rmse': float(np.sqrt(mean_squared_error(y_test, preds))),
        'r2': float(r2_score(y_test, preds))
    }

    print(f"📊 {area_name} RESULTS - MAE: {metrics['mae']:.2f} | RMSE: {metrics['rmse']:.2f} | R2: {metrics['r2']:.4f}")

    # --- GIT HASH FIX ---
    # Grabs the specific code version from GitHub actions. If you run it locally, it marks it as 'local-dev'
    git_hash = os.getenv('GITHUB_SHA', 'local-dev')

    # --- SAVE AND DEPLOY ---
    # 1. Local Backup
    local_path = f"models/latest/{area_name.lower()}/model.pkl"
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    joblib.dump(best_model, local_path)
    
    # 2. Push to Neon Model Registry (Passing all our new MLOps artifacts)
    upload_model_to_neon(
        model=best_model, 
        feature_list=X_train.columns.tolist(), 
        area_name=area_name, 
        metrics=metrics,
        hyperparams=study.best_params,        # <--- The Optuna winning parameters
        start_date=training_start_date,       # <--- The exact date the data started
        end_date=training_end_date,           # <--- The exact date the data ended
        git_hash=git_hash                     # <--- The exact code version
    )

if __name__ == "__main__":
    # Start the "Tee" logger to capture terminal output into a .txt file
    setup_artifact_logger("train")
    
    for area in ['DK1', 'DK2']:
        try:
            train_area_model(area)
        except Exception as e:
            print(f"❌ Critical error training {area}: {e}")
