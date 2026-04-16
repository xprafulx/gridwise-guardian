import sys
import os
import io
import json
import joblib
import numpy as np
import pandas as pd
import optuna
import xgboost as xgb
import holidays
from datetime import datetime
from sqlalchemy import text
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.preprocessing import StandardScaler
from src.database.connection import get_db_connection
from src.utils.logger import setup_artifact_logger

# --- PATH FIX ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

def upload_model_to_neon(model, scaler, feature_list, area_name, metrics, hyperparams, start_date, end_date, git_hash):
    engine = get_db_connection()
    area_key = area_name.lower()
    
    # 1. Serialize model, features, AND scaler to a binary buffer
    buffer = io.BytesIO()
    joblib.dump({
        'model': model, 
        'scaler': scaler, 
        'features': feature_list
    }, buffer)
    model_binary = buffer.getvalue()

    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE model_registry 
            SET is_active = FALSE 
            WHERE model_name = :name AND is_active = TRUE
        """), {"name": f"co2_{area_key}"})
        
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

def create_features(df):
    df = df.copy()
    df['ds'] = pd.to_datetime(df['datetime_utc'], utc=True)
    df = df.sort_values(['price_area', 'ds'])
    
    for i in [1, 2, 24, 168]:
        df[f'co2_lag_{i}h'] = df.groupby('price_area')['co2_emissions_g_kwh'].shift(i)
    
    df = df.set_index('ds')
    df['hour'] = df.index.hour
    df['day_of_week'] = df.index.dayofweek
    df['month'] = df.index.month
    df['hour_sin'] = np.sin(2 * np.pi * df['hour']/24)
    df['hour_cos'] = np.cos(2 * np.pi * df['hour']/24)
    
    dk_holidays = holidays.Denmark()
    df['is_holiday'] = [1 if d.date() in dk_holidays else 0 for d in df.index]
    df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
    
    return df.drop(columns=['datetime_utc']).dropna()

def train_area_model(area_name):
    print(f"\n🚀 TRAINING PIPELINE: {area_name}")
    engine = get_db_connection()
    
    query = f"""
        SELECT datetime_utc, price_area, spot_price_dkk_kwh, co2_emissions_g_kwh, wind_speed, solar_radiation 
        FROM processed_features 
        WHERE price_area = '{area_name}' 
        AND is_forecast = FALSE
        AND datetime_utc >= '2022-01-01' 
        ORDER BY datetime_utc ASC
    """
    raw_df = pd.read_sql(query, engine)
    df = create_features(raw_df)
    
    training_start_date = df.index.min().to_pydatetime()
    training_end_date = df.index.max().to_pydatetime()

    X = df.drop(columns=['co2_emissions_g_kwh', 'price_area'])
    y = df['co2_emissions_g_kwh']

    # --- 70/15/15 CHRONOLOGICAL SPLIT ---
    n = len(X)
    train_end = int(n * 0.70)
    eval_end = int(n * 0.85)

    X_train, y_train = X.iloc[:train_end], y.iloc[:train_end]
    X_eval, y_eval = X.iloc[train_end:eval_end], y.iloc[train_end:eval_end]
    X_test, y_test = X.iloc[eval_end:], y.iloc[eval_end:]

    # --- FEATURE SCALING ---
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_eval_scaled = scaler.transform(X_eval)
    X_test_scaled = scaler.transform(X_test)

    # --- OPTUNA TUNING (Using Eval Set) ---
    def objective(trial):
        param = {
            'max_depth': trial.suggest_int('max_depth', 3, 10),
            'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.1, log=True),
            'n_estimators': 500,
            'early_stopping_rounds': 50,
            'tree_method': 'hist'
        }
        model = xgb.XGBRegressor(**param)
        model.fit(X_train_scaled, y_train, eval_set=[(X_eval_scaled, y_eval)], verbose=False)
        return mean_absolute_error(y_eval, model.predict(X_eval_scaled))

    study = optuna.create_study(direction='minimize')
    study.optimize(objective, n_trials=10)

    # --- FINAL MODEL (Trained on Train, Checked against Eval) ---
    best_model = xgb.XGBRegressor(**study.best_params, n_estimators=1000, early_stopping_rounds=50)
    best_model.fit(X_train_scaled, y_train, eval_set=[(X_eval_scaled, y_eval)], verbose=False)

    # --- EVALUATION (Strictly on Test Set) ---
    test_preds = best_model.predict(X_test_scaled)
    metrics = {
        'mae': float(mean_absolute_error(y_test, test_preds)),
        'rmse': float(np.sqrt(mean_squared_error(y_test, test_preds))),
        'r2': float(r2_score(y_test, test_preds))
    }

    print(f"📊 {area_name} TEST RESULTS - R2: {metrics['r2']:.4f}")

    git_hash = os.getenv('GITHUB_SHA', 'local-dev')
    upload_model_to_neon(
        best_model, scaler, X_train.columns.tolist(), area_name, 
        metrics, study.best_params, training_start_date, training_end_date, git_hash
    )

if __name__ == "__main__":
    setup_artifact_logger("train")
    for area in ['DK1', 'DK2']:
        train_area_model(area)
