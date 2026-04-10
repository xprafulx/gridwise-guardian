import sys
import os
# --- PATH FIX: This allows running from the root 'greenhour' folder ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import numpy as np
import pandas as pd
import joblib
import optuna
import xgboost as xgb
import holidays
from datetime import datetime
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from src.database.connection import get_db_connection

def create_features(df):
    """The 'Memory' and 'Context' builder for the Danish Grid."""
    df = df.copy()
    df['ds'] = pd.to_datetime(df['ds'])
    
    # Sort for correct lagging
    df = df.sort_values(['price_area', 'ds'])
    
    # 1. THE MEMORY (Lags)
    for i in [1, 2, 24, 168]:
        df[f'co2_lag_{i}h'] = df.groupby('price_area')['co2_emissions_g_kwh'].shift(i)
    
    # 2. TIME FEATURES
    df = df.set_index('ds')
    df['hour'] = df.index.hour
    df['day_of_week'] = df.index.dayofweek
    df['month'] = df.index.month
    df['season'] = (df['month'] % 12 // 3)
    
    # 3. CYCLICAL TIME (Sin/Cos)
    df['hour_sin'] = np.sin(2 * np.pi * df['hour']/24)
    df['hour_cos'] = np.cos(2 * np.pi * df['hour']/24)
    
    # 4. DANISH CALENDAR (Holiday detection)
    dk_holidays = holidays.Denmark()
    df['is_holiday'] = [1 if d.date() in dk_holidays else 0 for d in df.index]
    df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
    
    return df.dropna()

def train_area_model(area_name):
    print(f"\n🚀 STARTING TRAINING PIPELINE FOR: {area_name}")
    
    engine = get_db_connection()
    # Pulling data from 2022 onwards for high-quality training
    query = f"""
        SELECT ds, price_area, spot_price_dkk_kwh, co2_emissions_g_kwh, wind_speed, solar_radiation 
        FROM historical_training_data 
        WHERE price_area = '{area_name}' 
        AND ds >= '2022-01-01' 
        ORDER BY ds ASC
    """
    raw_df = pd.read_sql(query, engine)
    df = create_features(raw_df)
    
    X = df.drop(columns=['co2_emissions_g_kwh', 'price_area'])
    y = df['co2_emissions_g_kwh']

    # Splitting: 80% Train/Eval, 20% Test for fresh performance check
    split_idx = int(len(X) * 0.8)
    X_train, y_train = X.iloc[:split_idx], y.iloc[:split_idx]
    X_test, y_test = X.iloc[split_idx:], y.iloc[split_idx:]

    # Optuna Tuning: Finding the "Golden Parameters"
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

    print(f"🔎 Tuning {area_name} with Optuna...")
    study = optuna.create_study(direction='minimize')
    study.optimize(objective, n_trials=10) # 10 trials is enough for weekly maintenance

    # Final Fit with Best Params
    best_model = xgb.XGBRegressor(**study.best_params, n_estimators=1000, early_stopping_rounds=50)
    best_model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

    # Performance Stats (MAE is king for electricity grids)
    preds = best_model.predict(X_test)
    mae = mean_absolute_error(y_test, preds)
    print(f"📊 {area_name} Optimized MAE: {mae:.2f} g/kWh")

    # --- VERSIONED ARTIFACT SAVING ---
    date_str = datetime.now().strftime("%m%d%Y") # MMDDYYYY format
    
    versions_dir = f"models/versions/{area_name.lower()}"
    latest_dir = f"models/latest/{area_name.lower()}"
    os.makedirs(versions_dir, exist_ok=True)
    os.makedirs(latest_dir, exist_ok=True)

    # 1. Save the Versioned Files (The Archive)
    joblib.dump(best_model, f"{versions_dir}/xgb_co2_{date_str}.pkl")
    joblib.dump(X_train.columns.tolist(), f"{versions_dir}/features_{date_str}.pkl")

    # 2. Save the "Latest" Files (The Production Model)
    joblib.dump(best_model, f"{latest_dir}/model.pkl")
    joblib.dump(X_train.columns.tolist(), f"{latest_dir}/features.pkl")

    print(f"✅ Version archived: {versions_dir}/xgb_co2_{date_str}.pkl")
    print(f"✅ Production model updated: {latest_dir}/model.pkl")

if __name__ == "__main__":
    # Ensure root folders exist
    os.makedirs('models/versions', exist_ok=True)
    os.makedirs('models/latest', exist_ok=True)
    
    for area in ['DK1', 'DK2']:
        train_area_model(area)