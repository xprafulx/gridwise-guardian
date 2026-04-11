import os
import numpy as np
import pandas as pd
import joblib
import optuna
import xgboost as xgb
import holidays
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from src.database.connection import get_db_connection

def create_features(df):
    """The 'Eyes' and 'Memory' of the Model."""
    df = df.copy()
    df['ds'] = pd.to_datetime(df['ds'])
    df = df.set_index('ds')
    
    # 1. TIME FEATURES
    df['hour'] = df.index.hour
    df['dayofweek'] = df.index.dayofweek
    df['month'] = df.index.month
    
    # 2. CALENDAR & HOLIDAYS (The Danish Context)
    dk_holidays = holidays.Denmark()
    df['is_holiday'] = df.index.map(lambda x: 1 if x in dk_holidays else 0)
    df['is_weekend'] = df['dayofweek'].map(lambda x: 1 if x >= 5 else 0)
    
    # 3. CYCLICAL ENCODING
    # Teaches the AI that 23:00 and 00:00 are adjacent in time
    df['hour_sin'] = np.sin(2 * np.pi * df['hour']/24)
    df['hour_cos'] = np.cos(2 * np.pi * df['hour']/24)
    
    # 4. THE MEMORY (Lags) - This is what fixed your R2
    # We use shift(1) to make sure we only use PAST data to predict the FUTURE
    for i in [1, 2, 3, 6, 24, 168]: # 168 is exactly one week ago
        df[f'co2_lag_{i}'] = df['y'].shift(i)
        
    # 5. ROLLING WINDOWS (Trends)
    df['co2_roll_6'] = df['y'].shift(1).rolling(window=6).mean()
    
    return df.dropna()

def run_training_pipeline():
    os.makedirs('models', exist_ok=True)
    
    # 1. Load Data (Starting from 2022 to avoid 'garbage' noise)
    print("📖 Fetching Modern DK1 data (2022+)...")
    engine = get_db_connection()
    query = """
        SELECT ds, co2_emissions_g_kwh as y, spot_price_dkk_kwh, wind_speed, solar_radiation 
        FROM historical_training_data 
        WHERE price_area = 'DK1' AND ds >= '2022-01-01' 
        ORDER BY ds ASC
    """
    raw_df = pd.read_sql(query, engine)
    
    # 2. Feature Engineering
    print("🛠️ Engineering features (Holidays, Lags, Cyclical Time)...")
    df = create_features(raw_df)
    
    # 3. Rigorous Splitting
    # Train: 22-23 | Eval: 2024 | Test: 2025-Now
    train_df = df[df.index < '2024-01-01']
    eval_df  = df[(df.index >= '2024-01-01') & (df.index < '2025-01-01')]
    test_df  = df[df.index >= '2025-01-01']

    X_train, y_train = train_df.drop('y', axis=1), train_df['y']
    X_eval, y_eval   = eval_df.drop('y', axis=1), eval_df['y']
    X_test, y_test   = test_df.drop('y', axis=1), test_df['y']

    print(f"📐 Split: Train ({len(X_train)}), Eval ({len(X_eval)}), Test ({len(X_test)})")

    # 4. Optuna Hyperparameter Tuning (Tuning on 2024 Data)
    def objective(trial):
        param = {
            'max_depth': trial.suggest_int('max_depth', 3, 10),
            'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.1, log=True),
            'n_estimators': 2000,
            'min_child_weight': trial.suggest_int('min_child_weight', 1, 10),
            'subsample': trial.suggest_float('subsample', 0.5, 1.0),
            'colsample_bytree': trial.suggest_float('colsample_bytree', 0.5, 1.0),
            'reg_alpha': trial.suggest_float('reg_alpha', 1e-3, 10.0, log=True),
            'reg_lambda': trial.suggest_float('reg_lambda', 1e-3, 10.0, log=True),
            'random_state': 42,
            'early_stopping_rounds': 50 # Modern XGBoost API
        }
        
        model = xgb.XGBRegressor(**param)
        model.fit(X_train, y_train, eval_set=[(X_eval, y_eval)], verbose=False)
        
        preds = model.predict(X_eval)
        return mean_absolute_error(y_eval, preds)

    print("🧪 Tuning XGBoost with Optuna...")
    study = optuna.create_study(direction='minimize')
    study.optimize(objective, n_trials=20)

    # 5. Final Training with Best Params
    print(f"🧠 Training Final Model with Best Params...")
    final_params = study.best_params
    final_params['early_stopping_rounds'] = 50
    
    best_model = xgb.XGBRegressor(**final_params)
    best_model.fit(X_train, y_train, eval_set=[(X_eval, y_eval)], verbose=False)

    # 6. Final Blind Test (2025-2026)
    preds = best_model.predict(X_test)
    mae = mean_absolute_error(y_test, preds)
    rmse = np.sqrt(mean_squared_error(y_test, preds))
    r2 = r2_score(y_test, preds)

    print("\n" + "="*45)
    print(f"🏆 FINAL PERFORMANCE (TEST SET 2025-2026):")
    print(f"   MAE:  {mae:.2f} g/kWh")
    print(f"   RMSE: {rmse:.2f} g/kWh")
    print(f"   R²:   {r2:.2f}")
    print("="*45)

    # 7. Save Artifacts
    joblib.dump(best_model, 'models/xgb_co2_v1_tuned.pkl')
    joblib.dump(X_train.columns.tolist(), 'models/feature_names.pkl')
    print("💾 Model and Feature Names saved successfully.")

if __name__ == "__main__":
    run_training_pipeline()