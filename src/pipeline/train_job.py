import sys
import os

# --- PATH FIX ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import io
import json
import joblib
import numpy as np
import pandas as pd
import optuna
import xgboost as xgb

from datetime import datetime
from sqlalchemy import text
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.preprocessing import StandardScaler

from src.database.connection import get_db_connection
from src.utils.logger import setup_artifact_logger


# ============================================================
# CONFIG
# ============================================================

TARGET_COLUMN = "co2_emissions_g_kwh"

# Removed:
# - forecast_wind_generation_gw
# - forecast_solar_generation_gw
#
# Reason:
# These future production forecast features are not consistently available
# for the full next-day prediction horizon.
#
# Kept:
# - wind_speed
# - solar_radiation
# because these are weather forecast features from Open-Meteo and are available.

FEATURE_COLUMNS = [
    "spot_price_dkk_kwh",

    "wind_speed",
    "solar_radiation",
    "temperature",

    "forecast_load_gw",

    "co2_lag_1h",
    "co2_lag_2h",
    "co2_lag_24h",
    "co2_lag_168h",

    "co2_rolling_3h",
    "co2_rolling_6h",
    "co2_rolling_24h",

    "co2_diff_1h",
    "co2_diff_24h",

    "hour",
    "day_of_week",
    "month",
    "day_of_year",

    "hour_sin",
    "hour_cos",
    "month_sin",
    "month_cos",
    "day_of_year_sin",
    "day_of_year_cos",

    "is_weekend",
    "is_holiday",
]

SOURCE_COLUMNS = [
    "datetime_utc",
    "price_area",

    "co2_emissions_g_kwh",
    "spot_price_dkk_kwh",

    "wind_speed",
    "solar_radiation",
    "temperature",

    "forecast_load_mw",

    "co2_lag_1h",
    "co2_lag_2h",
    "co2_lag_24h",
    "co2_lag_168h",

    "co2_rolling_3h",
    "co2_rolling_6h",
    "co2_rolling_24h",

    "co2_diff_1h",
    "co2_diff_24h",

    "hour",
    "day_of_week",
    "month",
    "day_of_year",

    "hour_sin",
    "hour_cos",
    "month_sin",
    "month_cos",
    "day_of_year_sin",
    "day_of_year_cos",

    "is_weekend",
    "is_holiday",
]


# ============================================================
# MODEL REGISTRY SCHEMA FIX
# ============================================================

def ensure_model_registry_table(engine):
    """
    Make sure model_registry exists and has all columns needed
    for storing the trained model, scaler, feature list, metrics,
    hyperparameters, and training period.
    """

    create_table_query = text("""
        CREATE TABLE IF NOT EXISTS model_registry (
            id BIGSERIAL PRIMARY KEY,
            model_name TEXT,
            model_version TEXT,
            model_binary BYTEA,
            mae DOUBLE PRECISION,
            rmse DOUBLE PRECISION,
            r2 DOUBLE PRECISION,
            is_active BOOLEAN DEFAULT FALSE,
            hyperparameters JSONB,
            training_start_date TIMESTAMPTZ,
            training_end_date TIMESTAMPTZ,
            git_commit_hash TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)

    alter_queries = [
        "ALTER TABLE model_registry ADD COLUMN IF NOT EXISTS model_name TEXT;",
        "ALTER TABLE model_registry ADD COLUMN IF NOT EXISTS model_version TEXT;",
        "ALTER TABLE model_registry ADD COLUMN IF NOT EXISTS model_binary BYTEA;",
        "ALTER TABLE model_registry ADD COLUMN IF NOT EXISTS mae DOUBLE PRECISION;",
        "ALTER TABLE model_registry ADD COLUMN IF NOT EXISTS rmse DOUBLE PRECISION;",
        "ALTER TABLE model_registry ADD COLUMN IF NOT EXISTS r2 DOUBLE PRECISION;",
        "ALTER TABLE model_registry ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT FALSE;",
        "ALTER TABLE model_registry ADD COLUMN IF NOT EXISTS hyperparameters JSONB;",
        "ALTER TABLE model_registry ADD COLUMN IF NOT EXISTS training_start_date TIMESTAMPTZ;",
        "ALTER TABLE model_registry ADD COLUMN IF NOT EXISTS training_end_date TIMESTAMPTZ;",
        "ALTER TABLE model_registry ADD COLUMN IF NOT EXISTS git_commit_hash TEXT;",
        "ALTER TABLE model_registry ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW();",
    ]

    with engine.begin() as conn:
        conn.execute(create_table_query)

        for query in alter_queries:
            conn.execute(text(query))


# ============================================================
# MODEL REGISTRY UPLOAD
# ============================================================

def upload_model_to_neon(
    model,
    scaler,
    feature_list,
    area_name,
    metrics,
    hyperparams,
    start_date,
    end_date,
    git_hash
):
    engine = get_db_connection()
    area_key = area_name.lower()

    ensure_model_registry_table(engine)

    # Serialize model + scaler + feature list
    buffer = io.BytesIO()

    joblib.dump(
        {
            "model": model,
            "scaler": scaler,
            "features": feature_list,
        },
        buffer
    )

    model_binary = buffer.getvalue()

    with engine.begin() as conn:
        # Deactivate previous active model for this area
        conn.execute(
            text("""
                UPDATE model_registry
                SET is_active = FALSE
                WHERE model_name = :name
                  AND is_active = TRUE
            """),
            {"name": f"co2_{area_key}"}
        )

        # Insert new active model version
        conn.execute(
            text("""
                INSERT INTO model_registry (
                    model_name,
                    model_version,
                    model_binary,
                    mae,
                    rmse,
                    r2,
                    is_active,
                    hyperparameters,
                    training_start_date,
                    training_end_date,
                    git_commit_hash
                )
                VALUES (
                    :name,
                    :version,
                    :binary,
                    :mae,
                    :rmse,
                    :r2,
                    TRUE,
                    :hyperparams,
                    :start_date,
                    :end_date,
                    :git_hash
                )
            """),
            {
                "name": f"co2_{area_key}",
                "version": datetime.now().strftime("%Y%m%d_%H%M"),
                "binary": model_binary,
                "mae": float(metrics["mae"]),
                "rmse": float(metrics["rmse"]),
                "r2": float(metrics["r2"]),
                "hyperparams": json.dumps(hyperparams),
                "start_date": start_date,
                "end_date": end_date,
                "git_hash": git_hash,
            }
        )


# ============================================================
# FEATURE PREPARATION
# ============================================================

def prepare_training_data(raw_df):
    df = raw_df.copy()

    df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True)
    df = df.sort_values("datetime_utc")

    # Convert MW to GW for model training only.
    # Database keeps original MW value.
    df["forecast_load_gw"] = df["forecast_load_mw"] / 1000

    # Convert booleans to 0/1 for model input.
    for col in ["is_weekend", "is_holiday"]:
        if col in df.columns:
            df[col] = df[col].astype(int)

    # Remove infinity values if any
    df = df.replace([np.inf, -np.inf], np.nan)

    # Keep only rows with all needed values
    needed_columns = [TARGET_COLUMN] + FEATURE_COLUMNS
    df = df.dropna(subset=needed_columns)

    # Use datetime as index for chronological split
    df = df.set_index("datetime_utc")

    return df


# ============================================================
# TRAINING
# ============================================================

def train_area_model(area_name):
    print(f"\n🚀 TRAINING PIPELINE: {area_name}")

    engine = get_db_connection()

    selected_cols = ", ".join(SOURCE_COLUMNS)

    query = text(f"""
        SELECT
            {selected_cols}
        FROM processed_features
        WHERE price_area = :area_name
          AND COALESCE(is_forecast, FALSE) = FALSE
          AND datetime_utc >= '2022-01-01'
        ORDER BY datetime_utc ASC;
    """)

    with engine.connect() as conn:
        raw_df = pd.read_sql(
            query,
            conn,
            params={"area_name": area_name}
        )

    if raw_df.empty:
        raise ValueError(f"No training data found for {area_name}.")

    print(f"📦 Raw rows loaded: {len(raw_df)}")

    df = prepare_training_data(raw_df)

    if df.empty:
        raise ValueError(
            f"No usable training rows left for {area_name} after dropping missing values."
        )

    print(f"✅ Usable training rows after cleaning: {len(df)}")

    training_start_date = df.index.min().to_pydatetime()
    training_end_date = df.index.max().to_pydatetime()

    X = df[FEATURE_COLUMNS]
    y = df[TARGET_COLUMN]

    print("🧠 Features used:")
    for feature in FEATURE_COLUMNS:
        print(f"  - {feature}")

    # --------------------------------------------------------
    # 70/15/15 chronological split
    # --------------------------------------------------------
    n = len(X)

    train_end = int(n * 0.70)
    eval_end = int(n * 0.85)

    X_train = X.iloc[:train_end]
    y_train = y.iloc[:train_end]

    X_eval = X.iloc[train_end:eval_end]
    y_eval = y.iloc[train_end:eval_end]

    X_test = X.iloc[eval_end:]
    y_test = y.iloc[eval_end:]

    print("\n📅 Split:")
    print(f"Train: {X_train.index.min()} → {X_train.index.max()} | rows: {len(X_train)}")
    print(f"Eval : {X_eval.index.min()} → {X_eval.index.max()} | rows: {len(X_eval)}")
    print(f"Test : {X_test.index.min()} → {X_test.index.max()} | rows: {len(X_test)}")

    # --------------------------------------------------------
    # Scaling
    # XGBoost does not strictly need scaling,
    # but we keep it because model registry stores scaler + model.
    # --------------------------------------------------------
    scaler = StandardScaler()

    X_train_scaled = scaler.fit_transform(X_train)
    X_eval_scaled = scaler.transform(X_eval)
    X_test_scaled = scaler.transform(X_test)

    # --------------------------------------------------------
    # Optuna tuning
    # --------------------------------------------------------
    def objective(trial):
        params = {
            "objective": "reg:squarederror",
            "tree_method": "hist",
            "random_state": 42,
            "n_jobs": -1,

            "max_depth": trial.suggest_int("max_depth", 3, 10),
            "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.15, log=True),
            "subsample": trial.suggest_float("subsample", 0.70, 1.00),
            "colsample_bytree": trial.suggest_float("colsample_bytree", 0.70, 1.00),
            "min_child_weight": trial.suggest_int("min_child_weight", 1, 10),
            "reg_lambda": trial.suggest_float("reg_lambda", 0.1, 10.0, log=True),

            "n_estimators": 700,
            "early_stopping_rounds": 50,
            "eval_metric": "mae",
        }

        model = xgb.XGBRegressor(**params)

        model.fit(
            X_train_scaled,
            y_train,
            eval_set=[(X_eval_scaled, y_eval)],
            verbose=False
        )

        eval_preds = model.predict(X_eval_scaled)

        return mean_absolute_error(y_eval, eval_preds)

    print("\n🔎 Running Optuna tuning...")

    study = optuna.create_study(direction="minimize")
    study.optimize(objective, n_trials=20)

    print("✅ Best Optuna params:")
    print(study.best_params)

    # --------------------------------------------------------
    # Final model
    # --------------------------------------------------------
    final_params = {
        **study.best_params,
        "objective": "reg:squarederror",
        "tree_method": "hist",
        "random_state": 42,
        "n_jobs": -1,
        "n_estimators": 1000,
        "early_stopping_rounds": 50,
        "eval_metric": "mae",
    }

    best_model = xgb.XGBRegressor(**final_params)

    best_model.fit(
        X_train_scaled,
        y_train,
        eval_set=[(X_eval_scaled, y_eval)],
        verbose=False
    )

    # --------------------------------------------------------
    # Test evaluation
    # --------------------------------------------------------
    test_preds = best_model.predict(X_test_scaled)

    metrics = {
        "mae": float(mean_absolute_error(y_test, test_preds)),
        "rmse": float(np.sqrt(mean_squared_error(y_test, test_preds))),
        "r2": float(r2_score(y_test, test_preds)),
    }

    print("\n📊 TEST RESULTS")
    print(f"MAE : {metrics['mae']:.4f} gCO₂/kWh")
    print(f"RMSE: {metrics['rmse']:.4f} gCO₂/kWh")
    print(f"R²  : {metrics['r2']:.4f}")

    # --------------------------------------------------------
    # Upload model to Neon
    # --------------------------------------------------------
    git_hash = os.getenv("GITHUB_SHA", "local-dev")

    upload_model_to_neon(
        model=best_model,
        scaler=scaler,
        feature_list=FEATURE_COLUMNS,
        area_name=area_name,
        metrics=metrics,
        hyperparams=final_params,
        start_date=training_start_date,
        end_date=training_end_date,
        git_hash=git_hash
    )

    print(f"\n🎉 Model uploaded to Neon as active model: co2_{area_name.lower()}")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    setup_artifact_logger("train")

    # Semester project focuses on DK1 only
    train_area_model("DK1")