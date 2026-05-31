import sys
import os

# --- PATH FIX ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import numpy as np
import pandas as pd

from sqlalchemy import text
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from src.database.connection import get_db_connection
from src.utils.logger import setup_artifact_logger


# ============================================================
# CONFIG
# ============================================================

PRICE_AREA = "DK1"
MIN_EVAL_ROWS = 12
ACCURACY_ALERT_THRESHOLD = 70.0


# ============================================================
# TABLE SETUP
# ============================================================

def ensure_model_performance_history_table(engine):
    """
    Stores daily prediction evaluation results.

    One row = one evaluation date + price area + model version.
    """

    create_query = text("""
        CREATE TABLE IF NOT EXISTS model_performance_history (
            eval_date DATE NOT NULL,
            price_area TEXT NOT NULL,
            model_version TEXT NOT NULL,

            mae DOUBLE PRECISION,
            rmse DOUBLE PRECISION,
            r2 DOUBLE PRECISION,
            accuracy_pct DOUBLE PRECISION,

            mean_actual_co2 DOUBLE PRECISION,
            row_count INTEGER,

            eval_start_utc TIMESTAMPTZ,
            eval_end_utc TIMESTAMPTZ,

            eval_timestamp TIMESTAMPTZ DEFAULT NOW()
        );
    """)

    alter_queries = [
        "ALTER TABLE model_performance_history ADD COLUMN IF NOT EXISTS eval_date DATE;",
        "ALTER TABLE model_performance_history ADD COLUMN IF NOT EXISTS price_area TEXT;",
        "ALTER TABLE model_performance_history ADD COLUMN IF NOT EXISTS model_version TEXT;",

        "ALTER TABLE model_performance_history ADD COLUMN IF NOT EXISTS mae DOUBLE PRECISION;",
        "ALTER TABLE model_performance_history ADD COLUMN IF NOT EXISTS rmse DOUBLE PRECISION;",
        "ALTER TABLE model_performance_history ADD COLUMN IF NOT EXISTS r2 DOUBLE PRECISION;",
        "ALTER TABLE model_performance_history ADD COLUMN IF NOT EXISTS accuracy_pct DOUBLE PRECISION;",

        "ALTER TABLE model_performance_history ADD COLUMN IF NOT EXISTS mean_actual_co2 DOUBLE PRECISION;",
        "ALTER TABLE model_performance_history ADD COLUMN IF NOT EXISTS row_count INTEGER;",

        "ALTER TABLE model_performance_history ADD COLUMN IF NOT EXISTS eval_start_utc TIMESTAMPTZ;",
        "ALTER TABLE model_performance_history ADD COLUMN IF NOT EXISTS eval_end_utc TIMESTAMPTZ;",

        "ALTER TABLE model_performance_history ADD COLUMN IF NOT EXISTS eval_timestamp TIMESTAMPTZ DEFAULT NOW();",
    ]

    with engine.begin() as conn:
        conn.execute(create_query)

        for query in alter_queries:
            conn.execute(text(query))

    print("✅ model_performance_history table is ready.")


# ============================================================
# TIME WINDOW
# ============================================================

def get_yesterday_cph_window():
    """
    Evaluate yesterday based on Danish local day.

    Example during Danish summer time:
    Yesterday 00:00–23:00 CPH
    becomes 22:00 previous day → 21:00 target day UTC.
    """

    cph_now = pd.Timestamp.now(tz="Europe/Copenhagen")

    yesterday_cph = (cph_now - pd.Timedelta(days=1)).normalize()
    today_cph = yesterday_cph + pd.Timedelta(days=1)

    start_utc = yesterday_cph.tz_convert("UTC")
    end_utc = today_cph.tz_convert("UTC")

    eval_date = yesterday_cph.date()

    return eval_date, start_utc, end_utc


# ============================================================
# LOAD DATA
# ============================================================

def load_prediction_vs_actual(engine, area_name, start_utc, end_utc):
    """
    Join predicted CO₂ from ai_forecasts with actual CO₂ from processed_features.
    """

    query = text("""
        SELECT
            f.datetime_utc,
            f.price_area,
            f.model_version,

            f.predicted_co2_g_kwh AS predicted_co2,
            p.co2_emissions_g_kwh AS actual_co2

        FROM ai_forecasts f
        INNER JOIN processed_features p
            ON f.datetime_utc = p.datetime_utc
           AND f.price_area = p.price_area

        WHERE f.price_area = :area_name
          AND f.datetime_utc >= :start_utc
          AND f.datetime_utc < :end_utc
          AND COALESCE(p.is_forecast, FALSE) = FALSE
          AND f.predicted_co2_g_kwh IS NOT NULL
          AND p.co2_emissions_g_kwh IS NOT NULL

        ORDER BY f.model_version, f.datetime_utc ASC;
    """)

    with engine.connect() as conn:
        df = pd.read_sql(
            query,
            conn,
            params={
                "area_name": area_name,
                "start_utc": start_utc.to_pydatetime(),
                "end_utc": end_utc.to_pydatetime(),
            },
        )

    if not df.empty:
        df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True)

    return df


# ============================================================
# METRICS
# ============================================================

def calculate_metrics(df):
    actual = df["actual_co2"]
    predicted = df["predicted_co2"]

    mae = mean_absolute_error(actual, predicted)
    rmse = np.sqrt(mean_squared_error(actual, predicted))

    if len(df) >= 2 and actual.nunique() > 1:
        r2 = r2_score(actual, predicted)
    else:
        r2 = np.nan

    mean_actual = actual.mean()

    if mean_actual and mean_actual != 0:
        accuracy_pct = 100 - ((mae / mean_actual) * 100)
        accuracy_pct = max(0.0, accuracy_pct)
    else:
        accuracy_pct = np.nan

    return {
        "mae": float(mae),
        "rmse": float(rmse),
        "r2": float(r2) if not pd.isna(r2) else None,
        "accuracy_pct": float(accuracy_pct) if not pd.isna(accuracy_pct) else None,
        "mean_actual_co2": float(mean_actual),
        "row_count": int(len(df)),
    }


# ============================================================
# SAVE METRICS
# ============================================================

def save_metrics(
    engine,
    eval_date,
    area_name,
    model_version,
    metrics,
    start_utc,
    end_utc,
):
    """
    DELETE + INSERT avoids ON CONFLICT problems if the old table
    was created without a unique constraint.
    """

    delete_query = text("""
        DELETE FROM model_performance_history
        WHERE eval_date = :eval_date
          AND price_area = :price_area
          AND model_version = :model_version;
    """)

    insert_query = text("""
        INSERT INTO model_performance_history (
            eval_date,
            price_area,
            model_version,

            mae,
            rmse,
            r2,
            accuracy_pct,

            mean_actual_co2,
            row_count,

            eval_start_utc,
            eval_end_utc,

            eval_timestamp
        )
        VALUES (
            :eval_date,
            :price_area,
            :model_version,

            :mae,
            :rmse,
            :r2,
            :accuracy_pct,

            :mean_actual_co2,
            :row_count,

            :eval_start_utc,
            :eval_end_utc,

            NOW()
        );
    """)

    params = {
        "eval_date": eval_date,
        "price_area": area_name,
        "model_version": model_version,

        "mae": metrics["mae"],
        "rmse": metrics["rmse"],
        "r2": metrics["r2"],
        "accuracy_pct": metrics["accuracy_pct"],

        "mean_actual_co2": metrics["mean_actual_co2"],
        "row_count": metrics["row_count"],

        "eval_start_utc": start_utc.to_pydatetime(),
        "eval_end_utc": end_utc.to_pydatetime(),
    }

    with engine.begin() as conn:
        conn.execute(delete_query, params)
        conn.execute(insert_query, params)

    print("✅ Evaluation metrics saved to model_performance_history.")


# ============================================================
# MAIN EVALUATION
# ============================================================

def run_evaluation(area_name):
    print(f"\n🧪 EVALUATING DK1 CO₂ PREDICTION PERFORMANCE")

    engine = get_db_connection()

    ensure_model_performance_history_table(engine)

    eval_date, start_utc, end_utc = get_yesterday_cph_window()

    print(f"📅 Evaluation Danish date: {eval_date}")
    print(f"🕒 UTC window: {start_utc} → {end_utc}")

    df = load_prediction_vs_actual(
        engine=engine,
        area_name=area_name,
        start_utc=start_utc,
        end_utc=end_utc,
    )

    if df.empty:
        print("⚠️ No prediction/actual pairs found.")
        print("Run predict_job.py first, then make sure ingest_job.py has synced actual CO₂ for yesterday.")
        return

    print(f"✅ Matched prediction/actual rows: {len(df)}")

    print("🔎 Available model versions:")
    print(df["model_version"].value_counts())

    for model_version, version_df in df.groupby("model_version"):
        version_df = version_df.sort_values("datetime_utc").reset_index(drop=True)

        print(f"\n📌 Evaluating model version: {model_version}")
        print(f"Rows: {len(version_df)}")

        if len(version_df) < MIN_EVAL_ROWS:
            print(
                f"⚠️ Not enough rows to evaluate model version {model_version}. "
                f"Need at least {MIN_EVAL_ROWS}, found {len(version_df)}."
            )
            continue

        metrics = calculate_metrics(version_df)

        print(f"   MAE          : {metrics['mae']:.2f} gCO₂/kWh")
        print(f"   RMSE         : {metrics['rmse']:.2f} gCO₂/kWh")

        if metrics["r2"] is not None:
            print(f"   R²           : {metrics['r2']:.4f}")
        else:
            print("   R²           : N/A")

        if metrics["accuracy_pct"] is not None:
            print(f"   Accuracy pct : {metrics['accuracy_pct']:.2f}%")
        else:
            print("   Accuracy pct : N/A")

        print(f"   Mean actual  : {metrics['mean_actual_co2']:.2f} gCO₂/kWh")

        save_metrics(
            engine=engine,
            eval_date=eval_date,
            area_name=area_name,
            model_version=model_version,
            metrics=metrics,
            start_utc=start_utc,
            end_utc=end_utc,
        )

        if (
            metrics["accuracy_pct"] is not None
            and metrics["accuracy_pct"] < ACCURACY_ALERT_THRESHOLD
        ):
            print(
                f"🚨 ALERT: Accuracy below {ACCURACY_ALERT_THRESHOLD:.0f}% "
                f"for {area_name}, model version {model_version}."
            )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    setup_artifact_logger("evaluate")

    # Current project scope is DK1 only.
    run_evaluation(PRICE_AREA)