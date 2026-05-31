import sys
import os
import pandas as pd
import numpy as np
from sqlalchemy import text

# --- PATH FIX ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.database.connection import get_db_connection


# ============================================================
# CONFIG
# ============================================================

PRICE_AREA = "DK1"
MODEL_NAME = "co2_dk1"

# Change to "TODAY" for testing.
# Normal use: "TOMORROW"
MODE = "TOMORROW"

PRICE_WEIGHT = 0.5
CO2_WEIGHT = 0.5


# ============================================================
# TIME SETUP
# ============================================================

cph_now = pd.Timestamp.now(tz="Europe/Copenhagen")

if MODE == "TODAY":
    TARGET_DATE_CPH = cph_now.normalize()
else:
    TARGET_DATE_CPH = cph_now.normalize() + pd.Timedelta(days=1)

TARGET_START_CPH = TARGET_DATE_CPH
TARGET_END_EXCLUSIVE_CPH = TARGET_DATE_CPH + pd.Timedelta(days=1)

TARGET_START_UTC = TARGET_START_CPH.tz_convert("UTC")
TARGET_END_EXCLUSIVE_UTC = TARGET_END_EXCLUSIVE_CPH.tz_convert("UTC")

print(f"🎯 MODE: {MODE}")
print(f"📍 Price area: {PRICE_AREA}")
print(f"📅 Target Danish day: {TARGET_DATE_CPH.strftime('%Y-%m-%d')}")
print(f"🕒 UTC range: {TARGET_START_UTC} → {TARGET_END_EXCLUSIVE_UTC}")


# ============================================================
# HELPERS
# ============================================================

def safe_minmax(series):
    series = pd.to_numeric(series, errors="coerce")
    min_value = series.min()
    max_value = series.max()

    if pd.isna(min_value) or pd.isna(max_value):
        return pd.Series(np.nan, index=series.index)

    if abs(max_value - min_value) < 1e-9:
        return pd.Series(0.5, index=series.index)

    return (series - min_value) / (max_value - min_value)


# ============================================================
# TABLE SETUP
# ============================================================

def ensure_signal_table(engine):
    """
    co2_aware_price_signals = final Gold Layer table.

    This raw-only version stores:
    - DK1 day-ahead price
    - predicted CO2
    - normalized price
    - normalized CO2
    - raw CO2-aware signal
    - BEST / CAUTION / AVOID recommendation
    """

    create_query = text("""
        CREATE TABLE IF NOT EXISTS co2_aware_price_signals (
            datetime_utc TIMESTAMPTZ NOT NULL,
            price_area TEXT NOT NULL,

            spot_price_dkk_kwh DOUBLE PRECISION,
            predicted_co2_g_kwh DOUBLE PRECISION,

            normalized_price DOUBLE PRECISION,
            normalized_co2 DOUBLE PRECISION,

            raw_co2_aware_signal DOUBLE PRECISION,

            model_name TEXT,
            model_version TEXT,

            price_weight DOUBLE PRECISION,
            co2_weight DOUBLE PRECISION,

            recommendation_status TEXT,
            should_charge BOOLEAN,
            is_peak_hour BOOLEAN,

            signal_created_at TIMESTAMPTZ DEFAULT NOW(),
            recommendation_created_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)

    alter_queries = [
        "ALTER TABLE co2_aware_price_signals ADD COLUMN IF NOT EXISTS datetime_utc TIMESTAMPTZ;",
        "ALTER TABLE co2_aware_price_signals ADD COLUMN IF NOT EXISTS price_area TEXT;",

        "ALTER TABLE co2_aware_price_signals ADD COLUMN IF NOT EXISTS spot_price_dkk_kwh DOUBLE PRECISION;",
        "ALTER TABLE co2_aware_price_signals ADD COLUMN IF NOT EXISTS predicted_co2_g_kwh DOUBLE PRECISION;",

        "ALTER TABLE co2_aware_price_signals ADD COLUMN IF NOT EXISTS normalized_price DOUBLE PRECISION;",
        "ALTER TABLE co2_aware_price_signals ADD COLUMN IF NOT EXISTS normalized_co2 DOUBLE PRECISION;",

        "ALTER TABLE co2_aware_price_signals ADD COLUMN IF NOT EXISTS raw_co2_aware_signal DOUBLE PRECISION;",

        "ALTER TABLE co2_aware_price_signals ADD COLUMN IF NOT EXISTS model_name TEXT;",
        "ALTER TABLE co2_aware_price_signals ADD COLUMN IF NOT EXISTS model_version TEXT;",

        "ALTER TABLE co2_aware_price_signals ADD COLUMN IF NOT EXISTS price_weight DOUBLE PRECISION;",
        "ALTER TABLE co2_aware_price_signals ADD COLUMN IF NOT EXISTS co2_weight DOUBLE PRECISION;",

        "ALTER TABLE co2_aware_price_signals ADD COLUMN IF NOT EXISTS recommendation_status TEXT;",
        "ALTER TABLE co2_aware_price_signals ADD COLUMN IF NOT EXISTS should_charge BOOLEAN;",
        "ALTER TABLE co2_aware_price_signals ADD COLUMN IF NOT EXISTS is_peak_hour BOOLEAN;",

        "ALTER TABLE co2_aware_price_signals ADD COLUMN IF NOT EXISTS signal_created_at TIMESTAMPTZ DEFAULT NOW();",
        "ALTER TABLE co2_aware_price_signals ADD COLUMN IF NOT EXISTS recommendation_created_at TIMESTAMPTZ;",
        "ALTER TABLE co2_aware_price_signals ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();",
    ]

    with engine.begin() as conn:
        conn.execute(create_query)

        for query in alter_queries:
            conn.execute(text(query))

    print("✅ co2_aware_price_signals table is ready.")


# ============================================================
# LOAD ACTIVE MODEL VERSION
# ============================================================

def get_active_model_version(engine):
    query = text("""
        SELECT model_version
        FROM model_registry
        WHERE model_name = :model_name
          AND is_active = TRUE
        ORDER BY created_at DESC
        LIMIT 1;
    """)

    with engine.connect() as conn:
        result = conn.execute(
            query,
            {"model_name": MODEL_NAME}
        ).fetchone()

    if result is None:
        raise ValueError(f"No active model found for {MODEL_NAME}")

    model_version = result[0]

    print(f"✅ Active model version: {model_version}")

    return model_version


# ============================================================
# LOAD PREDICTIONS FROM ai_forecasts
# ============================================================

def load_ai_forecasts(engine, model_version):
    """
    Load prediction output from ai_forecasts.
    This table is created by predict_job.py.
    """

    query = text("""
        SELECT
            datetime_utc,
            price_area,
            model_name,
            model_version,

            spot_price_dkk_kwh,
            predicted_co2_g_kwh

        FROM ai_forecasts
        WHERE price_area = :price_area
          AND model_version = :model_version
          AND datetime_utc >= :start_utc
          AND datetime_utc < :end_utc
        ORDER BY datetime_utc ASC;
    """)

    with engine.connect() as conn:
        df = pd.read_sql(
            query,
            conn,
            params={
                "price_area": PRICE_AREA,
                "model_version": model_version,
                "start_utc": TARGET_START_UTC.to_pydatetime(),
                "end_utc": TARGET_END_EXCLUSIVE_UTC.to_pydatetime(),
            }
        )

    if df.empty:
        raise ValueError(
            "No ai_forecasts rows found for the target day. "
            "Run predict_job.py first."
        )

    df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True)

    print(f"✅ Loaded {len(df)} rows from ai_forecasts.")

    print("🔎 Loaded UTC range:")
    print(df["datetime_utc"].min(), "→", df["datetime_utc"].max())

    print("🔎 Loaded CPH range:")
    print(
        df["datetime_utc"].min().tz_convert("Europe/Copenhagen"),
        "→",
        df["datetime_utc"].max().tz_convert("Europe/Copenhagen"),
    )

    return df


# ============================================================
# CREATE RAW CO2-AWARE PRICE SIGNAL
# ============================================================

def create_raw_co2_aware_price_signal(df):
    """
    Create raw CO2-aware price signal.

    Formula:
        raw_co2_aware_signal =
            0.5 * normalized_price
            + 0.5 * normalized_co2

    This version does NOT smooth the signal.
    """

    df = df.copy()
    df = df.sort_values("datetime_utc").reset_index(drop=True)

    required_cols = [
        "spot_price_dkk_kwh",
        "predicted_co2_g_kwh",
    ]

    if df[required_cols].isna().any().any():
        bad_cols = df[required_cols].columns[df[required_cols].isna().any()].tolist()
        raise ValueError(f"Missing values found before signal creation: {bad_cols}")

    df["normalized_price"] = safe_minmax(df["spot_price_dkk_kwh"])
    df["normalized_co2"] = safe_minmax(df["predicted_co2_g_kwh"])

    df["raw_co2_aware_signal"] = (
        PRICE_WEIGHT * df["normalized_price"]
        + CO2_WEIGHT * df["normalized_co2"]
    )

    df["price_weight"] = PRICE_WEIGHT
    df["co2_weight"] = CO2_WEIGHT

    print("✅ Raw CO₂-aware price signal created.")

    print("📊 Signal preview:")
    print(df[[
        "datetime_utc",
        "spot_price_dkk_kwh",
        "predicted_co2_g_kwh",
        "normalized_price",
        "normalized_co2",
        "raw_co2_aware_signal",
    ]])

    return df


# ============================================================
# RECOMMENDATION LOGIC
# ============================================================

def add_recommendations(df):
    """
    Apply 25% rule using raw CO2-aware signal.

    Rule:
    - Lowest 25% raw signal = BEST
    - Highest 25% raw signal = AVOID
    - Middle 50% = CAUTION

    This is simpler and fully consistent with using raw signal only.
    """

    df = df.copy()

    required_cols = [
        "raw_co2_aware_signal",
        "spot_price_dkk_kwh",
        "predicted_co2_g_kwh",
    ]

    if df[required_cols].isna().any().any():
        bad_cols = df[required_cols].columns[df[required_cols].isna().any()].tolist()
        raise ValueError(f"Missing values found before recommendation: {bad_cols}")

    signal_q25 = df["raw_co2_aware_signal"].quantile(0.25)
    signal_q75 = df["raw_co2_aware_signal"].quantile(0.75)

    print("📊 Daily thresholds:")
    print(f"  Raw signal q25: {signal_q25:.4f}")
    print(f"  Raw signal q75: {signal_q75:.4f}")

    local_dt = df["datetime_utc"].dt.tz_convert("Europe/Copenhagen")
    df["hour_cph"] = local_dt.dt.hour

    # Peak hour is stored only for interpretation.
    # It does not automatically force AVOID.
    df["is_peak_hour"] = df["hour_cph"].between(17, 21)

    def classify(row):
        signal = row["raw_co2_aware_signal"]

        if signal <= signal_q25:
            return "BEST"

        if signal >= signal_q75:
            return "AVOID"

        return "CAUTION"

    df["recommendation_status"] = df.apply(classify, axis=1)
    df["should_charge"] = df["recommendation_status"] == "BEST"

    print("✅ Recommendation counts:")
    print(df["recommendation_status"].value_counts())

    print("📋 Recommendation preview:")
    print(df[[
        "datetime_utc",
        "spot_price_dkk_kwh",
        "predicted_co2_g_kwh",
        "raw_co2_aware_signal",
        "recommendation_status",
        "should_charge",
        "is_peak_hour",
    ]])

    return df


# ============================================================
# SAVE TO co2_aware_price_signals
# ============================================================

def save_to_co2_aware_price_signals(engine, df):
    """
    Save final raw signal + recommendation.

    DELETE + INSERT is used instead of ON CONFLICT.
    This avoids errors if the old Neon table was created without
    a primary key or unique constraint.
    """

    print("💾 Saving final raw signal and recommendations to co2_aware_price_signals...")

    output_columns = [
        "datetime_utc",
        "price_area",

        "spot_price_dkk_kwh",
        "predicted_co2_g_kwh",

        "normalized_price",
        "normalized_co2",

        "raw_co2_aware_signal",

        "model_name",
        "model_version",

        "price_weight",
        "co2_weight",

        "recommendation_status",
        "should_charge",
        "is_peak_hour",
    ]

    save_df = df[output_columns].copy()

    save_df["should_charge"] = save_df["should_charge"].astype(bool)
    save_df["is_peak_hour"] = save_df["is_peak_hour"].astype(bool)

    temp_table = "temp_co2_aware_price_signals"

    save_df.to_sql(
        temp_table,
        engine,
        if_exists="replace",
        index=False,
    )

    delete_query = text(f"""
        DELETE FROM co2_aware_price_signals s
        USING {temp_table} t
        WHERE s.datetime_utc = t.datetime_utc
          AND s.price_area = t.price_area;
    """)

    insert_query = text(f"""
        INSERT INTO co2_aware_price_signals (
            datetime_utc,
            price_area,

            spot_price_dkk_kwh,
            predicted_co2_g_kwh,

            normalized_price,
            normalized_co2,

            raw_co2_aware_signal,

            model_name,
            model_version,

            price_weight,
            co2_weight,

            recommendation_status,
            should_charge,
            is_peak_hour,

            signal_created_at,
            recommendation_created_at,
            updated_at
        )
        SELECT
            datetime_utc,
            price_area,

            spot_price_dkk_kwh,
            predicted_co2_g_kwh,

            normalized_price,
            normalized_co2,

            raw_co2_aware_signal,

            model_name,
            model_version,

            price_weight,
            co2_weight,

            recommendation_status,
            should_charge,
            is_peak_hour,

            NOW(),
            NOW(),
            NOW()
        FROM {temp_table};
    """)

    with engine.begin() as conn:
        conn.execute(delete_query)
        conn.execute(insert_query)
        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table};"))

    print(f"✅ Saved {len(save_df)} rows to co2_aware_price_signals.")


# ============================================================
# MAIN JOB
# ============================================================

def run_recommendation_engine():
    print("\n🚀 Starting DK1 raw CO₂-aware price signal + recommendation job")

    engine = get_db_connection()

    ensure_signal_table(engine)

    active_model_version = get_active_model_version(engine)

    forecast_df = load_ai_forecasts(
        engine=engine,
        model_version=active_model_version,
    )

    signal_df = create_raw_co2_aware_price_signal(forecast_df)

    final_df = add_recommendations(signal_df)

    save_to_co2_aware_price_signals(engine, final_df)

    print("\n🎉 Recommendation job completed successfully.")
    print("✅ Final raw CO₂-aware price signal saved to co2_aware_price_signals")


if __name__ == "__main__":
    run_recommendation_engine()