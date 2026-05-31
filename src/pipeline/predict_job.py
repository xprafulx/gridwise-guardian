import sys
import os
import io
import json
import time
import joblib
import requests
import holidays
import numpy as np
import pandas as pd

from sqlalchemy import text
from entsoe import EntsoePandasClient

# --- PATH FIX ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.database.connection import get_db_connection


# ============================================================
# CONFIG
# ============================================================

PRICE_AREA = "DK1"
MODEL_NAME = "co2_dk1"

# Change to "TODAY" for testing.
# Normal project use: "TOMORROW"
MODE = "TOMORROW"

EDS_BASE_URL = "https://api.energidataservice.dk/dataset"

LOCATIONS = {
    "DK1": {
        "lat": 57.0488,
        "lon": 9.9187,
    }
}


# ============================================================
# TIME SETUP
# ============================================================

# Copenhagen time is used for:
# - deciding TODAY/TOMORROW
# - Danish day boundary
# - time features
# - Energi Data Service request windows
#
# UTC is used for:
# - database storage
# - merge key
# - datetime_utc column

cph_now = pd.Timestamp.now(tz="Europe/Copenhagen")
cph_now_floor = cph_now.floor("h")

if MODE == "TODAY":
    TARGET_DATE_CPH = cph_now.normalize()
else:
    TARGET_DATE_CPH = cph_now.normalize() + pd.Timedelta(days=1)

TARGET_START_CPH = TARGET_DATE_CPH
TARGET_END_EXCLUSIVE_CPH = TARGET_DATE_CPH + pd.Timedelta(days=1)

# Start recursive prediction from current Danish hour.
# This helps create tomorrow's lag features using predicted future CO2 values.
PREDICTION_START_CPH = cph_now_floor

print(f"🎯 MODE: {MODE}")
print(f"📍 Price area: {PRICE_AREA}")
print(f"📅 Target Danish day: {TARGET_DATE_CPH.strftime('%Y-%m-%d')}")
print(f"🕒 Current Danish hour: {cph_now_floor}")


# ============================================================
# SHARED HELPERS
# ============================================================

def fetch_eds_dataset(dataset_name, params, max_retries=3):
    url = f"{EDS_BASE_URL}/{dataset_name}"

    for attempt in range(1, max_retries + 1):
        try:
            res = requests.get(url, params=params, timeout=60)

            if res.status_code == 429:
                wait_s = 10 * attempt
                print(f"⏳ Rate limit for {dataset_name}. Waiting {wait_s}s...")
                time.sleep(wait_s)
                continue

            res.raise_for_status()
            return res.json().get("records", [])

        except Exception as e:
            if attempt == max_retries:
                raise RuntimeError(f"Could not fetch {dataset_name}: {e}")

            wait_s = 5 * attempt
            print(f"⚠️ {dataset_name} fetch failed. Retrying in {wait_s}s...")
            time.sleep(wait_s)

    return []


def format_eds_time(ts_cph):
    """
    Energi Data Service request windows should use Danish local time.
    Returned UTC columns are still parsed and stored as UTC.
    """
    return ts_cph.tz_convert("Europe/Copenhagen").strftime("%Y-%m-%dT%H:%M")


def filter_target_day_rows(df, target_start_cph, target_end_exclusive_cph):
    """
    Keep only the target Danish local day, but store datetime as UTC.

    Example during Danish summer time:
    2026-05-31 00:00–23:00 CPH
    becomes
    2026-05-30 22:00 → 2026-05-31 21:00 UTC
    """

    df = df.copy()
    df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True)

    local_dt = df["datetime_utc"].dt.tz_convert("Europe/Copenhagen")

    mask = (
        (local_dt >= target_start_cph)
        & (local_dt < target_end_exclusive_cph)
    )

    filtered_df = (
        df[mask]
        .copy()
        .sort_values("datetime_utc")
        .reset_index(drop=True)
    )

    if filtered_df.empty:
        raise ValueError("No rows found for target Danish day after filtering.")

    print("✅ Filtered prediction rows for target Danish day.")
    print("🔎 UTC range to save:")
    print(filtered_df["datetime_utc"].min(), "→", filtered_df["datetime_utc"].max())

    print("🔎 CPH range to save:")
    print(
        filtered_df["datetime_utc"].min().tz_convert("Europe/Copenhagen"),
        "→",
        filtered_df["datetime_utc"].max().tz_convert("Europe/Copenhagen"),
    )

    print(f"✅ Rows to save: {len(filtered_df)}")

    return filtered_df


# ============================================================
# MODEL REGISTRY
# ============================================================

def load_active_model_from_neon(engine):
    query = text("""
        SELECT
            model_binary,
            model_version
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
        raise ValueError(
            f"No active model found in model_registry for model_name = {MODEL_NAME}"
        )

    model_binary = bytes(result[0])
    model_version = result[1]

    payload = joblib.load(io.BytesIO(model_binary))

    model = payload["model"]
    scaler = payload["scaler"]
    feature_columns = payload["features"]

    print(f"✅ Loaded active model: {MODEL_NAME}")
    print(f"🏷️ Model version: {model_version}")
    print(f"🧠 Feature count: {len(feature_columns)}")

    removed_features = [
        "forecast_wind_generation_gw",
        "forecast_solar_generation_gw",
    ]

    still_expected = [col for col in removed_features if col in feature_columns]

    if still_expected:
        raise ValueError(
            "The active model still expects removed wind/solar production forecast features: "
            f"{still_expected}. Retrain train_job.py first, then run predict_job.py again."
        )

    return model, scaler, feature_columns, model_version


# ============================================================
# AI FORECASTS TABLE
# ============================================================

def ensure_ai_forecasts_table(engine):
    """
    ai_forecasts = prediction audit table.
    Stores all features used for prediction + predicted CO2.
    """

    create_query = text("""
        CREATE TABLE IF NOT EXISTS ai_forecasts (
            datetime_utc TIMESTAMPTZ NOT NULL,
            price_area TEXT NOT NULL,
            model_version TEXT NOT NULL,

            model_name TEXT,

            spot_price_dkk_kwh DOUBLE PRECISION,
            market_price_dkk_kwh DOUBLE PRECISION,

            wind_speed DOUBLE PRECISION,
            solar_radiation DOUBLE PRECISION,
            temperature DOUBLE PRECISION,

            forecast_load_mw DOUBLE PRECISION,
            forecast_load_gw DOUBLE PRECISION,

            co2_lag_1h DOUBLE PRECISION,
            co2_lag_2h DOUBLE PRECISION,
            co2_lag_24h DOUBLE PRECISION,
            co2_lag_168h DOUBLE PRECISION,

            co2_rolling_3h DOUBLE PRECISION,
            co2_rolling_6h DOUBLE PRECISION,
            co2_rolling_24h DOUBLE PRECISION,

            co2_diff_1h DOUBLE PRECISION,
            co2_diff_24h DOUBLE PRECISION,

            hour INTEGER,
            day_of_week INTEGER,
            month INTEGER,
            day_of_year INTEGER,

            hour_sin DOUBLE PRECISION,
            hour_cos DOUBLE PRECISION,
            month_sin DOUBLE PRECISION,
            month_cos DOUBLE PRECISION,
            day_of_year_sin DOUBLE PRECISION,
            day_of_year_cos DOUBLE PRECISION,

            is_weekend BOOLEAN,
            is_holiday BOOLEAN,

            predicted_co2_g_kwh DOUBLE PRECISION,
            predicted_co2 DOUBLE PRECISION,

            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),

            PRIMARY KEY (datetime_utc, price_area, model_version)
        );
    """)

    alter_queries = [
        "ALTER TABLE ai_forecasts ADD COLUMN IF NOT EXISTS model_name TEXT;",
        "ALTER TABLE ai_forecasts ADD COLUMN IF NOT EXISTS spot_price_dkk_kwh DOUBLE PRECISION;",
        "ALTER TABLE ai_forecasts ADD COLUMN IF NOT EXISTS market_price_dkk_kwh DOUBLE PRECISION;",

        "ALTER TABLE ai_forecasts ADD COLUMN IF NOT EXISTS wind_speed DOUBLE PRECISION;",
        "ALTER TABLE ai_forecasts ADD COLUMN IF NOT EXISTS solar_radiation DOUBLE PRECISION;",
        "ALTER TABLE ai_forecasts ADD COLUMN IF NOT EXISTS temperature DOUBLE PRECISION;",

        "ALTER TABLE ai_forecasts ADD COLUMN IF NOT EXISTS forecast_load_mw DOUBLE PRECISION;",
        "ALTER TABLE ai_forecasts ADD COLUMN IF NOT EXISTS forecast_load_gw DOUBLE PRECISION;",

        "ALTER TABLE ai_forecasts ADD COLUMN IF NOT EXISTS co2_lag_1h DOUBLE PRECISION;",
        "ALTER TABLE ai_forecasts ADD COLUMN IF NOT EXISTS co2_lag_2h DOUBLE PRECISION;",
        "ALTER TABLE ai_forecasts ADD COLUMN IF NOT EXISTS co2_lag_24h DOUBLE PRECISION;",
        "ALTER TABLE ai_forecasts ADD COLUMN IF NOT EXISTS co2_lag_168h DOUBLE PRECISION;",

        "ALTER TABLE ai_forecasts ADD COLUMN IF NOT EXISTS co2_rolling_3h DOUBLE PRECISION;",
        "ALTER TABLE ai_forecasts ADD COLUMN IF NOT EXISTS co2_rolling_6h DOUBLE PRECISION;",
        "ALTER TABLE ai_forecasts ADD COLUMN IF NOT EXISTS co2_rolling_24h DOUBLE PRECISION;",

        "ALTER TABLE ai_forecasts ADD COLUMN IF NOT EXISTS co2_diff_1h DOUBLE PRECISION;",
        "ALTER TABLE ai_forecasts ADD COLUMN IF NOT EXISTS co2_diff_24h DOUBLE PRECISION;",

        "ALTER TABLE ai_forecasts ADD COLUMN IF NOT EXISTS hour INTEGER;",
        "ALTER TABLE ai_forecasts ADD COLUMN IF NOT EXISTS day_of_week INTEGER;",
        "ALTER TABLE ai_forecasts ADD COLUMN IF NOT EXISTS month INTEGER;",
        "ALTER TABLE ai_forecasts ADD COLUMN IF NOT EXISTS day_of_year INTEGER;",

        "ALTER TABLE ai_forecasts ADD COLUMN IF NOT EXISTS hour_sin DOUBLE PRECISION;",
        "ALTER TABLE ai_forecasts ADD COLUMN IF NOT EXISTS hour_cos DOUBLE PRECISION;",
        "ALTER TABLE ai_forecasts ADD COLUMN IF NOT EXISTS month_sin DOUBLE PRECISION;",
        "ALTER TABLE ai_forecasts ADD COLUMN IF NOT EXISTS month_cos DOUBLE PRECISION;",
        "ALTER TABLE ai_forecasts ADD COLUMN IF NOT EXISTS day_of_year_sin DOUBLE PRECISION;",
        "ALTER TABLE ai_forecasts ADD COLUMN IF NOT EXISTS day_of_year_cos DOUBLE PRECISION;",

        "ALTER TABLE ai_forecasts ADD COLUMN IF NOT EXISTS is_weekend BOOLEAN;",
        "ALTER TABLE ai_forecasts ADD COLUMN IF NOT EXISTS is_holiday BOOLEAN;",

        "ALTER TABLE ai_forecasts ADD COLUMN IF NOT EXISTS predicted_co2_g_kwh DOUBLE PRECISION;",
        "ALTER TABLE ai_forecasts ADD COLUMN IF NOT EXISTS predicted_co2 DOUBLE PRECISION;",

        "ALTER TABLE ai_forecasts ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW();",
        "ALTER TABLE ai_forecasts ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();",
    ]

    with engine.begin() as conn:
        conn.execute(create_query)

        for query in alter_queries:
            conn.execute(text(query))

    print("✅ ai_forecasts table is ready.")


# ============================================================
# DATA FETCHING
# ============================================================

def fetch_day_ahead_prices(area_name, start_cph, end_exclusive_cph):
    print("📡 Fetching DK1 day-ahead prices...")

    params = {
        "start": format_eds_time(start_cph),
        "end": format_eds_time(end_exclusive_cph),
        "filter": json.dumps({"PriceArea": [area_name]}),
        "sort": "TimeUTC",
        "limit": 0,
    }

    print("🔎 DayAheadPrices request window CPH:")
    print(params["start"], "→", params["end"])

    records = fetch_eds_dataset("DayAheadPrices", params)
    df = pd.DataFrame(records)

    if df.empty:
        raise ValueError("No DayAheadPrices data returned.")

    if "TimeUTC" not in df.columns:
        raise ValueError(
            f"TimeUTC column missing in DayAheadPrices. Columns: {df.columns.tolist()}"
        )

    if "DayAheadPriceDKK" not in df.columns:
        raise ValueError(
            f"DayAheadPriceDKK column missing in DayAheadPrices. Columns: {df.columns.tolist()}"
        )

    df["datetime_utc"] = pd.to_datetime(df["TimeUTC"], utc=True)

    # Energi Data Service price is DKK/MWh.
    # Divide by 1000 to get DKK/kWh.
    df["spot_price_dkk_kwh"] = pd.to_numeric(
        df["DayAheadPriceDKK"],
        errors="coerce"
    ) / 1000

    # If API returns 15-minute values, convert to hourly average.
    df = (
        df.set_index("datetime_utc")["spot_price_dkk_kwh"]
        .resample("h")
        .mean()
        .dropna()
        .reset_index()
    )

    print("🔎 Price time range UTC:")
    print(df["datetime_utc"].min(), "→", df["datetime_utc"].max())

    print("🔎 Price time range CPH:")
    print(
        df["datetime_utc"].min().tz_convert("Europe/Copenhagen"),
        "→",
        df["datetime_utc"].max().tz_convert("Europe/Copenhagen"),
    )

    return df


def fetch_weather_forecast(area_name, start_cph, end_exclusive_cph):
    print("🌦️ Fetching weather forecast from Open-Meteo...")

    lat = LOCATIONS[area_name]["lat"]
    lon = LOCATIONS[area_name]["lon"]

    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "wind_speed_10m,shortwave_radiation,temperature_2m",
        "timezone": "UTC",
        "forecast_days": 7,
    }

    res = requests.get(url, params=params, timeout=60)
    res.raise_for_status()

    data = res.json()

    if "hourly" not in data:
        raise ValueError("Open-Meteo response missing hourly data.")

    hourly = data["hourly"]

    df = pd.DataFrame({
        "datetime_utc": pd.to_datetime(hourly["time"], utc=True),
        "wind_speed": hourly["wind_speed_10m"],
        "solar_radiation": hourly["shortwave_radiation"],
        "temperature": hourly["temperature_2m"],
    })

    start_utc = start_cph.tz_convert("UTC")
    end_utc = end_exclusive_cph.tz_convert("UTC")

    df = df[
        (df["datetime_utc"] >= start_utc)
        & (df["datetime_utc"] < end_utc)
    ].copy()

    df[["wind_speed", "solar_radiation", "temperature"]] = df[
        ["wind_speed", "solar_radiation", "temperature"]
    ].apply(pd.to_numeric, errors="coerce")

    return df


def fetch_forecast_load(area_name, start_cph, end_exclusive_cph):
    print("⚡ Fetching ENTSO-E load forecast...")

    token = os.getenv("ENTSOE_TOKEN")

    if not token:
        raise ValueError("ENTSOE_TOKEN not found in environment variables.")

    if area_name != "DK1":
        raise ValueError("This prediction job is DK1 only.")

    client = EntsoePandasClient(api_key=token)

    start = pd.Timestamp(start_cph).tz_convert("Europe/Copenhagen")
    end = pd.Timestamp(end_exclusive_cph).tz_convert("Europe/Copenhagen")

    load_data = client.query_load_forecast(
        country_code="DK_1",
        start=start,
        end=end,
    )

    if load_data is None or len(load_data) == 0:
        raise ValueError("ENTSO-E returned no load forecast data.")

    if isinstance(load_data, pd.DataFrame):
        numeric_cols = load_data.select_dtypes(include=[np.number]).columns.tolist()

        if not numeric_cols:
            raise ValueError("ENTSO-E load forecast returned no numeric columns.")

        load_series = load_data[numeric_cols[0]]
    else:
        load_series = load_data

    load_df = load_series.reset_index()
    load_df.columns = ["datetime_utc", "forecast_load_mw"]

    load_df["datetime_utc"] = pd.to_datetime(load_df["datetime_utc"], utc=True)
    load_df["forecast_load_mw"] = pd.to_numeric(
        load_df["forecast_load_mw"],
        errors="coerce",
    )

    load_df = (
        load_df
        .set_index("datetime_utc")
        .resample("h")
        .mean(numeric_only=True)
        .reset_index()
    )

    return load_df[["datetime_utc", "forecast_load_mw"]]


# ============================================================
# CO2 HISTORY FOR LAG FEATURES
# ============================================================

def fetch_recent_co2_history_from_api(area_name, prediction_start_cph):
    print("📡 Fetching recent CO₂ history from Energi Data Service...")

    history_end_cph = prediction_start_cph - pd.Timedelta(hours=1)
    history_start_cph = history_end_cph - pd.Timedelta(days=8)

    params = {
        "start": format_eds_time(history_start_cph),
        "end": format_eds_time(history_end_cph + pd.Timedelta(hours=1)),
        "filter": json.dumps({"PriceArea": [area_name]}),
        "sort": "Minutes5UTC",
        "limit": 0,
    }

    try:
        records = fetch_eds_dataset("CO2Emis", params)
        df = pd.DataFrame(records)

        if df.empty:
            print("⚠️ CO₂ API returned empty history.")
            return None

        if "Minutes5UTC" not in df.columns:
            print(f"⚠️ Minutes5UTC missing in CO2Emis. Columns: {df.columns.tolist()}")
            return None

        if "CO2Emission" not in df.columns:
            print(f"⚠️ CO2Emission missing in CO2Emis. Columns: {df.columns.tolist()}")
            return None

        df["datetime_utc"] = pd.to_datetime(df["Minutes5UTC"], utc=True)
        df["co2_emissions_g_kwh"] = pd.to_numeric(
            df["CO2Emission"],
            errors="coerce",
        )

        hourly = (
            df.set_index("datetime_utc")["co2_emissions_g_kwh"]
            .resample("h")
            .mean()
            .ffill()
            .bfill()
        )

        history_end_utc = history_end_cph.tz_convert("UTC")
        hourly = hourly[hourly.index <= history_end_utc]

        history = hourly.dropna().tail(168).tolist()

        if len(history) < 168:
            print(f"⚠️ CO₂ API history has only {len(history)} hourly values.")
            return None

        print("✅ Recent CO₂ history loaded from API.")
        return history

    except Exception as e:
        print(f"⚠️ CO₂ API history fetch failed: {e}")
        return None


def fetch_recent_co2_history_from_neon(engine, area_name, prediction_start_cph):
    print("🗄️ Fetching recent CO₂ history from Neon fallback...")

    prediction_start_utc = prediction_start_cph.tz_convert("UTC")

    query = text("""
        SELECT
            datetime_utc,
            co2_emissions_g_kwh
        FROM processed_features
        WHERE price_area = :area_name
          AND COALESCE(is_forecast, FALSE) = FALSE
          AND datetime_utc < :prediction_start
          AND co2_emissions_g_kwh IS NOT NULL
        ORDER BY datetime_utc DESC
        LIMIT 300;
    """)

    with engine.connect() as conn:
        df = pd.read_sql(
            query,
            conn,
            params={
                "area_name": area_name,
                "prediction_start": prediction_start_utc.to_pydatetime(),
            },
        )

    if df.empty:
        raise ValueError("No CO₂ history found in Neon.")

    df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True)
    df = df.sort_values("datetime_utc")

    history = df["co2_emissions_g_kwh"].dropna().tail(168).tolist()

    if len(history) < 168:
        raise ValueError(
            f"Need at least 168 CO₂ history values, but only found {len(history)}."
        )

    print("✅ Recent CO₂ history loaded from Neon.")
    return history


def get_recent_co2_history(engine, area_name, prediction_start_cph):
    history = fetch_recent_co2_history_from_api(area_name, prediction_start_cph)

    if history is not None:
        return history

    return fetch_recent_co2_history_from_neon(
        engine,
        area_name,
        prediction_start_cph,
    )


# ============================================================
# FEATURE ENGINEERING
# ============================================================

def add_time_features(df):
    df = df.copy()

    local_dt = df["datetime_utc"].dt.tz_convert("Europe/Copenhagen")

    df["hour"] = local_dt.dt.hour
    df["day_of_week"] = local_dt.dt.dayofweek
    df["month"] = local_dt.dt.month
    df["day_of_year"] = local_dt.dt.dayofyear

    df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
    df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)

    df["month_sin"] = np.sin(2 * np.pi * (df["month"] - 1) / 12)
    df["month_cos"] = np.cos(2 * np.pi * (df["month"] - 1) / 12)

    df["day_of_year_sin"] = np.sin(2 * np.pi * df["day_of_year"] / 365.25)
    df["day_of_year_cos"] = np.cos(2 * np.pi * df["day_of_year"] / 365.25)

    df["is_weekend"] = (df["day_of_week"] >= 5).astype(int)

    dk_holidays = holidays.country_holidays("DK")
    df["is_holiday"] = local_dt.dt.date.map(lambda d: d in dk_holidays).astype(int)

    return df


def build_future_feature_base(area_name, prediction_start_cph, target_end_exclusive_cph):
    print("🧱 Building future feature base...")

    timestamps_cph = pd.date_range(
        start=prediction_start_cph,
        end=target_end_exclusive_cph - pd.Timedelta(hours=1),
        freq="h",
    )

    base_df = pd.DataFrame({
        "datetime_utc": timestamps_cph.tz_convert("UTC")
    })

    base_df["price_area"] = area_name

    price_df = fetch_day_ahead_prices(
        area_name,
        prediction_start_cph,
        target_end_exclusive_cph,
    )

    weather_df = fetch_weather_forecast(
        area_name,
        prediction_start_cph,
        target_end_exclusive_cph,
    )

    load_df = fetch_forecast_load(
        area_name,
        prediction_start_cph,
        target_end_exclusive_cph,
    )

    df = base_df.merge(price_df, on="datetime_utc", how="left")
    df = df.merge(weather_df, on="datetime_utc", how="left")
    df = df.merge(load_df, on="datetime_utc", how="left")

    df = df.sort_values("datetime_utc").reset_index(drop=True)

    # Convert MW to GW for prediction only.
    # Neon/source data stays in MW.
    df["forecast_load_gw"] = df["forecast_load_mw"] / 1000

    df = add_time_features(df)

    required_source_columns = [
        "spot_price_dkk_kwh",
        "wind_speed",
        "solar_radiation",
        "temperature",
        "forecast_load_mw",
        "forecast_load_gw",
    ]

    missing_summary = df[required_source_columns].isna().sum()
    missing_columns = missing_summary[missing_summary > 0]

    if not missing_columns.empty:
        print("❌ Missing future feature values:")
        print(missing_columns)

        debug_df = df[
            df[required_source_columns].isna().any(axis=1)
        ][
            ["datetime_utc"] + required_source_columns
        ].copy()

        debug_df["datetime_cph"] = debug_df["datetime_utc"].dt.tz_convert("Europe/Copenhagen")

        print("🔎 Missing rows:")
        print(debug_df[["datetime_utc", "datetime_cph"] + required_source_columns])

        raise ValueError(
            "Future feature base has missing values. "
            "Check price, Open-Meteo weather forecast, or ENTSO-E load forecast."
        )

    print(f"✅ Future feature base ready with {len(df)} rows.")

    return df


# ============================================================
# PREDICTION
# ============================================================

def recursive_predict_co2(
    future_df,
    history,
    model,
    scaler,
    feature_columns,
):
    print("🔮 Running recursive CO₂ prediction...")

    predicted_rows = []
    history = list(history)

    if len(history) < 168:
        raise ValueError("Need 168 historical CO₂ values for recursive prediction.")

    for _, row in future_df.iterrows():
        dt_utc = row["datetime_utc"]

        co2_lag_1h = history[-1]
        co2_lag_2h = history[-2]
        co2_lag_24h = history[-24]
        co2_lag_168h = history[-168]

        co2_rolling_3h = float(np.mean(history[-3:]))
        co2_rolling_6h = float(np.mean(history[-6:]))
        co2_rolling_24h = float(np.mean(history[-24:]))

        co2_diff_1h = co2_lag_1h - co2_lag_2h
        co2_diff_24h = co2_lag_1h - co2_lag_24h

        feature_dict = {
            "spot_price_dkk_kwh": row["spot_price_dkk_kwh"],

            "wind_speed": row["wind_speed"],
            "solar_radiation": row["solar_radiation"],
            "temperature": row["temperature"],

            "forecast_load_gw": row["forecast_load_gw"],

            "co2_lag_1h": co2_lag_1h,
            "co2_lag_2h": co2_lag_2h,
            "co2_lag_24h": co2_lag_24h,
            "co2_lag_168h": co2_lag_168h,

            "co2_rolling_3h": co2_rolling_3h,
            "co2_rolling_6h": co2_rolling_6h,
            "co2_rolling_24h": co2_rolling_24h,

            "co2_diff_1h": co2_diff_1h,
            "co2_diff_24h": co2_diff_24h,

            "hour": row["hour"],
            "day_of_week": row["day_of_week"],
            "month": row["month"],
            "day_of_year": row["day_of_year"],

            "hour_sin": row["hour_sin"],
            "hour_cos": row["hour_cos"],
            "month_sin": row["month_sin"],
            "month_cos": row["month_cos"],
            "day_of_year_sin": row["day_of_year_sin"],
            "day_of_year_cos": row["day_of_year_cos"],

            "is_weekend": int(row["is_weekend"]),
            "is_holiday": int(row["is_holiday"]),
        }

        missing_model_features = [
            col for col in feature_columns
            if col not in feature_dict
        ]

        if missing_model_features:
            raise ValueError(
                f"Model expects features missing from prediction code: {missing_model_features}"
            )

        X = pd.DataFrame([feature_dict])[feature_columns]
        X = X.replace([np.inf, -np.inf], np.nan)

        if X.isna().any().any():
            bad_cols = X.columns[X.isna().any()].tolist()
            raise ValueError(
                f"NaN found in model features at {dt_utc}. Bad columns: {bad_cols}"
            )

        X_scaled = scaler.transform(X)

        predicted_co2 = float(model.predict(X_scaled)[0])
        predicted_co2 = max(0.0, predicted_co2)

        predicted_rows.append({
            "datetime_utc": dt_utc,
            "price_area": row["price_area"],

            "spot_price_dkk_kwh": float(row["spot_price_dkk_kwh"]),

            "wind_speed": float(row["wind_speed"]),
            "solar_radiation": float(row["solar_radiation"]),
            "temperature": float(row["temperature"]),

            "forecast_load_mw": float(row["forecast_load_mw"]),
            "forecast_load_gw": float(row["forecast_load_gw"]),

            **feature_dict,

            "predicted_co2_g_kwh": predicted_co2,
        })

        # Append prediction so future lag features can be created.
        history.append(predicted_co2)

    prediction_df = pd.DataFrame(predicted_rows)

    print(f"✅ CO₂ prediction complete for {len(prediction_df)} rows.")

    return prediction_df


# ============================================================
# SAVE TO NEON
# ============================================================

def save_ai_forecasts_to_neon(engine, prediction_df, model_version):
    print("💾 Saving prediction audit rows to ai_forecasts...")

    df = prediction_df.copy()

    df["model_name"] = MODEL_NAME
    df["model_version"] = model_version

    # Compatibility aliases for old dashboard/code.
    df["market_price_dkk_kwh"] = df["spot_price_dkk_kwh"]
    df["predicted_co2"] = df["predicted_co2_g_kwh"]

    df["is_weekend"] = df["is_weekend"].astype(bool)
    df["is_holiday"] = df["is_holiday"].astype(bool)

    output_columns = [
        "datetime_utc",
        "price_area",
        "model_version",
        "model_name",

        "spot_price_dkk_kwh",
        "market_price_dkk_kwh",

        "wind_speed",
        "solar_radiation",
        "temperature",

        "forecast_load_mw",
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

        "predicted_co2_g_kwh",
        "predicted_co2",
    ]

    df = df[output_columns]

    temp_table = "temp_ai_forecasts"

    df.to_sql(
        temp_table,
        engine,
        if_exists="replace",
        index=False,
    )

    update_columns = [
        col for col in output_columns
        if col not in ["datetime_utc", "price_area", "model_version"]
    ]

    update_sql = ",\n            ".join(
        [f"{col} = EXCLUDED.{col}" for col in update_columns]
    )

    insert_columns_sql = ",\n            ".join(output_columns + ["created_at", "updated_at"])
    select_columns_sql = ",\n            ".join(output_columns + ["NOW()", "NOW()"])

    upsert_query = text(f"""
        INSERT INTO ai_forecasts (
            {insert_columns_sql}
        )
        SELECT
            {select_columns_sql}
        FROM {temp_table}
        ON CONFLICT (datetime_utc, price_area, model_version)
        DO UPDATE SET
            {update_sql},
            updated_at = NOW();
    """)

    with engine.begin() as conn:
        conn.execute(upsert_query)
        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table};"))

    print(f"✅ Saved {len(df)} rows to ai_forecasts.")


# ============================================================
# MAIN JOB
# ============================================================

def run_job():
    print("\n🚀 Starting DK1 CO₂ prediction job")

    engine = get_db_connection()

    ensure_ai_forecasts_table(engine)

    model, scaler, feature_columns, model_version = load_active_model_from_neon(engine)

    history = get_recent_co2_history(
        engine=engine,
        area_name=PRICE_AREA,
        prediction_start_cph=PREDICTION_START_CPH,
    )

    future_df = build_future_feature_base(
        area_name=PRICE_AREA,
        prediction_start_cph=PREDICTION_START_CPH,
        target_end_exclusive_cph=TARGET_END_EXCLUSIVE_CPH,
    )

    full_prediction_df = recursive_predict_co2(
        future_df=future_df,
        history=history,
        model=model,
        scaler=scaler,
        feature_columns=feature_columns,
    )

    # Save only the target Danish day into ai_forecasts.
    # Example summer time:
    # 00:00–23:00 CPH = 22:00 previous day → 21:00 target day UTC.
    target_prediction_df = filter_target_day_rows(
        df=full_prediction_df,
        target_start_cph=TARGET_START_CPH,
        target_end_exclusive_cph=TARGET_END_EXCLUSIVE_CPH,
    )

    save_ai_forecasts_to_neon(
        engine=engine,
        prediction_df=target_prediction_df,
        model_version=model_version,
    )

    print("\n🎉 DK1 prediction job completed successfully.")
    print("✅ Prediction audit output saved to ai_forecasts")


if __name__ == "__main__":
    run_job()