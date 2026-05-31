import json
import os
import time
import numpy as np
import pandas as pd
import requests

from datetime import datetime, timedelta, timezone
from sqlalchemy import text
from entsoe import EntsoePandasClient

from src.database.connection import get_db_connection


# ============================================================
# CONFIG
# ============================================================

PRICE_AREAS = ["DK1"]

LOCATIONS = {
    "DK1": {"lat": 57.0488, "lon": 9.9187}  # Aalborg / DK1
}

EDS_BASE_URL = "https://api.energidataservice.dk/dataset"


# ============================================================
# HELPERS
# ============================================================

def find_time_column(df):
    """Find timestamp column from different API datasets."""
    candidates = [
        "HourUTC",
        "TimeUTC",
        "Minutes5UTC",
        "HourDK",
        "TimeDK",
        "Minutes5DK",
        "ds"
    ]

    for col in candidates:
        if col in df.columns:
            return col

    return None


def fetch_eds_dataset(dataset_name, params, max_retries=3):
    """Fetch data from Energi Data Service."""
    url = f"{EDS_BASE_URL}/{dataset_name}"

    for attempt in range(1, max_retries + 1):
        res = requests.get(url, params=params, timeout=60)

        if res.status_code == 429:
            wait_s = 10 * attempt
            print(f"⏳ Rate limit for {dataset_name}. Waiting {wait_s}s...")
            time.sleep(wait_s)
            continue

        res.raise_for_status()
        return res.json().get("records", [])

    raise RuntimeError(f"Could not fetch {dataset_name} after {max_retries} retries.")


def get_weather_data(lat, lon, days_back=4):
    """
    Fetch weather from Open-Meteo Forecast API.

    Stored columns:
    wind_speed
    solar_radiation
    temperature
    """
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        "&hourly=wind_speed_10m,shortwave_radiation,temperature_2m"
        f"&past_days={days_back}"
        "&timezone=UTC"
    )

    try:
        res = requests.get(url, timeout=30)

        if res.status_code != 200:
            print(f"⚠️ Weather API returned status {res.status_code}")
            return pd.DataFrame()

        data = res.json()

        return pd.DataFrame({
            "datetime_utc": pd.to_datetime(data["hourly"]["time"], utc=True),
            "wind_speed": data["hourly"]["wind_speed_10m"],
            "solar_radiation": data["hourly"]["shortwave_radiation"],
            "temperature": data["hourly"]["temperature_2m"],
        })

    except Exception as e:
        print(f"⚠️ Weather fetch failed: {e}")
        return pd.DataFrame()


def get_forecast_renewables(start_date, end_date):
    """
    Fetch forecast wind + solar generation from Energi Data Service Forecasts_Hour.

    Output:
    forecast_wind_generation_mw
    forecast_solar_generation_mw
    """
    params = {
        "start": start_date,
        "end": end_date,
        "filter": json.dumps({"PriceArea": PRICE_AREAS}),
        "sort": "HourUTC",
        "limit": 0,
    }

    records = fetch_eds_dataset("Forecasts_Hour", params)
    df = pd.DataFrame(records)

    empty_cols = [
        "datetime_utc",
        "price_area",
        "forecast_wind_generation_mw",
        "forecast_solar_generation_mw",
    ]

    if df.empty:
        print("⚠️ No Forecasts_Hour records found.")
        return pd.DataFrame(columns=empty_cols)

    time_col = find_time_column(df)

    if time_col is None:
        print("⚠️ No time column found in Forecasts_Hour.")
        print(df.columns.tolist())
        return pd.DataFrame(columns=empty_cols)

    if "ForecastType" not in df.columns:
        print("⚠️ ForecastType column not found in Forecasts_Hour.")
        print(df.columns.tolist())
        return pd.DataFrame(columns=empty_cols)

    value_col = None

    for candidate in [
        "ForecastDayAhead",
        "Forecast1Hour",
        "Forecast5Hour",
        "ForecastCurrent",
    ]:
        if candidate in df.columns:
            value_col = candidate
            break

    if value_col is None:
        print("⚠️ No forecast value column found in Forecasts_Hour.")
        print(df.columns.tolist())
        return pd.DataFrame(columns=empty_cols)

    df["datetime_utc"] = pd.to_datetime(df[time_col], utc=True)
    df["price_area"] = df["PriceArea"]
    df[value_col] = pd.to_numeric(df[value_col], errors="coerce")

    df["ForecastType"] = df["ForecastType"].astype(str).str.strip()

    wind_mask = df["ForecastType"].str.contains("wind", case=False, na=False)
    solar_mask = df["ForecastType"].str.contains("solar", case=False, na=False)

    df["feature_name"] = None
    df.loc[wind_mask, "feature_name"] = "forecast_wind_generation_mw"
    df.loc[solar_mask, "feature_name"] = "forecast_solar_generation_mw"

    df = df.dropna(subset=["feature_name"])

    if df.empty:
        print("⚠️ No wind/solar rows matched in Forecasts_Hour.")
        return pd.DataFrame(columns=empty_cols)

    pivot_df = (
        df.pivot_table(
            index=["datetime_utc", "price_area"],
            columns="feature_name",
            values=value_col,
            aggfunc="sum"
        )
        .reset_index()
    )

    for col in ["forecast_wind_generation_mw", "forecast_solar_generation_mw"]:
        if col not in pivot_df.columns:
            pivot_df[col] = np.nan

    return pivot_df[empty_cols]


def get_forecast_load(start_ts, end_exclusive_ts):
    """
    Fetch DK1 day-ahead total load forecast from ENTSO-E.

    Requires:
    ENTSOE_TOKEN in environment variables.

    Output:
    forecast_load_mw
    """
    token = os.getenv("ENTSOE_TOKEN")

    empty_cols = ["datetime_utc", "price_area", "forecast_load_mw"]

    if not token:
        print("⚠️ ENTSOE_TOKEN not found. forecast_load_mw will stay empty.")
        return pd.DataFrame(columns=empty_cols)

    try:
        client = EntsoePandasClient(api_key=token)

        start = pd.Timestamp(start_ts)

        if start.tzinfo is None:
            start = start.tz_localize("UTC")

        start = start.tz_convert("Europe/Copenhagen")

        end = pd.Timestamp(end_exclusive_ts)

        if end.tzinfo is None:
            end = end.tz_localize("UTC")

        end = end.tz_convert("Europe/Copenhagen")

        load_data = client.query_load_forecast(
            country_code="DK_1",
            start=start,
            end=end
        )

        if load_data is None or len(load_data) == 0:
            print("⚠️ ENTSO-E returned no load forecast data.")
            return pd.DataFrame(columns=empty_cols)

        if isinstance(load_data, pd.DataFrame):
            numeric_cols = load_data.select_dtypes(include=[np.number]).columns.tolist()

            if not numeric_cols:
                print("⚠️ ENTSO-E load forecast has no numeric column.")
                return pd.DataFrame(columns=empty_cols)

            load_series = load_data[numeric_cols[0]]

        else:
            load_series = load_data

        load_df = load_series.reset_index()
        load_df.columns = ["datetime_utc", "forecast_load_mw"]

        load_df["datetime_utc"] = pd.to_datetime(load_df["datetime_utc"], utc=True)
        load_df["forecast_load_mw"] = pd.to_numeric(
            load_df["forecast_load_mw"],
            errors="coerce"
        )

        load_df["price_area"] = "DK1"

        load_df = (
            load_df
            .set_index("datetime_utc")
            .groupby("price_area")
            .resample("h")
            .mean(numeric_only=True)
            .reset_index()
        )

        return load_df[empty_cols]

    except Exception as e:
        print(f"⚠️ ENTSO-E load forecast fetch failed: {e}")
        return pd.DataFrame(columns=empty_cols)


def add_time_features(df):
    """Add Danish local time features."""
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

    df["is_weekend"] = df["day_of_week"] >= 5

    try:
        import holidays
        dk_holidays = holidays.country_holidays("DK")
        df["is_holiday"] = local_dt.dt.date.map(lambda d: d in dk_holidays)
    except Exception:
        print("⚠️ holidays package not available. Setting is_holiday = False.")
        df["is_holiday"] = False

    return df


def add_co2_lag_features(final_df, engine, history_start_utc, end_exclusive_utc):
    """
    Add CO2 lag, rolling, and diff features using existing Neon history.
    Rolling features use past CO2 only.
    """
    final_df = final_df.copy()

    query = text("""
        SELECT
            datetime_utc,
            price_area,
            co2_emissions_g_kwh
        FROM processed_features
        WHERE price_area = 'DK1'
          AND datetime_utc >= :history_start
          AND datetime_utc < :end_exclusive
        ORDER BY datetime_utc;
    """)

    try:
        with engine.connect() as conn:
            history_df = pd.read_sql(
                query,
                conn,
                params={
                    "history_start": history_start_utc,
                    "end_exclusive": end_exclusive_utc,
                }
            )
    except Exception as e:
        print(f"⚠️ Could not read existing history for lag features: {e}")
        history_df = pd.DataFrame(
            columns=["datetime_utc", "price_area", "co2_emissions_g_kwh"]
        )

    history_df["datetime_utc"] = pd.to_datetime(history_df["datetime_utc"], utc=True)

    new_co2 = final_df[
        ["datetime_utc", "price_area", "co2_emissions_g_kwh"]
    ].copy()

    combined = pd.concat([history_df, new_co2], ignore_index=True)

    combined = combined.drop_duplicates(
        subset=["datetime_utc", "price_area"],
        keep="last"
    )

    combined = combined.sort_values(["price_area", "datetime_utc"])

    output_groups = []

    for _, group in combined.groupby("price_area", group_keys=False):
        group = group.copy()
        y = group["co2_emissions_g_kwh"]

        group["co2_lag_1h"] = y.shift(1)
        group["co2_lag_2h"] = y.shift(2)
        group["co2_lag_24h"] = y.shift(24)
        group["co2_lag_168h"] = y.shift(168)

        past_y = y.shift(1)

        group["co2_rolling_3h"] = past_y.rolling(3, min_periods=1).mean()
        group["co2_rolling_6h"] = past_y.rolling(6, min_periods=1).mean()
        group["co2_rolling_24h"] = past_y.rolling(24, min_periods=1).mean()

        group["co2_diff_1h"] = y.shift(1) - y.shift(2)
        group["co2_diff_24h"] = y.shift(1) - y.shift(24)

        output_groups.append(group)

    combined = pd.concat(output_groups, ignore_index=True)

    feature_cols = [
        "co2_lag_1h",
        "co2_lag_2h",
        "co2_lag_24h",
        "co2_lag_168h",
        "co2_rolling_3h",
        "co2_rolling_6h",
        "co2_rolling_24h",
        "co2_diff_1h",
        "co2_diff_24h",
    ]

    final_df = final_df.drop(
        columns=[c for c in feature_cols if c in final_df.columns],
        errors="ignore"
    )

    final_df = final_df.merge(
        combined[["datetime_utc", "price_area"] + feature_cols],
        on=["datetime_utc", "price_area"],
        how="left"
    )

    return final_df


def ensure_processed_features_table(engine):
    """Make sure processed_features table has all needed columns."""
    create_table_query = text("""
        CREATE TABLE IF NOT EXISTS processed_features (
            datetime_utc TIMESTAMPTZ NOT NULL,
            price_area TEXT NOT NULL,

            co2_emissions_g_kwh DOUBLE PRECISION,
            spot_price_dkk_kwh DOUBLE PRECISION,
            wind_speed DOUBLE PRECISION,
            solar_radiation DOUBLE PRECISION,
            temperature DOUBLE PRECISION,

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

            co2_lag_1h DOUBLE PRECISION,
            co2_lag_2h DOUBLE PRECISION,
            co2_lag_24h DOUBLE PRECISION,
            co2_lag_168h DOUBLE PRECISION,

            co2_rolling_3h DOUBLE PRECISION,
            co2_rolling_6h DOUBLE PRECISION,
            co2_rolling_24h DOUBLE PRECISION,

            co2_diff_1h DOUBLE PRECISION,
            co2_diff_24h DOUBLE PRECISION,

            forecast_wind_generation_mw DOUBLE PRECISION,
            forecast_solar_generation_mw DOUBLE PRECISION,
            forecast_load_mw DOUBLE PRECISION,

            is_forecast BOOLEAN DEFAULT FALSE,

            PRIMARY KEY (datetime_utc, price_area)
        );
    """)

    alter_table_queries = [
        "ALTER TABLE processed_features ADD COLUMN IF NOT EXISTS co2_emissions_g_kwh DOUBLE PRECISION;",
        "ALTER TABLE processed_features ADD COLUMN IF NOT EXISTS spot_price_dkk_kwh DOUBLE PRECISION;",
        "ALTER TABLE processed_features ADD COLUMN IF NOT EXISTS wind_speed DOUBLE PRECISION;",
        "ALTER TABLE processed_features ADD COLUMN IF NOT EXISTS solar_radiation DOUBLE PRECISION;",
        "ALTER TABLE processed_features ADD COLUMN IF NOT EXISTS temperature DOUBLE PRECISION;",

        "ALTER TABLE processed_features ADD COLUMN IF NOT EXISTS hour INTEGER;",
        "ALTER TABLE processed_features ADD COLUMN IF NOT EXISTS day_of_week INTEGER;",
        "ALTER TABLE processed_features ADD COLUMN IF NOT EXISTS month INTEGER;",
        "ALTER TABLE processed_features ADD COLUMN IF NOT EXISTS day_of_year INTEGER;",

        "ALTER TABLE processed_features ADD COLUMN IF NOT EXISTS hour_sin DOUBLE PRECISION;",
        "ALTER TABLE processed_features ADD COLUMN IF NOT EXISTS hour_cos DOUBLE PRECISION;",
        "ALTER TABLE processed_features ADD COLUMN IF NOT EXISTS month_sin DOUBLE PRECISION;",
        "ALTER TABLE processed_features ADD COLUMN IF NOT EXISTS month_cos DOUBLE PRECISION;",
        "ALTER TABLE processed_features ADD COLUMN IF NOT EXISTS day_of_year_sin DOUBLE PRECISION;",
        "ALTER TABLE processed_features ADD COLUMN IF NOT EXISTS day_of_year_cos DOUBLE PRECISION;",

        "ALTER TABLE processed_features ADD COLUMN IF NOT EXISTS is_weekend BOOLEAN;",
        "ALTER TABLE processed_features ADD COLUMN IF NOT EXISTS is_holiday BOOLEAN;",

        "ALTER TABLE processed_features ADD COLUMN IF NOT EXISTS co2_lag_1h DOUBLE PRECISION;",
        "ALTER TABLE processed_features ADD COLUMN IF NOT EXISTS co2_lag_2h DOUBLE PRECISION;",
        "ALTER TABLE processed_features ADD COLUMN IF NOT EXISTS co2_lag_24h DOUBLE PRECISION;",
        "ALTER TABLE processed_features ADD COLUMN IF NOT EXISTS co2_lag_168h DOUBLE PRECISION;",

        "ALTER TABLE processed_features ADD COLUMN IF NOT EXISTS co2_rolling_3h DOUBLE PRECISION;",
        "ALTER TABLE processed_features ADD COLUMN IF NOT EXISTS co2_rolling_6h DOUBLE PRECISION;",
        "ALTER TABLE processed_features ADD COLUMN IF NOT EXISTS co2_rolling_24h DOUBLE PRECISION;",

        "ALTER TABLE processed_features ADD COLUMN IF NOT EXISTS co2_diff_1h DOUBLE PRECISION;",
        "ALTER TABLE processed_features ADD COLUMN IF NOT EXISTS co2_diff_24h DOUBLE PRECISION;",

        "ALTER TABLE processed_features ADD COLUMN IF NOT EXISTS forecast_wind_generation_mw DOUBLE PRECISION;",
        "ALTER TABLE processed_features ADD COLUMN IF NOT EXISTS forecast_solar_generation_mw DOUBLE PRECISION;",
        "ALTER TABLE processed_features ADD COLUMN IF NOT EXISTS forecast_load_mw DOUBLE PRECISION;",

        "ALTER TABLE processed_features ADD COLUMN IF NOT EXISTS is_forecast BOOLEAN DEFAULT FALSE;",
    ]

    with engine.begin() as conn:
        conn.execute(create_table_query)

        for query in alter_table_queries:
            conn.execute(text(query))


# ============================================================
# MAIN INGEST JOB
# ============================================================

def ingest_job():
    engine = get_db_connection()

    # --------------------------------------------------------
    # 3-DAY ROLLING UPDATE
    # If today is May 30:
    # fetch May 27 00:00 → May 29 23:00
    # This means:
    # yesterday - 2
    # yesterday - 1
    # yesterday
    # --------------------------------------------------------

    now = datetime.now(timezone.utc)

    start_ts = (now - timedelta(days=3)).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0
    )

    target_end_ts = (now - timedelta(days=1)).replace(
        hour=23,
        minute=0,
        second=0,
        microsecond=0
    )

    # API end is exclusive, so add 1 hour to include yesterday 23:00
    end_exclusive_ts = target_end_ts + timedelta(hours=1)

    start_date = start_ts.strftime("%Y-%m-%dT%H:%M")
    end_date = end_exclusive_ts.strftime("%Y-%m-%dT%H:%M")

    print("📥 Automated DK1 ingest")
    print(f"Range to store: {start_ts} → {target_end_ts}")
    print(f"API request end exclusive: {end_exclusive_ts}")

    try:
        ensure_processed_features_table(engine)

        # ----------------------------------------------------
        # 1. FETCH CO2
        # ----------------------------------------------------
        print("🌫️ Fetching CO2Emis...")

        co2_records = fetch_eds_dataset(
            "CO2Emis",
            {
                "start": start_date,
                "end": end_date,
                "filter": json.dumps({"PriceArea": PRICE_AREAS}),
                "sort": "Minutes5UTC",
                "limit": 0,
            }
        )

        df_co2 = pd.DataFrame(co2_records)

        if df_co2.empty:
            print("⚠️ No CO2 records found.")
            return

        time_col = find_time_column(df_co2)

        if time_col is None:
            raise ValueError(f"No time column found in CO2 data: {df_co2.columns.tolist()}")

        df_co2["datetime_utc"] = pd.to_datetime(df_co2[time_col], utc=True)

        df_co2 = (
            df_co2
            .set_index("datetime_utc")
            .groupby("PriceArea")
            .resample("h")
            .mean(numeric_only=True)
            .reset_index()
        )

        # ----------------------------------------------------
        # 2. FETCH DAY-AHEAD PRICE
        # ----------------------------------------------------
        print("💰 Fetching DayAheadPrices...")

        price_records = fetch_eds_dataset(
            "DayAheadPrices",
            {
                "start": start_date,
                "end": end_date,
                "filter": json.dumps({"PriceArea": PRICE_AREAS}),
                "sort": "TimeUTC",
                "limit": 0,
            }
        )

        df_price = pd.DataFrame(price_records)

        if df_price.empty:
            print("⚠️ No price records found.")
            return

        time_col_p = find_time_column(df_price)

        if time_col_p is None:
            raise ValueError(f"No time column found in price data: {df_price.columns.tolist()}")

        price_col = None

        for candidate in ["DayAheadPriceDKK", "SpotPriceDKK"]:
            if candidate in df_price.columns:
                price_col = candidate
                break

        if price_col is None:
            raise ValueError(f"No price column found. Columns: {df_price.columns.tolist()}")

        df_price["datetime_utc"] = pd.to_datetime(df_price[time_col_p], utc=True)

        df_price = (
            df_price
            .set_index("datetime_utc")
            .groupby("PriceArea")
            .resample("h")
            .mean(numeric_only=True)
            .reset_index()
        )

        df_price = df_price[["datetime_utc", "PriceArea", price_col]]

        # ----------------------------------------------------
        # 3. MERGE CO2 + PRICE
        # ----------------------------------------------------
        final_df = pd.merge(
            df_co2,
            df_price,
            on=["datetime_utc", "PriceArea"],
            how="left"
        )

        final_df = final_df.rename(columns={
            "PriceArea": "price_area",
            "CO2Emission": "co2_emissions_g_kwh",
            price_col: "spot_price_dkk_kwh",
        })

        # Convert DKK/MWh to DKK/kWh
        final_df["spot_price_dkk_kwh"] = pd.to_numeric(
            final_df["spot_price_dkk_kwh"],
            errors="coerce"
        ) / 1000

        # ----------------------------------------------------
        # 4. FETCH WEATHER
        # ----------------------------------------------------
        print("🌦️ Fetching weather...")

        weather_list = []

        for area in PRICE_AREAS:
            weather_df = get_weather_data(
                LOCATIONS[area]["lat"],
                LOCATIONS[area]["lon"],
                days_back=4
            )

            if weather_df.empty:
                continue

            weather_df["price_area"] = area
            weather_list.append(weather_df)

        if weather_list:
            weather_all = pd.concat(weather_list, ignore_index=True)

            final_df = final_df.merge(
                weather_all,
                on=["datetime_utc", "price_area"],
                how="left"
            )
        else:
            final_df["wind_speed"] = np.nan
            final_df["solar_radiation"] = np.nan
            final_df["temperature"] = np.nan

        # ----------------------------------------------------
        # 5. FETCH FORECAST WIND + SOLAR GENERATION
        # ----------------------------------------------------
        print("🌬️☀️ Fetching Forecasts_Hour wind/solar...")

        renewable_df = get_forecast_renewables(start_date, end_date)

        if not renewable_df.empty:
            final_df = final_df.merge(
                renewable_df,
                on=["datetime_utc", "price_area"],
                how="left"
            )
        else:
            final_df["forecast_wind_generation_mw"] = np.nan
            final_df["forecast_solar_generation_mw"] = np.nan

        # ----------------------------------------------------
        # 6. FETCH FORECAST LOAD FROM ENTSO-E
        # ----------------------------------------------------
        print("⚡ Fetching ENTSO-E forecast load...")

        load_df = get_forecast_load(start_ts, end_exclusive_ts)

        if not load_df.empty:
            final_df = final_df.merge(
                load_df,
                on=["datetime_utc", "price_area"],
                how="left"
            )
        else:
            final_df["forecast_load_mw"] = np.nan

        # ----------------------------------------------------
        # 7. FILTER EXACT TARGET RANGE
        # ----------------------------------------------------
        final_df = final_df[
            (final_df["datetime_utc"] >= pd.Timestamp(start_ts)) &
            (final_df["datetime_utc"] <= pd.Timestamp(target_end_ts))
        ].copy()

        # ----------------------------------------------------
        # 8. ADD TIME FEATURES
        # ----------------------------------------------------
        final_df = add_time_features(final_df)

        # ----------------------------------------------------
        # 9. ADD CO2 LAG / ROLLING / DIFF FEATURES
        # ----------------------------------------------------
        history_start_utc = start_ts - timedelta(days=8)

        final_df = add_co2_lag_features(
            final_df=final_df,
            engine=engine,
            history_start_utc=history_start_utc,
            end_exclusive_utc=end_exclusive_ts
        )

        # ----------------------------------------------------
        # 10. FINAL COLUMNS
        # ----------------------------------------------------
        final_df["is_forecast"] = False

        required_columns = [
            "datetime_utc",
            "price_area",

            "co2_emissions_g_kwh",
            "spot_price_dkk_kwh",
            "wind_speed",
            "solar_radiation",
            "temperature",

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

            "co2_lag_1h",
            "co2_lag_2h",
            "co2_lag_24h",
            "co2_lag_168h",

            "co2_rolling_3h",
            "co2_rolling_6h",
            "co2_rolling_24h",

            "co2_diff_1h",
            "co2_diff_24h",

            "forecast_wind_generation_mw",
            "forecast_solar_generation_mw",
            "forecast_load_mw",

            "is_forecast",
        ]

        for col in required_columns:
            if col not in final_df.columns:
                final_df[col] = np.nan

        final_df = final_df[required_columns]

        final_df = final_df.drop_duplicates(
            subset=["datetime_utc", "price_area"],
            keep="last"
        )

        final_df["is_weekend"] = final_df["is_weekend"].astype(bool)
        final_df["is_holiday"] = final_df["is_holiday"].astype(bool)
        final_df["is_forecast"] = final_df["is_forecast"].astype(bool)

        print("🔍 Missing values before upload:")
        print(final_df.isna().sum())

        print("🔍 Preview:")
        print(final_df.tail())

        # ----------------------------------------------------
        # 11. UPLOAD TO TEMP TABLE
        # ----------------------------------------------------
        print("📤 Uploading to temp_ingest...")

        final_df.to_sql(
            "temp_ingest",
            engine,
            if_exists="replace",
            index=False
        )

        # ----------------------------------------------------
        # 12. UPSERT INTO processed_features
        # ----------------------------------------------------
        insert_cols = ", ".join(required_columns)
        select_cols = ", ".join(required_columns)

        update_cols = [
            col for col in required_columns
            if col not in ["datetime_utc", "price_area"]
        ]

        update_sql = ",\n                ".join(
            [f"{col} = EXCLUDED.{col}" for col in update_cols]
        )

        upsert_query = text(f"""
            INSERT INTO processed_features (
                {insert_cols}
            )
            SELECT
                {select_cols}
            FROM temp_ingest
            ON CONFLICT (datetime_utc, price_area)
            DO UPDATE SET
                {update_sql};
        """)

        print("🔁 Upserting into processed_features...")

        with engine.begin() as conn:
            conn.execute(upsert_query)
            conn.execute(text("DROP TABLE IF EXISTS temp_ingest;"))

        # ----------------------------------------------------
        # 13. VERIFY
        # ----------------------------------------------------
        with engine.connect() as conn:
            latest_rows = conn.execute(text("""
                SELECT
                    datetime_utc,
                    price_area,
                    co2_emissions_g_kwh,
                    spot_price_dkk_kwh,
                    wind_speed,
                    solar_radiation,
                    temperature,
                    forecast_wind_generation_mw,
                    forecast_solar_generation_mw,
                    forecast_load_mw,
                    co2_lag_1h,
                    co2_lag_24h,
                    co2_lag_168h,
                    is_forecast
                FROM processed_features
                WHERE price_area = 'DK1'
                ORDER BY datetime_utc DESC
                LIMIT 5;
            """)).fetchall()

            missing_check = conn.execute(text("""
                SELECT
                    COUNT(*) AS total_rows,
                    COUNT(*) FILTER (WHERE co2_emissions_g_kwh IS NULL) AS missing_co2,
                    COUNT(*) FILTER (WHERE spot_price_dkk_kwh IS NULL) AS missing_price,
                    COUNT(*) FILTER (WHERE wind_speed IS NULL) AS missing_wind_speed,
                    COUNT(*) FILTER (WHERE solar_radiation IS NULL) AS missing_solar_radiation,
                    COUNT(*) FILTER (WHERE temperature IS NULL) AS missing_temperature,
                    COUNT(*) FILTER (WHERE forecast_wind_generation_mw IS NULL) AS missing_forecast_wind,
                    COUNT(*) FILTER (WHERE forecast_solar_generation_mw IS NULL) AS missing_forecast_solar,
                    COUNT(*) FILTER (WHERE forecast_load_mw IS NULL) AS missing_forecast_load
                FROM processed_features
                WHERE price_area = 'DK1';
            """)).fetchone()

        print(f"✅ Success! {len(final_df)} DK1 rows synced to Neon.")
        print("🕒 Latest DK1 rows:")

        for row in latest_rows:
            print(row)

        print("🔍 Missing check:")
        print(missing_check)

    except Exception as e:
        print(f"❌ Ingestion Error: {str(e)}")


if __name__ == "__main__":
    ingest_job()