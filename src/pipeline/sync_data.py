import pandas as pd
from sqlalchemy import text
from src.database.connection import get_db_connection


def convert_to_boolean(series):
    """
    Convert 0/1, True/False, true/false values into real boolean values.
    This prevents PostgreSQL error:
    column is of type boolean but expression is of type bigint
    """
    if series.dtype == bool:
        return series

    return series.map(
        lambda x: True if str(x).strip().lower() in ["1", "true", "yes"] else False
    )


def sync_huggingface_to_postgres():
    hf_url = "https://huggingface.co/datasets/appleballcay/co2/resolve/main/base_df_with_temperature.csv"

    print("📥 Downloading master dataset from Hugging Face...")

    try:
        # 1. Read Hugging Face CSV
        df = pd.read_csv(hf_url)
        df.columns = df.columns.str.strip()

        print("📋 Columns found in Hugging Face CSV:")
        print(df.columns.tolist())

        # 2. Fix datetime column
        if "datetime_utc" in df.columns:
            pass

        elif "timestamp_utc" in df.columns:
            df = df.rename(columns={"timestamp_utc": "datetime_utc"})

        elif "Unnamed: 0" in df.columns:
            df = df.rename(columns={"Unnamed: 0": "datetime_utc"})

        elif "ds" in df.columns:
            df = df.rename(columns={"ds": "datetime_utc"})

        elif "datetime" in df.columns:
            df = df.rename(columns={"datetime": "datetime_utc"})

        elif "timestamp" in df.columns:
            df = df.rename(columns={"timestamp": "datetime_utc"})

        else:
            raise ValueError(
                f"No datetime column found. Available columns: {df.columns.tolist()}"
            )

        # Convert datetime to UTC
        df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True)

        # 3. Add missing project columns
        if "price_area" not in df.columns:
            df["price_area"] = "DK1"

        if "is_forecast" not in df.columns:
            df["is_forecast"] = False

        # 4. Convert boolean columns properly
        bool_columns = ["is_weekend", "is_holiday", "is_forecast"]

        for col in bool_columns:
            if col in df.columns:
                df[col] = convert_to_boolean(df[col])

        # 5. Required columns for processed_features
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

        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            raise ValueError(
                f"Missing required columns: {missing_columns}\n"
                f"Available columns: {df.columns.tolist()}"
            )

        df = df[required_columns]

        # 6. Remove duplicate datetime + area rows
        df = df.drop_duplicates(subset=["datetime_utc", "price_area"], keep="last")

        print("🔍 Missing values before upload:")
        print(df.isna().sum())

        print("🔍 Data types before upload:")
        print(df.dtypes)

        print(f"✅ Prepared {len(df)} rows for Neon upload.")

        # 7. Connect to Neon
        engine = get_db_connection()

        # 8. Create table if it does not exist
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

        print("🛠️ Preparing processed_features table...")

        with engine.begin() as conn:
            conn.execute(create_table_query)

            for query in alter_table_queries:
                conn.execute(text(query))

        # 9. Upload dataframe to temporary table
        print("📤 Uploading data to temporary table...")

        df.to_sql(
            "temp_master_import",
            engine,
            if_exists="replace",
            index=False
        )

        # 10. Upsert into processed_features
        upsert_query = text("""
            INSERT INTO processed_features (
                datetime_utc,
                price_area,

                co2_emissions_g_kwh,
                spot_price_dkk_kwh,
                wind_speed,
                solar_radiation,
                temperature,

                hour,
                day_of_week,
                month,
                day_of_year,

                hour_sin,
                hour_cos,
                month_sin,
                month_cos,
                day_of_year_sin,
                day_of_year_cos,

                is_weekend,
                is_holiday,

                co2_lag_1h,
                co2_lag_2h,
                co2_lag_24h,
                co2_lag_168h,

                co2_rolling_3h,
                co2_rolling_6h,
                co2_rolling_24h,

                co2_diff_1h,
                co2_diff_24h,

                forecast_wind_generation_mw,
                forecast_solar_generation_mw,
                forecast_load_mw,

                is_forecast
            )
            SELECT
                datetime_utc,
                price_area,

                co2_emissions_g_kwh,
                spot_price_dkk_kwh,
                wind_speed,
                solar_radiation,
                temperature,

                hour,
                day_of_week,
                month,
                day_of_year,

                hour_sin,
                hour_cos,
                month_sin,
                month_cos,
                day_of_year_sin,
                day_of_year_cos,

                is_weekend,
                is_holiday,

                co2_lag_1h,
                co2_lag_2h,
                co2_lag_24h,
                co2_lag_168h,

                co2_rolling_3h,
                co2_rolling_6h,
                co2_rolling_24h,

                co2_diff_1h,
                co2_diff_24h,

                forecast_wind_generation_mw,
                forecast_solar_generation_mw,
                forecast_load_mw,

                is_forecast
            FROM temp_master_import
            ON CONFLICT (datetime_utc, price_area)
            DO UPDATE SET
                co2_emissions_g_kwh = EXCLUDED.co2_emissions_g_kwh,
                spot_price_dkk_kwh = EXCLUDED.spot_price_dkk_kwh,
                wind_speed = EXCLUDED.wind_speed,
                solar_radiation = EXCLUDED.solar_radiation,
                temperature = EXCLUDED.temperature,

                hour = EXCLUDED.hour,
                day_of_week = EXCLUDED.day_of_week,
                month = EXCLUDED.month,
                day_of_year = EXCLUDED.day_of_year,

                hour_sin = EXCLUDED.hour_sin,
                hour_cos = EXCLUDED.hour_cos,
                month_sin = EXCLUDED.month_sin,
                month_cos = EXCLUDED.month_cos,
                day_of_year_sin = EXCLUDED.day_of_year_sin,
                day_of_year_cos = EXCLUDED.day_of_year_cos,

                is_weekend = EXCLUDED.is_weekend,
                is_holiday = EXCLUDED.is_holiday,

                co2_lag_1h = EXCLUDED.co2_lag_1h,
                co2_lag_2h = EXCLUDED.co2_lag_2h,
                co2_lag_24h = EXCLUDED.co2_lag_24h,
                co2_lag_168h = EXCLUDED.co2_lag_168h,

                co2_rolling_3h = EXCLUDED.co2_rolling_3h,
                co2_rolling_6h = EXCLUDED.co2_rolling_6h,
                co2_rolling_24h = EXCLUDED.co2_rolling_24h,

                co2_diff_1h = EXCLUDED.co2_diff_1h,
                co2_diff_24h = EXCLUDED.co2_diff_24h,

                forecast_wind_generation_mw = EXCLUDED.forecast_wind_generation_mw,
                forecast_solar_generation_mw = EXCLUDED.forecast_solar_generation_mw,
                forecast_load_mw = EXCLUDED.forecast_load_mw,

                is_forecast = EXCLUDED.is_forecast;
        """)

        print("🔁 Syncing all feature columns to processed_features...")

        with engine.begin() as conn:
            conn.execute(upsert_query)
            conn.execute(text("DROP TABLE IF EXISTS temp_master_import;"))

        # 11. Final verification
        with engine.connect() as conn:
            total_count = conn.execute(
                text("SELECT COUNT(*) FROM processed_features;")
            ).scalar()

            latest_rows = conn.execute(text("""
                SELECT 
                    datetime_utc,
                    price_area,
                    co2_emissions_g_kwh,
                    spot_price_dkk_kwh,
                    is_weekend,
                    is_holiday,
                    is_forecast
                FROM processed_features
                ORDER BY datetime_utc DESC
                LIMIT 5;
            """)).fetchall()

        print(f"🎉 SUCCESS! Neon now contains {total_count} rows in processed_features.")
        print("🕒 Latest rows:")

        for row in latest_rows:
            print(row)

    except Exception as e:
        print(f"❌ Error during sync: {e}")


if __name__ == "__main__":
    sync_huggingface_to_postgres()