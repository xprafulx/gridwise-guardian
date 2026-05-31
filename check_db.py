import pandas as pd
from src.database.connection import get_db_connection

# Show all columns clearly
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 1000)


def inspect_database():
    engine = get_db_connection()

    print("\n" + "🛡️ " * 15)
    print("   GREENHOUR V2 DATABASE INSPECTION")
    print("🛡️ " * 15 + "\n")

    # --- PART 1: LIST ALL TABLES ---
    print("📊 PART 1: DATABASE TABLES")
    print("-" * 80)
    try:
        query_tables = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """
        df_tables = pd.read_sql(query_tables, engine)
        print(df_tables)
    except Exception as e:
        print(f"Table List Error: {e}")

    # --- PART 2: RAW ELECTRICITY PRICE DATA ---
    print("\n⚡ PART 2: RAW ELECTRICITY PRICES")
    print("Showing 5 most recent DK1 electricity price records...")
    print("-" * 80)
    try:
        query_prices = """
            SELECT *
            FROM raw_electricity_prices
            WHERE price_area = 'DK1'
            ORDER BY datetime_utc DESC
            LIMIT 5;
        """
        df_prices = pd.read_sql(query_prices, engine)
        print(df_prices)
    except Exception as e:
        print(f"Raw Electricity Price Error: {e}")

    # --- PART 3: RAW CO2 DATA ---
    print("\n🌍 PART 3: RAW CO2 EMISSIONS")
    print("Showing 5 most recent DK1 CO2 emission records...")
    print("-" * 80)
    try:
        query_co2 = """
            SELECT *
            FROM raw_co2_emissions
            WHERE price_area = 'DK1'
            ORDER BY datetime_utc DESC
            LIMIT 5;
        """
        df_co2 = pd.read_sql(query_co2, engine)
        print(df_co2)
    except Exception as e:
        print(f"Raw CO2 Error: {e}")

    # --- PART 4: RAW WEATHER DATA ---
    print("\n🌦️ PART 4: RAW WEATHER FORECAST")
    print("Showing 5 most recent DK1 weather records...")
    print("-" * 80)
    try:
        query_weather = """
            SELECT *
            FROM raw_weather_forecast
            WHERE price_area = 'DK1'
            ORDER BY datetime_utc DESC
            LIMIT 5;
        """
        df_weather = pd.read_sql(query_weather, engine)
        print(df_weather)
    except Exception as e:
        print(f"Raw Weather Error: {e}")

    # --- PART 5: PROCESSED FEATURES ---
    print("\n🧪 PART 5: PROCESSED FEATURES")
    print("Showing 5 most recent DK1 processed feature records...")
    print("-" * 80)
    try:
        query_features = """
            SELECT *
            FROM processed_features
            WHERE price_area = 'DK1'
            ORDER BY datetime_utc DESC
            LIMIT 5;
        """
        df_features = pd.read_sql(query_features, engine)
        print(df_features)
    except Exception as e:
        print(f"Processed Features Error: {e}")

    # --- PART 6: V1 AI FORECASTS ---
    print("\n🔮 PART 6: V1 AI FORECASTS")
    print("Showing 5 most recent DK1 AI forecast records...")
    print("-" * 80)
    try:
        query_forecasts = """
            SELECT *
            FROM ai_forecasts
            WHERE price_area = 'DK1'
            ORDER BY datetime_utc DESC
            LIMIT 5;
        """
        df_forecasts = pd.read_sql(query_forecasts, engine)
        print(df_forecasts)
    except Exception as e:
        print(f"AI Forecasts Error: {e}")

    # --- PART 7: V2 CO2-AWARE PRICE SIGNALS ---
    print("\n🟢 PART 7: V2 CO2-AWARE PRICE SIGNALS")
    print("Showing 5 most recent DK1 CO2-aware price signal records...")
    print("-" * 80)
    try:
        query_signals = """
            SELECT *
            FROM co2_aware_price_signals
            WHERE price_area = 'DK1'
            ORDER BY datetime_utc DESC
            LIMIT 5;
        """
        df_signals = pd.read_sql(query_signals, engine)
        print(df_signals)
    except Exception as e:
        print(f"CO2-Aware Signal Error: {e}")

    # --- PART 8: MODEL REGISTRY ---
    print("\n🤖 PART 8: MODEL REGISTRY")
    print("Showing latest model registry records...")
    print("-" * 80)
    try:
        query_models = """
            SELECT 
                model_name,
                model_version,
                mae,
                rmse,
                r2,
                created_at,
                is_active
            FROM model_registry
            ORDER BY created_at DESC
            LIMIT 10;
        """
        df_models = pd.read_sql(query_models, engine)
        print(df_models)
    except Exception as e:
        print(f"Model Registry Error: {e}")

    print("\n✅ Greenhour V2 database inspection complete.\n")


if __name__ == "__main__":
    inspect_database()