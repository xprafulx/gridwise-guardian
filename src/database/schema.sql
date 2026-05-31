-- ==========================================
-- 1. RAW DATA TABLES (Bronze Layer)
-- ==========================================

CREATE TABLE IF NOT EXISTS raw_electricity_prices (
    datetime_utc TIMESTAMPTZ NOT NULL,
    price_area VARCHAR(5) NOT NULL,
    spot_price_dkk_kwh FLOAT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (datetime_utc, price_area)
);

CREATE TABLE IF NOT EXISTS raw_co2_emissions (
    datetime_utc TIMESTAMPTZ NOT NULL,
    price_area VARCHAR(5) NOT NULL,
    co2_emissions_g_kwh FLOAT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (datetime_utc, price_area)
);

CREATE TABLE IF NOT EXISTS raw_weather_forecast (
    datetime_utc TIMESTAMPTZ NOT NULL,
    price_area VARCHAR(5) NOT NULL,
    wind_speed FLOAT,
    solar_radiation FLOAT,
    temperature FLOAT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (datetime_utc, price_area)
);

-- ==========================================
-- 2. PROCESSED FEATURES (Silver Layer)
-- ==========================================

CREATE TABLE IF NOT EXISTS processed_features (
    datetime_utc TIMESTAMPTZ NOT NULL,
    price_area VARCHAR(5) NOT NULL,

    co2_emissions_g_kwh FLOAT,
    spot_price_dkk_kwh FLOAT,
    wind_speed FLOAT,
    solar_radiation FLOAT,

    -- Metadata for MLOps
    is_forecast BOOLEAN DEFAULT FALSE,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (datetime_utc, price_area)
);

-- ==========================================
-- 3. AI OUTPUTS (Gold Layer) - V1 Forecasts
-- ==========================================

CREATE TABLE IF NOT EXISTS ai_forecasts (
    datetime_utc TIMESTAMPTZ NOT NULL,
    price_area VARCHAR(5) NOT NULL,
    model_version VARCHAR(50) NOT NULL,

    predicted_co2 FLOAT,
    market_price_dkk_kwh FLOAT,
    should_charge BOOLEAN,

    recommendation_status VARCHAR(20), -- Stores 'BEST', 'CAUTION', or 'AVOID'

    prediction_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (datetime_utc, price_area, model_version)
);

-- ==========================================
-- 3B. AI OUTPUTS (Gold Layer) - V2 CO2-Aware Price Signal
-- ==========================================

CREATE TABLE IF NOT EXISTS co2_aware_price_signals (
    datetime_utc TIMESTAMPTZ NOT NULL,
    price_area VARCHAR(5) NOT NULL,
    model_version VARCHAR(50) NOT NULL,

    -- Inputs used to create the signal
    predicted_co2_g_kwh FLOAT,
    day_ahead_price_dkk_kwh FLOAT,

    -- Normalized input values
    normalized_co2 FLOAT,
    normalized_price FLOAT,

    -- Signal outputs
    raw_co2_aware_signal FLOAT,
    smoothed_co2_aware_signal FLOAT,

    -- Smoothing information
    smoothing_method VARCHAR(50),
    smoothing_window_hours INTEGER,
    max_hourly_change FLOAT,

    prediction_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (datetime_utc, price_area, model_version)
);

-- ==========================================
-- 3C. MODEL REGISTRY
-- ==========================================

CREATE TABLE IF NOT EXISTS model_registry (
    model_name VARCHAR(100),
    model_version VARCHAR(50),
    model_binary BYTEA,
    mae FLOAT,
    rmse FLOAT,
    r2 FLOAT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT FALSE,

    PRIMARY KEY (model_name, model_version)
);

-- ==========================================
-- 4. PERFORMANCE & INDEXING
-- ==========================================

CREATE INDEX IF NOT EXISTS idx_processed_time
ON processed_features (datetime_utc DESC);

CREATE INDEX IF NOT EXISTS idx_forecast_time
ON ai_forecasts (datetime_utc DESC);

CREATE INDEX IF NOT EXISTS idx_signal_time
ON co2_aware_price_signals (datetime_utc DESC);