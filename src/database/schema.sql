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
-- 3. AI OUTPUTS (Gold Layer)
-- ==========================================
CREATE TABLE IF NOT EXISTS ai_forecasts (
    datetime_utc TIMESTAMPTZ NOT NULL,
    price_area VARCHAR(5) NOT NULL,
    model_version VARCHAR(50) NOT NULL, 
    
    predicted_co2 FLOAT,
    market_price_dkk_kwh FLOAT,
    should_charge BOOLEAN, 
    
    -- 🟢 ADD THIS LINE BELOW:
    recommendation_status VARCHAR(20), -- Stores 'BEST', 'CAUTION', or 'AVOID'
    
    prediction_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (datetime_utc, price_area, model_version)
);

CREATE TABLE IF NOT EXISTS model_registry (
    model_name VARCHAR(100),
    model_version VARCHAR(50),
    model_binary BYTEA, -- This is the 'blob' that stores the actual file
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
-- We index the time column for faster dashboard loading
CREATE INDEX IF NOT EXISTS idx_processed_time ON processed_features (datetime_utc DESC);
CREATE INDEX IF NOT EXISTS idx_forecast_time ON ai_forecasts (datetime_utc DESC);
