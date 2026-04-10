-- ==========================================
-- 1. RAW DATA TABLES (The "Daily Inbox")
-- ==========================================

-- Standardized to DKK/kWh to match your project decision
CREATE TABLE IF NOT EXISTS raw_electricity_prices (
    datetime_utc TIMESTAMP NOT NULL,
    price_area VARCHAR(5) NOT NULL,
    spot_price_dkk_kwh FLOAT, -- Changed from MWh to kWh
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (datetime_utc, price_area)
);

CREATE TABLE IF NOT EXISTS raw_co2_emissions (
    datetime_utc TIMESTAMP NOT NULL,
    price_area VARCHAR(5) NOT NULL,
    co2_emissions_g_kwh FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (datetime_utc, price_area)
);

-- Updated to include the specific weather features your AI uses
CREATE TABLE IF NOT EXISTS raw_weather_forecast (
    datetime_utc TIMESTAMP NOT NULL,
    price_area VARCHAR(5) NOT NULL,
    wind_speed FLOAT,        -- Essential for your Prophet Regressor
    solar_radiation FLOAT,   -- Essential for your Prophet Regressor
    temperature FLOAT,       -- Good for V2
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (datetime_utc, price_area)
);

-- ==========================================
-- 2. PROCESSED FEATURES (The "ML-Ready" Table)
-- ==========================================
CREATE TABLE IF NOT EXISTS processed_features (
    datetime_utc TIMESTAMP NOT NULL,
    price_area VARCHAR(5) NOT NULL,
    
    -- Features exactly as they appear in your master dataset
    co2_emissions_g_kwh FLOAT,
    spot_price_dkk_kwh FLOAT,
    wind_speed FLOAT,
    solar_radiation FLOAT,
    
    -- Metadata for MLOps
    is_forecast BOOLEAN DEFAULT FALSE, -- To distinguish historical from predicted
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (datetime_utc, price_area)
);

-- ==========================================
-- 3. AI OUTPUTS (The "Decision" Table)
-- ==========================================
CREATE TABLE IF NOT EXISTS ai_forecasts (
    datetime_utc TIMESTAMP NOT NULL,
    price_area VARCHAR(5) NOT NULL,
    
    predicted_co2 FLOAT,
    predicted_price_dkk_kwh FLOAT,
    
    -- The actual "Smart Trigger" logic
    -- TRUE = Low CO2/Price, start charging!
    should_charge BOOLEAN, 
    
    model_version VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (datetime_utc, price_area)
);

-- ==========================================
-- 4. INDEXING (For Speed)
-- ==========================================
CREATE INDEX IF NOT EXISTS idx_processed_time ON processed_features (datetime_utc DESC);
CREATE INDEX IF NOT EXISTS idx_forecast_time ON ai_forecasts (datetime_utc DESC);