-- Bronze Layer: Raw Data
CREATE TABLE IF NOT EXISTS cleaned_bitcoin (
    "Timestamp" TIMESTAMP,
    "Open" NUMERIC,
    "High" NUMERIC,
    "Low" NUMERIC,
    "Close" NUMERIC,
    "Volume" NUMERIC,
    "Weighted_Price" NUMERIC
);

-- Silver Layer: Daily Aggregates (Requires PRIMARY KEY for Upserts)
CREATE TABLE IF NOT EXISTS bitcoin_daily_summary (
    trade_date DATE PRIMARY KEY,
    daily_high NUMERIC,
    daily_low NUMERIC,
    avg_price NUMERIC,
    total_volume NUMERIC
);

-- Gold Layer: Analytical View
-- We use a View or a Table that we refresh
CREATE TABLE IF NOT EXISTS bitcoin_gold_metrics (
    trade_date DATE PRIMARY KEY,
    avg_price NUMERIC,
    rolling_7d_avg NUMERIC,
    daily_volatility_pct NUMERIC
);