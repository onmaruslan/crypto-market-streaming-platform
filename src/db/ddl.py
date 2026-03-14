MARKET_TRADES_DDL = """
CREATE TABLE IF NOT EXISTS market_trades (
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    trade_id TEXT NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    quantity DOUBLE PRECISION NOT NULL,
    side TEXT NOT NULL,
    trade_time TIMESTAMPTZ NOT NULL,
    ingestion_time TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (exchange, symbol, trade_id)
);
"""


LOADED_STAGING_FILES_DDL = """
CREATE TABLE IF NOT EXISTS loaded_staging_files (
    parquet_key TEXT PRIMARY KEY,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""
