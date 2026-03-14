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

TRADES_1M_AGG_DDL = """
CREATE TABLE IF NOT EXISTS trades_1m_agg (
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    minute_bucket TIMESTAMPTZ NOT NULL,
    trade_count integer NOT NULL,
    volume numeric NOT NULL,
    avg_price numeric NOT NULL,
    min_price numeric NOT NULL,
    max_price numeric NOT NULL,
    PRIMARY KEY (exchange, symbol, minute_bucket)
);
"""
