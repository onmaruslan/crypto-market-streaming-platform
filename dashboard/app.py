import os

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text


# -----------------------------
# Page config
# -----------------------------
st.set_page_config(
    page_title="Crypto Market Dashboard",
    page_icon="📈",
    layout="wide",
)


# -----------------------------
# Database config
# -----------------------------
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.getenv("POSTGRES_USER", "airflow")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")
POSTGRES_DATA_DB = os.getenv("POSTGRES_DATA_DB", "crypto_market")

POSTGRES_URI = (
    f"postgresql+psycopg://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATA_DB}"
)


# -----------------------------
# Database connection
# -----------------------------
@st.cache_resource
def get_engine():
    return create_engine(POSTGRES_URI)


engine = get_engine()


# -----------------------------
# Data loaders
# -----------------------------
@st.cache_data(ttl=30)
def load_symbols():
    query = """
    SELECT DISTINCT symbol
    FROM market_trades
    ORDER BY symbol
    """
    return pd.read_sql(query, engine)["symbol"].tolist()


@st.cache_data(ttl=30)
def load_exchanges():
    query = """
    SELECT DISTINCT exchange
    FROM market_trades
    ORDER BY exchange
    """
    return pd.read_sql(query, engine)["exchange"].tolist()


@st.cache_data(ttl=30)
def load_latest_metrics(symbol: str, exchange: str):
    query = text(
        """
        SELECT
            exchange,
            symbol,
            minute_bucket,
            trade_count,
            volume,
            avg_price,
            min_price,
            max_price
        FROM trades_1m_agg
        WHERE symbol = :symbol
          AND exchange = :exchange
        ORDER BY minute_bucket DESC
        LIMIT 1
        """
    )
    return pd.read_sql(query, engine, params={"symbol": symbol, "exchange": exchange})


@st.cache_data(ttl=30)
def load_ohlc(symbol: str, exchange: str, limit: int = 100):
    query = text(
        """
        SELECT
            minute_bucket,
            open_price,
            high_price,
            low_price,
            close_price,
            volume
        FROM ohlc_1m
        WHERE symbol = :symbol
          AND exchange = :exchange
        ORDER BY minute_bucket DESC
        LIMIT :limit
        """
    )
    df = pd.read_sql(
        query,
        engine,
        params={"symbol": symbol, "exchange": exchange, "limit": limit},
    )

    if df.empty:
        return df

    return df.sort_values("minute_bucket")


@st.cache_data(ttl=30)
def load_spread(symbol: str, limit: int = 100):
    query = text(
        """
        SELECT
            minute_bucket,
            min_exchange,
            min_price,
            max_exchange,
            max_price,
            spread_abs,
            spread_pct
        FROM spread_1m
        WHERE symbol = :symbol
        ORDER BY minute_bucket DESC
        LIMIT :limit
        """
    )
    df = pd.read_sql(query, engine, params={"symbol": symbol, "limit": limit})

    if df.empty:
        return df

    return df.sort_values("minute_bucket")


@st.cache_data(ttl=30)
def load_recent_trades(symbol: str, exchange: str, limit: int = 50):
    query = text(
        """
        SELECT
            exchange,
            symbol,
            trade_id,
            price,
            quantity,
            side,
            trade_time,
            ingestion_time
        FROM market_trades
        WHERE symbol = :symbol
          AND exchange = :exchange
        ORDER BY trade_time DESC
        LIMIT :limit
        """
    )
    return pd.read_sql(
        query,
        engine,
        params={"symbol": symbol, "exchange": exchange, "limit": limit},
    )


# -----------------------------
# Page header
# -----------------------------
st.title("Crypto Market Dashboard")
st.caption("Market trades, 1-minute aggregates, OHLC, and exchange spread")


symbols = load_symbols()
exchanges = load_exchanges()

if not symbols or not exchanges:
    st.warning(
        "No data available in PostgreSQL yet. Check the pipeline and source tables."
    )
    st.stop()


# -----------------------------
# Filters
# -----------------------------
col1, col2 = st.columns(2)

with col1:
    selected_symbol = st.selectbox("Symbol", symbols, index=0)

with col2:
    selected_exchange = st.selectbox("Exchange", exchanges, index=0)


# -----------------------------
# Top metrics
# -----------------------------
latest_metrics = load_latest_metrics(selected_symbol, selected_exchange)

if latest_metrics.empty:
    st.info("No aggregated metrics available for the selected symbol and exchange.")
else:
    row = latest_metrics.iloc[0]

    m1, m2, m3, m4 = st.columns(4)

    m1.metric("Last Avg Price", f"{row['avg_price']}")
    m2.metric("Last Minute Volume", f"{row['volume']}")
    m3.metric("Last Minute Trades", f"{row['trade_count']}")
    m4.metric("Minute Bucket", str(row["minute_bucket"]))


# -----------------------------
# OHLC section
# -----------------------------
st.subheader("OHLC 1m")

ohlc_df = load_ohlc(selected_symbol, selected_exchange)

if ohlc_df.empty:
    st.info("No OHLC data available for the selected symbol and exchange.")
else:
    chart_df = ohlc_df.set_index("minute_bucket")[["close_price"]]
    st.line_chart(chart_df)

    with st.expander("Show OHLC table"):
        st.dataframe(ohlc_df, use_container_width=True)


# -----------------------------
# Spread section
# -----------------------------
st.subheader("Spread 1m")

spread_df = load_spread(selected_symbol)

if spread_df.empty:
    st.info("No spread data available for the selected symbol.")
else:
    spread_chart_df = spread_df.set_index("minute_bucket")[["spread_pct"]]
    st.line_chart(spread_chart_df)

    with st.expander("Show spread table"):
        st.dataframe(spread_df, use_container_width=True)


# -----------------------------
# Recent trades section
# -----------------------------
st.subheader("Recent Trades")

recent_trades_df = load_recent_trades(selected_symbol, selected_exchange)

if recent_trades_df.empty:
    st.info("No recent trades available for the selected symbol and exchange.")
else:
    st.dataframe(recent_trades_df, use_container_width=True)
