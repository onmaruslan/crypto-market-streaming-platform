import pandas as pd
from sqlalchemy import create_engine, text

from src.config.settings import POSTGRES_URI


engine = create_engine(POSTGRES_URI)


def aggregate_trades_1m():

    with engine.connect() as conn:
        last_minute = conn.execute(
            text("SELECT max(minute_bucket) FROM trades_1m_agg")
        ).scalar()

        if last_minute is None:
            last_minute = "1970-01-01"

        query = """
        SELECT
            exchange,
            symbol,
            date_trunc('minute', trade_time) AS minute_bucket,
            count(*) AS trade_count,
            sum(quantity) AS volume,
            avg(price) AS avg_price,
            min(price) AS min_price,
            max(price) AS max_price
        FROM market_trades
        WHERE trade_time > :last_minute
        GROUP BY exchange, symbol, minute_bucket
        """

        df = pd.read_sql(
            text(query),
            conn,
            params={"last_minute": last_minute},
        )

    if df.empty:
        print("No new data to aggregate")
        return

    df["volume"] = df["volume"].round(4)
    df["avg_price"] = df["avg_price"].round(2)
    df["min_price"] = df["min_price"].round(2)
    df["max_price"] = df["max_price"].round(2)

    df.to_sql(
        "trades_1m_agg",
        engine,
        if_exists="append",
        index=False,
        method="multi",
    )

    print(f"Inserted {len(df)} rows into trades_1m_agg")
