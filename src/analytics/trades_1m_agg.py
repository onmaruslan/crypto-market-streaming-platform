import pandas as pd
from sqlalchemy import create_engine, text

from src.config.settings import POSTGRES_URI


engine = create_engine(POSTGRES_URI)


def aggregate_trades_1m():
    with engine.begin() as conn:
        last_minute = conn.execute(
            text("SELECT max(minute_bucket) FROM trades_1m_agg")
        ).scalar()

        if last_minute is None:
            last_minute = pd.Timestamp("1970-01-01", tz="UTC")

        query = """
        SELECT
            exchange,
            symbol,
            date_trunc('minute', trade_time) AS minute_bucket,
            count(*) AS trade_count,
            round(sum(quantity)::numeric, 4) AS volume,
            round(avg(price)::numeric, 2) AS avg_price,
            round(min(price)::numeric, 2) AS min_price,
            round(max(price)::numeric, 2) AS max_price
        FROM market_trades
        WHERE trade_time >= :last_minute
        GROUP BY exchange, symbol, minute_bucket
        """

        df = pd.read_sql(
            text(query),
            conn,
            params={"last_minute": last_minute},
        )

        if df.empty:
            print("No new data to aggregate.")
            return

        conn.execute(
            text(
                """
                DELETE FROM trades_1m_agg
                WHERE minute_bucket >= :last_minute
                """
            ),
            {"last_minute": last_minute},
        )

        df.to_sql(
            "trades_1m_agg",
            con=conn,
            if_exists="append",
            index=False,
            method="multi",
        )

        print(f"Inserted {len(df)} rows into trades_1m_agg")
