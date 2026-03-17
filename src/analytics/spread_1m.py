import pandas as pd
from sqlalchemy import create_engine, text

from src.config.settings import POSTGRES_URI


engine = create_engine(POSTGRES_URI)


def aggregate_spread_1m():
    with engine.begin() as conn:
        last_minute = conn.execute(
            text("SELECT max(minute_bucket) FROM spread_1m")
        ).scalar()

        if last_minute is None:
            last_minute = pd.Timestamp("1970-01-01", tz="UTC")

        query = """
            WITH spreads AS (
                SELECT 
                    symbol,
                    minute_bucket,
                    (array_agg(exchange ORDER BY low_price ASC, exchange ASC))[1] AS min_exchange,
                    (array_agg(low_price ORDER BY low_price ASC, exchange ASC))[1] AS min_price,
                    (array_agg(exchange ORDER BY high_price DESC, exchange ASC))[1] AS max_exchange,
                    (array_agg(high_price ORDER BY high_price DESC, exchange ASC))[1] AS max_price
                FROM ohlc_1m
                WHERE minute_bucket >= :last_minute
                GROUP BY symbol, minute_bucket
            )
            SELECT
                symbol,
                minute_bucket,
                min_exchange,
                min_price,
                max_exchange,
                max_price,
                round((max_price - min_price)::numeric, 4) AS spread_abs,
                round(((max_price - min_price) / min_price * 100)::numeric, 4) AS spread_pct
            FROM spreads
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
                DELETE FROM spread_1m
                WHERE minute_bucket >= :last_minute
                """
            ),
            {"last_minute": last_minute},
        )

        df.to_sql(
            "spread_1m",
            con=conn,
            if_exists="append",
            index=False,
            method="multi",
        )

        print(f"Inserted {len(df)} rows into spread_1m")
