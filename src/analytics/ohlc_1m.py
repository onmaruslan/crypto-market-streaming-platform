import pandas as pd
from sqlalchemy import create_engine, text

from src.config.settings import POSTGRES_URI


engine = create_engine(POSTGRES_URI)


def aggregate_ohlc_1m():
    with engine.begin() as conn:
        last_minute = conn.execute(
            text("SELECT max(minute_bucket) FROM ohlc_1m")
        ).scalar()

        if last_minute is None:
            last_minute = pd.Timestamp("1970-01-01", tz="UTC")

        query = """
        SELECT
            mt.exchange,
            mt.symbol,
            date_trunc('minute', mt.trade_time) AS minute_bucket,
            round((array_agg(mt.price ORDER BY mt.trade_time ASC, mt.trade_id ASC))[1]::numeric, 2) AS open_price,
            round(max(mt.price)::numeric, 2) AS high_price,
            round(min(mt.price)::numeric, 2) AS low_price,
            round((array_agg(mt.price ORDER BY mt.trade_time DESC, mt.trade_id DESC))[1]::numeric, 2) AS close_price,
            round(sum(mt.quantity)::numeric, 4) AS volume
        FROM market_trades mt
        WHERE mt.trade_time >= :last_minute
        GROUP BY
            mt.exchange,
            mt.symbol,
            date_trunc('minute', mt.trade_time)
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
                DELETE FROM ohlc_1m
                WHERE minute_bucket >= :last_minute
                """
            ),
            {"last_minute": last_minute},
        )

        df.to_sql(
            "ohlc_1m",
            con=conn,
            if_exists="append",
            index=False,
            method="multi",
        )

        print(f"Inserted {len(df)} rows into ohlc_1m")
