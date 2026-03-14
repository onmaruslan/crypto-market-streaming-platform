import boto3
import pandas as pd
from io import BytesIO
from sqlalchemy import create_engine, text

from src.config.settings import S3_BUCKET, S3_ENDPOINT_URL, POSTGRES_URI


s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT_URL)


def load_one_staging_file(parquet_key: str):
    engine = create_engine(POSTGRES_URI)

    response = s3.get_object(
        Bucket=S3_BUCKET,
        Key=parquet_key,
    )

    body = response["Body"].read()
    df = pd.read_parquet(BytesIO(body))

    df.to_sql(
        "market_trades",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
    )

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO loaded_staging_files (parquet_key)
                VALUES (:parquet_key)
                ON CONFLICT (parquet_key) DO NOTHING
                """
            ),
            {"parquet_key": parquet_key},
        )
