import json
from io import BytesIO

import boto3
import pandas as pd

from src.config.settings import S3_BUCKET, S3_ENDPOINT_URL


s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT_URL)


def convert_raw_to_parquet(key: str):
    response = s3.get_object(
        Bucket=S3_BUCKET,
        Key=key,
    )

    body = response["Body"].read().decode("utf-8")
    rows = [json.loads(line) for line in body.splitlines() if line.strip()]

    df = pd.DataFrame(rows)

    if df.empty:
        return None

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["trade_time"] = pd.to_datetime(df["trade_time"], utc=True, errors="coerce")
    df["ingestion_time"] = pd.to_datetime(
        df["ingestion_time"], utc=True, errors="coerce"
    )

    df = df.dropna(
        subset=[
            "exchange",
            "symbol",
            "trade_id",
            "price",
            "quantity",
            "side",
            "trade_time",
            "ingestion_time",
        ]
    )

    df["trade_id"] = df["trade_id"].astype(str)
    df["exchange"] = df["exchange"].astype(str)
    df["symbol"] = df["symbol"].astype(str)
    df["side"] = df["side"].astype(str)

    buffer = BytesIO()
    df.to_parquet(buffer, index=False)

    parquet_key = key.replace("raw/", "staging/").replace(".json", ".parquet")

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=parquet_key,
        Body=buffer.getvalue(),
    )

    return parquet_key
