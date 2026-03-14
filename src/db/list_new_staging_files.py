import boto3
from sqlalchemy import create_engine, text

from src.config.settings import S3_BUCKET, S3_ENDPOINT_URL, POSTGRES_URI


s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT_URL)


def list_new_staging_files():
    engine = create_engine(POSTGRES_URI)

    response = s3.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix="staging/trades/",
    )

    all_staging_keys = [
        obj["Key"]
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]

    with engine.begin() as conn:
        result = conn.execute(text("SELECT parquet_key FROM loaded_staging_files"))
        loaded_keys = {row[0] for row in result.fetchall()}

    new_keys = [key for key in all_staging_keys if key not in loaded_keys]

    return new_keys
