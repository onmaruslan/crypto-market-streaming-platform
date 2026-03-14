import boto3

from src.config.settings import S3_BUCKET, S3_ENDPOINT_URL


s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT_URL)


def list_raw_files():
    response = s3.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix="raw/trades/",
    )

    raw_keys = []

    for obj in response.get("Contents", []):
        raw_key = obj["Key"]

        if not raw_key.endswith(".json"):
            continue

        parquet_key = raw_key.replace("raw/", "staging/").replace(".json", ".parquet")

        if not _object_exists(parquet_key):
            raw_keys.append(raw_key)

    return raw_keys


def _object_exists(key: str) -> bool:
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except Exception:
        return False
