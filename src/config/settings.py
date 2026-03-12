import os
from dotenv import load_dotenv

load_dotenv()

# -----------------------------
# Kafka
# -----------------------------

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_CRYPTO = os.getenv("KAFKA_TOPIC_CRYPTO", "crypto_market_raw")


# -----------------------------
# S3 / LocalStack
# -----------------------------

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "test")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://localstack:4566")
S3_BUCKET = os.getenv("S3_BUCKET", "crypto-market-raw-data")


# -----------------------------
# PostgreSQL
# -----------------------------

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

POSTGRES_USER = os.getenv("POSTGRES_USER", "airflow")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")

POSTGRES_DB_AIRFLOW = os.getenv("POSTGRES_DB", "airflow")
POSTGRES_DB_DATA = os.getenv("POSTGRES_DATA_DB", "crypto_market")


# SQLAlchemy
POSTGRES_URI = (
    f"postgresql+psycopg://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB_DATA}"
)
