import json
import logging
import time
from datetime import datetime, UTC
from kafka import KafkaConsumer
import boto3

from src.config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_CRYPTO,
    S3_BUCKET,
    S3_ENDPOINT_URL,
)

logger = logging.getLogger(__name__)


class RawToS3Consumer:
    def __init__(self):
        # Kafka consumer
        self.consumer = KafkaConsumer(
            KAFKA_TOPIC_CRYPTO,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )

        # S3 client
        self.s3_client = boto3.client("s3", endpoint_url=S3_ENDPOINT_URL)

        # batching
        self.buffer = []
        self.batch_size = 1000
        self.last_flush = time.time()
        self.flush_interval = 10

    def start(self):

        logger.info("Starting RawToS3Consumer")

        for message in self.consumer:
            event = message.value
            self.buffer.append(event)

            if self._should_flush():
                self._flush()

    def _should_flush(self):

        if len(self.buffer) >= self.batch_size:
            return True

        if time.time() - self.last_flush >= self.flush_interval:
            return True

        return False

    def _flush(self):

        if not self.buffer:
            return

        now = datetime.now(UTC)

        exchange = self.buffer[0]["exchange"]
        symbol = self.buffer[0]["symbol"]

        key = (
            f"raw/trades/"
            f"exchange={exchange}/"
            f"symbol={symbol}/"
            f"date={now.date()}/"
            f"hour={now.hour}/"
            f"{int(time.time())}.json"
        )

        body = "\n".join(json.dumps(x) for x in self.buffer)

        self.s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=body,
        )

        logger.info(f"Flushed {len(self.buffer)} records to S3 {key}")

        self.buffer = []
        self.last_flush = time.time()
