import json
import logging
from kafka import KafkaProducer

from src.config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_CRYPTO


class CryptoKafkaProducer:
    """
    Wrapper around KafkaProducer used to send normalized trade events
    into Kafka.
    """

    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=5,
        )

    def send_trade_event(self, trade_event: dict):
        """
        Send trade event to Kafka topic.
        """

        try:
            self.producer.send(KAFKA_TOPIC_CRYPTO, trade_event)

        except Exception as e:
            logger.error(f"Failed to send event to Kafka: {e}")

    def flush(self):
        """
        Force sending all buffered messages.
        """
        self.producer.flush()

    def close(self):
        """
        Close Kafka producer connection.
        """
        self.producer.close()
