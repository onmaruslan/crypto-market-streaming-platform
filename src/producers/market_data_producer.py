import asyncio
import logging

from src.producers.kafka_producer import CryptoKafkaProducer
from src.producers.binance_stream import BinanceStream
from src.producers.coinbase_stream import CoinbaseStream
from src.producers.kraken_stream import KrakenStream

logger = logging.getLogger(__name__)


class MarketDataProducer:
    def __init__(self):
        self.kafka = CryptoKafkaProducer()

    async def start(self):
        """
        Start all exchange streams in parallel.
        """

        logger.info("Starting Market Data Producer...")

        binance_stream = BinanceStream(self.kafka)
        coinbase_stream = CoinbaseStream(self.kafka)
        kraken_stream = KrakenStream(self.kafka)

        await asyncio.gather(
            binance_stream.start(),
            coinbase_stream.start(),
            kraken_stream.start(),
        )
