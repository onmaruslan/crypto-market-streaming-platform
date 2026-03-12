import asyncio
import logging

from src.producers.market_data_producer import MarketDataProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


async def main():

    producer = MarketDataProducer()

    await producer.start()


if __name__ == "__main__":
    asyncio.run(main())
