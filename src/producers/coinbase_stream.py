import asyncio
import json
import logging
import websockets
from datetime import datetime, UTC

from src.schemas.trade_schema import TradeEvent
from src.producers.kafka_producer import CryptoKafkaProducer

logger = logging.getLogger(__name__)


COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"


SYMBOLS = ["BTC-USD", "ETH-USD", "SOL-USD"]


class CoinbaseStream:
    def __init__(self, kafka_producer: CryptoKafkaProducer):
        self.kafka = kafka_producer

    async def start(self):
        """
        Start Coinbase websocket stream with auto reconnect.
        """

        while True:
            try:
                logger.info(f"Connecting to Coinbase stream: {COINBASE_WS_URL}")

                async with websockets.connect(COINBASE_WS_URL) as ws:
                    logger.info("Connected to Coinbase WebSocket")

                    await ws.send(
                        json.dumps(
                            {
                                "type": "subscribe",
                                "channels": [
                                    {"name": "matches", "product_ids": SYMBOLS}
                                ],
                            }
                        )
                    )
                    while True:
                        message = await ws.recv()
                        data = json.loads(message)

                        trade_event = self._parse_trade(data)

                        if trade_event:
                            self.kafka.send_trade_event(trade_event.to_dict())

            except Exception as e:
                logger.error(f"Coinbase stream error: {e}")
                logger.info("Reconnecting in 5 seconds...")
                await asyncio.sleep(5)

    def _parse_trade(self, data):
        try:
            if data.get("type") != "match":
                return None

            symbol = data["product_id"]
            trade_id = data["trade_id"]
            price = float(data["price"])
            quantity = float(data["size"])
            side = data["side"]
            trade_time = datetime.fromisoformat(data["time"].replace("Z", "+00:00"))

            event = TradeEvent(
                exchange="coinbase",
                symbol=symbol,
                trade_id=str(trade_id),
                price=price,
                quantity=quantity,
                side=side,
                trade_time=trade_time,
                ingestion_time=datetime.now(UTC),
            )

            return event

        except Exception as e:
            logger.error(f"Failed to parse Coinbase trade: {e}")
            return None
