import asyncio
import json
import logging
import websockets
from datetime import datetime, UTC

from src.schemas.trade_schema import TradeEvent
from src.producers.kafka_producer import CryptoKafkaProducer

logger = logging.getLogger(__name__)


KRAKEN_WS_URL = "wss://ws.kraken.com/v2"


SYMBOLS = ["BTC/USD", "ETH/USD", "SOL/USD"]


class KrakenStream:
    def __init__(self, kafka_producer: CryptoKafkaProducer):
        self.kafka = kafka_producer

    async def start(self):
        """
        Start Kraken websocket stream with auto reconnect.
        """

        while True:
            try:
                logger.info(f"Connecting to Kraken stream: {KRAKEN_WS_URL}")

                async with websockets.connect(KRAKEN_WS_URL) as ws:
                    logger.info("Connected to Kraken WebSocket")

                    subscribe = {
                        "method": "subscribe",
                        "params": {"channel": "trade", "symbol": SYMBOLS},
                    }

                    await ws.send(json.dumps(subscribe))

                    while True:
                        message = await ws.recv()
                        data = json.loads(message)

                        trade_event = self._parse_trade(data)

                        if trade_event:
                            self.kafka.send_trade_event(trade_event.to_dict())

            except Exception as e:
                logger.error(f"Kraken stream error: {e}")
                logger.info("Reconnecting in 5 seconds...")
                await asyncio.sleep(5)

    def _parse_trade(self, data):
        try:
            if data.get("type") != "update" or not data.get("data"):
                return None

            trade = data["data"][0]

            if not trade.get("trade_id"):
                return None

            symbol = self._normalize_symbol(trade["symbol"])
            trade_id = trade["trade_id"]
            price = trade["price"]
            quantity = trade["qty"]
            side = trade["side"]
            trade_time = datetime.fromisoformat(
                trade["timestamp"].replace("Z", "+00:00")
            )

            event = TradeEvent(
                exchange="kraken",
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
            logger.error(f"Failed to parse Kraken trade: {e}")
            return None

    def _normalize_symbol(self, symbol: str):
        """
        Convert Kraken symbol format BTC/USD → BTC-USD
        """
        return symbol.replace("/", "-")
