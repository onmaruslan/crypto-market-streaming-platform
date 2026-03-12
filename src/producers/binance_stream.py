import asyncio
import json
import logging
import websockets
from datetime import datetime, UTC

from src.schemas.trade_schema import TradeEvent
from src.producers.kafka_producer import CryptoKafkaProducer


logger = logging.getLogger(__name__)


BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"


SYMBOLS = ["btcusdt", "ethusdt", "solusdt"]


class BinanceStream:
    def __init__(self, kafka_producer: CryptoKafkaProducer):
        self.kafka = kafka_producer

    async def start(self):
        """
        Start Binance websocket stream with auto reconnect.
        """

        while True:
            try:
                stream_url = self._build_stream_url()

                logger.info(f"Connecting to Binance stream: {stream_url}")

                async with websockets.connect(stream_url) as ws:
                    logger.info("Connected to Binance WebSocket")

                    while True:
                        message = await ws.recv()

                        data = json.loads(message)

                        trade_event = self._parse_trade(data)

                        if trade_event:
                            self.kafka.send_trade_event(trade_event.to_dict())

            except Exception as e:
                logger.error(f"Binance stream error: {e}")

                logger.info("Reconnecting in 5 seconds...")

                await asyncio.sleep(5)

    def _build_stream_url(self):

        streams = [f"{symbol}@trade" for symbol in SYMBOLS]

        stream_path = "/".join(streams)

        return f"{BINANCE_WS_URL}/{stream_path}"

    def _parse_trade(self, data):

        try:
            symbol = data["s"]
            trade_id = data["t"]

            price = float(data["p"])
            quantity = float(data["q"])

            trade_time = datetime.fromtimestamp(data["T"] / 1000, tz=UTC)

            side = "sell" if data["m"] else "buy"

            event = TradeEvent(
                exchange="binance",
                symbol=self._normalize_symbol(symbol),
                trade_id=str(trade_id),
                price=price,
                quantity=quantity,
                side=side,
                trade_time=trade_time,
                ingestion_time=datetime.now(UTC),
            )

            return event

        except Exception as e:
            logger.error(f"Failed to parse Binance trade: {e}")

            return None

    def _normalize_symbol(self, symbol: str):
        """
        Convert Binance symbol format BTCUSDT → BTC-USD
        """

        if symbol.endswith("USDT"):
            base = symbol[:-4]
            return f"{base}-USD"

        return symbol
