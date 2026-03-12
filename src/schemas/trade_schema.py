from dataclasses import dataclass
from datetime import datetime
from typing import Literal
import numpy as np


@dataclass
class TradeEvent:
    # Unified trade event schema used across the entire pipeline.
    exchange: Literal["binance", "coinbase", "kraken"]
    symbol: str
    trade_id: str
    price: float
    quantity: float
    side: Literal["buy", "sell"]
    trade_time: datetime
    ingestion_time: datetime

    def to_dict(self) -> dict:
        return {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "trade_id": self.trade_id,
            "price": self.price,
            "quantity": self.quantity,
            "side": self.side,
            "trade_time": self.trade_time.isoformat(),
            "ingestion_time": self.ingestion_time.isoformat(),
        }
