"""Live market-state snapshots used by the vnext scanner/selector boundary."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LiveMarketState:
    market: str
    trade_price: float
    acc_trade_price_24h: float
    ts_ms: int

