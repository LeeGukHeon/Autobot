"""Data models used by Upbit WebSocket quotation client."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Subscription:
    type: str
    codes: tuple[str, ...]
    is_only_snapshot: bool | None = None
    is_only_realtime: bool | None = None


@dataclass(frozen=True)
class TickerEvent:
    market: str
    ts_ms: int
    trade_price: float
    acc_trade_price_24h: float
    stream_type: str = "ticker"
    market_state: str | None = None
    market_warning: str | None = None

