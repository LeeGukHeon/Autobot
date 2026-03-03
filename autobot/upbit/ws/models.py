"""Data models used by Upbit WebSocket quotation client."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


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


@dataclass(frozen=True)
class MyOrderEvent:
    ts_ms: int
    uuid: str | None
    identifier: str | None
    market: str | None
    side: str | None
    ord_type: str | None
    state: str | None
    price: float | None
    volume: float | None
    executed_volume: float | None
    stream_type: str = "myOrder"
    raw: dict[str, Any] | None = None


@dataclass(frozen=True)
class MyAssetEvent:
    ts_ms: int
    currency: str | None
    balance: float | None
    locked: float | None
    avg_buy_price: float | None
    stream_type: str = "myAsset"
    raw: dict[str, Any] | None = None
