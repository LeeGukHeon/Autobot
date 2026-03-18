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
class TradeEvent:
    market: str
    ts_ms: int
    trade_price: float
    trade_volume: float
    ask_bid: str
    sequential_id: int | None = None
    stream_type: str = "trade"


@dataclass(frozen=True)
class OrderbookUnit:
    ask_price: float | None
    ask_size: float | None
    bid_price: float | None
    bid_size: float | None


@dataclass(frozen=True)
class OrderbookEvent:
    market: str
    ts_ms: int
    total_ask_size: float | None
    total_bid_size: float | None
    units: tuple[OrderbookUnit, ...]
    level: int | str | None = None
    stream_type: str = "orderbook"


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
