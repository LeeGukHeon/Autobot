"""Execution intent contract shared by strategy and execution layers."""

from __future__ import annotations

from dataclasses import dataclass, field
import time
import uuid


@dataclass(frozen=True)
class OrderIntent:
    intent_id: str
    ts_ms: int
    market: str
    side: str
    ord_type: str
    price: float
    volume: float
    time_in_force: str
    reason_code: str
    meta: dict[str, object] = field(default_factory=dict)


def new_order_intent(
    *,
    market: str,
    side: str,
    price: float,
    volume: float,
    reason_code: str,
    ord_type: str = "limit",
    time_in_force: str = "gtc",
    meta: dict[str, object] | None = None,
    ts_ms: int | None = None,
) -> OrderIntent:
    side_value = str(side).strip().lower()
    if side_value not in {"bid", "ask"}:
        raise ValueError("side must be bid or ask")

    ord_type_value = str(ord_type).strip().lower()
    if ord_type_value != "limit":
        raise ValueError("only limit order intents are supported")

    tif_value = str(time_in_force).strip().lower()
    if tif_value not in {"gtc", "ioc", "fok"}:
        raise ValueError("time_in_force must be one of: gtc, ioc, fok")

    market_value = str(market).strip().upper()
    if not market_value:
        raise ValueError("market is required")

    if price <= 0:
        raise ValueError("price must be positive")
    if volume <= 0:
        raise ValueError("volume must be positive")

    return OrderIntent(
        intent_id=uuid.uuid4().hex,
        ts_ms=int(ts_ms if ts_ms is not None else time.time() * 1000),
        market=market_value,
        side=side_value,
        ord_type=ord_type_value,
        price=float(price),
        volume=float(volume),
        time_in_force=tif_value,
        reason_code=str(reason_code),
        meta=dict(meta or {}),
    )