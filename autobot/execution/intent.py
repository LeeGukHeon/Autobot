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
    price: float | None
    volume: float | None
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
    if ord_type_value not in {"limit", "best"}:
        raise ValueError("ord_type must be one of: limit, best")

    tif_value = str(time_in_force).strip().lower()
    if tif_value not in {"gtc", "ioc", "fok", "post_only"}:
        raise ValueError("time_in_force must be one of: gtc, ioc, fok, post_only")

    market_value = str(market).strip().upper()
    if not market_value:
        raise ValueError("market is required")

    price_value = float(price) if price is not None else None
    volume_value = float(volume) if volume is not None else None
    if ord_type_value == "limit":
        if price_value is None or price_value <= 0:
            raise ValueError("price must be positive for limit orders")
        if volume_value is None or volume_value <= 0:
            raise ValueError("volume must be positive for limit orders")
    else:
        if tif_value not in {"ioc", "fok"}:
            raise ValueError("best orders require time_in_force=ioc or fok")
        if side_value == "bid":
            if price_value is None or price_value <= 0:
                raise ValueError("price must be positive for best bid orders")
            volume_value = None
        else:
            if volume_value is None or volume_value <= 0:
                raise ValueError("volume must be positive for best ask orders")
            price_value = None

    return OrderIntent(
        intent_id=uuid.uuid4().hex,
        ts_ms=int(ts_ms if ts_ms is not None else time.time() * 1000),
        market=market_value,
        side=side_value,
        ord_type=ord_type_value,
        price=price_value,
        volume=volume_value,
        time_in_force=tif_value,
        reason_code=str(reason_code),
        meta=dict(meta or {}),
    )
