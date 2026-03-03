"""Glue helpers that feed live events into LiveRiskManager."""

from __future__ import annotations

from typing import Any

from autobot.risk.live_risk_manager import LiveRiskManager


def apply_ticker_event(
    *,
    risk_manager: LiveRiskManager,
    event: Any,
) -> list[dict[str, Any]]:
    market = str(getattr(event, "market", "")).strip().upper()
    trade_price = getattr(event, "trade_price", None)
    ts_ms = getattr(event, "ts_ms", None)
    if not market or trade_price is None:
        return []
    try:
        price_value = float(trade_price)
    except (TypeError, ValueError):
        return []
    return risk_manager.evaluate_price(market=market, last_price=price_value, ts_ms=_as_int(ts_ms))


def apply_executor_event(
    *,
    risk_manager: LiveRiskManager,
    event: dict[str, Any],
) -> dict[str, Any] | None:
    if not isinstance(event, dict):
        return None
    return risk_manager.handle_executor_event(event)


def _as_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
