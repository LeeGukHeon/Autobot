"""Parsers that normalize Upbit WebSocket ticker payloads."""

from __future__ import annotations

import json
from typing import Any

from .models import TickerEvent


def decode_ws_message(raw_message: bytes | str) -> Any:
    if isinstance(raw_message, bytes):
        text = raw_message.decode("utf-8")
    else:
        text = raw_message
    return json.loads(text)


def parse_ticker_event(payload: Any) -> TickerEvent | None:
    message = _extract_mapping(payload)
    if message is None:
        return None

    market = _to_str(_coalesce(message, "code", "cd"))
    ts_ms = _to_int(_coalesce(message, "timestamp", "tms"))
    trade_price = _to_float(_coalesce(message, "trade_price", "tp"))
    acc_trade_price_24h = _to_float(_coalesce(message, "acc_trade_price_24h", "atp24h"))
    if market is None or ts_ms is None or trade_price is None or acc_trade_price_24h is None:
        return None

    stream_type = _to_str(_coalesce(message, "type", "ty")) or "ticker"
    market_state = _to_str(_coalesce(message, "market_state", "ms"))
    market_warning = _to_str(_coalesce(message, "market_warning", "mw"))
    return TickerEvent(
        market=market.upper(),
        ts_ms=ts_ms,
        trade_price=trade_price,
        acc_trade_price_24h=acc_trade_price_24h,
        stream_type=stream_type,
        market_state=market_state,
        market_warning=market_warning,
    )


def _extract_mapping(payload: Any) -> dict[str, Any] | None:
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                return item
    return None


def _coalesce(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in payload:
            return payload[key]
    return None


def _to_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

