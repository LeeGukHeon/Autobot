"""Parsers that normalize Upbit WebSocket ticker payloads."""

from __future__ import annotations

import json
import time
from typing import Any

from .models import MyAssetEvent, MyOrderEvent, TickerEvent


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


def parse_private_event(payload: Any) -> MyOrderEvent | MyAssetEvent | None:
    message = _extract_mapping(payload)
    if message is None:
        return None

    raw_stream_type = _to_str(_coalesce(message, "type", "ty"))
    if raw_stream_type is None:
        return None
    stream_type = raw_stream_type.strip()
    normalized = stream_type.lower()
    ts_ms = _to_int(_coalesce(message, "timestamp", "tms", "trade_timestamp"))
    if ts_ms is None:
        ts_ms = int(time.time() * 1000)

    if normalized == "myorder":
        market = _to_str(_coalesce(message, "code", "cd", "market"))
        return MyOrderEvent(
            ts_ms=ts_ms,
            uuid=_to_str(_coalesce(message, "uuid", "uid")),
            identifier=_to_str(_coalesce(message, "identifier", "i")),
            market=market.upper() if market else None,
            side=_to_str(_coalesce(message, "side", "sd")),
            ord_type=_to_str(_coalesce(message, "ord_type", "ot")),
            state=_to_str(_coalesce(message, "state", "st")),
            price=_to_float(_coalesce(message, "price", "p")),
            volume=_to_float(_coalesce(message, "volume", "v")),
            executed_volume=_to_float(_coalesce(message, "executed_volume", "ev")),
            stream_type="myOrder",
            raw=dict(message),
        )

    if normalized == "myasset":
        currency = _to_str(_coalesce(message, "currency", "cy"))
        return MyAssetEvent(
            ts_ms=ts_ms,
            currency=currency.upper() if currency else None,
            balance=_to_float(_coalesce(message, "balance", "bl")),
            locked=_to_float(_coalesce(message, "locked", "lk")),
            avg_buy_price=_to_float(_coalesce(message, "avg_buy_price", "abp")),
            stream_type="myAsset",
            raw=dict(message),
        )

    return None


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
