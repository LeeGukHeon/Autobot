"""Parsers that normalize Upbit WebSocket ticker payloads."""

from __future__ import annotations

import json
import time
from typing import Any

from .models import MyAssetEvent, MyOrderEvent, OrderbookEvent, OrderbookUnit, TickerEvent, TradeEvent


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


def parse_trade_event(payload: Any) -> TradeEvent | None:
    message = _extract_mapping(payload)
    if message is None:
        return None

    market = _to_str(_coalesce(message, "code", "cd", "market"))
    ts_ms = _to_int(_coalesce(message, "trade_timestamp", "ttms", "timestamp", "tms"))
    trade_price = _to_float(_coalesce(message, "trade_price", "tp"))
    trade_volume = _to_float(_coalesce(message, "trade_volume", "tv"))
    ask_bid = _to_str(_coalesce(message, "ask_bid", "ab"))
    sequential_id = _to_int(_coalesce(message, "sequential_id", "sid"))
    if market is None or ts_ms is None or trade_price is None or trade_volume is None or ask_bid is None:
        return None

    stream_type = _to_str(_coalesce(message, "type", "ty")) or "trade"
    return TradeEvent(
        market=market.upper(),
        ts_ms=ts_ms,
        trade_price=trade_price,
        trade_volume=trade_volume,
        ask_bid=ask_bid.upper(),
        sequential_id=sequential_id,
        stream_type=stream_type,
    )


def parse_orderbook_event(payload: Any) -> OrderbookEvent | None:
    message = _extract_mapping(payload)
    if message is None:
        return None

    market = _to_str(_coalesce(message, "code", "cd", "market"))
    ts_ms = _to_int(_coalesce(message, "timestamp", "tms"))
    units_raw = _coalesce(message, "orderbook_units", "obu")
    if market is None or ts_ms is None or not isinstance(units_raw, list):
        return None

    units: list[OrderbookUnit] = []
    for item in units_raw:
        if not isinstance(item, dict):
            continue
        units.append(
            OrderbookUnit(
                ask_price=_to_float(_coalesce(item, "ask_price", "ap")),
                ask_size=_to_float(_coalesce(item, "ask_size", "as")),
                bid_price=_to_float(_coalesce(item, "bid_price", "bp")),
                bid_size=_to_float(_coalesce(item, "bid_size", "bs")),
            )
        )
    if not units:
        return None

    stream_type = _to_str(_coalesce(message, "type", "ty")) or "orderbook"
    level = _coalesce(message, "level", "lv", "orderbook_level")
    return OrderbookEvent(
        market=market.upper(),
        ts_ms=ts_ms,
        total_ask_size=_to_float(_coalesce(message, "total_ask_size", "tas")),
        total_bid_size=_to_float(_coalesce(message, "total_bid_size", "tbs")),
        units=tuple(units),
        level=level,
        stream_type=stream_type,
    )


def parse_private_event(payload: Any) -> MyOrderEvent | MyAssetEvent | None:
    events = parse_private_events(payload)
    return events[0] if events else None


def parse_private_events(payload: Any) -> list[MyOrderEvent | MyAssetEvent]:
    message = _extract_mapping(payload)
    if message is None:
        return []

    raw_stream_type = _to_str(_coalesce(message, "type", "ty"))
    if raw_stream_type is None:
        return []
    stream_type = raw_stream_type.strip()
    normalized = stream_type.lower()
    ts_ms = _to_int(
        _coalesce(
            message,
            "timestamp",
            "tms",
            "trade_timestamp",
            "ttms",
            "order_timestamp",
            "otms",
        )
    )
    if ts_ms is None:
        ts_ms = int(time.time() * 1000)

    if normalized == "myorder":
        market = _to_str(_coalesce(message, "code", "cd", "market"))
        return [
            MyOrderEvent(
                ts_ms=ts_ms,
                uuid=_to_str(_coalesce(message, "uuid", "uid")),
                identifier=_to_str(_coalesce(message, "identifier", "id", "i")),
                market=market.upper() if market else None,
                side=_to_str(_coalesce(message, "side", "sd", "ask_bid", "ab")),
                ord_type=_to_str(_coalesce(message, "ord_type", "order_type", "ot")),
                state=_to_str(_coalesce(message, "state", "status", "s")),
                price=_to_float(_coalesce(message, "price", "p")),
                volume=_to_float(_coalesce(message, "volume", "v")),
                executed_volume=_to_float(_coalesce(message, "executed_volume", "ev")),
                stream_type=_to_str(_coalesce(message, "stream_type", "st")) or "myOrder",
                raw=dict(message),
            )
        ]

    if normalized == "myasset":
        asset_events = _parse_myasset_events(message=message, fallback_ts_ms=ts_ms)
        if asset_events:
            return asset_events
        currency = _to_str(_coalesce(message, "currency", "cy"))
        return [
            MyAssetEvent(
                ts_ms=ts_ms,
                currency=currency.upper() if currency else None,
                balance=_to_float(_coalesce(message, "balance", "bl")),
                locked=_to_float(_coalesce(message, "locked", "lk")),
                avg_buy_price=_to_float(_coalesce(message, "avg_buy_price", "abp")),
                stream_type=_to_str(_coalesce(message, "stream_type", "st")) or "myAsset",
                raw=dict(message),
            )
        ]

    return []


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


def _parse_myasset_events(*, message: dict[str, Any], fallback_ts_ms: int) -> list[MyAssetEvent]:
    assets = message.get("assets")
    if not isinstance(assets, list):
        return []
    asset_ts_ms = _to_int(_coalesce(message, "asset_timestamp", "atms"))
    stream_type = _to_str(_coalesce(message, "stream_type", "st")) or "myAsset"
    events: list[MyAssetEvent] = []
    for item in assets:
        if not isinstance(item, dict):
            continue
        currency = _to_str(_coalesce(item, "currency", "cy"))
        if currency is None:
            continue
        events.append(
            MyAssetEvent(
                ts_ms=asset_ts_ms if asset_ts_ms is not None else int(fallback_ts_ms),
                currency=currency.upper(),
                balance=_to_float(_coalesce(item, "balance", "bl")),
                locked=_to_float(_coalesce(item, "locked", "lk")),
                avg_buy_price=_to_float(_coalesce(item, "avg_buy_price", "abp")),
                stream_type=stream_type,
                raw=dict(item),
            )
        )
    return events
