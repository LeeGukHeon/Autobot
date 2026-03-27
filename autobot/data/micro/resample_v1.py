"""Resampling helpers for micro v1 (1m -> 5m)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import polars as pl


@dataclass
class _ResampleState:
    trade_count: int = 0
    buy_count: int = 0
    sell_count: int = 0
    trade_volume_total: float = 0.0
    buy_volume: float = 0.0
    sell_volume: float = 0.0
    vwap_num: float = 0.0
    max_trade_size: float | None = None
    last_trade_bar_ts_ms: int | None = None
    last_trade_price: float | None = None
    trade_events: int = 0
    trade_min_ts_ms: int | None = None
    trade_max_ts_ms: int | None = None
    has_trade_ws: bool = False
    has_trade_rest: bool = False

    book_events: int = 0
    sum_mid_weighted: float = 0.0
    sum_spread_weighted: float = 0.0
    sum_depth_bid_weighted: float = 0.0
    sum_depth_ask_weighted: float = 0.0
    sum_imbalance_weighted: float = 0.0
    sum_microprice_bias_weighted: float = 0.0
    microprice_bias_weight: int = 0
    book_min_ts_ms: int | None = None
    book_max_ts_ms: int | None = None


def resample_micro_1m_to_5m(frame_1m: pl.DataFrame) -> pl.DataFrame:
    if frame_1m.height <= 0:
        return frame_1m.clone()

    required = [
        "market",
        "ts_ms",
        "trade_source",
        "trade_count",
        "buy_count",
        "sell_count",
        "trade_volume_total",
        "buy_volume",
        "sell_volume",
        "vwap",
        "max_trade_size",
        "last_trade_price",
        "trade_events",
        "trade_min_ts_ms",
        "trade_max_ts_ms",
        "book_events",
        "mid_mean",
        "spread_bps_mean",
        "depth_bid_top5_mean",
        "depth_ask_top5_mean",
        "imbalance_top5_mean",
        "microprice_bias_bps_mean",
        "book_min_ts_ms",
        "book_max_ts_ms",
    ]
    missing = [col for col in required if col not in frame_1m.columns]
    if missing:
        raise ValueError(f"1m frame missing columns: {', '.join(missing)}")

    states: dict[tuple[str, int], _ResampleState] = {}
    for row in frame_1m.sort(["market", "ts_ms"]).iter_rows(named=True):
        market = str(row["market"])
        ts_ms = int(row["ts_ms"])
        ts_5m = (ts_ms // 300_000) * 300_000
        key = (market, ts_5m)
        state = states.setdefault(key, _ResampleState())

        trade_count = int(row.get("trade_count") or 0)
        buy_count = int(row.get("buy_count") or 0)
        sell_count = int(row.get("sell_count") or 0)
        trade_volume_total = float(row.get("trade_volume_total") or 0.0)
        buy_volume = float(row.get("buy_volume") or 0.0)
        sell_volume = float(row.get("sell_volume") or 0.0)
        trade_events = int(row.get("trade_events") or 0)

        state.trade_count += trade_count
        state.buy_count += buy_count
        state.sell_count += sell_count
        state.trade_volume_total += trade_volume_total
        state.buy_volume += buy_volume
        state.sell_volume += sell_volume
        state.trade_events += trade_events

        vwap = _to_float_or_none(row.get("vwap"))
        if vwap is not None and trade_volume_total > 0.0:
            state.vwap_num += vwap * trade_volume_total

        max_trade_size = _to_float_or_none(row.get("max_trade_size"))
        if max_trade_size is not None:
            state.max_trade_size = max(max_trade_size, state.max_trade_size or max_trade_size)

        last_trade_price = _to_float_or_none(row.get("last_trade_price"))
        if last_trade_price is not None and trade_events > 0:
            if state.last_trade_bar_ts_ms is None or ts_ms >= state.last_trade_bar_ts_ms:
                state.last_trade_bar_ts_ms = ts_ms
                state.last_trade_price = last_trade_price

        trade_min_ts = _to_int(row.get("trade_min_ts_ms"))
        trade_max_ts = _to_int(row.get("trade_max_ts_ms"))
        if trade_min_ts is not None:
            if state.trade_min_ts_ms is None or trade_min_ts < state.trade_min_ts_ms:
                state.trade_min_ts_ms = trade_min_ts
        if trade_max_ts is not None:
            if state.trade_max_ts_ms is None or trade_max_ts > state.trade_max_ts_ms:
                state.trade_max_ts_ms = trade_max_ts

        source = str(row.get("trade_source") or "none").strip().lower()
        if source == "ws" and trade_events > 0:
            state.has_trade_ws = True
        elif source == "rest" and trade_events > 0:
            state.has_trade_rest = True

        book_events = int(row.get("book_events") or 0)
        state.book_events += book_events
        if book_events > 0:
            mid_mean = _to_float_or_none(row.get("mid_mean"))
            spread_bps_mean = _to_float_or_none(row.get("spread_bps_mean"))
            depth_bid_mean = _to_float_or_none(row.get("depth_bid_top5_mean"))
            depth_ask_mean = _to_float_or_none(row.get("depth_ask_top5_mean"))
            imbalance_mean = _to_float_or_none(row.get("imbalance_top5_mean"))
            microprice_bias_mean = _to_float_or_none(row.get("microprice_bias_bps_mean"))

            if mid_mean is not None:
                state.sum_mid_weighted += mid_mean * book_events
            if spread_bps_mean is not None:
                state.sum_spread_weighted += spread_bps_mean * book_events
            if depth_bid_mean is not None:
                state.sum_depth_bid_weighted += depth_bid_mean * book_events
            if depth_ask_mean is not None:
                state.sum_depth_ask_weighted += depth_ask_mean * book_events
            if imbalance_mean is not None:
                state.sum_imbalance_weighted += imbalance_mean * book_events
            if microprice_bias_mean is not None:
                state.sum_microprice_bias_weighted += microprice_bias_mean * book_events
                state.microprice_bias_weight += book_events

        book_min_ts = _to_int(row.get("book_min_ts_ms"))
        book_max_ts = _to_int(row.get("book_max_ts_ms"))
        if book_min_ts is not None:
            if state.book_min_ts_ms is None or book_min_ts < state.book_min_ts_ms:
                state.book_min_ts_ms = book_min_ts
        if book_max_ts is not None:
            if state.book_max_ts_ms is None or book_max_ts > state.book_max_ts_ms:
                state.book_max_ts_ms = book_max_ts

    rows: list[dict[str, Any]] = []
    for (market, ts_ms), state in sorted(states.items()):
        trade_source = "none"
        if state.has_trade_ws:
            trade_source = "ws"
        elif state.has_trade_rest:
            trade_source = "rest"

        vwap = (state.vwap_num / state.trade_volume_total) if state.trade_volume_total > 0.0 else None
        avg_trade_size = (
            state.trade_volume_total / float(state.trade_count) if state.trade_count > 0 else None
        )
        trade_imbalance = (
            (state.buy_volume - state.sell_volume) / max(state.trade_volume_total, 1e-12)
            if state.trade_volume_total > 0.0
            else 0.0
        )
        trade_coverage_ms = _coverage_ms(state.trade_min_ts_ms, state.trade_max_ts_ms)
        book_coverage_ms = _coverage_ms(state.book_min_ts_ms, state.book_max_ts_ms)

        row = {
            "market": market,
            "tf": "5m",
            "ts_ms": int(ts_ms),
            "trade_source": trade_source,
            "trade_events": int(state.trade_events),
            "book_events": int(state.book_events),
            "trade_min_ts_ms": state.trade_min_ts_ms,
            "trade_max_ts_ms": state.trade_max_ts_ms,
            "book_min_ts_ms": state.book_min_ts_ms,
            "book_max_ts_ms": state.book_max_ts_ms,
            "trade_coverage_ms": trade_coverage_ms,
            "book_coverage_ms": book_coverage_ms,
            "micro_trade_available": bool(state.trade_events > 0),
            "micro_book_available": bool(state.book_events > 0),
            "micro_available": bool(state.trade_events > 0 or state.book_events > 0),
            "trade_count": int(state.trade_count),
            "buy_count": int(state.buy_count),
            "sell_count": int(state.sell_count),
            "trade_volume_total": float(state.trade_volume_total),
            "buy_volume": float(state.buy_volume),
            "sell_volume": float(state.sell_volume),
            "trade_imbalance": float(trade_imbalance),
            "vwap": (float(vwap) if vwap is not None else None),
            "avg_trade_size": (float(avg_trade_size) if avg_trade_size is not None else None),
            "max_trade_size": state.max_trade_size,
            "last_trade_price": state.last_trade_price,
            "mid_mean": (
                state.sum_mid_weighted / float(state.book_events) if state.book_events > 0 else None
            ),
            "spread_bps_mean": (
                state.sum_spread_weighted / float(state.book_events) if state.book_events > 0 else None
            ),
            "depth_bid_top5_mean": (
                state.sum_depth_bid_weighted / float(state.book_events) if state.book_events > 0 else None
            ),
            "depth_ask_top5_mean": (
                state.sum_depth_ask_weighted / float(state.book_events) if state.book_events > 0 else None
            ),
            "imbalance_top5_mean": (
                state.sum_imbalance_weighted / float(state.book_events) if state.book_events > 0 else None
            ),
            "microprice_bias_bps_mean": (
                state.sum_microprice_bias_weighted / float(state.microprice_bias_weight)
                if state.microprice_bias_weight > 0
                else None
            ),
            "book_update_count": int(state.book_events),
        }
        rows.append(row)

    if not rows:
        return pl.DataFrame([], schema=frame_1m.schema, orient="row")
    return pl.DataFrame(rows, schema=frame_1m.schema, orient="row").sort(["market", "ts_ms"])


def _coverage_ms(min_ts: int | None, max_ts: int | None) -> int:
    if min_ts is None or max_ts is None:
        return 0
    if int(max_ts) < int(min_ts):
        return 0
    return int(max_ts) - int(min_ts)


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            return int(float(text))
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
