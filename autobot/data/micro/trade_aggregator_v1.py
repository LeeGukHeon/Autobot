"""Trade micro aggregation helpers (ws preferred, rest fallback)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl

from .raw_readers import (
    ParseCounters,
    bar_ts_floor,
    discover_rest_tick_files,
    discover_ws_files,
    iter_jsonl_zst_rows,
    normalize_rest_trade_row,
    normalize_ws_trade_row,
)


TRADE_BAR_COLUMNS: tuple[str, ...] = (
    "market",
    "ts_ms",
    "trade_count",
    "buy_count",
    "sell_count",
    "trade_volume_total",
    "buy_volume",
    "sell_volume",
    "trade_imbalance",
    "vwap",
    "avg_trade_size",
    "max_trade_size",
    "last_trade_price",
    "trade_events",
    "trade_min_ts_ms",
    "trade_max_ts_ms",
)


@dataclass
class TradeBarState:
    trade_count: int = 0
    buy_count: int = 0
    sell_count: int = 0
    trade_volume_total: float = 0.0
    buy_volume: float = 0.0
    sell_volume: float = 0.0
    vwap_num: float = 0.0
    max_trade_size: float = 0.0
    last_trade_ts_ms: int | None = None
    last_trade_price: float | None = None
    trade_min_ts_ms: int | None = None
    trade_max_ts_ms: int | None = None


@dataclass(frozen=True)
class TradeAggregateResult:
    frame: pl.DataFrame
    counters: ParseCounters


def aggregate_ws_trade_bars_1m(
    *,
    raw_ws_root: Path,
    date_value: str,
    markets: set[str] | None,
    chunk_rows: int = 200_000,
) -> TradeAggregateResult:
    files = discover_ws_files(raw_ws_root=raw_ws_root, channel="trade", date_value=date_value)
    states: dict[tuple[str, int], TradeBarState] = {}
    counters = ParseCounters()

    for part_file in files:
        _aggregate_trade_part_file(
            part_file=part_file,
            states=states,
            counters=counters,
            markets=markets,
            interval_ms=60_000,
            normalizer=normalize_ws_trade_row,
            chunk_rows=chunk_rows,
        )

    return TradeAggregateResult(frame=_states_to_frame(states), counters=counters)


def aggregate_rest_trade_bars_1m(
    *,
    raw_ticks_root: Path,
    date_value: str,
    markets: set[str] | None,
    chunk_rows: int = 200_000,
) -> TradeAggregateResult:
    files = discover_rest_tick_files(raw_ticks_root=raw_ticks_root, date_value=date_value, markets=markets)
    states: dict[tuple[str, int], TradeBarState] = {}
    counters = ParseCounters()

    for part_file in files:
        _aggregate_trade_part_file(
            part_file=part_file,
            states=states,
            counters=counters,
            markets=markets,
            interval_ms=60_000,
            normalizer=normalize_rest_trade_row,
            chunk_rows=chunk_rows,
        )

    return TradeAggregateResult(frame=_states_to_frame(states), counters=counters)


def merge_trade_bars_with_precedence(
    *,
    ws_frame: pl.DataFrame,
    rest_frame: pl.DataFrame,
) -> pl.DataFrame:
    ws_map = _frame_to_map(ws_frame)
    rest_map = _frame_to_map(rest_frame)

    keys = sorted(set(ws_map) | set(rest_map))
    rows: list[dict[str, Any]] = []
    for key in keys:
        ws_row = ws_map.get(key)
        rest_row = rest_map.get(key)
        chosen: dict[str, Any]
        source: str

        if ws_row is not None and int(ws_row.get("trade_count") or 0) > 0:
            chosen = dict(ws_row)
            source = "ws"
        elif rest_row is not None and int(rest_row.get("trade_count") or 0) > 0:
            chosen = dict(rest_row)
            source = "rest"
        else:
            chosen = dict(ws_row or rest_row or {})
            source = "none"

        if not chosen:
            continue
        chosen["trade_source"] = source
        rows.append(chosen)

    if not rows:
        return pl.DataFrame(
            [],
            schema={
                **{name: pl.Utf8 if name in {"market"} else pl.Int64 for name in ("market", "ts_ms")},
                "trade_count": pl.Int64,
                "buy_count": pl.Int64,
                "sell_count": pl.Int64,
                "trade_volume_total": pl.Float64,
                "buy_volume": pl.Float64,
                "sell_volume": pl.Float64,
                "trade_imbalance": pl.Float64,
                "vwap": pl.Float64,
                "avg_trade_size": pl.Float64,
                "max_trade_size": pl.Float64,
                "last_trade_price": pl.Float64,
                "trade_events": pl.Int64,
                "trade_min_ts_ms": pl.Int64,
                "trade_max_ts_ms": pl.Int64,
                "trade_source": pl.Utf8,
            },
            orient="row",
        )

    frame = pl.DataFrame(rows).sort(["market", "ts_ms"])
    return frame


def _aggregate_trade_part_file(
    *,
    part_file: Path,
    states: dict[tuple[str, int], TradeBarState],
    counters: ParseCounters,
    markets: set[str] | None,
    interval_ms: int,
    normalizer: Any,
    chunk_rows: int,
) -> None:
    _ = max(int(chunk_rows), 1)
    for raw in iter_jsonl_zst_rows(part_file):
        counters.raw_rows += 1
        normalized = normalizer(raw)
        if normalized is None:
            counters.parse_drop += 1
            continue

        market = str(normalized["market"])
        if markets is not None and market not in markets:
            counters.filtered_rows += 1
            continue

        counters.parsed_rows += 1
        event_ts_ms = int(normalized["event_ts_ms"])
        bar_ts_ms = bar_ts_floor(event_ts_ms, interval_ms=interval_ms)
        key = (market, bar_ts_ms)
        state = states.setdefault(key, TradeBarState())

        volume = float(normalized["volume"])
        price = float(normalized["price"])
        side = str(normalized["side"])

        state.trade_count += 1
        state.trade_volume_total += volume
        state.vwap_num += price * volume
        if side == "buy":
            state.buy_count += 1
            state.buy_volume += volume
        else:
            state.sell_count += 1
            state.sell_volume += volume

        if volume > state.max_trade_size:
            state.max_trade_size = volume
        if state.last_trade_ts_ms is None or event_ts_ms >= state.last_trade_ts_ms:
            state.last_trade_ts_ms = event_ts_ms
            state.last_trade_price = price
        if state.trade_min_ts_ms is None or event_ts_ms < state.trade_min_ts_ms:
            state.trade_min_ts_ms = event_ts_ms
        if state.trade_max_ts_ms is None or event_ts_ms > state.trade_max_ts_ms:
            state.trade_max_ts_ms = event_ts_ms


def _states_to_frame(states: dict[tuple[str, int], TradeBarState]) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for (market, ts_ms), state in sorted(states.items()):
        total = float(state.trade_volume_total)
        trade_count = int(state.trade_count)
        vwap = (state.vwap_num / total) if total > 0.0 else None
        avg_trade_size = (total / float(trade_count)) if trade_count > 0 else None
        imbalance = ((state.buy_volume - state.sell_volume) / max(total, 1e-12)) if total > 0.0 else 0.0
        rows.append(
            {
                "market": market,
                "ts_ms": int(ts_ms),
                "trade_count": trade_count,
                "buy_count": int(state.buy_count),
                "sell_count": int(state.sell_count),
                "trade_volume_total": total,
                "buy_volume": float(state.buy_volume),
                "sell_volume": float(state.sell_volume),
                "trade_imbalance": float(imbalance),
                "vwap": (float(vwap) if vwap is not None else None),
                "avg_trade_size": (float(avg_trade_size) if avg_trade_size is not None else None),
                "max_trade_size": (float(state.max_trade_size) if trade_count > 0 else None),
                "last_trade_price": (
                    float(state.last_trade_price) if state.last_trade_price is not None else None
                ),
                "trade_events": trade_count,
                "trade_min_ts_ms": state.trade_min_ts_ms,
                "trade_max_ts_ms": state.trade_max_ts_ms,
            }
        )

    if not rows:
        schema = {
            "market": pl.Utf8,
            "ts_ms": pl.Int64,
            "trade_count": pl.Int64,
            "buy_count": pl.Int64,
            "sell_count": pl.Int64,
            "trade_volume_total": pl.Float64,
            "buy_volume": pl.Float64,
            "sell_volume": pl.Float64,
            "trade_imbalance": pl.Float64,
            "vwap": pl.Float64,
            "avg_trade_size": pl.Float64,
            "max_trade_size": pl.Float64,
            "last_trade_price": pl.Float64,
            "trade_events": pl.Int64,
            "trade_min_ts_ms": pl.Int64,
            "trade_max_ts_ms": pl.Int64,
        }
        return pl.DataFrame([], schema=schema, orient="row")

    return pl.DataFrame(rows).sort(["market", "ts_ms"])


def _frame_to_map(frame: pl.DataFrame) -> dict[tuple[str, int], dict[str, Any]]:
    if frame.height <= 0:
        return {}
    mapping: dict[tuple[str, int], dict[str, Any]] = {}
    for row in frame.iter_rows(named=True):
        key = (str(row["market"]), int(row["ts_ms"]))
        mapping[key] = dict(row)
    return mapping
