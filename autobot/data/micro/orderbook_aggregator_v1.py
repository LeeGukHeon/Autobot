"""Orderbook micro aggregation helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl

from .raw_readers import (
    ParseCounters,
    bar_ts_floor,
    discover_ws_files,
    iter_jsonl_zst_rows,
    normalize_ws_orderbook_row,
)


@dataclass
class OrderbookBarState:
    book_events: int = 0
    sum_mid: float = 0.0
    sum_spread_bps: float = 0.0
    sum_depth_bid: float = 0.0
    sum_depth_ask: float = 0.0
    sum_imbalance: float = 0.0
    sum_microprice_bias_bps: float = 0.0
    microprice_bias_count: int = 0
    min_ts_ms: int | None = None
    max_ts_ms: int | None = None


@dataclass(frozen=True)
class OrderbookAggregateResult:
    frame: pl.DataFrame
    counters: ParseCounters


def aggregate_orderbook_ws_bars_1m(
    *,
    raw_ws_root: Path,
    date_value: str,
    markets: set[str] | None,
    topk: int = 5,
    chunk_rows: int = 200_000,
) -> OrderbookAggregateResult:
    files = discover_ws_files(raw_ws_root=raw_ws_root, channel="orderbook", date_value=date_value)
    states: dict[tuple[str, int], OrderbookBarState] = {}
    counters = ParseCounters()

    for part_file in files:
        _aggregate_orderbook_part_file(
            part_file=part_file,
            states=states,
            counters=counters,
            markets=markets,
            topk=topk,
            interval_ms=60_000,
            chunk_rows=chunk_rows,
        )

    return OrderbookAggregateResult(frame=_states_to_frame(states), counters=counters)


def aggregate_orderbook_events_to_1m(events: list[dict[str, Any]], *, topk: int = 5) -> pl.DataFrame:
    states: dict[tuple[str, int], OrderbookBarState] = {}
    for raw in events:
        normalized = normalize_ws_orderbook_row(raw, topk=topk)
        if normalized is None:
            continue
        market = str(normalized["market"])
        ts_ms = int(normalized["event_ts_ms"])
        bar_ts = bar_ts_floor(ts_ms, interval_ms=60_000)
        _update_state(states.setdefault((market, bar_ts), OrderbookBarState()), normalized)
    return _states_to_frame(states)


def _aggregate_orderbook_part_file(
    *,
    part_file: Path,
    states: dict[tuple[str, int], OrderbookBarState],
    counters: ParseCounters,
    markets: set[str] | None,
    topk: int,
    interval_ms: int,
    chunk_rows: int,
) -> None:
    _ = max(int(chunk_rows), 1)
    for raw in iter_jsonl_zst_rows(part_file):
        counters.raw_rows += 1
        normalized = normalize_ws_orderbook_row(raw, topk=topk)
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
        state = states.setdefault((market, bar_ts_ms), OrderbookBarState())
        _update_state(state, normalized)


def _update_state(state: OrderbookBarState, row: dict[str, Any]) -> None:
    ts_ms = int(row["event_ts_ms"])
    mid = float(row["mid"])
    spread_bps = float(row["spread_bps"])
    depth_bid = float(row["depth_bid_topk"])
    depth_ask = float(row["depth_ask_topk"])
    imbalance = float(row["imbalance_topk"])
    microprice_bias = row.get("microprice_bias_bps")

    state.book_events += 1
    state.sum_mid += mid
    state.sum_spread_bps += spread_bps
    state.sum_depth_bid += depth_bid
    state.sum_depth_ask += depth_ask
    state.sum_imbalance += imbalance

    if microprice_bias is not None:
        state.sum_microprice_bias_bps += float(microprice_bias)
        state.microprice_bias_count += 1

    if state.min_ts_ms is None or ts_ms < state.min_ts_ms:
        state.min_ts_ms = ts_ms
    if state.max_ts_ms is None or ts_ms > state.max_ts_ms:
        state.max_ts_ms = ts_ms


def _states_to_frame(states: dict[tuple[str, int], OrderbookBarState]) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for (market, ts_ms), state in sorted(states.items()):
        count = max(int(state.book_events), 0)
        if count <= 0:
            continue

        rows.append(
            {
                "market": market,
                "ts_ms": int(ts_ms),
                "mid_mean": state.sum_mid / float(count),
                "spread_bps_mean": state.sum_spread_bps / float(count),
                "depth_bid_top5_mean": state.sum_depth_bid / float(count),
                "depth_ask_top5_mean": state.sum_depth_ask / float(count),
                "imbalance_top5_mean": state.sum_imbalance / float(count),
                "microprice_bias_bps_mean": (
                    state.sum_microprice_bias_bps / float(state.microprice_bias_count)
                    if state.microprice_bias_count > 0
                    else None
                ),
                "book_update_count": count,
                "book_events": count,
                "book_min_ts_ms": state.min_ts_ms,
                "book_max_ts_ms": state.max_ts_ms,
            }
        )

    if not rows:
        schema = {
            "market": pl.Utf8,
            "ts_ms": pl.Int64,
            "mid_mean": pl.Float64,
            "spread_bps_mean": pl.Float64,
            "depth_bid_top5_mean": pl.Float64,
            "depth_ask_top5_mean": pl.Float64,
            "imbalance_top5_mean": pl.Float64,
            "microprice_bias_bps_mean": pl.Float64,
            "book_update_count": pl.Int64,
            "book_events": pl.Int64,
            "book_min_ts_ms": pl.Int64,
            "book_max_ts_ms": pl.Int64,
        }
        return pl.DataFrame([], schema=schema, orient="row")

    return pl.DataFrame(rows).sort(["market", "ts_ms"])
