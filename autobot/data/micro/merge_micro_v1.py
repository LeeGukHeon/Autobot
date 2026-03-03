"""Micro v1 merge/orchestration (trade + orderbook -> 1m/5m)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import time
from typing import Any

import polars as pl

from .orderbook_aggregator_v1 import aggregate_orderbook_ws_bars_1m
from .raw_readers import ParseCounters, parse_date_range
from .resample_v1 import resample_micro_1m_to_5m
from .spec_micro_v1 import write_micro_spec
from .store import (
    append_micro_manifest_rows,
    manifest_path,
    validate_report_path,
    write_json_report,
    write_micro_partitions,
)
from .trade_aggregator_v1 import (
    aggregate_rest_trade_bars_1m,
    aggregate_ws_trade_bars_1m,
    merge_trade_bars_with_precedence,
)
from .validate_micro_v1 import detect_alignment_mode


@dataclass(frozen=True)
class MicroAggregateOptions:
    tf_set: tuple[str, ...] = ("1m", "5m")
    start: str = "2026-03-03"
    end: str = "2026-03-03"
    quote: str = "KRW"
    top_n: int = 20
    fixed_markets: tuple[str, ...] | None = None
    raw_ticks_root: Path = Path("data/raw_ticks/upbit/trades")
    raw_ws_root: Path = Path("data/raw_ws/upbit/quotation")
    out_root: Path = Path("data/parquet/micro_v1")
    base_candles_root: Path = Path("data/parquet/candles_v1")
    mode: str = "append"
    chunk_rows: int = 200_000
    topk: int = 5
    alignment_mode: str = "auto"
    sample_market: str = "KRW-BTC"

    @property
    def aggregate_report_file(self) -> Path:
        return self.out_root / "_meta" / "aggregate_report.json"


@dataclass(frozen=True)
class MicroAggregateSummary:
    run_id: str
    started_at: int
    finished_at: int
    tf_set: tuple[str, ...]
    dates: tuple[str, ...]
    markets: tuple[str, ...]
    alignment_mode: str
    rows_written_total: int
    parse_ok_ratio: float
    manifest_file: Path
    aggregate_report_file: Path
    details: tuple[dict[str, Any], ...]


def aggregate_micro_v1(options: MicroAggregateOptions) -> MicroAggregateSummary:
    started_at = int(time.time())
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    tf_set = _normalize_tf_set(options.tf_set)
    date_values = parse_date_range(start=options.start, end=options.end)
    markets = _select_markets(options=options, date_values=date_values)

    parse_counters = ParseCounters()
    details: list[dict[str, Any]] = []
    manifest_rows: list[dict[str, Any]] = []

    requested_alignment = str(options.alignment_mode).strip().lower() or "auto"
    if requested_alignment not in {"auto", "start", "end"}:
        raise ValueError("alignment_mode must be one of: auto,start,end")

    resolved_alignment = "start" if requested_alignment == "auto" else requested_alignment
    alignment_detail: dict[str, Any] | None = None

    for date_value in date_values:
        ws_trade = aggregate_ws_trade_bars_1m(
            raw_ws_root=options.raw_ws_root,
            date_value=date_value,
            markets=set(markets),
            chunk_rows=options.chunk_rows,
        )
        rest_trade = aggregate_rest_trade_bars_1m(
            raw_ticks_root=options.raw_ticks_root,
            date_value=date_value,
            markets=set(markets),
            chunk_rows=options.chunk_rows,
        )
        orderbook = aggregate_orderbook_ws_bars_1m(
            raw_ws_root=options.raw_ws_root,
            date_value=date_value,
            markets=set(markets),
            topk=max(int(options.topk), 1),
            chunk_rows=options.chunk_rows,
        )

        parse_counters.add(ws_trade.counters)
        parse_counters.add(rest_trade.counters)
        parse_counters.add(orderbook.counters)

        trade_merged = merge_trade_bars_with_precedence(ws_frame=ws_trade.frame, rest_frame=rest_trade.frame)
        micro_1m = merge_trade_and_orderbook_bars(
            trade_frame=trade_merged,
            orderbook_frame=orderbook.frame,
            tf="1m",
        )

        if requested_alignment == "auto" and alignment_detail is None and micro_1m.height > 0:
            detected = detect_alignment_mode(
                micro_frame=micro_1m,
                base_candles_root=options.base_candles_root,
                sample_market=options.sample_market,
                sample_date=date_value,
                interval_ms=60_000,
            )
            alignment_detail = detected
            resolved_alignment = str(detected.get("mode") or "start")

        shifted_1m = apply_alignment_mode(micro_1m, mode=resolved_alignment, interval_ms=60_000)

        rows_1m = 0
        rows_5m = 0
        if "1m" in tf_set and shifted_1m.height > 0:
            rows = write_micro_partitions(
                frame=shifted_1m,
                out_root=options.out_root,
                tf="1m",
                run_id=run_id,
                mode=options.mode,
            )
            manifest_rows.extend(rows)
            rows_1m = int(sum(int(item.get("rows") or 0) for item in rows))

        if "5m" in tf_set and shifted_1m.height > 0:
            frame_5m = resample_micro_1m_to_5m(shifted_1m)
            rows = write_micro_partitions(
                frame=frame_5m,
                out_root=options.out_root,
                tf="5m",
                run_id=run_id,
                mode=options.mode,
            )
            manifest_rows.extend(rows)
            rows_5m = int(sum(int(item.get("rows") or 0) for item in rows))

        details.append(
            {
                "date": date_value,
                "markets": len(markets),
                "ws_trade_rows": int(ws_trade.frame.height),
                "rest_trade_rows": int(rest_trade.frame.height),
                "orderbook_rows": int(orderbook.frame.height),
                "micro_1m_rows": int(shifted_1m.height),
                "written_1m_rows": rows_1m,
                "written_5m_rows": rows_5m,
                "parse": {
                    "ws_trade": ws_trade.counters.to_dict(),
                    "rest_trade": rest_trade.counters.to_dict(),
                    "orderbook": orderbook.counters.to_dict(),
                },
            }
        )

    write_micro_spec(options.out_root / "_meta" / "spec.json", topk=max(int(options.topk), 1))
    append_micro_manifest_rows(manifest_path(options.out_root), manifest_rows)

    rows_written_total = int(sum(int(item.get("rows") or 0) for item in manifest_rows))
    source_ws_rows = int(sum(int(item.get("trade_source_ws_rows") or 0) for item in manifest_rows))
    source_rest_rows = int(sum(int(item.get("trade_source_rest_rows") or 0) for item in manifest_rows))
    source_none_rows = int(sum(int(item.get("trade_source_none_rows") or 0) for item in manifest_rows))
    micro_available_rows = int(sum(int(item.get("micro_available_rows") or 0) for item in manifest_rows))

    aggregate_report = {
        "started_at": started_at,
        "finished_at": int(time.time()),
        "run_id": run_id,
        "tf_set": list(tf_set),
        "start": options.start,
        "end": options.end,
        "dates": list(date_values),
        "quote": options.quote,
        "top_n": int(options.top_n),
        "markets": list(markets),
        "raw_ticks_root": str(options.raw_ticks_root),
        "raw_ws_root": str(options.raw_ws_root),
        "out_root": str(options.out_root),
        "base_candles_root": str(options.base_candles_root),
        "mode": options.mode,
        "chunk_rows": int(options.chunk_rows),
        "topk": int(options.topk),
        "alignment_mode_requested": requested_alignment,
        "alignment_mode": resolved_alignment,
        "alignment_detection": alignment_detail,
        "rows_written_total": rows_written_total,
        "manifest_parts_written": len(manifest_rows),
        "micro_available_rows": micro_available_rows,
        "micro_available_ratio": (
            float(micro_available_rows) / float(rows_written_total) if rows_written_total > 0 else 0.0
        ),
        "trade_source_ratio": {
            "ws": (float(source_ws_rows) / float(rows_written_total) if rows_written_total > 0 else 0.0),
            "rest": (float(source_rest_rows) / float(rows_written_total) if rows_written_total > 0 else 0.0),
            "none": (float(source_none_rows) / float(rows_written_total) if rows_written_total > 0 else 0.0),
        },
        "parse": parse_counters.to_dict(),
        "parse_ok_ratio": round(parse_counters.parse_ok_ratio, 8),
        "manifest_file": str(manifest_path(options.out_root)),
        "validate_report_file": str(validate_report_path(options.out_root)),
        "details": details,
    }
    write_json_report(options.aggregate_report_file, aggregate_report)

    return MicroAggregateSummary(
        run_id=run_id,
        started_at=started_at,
        finished_at=int(aggregate_report["finished_at"]),
        tf_set=tf_set,
        dates=date_values,
        markets=markets,
        alignment_mode=resolved_alignment,
        rows_written_total=rows_written_total,
        parse_ok_ratio=float(parse_counters.parse_ok_ratio),
        manifest_file=manifest_path(options.out_root),
        aggregate_report_file=options.aggregate_report_file,
        details=tuple(details),
    )


def merge_trade_and_orderbook_bars(
    *,
    trade_frame: pl.DataFrame,
    orderbook_frame: pl.DataFrame,
    tf: str,
) -> pl.DataFrame:
    trade_map = _frame_to_map(trade_frame)
    book_map = _frame_to_map(orderbook_frame)

    keys = sorted(set(trade_map) | set(book_map))
    rows: list[dict[str, Any]] = []
    for market, ts_ms in keys:
        trade_row = trade_map.get((market, ts_ms), {})
        book_row = book_map.get((market, ts_ms), {})

        trade_events = int(trade_row.get("trade_events") or 0)
        book_events = int(book_row.get("book_events") or 0)
        trade_min_ts = _to_int(trade_row.get("trade_min_ts_ms"))
        trade_max_ts = _to_int(trade_row.get("trade_max_ts_ms"))
        book_min_ts = _to_int(book_row.get("book_min_ts_ms"))
        book_max_ts = _to_int(book_row.get("book_max_ts_ms"))

        row = {
            "market": market,
            "tf": str(tf),
            "ts_ms": int(ts_ms),
            "trade_source": str(trade_row.get("trade_source") or "none"),
            "trade_events": trade_events,
            "book_events": book_events,
            "trade_min_ts_ms": trade_min_ts,
            "trade_max_ts_ms": trade_max_ts,
            "book_min_ts_ms": book_min_ts,
            "book_max_ts_ms": book_max_ts,
            "trade_coverage_ms": _coverage_ms(trade_min_ts, trade_max_ts),
            "book_coverage_ms": _coverage_ms(book_min_ts, book_max_ts),
            "micro_trade_available": bool(trade_events > 0),
            "micro_book_available": bool(book_events > 0),
            "micro_available": bool(trade_events > 0 or book_events > 0),
            "trade_count": int(trade_row.get("trade_count") or 0),
            "buy_count": int(trade_row.get("buy_count") or 0),
            "sell_count": int(trade_row.get("sell_count") or 0),
            "trade_volume_total": float(trade_row.get("trade_volume_total") or 0.0),
            "buy_volume": float(trade_row.get("buy_volume") or 0.0),
            "sell_volume": float(trade_row.get("sell_volume") or 0.0),
            "trade_imbalance": float(trade_row.get("trade_imbalance") or 0.0),
            "vwap": _to_float_or_none(trade_row.get("vwap")),
            "avg_trade_size": _to_float_or_none(trade_row.get("avg_trade_size")),
            "max_trade_size": _to_float_or_none(trade_row.get("max_trade_size")),
            "last_trade_price": _to_float_or_none(trade_row.get("last_trade_price")),
            "mid_mean": _to_float_or_none(book_row.get("mid_mean")),
            "spread_bps_mean": _to_float_or_none(book_row.get("spread_bps_mean")),
            "depth_bid_top5_mean": _to_float_or_none(book_row.get("depth_bid_top5_mean")),
            "depth_ask_top5_mean": _to_float_or_none(book_row.get("depth_ask_top5_mean")),
            "imbalance_top5_mean": _to_float_or_none(book_row.get("imbalance_top5_mean")),
            "microprice_bias_bps_mean": _to_float_or_none(book_row.get("microprice_bias_bps_mean")),
            "book_update_count": int(book_row.get("book_update_count") or 0),
        }
        rows.append(row)

    if not rows:
        return pl.DataFrame([], schema=_micro_schema(), orient="row")

    return pl.DataFrame(rows).sort(["market", "ts_ms"])


def apply_alignment_mode(frame: pl.DataFrame, *, mode: str, interval_ms: int) -> pl.DataFrame:
    if frame.height <= 0:
        return frame
    mode_value = str(mode).strip().lower()
    if mode_value == "end":
        return frame.with_columns((pl.col("ts_ms") + int(interval_ms)).cast(pl.Int64).alias("ts_ms")).sort(
            ["market", "ts_ms"]
        )
    return frame.sort(["market", "ts_ms"])


def _normalize_tf_set(tf_set: tuple[str, ...]) -> tuple[str, ...]:
    values: list[str] = []
    seen: set[str] = set()
    for raw in tf_set:
        tf = str(raw).strip().lower()
        if tf not in {"1m", "5m"} or tf in seen:
            continue
        seen.add(tf)
        values.append(tf)
    if not values:
        return ("1m", "5m")
    return tuple(values)


def _select_markets(options: MicroAggregateOptions, *, date_values: tuple[str, ...]) -> tuple[str, ...]:
    if options.fixed_markets:
        selected = [str(item).strip().upper() for item in options.fixed_markets if str(item).strip()]
        selected = [item for item in selected if item.startswith(f"{options.quote}-")]
        return tuple(selected)

    ws_plan_codes = _load_ws_plan_codes(options.raw_ws_root.parent / "_meta" / "ws_public_plan.json")
    tick_markets = _discover_tick_markets(raw_ticks_root=options.raw_ticks_root, date_values=date_values)

    ordered: list[str] = []
    seen: set[str] = set()
    for market in ws_plan_codes + tick_markets:
        market_value = str(market).strip().upper()
        if not market_value.startswith(f"{options.quote}-"):
            continue
        if market_value in seen:
            continue
        seen.add(market_value)
        ordered.append(market_value)

    if options.top_n > 0:
        ordered = ordered[: max(int(options.top_n), 1)]
    return tuple(ordered)


def _load_ws_plan_codes(path: Path) -> list[str]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(payload, dict):
        return []
    codes = payload.get("codes") or payload.get("selected_markets")
    if not isinstance(codes, list):
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for item in codes:
        market = str(item).strip().upper()
        if not market or market in seen:
            continue
        seen.add(market)
        normalized.append(market)
    return normalized


def _discover_tick_markets(*, raw_ticks_root: Path, date_values: tuple[str, ...]) -> list[str]:
    markets: list[str] = []
    seen: set[str] = set()
    for date_value in date_values:
        date_dir = raw_ticks_root / f"date={date_value}"
        if not date_dir.exists():
            continue
        for market_dir in sorted(date_dir.glob("market=*")):
            if not market_dir.is_dir():
                continue
            market = market_dir.name.replace("market=", "", 1).strip().upper()
            if not market or market in seen:
                continue
            seen.add(market)
            markets.append(market)
    return markets


def _frame_to_map(frame: pl.DataFrame) -> dict[tuple[str, int], dict[str, Any]]:
    if frame.height <= 0:
        return {}
    mapping: dict[tuple[str, int], dict[str, Any]] = {}
    for row in frame.iter_rows(named=True):
        mapping[(str(row["market"]), int(row["ts_ms"]))] = dict(row)
    return mapping


def _coverage_ms(min_ts: int | None, max_ts: int | None) -> int:
    if min_ts is None or max_ts is None:
        return 0
    if int(max_ts) < int(min_ts):
        return 0
    return int(max_ts) - int(min_ts)


def _to_float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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


def _micro_schema() -> dict[str, pl.DataType]:
    return {
        "market": pl.Utf8,
        "tf": pl.Utf8,
        "ts_ms": pl.Int64,
        "trade_source": pl.Utf8,
        "trade_events": pl.Int64,
        "book_events": pl.Int64,
        "trade_min_ts_ms": pl.Int64,
        "trade_max_ts_ms": pl.Int64,
        "book_min_ts_ms": pl.Int64,
        "book_max_ts_ms": pl.Int64,
        "trade_coverage_ms": pl.Int64,
        "book_coverage_ms": pl.Int64,
        "micro_trade_available": pl.Boolean,
        "micro_book_available": pl.Boolean,
        "micro_available": pl.Boolean,
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
        "mid_mean": pl.Float64,
        "spread_bps_mean": pl.Float64,
        "depth_bid_top5_mean": pl.Float64,
        "depth_ask_top5_mean": pl.Float64,
        "imbalance_top5_mean": pl.Float64,
        "microprice_bias_bps_mean": pl.Float64,
        "book_update_count": pl.Int64,
    }
