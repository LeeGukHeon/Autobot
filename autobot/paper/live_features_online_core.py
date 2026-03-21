"""Shared online runtime feature-builder helpers for LIVE_V3/LIVE_V4.

This module holds the version-agnostic state containers and pure helper
functions that were previously embedded directly in ``live_features_v3.py``.
It is intentionally limited to helpers with no knowledge of a specific
feature contract so that both the legacy v3 builder and a future native v4
builder can reuse the same online runtime core.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
import math
from pathlib import Path
from typing import Any, Sequence

import polars as pl

from autobot.data import expected_interval_ms
from autobot.features.micro_join import prefixed_micro_columns


@dataclass
class _ActiveMinute:
    minute_start_ts_ms: int
    open: float
    high: float
    low: float
    close: float
    volume_base: float
    trade_events: int


@dataclass(frozen=True)
class _MinuteCandle:
    ts_ms: int
    open: float
    high: float
    low: float
    close: float
    volume_base: float
    is_synth_1m: bool


@dataclass
class _MarketState:
    bootstrap_loaded: bool = False
    bootstrap_status: str = "UNSET"
    bootstrap_rows: int = 0
    candles_1m: deque[_MinuteCandle] | None = None
    active: _ActiveMinute | None = None
    last_price: float | None = None
    last_closed_price: float | None = None
    last_event_ts_ms: int | None = None
    prev_acc_trade_price_24h: float | None = None


def _rollup_from_1m(*, one_m: pl.DataFrame, tf: str) -> pl.DataFrame:
    if one_m.height <= 0:
        return pl.DataFrame(
            schema={
                "ts_ms": pl.Int64,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume_base": pl.Float64,
            }
        )
    interval_ms = int(expected_interval_ms(tf))
    grouped = (
        one_m.sort("ts_ms")
        .with_columns(
            (((pl.col("ts_ms") // interval_ms) * interval_ms) + interval_ms).cast(pl.Int64).alias("__ts_tf")
        )
        .group_by("__ts_tf")
        .agg(
            [
                pl.col("open").first().cast(pl.Float64).alias("open"),
                pl.col("high").max().cast(pl.Float64).alias("high"),
                pl.col("low").min().cast(pl.Float64).alias("low"),
                pl.col("close").last().cast(pl.Float64).alias("close"),
                pl.col("volume_base").sum().cast(pl.Float64).alias("volume_base"),
            ]
        )
        .rename({"__ts_tf": "ts_ms"})
        .sort("ts_ms")
    )
    return grouped


def _rows_to_frame(*, rows: list[dict[str, Any]], feature_columns: Sequence[str], extra_columns: Sequence[str] = ()) -> pl.DataFrame:
    schema: dict[str, pl.DataType] = {"ts_ms": pl.Int64, "market": pl.Utf8}
    feature_col_set = {str(col) for col in feature_columns}
    for col in feature_columns:
        schema[str(col)] = pl.Float32
    for col in extra_columns:
        name = str(col)
        if name == "close" or name in feature_col_set:
            continue
        schema[name] = pl.Float64
    schema["close"] = pl.Float64

    if not rows:
        return pl.DataFrame(schema=schema)
    frame = pl.DataFrame(rows)
    ordered = [
        "ts_ms",
        "market",
        *[str(col) for col in feature_columns],
        *[str(col) for col in extra_columns if str(col) != "close" and str(col) not in feature_col_set],
        "close",
    ]
    present = [name for name in ordered if name in frame.columns]
    return frame.select(present).sort("market")


def _resolve_extra_row_value(*, column: str, base_row: dict[str, Any], aux_row: dict[str, Any], close_value: float) -> float | None:
    name = str(column).strip()
    if not name:
        return None
    if name in aux_row:
        return _safe_float(aux_row.get(name))
    if name in base_row:
        return _safe_float(base_row.get(name))
    if name == "rv_12":
        return _safe_float(base_row.get("vol_12"))
    if name == "rv_36":
        return _safe_float(base_row.get("vol_36"))
    if name == "atr_pct_14":
        atr = _safe_float(aux_row.get("atr_14"))
        if atr is None or close_value <= 0.0:
            return None
        return float(atr) / float(close_value)
    return None


def _attach_runtime_aux_columns(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.height <= 0:
        return frame
    return frame.sort("ts_ms").with_columns(
        [
            pl.max_horizontal(
                [
                    (pl.col("high") - pl.col("low")),
                    (pl.col("high") - pl.col("close").shift(1)).abs(),
                    (pl.col("low") - pl.col("close").shift(1)).abs(),
                ]
            ).alias("__true_range"),
        ]
    ).with_columns(
        [
            pl.col("__true_range").rolling_mean(window_size=14, min_samples=14).alias("atr_14"),
            (pl.col("__true_range").rolling_mean(window_size=14, min_samples=14) / pl.col("close")).alias("atr_pct_14"),
        ]
    ).drop("__true_range")


def _default_micro_feature_values() -> dict[str, float]:
    values: dict[str, float] = {}
    for name in prefixed_micro_columns():
        values[name] = 0.0
    values.update(
        {
            "m_spread_proxy": 0.0,
            "m_trade_volume_base": 0.0,
            "m_trade_buy_ratio": 0.0,
            "m_signed_volume": 0.0,
            "m_source_ws": 0.0,
            "m_source_rest": 0.0,
        }
    )
    return values


def _resolve_dataset_path(*, parquet_root: Path, dataset_name: str) -> Path:
    candidate = Path(str(dataset_name).strip())
    if candidate.exists() or candidate.is_absolute():
        return candidate
    return parquet_root / candidate


def _market_files(*, dataset_root: Path, tf: str, market: str) -> list[Path]:
    market_dir = dataset_root / f"tf={str(tf).strip().lower()}" / f"market={str(market).strip().upper()}"
    if not market_dir.exists():
        return []
    direct = sorted(path for path in market_dir.glob("part-*.parquet") if path.is_file())
    if direct:
        return direct
    legacy = market_dir / "part.parquet"
    if legacy.exists():
        return [legacy]
    nested: list[Path] = []
    for date_dir in sorted(market_dir.glob("date=*")):
        if not date_dir.is_dir():
            continue
        nested.extend(sorted(path for path in date_dir.glob("*.parquet") if path.is_file()))
    if nested:
        return nested
    return sorted(path for path in market_dir.rglob("*.parquet") if path.is_file())


def _estimate_volume_base_from_ticker(
    *,
    trade_price: float,
    acc_trade_price_24h: float,
    prev_acc_trade_price_24h: float | None,
) -> float:
    price = max(float(trade_price), 1e-12)
    current_acc = max(float(acc_trade_price_24h), 0.0)
    if prev_acc_trade_price_24h is None:
        notional_delta = max(price * 1e-8, 1e-8)
    elif current_acc >= float(prev_acc_trade_price_24h):
        notional_delta = max(current_acc - float(prev_acc_trade_price_24h), 0.0)
        if notional_delta <= 0:
            notional_delta = max(price * 1e-8, 1e-8)
    else:
        notional_delta = max(price * 1e-8, 1e-8)
    return max(notional_delta / price, 1e-12)


def _to_feature_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    try:
        casted = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(casted):
        return None
    return casted


def _normalize_markets(markets: Sequence[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in markets:
        market = str(raw).strip().upper()
        if not market or market in seen:
            continue
        seen.add(market)
        out.append(market)
    return tuple(out)


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            return float(text)
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
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


def _collect_lazy(lazy_frame: pl.LazyFrame) -> pl.DataFrame:
    try:
        return lazy_frame.collect(engine="streaming")
    except TypeError:
        return lazy_frame.collect(streaming=True)


__all__ = [
    "_ActiveMinute",
    "_MinuteCandle",
    "_MarketState",
    "_rollup_from_1m",
    "_rows_to_frame",
    "_resolve_extra_row_value",
    "_attach_runtime_aux_columns",
    "_default_micro_feature_values",
    "_resolve_dataset_path",
    "_market_files",
    "_estimate_volume_base_from_ticker",
    "_to_feature_float",
    "_normalize_markets",
    "_safe_float",
    "_safe_int",
    "_collect_lazy",
]
