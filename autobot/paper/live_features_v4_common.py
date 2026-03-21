"""Shared helpers for LIVE_V4 and LIVE_V4_NATIVE runtime builders."""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from typing import Sequence

import polars as pl

from .live_features_online_core import _market_files


def project_requested_v4_columns(
    *,
    frame: pl.DataFrame,
    feature_columns: Sequence[str],
    extra_columns: Sequence[str] = (),
) -> tuple[pl.DataFrame, tuple[str, ...]]:
    feature_col_set = {str(col) for col in feature_columns}
    required_order = [
        "ts_ms",
        "market",
        *[str(col) for col in extra_columns if str(col) != "close" and str(col) not in feature_col_set],
        "close",
        *list(feature_columns),
    ]
    working = frame
    missing_columns: list[str] = []
    if "ts_ms" not in working.columns:
        working = working.with_columns(pl.lit(0, dtype=pl.Int64).alias("ts_ms"))
    if "market" not in working.columns:
        working = working.with_columns(pl.lit("", dtype=pl.Utf8).alias("market"))
    if "close" not in working.columns:
        working = working.with_columns(pl.lit(0.0, dtype=pl.Float32).alias("close"))
    for name in extra_columns:
        col_name = str(name).strip()
        if not col_name or col_name == "close":
            continue
        if col_name not in working.columns:
            working = working.with_columns(pl.lit(None, dtype=pl.Float64).alias(col_name))
    for name in feature_columns:
        if name in working.columns:
            continue
        missing_columns.append(str(name))
    if missing_columns:
        schema: dict[str, pl.DataType] = {}
        for name in required_order:
            if name in working.columns:
                schema[name] = working.schema[name]
            elif name == "ts_ms":
                schema[name] = pl.Int64
            elif name == "market":
                schema[name] = pl.Utf8
            else:
                schema[name] = pl.Float32
        return pl.DataFrame(schema=schema), tuple(missing_columns)
    return working.select(required_order), tuple()


def resolve_ctrend_history_roots(*, parquet_root: Path, primary_root: Path) -> tuple[Path, ...]:
    roots: list[Path] = []
    fallback = parquet_root / "candles_v1"
    if fallback.exists():
        roots.append(fallback)
    if primary_root not in roots:
        roots.append(primary_root)
    return tuple(roots)


def load_market_candles_merged(
    *,
    roots: Sequence[Path],
    market: str,
    tf: str,
    start_ts_ms: int,
    end_ts_ms: int,
) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    for root in roots:
        if not root.exists():
            continue
        frame = load_market_candles_window(
            dataset_root=root,
            market=market,
            tf=tf,
            start_ts_ms=start_ts_ms,
            end_ts_ms=end_ts_ms,
        )
        if frame.height > 0:
            frames.append(frame)
    if not frames:
        return pl.DataFrame(
            schema={
                "ts_ms": pl.Int64,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume_base": pl.Float64,
                "market": pl.Utf8,
            }
        )
    merged = pl.concat(frames, how="vertical_relaxed").sort("ts_ms")
    return merged.unique(subset=["ts_ms"], keep="last", maintain_order=True)


def load_market_candles_window(
    *,
    dataset_root: Path,
    market: str,
    tf: str,
    start_ts_ms: int,
    end_ts_ms: int,
) -> pl.DataFrame:
    files = _market_files(dataset_root=dataset_root, tf=tf, market=market)
    if not files:
        return pl.DataFrame()
    try:
        return (
            pl.scan_parquet([str(path) for path in files])
            .select(
                [
                    pl.col("ts_ms").cast(pl.Int64).alias("ts_ms"),
                    pl.col("open").cast(pl.Float64).alias("open"),
                    pl.col("high").cast(pl.Float64).alias("high"),
                    pl.col("low").cast(pl.Float64).alias("low"),
                    pl.col("close").cast(pl.Float64).alias("close"),
                    pl.col("volume_base").cast(pl.Float64).alias("volume_base"),
                ]
            )
            .filter((pl.col("ts_ms") >= int(start_ts_ms)) & (pl.col("ts_ms") < int(end_ts_ms)))
            .sort("ts_ms")
            .collect()
            .with_columns(pl.lit(str(market).strip().upper(), dtype=pl.Utf8).alias("market"))
        )
    except Exception:
        return pl.DataFrame()


def utc_day_start_ts_ms(day: date) -> int:
    return int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp() * 1000)
