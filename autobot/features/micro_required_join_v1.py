"""Mandatory micro join helpers for FeatureSet v3."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import polars as pl

from autobot.data.micro.resample_v1 import resample_micro_1m_to_5m

from .micro_join import (
    MicroJoinStats,
    join_market_micro,
    load_market_micro_frame,
    resolve_dataset_path,
)


MIN_REQUIRED_MICRO_COLUMNS: tuple[str, ...] = (
    "trade_events",
    "book_events",
    "trade_coverage_ms",
    "book_coverage_ms",
    "trade_volume_total",
    "spread_bps_mean",
)


@dataclass(frozen=True)
class MicroRequiredJoinResult:
    frame: pl.DataFrame
    join_stats: MicroJoinStats
    rows_before: int
    rows_after: int
    rows_dropped_no_micro: int
    micro_tf_used: str


def resolve_micro_dataset_root(*, dataset: str | Path, parquet_root: Path) -> Path:
    return resolve_dataset_path(dataset=dataset, parquet_root=parquet_root)


def load_market_micro_for_base(
    *,
    micro_root: Path,
    market: str,
    base_tf: str,
    from_ts_ms: int,
    to_ts_ms: int,
) -> tuple[pl.DataFrame, str]:
    base_tf_value = str(base_tf).strip().lower()
    direct = load_market_micro_frame(
        micro_root=micro_root,
        tf=base_tf_value,
        market=market,
        from_ts_ms=from_ts_ms,
        to_ts_ms=to_ts_ms,
    )
    if direct.height > 0:
        _require_columns(direct.columns, MIN_REQUIRED_MICRO_COLUMNS)
        return direct, base_tf_value

    if base_tf_value != "5m":
        _require_columns(direct.columns, MIN_REQUIRED_MICRO_COLUMNS)
        return direct, base_tf_value

    one_m = load_market_micro_frame(
        micro_root=micro_root,
        tf="1m",
        market=market,
        from_ts_ms=from_ts_ms - 300_000,
        to_ts_ms=to_ts_ms,
    )
    if one_m.height <= 0:
        _require_columns(one_m.columns, MIN_REQUIRED_MICRO_COLUMNS)
        return one_m, "1m_resampled"

    resampled = resample_micro_1m_to_5m(one_m)
    if resampled.height > 0:
        resampled = resampled.filter((pl.col("ts_ms") >= int(from_ts_ms)) & (pl.col("ts_ms") <= int(to_ts_ms)))
    _require_columns(resampled.columns, MIN_REQUIRED_MICRO_COLUMNS)
    return resampled, "1m_resampled"


def join_micro_required(
    *,
    base_frame: pl.DataFrame,
    micro_frame: pl.DataFrame,
    micro_tf_used: str,
) -> MicroRequiredJoinResult:
    rows_before = int(base_frame.height)
    if rows_before <= 0:
        joined, stats = join_market_micro(base_frame=base_frame, micro_frame=micro_frame)
        return MicroRequiredJoinResult(
            frame=joined,
            join_stats=stats,
            rows_before=0,
            rows_after=0,
            rows_dropped_no_micro=0,
            micro_tf_used=micro_tf_used,
        )

    joined, stats = join_market_micro(base_frame=base_frame, micro_frame=micro_frame)
    source_ts = (
        micro_frame.select([pl.col("ts_ms").cast(pl.Int64).alias("ts_ms"), pl.col("ts_ms").cast(pl.Int64).alias("src_ts_micro")])
        .unique(subset=["ts_ms"], keep="last", maintain_order=True)
        .sort("ts_ms")
        if micro_frame.height > 0 and "ts_ms" in micro_frame.columns
        else pl.DataFrame(schema={"ts_ms": pl.Int64, "src_ts_micro": pl.Int64})
    )
    joined = joined.join(source_ts, on="ts_ms", how="left")
    joined = joined.with_columns(
        [
            pl.col("src_ts_micro").cast(pl.Int64).alias("src_ts_micro"),
            pl.col("m_spread_bps_mean").cast(pl.Float64).alias("m_spread_proxy"),
            pl.col("m_trade_volume_total").cast(pl.Float64).alias("m_trade_volume_base"),
            pl.when(pl.col("m_trade_volume_total") > 0.0)
            .then(pl.col("m_buy_volume") / pl.col("m_trade_volume_total"))
            .otherwise(None)
            .cast(pl.Float64)
            .alias("m_trade_buy_ratio"),
            (pl.col("m_buy_volume") - pl.col("m_sell_volume")).cast(pl.Float64).alias("m_signed_volume"),
            (pl.col("m_trade_source").cast(pl.Utf8).str.to_lowercase() == "ws").cast(pl.Int8).alias("m_source_ws"),
            (pl.col("m_trade_source").cast(pl.Utf8).str.to_lowercase() == "rest").cast(pl.Int8).alias("m_source_rest"),
        ]
    )
    joined = joined.filter(pl.col("m_micro_available") == True)  # noqa: E712
    rows_after = int(joined.height)
    rows_dropped = max(rows_before - rows_after, 0)
    return MicroRequiredJoinResult(
        frame=joined,
        join_stats=stats,
        rows_before=rows_before,
        rows_after=rows_after,
        rows_dropped_no_micro=rows_dropped,
        micro_tf_used=micro_tf_used,
    )


def _require_columns(columns: Iterable[str], required: Iterable[str]) -> None:
    names = set(str(item) for item in columns)
    missing = [name for name in required if name not in names]
    if missing:
        raise ValueError(f"micro dataset missing required columns: {missing}")
