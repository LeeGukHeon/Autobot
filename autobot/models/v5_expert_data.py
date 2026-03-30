"""Shared data-loading helpers for v5 expert trainers."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl

from autobot.features.multitf_join_v1 import bucket_end_timestamp_expr

from autobot.data.collect.sequence_tensor_store import (
    SUPPORT_LEVEL_REDUCED_CONTEXT,
    SUPPORT_LEVEL_STRICT_FULL,
)


def support_level_weight(level: str) -> float:
    normalized = str(level or "").strip().lower()
    if normalized == SUPPORT_LEVEL_STRICT_FULL:
        return 1.0
    if normalized == SUPPORT_LEVEL_REDUCED_CONTEXT:
        return 0.5
    return 0.0


def strict_eval_indices(indices: np.ndarray, support_levels: np.ndarray) -> np.ndarray:
    idx = np.asarray(indices, dtype=np.int64)
    if idx.size <= 0:
        return idx
    support = np.asarray(support_levels, dtype=object)
    strict = idx[np.asarray(support[idx] == SUPPORT_LEVEL_STRICT_FULL, dtype=bool)]
    if strict.size >= max(8, min(32, idx.size // 4 if idx.size > 0 else 0)):
        return strict
    return idx


def load_minute_close_map_sources(*, market: str, roots: tuple[Path, ...]) -> dict[int, float]:
    frames: list[pl.DataFrame] = []
    for root in roots:
        market_root = root / f"market={market}"
        files = sorted(market_root.glob("*.parquet"))
        if not files:
            for date_dir in sorted(market_root.glob("date=*")):
                if not date_dir.is_dir():
                    continue
                files.extend(sorted(path for path in date_dir.glob("*.parquet") if path.is_file()))
        if not files:
            continue
        frame = pl.concat([pl.read_parquet(path) for path in files], how="vertical")
        if "ts_ms" not in frame.columns or "close" not in frame.columns:
            continue
        frame = (
            frame.select(["ts_ms", "close"])
            .with_columns(
                bucket_end_timestamp_expr(pl.col("ts_ms"), interval_ms=60_000).alias("ts_ms")
            )
            .with_row_index("__row_id")
            .sort(["ts_ms", "__row_id"])
            .unique(subset=["ts_ms"], keep="last")
            .sort("ts_ms")
            .drop("__row_id")
        )
        frames.append(frame)
    if not frames:
        return {}
    frame = (
        pl.concat(frames, how="vertical")
        .with_row_index("__row_id")
        .sort(["ts_ms", "__row_id"])
        .unique(subset=["ts_ms"], keep="last")
        .sort("ts_ms")
        .drop("__row_id")
    )
    return {int(row["ts_ms"]): float(row["close"]) for row in frame.iter_rows(named=True)}
