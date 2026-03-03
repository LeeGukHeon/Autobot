"""Time-based split helpers with embargo guard."""

from __future__ import annotations

from dataclasses import dataclass
import math

import numpy as np


SPLIT_TRAIN = "train"
SPLIT_VALID = "valid"
SPLIT_TEST = "test"
SPLIT_DROP = "drop"


@dataclass(frozen=True)
class TimeSplitInfo:
    valid_start_ts: int
    test_start_ts: int
    embargo_bars: int
    embargo_ms: int
    interval_ms: int
    counts: dict[str, int]


def compute_time_splits(
    ts_ms: np.ndarray,
    *,
    train_ratio: float = 0.70,
    valid_ratio: float = 0.15,
    test_ratio: float = 0.15,
    embargo_bars: int = 12,
    interval_ms: int = 300_000,
) -> tuple[np.ndarray, TimeSplitInfo]:
    if ts_ms.size <= 0:
        raise ValueError("ts_ms must not be empty")

    total = float(train_ratio) + float(valid_ratio) + float(test_ratio)
    if not math.isclose(total, 1.0, rel_tol=1e-6, abs_tol=1e-6):
        raise ValueError("train_ratio + valid_ratio + test_ratio must sum to 1.0")

    unique_ts = np.unique(ts_ms.astype(np.int64, copy=False))
    if unique_ts.size < 3:
        raise ValueError("need at least three unique timestamps to create train/valid/test split")

    valid_start_idx = max(1, int(math.floor(unique_ts.size * float(train_ratio))))
    valid_start_idx = min(valid_start_idx, unique_ts.size - 2)
    test_start_idx = int(math.floor(unique_ts.size * float(train_ratio + valid_ratio)))
    test_start_idx = max(valid_start_idx + 1, test_start_idx)
    test_start_idx = min(test_start_idx, unique_ts.size - 1)

    valid_start_ts = int(unique_ts[valid_start_idx])
    test_start_ts = int(unique_ts[test_start_idx])

    labels = np.full(ts_ms.shape[0], SPLIT_DROP, dtype=object)
    labels[ts_ms < valid_start_ts] = SPLIT_TRAIN
    labels[(ts_ms >= valid_start_ts) & (ts_ms < test_start_ts)] = SPLIT_VALID
    labels[ts_ms >= test_start_ts] = SPLIT_TEST

    embargo_ms = max(int(embargo_bars), 0) * max(int(interval_ms), 1)
    if embargo_ms > 0:
        valid_boundary_drop = np.abs(ts_ms.astype(np.int64, copy=False) - valid_start_ts) <= embargo_ms
        test_boundary_drop = np.abs(ts_ms.astype(np.int64, copy=False) - test_start_ts) <= embargo_ms
        labels[valid_boundary_drop | test_boundary_drop] = SPLIT_DROP

    counts = {
        SPLIT_TRAIN: int(np.sum(labels == SPLIT_TRAIN)),
        SPLIT_VALID: int(np.sum(labels == SPLIT_VALID)),
        SPLIT_TEST: int(np.sum(labels == SPLIT_TEST)),
        SPLIT_DROP: int(np.sum(labels == SPLIT_DROP)),
    }
    info = TimeSplitInfo(
        valid_start_ts=valid_start_ts,
        test_start_ts=test_start_ts,
        embargo_bars=max(int(embargo_bars), 0),
        embargo_ms=embargo_ms,
        interval_ms=max(int(interval_ms), 1),
        counts=counts,
    )
    return labels, info


def split_masks(labels: np.ndarray) -> dict[str, np.ndarray]:
    return {
        SPLIT_TRAIN: labels == SPLIT_TRAIN,
        SPLIT_VALID: labels == SPLIT_VALID,
        SPLIT_TEST: labels == SPLIT_TEST,
        SPLIT_DROP: labels == SPLIT_DROP,
    }
