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


@dataclass(frozen=True)
class AnchoredWalkForwardInfo:
    window_index: int
    valid_start_ts: int
    test_start_ts: int
    test_end_ts: int
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


def compute_anchored_walk_forward_splits(
    ts_ms: np.ndarray,
    *,
    valid_ratio: float = 0.15,
    test_ratio: float = 0.15,
    window_count: int = 3,
    embargo_bars: int = 12,
    interval_ms: int = 300_000,
) -> list[tuple[np.ndarray, AnchoredWalkForwardInfo]]:
    if ts_ms.size <= 0:
        raise ValueError("ts_ms must not be empty")
    if int(window_count) <= 0:
        raise ValueError("window_count must be >= 1")

    valid_value = float(valid_ratio)
    test_value = float(test_ratio)
    if not 0.0 < valid_value < 1.0:
        raise ValueError("valid_ratio must be between 0 and 1")
    if not 0.0 < test_value < 1.0:
        raise ValueError("test_ratio must be between 0 and 1")

    unique_ts = np.unique(ts_ms.astype(np.int64, copy=False))
    if unique_ts.size < 5:
        raise ValueError("need at least five unique timestamps to create walk-forward splits")

    valid_len = max(int(math.floor(unique_ts.size * valid_value)), 1)
    test_len = max(int(math.floor(unique_ts.size * test_value)), 1)
    max_windows = max((unique_ts.size - valid_len - 1) // max(test_len, 1), 0)
    effective_windows = min(max(int(window_count), 1), max_windows)
    if effective_windows <= 0:
        raise ValueError("not enough unique timestamps to create requested walk-forward windows")

    initial_train_len = unique_ts.size - valid_len - (effective_windows * test_len)
    if initial_train_len <= 0:
        raise ValueError("not enough timestamps left for initial train segment in walk-forward split")

    embargo_ms = max(int(embargo_bars), 0) * max(int(interval_ms), 1)
    windows: list[tuple[np.ndarray, AnchoredWalkForwardInfo]] = []
    ts_values = ts_ms.astype(np.int64, copy=False)

    for window_index in range(effective_windows):
        valid_start_idx = initial_train_len + (window_index * test_len)
        test_start_idx = valid_start_idx + valid_len
        test_end_excl = min(test_start_idx + test_len, unique_ts.size)
        if test_start_idx >= unique_ts.size or test_end_excl <= test_start_idx:
            continue

        valid_start_ts = int(unique_ts[valid_start_idx])
        test_start_ts = int(unique_ts[test_start_idx])
        test_end_ts = int(unique_ts[test_end_excl - 1])

        labels = np.full(ts_ms.shape[0], SPLIT_DROP, dtype=object)
        labels[ts_values < valid_start_ts] = SPLIT_TRAIN
        labels[(ts_values >= valid_start_ts) & (ts_values < test_start_ts)] = SPLIT_VALID
        labels[(ts_values >= test_start_ts) & (ts_values <= test_end_ts)] = SPLIT_TEST

        if embargo_ms > 0:
            valid_boundary_drop = np.abs(ts_values - valid_start_ts) <= embargo_ms
            test_boundary_drop = np.abs(ts_values - test_start_ts) <= embargo_ms
            labels[valid_boundary_drop | test_boundary_drop] = SPLIT_DROP

        counts = {
            SPLIT_TRAIN: int(np.sum(labels == SPLIT_TRAIN)),
            SPLIT_VALID: int(np.sum(labels == SPLIT_VALID)),
            SPLIT_TEST: int(np.sum(labels == SPLIT_TEST)),
            SPLIT_DROP: int(np.sum(labels == SPLIT_DROP)),
        }
        info = AnchoredWalkForwardInfo(
            window_index=window_index,
            valid_start_ts=valid_start_ts,
            test_start_ts=test_start_ts,
            test_end_ts=test_end_ts,
            embargo_bars=max(int(embargo_bars), 0),
            embargo_ms=embargo_ms,
            interval_ms=max(int(interval_ms), 1),
            counts=counts,
        )
        windows.append((labels, info))
    if not windows:
        raise ValueError("walk-forward builder did not produce any usable windows")
    return windows


def split_masks(labels: np.ndarray) -> dict[str, np.ndarray]:
    return {
        SPLIT_TRAIN: labels == SPLIT_TRAIN,
        SPLIT_VALID: labels == SPLIT_VALID,
        SPLIT_TEST: labels == SPLIT_TEST,
        SPLIT_DROP: labels == SPLIT_DROP,
    }
