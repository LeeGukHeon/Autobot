from __future__ import annotations

import numpy as np

from autobot.models.split import (
    SPLIT_DROP,
    SPLIT_TEST,
    SPLIT_TRAIN,
    SPLIT_VALID,
    compute_anchored_walk_forward_splits,
    compute_time_splits,
    split_masks,
)


def test_time_split_with_embargo_drops_boundary_rows() -> None:
    ts_ms = np.array([1_700_000_000_000 + (i * 300_000) for i in range(200)], dtype=np.int64)
    labels, info = compute_time_splits(
        ts_ms,
        train_ratio=0.70,
        valid_ratio=0.15,
        test_ratio=0.15,
        embargo_bars=3,
        interval_ms=300_000,
    )
    masks = split_masks(labels)

    assert int(np.sum(masks[SPLIT_TRAIN])) > 0
    assert int(np.sum(masks[SPLIT_VALID])) > 0
    assert int(np.sum(masks[SPLIT_TEST])) > 0
    assert int(np.sum(masks[SPLIT_DROP])) > 0

    for value in ts_ms[masks[SPLIT_DROP]]:
        near_valid = abs(int(value) - int(info.valid_start_ts)) <= info.embargo_ms
        near_test = abs(int(value) - int(info.test_start_ts)) <= info.embargo_ms
        assert near_valid or near_test


def test_time_split_ratio_validation() -> None:
    ts_ms = np.array([1_700_000_000_000 + (i * 300_000) for i in range(50)], dtype=np.int64)
    try:
        compute_time_splits(ts_ms, train_ratio=0.8, valid_ratio=0.2, test_ratio=0.2)
    except ValueError as exc:
        assert "sum to 1.0" in str(exc)
    else:
        raise AssertionError("expected ValueError for invalid split ratio")


def test_anchored_walk_forward_splits_produce_increasing_test_windows() -> None:
    base = np.array([1_700_000_000_000 + (i * 300_000) for i in range(120)], dtype=np.int64)
    ts_ms = np.repeat(base, 2)
    windows = compute_anchored_walk_forward_splits(
        ts_ms,
        valid_ratio=0.10,
        test_ratio=0.10,
        window_count=3,
        embargo_bars=1,
        interval_ms=300_000,
    )

    assert len(windows) == 3
    previous_test_end = None
    for labels, info in windows:
        masks = split_masks(labels)
        assert int(np.sum(masks[SPLIT_TRAIN])) > 0
        assert int(np.sum(masks[SPLIT_VALID])) > 0
        assert int(np.sum(masks[SPLIT_TEST])) > 0
        if previous_test_end is not None:
            assert int(info.test_start_ts) > int(previous_test_end)
        previous_test_end = int(info.test_end_ts)
