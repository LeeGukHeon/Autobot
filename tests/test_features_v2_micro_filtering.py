from __future__ import annotations

import polars as pl

from autobot.features.feature_set_v2 import MicroFilterPolicy, apply_micro_filter


def test_micro_filter_keeps_only_rows_meeting_thresholds() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [0, 1, 2, 3],
            "m_micro_available": [True, True, False, True],
            "m_trade_events": [2, 0, 3, 1],
            "m_book_events": [2, 1, 4, 0],
            "m_trade_coverage_ms": [70_000, 10_000, 80_000, 70_000],
            "m_book_coverage_ms": [70_000, 80_000, 90_000, 10_000],
        }
    )
    policy = MicroFilterPolicy(
        require_micro_available=True,
        min_trade_events=1,
        min_trade_coverage_ms=60_000,
        min_book_events=1,
        min_book_coverage_ms=60_000,
    )

    filtered = apply_micro_filter(frame, policy=policy)
    assert filtered.height == 1
    assert filtered.item(0, "ts_ms") == 0
