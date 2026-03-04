from __future__ import annotations

import polars as pl

from autobot.features.multitf_join_v1 import join_1m_aggregate, join_high_tf_asof


def test_high_tf_asof_join_does_not_use_future_rows() -> None:
    base = pl.DataFrame({"ts_ms": [300_000, 600_000, 900_000], "x": [1.0, 2.0, 3.0]})
    high = pl.DataFrame(
        {
            "ts_ms": [0, 900_000],
            "tf15m_ret_1": [0.1, 0.2],
            "tf15m_ret_3": [0.1, 0.2],
            "tf15m_vol_3": [0.1, 0.2],
            "tf15m_trend_slope": [0.1, 0.2],
            "tf15m_regime_flag": [1, 1],
        }
    )

    joined, stats = join_high_tf_asof(
        base_frame=base,
        high_tf_features=high,
        tf="15m",
        max_staleness_ms=300_000,
    )

    assert joined.get_column("src_ts_15m").to_list() == [0, 0, 900_000]
    assert joined.get_column("tf15m_stale").to_list() == [False, True, False]
    assert stats.rows_stale == 1


def test_1m_join_marks_missing_ratio_and_failures() -> None:
    base = pl.DataFrame({"ts_ms": [300_000, 600_000]})
    one_m = pl.DataFrame(
        {
            "ts_ms": [300_000, 600_000],
            "one_m_count": [5, 3],
            "one_m_last_ts": [300_000, 600_000],
            "one_m_ret_mean": [0.01, 0.02],
            "one_m_ret_std": [0.01, 0.02],
            "one_m_volume_sum": [10.0, 20.0],
            "one_m_range_mean": [0.01, 0.01],
        }
    )

    joined, stats = join_1m_aggregate(
        base_frame=base,
        one_m_agg=one_m,
        required_bars=5,
        max_missing_ratio=0.2,
    )

    assert joined.get_column("one_m_missing_ratio").to_list() == [0.0, 0.4]
    assert joined.get_column("one_m_fail").to_list() == [False, True]
    assert stats.rows_failed == 1
