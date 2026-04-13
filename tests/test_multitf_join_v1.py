from __future__ import annotations

import polars as pl

from autobot.features.multitf_join_v1 import (
    aggregate_1m_for_base,
    densify_1m_candles,
    effective_one_m_required_bars,
    join_1m_aggregate,
    join_high_tf_asof,
)


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


def test_densify_1m_fills_gaps_with_prev_close() -> None:
    one_m = pl.DataFrame(
        {
            "ts_ms": [60_000, 180_000],
            "open": [100.0, 103.0],
            "high": [101.0, 104.0],
            "low": [99.0, 102.0],
            "close": [100.0, 103.0],
            "volume_base": [5.0, 7.0],
        }
    )
    dense = densify_1m_candles(one_m, start_ts_ms=60_000, end_ts_ms=180_000)

    assert dense.get_column("ts_ms").to_list() == [60_000, 120_000, 180_000]
    assert dense.get_column("is_synth_1m").to_list() == [False, True, False]
    assert dense.get_column("close").to_list() == [100.0, 100.0, 103.0]
    assert dense.get_column("volume_base").to_list() == [5.0, 0.0, 7.0]


def test_aggregate_1m_for_base_keeps_boundary_minute_in_current_bar() -> None:
    one_m = pl.DataFrame(
        {
            "ts_ms": [60_000, 120_000, 180_000, 240_000, 300_000, 360_000],
            "open": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            "high": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            "low": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            "close": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            "volume_base": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
            "is_synth_1m": [False, False, False, False, False, False],
        }
    )

    grouped = aggregate_1m_for_base(one_m, base_tf="5m", float_dtype="float64")

    assert grouped.get_column("ts_ms").to_list() == [300_000, 600_000]
    assert grouped.get_column("one_m_count").to_list() == [5, 1]
    assert grouped.get_column("one_m_last_ts").to_list() == [300_000, 360_000]
    assert grouped.get_column("one_m_volume_sum").to_list() == [150.0, 60.0]
    assert grouped.get_column("one_m_ret_std").to_list()[1] == 0.0


def test_1m_join_can_drop_windows_with_no_real_1m() -> None:
    base = pl.DataFrame({"ts_ms": [300_000]})
    one_m = pl.DataFrame(
        {
            "ts_ms": [300_000],
            "one_m_count": [5],
            "one_m_last_ts": [300_000],
            "one_m_ret_mean": [0.0],
            "one_m_ret_std": [0.0],
            "one_m_volume_sum": [0.0],
            "one_m_range_mean": [0.0],
            "one_m_synth_count": [5],
            "one_m_real_count": [0],
            "one_m_real_volume_sum": [0.0],
        }
    )

    joined, stats = join_1m_aggregate(
        base_frame=base,
        one_m_agg=one_m,
        required_bars=5,
        max_missing_ratio=0.2,
        drop_if_real_count_zero=True,
    )

    assert joined.get_column("one_m_no_real").to_list() == [True]
    assert joined.get_column("one_m_fail").to_list() == [True]
    assert stats.rows_no_real == 1


def test_effective_one_m_required_bars_collapses_to_one_for_one_minute_base_tf() -> None:
    assert effective_one_m_required_bars(base_tf="1m", required_bars=5) == 1
    assert effective_one_m_required_bars(base_tf="5m", required_bars=5) == 5
