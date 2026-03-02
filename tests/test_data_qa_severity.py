from __future__ import annotations

import polars as pl

from autobot.data.ingest_csv_to_parquet import IngestOptions, _validate_and_fix_frame, _validate_frame


def test_validate_frame_gap_and_quote_est_are_info_by_default() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [1_700_000_000_000, 1_700_000_120_000],
            "open": [1.0, 2.0],
            "high": [1.1, 2.1],
            "low": [0.9, 1.9],
            "close": [1.0, 2.0],
            "volume_base": [10.0, 11.0],
            "volume_quote": [10.0, 22.0],
            "volume_quote_est": [True, True],
        }
    )

    stats = _validate_frame(
        frame,
        tf="1m",
        gap_severity="info",
        quote_est_severity="info",
        ohlc_violation_policy="drop_row_and_warn",
    )
    assert stats["status"] == "OK"
    assert "GAPS_FOUND" in stats["status_reasons"]
    assert "VOLUME_QUOTE_ESTIMATED" in stats["status_reasons"]


def test_validate_frame_gap_can_be_warn_by_config() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [1_700_000_000_000, 1_700_000_120_000],
            "open": [1.0, 2.0],
            "high": [1.1, 2.1],
            "low": [0.9, 1.9],
            "close": [1.0, 2.0],
            "volume_base": [10.0, 11.0],
            "volume_quote": [10.0, 22.0],
            "volume_quote_est": [False, False],
        }
    )

    stats = _validate_frame(
        frame,
        tf="1m",
        gap_severity="warn",
        quote_est_severity="info",
        ohlc_violation_policy="drop_row_and_warn",
    )
    assert stats["status"] == "WARN"
    assert "GAPS_FOUND" in stats["status_reasons"]


def test_validate_and_fix_warns_on_non_monotonic_and_duplicates_dropped() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [1_700_000_060_000, 1_700_000_000_000, 1_700_000_000_000],
            "open": [1.0, 2.0, 2.1],
            "high": [1.1, 2.1, 2.2],
            "low": [0.9, 1.9, 1.8],
            "close": [1.0, 2.0, 2.1],
            "volume_base": [10.0, 11.0, 12.0],
            "volume_quote": [10.0, 22.0, 23.0],
            "volume_quote_est": [False, False, False],
        }
    )
    options = IngestOptions(
        engine="polars",
        allow_sort_on_non_monotonic=True,
        allow_dedupe_on_duplicate_ts=True,
    )

    _, stats = _validate_and_fix_frame(frame, tf="1m", options=options)
    assert stats["status"] == "WARN"
    assert "NON_MONOTONIC_SORTED" in stats["status_reasons"]
    assert "DUPLICATES_DROPPED" in stats["status_reasons"]


def test_validate_frame_fails_when_ts_has_null() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [1_700_000_000_000, None],
            "open": [1.0, 2.0],
            "high": [1.1, 2.1],
            "low": [0.9, 1.9],
            "close": [1.0, 2.0],
            "volume_base": [10.0, 11.0],
            "volume_quote": [10.0, 22.0],
            "volume_quote_est": [False, False],
        }
    )

    stats = _validate_frame(
        frame,
        tf="1m",
        gap_severity="info",
        quote_est_severity="info",
        ohlc_violation_policy="drop_row_and_warn",
    )
    assert stats["status"] == "FAIL"
    assert "TS_NULL_FOUND" in stats["status_reasons"]


def test_validate_and_fix_drops_ohlc_violation_and_warns() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [1_700_000_000_000, 1_700_000_060_000],
            "open": [100.0, 101.0],
            "high": [101.0, 102.0],
            "low": [99.0, 100.0],
            "close": [102.0, 101.5],  # close > high on first row
            "volume_base": [10.0, 12.0],
            "volume_quote": [1000.0, 1218.0],
            "volume_quote_est": [False, False],
        }
    )
    options = IngestOptions(engine="polars")

    fixed, stats = _validate_and_fix_frame(frame, tf="1m", options=options)
    assert fixed.height == 1
    assert stats["invalid_rows_dropped"] == 1
    assert stats["status"] == "WARN"
    assert "INVALID_ROWS_DROPPED" in stats["status_reasons"]
    assert "OHLC_VIOLATIONS" not in stats["status_reasons"]


def test_validate_frame_ohlc_violation_policy_fail() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [1_700_000_000_000],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [102.0],  # close > high
            "volume_base": [10.0],
            "volume_quote": [1000.0],
            "volume_quote_est": [False],
        }
    )

    stats = _validate_frame(
        frame,
        tf="1m",
        gap_severity="info",
        quote_est_severity="info",
        ohlc_violation_policy="fail",
    )
    assert stats["status"] == "FAIL"
    assert "OHLC_VIOLATIONS" in stats["status_reasons"]
