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

    stats = _validate_frame(frame, tf="1m", gap_severity="info", quote_est_severity="info")
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

    stats = _validate_frame(frame, tf="1m", gap_severity="warn", quote_est_severity="info")
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

    stats = _validate_frame(frame, tf="1m", gap_severity="info", quote_est_severity="info")
    assert stats["status"] == "FAIL"
    assert "TS_NULL_FOUND" in stats["status_reasons"]

