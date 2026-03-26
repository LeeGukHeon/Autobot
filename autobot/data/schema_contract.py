"""Candle data contract v1."""

from __future__ import annotations

import polars as pl


EXPECTED_INTERVAL_MS: dict[str, int] = {
    "1s": 1_000,
    "1m": 60_000,
    "5m": 300_000,
    "15m": 900_000,
    "60m": 3_600_000,
    "240m": 14_400_000,
}


class CandleSchemaV1:
    """Column names and dtypes for the standardized dataset."""

    COLUMN_ORDER = (
        "ts_ms",
        "open",
        "high",
        "low",
        "close",
        "volume_base",
        "volume_quote",
        "volume_quote_est",
    )

    REQUIRED_COLUMNS = (
        "ts_ms",
        "open",
        "high",
        "low",
        "close",
        "volume_base",
    )

    DTYPES: dict[str, pl.DataType] = {
        "ts_ms": pl.Int64,
        "open": pl.Float64,
        "high": pl.Float64,
        "low": pl.Float64,
        "close": pl.Float64,
        "volume_base": pl.Float64,
        "volume_quote": pl.Float64,
        "volume_quote_est": pl.Boolean,
    }


def expected_interval_ms(tf: str) -> int:
    """Return expected timeframe interval in milliseconds."""

    normalized = tf.lower().strip()
    if normalized not in EXPECTED_INTERVAL_MS:
        raise ValueError(f"Unsupported timeframe: {tf}")
    return EXPECTED_INTERVAL_MS[normalized]
