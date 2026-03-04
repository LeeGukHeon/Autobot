"""Parquet writer for API-collected candle partitions."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from ..schema_contract import CandleSchemaV1


def write_candle_partition(
    *,
    dataset_root: Path,
    tf: str,
    market: str,
    candles: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> dict[str, Any]:
    tf_value = str(tf).strip().lower()
    market_value = str(market).strip().upper()
    part_file = dataset_root / f"tf={tf_value}" / f"market={market_value}" / "part-000.parquet"
    part_file.parent.mkdir(parents=True, exist_ok=True)

    incoming = _to_candle_frame(candles)
    if incoming.height <= 0:
        return {
            "part_file": str(part_file),
            "rows": 0,
            "min_ts_ms": None,
            "max_ts_ms": None,
        }

    existing_files = sorted(path for path in part_file.parent.glob("*.parquet") if path.is_file())
    if existing_files:
        existing = pl.concat([pl.read_parquet(path) for path in existing_files], how="vertical")
        combined = pl.concat([existing, incoming], how="vertical")
    else:
        combined = incoming

    combined = (
        combined.with_row_index("__row_id")
        .sort(["ts_ms", "__row_id"])
        .unique(subset=["ts_ms"], keep="last")
        .sort("ts_ms")
        .drop("__row_id")
        .select(CandleSchemaV1.COLUMN_ORDER)
    )
    combined.write_parquet(part_file, compression="zstd")

    min_ts_ms = int(combined.get_column("ts_ms").min()) if combined.height > 0 else None
    max_ts_ms = int(combined.get_column("ts_ms").max()) if combined.height > 0 else None
    return {
        "part_file": str(part_file),
        "rows": int(combined.height),
        "min_ts_ms": min_ts_ms,
        "max_ts_ms": max_ts_ms,
    }


def _to_candle_frame(candles: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for row in candles:
        if not isinstance(row, dict):
            continue
        rows.append(
            {
                "ts_ms": _as_int(row.get("ts_ms")),
                "open": _as_float(row.get("open")),
                "high": _as_float(row.get("high")),
                "low": _as_float(row.get("low")),
                "close": _as_float(row.get("close")),
                "volume_base": _as_float(row.get("volume_base")),
                "volume_quote": _as_float(row.get("volume_quote")),
                "volume_quote_est": bool(row.get("volume_quote_est") or False),
            }
        )
    frame = pl.DataFrame(rows, schema=CandleSchemaV1.DTYPES, orient="row")
    if frame.height <= 0:
        return frame
    required_not_null = (
        pl.col("ts_ms").is_not_null()
        & pl.col("open").is_not_null()
        & pl.col("high").is_not_null()
        & pl.col("low").is_not_null()
        & pl.col("close").is_not_null()
        & pl.col("volume_base").is_not_null()
    )
    return frame.filter(required_not_null).select(CandleSchemaV1.COLUMN_ORDER)


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            return int(float(text))
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
