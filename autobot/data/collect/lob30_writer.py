"""Parquet writer for lob30 market/date snapshots."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl


LOB30_COLUMN_ORDER = (
    "ts_ms",
    "collected_at_ms",
    "stream_type",
    "market",
    "source",
    "level",
    "requested_depth",
    "levels_present",
    "total_ask_size",
    "total_bid_size",
) + tuple(
    item
    for idx in range(1, 31)
    for item in (f"ask{idx}_price", f"ask{idx}_size", f"bid{idx}_price", f"bid{idx}_size")
)

LOB30_DTYPES: dict[str, pl.DataType] = {
    "ts_ms": pl.Int64,
    "collected_at_ms": pl.Int64,
    "stream_type": pl.Utf8,
    "market": pl.Utf8,
    "source": pl.Utf8,
    "level": pl.Float64,
    "requested_depth": pl.Int64,
    "levels_present": pl.Int64,
    "total_ask_size": pl.Float64,
    "total_bid_size": pl.Float64,
}
for _idx in range(1, 31):
    LOB30_DTYPES[f"ask{_idx}_price"] = pl.Float64
    LOB30_DTYPES[f"ask{_idx}_size"] = pl.Float64
    LOB30_DTYPES[f"bid{_idx}_price"] = pl.Float64
    LOB30_DTYPES[f"bid{_idx}_size"] = pl.Float64


def write_lob30_partition(
    *,
    dataset_root: Path,
    market: str,
    rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> dict[str, Any]:
    market_value = str(market).strip().upper()
    date_value = _date_utc_from_ts_ms(int(rows[0]["ts_ms"])) if rows else datetime.now(timezone.utc).date().isoformat()
    part_file = dataset_root / f"market={market_value}" / f"date={date_value}" / "part-000.parquet"
    part_file.parent.mkdir(parents=True, exist_ok=True)

    incoming = _to_lob30_frame(rows)
    if incoming.height <= 0:
        return {
            "part_file": str(part_file),
            "rows": 0,
            "min_ts_ms": None,
            "max_ts_ms": None,
            "date": date_value,
        }

    existing_files = sorted(path for path in part_file.parent.glob("*.parquet") if path.is_file())
    if existing_files:
        existing = pl.concat([pl.read_parquet(path) for path in existing_files], how="vertical")
        combined = pl.concat([existing, incoming], how="vertical")
    else:
        combined = incoming

    combined = (
        combined.with_row_index("__row_id")
        .sort(["ts_ms", "collected_at_ms", "__row_id"])
        .unique(subset=["ts_ms"], keep="last")
        .sort("ts_ms")
        .drop("__row_id")
        .select(LOB30_COLUMN_ORDER)
    )
    combined.write_parquet(part_file, compression="zstd")

    min_ts_ms = int(combined.get_column("ts_ms").min()) if combined.height > 0 else None
    max_ts_ms = int(combined.get_column("ts_ms").max()) if combined.height > 0 else None
    return {
        "part_file": str(part_file),
        "rows": int(combined.height),
        "min_ts_ms": min_ts_ms,
        "max_ts_ms": max_ts_ms,
        "date": date_value,
    }


def _to_lob30_frame(rows: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> pl.DataFrame:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        item: dict[str, Any] = {}
        for column in LOB30_COLUMN_ORDER:
            if column in {"stream_type", "market", "source"}:
                value = row.get(column)
                item[column] = None if value is None else str(value)
            elif column in {"ts_ms", "collected_at_ms", "requested_depth", "levels_present"}:
                item[column] = _as_int(row.get(column))
            else:
                item[column] = _as_float(row.get(column))
        normalized.append(item)

    frame = pl.DataFrame(normalized, schema=LOB30_DTYPES, orient="row")
    if frame.height <= 0:
        return frame
    return frame.filter(
        pl.col("ts_ms").is_not_null()
        & pl.col("market").is_not_null()
        & pl.col("requested_depth").is_not_null()
        & pl.col("levels_present").is_not_null()
        & pl.col("ask1_price").is_not_null()
        & pl.col("bid1_price").is_not_null()
    ).select(LOB30_COLUMN_ORDER)


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


def _date_utc_from_ts_ms(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc)
    return dt.date().isoformat()
