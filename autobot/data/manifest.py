"""Manifest utilities for ingestion reproducibility and incremental skip."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import polars as pl


MANIFEST_SCHEMA: dict[str, pl.DataType] = {
    "quote": pl.Utf8,
    "symbol": pl.Utf8,
    "market": pl.Utf8,
    "tf": pl.Utf8,
    "source_csv_relpath": pl.Utf8,
    "source_csv_size": pl.Int64,
    "source_csv_mtime": pl.Int64,
    "ingested_at": pl.Int64,
    "rows": pl.Int64,
    "min_ts_ms": pl.Int64,
    "max_ts_ms": pl.Int64,
    "duplicates_dropped": pl.Int64,
    "non_monotonic_found": pl.Boolean,
    "gaps_found": pl.Int64,
    "invalid_rows_dropped": pl.Int64,
    "ohlc_violations": pl.Int64,
    "status": pl.Utf8,
    "reasons_json": pl.Utf8,
    "error_message": pl.Utf8,
    "timestamp_source": pl.Utf8,
    "timestamp_policy": pl.Utf8,
    "engine": pl.Utf8,
}

STATUS_VALUES: set[str] = {"OK", "WARN", "FAIL"}


def manifest_path(dataset_root: Path) -> Path:
    return dataset_root / "_meta" / "manifest.parquet"


def empty_manifest() -> pl.DataFrame:
    empty_rows: list[dict[str, Any]] = []
    return pl.DataFrame(empty_rows, schema=MANIFEST_SCHEMA, orient="row")


def load_manifest(path: Path) -> pl.DataFrame:
    if not path.exists():
        return empty_manifest()
    return _align_manifest_columns(pl.read_parquet(path))


def save_manifest(path: Path, frame: pl.DataFrame) -> None:
    aligned = _align_manifest_columns(frame)
    path.parent.mkdir(parents=True, exist_ok=True)
    aligned.write_parquet(path, compression="zstd")


def append_manifest_rows(path: Path, rows: list[dict[str, Any]]) -> pl.DataFrame:
    if not rows:
        frame = load_manifest(path)
        save_manifest(path, frame)
        return frame

    incoming = normalize_manifest_rows(rows)
    if path.exists():
        combined = pl.concat([load_manifest(path), incoming], how="vertical")
    else:
        combined = incoming

    save_manifest(path, combined)
    return combined


def normalize_manifest_rows(rows: list[dict[str, Any]]) -> pl.DataFrame:
    normalized_rows: list[dict[str, Any]] = []
    for raw in rows:
        item = raw if isinstance(raw, dict) else {}
        normalized = {
            "quote": _coerce_str(item.get("quote")),
            "symbol": _coerce_str(item.get("symbol")),
            "market": _coerce_str(item.get("market")),
            "tf": _coerce_str(item.get("tf")),
            "source_csv_relpath": _coerce_str(item.get("source_csv_relpath")),
            "source_csv_size": _coerce_int(item.get("source_csv_size")),
            "source_csv_mtime": _coerce_int(item.get("source_csv_mtime")),
            "ingested_at": _coerce_int(item.get("ingested_at")),
            "rows": _coerce_int(item.get("rows")),
            "min_ts_ms": _coerce_int(item.get("min_ts_ms")),
            "max_ts_ms": _coerce_int(item.get("max_ts_ms")),
            "duplicates_dropped": _coerce_int(item.get("duplicates_dropped")),
            "non_monotonic_found": _coerce_bool(item.get("non_monotonic_found")),
            "gaps_found": _coerce_int(item.get("gaps_found")),
            "invalid_rows_dropped": _coerce_int(item.get("invalid_rows_dropped")),
            "ohlc_violations": _coerce_int(item.get("ohlc_violations", item.get("ohlc_consistency_violations"))),
            "status": _coerce_status(item.get("status")),
            "reasons_json": _coerce_reasons_json(
                item.get("reasons_json", item.get("status_reasons", item.get("reasons")))
            ),
            "error_message": _coerce_str(item.get("error_message")),
            "timestamp_source": _coerce_str(item.get("timestamp_source")),
            "timestamp_policy": _coerce_str(item.get("timestamp_policy")),
            "engine": _coerce_str(item.get("engine")),
        }
        normalized_rows.append(normalized)

    return pl.DataFrame(normalized_rows, schema=MANIFEST_SCHEMA, orient="row")


def build_manifest_index(manifest_df: pl.DataFrame) -> dict[str, dict[str, Any]]:
    if manifest_df.is_empty():
        return {}

    ordered = manifest_df.sort("ingested_at")
    index: dict[str, dict[str, Any]] = {}
    for row in ordered.iter_rows(named=True):
        relpath = str(row["source_csv_relpath"])
        index[relpath] = row
    return index


def should_skip_file(
    manifest_index: dict[str, dict[str, Any]],
    source_csv_relpath: str,
    source_csv_size: int,
    source_csv_mtime: int,
) -> bool:
    row = manifest_index.get(source_csv_relpath)
    if row is None:
        return False

    if row.get("status") not in {"OK", "WARN"}:
        return False

    row_size = _coerce_int(row.get("source_csv_size"))
    row_mtime = _coerce_int(row.get("source_csv_mtime"))
    if row_size is None or row_mtime is None:
        return False

    return row_size == int(source_csv_size) and row_mtime == int(source_csv_mtime)


def _align_manifest_columns(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return empty_manifest()
    rows = [dict(row) for row in df.iter_rows(named=True)]
    return normalize_manifest_rows(rows)


def _coerce_int(value: Any) -> int | None:
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


def _coerce_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "t", "yes", "y"}:
            return True
        if lowered in {"0", "false", "f", "no", "n"}:
            return False
    return None


def _coerce_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text


def _coerce_status(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    if not text:
        return None
    if text not in STATUS_VALUES:
        return None
    return text


def _coerce_reasons_json(value: Any) -> str:
    if value is None:
        return json.dumps([], ensure_ascii=False)

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return json.dumps([], ensure_ascii=False)
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return json.dumps([text], ensure_ascii=False)
        return _reasons_to_json(parsed)

    return _reasons_to_json(value)


def _reasons_to_json(value: Any) -> str:
    if isinstance(value, list):
        reasons = [str(item) for item in value if str(item).strip()]
        return json.dumps(reasons, ensure_ascii=False)
    if isinstance(value, tuple):
        reasons = [str(item) for item in value if str(item).strip()]
        return json.dumps(reasons, ensure_ascii=False)
    if isinstance(value, str):
        text = value.strip()
        reasons = [text] if text else []
        return json.dumps(reasons, ensure_ascii=False)
    reasons = [str(value)]
    return json.dumps(reasons, ensure_ascii=False)
