"""Feature store manifest helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import polars as pl


FEATURE_MANIFEST_SCHEMA: dict[str, pl.DataType] = {
    "dataset_name": pl.Utf8,
    "tf": pl.Utf8,
    "market": pl.Utf8,
    "rows": pl.Int64,
    "min_ts_ms": pl.Int64,
    "max_ts_ms": pl.Int64,
    "feature_cols_hash": pl.Utf8,
    "label_cols_hash": pl.Utf8,
    "null_ratio_overall": pl.Float64,
    "null_ratio_by_col_json": pl.Utf8,
    "status": pl.Utf8,
    "reasons_json": pl.Utf8,
    "error_message": pl.Utf8,
    "built_at": pl.Int64,
}

STATUS_VALUES = {"OK", "WARN", "FAIL"}


def manifest_path(dataset_root: Path) -> Path:
    return dataset_root / "_meta" / "manifest.parquet"


def empty_manifest() -> pl.DataFrame:
    return pl.DataFrame([], schema=FEATURE_MANIFEST_SCHEMA, orient="row")


def load_manifest(path: Path) -> pl.DataFrame:
    if not path.exists():
        return empty_manifest()
    return _align_columns(pl.read_parquet(path))


def save_manifest(path: Path, frame: pl.DataFrame) -> None:
    aligned = _align_columns(frame)
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
    for row in rows:
        item = row if isinstance(row, dict) else {}
        normalized_rows.append(
            {
                "dataset_name": _coerce_str(item.get("dataset_name")),
                "tf": _coerce_str(item.get("tf")),
                "market": _coerce_str(item.get("market")),
                "rows": _coerce_int(item.get("rows")),
                "min_ts_ms": _coerce_int(item.get("min_ts_ms")),
                "max_ts_ms": _coerce_int(item.get("max_ts_ms")),
                "feature_cols_hash": _coerce_str(item.get("feature_cols_hash")),
                "label_cols_hash": _coerce_str(item.get("label_cols_hash")),
                "null_ratio_overall": _coerce_float(item.get("null_ratio_overall")),
                "null_ratio_by_col_json": _coerce_json_text(item.get("null_ratio_by_col_json")),
                "status": _coerce_status(item.get("status")),
                "reasons_json": _coerce_json_text(item.get("reasons_json")),
                "error_message": _coerce_str(item.get("error_message")),
                "built_at": _coerce_int(item.get("built_at")),
            }
        )
    return pl.DataFrame(normalized_rows, schema=FEATURE_MANIFEST_SCHEMA, orient="row")


def _align_columns(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.is_empty():
        return empty_manifest()
    rows = [dict(row) for row in frame.iter_rows(named=True)]
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


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            return float(text)
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_status(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    if not text:
        return None
    if text not in STATUS_VALUES:
        return None
    return text


def _coerce_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text


def _coerce_json_text(value: Any) -> str:
    if value is None:
        return "[]"
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return "[]"
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return json.dumps([text], ensure_ascii=False)
        return json.dumps(parsed, ensure_ascii=False)
    return json.dumps(value, ensure_ascii=False)
