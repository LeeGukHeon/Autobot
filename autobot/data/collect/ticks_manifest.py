"""Manifest helpers for raw ticks collection outputs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import polars as pl


TICKS_MANIFEST_SCHEMA: dict[str, pl.DataType] = {
    "run_id": pl.Utf8,
    "date": pl.Utf8,
    "market": pl.Utf8,
    "days_ago": pl.Int32,
    "rows": pl.Int64,
    "min_ts_ms": pl.Int64,
    "max_ts_ms": pl.Int64,
    "dup_ratio": pl.Float64,
    "status": pl.Utf8,
    "reasons_json": pl.Utf8,
    "calls_made": pl.Int64,
    "pages_collected": pl.Int64,
    "part_file": pl.Utf8,
    "error_message": pl.Utf8,
    "collected_at_ms": pl.Int64,
}

STATUS_VALUES: set[str] = {"OK", "WARN", "FAIL"}


def empty_ticks_manifest() -> pl.DataFrame:
    return pl.DataFrame([], schema=TICKS_MANIFEST_SCHEMA, orient="row")


def load_ticks_manifest(path: Path) -> pl.DataFrame:
    if not path.exists():
        return empty_ticks_manifest()
    return _align_columns(pl.read_parquet(path))


def save_ticks_manifest(path: Path, frame: pl.DataFrame) -> None:
    aligned = _align_columns(frame)
    path.parent.mkdir(parents=True, exist_ok=True)
    aligned.write_parquet(path, compression="zstd")


def append_ticks_manifest_rows(path: Path, rows: list[dict[str, Any]]) -> pl.DataFrame:
    if not rows:
        frame = load_ticks_manifest(path)
        save_ticks_manifest(path, frame)
        return frame

    incoming = normalize_ticks_manifest_rows(rows)
    if path.exists():
        combined = pl.concat([load_ticks_manifest(path), incoming], how="vertical")
    else:
        combined = incoming
    save_ticks_manifest(path, combined)
    return combined


def normalize_ticks_manifest_rows(rows: list[dict[str, Any]]) -> pl.DataFrame:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        item = row if isinstance(row, dict) else {}
        normalized.append(
            {
                "run_id": _coerce_str(item.get("run_id")),
                "date": _coerce_str(item.get("date")),
                "market": _coerce_str(item.get("market")),
                "days_ago": _coerce_int(item.get("days_ago"), bit32=True),
                "rows": _coerce_int(item.get("rows")),
                "min_ts_ms": _coerce_int(item.get("min_ts_ms")),
                "max_ts_ms": _coerce_int(item.get("max_ts_ms")),
                "dup_ratio": _coerce_float(item.get("dup_ratio")),
                "status": _coerce_status(item.get("status")),
                "reasons_json": _coerce_json_text(item.get("reasons_json")),
                "calls_made": _coerce_int(item.get("calls_made")),
                "pages_collected": _coerce_int(item.get("pages_collected")),
                "part_file": _coerce_str(item.get("part_file")),
                "error_message": _coerce_str(item.get("error_message")),
                "collected_at_ms": _coerce_int(item.get("collected_at_ms")),
            }
        )
    return pl.DataFrame(normalized, schema=TICKS_MANIFEST_SCHEMA, orient="row")


def _align_columns(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.height <= 0:
        return empty_ticks_manifest()
    rows = [dict(row) for row in frame.iter_rows(named=True)]
    return normalize_ticks_manifest_rows(rows)


def _coerce_int(value: Any, *, bit32: bool = False) -> int | None:
    if value is None:
        return None
    try:
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            parsed = int(float(text))
        else:
            parsed = int(value)
        if bit32 and (parsed < -2_147_483_648 or parsed > 2_147_483_647):
            return None
        return parsed
    except (TypeError, ValueError):
        return None


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
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
