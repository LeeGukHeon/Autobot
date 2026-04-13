"""Manifest helpers for raw Upbit public websocket collection outputs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import polars as pl


WS_MANIFEST_SCHEMA: dict[str, pl.DataType] = {
    "run_id": pl.Utf8,
    "channel": pl.Utf8,
    "date": pl.Utf8,
    "hour": pl.Utf8,
    "rows": pl.Int64,
    "min_ts_ms": pl.Int64,
    "max_ts_ms": pl.Int64,
    "bytes": pl.Int64,
    "status": pl.Utf8,
    "reasons_json": pl.Utf8,
    "part_file": pl.Utf8,
    "collected_at_ms": pl.Int64,
}

STATUS_VALUES: set[str] = {"OK", "WARN", "FAIL"}


def empty_ws_manifest() -> pl.DataFrame:
    return pl.DataFrame([], schema=WS_MANIFEST_SCHEMA, orient="row")


def load_ws_manifest(path: Path) -> pl.DataFrame:
    if not path.exists():
        return empty_ws_manifest()
    return _align_columns(pl.read_parquet(path))


def save_ws_manifest(path: Path, frame: pl.DataFrame) -> None:
    aligned = _align_columns(frame)
    path.parent.mkdir(parents=True, exist_ok=True)
    aligned.write_parquet(path, compression="zstd")


def append_ws_manifest_rows(path: Path, rows: list[dict[str, Any]]) -> pl.DataFrame:
    if not rows:
        frame = load_ws_manifest(path)
        save_ws_manifest(path, frame)
        return frame

    incoming = normalize_ws_manifest_rows(rows)
    if path.exists():
        combined = pl.concat([load_ws_manifest(path), incoming], how="vertical")
    else:
        combined = incoming
    save_ws_manifest(path, combined)
    return combined


def normalize_ws_manifest_rows(rows: list[dict[str, Any]]) -> pl.DataFrame:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        item = row if isinstance(row, dict) else {}
        normalized.append(
            {
                "run_id": _coerce_str(item.get("run_id")),
                "channel": _coerce_channel(item.get("channel")),
                "date": _coerce_str(item.get("date")),
                "hour": _coerce_hour(item.get("hour")),
                "rows": _coerce_int(item.get("rows")),
                "min_ts_ms": _coerce_int(item.get("min_ts_ms")),
                "max_ts_ms": _coerce_int(item.get("max_ts_ms")),
                "bytes": _coerce_int(item.get("bytes")),
                "status": _coerce_status(item.get("status")),
                "reasons_json": _coerce_json_text(item.get("reasons_json")),
                "part_file": _coerce_str(item.get("part_file")),
                "collected_at_ms": _coerce_int(item.get("collected_at_ms")),
            }
        )
    return pl.DataFrame(normalized, schema=WS_MANIFEST_SCHEMA, orient="row")


def _align_columns(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.height <= 0:
        return empty_ws_manifest()
    rows = [dict(row) for row in frame.iter_rows(named=True)]
    return normalize_ws_manifest_rows(rows)


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


def _coerce_hour(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) == 1 and text.isdigit():
        return f"0{text}"
    if len(text) == 2 and text.isdigit():
        return text
    return None


def _coerce_channel(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if text not in {"ticker", "trade", "orderbook"}:
        return None
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


def _coerce_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


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
