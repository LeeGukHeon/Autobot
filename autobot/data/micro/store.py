"""Parquet store + manifest helpers for micro_v1."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import time
from typing import Any

import polars as pl


MICRO_MANIFEST_SCHEMA: dict[str, pl.DataType] = {
    "run_id": pl.Utf8,
    "tf": pl.Utf8,
    "market": pl.Utf8,
    "date": pl.Utf8,
    "rows": pl.Int64,
    "min_ts_ms": pl.Int64,
    "max_ts_ms": pl.Int64,
    "micro_available_rows": pl.Int64,
    "micro_trade_available_rows": pl.Int64,
    "micro_book_available_rows": pl.Int64,
    "trade_source_ws_rows": pl.Int64,
    "trade_source_rest_rows": pl.Int64,
    "trade_source_none_rows": pl.Int64,
    "part_file": pl.Utf8,
    "status": pl.Utf8,
    "reasons_json": pl.Utf8,
    "error_message": pl.Utf8,
    "built_at_ms": pl.Int64,
}


def manifest_path(out_root: Path) -> Path:
    return out_root / "_meta" / "manifest.parquet"


def aggregate_report_path(out_root: Path) -> Path:
    return out_root / "_meta" / "aggregate_report.json"


def validate_report_path(out_root: Path) -> Path:
    return out_root / "_meta" / "validate_report.json"


def empty_micro_manifest() -> pl.DataFrame:
    return pl.DataFrame([], schema=MICRO_MANIFEST_SCHEMA, orient="row")


def load_micro_manifest(path: Path) -> pl.DataFrame:
    if not path.exists():
        return empty_micro_manifest()
    return _align_columns(pl.read_parquet(path))


def save_micro_manifest(path: Path, frame: pl.DataFrame) -> None:
    aligned = _align_columns(frame)
    path.parent.mkdir(parents=True, exist_ok=True)
    aligned.write_parquet(path, compression="zstd")


def append_micro_manifest_rows(path: Path, rows: list[dict[str, Any]]) -> pl.DataFrame:
    if not rows:
        frame = load_micro_manifest(path)
        save_micro_manifest(path, frame)
        return frame

    incoming = normalize_micro_manifest_rows(rows)
    if path.exists():
        combined = pl.concat([load_micro_manifest(path), incoming], how="vertical")
    else:
        combined = incoming
    save_micro_manifest(path, combined)
    return combined


def normalize_micro_manifest_rows(rows: list[dict[str, Any]]) -> pl.DataFrame:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        item = row if isinstance(row, dict) else {}
        normalized.append(
            {
                "run_id": _coerce_str(item.get("run_id")),
                "tf": _coerce_str(item.get("tf")),
                "market": _coerce_str(item.get("market")),
                "date": _coerce_str(item.get("date")),
                "rows": _coerce_int(item.get("rows")),
                "min_ts_ms": _coerce_int(item.get("min_ts_ms")),
                "max_ts_ms": _coerce_int(item.get("max_ts_ms")),
                "micro_available_rows": _coerce_int(item.get("micro_available_rows")),
                "micro_trade_available_rows": _coerce_int(item.get("micro_trade_available_rows")),
                "micro_book_available_rows": _coerce_int(item.get("micro_book_available_rows")),
                "trade_source_ws_rows": _coerce_int(item.get("trade_source_ws_rows")),
                "trade_source_rest_rows": _coerce_int(item.get("trade_source_rest_rows")),
                "trade_source_none_rows": _coerce_int(item.get("trade_source_none_rows")),
                "part_file": _coerce_str(item.get("part_file")),
                "status": _coerce_status(item.get("status")),
                "reasons_json": _coerce_json_text(item.get("reasons_json")),
                "error_message": _coerce_str(item.get("error_message")),
                "built_at_ms": _coerce_int(item.get("built_at_ms")),
            }
        )
    return pl.DataFrame(normalized, schema=MICRO_MANIFEST_SCHEMA, orient="row")


def write_micro_partitions(
    *,
    frame: pl.DataFrame,
    out_root: Path,
    tf: str,
    run_id: str,
    mode: str = "append",
) -> list[dict[str, Any]]:
    if frame.height <= 0:
        return []

    tf_value = str(tf).strip().lower()
    mode_value = str(mode).strip().lower()
    if mode_value not in {"append", "overwrite"}:
        raise ValueError("mode must be one of: append,overwrite")

    prepared = (
        frame.sort(["market", "ts_ms"])
        .with_columns(
            pl.from_epoch(pl.col("ts_ms"), time_unit="ms").dt.strftime("%Y-%m-%d").alias("date")
        )
        .select(frame.columns + ["date"])
    )

    built_at_ms = int(time.time() * 1000)
    rows: list[dict[str, Any]] = []
    for part in prepared.partition_by(["market", "date"], as_dict=False, maintain_order=True):
        market = str(part.item(0, "market"))
        date_value = str(part.item(0, "date"))
        part_dir = out_root / f"tf={tf_value}" / f"market={market}" / f"date={date_value}"
        part_dir.mkdir(parents=True, exist_ok=True)

        if mode_value == "overwrite":
            for old_file in part_dir.glob("part-*.parquet"):
                try:
                    old_file.unlink()
                except Exception:
                    pass

        output_file = _next_part_file(part_dir=part_dir, run_id=run_id)
        payload = part.drop("date")
        payload.write_parquet(output_file, compression="zstd")

        total_rows = int(payload.height)
        min_ts_ms = _to_int(payload.get_column("ts_ms").min()) if total_rows > 0 else None
        max_ts_ms = _to_int(payload.get_column("ts_ms").max()) if total_rows > 0 else None
        micro_available_rows = int(payload.get_column("micro_available").cast(pl.Int64).sum())
        micro_trade_available_rows = int(payload.get_column("micro_trade_available").cast(pl.Int64).sum())
        micro_book_available_rows = int(payload.get_column("micro_book_available").cast(pl.Int64).sum())

        source_counts = _trade_source_counts(payload)

        rows.append(
            {
                "run_id": run_id,
                "tf": tf_value,
                "market": market,
                "date": date_value,
                "rows": total_rows,
                "min_ts_ms": min_ts_ms,
                "max_ts_ms": max_ts_ms,
                "micro_available_rows": micro_available_rows,
                "micro_trade_available_rows": micro_trade_available_rows,
                "micro_book_available_rows": micro_book_available_rows,
                "trade_source_ws_rows": int(source_counts.get("ws", 0)),
                "trade_source_rest_rows": int(source_counts.get("rest", 0)),
                "trade_source_none_rows": int(source_counts.get("none", 0)),
                "part_file": str(output_file),
                "status": "OK" if total_rows > 0 else "WARN",
                "reasons_json": json.dumps([] if total_rows > 0 else ["NO_ROWS"], ensure_ascii=False),
                "error_message": None,
                "built_at_ms": built_at_ms,
            }
        )

    return rows


def write_json_report(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _next_part_file(*, part_dir: Path, run_id: str) -> Path:
    candidate = part_dir / f"part-{run_id}.parquet"
    if not candidate.exists():
        return candidate
    for idx in range(1, 10_000):
        numbered = part_dir / f"part-{run_id}-{idx:04d}.parquet"
        if not numbered.exists():
            return numbered
    raise RuntimeError(f"Too many part files in {part_dir}")


def _trade_source_counts(frame: pl.DataFrame) -> dict[str, int]:
    if frame.height <= 0 or "trade_source" not in frame.columns:
        return {"ws": 0, "rest": 0, "none": 0}
    grouped = frame.group_by("trade_source").len()
    counts = {"ws": 0, "rest": 0, "none": 0}
    for row in grouped.iter_rows(named=True):
        key = str(row.get("trade_source") or "none").strip().lower()
        if key not in counts:
            continue
        counts[key] = int(row.get("len") or 0)
    return counts


def _align_columns(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.height <= 0:
        return empty_micro_manifest()
    rows = [dict(row) for row in frame.iter_rows(named=True)]
    return normalize_micro_manifest_rows(rows)


def _coerce_int(value: Any) -> int | None:
    return _to_int(value)


def _coerce_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _coerce_status(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    if text not in {"OK", "WARN", "FAIL"}:
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


def _to_int(value: Any) -> int | None:
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
