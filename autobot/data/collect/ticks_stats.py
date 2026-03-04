"""Stats helpers for collected raw ticks datasets."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import polars as pl


def collect_ticks_stats(
    *,
    raw_root: Path = Path("data/raw_ticks/upbit/trades"),
    meta_dir: Path = Path("data/raw_ticks/upbit/_meta"),
    date_filter: str | None = None,
) -> dict[str, Any]:
    manifest_file = meta_dir / "ticks_manifest.parquet"
    part_files = (
        sorted(path for path in raw_root.glob(f"date={date_filter}/market=*/*.jsonl.zst") if path.is_file())
        if date_filter
        else sorted(path for path in raw_root.glob("date=*/market=*/*.jsonl.zst") if path.is_file())
    )

    manifest_summary = _manifest_summary(manifest_file=manifest_file, date_filter=date_filter)
    collect_report = _load_json(meta_dir / "ticks_collect_report.json")
    validate_report = _load_json(meta_dir / "ticks_validate_report.json")

    return {
        "raw_root": str(raw_root),
        "meta_dir": str(meta_dir),
        "date_filter": date_filter,
        "part_files": len(part_files),
        "manifest": manifest_summary,
        "collect_report": _report_excerpt(collect_report),
        "validate_report": _report_excerpt(validate_report),
    }


def _manifest_summary(*, manifest_file: Path, date_filter: str | None) -> dict[str, Any]:
    if not manifest_file.exists():
        return {
            "available": False,
            "manifest_file": str(manifest_file),
        }
    frame = pl.read_parquet(manifest_file)
    if date_filter:
        frame = frame.filter(pl.col("date") == date_filter)

    if frame.height <= 0:
        return {
            "available": True,
            "manifest_file": str(manifest_file),
            "rows": 0,
            "parts": 0,
        }

    status_counts = frame.group_by("status").len().sort("status")
    by_status = {
        str(row["status"]): int(row["len"])
        for row in status_counts.iter_rows(named=True)
        if row.get("status") is not None
    }
    rows_total = int(frame.get_column("rows").fill_null(0).sum())
    avg_dup_ratio = float(frame.get_column("dup_ratio").fill_null(0.0).mean())

    by_date_counts = frame.group_by("date").len().sort("date")
    by_date = [
        {"date": str(row["date"]), "parts": int(row["len"])}
        for row in by_date_counts.iter_rows(named=True)
        if row.get("date") is not None
    ]
    return {
        "available": True,
        "manifest_file": str(manifest_file),
        "parts": int(frame.height),
        "rows_total": rows_total,
        "avg_dup_ratio": round(avg_dup_ratio, 8),
        "by_status": by_status,
        "by_date": by_date,
    }


def _load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _report_excerpt(report: dict[str, Any] | None) -> dict[str, Any] | None:
    if report is None:
        return None
    keys = (
        "started_at",
        "finished_at",
        "checked_files",
        "ok_files",
        "warn_files",
        "fail_files",
        "discovered_targets",
        "selected_targets",
        "processed_targets",
        "ok_targets",
        "warn_targets",
        "fail_targets",
        "calls_made",
        "throttled_count",
        "backoff_count",
        "rows_collected_total",
        "dup_ratio_overall",
        "schema_ok_ratio",
    )
    return {key: report.get(key) for key in keys if key in report}
