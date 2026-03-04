"""Stats helpers for collected raw public websocket datasets."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import polars as pl


def collect_ws_public_stats(
    *,
    raw_root: Path = Path("data/raw_ws/upbit/public"),
    meta_dir: Path = Path("data/raw_ws/upbit/_meta"),
    date_filter: str | None = None,
) -> dict[str, Any]:
    manifest_file = meta_dir / "ws_manifest.parquet"
    patterns = (
        [f"trade/date={date_filter}/hour=*/*.jsonl.zst", f"orderbook/date={date_filter}/hour=*/*.jsonl.zst"]
        if date_filter
        else ["trade/date=*/hour=*/*.jsonl.zst", "orderbook/date=*/hour=*/*.jsonl.zst"]
    )
    part_files = []
    for pattern in patterns:
        part_files.extend(path for path in raw_root.glob(pattern) if path.is_file())

    manifest_summary = _manifest_summary(manifest_file=manifest_file, date_filter=date_filter)
    collect_report = _load_json(meta_dir / "ws_collect_report.json")
    validate_report = _load_json(meta_dir / "ws_validate_report.json")
    runs_summary = _load_json(meta_dir / "ws_runs_summary.json")
    health_snapshot = _load_json(meta_dir / "ws_public_health.json")

    return {
        "raw_root": str(raw_root),
        "meta_dir": str(meta_dir),
        "date_filter": date_filter,
        "part_files": len(part_files),
        "manifest": manifest_summary,
        "collect_report": _report_excerpt(collect_report),
        "validate_report": _report_excerpt(validate_report),
        "runs_summary_latest": _latest_run_excerpt(runs_summary),
        "health_snapshot": _health_excerpt(health_snapshot),
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
    channel_rows = frame.group_by("channel").agg(pl.col("rows").sum().alias("rows_total")).sort("channel")
    by_channel = {
        str(row["channel"]): int(row["rows_total"])
        for row in channel_rows.iter_rows(named=True)
        if row.get("channel") is not None
    }

    rows_total = int(frame.get_column("rows").fill_null(0).sum())
    bytes_total = int(frame.get_column("bytes").fill_null(0).sum())

    by_date_counts = frame.group_by(["date", "hour", "channel"]).len().sort(["date", "hour", "channel"])
    by_date_hour = [
        {
            "date": str(row["date"]),
            "hour": str(row["hour"]),
            "channel": str(row["channel"]),
            "parts": int(row["len"]),
        }
        for row in by_date_counts.iter_rows(named=True)
        if row.get("date") is not None
    ]
    return {
        "available": True,
        "manifest_file": str(manifest_file),
        "parts": int(frame.height),
        "rows_total": rows_total,
        "bytes_total": bytes_total,
        "by_status": by_status,
        "by_channel": by_channel,
        "by_date_hour": by_date_hour,
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
        "run_id",
        "duration_sec",
        "codes_count",
        "received_trade",
        "received_orderbook",
        "written_trade",
        "written_orderbook",
        "dropped_orderbook_by_interval",
        "dropped_by_parse_error",
        "reconnect_count",
        "ping_sent_count",
        "pong_rx_count",
        "files_written",
        "bytes_written",
        "checked_files",
        "ok_files",
        "warn_files",
        "fail_files",
        "parse_ok_ratio",
    )
    return {key: report.get(key) for key in keys if key in report}


def _latest_run_excerpt(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if payload is None:
        return None
    runs = payload.get("runs")
    if not isinstance(runs, list) or not runs:
        return None
    last = runs[-1]
    if not isinstance(last, dict):
        return None
    keys = (
        "run_id",
        "parts",
        "rows_total",
        "bytes_total",
        "trade_rows",
        "orderbook_rows",
        "ok_parts",
        "warn_parts",
        "fail_parts",
        "min_date",
        "max_date",
    )
    return {key: last.get(key) for key in keys if key in last}


def _health_excerpt(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if payload is None:
        return None
    keys = (
        "run_id",
        "updated_at_ms",
        "connected",
        "reconnect_count",
        "last_rx_ts_ms",
        "written_rows",
        "dropped_rows",
        "subscribed_markets_count",
        "keepalive",
        "refresh",
        "fatal_reason",
    )
    return {key: payload.get(key) for key in keys if key in payload}
