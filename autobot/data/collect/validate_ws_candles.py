"""Validation for websocket candle parquet datasets."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

import polars as pl

from ..ingest_csv_to_parquet import _validate_frame


@dataclass(frozen=True)
class WsCandleValidateSummary:
    checked_files: int
    ok_files: int
    warn_files: int
    fail_files: int
    schema_ok: bool
    ohlc_ok: bool
    validate_report_file: Path
    details: tuple[dict[str, Any], ...]


def validate_ws_candle_dataset(
    *,
    parquet_root: Path = Path("data/parquet"),
    dataset_name: str = "ws_candle_v1",
    report_path: Path = Path("data/collect/_meta/ws_candle_validate_report.json"),
) -> WsCandleValidateSummary:
    dataset_root = parquet_root / dataset_name
    part_files = sorted(path for path in dataset_root.glob("tf=*/market=*/*.parquet") if path.is_file())
    details: list[dict[str, Any]] = []
    ok_files = 0
    warn_files = 0
    fail_files = 0

    for part_file in part_files:
        tf = part_file.parent.parent.name.replace("tf=", "", 1).lower()
        market = part_file.parent.name.replace("market=", "", 1).upper()
        try:
            frame = pl.read_parquet(part_file)
            stats = _validate_frame(
                frame,
                tf=tf,
                gap_severity="info",
                quote_est_severity="info",
                ohlc_violation_policy="drop_row_and_warn",
            )
            sparse_gap_count = int(stats.get("gaps_found", 0))
            status = str(stats.get("status", "FAIL")).upper()
            reasons = [
                str(reason)
                for reason in list(stats.get("status_reasons", []))
                if str(reason).strip() and str(reason).strip().upper() != "GAPS_FOUND"
            ]
            detail = {
                "file": str(part_file),
                "market": market,
                "tf": tf,
                "rows": int(stats.get("rows", 0)),
                "min_ts_ms": stats.get("min_ts_ms"),
                "max_ts_ms": stats.get("max_ts_ms"),
                "status": status,
                "status_reasons": reasons,
                "ohlc_violations": int(stats.get("ohlc_violations", 0)),
                "sparse_gap_count_ignored": sparse_gap_count,
            }
        except Exception as exc:
            status = "FAIL"
            detail = {
                "file": str(part_file),
                "market": market,
                "tf": tf,
                "rows": 0,
                "min_ts_ms": None,
                "max_ts_ms": None,
                "status": status,
                "status_reasons": ["VALIDATE_EXCEPTION"],
                "error_message": str(exc),
                "ohlc_violations": 0,
                "sparse_gap_count_ignored": 0,
            }

        details.append(detail)
        if status == "OK":
            ok_files += 1
        elif status == "WARN":
            warn_files += 1
        else:
            fail_files += 1

    schema_ok = fail_files == 0
    ohlc_ok = all(int(item.get("ohlc_violations", 0)) == 0 for item in details)
    report = {
        "dataset_name": dataset_name,
        "dataset_root": str(dataset_root),
        "sparse_stream_semantics": {
            "no_trade_no_candle_message": True,
            "duplicate_candle_updates_latest_wins": True,
        },
        "checked_files": len(details),
        "ok_files": ok_files,
        "warn_files": warn_files,
        "fail_files": fail_files,
        "schema_ok": schema_ok,
        "ohlc_ok": ohlc_ok,
        "details": details,
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    return WsCandleValidateSummary(
        checked_files=len(details),
        ok_files=ok_files,
        warn_files=warn_files,
        fail_files=fail_files,
        schema_ok=schema_ok,
        ohlc_ok=ohlc_ok,
        validate_report_file=report_path,
        details=tuple(details),
    )
