"""Validation for lob30 parquet datasets."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json
from typing import Any

import polars as pl


@dataclass(frozen=True)
class Lob30ValidateSummary:
    checked_files: int
    ok_files: int
    warn_files: int
    fail_files: int
    validate_report_file: Path
    details: tuple[dict[str, Any], ...]


def validate_lob30_dataset(
    *,
    parquet_root: Path = Path("data/parquet"),
    dataset_name: str = "lob30_v1",
    report_path: Path = Path("data/collect/_meta/lob30_validate_report.json"),
) -> Lob30ValidateSummary:
    dataset_root = parquet_root / dataset_name
    part_files = sorted(path for path in dataset_root.glob("market=*/date=*/*.parquet") if path.is_file())
    details: list[dict[str, Any]] = []
    ok_files = 0
    warn_files = 0
    fail_files = 0

    for part_file in part_files:
        market = part_file.parent.parent.name.replace("market=", "", 1).upper()
        date_value = part_file.parent.name.replace("date=", "", 1)
        try:
            frame = pl.read_parquet(part_file)
            detail = _validate_frame(frame=frame, file=part_file, market=market, date_value=date_value)
            status = detail["status"]
        except Exception as exc:
            status = "FAIL"
            detail = {
                "file": str(part_file),
                "market": market,
                "date": date_value,
                "rows": 0,
                "status": "FAIL",
                "reasons": ["VALIDATE_EXCEPTION"],
                "error_message": str(exc),
            }

        details.append(detail)
        if status == "OK":
            ok_files += 1
        elif status == "WARN":
            warn_files += 1
        else:
            fail_files += 1

    report = {
        "dataset_name": dataset_name,
        "dataset_root": str(dataset_root),
        "checked_files": len(details),
        "ok_files": ok_files,
        "warn_files": warn_files,
        "fail_files": fail_files,
        "details": details,
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return Lob30ValidateSummary(
        checked_files=len(details),
        ok_files=ok_files,
        warn_files=warn_files,
        fail_files=fail_files,
        validate_report_file=report_path,
        details=tuple(details),
    )


def _validate_frame(*, frame: pl.DataFrame, file: Path, market: str, date_value: str) -> dict[str, Any]:
    required = [
        "ts_ms",
        "collected_at_ms",
        "market",
        "requested_depth",
        "levels_present",
        "ask1_price",
        "bid1_price",
    ] + [
        name
        for idx in range(1, 31)
        for name in (f"ask{idx}_price", f"ask{idx}_size", f"bid{idx}_price", f"bid{idx}_size")
    ]
    missing = [col for col in required if col not in frame.columns]
    if missing:
        return {
            "file": str(file),
            "market": market,
            "date": date_value,
            "rows": int(frame.height),
            "status": "FAIL",
            "reasons": ["MISSING_REQUIRED_COLUMNS"],
            "missing_columns": missing,
        }

    reasons: list[str] = []
    status = "OK"
    if frame.height <= 0:
        status = "WARN"
        reasons.append("NO_ROWS_COLLECTED")
    else:
        if int(frame.select((pl.col("requested_depth") != 30).sum()).item()) > 0:
            status = "FAIL"
            reasons.append("REQUESTED_DEPTH_NOT_30")
        if int(frame.select((pl.col("levels_present") < 30).sum()).item()) > 0:
            status = "FAIL"
            reasons.append("LEVELS_PRESENT_LT_30")
        if int(frame.select((pl.col("level").fill_null(0.0) != 0.0).sum()).item()) > 0:
            status = "FAIL"
            reasons.append("GROUPED_LEVEL_NOT_ZERO")
        if int(frame.select((pl.col("ask1_price") < pl.col("bid1_price")).sum()).item()) > 0:
            status = "FAIL"
            reasons.append("CROSSED_TOP_OF_BOOK")
        monotonic_violations = 0
        for row in frame.iter_rows(named=True):
            if not _levels_monotonic(row):
                monotonic_violations += 1
        if monotonic_violations > 0:
            status = "FAIL"
            reasons.append("NON_MONOTONIC_LEVELS")

    return {
        "file": str(file),
        "market": market,
        "date": date_value,
        "rows": int(frame.height),
        "min_ts_ms": _series_int(frame.get_column("ts_ms").min()) if frame.height > 0 else None,
        "max_ts_ms": _series_int(frame.get_column("ts_ms").max()) if frame.height > 0 else None,
        "status": status,
        "reasons": reasons,
    }


def _levels_monotonic(row: dict[str, Any]) -> bool:
    ask_prices = [_as_float(row.get(f"ask{idx}_price")) for idx in range(1, 31)]
    bid_prices = [_as_float(row.get(f"bid{idx}_price")) for idx in range(1, 31)]
    ask_clean = [value for value in ask_prices if value is not None]
    bid_clean = [value for value in bid_prices if value is not None]
    if len(ask_clean) < 30 or len(bid_clean) < 30:
        return False
    if any(ask_clean[idx] < ask_clean[idx - 1] for idx in range(1, len(ask_clean))):
        return False
    if any(bid_clean[idx] > bid_clean[idx - 1] for idx in range(1, len(bid_clean))):
        return False
    return True


def _series_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
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
