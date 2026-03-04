"""Validation for API-collected candle dataset."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

import polars as pl

from ..ingest_csv_to_parquet import _validate_frame
from ..inventory import build_candle_inventory, parse_utc_ts_ms


@dataclass(frozen=True)
class CandleValidateSummary:
    checked_files: int
    ok_files: int
    warn_files: int
    fail_files: int
    schema_ok: bool
    ohlc_ok: bool
    validate_report_file: Path
    details: tuple[dict[str, Any], ...]


def validate_candles_api_dataset(
    *,
    parquet_root: Path = Path("data/parquet"),
    dataset_name: str = "candles_api_v1",
    plan_path: Path | None = None,
    report_path: Path = Path("data/collect/_meta/candle_validate_report.json"),
    gap_severity: str = "info",
    quote_est_severity: str = "info",
    ohlc_violation_policy: str = "drop_row_and_warn",
) -> CandleValidateSummary:
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
                gap_severity=gap_severity,
                quote_est_severity=quote_est_severity,
                ohlc_violation_policy=ohlc_violation_policy,
            )
            status = str(stats.get("status", "FAIL")).upper()
            detail = {
                "file": str(part_file),
                "market": market,
                "tf": tf,
                "rows": int(stats.get("rows", 0)),
                "min_ts_ms": stats.get("min_ts_ms"),
                "max_ts_ms": stats.get("max_ts_ms"),
                "status": status,
                "status_reasons": list(stats.get("status_reasons", [])),
                "ohlc_violations": int(stats.get("ohlc_violations", 0)),
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
            }

        details.append(detail)
        if status == "OK":
            ok_files += 1
        elif status == "WARN":
            warn_files += 1
        else:
            fail_files += 1

    coverage_delta = _coverage_delta(plan_path=plan_path, parquet_root=parquet_root, dataset_name=dataset_name)
    schema_ok = fail_files == 0
    ohlc_ok = all(int(item.get("ohlc_violations", 0)) == 0 for item in details)

    report = {
        "dataset_name": dataset_name,
        "dataset_root": str(dataset_root),
        "checked_files": len(details),
        "ok_files": ok_files,
        "warn_files": warn_files,
        "fail_files": fail_files,
        "schema_ok": schema_ok,
        "ohlc_ok": ohlc_ok,
        "coverage_delta": coverage_delta,
        "details": details,
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    return CandleValidateSummary(
        checked_files=len(details),
        ok_files=ok_files,
        warn_files=warn_files,
        fail_files=fail_files,
        schema_ok=schema_ok,
        ohlc_ok=ohlc_ok,
        validate_report_file=report_path,
        details=tuple(details),
    )


def _coverage_delta(*, plan_path: Path | None, parquet_root: Path, dataset_name: str) -> dict[str, Any]:
    if plan_path is None or not plan_path.exists():
        return {"available": False, "reason": "PLAN_NOT_FOUND"}
    try:
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
    except Exception:
        return {"available": False, "reason": "PLAN_PARSE_FAILED"}
    if not isinstance(plan, dict):
        return {"available": False, "reason": "PLAN_INVALID"}

    window = plan.get("window", {}) if isinstance(plan.get("window"), dict) else {}
    start_ts_ms = _as_int(window.get("start_ts_ms"))
    end_ts_ms = _as_int(window.get("end_ts_ms"))
    if start_ts_ms is None:
        start_ts_ms = parse_utc_ts_ms(window.get("start_utc"))
    if end_ts_ms is None:
        end_ts_ms = parse_utc_ts_ms(window.get("end_utc"))
    if start_ts_ms is None or end_ts_ms is None:
        return {"available": False, "reason": "PLAN_WINDOW_MISSING"}

    base_dataset = str(plan.get("base_dataset") or "candles_v1")
    base_root = parquet_root / base_dataset
    api_root = parquet_root / dataset_name

    targets = [dict(item) for item in plan.get("targets", []) if isinstance(item, dict)]
    tf_set = tuple(sorted({str(item.get("tf", "")).strip().lower() for item in targets if str(item.get("tf", "")).strip()}))
    quote = str(plan.get("filters", {}).get("quote", "KRW")).strip().upper() if isinstance(plan.get("filters"), dict) else "KRW"

    base_inventory = build_candle_inventory(
        base_root,
        tf_filter=tf_set if tf_set else None,
        quote=quote,
        window_start_ts_ms=start_ts_ms,
        window_end_ts_ms=end_ts_ms,
    )
    api_inventory = build_candle_inventory(
        api_root,
        tf_filter=tf_set if tf_set else None,
        quote=quote,
        window_start_ts_ms=start_ts_ms,
        window_end_ts_ms=end_ts_ms,
    )
    base_map = {(row["market"], row["tf"]): row for row in base_inventory.get("entries", [])}
    api_map = {(row["market"], row["tf"]): row for row in api_inventory.get("entries", [])}

    seen_pairs: set[tuple[str, str]] = set()
    entries: list[dict[str, Any]] = []
    for target in targets:
        market = str(target.get("market", "")).strip().upper()
        tf = str(target.get("tf", "")).strip().lower()
        if not market or not tf:
            continue
        key = (market, tf)
        if key in seen_pairs:
            continue
        seen_pairs.add(key)

        before = base_map.get(key)
        extra = api_map.get(key)
        before_pct = float(before.get("coverage_pct", 0.0)) if before else 0.0
        after_pct = _merged_coverage_pct(before, extra, start_ts_ms=start_ts_ms, end_ts_ms=end_ts_ms)
        entries.append(
            {
                "market": market,
                "tf": tf,
                "coverage_before_pct": round(before_pct, 6),
                "coverage_after_pct": round(after_pct, 6),
                "coverage_delta_pct": round(after_pct - before_pct, 6),
            }
        )

    if not entries:
        return {"available": True, "entries": [], "average_before_pct": 0.0, "average_after_pct": 0.0}
    avg_before = sum(float(item["coverage_before_pct"]) for item in entries) / float(len(entries))
    avg_after = sum(float(item["coverage_after_pct"]) for item in entries) / float(len(entries))
    return {
        "available": True,
        "entries": entries,
        "average_before_pct": round(avg_before, 6),
        "average_after_pct": round(avg_after, 6),
        "average_delta_pct": round(avg_after - avg_before, 6),
    }


def _merged_coverage_pct(
    base_row: dict[str, Any] | None,
    extra_row: dict[str, Any] | None,
    *,
    start_ts_ms: int,
    end_ts_ms: int,
) -> float:
    min_candidates = [
        _as_int(base_row.get("min_ts_ms")) if base_row else None,
        _as_int(extra_row.get("min_ts_ms")) if extra_row else None,
    ]
    max_candidates = [
        _as_int(base_row.get("max_ts_ms")) if base_row else None,
        _as_int(extra_row.get("max_ts_ms")) if extra_row else None,
    ]
    mins = [int(item) for item in min_candidates if item is not None]
    maxs = [int(item) for item in max_candidates if item is not None]
    if not mins or not maxs:
        return 0.0
    min_ts_ms = min(mins)
    max_ts_ms = max(maxs)
    window_ms = max(int(end_ts_ms - start_ts_ms), 1)
    overlap_start = max(min_ts_ms, start_ts_ms)
    overlap_end = min(max_ts_ms, end_ts_ms)
    if overlap_end <= overlap_start:
        return 0.0
    return max(0.0, min(100.0, (float(overlap_end - overlap_start) / float(window_ms)) * 100.0))


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
