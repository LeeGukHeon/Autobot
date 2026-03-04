"""Validation for raw ticks (`jsonl.zst`) dataset."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from .ticks_writer import read_ticks_part_file


VALID_ASK_BID = {"ASK", "BID"}


@dataclass(frozen=True)
class TicksValidateSummary:
    checked_files: int
    ok_files: int
    warn_files: int
    fail_files: int
    rows_total: int
    schema_ok_ratio: float
    dup_ratio_overall: float
    validate_report_file: Path
    details: tuple[dict[str, Any], ...]


def validate_ticks_raw_dataset(
    *,
    raw_root: Path = Path("data/raw_ticks/upbit/trades"),
    report_path: Path = Path("data/raw_ticks/upbit/_meta/ticks_validate_report.json"),
    date_filter: str | None = None,
    dup_ratio_warn_threshold: float = 0.05,
) -> TicksValidateSummary:
    part_files = _discover_part_files(raw_root=raw_root, date_filter=date_filter)

    details: list[dict[str, Any]] = []
    ok_files = 0
    warn_files = 0
    fail_files = 0
    rows_total = 0
    rows_raw_total = 0
    rows_schema_ok = 0

    seq_counts: dict[tuple[str, int], int] = {}
    market_rows: dict[str, int] = {}
    market_dup_rows: dict[str, int] = {}

    for part_file in part_files:
        date_tag, market_tag = _partition_tags(part_file)
        try:
            raw_rows = read_ticks_part_file(part_file)
        except Exception as exc:
            detail = {
                "file": str(part_file),
                "date": date_tag,
                "market": market_tag,
                "rows_raw": 0,
                "rows_valid": 0,
                "schema_errors": 1,
                "dup_ratio": 0.0,
                "status": "FAIL",
                "reasons": ["READ_OR_PARSE_EXCEPTION"],
                "error_message": str(exc),
                "min_ts_ms": None,
                "max_ts_ms": None,
            }
            fail_files += 1
            details.append(detail)
            continue

        rows_raw_total += len(raw_rows)
        rows_valid = 0
        schema_errors = 0
        partition_mismatch = 0
        duplicate_rows_in_file = 0
        seen_seq: set[int] = set()
        min_ts_ms: int | None = None
        max_ts_ms: int | None = None

        for row in raw_rows:
            normalized, valid = _validate_row(row)
            if not valid:
                schema_errors += 1
                continue

            rows_valid += 1
            rows_schema_ok += 1
            rows_total += 1
            ts_ms = int(normalized["timestamp_ms"])
            seq = int(normalized["sequential_id"])
            market = str(normalized["market"])
            date_utc = _date_utc_from_ts_ms(ts_ms)

            if market != market_tag or date_utc != date_tag:
                partition_mismatch += 1

            key = (market, seq)
            current_count = seq_counts.get(key, 0) + 1
            seq_counts[key] = current_count
            if current_count > 1:
                market_dup_rows[market] = int(market_dup_rows.get(market, 0) + 1)

            market_rows[market] = int(market_rows.get(market, 0) + 1)
            if seq in seen_seq:
                duplicate_rows_in_file += 1
            else:
                seen_seq.add(seq)

            if min_ts_ms is None or ts_ms < min_ts_ms:
                min_ts_ms = ts_ms
            if max_ts_ms is None or ts_ms > max_ts_ms:
                max_ts_ms = ts_ms

        reasons: list[str] = []
        status = "OK"
        dup_ratio = (float(duplicate_rows_in_file) / float(rows_valid)) if rows_valid > 0 else 0.0
        if schema_errors > 0:
            status = "FAIL"
            reasons.append("SCHEMA_INVALID")
        elif rows_valid <= 0:
            status = "WARN"
            reasons.append("NO_ROWS_COLLECTED")
        else:
            if partition_mismatch > 0:
                status = "WARN"
                reasons.append("PARTITION_MISMATCH")
            if dup_ratio >= float(dup_ratio_warn_threshold):
                status = "WARN"
                reasons.append("DUP_RATIO_HIGH")

        detail = {
            "file": str(part_file),
            "date": date_tag,
            "market": market_tag,
            "rows_raw": len(raw_rows),
            "rows_valid": rows_valid,
            "schema_errors": schema_errors,
            "partition_mismatch": partition_mismatch,
            "dup_ratio": round(dup_ratio, 8),
            "status": status,
            "reasons": reasons,
            "min_ts_ms": min_ts_ms,
            "max_ts_ms": max_ts_ms,
        }
        details.append(detail)

        if status == "OK":
            ok_files += 1
        elif status == "WARN":
            warn_files += 1
        else:
            fail_files += 1

    dup_rows_total = sum(max(count - 1, 0) for count in seq_counts.values())
    dup_ratio_overall = (float(dup_rows_total) / float(rows_total)) if rows_total > 0 else 0.0
    schema_ok_ratio = (float(rows_schema_ok) / float(rows_raw_total)) if rows_raw_total > 0 else 1.0

    dup_ratio_by_market = [
        {
            "market": market,
            "rows": int(market_rows.get(market, 0)),
            "dup_rows": int(market_dup_rows.get(market, 0)),
            "dup_ratio": (
                float(market_dup_rows.get(market, 0)) / float(market_rows.get(market, 1))
                if int(market_rows.get(market, 0)) > 0
                else 0.0
            ),
        }
        for market in sorted(market_rows)
    ]

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "raw_root": str(raw_root),
        "date_filter": date_filter,
        "checked_files": len(details),
        "ok_files": ok_files,
        "warn_files": warn_files,
        "fail_files": fail_files,
        "rows_raw_total": rows_raw_total,
        "rows_valid_total": rows_total,
        "schema_ok_ratio": round(schema_ok_ratio, 8),
        "dup_ratio_overall": round(dup_ratio_overall, 8),
        "dup_ratio_warn_threshold": float(dup_ratio_warn_threshold),
        "dup_ratio_by_market": dup_ratio_by_market,
        "details": details,
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    return TicksValidateSummary(
        checked_files=len(details),
        ok_files=ok_files,
        warn_files=warn_files,
        fail_files=fail_files,
        rows_total=rows_total,
        schema_ok_ratio=float(schema_ok_ratio),
        dup_ratio_overall=float(dup_ratio_overall),
        validate_report_file=report_path,
        details=tuple(details),
    )


def _discover_part_files(*, raw_root: Path, date_filter: str | None) -> list[Path]:
    if date_filter:
        return sorted(path for path in raw_root.glob(f"date={date_filter}/market=*/*.jsonl.zst") if path.is_file())
    return sorted(path for path in raw_root.glob("date=*/market=*/*.jsonl.zst") if path.is_file())


def _partition_tags(path: Path) -> tuple[str, str]:
    date_tag = path.parent.parent.name.replace("date=", "", 1)
    market_tag = path.parent.name.replace("market=", "", 1).upper()
    return date_tag, market_tag


def _validate_row(row: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    if not isinstance(row, dict):
        return {}, False
    market = _as_str(row.get("market"), upper=True)
    timestamp_ms = _as_int(row.get("timestamp_ms"))
    trade_price = _as_float(row.get("trade_price"))
    trade_volume = _as_float(row.get("trade_volume"))
    ask_bid = _as_str(row.get("ask_bid"), upper=True)
    sequential_id = _as_int(row.get("sequential_id"))
    days_ago = _as_int(row.get("days_ago"))
    collected_at_ms = _as_int(row.get("collected_at_ms"))

    if not market or timestamp_ms is None:
        return {}, False
    if trade_price is None or trade_volume is None:
        return {}, False
    if ask_bid not in VALID_ASK_BID:
        return {}, False
    if sequential_id is None or days_ago is None or collected_at_ms is None:
        return {}, False
    if days_ago < 1 or days_ago > 7:
        return {}, False

    normalized = {
        "market": market,
        "timestamp_ms": int(timestamp_ms),
        "trade_price": float(trade_price),
        "trade_volume": float(trade_volume),
        "ask_bid": ask_bid,
        "sequential_id": int(sequential_id),
        "days_ago": int(days_ago),
        "collected_at_ms": int(collected_at_ms),
    }
    return normalized, True


def _date_utc_from_ts_ms(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc)
    return dt.date().isoformat()


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


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_str(value: Any, *, upper: bool) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text.upper() if upper else text
