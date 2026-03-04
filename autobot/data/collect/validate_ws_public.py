"""Validation for raw public websocket (`jsonl.zst`) dataset."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import time
from typing import Any

from .ws_public_writer import read_ws_part_file


VALID_ASK_BID = {"ASK", "BID"}


@dataclass(frozen=True)
class WsPublicValidateSummary:
    checked_files: int
    ok_files: int
    warn_files: int
    fail_files: int
    rows_total: int
    parse_ok_ratio: float
    validate_report_file: Path
    details: tuple[dict[str, Any], ...]
    quarantined_files: int = 0
    quarantine_report_file: Path | None = None


def validate_ws_public_raw_dataset(
    *,
    raw_root: Path = Path("data/raw_ws/upbit/public"),
    meta_dir: Path = Path("data/raw_ws/upbit/_meta"),
    report_path: Path | None = None,
    date_filter: str | None = None,
    quarantine_corrupt: bool = False,
    quarantine_dir: Path | None = None,
    min_age_sec: int = 300,
) -> WsPublicValidateSummary:
    output_path = report_path or (meta_dir / "ws_validate_report.json")
    quarantine_root = quarantine_dir if quarantine_dir is not None else (raw_root.parent / "_quarantine")
    quarantine_report_path = meta_dir / "ws_quarantine_report.json"
    part_files = _discover_part_files(raw_root=raw_root, date_filter=date_filter)

    details: list[dict[str, Any]] = []
    quarantine_events: list[dict[str, Any]] = []
    ok_files = 0
    warn_files = 0
    fail_files = 0
    rows_total = 0
    rows_raw_total = 0
    rows_schema_ok = 0

    per_market_counts: dict[str, dict[str, int]] = {}
    coverage: dict[str, dict[str, dict[str, int | None]]] = {"trade": {}, "orderbook": {}}

    for part_file in part_files:
        channel_tag, date_tag, hour_tag = _partition_tags(part_file)
        try:
            raw_rows = read_ws_part_file(part_file)
        except Exception as exc:
            error_message = str(exc)
            quarantined_detail: dict[str, Any] | None = None
            if quarantine_corrupt and _is_zstd_corrupt_error(exc):
                file_age_sec = _file_age_sec(path=part_file)
                if file_age_sec is not None and file_age_sec >= max(int(min_age_sec), 0):
                    quarantined_entry = _quarantine_corrupt_part(
                        part_file=part_file,
                        raw_root=raw_root,
                        quarantine_root=quarantine_root,
                        reason="ZSTD_CORRUPT",
                        error_message=error_message,
                    )
                    quarantine_events.append(quarantined_entry)
                    quarantined_detail = {
                        "file": str(part_file),
                        "channel": channel_tag,
                        "date": date_tag,
                        "hour": hour_tag,
                        "rows_raw": 0,
                        "rows_valid": 0,
                        "schema_errors": 0,
                        "status": "WARN",
                        "reasons": ["QUARANTINED_ZSTD_CORRUPT"],
                        "error_message": error_message,
                        "min_ts_ms": None,
                        "max_ts_ms": None,
                        "quarantine": quarantined_entry,
                    }
                else:
                    quarantined_detail = {
                        "file": str(part_file),
                        "channel": channel_tag,
                        "date": date_tag,
                        "hour": hour_tag,
                        "rows_raw": 0,
                        "rows_valid": 0,
                        "schema_errors": 1,
                        "status": "FAIL",
                        "reasons": ["READ_OR_PARSE_EXCEPTION", "ZSTD_CORRUPT_TOO_FRESH_FOR_QUARANTINE"],
                        "error_message": error_message,
                        "min_ts_ms": None,
                        "max_ts_ms": None,
                        "quarantine_attempted": True,
                        "min_age_sec": int(max(int(min_age_sec), 0)),
                        "file_age_sec": file_age_sec,
                    }
            if quarantined_detail is not None:
                if quarantined_detail.get("status") == "WARN":
                    warn_files += 1
                else:
                    fail_files += 1
                details.append(quarantined_detail)
                continue

            detail = {
                "file": str(part_file),
                "channel": channel_tag,
                "date": date_tag,
                "hour": hour_tag,
                "rows_raw": 0,
                "rows_valid": 0,
                "schema_errors": 1,
                "status": "FAIL",
                "reasons": ["READ_OR_PARSE_EXCEPTION"],
                "error_message": error_message,
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
        min_ts_ms: int | None = None
        max_ts_ms: int | None = None

        for row in raw_rows:
            normalized, valid = _validate_row(row=row, expected_channel=channel_tag)
            if not valid:
                schema_errors += 1
                continue

            rows_valid += 1
            rows_schema_ok += 1
            rows_total += 1
            ts_ms = int(normalized["event_ts_ms"])
            market = str(normalized["market"])
            channel = str(normalized["channel"])
            date_utc = _date_utc_from_ts_ms(ts_ms)

            if date_utc != date_tag:
                partition_mismatch += 1

            if min_ts_ms is None or ts_ms < min_ts_ms:
                min_ts_ms = ts_ms
            if max_ts_ms is None or ts_ms > max_ts_ms:
                max_ts_ms = ts_ms

            market_bucket = per_market_counts.setdefault(market, {"trade": 0, "orderbook": 0})
            market_bucket[channel] = int(market_bucket.get(channel, 0) + 1)

            market_coverage = coverage[channel].setdefault(market, {"min_ts_ms": None, "max_ts_ms": None})
            if market_coverage["min_ts_ms"] is None or ts_ms < int(market_coverage["min_ts_ms"]):
                market_coverage["min_ts_ms"] = ts_ms
            if market_coverage["max_ts_ms"] is None or ts_ms > int(market_coverage["max_ts_ms"]):
                market_coverage["max_ts_ms"] = ts_ms

        reasons: list[str] = []
        status = "OK"
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

        detail = {
            "file": str(part_file),
            "channel": channel_tag,
            "date": date_tag,
            "hour": hour_tag,
            "rows_raw": len(raw_rows),
            "rows_valid": rows_valid,
            "schema_errors": schema_errors,
            "partition_mismatch": partition_mismatch,
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

    parse_ok_ratio = (float(rows_schema_ok) / float(rows_raw_total)) if rows_raw_total > 0 else 1.0
    per_channel_messages = {
        "trade": int(sum(item.get("trade", 0) for item in per_market_counts.values())),
        "orderbook": int(sum(item.get("orderbook", 0) for item in per_market_counts.values())),
    }
    coverage_payload = _coverage_payload(coverage)
    zero_markets = _warn_zero_markets(meta_dir=meta_dir, per_market_counts=per_market_counts)

    collect_report = _load_json(meta_dir / "ws_collect_report.json")
    downsample_ratio = 0.0
    if isinstance(collect_report, dict):
        received_ob = int(_to_int(collect_report.get("received_orderbook")) or 0)
        dropped_ob = int(_to_int(collect_report.get("dropped_orderbook_by_interval")) or 0)
        if received_ob > 0:
            downsample_ratio = float(dropped_ob) / float(received_ob)

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "raw_root": str(raw_root),
        "meta_dir": str(meta_dir),
        "date_filter": date_filter,
        "quarantine_corrupt": bool(quarantine_corrupt),
        "quarantine_dir": str(quarantine_root),
        "quarantine_min_age_sec": int(max(int(min_age_sec), 0)),
        "quarantined_files": int(len(quarantine_events)),
        "checked_files": len(details),
        "ok_files": ok_files,
        "warn_files": warn_files,
        "fail_files": fail_files,
        "rows_raw_total": rows_raw_total,
        "rows_valid_total": rows_total,
        "parse_ok_ratio": round(parse_ok_ratio, 8),
        "per_channel_messages": per_channel_messages,
        "per_market_counts": [
            {
                "market": market,
                "trade": int(counts.get("trade", 0)),
                "orderbook": int(counts.get("orderbook", 0)),
            }
            for market, counts in sorted(per_market_counts.items())
        ],
        "time_coverage": coverage_payload,
        "downsample_applied_ratio": round(downsample_ratio, 8),
        "zero_rows_markets_warn": zero_markets,
        "details": details,
    }
    if quarantine_events:
        report["quarantine_events"] = quarantine_events
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    if quarantine_events:
        _append_quarantine_report(
            report_path=quarantine_report_path,
            quarantine_dir=quarantine_root,
            entries=quarantine_events,
        )

    return WsPublicValidateSummary(
        checked_files=len(details),
        ok_files=ok_files,
        warn_files=warn_files + len(zero_markets),
        fail_files=fail_files,
        rows_total=rows_total,
        parse_ok_ratio=float(parse_ok_ratio),
        validate_report_file=output_path,
        details=tuple(details),
        quarantined_files=len(quarantine_events),
        quarantine_report_file=(quarantine_report_path if quarantine_events else None),
    )


def _is_zstd_corrupt_error(exc: Exception) -> bool:
    text = str(exc).strip().lower()
    type_text = str(type(exc)).strip().lower()
    return any(
        marker in text or marker in type_text
        for marker in (
            "zstd",
            "zstandard",
            "frame descriptor",
            "decompress",
            "dictionary",
        )
    )


def _file_age_sec(*, path: Path) -> int | None:
    try:
        mtime = path.stat().st_mtime
    except Exception:
        return None
    age = time.time() - float(mtime)
    if age < 0:
        return 0
    return int(age)


def _quarantine_corrupt_part(
    *,
    part_file: Path,
    raw_root: Path,
    quarantine_root: Path,
    reason: str,
    error_message: str,
) -> dict[str, Any]:
    relative = _quarantine_relative_path(part_file=part_file, raw_root=raw_root)
    destination = quarantine_root / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        destination = destination.with_name(f"{destination.stem}-{stamp}{destination.suffix}")
    part_file.replace(destination)
    return {
        "original_path": str(part_file),
        "new_path": str(destination),
        "reason": str(reason).strip().upper() or "ZSTD_CORRUPT",
        "error": error_message,
        "moved_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


def _quarantine_relative_path(*, part_file: Path, raw_root: Path) -> Path:
    try:
        return part_file.relative_to(raw_root)
    except ValueError:
        return Path(part_file.name)


def _append_quarantine_report(
    *,
    report_path: Path,
    quarantine_dir: Path,
    entries: list[dict[str, Any]],
) -> None:
    existing: dict[str, Any] = {}
    if report_path.exists():
        try:
            payload = json.loads(report_path.read_text(encoding="utf-8"))
        except Exception:
            payload = {}
        if isinstance(payload, dict):
            existing = payload
    old_entries = existing.get("entries")
    merged: list[dict[str, Any]] = list(old_entries) if isinstance(old_entries, list) else []
    merged.extend(entries)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "quarantine_dir": str(quarantine_dir),
        "total_entries": len(merged),
        "new_entries": len(entries),
        "entries": merged,
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _discover_part_files(*, raw_root: Path, date_filter: str | None) -> list[Path]:
    if date_filter:
        patterns = [
            f"trade/date={date_filter}/hour=*/*.jsonl.zst",
            f"orderbook/date={date_filter}/hour=*/*.jsonl.zst",
        ]
    else:
        patterns = [
            "trade/date=*/hour=*/*.jsonl.zst",
            "orderbook/date=*/hour=*/*.jsonl.zst",
        ]
    files: list[Path] = []
    for pattern in patterns:
        files.extend(path for path in raw_root.glob(pattern) if path.is_file())
    return sorted(files)


def _partition_tags(path: Path) -> tuple[str, str, str]:
    hour_tag = path.parent.name.replace("hour=", "", 1)
    date_tag = path.parent.parent.name.replace("date=", "", 1)
    channel_tag = path.parent.parent.parent.name.strip().lower()
    return channel_tag, date_tag, hour_tag


def _validate_row(*, row: dict[str, Any], expected_channel: str) -> tuple[dict[str, Any], bool]:
    if not isinstance(row, dict):
        return {}, False
    channel = _as_str(row.get("channel"), upper=False)
    market = _as_str(row.get("market"), upper=True)
    source = _as_str(row.get("source"), upper=False)
    collected_at_ms = _to_int(row.get("collected_at_ms"))
    if channel is None or market is None or source is None or collected_at_ms is None:
        return {}, False
    if channel != expected_channel:
        return {}, False
    if source.lower() != "ws":
        return {}, False

    if channel == "trade":
        ts_ms = _to_int(row.get("trade_ts_ms"))
        price = _to_float(row.get("price"))
        volume = _to_float(row.get("volume"))
        ask_bid = _as_str(row.get("ask_bid"), upper=True)
        if ts_ms is None or price is None or volume is None:
            return {}, False
        if ask_bid not in VALID_ASK_BID:
            return {}, False
        return {
            "channel": channel,
            "market": market,
            "event_ts_ms": int(ts_ms),
        }, True

    if channel == "orderbook":
        ts_ms = _to_int(row.get("ts_ms"))
        topk = _to_int(row.get("topk"))
        if ts_ms is None or topk is None or topk < 1:
            return {}, False
        return {
            "channel": channel,
            "market": market,
            "event_ts_ms": int(ts_ms),
        }, True

    return {}, False


def _coverage_payload(coverage: dict[str, dict[str, dict[str, int | None]]]) -> dict[str, list[dict[str, Any]]]:
    payload: dict[str, list[dict[str, Any]]] = {}
    for channel, markets in coverage.items():
        rows: list[dict[str, Any]] = []
        for market, span in sorted(markets.items()):
            min_ts = _to_int(span.get("min_ts_ms"))
            max_ts = _to_int(span.get("max_ts_ms"))
            rows.append(
                {
                    "market": market,
                    "min_ts_ms": min_ts,
                    "max_ts_ms": max_ts,
                    "min_utc": _to_utc_text(min_ts),
                    "max_utc": _to_utc_text(max_ts),
                }
            )
        payload[channel] = rows
    return payload


def _warn_zero_markets(*, meta_dir: Path, per_market_counts: dict[str, dict[str, int]]) -> list[dict[str, Any]]:
    plan_path = meta_dir / "ws_public_plan.json"
    if not plan_path.exists():
        return []
    try:
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(plan, dict):
        return []
    codes_raw = plan.get("codes") or plan.get("selected_markets")
    codes = [str(item).strip().upper() for item in codes_raw if str(item).strip()] if isinstance(codes_raw, list) else []
    warnings: list[dict[str, Any]] = []
    for market in sorted(set(codes)):
        counts = per_market_counts.get(market, {})
        total = int(counts.get("trade", 0)) + int(counts.get("orderbook", 0))
        if total <= 0:
            warnings.append(
                {
                    "market": market,
                    "status": "WARN",
                    "reason": "NO_ROWS_RECEIVED_MARKET",
                }
            )
    return warnings


def _date_utc_from_ts_ms(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc)
    return dt.date().isoformat()


def _to_utc_text(ts_ms: int | None) -> str | None:
    if ts_ms is None:
        return None
    dt = datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc)
    return dt.isoformat(timespec="seconds")


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


def _to_float(value: Any) -> float | None:
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
