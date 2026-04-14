"""Plan-driven collector for Upbit REST trades/ticks data."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
import shutil
import time
from typing import Any

from ...upbit import RateLimitError, ValidationError, load_upbit_settings
from .plan_ticks import TicksPlanOptions, generate_ticks_collection_plan
from .ticks_checkpoint import load_ticks_checkpoint, save_ticks_checkpoint, update_ticks_checkpoint
from .ticks_manifest import append_ticks_manifest_rows
from .ticks_writer import write_ticks_partitions
from .upbit_ticks_client import TicksFetchResult, UpbitTicksClient


@dataclass(frozen=True)
class TicksCollectOptions:
    plan_path: Path = Path("data/raw_ticks/upbit/_meta/ticks_plan.json")
    raw_root: Path = Path("data/raw_ticks/upbit/trades")
    meta_dir: Path = Path("data/raw_ticks/upbit/_meta")
    parquet_root: Path = Path("data/parquet")
    base_dataset: str = "candles_v1"
    quote: str = "KRW"
    market_mode: str = "top_n_by_recent_value_est"
    top_n: int = 20
    fixed_markets: tuple[str, ...] | None = None
    days_ago: tuple[int, ...] = (1, 2, 3, 4, 5, 6, 7)
    mode: str = "backfill"
    dry_run: bool = False
    workers: int = 1
    rate_limit_strict: bool = True
    max_pages_per_target: int | None = None
    max_requests: int | None = None
    retention_days: int = 30
    config_dir: Path = Path("config")

    @property
    def collect_report_path(self) -> Path:
        return self.meta_dir / "ticks_collect_report.json"

    @property
    def validate_report_path(self) -> Path:
        return self.meta_dir / "ticks_validate_report.json"

    @property
    def manifest_path(self) -> Path:
        return self.meta_dir / "ticks_manifest.parquet"

    @property
    def checkpoint_path(self) -> Path:
        return self.meta_dir / "ticks_checkpoint.json"


@dataclass(frozen=True)
class TicksCollectSummary:
    discovered_targets: int
    selected_targets: int
    processed_targets: int
    ok_targets: int
    warn_targets: int
    fail_targets: int
    calls_made: int
    throttled_count: int
    backoff_count: int
    rows_collected_total: int
    workers_effective: int
    run_id: str
    plan_file: Path
    manifest_file: Path
    collect_report_file: Path
    checkpoint_file: Path
    details: tuple[dict[str, Any], ...]
    failures: tuple[dict[str, Any], ...]


def collect_ticks_from_plan(
    options: TicksCollectOptions,
    *,
    client: UpbitTicksClient | None = None,
) -> TicksCollectSummary:
    started_at = int(time.time())
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    plan, plan_path = _load_or_build_plan(options)
    targets = [dict(item) for item in plan.get("targets", []) if isinstance(item, dict)]
    discovered_targets = len(targets)
    selected_targets = discovered_targets
    if options.max_requests is not None and int(options.max_requests) <= 0:
        selected_targets = 0

    workers_effective = 1
    calls_made_total = 0
    throttled_count_total = 0
    backoff_count_total = 0
    rows_collected_total = 0
    ok_targets = 0
    warn_targets = 0
    fail_targets = 0
    processed_targets = 0

    details: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    manifest_rows: list[dict[str, Any]] = []
    no_rows_targets: list[str] = []

    checkpoint = load_ticks_checkpoint(options.checkpoint_path)
    abort_after_target = False
    client_instance = client
    if not options.dry_run and client_instance is None:
        settings = load_upbit_settings(options.config_dir)
        client_instance = UpbitTicksClient(settings=settings)

    max_requests_global = int(options.max_requests) if options.max_requests is not None else None
    for index, target in enumerate(targets, start=1):
        if abort_after_target:
            break
        if max_requests_global is not None and calls_made_total >= max_requests_global:
            break

        detail = _target_detail(index=index, target=target)
        processed_targets += 1
        market = detail["market"]
        days_ago = int(detail["days_ago"])
        target_date = _expected_target_date(days_ago)
        checkpoint_key = f"{market}|{target_date}"
        checkpoint_entry = checkpoint.get(checkpoint_key) if isinstance(checkpoint.get(checkpoint_key), dict) else {}
        start_cursor = _as_str(checkpoint_entry.get("last_cursor")) if checkpoint_entry else None
        detail["start_cursor"] = start_cursor
        detail["target_date"] = target_date

        if options.dry_run:
            detail["status"] = "DRY_RUN"
            detail["reasons"] = []
            details.append(detail)
            continue

        assert client_instance is not None
        now_ms = int(time.time() * 1000)
        max_requests_remaining = (
            max(max_requests_global - calls_made_total, 0) if max_requests_global is not None else None
        )
        try:
            result = client_instance.fetch_trades_ticks(
                market=market,
                days_ago=days_ago,
                start_cursor=start_cursor,
                max_pages=options.max_pages_per_target,
                max_requests=max_requests_remaining,
                count=200,
            )
            detail.update(_result_detail_payload(result))
            calls_made_total += int(result.calls_made)
            throttled_count_total += int(result.throttled_count)
            backoff_count_total += int(result.backoff_count)
            rows_collected_total += int(result.unique_rows)

            reasons: list[str] = []
            status = "OK"
            if result.unique_rows <= 0:
                status = "WARN"
                reasons.append("NO_ROWS_COLLECTED")
                no_rows_targets.append(checkpoint_key)
            if result.loop_guard_triggered:
                status = "WARN"
                reasons.append("PAGING_LOOP_GUARD_TRIGGERED")
            if result.truncated_by_budget:
                status = "WARN"
                reasons.append("MAX_REQUEST_BUDGET_REACHED")

            detail["status"] = status
            detail["reasons"] = reasons

            if result.ticks:
                target_run_id = f"{run_id}-t{index:04d}"
                written_parts = write_ticks_partitions(
                    raw_root=options.raw_root,
                    ticks=result.ticks,
                    run_id=target_run_id,
                )
                detail["rows_written"] = int(sum(int(item.get("rows", 0)) for item in written_parts))
                detail["part_files"] = [str(item.get("part_file")) for item in written_parts]
                if written_parts:
                    detail["min_ts_ms"] = min(int(item["min_ts_ms"]) for item in written_parts)
                    detail["max_ts_ms"] = max(int(item["max_ts_ms"]) for item in written_parts)

                manifest_rows.extend(
                    _manifest_rows_from_parts(
                        run_id=run_id,
                        days_ago=days_ago,
                        result=result,
                        status=status,
                        reasons=reasons,
                        collected_at_ms=now_ms,
                        parts=written_parts,
                    )
                )
            else:
                manifest_rows.append(
                    {
                        "run_id": run_id,
                        "date": _expected_target_date(days_ago),
                        "target_date": target_date,
                        "market": market,
                        "days_ago": days_ago,
                        "rows": 0,
                        "min_ts_ms": None,
                        "max_ts_ms": None,
                        "dup_ratio": _dup_ratio(result),
                        "status": status,
                        "reasons_json": json.dumps(reasons, ensure_ascii=False),
                        "calls_made": int(result.calls_made),
                        "pages_collected": int(result.pages_collected),
                        "part_file": "",
                        "error_message": None,
                        "collected_at_ms": now_ms,
                    }
                )

            update_ticks_checkpoint(
                checkpoint,
                market=market,
                days_ago=days_ago,
                target_date=target_date,
                last_cursor=result.last_cursor,
                last_success_ts_ms=now_ms,
                pages_collected=result.pages_collected,
                updated_at_ms=now_ms,
            )
            save_ticks_checkpoint(options.checkpoint_path, checkpoint)

            if status == "OK":
                ok_targets += 1
            elif status == "WARN":
                warn_targets += 1
            else:
                fail_targets += 1
                failures.append(detail)
        except RateLimitError as exc:
            detail["status"] = "FAIL"
            detail["error_message"] = str(exc)
            detail["calls_made"] = int(detail.get("calls_made", 0))
            detail["throttled_count"] = int(detail.get("throttled_count", 0))
            detail["backoff_count"] = int(detail.get("backoff_count", 0))
            reason = "RATE_LIMIT_BANNED_418" if bool(exc.banned) else "RATE_LIMIT_EXCEPTION"
            detail["reasons"] = [reason]
            fail_targets += 1
            failures.append(detail)
            manifest_rows.append(
                {
                    "run_id": run_id,
                    "date": _expected_target_date(days_ago),
                    "target_date": target_date,
                    "market": market,
                    "days_ago": days_ago,
                    "rows": 0,
                    "min_ts_ms": None,
                    "max_ts_ms": None,
                    "dup_ratio": None,
                    "status": "FAIL",
                    "reasons_json": json.dumps([reason], ensure_ascii=False),
                    "calls_made": int(detail.get("calls_made", 0)),
                    "pages_collected": 0,
                    "part_file": "",
                    "error_message": str(exc),
                    "collected_at_ms": int(time.time() * 1000),
                }
            )
            if bool(exc.banned):
                abort_after_target = True
        except Exception as exc:
            if _is_terminal_market_unavailable(exc):
                detail["status"] = "WARN"
                detail["error_message"] = str(exc)
                detail["calls_made"] = int(detail.get("calls_made", 0))
                detail["throttled_count"] = int(detail.get("throttled_count", 0))
                detail["backoff_count"] = int(detail.get("backoff_count", 0))
                detail["reasons"] = ["DELISTED_OR_INACTIVE_MARKET"]
                warn_targets += 1
                manifest_rows.append(
                    {
                        "run_id": run_id,
                        "date": _expected_target_date(days_ago),
                        "target_date": target_date,
                        "market": market,
                        "days_ago": days_ago,
                        "rows": 0,
                        "min_ts_ms": None,
                        "max_ts_ms": None,
                        "dup_ratio": None,
                        "status": "WARN",
                        "reasons_json": json.dumps(["DELISTED_OR_INACTIVE_MARKET"], ensure_ascii=False),
                        "calls_made": int(detail.get("calls_made", 0)),
                        "pages_collected": 0,
                        "part_file": "",
                        "error_message": str(exc),
                        "collected_at_ms": int(time.time() * 1000),
                    }
                )
                details.append(detail)
                continue

            detail["status"] = "FAIL"
            detail["error_message"] = str(exc)
            detail["calls_made"] = int(detail.get("calls_made", 0))
            detail["throttled_count"] = int(detail.get("throttled_count", 0))
            detail["backoff_count"] = int(detail.get("backoff_count", 0))
            detail["reasons"] = ["COLLECT_EXCEPTION"]
            fail_targets += 1
            failures.append(detail)
            manifest_rows.append(
                {
                    "run_id": run_id,
                    "date": _expected_target_date(days_ago),
                    "target_date": target_date,
                    "market": market,
                    "days_ago": days_ago,
                    "rows": 0,
                    "min_ts_ms": None,
                    "max_ts_ms": None,
                    "dup_ratio": None,
                    "status": "FAIL",
                    "reasons_json": json.dumps(["COLLECT_EXCEPTION"], ensure_ascii=False),
                    "calls_made": int(detail.get("calls_made", 0)),
                    "pages_collected": 0,
                    "part_file": "",
                    "error_message": str(exc),
                    "collected_at_ms": int(time.time() * 1000),
                }
            )

        details.append(detail)

    if not options.dry_run:
        append_ticks_manifest_rows(options.manifest_path, manifest_rows)

    pruned_dates: list[str] = []
    if not options.dry_run and options.retention_days > 0:
        pruned_dates = _prune_retention(raw_root=options.raw_root, retention_days=max(int(options.retention_days), 1))

    collect_report = {
        "started_at": started_at,
        "finished_at": int(time.time()),
        "plan_file": str(plan_path),
        "run_id": run_id,
        "raw_root": str(options.raw_root),
        "meta_dir": str(options.meta_dir),
        "dry_run": bool(options.dry_run),
        "workers_requested": int(options.workers),
        "workers_effective": workers_effective,
        "rate_limit_strict": bool(options.rate_limit_strict),
        "mode": str(options.mode).strip().lower(),
        "discovered_targets": discovered_targets,
        "selected_targets": selected_targets,
        "processed_targets": processed_targets if options.dry_run else len(details),
        "ok_targets": ok_targets,
        "warn_targets": warn_targets,
        "fail_targets": fail_targets,
        "calls_made": calls_made_total,
        "throttled_count": throttled_count_total,
        "backoff_count": backoff_count_total,
        "rows_collected_total": rows_collected_total,
        "no_rows_targets_count": len(no_rows_targets),
        "no_rows_targets_sample": no_rows_targets[:20],
        "retention_pruned_dates": pruned_dates,
        "details": details,
        "failures": failures,
        "manifest_file": str(options.manifest_path),
        "checkpoint_file": str(options.checkpoint_path),
    }
    options.collect_report_path.parent.mkdir(parents=True, exist_ok=True)
    options.collect_report_path.write_text(
        json.dumps(collect_report, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    return TicksCollectSummary(
        discovered_targets=discovered_targets,
        selected_targets=selected_targets,
        processed_targets=(processed_targets if options.dry_run else len(details)),
        ok_targets=ok_targets,
        warn_targets=warn_targets,
        fail_targets=fail_targets,
        calls_made=calls_made_total,
        throttled_count=throttled_count_total,
        backoff_count=backoff_count_total,
        rows_collected_total=rows_collected_total,
        workers_effective=workers_effective,
        run_id=run_id,
        plan_file=plan_path,
        manifest_file=options.manifest_path,
        collect_report_file=options.collect_report_path,
        checkpoint_file=options.checkpoint_path,
        details=tuple(details),
        failures=tuple(failures),
    )


def _is_terminal_market_unavailable(exc: Exception) -> bool:
    if not isinstance(exc, ValidationError):
        return False
    if int(getattr(exc, "status_code", 0) or 0) != 404:
        return False
    endpoint = str(getattr(exc, "endpoint", "") or "").strip().lower()
    if "/v1/trades/ticks" not in endpoint:
        return False
    message = str(getattr(exc, "message", "") or exc).strip().lower()
    if "code not found" in message:
        return True
    if "market not found" in message:
        return True
    return False


def _load_or_build_plan(options: TicksCollectOptions) -> tuple[dict[str, Any], Path]:
    plan_path = options.plan_path
    if plan_path.exists():
        raw = json.loads(plan_path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("ticks plan file must contain JSON object")
        return raw, plan_path

    plan = generate_ticks_collection_plan(
        TicksPlanOptions(
            parquet_root=options.parquet_root,
            base_dataset=options.base_dataset,
            output_path=plan_path,
            quote=options.quote,
            market_mode=options.market_mode,
            top_n=max(int(options.top_n), 1),
            fixed_markets=options.fixed_markets,
            days_ago=tuple(int(item) for item in options.days_ago),
        )
    )
    return plan, plan_path


def _target_detail(*, index: int, target: dict[str, Any]) -> dict[str, Any]:
    market = str(target.get("market", "")).strip().upper()
    days_ago = _as_int(target.get("days_ago"))
    if not market:
        raise ValueError(f"target[{index}] market is required")
    if days_ago is None or days_ago < 1 or days_ago > 7:
        raise ValueError(f"target[{index}] days_ago must be between 1 and 7")
    return {
        "idx": index,
        "market": market,
        "days_ago": int(days_ago),
        "target_key": str(target.get("target_key") or f"{market}|{days_ago}"),
        "reason": str(target.get("reason") or ""),
    }


def _result_detail_payload(result: TicksFetchResult) -> dict[str, Any]:
    return {
        "calls_made": int(result.calls_made),
        "throttled_count": int(result.throttled_count),
        "backoff_count": int(result.backoff_count),
        "pages_collected": int(result.pages_collected),
        "rows_fetched_raw": int(result.raw_rows),
        "rows_fetched_unique": int(result.unique_rows),
        "loop_guard_triggered": bool(result.loop_guard_triggered),
        "truncated_by_budget": bool(result.truncated_by_budget),
        "start_cursor": result.start_cursor,
        "last_cursor": result.last_cursor,
        "min_ts_ms": result.min_ts_ms,
        "max_ts_ms": result.max_ts_ms,
        "dup_ratio_internal": round(_dup_ratio(result), 8),
    }


def _manifest_rows_from_parts(
    *,
    run_id: str,
    days_ago: int,
    result: TicksFetchResult,
    status: str,
    reasons: list[str],
    collected_at_ms: int,
    parts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    dup_ratio = _dup_ratio(result)
    rows: list[dict[str, Any]] = []
    for part in parts:
        rows.append(
            {
                "run_id": run_id,
                "date": str(part.get("date")),
                "market": str(part.get("market")),
                "days_ago": int(days_ago),
                "rows": int(part.get("rows", 0)),
                "min_ts_ms": _as_int(part.get("min_ts_ms")),
                "max_ts_ms": _as_int(part.get("max_ts_ms")),
                "dup_ratio": float(dup_ratio),
                "status": status,
                "reasons_json": json.dumps(reasons, ensure_ascii=False),
                "calls_made": int(result.calls_made),
                "pages_collected": int(result.pages_collected),
                "part_file": str(part.get("part_file") or ""),
                "error_message": None,
                "collected_at_ms": int(collected_at_ms),
            }
        )
    return rows


def _prune_retention(*, raw_root: Path, retention_days: int) -> list[str]:
    if not raw_root.exists():
        return []
    cutoff = datetime.now(timezone.utc).date() - timedelta(days=max(int(retention_days), 1))
    removed: list[str] = []
    for date_dir in sorted(raw_root.glob("date=*")):
        if not date_dir.is_dir():
            continue
        date_text = date_dir.name.replace("date=", "", 1).strip()
        try:
            parsed = date.fromisoformat(date_text)
        except ValueError:
            continue
        if parsed < cutoff:
            shutil.rmtree(date_dir, ignore_errors=True)
            removed.append(date_text)
    return removed


def _expected_target_date(days_ago: int) -> str:
    target = datetime.now(timezone.utc).date() - timedelta(days=max(int(days_ago), 1))
    return target.isoformat()


def _dup_ratio(result: TicksFetchResult) -> float:
    if result.raw_rows <= 0:
        return 0.0
    duplicates = max(int(result.raw_rows) - int(result.unique_rows), 0)
    return float(duplicates) / float(result.raw_rows)


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


def _as_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
