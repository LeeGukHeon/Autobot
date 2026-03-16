"""Plan-driven candle collection runner."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import time
from typing import Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from ...upbit import ValidationError
from ...upbit import load_upbit_settings
from ..inventory import ts_ms_to_utc_text
from .candle_manifest import append_manifest_rows, manifest_path
from .candle_writer import write_candle_partition
from .upbit_candles_client import UpbitCandlesClient


@dataclass(frozen=True)
class CandleCollectOptions:
    plan_path: Path
    parquet_root: Path = Path("data/parquet")
    out_dataset: str = "candles_api_v1"
    collect_meta_dir: Path = Path("data/collect/_meta")
    dry_run: bool = True
    workers: int = 1
    max_requests: int | None = None
    stop_on_first_fail: bool = False
    rate_limit_strict: bool = True
    config_dir: Path = Path("config")

    @property
    def dataset_root(self) -> Path:
        return self.parquet_root / self.out_dataset

    @property
    def collect_report_path(self) -> Path:
        return self.collect_meta_dir / "candle_collect_report.json"

    @property
    def build_report_path(self) -> Path:
        return self.dataset_root / "_meta" / "build_report.json"


@dataclass(frozen=True)
class CandleCollectSummary:
    discovered_targets: int
    selected_targets: int
    processed_targets: int
    ok_targets: int
    warn_targets: int
    fail_targets: int
    calls_made: int
    throttled_count: int
    backoff_count: int
    manifest_file: Path
    collect_report_file: Path
    build_report_file: Path
    details: tuple[dict[str, Any], ...]
    failures: tuple[dict[str, Any], ...]


def collect_candles_from_plan(
    options: CandleCollectOptions,
    *,
    client: UpbitCandlesClient | None = None,
) -> CandleCollectSummary:
    started_at = int(time.time())
    plan = _load_plan(options.plan_path)
    targets = [dict(item) for item in plan.get("targets", []) if isinstance(item, dict)]
    discovered_targets = len(targets)

    selected_targets = discovered_targets
    if options.max_requests is not None and int(options.max_requests) <= 0:
        selected_targets = 0

    settings = None
    if not options.dry_run:
        settings = load_upbit_settings(options.config_dir)

    dataset_root = options.dataset_root
    dataset_root.mkdir(parents=True, exist_ok=True)
    manifest_file = manifest_path(dataset_root)

    calls_made_total = 0
    throttled_count_total = 0
    backoff_count_total = 0

    ok_targets = 0
    warn_targets = 0
    fail_targets = 0
    processed_targets = 0

    details: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    manifest_rows: list[dict[str, Any]] = []

    window = plan.get("window", {}) if isinstance(plan.get("window"), dict) else {}
    window_tag = (
        f"{window.get('start_utc', '')}__{window.get('end_utc', '')}"
        .replace(":", "")
        .replace("+00", "Z")
        .replace(" ", "")
    )
    max_requests_global = int(options.max_requests) if options.max_requests is not None else None

    effective_workers = max(int(options.workers), 1)
    if options.rate_limit_strict:
        effective_workers = 1
    if options.stop_on_first_fail or options.max_requests is not None or client is not None:
        effective_workers = 1

    if options.dry_run:
        for index, target in enumerate(targets, start=1):
            detail = _target_detail(index, target)
            detail["status"] = "DRY_RUN"
            details.append(detail)
    elif effective_workers <= 1:
        for index, target in enumerate(targets, start=1):
            if max_requests_global is not None and calls_made_total >= max_requests_global:
                break
            detail, manifest_row = _collect_one_target(
                index=index,
                target=target,
                options=options,
                dataset_root=dataset_root,
                window_tag=window_tag,
                client=(client or UpbitCandlesClient(settings=settings)),
                max_requests_remaining=(
                    max(max_requests_global - calls_made_total, 0) if max_requests_global is not None else None
                ),
            )
            details.append(detail)
            manifest_rows.append(manifest_row)
            processed_targets += 1
            calls_made_total += int(detail.get("calls_made", 0))
            throttled_count_total += int(detail.get("throttled_count", 0))
            backoff_count_total += int(detail.get("backoff_count", 0))
            status = str(detail.get("status", "FAIL")).upper()
            if status == "OK":
                ok_targets += 1
            elif status == "WARN":
                warn_targets += 1
            else:
                fail_targets += 1
                failures.append(detail)
                if options.stop_on_first_fail:
                    break
    else:
        worker_count = min(effective_workers, max(len(targets), 1))
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {
                executor.submit(
                    _collect_one_target,
                    index=index,
                    target=target,
                    options=options,
                    dataset_root=dataset_root,
                    window_tag=window_tag,
                    client=UpbitCandlesClient(settings=settings),
                    max_requests_remaining=None,
                ): index
                for index, target in enumerate(targets, start=1)
            }
            for future in as_completed(futures):
                detail, manifest_row = future.result()
                details.append(detail)
                manifest_rows.append(manifest_row)
                processed_targets += 1
                calls_made_total += int(detail.get("calls_made", 0))
                throttled_count_total += int(detail.get("throttled_count", 0))
                backoff_count_total += int(detail.get("backoff_count", 0))
                status = str(detail.get("status", "FAIL")).upper()
                if status == "OK":
                    ok_targets += 1
                elif status == "WARN":
                    warn_targets += 1
                else:
                    fail_targets += 1
                    failures.append(detail)

        details.sort(key=lambda item: int(item.get("idx", 0)))

    if not options.dry_run:
        append_manifest_rows(manifest_file, manifest_rows)

    collect_report = {
        "started_at": started_at,
        "finished_at": int(time.time()),
        "plan_file": str(options.plan_path),
        "dataset_root": str(dataset_root),
        "dry_run": bool(options.dry_run),
        "workers_requested": int(options.workers),
        "workers_effective": effective_workers,
        "discovered_targets": discovered_targets,
        "selected_targets": selected_targets,
        "processed_targets": processed_targets,
        "ok_targets": ok_targets,
        "warn_targets": warn_targets,
        "fail_targets": fail_targets,
        "calls_made": calls_made_total,
        "throttled_count": throttled_count_total,
        "backoff_count": backoff_count_total,
        "failures": failures,
        "details": details,
    }
    options.collect_report_path.parent.mkdir(parents=True, exist_ok=True)
    options.collect_report_path.write_text(
        json.dumps(collect_report, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    build_report = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "dataset_name": options.out_dataset,
        "dataset_root": str(dataset_root),
        "manifest_file": str(manifest_file),
        "collect_report_file": str(options.collect_report_path),
        "summary": {
            "processed_targets": processed_targets,
            "ok_targets": ok_targets,
            "warn_targets": warn_targets,
            "fail_targets": fail_targets,
            "calls_made": calls_made_total,
        },
    }
    options.build_report_path.parent.mkdir(parents=True, exist_ok=True)
    options.build_report_path.write_text(
        json.dumps(build_report, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    return CandleCollectSummary(
        discovered_targets=discovered_targets,
        selected_targets=selected_targets,
        processed_targets=processed_targets,
        ok_targets=ok_targets,
        warn_targets=warn_targets,
        fail_targets=fail_targets,
        calls_made=calls_made_total,
        throttled_count=throttled_count_total,
        backoff_count=backoff_count_total,
        manifest_file=manifest_file,
        collect_report_file=options.collect_report_path,
        build_report_file=options.build_report_path,
        details=tuple(details),
        failures=tuple(failures),
    )


def _load_plan(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"plan file not found: {path}")
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("plan file must contain JSON object")
    return raw


def _target_detail(index: int, target: dict[str, Any]) -> dict[str, Any]:
    market = str(target.get("market", "")).strip().upper()
    tf = str(target.get("tf", "")).strip().lower()
    need_from_ts_ms = int(target.get("need_from_ts_ms"))
    need_to_ts_ms = int(target.get("need_to_ts_ms"))
    return {
        "idx": index,
        "market": market,
        "tf": tf,
        "need_from_ts_ms": need_from_ts_ms,
        "need_to_ts_ms": need_to_ts_ms,
        "need_from_utc": ts_ms_to_utc_text(need_from_ts_ms),
        "need_to_utc": ts_ms_to_utc_text(need_to_ts_ms),
        "reason": str(target.get("reason", "")),
    }


def _collect_one_target(
    *,
    index: int,
    target: dict[str, Any],
    options: CandleCollectOptions,
    dataset_root: Path,
    window_tag: str,
    client: UpbitCandlesClient,
    max_requests_remaining: int | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    detail = _target_detail(index, target)
    market = detail["market"]
    tf = detail["tf"]
    need_from_ts_ms = int(detail["need_from_ts_ms"])
    need_to_ts_ms = int(detail["need_to_ts_ms"])

    try:
        result = client.fetch_minutes_range(
            market=market,
            tf=tf,
            start_ts_ms=need_from_ts_ms,
            end_ts_ms=need_to_ts_ms,
            max_requests=max_requests_remaining,
        )
        detail["calls_made"] = int(result.calls_made)
        detail["throttled_count"] = int(result.throttled_count)
        detail["backoff_count"] = int(result.backoff_count)
        detail["rows_fetched"] = len(result.candles)
        detail["loop_guard_triggered"] = bool(result.loop_guard_triggered)
        detail["truncated_by_budget"] = bool(result.truncated_by_budget)

        reasons: list[str] = []
        status = "OK"
        if not result.candles:
            status = "WARN"
            reasons.append("NO_ROWS_COLLECTED")
        else:
            write_result = write_candle_partition(
                dataset_root=dataset_root,
                tf=tf,
                market=market,
                candles=list(result.candles),
            )
            detail.update(
                {
                    "output_part_file": write_result["part_file"],
                    "rows_after_merge": int(write_result["rows"]),
                    "min_ts_ms": write_result["min_ts_ms"],
                    "max_ts_ms": write_result["max_ts_ms"],
                }
            )
            if result.loop_guard_triggered:
                status = "WARN"
                reasons.append("PAGING_LOOP_GUARD_TRIGGERED")
            if result.truncated_by_budget:
                status = "WARN"
                reasons.append("MAX_REQUESTS_BUDGET_REACHED")

        detail["status"] = status
        detail["reasons"] = reasons
        manifest_row = {
            "dataset_name": options.out_dataset,
            "source": "upbit_api",
            "window_tag": window_tag,
            "market": market,
            "tf": tf,
            "rows": int(detail.get("rows_after_merge", detail.get("rows_fetched", 0))),
            "min_ts_ms": detail.get("min_ts_ms", result.min_ts_ms),
            "max_ts_ms": detail.get("max_ts_ms", result.max_ts_ms),
            "calls_made": int(result.calls_made),
            "status": status,
            "reasons_json": json.dumps(reasons, ensure_ascii=False),
            "error_message": None,
            "part_file": str(detail.get("output_part_file", "")),
            "collected_at": int(time.time()),
        }
        return detail, manifest_row
    except Exception as exc:
        if _is_terminal_market_unavailable(exc):
            detail["status"] = "WARN"
            detail["error_message"] = str(exc)
            detail["reasons"] = ["DELISTED_OR_INACTIVE_MARKET"]
            detail["calls_made"] = int(detail.get("calls_made", 0))
            detail["throttled_count"] = int(detail.get("throttled_count", 0))
            detail["backoff_count"] = int(detail.get("backoff_count", 0))
            manifest_row = {
                "dataset_name": options.out_dataset,
                "source": "upbit_api",
                "window_tag": window_tag,
                "market": market,
                "tf": tf,
                "rows": 0,
                "min_ts_ms": None,
                "max_ts_ms": None,
                "calls_made": int(detail.get("calls_made", 0)),
                "status": "WARN",
                "reasons_json": json.dumps(["DELISTED_OR_INACTIVE_MARKET"], ensure_ascii=False),
                "error_message": str(exc),
                "part_file": "",
                "collected_at": int(time.time()),
            }
            return detail, manifest_row

        detail["status"] = "FAIL"
        detail["error_message"] = str(exc)
        detail["reasons"] = ["COLLECT_EXCEPTION"]
        detail["calls_made"] = int(detail.get("calls_made", 0))
        detail["throttled_count"] = int(detail.get("throttled_count", 0))
        detail["backoff_count"] = int(detail.get("backoff_count", 0))
        manifest_row = {
            "dataset_name": options.out_dataset,
            "source": "upbit_api",
            "window_tag": window_tag,
            "market": market,
            "tf": tf,
            "rows": 0,
            "min_ts_ms": None,
            "max_ts_ms": None,
            "calls_made": int(detail.get("calls_made", 0)),
            "status": "FAIL",
            "reasons_json": json.dumps(["COLLECT_EXCEPTION"], ensure_ascii=False),
            "error_message": str(exc),
            "part_file": "",
            "collected_at": int(time.time()),
        }
        return detail, manifest_row


def _is_terminal_market_unavailable(exc: Exception) -> bool:
    if not isinstance(exc, ValidationError):
        return False
    if int(getattr(exc, "status_code", 0) or 0) != 404:
        return False
    endpoint = str(getattr(exc, "endpoint", "") or "").strip().lower()
    if "/v1/candles/minutes/" not in endpoint:
        return False
    message = str(getattr(exc, "message", "") or exc).strip().lower()
    if "code not found" in message:
        return True
    if "market not found" in message:
        return True
    return False
