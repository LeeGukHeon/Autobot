"""Server storage retention helpers for operational cleanup."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any


DATE_DIR_RE = re.compile(r"^date=(\d{4}-\d{2}-\d{2})$")
RUN_DIR_RE = re.compile(r"^\d{8}T\d{6}Z-[A-Za-z0-9-]+$")


@dataclass(frozen=True)
class StorageRetentionPolicy:
    ws_public_retention_days: int = 14
    raw_ticks_retention_days: int = 30
    micro_parquet_retention_days: int = 90
    candles_api_retention_days: int = 90
    paper_runs_retention_days: int = 30
    backtest_runs_retention_days: int = 1
    execution_backtest_retention_days: int = 1
    acceptance_backtest_cache_retention_days: int = 14
    registry_retention_days: int = 30
    registry_keep_recent_count: int = 6


@dataclass(frozen=True)
class EmergencyRetentionPolicy:
    paper_runs_retention_days: int = 7
    backtest_runs_retention_days: int = 1
    execution_backtest_retention_days: int = 1
    acceptance_backtest_cache_retention_days: int = 7
    registry_retention_days: int = 14
    registry_keep_recent_count: int = 3


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        raw = path.read_text(encoding="utf-8").strip()
    except OSError:
        return {}
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _disk_usage(path: Path) -> dict[str, Any]:
    usage = shutil.disk_usage(path)
    total = int(usage.total)
    used = int(usage.used)
    free = int(usage.free)
    used_pct = (float(used) / float(total) * 100.0) if total > 0 else 0.0
    return {
        "path": str(path),
        "total_bytes": total,
        "used_bytes": used,
        "free_bytes": free,
        "used_pct": round(used_pct, 3),
        "total_gb": round(total / (1024**3), 3),
        "used_gb": round(used / (1024**3), 3),
        "free_gb": round(free / (1024**3), 3),
    }


def _directory_size_bytes(path: Path) -> int:
    total = 0
    if not path.exists():
        return total
    for entry in path.rglob("*"):
        if not entry.is_file():
            continue
        try:
            total += int(entry.stat().st_size)
        except FileNotFoundError:
            continue
    return total


def _remove_tree(path: Path, *, dry_run: bool) -> None:
    if dry_run:
        return
    shutil.rmtree(path, ignore_errors=False)


def _remove_file(path: Path, *, dry_run: bool) -> None:
    if dry_run:
        return
    path.unlink(missing_ok=True)


def _prune_empty_ancestors(*, start: Path, stop_at: Path, dry_run: bool) -> list[str]:
    removed: list[str] = []
    current = start
    stop_resolved = stop_at.resolve()
    while True:
        try:
            current_resolved = current.resolve()
        except FileNotFoundError:
            break
        if current_resolved == stop_resolved:
            break
        if not current.exists() or not current.is_dir():
            break
        try:
            next(current.iterdir())
            break
        except StopIteration:
            removed.append(str(current))
            if not dry_run:
                current.rmdir()
            current = current.parent
            continue
    return removed


def _date_cutoff(retention_days: int) -> date:
    days = max(int(retention_days), 1)
    return _utc_now().date() - timedelta(days=days - 1)


def _scan_date_partition_dirs(root: Path) -> list[tuple[Path, date]]:
    found: list[tuple[Path, date]] = []
    if not root.exists():
        return found
    for entry in root.rglob("date=*"):
        if not entry.is_dir():
            continue
        match = DATE_DIR_RE.fullmatch(entry.name)
        if match is None:
            continue
        try:
            entry_date = date.fromisoformat(match.group(1))
        except ValueError:
            continue
        found.append((entry, entry_date))
    found.sort(key=lambda item: (item[1], str(item[0])))
    return found


def prune_date_partition_dirs(*, root: Path, retention_days: int, dry_run: bool) -> dict[str, Any]:
    if retention_days <= 0:
        return {
            "root": str(root),
            "retention_days": retention_days,
            "removed_count": 0,
            "freed_bytes": 0,
            "removed_paths": [],
            "removed_empty_parents": [],
            "skipped": True,
            "reason": "RETENTION_DISABLED",
        }
    if not root.exists():
        return {
            "root": str(root),
            "retention_days": retention_days,
            "removed_count": 0,
            "freed_bytes": 0,
            "removed_paths": [],
            "removed_empty_parents": [],
            "skipped": True,
            "reason": "ROOT_MISSING",
        }
    cutoff = _date_cutoff(retention_days)
    removed_paths: list[str] = []
    removed_empty_parents: list[str] = []
    freed_bytes = 0
    for path, entry_date in _scan_date_partition_dirs(root):
        if entry_date >= cutoff:
            continue
        freed_bytes += _directory_size_bytes(path)
        removed_paths.append(str(path))
        _remove_tree(path, dry_run=dry_run)
        removed_empty_parents.extend(_prune_empty_ancestors(start=path.parent, stop_at=root, dry_run=dry_run))
    return {
        "root": str(root),
        "retention_days": retention_days,
        "cutoff_date": cutoff.isoformat(),
        "removed_count": len(removed_paths),
        "freed_bytes": freed_bytes,
        "removed_paths": removed_paths,
        "removed_empty_parents": removed_empty_parents,
        "dry_run": bool(dry_run),
        "skipped": False,
    }


def prune_run_dirs(
    *,
    root: Path,
    retention_days: int,
    dry_run: bool,
    keep_names: set[str] | None = None,
    allowed_prefixes: tuple[str, ...] = (),
) -> dict[str, Any]:
    protected = set(keep_names or set())
    if retention_days <= 0:
        return {
            "root": str(root),
            "retention_days": retention_days,
            "removed_count": 0,
            "freed_bytes": 0,
            "removed_paths": [],
            "protected_names": sorted(protected),
            "skipped": True,
            "reason": "RETENTION_DISABLED",
        }
    if not root.exists():
        return {
            "root": str(root),
            "retention_days": retention_days,
            "removed_count": 0,
            "freed_bytes": 0,
            "removed_paths": [],
            "protected_names": sorted(protected),
            "skipped": True,
            "reason": "ROOT_MISSING",
        }
    cutoff_ts = (_utc_now() - timedelta(days=max(int(retention_days), 1))).timestamp()
    removed_paths: list[str] = []
    freed_bytes = 0
    for entry in sorted((path for path in root.iterdir() if path.is_dir()), key=lambda path: path.name):
        if entry.name in protected:
            continue
        if allowed_prefixes and not entry.name.startswith(allowed_prefixes):
            continue
        try:
            mtime = entry.stat().st_mtime
        except FileNotFoundError:
            continue
        if mtime >= cutoff_ts:
            continue
        freed_bytes += _directory_size_bytes(entry)
        removed_paths.append(str(entry))
        _remove_tree(entry, dry_run=dry_run)
    return {
        "root": str(root),
        "retention_days": retention_days,
        "removed_count": len(removed_paths),
        "freed_bytes": freed_bytes,
        "removed_paths": removed_paths,
        "protected_names": sorted(protected),
        "dry_run": bool(dry_run),
        "skipped": False,
    }


def _read_pointer_run_id(path: Path) -> str:
    payload = _load_json(path)
    return str(payload.get("run_id", "")).strip()


def collect_protected_registry_run_ids(*, project_root: Path, model_family: str) -> set[str]:
    protected: set[str] = set()
    family_root = project_root / "models" / "registry" / model_family
    for name in ("champion.json", "latest.json", "latest_candidate.json"):
        run_id = _read_pointer_run_id(family_root / name)
        if run_id:
            protected.add(run_id)
    current_state = _load_json(project_root / "logs" / "model_v4_challenger" / "current_state.json")
    for key in (
        "candidate_run_id",
        "champion_run_id_at_start",
        "champion_after_run_id",
        "champion_before_run_id",
    ):
        value = str(current_state.get(key, "")).strip()
        if value:
            protected.add(value)
    return protected


def prune_registry_family(
    *,
    family_root: Path,
    retention_days: int,
    keep_recent_count: int,
    protected_run_ids: set[str],
    dry_run: bool,
) -> dict[str, Any]:
    if retention_days <= 0:
        return {
            "root": str(family_root),
            "retention_days": retention_days,
            "removed_count": 0,
            "freed_bytes": 0,
            "removed_paths": [],
            "protected_run_ids": sorted(protected_run_ids),
            "skipped": True,
            "reason": "RETENTION_DISABLED",
        }
    if not family_root.exists():
        return {
            "root": str(family_root),
            "retention_days": retention_days,
            "removed_count": 0,
            "freed_bytes": 0,
            "removed_paths": [],
            "protected_run_ids": sorted(protected_run_ids),
            "skipped": True,
            "reason": "ROOT_MISSING",
        }
    run_dirs = [path for path in family_root.iterdir() if path.is_dir() and RUN_DIR_RE.fullmatch(path.name)]
    run_dirs.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    newest_names = {path.name for path in run_dirs[: max(int(keep_recent_count), 0)]}
    cutoff_ts = (_utc_now() - timedelta(days=max(int(retention_days), 1))).timestamp()
    removed_paths: list[str] = []
    freed_bytes = 0
    for entry in run_dirs:
        if entry.name in protected_run_ids or entry.name in newest_names:
            continue
        try:
            mtime = entry.stat().st_mtime
        except FileNotFoundError:
            continue
        if mtime >= cutoff_ts:
            continue
        freed_bytes += _directory_size_bytes(entry)
        removed_paths.append(str(entry))
        _remove_tree(entry, dry_run=dry_run)
    return {
        "root": str(family_root),
        "retention_days": retention_days,
        "keep_recent_count": max(int(keep_recent_count), 0),
        "removed_count": len(removed_paths),
        "freed_bytes": freed_bytes,
        "removed_paths": removed_paths,
        "protected_run_ids": sorted(protected_run_ids),
        "newest_names": sorted(newest_names),
        "dry_run": bool(dry_run),
        "skipped": False,
    }


def _resolve_systemctl_path() -> str:
    resolved = shutil.which("systemctl")
    if resolved:
        return resolved
    for candidate in ("/usr/bin/systemctl", "/bin/systemctl"):
        if Path(candidate).exists():
            return candidate
    return ""


def _systemd_unit_active(unit_name: str) -> bool:
    systemctl_path = _resolve_systemctl_path()
    if not unit_name or not systemctl_path:
        return False
    completed = subprocess.run(
        [systemctl_path, "is-active", unit_name],
        capture_output=True,
        text=True,
        check=False,
    )
    state = str(completed.stdout or "").strip().lower()
    return state in {"active", "activating", "reloading"}


def compact_ts_parquet_dataset(
    *,
    dataset_root: Path,
    retention_days: int,
    dry_run: bool,
    skip_reason: str = "",
) -> dict[str, Any]:
    if retention_days <= 0:
        return {
            "root": str(dataset_root),
            "retention_days": retention_days,
            "files_rewritten": 0,
            "files_removed": 0,
            "freed_bytes": 0,
            "skipped": True,
            "reason": "RETENTION_DISABLED",
        }
    if skip_reason:
        return {
            "root": str(dataset_root),
            "retention_days": retention_days,
            "files_rewritten": 0,
            "files_removed": 0,
            "freed_bytes": 0,
            "skipped": True,
            "reason": skip_reason,
        }
    if not dataset_root.exists():
        return {
            "root": str(dataset_root),
            "retention_days": retention_days,
            "files_rewritten": 0,
            "files_removed": 0,
            "freed_bytes": 0,
            "skipped": True,
            "reason": "ROOT_MISSING",
        }
    try:
        import polars as pl
    except ModuleNotFoundError:
        return {
            "root": str(dataset_root),
            "retention_days": retention_days,
            "files_rewritten": 0,
            "files_removed": 0,
            "freed_bytes": 0,
            "skipped": True,
            "reason": "POLARS_MISSING",
        }
    cutoff_ts_ms = int((_utc_now() - timedelta(days=max(int(retention_days), 1))).timestamp() * 1000.0)
    files_rewritten = 0
    files_removed = 0
    freed_bytes = 0
    rewritten_paths: list[str] = []
    removed_paths: list[str] = []
    for path in sorted(dataset_root.rglob("*.parquet")):
        if "_meta" in path.parts:
            continue
        before_bytes = int(path.stat().st_size)
        stats = (
            pl.scan_parquet(str(path))
            .select(
                [
                    pl.len().alias("rows"),
                    pl.col("ts_ms").min().alias("min_ts_ms"),
                    pl.col("ts_ms").max().alias("max_ts_ms"),
                ]
            )
            .collect()
            .row(0, named=True)
        )
        min_ts_ms = int(stats.get("min_ts_ms") or 0)
        max_ts_ms = int(stats.get("max_ts_ms") or 0)
        if max_ts_ms <= 0 or min_ts_ms >= cutoff_ts_ms:
            continue
        if max_ts_ms < cutoff_ts_ms:
            files_removed += 1
            freed_bytes += before_bytes
            removed_paths.append(str(path))
            _remove_file(path, dry_run=dry_run)
            continue
        filtered = pl.scan_parquet(str(path)).filter(pl.col("ts_ms") >= cutoff_ts_ms).collect()
        if filtered.height <= 0:
            files_removed += 1
            freed_bytes += before_bytes
            removed_paths.append(str(path))
            _remove_file(path, dry_run=dry_run)
            continue
        files_rewritten += 1
        rewritten_paths.append(str(path))
        if not dry_run:
            tmp_path = path.with_suffix(path.suffix + ".tmp")
            filtered.write_parquet(tmp_path, compression="zstd")
            after_bytes = int(tmp_path.stat().st_size)
            os.replace(tmp_path, path)
        else:
            after_bytes = before_bytes
        freed_bytes += max(before_bytes - after_bytes, 0)
    return {
        "root": str(dataset_root),
        "retention_days": retention_days,
        "cutoff_ts_ms": cutoff_ts_ms,
        "files_rewritten": files_rewritten,
        "files_removed": files_removed,
        "freed_bytes": freed_bytes,
        "rewritten_paths": rewritten_paths,
        "removed_paths": removed_paths,
        "dry_run": bool(dry_run),
        "skipped": False,
    }


def _write_ws_retention_reports(*, project_root: Path, payload: dict[str, Any]) -> None:
    meta_dir = project_root / "data" / "raw_ws" / "upbit" / "_meta"
    removed_by_channel: dict[str, list[str]] = {"trade": [], "orderbook": []}
    for raw_path in payload.get("removed_paths", []):
        path = Path(raw_path)
        if not path.name.startswith("date="):
            continue
        try:
            channel_name = path.parent.name
        except Exception:
            continue
        if channel_name not in removed_by_channel:
            continue
        removed_by_channel[channel_name].append(path.name.split("=", 1)[1])
    report = {
        "generated_at": payload.get("generated_at"),
        "raw_root": str(project_root / "data" / "raw_ws" / "upbit" / "public"),
        "retention_days": payload.get("retention_days"),
        "removed": removed_by_channel,
        "removed_counts": {
            "trade": len(removed_by_channel["trade"]),
            "orderbook": len(removed_by_channel["orderbook"]),
        },
        "dry_run": bool(payload.get("dry_run", False)),
    }
    _write_json(meta_dir / "retention_report.json", report)
    _write_json(meta_dir / "ws_purge_report.json", report)


def _section_payload(name: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {"name": name, **payload}


def _build_budget_summary(*, project_root: Path, model_family: str) -> dict[str, Any]:
    buckets = {
        "raw_ws": project_root / "data" / "raw_ws",
        "raw_ticks": project_root / "data" / "raw_ticks",
        "parquet": project_root / "data" / "parquet",
        "paper_runs": project_root / "data" / "paper" / "runs",
        "backtest_runs": project_root / "data" / "backtest" / "runs",
        "execution_backtest_runs": project_root / "logs" / "train_v4_execution_backtest" / "runs",
        "registry_family": project_root / "models" / "registry" / model_family,
        "model_v4_acceptance": project_root / "logs" / "model_v4_acceptance",
        "model_v4_challenger": project_root / "logs" / "model_v4_challenger",
        "paper_micro_smoke": project_root / "logs" / "paper_micro_smoke",
        "micro_tiering": project_root / "logs" / "micro_tiering",
    }
    sections: dict[str, Any] = {}
    total_bytes = 0
    for name, path in buckets.items():
        size_bytes = _directory_size_bytes(path)
        total_bytes += size_bytes
        sections[name] = {
            "path": str(path),
            "size_bytes": int(size_bytes),
            "size_gb": round(float(size_bytes) / float(1024**3), 6),
            "exists": bool(path.exists()),
        }
    return {
        "tracked_total_bytes": int(total_bytes),
        "tracked_total_gb": round(float(total_bytes) / float(1024**3), 6),
        "sections": sections,
    }


def run_storage_retention(
    *,
    project_root: Path,
    policy: StorageRetentionPolicy,
    emergency_policy: EmergencyRetentionPolicy,
    model_family: str,
    report_dir: Path,
    warning_threshold_gb: float,
    force_threshold_gb: float,
    dry_run: bool,
    compact_candles_api: bool,
) -> dict[str, Any]:
    resolved_root = project_root.resolve()
    resolved_report_dir = report_dir if report_dir.is_absolute() else (resolved_root / report_dir)
    before_usage = _disk_usage(resolved_root)
    protected_run_ids = collect_protected_registry_run_ids(project_root=resolved_root, model_family=model_family)
    generated_at = _utc_now().isoformat()

    sections: list[dict[str, Any]] = []
    ws_payload = prune_date_partition_dirs(
        root=resolved_root / "data" / "raw_ws" / "upbit" / "public",
        retention_days=policy.ws_public_retention_days,
        dry_run=dry_run,
    )
    ws_payload["generated_at"] = generated_at
    if (not dry_run) and (not ws_payload.get("skipped", False)):
        _write_ws_retention_reports(project_root=resolved_root, payload=ws_payload)
    sections.append(_section_payload("ws_public", ws_payload))
    sections.append(
        _section_payload(
            "raw_ticks",
            prune_date_partition_dirs(
                root=resolved_root / "data" / "raw_ticks" / "upbit" / "trades",
                retention_days=policy.raw_ticks_retention_days,
                dry_run=dry_run,
            ),
        )
    )
    sections.append(
        _section_payload(
            "micro_parquet",
            prune_date_partition_dirs(
                root=resolved_root / "data" / "parquet" / "micro_v1",
                retention_days=policy.micro_parquet_retention_days,
                dry_run=dry_run,
            ),
        )
    )

    skip_candles_api = ""
    if compact_candles_api and _systemd_unit_active("autobot-v4-challenger-spawn.service"):
        skip_candles_api = "SPAWN_SERVICE_ACTIVE"
    sections.append(
        _section_payload(
            "candles_api_v1",
            compact_ts_parquet_dataset(
                dataset_root=resolved_root / "data" / "parquet" / "candles_api_v1",
                retention_days=policy.candles_api_retention_days,
                dry_run=dry_run,
                skip_reason=skip_candles_api if compact_candles_api else "COMPACTION_DISABLED",
            ),
        )
    )
    sections.append(
        _section_payload(
            "paper_runs",
            prune_run_dirs(
                root=resolved_root / "data" / "paper" / "runs",
                retention_days=policy.paper_runs_retention_days,
                dry_run=dry_run,
                allowed_prefixes=("paper-",),
            ),
        )
    )
    sections.append(
        _section_payload(
            "backtest_runs",
            prune_run_dirs(
                root=resolved_root / "data" / "backtest" / "runs",
                retention_days=policy.backtest_runs_retention_days,
                dry_run=dry_run,
            ),
        )
    )
    sections.append(
        _section_payload(
            "execution_backtest_runs",
            prune_run_dirs(
                root=resolved_root / "logs" / "train_v4_execution_backtest" / "runs",
                retention_days=policy.execution_backtest_retention_days,
                dry_run=dry_run,
            ),
        )
    )
    sections.append(
        _section_payload(
            "acceptance_backtest_cache",
            prune_run_dirs(
                root=resolved_root / "models" / "registry" / model_family / "_acceptance_backtest_cache",
                retention_days=policy.acceptance_backtest_cache_retention_days,
                dry_run=dry_run,
            ),
        )
    )
    sections.append(
        _section_payload(
            "registry_family",
            prune_registry_family(
                family_root=resolved_root / "models" / "registry" / model_family,
                retention_days=policy.registry_retention_days,
                keep_recent_count=policy.registry_keep_recent_count,
                protected_run_ids=protected_run_ids,
                dry_run=dry_run,
            ),
        )
    )

    after_standard_usage = _disk_usage(resolved_root)
    emergency_sections: list[dict[str, Any]] = []
    force_threshold_bytes = int(float(force_threshold_gb) * (1024**3))
    if after_standard_usage["used_bytes"] > force_threshold_bytes:
        emergency_sections.append(
            _section_payload(
                "paper_runs_emergency",
                prune_run_dirs(
                    root=resolved_root / "data" / "paper" / "runs",
                    retention_days=emergency_policy.paper_runs_retention_days,
                    dry_run=dry_run,
                    allowed_prefixes=("paper-",),
                ),
            )
        )
        emergency_sections.append(
            _section_payload(
                "backtest_runs_emergency",
                prune_run_dirs(
                    root=resolved_root / "data" / "backtest" / "runs",
                    retention_days=emergency_policy.backtest_runs_retention_days,
                    dry_run=dry_run,
                ),
            )
        )
        emergency_sections.append(
            _section_payload(
                "execution_backtest_runs_emergency",
                prune_run_dirs(
                    root=resolved_root / "logs" / "train_v4_execution_backtest" / "runs",
                    retention_days=emergency_policy.execution_backtest_retention_days,
                    dry_run=dry_run,
                ),
            )
        )
        emergency_sections.append(
            _section_payload(
                "acceptance_backtest_cache_emergency",
                prune_run_dirs(
                    root=resolved_root / "models" / "registry" / model_family / "_acceptance_backtest_cache",
                    retention_days=emergency_policy.acceptance_backtest_cache_retention_days,
                    dry_run=dry_run,
                ),
            )
        )
        emergency_sections.append(
            _section_payload(
                "registry_family_emergency",
                prune_registry_family(
                    family_root=resolved_root / "models" / "registry" / model_family,
                    retention_days=emergency_policy.registry_retention_days,
                    keep_recent_count=emergency_policy.registry_keep_recent_count,
                    protected_run_ids=protected_run_ids,
                    dry_run=dry_run,
                ),
            )
        )

    after_usage = _disk_usage(resolved_root)
    warning_threshold_bytes = int(float(warning_threshold_gb) * (1024**3))
    markers: list[str] = []
    status = "ok"
    if after_usage["used_bytes"] > force_threshold_bytes:
        status = "force_threshold_exceeded"
        markers.append("HARD_BUDGET_EXCEEDED")
    elif after_usage["used_bytes"] > warning_threshold_bytes:
        status = "warning_threshold_exceeded"
        markers.append("SOFT_BUDGET_EXCEEDED")
    total_freed = sum(int(section.get("freed_bytes", 0) or 0) for section in sections + emergency_sections)
    budget_summary = _build_budget_summary(project_root=resolved_root, model_family=model_family)
    payload = {
        "generated_at": generated_at,
        "project_root": str(resolved_root),
        "model_family": model_family,
        "dry_run": bool(dry_run),
        "status": status,
        "markers": markers,
        "thresholds": {"warning_gb": float(warning_threshold_gb), "force_gb": float(force_threshold_gb)},
        "policy": asdict(policy),
        "emergency_policy": asdict(emergency_policy),
        "usage": {"before": before_usage, "after_standard": after_standard_usage, "after": after_usage},
        "budget_summary": budget_summary,
        "protected_registry_run_ids": sorted(protected_run_ids),
        "total_freed_bytes": total_freed,
        "total_freed_gb": round(total_freed / (1024**3), 3),
        "sections": sections,
        "emergency_sections": emergency_sections,
    }
    stamp = _utc_now().strftime("%Y%m%d-%H%M%S")
    report_path = resolved_report_dir / f"storage_retention_{stamp}.json"
    latest_path = resolved_report_dir / "latest.json"
    if not dry_run:
        _write_json(report_path, payload)
        _write_json(latest_path, payload)
    payload["report_path"] = str(report_path)
    payload["latest_path"] = str(latest_path)
    return payload


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Server storage retention cleanup")
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--report-dir", default="logs/storage_retention")
    parser.add_argument("--model-family", default="train_v4_crypto_cs")
    parser.add_argument("--ws-public-retention-days", type=int, default=14)
    parser.add_argument("--raw-ticks-retention-days", type=int, default=30)
    parser.add_argument("--micro-parquet-retention-days", type=int, default=90)
    parser.add_argument("--candles-api-retention-days", type=int, default=90)
    parser.add_argument("--paper-runs-retention-days", type=int, default=30)
    parser.add_argument("--backtest-runs-retention-days", type=int, default=5)
    parser.add_argument("--execution-backtest-retention-days", type=int, default=2)
    parser.add_argument("--acceptance-backtest-cache-retention-days", type=int, default=14)
    parser.add_argument("--registry-retention-days", type=int, default=30)
    parser.add_argument("--registry-keep-recent-count", type=int, default=6)
    parser.add_argument("--emergency-paper-runs-retention-days", type=int, default=7)
    parser.add_argument("--emergency-backtest-runs-retention-days", type=int, default=3)
    parser.add_argument("--emergency-execution-backtest-retention-days", type=int, default=1)
    parser.add_argument("--emergency-acceptance-backtest-cache-retention-days", type=int, default=7)
    parser.add_argument("--emergency-registry-retention-days", type=int, default=14)
    parser.add_argument("--emergency-registry-keep-recent-count", type=int, default=3)
    parser.add_argument("--warning-threshold-gb", type=float, default=100.0)
    parser.add_argument("--force-threshold-gb", type=float, default=120.0)
    parser.add_argument("--compact-candles-api", default="true", help="true|false")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def _parse_bool_text(value: str, *, default: bool) -> bool:
    lowered = str(value).strip().lower()
    if lowered in {"1", "true", "yes", "y", "on"}:
        return True
    if lowered in {"0", "false", "no", "n", "off"}:
        return False
    return default


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    payload = run_storage_retention(
        project_root=Path(str(args.project_root).strip() or ".").resolve(),
        policy=StorageRetentionPolicy(
            ws_public_retention_days=max(int(args.ws_public_retention_days), 1),
            raw_ticks_retention_days=max(int(args.raw_ticks_retention_days), 1),
            micro_parquet_retention_days=max(int(args.micro_parquet_retention_days), 1),
            candles_api_retention_days=max(int(args.candles_api_retention_days), 1),
            paper_runs_retention_days=max(int(args.paper_runs_retention_days), 1),
            backtest_runs_retention_days=max(int(args.backtest_runs_retention_days), 1),
            execution_backtest_retention_days=max(int(args.execution_backtest_retention_days), 1),
            acceptance_backtest_cache_retention_days=max(int(args.acceptance_backtest_cache_retention_days), 1),
            registry_retention_days=max(int(args.registry_retention_days), 1),
            registry_keep_recent_count=max(int(args.registry_keep_recent_count), 0),
        ),
        emergency_policy=EmergencyRetentionPolicy(
            paper_runs_retention_days=max(int(args.emergency_paper_runs_retention_days), 1),
            backtest_runs_retention_days=max(int(args.emergency_backtest_runs_retention_days), 1),
            execution_backtest_retention_days=max(int(args.emergency_execution_backtest_retention_days), 1),
            acceptance_backtest_cache_retention_days=max(int(args.emergency_acceptance_backtest_cache_retention_days), 1),
            registry_retention_days=max(int(args.emergency_registry_retention_days), 1),
            registry_keep_recent_count=max(int(args.emergency_registry_keep_recent_count), 0),
        ),
        model_family=str(args.model_family).strip() or "train_v4_crypto_cs",
        report_dir=Path(str(args.report_dir).strip() or "logs/storage_retention"),
        warning_threshold_gb=max(float(args.warning_threshold_gb), 1.0),
        force_threshold_gb=max(float(args.force_threshold_gb), 1.0),
        dry_run=bool(args.dry_run),
        compact_candles_api=_parse_bool_text(args.compact_candles_api, default=True),
    )
    print(
        "[storage-retention] "
        f"status={payload['status']} "
        f"freed_gb={payload['total_freed_gb']:.3f} "
        f"used_before_gb={payload['usage']['before']['used_gb']:.3f} "
        f"used_after_gb={payload['usage']['after']['used_gb']:.3f}"
    )
    print(f"[storage-retention] report={payload['report_path']}")
    print(f"[storage-retention] latest={payload['latest_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
