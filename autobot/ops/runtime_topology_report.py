"""Machine-readable runtime topology report builders."""

from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
from pathlib import Path
from typing import Any

from autobot.live.breaker_taxonomy import annotate_reason_payload
from autobot.live.model_handoff import build_live_runtime_sync_status, load_ws_public_runtime_contract, resolve_live_runtime_model_contract
from autobot.live.rollout import load_rollout_latest
from autobot.models.registry import load_json
from .data_contract_registry import build_data_contract_registry


RUNTIME_TOPOLOGY_REPORT_VERSION = 1
DEFAULT_REPORT_REL_PATH = Path("logs") / "runtime_topology" / "latest.json"
LEGACY_REPLAY_UNITS = (
    "autobot-paper-v4-replay.service",
    "autobot-live-alpha-replay-shadow.service",
)


def build_runtime_topology_report(
    *,
    project_root: str | Path,
    target_unit: str | None = None,
    ts_ms: int | None = None,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    effective_ts_ms = int(ts_ms or 0)
    registry_root = root / "models" / "registry"
    data_contract_registry = build_data_contract_registry(project_root=root)

    family = "train_v4_crypto_cs"
    family_dir = registry_root / family
    pointers = {
        "champion": _load_pointer(family_dir / "champion.json"),
        "latest": _load_pointer(family_dir / "latest.json"),
        "latest_candidate": _load_pointer(family_dir / "latest_candidate.json"),
        "global_latest": _load_pointer(registry_root / "latest.json"),
        "global_latest_candidate": _load_pointer(registry_root / "latest_candidate.json"),
    }

    live_db_candidates = [
        root / "data" / "state" / "live" / "live_state.db",
        root / "data" / "state" / "live_state.db",
    ]
    candidate_db_candidates = [
        root / "data" / "state" / "live_candidate" / "live_state.db",
    ]
    live_db = _first_existing_file(live_db_candidates)
    candidate_db = _first_existing_file(candidate_db_candidates)
    target_topology = _target_topology_contract(
        project_root=root,
        live_db_candidates=live_db_candidates,
        candidate_db_candidates=candidate_db_candidates,
    )

    live_state = _load_state_topology(db_path=live_db)
    candidate_state = _load_state_topology(db_path=candidate_db)

    rollout_latest = load_rollout_latest(root, target_unit=target_unit)

    live_lane_defaults = _load_unit_runtime_defaults(
        root,
        unit_name="autobot-live-alpha.service",
        fallback_model_ref_source="champion_v4",
    )
    candidate_lane_defaults = _load_unit_runtime_defaults(
        root,
        unit_name="autobot-live-alpha-candidate.service",
        fallback_model_ref_source="latest_candidate_v4",
    )
    effective_target_unit = str(target_unit or "").strip() or None
    daemon_defaults = (
        candidate_lane_defaults
        if effective_target_unit in {"autobot-live-alpha-candidate.service", "autobot-paper-v4-challenger.service"}
        else live_lane_defaults
    )
    ws_public_contract = load_ws_public_runtime_contract(
        meta_dir=Path(str(daemon_defaults["ws_public_meta_dir"])),
        raw_root=Path(str(daemon_defaults["ws_public_raw_root"])),
        stale_threshold_sec=int(daemon_defaults["ws_public_stale_threshold_sec"]),
        micro_aggregate_report_path=Path(str(daemon_defaults["micro_aggregate_report_path"])),
        ts_ms=effective_ts_ms,
    )

    live_current_contract: dict[str, Any] = {}
    live_runtime_contract_error = ""
    try:
        live_current_contract = resolve_live_runtime_model_contract(
            registry_root=registry_root,
            model_ref=str(live_lane_defaults["runtime_model_ref_source"]),
            model_family=str(live_lane_defaults["runtime_model_family"]),
            ts_ms=effective_ts_ms,
        )
    except Exception as exc:  # pragma: no cover - defensive path
        live_runtime_contract_error = str(exc)

    candidate_current_contract: dict[str, Any] = {}
    candidate_runtime_contract_error = ""
    try:
        candidate_current_contract = resolve_live_runtime_model_contract(
            registry_root=registry_root,
            model_ref=str(candidate_lane_defaults["runtime_model_ref_source"]),
            model_family=str(candidate_lane_defaults["runtime_model_family"]),
            ts_ms=effective_ts_ms,
        )
    except Exception as exc:  # pragma: no cover - defensive path
        candidate_runtime_contract_error = str(exc)

    live_runtime_sync_status = build_live_runtime_sync_status(
        pinned_contract=dict((live_state.get("runtime_contract") or {})),
        current_contract=live_current_contract,
        ws_public_contract=ws_public_contract,
    )

    candidate_runtime_sync_status = build_live_runtime_sync_status(
        pinned_contract=dict((candidate_state.get("runtime_contract") or {})),
        current_contract=candidate_current_contract,
        ws_public_contract=ws_public_contract,
    )
    is_candidate_target = effective_target_unit in {
        "autobot-live-alpha-candidate.service",
        "autobot-paper-v4-challenger.service",
    }
    current_contract = candidate_current_contract if is_candidate_target else live_current_contract
    persisted_runtime_contract = (
        dict((candidate_state.get("runtime_contract") or {}))
        if is_candidate_target
        else dict((live_state.get("runtime_contract") or {}))
    )
    runtime_sync_status = candidate_runtime_sync_status if is_candidate_target else live_runtime_sync_status
    runtime_contract_error = (
        candidate_runtime_contract_error
        if is_candidate_target
        else live_runtime_contract_error
    )
    systemd_snapshot = _systemd_topology_snapshot()
    git_snapshot = _git_topology_snapshot(root=root)
    project_topology = _project_topology_snapshot(root=root)
    legacy_replay = _legacy_replay_snapshot(systemd_snapshot=systemd_snapshot, project_topology=project_topology)
    topology_health = _topology_health_status(
        systemd_snapshot=systemd_snapshot,
        runtime_sync_status=live_runtime_sync_status,
        ws_public_contract=ws_public_contract,
        live_db=live_db,
        candidate_db=candidate_db,
        legacy_replay=legacy_replay,
    )

    report = {
        "version": RUNTIME_TOPOLOGY_REPORT_VERSION,
        "project_root": str(root),
        "generated_at_ts_ms": effective_ts_ms if effective_ts_ms > 0 else None,
        "registry_root": str(registry_root),
        "model_family": family,
        "pointers": pointers,
        "data_contract_registry_excerpt": {
            "path_default": data_contract_registry.get("registry_path_default"),
            "summary": dict(data_contract_registry.get("summary") or {}),
        },
        "server_intended_defaults": daemon_defaults,
        "lane_runtime_defaults": {
            "live_main": live_lane_defaults,
            "live_candidate": candidate_lane_defaults,
        },
        "live_lane": live_state,
        "candidate_lane": candidate_state,
        "ws_public_contract": ws_public_contract,
        "current_runtime_contract": current_contract,
        "persisted_runtime_contract": persisted_runtime_contract,
        "live_runtime_contract": live_current_contract,
        "candidate_runtime_contract": candidate_current_contract,
        "runtime_sync_status": runtime_sync_status,
        "live_runtime_sync_status": live_runtime_sync_status,
        "candidate_runtime_sync_status": candidate_runtime_sync_status,
        "runtime_contract_error": runtime_contract_error or None,
        "runtime_contract_errors": {
            "live": live_runtime_contract_error or None,
            "candidate": candidate_runtime_contract_error or None,
        },
        "rollout_latest": rollout_latest,
        "systemd": systemd_snapshot,
        "git": git_snapshot,
        "project_topology": project_topology,
        "target_topology": target_topology,
        "topology_health": topology_health,
        "legacy_replay": legacy_replay,
        "summary": {
            "champion_run_id": _run_id_from_pointer(pointers.get("champion")),
            "latest_run_id": _run_id_from_pointer(pointers.get("latest")),
            "latest_candidate_run_id": _run_id_from_pointer(pointers.get("latest_candidate")),
            "all_primary_pointers_equal": _all_primary_pointers_equal(pointers),
            "candidate_db_present": bool(candidate_db),
            "live_db_present": bool(live_db),
            "model_pointer_divergence": bool(runtime_sync_status.get("model_pointer_divergence", False)),
            "ws_public_stale": bool(ws_public_contract.get("ws_public_stale", False)),
            "systemd_available": bool(systemd_snapshot.get("available", False)),
            "service_active_count": int(sum(1 for item in (systemd_snapshot.get("services") or []) if str(item.get("active", "")).strip().lower() == "active")),
            "target_service_active_count": int(sum(1 for item in (systemd_snapshot.get("services") or []) if _is_target_topology_service(item) and str(item.get("active", "")).strip().lower() == "active")),
            "timer_active_count": int(sum(1 for item in (systemd_snapshot.get("timers") or []) if str(item.get("active", "")).strip().lower() == "active")),
            "git_dirty": bool(git_snapshot.get("dirty", False)),
            "replay_path_present": bool(project_topology.get("replay_path_present", False)),
            "legacy_replay_present": bool(legacy_replay.get("present", False)),
            "legacy_replay_active": bool(int(legacy_replay.get("active_unit_count", 0)) > 0),
            "target_topology_replay_excluded": bool(legacy_replay.get("target_topology_excluded", False)),
            "topology_health_status": str(topology_health.get("status", "unknown")).strip().lower() or "unknown",
            "topology_health_reason_codes": list(topology_health.get("reason_codes") or []),
        },
    }
    return report


def write_runtime_topology_report(
    *,
    project_root: str | Path,
    output_path: str | Path | None = None,
    target_unit: str | None = None,
    ts_ms: int | None = None,
) -> Path:
    root = Path(project_root).resolve()
    path = Path(output_path).resolve() if output_path is not None else (root / DEFAULT_REPORT_REL_PATH)
    payload = build_runtime_topology_report(project_root=root, target_unit=target_unit, ts_ms=ts_ms)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _load_live_defaults(project_root: Path, *, target_unit: str | None = None) -> dict[str, Any]:
    target_unit_text = str(target_unit or "").strip().lower()
    runtime_model_ref_source = (
        "latest_candidate_v4"
        if target_unit_text in {"autobot-live-alpha-candidate.service", "autobot-paper-v4-challenger.service"}
        else "champion_v4"
    )
    return {
        "registry_root": str(project_root / "models" / "registry"),
        "runtime_model_ref_source": runtime_model_ref_source,
        "runtime_model_family": "train_v4_crypto_cs",
        "ws_public_raw_root": str(project_root / "data" / "raw_ws" / "upbit" / "public"),
        "ws_public_meta_dir": str(project_root / "data" / "raw_ws" / "upbit" / "_meta"),
        "ws_public_stale_threshold_sec": 180,
        "micro_aggregate_report_path": str(project_root / "data" / "parquet" / "micro_v1" / "_meta" / "aggregate_report.json"),
    }


def _load_unit_runtime_defaults(
    project_root: Path,
    *,
    unit_name: str,
    fallback_model_ref_source: str,
) -> dict[str, Any]:
    defaults = _load_live_defaults(project_root, target_unit=unit_name)
    payload = _systemd_show_properties(unit_name, "Environment")
    environment = _parse_systemd_environment(str(payload.get("Environment", "")))
    model_ref_source = str(environment.get("AUTOBOT_LIVE_MODEL_REF_SOURCE") or defaults["runtime_model_ref_source"]).strip() or defaults["runtime_model_ref_source"]
    model_family = str(environment.get("AUTOBOT_LIVE_MODEL_FAMILY") or defaults["runtime_model_family"]).strip() or defaults["runtime_model_family"]
    defaults["runtime_model_ref_source"] = model_ref_source or fallback_model_ref_source
    defaults["runtime_model_family"] = model_family
    defaults["source_unit"] = unit_name
    return defaults


def _load_pointer(path: Path) -> dict[str, Any]:
    payload = load_json(path)
    if not isinstance(payload, dict):
        return {}
    result = dict(payload)
    result["path"] = str(path)
    result["exists"] = path.exists()
    return result


def _load_state_topology(*, db_path: Path | None) -> dict[str, Any]:
    if db_path is None or not db_path.exists():
        return {
            "db_path": str(db_path) if db_path is not None else None,
            "exists": False,
            "runtime_contract": {},
            "live_runtime_health": {},
            "live_rollout_status": {},
            "live_rollout_contract": {},
            "last_run": {},
            "breaker_state": {},
        }
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        table_names = {
            str(row["name"])
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
            if row["name"]
        }
        checkpoints = {}
        if "checkpoints" in table_names:
            for name in (
                "live_runtime_contract",
                "live_runtime_health",
                "live_rollout_status",
                "live_rollout_contract",
                "daemon_last_run",
                "live_model_alpha_last_run",
            ):
                row = conn.execute("SELECT payload_json FROM checkpoints WHERE name = ?", (name,)).fetchone()
                checkpoints[name] = _parse_json_text(row["payload_json"]) if row is not None else {}
        breaker_state = {}
        if "breaker_state" in table_names:
            breaker_row = conn.execute(
                "SELECT active, action, source, reason_codes_json, details_json, updated_ts, armed_ts FROM breaker_state WHERE breaker_key = ?",
                ("live",),
            ).fetchone()
            if breaker_row is not None:
                breaker_state = {
                    "active": bool(breaker_row["active"]),
                    "action": breaker_row["action"],
                    "source": breaker_row["source"],
                    "reason_codes": _parse_json_text(breaker_row["reason_codes_json"]) or [],
                    "details": _parse_json_text(breaker_row["details_json"]) or {},
                    "updated_ts": breaker_row["updated_ts"],
                    "armed_ts": breaker_row["armed_ts"],
                }
                breaker_state = annotate_reason_payload(
                    breaker_state,
                    reason_codes=breaker_state.get("reason_codes") or [],
                )
        return {
            "db_path": str(db_path),
            "exists": True,
            "runtime_contract": checkpoints.get("live_runtime_contract") or {},
            "live_runtime_health": checkpoints.get("live_runtime_health") or {},
            "live_rollout_status": checkpoints.get("live_rollout_status") or {},
            "live_rollout_contract": checkpoints.get("live_rollout_contract") or {},
            "last_run": checkpoints.get("live_model_alpha_last_run") or checkpoints.get("daemon_last_run") or {},
            "breaker_state": breaker_state,
        }
    finally:
        conn.close()


def _parse_json_text(value: Any) -> Any:
    if value in (None, ""):
        return {}
    try:
        return json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}


def _first_existing_file(candidates: list[Path]) -> Path | None:
    for path in candidates:
        if path.exists():
            return path
    return None


def _run_id_from_pointer(pointer: dict[str, Any] | None) -> str:
    if not isinstance(pointer, dict):
        return ""
    return str(pointer.get("run_id", "")).strip()


def _all_primary_pointers_equal(pointers: dict[str, dict[str, Any]]) -> bool:
    values = [
        _run_id_from_pointer(pointers.get("champion")),
        _run_id_from_pointer(pointers.get("latest")),
        _run_id_from_pointer(pointers.get("latest_candidate")),
    ]
    values = [item for item in values if item]
    return len(set(values)) == 1 if values else False


def _systemd_topology_snapshot() -> dict[str, Any]:
    service_cmd = [
        "systemctl",
        "list-units",
        "autobot*",
        "--type=service",
        "--all",
        "--no-pager",
        "--plain",
        "--no-legend",
    ]
    timer_cmd = [
        "systemctl",
        "list-units",
        "autobot*",
        "--type=timer",
        "--all",
        "--no-pager",
        "--plain",
        "--no-legend",
    ]
    unit_file_cmd = [
        "systemctl",
        "list-unit-files",
        "autobot*",
        "--no-pager",
        "--plain",
        "--no-legend",
    ]
    service_result = _run_command(service_cmd)
    timer_result = _run_command(timer_cmd)
    unit_file_result = _run_command(unit_file_cmd)
    available = bool(service_result["ok"] or timer_result["ok"] or unit_file_result["ok"])
    return {
        "available": available,
        "services": _parse_systemd_unit_rows(service_result["stdout"]),
        "timers": _parse_systemd_unit_rows(timer_result["stdout"]),
        "unit_files": _parse_systemd_unit_file_rows(unit_file_result["stdout"]),
        "errors": {
            "services": service_result["stderr"] if not service_result["ok"] else "",
            "timers": timer_result["stderr"] if not timer_result["ok"] else "",
            "unit_files": unit_file_result["stderr"] if not unit_file_result["ok"] else "",
        },
    }


def _systemd_show_properties(unit_name: str, *properties: str) -> dict[str, str]:
    command = ["systemctl", "show", str(unit_name)]
    if properties:
        command.extend(["--property", ",".join(str(item) for item in properties if str(item).strip())])
    command.extend(["--no-pager"])
    result = _run_command(command)
    if not result["ok"]:
        return {}
    payload: dict[str, str] = {}
    for line in str(result["stdout"]).splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        payload[str(key).strip()] = str(value).strip()
    return payload


def _parse_systemd_environment(text: str) -> dict[str, str]:
    payload: dict[str, str] = {}
    for token in str(text or "").split(" "):
        item = str(token).strip()
        if not item or "=" not in item:
            continue
        key, value = item.split("=", 1)
        key = str(key).strip()
        if not key:
            continue
        payload[key] = str(value).strip().strip('"')
    return payload


def _git_topology_snapshot(*, root: Path) -> dict[str, Any]:
    head_result = _run_command(["git", "rev-parse", "HEAD"], cwd=root)
    branch_result = _run_command(["git", "branch", "--show-current"], cwd=root)
    status_result = _run_command(["git", "status", "--short"], cwd=root)
    remote_result = _run_command(["git", "remote", "get-url", "origin"], cwd=root)
    status_lines = [line.rstrip() for line in str(status_result["stdout"]).splitlines() if line.strip()]
    return {
        "available": bool(head_result["ok"]),
        "head": str(head_result["stdout"]).strip() if head_result["ok"] else "",
        "branch": str(branch_result["stdout"]).strip() if branch_result["ok"] else "",
        "remote_origin": str(remote_result["stdout"]).strip() if remote_result["ok"] else "",
        "status_short": status_lines,
        "dirty": bool(status_lines),
        "errors": {
            "head": head_result["stderr"] if not head_result["ok"] else "",
            "branch": branch_result["stderr"] if not branch_result["ok"] else "",
            "status": status_result["stderr"] if not status_result["ok"] else "",
            "remote_origin": remote_result["stderr"] if not remote_result["ok"] else "",
        },
    }


def _project_topology_snapshot(*, root: Path) -> dict[str, Any]:
    parent = root.parent
    sibling_dirs = sorted(path.name for path in parent.iterdir() if path.is_dir()) if parent.exists() else []
    replay_paths = [name for name in sibling_dirs if name.startswith("Autobot_replay")]
    return {
        "project_root_parent": str(parent),
        "sibling_directories": sibling_dirs,
        "replay_like_paths": replay_paths,
        "replay_path_present": bool(replay_paths),
    }


def _target_topology_contract(
    *,
    project_root: Path,
    live_db_candidates: list[Path],
    candidate_db_candidates: list[Path],
) -> dict[str, Any]:
    return {
        "mode": "champion_candidate_two_lane_v1",
        "lanes": {
            "champion": {
                "paper_unit": "autobot-paper-v4.service",
                "live_unit": "autobot-live-alpha.service",
                "model_ref_source": "champion_v4",
            },
            "candidate": {
                "paper_unit": "autobot-paper-v4-challenger.service",
                "paired_paper_unit": "autobot-paper-v4-paired.service",
                "live_unit": "autobot-live-alpha-candidate.service",
                "model_ref_source": "latest_candidate_v4",
            },
        },
        "shared_units": [
            "autobot-ws-public.service",
            "autobot-dashboard.service",
        ],
        "excluded_legacy_units": list(LEGACY_REPLAY_UNITS),
        "expected_state_db_paths": {
            "live": [str(path) for path in live_db_candidates],
            "candidate": [str(path) for path in candidate_db_candidates],
        },
        "project_root": str(project_root),
    }


def _legacy_replay_snapshot(*, systemd_snapshot: dict[str, Any], project_topology: dict[str, Any]) -> dict[str, Any]:
    services = systemd_snapshot.get("services") or []
    unit_files = systemd_snapshot.get("unit_files") or []
    service_units: list[dict[str, Any]] = []
    active_unit_count = 0
    enabled_unit_count = 0
    for unit_name in LEGACY_REPLAY_UNITS:
        service_match = next((item for item in services if str(item.get("unit", "")).strip() == unit_name), {})
        unit_file_match = next((item for item in unit_files if str(item.get("unit_file", "")).strip() == unit_name), {})
        active = str(service_match.get("active", "")).strip().lower()
        sub = str(service_match.get("sub", "")).strip().lower()
        is_active_like = active == "active" and sub in {"running", "waiting", "listening", "exited"}
        if is_active_like:
            active_unit_count += 1
        unit_file_state = str(unit_file_match.get("state", "")).strip().lower()
        if unit_file_state == "enabled":
            enabled_unit_count += 1
        service_units.append(
            {
                "unit": unit_name,
                "present": bool(service_match) or bool(unit_file_match),
                "active": service_match.get("active"),
                "sub": service_match.get("sub"),
                "description": service_match.get("description"),
                "unit_file_state": unit_file_match.get("state"),
                "unit_file_preset": unit_file_match.get("preset"),
                "is_active_like": is_active_like,
            }
        )
    clone_paths = list(project_topology.get("replay_like_paths") or [])
    present = bool(clone_paths or any(bool(item.get("present", False)) for item in service_units))
    return {
        "policy": "replay_legacy_cleanup_v1",
        "classification": "legacy_excluded_from_target_topology",
        "target_topology_excluded": True,
        "present": present,
        "clone_paths": clone_paths,
        "service_units": service_units,
        "active_unit_count": active_unit_count,
        "enabled_unit_count": enabled_unit_count,
        "cleanup_required": present,
        "recommended_actions": [
            "stop_and_disable_replay_services",
            "archive_or_remove_replay_clone_after_review",
        ],
    }


def _topology_health_status(
    *,
    systemd_snapshot: dict[str, Any],
    runtime_sync_status: dict[str, Any],
    ws_public_contract: dict[str, Any],
    live_db: Path | None,
    candidate_db: Path | None,
    legacy_replay: dict[str, Any],
) -> dict[str, Any]:
    reason_codes: list[str] = []
    if not bool(systemd_snapshot.get("available", False)):
        reason_codes.append("SYSTEMD_UNAVAILABLE")
    if live_db is None:
        reason_codes.append("LIVE_DB_MISSING")
    if candidate_db is None:
        reason_codes.append("CANDIDATE_DB_MISSING")
    if bool(ws_public_contract.get("ws_public_stale", False)):
        reason_codes.append("WS_PUBLIC_STALE")
    runtime_current_contract = dict(runtime_sync_status.get("current_contract") or {})
    pointer_name = str(runtime_current_contract.get("resolved_pointer_name") or "").strip().lower()
    monitored_unit = (
        "autobot-live-alpha-candidate.service"
        if pointer_name == "latest_candidate"
        else "autobot-live-alpha.service"
    )
    if bool(runtime_sync_status.get("model_pointer_divergence", False)) and _systemd_unit_active_like(
        systemd_snapshot=systemd_snapshot,
        unit_name=monitored_unit,
    ):
        reason_codes.append("MODEL_POINTER_DIVERGENCE")
    if bool(legacy_replay.get("active_unit_count", 0)):
        reason_codes.append("LEGACY_REPLAY_ACTIVE")
    if not reason_codes:
        status = "healthy"
    elif any(code in {"SYSTEMD_UNAVAILABLE", "LIVE_DB_MISSING", "CANDIDATE_DB_MISSING", "MODEL_POINTER_DIVERGENCE"} for code in reason_codes):
        status = "violation"
    else:
        status = "degraded"
    return {
        "status": status,
        "reason_codes": reason_codes,
    }


def _systemd_unit_active_like(*, systemd_snapshot: dict[str, Any], unit_name: str) -> bool:
    target = str(unit_name).strip()
    for item in systemd_snapshot.get("services") or []:
        if str(item.get("unit", "")).strip() != target:
            continue
        active = str(item.get("active", "")).strip().lower()
        sub = str(item.get("sub", "")).strip().lower()
        return active == "active" and sub in {"running", "waiting", "listening", "exited"}
    return False


def _is_target_topology_service(item: dict[str, Any]) -> bool:
    unit_name = str(item.get("unit", "")).strip()
    if not unit_name:
        return False
    return unit_name not in LEGACY_REPLAY_UNITS


def _run_command(command: list[str], *, cwd: Path | None = None) -> dict[str, Any]:
    try:
        completed = subprocess.run(
            command,
            cwd=str(cwd) if cwd is not None else None,
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError as exc:
        return {
            "ok": False,
            "stdout": "",
            "stderr": str(exc),
            "returncode": 127,
        }
    return {
        "ok": completed.returncode == 0,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
        "returncode": completed.returncode,
    }


def _parse_systemd_unit_rows(text: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw_line in str(text).splitlines():
        line = raw_line.strip()
        if not line:
            continue
        parts = line.split(None, 4)
        if len(parts) < 4:
            rows.append({"raw": line})
            continue
        payload = {
            "unit": parts[0],
            "load": parts[1],
            "active": parts[2],
            "sub": parts[3],
            "description": parts[4] if len(parts) > 4 else "",
        }
        rows.append(payload)
    return rows


def _parse_systemd_unit_file_rows(text: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw_line in str(text).splitlines():
        line = raw_line.strip()
        if not line:
            continue
        parts = line.split(None, 2)
        if len(parts) < 2:
            rows.append({"raw": line})
            continue
        payload = {
            "unit_file": parts[0],
            "state": parts[1],
            "preset": parts[2] if len(parts) > 2 else "",
        }
        rows.append(payload)
    return rows


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build machine-readable runtime topology report.")
    parser.add_argument("--project-root", default=".", help="Project root directory")
    parser.add_argument("--out", default="", help="Optional output path override")
    parser.add_argument("--target-unit", default="", help="Optional rollout target unit")
    parser.add_argument("--ts-ms", type=int, default=0, help="Override timestamp in milliseconds")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    output_path = Path(str(args.out)).resolve() if str(args.out).strip() else None
    path = write_runtime_topology_report(
        project_root=Path(str(args.project_root)),
        output_path=output_path,
        target_unit=(str(args.target_unit).strip() or None),
        ts_ms=(int(args.ts_ms) if int(args.ts_ms) > 0 else None),
    )
    print(f"[ops][runtime-topology] path={path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
