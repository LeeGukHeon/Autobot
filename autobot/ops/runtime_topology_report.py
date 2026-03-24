"""Machine-readable runtime topology report builders."""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any

from autobot.live.model_handoff import build_live_runtime_sync_status, load_ws_public_runtime_contract, resolve_live_runtime_model_contract
from autobot.live.rollout import load_rollout_latest
from autobot.models.registry import load_json
from .data_contract_registry import build_data_contract_registry


RUNTIME_TOPOLOGY_REPORT_VERSION = 1
DEFAULT_REPORT_REL_PATH = Path("logs") / "runtime_topology" / "latest.json"


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

    live_state = _load_state_topology(db_path=live_db)
    candidate_state = _load_state_topology(db_path=candidate_db)

    rollout_latest = load_rollout_latest(root, target_unit=target_unit)

    daemon_defaults = _load_live_defaults(root)
    ws_public_contract = load_ws_public_runtime_contract(
        meta_dir=Path(str(daemon_defaults["ws_public_meta_dir"])),
        raw_root=Path(str(daemon_defaults["ws_public_raw_root"])),
        stale_threshold_sec=int(daemon_defaults["ws_public_stale_threshold_sec"]),
        micro_aggregate_report_path=Path(str(daemon_defaults["micro_aggregate_report_path"])),
        ts_ms=effective_ts_ms,
    )

    current_contract = {}
    runtime_contract_error = ""
    try:
        current_contract = resolve_live_runtime_model_contract(
            registry_root=registry_root,
            model_ref=str(daemon_defaults["runtime_model_ref_source"]),
            model_family=str(daemon_defaults["runtime_model_family"]),
            ts_ms=effective_ts_ms,
        )
    except Exception as exc:  # pragma: no cover - defensive path
        runtime_contract_error = str(exc)

    persisted_runtime_contract = dict((candidate_state.get("runtime_contract") or {}))
    runtime_sync_status = build_live_runtime_sync_status(
        pinned_contract=persisted_runtime_contract,
        current_contract=current_contract,
        ws_public_contract=ws_public_contract,
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
        "live_lane": live_state,
        "candidate_lane": candidate_state,
        "ws_public_contract": ws_public_contract,
        "current_runtime_contract": current_contract,
        "persisted_runtime_contract": persisted_runtime_contract,
        "runtime_sync_status": runtime_sync_status,
        "runtime_contract_error": runtime_contract_error or None,
        "rollout_latest": rollout_latest,
        "summary": {
            "champion_run_id": _run_id_from_pointer(pointers.get("champion")),
            "latest_run_id": _run_id_from_pointer(pointers.get("latest")),
            "latest_candidate_run_id": _run_id_from_pointer(pointers.get("latest_candidate")),
            "all_primary_pointers_equal": _all_primary_pointers_equal(pointers),
            "candidate_db_present": bool(candidate_db),
            "live_db_present": bool(live_db),
            "model_pointer_divergence": bool(runtime_sync_status.get("model_pointer_divergence", False)),
            "ws_public_stale": bool(ws_public_contract.get("ws_public_stale", False)),
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


def _load_live_defaults(project_root: Path) -> dict[str, Any]:
    return {
        "registry_root": str(project_root / "models" / "registry"),
        "runtime_model_ref_source": "champion_v4",
        "runtime_model_family": "train_v4_crypto_cs",
        "ws_public_raw_root": str(project_root / "data" / "raw_ws" / "upbit" / "public"),
        "ws_public_meta_dir": str(project_root / "data" / "raw_ws" / "upbit" / "_meta"),
        "ws_public_stale_threshold_sec": 180,
        "micro_aggregate_report_path": str(project_root / "data" / "parquet" / "micro_v1" / "_meta" / "aggregate_report.json"),
    }


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
