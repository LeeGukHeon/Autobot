"""Machine-readable pointer consistency report builders."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from autobot.models.registry import load_json
from .runtime_topology_report import _systemd_topology_snapshot


POINTER_CONSISTENCY_REPORT_VERSION = 1
DEFAULT_REPORT_REL_PATH = Path("logs") / "ops" / "pointer_consistency" / "latest.json"
DEFAULT_MODEL_FAMILY = "train_v5_fusion"


def build_pointer_consistency_report(
    *,
    project_root: str | Path,
    model_family: str = DEFAULT_MODEL_FAMILY,
    champion_pointer_family: str | None = None,
    ts_ms: int | None = None,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    registry_root = root / "models" / "registry"
    effective_ts_ms = int(ts_ms or 0)
    current_state_path = root / "logs" / "model_v4_challenger" / "current_state.json"
    current_state = _load_json_file(current_state_path)
    effective_champion_pointer_family = (
        str(
            champion_pointer_family
            or current_state.get("champion_model_family_at_start")
            or current_state.get("champion_compare_model_family")
            or model_family
        ).strip()
        or model_family
    )
    family_dir = registry_root / model_family
    champion_family_dir = registry_root / effective_champion_pointer_family

    pointers = {
        "champion": _load_pointer_snapshot(
            path=champion_family_dir / "champion.json",
            registry_root=registry_root,
            default_family=effective_champion_pointer_family,
        ),
        "latest": _load_pointer_snapshot(
            path=family_dir / "latest.json",
            registry_root=registry_root,
            default_family=model_family,
        ),
        "latest_candidate": _load_pointer_snapshot(
            path=family_dir / "latest_candidate.json",
            registry_root=registry_root,
            default_family=model_family,
        ),
        "global_latest": _load_pointer_snapshot(
            path=registry_root / "latest.json",
            registry_root=registry_root,
            default_family=None,
        ),
        "global_latest_candidate": _load_pointer_snapshot(
            path=registry_root / "latest_candidate.json",
            registry_root=registry_root,
            default_family=None,
        ),
    }
    latest_challenger_report_path = root / "logs" / "model_v4_challenger" / "latest.json"
    latest_challenger_report = _load_json_file(latest_challenger_report_path)

    systemd_snapshot = _systemd_topology_snapshot()
    runtime_units = {
        "candidate_live": _service_snapshot(systemd_snapshot, "autobot-live-alpha-candidate.service"),
        "champion_live": _service_snapshot(systemd_snapshot, "autobot-live-alpha.service"),
        "candidate_paper": _service_snapshot(systemd_snapshot, "autobot-paper-v4-challenger.service"),
        "champion_paper": _service_snapshot(systemd_snapshot, "autobot-paper-v4.service"),
    }

    checks: list[dict[str, Any]] = []

    _check_pointer_pair(
        checks=checks,
        family_pointer=pointers["latest"],
        global_pointer=pointers["global_latest"],
        pointer_name="latest",
        model_family=model_family,
        require_global_alignment=(model_family == DEFAULT_MODEL_FAMILY),
        required=True,
    )
    _check_pointer_pair(
        checks=checks,
        family_pointer=pointers["latest_candidate"],
        global_pointer=pointers["global_latest_candidate"],
        pointer_name="latest_candidate",
        model_family=model_family,
        require_global_alignment=(model_family == DEFAULT_MODEL_FAMILY),
        required=False,
    )
    _check_run_dir_exists(checks=checks, pointer_name="champion", pointer=pointers["champion"], required=True)
    _check_run_dir_exists(checks=checks, pointer_name="latest", pointer=pointers["latest"], required=True)
    _check_run_dir_exists(checks=checks, pointer_name="latest_candidate", pointer=pointers["latest_candidate"], required=False)
    _check_candidate_state_alignment(
        checks=checks,
        current_state=current_state,
        current_state_path=current_state_path,
        latest_candidate_pointer=pointers["latest_candidate"],
        champion_pointer=pointers["champion"],
    )
    _check_current_state_contract_fields(
        checks=checks,
        current_state=current_state,
        current_state_path=current_state_path,
    )
    _check_candidate_units(
        checks=checks,
        runtime_units=runtime_units,
        latest_candidate_pointer=pointers["latest_candidate"],
        current_state=current_state,
    )
    _check_latest_challenger_report(
        checks=checks,
        latest_challenger_report=latest_challenger_report,
        latest_candidate_pointer=pointers["latest_candidate"],
        current_state=current_state,
    )

    summary = _summarize_checks(checks)
    return {
        "version": POINTER_CONSISTENCY_REPORT_VERSION,
        "project_root": str(root),
        "registry_root": str(registry_root),
        "model_family": model_family,
        "champion_pointer_family": effective_champion_pointer_family,
        "generated_at_ts_ms": effective_ts_ms if effective_ts_ms > 0 else None,
        "pointers": pointers,
        "challenger_state": {
            "current_state_path": str(current_state_path),
            "current_state_exists": current_state_path.exists(),
            "current_state": {
                "candidate_run_id": str(current_state.get("candidate_run_id", "")).strip() or None,
                "champion_run_id_at_start": str(current_state.get("champion_run_id_at_start", "")).strip() or None,
                "started_ts_ms": int(current_state.get("started_ts_ms") or 0) if current_state.get("started_ts_ms") is not None else None,
                "started_at_utc": str(current_state.get("started_at_utc", "")).strip() or None,
                "lane_mode": str(current_state.get("lane_mode", "")).strip() or None,
                "promotion_eligible": bool(current_state.get("promotion_eligible", False)),
                "bootstrap_only": bool(current_state.get("bootstrap_only", False)),
            },
            "latest_report_path": str(latest_challenger_report_path),
            "latest_report_exists": latest_challenger_report_path.exists(),
            "latest_report": {
                "mode": str(latest_challenger_report.get("mode", "")).strip() or None,
                "batch_date": str(latest_challenger_report.get("batch_date", "")).strip() or None,
                "exception_message": str((latest_challenger_report.get("exception") or {}).get("message", "")).strip() or None,
                "promote_previous_challenger_reason": (
                    str((((latest_challenger_report.get("steps") or {}).get("promote_previous_challenger") or {}).get("reason", "")).strip())
                    or None
                ),
                "clear_latest_candidate_reason": (
                    str((((latest_challenger_report.get("steps") or {}).get("clear_latest_candidate") or {}).get("reason", "")).strip())
                    or None
                ),
            },
        },
        "runtime_units": runtime_units,
        "checks": checks,
        "summary": summary,
    }


def write_pointer_consistency_report(
    *,
    project_root: str | Path,
    output_path: str | Path | None = None,
    model_family: str = DEFAULT_MODEL_FAMILY,
    champion_pointer_family: str | None = None,
    ts_ms: int | None = None,
) -> Path:
    root = Path(project_root).resolve()
    path = Path(output_path).resolve() if output_path is not None else (root / DEFAULT_REPORT_REL_PATH)
    payload = build_pointer_consistency_report(
        project_root=root,
        model_family=model_family,
        champion_pointer_family=champion_pointer_family,
        ts_ms=ts_ms,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _load_pointer_snapshot(
    *,
    path: Path,
    registry_root: Path,
    default_family: str | None,
) -> dict[str, Any]:
    payload = load_json(path)
    run_id = str(payload.get("run_id", "")).strip()
    resolved_family = str(payload.get("model_family", default_family or "")).strip()
    run_dir = registry_root / resolved_family / run_id if run_id and resolved_family else None
    return {
        **payload,
        "path": str(path),
        "exists": path.exists(),
        "run_id": run_id or None,
        "resolved_family": resolved_family or None,
        "run_dir": str(run_dir) if run_dir is not None else None,
        "run_dir_exists": bool(run_dir and run_dir.exists()),
    }


def _load_json_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _service_snapshot(systemd_snapshot: dict[str, Any], unit_name: str) -> dict[str, Any]:
    services = systemd_snapshot.get("services") or []
    match = next((item for item in services if str(item.get("unit", "")).strip() == unit_name), {})
    active = str(match.get("active", "")).strip().lower()
    sub = str(match.get("sub", "")).strip().lower()
    return {
        "unit": unit_name,
        "present": bool(match),
        "load": match.get("load"),
        "active": match.get("active"),
        "sub": match.get("sub"),
        "description": match.get("description"),
        "is_active_like": active == "active" and sub in {"running", "waiting", "listening", "exited"},
    }


def _check_pointer_pair(
    *,
    checks: list[dict[str, Any]],
    family_pointer: dict[str, Any],
    global_pointer: dict[str, Any],
    pointer_name: str,
    model_family: str,
    require_global_alignment: bool,
    required: bool,
) -> None:
    family_run_id = str(family_pointer.get("run_id") or "").strip()
    global_run_id = str(global_pointer.get("run_id") or "").strip()
    global_family = str(global_pointer.get("resolved_family") or "").strip()
    if not require_global_alignment:
        if not family_run_id:
            if required:
                _append_check(
                    checks,
                    code=f"{pointer_name.upper()}_POINTER_MISSING",
                    status="violation",
                    message=f"{pointer_name} pointer is missing in family scope.",
                )
            else:
                _append_check(
                    checks,
                    code=f"{pointer_name.upper()}_POINTER_ABSENT",
                    status="pass",
                    message=f"{pointer_name} pointer is absent in family scope.",
                )
            return
        _append_check(
            checks,
            code=f"{pointer_name.upper()}_FAMILY_PRESENT_GLOBAL_NOT_ENFORCED",
            status="pass",
            message=f"{pointer_name} pointer is present in family scope and global alignment is not enforced for {model_family}.",
            evidence={"run_id": family_run_id or None},
        )
        return
    if not family_run_id and not global_run_id:
        if required:
            _append_check(
                checks,
                code=f"{pointer_name.upper()}_POINTER_MISSING",
                status="violation",
                message=f"{pointer_name} pointer is missing in both family and global registry scopes.",
            )
        else:
            _append_check(
                checks,
                code=f"{pointer_name.upper()}_POINTER_ABSENT",
                status="pass",
                message=f"{pointer_name} pointer is absent in both family and global scopes.",
            )
        return
    if bool(family_run_id) != bool(global_run_id):
        _append_check(
            checks,
            code=f"{pointer_name.upper()}_FAMILY_GLOBAL_PRESENCE_MISMATCH",
            status="violation",
            message=f"{pointer_name} pointer exists in only one of family/global scopes.",
            evidence={
                "family_exists": bool(family_run_id),
                "global_exists": bool(global_run_id),
            },
        )
        return
    if global_family and global_family != model_family:
        _append_check(
            checks,
            code=f"{pointer_name.upper()}_GLOBAL_FAMILY_MISMATCH",
            status="violation",
            message=f"global {pointer_name} pointer family does not match {model_family}.",
            evidence={"global_family": global_family},
        )
        return
    if family_run_id != global_run_id:
        _append_check(
            checks,
            code=f"{pointer_name.upper()}_FAMILY_GLOBAL_RUN_ID_MISMATCH",
            status="violation",
            message=f"{pointer_name} pointer run_id differs between family and global scopes.",
            evidence={
                "family_run_id": family_run_id or None,
                "global_run_id": global_run_id or None,
            },
        )
        return
    _append_check(
        checks,
        code=f"{pointer_name.upper()}_FAMILY_GLOBAL_MATCH",
        status="pass",
        message=f"{pointer_name} pointer is aligned between family and global scopes.",
        evidence={"run_id": family_run_id or None},
    )


def _check_run_dir_exists(
    *,
    checks: list[dict[str, Any]],
    pointer_name: str,
    pointer: dict[str, Any],
    required: bool,
) -> None:
    run_id = str(pointer.get("run_id") or "").strip()
    if not run_id:
        if required:
            _append_check(
                checks,
                code=f"{pointer_name.upper()}_RUN_DIR_UNRESOLVED",
                status="violation",
                message=f"{pointer_name} pointer has no resolvable run_id.",
            )
        else:
            _append_check(
                checks,
                code=f"{pointer_name.upper()}_RUN_DIR_ABSENT",
                status="pass",
                message=f"{pointer_name} pointer is absent so no run_dir is expected.",
            )
        return
    if not bool(pointer.get("run_dir_exists", False)):
        _append_check(
            checks,
            code=f"{pointer_name.upper()}_RUN_DIR_MISSING",
            status="violation",
            message=f"{pointer_name} pointer run_dir is missing.",
            evidence={"run_dir": pointer.get("run_dir")},
        )
        return
    _append_check(
        checks,
        code=f"{pointer_name.upper()}_RUN_DIR_PRESENT",
        status="pass",
        message=f"{pointer_name} pointer resolves to an existing run_dir.",
        evidence={"run_dir": pointer.get("run_dir")},
    )


def _check_candidate_state_alignment(
    *,
    checks: list[dict[str, Any]],
    current_state: dict[str, Any],
    current_state_path: Path,
    latest_candidate_pointer: dict[str, Any],
    champion_pointer: dict[str, Any],
) -> None:
    latest_candidate_run_id = str(latest_candidate_pointer.get("run_id") or "").strip()
    champion_run_id = str(champion_pointer.get("run_id") or "").strip()
    current_state_exists = current_state_path.exists()
    current_candidate_run_id = str(current_state.get("candidate_run_id", "")).strip()
    champion_run_id_at_start = str(current_state.get("champion_run_id_at_start", "")).strip()

    if latest_candidate_run_id and not current_state_exists:
        _append_check(
            checks,
            code="LATEST_CANDIDATE_WITHOUT_CURRENT_STATE",
            status="violation",
            message="latest_candidate pointer exists but current_state.json is missing.",
            evidence={"latest_candidate_run_id": latest_candidate_run_id},
        )
    elif not latest_candidate_run_id and current_state_exists:
        _append_check(
            checks,
            code="CURRENT_STATE_WITHOUT_LATEST_CANDIDATE",
            status="violation",
            message="current_state.json exists but latest_candidate pointer is missing.",
            evidence={"current_state_candidate_run_id": current_candidate_run_id or None},
        )
    elif latest_candidate_run_id and current_state_exists and current_candidate_run_id != latest_candidate_run_id:
        _append_check(
            checks,
            code="LATEST_CANDIDATE_CURRENT_STATE_MISMATCH",
            status="violation",
            message="latest_candidate pointer does not match current_state candidate_run_id.",
            evidence={
                "latest_candidate_run_id": latest_candidate_run_id,
                "current_state_candidate_run_id": current_candidate_run_id or None,
            },
        )
    else:
        _append_check(
            checks,
            code="LATEST_CANDIDATE_CURRENT_STATE_ALIGNMENT",
            status="pass",
            message="latest_candidate pointer and current_state alignment look consistent.",
            evidence={
                "latest_candidate_run_id": latest_candidate_run_id or None,
                "current_state_exists": current_state_exists,
            },
        )

    if current_state_exists and not current_candidate_run_id:
        _append_check(
            checks,
            code="CURRENT_STATE_CANDIDATE_RUN_ID_MISSING",
            status="violation",
            message="current_state.json is present but candidate_run_id is blank.",
        )
    if current_state_exists and champion_run_id_at_start and champion_run_id and champion_run_id_at_start != champion_run_id:
        _append_check(
            checks,
            code="CURRENT_STATE_CHAMPION_BASELINE_DRIFTED",
            status="warning",
            message="current_state champion_run_id_at_start differs from the current champion pointer.",
            evidence={
                "champion_run_id_at_start": champion_run_id_at_start,
                "current_champion_run_id": champion_run_id,
            },
        )

    if champion_run_id and latest_candidate_run_id and champion_run_id == latest_candidate_run_id:
        status = "warning" if current_state_exists else "violation"
        code = (
            "CHAMPION_EQUALS_LATEST_CANDIDATE_TRANSITIONAL"
            if current_state_exists
            else "CHAMPION_EQUALS_LATEST_CANDIDATE_NO_TRANSITION_STATE"
        )
        _append_check(
            checks,
            code=code,
            status=status,
            message="champion and latest_candidate point to the same run_id.",
            evidence={"run_id": champion_run_id},
        )
    else:
        _append_check(
            checks,
            code="CHAMPION_LATEST_CANDIDATE_DISTINCT",
            status="pass",
            message="champion and latest_candidate pointers are distinct.",
            evidence={
                "champion_run_id": champion_run_id or None,
                "latest_candidate_run_id": latest_candidate_run_id or None,
            },
        )


def _check_candidate_units(
    *,
    checks: list[dict[str, Any]],
    runtime_units: dict[str, dict[str, Any]],
    latest_candidate_pointer: dict[str, Any],
    current_state: dict[str, Any],
) -> None:
    latest_candidate_run_id = str(latest_candidate_pointer.get("run_id") or "").strip()
    candidate_live_active = bool((runtime_units.get("candidate_live") or {}).get("is_active_like", False))
    candidate_paper_active = bool((runtime_units.get("candidate_paper") or {}).get("is_active_like", False))
    candidate_lane_active = candidate_live_active or candidate_paper_active
    current_state_exists = bool(current_state)

    if candidate_lane_active and not latest_candidate_run_id:
        _append_check(
            checks,
            code="CANDIDATE_UNITS_ACTIVE_WITHOUT_LATEST_CANDIDATE",
            status="violation",
            message="candidate lane units are active but latest_candidate pointer is absent.",
            evidence={
                "candidate_live_active": candidate_live_active,
                "candidate_paper_active": candidate_paper_active,
            },
        )
        return
    if latest_candidate_run_id and not candidate_lane_active:
        _append_check(
            checks,
            code="LATEST_CANDIDATE_WITH_NO_ACTIVE_CANDIDATE_LANE",
            status="warning",
            message="latest_candidate pointer exists but neither candidate paper nor candidate live is active.",
            evidence={"latest_candidate_run_id": latest_candidate_run_id},
        )
        return
    _append_check(
        checks,
        code="CANDIDATE_LANE_ACTIVITY_CONSISTENT",
        status="pass",
        message="candidate lane activity is consistent with latest_candidate pointer presence.",
        evidence={
            "latest_candidate_run_id": latest_candidate_run_id or None,
            "candidate_live_active": candidate_live_active,
            "candidate_paper_active": candidate_paper_active,
            "current_state_exists": current_state_exists,
        },
    )


def _check_current_state_contract_fields(
    *,
    checks: list[dict[str, Any]],
    current_state: dict[str, Any],
    current_state_path: Path,
) -> None:
    if not current_state_path.exists():
        return
    candidate_run_id = str(current_state.get("candidate_run_id") or "").strip()
    champion_run_id_at_start = str(current_state.get("champion_run_id_at_start") or "").strip()
    started_ts_ms = current_state.get("started_ts_ms")
    lane_mode = str(current_state.get("lane_mode") or "").strip()
    if not candidate_run_id:
        _append_check(
            checks,
            code="CURRENT_STATE_CANDIDATE_RUN_ID_MISSING",
            status="violation",
            message="current_state.json is present but candidate_run_id is blank.",
        )
    if not champion_run_id_at_start:
        _append_check(
            checks,
            code="CURRENT_STATE_CHAMPION_BASELINE_MISSING",
            status="violation",
            message="current_state.json is present but champion_run_id_at_start is blank.",
        )
    if started_ts_ms in (None, "", 0):
        _append_check(
            checks,
            code="CURRENT_STATE_STARTED_TS_MISSING",
            status="violation",
            message="current_state.json is present but started_ts_ms is missing.",
        )
    if not lane_mode:
        _append_check(
            checks,
            code="CURRENT_STATE_LANE_MODE_MISSING",
            status="violation",
            message="current_state.json is present but lane_mode is missing.",
        )
    if candidate_run_id and champion_run_id_at_start and started_ts_ms not in (None, "", 0) and lane_mode:
        _append_check(
            checks,
            code="CURRENT_STATE_CONTRACT_FIELDS_PRESENT",
            status="pass",
            message="current_state.json carries the required challenger handoff fields.",
            evidence={
                "candidate_run_id": candidate_run_id,
                "champion_run_id_at_start": champion_run_id_at_start,
                "started_ts_ms": int(started_ts_ms),
                "lane_mode": lane_mode,
            },
        )


def _check_latest_challenger_report(
    *,
    checks: list[dict[str, Any]],
    latest_challenger_report: dict[str, Any],
    latest_candidate_pointer: dict[str, Any],
    current_state: dict[str, Any],
) -> None:
    latest_candidate_run_id = str(latest_candidate_pointer.get("run_id") or "").strip()
    exception_message = str((latest_challenger_report.get("exception") or {}).get("message", "")).strip()
    if latest_candidate_run_id and not current_state and exception_message:
        _append_check(
            checks,
            code="LATEST_CANDIDATE_WITH_EXCEPTION_ONLY_CHALLENGER_REPORT",
            status="warning",
            message="latest challenger report ended with an exception while latest_candidate still points to a run and current_state is absent.",
            evidence={"exception_message": exception_message},
        )
    else:
        _append_check(
            checks,
            code="LATEST_CHALLENGER_REPORT_NOTES",
            status="pass",
            message="latest challenger report does not add an extra pointer-consistency warning.",
            evidence={"exception_message": exception_message or None},
        )


def _append_check(
    checks: list[dict[str, Any]],
    *,
    code: str,
    status: str,
    message: str,
    evidence: dict[str, Any] | None = None,
) -> None:
    checks.append(
        {
            "code": code,
            "status": status,
            "message": message,
            "evidence": dict(evidence or {}),
        }
    )


def _summarize_checks(checks: list[dict[str, Any]]) -> dict[str, Any]:
    violation_codes = [item["code"] for item in checks if item.get("status") == "violation"]
    warning_codes = [item["code"] for item in checks if item.get("status") == "warning"]
    pass_count = sum(1 for item in checks if item.get("status") == "pass")
    return {
        "status": "violation" if violation_codes else ("warning" if warning_codes else "healthy"),
        "pass_count": pass_count,
        "warning_count": len(warning_codes),
        "violation_count": len(violation_codes),
        "warning_codes": warning_codes,
        "violation_codes": violation_codes,
        "reason_codes": violation_codes + warning_codes,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build machine-readable pointer consistency report.")
    parser.add_argument("--project-root", default=".", help="Project root directory")
    parser.add_argument("--model-family", default=DEFAULT_MODEL_FAMILY, help="Model family to inspect")
    parser.add_argument("--champion-pointer-family", default="", help="Optional family to use when resolving the champion pointer")
    parser.add_argument("--out", default="", help="Optional output path override")
    parser.add_argument("--ts-ms", type=int, default=0, help="Override timestamp in milliseconds")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    output_path = Path(str(args.out)).resolve() if str(args.out).strip() else None
    path = write_pointer_consistency_report(
        project_root=Path(str(args.project_root)),
        output_path=output_path,
        model_family=str(args.model_family).strip() or DEFAULT_MODEL_FAMILY,
        champion_pointer_family=(str(args.champion_pointer_family).strip() or None),
        ts_ms=(int(args.ts_ms) if int(args.ts_ms) > 0 else None),
    )
    print(f"[ops][pointer-consistency] path={path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
