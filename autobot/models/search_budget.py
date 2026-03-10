"""Daily search-budget manager for bounded v4 training on shared servers."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import shutil
from pathlib import Path
from typing import Any

from .experiment_ledger import load_latest_experiment_ledger_summary


@dataclass(frozen=True)
class V4SearchBudgetPolicy:
    soft_disk_used_gb: float = 80.0
    hard_disk_used_gb: float = 100.0
    soft_wall_time_sec: int = 7_200
    hard_wall_time_sec: int = 10_800
    soft_booster_trial_cap: int = 8
    hard_booster_trial_cap: int = 5
    soft_runtime_profile: str = "compact"
    hard_runtime_profile: str = "tiny"


@dataclass(frozen=True)
class V4PromotionEligibleBudgetContract:
    contract_id: str = "v4_promotion_eligible_budget_v1"
    min_booster_sweep_trials: int = 10
    required_runtime_recommendation_profile: str = "full"
    require_cpcv_lite_auto_disabled: bool = True


def resolve_v4_search_budget(
    *,
    project_root: Path,
    logs_root: Path,
    registry_root: Path,
    model_family: str,
    run_scope: str,
    requested_booster_sweep_trials: int,
    factor_block_selection_context: dict[str, Any] | None,
    cpcv_requested: bool,
    policy: V4SearchBudgetPolicy | None = None,
    promotion_contract: V4PromotionEligibleBudgetContract | None = None,
) -> dict[str, Any]:
    active_policy = policy or V4SearchBudgetPolicy()
    active_contract = promotion_contract or V4PromotionEligibleBudgetContract()
    requested_trials = max(int(requested_booster_sweep_trials), 1)
    applied_trials = requested_trials
    runtime_profile = "full"
    markers: list[str] = []
    reasons: list[str] = []

    disk = shutil.disk_usage(project_root)
    filesystem_used_gb = float(disk.used) / float(1024**3) if int(disk.total) > 0 else 0.0
    filesystem_total_gb = float(disk.total) / float(1024**3) if int(disk.total) > 0 else 0.0
    project_used_bytes = _directory_size_bytes(project_root)
    project_used_gb = float(project_used_bytes) / float(1024**3)

    latest_train_report = _load_json(Path(logs_root) / _scoped_filename("train_v4_report.json", run_scope))
    previous_duration_sec = _safe_float(latest_train_report.get("duration_sec"))
    experiment_ledger_summary = load_latest_experiment_ledger_summary(
        registry_root=registry_root,
        model_family=model_family,
        run_scope=run_scope,
    )
    experiment_records_considered = int(experiment_ledger_summary.get("records_considered", 0) or 0)
    experiment_mean_duration_sec = _safe_float(experiment_ledger_summary.get("mean_duration_sec"))
    experiment_duplicate_rate = _safe_float(experiment_ledger_summary.get("duplicate_candidate_rate"))
    experiment_duplicate_streak = int(experiment_ledger_summary.get("duplicate_candidate_streak", 0) or 0)

    if project_used_gb >= float(active_policy.hard_disk_used_gb):
        applied_trials = min(applied_trials, max(int(active_policy.hard_booster_trial_cap), 1))
        runtime_profile = _max_runtime_profile(runtime_profile, str(active_policy.hard_runtime_profile))
        markers.append("HARD_DISK_BUDGET_PRESSURE")
        reasons.append("PROJECT_USED_GB_AT_OR_ABOVE_HARD_THRESHOLD")
    elif project_used_gb >= float(active_policy.soft_disk_used_gb):
        applied_trials = min(applied_trials, max(int(active_policy.soft_booster_trial_cap), 1))
        runtime_profile = _max_runtime_profile(runtime_profile, str(active_policy.soft_runtime_profile))
        markers.append("SOFT_DISK_BUDGET_PRESSURE")
        reasons.append("PROJECT_USED_GB_AT_OR_ABOVE_SOFT_THRESHOLD")

    if previous_duration_sec >= float(active_policy.hard_wall_time_sec):
        applied_trials = min(applied_trials, max(int(active_policy.hard_booster_trial_cap), 1))
        runtime_profile = _max_runtime_profile(runtime_profile, str(active_policy.hard_runtime_profile))
        markers.append("HARD_WALL_TIME_PRESSURE")
        reasons.append("PREVIOUS_TRAIN_DURATION_AT_OR_ABOVE_HARD_THRESHOLD")
    elif previous_duration_sec >= float(active_policy.soft_wall_time_sec):
        applied_trials = min(applied_trials, max(int(active_policy.soft_booster_trial_cap), 1))
        runtime_profile = _max_runtime_profile(runtime_profile, str(active_policy.soft_runtime_profile))
        markers.append("SOFT_WALL_TIME_PRESSURE")
        reasons.append("PREVIOUS_TRAIN_DURATION_AT_OR_ABOVE_SOFT_THRESHOLD")

    if experiment_records_considered >= 2:
        if experiment_mean_duration_sec >= float(active_policy.hard_wall_time_sec):
            applied_trials = min(applied_trials, max(int(active_policy.hard_booster_trial_cap), 1))
            runtime_profile = _max_runtime_profile(runtime_profile, str(active_policy.hard_runtime_profile))
            markers.append("LEDGER_HARD_WALL_TIME_PRESSURE")
            reasons.append("RECENT_MEAN_DURATION_AT_OR_ABOVE_HARD_THRESHOLD")
        elif experiment_mean_duration_sec >= float(active_policy.soft_wall_time_sec):
            applied_trials = min(applied_trials, max(int(active_policy.soft_booster_trial_cap), 1))
            runtime_profile = _max_runtime_profile(runtime_profile, str(active_policy.soft_runtime_profile))
            markers.append("LEDGER_SOFT_WALL_TIME_PRESSURE")
            reasons.append("RECENT_MEAN_DURATION_AT_OR_ABOVE_SOFT_THRESHOLD")

    if experiment_duplicate_streak >= 2:
        applied_trials = min(applied_trials, max(int(active_policy.soft_booster_trial_cap), 1))
        runtime_profile = _max_runtime_profile(runtime_profile, "compact")
        markers.append("LEDGER_DUPLICATE_STREAK_PRESSURE")
        reasons.append("RECENT_DUPLICATE_STREAK_AT_OR_ABOVE_2")
    elif experiment_records_considered >= 4 and experiment_duplicate_rate >= 0.5:
        applied_trials = min(applied_trials, max(int(active_policy.soft_booster_trial_cap), 1))
        runtime_profile = _max_runtime_profile(runtime_profile, "compact")
        markers.append("LEDGER_DUPLICATE_RATE_PRESSURE")
        reasons.append("RECENT_DUPLICATE_RATE_AT_OR_ABOVE_0_50")

    guarded_pruning_applied = (
        isinstance(factor_block_selection_context, dict)
        and bool(factor_block_selection_context.get("applied", False))
        and str(factor_block_selection_context.get("resolution_source", "")).strip() == "guarded_policy"
    )
    if guarded_pruning_applied:
        runtime_profile = _max_runtime_profile(runtime_profile, "compact")
        markers.append("GUARDED_POLICY_ACTIVE")
        reasons.append("PRUNED_FEATURE_SET_ACTIVE")

    cpcv_auto_enabled = bool(guarded_pruning_applied)
    if cpcv_auto_enabled or bool(cpcv_requested):
        runtime_profile = _max_runtime_profile(runtime_profile, "compact")
        markers.append("CPCV_LOAD_SHED_ACTIVE")
        reasons.append("CPCV_LITE_EXPECTED_OR_REQUESTED")

    status = "default"
    if runtime_profile == "tiny" or int(applied_trials) < int(requested_trials):
        status = "throttled"
    elif runtime_profile != "full":
        status = "adjusted"

    lane_class_requested = _resolve_budget_lane_class(run_scope)
    promotion_eligible_requested = lane_class_requested == "promotion_eligible"
    promotion_eligible_satisfied = (
        promotion_eligible_requested
        and int(applied_trials) >= int(active_contract.min_booster_sweep_trials)
        and str(runtime_profile).strip().lower() == str(active_contract.required_runtime_recommendation_profile).strip().lower()
        and (
            (not bool(active_contract.require_cpcv_lite_auto_disabled))
            or (not bool(cpcv_auto_enabled))
        )
    )
    lane_class_effective = "promotion_eligible" if promotion_eligible_satisfied else "scout"
    budget_contract_id = (
        str(active_contract.contract_id).strip() if promotion_eligible_requested else "v4_scout_budget_v1"
    )

    return {
        "version": 1,
        "policy": "v4_daily_search_budget_v1",
        "status": status,
        "lane_class_requested": lane_class_requested,
        "lane_class_effective": lane_class_effective,
        "budget_contract_id": budget_contract_id,
        "policy_inputs": asdict(active_policy),
        "resource_state": {
            "project_root": str(project_root),
            "project_used_bytes": int(project_used_bytes),
            "project_used_gb": round(project_used_gb, 3),
            "filesystem_total_gb": round(filesystem_total_gb, 3),
            "filesystem_used_gb": round(filesystem_used_gb, 3),
            "previous_train_duration_sec": round(previous_duration_sec, 3),
        },
        "requested": {
            "booster_sweep_trials": int(requested_trials),
            "cpcv_lite_requested": bool(cpcv_requested),
        },
        "applied": {
            "booster_sweep_trials": int(applied_trials),
            "runtime_recommendation_profile": runtime_profile,
            "cpcv_lite_auto_enabled": bool(cpcv_auto_enabled),
        },
        "promotion_eligible_contract": {
            "requested": bool(promotion_eligible_requested),
            "satisfied": bool(promotion_eligible_satisfied),
            "contract_id": str(active_contract.contract_id).strip(),
            "min_booster_sweep_trials": int(active_contract.min_booster_sweep_trials),
            "required_runtime_recommendation_profile": str(active_contract.required_runtime_recommendation_profile).strip()
            or "full",
            "require_cpcv_lite_auto_disabled": bool(active_contract.require_cpcv_lite_auto_disabled),
        },
        "factor_block_context": {
            "applied": bool((factor_block_selection_context or {}).get("applied", False)),
            "resolution_source": str((factor_block_selection_context or {}).get("resolution_source", "")).strip(),
            "resolved_run_id": str((factor_block_selection_context or {}).get("resolved_run_id", "")).strip(),
        },
        "experiment_ledger_context": {
            "available": bool(experiment_ledger_summary),
            "summary_path": str(registry_root / model_family / _scoped_filename("latest_experiment_ledger_summary.json", run_scope)),
            "records_considered": int(experiment_records_considered),
            "duplicate_candidate_rate": round(experiment_duplicate_rate, 6),
            "duplicate_candidate_streak": int(experiment_duplicate_streak),
            "mean_duration_sec": round(experiment_mean_duration_sec, 3),
            "balanced_pareto_comparable_rate": round(
                _safe_float(experiment_ledger_summary.get("balanced_pareto_comparable_rate")),
                6,
            ),
            "white_rc_comparable_rate": round(
                _safe_float(experiment_ledger_summary.get("white_rc_comparable_rate")),
                6,
            ),
            "hansen_spa_comparable_rate": round(
                _safe_float(experiment_ledger_summary.get("hansen_spa_comparable_rate")),
                6,
            ),
        },
        "markers": list(dict.fromkeys(markers)),
        "reasons": list(dict.fromkeys(reasons)),
    }


def write_search_budget_decision(
    *,
    run_dir: Path,
    decision: dict[str, Any],
) -> Path:
    path = run_dir / "search_budget_decision.json"
    path.write_text(json.dumps(decision, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _directory_size_bytes(root: Path) -> int:
    if not root.exists():
        return 0
    total = 0
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        try:
            total += int(path.stat().st_size)
        except FileNotFoundError:
            continue
    return total


def _runtime_profile_rank(value: str) -> int:
    raw = str(value or "").strip().lower()
    if raw == "tiny":
        return 2
    if raw == "compact":
        return 1
    return 0


def _max_runtime_profile(left: str, right: str) -> str:
    profiles = {0: "full", 1: "compact", 2: "tiny"}
    return profiles[max(_runtime_profile_rank(left), _runtime_profile_rank(right))]


def _normalize_run_scope(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return "scheduled_daily"
    normalized = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in raw)
    normalized = normalized.strip("_-")
    return normalized or "scheduled_daily"


def _resolve_budget_lane_class(run_scope: Any) -> str:
    normalized = _normalize_run_scope(run_scope)
    if normalized == "manual_daily" or "scout" in normalized:
        return "scout"
    return "promotion_eligible"


def _scoped_filename(filename: str, run_scope: Any) -> str:
    normalized_scope = _normalize_run_scope(run_scope)
    if normalized_scope == "scheduled_daily":
        return filename
    path = Path(filename)
    return f"{path.stem}.{normalized_scope}{path.suffix}"
