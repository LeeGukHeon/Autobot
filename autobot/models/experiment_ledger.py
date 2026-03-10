"""Compact family-level experiment ledger for automated v4 training."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Sequence


_EXPERIMENT_LEDGER_PATH = "experiment_ledger.jsonl"
_LATEST_EXPERIMENT_SUMMARY_PATH = "latest_experiment_ledger_summary.json"
_DEFAULT_HISTORY_WINDOW_RUNS = 8
_DEFAULT_RUN_SCOPE = "scheduled_daily"


def build_experiment_ledger_record(
    *,
    run_id: str,
    task: str,
    status: str,
    duration_sec: float,
    run_dir: Path,
    search_budget_decision: dict[str, Any],
    walk_forward: dict[str, Any],
    cpcv_lite: dict[str, Any],
    factor_block_selection: dict[str, Any],
    factor_block_policy: dict[str, Any],
    factor_block_selection_context: dict[str, Any],
    execution_acceptance: dict[str, Any],
    runtime_recommendations: dict[str, Any],
    promotion: dict[str, Any],
    duplicate_candidate: bool,
    economic_objective_profile: dict[str, Any] | None = None,
    run_scope: str | None = None,
) -> dict[str, Any]:
    walk_forward_summary = dict((walk_forward or {}).get("summary") or {})
    compare_doc = dict((walk_forward or {}).get("compare_to_champion") or {})
    spa_like_doc = dict((walk_forward or {}).get("spa_like_window_test") or {})
    white_rc_doc = dict((walk_forward or {}).get("white_reality_check") or {})
    hansen_spa_doc = dict((walk_forward or {}).get("hansen_spa") or {})
    cpcv_summary = dict((cpcv_lite or {}).get("summary") or {})
    factor_selection_summary = dict((factor_block_selection or {}).get("summary") or {})
    factor_selection_support = dict((factor_block_selection or {}).get("sample_support") or {})
    factor_policy_summary = dict((factor_block_policy or {}).get("summary") or {})
    execution_compare = dict((execution_acceptance or {}).get("compare_to_champion") or {})
    search_applied = dict((search_budget_decision or {}).get("applied") or {})
    runtime_status = str((runtime_recommendations or {}).get("status", "")).strip()
    runtime_reason = str((runtime_recommendations or {}).get("reason", "")).strip()

    return {
        "version": 1,
        "policy": "v4_experiment_ledger_v1",
        "recorded_at_utc": _utc_now(),
        "run_id": str(run_id).strip(),
        "run_scope": normalize_run_scope(run_scope),
        "task": str(task).strip().lower() or "cls",
        "status": str(status).strip() or "candidate",
        "duration_sec": round(_safe_float(duration_sec), 3),
        "run_dir_size_bytes": int(_directory_size_bytes(run_dir)),
        "duplicate_candidate": bool(duplicate_candidate),
        "search_budget": {
            "status": str((search_budget_decision or {}).get("status", "")).strip() or "default",
            "lane_class_requested": str((search_budget_decision or {}).get("lane_class_requested", "")).strip()
            or "promotion_eligible",
            "lane_class_effective": str((search_budget_decision or {}).get("lane_class_effective", "")).strip()
            or "promotion_eligible",
            "budget_contract_id": str((search_budget_decision or {}).get("budget_contract_id", "")).strip()
            or "v4_promotion_eligible_budget_v1",
            "promotion_eligible_satisfied": bool(
                ((search_budget_decision or {}).get("promotion_eligible_contract") or {}).get("satisfied", False)
            ),
            "applied_booster_sweep_trials": int(search_applied.get("booster_sweep_trials", 0) or 0),
            "runtime_recommendation_profile": str(search_applied.get("runtime_recommendation_profile", "")).strip() or "full",
            "cpcv_lite_auto_enabled": bool(search_applied.get("cpcv_lite_auto_enabled", False)),
            "markers": [str(item).strip() for item in ((search_budget_decision or {}).get("markers") or []) if str(item).strip()],
        },
        "economic_objective": {
            "profile_id": str((economic_objective_profile or {}).get("profile_id", "")).strip()
            or "v4_shared_economic_objective_v1",
            "objective_family": str((economic_objective_profile or {}).get("objective_family", "")).strip()
            or "economic_return_first",
            "offline_policy": str(((economic_objective_profile or {}).get("offline_compare") or {}).get("policy", "")).strip()
            or "balanced_pareto_offline",
            "execution_policy": str(((economic_objective_profile or {}).get("execution_compare") or {}).get("policy", "")).strip()
            or "balanced_pareto_execution",
        },
        "walk_forward": {
            "windows_run": int(walk_forward_summary.get("windows_run", 0) or 0),
            "balanced_pareto_comparable": bool(compare_doc.get("comparable", False)),
            "balanced_pareto_decision": str(compare_doc.get("decision", "")).strip(),
            "spa_like_comparable": bool(spa_like_doc.get("comparable", False)),
            "spa_like_decision": str(spa_like_doc.get("decision", "")).strip(),
            "white_rc_comparable": bool(white_rc_doc.get("comparable", False)),
            "hansen_spa_comparable": bool(hansen_spa_doc.get("comparable", False)),
        },
        "cpcv_lite": {
            "enabled": bool((cpcv_lite or {}).get("enabled", False)),
            "trigger": str((cpcv_lite or {}).get("trigger", "")).strip() or "disabled",
            "status": str(cpcv_summary.get("status", "")).strip() or "disabled",
            "folds_run": int(cpcv_summary.get("folds_run", 0) or 0),
            "comparable_fold_count": int(cpcv_summary.get("comparable_fold_count", 0) or 0),
        },
        "factor_block_selection": {
            "mode": str((factor_block_selection or {}).get("selection_mode", "")).strip() or "off",
            "summary_status": str(factor_selection_summary.get("status", "")).strip() or "unknown",
            "weak_sample": bool(factor_selection_support.get("weak_sample", False)),
            "accepted_block_count": int(factor_selection_summary.get("accepted_block_count", 0) or 0),
            "rejected_block_count": int(factor_selection_summary.get("rejected_block_count", 0) or 0),
            "feature_set_applied": bool((factor_block_selection_context or {}).get("applied", False)),
            "resolution_source": str((factor_block_selection_context or {}).get("resolution_source", "")).strip() or "full_set",
        },
        "factor_block_policy": {
            "status": str(factor_policy_summary.get("status", "")).strip() or "warming",
            "apply_pruned_feature_set": bool((factor_block_policy or {}).get("apply_pruned_feature_set", False)),
        },
        "execution_acceptance": {
            "status": str((execution_acceptance or {}).get("status", "")).strip() or "disabled",
            "comparable": bool(execution_compare.get("comparable", False)),
            "decision": str(execution_compare.get("decision", "")).strip(),
        },
        "runtime_recommendations": {
            "status": runtime_status or "unknown",
            "reason": runtime_reason,
        },
        "promotion": {
            "status": str((promotion or {}).get("status", "")).strip() or "candidate",
            "promotion_mode": str((promotion or {}).get("promotion_mode", "")).strip(),
            "reasons": [str(item).strip() for item in ((promotion or {}).get("reasons") or []) if str(item).strip()],
        },
    }


def load_experiment_ledger(
    *,
    registry_root: Path,
    model_family: str,
    run_scope: str | None = None,
) -> list[dict[str, Any]]:
    path = registry_root / model_family / _scoped_filename(_EXPERIMENT_LEDGER_PATH, normalize_run_scope(run_scope))
    if not path.exists():
        return []
    deduped: dict[str, dict[str, Any]] = {}
    ordered: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        text = str(line).strip()
        if not text:
            continue
        try:
            record = json.loads(text)
        except json.JSONDecodeError:
            continue
        if not isinstance(record, dict):
            continue
        run_id = str(record.get("run_id", "")).strip()
        if not run_id:
            continue
        if run_id not in deduped:
            ordered.append(run_id)
        deduped[run_id] = record
    return [deduped[run_id] for run_id in ordered]


def append_experiment_ledger_record(
    *,
    registry_root: Path,
    model_family: str,
    record: dict[str, Any],
    run_scope: str | None = None,
) -> Path | None:
    run_id = str((record or {}).get("run_id", "")).strip()
    if not run_id:
        return None
    normalized_scope = normalize_run_scope(run_scope or record.get("run_scope"))
    path = registry_root / model_family / _scoped_filename(_EXPERIMENT_LEDGER_PATH, normalized_scope)
    records = load_experiment_ledger(
        registry_root=registry_root,
        model_family=model_family,
        run_scope=normalized_scope,
    )
    records = [item for item in records if str(item.get("run_id", "")).strip() != run_id]
    records.append(dict(record))
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = "\n".join(json.dumps(item, ensure_ascii=False, sort_keys=True) for item in records)
    path.write_text((payload + "\n") if payload else "", encoding="utf-8")
    return path


def build_recent_experiment_ledger_summary(
    *,
    history_records: Sequence[dict[str, Any]],
    history_window_runs: int = _DEFAULT_HISTORY_WINDOW_RUNS,
) -> dict[str, Any]:
    window_runs = max(int(history_window_runs), 1)
    records = [dict(item) for item in history_records if isinstance(item, dict)]
    recent_records = records[-window_runs:]
    duplicate_values = [1.0 if bool(item.get("duplicate_candidate", False)) else 0.0 for item in recent_records]
    duration_values = [_safe_float(item.get("duration_sec")) for item in recent_records]
    run_dir_size_values = [
        float(int(item.get("run_dir_size_bytes", 0) or 0)) / float(1024**2)
        for item in recent_records
    ]
    throttled_values = [
        1.0 if str(((item.get("search_budget") or {}).get("status", "")).strip()) == "throttled" else 0.0
        for item in recent_records
    ]
    compact_runtime_values = [
        1.0
        if str(((item.get("search_budget") or {}).get("runtime_recommendation_profile", "")).strip()) in {"compact", "tiny"}
        else 0.0
        for item in recent_records
    ]
    balanced_comparable_values = [
        1.0 if bool((item.get("walk_forward") or {}).get("balanced_pareto_comparable", False)) else 0.0
        for item in recent_records
    ]
    white_comparable_values = [
        1.0 if bool((item.get("walk_forward") or {}).get("white_rc_comparable", False)) else 0.0
        for item in recent_records
    ]
    hansen_comparable_values = [
        1.0 if bool((item.get("walk_forward") or {}).get("hansen_spa_comparable", False)) else 0.0
        for item in recent_records
    ]
    pruning_applied_values = [
        1.0 if bool((item.get("factor_block_selection") or {}).get("feature_set_applied", False)) else 0.0
        for item in recent_records
    ]
    cpcv_enabled_values = [
        1.0 if bool((item.get("cpcv_lite") or {}).get("enabled", False)) else 0.0
        for item in recent_records
    ]
    cpcv_partial_or_better_values = [
        1.0
        if str(((item.get("cpcv_lite") or {}).get("status", "")).strip()) in {"partial", "trusted"}
        else 0.0
        for item in recent_records
    ]

    duplicate_streak = 0
    for item in reversed(recent_records):
        if not bool(item.get("duplicate_candidate", False)):
            break
        duplicate_streak += 1

    return {
        "version": 1,
        "policy": "v4_experiment_ledger_summary_v1",
        "history_window_runs": int(window_runs),
        "records_considered": int(len(recent_records)),
        "recent_run_ids": [str(item.get("run_id", "")).strip() for item in recent_records if str(item.get("run_id", "")).strip()],
        "duplicate_candidate_rate": _mean(duplicate_values),
        "duplicate_candidate_streak": int(duplicate_streak),
        "mean_duration_sec": _mean(duration_values),
        "max_duration_sec": max(duration_values) if duration_values else 0.0,
        "mean_run_dir_size_mb": _mean(run_dir_size_values),
        "throttled_budget_rate": _mean(throttled_values),
        "compact_runtime_profile_rate": _mean(compact_runtime_values),
        "balanced_pareto_comparable_rate": _mean(balanced_comparable_values),
        "white_rc_comparable_rate": _mean(white_comparable_values),
        "hansen_spa_comparable_rate": _mean(hansen_comparable_values),
        "guarded_pruning_applied_rate": _mean(pruning_applied_values),
        "cpcv_enabled_rate": _mean(cpcv_enabled_values),
        "cpcv_partial_or_better_rate": _mean(cpcv_partial_or_better_values),
    }


def load_latest_experiment_ledger_summary(
    *,
    registry_root: Path,
    model_family: str,
    run_scope: str | None = None,
) -> dict[str, Any]:
    path = registry_root / model_family / _scoped_filename(
        _LATEST_EXPERIMENT_SUMMARY_PATH,
        normalize_run_scope(run_scope),
    )
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def write_latest_experiment_ledger_summary(
    *,
    registry_root: Path,
    model_family: str,
    run_id: str,
    summary: dict[str, Any],
    run_scope: str | None = None,
) -> Path | None:
    if not isinstance(summary, dict):
        return None
    normalized_scope = normalize_run_scope(run_scope)
    path = registry_root / model_family / _scoped_filename(_LATEST_EXPERIMENT_SUMMARY_PATH, normalized_scope)
    payload = dict(summary)
    payload["updated_by_run_id"] = str(run_id).strip()
    payload["run_scope"] = normalized_scope
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


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


def _mean(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    return round(float(sum(values)) / float(len(values)), 6)


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def normalize_run_scope(value: str | None) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return _DEFAULT_RUN_SCOPE
    normalized = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in raw)
    normalized = normalized.strip("_-")
    return normalized or _DEFAULT_RUN_SCOPE


def _scoped_filename(filename: str, run_scope: str) -> str:
    normalized_scope = normalize_run_scope(run_scope)
    if normalized_scope == _DEFAULT_RUN_SCOPE:
        return filename
    path = Path(filename)
    return f"{path.stem}.{normalized_scope}{path.suffix}"


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
