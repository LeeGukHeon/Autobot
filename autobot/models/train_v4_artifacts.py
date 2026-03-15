"""Artifact and report builders for trainer=v4_crypto_cs."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from autobot import __version__ as autobot_version
from autobot.features.feature_spec import parse_date_to_ts_ms

from .factor_block_selector import (
    normalize_factor_block_selection_mode,
    normalize_run_scope as normalize_factor_block_run_scope,
)


def build_v4_metrics_doc(
    *,
    run_id: str,
    options: Any,
    task: str,
    split_info: Any,
    interval_ms: int,
    rows: dict[str, int],
    valid_metrics: dict[str, Any],
    test_metrics: dict[str, Any],
    walk_forward_summary: dict[str, Any],
    cpcv_lite_summary: dict[str, Any],
    factor_block_selection_summary: dict[str, Any],
    best_params: dict[str, Any],
    sweep_records: list[dict[str, Any]],
    ranker_budget_profile: dict[str, Any],
    cpcv_lite_runtime: dict[str, Any],
    search_budget_decision: dict[str, Any],
    economic_objective_profile: dict[str, Any],
    lane_governance: dict[str, Any],
) -> dict[str, Any]:
    backend = "xgboost_ranker" if task == "rank" else "xgboost_regressor" if task == "reg" else "xgboost"
    objective = "rank:pairwise" if task == "rank" else "reg:squarederror" if task == "reg" else "binary:logistic"
    return {
        "run_id": run_id,
        "created_at_utc": _utc_now(),
        "trainer": "v4_crypto_cs",
        "task": task,
        "model_family": options.model_family,
        "tf": options.tf,
        "quote": options.quote,
        "top_n": options.top_n,
        "time_range": {
            "start": options.start,
            "end": options.end,
            "start_ts_ms": parse_date_to_ts_ms(options.start),
            "end_ts_ms": parse_date_to_ts_ms(options.end, end_of_day=True),
        },
        "split_policy": {
            "train_ratio": options.train_ratio,
            "valid_ratio": options.valid_ratio,
            "test_ratio": options.test_ratio,
            "embargo_bars": options.embargo_bars,
            "interval_ms": interval_ms,
            "valid_start_ts": split_info.valid_start_ts,
            "test_start_ts": split_info.test_start_ts,
            "counts": split_info.counts,
        },
        "rows": rows,
        "fee_policy_bps": {
            "fee_bps_est": options.fee_bps_est,
            "safety_bps": options.safety_bps,
        },
        "booster": {
            "backend": backend,
            "params": best_params,
            "objective": objective,
            "grouping_policy": {"query_key": "ts_ms"} if task == "rank" else {},
            "valid": valid_metrics,
            "test": test_metrics,
        },
        "booster_sweep": {"trials": len(sweep_records), "records": sweep_records},
        "ranker_budget_profile": ranker_budget_profile,
        "champion": {
            "name": "booster",
            "backend": backend,
            "params": best_params,
        },
        "champion_metrics": test_metrics,
        "walk_forward": walk_forward_summary,
        "research_only": {
            "cpcv_lite": {
                "summary": cpcv_lite_summary,
                "runtime": dict(cpcv_lite_runtime or {}),
            },
        },
        "search_budget": dict(search_budget_decision or {}),
        "lane_governance": dict(lane_governance or {}),
        "economic_objective": {
            "profile_id": str((economic_objective_profile or {}).get("profile_id", "")).strip()
            or "v4_shared_economic_objective_v3",
            "trainer_sweep": dict(((economic_objective_profile or {}).get("trainer_sweep") or {}).get("task_profiles", {}).get(task) or {}),
            "walk_forward_selection": dict((economic_objective_profile or {}).get("walk_forward_selection") or {}),
            "offline_compare": dict((economic_objective_profile or {}).get("offline_compare") or {}),
            "execution_compare": dict((economic_objective_profile or {}).get("execution_compare") or {}),
            "promotion_compare": dict((economic_objective_profile or {}).get("promotion_compare") or {}),
        },
        "factor_block_selection": factor_block_selection_summary,
    }


def train_config_snapshot_v4(
    *,
    asdict_fn: Any,
    options: Any,
    task: str,
    feature_cols: tuple[str, ...],
    markets: tuple[str, ...],
    selection_recommendations: dict[str, Any],
    selection_policy: dict[str, Any],
    selection_calibration: dict[str, Any],
    research_support_lane: dict[str, Any],
    ranker_budget_profile: dict[str, Any],
    cpcv_lite_summary: dict[str, Any],
    factor_block_selection: dict[str, Any],
    factor_block_selection_context: dict[str, Any],
    cpcv_lite_runtime: dict[str, Any],
    search_budget_decision: dict[str, Any],
    lane_governance: dict[str, Any],
) -> dict[str, Any]:
    payload = asdict_fn(options)
    payload["dataset_root"] = str(options.dataset_root)
    payload["registry_root"] = str(options.registry_root)
    payload["logs_root"] = str(options.logs_root)
    payload["execution_acceptance_parquet_root"] = str(options.execution_acceptance_parquet_root)
    payload["execution_acceptance_output_root"] = str(options.execution_acceptance_output_root)
    payload["execution_acceptance_top_n"] = max(
        int(options.execution_acceptance_top_n) if int(options.execution_acceptance_top_n) > 0 else int(options.top_n),
        1,
    )
    payload["feature_columns"] = list(feature_cols)
    payload["markets"] = list(markets)
    payload["start_ts_ms"] = parse_date_to_ts_ms(options.start)
    payload["end_ts_ms"] = parse_date_to_ts_ms(options.end, end_of_day=True)
    payload["created_at_utc"] = _utc_now()
    payload["autobot_version"] = autobot_version
    payload["trainer"] = "v4_crypto_cs"
    payload["run_scope"] = normalize_factor_block_run_scope(options.run_scope)
    payload["task"] = task
    payload["y_cls_column"] = "y_cls_topq_12"
    payload["y_reg_column"] = "y_reg_net_12"
    payload["y_rank_column"] = "y_rank_cs_12"
    payload["label_columns"] = ["y_cls_topq_12", "y_reg_net_12", "y_rank_cs_12"]
    payload["ranker_budget_profile"] = ranker_budget_profile
    payload["cpcv_lite"] = {
        "enabled": bool((cpcv_lite_runtime or {}).get("enabled", False)),
        "requested": bool(options.cpcv_lite_enabled),
        "trigger": str((cpcv_lite_runtime or {}).get("trigger", "disabled")).strip() or "disabled",
        "group_count": int(options.cpcv_lite_group_count),
        "test_group_count": int(options.cpcv_lite_test_group_count),
        "max_combinations": int(options.cpcv_lite_max_combinations),
        "summary": dict(cpcv_lite_summary or {}),
    }
    payload["factor_block_selection"] = {
        "mode": normalize_factor_block_selection_mode(options.factor_block_selection_mode),
        "summary": dict((factor_block_selection or {}).get("summary") or {}),
        "refit_support": dict((factor_block_selection or {}).get("refit_support") or {}),
        "resolution_context": dict(factor_block_selection_context or {}),
    }
    payload["search_budget"] = dict(search_budget_decision or {})
    payload["lane_governance"] = dict(lane_governance or {})
    payload["research_support_lane"] = dict(research_support_lane or {})
    payload["selection_recommendations"] = selection_recommendations
    payload["selection_policy"] = dict(selection_policy or {})
    payload["selection_calibration"] = dict(selection_calibration or {})
    return payload


def build_decision_surface_v4(
    *,
    options: Any,
    task: str,
    selection_policy: dict[str, Any],
    selection_calibration: dict[str, Any],
    factor_block_selection: dict[str, Any],
    research_support_lane: dict[str, Any],
    factor_block_selection_context: dict[str, Any],
    cpcv_lite_runtime: dict[str, Any],
    search_budget_decision: dict[str, Any],
    execution_acceptance: dict[str, Any],
    runtime_recommendations: dict[str, Any],
    promotion: dict[str, Any],
    economic_objective_profile: dict[str, Any],
    lane_governance: dict[str, Any],
) -> dict[str, Any]:
    normalized_run_scope = normalize_factor_block_run_scope(options.run_scope)
    search_applied = dict((search_budget_decision or {}).get("applied") or {})
    execution_compare = dict((execution_acceptance or {}).get("compare_to_champion") or {})
    runtime_exit = dict((runtime_recommendations or {}).get("exit") or {})
    runtime_trade_action = dict((runtime_recommendations or {}).get("trade_action") or {})
    runtime_risk_control = dict((runtime_recommendations or {}).get("risk_control") or {})
    factor_block_refit_support = dict((factor_block_selection or {}).get("refit_support") or {})
    factor_block_refit_summary = dict(factor_block_refit_support.get("summary") or {})
    research_support_summary = dict((research_support_lane or {}).get("summary") or {})
    promotion_reasons = [
        str(item).strip()
        for item in ((promotion or {}).get("reasons") or [])
        if str(item).strip()
    ]
    warnings: list[str] = [
        "TRAINER_RESEARCH_PRIOR_IS_TRAIN_PRODUCED",
        "PROMOTION_DECISION_CONTAINS_TRAIN_PRODUCED_RESEARCH_ARTIFACTS",
    ]
    if bool(options.execution_acceptance_enabled):
        warnings.append("EXECUTION_ACCEPTANCE_REUSES_TRAIN_WINDOW")
        warnings.append("RUNTIME_RECOMMENDATIONS_REUSE_TRAIN_WINDOW")
    if str((search_budget_decision or {}).get("status", "")).strip().lower() in {"adjusted", "throttled"}:
        warnings.append("SEARCH_BUDGET_ALTERS_RESEARCH_BREADTH")
    if (
        normalize_factor_block_selection_mode(options.factor_block_selection_mode) == "guarded_auto"
        and not bool((factor_block_selection_context or {}).get("applied", False))
    ):
        warnings.append("GUARDED_FACTOR_POLICY_NOT_ACTIVE")
    if str(factor_block_refit_summary.get("status", "")).strip().lower() in {"insufficient", "partial"}:
        warnings.append("FACTOR_BLOCK_REFIT_SUPPORT_NOT_FULLY_AVAILABLE")
    if str(research_support_summary.get("status", "")).strip().lower() in {"insufficient", "partial", "disabled"}:
        warnings.append("RESEARCH_SUPPORT_LANE_NOT_FULLY_SUPPORTED")
    if bool((lane_governance or {}).get("shadow_only", False)):
        warnings.append("LANE_GOVERNANCE_SHADOW_ONLY")

    return {
        "version": 1,
        "policy": "v4_decision_surface_v1",
        "trainer_entrypoint": {
            "trainer": "v4_crypto_cs",
            "task": str(task).strip().lower() or "cls",
            "model_family": str(options.model_family).strip(),
            "feature_set": str(options.feature_set).strip().lower() or "v4",
            "label_set": str(options.label_set).strip().lower() or "v2",
            "run_scope": normalized_run_scope,
            "dataset_window": {
                "start": str(options.start).strip(),
                "end": str(options.end).strip(),
                "start_ts_ms": parse_date_to_ts_ms(options.start),
                "end_ts_ms": parse_date_to_ts_ms(options.end, end_of_day=True),
            },
            "top_n": int(options.top_n),
            "booster_sweep_trials_requested": int(options.booster_sweep_trials),
            "booster_sweep_trials_applied": int(
                search_applied.get("booster_sweep_trials", options.booster_sweep_trials) or options.booster_sweep_trials
            ),
        },
        "data_split_contract": {
            "train_ratio": float(options.train_ratio),
            "valid_ratio": float(options.valid_ratio),
            "test_ratio": float(options.test_ratio),
            "embargo_bars": int(options.embargo_bars),
            "walk_forward_enabled": bool(options.walk_forward_enabled),
            "walk_forward_windows": int(options.walk_forward_windows),
            "walk_forward_min_train_rows": int(options.walk_forward_min_train_rows),
            "walk_forward_min_test_rows": int(options.walk_forward_min_test_rows),
            "cpcv_lite_requested": bool(options.cpcv_lite_enabled),
            "cpcv_lite_trigger": str((cpcv_lite_runtime or {}).get("trigger", "disabled")).strip() or "disabled",
            "cpcv_lite_enabled": bool((cpcv_lite_runtime or {}).get("enabled", False)),
        },
        "selection_runtime_contract": {
            "selection_policy_mode": str((selection_policy or {}).get("mode", "")).strip() or "unknown",
            "selection_policy_threshold_key": str((selection_policy or {}).get("threshold_key", "")).strip(),
            "selection_policy_source": str(
                (selection_policy or {}).get("selection_recommendation_source", "")
            ).strip(),
            "selection_calibration_method": str((selection_calibration or {}).get("method", "")).strip(),
            "selection_calibration_sample_count": int((selection_calibration or {}).get("sample_count", 0) or 0),
        },
        "research_support_contract": {
            "policy": str((research_support_lane or {}).get("policy", "")).strip()
            or "v4_certification_support_lane_v1",
            "status": str(research_support_summary.get("status", "")).strip() or "unknown",
            "reasons": [
                str(item).strip()
                for item in (research_support_summary.get("reasons") or [])
                if str(item).strip()
            ],
            "multiple_testing_supported": bool(research_support_summary.get("multiple_testing_supported", False)),
            "cpcv_lite_status": str(research_support_summary.get("cpcv_lite_status", "")).strip() or "unknown",
            "support_only": True,
        },
        "lane_governance": dict(lane_governance or {}),
        "economic_objective_contract": {
            "profile_id": str((economic_objective_profile or {}).get("profile_id", "")).strip()
            or "v4_shared_economic_objective_v3",
            "objective_family": str((economic_objective_profile or {}).get("objective_family", "")).strip()
            or "economic_return_first",
            "trainer_sweep": dict(
                (((economic_objective_profile or {}).get("trainer_sweep") or {}).get("task_profiles") or {}).get(task)
                or {}
            ),
            "walk_forward_selection": dict((economic_objective_profile or {}).get("walk_forward_selection") or {}),
            "offline_compare": dict((economic_objective_profile or {}).get("offline_compare") or {}),
            "execution_compare": dict((economic_objective_profile or {}).get("execution_compare") or {}),
            "promotion_compare": dict((economic_objective_profile or {}).get("promotion_compare") or {}),
        },
        "factor_block_contract": {
            "selection_mode": normalize_factor_block_selection_mode(options.factor_block_selection_mode),
            "resolution_source": str((factor_block_selection_context or {}).get("resolution_source", "")).strip()
            or "full_set",
            "applied": bool((factor_block_selection_context or {}).get("applied", False)),
            "resolved_run_id": str((factor_block_selection_context or {}).get("resolved_run_id", "")).strip(),
            "refit_support_status": str(factor_block_refit_summary.get("status", "")).strip() or "unknown",
            "refit_support": factor_block_refit_support,
        },
        "search_budget_contract": {
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
            "markers": [
                str(item).strip()
                for item in ((search_budget_decision or {}).get("markers") or [])
                if str(item).strip()
            ],
            "runtime_recommendation_profile": str(
                search_applied.get("runtime_recommendation_profile", "full")
            ).strip()
            or "full",
            "cpcv_lite_auto_enabled": bool(search_applied.get("cpcv_lite_auto_enabled", False)),
        },
        "execution_acceptance_contract": {
            "enabled": bool(options.execution_acceptance_enabled),
            "window_source": "train_options_start_end",
            "window_start": str(options.start).strip(),
            "window_end": str(options.end).strip(),
            "selection_use_learned_recommendations": False,
            "exit_use_learned_exit_mode": False,
            "exit_use_learned_hold_bars": False,
            "execution_use_learned_recommendations": False,
            "status": str((execution_acceptance or {}).get("status", "")).strip() or "disabled",
            "compare_decision": str(execution_compare.get("decision", "")).strip(),
            "compare_comparable": bool(execution_compare.get("comparable", False)),
        },
        "runtime_recommendation_contract": {
            "enabled": bool(options.execution_acceptance_enabled),
            "window_source": "train_options_start_end",
            "window_start": str(options.start).strip(),
            "window_end": str(options.end).strip(),
            "selection_use_learned_recommendations": True,
            "exit_use_learned_exit_mode": False,
            "exit_use_learned_hold_bars": False,
            "execution_use_learned_recommendations": False,
            "status": str((runtime_recommendations or {}).get("status", "")).strip() or "unknown",
            "recommended_exit_mode": str(runtime_exit.get("recommended_exit_mode", "")).strip(),
            "recommended_hold_bars": int(runtime_exit.get("recommended_hold_bars", 0) or 0),
            "exit_hold_family_status": str(runtime_exit.get("hold_family_status", "")).strip(),
            "exit_risk_family_status": str(runtime_exit.get("risk_family_status", "")).strip(),
            "exit_family_compare_status": str(runtime_exit.get("family_compare_status", "")).strip(),
            "exit_chosen_family": str(runtime_exit.get("chosen_family", "")).strip(),
            "exit_chosen_rule_id": str(runtime_exit.get("chosen_rule_id", "")).strip(),
            "trade_action_policy_status": str(runtime_trade_action.get("status", "")).strip() or "missing",
            "trade_action_policy_source": str(runtime_trade_action.get("source", "")).strip(),
            "trade_action_risk_feature_name": str(runtime_trade_action.get("risk_feature_name", "")).strip(),
            "trade_action_runtime_decision_source": str(runtime_trade_action.get("runtime_decision_source", "")).strip(),
            "trade_action_state_feature_names": [
                str(value).strip()
                for value in (runtime_trade_action.get("state_feature_names") or [])
                if str(value).strip()
            ],
            "trade_action_tail_confidence_level": (
                float(runtime_trade_action.get("tail_confidence_level"))
                if runtime_trade_action.get("tail_confidence_level") is not None
                else None
            ),
            "trade_action_ctm_order": (
                int(runtime_trade_action.get("ctm_order"))
                if runtime_trade_action.get("ctm_order") is not None
                else None
            ),
            "trade_action_tail_risk_method": str(
                ((runtime_trade_action.get("tail_risk_contract") or {}).get("method")) or ""
            ).strip(),
            "trade_action_conditional_model_status": str(
                ((runtime_trade_action.get("conditional_action_model") or {}).get("status")) or ""
            ).strip(),
            "trade_action_conditional_model": str(
                ((runtime_trade_action.get("conditional_action_model") or {}).get("model")) or ""
            ).strip(),
            "risk_control_status": str(runtime_risk_control.get("status", "")).strip() or "missing",
            "risk_control_contract_status": str(runtime_risk_control.get("contract_status", "")).strip(),
            "risk_control_decision_metric_name": str(runtime_risk_control.get("decision_metric_name", "")).strip(),
            "risk_control_selected_threshold": (
                float(runtime_risk_control.get("selected_threshold"))
                if runtime_risk_control.get("selected_threshold") is not None
                else None
            ),
            "risk_control_selected_coverage": (
                int(runtime_risk_control.get("selected_coverage"))
                if runtime_risk_control.get("selected_coverage") is not None
                else None
            ),
            "risk_control_selected_nonpositive_rate_ucb": (
                float(runtime_risk_control.get("selected_nonpositive_rate_ucb"))
                if runtime_risk_control.get("selected_nonpositive_rate_ucb") is not None
                else None
            ),
            "risk_control_selected_severe_loss_rate_ucb": (
                float(runtime_risk_control.get("selected_severe_loss_rate_ucb"))
                if runtime_risk_control.get("selected_severe_loss_rate_ucb") is not None
                else None
            ),
            "risk_control_live_gate_enabled": bool(((runtime_risk_control.get("live_gate") or {}).get("enabled", False))),
            "risk_control_live_gate_metric_name": str(
                ((runtime_risk_control.get("live_gate") or {}).get("metric_name")) or ""
            ).strip(),
            "risk_control_live_gate_skip_reason_code": str(
                ((runtime_risk_control.get("live_gate") or {}).get("skip_reason_code")) or ""
            ).strip(),
            "risk_control_subgroup_feature_name": str(
                ((runtime_risk_control.get("subgroup_family") or {}).get("feature_name")) or ""
            ).strip(),
            "risk_control_subgroup_bucket_count_effective": (
                int((runtime_risk_control.get("subgroup_family") or {}).get("bucket_count_effective"))
                if (runtime_risk_control.get("subgroup_family") or {}).get("bucket_count_effective") is not None
                else None
            ),
            "risk_control_subgroup_min_coverage": (
                int((runtime_risk_control.get("subgroup_family") or {}).get("min_coverage"))
                if (runtime_risk_control.get("subgroup_family") or {}).get("min_coverage") is not None
                else None
            ),
            "risk_control_size_ladder_status": str(
                ((runtime_risk_control.get("size_ladder") or {}).get("status")) or ""
            ).strip(),
            "risk_control_size_ladder_global_max_multiplier": (
                float((runtime_risk_control.get("size_ladder") or {}).get("global_max_multiplier"))
                if (runtime_risk_control.get("size_ladder") or {}).get("global_max_multiplier") is not None
                else None
            ),
            "risk_control_weighting_mode": str(((runtime_risk_control.get("weighting") or {}).get("mode")) or "").strip(),
            "risk_control_weighting_half_life_windows": (
                float((runtime_risk_control.get("weighting") or {}).get("half_life_windows"))
                if (runtime_risk_control.get("weighting") or {}).get("half_life_windows") is not None
                else None
            ),
            "risk_control_weighting_covariate_similarity_mode": str(
                (((runtime_risk_control.get("weighting") or {}).get("covariate_similarity") or {}).get("mode")) or ""
            ).strip(),
            "risk_control_weighting_density_ratio_mode": str(
                (((runtime_risk_control.get("weighting") or {}).get("density_ratio") or {}).get("mode")) or ""
            ).strip(),
            "risk_control_weighting_density_ratio_classifier_status": str(
                (((runtime_risk_control.get("weighting") or {}).get("density_ratio") or {}).get("classifier_status")) or ""
            ).strip(),
            "risk_control_weighting_density_ratio_clip_fraction": (
                float((((runtime_risk_control.get("weighting") or {}).get("density_ratio") or {}).get("clip_fraction")))
                if (((runtime_risk_control.get("weighting") or {}).get("density_ratio") or {}).get("clip_fraction")) is not None
                else None
            ),
            "risk_control_online_adaptation_mode": str(
                ((runtime_risk_control.get("online_adaptation") or {}).get("mode")) or ""
            ).strip(),
            "risk_control_online_lookback_trades": (
                int((runtime_risk_control.get("online_adaptation") or {}).get("lookback_trades"))
                if (runtime_risk_control.get("online_adaptation") or {}).get("lookback_trades") is not None
                else None
            ),
            "risk_control_online_martingale_halt_threshold": (
                float((runtime_risk_control.get("online_adaptation") or {}).get("martingale_halt_threshold"))
                if (runtime_risk_control.get("online_adaptation") or {}).get("martingale_halt_threshold") is not None
                else None
            ),
            "risk_control_online_martingale_escalation_threshold": (
                float((runtime_risk_control.get("online_adaptation") or {}).get("martingale_escalation_threshold"))
                if (runtime_risk_control.get("online_adaptation") or {}).get("martingale_escalation_threshold") is not None
                else None
            ),
            "risk_control_online_martingale_clear_threshold": (
                float((runtime_risk_control.get("online_adaptation") or {}).get("martingale_clear_threshold"))
                if (runtime_risk_control.get("online_adaptation") or {}).get("martingale_clear_threshold") is not None
                else None
            ),
            "risk_control_online_martingale_halt_reason_code": str(
                ((runtime_risk_control.get("online_adaptation") or {}).get("martingale_halt_reason_code")) or ""
            ).strip(),
            "risk_control_online_martingale_critical_reason_code": str(
                ((runtime_risk_control.get("online_adaptation") or {}).get("martingale_critical_reason_code")) or ""
            ).strip(),
        },
        "promotion_contract": {
            "promotion_mode": str((promotion or {}).get("promotion_mode", "")).strip() or "candidate",
            "trainer_evidence_source": "certification_artifact.research_evidence",
            "trainer_research_prior_path": "trainer_research_evidence.json",
            "trainer_research_prior_role": "audit_only_prior",
            "trainer_evidence_expected_consumer": "scripts/candidate_acceptance.ps1",
            "trainer_evidence_includes_execution_acceptance": bool(options.execution_acceptance_enabled),
            "trainer_evidence_includes_risk_control_governance": True,
            "trainer_risk_control_required": bool(((promotion or {}).get("risk_control_acceptance") or {}).get("required", False)),
            "trainer_risk_control_pass": bool(((promotion or {}).get("risk_control_acceptance") or {}).get("pass", False)),
            "promotion_reasons": promotion_reasons,
        },
        "known_methodology_warnings": sorted(set(warnings)),
    }


def write_train_report_v4(logs_root: Path, run_scope: str, payload: dict[str, Any]) -> Path:
    logs_root.mkdir(parents=True, exist_ok=True)
    path = logs_root / _scoped_train_report_filename(run_scope)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _scoped_train_report_filename(run_scope: str) -> str:
    normalized_scope = normalize_factor_block_run_scope(run_scope)
    if normalized_scope == "scheduled_daily":
        return "train_v4_report.json"
    return f"train_v4_report.{normalized_scope}.json"


def _safe_float(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except Exception:
        return 0.0


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
