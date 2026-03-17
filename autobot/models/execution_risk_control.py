from __future__ import annotations

import math
from typing import Any

import numpy as np

from .trade_action_policy import (
    build_trade_action_oos_trade_rows,
    normalize_trade_action_policy,
    resolve_trade_action,
)


EXECUTION_RISK_CONTROL_VERSION = 1
EXECUTION_RISK_CONTROL_POLICY_ID = "execution_risk_control_hoeffding_v1"
DEFAULT_DECISION_METRIC = "expected_action_value"
DEFAULT_NONPOSITIVE_ALPHA = 0.45
DEFAULT_SEVERE_LOSS_ALPHA = 0.20
DEFAULT_SEVERE_LOSS_RETURN_THRESHOLD = 0.01
DEFAULT_CONFIDENCE_DELTA = 0.10
DEFAULT_MIN_COVERAGE = 20
DEFAULT_SUBGROUP_BUCKET_COUNT = 3
DEFAULT_SUBGROUP_MIN_COVERAGE = 8
DEFAULT_SIZE_LADDER_STEPS = 5
DEFAULT_SKIP_REASON_CODE = "RISK_CONTROL_BELOW_THRESHOLD"
DEFAULT_SIZE_SKIP_REASON_CODE = "SIZE_LADDER_NO_ADMISSIBLE_MULTIPLIER"
DEFAULT_WEIGHTING_HALF_LIFE_WINDOWS = 2.0
DEFAULT_WEIGHTING_COVARIATE_BANDWIDTH = 1.0
DEFAULT_WEIGHTING_DENSITY_RATIO_CLIP_MIN = 0.25
DEFAULT_WEIGHTING_DENSITY_RATIO_CLIP_MAX = 4.0
DEFAULT_WEIGHTING_DENSITY_RATIO_TRAIN_ITERS = 200
DEFAULT_WEIGHTING_DENSITY_RATIO_LR = 0.1
DEFAULT_WEIGHTING_DENSITY_RATIO_L2 = 1e-3
DEFAULT_WEIGHTING_DENSITY_RATIO_CROSSFIT_FOLDS = 3
DEFAULT_ONLINE_LOOKBACK_TRADES = 12
DEFAULT_ONLINE_MAX_STEP_UP = 2
DEFAULT_ONLINE_RECOVERY_STREAK_REQUIRED = 2
DEFAULT_ONLINE_HALT_BREACH_STREAK = 3
DEFAULT_ONLINE_HALT_REASON_CODE = "RISK_CONTROL_ONLINE_BREACH_STREAK"
DEFAULT_ONLINE_MARTINGALE_HALT_REASON_CODE = "RISK_CONTROL_MARTINGALE_EVIDENCE"
DEFAULT_ONLINE_MARTINGALE_CRITICAL_REASON_CODE = "RISK_CONTROL_MARTINGALE_CRITICAL_EVIDENCE"
DEFAULT_ONLINE_MARTINGALE_HALT_THRESHOLD = 20.0
DEFAULT_ONLINE_MARTINGALE_CLEAR_THRESHOLD = 2.0
DEFAULT_ONLINE_MARTINGALE_ESCALATION_THRESHOLD = 100.0
DEFAULT_ONLINE_MARTINGALE_BET_FRACTION = 1.0


def build_execution_risk_control_from_oos_rows(
    *,
    oos_rows: list[dict[str, Any]] | None,
    selection_calibration: dict[str, Any] | None,
    trade_action_policy: dict[str, Any] | None,
    hold_policy_template: dict[str, Any] | None,
    risk_policy_template: dict[str, Any] | None,
    nonpositive_alpha: float = DEFAULT_NONPOSITIVE_ALPHA,
    severe_loss_alpha: float = DEFAULT_SEVERE_LOSS_ALPHA,
    severe_loss_return_threshold: float = DEFAULT_SEVERE_LOSS_RETURN_THRESHOLD,
    confidence_delta: float = DEFAULT_CONFIDENCE_DELTA,
    min_coverage: int = DEFAULT_MIN_COVERAGE,
    subgroup_bucket_count: int = DEFAULT_SUBGROUP_BUCKET_COUNT,
    subgroup_min_coverage: int = DEFAULT_SUBGROUP_MIN_COVERAGE,
    size_ladder_steps: int = DEFAULT_SIZE_LADDER_STEPS,
    weighting_half_life_windows: float = DEFAULT_WEIGHTING_HALF_LIFE_WINDOWS,
    weighting_covariate_similarity_enabled: bool = False,
    weighting_covariate_bandwidth: float = DEFAULT_WEIGHTING_COVARIATE_BANDWIDTH,
    weighting_density_ratio_enabled: bool = False,
    weighting_density_ratio_clip_min: float = DEFAULT_WEIGHTING_DENSITY_RATIO_CLIP_MIN,
    weighting_density_ratio_clip_max: float = DEFAULT_WEIGHTING_DENSITY_RATIO_CLIP_MAX,
    decision_metric_name: str = DEFAULT_DECISION_METRIC,
) -> dict[str, Any]:
    policy = normalize_trade_action_policy(trade_action_policy)
    if policy.get("status") != "ready":
        return {
            "version": EXECUTION_RISK_CONTROL_VERSION,
            "policy": EXECUTION_RISK_CONTROL_POLICY_ID,
            "status": "skipped",
            "reason": "TRADE_ACTION_POLICY_NOT_READY",
        }

    hold_template = dict(hold_policy_template or {})
    risk_template = dict(risk_policy_template or {})
    trade_rows, window_count = build_trade_action_oos_trade_rows(
        oos_rows=list(oos_rows or []),
        selection_calibration=selection_calibration,
        hold_policy_template=hold_template,
        risk_policy_template=risk_template,
    )
    if not trade_rows:
        return {
            "version": EXECUTION_RISK_CONTROL_VERSION,
            "policy": EXECUTION_RISK_CONTROL_POLICY_ID,
            "status": "skipped",
            "reason": "NO_OOS_TRADE_ROWS",
            "rows_total": 0,
            "windows_covered": 0,
        }

    decision_rows: list[dict[str, Any]] = []
    for row in trade_rows:
        selection_score = _safe_optional_float(row.get("selection_score"))
        if selection_score is None:
            continue
        decision = resolve_trade_action(
            policy,
            selection_score=float(selection_score),
            row=row,
        )
        if not isinstance(decision, dict):
            continue
        if str(decision.get("status", "")).strip().lower() == "insufficient_evidence":
            continue
        action = str(decision.get("recommended_action", "")).strip().lower()
        if action == "risk":
            realized_return = _safe_optional_float(row.get("risk_return"))
        elif action == "hold":
            realized_return = _safe_optional_float(row.get("hold_return"))
        else:
            continue
        metric_value = _resolve_decision_metric_value(
            decision=decision,
            selection_score=float(selection_score),
            metric_name=decision_metric_name,
        )
        if realized_return is None or metric_value is None or not math.isfinite(float(metric_value)):
            continue
        decision_rows.append(
            {
                "_row_id": int(len(decision_rows)),
                "selection_score": float(selection_score),
                "metric_value": float(metric_value),
                "recommended_action": action,
                "realized_return": float(realized_return),
                "window_index": int(row.get("window_index", -1) or -1),
                "rv_12": _safe_optional_float(row.get("rv_12")),
                "rv_36": _safe_optional_float(row.get("rv_36")),
                "atr_pct_14": _safe_optional_float(row.get("atr_pct_14")),
                "risk_feature_name": str(decision.get("risk_feature_name", "")).strip(),
                "risk_feature_value": _safe_optional_float(decision.get("risk_feature_value")),
                "expected_action_value": _safe_optional_float(
                    decision.get("expected_action_value", decision.get("expected_objective_score"))
                ),
                "expected_edge": _safe_optional_float(decision.get("expected_edge")),
                "expected_es": _safe_optional_float(decision.get("expected_es")),
                "expected_tail_probability": _safe_optional_float(decision.get("expected_tail_probability")),
            }
        )

    if len(decision_rows) < max(int(min_coverage), 1):
        return {
            "version": EXECUTION_RISK_CONTROL_VERSION,
            "policy": EXECUTION_RISK_CONTROL_POLICY_ID,
            "status": "skipped",
            "reason": "INSUFFICIENT_DECISION_ROWS",
            "rows_total": int(len(decision_rows)),
            "windows_covered": int(window_count),
        }

    delta_value = _clamp_probability(confidence_delta, default=DEFAULT_CONFIDENCE_DELTA)
    per_metric_delta = max(float(delta_value) / 2.0, 1e-12)
    nonpositive_alpha_value = _clamp_probability(nonpositive_alpha, default=DEFAULT_NONPOSITIVE_ALPHA)
    severe_alpha_value = _clamp_probability(severe_loss_alpha, default=DEFAULT_SEVERE_LOSS_ALPHA)
    severe_threshold = max(float(severe_loss_return_threshold), 0.0)
    min_coverage_value = max(int(min_coverage), 1)
    subgroup_bucket_count_value = max(int(subgroup_bucket_count), 1)
    subgroup_min_coverage_value = max(int(subgroup_min_coverage), 1)
    weighting_half_life_value = max(float(weighting_half_life_windows), 1e-6)
    weighting_covariate_bandwidth_value = max(float(weighting_covariate_bandwidth), 1e-6)
    weighting_density_ratio_clip_min_value = max(float(weighting_density_ratio_clip_min), 1e-6)
    weighting_density_ratio_clip_max_value = max(
        float(weighting_density_ratio_clip_max),
        weighting_density_ratio_clip_min_value,
    )
    max_window_index = max((int(item.get("window_index", -1) or -1) for item in decision_rows), default=-1)
    covariate_similarity = _build_covariate_similarity_contract(
        decision_rows=decision_rows,
        enabled=bool(weighting_covariate_similarity_enabled),
        bandwidth=weighting_covariate_bandwidth_value,
    )
    density_ratio = _build_density_ratio_contract(
        decision_rows=decision_rows,
        enabled=bool(weighting_density_ratio_enabled),
        clip_min=weighting_density_ratio_clip_min_value,
        clip_max=weighting_density_ratio_clip_max_value,
    )
    for item in decision_rows:
        recency_weight = _resolve_recency_weight(
            window_index=int(item.get("window_index", -1) or -1),
            max_window_index=max_window_index,
            half_life_windows=weighting_half_life_value,
        )
        similarity_weight = _resolve_covariate_similarity_weight(
            row=item,
            contract=covariate_similarity,
        )
        density_ratio_weight = _resolve_density_ratio_weight(
            row=item,
            contract=density_ratio,
        )
        item["weight"] = float(recency_weight) * float(similarity_weight) * float(density_ratio_weight)
    subgroup_feature_name = _resolve_subgroup_feature_name(decision_rows)
    subgroup_values = [
        float(item["risk_feature_value"])
        for item in decision_rows
        if item.get("risk_feature_value") is not None and math.isfinite(float(item["risk_feature_value"]))
    ]
    subgroup_bounds = _quantile_bounds(subgroup_values, subgroup_bucket_count_value)
    subgroup_bucket_count_effective = max(len(subgroup_bounds) - 1, 0)
    for item in decision_rows:
        subgroup_value = _safe_optional_float(item.get("risk_feature_value"))
        if subgroup_value is None or subgroup_bucket_count_effective <= 0:
            item["subgroup_bucket"] = None
            item["subgroup_label"] = ""
            continue
        subgroup_bucket_index = _resolve_bucket_index(float(subgroup_value), subgroup_bounds)
        item["subgroup_bucket"] = int(subgroup_bucket_index)
        item["subgroup_label"] = _subgroup_label(
            feature_name=subgroup_feature_name,
            bucket_index=int(subgroup_bucket_index),
        )

    thresholds = sorted({float(item["metric_value"]) for item in decision_rows}, reverse=True)
    threshold_rows: list[dict[str, Any]] = []
    feasible_rows: list[dict[str, Any]] = []
    total_rows = len(decision_rows)
    for threshold in thresholds:
        selected = [item for item in decision_rows if float(item["metric_value"]) >= float(threshold)]
        coverage = len(selected)
        effective_sample_size = _effective_sample_size(selected)
        total_weight = sum(max(float(item.get("weight", 0.0) or 0.0), 0.0) for item in selected)
        if coverage <= 0 or effective_sample_size <= 0.0 or total_weight <= 0.0:
            continue
        nonpositive_rate = _weighted_indicator_rate(selected, lambda item: float(item["realized_return"]) <= 0.0)
        severe_rate = _weighted_indicator_rate(
            selected,
            lambda item: float(item["realized_return"]) <= -float(severe_threshold),
        )
        nonpositive_ucb = _hoeffding_ucb(nonpositive_rate, effective_sample_size, per_metric_delta)
        severe_ucb = _hoeffding_ucb(severe_rate, effective_sample_size, per_metric_delta)
        mean_return = _weighted_mean(selected, key="realized_return")
        subgroup_results = _build_subgroup_results(
            selected_rows=selected,
            nonpositive_alpha=nonpositive_alpha_value,
            severe_loss_alpha=severe_alpha_value,
            severe_threshold=severe_threshold,
            delta=per_metric_delta,
            subgroup_min_coverage=subgroup_min_coverage_value,
        )
        subgroup_blocking_count = int(sum(1 for item in subgroup_results if item.get("status") == "violated"))
        row_doc = {
            "threshold": float(threshold),
            "coverage": int(coverage),
            "coverage_ratio": float(coverage) / float(max(total_rows, 1)),
            "effective_sample_size": float(effective_sample_size),
            "weight_total": float(total_weight),
            "mean_return": float(mean_return),
            "nonpositive_rate": float(nonpositive_rate),
            "nonpositive_rate_ucb": float(nonpositive_ucb),
            "severe_loss_rate": float(severe_rate),
            "severe_loss_rate_ucb": float(severe_ucb),
            "subgroup_feasible": bool(subgroup_blocking_count == 0),
            "subgroup_blocking_count": int(subgroup_blocking_count),
            "subgroup_results": subgroup_results,
            "feasible": bool(
                coverage >= min_coverage_value
                and nonpositive_ucb <= float(nonpositive_alpha_value)
                and severe_ucb <= float(severe_alpha_value)
                and subgroup_blocking_count == 0
            ),
        }
        threshold_rows.append(row_doc)
        if row_doc["feasible"]:
            feasible_rows.append(row_doc)

    if not feasible_rows:
        return {
            "version": EXECUTION_RISK_CONTROL_VERSION,
            "policy": EXECUTION_RISK_CONTROL_POLICY_ID,
            "status": "skipped",
            "reason": "NO_FEASIBLE_THRESHOLD",
            "rows_total": int(total_rows),
            "windows_covered": int(window_count),
            "decision_metric_name": str(decision_metric_name).strip() or DEFAULT_DECISION_METRIC,
            "nonpositive_alpha": float(nonpositive_alpha_value),
            "severe_loss_alpha": float(severe_alpha_value),
            "severe_loss_return_threshold": float(severe_threshold),
            "confidence_delta": float(delta_value),
            "min_coverage": int(min_coverage_value),
            "weighting": {
                "enabled": True,
                "mode": "window_recency_exponential_v1",
                "half_life_windows": float(weighting_half_life_value),
                "max_window_index": int(max_window_index),
                "covariate_similarity": covariate_similarity,
                "density_ratio": _sanitize_density_ratio_contract(density_ratio),
            },
            "subgroup_family": {
                "enabled": True,
                "feature_name": str(subgroup_feature_name),
                "bucket_count_requested": int(subgroup_bucket_count_value),
                "bucket_count_effective": int(subgroup_bucket_count_effective),
                "bounds": [float(value) for value in subgroup_bounds],
                "min_coverage": int(subgroup_min_coverage_value),
            },
            "threshold_results": threshold_rows[:20],
        }

    selected_row = max(
        feasible_rows,
        key=lambda item: (
            float(item["mean_return"]),
            float(item["coverage"]),
            -float(item["nonpositive_rate_ucb"]),
            -float(item["severe_loss_rate_ucb"]),
            -float(item["threshold"]),
        ),
    )
    return {
        "version": EXECUTION_RISK_CONTROL_VERSION,
        "policy": EXECUTION_RISK_CONTROL_POLICY_ID,
        "status": "ready",
        "source": "walk_forward_oos_trade_replay",
        "decision_metric_name": str(decision_metric_name).strip() or DEFAULT_DECISION_METRIC,
        "rows_total": int(total_rows),
        "windows_covered": int(window_count),
        "nonpositive_alpha": float(nonpositive_alpha_value),
        "severe_loss_alpha": float(severe_alpha_value),
        "severe_loss_return_threshold": float(severe_threshold),
        "confidence_delta": float(delta_value),
        "min_coverage": int(min_coverage_value),
        "weighting": {
            "enabled": True,
            "mode": "window_recency_exponential_v1",
            "half_life_windows": float(weighting_half_life_value),
            "max_window_index": int(max_window_index),
            "covariate_similarity": covariate_similarity,
            "density_ratio": _sanitize_density_ratio_contract(density_ratio),
        },
        "subgroup_family": {
            "enabled": True,
            "feature_name": str(subgroup_feature_name),
            "bucket_count_requested": int(subgroup_bucket_count_value),
            "bucket_count_effective": int(subgroup_bucket_count_effective),
            "bounds": [float(value) for value in subgroup_bounds],
            "min_coverage": int(subgroup_min_coverage_value),
        },
        "selected_threshold": float(selected_row["threshold"]),
        "selected_coverage": int(selected_row["coverage"]),
        "selected_coverage_ratio": float(selected_row["coverage_ratio"]),
        "selected_effective_sample_size": float(selected_row.get("effective_sample_size", 0.0) or 0.0),
        "selected_mean_return": float(selected_row["mean_return"]),
        "selected_nonpositive_rate": float(selected_row["nonpositive_rate"]),
        "selected_nonpositive_rate_ucb": float(selected_row["nonpositive_rate_ucb"]),
        "selected_severe_loss_rate": float(selected_row["severe_loss_rate"]),
        "selected_severe_loss_rate_ucb": float(selected_row["severe_loss_rate_ucb"]),
        "selected_subgroup_results": list(selected_row.get("subgroup_results") or []),
        "size_ladder": _build_size_ladder_contract(
            selected_rows=[
                item for item in decision_rows if float(item["metric_value"]) >= float(selected_row["threshold"])
            ],
            subgroup_feature_name=subgroup_feature_name,
            severe_threshold=severe_threshold,
            severe_alpha=severe_alpha_value,
            confidence_delta=delta_value,
            subgroup_min_coverage=subgroup_min_coverage_value,
            steps=size_ladder_steps,
            trade_action_policy=policy,
        ),
        "feasible_threshold_count": int(len(feasible_rows)),
        "threshold_results": threshold_rows[:20],
        "live_gate": {
            "enabled": True,
            "metric_name": str(decision_metric_name).strip() or DEFAULT_DECISION_METRIC,
            "threshold": float(selected_row["threshold"]),
            "skip_reason_code": DEFAULT_SKIP_REASON_CODE,
            "action_source_field": "trade_action.recommended_action",
            "selection_score_field": "model_prob",
            "subgroup_feature_name": str(subgroup_feature_name),
        },
        "online_adaptation": {
            "enabled": True,
            "mode": "recent_closed_trade_hoeffding_stepup_v1",
            "lookback_trades": int(DEFAULT_ONLINE_LOOKBACK_TRADES),
            "max_step_up": int(DEFAULT_ONLINE_MAX_STEP_UP),
            "recovery_streak_required": int(DEFAULT_ONLINE_RECOVERY_STREAK_REQUIRED),
            "halt_breach_streak": int(DEFAULT_ONLINE_HALT_BREACH_STREAK),
            "halt_reason_code": DEFAULT_ONLINE_HALT_REASON_CODE,
            "martingale_enabled": True,
            "martingale_mode": "bernoulli_betting_eprocess_v1",
            "martingale_bet_fraction": float(DEFAULT_ONLINE_MARTINGALE_BET_FRACTION),
            "martingale_halt_threshold": float(DEFAULT_ONLINE_MARTINGALE_HALT_THRESHOLD),
            "martingale_clear_threshold": float(DEFAULT_ONLINE_MARTINGALE_CLEAR_THRESHOLD),
            "martingale_escalation_threshold": float(DEFAULT_ONLINE_MARTINGALE_ESCALATION_THRESHOLD),
            "martingale_halt_reason_code": DEFAULT_ONLINE_MARTINGALE_HALT_REASON_CODE,
            "martingale_critical_reason_code": DEFAULT_ONLINE_MARTINGALE_CRITICAL_REASON_CODE,
            "confidence_delta": float(delta_value),
            "checkpoint_name": "execution_risk_control_online_buffer",
        },
    }


def normalize_execution_risk_control_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    normalized = dict(payload or {})
    backfilled_fields: list[str] = []
    issues: list[str] = []
    version = _safe_optional_int(normalized.get("version"))
    if version is None:
        normalized["version"] = EXECUTION_RISK_CONTROL_VERSION
        backfilled_fields.append("version")
    else:
        normalized["version"] = int(version)
    if int(normalized["version"]) != EXECUTION_RISK_CONTROL_VERSION:
        issues.append("EXECUTION_RISK_CONTROL_VERSION_UNSUPPORTED")
    policy = str(normalized.get("policy", "")).strip()
    if not policy:
        normalized["policy"] = EXECUTION_RISK_CONTROL_POLICY_ID
        backfilled_fields.append("policy")
    elif policy != EXECUTION_RISK_CONTROL_POLICY_ID:
        issues.append("EXECUTION_RISK_CONTROL_POLICY_UNSUPPORTED")
    status = str(normalized.get("status", "")).strip().lower()
    normalized["status"] = status or "skipped"
    live_gate = normalized.get("live_gate")
    if isinstance(live_gate, dict):
        if _safe_optional_float(live_gate.get("threshold")) is not None:
            live_gate["threshold"] = float(live_gate["threshold"])
        live_gate["enabled"] = bool(live_gate.get("enabled", False))
        live_gate["metric_name"] = str(
            live_gate.get("metric_name", normalized.get("decision_metric_name", DEFAULT_DECISION_METRIC))
        ).strip() or DEFAULT_DECISION_METRIC
        live_gate["skip_reason_code"] = str(
            live_gate.get("skip_reason_code", DEFAULT_SKIP_REASON_CODE)
        ).strip() or DEFAULT_SKIP_REASON_CODE
        live_gate["subgroup_feature_name"] = str(
            live_gate.get("subgroup_feature_name", normalized.get("subgroup_family", {}).get("feature_name", ""))
        ).strip()
        normalized["live_gate"] = live_gate
    subgroup_family = normalized.get("subgroup_family")
    if isinstance(subgroup_family, dict):
        subgroup_family["enabled"] = bool(subgroup_family.get("enabled", False))
        subgroup_family["feature_name"] = str(subgroup_family.get("feature_name", "")).strip()
        subgroup_family["bucket_count_requested"] = int(subgroup_family.get("bucket_count_requested", 0) or 0)
        subgroup_family["bucket_count_effective"] = int(subgroup_family.get("bucket_count_effective", 0) or 0)
        subgroup_family["bounds"] = [
            float(value) for value in (subgroup_family.get("bounds") or []) if _safe_optional_float(value) is not None
        ]
        subgroup_family["min_coverage"] = int(subgroup_family.get("min_coverage", 0) or 0)
        normalized["subgroup_family"] = subgroup_family
    if isinstance(normalized.get("selected_subgroup_results"), list):
        normalized["selected_subgroup_results"] = [
            _normalize_subgroup_result(item)
            for item in normalized.get("selected_subgroup_results") or []
            if isinstance(item, dict)
        ]
    weighting = normalized.get("weighting")
    if not isinstance(weighting, dict):
        weighting = {
            "enabled": False,
            "mode": "",
            "half_life_windows": 0.0,
            "max_window_index": -1,
        }
        normalized["weighting"] = weighting
        backfilled_fields.append("weighting")
    if isinstance(weighting, dict):
        weighting["enabled"] = bool(weighting.get("enabled", False))
        weighting["mode"] = str(weighting.get("mode", "")).strip()
        weighting["half_life_windows"] = float(weighting.get("half_life_windows", 0.0) or 0.0)
        weighting["max_window_index"] = int(weighting.get("max_window_index", -1) or -1)
        weighting["covariate_similarity"] = _normalize_covariate_similarity_contract(
            weighting.get("covariate_similarity") if isinstance(weighting.get("covariate_similarity"), dict) else {}
        )
        weighting["density_ratio"] = _normalize_density_ratio_contract(
            weighting.get("density_ratio") if isinstance(weighting.get("density_ratio"), dict) else {}
        )
        normalized["weighting"] = weighting
    online_adaptation = normalized.get("online_adaptation")
    if not isinstance(online_adaptation, dict):
        online_adaptation = {
            "enabled": False,
            "mode": "",
            "lookback_trades": 0,
            "max_step_up": 0,
            "recovery_streak_required": 0,
            "halt_breach_streak": 0,
            "halt_reason_code": "",
            "martingale_enabled": False,
            "martingale_mode": "",
            "martingale_bet_fraction": 0.0,
            "martingale_halt_threshold": 0.0,
            "martingale_clear_threshold": 0.0,
            "martingale_escalation_threshold": 0.0,
            "martingale_halt_reason_code": "",
            "martingale_critical_reason_code": "",
            "confidence_delta": 0.0,
            "checkpoint_name": "",
        }
        normalized["online_adaptation"] = online_adaptation
        backfilled_fields.append("online_adaptation")
    if isinstance(online_adaptation, dict):
        online_adaptation["enabled"] = bool(online_adaptation.get("enabled", False))
        online_adaptation["mode"] = str(online_adaptation.get("mode", "")).strip()
        online_adaptation["lookback_trades"] = int(online_adaptation.get("lookback_trades", 0) or 0)
        online_adaptation["max_step_up"] = int(online_adaptation.get("max_step_up", 0) or 0)
        online_adaptation["recovery_streak_required"] = int(
            online_adaptation.get("recovery_streak_required", 0) or 0
        )
        online_adaptation["halt_breach_streak"] = int(online_adaptation.get("halt_breach_streak", 0) or 0)
        online_adaptation["halt_reason_code"] = str(online_adaptation.get("halt_reason_code", "")).strip()
        online_adaptation["martingale_enabled"] = bool(online_adaptation.get("martingale_enabled", False))
        online_adaptation["martingale_mode"] = str(online_adaptation.get("martingale_mode", "")).strip()
        online_adaptation["martingale_bet_fraction"] = float(
            online_adaptation.get("martingale_bet_fraction", 0.0) or 0.0
        )
        online_adaptation["martingale_halt_threshold"] = float(
            online_adaptation.get("martingale_halt_threshold", 0.0) or 0.0
        )
        online_adaptation["martingale_clear_threshold"] = float(
            online_adaptation.get("martingale_clear_threshold", 0.0) or 0.0
        )
        online_adaptation["martingale_escalation_threshold"] = float(
            online_adaptation.get("martingale_escalation_threshold", 0.0) or 0.0
        )
        online_adaptation["martingale_halt_reason_code"] = str(
            online_adaptation.get("martingale_halt_reason_code", "")
        ).strip()
        online_adaptation["martingale_critical_reason_code"] = str(
            online_adaptation.get("martingale_critical_reason_code", "")
        ).strip()
        online_adaptation["confidence_delta"] = float(online_adaptation.get("confidence_delta", 0.0) or 0.0)
        online_adaptation["checkpoint_name"] = str(online_adaptation.get("checkpoint_name", "")).strip()
        normalized["online_adaptation"] = online_adaptation
    size_ladder = normalized.get("size_ladder")
    if isinstance(size_ladder, dict):
        normalized["size_ladder"] = _normalize_size_ladder(size_ladder)
    if normalized["status"] == "ready":
        if _safe_optional_float(normalized.get("selected_threshold")) is None:
            issues.append("EXECUTION_RISK_CONTROL_THRESHOLD_MISSING")
        if _safe_optional_int(normalized.get("selected_coverage")) is None:
            issues.append("EXECUTION_RISK_CONTROL_COVERAGE_MISSING")
        if _safe_optional_float(normalized.get("selected_nonpositive_rate_ucb")) is None:
            issues.append("EXECUTION_RISK_CONTROL_NONPOSITIVE_UCB_MISSING")
        if _safe_optional_float(normalized.get("selected_severe_loss_rate_ucb")) is None:
            issues.append("EXECUTION_RISK_CONTROL_SEVERE_UCB_MISSING")
        if not isinstance(normalized.get("live_gate"), dict):
            issues.append("EXECUTION_RISK_CONTROL_LIVE_GATE_MISSING")
        if not isinstance(normalized.get("subgroup_family"), dict):
            issues.append("EXECUTION_RISK_CONTROL_SUBGROUP_FAMILY_MISSING")
    normalized["contract_backfilled_fields"] = list(dict.fromkeys(backfilled_fields))
    normalized["contract_issues"] = list(dict.fromkeys(issues))
    if normalized["contract_issues"]:
        normalized["contract_status"] = "invalid"
    elif normalized["contract_backfilled_fields"]:
        normalized["contract_status"] = "backfilled"
    else:
        normalized["contract_status"] = "ok"
    return normalized


def resolve_execution_risk_control_decision(
    *,
    risk_control_payload: dict[str, Any] | None,
    selection_score: float | None,
    trade_action: dict[str, Any] | None,
    threshold_override: float | None = None,
) -> dict[str, Any]:
    normalized = normalize_execution_risk_control_payload(risk_control_payload)
    live_gate = normalized.get("live_gate") if isinstance(normalized.get("live_gate"), dict) else {}
    if (
        normalized.get("status") != "ready"
        or normalized.get("contract_status") == "invalid"
        or not bool(live_gate.get("enabled", False))
    ):
        return {
            "enabled": False,
            "allowed": True,
            "reason_code": "",
            "contract_status": str(normalized.get("contract_status", "")).strip(),
        }
    threshold = _safe_optional_float(threshold_override)
    if threshold is None:
        threshold = _safe_optional_float(live_gate.get("threshold"))
    metric_name = str(live_gate.get("metric_name", DEFAULT_DECISION_METRIC)).strip() or DEFAULT_DECISION_METRIC
    metric_value = _resolve_runtime_metric_value(
        metric_name=metric_name,
        selection_score=selection_score,
        trade_action=trade_action,
    )
    if threshold is None or metric_value is None:
        subgroup_info = _resolve_live_subgroup_info(
            subgroup_family=normalized.get("subgroup_family") if isinstance(normalized.get("subgroup_family"), dict) else {},
            trade_action=trade_action,
        )
        return {
            "enabled": True,
            "allowed": False,
            "reason_code": str(live_gate.get("skip_reason_code", DEFAULT_SKIP_REASON_CODE)).strip()
            or DEFAULT_SKIP_REASON_CODE,
            "metric_name": metric_name,
            "metric_value": metric_value,
            "threshold": threshold,
            "contract_status": str(normalized.get("contract_status", "")).strip(),
            "reason": "METRIC_UNAVAILABLE",
            **subgroup_info,
        }
    subgroup_info = _resolve_live_subgroup_info(
        subgroup_family=normalized.get("subgroup_family") if isinstance(normalized.get("subgroup_family"), dict) else {},
        trade_action=trade_action,
    )
    return {
        "enabled": True,
        "allowed": float(metric_value) >= float(threshold),
        "reason_code": (
            ""
            if float(metric_value) >= float(threshold)
            else str(live_gate.get("skip_reason_code", DEFAULT_SKIP_REASON_CODE)).strip() or DEFAULT_SKIP_REASON_CODE
        ),
        "metric_name": metric_name,
        "metric_value": float(metric_value),
        "threshold": float(threshold),
        "contract_status": str(normalized.get("contract_status", "")).strip(),
        **subgroup_info,
    }


def resolve_execution_risk_control_size_decision(
    *,
    risk_control_payload: dict[str, Any] | None,
    trade_action: dict[str, Any] | None,
    requested_multiplier: float | None,
) -> dict[str, Any]:
    normalized = normalize_execution_risk_control_payload(risk_control_payload)
    size_ladder = normalized.get("size_ladder") if isinstance(normalized.get("size_ladder"), dict) else {}
    if (
        normalized.get("status") != "ready"
        or normalized.get("contract_status") == "invalid"
        or not bool(size_ladder.get("enabled", False))
    ):
        return {
            "enabled": False,
            "allowed": True,
            "requested_multiplier": _safe_optional_float(requested_multiplier),
            "resolved_multiplier": _safe_optional_float(requested_multiplier),
        }
    subgroup_info = _resolve_live_subgroup_info(
        subgroup_family=normalized.get("subgroup_family") if isinstance(normalized.get("subgroup_family"), dict) else {},
        trade_action=trade_action,
    )
    requested_value = max(float(_safe_optional_float(requested_multiplier) or 1.0), 0.0)
    subgroup_bucket = subgroup_info.get("subgroup_bucket")
    group_limits = {
        int(item.get("bucket_index", -1)): float(item.get("max_multiplier", 0.0) or 0.0)
        for item in (size_ladder.get("group_limits") or [])
        if isinstance(item, dict) and _safe_optional_int(item.get("bucket_index")) is not None
    }
    subgroup_max = (
        group_limits.get(int(subgroup_bucket), None)
        if subgroup_bucket is not None
        else None
    )
    global_max = max(float(size_ladder.get("global_max_multiplier", 0.0) or 0.0), 0.0)
    admissible_max = float(subgroup_max) if subgroup_max is not None else float(global_max)
    resolved_multiplier = min(requested_value, admissible_max)
    return {
        "enabled": True,
        "allowed": bool(resolved_multiplier > 0.0),
        "reason_code": (
            ""
            if resolved_multiplier > 0.0
            else str(size_ladder.get("skip_reason_code", DEFAULT_SIZE_SKIP_REASON_CODE)).strip() or DEFAULT_SIZE_SKIP_REASON_CODE
        ),
        "requested_multiplier": float(requested_value),
        "resolved_multiplier": float(resolved_multiplier),
        "global_max_multiplier": float(global_max),
        "subgroup_max_multiplier": float(subgroup_max) if subgroup_max is not None else None,
        "subgroup_bucket": subgroup_bucket,
        "subgroup_label": subgroup_info.get("subgroup_label"),
        "subgroup_feature_name": subgroup_info.get("subgroup_feature_name"),
    }


def resolve_execution_risk_control_online_state(
    *,
    risk_control_payload: dict[str, Any] | None,
    previous_state: dict[str, Any] | None,
    recent_trade_count: int,
    recent_nonpositive_rate_ucb: float,
    recent_severe_loss_rate_ucb: float,
    recent_max_exit_ts_ms: int | None = None,
) -> dict[str, Any]:
    normalized = normalize_execution_risk_control_payload(risk_control_payload)
    online_cfg = dict(normalized.get("online_adaptation") or {})
    base_threshold = _safe_optional_float(normalized.get("selected_threshold"))
    threshold_values = sorted(
        {
            float(item.get("threshold"))
            for item in (normalized.get("threshold_results") or [])
            if isinstance(item, dict) and _safe_optional_float(item.get("threshold")) is not None
        }
    )
    if base_threshold is None and threshold_values:
        base_threshold = float(threshold_values[0])
    if not bool(online_cfg.get("enabled", False)) or base_threshold is None:
        return {
            "enabled": False,
            "base_threshold": base_threshold,
            "adaptive_threshold": base_threshold,
        }
    if not threshold_values:
        threshold_values = [float(base_threshold)]
    elif not any(abs(float(value) - float(base_threshold)) < 1e-12 for value in threshold_values):
        threshold_values = sorted([*threshold_values, float(base_threshold)])
    nonpositive_alpha = max(float(normalized.get("nonpositive_alpha", 1.0) or 1.0), 1e-6)
    severe_alpha = max(float(normalized.get("severe_loss_alpha", 1.0) or 1.0), 1e-6)
    breach_count = int(float(recent_nonpositive_rate_ucb) > nonpositive_alpha) + int(
        float(recent_severe_loss_rate_ucb) > severe_alpha
    )
    max_step_up = max(int(online_cfg.get("max_step_up", 0) or 0), 0)
    recovery_streak_required = max(int(online_cfg.get("recovery_streak_required", 0) or 0), 1)
    halt_breach_streak = max(int(online_cfg.get("halt_breach_streak", 0) or 0), 1)
    previous = dict(previous_state or {})
    previous_step_up = max(int(previous.get("step_up", 0) or 0), 0)
    previous_breach_streak = max(int(previous.get("breach_streak", 0) or 0), 0)
    previous_recovery_streak = max(int(previous.get("recovery_streak", 0) or 0), 0)
    previous_halt_triggered = bool(previous.get("halt_triggered"))
    previous_last_processed_exit_ts_ms = _safe_optional_int(previous.get("last_processed_exit_ts_ms"))
    has_new_evidence = (
        recent_max_exit_ts_ms is not None
        and (
            previous_last_processed_exit_ts_ms is None
            or int(recent_max_exit_ts_ms) > int(previous_last_processed_exit_ts_ms)
        )
    )

    if not has_new_evidence and previous_last_processed_exit_ts_ms is not None:
        step_up = previous_step_up
        breach_streak = previous_breach_streak
        recovery_streak = previous_recovery_streak
    elif breach_count > 0:
        step_up = min(max(previous_step_up, breach_count), max_step_up)
        breach_streak = previous_breach_streak + 1
        recovery_streak = 0
    else:
        breach_streak = 0
        step_up = previous_step_up
        recovery_streak = previous_recovery_streak
        if int(recent_trade_count) > 0 and step_up > 0:
            recovery_streak += 1
            if recovery_streak >= recovery_streak_required:
                step_up = max(step_up - 1, 0)
                recovery_streak = 0

    base_index = 0
    for index, value in enumerate(threshold_values):
        if float(value) >= float(base_threshold):
            base_index = index
            break
    adaptive_index = min(base_index + int(step_up), max(len(threshold_values) - 1, 0))
    adaptive_threshold = float(threshold_values[adaptive_index])
    halt_triggered = bool(breach_streak >= halt_breach_streak)
    if not has_new_evidence and previous_last_processed_exit_ts_ms is not None:
        halt_triggered = previous_halt_triggered
    clear_halt = bool(previous_halt_triggered) and (not halt_triggered) and int(step_up) == 0 and breach_count == 0 and bool(has_new_evidence)
    halt_reason_code = (
        str(online_cfg.get("halt_reason_code", DEFAULT_ONLINE_HALT_REASON_CODE)).strip()
        or DEFAULT_ONLINE_HALT_REASON_CODE
    )
    return {
        "enabled": True,
        "mode": str(online_cfg.get("mode", "")).strip(),
        "base_threshold": float(base_threshold),
        "adaptive_threshold": float(adaptive_threshold),
        "recent_trade_count": int(recent_trade_count),
        "recent_nonpositive_rate_ucb": float(recent_nonpositive_rate_ucb),
        "recent_severe_loss_rate_ucb": float(recent_severe_loss_rate_ucb),
        "breach_count": int(breach_count),
        "previous_step_up": int(previous_step_up),
        "step_up": int(step_up),
        "breach_streak": int(breach_streak),
        "recovery_streak": int(recovery_streak),
        "halt_triggered": bool(halt_triggered),
        "halt_reason_code": halt_reason_code,
        "halt_reason_codes": [halt_reason_code] if halt_triggered else [],
        "halt_action": "HALT_NEW_INTENTS",
        "clear_halt": bool(clear_halt),
        "clear_reason_codes": [halt_reason_code] if clear_halt else [],
        "new_evidence": bool(has_new_evidence),
        "last_processed_exit_ts_ms": (
            int(recent_max_exit_ts_ms)
            if has_new_evidence and recent_max_exit_ts_ms is not None
            else previous_last_processed_exit_ts_ms
        ),
    }


def resolve_execution_risk_control_martingale_state(
    *,
    risk_control_payload: dict[str, Any] | None,
    previous_state: dict[str, Any] | None,
    observations: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    normalized = normalize_execution_risk_control_payload(risk_control_payload)
    online_cfg = dict(normalized.get("online_adaptation") or {})
    if not bool(online_cfg.get("martingale_enabled", False)):
        return {
            "enabled": False,
            "martingale_nonpositive_e_value": 1.0,
            "martingale_severe_e_value": 1.0,
        }
    previous = dict(previous_state or {})
    nonpositive_e = max(float(previous.get("martingale_nonpositive_e_value", 1.0) or 1.0), 1e-12)
    severe_e = max(float(previous.get("martingale_severe_e_value", 1.0) or 1.0), 1e-12)
    last_processed_exit_ts_ms = _safe_optional_int(previous.get("martingale_last_processed_exit_ts_ms"))
    latest_processed = int(last_processed_exit_ts_ms) if last_processed_exit_ts_ms is not None else -1
    alpha_nonpositive = max(float(normalized.get("nonpositive_alpha", 1.0) or 1.0), 1e-6)
    alpha_severe = max(float(normalized.get("severe_loss_alpha", 1.0) or 1.0), 1e-6)
    severe_threshold = max(float(normalized.get("severe_loss_return_threshold", 0.0) or 0.0), 0.0)
    bet_fraction = min(max(float(online_cfg.get("martingale_bet_fraction", 1.0) or 1.0), 0.0), 1.0)
    martingale_halt_threshold = max(
        float(online_cfg.get("martingale_halt_threshold", DEFAULT_ONLINE_MARTINGALE_HALT_THRESHOLD) or DEFAULT_ONLINE_MARTINGALE_HALT_THRESHOLD),
        1.0,
    )
    martingale_clear_threshold = max(
        float(online_cfg.get("martingale_clear_threshold", DEFAULT_ONLINE_MARTINGALE_CLEAR_THRESHOLD) or DEFAULT_ONLINE_MARTINGALE_CLEAR_THRESHOLD),
        1.0,
    )
    martingale_escalation_threshold = max(
        float(
            online_cfg.get(
                "martingale_escalation_threshold",
                DEFAULT_ONLINE_MARTINGALE_ESCALATION_THRESHOLD,
            )
            or DEFAULT_ONLINE_MARTINGALE_ESCALATION_THRESHOLD
        ),
        float(martingale_halt_threshold),
    )
    new_observations = []
    for item in observations or []:
        exit_ts_ms = _safe_optional_int(item.get("exit_ts_ms"))
        pnl_pct = _safe_optional_float(item.get("pnl_pct"))
        if exit_ts_ms is None or pnl_pct is None:
            continue
        if int(exit_ts_ms) <= latest_processed:
            continue
        new_observations.append({"exit_ts_ms": int(exit_ts_ms), "pnl_pct": float(pnl_pct)})
    new_observations.sort(key=lambda item: int(item["exit_ts_ms"]))
    for item in new_observations:
        nonpositive_x = 1.0 if float(item["pnl_pct"]) <= 0.0 else 0.0
        severe_x = 1.0 if float(item["pnl_pct"]) <= -float(severe_threshold) else 0.0
        nonpositive_e *= max(1.0 + bet_fraction * (nonpositive_x - alpha_nonpositive), 1e-9)
        severe_e *= max(1.0 + bet_fraction * (severe_x - alpha_severe), 1e-9)
        latest_processed = max(latest_processed, int(item["exit_ts_ms"]))
    max_e_value = max(float(nonpositive_e), float(severe_e))
    halt_triggered = bool(max_e_value >= martingale_halt_threshold)
    critical_triggered = bool(max_e_value >= martingale_escalation_threshold)
    clear_halt = bool(previous.get("martingale_halt_triggered")) and (max_e_value <= martingale_clear_threshold)
    halt_reason_code = (
        str(
            online_cfg.get(
                "martingale_critical_reason_code" if critical_triggered else "martingale_halt_reason_code",
                (
                    DEFAULT_ONLINE_MARTINGALE_CRITICAL_REASON_CODE
                    if critical_triggered
                    else DEFAULT_ONLINE_MARTINGALE_HALT_REASON_CODE
                ),
            )
        ).strip()
        or (
            DEFAULT_ONLINE_MARTINGALE_CRITICAL_REASON_CODE
            if critical_triggered
            else DEFAULT_ONLINE_MARTINGALE_HALT_REASON_CODE
        )
    )
    previous_halt_reason_code = (
        str(previous.get("martingale_halt_reason_code", "")).strip()
        or str(online_cfg.get("martingale_halt_reason_code", DEFAULT_ONLINE_MARTINGALE_HALT_REASON_CODE)).strip()
        or DEFAULT_ONLINE_MARTINGALE_HALT_REASON_CODE
    )
    return {
        "enabled": True,
        "mode": str(online_cfg.get("martingale_mode", "")).strip() or "bernoulli_betting_eprocess_v1",
        "martingale_nonpositive_e_value": float(nonpositive_e),
        "martingale_severe_e_value": float(severe_e),
        "martingale_max_e_value": float(max_e_value),
        "martingale_halt_threshold": float(martingale_halt_threshold),
        "martingale_clear_threshold": float(martingale_clear_threshold),
        "martingale_escalation_threshold": float(martingale_escalation_threshold),
        "martingale_new_observation_count": int(len(new_observations)),
        "martingale_last_processed_exit_ts_ms": int(latest_processed) if latest_processed >= 0 else None,
        "martingale_halt_triggered": bool(halt_triggered),
        "martingale_critical_triggered": bool(critical_triggered),
        "martingale_halt_reason_code": halt_reason_code,
        "martingale_halt_action": (
            "HALT_AND_CANCEL_BOT_ORDERS" if critical_triggered else "HALT_NEW_INTENTS"
        ),
        "martingale_clear_halt": bool(clear_halt),
        "martingale_clear_reason_codes": [previous_halt_reason_code] if clear_halt else [],
    }


def _resolve_decision_metric_value(
    *,
    decision: dict[str, Any],
    selection_score: float,
    metric_name: str,
) -> float | None:
    normalized_metric = str(metric_name).strip().lower() or DEFAULT_DECISION_METRIC
    if normalized_metric == "selection_score":
        return float(selection_score)
    return _safe_optional_float(
        decision.get("expected_action_value", decision.get("expected_objective_score"))
    )


def _resolve_runtime_metric_value(
    *,
    metric_name: str,
    selection_score: float | None,
    trade_action: dict[str, Any] | None,
) -> float | None:
    normalized_metric = str(metric_name).strip().lower() or DEFAULT_DECISION_METRIC
    if normalized_metric == "selection_score":
        return _safe_optional_float(selection_score)
    trade_action_payload = dict(trade_action or {})
    return _safe_optional_float(
        trade_action_payload.get(
            "expected_action_value",
            trade_action_payload.get("expected_objective_score"),
        )
    )


def _build_size_ladder_contract(
    *,
    selected_rows: list[dict[str, Any]],
    subgroup_feature_name: str,
    severe_threshold: float,
    severe_alpha: float,
    confidence_delta: float,
    subgroup_min_coverage: int,
    steps: int,
    trade_action_policy: dict[str, Any],
) -> dict[str, Any]:
    requested_min = _safe_optional_float(
        ((trade_action_policy.get("notional_model") or {}).get("deprecated_requested_size_multiplier_min"))
    )
    requested_max = _safe_optional_float(
        ((trade_action_policy.get("notional_model") or {}).get("deprecated_requested_size_multiplier_max"))
    )
    ladder = _build_size_ladder_candidates(
        requested_min=requested_min,
        requested_max=requested_max,
        steps=steps,
    )
    if not selected_rows or not ladder:
        return {
            "enabled": False,
            "status": "skipped",
            "reason": "NO_SELECTED_ROWS",
        }
    per_ladder_delta = max(float(confidence_delta), 1e-12)
    global_rows = _evaluate_size_ladder_rows(
        rows=selected_rows,
        ladder=ladder,
        severe_threshold=severe_threshold,
        severe_alpha=severe_alpha,
        delta=per_ladder_delta,
    )
    global_max = _resolve_max_feasible_multiplier(global_rows)
    grouped: dict[tuple[int, str], list[dict[str, Any]]] = {}
    for item in selected_rows:
        subgroup_bucket = item.get("subgroup_bucket")
        subgroup_label = str(item.get("subgroup_label", "")).strip()
        if subgroup_bucket is None or not subgroup_label:
            continue
        grouped.setdefault((int(subgroup_bucket), subgroup_label), []).append(item)
    group_limits: list[dict[str, Any]] = []
    for (bucket_index, subgroup_label), rows in sorted(grouped.items(), key=lambda pair: pair[0][0]):
        evaluations = _evaluate_size_ladder_rows(
            rows=rows,
            ladder=ladder,
            severe_threshold=severe_threshold,
            severe_alpha=severe_alpha,
            delta=per_ladder_delta,
        )
        group_limits.append(
            {
                "bucket_index": int(bucket_index),
                "label": subgroup_label,
                "coverage": int(len(rows)),
                "max_multiplier": (
                    float(_resolve_max_feasible_multiplier(evaluations))
                    if len(rows) >= int(subgroup_min_coverage)
                    else None
                ),
                "status": (
                    "ok"
                    if len(rows) >= int(subgroup_min_coverage)
                    else "insufficient_coverage"
                ),
                "ladder_results": evaluations[:10],
            }
        )
    return {
        "enabled": True,
        "status": "ready",
        "method": "finite_size_ladder_tail_ucb_v1",
        "feature_name": str(subgroup_feature_name).strip(),
        "ladder_multipliers": [float(value) for value in ladder],
        "global_max_multiplier": float(global_max),
        "skip_reason_code": DEFAULT_SIZE_SKIP_REASON_CODE,
        "global_ladder_results": global_rows[:10],
        "group_limits": group_limits,
    }


def _normalize_size_ladder(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload or {})
    normalized["enabled"] = bool(normalized.get("enabled", False))
    normalized["status"] = str(normalized.get("status", "")).strip().lower()
    normalized["method"] = str(normalized.get("method", "")).strip()
    normalized["feature_name"] = str(normalized.get("feature_name", "")).strip()
    normalized["ladder_multipliers"] = [
        float(value) for value in (normalized.get("ladder_multipliers") or []) if _safe_optional_float(value) is not None
    ]
    normalized["global_max_multiplier"] = float(normalized.get("global_max_multiplier", 0.0) or 0.0)
    normalized["skip_reason_code"] = str(
        normalized.get("skip_reason_code", DEFAULT_SIZE_SKIP_REASON_CODE)
    ).strip() or DEFAULT_SIZE_SKIP_REASON_CODE
    normalized["global_ladder_results"] = [
        _normalize_ladder_result(item)
        for item in (normalized.get("global_ladder_results") or [])
        if isinstance(item, dict)
    ]
    normalized["group_limits"] = [
        {
            "bucket_index": int(item.get("bucket_index", 0) or 0),
            "label": str(item.get("label", "")).strip(),
            "coverage": int(item.get("coverage", 0) or 0),
            "max_multiplier": _safe_optional_float(item.get("max_multiplier")),
            "status": str(item.get("status", "")).strip(),
            "ladder_results": [
                _normalize_ladder_result(child)
                for child in (item.get("ladder_results") or [])
                if isinstance(child, dict)
            ],
        }
        for item in (normalized.get("group_limits") or [])
        if isinstance(item, dict)
    ]
    return normalized


def _normalize_ladder_result(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "multiplier": float(payload.get("multiplier", 0.0) or 0.0),
        "coverage": int(payload.get("coverage", 0) or 0),
        "severe_loss_rate": float(payload.get("severe_loss_rate", 0.0) or 0.0),
        "severe_loss_rate_ucb": float(payload.get("severe_loss_rate_ucb", 0.0) or 0.0),
        "feasible": bool(payload.get("feasible", False)),
    }


def _build_size_ladder_candidates(
    *,
    requested_min: float | None,
    requested_max: float | None,
    steps: int,
) -> list[float]:
    minimum = max(float(requested_min or 0.5), 0.0)
    maximum = max(float(requested_max or max(minimum, 1.0)), minimum)
    if maximum <= 0.0:
        return [1.0]
    if maximum <= minimum:
        return [float(maximum)]
    grid = [minimum + ((maximum - minimum) * idx / float(max(int(steps), 2) - 1)) for idx in range(max(int(steps), 2))]
    values = sorted({round(float(value), 6) for value in [minimum, 1.0, maximum, *grid] if float(value) > 0.0})
    return [float(value) for value in values]


def _evaluate_size_ladder_rows(
    *,
    rows: list[dict[str, Any]],
    ladder: list[float],
    severe_threshold: float,
    severe_alpha: float,
    delta: float,
) -> list[dict[str, Any]]:
    coverage = len(rows)
    if coverage <= 0:
        return []
    returns = [float(item["realized_return"]) for item in rows]
    effective_sample_size = _effective_sample_size(rows)
    results: list[dict[str, Any]] = []
    for multiplier in ladder:
        scaled_returns = [float(multiplier) * value for value in returns]
        severe_rate = _weighted_indicator_rate_from_values(
            rows,
            scaled_returns,
            lambda value: value <= -float(severe_threshold),
        )
        severe_ucb = _hoeffding_ucb(severe_rate, effective_sample_size, delta)
        results.append(
            {
                "multiplier": float(multiplier),
                "coverage": int(coverage),
                "effective_sample_size": float(effective_sample_size),
                "severe_loss_rate": float(severe_rate),
                "severe_loss_rate_ucb": float(severe_ucb),
                "feasible": bool(severe_ucb <= float(severe_alpha)),
            }
        )
    return results


def _resolve_max_feasible_multiplier(results: list[dict[str, Any]]) -> float:
    feasible = [float(item["multiplier"]) for item in results if bool(item.get("feasible", False))]
    return max(feasible, default=0.0)


def _resolve_recency_weight(
    *,
    window_index: int,
    max_window_index: int,
    half_life_windows: float,
) -> float:
    if window_index < 0 or max_window_index < 0:
        return 1.0
    lag = max(int(max_window_index) - int(window_index), 0)
    decay = math.log(2.0) / max(float(half_life_windows), 1e-6)
    return float(math.exp(-decay * float(lag)))


def _build_covariate_similarity_contract(
    *,
    decision_rows: list[dict[str, Any]],
    enabled: bool,
    bandwidth: float,
) -> dict[str, Any]:
    if not bool(enabled):
        return {
            "enabled": False,
            "mode": "",
            "bandwidth": float(bandwidth),
            "reference_window_index": None,
            "feature_names": [],
            "center": {},
            "scale": {},
        }
    feature_names = ["selection_score", "rv_12", "rv_36", "atr_pct_14"]
    latest_window_index = max((int(item.get("window_index", -1) or -1) for item in decision_rows), default=-1)
    reference_rows = [item for item in decision_rows if int(item.get("window_index", -1) or -1) == latest_window_index]
    center: dict[str, float] = {}
    scale: dict[str, float] = {}
    active_features: list[str] = []
    for feature_name in feature_names:
        reference_values = [
            float(item.get(feature_name))
            for item in reference_rows
            if _safe_optional_float(item.get(feature_name)) is not None
        ]
        all_values = [
            float(item.get(feature_name))
            for item in decision_rows
            if _safe_optional_float(item.get(feature_name)) is not None
        ]
        if not reference_values or not all_values:
            continue
        center_value = _median(reference_values)
        dispersion = _stddev(all_values)
        if dispersion <= 1e-9:
            dispersion = max(abs(float(center_value)) * 0.1, 1e-6)
        center[feature_name] = float(center_value)
        scale[feature_name] = float(dispersion)
        active_features.append(feature_name)
    return {
        "enabled": bool(active_features),
        "mode": "latest_window_gaussian_state_similarity_v1" if active_features else "",
        "bandwidth": float(max(float(bandwidth), 1e-6)),
        "reference_window_index": int(latest_window_index) if latest_window_index >= 0 else None,
        "feature_names": active_features,
        "center": center,
        "scale": scale,
    }


def _normalize_covariate_similarity_contract(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload or {})
    normalized["enabled"] = bool(normalized.get("enabled", False))
    normalized["mode"] = str(normalized.get("mode", "")).strip()
    normalized["bandwidth"] = float(normalized.get("bandwidth", 0.0) or 0.0)
    normalized["reference_window_index"] = (
        int(normalized.get("reference_window_index"))
        if normalized.get("reference_window_index") is not None
        else None
    )
    normalized["feature_names"] = [
        str(value).strip()
        for value in (normalized.get("feature_names") or [])
        if str(value).strip()
    ]
    normalized["center"] = {
        str(key).strip(): float(value)
        for key, value in dict(normalized.get("center") or {}).items()
        if str(key).strip() and _safe_optional_float(value) is not None
    }
    normalized["scale"] = {
        str(key).strip(): float(value)
        for key, value in dict(normalized.get("scale") or {}).items()
        if str(key).strip() and _safe_optional_float(value) is not None
    }
    return normalized


def _resolve_covariate_similarity_weight(
    *,
    row: dict[str, Any],
    contract: dict[str, Any],
) -> float:
    normalized = _normalize_covariate_similarity_contract(contract)
    if not bool(normalized.get("enabled", False)):
        return 1.0
    feature_names = list(normalized.get("feature_names") or [])
    center = dict(normalized.get("center") or {})
    scale = dict(normalized.get("scale") or {})
    if not feature_names:
        return 1.0
    sq_dist = 0.0
    used = 0
    for feature_name in feature_names:
        value = _safe_optional_float(row.get(feature_name))
        if value is None or feature_name not in center or feature_name not in scale:
            continue
        denom = max(float(scale.get(feature_name, 1.0) or 1.0), 1e-6)
        sq_dist += ((float(value) - float(center[feature_name])) / denom) ** 2
        used += 1
    if used <= 0:
        return 1.0
    mean_sq_dist = sq_dist / float(used)
    bandwidth = max(float(normalized.get("bandwidth", 1.0) or 1.0), 1e-6)
    return float(math.exp(-0.5 * mean_sq_dist / (bandwidth * bandwidth)))


def _build_density_ratio_contract(
    *,
    decision_rows: list[dict[str, Any]],
    enabled: bool,
    clip_min: float,
    clip_max: float,
) -> dict[str, Any]:
    if not bool(enabled):
        return {
            "enabled": False,
            "mode": "",
            "reference_window_index": None,
            "feature_names": [],
            "positive_center": {},
            "positive_scale": {},
            "negative_center": {},
            "negative_scale": {},
            "clip_min": float(clip_min),
            "clip_max": float(clip_max),
        }
    feature_names = ["selection_score", "rv_12", "rv_36", "atr_pct_14"]
    latest_window_index = max((int(item.get("window_index", -1) or -1) for item in decision_rows), default=-1)
    positive_rows = [item for item in decision_rows if int(item.get("window_index", -1) or -1) == latest_window_index]
    negative_rows = [item for item in decision_rows if int(item.get("window_index", -1) or -1) != latest_window_index]
    if not positive_rows or not negative_rows:
        return {
            "enabled": False,
            "mode": "",
            "reference_window_index": int(latest_window_index) if latest_window_index >= 0 else None,
            "feature_names": [],
            "positive_center": {},
            "positive_scale": {},
            "negative_center": {},
            "negative_scale": {},
            "clip_min": float(clip_min),
            "clip_max": float(clip_max),
        }
    active_features: list[str] = []
    positive_center: dict[str, float] = {}
    positive_scale: dict[str, float] = {}
    negative_center: dict[str, float] = {}
    negative_scale: dict[str, float] = {}
    for feature_name in feature_names:
        pos_values = [
            float(item.get(feature_name))
            for item in positive_rows
            if _safe_optional_float(item.get(feature_name)) is not None
        ]
        neg_values = [
            float(item.get(feature_name))
            for item in negative_rows
            if _safe_optional_float(item.get(feature_name)) is not None
        ]
        if not pos_values or not neg_values:
            continue
        pos_std = _stddev(pos_values)
        neg_std = _stddev(neg_values)
        positive_center[feature_name] = float(sum(pos_values) / float(len(pos_values)))
        negative_center[feature_name] = float(sum(neg_values) / float(len(neg_values)))
        positive_scale[feature_name] = float(max(pos_std, 1e-6))
        negative_scale[feature_name] = float(max(neg_std, 1e-6))
        active_features.append(feature_name)
    if active_features:
        logistic_contract = _fit_logistic_density_ratio_contract(
            decision_rows=decision_rows,
            latest_window_index=latest_window_index,
            feature_names=active_features,
            clip_min=clip_min,
            clip_max=clip_max,
        )
        if bool(logistic_contract.get("enabled", False)):
            return logistic_contract
    return {
        "enabled": bool(active_features),
        "mode": "latest_window_diagonal_gaussian_density_ratio_v1" if active_features else "",
        "reference_window_index": int(latest_window_index) if latest_window_index >= 0 else None,
        "feature_names": active_features,
        "positive_center": positive_center,
        "positive_scale": positive_scale,
        "negative_center": negative_center,
        "negative_scale": negative_scale,
        "clip_min": float(max(float(clip_min), 1e-6)),
        "clip_max": float(max(float(clip_max), max(float(clip_min), 1e-6))),
        "classifier_status": "fallback_diagonal_gaussian",
    }


def _normalize_density_ratio_contract(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload or {})
    normalized.pop("_builder_ratio_by_row_id", None)
    normalized["enabled"] = bool(normalized.get("enabled", False))
    normalized["mode"] = str(normalized.get("mode", "")).strip()
    normalized["reference_window_index"] = (
        int(normalized.get("reference_window_index"))
        if normalized.get("reference_window_index") is not None
        else None
    )
    normalized["feature_names"] = [
        str(value).strip()
        for value in (normalized.get("feature_names") or [])
        if str(value).strip()
    ]
    for key_name in ["positive_center", "positive_scale", "negative_center", "negative_scale"]:
        normalized[key_name] = {
            str(key).strip(): float(value)
            for key, value in dict(normalized.get(key_name) or {}).items()
            if str(key).strip() and _safe_optional_float(value) is not None
        }
    normalized["classifier_status"] = str(normalized.get("classifier_status", "")).strip()
    normalized["positive_rows"] = int(normalized.get("positive_rows", 0) or 0)
    normalized["negative_rows"] = int(normalized.get("negative_rows", 0) or 0)
    normalized["clip_fraction"] = float(normalized.get("clip_fraction", 0.0) or 0.0)
    normalized["positive_probability_mean"] = _safe_optional_float(normalized.get("positive_probability_mean"))
    normalized["negative_probability_mean"] = _safe_optional_float(normalized.get("negative_probability_mean"))
    normalized["crossfit_fold_count"] = int(normalized.get("crossfit_fold_count", 0) or 0)
    normalized["crossfit_probability_source"] = str(normalized.get("crossfit_probability_source", "")).strip()
    normalized["crossfit_log_loss"] = _safe_optional_float(normalized.get("crossfit_log_loss"))
    normalized["model"] = _normalize_logistic_ratio_model(
        normalized.get("model") if isinstance(normalized.get("model"), dict) else {}
    )
    normalized["clip_min"] = float(normalized.get("clip_min", 1.0) or 1.0)
    normalized["clip_max"] = float(normalized.get("clip_max", normalized["clip_min"]) or normalized["clip_min"])
    return normalized


def _resolve_density_ratio_weight(
    *,
    row: dict[str, Any],
    contract: dict[str, Any],
) -> float:
    normalized = _normalize_density_ratio_contract(contract)
    if not bool(normalized.get("enabled", False)):
        return 1.0
    row_id = _safe_optional_int(row.get("_row_id"))
    builder_ratio_by_row_id = dict(contract.get("_builder_ratio_by_row_id") or {})
    if row_id is not None and builder_ratio_by_row_id:
        builder_weight = _safe_optional_float(builder_ratio_by_row_id.get(int(row_id)))
        if builder_weight is not None and builder_weight > 0.0:
            return float(builder_weight)
    if str(normalized.get("mode", "")).strip() == "latest_window_logistic_density_ratio_v1":
        return _resolve_logistic_density_ratio_weight(row=row, contract=normalized)
    if str(normalized.get("mode", "")).strip() == "latest_window_logistic_density_ratio_crossfit_v1":
        return _resolve_logistic_density_ratio_weight(row=row, contract=normalized)
    feature_names = list(normalized.get("feature_names") or [])
    if not feature_names:
        return 1.0
    positive_center = dict(normalized.get("positive_center") or {})
    positive_scale = dict(normalized.get("positive_scale") or {})
    negative_center = dict(normalized.get("negative_center") or {})
    negative_scale = dict(normalized.get("negative_scale") or {})
    log_ratio = 0.0
    used = 0
    for feature_name in feature_names:
        value = _safe_optional_float(row.get(feature_name))
        if (
            value is None
            or feature_name not in positive_center
            or feature_name not in positive_scale
            or feature_name not in negative_center
            or feature_name not in negative_scale
        ):
            continue
        pos_sigma = max(float(positive_scale[feature_name]), 1e-6)
        neg_sigma = max(float(negative_scale[feature_name]), 1e-6)
        pos_mu = float(positive_center[feature_name])
        neg_mu = float(negative_center[feature_name])
        x = float(value)
        log_ratio += (
            math.log(neg_sigma / pos_sigma)
            + ((x - neg_mu) ** 2) / (2.0 * neg_sigma * neg_sigma)
            - ((x - pos_mu) ** 2) / (2.0 * pos_sigma * pos_sigma)
        )
        used += 1
    if used <= 0:
        return 1.0
    clipped_log_ratio = min(max(float(log_ratio), -20.0), 20.0)
    ratio = math.exp(clipped_log_ratio)
    clip_min = max(float(normalized.get("clip_min", 1.0) or 1.0), 1e-6)
    clip_max = max(float(normalized.get("clip_max", clip_min) or clip_min), clip_min)
    return float(min(max(ratio, clip_min), clip_max))


def _fit_logistic_density_ratio_contract(
    *,
    decision_rows: list[dict[str, Any]],
    latest_window_index: int,
    feature_names: list[str],
    clip_min: float,
    clip_max: float,
) -> dict[str, Any]:
    x_rows: list[list[float]] = []
    y_rows: list[int] = []
    row_ids: list[int] = []
    for item in decision_rows:
        values: list[float] = []
        valid = True
        for feature_name in feature_names:
            value = _safe_optional_float(item.get(feature_name))
            if value is None:
                valid = False
                break
            values.append(float(value))
        if not valid:
            continue
        x_rows.append(values)
        y_rows.append(1 if int(item.get("window_index", -1) or -1) == int(latest_window_index) else 0)
        row_ids.append(int(item.get("_row_id", len(row_ids))))
    if not x_rows or not y_rows:
        return {"enabled": False}
    y = np.asarray(y_rows, dtype=np.float64)
    positive_rows = int(np.sum(y > 0.5))
    negative_rows = int(len(y_rows) - positive_rows)
    if positive_rows <= 0 or negative_rows <= 0:
        return {"enabled": False}
    x = np.asarray(x_rows, dtype=np.float64)
    fitted_model = _fit_logistic_ratio_model(x=x, y=y)
    probs = _predict_logistic_ratio_probabilities(x=x, model=fitted_model)
    prior_ratio = float(max(negative_rows, 1)) / float(max(positive_rows, 1))
    odds = np.clip(probs, 1e-6, 1.0 - 1e-6) / np.clip(1.0 - probs, 1e-6, 1.0)
    raw_ratios = odds * prior_ratio
    clip_min_value = float(max(float(clip_min), 1e-6))
    clip_max_value = float(max(float(clip_max), clip_min_value))
    clipped = np.clip(raw_ratios, clip_min_value, clip_max_value)
    clip_fraction = float(np.mean(raw_ratios != clipped)) if raw_ratios.size > 0 else 0.0
    crossfit = _fit_crossfit_logistic_density_ratio(
        x=x,
        y=y,
        row_ids=row_ids,
        prior_ratio=prior_ratio,
        clip_min=clip_min_value,
        clip_max=clip_max_value,
    )
    classifier_status = "ready_crossfit" if bool(crossfit.get("enabled", False)) else "ready"
    mode = (
        "latest_window_logistic_density_ratio_crossfit_v1"
        if bool(crossfit.get("enabled", False))
        else "latest_window_logistic_density_ratio_v1"
    )
    return {
        "enabled": True,
        "mode": mode,
        "reference_window_index": int(latest_window_index),
        "feature_names": list(feature_names),
        "positive_center": {},
        "positive_scale": {},
        "negative_center": {},
        "negative_scale": {},
        "clip_min": clip_min_value,
        "clip_max": clip_max_value,
        "classifier_status": classifier_status,
        "positive_rows": int(positive_rows),
        "negative_rows": int(negative_rows),
        "clip_fraction": float(crossfit.get("clip_fraction", clip_fraction) or clip_fraction),
        "positive_probability_mean": (
            _safe_optional_float(crossfit.get("positive_probability_mean"))
            if bool(crossfit.get("enabled", False))
            else float(np.mean(probs[y > 0.5])) if positive_rows > 0 else None
        ),
        "negative_probability_mean": (
            _safe_optional_float(crossfit.get("negative_probability_mean"))
            if bool(crossfit.get("enabled", False))
            else float(np.mean(probs[y <= 0.5])) if negative_rows > 0 else None
        ),
        "crossfit_fold_count": int(crossfit.get("fold_count", 0) or 0),
        "crossfit_probability_source": "out_of_fold" if bool(crossfit.get("enabled", False)) else "in_sample",
        "crossfit_log_loss": _safe_optional_float(crossfit.get("log_loss")),
        "model": {
            "coef": list(fitted_model.get("coef") or []),
            "intercept": float(fitted_model.get("intercept", 0.0) or 0.0),
            "x_mean": list(fitted_model.get("x_mean") or []),
            "x_scale": list(fitted_model.get("x_scale") or []),
            "prior_ratio": float(prior_ratio),
        },
        "_builder_ratio_by_row_id": dict(crossfit.get("ratio_by_row_id") or {}),
    }


def _sanitize_density_ratio_contract(contract: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_density_ratio_contract(contract)
    normalized.pop("_builder_ratio_by_row_id", None)
    return normalized


def _fit_logistic_ratio_model(*, x: np.ndarray, y: np.ndarray) -> dict[str, Any]:
    x_mean = np.mean(x, axis=0)
    x_scale = np.std(x, axis=0)
    x_scale = np.where(x_scale > 1e-6, x_scale, 1.0)
    x_scaled = (x - x_mean) / x_scale
    coef = np.zeros(x.shape[1], dtype=np.float64)
    intercept = 0.0
    for _ in range(DEFAULT_WEIGHTING_DENSITY_RATIO_TRAIN_ITERS):
        logits = np.clip((x_scaled @ coef) + intercept, -30.0, 30.0)
        probs = 1.0 / (1.0 + np.exp(-logits))
        error = probs - y
        grad_coef = (x_scaled.T @ error) / float(len(y)) + (DEFAULT_WEIGHTING_DENSITY_RATIO_L2 * coef)
        grad_intercept = float(np.mean(error))
        coef -= DEFAULT_WEIGHTING_DENSITY_RATIO_LR * grad_coef
        intercept -= DEFAULT_WEIGHTING_DENSITY_RATIO_LR * grad_intercept
    return {
        "coef": coef.tolist(),
        "intercept": float(intercept),
        "x_mean": x_mean.tolist(),
        "x_scale": x_scale.tolist(),
    }


def _predict_logistic_ratio_probabilities(*, x: np.ndarray, model: dict[str, Any]) -> np.ndarray:
    coef = np.asarray(model.get("coef") or [], dtype=np.float64)
    x_mean = np.asarray(model.get("x_mean") or [], dtype=np.float64)
    x_scale = np.asarray(model.get("x_scale") or [], dtype=np.float64)
    if coef.size != x.shape[1] or x_mean.size != x.shape[1] or x_scale.size != x.shape[1]:
        return np.full(x.shape[0], 0.5, dtype=np.float64)
    scaled = (x - x_mean) / np.where(x_scale > 1e-6, x_scale, 1.0)
    logits = np.clip((scaled @ coef) + float(model.get("intercept", 0.0) or 0.0), -30.0, 30.0)
    return 1.0 / (1.0 + np.exp(-logits))


def _fit_crossfit_logistic_density_ratio(
    *,
    x: np.ndarray,
    y: np.ndarray,
    row_ids: list[int],
    prior_ratio: float,
    clip_min: float,
    clip_max: float,
) -> dict[str, Any]:
    fold_indices = _build_stratified_crossfit_folds(
        y=y,
        max_folds=DEFAULT_WEIGHTING_DENSITY_RATIO_CROSSFIT_FOLDS,
    )
    if len(fold_indices) < 2:
        return {"enabled": False}
    oof_probs = np.full(y.shape[0], np.nan, dtype=np.float64)
    for fold in fold_indices:
        train_mask = np.ones(y.shape[0], dtype=bool)
        train_mask[fold] = False
        y_train = y[train_mask]
        if int(np.sum(y_train > 0.5)) <= 0 or int(np.sum(y_train <= 0.5)) <= 0:
            return {"enabled": False}
        model = _fit_logistic_ratio_model(x=x[train_mask], y=y_train)
        oof_probs[fold] = _predict_logistic_ratio_probabilities(x=x[fold], model=model)
    if np.isnan(oof_probs).any():
        return {"enabled": False}
    odds = np.clip(oof_probs, 1e-6, 1.0 - 1e-6) / np.clip(1.0 - oof_probs, 1e-6, 1.0)
    raw_ratios = odds * float(prior_ratio)
    clipped = np.clip(raw_ratios, float(clip_min), float(clip_max))
    log_loss = -np.mean(
        (y * np.log(np.clip(oof_probs, 1e-6, 1.0 - 1e-6)))
        + ((1.0 - y) * np.log(np.clip(1.0 - oof_probs, 1e-6, 1.0 - 1e-6)))
    )
    return {
        "enabled": True,
        "fold_count": int(len(fold_indices)),
        "clip_fraction": float(np.mean(raw_ratios != clipped)) if raw_ratios.size > 0 else 0.0,
        "positive_probability_mean": float(np.mean(oof_probs[y > 0.5])) if np.any(y > 0.5) else None,
        "negative_probability_mean": float(np.mean(oof_probs[y <= 0.5])) if np.any(y <= 0.5) else None,
        "log_loss": float(log_loss),
        "ratio_by_row_id": {
            int(row_id): float(weight)
            for row_id, weight in zip(row_ids, clipped.tolist(), strict=False)
        },
    }


def _build_stratified_crossfit_folds(*, y: np.ndarray, max_folds: int) -> list[np.ndarray]:
    positive_indices = [int(index) for index in np.flatnonzero(y > 0.5)]
    negative_indices = [int(index) for index in np.flatnonzero(y <= 0.5)]
    fold_count = min(max(int(max_folds), 0), len(positive_indices), len(negative_indices))
    if fold_count < 2:
        return []
    folds: list[list[int]] = [[] for _ in range(fold_count)]
    for rank, index in enumerate(positive_indices):
        folds[rank % fold_count].append(int(index))
    for rank, index in enumerate(negative_indices):
        folds[rank % fold_count].append(int(index))
    return [np.asarray(sorted(indices), dtype=np.int64) for indices in folds if indices]


def _normalize_logistic_ratio_model(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload or {})
    normalized["coef"] = [float(value) for value in (normalized.get("coef") or []) if _safe_optional_float(value) is not None]
    normalized["intercept"] = float(normalized.get("intercept", 0.0) or 0.0)
    normalized["x_mean"] = [float(value) for value in (normalized.get("x_mean") or []) if _safe_optional_float(value) is not None]
    normalized["x_scale"] = [float(value) for value in (normalized.get("x_scale") or []) if _safe_optional_float(value) is not None]
    normalized["prior_ratio"] = float(normalized.get("prior_ratio", 1.0) or 1.0)
    return normalized


def _resolve_logistic_density_ratio_weight(*, row: dict[str, Any], contract: dict[str, Any]) -> float:
    model = _normalize_logistic_ratio_model(contract.get("model") if isinstance(contract.get("model"), dict) else {})
    feature_names = list(contract.get("feature_names") or [])
    if not feature_names or not model.get("coef"):
        return 1.0
    values: list[float] = []
    for feature_name in feature_names:
        value = _safe_optional_float(row.get(feature_name))
        if value is None:
            return 1.0
        values.append(float(value))
    x = np.asarray(values, dtype=np.float64)
    coef = np.asarray(model.get("coef") or [], dtype=np.float64)
    x_mean = np.asarray(model.get("x_mean") or [], dtype=np.float64)
    x_scale = np.asarray(model.get("x_scale") or [], dtype=np.float64)
    if coef.size != x.size or x_mean.size != x.size or x_scale.size != x.size:
        return 1.0
    scaled = (x - x_mean) / np.where(x_scale > 1e-6, x_scale, 1.0)
    logits = float(np.clip(float(model.get("intercept", 0.0) or 0.0) + float(np.dot(coef, scaled)), -30.0, 30.0))
    prob = 1.0 / (1.0 + math.exp(-logits))
    odds = prob / max(1.0 - prob, 1e-6)
    ratio = odds * float(model.get("prior_ratio", 1.0) or 1.0)
    clip_min = max(float(contract.get("clip_min", 1.0) or 1.0), 1e-6)
    clip_max = max(float(contract.get("clip_max", clip_min) or clip_min), clip_min)
    return float(min(max(ratio, clip_min), clip_max))


def _effective_sample_size(rows: list[dict[str, Any]]) -> float:
    weights = [max(float(item.get("weight", 0.0) or 0.0), 0.0) for item in rows]
    total = sum(weights)
    denom = sum(value * value for value in weights)
    if total <= 0.0 or denom <= 0.0:
        return 0.0
    return float((total * total) / denom)


def _weighted_indicator_rate(rows: list[dict[str, Any]], predicate: callable) -> float:
    weights = [max(float(item.get("weight", 0.0) or 0.0), 0.0) for item in rows]
    total = sum(weights)
    if total <= 0.0:
        return 1.0
    weighted_hits = sum(weight for item, weight in zip(rows, weights, strict=False) if predicate(item))
    return float(weighted_hits / total)


def _weighted_indicator_rate_from_values(rows: list[dict[str, Any]], values: list[float], predicate: callable) -> float:
    weights = [max(float(item.get("weight", 0.0) or 0.0), 0.0) for item in rows]
    total = sum(weights)
    if total <= 0.0:
        return 1.0
    weighted_hits = sum(weight for value, weight in zip(values, weights, strict=False) if predicate(value))
    return float(weighted_hits / total)


def _weighted_mean(rows: list[dict[str, Any]], *, key: str) -> float:
    weights = [max(float(item.get("weight", 0.0) or 0.0), 0.0) for item in rows]
    total = sum(weights)
    if total <= 0.0:
        return 0.0
    weighted_sum = sum(weight * float(item.get(key, 0.0) or 0.0) for item, weight in zip(rows, weights, strict=False))
    return float(weighted_sum / total)


def _median(values: list[float]) -> float:
    ordered = sorted(float(value) for value in values)
    size = len(ordered)
    if size <= 0:
        return 0.0
    mid = size // 2
    if size % 2 == 1:
        return float(ordered[mid])
    return float((ordered[mid - 1] + ordered[mid]) / 2.0)


def _stddev(values: list[float]) -> float:
    if not values:
        return 0.0
    mean_value = sum(float(value) for value in values) / float(len(values))
    variance = sum((float(value) - mean_value) ** 2 for value in values) / float(len(values))
    return float(math.sqrt(max(variance, 0.0)))


def _build_subgroup_results(
    *,
    selected_rows: list[dict[str, Any]],
    nonpositive_alpha: float,
    severe_loss_alpha: float,
    severe_threshold: float,
    delta: float,
    subgroup_min_coverage: int,
) -> list[dict[str, Any]]:
    grouped: dict[tuple[int, str], list[dict[str, Any]]] = {}
    for item in selected_rows:
        subgroup_bucket = item.get("subgroup_bucket")
        subgroup_label = str(item.get("subgroup_label", "")).strip()
        if subgroup_bucket is None or not subgroup_label:
            continue
        grouped.setdefault((int(subgroup_bucket), subgroup_label), []).append(item)
    results: list[dict[str, Any]] = []
    for (bucket_index, subgroup_label), rows in sorted(grouped.items(), key=lambda pair: pair[0][0]):
        coverage = len(rows)
        effective_sample_size = _effective_sample_size(rows)
        nonpositive_rate = _weighted_indicator_rate(rows, lambda item: float(item["realized_return"]) <= 0.0)
        severe_rate = _weighted_indicator_rate(
            rows,
            lambda item: float(item["realized_return"]) <= -float(severe_threshold),
        )
        nonpositive_ucb = _hoeffding_ucb(nonpositive_rate, effective_sample_size, delta)
        severe_ucb = _hoeffding_ucb(severe_rate, effective_sample_size, delta)
        if coverage < int(subgroup_min_coverage):
            status = "insufficient_coverage"
        elif nonpositive_ucb > float(nonpositive_alpha) or severe_ucb > float(severe_loss_alpha):
            status = "violated"
        else:
            status = "ok"
        results.append(
            {
                "bucket_index": int(bucket_index),
                "label": subgroup_label,
                "coverage": int(coverage),
                "effective_sample_size": float(effective_sample_size),
                "nonpositive_rate": float(nonpositive_rate),
                "nonpositive_rate_ucb": float(nonpositive_ucb),
                "severe_loss_rate": float(severe_rate),
                "severe_loss_rate_ucb": float(severe_ucb),
                "status": status,
            }
        )
    return results


def _normalize_subgroup_result(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "bucket_index": int(payload.get("bucket_index", 0) or 0),
        "label": str(payload.get("label", "")).strip(),
        "coverage": int(payload.get("coverage", 0) or 0),
        "effective_sample_size": float(payload.get("effective_sample_size", 0.0) or 0.0),
        "nonpositive_rate": float(payload.get("nonpositive_rate", 0.0) or 0.0),
        "nonpositive_rate_ucb": float(payload.get("nonpositive_rate_ucb", 0.0) or 0.0),
        "severe_loss_rate": float(payload.get("severe_loss_rate", 0.0) or 0.0),
        "severe_loss_rate_ucb": float(payload.get("severe_loss_rate_ucb", 0.0) or 0.0),
        "status": str(payload.get("status", "")).strip(),
    }


def _resolve_subgroup_feature_name(decision_rows: list[dict[str, Any]]) -> str:
    for item in decision_rows:
        name = str(item.get("risk_feature_name", "")).strip()
        if name:
            return name
    return "risk_feature_value"


def _resolve_live_subgroup_info(
    *,
    subgroup_family: dict[str, Any],
    trade_action: dict[str, Any] | None,
) -> dict[str, Any]:
    family = dict(subgroup_family or {})
    if not bool(family.get("enabled", False)):
        return {
            "subgroup_feature_name": "",
            "subgroup_value": None,
            "subgroup_bucket": None,
            "subgroup_label": "",
        }
    trade_action_payload = dict(trade_action or {})
    subgroup_value = _safe_optional_float(trade_action_payload.get("risk_feature_value"))
    bounds = [float(value) for value in (family.get("bounds") or []) if _safe_optional_float(value) is not None]
    if subgroup_value is None or len(bounds) < 2:
        return {
            "subgroup_feature_name": str(family.get("feature_name", "")).strip(),
            "subgroup_value": subgroup_value,
            "subgroup_bucket": None,
            "subgroup_label": "",
        }
    bucket_index = _resolve_bucket_index(float(subgroup_value), bounds)
    return {
        "subgroup_feature_name": str(family.get("feature_name", "")).strip(),
        "subgroup_value": float(subgroup_value),
        "subgroup_bucket": int(bucket_index),
        "subgroup_label": _subgroup_label(
            feature_name=str(family.get("feature_name", "")).strip(),
            bucket_index=int(bucket_index),
        ),
    }


def _resolve_bucket_index(value: float, bounds: list[float]) -> int:
    if len(bounds) < 2:
        return 0
    last_bucket = max(len(bounds) - 2, 0)
    for index in range(last_bucket):
        upper = float(bounds[index + 1])
        if float(value) < upper:
            return index
    return last_bucket


def _subgroup_label(*, feature_name: str, bucket_index: int) -> str:
    return f"{str(feature_name).strip()}:q{int(bucket_index)}"


def _quantile_bounds(values: list[float], bucket_count: int) -> list[float]:
    if not values:
        return [0.0, 1.0]
    sorted_values = sorted(float(value) for value in values)
    requested = max(int(bucket_count), 1)
    bounds = [sorted_values[0]]
    for bucket_index in range(1, requested):
        quantile = float(bucket_index) / float(requested)
        pos = quantile * float(len(sorted_values) - 1)
        lower_index = int(math.floor(pos))
        upper_index = int(math.ceil(pos))
        if lower_index == upper_index:
            value = sorted_values[lower_index]
        else:
            weight = pos - float(lower_index)
            value = (1.0 - weight) * sorted_values[lower_index] + weight * sorted_values[upper_index]
        if value > bounds[-1]:
            bounds.append(float(value))
    upper = sorted_values[-1]
    if upper <= bounds[-1]:
        upper = bounds[-1] + 1e-9
    bounds.append(float(upper))
    if len(bounds) < 2:
        return [sorted_values[0], sorted_values[-1] + 1e-9]
    return bounds


def _hoeffding_ucb(empirical_rate: float, sample_count: int, delta: float) -> float:
    if sample_count <= 0:
        return 1.0
    rate = min(max(float(empirical_rate), 0.0), 1.0)
    bonus = math.sqrt(max(math.log(1.0 / max(float(delta), 1e-12)), 0.0) / (2.0 * float(sample_count)))
    return min(rate + bonus, 1.0)


def _clamp_probability(value: float, *, default: float) -> float:
    if value is None:
        return float(default)
    try:
        return min(max(float(value), 1e-6), 1.0)
    except (TypeError, ValueError):
        return float(default)


def _safe_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def _safe_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
