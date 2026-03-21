from __future__ import annotations

import copy
from typing import Any


_V4_SHARED_ECONOMIC_OBJECTIVE_PROFILE: dict[str, Any] = {
    "version": 3,
    "policy": "v4_shared_economic_objective_contract",
    "profile_id": "v4_shared_economic_objective_v3",
    "objective_family": "economic_return_first",
    "principles": [
        "prefer realized economic edge before secondary classifier quality metrics",
        "use the same declared profile across trainer sweep, walk-forward selection, research compare, and promotion compare",
        "when one lane cannot support the full profile, declare the reduced context explicitly",
        "when comparing runtime exit policies, evaluate realized return and downside protection before execution-friction tie-breaks",
    ],
    "trainer_sweep": {
        "task_profiles": {
            "cls": {
                "primary_metric_order": ["ev_net_top5", "precision_top5", "pr_auc", "roc_auc"],
                "admissibility_constraints": [],
            },
            "reg": {
                "primary_metric_order": ["ev_net_top5", "precision_top5", "pr_auc", "roc_auc"],
                "admissibility_constraints": [],
            },
            "rank": {
                "primary_metric_order": ["ev_net_top5", "ndcg_at_5_mean", "top1_match_rate"],
                "admissibility_constraints": ["requires_ts_grouped_ranking_metrics"],
            },
        }
    },
    "walk_forward_selection": {
        "primary_metric_order": ["objective_score"],
        "tie_break_order": [
            "feasible_window_ratio",
            "positive_active_ts_ratio_mean",
            "active_ts_ratio_mean",
            "selected_rows_mean",
            "lower_top_pct",
        ],
        "admissibility_constraints": [
            "prefer_grid_points_with_feasible_window_ratio_at_or_above_0_5",
            "fallback_to_best_available_grid_point_if_no_feasible_point_exists",
        ],
    },
    "offline_compare": {
        "policy": "balanced_pareto_offline",
        "higher_is_better": [
            "ev_net_selected_mean",
            "precision_selected_mean",
            "pr_auc_mean",
            "positive_window_ratio",
        ],
        "lower_is_better": [
            "log_loss_mean",
            "brier_score_mean",
        ],
        "utility_aggregation": "mean_normalized_advantage",
        "decision_order": ["pareto_domination", "utility_tie_break"],
        "admissibility_constraints": ["candidate_summary_required", "champion_summary_required"],
    },
    "execution_compare": {
        "policy": "paired_sortino_lpm_execution_v1",
        "higher_is_better": [
            "realized_pnl_quote",
            "fill_rate",
        ],
        "lower_is_better": [
            "max_drawdown_pct",
            "slippage_bps_mean",
        ],
        "primary_higher_is_better": ["realized_pnl_quote"],
        "primary_lower_is_better": ["max_drawdown_pct"],
        "implementation_higher_is_better": ["fill_rate"],
        "implementation_lower_is_better": ["slippage_bps_mean"],
        "utility_aggregation": "summary_pareto_for_audit_only",
        "decision_order": [
            "pareto_domination",
            "downside_validated_kfold_compare",
            "summary_audit_tie_break",
        ],
        "validation_method": "rolling_window_sortino_lpm_cv_v1",
        "validation_window_minutes": 60,
        "validation_fold_count": 6,
        "validation_min_active_windows": 12,
        "validation_alpha": 0.10,
        "validation_target_return": 0.0,
        "validation_lpm_order": 2,
        "admissibility_constraints": [
            "candidate_summary_required",
            "champion_summary_required",
            "candidate_orders_filled_gt_zero",
            "champion_orders_filled_gt_zero",
            "candidate_execution_validation_required",
            "champion_execution_validation_required",
        ],
    },
    "promotion_compare": {
        "policy": "balanced_pareto_calmar_gate",
        "pareto_higher_is_better": [
            "realized_pnl_quote",
            "fill_rate",
        ],
        "pareto_lower_is_better": [
            "max_drawdown_pct",
            "slippage_bps_mean",
        ],
        "utility_metric": "calmar_like",
        "decision_order": [
            "candidate_min_orders",
            "candidate_min_realized_pnl",
            "candidate_min_deflated_sharpe_ratio",
            "pareto_domination",
            "strict_pnl_guard",
            "utility_tie_break",
            "stability_override",
        ],
        "admissibility_constraints": [
            "candidate_min_orders_filled",
            "candidate_min_realized_pnl_quote",
            "candidate_min_deflated_sharpe_ratio",
        ],
        "threshold_defaults": {
            "candidate_min_orders_filled": 30,
            "candidate_min_realized_pnl_quote": 0.0,
            "candidate_min_deflated_sharpe_ratio": 0.20,
            "candidate_min_pnl_delta_vs_champion": 0.0,
            "champion_min_drawdown_improvement_pct": 0.10,
        },
        "policy_variants": {
            "balanced_pareto": {
                "allow_stability_override": True,
                "champion_pnl_tolerance_pct": 0.05,
                "champion_max_fill_rate_degradation": 0.02,
                "champion_max_slippage_deterioration_bps": 2.5,
                "champion_min_utility_edge_pct": 0.0,
                "use_pareto": True,
                "use_utility_tie_break": True,
                "backtest_compare_required": True,
                "paper_final_gate": False,
            },
            "strict": {
                "allow_stability_override": False,
                "champion_pnl_tolerance_pct": 0.0,
                "champion_max_fill_rate_degradation": 0.0,
                "champion_max_slippage_deterioration_bps": 0.0,
                "champion_min_utility_edge_pct": 0.0,
                "use_pareto": False,
                "use_utility_tie_break": False,
                "backtest_compare_required": True,
                "paper_final_gate": False,
            },
            "conservative_pareto": {
                "allow_stability_override": True,
                "champion_pnl_tolerance_pct": 0.02,
                "champion_max_fill_rate_degradation": 0.01,
                "champion_max_slippage_deterioration_bps": 1.0,
                "champion_min_utility_edge_pct": 0.05,
                "use_pareto": True,
                "use_utility_tie_break": True,
                "backtest_compare_required": True,
                "paper_final_gate": False,
            },
            "paper_final_balanced": {
                "allow_stability_override": True,
                "champion_pnl_tolerance_pct": 0.05,
                "champion_max_fill_rate_degradation": 0.02,
                "champion_max_slippage_deterioration_bps": 2.5,
                "champion_min_utility_edge_pct": 0.0,
                "use_pareto": True,
                "use_utility_tie_break": True,
                "backtest_compare_required": False,
                "paper_final_gate": True,
            },
        },
    },
}


def build_v4_shared_economic_objective_profile() -> dict[str, Any]:
    return copy.deepcopy(_V4_SHARED_ECONOMIC_OBJECTIVE_PROFILE)


def resolve_v4_promotion_compare_contract(
    policy_name: str | None,
    *,
    overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    profile = build_v4_shared_economic_objective_profile()
    promotion_compare = dict(profile.get("promotion_compare") or {})
    threshold_defaults = dict(promotion_compare.get("threshold_defaults") or {})
    policy_variants = dict(promotion_compare.get("policy_variants") or {})
    requested = str(policy_name or "").strip() or "balanced_pareto"
    effective = requested if requested in policy_variants else "balanced_pareto"
    resolved: dict[str, Any] = {
        "profile_id": str(profile.get("profile_id", "")).strip(),
        "policy_name_requested": requested,
        "policy_name_effective": effective,
        "threshold_source": "economic_objective_profile",
        "cli_override_keys": [],
    }
    resolved.update(threshold_defaults)
    resolved.update(dict(policy_variants.get(effective) or {}))
    override_keys: list[str] = []
    for key, value in dict(overrides or {}).items():
        if key not in resolved or value is None:
            continue
        resolved[key] = value
        override_keys.append(str(key))
    resolved["cli_override_keys"] = override_keys
    return resolved


def resolve_v4_execution_compare_contract() -> dict[str, Any]:
    profile = build_v4_shared_economic_objective_profile()
    execution_compare = dict(profile.get("execution_compare") or {})
    return {
        "profile_id": str(profile.get("profile_id", "")).strip(),
        "policy": str(execution_compare.get("policy", "")).strip() or "paired_sortino_lpm_execution_v1",
        "validation_method": str(execution_compare.get("validation_method", "")).strip()
        or "rolling_window_sortino_lpm_cv_v1",
        "validation_window_minutes": max(int(execution_compare.get("validation_window_minutes", 60) or 60), 1),
        "validation_fold_count": max(int(execution_compare.get("validation_fold_count", 6) or 6), 2),
        "validation_min_active_windows": max(
            int(execution_compare.get("validation_min_active_windows", 12) or 12),
            2,
        ),
        "validation_alpha": min(max(float(execution_compare.get("validation_alpha", 0.10) or 0.10), 0.0), 1.0),
        "validation_target_return": float(execution_compare.get("validation_target_return", 0.0) or 0.0),
        "validation_lpm_order": max(int(execution_compare.get("validation_lpm_order", 2) or 2), 1),
        "decision_order": list(execution_compare.get("decision_order") or []),
    }


def build_v4_trainer_sweep_sort_key(metrics: dict[str, Any] | None, *, task: str) -> tuple[float, ...]:
    profile = build_v4_shared_economic_objective_profile()
    task_profile = (
        (((profile.get("trainer_sweep") or {}).get("task_profiles") or {}).get(str(task).strip().lower()))
        or (((profile.get("trainer_sweep") or {}).get("task_profiles") or {}).get("cls"))
        or {}
    )
    metric_order = tuple(task_profile.get("primary_metric_order") or ())
    return tuple(_metric_value_from_metrics(metrics, name) for name in metric_order)


def build_v4_walk_forward_grid_sort_key(row: dict[str, Any] | None) -> tuple[float, ...]:
    row = dict(row or {})
    return (
        _safe_float(row.get("objective_score")),
        _safe_float(row.get("feasible_window_ratio")),
        _safe_float(row.get("positive_active_ts_ratio_mean")),
        _safe_float(row.get("active_ts_ratio_mean")),
        _safe_float(row.get("selected_rows_mean")),
        -_safe_float(row.get("top_pct"), 1.0),
    )


def build_v4_walk_forward_threshold_sort_key(row: dict[str, Any] | None) -> tuple[float, ...]:
    row = dict(row or {})
    return (
        _safe_float(row.get("feasible_window_ratio")),
        _safe_float(row.get("active_ts_ratio_mean")),
        _safe_float(row.get("positive_active_ts_ratio_mean")),
        _safe_float(row.get("objective_score")),
        _safe_float(row.get("selected_rows_mean")),
        0.0 if bool(row.get("fallback_used")) else 1.0,
    )


def compare_v4_profiled_pareto(
    candidate_summary: dict[str, Any] | None,
    champion_summary: dict[str, Any] | None,
    *,
    context: str,
) -> dict[str, Any]:
    profile = build_v4_shared_economic_objective_profile()
    context_doc = dict((profile.get(context) or {}))
    candidate = dict(candidate_summary or {})
    champion = dict(champion_summary or {})
    if not candidate or not champion:
        missing = []
        if not candidate:
            missing.append("candidate_summary")
        if not champion:
            missing.append("champion_summary")
        primary_metric_order = _metric_partition(
            context_doc,
            kind="primary",
        )
        implementation_metric_order = _metric_partition(
            context_doc,
            kind="implementation",
        )
        return {
            "policy": str(context_doc.get("policy", "")).strip() or str(context).strip() or "unknown",
            "comparable": False,
            "reasons": [f"MISSING_{name.upper()}" for name in missing],
            "candidate_dominates": False,
            "champion_dominates": False,
            "utility_score": 0.0,
            "utility_components": {},
            "primary_utility_score": 0.0,
            "primary_utility_components": {},
            "implementation_utility_score": 0.0,
            "implementation_utility_components": {},
            "all_metric_utility_score": 0.0,
            "decision": "insufficient_evidence",
            "economic_objective_profile_id": str(profile.get("profile_id", "")).strip(),
            "economic_objective_context": str(context).strip(),
            "metric_order": {
                "higher_is_better": list(context_doc.get("higher_is_better") or []),
                "lower_is_better": list(context_doc.get("lower_is_better") or []),
            },
            "primary_metric_order": primary_metric_order,
            "implementation_metric_order": implementation_metric_order,
            "admissibility_constraints": list(context_doc.get("admissibility_constraints") or []),
        }

    if str(context).strip() == "execution_compare":
        candidate_fills = int(candidate.get("orders_filled", 0) or 0)
        champion_fills = int(champion.get("orders_filled", 0) or 0)
        reasons: list[str] = []
        if candidate_fills <= 0:
            reasons.append("CANDIDATE_NO_FILLS")
        if champion_fills <= 0:
            reasons.append("CHAMPION_NO_FILLS")
        if reasons:
            primary_metric_order = _metric_partition(
                context_doc,
                kind="primary",
            )
            implementation_metric_order = _metric_partition(
                context_doc,
                kind="implementation",
            )
            return {
                "policy": str(context_doc.get("policy", "")).strip() or "paired_sortino_lpm_execution_v1",
                "comparable": False,
                "reasons": reasons,
                "candidate_dominates": False,
                "champion_dominates": False,
                "utility_score": 0.0,
                "utility_components": {},
                "primary_utility_score": 0.0,
                "primary_utility_components": {},
                "implementation_utility_score": 0.0,
                "implementation_utility_components": {},
                "all_metric_utility_score": 0.0,
                "decision": "insufficient_evidence",
                "economic_objective_profile_id": str(profile.get("profile_id", "")).strip(),
                "economic_objective_context": str(context).strip(),
                "metric_order": {
                    "higher_is_better": list(context_doc.get("higher_is_better") or []),
                    "lower_is_better": list(context_doc.get("lower_is_better") or []),
                },
                "primary_metric_order": primary_metric_order,
                "implementation_metric_order": implementation_metric_order,
                "admissibility_constraints": list(context_doc.get("admissibility_constraints") or []),
            }

    higher_metrics = tuple(str(item).strip() for item in (context_doc.get("higher_is_better") or []) if str(item).strip())
    lower_metrics = tuple(str(item).strip() for item in (context_doc.get("lower_is_better") or []) if str(item).strip())
    candidate_dominates = True
    champion_dominates = True
    strict_candidate_edge = False
    strict_champion_edge = False
    utility_components: dict[str, float] = {}
    deltas: dict[str, float] = {}

    for key in higher_metrics:
        cand = _summary_metric(candidate, key)
        champ = _summary_metric(champion, key)
        deltas[key] = cand - champ
        utility_components[key] = _normalized_advantage(cand, champ, higher_is_better=True)
        if cand < champ:
            candidate_dominates = False
        if cand > champ:
            strict_candidate_edge = True
            champion_dominates = False
        elif cand < champ:
            strict_champion_edge = True

    for key in lower_metrics:
        cand = _summary_metric(candidate, key)
        champ = _summary_metric(champion, key)
        deltas[key] = champ - cand
        utility_components[key] = _normalized_advantage(cand, champ, higher_is_better=False)
        if cand > champ:
            candidate_dominates = False
        if cand < champ:
            strict_candidate_edge = True
            champion_dominates = False
        elif cand > champ:
            strict_champion_edge = True

    candidate_dominates = candidate_dominates and strict_candidate_edge
    champion_dominates = champion_dominates and strict_champion_edge
    all_metric_utility_score = _mean_mapping_values(utility_components)
    primary_metric_order = _metric_partition(
        context_doc,
        kind="primary",
        fallback_higher=higher_metrics,
        fallback_lower=lower_metrics,
    )
    implementation_metric_order = _metric_partition(
        context_doc,
        kind="implementation",
    )
    use_partitioned_execution_utility = any(
        bool(context_doc.get(key))
        for key in (
            "primary_higher_is_better",
            "primary_lower_is_better",
            "implementation_higher_is_better",
            "implementation_lower_is_better",
        )
    )
    primary_utility_components = _subset_metric_components(utility_components, primary_metric_order)
    implementation_utility_components = _subset_metric_components(utility_components, implementation_metric_order)
    primary_utility_score = (
        _mean_mapping_values(primary_utility_components) if use_partitioned_execution_utility else all_metric_utility_score
    )
    implementation_utility_score = _mean_mapping_values(implementation_utility_components)
    utility_score = primary_utility_score if use_partitioned_execution_utility else all_metric_utility_score

    if candidate_dominates and not champion_dominates:
        decision = "candidate_edge"
        reasons = ["PARETO_DOMINANCE"]
    elif champion_dominates and not candidate_dominates:
        decision = "champion_edge"
        reasons = ["CHAMPION_PARETO_DOMINANCE"]
    elif use_partitioned_execution_utility and primary_utility_score > 0.0:
        decision = "candidate_edge"
        reasons = ["PRIMARY_RETURN_DOWNSIDE_UTILITY_PASS"]
    elif use_partitioned_execution_utility and primary_utility_score < 0.0:
        decision = "champion_edge"
        reasons = ["PRIMARY_RETURN_DOWNSIDE_UTILITY_FAIL"]
    elif use_partitioned_execution_utility and implementation_utility_score > 0.0:
        decision = "candidate_edge"
        reasons = ["IMPLEMENTATION_TIE_BREAK_PASS"]
    elif use_partitioned_execution_utility and implementation_utility_score < 0.0:
        decision = "champion_edge"
        reasons = ["IMPLEMENTATION_TIE_BREAK_FAIL"]
    elif utility_score > 0.0:
        decision = "candidate_edge"
        reasons = ["UTILITY_TIE_BREAK_PASS"]
    elif utility_score < 0.0:
        decision = "champion_edge"
        reasons = ["UTILITY_TIE_BREAK_FAIL"]
    else:
        decision = "indeterminate"
        reasons = (
            ["PRIMARY_AND_IMPLEMENTATION_UTILITY_FLAT"]
            if use_partitioned_execution_utility
            else ["UTILITY_TIE_BREAK_FLAT"]
        )

    return {
        "policy": str(context_doc.get("policy", "")).strip() or str(context).strip() or "unknown",
        "comparable": True,
        "candidate_dominates": bool(candidate_dominates),
        "champion_dominates": bool(champion_dominates),
        "utility_score": float(utility_score),
        "utility_components": utility_components,
        "primary_utility_score": float(primary_utility_score),
        "primary_utility_components": primary_utility_components,
        "implementation_utility_score": float(implementation_utility_score),
        "implementation_utility_components": implementation_utility_components,
        "all_metric_utility_score": float(all_metric_utility_score),
        "deltas": deltas,
        "decision": decision,
        "reasons": reasons,
        "economic_objective_profile_id": str(profile.get("profile_id", "")).strip(),
        "economic_objective_context": str(context).strip(),
        "metric_order": {
            "higher_is_better": list(higher_metrics),
            "lower_is_better": list(lower_metrics),
        },
        "primary_metric_order": primary_metric_order,
        "implementation_metric_order": implementation_metric_order,
        "utility_aggregation": str(context_doc.get("utility_aggregation", "")).strip() or "mean_normalized_advantage",
        "decision_order": list(context_doc.get("decision_order") or []),
        "admissibility_constraints": list(context_doc.get("admissibility_constraints") or []),
    }


def _metric_value_from_metrics(metrics: dict[str, Any] | None, name: str) -> float:
    payload = dict(metrics or {})
    trading = dict(payload.get("trading") or {})
    classification = dict(payload.get("classification") or {})
    ranking = dict(payload.get("ranking") or {})
    top5 = dict(trading.get("top_5pct") or {})
    normalized = str(name).strip().lower()
    if normalized == "ev_net_top5":
        return _safe_float(top5.get("ev_net"))
    if normalized == "precision_top5":
        return _safe_float(top5.get("precision"))
    if normalized == "pr_auc":
        return _safe_float(classification.get("pr_auc"))
    if normalized == "roc_auc":
        return _safe_float(classification.get("roc_auc"))
    if normalized == "ndcg_at_5_mean":
        return _safe_float(ranking.get("ndcg_at_5_mean"))
    if normalized == "top1_match_rate":
        return _safe_float(ranking.get("top1_match_rate"))
    return 0.0


def _summary_metric(summary: dict[str, Any], key: str) -> float:
    normalized = str(key).strip()
    if normalized == "ev_net_selected_mean":
        if key in summary:
            return _safe_float(summary.get(key))
        return _safe_float(summary.get("ev_net_top5_mean"))
    if normalized == "precision_selected_mean":
        if key in summary:
            return _safe_float(summary.get(key))
        return _safe_float(summary.get("precision_top5_mean"))
    return _safe_float(summary.get(key))


def _normalized_advantage(candidate_value: float, champion_value: float, *, higher_is_better: bool) -> float:
    left = float(candidate_value)
    right = float(champion_value)
    delta = (left - right) if higher_is_better else (right - left)
    scale = max(abs(left), abs(right), 1e-9)
    return float(delta / scale)


def _metric_partition(
    context_doc: dict[str, Any],
    *,
    kind: str,
    fallback_higher: tuple[str, ...] = (),
    fallback_lower: tuple[str, ...] = (),
) -> dict[str, list[str]]:
    prefix = str(kind).strip().lower()
    higher_metrics = [
        str(item).strip()
        for item in (context_doc.get(f"{prefix}_higher_is_better") or fallback_higher)
        if str(item).strip()
    ]
    lower_metrics = [
        str(item).strip()
        for item in (context_doc.get(f"{prefix}_lower_is_better") or fallback_lower)
        if str(item).strip()
    ]
    return {
        "higher_is_better": higher_metrics,
        "lower_is_better": lower_metrics,
    }


def _subset_metric_components(
    utility_components: dict[str, float],
    metric_order: dict[str, list[str]],
) -> dict[str, float]:
    names = [
        str(item).strip()
        for item in (
            list(metric_order.get("higher_is_better") or [])
            + list(metric_order.get("lower_is_better") or [])
        )
        if str(item).strip()
    ]
    return {name: float(utility_components.get(name, 0.0)) for name in names if name in utility_components}


def _mean_mapping_values(values: dict[str, float]) -> float:
    if not values:
        return 0.0
    return float(sum(float(value) for value in values.values()) / float(len(values)))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return float(default)
        return float(value)
    except (TypeError, ValueError):
        return float(default)
