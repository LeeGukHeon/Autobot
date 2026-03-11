"""Research-lane acceptance helpers for multi-metric offline comparison."""

from __future__ import annotations

from typing import Any

from .economic_objective import compare_v4_profiled_pareto, resolve_v4_execution_compare_contract
from .execution_validation import build_execution_validation_summary, compare_execution_validation_folds


def compare_balanced_pareto(
    candidate_summary: dict[str, Any] | None,
    champion_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    return compare_v4_profiled_pareto(
        candidate_summary,
        champion_summary,
        context="offline_compare",
    )


def summarize_walk_forward_windows(
    windows: list[dict[str, Any]],
    *,
    threshold_key: str = "top_5pct",
) -> dict[str, Any]:
    if not windows:
        return {
            "windows_run": 0,
            "selected_threshold_key": str(threshold_key).strip() or "top_5pct",
            "precision_selected_mean": 0.0,
            "ev_net_selected_mean": 0.0,
            "precision_top5_mean": 0.0,
            "ev_net_top5_mean": 0.0,
            "pr_auc_mean": 0.0,
            "roc_auc_mean": 0.0,
            "log_loss_mean": 0.0,
            "brier_score_mean": 0.0,
            "positive_window_ratio": 0.0,
        }

    resolved_threshold_key = str(threshold_key).strip() or "top_5pct"
    precision_values = [_safe_float(_trading_metric(window, resolved_threshold_key, "precision")) for window in windows]
    ev_values = [_safe_float(_trading_metric(window, resolved_threshold_key, "ev_net")) for window in windows]
    pr_auc_values = [_safe_float(_nested(window, "metrics", "classification", "pr_auc")) for window in windows]
    roc_auc_values = [_safe_float(_nested(window, "metrics", "classification", "roc_auc")) for window in windows]
    log_loss_values = [_safe_float(_nested(window, "metrics", "classification", "log_loss")) for window in windows]
    brier_values = [_safe_float(_nested(window, "metrics", "classification", "brier_score")) for window in windows]
    positive_windows = sum(1 for value in ev_values if value > 0.0)

    count = float(len(windows))
    return {
        "windows_run": int(len(windows)),
        "selected_threshold_key": resolved_threshold_key,
        "precision_selected_mean": sum(precision_values) / count,
        "ev_net_selected_mean": sum(ev_values) / count,
        "precision_top5_mean": sum(precision_values) / count,
        "ev_net_top5_mean": sum(ev_values) / count,
        "pr_auc_mean": sum(pr_auc_values) / count,
        "roc_auc_mean": sum(roc_auc_values) / count,
        "log_loss_mean": sum(log_loss_values) / count,
        "brier_score_mean": sum(brier_values) / count,
        "positive_window_ratio": float(positive_windows) / count,
    }


def compare_execution_balanced_pareto(
    candidate_summary: dict[str, Any] | None,
    champion_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    candidate_payload = dict(candidate_summary or {})
    champion_payload = dict(champion_summary or {})
    summary_compare = compare_v4_profiled_pareto(
        candidate_payload,
        champion_payload,
        context="execution_compare",
    )
    contract = resolve_v4_execution_compare_contract()
    candidate_validation = build_execution_validation_summary(
        candidate_payload,
        window_minutes=int(contract.get("validation_window_minutes", 60) or 60),
        fold_count=int(contract.get("validation_fold_count", 6) or 6),
        min_active_windows=int(contract.get("validation_min_active_windows", 12) or 12),
        target_return=float(contract.get("validation_target_return", 0.0) or 0.0),
        lpm_order=int(contract.get("validation_lpm_order", 2) or 2),
    )
    champion_validation = build_execution_validation_summary(
        champion_payload,
        window_minutes=int(contract.get("validation_window_minutes", 60) or 60),
        fold_count=int(contract.get("validation_fold_count", 6) or 6),
        min_active_windows=int(contract.get("validation_min_active_windows", 12) or 12),
        target_return=float(contract.get("validation_target_return", 0.0) or 0.0),
        lpm_order=int(contract.get("validation_lpm_order", 2) or 2),
    )
    validation_compare = compare_execution_validation_folds(
        candidate_validation,
        champion_validation,
        alpha=float(contract.get("validation_alpha", 0.10) or 0.10),
    )
    result = dict(summary_compare)
    result["policy"] = str(contract.get("policy", "")).strip() or str(result.get("policy", "")).strip()
    result["decision_order"] = list(contract.get("decision_order") or result.get("decision_order") or [])
    result["execution_validation_method"] = str(contract.get("validation_method", "")).strip()
    result["execution_validation"] = {
        "candidate": candidate_validation,
        "champion": champion_validation,
        "compare": validation_compare,
    }
    result["utility_score"] = float(validation_compare.get("mean_diff", result.get("utility_score", 0.0)) or 0.0)

    if not bool(summary_compare.get("comparable")):
        result["comparable"] = False
        return result
    if not bool(validation_compare.get("comparable")):
        legacy_summary_only = all(
            not str(payload.get("run_dir", "")).strip() and not isinstance(payload.get("execution_validation"), dict)
            for payload in (candidate_payload, champion_payload)
        )
        if legacy_summary_only:
            result["comparable"] = bool(summary_compare.get("comparable"))
            result["decision"] = str(summary_compare.get("decision", "")).strip() or "indeterminate"
            result["reasons"] = [
                *[str(item).strip() for item in (summary_compare.get("reasons") or []) if str(item).strip()],
                "LEGACY_SUMMARY_ONLY_FALLBACK",
            ]
            return result
        reasons = [
            *[str(item).strip() for item in (summary_compare.get("reasons") or []) if str(item).strip()],
            *[str(item).strip() for item in (validation_compare.get("reasons") or []) if str(item).strip()],
        ]
        result["comparable"] = False
        result["decision"] = "insufficient_evidence"
        result["reasons"] = reasons or ["INSUFFICIENT_EXECUTION_VALIDATION"]
        return result

    summary_candidate_dominates = bool(summary_compare.get("candidate_dominates"))
    summary_champion_dominates = bool(summary_compare.get("champion_dominates"))
    validation_decision = str(validation_compare.get("decision", "")).strip().lower()
    if summary_candidate_dominates and validation_decision == "champion_edge":
        result["decision"] = "indeterminate"
        result["reasons"] = ["SUMMARY_VALIDATION_CONFLICT"]
    elif summary_champion_dominates and validation_decision == "candidate_edge":
        result["decision"] = "indeterminate"
        result["reasons"] = ["SUMMARY_VALIDATION_CONFLICT"]
    elif summary_candidate_dominates and validation_decision == "candidate_edge":
        result["decision"] = "candidate_edge"
        result["reasons"] = ["PARETO_DOMINANCE", "DOWNSIDE_VALIDATED_CV_PASS"]
    elif summary_champion_dominates and validation_decision == "champion_edge":
        result["decision"] = "champion_edge"
        result["reasons"] = ["CHAMPION_PARETO_DOMINANCE", "DOWNSIDE_VALIDATED_CV_FAIL"]
    else:
        result["decision"] = str(validation_compare.get("decision", "")).strip() or "indeterminate"
        result["reasons"] = [
            str(item).strip() for item in (validation_compare.get("reasons") or []) if str(item).strip()
        ] or ["DOWNSIDE_VALIDATED_CV_HOLD"]
    result["comparable"] = True
    return result


def compare_spa_like_window_test(
    candidate_windows: list[dict[str, Any]] | None,
    champion_windows: list[dict[str, Any]] | None,
    *,
    candidate_threshold_key: str = "top_5pct",
    champion_threshold_key: str | None = None,
    alpha: float = 0.20,
) -> dict[str, Any]:
    candidate_map = {
        int(window.get("window_index", -1)): window
        for window in (candidate_windows or [])
        if isinstance(window, dict) and int(window.get("window_index", -1)) >= 0
    }
    champion_map = {
        int(window.get("window_index", -1)): window
        for window in (champion_windows or [])
        if isinstance(window, dict) and int(window.get("window_index", -1)) >= 0
    }
    common_indices = sorted(set(candidate_map).intersection(champion_map))
    if len(common_indices) < 2:
        return {
            "policy": "spa_like_window_ev_net",
            "comparable": False,
            "decision": "insufficient_evidence",
            "reasons": ["INSUFFICIENT_COMMON_WINDOWS"],
            "window_count": int(len(common_indices)),
            "alpha": float(alpha),
            "candidate_threshold_key": str(candidate_threshold_key).strip() or "top_5pct",
            "champion_threshold_key": str(champion_threshold_key or candidate_threshold_key).strip() or "top_5pct",
        }

    candidate_key = str(candidate_threshold_key).strip() or "top_5pct"
    champion_key = str(champion_threshold_key or candidate_threshold_key).strip() or "top_5pct"
    diffs: list[float] = []
    for index in common_indices:
        candidate_value = _safe_float(_trading_metric(candidate_map[index], candidate_key, "ev_net"))
        champion_value = _safe_float(_trading_metric(champion_map[index], champion_key, "ev_net"))
        diffs.append(candidate_value - champion_value)

    mean_diff = sum(diffs) / float(len(diffs))
    positive_ratio = float(sum(1 for value in diffs if value > 0.0)) / float(len(diffs))
    p_value_upper = _exact_sign_flip_tail_probability(diffs, upper_tail=True)
    p_value_lower = _exact_sign_flip_tail_probability(diffs, upper_tail=False)
    t_stat = _studentized_mean(diffs)

    if mean_diff > 0.0 and p_value_upper <= float(alpha):
        decision = "candidate_edge"
        reasons = ["SPA_LIKE_PASS"]
    elif mean_diff < 0.0 and p_value_lower <= float(alpha):
        decision = "champion_edge"
        reasons = ["SPA_LIKE_FAIL"]
    else:
        decision = "indeterminate"
        reasons = ["SPA_LIKE_HOLD"]

    return {
        "policy": "spa_like_window_ev_net",
        "comparable": True,
        "decision": decision,
        "reasons": reasons,
        "alpha": float(alpha),
        "window_count": int(len(common_indices)),
        "mean_diff_ev_net": float(mean_diff),
        "positive_diff_ratio": float(positive_ratio),
        "p_value_upper": float(p_value_upper),
        "p_value_lower": float(p_value_lower),
        "studentized_mean_t": float(t_stat),
        "window_indices": common_indices,
        "candidate_threshold_key": candidate_key,
        "champion_threshold_key": champion_key,
    }


def _normalized_advantage(candidate: float, champion: float, *, higher_is_better: bool) -> float:
    scale = max(abs(candidate), abs(champion), 1e-9)
    if higher_is_better:
        raw = (candidate - champion) / scale
    else:
        raw = (champion - candidate) / scale
    return max(min(float(raw), 1.0), -1.0)


def _summary_metric(payload: dict[str, Any], key: str, legacy_key: str | None) -> float:
    if key in payload:
        return _safe_float(payload.get(key))
    if legacy_key and legacy_key in payload:
        return _safe_float(payload.get(legacy_key))
    return 0.0


def _trading_metric(payload: dict[str, Any] | None, threshold_key: str, metric_name: str) -> Any:
    trading = _nested(payload, "metrics", "trading")
    if isinstance(trading, dict):
        entry = trading.get(threshold_key)
        if isinstance(entry, dict) and metric_name in entry:
            return entry.get(metric_name)
        fallback = trading.get("top_5pct")
        if isinstance(fallback, dict):
            return fallback.get(metric_name)
    return None


def _nested(payload: dict[str, Any] | None, *keys: str) -> Any:
    node: Any = payload or {}
    for key in keys:
        if not isinstance(node, dict):
            return None
        node = node.get(key)
    return node


def _safe_float(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except Exception:
        return 0.0


def _studentized_mean(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean_value = sum(values) / float(len(values))
    variance = sum((float(value) - mean_value) ** 2 for value in values) / float(len(values) - 1)
    std_value = variance ** 0.5
    if std_value <= 0.0:
        return 0.0 if mean_value == 0.0 else float("inf")
    return float(mean_value / (std_value / (float(len(values)) ** 0.5)))


def _exact_sign_flip_tail_probability(values: list[float], *, upper_tail: bool) -> float:
    if not values:
        return 1.0
    observed = sum(values) / float(len(values))
    total = 1 << len(values)
    extreme = 0
    for mask in range(total):
        signed_total = 0.0
        for index, value in enumerate(values):
            sign = -1.0 if ((mask >> index) & 1) else 1.0
            signed_total += sign * float(value)
        candidate_mean = signed_total / float(len(values))
        if upper_tail:
            if candidate_mean >= observed - 1e-12:
                extreme += 1
        else:
            if candidate_mean <= observed + 1e-12:
                extreme += 1
    return float(extreme) / float(total)
