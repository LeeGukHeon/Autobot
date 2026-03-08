"""Research-lane acceptance helpers for multi-metric offline comparison."""

from __future__ import annotations

from typing import Any


_HIGHER_IS_BETTER = (
    "ev_net_top5_mean",
    "precision_top5_mean",
    "pr_auc_mean",
    "positive_window_ratio",
)
_LOWER_IS_BETTER = (
    "log_loss_mean",
    "brier_score_mean",
)
_EXEC_HIGHER_IS_BETTER = (
    "realized_pnl_quote",
    "fill_rate",
)
_EXEC_LOWER_IS_BETTER = (
    "max_drawdown_pct",
    "slippage_bps_mean",
)


def compare_balanced_pareto(
    candidate_summary: dict[str, Any] | None,
    champion_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    candidate = dict(candidate_summary or {})
    champion = dict(champion_summary or {})
    if not candidate or not champion:
        missing = []
        if not candidate:
            missing.append("candidate_summary")
        if not champion:
            missing.append("champion_summary")
        return {
            "policy": "balanced_pareto_offline",
            "comparable": False,
            "reasons": [f"MISSING_{name.upper()}" for name in missing],
            "candidate_dominates": False,
            "champion_dominates": False,
            "utility_score": 0.0,
            "utility_components": {},
            "decision": "insufficient_evidence",
        }

    candidate_dominates = True
    champion_dominates = True
    strict_candidate_edge = False
    strict_champion_edge = False
    utility_components: dict[str, float] = {}
    deltas: dict[str, float] = {}

    for key in _HIGHER_IS_BETTER:
        cand = _safe_float(candidate.get(key))
        champ = _safe_float(champion.get(key))
        deltas[key] = cand - champ
        utility_components[key] = _normalized_advantage(cand, champ, higher_is_better=True)
        if cand < champ:
            candidate_dominates = False
        if cand > champ:
            strict_candidate_edge = True
            champion_dominates = False
        elif cand < champ:
            strict_champion_edge = True

    for key in _LOWER_IS_BETTER:
        cand = _safe_float(candidate.get(key))
        champ = _safe_float(champion.get(key))
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
    utility_score = sum(utility_components.values()) / float(len(utility_components)) if utility_components else 0.0

    if candidate_dominates and not champion_dominates:
        decision = "candidate_edge"
        reasons = ["PARETO_DOMINANCE"]
    elif champion_dominates and not candidate_dominates:
        decision = "champion_edge"
        reasons = ["CHAMPION_PARETO_DOMINANCE"]
    elif utility_score > 0.0:
        decision = "candidate_edge"
        reasons = ["UTILITY_TIE_BREAK_PASS"]
    elif utility_score < 0.0:
        decision = "champion_edge"
        reasons = ["UTILITY_TIE_BREAK_FAIL"]
    else:
        decision = "indeterminate"
        reasons = ["UTILITY_TIE_BREAK_FLAT"]

    return {
        "policy": "balanced_pareto_offline",
        "comparable": True,
        "candidate_dominates": bool(candidate_dominates),
        "champion_dominates": bool(champion_dominates),
        "utility_score": float(utility_score),
        "utility_components": utility_components,
        "deltas": deltas,
        "decision": decision,
        "reasons": reasons,
    }


def summarize_walk_forward_windows(windows: list[dict[str, Any]]) -> dict[str, Any]:
    if not windows:
        return {
            "windows_run": 0,
            "precision_top5_mean": 0.0,
            "ev_net_top5_mean": 0.0,
            "pr_auc_mean": 0.0,
            "roc_auc_mean": 0.0,
            "log_loss_mean": 0.0,
            "brier_score_mean": 0.0,
            "positive_window_ratio": 0.0,
        }

    precision_values = [_safe_float(_nested(window, "metrics", "trading", "top_5pct", "precision")) for window in windows]
    ev_values = [_safe_float(_nested(window, "metrics", "trading", "top_5pct", "ev_net")) for window in windows]
    pr_auc_values = [_safe_float(_nested(window, "metrics", "classification", "pr_auc")) for window in windows]
    roc_auc_values = [_safe_float(_nested(window, "metrics", "classification", "roc_auc")) for window in windows]
    log_loss_values = [_safe_float(_nested(window, "metrics", "classification", "log_loss")) for window in windows]
    brier_values = [_safe_float(_nested(window, "metrics", "classification", "brier_score")) for window in windows]
    positive_windows = sum(1 for value in ev_values if value > 0.0)

    count = float(len(windows))
    return {
        "windows_run": int(len(windows)),
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
    candidate = dict(candidate_summary or {})
    champion = dict(champion_summary or {})
    if not candidate or not champion:
        missing = []
        if not candidate:
            missing.append("candidate_summary")
        if not champion:
            missing.append("champion_summary")
        return {
            "policy": "balanced_pareto_execution",
            "comparable": False,
            "reasons": [f"MISSING_{name.upper()}" for name in missing],
            "candidate_dominates": False,
            "champion_dominates": False,
            "utility_score": 0.0,
            "utility_components": {},
            "decision": "insufficient_evidence",
        }

    candidate_fills = int(candidate.get("orders_filled", 0) or 0)
    champion_fills = int(champion.get("orders_filled", 0) or 0)
    reasons: list[str] = []
    if candidate_fills <= 0:
        reasons.append("CANDIDATE_NO_FILLS")
    if champion_fills <= 0:
        reasons.append("CHAMPION_NO_FILLS")
    if reasons:
        return {
            "policy": "balanced_pareto_execution",
            "comparable": False,
            "reasons": reasons,
            "candidate_dominates": False,
            "champion_dominates": False,
            "utility_score": 0.0,
            "utility_components": {},
            "decision": "insufficient_evidence",
        }

    candidate_dominates = True
    champion_dominates = True
    strict_candidate_edge = False
    strict_champion_edge = False
    utility_components: dict[str, float] = {}
    deltas: dict[str, float] = {}

    for key in _EXEC_HIGHER_IS_BETTER:
        cand = _safe_float(candidate.get(key))
        champ = _safe_float(champion.get(key))
        deltas[key] = cand - champ
        utility_components[key] = _normalized_advantage(cand, champ, higher_is_better=True)
        if cand < champ:
            candidate_dominates = False
        if cand > champ:
            strict_candidate_edge = True
            champion_dominates = False
        elif cand < champ:
            strict_champion_edge = True

    for key in _EXEC_LOWER_IS_BETTER:
        cand = _safe_float(candidate.get(key))
        champ = _safe_float(champion.get(key))
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
    utility_score = sum(utility_components.values()) / float(len(utility_components)) if utility_components else 0.0

    if candidate_dominates and not champion_dominates:
        decision = "candidate_edge"
        reasons = ["PARETO_DOMINANCE"]
    elif champion_dominates and not candidate_dominates:
        decision = "champion_edge"
        reasons = ["CHAMPION_PARETO_DOMINANCE"]
    elif utility_score > 0.0:
        decision = "candidate_edge"
        reasons = ["UTILITY_TIE_BREAK_PASS"]
    elif utility_score < 0.0:
        decision = "champion_edge"
        reasons = ["UTILITY_TIE_BREAK_FAIL"]
    else:
        decision = "indeterminate"
        reasons = ["UTILITY_TIE_BREAK_FLAT"]

    return {
        "policy": "balanced_pareto_execution",
        "comparable": True,
        "candidate_dominates": bool(candidate_dominates),
        "champion_dominates": bool(champion_dominates),
        "utility_score": float(utility_score),
        "utility_components": utility_components,
        "deltas": deltas,
        "decision": decision,
        "reasons": reasons,
    }


def compare_spa_like_window_test(
    candidate_windows: list[dict[str, Any]] | None,
    champion_windows: list[dict[str, Any]] | None,
    *,
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
        }

    diffs: list[float] = []
    for index in common_indices:
        candidate_value = _safe_float(_nested(candidate_map[index], "metrics", "trading", "top_5pct", "ev_net"))
        champion_value = _safe_float(_nested(champion_map[index], "metrics", "trading", "top_5pct", "ev_net"))
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
    }


def _normalized_advantage(candidate: float, champion: float, *, higher_is_better: bool) -> float:
    scale = max(abs(candidate), abs(champion), 1e-9)
    if higher_is_better:
        raw = (candidate - champion) / scale
    else:
        raw = (champion - candidate) / scale
    return max(min(float(raw), 1.0), -1.0)


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
