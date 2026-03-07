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
