from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ACTION_CONFIGS: dict[str, dict[str, Any]] = {
    "LIMIT_GTC_PASSIVE_MAKER": {
        "ord_type": "limit",
        "time_in_force": "gtc",
        "price_mode": "PASSIVE_MAKER",
        "aggressiveness_rank": 0,
    },
    "LIMIT_POST_ONLY": {
        "ord_type": "limit",
        "time_in_force": "post_only",
        "price_mode": "PASSIVE_MAKER",
        "aggressiveness_rank": 1,
    },
    "LIMIT_GTC_JOIN": {
        "ord_type": "limit",
        "time_in_force": "gtc",
        "price_mode": "JOIN",
        "aggressiveness_rank": 2,
    },
    "LIMIT_IOC_JOIN": {
        "ord_type": "limit",
        "time_in_force": "ioc",
        "price_mode": "JOIN",
        "aggressiveness_rank": 3,
    },
    "LIMIT_FOK_JOIN": {
        "ord_type": "limit",
        "time_in_force": "fok",
        "price_mode": "JOIN",
        "aggressiveness_rank": 4,
    },
    "BEST_IOC": {
        "ord_type": "best",
        "time_in_force": "ioc",
        "price_mode": "CROSS_1T",
        "aggressiveness_rank": 5,
    },
    "BEST_FOK": {
        "ord_type": "best",
        "time_in_force": "fok",
        "price_mode": "CROSS_1T",
        "aggressiveness_rank": 6,
    },
}
DEFAULT_ACTION_CODE = "LIMIT_GTC_JOIN"
DEFAULT_EXECUTION_CONTRACT_ARTIFACT_PATH = "logs/live_execution_policy/combined_live_execution_policy.json"
DEFAULT_FILL_HORIZONS_MS: tuple[int, ...] = (1_000, 3_000, 10_000, 30_000, 60_000, 180_000, 300_000, 600_000)
CANARY_STRONG_EDGE_THRESHOLD_BPS = 50.0
CANARY_STRONG_EDGE_UTILITY_MARGIN_BPS = 20.0
FILL_ACTION_PSEUDOCOUNT = 8.0
FILL_PRICE_MODE_PSEUDOCOUNT = 6.0
FILL_GLOBAL_PSEUDOCOUNT = 12.0
MISS_ACTION_PSEUDOCOUNT = 4.0
MISS_PRICE_MODE_PSEUDOCOUNT = 3.0
MISS_GLOBAL_PSEUDOCOUNT = 6.0


def build_live_execution_survival_model(
    *,
    attempts: list[dict[str, Any]] | None,
    horizons_ms: tuple[int, ...] = DEFAULT_FILL_HORIZONS_MS,
) -> dict[str, Any]:
    normalized_horizons = _normalize_horizons_ms(horizons_ms)
    normalized_attempts = [dict(item) for item in (attempts or []) if isinstance(item, dict)]
    by_action: dict[str, list[dict[str, Any]]] = {}
    by_price_mode: dict[str, list[dict[str, Any]]] = {}
    by_state_action: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for item in normalized_attempts:
        action_code = _normalize_action_code(item.get("action_code"))
        state_key = state_bucket_key(item)
        price_mode = _action_price_mode(action_code)
        by_action.setdefault(action_code, []).append(item)
        by_price_mode.setdefault(price_mode, []).append(item)
        by_state_action.setdefault((state_key, action_code), []).append(item)
    return {
        "policy": "live_fill_hazard_survival_v2",
        "status": "ready" if normalized_attempts else "insufficient_history",
        "rows_total": int(len(normalized_attempts)),
        "horizons_ms": [int(value) for value in normalized_horizons],
        "action_stats": {
            action_code: _summarize_attempt_rows(rows=rows, horizons_ms=normalized_horizons)
            for action_code, rows in sorted(by_action.items())
        },
        "price_mode_stats": {
            price_mode: _summarize_attempt_rows(rows=rows, horizons_ms=normalized_horizons)
            for price_mode, rows in sorted(by_price_mode.items())
        },
        "state_action_stats": {
            f"{state_key}|{action_code}": _summarize_attempt_rows(rows=rows, horizons_ms=normalized_horizons)
            for (state_key, action_code), rows in sorted(by_state_action.items())
        },
        "global_stats": _summarize_attempt_rows(rows=normalized_attempts, horizons_ms=normalized_horizons),
    }


def build_live_execution_contract(
    *,
    attempts: list[dict[str, Any]] | None,
    horizons_ms: tuple[int, ...] = DEFAULT_FILL_HORIZONS_MS,
) -> dict[str, Any]:
    normalized_horizons = _normalize_horizons_ms(horizons_ms)
    normalized_attempts = [dict(item) for item in (attempts or []) if isinstance(item, dict)]
    fill_model = build_live_execution_survival_model(
        attempts=normalized_attempts,
        horizons_ms=normalized_horizons,
    )
    miss_cost_model = _build_miss_cost_model(attempts=normalized_attempts)
    return {
        "policy": "live_execution_contract_v2",
        "status": str(fill_model.get("status", "insufficient_history")).strip() or "insufficient_history",
        "rows_total": int(len(normalized_attempts)),
        "horizons_ms": [int(value) for value in normalized_horizons],
        "fill_model": fill_model,
        "miss_cost_model": miss_cost_model,
    }


def normalize_live_execution_contract(payload: dict[str, Any] | None) -> dict[str, Any]:
    doc = dict(payload or {})
    policy = str(doc.get("policy", "")).strip()
    if policy in {"live_execution_contract_v1", "live_execution_contract_v2"}:
        fill_model = dict(doc.get("fill_model") or {})
        miss_cost_model = dict(doc.get("miss_cost_model") or {})
        return {
            "policy": policy or "live_execution_contract_v2",
            "status": str(doc.get("status", fill_model.get("status", "insufficient_history"))).strip()
            or "insufficient_history",
            "rows_total": int(doc.get("rows_total", fill_model.get("rows_total", 0)) or 0),
            "horizons_ms": [int(value) for value in (doc.get("horizons_ms") or fill_model.get("horizons_ms") or [])],
            "fill_model": fill_model,
            "miss_cost_model": miss_cost_model,
        }
    if doc:
        fill_model = dict(doc)
        return {
            "policy": "live_execution_contract_v2",
            "status": str(fill_model.get("status", "insufficient_history")).strip() or "insufficient_history",
            "rows_total": int(fill_model.get("rows_total", 0) or 0),
            "horizons_ms": [int(value) for value in (fill_model.get("horizons_ms") or [])],
            "fill_model": fill_model,
            "miss_cost_model": _build_miss_cost_model(attempts=[]),
        }
    return {
        "policy": "live_execution_contract_v2",
        "status": "insufficient_history",
        "rows_total": 0,
        "horizons_ms": [],
        "fill_model": {},
        "miss_cost_model": _build_miss_cost_model(attempts=[]),
    }


def load_live_execution_contract_artifact(
    *,
    project_root: str | Path,
    artifact_path: str | Path = DEFAULT_EXECUTION_CONTRACT_ARTIFACT_PATH,
) -> dict[str, Any]:
    root = Path(project_root)
    candidate = Path(artifact_path)
    resolved = candidate if candidate.is_absolute() else (root / candidate)
    if not resolved.exists():
        return normalize_live_execution_contract({})
    try:
        parsed = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return normalize_live_execution_contract({})
    if not isinstance(parsed, dict):
        return normalize_live_execution_contract({})
    if isinstance(parsed.get("execution_contract"), dict):
        return normalize_live_execution_contract(parsed.get("execution_contract"))
    if isinstance(parsed.get("model"), dict):
        return normalize_live_execution_contract(parsed.get("model"))
    return normalize_live_execution_contract(parsed)


def build_execution_policy_state(
    *,
    micro_state: dict[str, Any] | None,
    expected_edge_bps: float | None,
) -> dict[str, Any]:
    payload = dict(micro_state or {})
    if expected_edge_bps is not None:
        payload["expected_edge_bps"] = float(expected_edge_bps)
    return payload


def select_live_execution_action(
    *,
    model_payload: dict[str, Any] | None,
    current_state: dict[str, Any] | None,
    expected_edge_bps: float | None,
    candidate_actions: list[str] | None = None,
    deadline_ms: int = 3_000,
) -> dict[str, Any]:
    contract = normalize_live_execution_contract(model_payload if isinstance(model_payload, dict) else {})
    fill_model = dict(contract.get("fill_model") or {})
    miss_cost_model = dict(contract.get("miss_cost_model") or {})
    fallback_candidates = [_normalize_action_code(item) for item in (candidate_actions or [DEFAULT_ACTION_CODE])]
    fallback_action_code = fallback_candidates[0] if fallback_candidates else DEFAULT_ACTION_CODE
    if int(contract.get("rows_total", 0) or 0) < 20:
        config = dict(ACTION_CONFIGS.get(fallback_action_code) or ACTION_CONFIGS[DEFAULT_ACTION_CODE])
        return {
            "policy": str(contract.get("policy", "live_execution_contract_v2")).strip() or "live_execution_contract_v2",
            "status": "fallback",
            "deadline_ms": int(deadline_ms),
            "state_key": state_bucket_key(current_state or {}),
            "selected_action_code": fallback_action_code,
            "selected_ord_type": str(config.get("ord_type", "limit")),
            "selected_time_in_force": str(config.get("time_in_force", "gtc")),
            "selected_price_mode": str(config.get("price_mode", "JOIN")),
            "selected_p_fill_deadline": 0.0,
            "selected_expected_shortfall_bps": 0.0,
            "selected_expected_time_to_first_fill_ms": None,
            "selected_utility_bps": float(expected_edge_bps or 0.0),
            "selected_expected_miss_cost_bps": float(expected_edge_bps or 0.0),
            "evaluated_actions": [],
            "skip_reason_code": "",
        }
    actions = [_normalize_action_code(item) for item in (candidate_actions or list(ACTION_CONFIGS.keys()))]
    state_key = state_bucket_key(current_state or {})
    expected_edge_value = max(float(expected_edge_bps or 0.0), 0.0)
    evaluated_actions: list[dict[str, Any]] = []
    selected: dict[str, Any] | None = None
    for action_code in actions:
        config = dict(ACTION_CONFIGS.get(action_code) or ACTION_CONFIGS[DEFAULT_ACTION_CODE])
        fill_estimate = _resolve_fill_estimate(
            fill_model=fill_model,
            state_key=state_key,
            action_code=action_code,
            deadline_ms=deadline_ms,
        )
        p_fill = float(fill_estimate.get("p_fill", 0.0) or 0.0)
        effective_sample_count = float(fill_estimate.get("effective_sample_count", 0.0) or 0.0)
        sample_count = int(fill_estimate.get("sample_count", 0) or 0)
        p_fill_lcb = _posterior_fill_lcb(
            p_fill=float(p_fill),
            sample_count=max(int(round(effective_sample_count)), sample_count),
        )
        shortfall_bps = float(fill_estimate.get("expected_shortfall_bps", 0.0) or 0.0)
        miss_cost_bps = _resolve_miss_cost_bps(
            miss_cost_model=miss_cost_model,
            state_key=state_key,
            action_code=action_code,
            fallback_expected_edge_bps=expected_edge_value,
        )
        fill_value_bps = float(expected_edge_value - shortfall_bps)
        utility_bps = (float(p_fill_lcb) * fill_value_bps) - (
            (1.0 - float(p_fill_lcb)) * miss_cost_bps
        )
        evaluation = {
            "action_code": action_code,
            "ord_type": str(config.get("ord_type")),
            "time_in_force": str(config.get("time_in_force")),
            "price_mode": str(config.get("price_mode")),
            "state_key": state_key,
            "stats_source": str(fill_estimate.get("stats_source", "fallback")),
            "sample_count": int(sample_count),
            "effective_sample_count": float(effective_sample_count),
            "p_fill_deadline": float(p_fill),
            "p_fill_deadline_lcb": float(p_fill_lcb),
            "p_fill_source_horizon_ms": _safe_optional_int(fill_estimate.get("p_fill_source_horizon_ms")),
            "expected_time_to_first_fill_ms": _safe_optional_float(fill_estimate.get("expected_time_to_first_fill_ms")),
            "expected_shortfall_bps": float(shortfall_bps),
            "expected_edge_bps": float(expected_edge_value),
            "miss_cost_bps": float(miss_cost_bps),
            "utility_bps": float(utility_bps),
            "aggressiveness_rank": int(config.get("aggressiveness_rank", 99) or 99),
        }
        evaluated_actions.append(evaluation)
        if selected is None or (
            float(evaluation["utility_bps"]) > float(selected["utility_bps"])
            or (
                float(evaluation["utility_bps"]) == float(selected["utility_bps"])
                and int(evaluation["aggressiveness_rank"]) < int(selected["aggressiveness_rank"])
            )
        ):
            selected = evaluation
    if selected is None:
        selected = {
            "action_code": DEFAULT_ACTION_CODE,
            **ACTION_CONFIGS[DEFAULT_ACTION_CODE],
            "state_key": state_key,
            "sample_count": 0,
            "p_fill_deadline": 0.0,
            "expected_time_to_first_fill_ms": None,
            "expected_shortfall_bps": 0.0,
            "expected_edge_bps": float(expected_edge_value),
            "miss_cost_bps": float(expected_edge_value),
            "utility_bps": -float(expected_edge_value),
            "aggressiveness_rank": int(ACTION_CONFIGS[DEFAULT_ACTION_CODE]["aggressiveness_rank"]),
        }
    selected, selection_reason_code, urgency_override = _maybe_apply_canary_strong_edge_stage_override(
        current_state=current_state or {},
        expected_edge_bps=float(expected_edge_value),
        evaluated_actions=evaluated_actions,
        selected=selected,
    )
    status = "selected" if float(selected.get("utility_bps", 0.0) or 0.0) > 0.0 else "skip"
    return {
        "policy": str(contract.get("policy", "live_execution_contract_v2")).strip() or "live_execution_contract_v2",
        "status": status,
        "deadline_ms": int(deadline_ms),
        "state_key": state_key,
        "selected_action_code": str(selected.get("action_code", DEFAULT_ACTION_CODE)),
        "selected_ord_type": str(selected.get("ord_type", "limit")),
        "selected_time_in_force": str(selected.get("time_in_force", "gtc")),
        "selected_price_mode": str(selected.get("price_mode", "JOIN")),
        "selected_stats_source": str(selected.get("stats_source", "fallback")),
        "selected_effective_sample_count": float(selected.get("effective_sample_count", 0.0) or 0.0),
        "selected_p_fill_deadline": float(selected.get("p_fill_deadline", 0.0) or 0.0),
        "selected_p_fill_source_horizon_ms": _safe_optional_int(selected.get("p_fill_source_horizon_ms")),
        "selected_expected_shortfall_bps": float(selected.get("expected_shortfall_bps", 0.0) or 0.0),
        "selected_expected_time_to_first_fill_ms": _safe_optional_float(selected.get("expected_time_to_first_fill_ms")),
        "selected_utility_bps": float(selected.get("utility_bps", 0.0) or 0.0),
        "selected_expected_miss_cost_bps": float(selected.get("miss_cost_bps", expected_edge_value) or expected_edge_value),
        "selection_reason_code": str(selection_reason_code).strip() or "UTILITY_MAX",
        "urgency_override": urgency_override,
        "evaluated_actions": evaluated_actions,
        "skip_reason_code": "LIVE_EXECUTION_NO_POSITIVE_UTILITY" if status == "skip" else "",
    }


def candidate_action_codes_for_price_mode(*, price_mode: str) -> list[str]:
    mode = str(price_mode or "").strip().upper()
    if mode == "PASSIVE_MAKER":
        return ["LIMIT_POST_ONLY", "LIMIT_GTC_PASSIVE_MAKER", "LIMIT_GTC_JOIN", "LIMIT_IOC_JOIN", "BEST_IOC"]
    if mode == "CROSS_1T":
        return ["BEST_IOC", "BEST_FOK", "LIMIT_IOC_JOIN", "LIMIT_FOK_JOIN"]
    return ["LIMIT_GTC_JOIN", "LIMIT_IOC_JOIN", "LIMIT_FOK_JOIN", "BEST_IOC"]


def state_bucket_key(payload: dict[str, Any] | None) -> str:
    doc = dict(payload or {})
    spread_bps = _safe_optional_float(doc.get("spread_bps"))
    depth_krw = _safe_optional_float(doc.get("depth_top5_notional_krw"))
    snapshot_age_ms = _safe_optional_float(doc.get("snapshot_age_ms"))
    expected_edge_bps = _safe_optional_float(doc.get("expected_edge_bps"))
    return "|".join(
        [
            _spread_bucket(spread_bps),
            _depth_bucket(depth_krw),
            _age_bucket(snapshot_age_ms),
            _edge_bucket(expected_edge_bps),
        ]
    )


def _summarize_attempt_rows(*, rows: list[dict[str, Any]], horizons_ms: tuple[int, ...]) -> dict[str, Any]:
    sample_count = len(rows)
    first_fill_delays = [
        max(int(row.get("first_fill_ts_ms")) - int(row.get("submitted_ts_ms")), 0)
        for row in rows
        if row.get("first_fill_ts_ms") is not None and row.get("submitted_ts_ms") is not None
    ]
    shortfalls = [
        float(row.get("shortfall_bps"))
        for row in rows
        if row.get("shortfall_bps") is not None
    ]
    summary = {
        "sample_count": int(sample_count),
        "filled_count": int(len(first_fill_delays)),
        "mean_time_to_first_fill_ms": (
            sum(float(value) for value in first_fill_delays) / float(len(first_fill_delays))
            if first_fill_delays
            else None
        ),
        "mean_shortfall_bps": (
            sum(float(value) for value in shortfalls) / float(len(shortfalls))
            if shortfalls
            else None
        ),
        "p_fill_within_default": 0.0,
        "default_horizon_ms": 3_000,
    }
    for horizon_ms in horizons_ms:
        hits = [
            row
            for row in rows
            if row.get("first_fill_ts_ms") is not None
            and row.get("submitted_ts_ms") is not None
            and (int(row.get("first_fill_ts_ms")) - int(row.get("submitted_ts_ms"))) <= int(horizon_ms)
        ]
        posterior_mean = (1.0 + float(len(hits))) / (2.0 + float(sample_count))
        summary[f"p_fill_within_{int(horizon_ms)}ms"] = float(posterior_mean)
        if int(horizon_ms) == 3_000:
            summary["p_fill_within_default"] = float(posterior_mean)
    return summary


def _build_miss_cost_model(*, attempts: list[dict[str, Any]]) -> dict[str, Any]:
    by_action: dict[str, list[float]] = {}
    by_price_mode: dict[str, list[float]] = {}
    by_state_action: dict[tuple[str, str], list[float]] = {}
    for item in attempts:
        final_state = str(item.get("final_state") or "").strip().upper()
        if final_state not in {"MISSED", "PARTIAL_CANCELLED"}:
            continue
        action_code = _normalize_action_code(item.get("action_code"))
        state_key = state_bucket_key(item)
        price_mode = _action_price_mode(action_code)
        miss_cost_bps = _safe_optional_float(item.get("expected_net_edge_bps"))
        if miss_cost_bps is None:
            miss_cost_bps = _safe_optional_float(item.get("expected_edge_bps"))
        if miss_cost_bps is None:
            continue
        by_action.setdefault(action_code, []).append(float(max(miss_cost_bps, 0.0)))
        by_price_mode.setdefault(price_mode, []).append(float(max(miss_cost_bps, 0.0)))
        by_state_action.setdefault((state_key, action_code), []).append(float(max(miss_cost_bps, 0.0)))
    global_costs = [value for values in by_action.values() for value in values]
    return {
        "policy": "execution_miss_cost_summary_v2",
        "status": "ready" if by_action else "insufficient_history",
        "action_stats": {
            action_code: _summarize_cost_rows(costs)
            for action_code, costs in sorted(by_action.items())
        },
        "price_mode_stats": {
            price_mode: _summarize_cost_rows(costs)
            for price_mode, costs in sorted(by_price_mode.items())
        },
        "state_action_stats": {
            f"{state_key}|{action_code}": _summarize_cost_rows(costs)
            for (state_key, action_code), costs in sorted(by_state_action.items())
        },
        "global_stats": _summarize_cost_rows(global_costs),
    }


def _summarize_cost_rows(costs: list[float]) -> dict[str, Any]:
    values = [float(max(value, 0.0)) for value in costs]
    sample_count = len(values)
    if sample_count <= 0:
        return {"sample_count": 0, "mean_miss_cost_bps": None}
    return {
        "sample_count": int(sample_count),
        "mean_miss_cost_bps": float(sum(values) / float(sample_count)),
    }


def _resolve_miss_cost_bps(
    *,
    miss_cost_model: dict[str, Any],
    state_key: str,
    action_code: str,
    fallback_expected_edge_bps: float,
) -> float:
    state_action_stats = dict(miss_cost_model.get("state_action_stats") or {})
    action_stats = dict(miss_cost_model.get("action_stats") or {})
    price_mode_stats = dict(miss_cost_model.get("price_mode_stats") or {})
    global_stats = dict(miss_cost_model.get("global_stats") or {})
    state_key_value = f"{state_key}|{action_code}"
    state_stats = dict(state_action_stats.get(state_key_value) or {})
    action_stats_row = dict(action_stats.get(action_code) or {})
    price_mode = _action_price_mode(action_code)
    price_mode_row = dict(price_mode_stats.get(price_mode) or {})
    components: list[tuple[float, float]] = []
    state_value = _safe_optional_float(state_stats.get("mean_miss_cost_bps"))
    state_count = int(state_stats.get("sample_count", 0) or 0)
    if state_value is not None and state_count > 0:
        components.append((float(state_value), float(state_count)))
    action_value = _safe_optional_float(action_stats_row.get("mean_miss_cost_bps"))
    if action_value is not None:
        components.append((float(action_value), float(MISS_ACTION_PSEUDOCOUNT)))
    price_mode_value = _safe_optional_float(price_mode_row.get("mean_miss_cost_bps"))
    if price_mode_value is not None:
        components.append((float(price_mode_value), float(MISS_PRICE_MODE_PSEUDOCOUNT)))
    global_value = _safe_optional_float(global_stats.get("mean_miss_cost_bps"))
    if global_value is not None:
        components.append((float(global_value), float(MISS_GLOBAL_PSEUDOCOUNT)))
    if components:
        numerator = sum(value * weight for value, weight in components)
        denominator = sum(weight for _, weight in components)
        if denominator > 0.0:
            return float(max(numerator / denominator, 0.0))
    return float(max(fallback_expected_edge_bps, 0.0))


def _posterior_fill_lcb(*, p_fill: float, sample_count: int, z_score: float = 1.28) -> float:
    count = max(int(sample_count), 0)
    if count <= 0:
        return 0.0
    mean = min(max(float(p_fill), 0.0), 1.0)
    variance = max(mean * (1.0 - mean), 0.0)
    stderr = (variance / float(max(count, 1))) ** 0.5
    return max(min(mean - (float(z_score) * stderr), 1.0), 0.0)


def _safe_optional_int(value: Any) -> int | None:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    if number < 0:
        return None
    return number


def _resolve_fill_estimate(
    *,
    fill_model: dict[str, Any],
    state_key: str,
    action_code: str,
    deadline_ms: int,
) -> dict[str, Any]:
    state_action_stats = dict(fill_model.get("state_action_stats") or {})
    action_stats = dict(fill_model.get("action_stats") or {})
    price_mode_stats = dict(fill_model.get("price_mode_stats") or {})
    global_stats = dict(fill_model.get("global_stats") or {})
    state_stats = dict(state_action_stats.get(f"{state_key}|{action_code}") or {})
    action_row = dict(action_stats.get(action_code) or {})
    price_mode = _action_price_mode(action_code)
    price_mode_row = dict(price_mode_stats.get(price_mode) or {})
    state_count = int(state_stats.get("sample_count", 0) or 0)
    action_count = int(action_row.get("sample_count", 0) or 0)
    price_mode_count = int(price_mode_row.get("sample_count", 0) or 0)
    global_count = int(global_stats.get("sample_count", 0) or 0)
    p_fill_value, p_fill_source_horizon_ms = _shrink_fill_probability(
        deadline_ms=deadline_ms,
        state_stats=state_stats,
        action_stats=action_row,
        price_mode_stats=price_mode_row,
        global_stats=global_stats,
    )
    expected_shortfall_bps = _shrink_optional_metric(
        metric_name="mean_shortfall_bps",
        state_stats=state_stats,
        action_stats=action_row,
        price_mode_stats=price_mode_row,
        global_stats=global_stats,
        action_pseudocount=float(FILL_ACTION_PSEUDOCOUNT),
        price_mode_pseudocount=float(FILL_PRICE_MODE_PSEUDOCOUNT),
        global_pseudocount=float(FILL_GLOBAL_PSEUDOCOUNT),
    )
    expected_time_to_first_fill_ms = _shrink_optional_metric(
        metric_name="mean_time_to_first_fill_ms",
        state_stats=state_stats,
        action_stats=action_row,
        price_mode_stats=price_mode_row,
        global_stats=global_stats,
        action_pseudocount=float(FILL_ACTION_PSEUDOCOUNT),
        price_mode_pseudocount=float(FILL_PRICE_MODE_PSEUDOCOUNT),
        global_pseudocount=float(FILL_GLOBAL_PSEUDOCOUNT),
    )
    if state_count > 0:
        stats_source = "state_action_shrunk"
        sample_count = state_count
    elif action_count > 0:
        stats_source = "action_shrunk"
        sample_count = action_count
    elif price_mode_count > 0:
        stats_source = "price_mode_prior"
        sample_count = price_mode_count
    elif global_count > 0:
        stats_source = "global_prior"
        sample_count = global_count
    else:
        stats_source = "fallback"
        sample_count = 0
    effective_sample_count = float(state_count)
    if action_count > 0:
        effective_sample_count += float(FILL_ACTION_PSEUDOCOUNT)
    if price_mode_count > 0:
        effective_sample_count += float(FILL_PRICE_MODE_PSEUDOCOUNT)
    if global_count > 0:
        effective_sample_count += float(FILL_GLOBAL_PSEUDOCOUNT)
    return {
        "stats_source": stats_source,
        "sample_count": int(sample_count),
        "effective_sample_count": float(effective_sample_count),
        "p_fill": float(p_fill_value),
        "p_fill_source_horizon_ms": _safe_optional_int(p_fill_source_horizon_ms),
        "expected_shortfall_bps": float(expected_shortfall_bps or 0.0),
        "expected_time_to_first_fill_ms": _safe_optional_float(expected_time_to_first_fill_ms),
    }


def _shrink_fill_probability(
    *,
    deadline_ms: int,
    state_stats: dict[str, Any],
    action_stats: dict[str, Any],
    price_mode_stats: dict[str, Any],
    global_stats: dict[str, Any],
) -> tuple[float, int | None]:
    state_value, state_horizon_ms = _resolve_deadline_probability(stats=state_stats, deadline_ms=deadline_ms)
    action_value, action_horizon_ms = _resolve_deadline_probability(stats=action_stats, deadline_ms=deadline_ms)
    price_mode_value, price_mode_horizon_ms = _resolve_deadline_probability(stats=price_mode_stats, deadline_ms=deadline_ms)
    global_value, global_horizon_ms = _resolve_deadline_probability(stats=global_stats, deadline_ms=deadline_ms)
    state_count = int(state_stats.get("sample_count", 0) or 0)
    action_count = int(action_stats.get("sample_count", 0) or 0)
    price_mode_count = int(price_mode_stats.get("sample_count", 0) or 0)
    global_count = int(global_stats.get("sample_count", 0) or 0)
    components: list[tuple[float, float]] = []
    if state_value is not None and state_count > 0:
        components.append((float(state_value), float(state_count)))
    if action_value is not None and action_count > 0:
        components.append((float(action_value), float(FILL_ACTION_PSEUDOCOUNT)))
    if price_mode_value is not None and price_mode_count > 0:
        components.append((float(price_mode_value), float(FILL_PRICE_MODE_PSEUDOCOUNT)))
    if global_value is not None and global_count > 0:
        components.append((float(global_value), float(FILL_GLOBAL_PSEUDOCOUNT)))
    if not components:
        return 0.0, None
    numerator = sum(value * weight for value, weight in components)
    denominator = sum(weight for _, weight in components)
    horizon_ms = (
        state_horizon_ms
        if state_horizon_ms is not None
        else action_horizon_ms
        if action_horizon_ms is not None
        else price_mode_horizon_ms
        if price_mode_horizon_ms is not None
        else global_horizon_ms
    )
    return float(max(min(numerator / denominator, 1.0), 0.0)), horizon_ms


def _resolve_deadline_probability(*, stats: dict[str, Any], deadline_ms: int) -> tuple[float | None, int | None]:
    direct = _safe_optional_float(stats.get(f"p_fill_within_{int(deadline_ms)}ms"))
    if direct is not None:
        return float(direct), int(deadline_ms)
    available_horizons = _available_horizons_from_stats(stats)
    if available_horizons:
        eligible = [horizon for horizon in available_horizons if int(horizon) <= int(deadline_ms)]
        selected_horizon = max(eligible) if eligible else min(available_horizons)
        value = _safe_optional_float(stats.get(f"p_fill_within_{int(selected_horizon)}ms"))
        if value is not None:
            return float(value), int(selected_horizon)
    fallback = _safe_optional_float(stats.get("p_fill_within_default"))
    if fallback is not None:
        return float(fallback), _safe_optional_int(stats.get("default_horizon_ms")) or 3_000
    return None, None


def _shrink_optional_metric(
    *,
    metric_name: str,
    state_stats: dict[str, Any],
    action_stats: dict[str, Any],
    price_mode_stats: dict[str, Any],
    global_stats: dict[str, Any],
    action_pseudocount: float,
    price_mode_pseudocount: float,
    global_pseudocount: float,
) -> float | None:
    state_value = _safe_optional_float(state_stats.get(metric_name))
    action_value = _safe_optional_float(action_stats.get(metric_name))
    price_mode_value = _safe_optional_float(price_mode_stats.get(metric_name))
    global_value = _safe_optional_float(global_stats.get(metric_name))
    state_count = int(state_stats.get("sample_count", 0) or 0)
    action_count = int(action_stats.get("sample_count", 0) or 0)
    price_mode_count = int(price_mode_stats.get("sample_count", 0) or 0)
    global_count = int(global_stats.get("sample_count", 0) or 0)
    components: list[tuple[float, float]] = []
    if state_value is not None and state_count > 0:
        components.append((float(state_value), float(state_count)))
    if action_value is not None and action_count > 0:
        components.append((float(action_value), float(action_pseudocount)))
    if price_mode_value is not None and price_mode_count > 0:
        components.append((float(price_mode_value), float(price_mode_pseudocount)))
    if global_value is not None and global_count > 0:
        components.append((float(global_value), float(global_pseudocount)))
    if not components:
        return None
    numerator = sum(value * weight for value, weight in components)
    denominator = sum(weight for _, weight in components)
    if denominator <= 0.0:
        return None
    return float(numerator / denominator)


def _available_horizons_from_stats(stats: dict[str, Any]) -> list[int]:
    horizons: list[int] = []
    for key in stats.keys():
        text = str(key)
        if not text.startswith("p_fill_within_") or not text.endswith("ms"):
            continue
        numeric = text[len("p_fill_within_") : -2]
        try:
            value = int(numeric)
        except (TypeError, ValueError):
            continue
        if value > 0:
            horizons.append(value)
    return sorted(set(horizons))


def _normalize_horizons_ms(horizons_ms: tuple[int, ...] | list[int] | None) -> tuple[int, ...]:
    values = sorted({max(int(item), 1) for item in (horizons_ms or DEFAULT_FILL_HORIZONS_MS) if int(item) > 0})
    return tuple(values or DEFAULT_FILL_HORIZONS_MS)


def _action_price_mode(action_code: str) -> str:
    config = dict(ACTION_CONFIGS.get(_normalize_action_code(action_code)) or ACTION_CONFIGS[DEFAULT_ACTION_CODE])
    return str(config.get("price_mode", "JOIN")).strip().upper() or "JOIN"


def _maybe_apply_canary_strong_edge_stage_override(
    *,
    current_state: dict[str, Any],
    expected_edge_bps: float,
    evaluated_actions: list[dict[str, Any]],
    selected: dict[str, Any],
) -> tuple[dict[str, Any], str, dict[str, Any]]:
    rollout_mode = str((current_state or {}).get("rollout_mode", "")).strip().lower()
    selected_price_mode = str(selected.get("price_mode", "")).strip().upper()
    if rollout_mode != "canary":
        return selected, "UTILITY_MAX", {}
    if float(expected_edge_bps) < float(CANARY_STRONG_EDGE_THRESHOLD_BPS):
        return selected, "UTILITY_MAX", {}
    if selected_price_mode != "PASSIVE_MAKER":
        return selected, "UTILITY_MAX", {}

    alternatives = [
        dict(item)
        for item in evaluated_actions
        if str(item.get("price_mode", "")).strip().upper() in {"JOIN", "CROSS_1T"}
        and float(item.get("utility_bps", 0.0) or 0.0) > 0.0
    ]
    if not alternatives:
        return selected, "UTILITY_MAX", {}

    best_alternative = max(
        alternatives,
        key=lambda item: (
            float(item.get("utility_bps", 0.0) or 0.0),
            -int(item.get("aggressiveness_rank", 99) or 99),
        ),
    )
    utility_gap = float(selected.get("utility_bps", 0.0) or 0.0) - float(best_alternative.get("utility_bps", 0.0) or 0.0)
    utility_margin = min(
        max(float(expected_edge_bps) * 0.25, 10.0),
        float(CANARY_STRONG_EDGE_UTILITY_MARGIN_BPS),
    )
    if utility_gap > utility_margin:
        return selected, "UTILITY_MAX", {}

    return (
        best_alternative,
        "CANARY_STRONG_EDGE_STAGE_ESCALATION",
        {
            "enabled": True,
            "reason_code": "CANARY_STRONG_EDGE_STAGE_ESCALATION",
            "rollout_mode": rollout_mode,
            "expected_edge_bps": float(expected_edge_bps),
            "strong_edge_threshold_bps": float(CANARY_STRONG_EDGE_THRESHOLD_BPS),
            "utility_margin_bps": float(utility_margin),
            "selected_before_override": {
                "action_code": str(selected.get("action_code", "")),
                "price_mode": selected_price_mode,
                "utility_bps": float(selected.get("utility_bps", 0.0) or 0.0),
            },
            "selected_after_override": {
                "action_code": str(best_alternative.get("action_code", "")),
                "price_mode": str(best_alternative.get("price_mode", "")).strip().upper(),
                "utility_bps": float(best_alternative.get("utility_bps", 0.0) or 0.0),
            },
            "utility_gap_bps": float(utility_gap),
        },
    )


def _normalize_action_code(value: Any) -> str:
    text = str(value or "").strip().upper()
    return text if text in ACTION_CONFIGS else DEFAULT_ACTION_CODE


def _spread_bucket(value: float | None) -> str:
    if value is None:
        return "spread_unknown"
    if float(value) <= 5.0:
        return "spread_tight"
    if float(value) <= 15.0:
        return "spread_mid"
    return "spread_wide"


def _depth_bucket(value: float | None) -> str:
    if value is None:
        return "depth_unknown"
    if float(value) >= 2_000_000.0:
        return "depth_deep"
    if float(value) >= 500_000.0:
        return "depth_mid"
    return "depth_shallow"


def _age_bucket(value: float | None) -> str:
    if value is None:
        return "age_unknown"
    if float(value) <= 500.0:
        return "age_fresh"
    if float(value) <= 2_000.0:
        return "age_warm"
    return "age_stale"


def _edge_bucket(value: float | None) -> str:
    if value is None:
        return "edge_unknown"
    if float(value) <= 5.0:
        return "edge_weak"
    if float(value) <= 15.0:
        return "edge_mid"
    return "edge_strong"


def _safe_optional_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
