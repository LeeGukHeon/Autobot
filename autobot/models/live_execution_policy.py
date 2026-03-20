from __future__ import annotations

from typing import Any

ACTION_CONFIGS: dict[str, dict[str, Any]] = {
    "LIMIT_POST_ONLY": {
        "ord_type": "limit",
        "time_in_force": "post_only",
        "price_mode": "PASSIVE_MAKER",
        "aggressiveness_rank": 0,
    },
    "LIMIT_GTC_JOIN": {
        "ord_type": "limit",
        "time_in_force": "gtc",
        "price_mode": "JOIN",
        "aggressiveness_rank": 1,
    },
    "LIMIT_IOC_JOIN": {
        "ord_type": "limit",
        "time_in_force": "ioc",
        "price_mode": "JOIN",
        "aggressiveness_rank": 2,
    },
    "LIMIT_FOK_JOIN": {
        "ord_type": "limit",
        "time_in_force": "fok",
        "price_mode": "JOIN",
        "aggressiveness_rank": 3,
    },
    "BEST_IOC": {
        "ord_type": "best",
        "time_in_force": "ioc",
        "price_mode": "CROSS_1T",
        "aggressiveness_rank": 4,
    },
    "BEST_FOK": {
        "ord_type": "best",
        "time_in_force": "fok",
        "price_mode": "CROSS_1T",
        "aggressiveness_rank": 5,
    },
}
DEFAULT_ACTION_CODE = "LIMIT_GTC_JOIN"


def build_live_execution_survival_model(
    *,
    attempts: list[dict[str, Any]] | None,
    horizons_ms: tuple[int, ...] = (1_000, 3_000, 10_000),
) -> dict[str, Any]:
    normalized_attempts = [dict(item) for item in (attempts or []) if isinstance(item, dict)]
    by_action: dict[str, list[dict[str, Any]]] = {}
    by_state_action: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for item in normalized_attempts:
        action_code = _normalize_action_code(item.get("action_code"))
        state_key = state_bucket_key(item)
        by_action.setdefault(action_code, []).append(item)
        by_state_action.setdefault((state_key, action_code), []).append(item)
    return {
        "policy": "live_fill_hazard_survival_v1",
        "status": "ready" if normalized_attempts else "insufficient_history",
        "rows_total": int(len(normalized_attempts)),
        "horizons_ms": [int(value) for value in horizons_ms],
        "action_stats": {
            action_code: _summarize_attempt_rows(rows=rows, horizons_ms=horizons_ms)
            for action_code, rows in sorted(by_action.items())
        },
        "state_action_stats": {
            f"{state_key}|{action_code}": _summarize_attempt_rows(rows=rows, horizons_ms=horizons_ms)
            for (state_key, action_code), rows in sorted(by_state_action.items())
        },
    }


def select_live_execution_action(
    *,
    model_payload: dict[str, Any] | None,
    current_state: dict[str, Any] | None,
    expected_edge_bps: float | None,
    candidate_actions: list[str] | None = None,
    deadline_ms: int = 3_000,
) -> dict[str, Any]:
    model = dict(model_payload or {})
    fallback_candidates = [_normalize_action_code(item) for item in (candidate_actions or [DEFAULT_ACTION_CODE])]
    fallback_action_code = fallback_candidates[0] if fallback_candidates else DEFAULT_ACTION_CODE
    if int(model.get("rows_total", 0) or 0) < 20:
        config = dict(ACTION_CONFIGS.get(fallback_action_code) or ACTION_CONFIGS[DEFAULT_ACTION_CODE])
        return {
            "policy": "live_fill_hazard_survival_v1",
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
            "evaluated_actions": [],
            "skip_reason_code": "",
        }
    actions = [_normalize_action_code(item) for item in (candidate_actions or list(ACTION_CONFIGS.keys()))]
    state_key = state_bucket_key(current_state or {})
    action_stats = dict(model.get("action_stats") or {})
    state_action_stats = dict(model.get("state_action_stats") or {})
    expected_edge_value = max(float(expected_edge_bps or 0.0), 0.0)
    evaluated_actions: list[dict[str, Any]] = []
    selected: dict[str, Any] | None = None
    for action_code in actions:
        config = dict(ACTION_CONFIGS.get(action_code) or ACTION_CONFIGS[DEFAULT_ACTION_CODE])
        state_stats = dict(state_action_stats.get(f"{state_key}|{action_code}") or {})
        global_stats = dict(action_stats.get(action_code) or {})
        stats = state_stats if int(state_stats.get("sample_count", 0) or 0) >= 3 else global_stats
        p_fill = _safe_optional_float(stats.get(f"p_fill_within_{int(deadline_ms)}ms"))
        if p_fill is None:
            p_fill = _safe_optional_float(stats.get("p_fill_within_default"))
        if p_fill is None:
            p_fill = 0.0
        shortfall_bps = max(_safe_optional_float(stats.get("mean_shortfall_bps")) or 0.0, 0.0)
        miss_cost_bps = expected_edge_value
        utility_bps = (float(p_fill) * max(expected_edge_value - shortfall_bps, -expected_edge_value)) - (
            (1.0 - float(p_fill)) * miss_cost_bps
        )
        evaluation = {
            "action_code": action_code,
            "ord_type": str(config.get("ord_type")),
            "time_in_force": str(config.get("time_in_force")),
            "price_mode": str(config.get("price_mode")),
            "state_key": state_key,
            "stats_source": "state_action" if stats is state_stats and state_stats else ("action" if global_stats else "fallback"),
            "sample_count": int(stats.get("sample_count", 0) or 0),
            "p_fill_deadline": float(p_fill),
            "expected_time_to_first_fill_ms": _safe_optional_float(stats.get("mean_time_to_first_fill_ms")),
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
    status = "selected" if float(selected.get("utility_bps", 0.0) or 0.0) > 0.0 else "skip"
    return {
        "policy": "live_fill_hazard_survival_v1",
        "status": status,
        "deadline_ms": int(deadline_ms),
        "state_key": state_key,
        "selected_action_code": str(selected.get("action_code", DEFAULT_ACTION_CODE)),
        "selected_ord_type": str(selected.get("ord_type", "limit")),
        "selected_time_in_force": str(selected.get("time_in_force", "gtc")),
        "selected_price_mode": str(selected.get("price_mode", "JOIN")),
        "selected_p_fill_deadline": float(selected.get("p_fill_deadline", 0.0) or 0.0),
        "selected_expected_shortfall_bps": float(selected.get("expected_shortfall_bps", 0.0) or 0.0),
        "selected_expected_time_to_first_fill_ms": _safe_optional_float(selected.get("expected_time_to_first_fill_ms")),
        "selected_utility_bps": float(selected.get("utility_bps", 0.0) or 0.0),
        "evaluated_actions": evaluated_actions,
        "skip_reason_code": "LIVE_EXECUTION_NO_POSITIVE_UTILITY" if status == "skip" else "",
    }


def candidate_action_codes_for_price_mode(*, price_mode: str) -> list[str]:
    mode = str(price_mode or "").strip().upper()
    if mode == "PASSIVE_MAKER":
        return ["LIMIT_POST_ONLY", "LIMIT_GTC_JOIN", "LIMIT_IOC_JOIN", "BEST_IOC"]
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
