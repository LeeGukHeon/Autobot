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
CANARY_STRONG_EDGE_THRESHOLD_BPS = 50.0
CANARY_STRONG_EDGE_UTILITY_MARGIN_BPS = 20.0


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


def build_live_execution_contract(
    *,
    attempts: list[dict[str, Any]] | None,
    horizons_ms: tuple[int, ...] = (1_000, 3_000, 10_000),
) -> dict[str, Any]:
    normalized_attempts = [dict(item) for item in (attempts or []) if isinstance(item, dict)]
    fill_model = build_live_execution_survival_model(
        attempts=normalized_attempts,
        horizons_ms=horizons_ms,
    )
    miss_cost_model = _build_miss_cost_model(attempts=normalized_attempts)
    return {
        "policy": "live_execution_contract_v1",
        "status": str(fill_model.get("status", "insufficient_history")).strip() or "insufficient_history",
        "rows_total": int(len(normalized_attempts)),
        "horizons_ms": [int(value) for value in horizons_ms],
        "fill_model": fill_model,
        "miss_cost_model": miss_cost_model,
    }


def normalize_live_execution_contract(payload: dict[str, Any] | None) -> dict[str, Any]:
    doc = dict(payload or {})
    if str(doc.get("policy", "")).strip() == "live_execution_contract_v1":
        fill_model = dict(doc.get("fill_model") or {})
        miss_cost_model = dict(doc.get("miss_cost_model") or {})
        return {
            "policy": "live_execution_contract_v1",
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
            "policy": "live_execution_contract_v1",
            "status": str(fill_model.get("status", "insufficient_history")).strip() or "insufficient_history",
            "rows_total": int(fill_model.get("rows_total", 0) or 0),
            "horizons_ms": [int(value) for value in (fill_model.get("horizons_ms") or [])],
            "fill_model": fill_model,
            "miss_cost_model": _build_miss_cost_model(attempts=[]),
        }
    return {
        "policy": "live_execution_contract_v1",
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
    model = dict(contract.get("fill_model") or {})
    miss_cost_model = dict(contract.get("miss_cost_model") or {})
    fallback_candidates = [_normalize_action_code(item) for item in (candidate_actions or [DEFAULT_ACTION_CODE])]
    fallback_action_code = fallback_candidates[0] if fallback_candidates else DEFAULT_ACTION_CODE
    if int(contract.get("rows_total", 0) or 0) < 20:
        config = dict(ACTION_CONFIGS.get(fallback_action_code) or ACTION_CONFIGS[DEFAULT_ACTION_CODE])
        return {
            "policy": str(contract.get("policy", "live_execution_contract_v1")).strip() or "live_execution_contract_v1",
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
        sample_count = int(stats.get("sample_count", 0) or 0)
        p_fill_lcb = _posterior_fill_lcb(p_fill=float(p_fill), sample_count=sample_count)
        shortfall_bps = max(_safe_optional_float(stats.get("mean_shortfall_bps")) or 0.0, 0.0)
        miss_cost_bps = _resolve_miss_cost_bps(
            miss_cost_model=miss_cost_model,
            state_key=state_key,
            action_code=action_code,
            fallback_expected_edge_bps=expected_edge_value,
        )
        utility_bps = (float(p_fill_lcb) * max(expected_edge_value - shortfall_bps, -expected_edge_value)) - (
            (1.0 - float(p_fill_lcb)) * miss_cost_bps
        )
        evaluation = {
            "action_code": action_code,
            "ord_type": str(config.get("ord_type")),
            "time_in_force": str(config.get("time_in_force")),
            "price_mode": str(config.get("price_mode")),
            "state_key": state_key,
            "stats_source": "state_action" if stats is state_stats and state_stats else ("action" if global_stats else "fallback"),
            "sample_count": int(sample_count),
            "p_fill_deadline": float(p_fill),
            "p_fill_deadline_lcb": float(p_fill_lcb),
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
    selected, selection_reason_code, urgency_override = _maybe_apply_canary_strong_edge_stage_override(
        current_state=current_state or {},
        expected_edge_bps=float(expected_edge_value),
        evaluated_actions=evaluated_actions,
        selected=selected,
    )
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


def _build_miss_cost_model(*, attempts: list[dict[str, Any]]) -> dict[str, Any]:
    by_action: dict[str, list[float]] = {}
    by_state_action: dict[tuple[str, str], list[float]] = {}
    for item in attempts:
        final_state = str(item.get("final_state") or "").strip().upper()
        if final_state not in {"MISSED", "PARTIAL_CANCELLED"}:
            continue
        action_code = _normalize_action_code(item.get("action_code"))
        state_key = state_bucket_key(item)
        miss_cost_bps = _safe_optional_float(item.get("expected_net_edge_bps"))
        if miss_cost_bps is None:
            miss_cost_bps = _safe_optional_float(item.get("expected_edge_bps"))
        if miss_cost_bps is None:
            continue
        by_action.setdefault(action_code, []).append(float(max(miss_cost_bps, 0.0)))
        by_state_action.setdefault((state_key, action_code), []).append(float(max(miss_cost_bps, 0.0)))
    return {
        "policy": "execution_miss_cost_summary_v1",
        "status": "ready" if by_action else "insufficient_history",
        "action_stats": {
            action_code: _summarize_cost_rows(costs)
            for action_code, costs in sorted(by_action.items())
        },
        "state_action_stats": {
            f"{state_key}|{action_code}": _summarize_cost_rows(costs)
            for (state_key, action_code), costs in sorted(by_state_action.items())
        },
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
    state_key_value = f"{state_key}|{action_code}"
    state_stats = dict(state_action_stats.get(state_key_value) or {})
    action_stats_row = dict(action_stats.get(action_code) or {})
    if int(state_stats.get("sample_count", 0) or 0) >= 3:
        value = _safe_optional_float(state_stats.get("mean_miss_cost_bps"))
        if value is not None:
            return float(max(value, 0.0))
    value = _safe_optional_float(action_stats_row.get("mean_miss_cost_bps"))
    if value is not None:
        return float(max(value, 0.0))
    return float(max(fallback_expected_edge_bps, 0.0))


def _posterior_fill_lcb(*, p_fill: float, sample_count: int, z_score: float = 1.28) -> float:
    count = max(int(sample_count), 0)
    if count <= 0:
        return 0.0
    mean = min(max(float(p_fill), 0.0), 1.0)
    variance = max(mean * (1.0 - mean), 0.0)
    stderr = (variance / float(max(count, 1))) ** 0.5
    return max(min(mean - (float(z_score) * stderr), 1.0), 0.0)


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
