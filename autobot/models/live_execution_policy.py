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
ACTION_BORROW_CHAINS: dict[str, tuple[str, ...]] = {
    "BEST_IOC": ("LIMIT_IOC_JOIN", "LIMIT_GTC_JOIN"),
    "BEST_FOK": ("LIMIT_FOK_JOIN", "LIMIT_IOC_JOIN", "LIMIT_GTC_JOIN"),
    "LIMIT_IOC_JOIN": ("LIMIT_GTC_JOIN",),
    "LIMIT_FOK_JOIN": ("LIMIT_IOC_JOIN", "LIMIT_GTC_JOIN"),
}
ACTION_PRIOR_CONFIGS: dict[str, dict[str, float]] = {
    "LIMIT_GTC_JOIN": {"p_fill": 0.42, "shortfall_bps": 1.5, "time_to_first_fill_ms": 15_000.0, "sample_count": 2.0, "miss_cost_scale": 0.90},
    "LIMIT_IOC_JOIN": {"p_fill": 0.58, "shortfall_bps": 3.5, "time_to_first_fill_ms": 5_000.0, "sample_count": 2.0, "miss_cost_scale": 0.70},
    "LIMIT_FOK_JOIN": {"p_fill": 0.38, "shortfall_bps": 3.5, "time_to_first_fill_ms": 4_000.0, "sample_count": 1.5, "miss_cost_scale": 0.75},
    "BEST_IOC": {"p_fill": 0.72, "shortfall_bps": 8.0, "time_to_first_fill_ms": 1_000.0, "sample_count": 2.0, "miss_cost_scale": 0.35},
    "BEST_FOK": {"p_fill": 0.48, "shortfall_bps": 8.0, "time_to_first_fill_ms": 750.0, "sample_count": 1.5, "miss_cost_scale": 0.50},
}


def build_live_execution_survival_model(
    *,
    attempts: list[dict[str, Any]] | None,
    horizons_ms: tuple[int, ...] = (1_000, 3_000, 10_000),
) -> dict[str, Any]:
    normalized_attempts = [dict(item) for item in (attempts or []) if isinstance(item, dict)]
    by_action: dict[str, list[dict[str, Any]]] = {}
    by_state_action: dict[tuple[str, str], list[dict[str, Any]]] = {}
    by_market_action: dict[tuple[str, str], list[dict[str, Any]]] = {}
    by_market_state_action: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for item in normalized_attempts:
        action_code = _normalize_action_code(item.get("action_code"))
        state_key = state_bucket_key(item)
        market = _normalize_market(item.get("market"))
        by_action.setdefault(action_code, []).append(item)
        by_state_action.setdefault((state_key, action_code), []).append(item)
        if market:
            by_market_action.setdefault((market, action_code), []).append(item)
            by_market_state_action.setdefault((market, state_key, action_code), []).append(item)
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
        "market_action_stats": {
            f"{market}|{action_code}": _summarize_attempt_rows(rows=rows, horizons_ms=horizons_ms)
            for (market, action_code), rows in sorted(by_market_action.items())
        },
        "market_state_action_stats": {
            f"{market}|{state_key}|{action_code}": _summarize_attempt_rows(rows=rows, horizons_ms=horizons_ms)
            for (market, state_key, action_code), rows in sorted(by_market_state_action.items())
        },
        "market_action_recent": _build_recent_action_stats(attempts=normalized_attempts, include_market=True),
        "action_recent": _build_recent_action_stats(attempts=normalized_attempts, include_market=False),
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
    market: str | None = None,
) -> dict[str, Any]:
    payload = dict(micro_state or {})
    if expected_edge_bps is not None:
        payload["expected_edge_bps"] = float(expected_edge_bps)
    market_value = _normalize_market(market or payload.get("market"))
    if market_value:
        payload["market"] = market_value
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
    expected_edge_value = max(float(expected_edge_bps or 0.0), 0.0)
    if int(contract.get("rows_total", 0) or 0) < 20:
        if expected_edge_value < 80.0:
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
    market = _normalize_market((current_state or {}).get("market"))
    action_stats = dict(model.get("action_stats") or {})
    state_action_stats = dict(model.get("state_action_stats") or {})
    market_action_stats = dict(model.get("market_action_stats") or {})
    market_state_action_stats = dict(model.get("market_state_action_stats") or {})
    action_recent = dict(model.get("action_recent") or {})
    market_action_recent = dict(model.get("market_action_recent") or {})
    evaluated_actions: list[dict[str, Any]] = []
    selected: dict[str, Any] | None = None
    for action_code in actions:
        config = dict(ACTION_CONFIGS.get(action_code) or ACTION_CONFIGS[DEFAULT_ACTION_CODE])
        state_stats = dict(state_action_stats.get(f"{state_key}|{action_code}") or {})
        global_stats = dict(action_stats.get(action_code) or {})
        market_stats = dict(market_action_stats.get(f"{market}|{action_code}") or {}) if market else {}
        market_state_stats = (
            dict(market_state_action_stats.get(f"{market}|{state_key}|{action_code}") or {}) if market else {}
        )
        stats, stats_source = _resolve_fill_stats(
            action_code=action_code,
            state_key=state_key,
            market=market,
            expected_edge_bps=expected_edge_value,
            market_state_stats=market_state_stats,
            state_stats=state_stats,
            market_stats=market_stats,
            global_stats=global_stats,
            market_state_action_stats=market_state_action_stats,
            state_action_stats=state_action_stats,
            market_action_stats=market_action_stats,
            action_stats=action_stats,
        )
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
            market=market,
            action_code=action_code,
            fallback_expected_edge_bps=expected_edge_value,
        )
        recent_stats = (
            dict(market_action_recent.get(f"{market}|{action_code}") or {})
            if market
            else dict(action_recent.get(action_code) or {})
        )
        p_fill_lcb, miss_cost_bps, recent_penalty = _apply_recent_miss_penalty(
            action_code=action_code,
            p_fill_lcb=float(p_fill_lcb),
            miss_cost_bps=float(miss_cost_bps),
            recent_stats=recent_stats,
        )
        utility_bps = (float(p_fill_lcb) * max(expected_edge_value - shortfall_bps, -expected_edge_value)) - (
            (1.0 - float(p_fill_lcb)) * miss_cost_bps
        )
        evaluation = {
            "action_code": action_code,
            "ord_type": str(config.get("ord_type")),
            "time_in_force": str(config.get("time_in_force")),
            "price_mode": str(config.get("price_mode")),
            "market": market,
            "state_key": state_key,
            "stats_source": stats_source,
            "sample_count": int(sample_count),
            "p_fill_deadline": float(p_fill),
            "p_fill_deadline_lcb": float(p_fill_lcb),
            "expected_time_to_first_fill_ms": _safe_optional_float(stats.get("mean_time_to_first_fill_ms")),
            "expected_shortfall_bps": float(shortfall_bps),
            "expected_edge_bps": float(expected_edge_value),
            "miss_cost_bps": float(miss_cost_bps),
            "utility_bps": float(utility_bps),
            "recent_penalty": recent_penalty,
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
        "selected_expected_miss_cost_bps": float(selected.get("miss_cost_bps", expected_edge_value) or expected_edge_value),
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
    by_market_action: dict[tuple[str, str], list[float]] = {}
    by_market_state_action: dict[tuple[str, str, str], list[float]] = {}
    for item in attempts:
        final_state = str(item.get("final_state") or "").strip().upper()
        if final_state not in {"MISSED", "PARTIAL_CANCELLED"}:
            continue
        action_code = _normalize_action_code(item.get("action_code"))
        state_key = state_bucket_key(item)
        market = _normalize_market(item.get("market"))
        miss_cost_bps = _safe_optional_float(item.get("expected_net_edge_bps"))
        if miss_cost_bps is None:
            miss_cost_bps = _safe_optional_float(item.get("expected_edge_bps"))
        if miss_cost_bps is None:
            continue
        normalized_cost = float(max(miss_cost_bps, 0.0))
        by_action.setdefault(action_code, []).append(normalized_cost)
        by_state_action.setdefault((state_key, action_code), []).append(normalized_cost)
        if market:
            by_market_action.setdefault((market, action_code), []).append(normalized_cost)
            by_market_state_action.setdefault((market, state_key, action_code), []).append(normalized_cost)
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
        "market_action_stats": {
            f"{market}|{action_code}": _summarize_cost_rows(costs)
            for (market, action_code), costs in sorted(by_market_action.items())
        },
        "market_state_action_stats": {
            f"{market}|{state_key}|{action_code}": _summarize_cost_rows(costs)
            for (market, state_key, action_code), costs in sorted(by_market_state_action.items())
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
    market: str | None,
    action_code: str,
    fallback_expected_edge_bps: float,
) -> float:
    state_action_stats = dict(miss_cost_model.get("state_action_stats") or {})
    action_stats = dict(miss_cost_model.get("action_stats") or {})
    market_action_stats = dict(miss_cost_model.get("market_action_stats") or {})
    market_state_action_stats = dict(miss_cost_model.get("market_state_action_stats") or {})
    state_key_value = f"{state_key}|{action_code}"
    state_stats = dict(state_action_stats.get(state_key_value) or {})
    action_stats_row = dict(action_stats.get(action_code) or {})
    market_state_stats = (
        dict(market_state_action_stats.get(f"{market}|{state_key}|{action_code}") or {}) if market else {}
    )
    market_stats_row = dict(market_action_stats.get(f"{market}|{action_code}") or {}) if market else {}
    if int(market_state_stats.get("sample_count", 0) or 0) >= 2:
        value = _safe_optional_float(market_state_stats.get("mean_miss_cost_bps"))
        if value is not None:
            return float(max(value, 0.0))
    if int(state_stats.get("sample_count", 0) or 0) >= 3:
        value = _safe_optional_float(state_stats.get("mean_miss_cost_bps"))
        if value is not None:
            return float(max(value, 0.0))
    if int(market_stats_row.get("sample_count", 0) or 0) >= 2:
        value = _safe_optional_float(market_stats_row.get("mean_miss_cost_bps"))
        if value is not None:
            return float(max(value, 0.0))
    value = _safe_optional_float(action_stats_row.get("mean_miss_cost_bps"))
    if value is not None:
        return float(max(value, 0.0))
    prior_config = dict(ACTION_PRIOR_CONFIGS.get(action_code) or {})
    prior_scale = _safe_optional_float(prior_config.get("miss_cost_scale"))
    if prior_scale is not None and float(fallback_expected_edge_bps) >= 80.0:
        return float(max(float(fallback_expected_edge_bps) * float(prior_scale), 0.0))
    return float(max(fallback_expected_edge_bps, 0.0))


def _posterior_fill_lcb(*, p_fill: float, sample_count: int, z_score: float = 1.28) -> float:
    count = max(int(sample_count), 0)
    if count <= 0:
        return 0.0
    mean = min(max(float(p_fill), 0.0), 1.0)
    variance = max(mean * (1.0 - mean), 0.0)
    stderr = (variance / float(max(count, 1))) ** 0.5
    return max(min(mean - (float(z_score) * stderr), 1.0), 0.0)


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


def _normalize_market(value: Any) -> str:
    text = str(value or "").strip().upper()
    return text


def _resolve_fill_stats(
    *,
    action_code: str,
    state_key: str,
    market: str | None,
    expected_edge_bps: float,
    market_state_stats: dict[str, Any],
    state_stats: dict[str, Any],
    market_stats: dict[str, Any],
    global_stats: dict[str, Any],
    market_state_action_stats: dict[str, Any],
    state_action_stats: dict[str, Any],
    market_action_stats: dict[str, Any],
    action_stats: dict[str, Any],
) -> tuple[dict[str, Any], str]:
    if int(market_state_stats.get("sample_count", 0) or 0) >= 2:
        return market_state_stats, "market_state_action"
    if int(state_stats.get("sample_count", 0) or 0) >= 3:
        return state_stats, "state_action"
    if int(market_stats.get("sample_count", 0) or 0) >= 2:
        return market_stats, "market_action"
    if int(global_stats.get("sample_count", 0) or 0) >= 1:
        return global_stats, "action"

    borrowed_stats, borrowed_source = _borrow_fill_stats(
        action_code=action_code,
        state_key=state_key,
        market=market,
        expected_edge_bps=expected_edge_bps,
        market_state_action_stats=market_state_action_stats,
        state_action_stats=state_action_stats,
        market_action_stats=market_action_stats,
        action_stats=action_stats,
    )
    if borrowed_stats:
        return borrowed_stats, borrowed_source

    prior_stats = _resolve_action_prior_stats(action_code=action_code, expected_edge_bps=expected_edge_bps)
    if prior_stats:
        return prior_stats, "action_prior"
    return {}, "fallback"


def _borrow_fill_stats(
    *,
    action_code: str,
    state_key: str,
    market: str | None,
    expected_edge_bps: float,
    market_state_action_stats: dict[str, Any],
    state_action_stats: dict[str, Any],
    market_action_stats: dict[str, Any],
    action_stats: dict[str, Any],
) -> tuple[dict[str, Any], str]:
    if float(expected_edge_bps) < 80.0:
        return {}, "fallback"
    for source_action_code in ACTION_BORROW_CHAINS.get(action_code, ()):
        candidates: list[tuple[str, dict[str, Any]]] = []
        if market:
            candidates.append(
                (
                    "market_state_action_borrowed",
                    dict(market_state_action_stats.get(f"{market}|{state_key}|{source_action_code}") or {}),
                )
            )
        candidates.append(("state_action_borrowed", dict(state_action_stats.get(f"{state_key}|{source_action_code}") or {})))
        if market:
            candidates.append(
                ("market_action_borrowed", dict(market_action_stats.get(f"{market}|{source_action_code}") or {}))
            )
        candidates.append(("action_borrowed", dict(action_stats.get(source_action_code) or {})))
        for source_name, stats in candidates:
            if int(stats.get("sample_count", 0) or 0) <= 0:
                continue
            transformed = _transform_borrowed_fill_stats(
                source_action_code=source_action_code,
                target_action_code=action_code,
                stats=stats,
            )
            if transformed:
                return transformed, source_name
    return {}, "fallback"


def _transform_borrowed_fill_stats(
    *,
    source_action_code: str,
    target_action_code: str,
    stats: dict[str, Any],
) -> dict[str, Any]:
    p_fill = _safe_optional_float(stats.get("p_fill_within_default"))
    shortfall_bps = max(_safe_optional_float(stats.get("mean_shortfall_bps")) or 0.0, 0.0)
    time_to_first_fill_ms = _safe_optional_float(stats.get("mean_time_to_first_fill_ms"))
    sample_count = int(stats.get("sample_count", 0) or 0)
    if p_fill is None or sample_count <= 0:
        return {}
    p_fill_bonus = 0.0
    shortfall_add = 0.0
    time_scale = 1.0
    if target_action_code == "LIMIT_IOC_JOIN":
        p_fill_bonus = 0.08
        shortfall_add = 2.0
        time_scale = 0.5
    elif target_action_code == "LIMIT_FOK_JOIN":
        p_fill_bonus = 0.02
        shortfall_add = 2.5
        time_scale = 0.4
    elif target_action_code == "BEST_IOC":
        p_fill_bonus = 0.18
        shortfall_add = 8.0
        time_scale = 0.1
    elif target_action_code == "BEST_FOK":
        p_fill_bonus = 0.08
        shortfall_add = 8.0
        time_scale = 0.08
    transformed = dict(stats)
    transformed["p_fill_within_default"] = min(max(float(p_fill) + p_fill_bonus, 0.0), 0.99)
    for key, value in list(stats.items()):
        if key.startswith("p_fill_within_"):
            base = _safe_optional_float(value)
            if base is not None:
                transformed[key] = min(max(float(base) + p_fill_bonus, 0.0), 0.99)
    transformed["mean_shortfall_bps"] = float(shortfall_bps + shortfall_add)
    if time_to_first_fill_ms is not None:
        transformed["mean_time_to_first_fill_ms"] = max(float(time_to_first_fill_ms) * time_scale, 250.0)
    transformed["sample_count"] = max(int(sample_count // 2), 1)
    transformed["borrowed_from_action_code"] = source_action_code
    return transformed


def _resolve_action_prior_stats(*, action_code: str, expected_edge_bps: float) -> dict[str, Any]:
    if float(expected_edge_bps) < 80.0:
        return {}
    config = dict(ACTION_PRIOR_CONFIGS.get(action_code) or {})
    if not config:
        return {}
    sample_count = int(max(float(config.get("sample_count", 0.0)), 0.0))
    if sample_count <= 0:
        return {}
    return {
        "sample_count": sample_count,
        "mean_time_to_first_fill_ms": float(config.get("time_to_first_fill_ms", 0.0) or 0.0),
        "mean_shortfall_bps": float(config.get("shortfall_bps", 0.0) or 0.0),
        "p_fill_within_default": float(config.get("p_fill", 0.0) or 0.0),
        "p_fill_within_1000ms": float(config.get("p_fill", 0.0) or 0.0),
        "p_fill_within_3000ms": float(config.get("p_fill", 0.0) or 0.0),
        "p_fill_within_10000ms": float(config.get("p_fill", 0.0) or 0.0),
    }


def _build_recent_action_stats(*, attempts: list[dict[str, Any]], include_market: bool, recent_limit: int = 5) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    ordered_attempts = sorted(
        [dict(item) for item in attempts if isinstance(item, dict)],
        key=lambda item: (int(item.get("submitted_ts_ms") or 0), int(item.get("updated_ts") or 0)),
        reverse=True,
    )
    for item in ordered_attempts:
        action_code = _normalize_action_code(item.get("action_code"))
        if include_market:
            market = _normalize_market(item.get("market"))
            if not market:
                continue
            key = f"{market}|{action_code}"
        else:
            key = action_code
        bucket = grouped.setdefault(key, [])
        if len(bucket) < max(int(recent_limit), 1):
            bucket.append(item)
    payload: dict[str, Any] = {}
    for key, rows in grouped.items():
        consecutive_misses = 0
        for row in rows:
            if _attempt_has_fill(row):
                break
            consecutive_misses += 1
        fills = sum(1 for row in rows if _attempt_has_fill(row))
        misses = sum(1 for row in rows if str(row.get("final_state") or "").strip().upper() in {"MISSED", "PARTIAL_CANCELLED"})
        payload[key] = {
            "sample_count": len(rows),
            "fill_count": fills,
            "miss_count": misses,
            "recent_fill_ratio": (float(fills) / float(len(rows))) if rows else 0.0,
            "consecutive_misses": consecutive_misses,
        }
    return payload


def _apply_recent_miss_penalty(
    *,
    action_code: str,
    p_fill_lcb: float,
    miss_cost_bps: float,
    recent_stats: dict[str, Any] | None,
) -> tuple[float, float, dict[str, Any]]:
    stats = dict(recent_stats or {})
    sample_count = int(stats.get("sample_count", 0) or 0)
    consecutive_misses = int(stats.get("consecutive_misses", 0) or 0)
    recent_fill_ratio = float(stats.get("recent_fill_ratio", 0.0) or 0.0)
    adjusted_p_fill_lcb = float(p_fill_lcb)
    adjusted_miss_cost_bps = float(miss_cost_bps)
    if sample_count > 0:
        if consecutive_misses >= 2:
            adjusted_p_fill_lcb *= 0.35 if action_code == "LIMIT_GTC_PASSIVE_MAKER" else 0.60
            adjusted_miss_cost_bps *= 1.25
        elif sample_count >= 3 and recent_fill_ratio <= 0.20:
            adjusted_p_fill_lcb *= 0.70
    return adjusted_p_fill_lcb, adjusted_miss_cost_bps, {
        "sample_count": sample_count,
        "consecutive_misses": consecutive_misses,
        "recent_fill_ratio": recent_fill_ratio,
    }


def _attempt_has_fill(row: dict[str, Any]) -> bool:
    final_state = str(row.get("final_state") or "").strip().upper()
    if final_state == "FILLED":
        return True
    return row.get("first_fill_ts_ms") is not None or row.get("full_fill_ts_ms") is not None
