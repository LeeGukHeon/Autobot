from __future__ import annotations

from typing import Any

from autobot.common.portfolio_signal_haircuts import resolve_portfolio_signal_haircuts

V5_POST_MODEL_CONTRACT_VERSION = "v5_post_model_contract_v1"
V5_ENTRY_OWNER = "predictor_boundary"
V5_SIZING_OWNER = "portfolio_budget_first"
V5_TRADE_ACTION_ROLE = "advisory_only_v1"
V5_EXIT_OWNER = "continuation_value_controller"
V5_ENTRY_GATE_ALPHA_LCB_REASON = "ENTRY_GATE_ALPHA_LCB_NOT_POSITIVE"
V5_ENTRY_GATE_EDGE_REASON = "ENTRY_GATE_EXPECTED_EDGE_NOT_POSITIVE_AFTER_COST"
V5_SIZING_NONPOSITIVE_REASON = "V5_TARGET_NOTIONAL_NONPOSITIVE"
V5_CONTINUATION_EXIT_REASON = "CONTINUATION_VALUE_EXIT"
V5_SAFETY_STOP_EXIT_REASON = "SAFETY_STOP_EXIT"
V5_STALE_TIMEOUT_EXIT_REASON = "STALE_TIMEOUT_EXIT"
V5_LIQUIDATION_EXECUTION_EXIT_REASON = "LIQUIDATION_EXECUTION_EXIT"


def is_v5_post_model_contract(runtime_recommendations: dict[str, Any] | None) -> bool:
    payload = dict(runtime_recommendations or {})
    return str(payload.get("decision_contract_version") or "").strip() == V5_POST_MODEL_CONTRACT_VERSION


def annotate_v5_runtime_recommendations(payload: dict[str, Any] | None) -> dict[str, Any]:
    normalized = dict(payload or {})
    normalized["decision_contract_version"] = V5_POST_MODEL_CONTRACT_VERSION
    normalized["entry_ownership"] = V5_ENTRY_OWNER
    normalized["sizing_ownership"] = V5_SIZING_OWNER
    normalized["trade_action_role"] = V5_TRADE_ACTION_ROLE
    normalized["exit_ownership"] = V5_EXIT_OWNER
    return normalized


def resolve_v5_entry_gate(
    *,
    market: str,
    final_expected_return: float | None,
    final_expected_es: float | None,
    final_tradability: float | None,
    final_uncertainty: float | None,
    final_alpha_lcb: float | None,
    entry_boundary_decision: dict[str, Any] | None,
    expected_net_edge_bps: float | None,
    portfolio_budget_allowed: bool = True,
    breaker_clear: bool = True,
    rollout_allowed: bool = True,
) -> dict[str, Any]:
    alpha_lcb = _safe_optional_float(final_alpha_lcb)
    expected_return = _safe_optional_float(final_expected_return)
    tradability = _safe_optional_float(final_tradability)
    uncertainty = _safe_optional_float(final_uncertainty)
    reason_codes: list[str] = []
    boundary = dict(entry_boundary_decision or {})
    if alpha_lcb is None or float(alpha_lcb) <= 0.0:
        reason_codes.append(V5_ENTRY_GATE_ALPHA_LCB_REASON)
    tradability_threshold = _safe_optional_float(boundary.get("tradability_threshold"))
    if (
        tradability_threshold is not None
        and tradability is not None
        and float(tradability) < float(tradability_threshold)
        and "ENTRY_BOUNDARY_TRADABILITY_BELOW_THRESHOLD" not in reason_codes
    ):
        reason_codes.append("ENTRY_BOUNDARY_TRADABILITY_BELOW_THRESHOLD")
    if bool(boundary.get("enabled")) and not bool(boundary.get("allowed")):
        for code in boundary.get("reason_codes") or []:
            normalized_code = str(code).strip()
            if normalized_code == "ENTRY_BOUNDARY_ALPHA_LCB_NOT_POSITIVE":
                continue
            if normalized_code and normalized_code not in reason_codes:
                reason_codes.append(normalized_code)
    resolved_edge_bps = _safe_optional_float(expected_net_edge_bps)
    if resolved_edge_bps is not None and float(resolved_edge_bps) <= 0.0:
        reason_codes.append(V5_ENTRY_GATE_EDGE_REASON)
    if not bool(portfolio_budget_allowed):
        reason_codes.append("ENTRY_GATE_PORTFOLIO_BUDGET_BLOCKED")
    if not bool(breaker_clear):
        reason_codes.append("ENTRY_GATE_BREAKER_ACTIVE")
    if not bool(rollout_allowed):
        reason_codes.append("ENTRY_GATE_ROLLOUT_BLOCKED")
    ranking_key = [
        -float(alpha_lcb if alpha_lcb is not None else float("-inf")),
        -float(expected_return if expected_return is not None else float("-inf")),
        float(uncertainty if uncertainty is not None else float("inf")),
        str(market).strip().upper(),
    ]
    return {
        "allowed": len(reason_codes) == 0,
        "reason_codes": reason_codes,
        "ranking_key": ranking_key,
        "selected_rank": None,
        "entry_owner": V5_ENTRY_OWNER,
        "decision_contract_version": V5_POST_MODEL_CONTRACT_VERSION,
        "expected_net_edge_bps": resolved_edge_bps,
        "final_expected_return": expected_return,
        "final_expected_es": _safe_optional_float(final_expected_es),
        "final_tradability": tradability,
        "final_uncertainty": uncertainty,
        "final_alpha_lcb": alpha_lcb,
        "primary_reason_code": reason_codes[0] if reason_codes else "",
        "primary_reason_family": _classify_primary_reason_family(reason_codes[0] if reason_codes else ""),
        "market": str(market).strip().upper(),
        "boundary_allowed": bool(boundary.get("allowed", True)) if boundary else True,
        "boundary_reason_codes": [
            str(code).strip() for code in (boundary.get("reason_codes") or []) if str(code).strip()
        ],
    }


def rank_v5_entry_candidates(candidate_payloads: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    rows = [dict(item) for item in (candidate_payloads or []) if isinstance(item, dict)]
    rows.sort(
        key=lambda item: (
            -float(_safe_optional_float(item.get("final_alpha_lcb")) or float("-inf")),
            -float(_safe_optional_float(item.get("final_expected_return")) or float("-inf")),
            float(_safe_optional_float(item.get("final_uncertainty")) or float("inf")),
            str(item.get("market", "")).strip().upper(),
        )
    )
    ranked: list[dict[str, Any]] = []
    for index, item in enumerate(rows, start=1):
        next_item = dict(item)
        next_item["selected_rank"] = int(index)
        ranked.append(next_item)
    return ranked


def build_v5_entry_decision_payload(
    *,
    gate_payload: dict[str, Any],
    selected_rank: int | None,
    legacy_selection_shadow: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = dict(gate_payload or {})
    payload["selected_rank"] = int(selected_rank) if selected_rank is not None else None
    payload["entry_owner"] = V5_ENTRY_OWNER
    payload["decision_contract_version"] = V5_POST_MODEL_CONTRACT_VERSION
    payload["legacy_selection_shadow"] = dict(legacy_selection_shadow or {})
    return payload


def finalize_v5_entry_decision(
    *,
    payload: dict[str, Any] | None,
    portfolio_budget_allowed: bool | None = None,
    breaker_clear: bool | None = None,
    rollout_allowed: bool | None = None,
    budget_reason_code: str | None = None,
    breaker_reason_codes: list[str] | None = None,
    rollout_reason_code: str | None = None,
) -> dict[str, Any]:
    normalized = dict(payload or {})
    reason_codes = [
        str(code).strip()
        for code in (normalized.get("reason_codes") or [])
        if str(code).strip()
    ]
    if portfolio_budget_allowed is not None:
        normalized["portfolio_budget_allowed"] = bool(portfolio_budget_allowed)
        if not bool(portfolio_budget_allowed):
            code = str(budget_reason_code or "ENTRY_GATE_PORTFOLIO_BUDGET_BLOCKED").strip()
            if code and code not in reason_codes:
                reason_codes.append(code)
    if breaker_clear is not None:
        normalized["breaker_clear"] = bool(breaker_clear)
        if not bool(breaker_clear):
            resolved_codes = [
                str(code).strip()
                for code in (breaker_reason_codes or ["ENTRY_GATE_BREAKER_ACTIVE"])
                if str(code).strip()
            ]
            for code in resolved_codes:
                if code not in reason_codes:
                    reason_codes.append(code)
    if rollout_allowed is not None:
        normalized["rollout_allowed"] = bool(rollout_allowed)
        if not bool(rollout_allowed):
            code = str(rollout_reason_code or "ENTRY_GATE_ROLLOUT_BLOCKED").strip()
            if code and code not in reason_codes:
                reason_codes.append(code)
    normalized["reason_codes"] = reason_codes
    normalized["allowed"] = len(reason_codes) == 0
    normalized["primary_reason_code"] = reason_codes[0] if reason_codes else ""
    normalized["primary_reason_family"] = _classify_primary_reason_family(reason_codes[0] if reason_codes else "")
    return normalized


def resolve_v5_target_notional(
    *,
    base_budget_quote: float | None,
    final_expected_return: float | None,
    final_expected_es: float | None,
    final_tradability: float | None,
    final_uncertainty: float | None,
    final_alpha_lcb: float | None,
    portfolio_remaining_budget_fraction: float = 1.0,
) -> dict[str, Any]:
    base_budget = max(_safe_optional_float(base_budget_quote) or 0.0, 0.0)
    expected_return_bps = _ratio_to_bps(final_expected_return)
    expected_es_bps = _ratio_to_bps(final_expected_es)
    alpha_lcb_bps = _ratio_to_bps(final_alpha_lcb)
    signal_haircuts = resolve_portfolio_signal_haircuts(
        uncertainty=final_uncertainty,
        expected_return_bps=expected_return_bps,
        expected_es_bps=expected_es_bps,
        tradability_prob=final_tradability,
        alpha_lcb_bps=alpha_lcb_bps,
    )
    portfolio_remaining = max(min(float(portfolio_remaining_budget_fraction), 1.0), 0.0)
    signal_multiplier = max(float(signal_haircuts.get("combined_haircut", 1.0) or 1.0), 0.0)
    requested_multiplier = max(signal_multiplier * portfolio_remaining, 0.0)
    target_notional_quote = (float(base_budget) * float(requested_multiplier)) if base_budget > 0.0 else None
    reason_codes = [str(code).strip() for code in (signal_haircuts.get("reason_codes") or []) if str(code).strip()]
    if float(requested_multiplier) <= 0.0 and V5_SIZING_NONPOSITIVE_REASON not in reason_codes:
        reason_codes.append(V5_SIZING_NONPOSITIVE_REASON)
    return {
        "decision_contract_version": V5_POST_MODEL_CONTRACT_VERSION,
        "sizing_owner": V5_SIZING_OWNER,
        "base_budget_quote": float(base_budget) if base_budget > 0.0 else None,
        "target_notional_quote": float(target_notional_quote) if target_notional_quote is not None else None,
        "requested_notional_multiplier": float(requested_multiplier),
        "resolved_notional_multiplier": float(requested_multiplier),
        "portfolio_remaining_budget_fraction": float(portfolio_remaining),
        "confidence_haircut": float(signal_haircuts.get("confidence_haircut", 1.0) or 1.0),
        "alpha_strength_haircut": float(signal_haircuts.get("alpha_strength_haircut", 1.0) or 1.0),
        "expected_es_haircut": float(signal_haircuts.get("expected_es_haircut", 1.0) or 1.0),
        "tradability_haircut": float(signal_haircuts.get("tradability_haircut", 1.0) or 1.0),
        "combined_signal_haircut": float(signal_multiplier),
        "reason_codes": reason_codes,
    }


def resolve_v5_exit_decision(
    *,
    continuation_guidance: dict[str, Any] | None,
    net_return_ratio: float | None,
    trailing_drawdown_ratio: float | None,
    stop_loss_ratio: float,
    trailing_ratio: float,
    timeout_elapsed: bool,
    mode: str,
) -> dict[str, Any]:
    guidance = dict(continuation_guidance or {})
    exit_now_value_net = _safe_optional_float(guidance.get("exit_now_value_net"))
    continue_value_net = _safe_optional_float(guidance.get("continue_value_net"))
    continue_value_lcb = _safe_optional_float(guidance.get("continue_value_lcb"))
    if continue_value_lcb is None:
        continue_value_lcb = continue_value_net
    alpha_decay_penalty = _safe_optional_float(guidance.get("alpha_decay_penalty_ratio"))
    expected_liquidation_cost = _safe_optional_float(guidance.get("immediate_exit_cost_ratio"))
    resolved_net_return = _safe_optional_float(net_return_ratio)
    if stop_loss_ratio > 0.0 and resolved_net_return is not None and float(resolved_net_return) <= -float(stop_loss_ratio):
        stop_breach_ratio = max(abs(float(resolved_net_return)) - float(stop_loss_ratio), 0.0)
        return {
            "should_exit": True,
            "decision_reason_code": V5_SAFETY_STOP_EXIT_REASON,
            "trigger_reason": "SL",
            "stop_breach_ratio": float(stop_breach_ratio),
            "mode": str(mode).strip().lower(),
            "exit_now_value_net": exit_now_value_net,
            "continue_value_net": continue_value_net,
            "continue_value_lcb": continue_value_lcb,
            "expected_liquidation_cost": expected_liquidation_cost,
            "alpha_decay_penalty": alpha_decay_penalty,
        }
    resolved_trailing = _safe_optional_float(trailing_drawdown_ratio)
    if trailing_ratio > 0.0 and resolved_trailing is not None and float(resolved_trailing) >= float(trailing_ratio):
        trailing_breach_ratio = max(float(resolved_trailing) - float(trailing_ratio), 0.0)
        return {
            "should_exit": True,
            "decision_reason_code": V5_SAFETY_STOP_EXIT_REASON,
            "trigger_reason": "TRAILING",
            "stop_breach_ratio": float(trailing_breach_ratio),
            "mode": str(mode).strip().lower(),
            "exit_now_value_net": exit_now_value_net,
            "continue_value_net": continue_value_net,
            "continue_value_lcb": continue_value_lcb,
            "expected_liquidation_cost": expected_liquidation_cost,
            "alpha_decay_penalty": alpha_decay_penalty,
        }
    if bool(timeout_elapsed):
        return {
            "should_exit": True,
            "decision_reason_code": V5_STALE_TIMEOUT_EXIT_REASON,
            "trigger_reason": "TIMEOUT",
            "stop_breach_ratio": 0.0,
            "mode": str(mode).strip().lower(),
            "exit_now_value_net": exit_now_value_net,
            "continue_value_net": continue_value_net,
            "continue_value_lcb": continue_value_lcb,
            "expected_liquidation_cost": expected_liquidation_cost,
            "alpha_decay_penalty": alpha_decay_penalty,
        }
    if (
        exit_now_value_net is not None
        and continue_value_lcb is not None
        and float(exit_now_value_net) > float(continue_value_lcb)
    ):
        return {
            "should_exit": True,
            "decision_reason_code": V5_CONTINUATION_EXIT_REASON,
            "trigger_reason": "PATH_RISK_CONTINUATION",
            "stop_breach_ratio": 0.0,
            "mode": str(mode).strip().lower(),
            "exit_now_value_net": exit_now_value_net,
            "continue_value_net": continue_value_net,
            "continue_value_lcb": continue_value_lcb,
            "expected_liquidation_cost": expected_liquidation_cost,
            "alpha_decay_penalty": alpha_decay_penalty,
        }
    return {
        "should_exit": False,
        "decision_reason_code": "",
        "trigger_reason": "",
        "stop_breach_ratio": 0.0,
        "mode": str(mode).strip().lower(),
        "exit_now_value_net": exit_now_value_net,
        "continue_value_net": continue_value_net,
        "continue_value_lcb": continue_value_lcb,
        "expected_liquidation_cost": expected_liquidation_cost,
        "alpha_decay_penalty": alpha_decay_penalty,
    }


def build_v5_liquidation_policy(
    *,
    exit_decision: dict[str, Any] | None,
    model_exit_plan: dict[str, Any] | None,
    execution_decision: dict[str, Any] | None = None,
    entry_price: float | None = None,
    qty: float | None = None,
    last_price: float | None = None,
    tick_size: float | None = None,
    ts_ms: int | None = None,
    created_ts_ms: int | None = None,
    trigger_ts_ms: int | None = None,
    breaker_action: str | None = None,
    micro_snapshot: Any | None = None,
    active_order_present: bool = False,
) -> dict[str, Any]:
    decision = dict(exit_decision or {})
    plan = dict(model_exit_plan or {})
    execution = dict(execution_decision or {})
    decision_reason_code = str(decision.get("decision_reason_code", "")).strip()
    resolved_entry_price = _safe_optional_float(entry_price)
    resolved_qty = _safe_optional_float(qty)
    resolved_last_price = _safe_optional_float(last_price)
    resolved_tick_size = _safe_optional_float(tick_size)
    resolved_ts_ms = _safe_optional_int(ts_ms)
    resolved_created_ts_ms = _safe_optional_int(created_ts_ms)
    resolved_trigger_ts_ms = _safe_optional_int(trigger_ts_ms)
    if (
        decision_reason_code
        and resolved_entry_price is not None
        and resolved_qty is not None
        and resolved_last_price is not None
        and resolved_tick_size is not None
        and resolved_ts_ms is not None
        and resolved_created_ts_ms is not None
    ):
        from autobot.risk.liquidation_policy import (
            protective_liquidation_policy_to_dict,
            resolve_protective_liquidation_policy,
        )

        liquidation_policy = resolve_protective_liquidation_policy(
            trigger_reason=str(decision.get("trigger_reason") or decision_reason_code).strip(),
            entry_price=float(resolved_entry_price),
            qty=float(max(resolved_qty, 0.0)),
            last_price=float(resolved_last_price),
            tick_size=float(max(resolved_tick_size, 1e-12)),
            base_exit_aggress_bps=max(_safe_optional_float(plan.get("expected_immediate_exit_slippage_bps")) or 8.0, 1.0),
            base_timeout_sec=max(int((_safe_optional_float(plan.get("expected_immediate_exit_time_to_fill_ms")) or 30_000) / 1000.0), 1),
            base_replace_max=1,
            ts_ms=int(resolved_ts_ms),
            created_ts_ms=int(resolved_created_ts_ms),
            trigger_ts_ms=int(resolved_trigger_ts_ms or resolved_ts_ms),
            breaker_action=str(breaker_action or "").strip() or None,
            micro_snapshot=micro_snapshot,
            active_order_present=bool(active_order_present),
            stop_breach_ratio=_safe_optional_float(decision.get("stop_breach_ratio")),
        )
        payload = protective_liquidation_policy_to_dict(liquidation_policy)
        payload["owner"] = "liquidation_execution_policy"
        payload["decision_reason_code"] = decision_reason_code
        payload["expected_liquidation_cost"] = _safe_optional_float(
            decision.get("expected_liquidation_cost", plan.get("expected_liquidation_cost"))
        )
        payload["exit_now_value_net"] = _safe_optional_float(decision.get("exit_now_value_net"))
        payload["continue_value_lcb"] = _safe_optional_float(decision.get("continue_value_lcb"))
        return payload

    if decision_reason_code == V5_SAFETY_STOP_EXIT_REASON:
        ord_type = "best"
        time_in_force = "ioc"
        price_mode = "CROSS_1T"
        timeout_ms = 15_000
        replace_interval_ms = 5_000
        max_replaces = 0
    elif decision_reason_code == V5_STALE_TIMEOUT_EXIT_REASON:
        ord_type = "limit"
        time_in_force = "ioc"
        price_mode = "JOIN"
        timeout_ms = 30_000
        replace_interval_ms = 15_000
        max_replaces = 1
    else:
        ord_type = "limit"
        time_in_force = "ioc"
        price_mode = str(
            plan.get("expected_immediate_exit_price_mode")
            or execution.get("selected_price_mode")
            or "JOIN"
        ).strip().upper()
        timeout_ms = int(_safe_optional_float(plan.get("expected_immediate_exit_time_to_fill_ms")) or 30_000)
        replace_interval_ms = max(int(timeout_ms // 2), 5_000)
        max_replaces = 1
    return {
        "owner": "liquidation_execution_policy",
        "decision_reason_code": decision_reason_code,
        "ord_type": str(ord_type),
        "time_in_force": str(time_in_force),
        "price_mode": str(price_mode),
        "timeout_ms": int(timeout_ms),
        "replace_interval_ms": int(replace_interval_ms),
        "max_replaces": int(max_replaces),
        "expected_liquidation_cost": _safe_optional_float(
            decision.get("expected_liquidation_cost", plan.get("expected_liquidation_cost"))
        ),
        "exit_now_value_net": _safe_optional_float(decision.get("exit_now_value_net")),
        "continue_value_lcb": _safe_optional_float(decision.get("continue_value_lcb")),
    }


def _ratio_to_bps(value: float | None) -> float | None:
    resolved = _safe_optional_float(value)
    if resolved is None:
        return None
    return float(resolved) * 10_000.0


def _safe_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _classify_primary_reason_family(reason_code: str) -> str:
    code = str(reason_code or "").strip().upper()
    if not code:
        return ""
    if code.startswith("ENTRY_GATE_"):
        return "entry_gate"
    if code.startswith("ENTRY_BOUNDARY_"):
        return "entry_boundary"
    return "safety_veto"
