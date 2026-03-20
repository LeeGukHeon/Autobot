"""Execution and submission helpers for live model_alpha runtime."""

from __future__ import annotations

from dataclasses import replace
import json
import math
from typing import Any, Callable

from autobot.execution.order_supervisor import OrderExecProfile, normalize_order_exec_profile
from autobot.models.execution_risk_control import (
    resolve_execution_risk_control_decision,
    resolve_execution_risk_control_martingale_state,
    resolve_execution_risk_control_online_state,
    resolve_execution_risk_control_size_decision,
)

CANARY_ENTRY_TIMEOUT_CAP_MS = 180_000


def record_strategy_intent(
    *,
    store: Any,
    market: str,
    side: str,
    price: float | None,
    volume: float | None,
    reason_code: str,
    meta_payload: dict[str, Any],
    status: str,
    ts_ms: int,
    intent_id: str | None = None,
    intent_record_cls: type,
) -> str:
    resolved_intent_id = str(intent_id or f"live-{market}-{side}-{ts_ms}").strip()
    store.upsert_intent(
        intent_record_cls(
            intent_id=resolved_intent_id,
            ts_ms=int(ts_ms),
            market=market,
            side=side,
            price=price,
            volume=volume,
            reason_code=reason_code,
            meta_json=json.dumps(meta_payload, ensure_ascii=False, sort_keys=True),
            status=str(status).strip().upper(),
        )
    )
    return resolved_intent_id


def handle_submit_reject(
    *,
    store: Any,
    intent: Any,
    ts_ms: int,
    market: str,
    side: str,
    reason_code: str,
    meta_payload: dict[str, Any],
    reject_reason: str,
    classify_executor_reject_reason_fn: Callable[[str], str],
    record_counter_failure_fn: Callable[..., Any],
    arm_breaker_fn: Callable[..., Any],
    action_full_kill_switch: str,
    record_strategy_intent_fn: Callable[..., str],
) -> None:
    classified = classify_executor_reject_reason_fn(reject_reason)
    if classified == "REPEATED_RATE_LIMIT_ERRORS":
        record_counter_failure_fn(
            store,
            counter_name="rate_limit_error",
            limit=3,
            source="live_model_alpha_submit",
            ts_ms=ts_ms,
            details={"market": market, "reason": reject_reason},
        )
    elif classified == "REPEATED_AUTH_ERRORS":
        record_counter_failure_fn(
            store,
            counter_name="auth_error",
            limit=2,
            source="live_model_alpha_submit",
            ts_ms=ts_ms,
            details={"market": market, "reason": reject_reason},
        )
    elif classified == "REPEATED_NONCE_ERRORS":
        record_counter_failure_fn(
            store,
            counter_name="nonce_error",
            limit=2,
            source="live_model_alpha_submit",
            ts_ms=ts_ms,
            details={"market": market, "reason": reject_reason},
        )
    elif classified == "IDENTIFIER_COLLISION":
        arm_breaker_fn(
            store,
            reason_codes=["IDENTIFIER_COLLISION"],
            source="live_model_alpha_submit",
            ts_ms=ts_ms,
            action=action_full_kill_switch,
            details={"market": market, "reason": reject_reason},
        )
    record_strategy_intent_fn(
        store=store,
        market=market,
        side=side,
        price=float(intent.price),
        volume=float(intent.volume),
        reason_code=reason_code,
        meta_payload={**meta_payload, "submit_result": {"accepted": False, "reason": reject_reason}},
        status="REJECTED",
        ts_ms=ts_ms,
        intent_id=str(intent.intent_id),
    )
    store.set_checkpoint(
        name="live_model_alpha_last_reject",
        payload={
            "intent_id": str(intent.intent_id),
            "market": market,
            "side": side,
            "reason": reject_reason,
            "classified_reject": classified,
        },
        ts_ms=ts_ms,
    )


def apply_canary_notional_cap(
    *,
    store: Any,
    settings: Any,
    target_notional_quote: float,
    safe_optional_float_fn: Callable[[object], float | None],
) -> float:
    resolved_target = max(float(target_notional_quote), 0.0)
    if not _is_canary_rollout(store=store, settings=settings):
        return resolved_target
    rollout_contract = store.live_rollout_contract() or {}
    cap_value = safe_optional_float_fn(rollout_contract.get("canary_max_notional_quote"))
    if cap_value is None or cap_value <= 0.0:
        return resolved_target
    return min(resolved_target, float(cap_value))


def _is_canary_rollout(*, store: Any, settings: Any) -> bool:
    return _matches_rollout_mode(store=store, settings=settings, allowed_modes={"canary"})


def _is_active_live_rollout(*, store: Any, settings: Any) -> bool:
    return _matches_rollout_mode(store=store, settings=settings, allowed_modes={"canary", "live"})


def _matches_rollout_mode(*, store: Any, settings: Any, allowed_modes: set[str]) -> bool:
    rollout_contract = store.live_rollout_contract() or {}
    rollout_status = store.live_rollout_status() or {}
    rollout_mode = (
        str(rollout_status.get("mode") or rollout_contract.get("mode") or settings.daemon.rollout_mode)
        .strip()
        .lower()
    )
    if rollout_mode not in {str(item).strip().lower() for item in allowed_modes}:
        return False
    contract_target_unit = str(rollout_contract.get("target_unit") or "").strip()
    if contract_target_unit and contract_target_unit != str(settings.daemon.rollout_target_unit).strip():
        return False
    return True


def apply_canary_entry_timeout_cap(
    *,
    store: Any,
    settings: Any,
    side: str,
    exec_profile: OrderExecProfile,
) -> OrderExecProfile:
    if str(side).strip().lower() != "bid":
        return exec_profile
    if not _is_active_live_rollout(store=store, settings=settings):
        return exec_profile
    timeout_cap_ms = max(int(CANARY_ENTRY_TIMEOUT_CAP_MS), 1)
    normalized = normalize_order_exec_profile(exec_profile)
    if int(normalized.timeout_ms) <= timeout_cap_ms and int(normalized.replace_interval_ms) <= timeout_cap_ms:
        return normalized
    return normalize_order_exec_profile(
        replace(
            normalized,
            timeout_ms=min(int(normalized.timeout_ms), timeout_cap_ms),
            replace_interval_ms=max(
                min(int(normalized.replace_interval_ms), timeout_cap_ms),
                int(normalized.min_replace_interval_ms_global),
            ),
        )
    )


def strategy_live_exec_profile(
    *,
    settings: Any,
    model_alpha_settings: Any,
    interval_ms_from_tf_fn: Callable[[str], int],
    make_legacy_exec_profile_fn: Callable[..., Any],
) -> Any:
    interval_ms = interval_ms_from_tf_fn(settings.tf)
    timeout_ms = max(int(model_alpha_settings.execution.timeout_bars), 1) * interval_ms
    return make_legacy_exec_profile_fn(
        timeout_ms=timeout_ms,
        replace_interval_ms=timeout_ms,
        max_replaces=max(int(model_alpha_settings.execution.replace_max), 0),
        price_mode=str(model_alpha_settings.execution.price_mode),
        max_chase_bps=10_000,
        min_replace_interval_ms_global=1_500,
    )


def effective_live_trade_gate_max_positions(settings: Any) -> int:
    max_positions = max(int(settings.max_positions), 1)
    if bool(settings.daemon.small_account_canary_enabled):
        max_positions = min(max_positions, max(int(settings.daemon.small_account_max_positions), 1))
    return max_positions


def order_emission_allowed(store: Any, *, new_intents_allowed_fn: Callable[[Any], bool]) -> bool:
    rollout_status = store.live_rollout_status() or {}
    return bool(rollout_status.get("order_emission_allowed")) and bool(new_intents_allowed_fn(store))


def resolve_live_expected_edge_bps(
    meta_payload: dict[str, Any] | None,
    *,
    safe_optional_float_fn: Callable[[object], float | None],
) -> float | None:
    if not isinstance(meta_payload, dict):
        return None
    strategy_meta = (
        ((meta_payload.get("strategy") or {}).get("meta"))
        if isinstance(meta_payload.get("strategy"), dict)
        else None
    )
    if not isinstance(strategy_meta, dict):
        return None
    trade_action = strategy_meta.get("trade_action")
    if not isinstance(trade_action, dict):
        return None
    raw_edge = safe_optional_float_fn(trade_action.get("expected_edge"))
    if raw_edge is None:
        return None
    return float(raw_edge) * 10_000.0


def resolve_live_trade_action(
    meta_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    if not isinstance(meta_payload, dict):
        return {}
    strategy_meta = (
        ((meta_payload.get("strategy") or {}).get("meta"))
        if isinstance(meta_payload.get("strategy"), dict)
        else None
    )
    if not isinstance(strategy_meta, dict):
        return {}
    trade_action = strategy_meta.get("trade_action")
    return dict(trade_action) if isinstance(trade_action, dict) else {}


def resolve_execution_risk_control_online_threshold(
    *,
    store: Any,
    run_id: str,
    risk_control_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    payload = dict(risk_control_payload or {})
    online_adaptation = dict(payload.get("online_adaptation") or {})
    if (
        str(payload.get("status", "")).strip().lower() != "ready"
        or str(payload.get("contract_status", "")).strip().lower() == "invalid"
        or not bool(online_adaptation.get("enabled", False))
    ):
        return {
            "enabled": False,
            "base_threshold": _safe_optional_float(payload.get("selected_threshold")),
            "adaptive_threshold": _safe_optional_float(payload.get("selected_threshold")),
            "checkpoint_name": _execution_risk_control_checkpoint_name(
                base_name=str(online_adaptation.get("checkpoint_name") or "execution_risk_control_online_buffer"),
                run_id=run_id,
            ),
            "checkpoint_base_name": str(
                online_adaptation.get("checkpoint_name") or "execution_risk_control_online_buffer"
            ).strip()
            or "execution_risk_control_online_buffer",
        }
    checkpoint_name = str(
        online_adaptation.get("checkpoint_name") or "execution_risk_control_online_buffer"
    ).strip() or "execution_risk_control_online_buffer"
    resolved_checkpoint_name = _execution_risk_control_checkpoint_name(
        base_name=checkpoint_name,
        run_id=run_id,
    )
    previous_checkpoint = store.get_checkpoint(name=resolved_checkpoint_name)
    previous_state = dict((previous_checkpoint or {}).get("payload") or {})
    lookback_trades = max(int(online_adaptation.get("lookback_trades", 0) or 0), 1)
    delta = max(float(online_adaptation.get("confidence_delta", 0.0) or 0.0), 1e-12)
    rows = []
    for row in store.list_trade_journal(statuses=("CLOSED",)):
        entry_meta = dict(row.get("entry_meta") or {})
        runtime = dict(entry_meta.get("runtime") or {}) if isinstance(entry_meta.get("runtime"), dict) else {}
        if str(runtime.get("live_runtime_model_run_id", "")).strip() != str(run_id).strip():
            continue
        exit_meta = dict(row.get("exit_meta") or {})
        if exit_meta.get("close_verified") is not True:
            continue
        pnl_pct = _safe_optional_float(row.get("realized_pnl_pct"))
        exit_ts_ms = _safe_optional_int(row.get("exit_ts_ms")) or _safe_optional_int(row.get("updated_ts"))
        if pnl_pct is None or exit_ts_ms is None:
            continue
        rows.append({"pnl_pct": float(pnl_pct), "exit_ts_ms": int(exit_ts_ms)})
    rows.sort(key=lambda item: int(item["exit_ts_ms"]), reverse=True)
    recent = rows[:lookback_trades]
    severe_threshold = max(float(payload.get("severe_loss_return_threshold", 0.0) or 0.0), 0.0)
    count = len(recent)
    if count <= 0:
        base_state = resolve_execution_risk_control_online_state(
            risk_control_payload=payload,
            previous_state=previous_state,
            recent_trade_count=0,
            recent_nonpositive_rate_ucb=0.0,
            recent_severe_loss_rate_ucb=0.0,
        )
        martingale_state = resolve_execution_risk_control_martingale_state(
            risk_control_payload=payload,
            previous_state=previous_state,
            observations=[],
        )
        merged = _merge_online_risk_states(base_state=base_state, martingale_state=martingale_state)
        merged["checkpoint_name"] = resolved_checkpoint_name
        merged["checkpoint_base_name"] = checkpoint_name
        return merged
    nonpositive_rate = sum(1 for item in recent if float(item["pnl_pct"]) <= 0.0) / float(count)
    severe_rate = sum(1 for item in recent if float(item["pnl_pct"]) <= -float(severe_threshold)) / float(count)
    nonpositive_ucb = _hoeffding_ucb_rate(nonpositive_rate, count, delta)
    severe_ucb = _hoeffding_ucb_rate(severe_rate, count, delta)
    base_state = resolve_execution_risk_control_online_state(
        risk_control_payload=payload,
        previous_state=previous_state,
        recent_trade_count=int(count),
        recent_nonpositive_rate_ucb=float(nonpositive_ucb),
        recent_severe_loss_rate_ucb=float(severe_ucb),
        recent_max_exit_ts_ms=max((int(item["exit_ts_ms"]) for item in recent), default=None),
    )
    base_state["recent_nonpositive_rate"] = float(nonpositive_rate)
    base_state["recent_severe_loss_rate"] = float(severe_rate)
    martingale_state = resolve_execution_risk_control_martingale_state(
        risk_control_payload=payload,
        previous_state=previous_state,
        observations=recent,
    )
    merged = _merge_online_risk_states(base_state=base_state, martingale_state=martingale_state)
    merged["checkpoint_name"] = resolved_checkpoint_name
    merged["checkpoint_base_name"] = checkpoint_name
    return merged


def _execution_risk_control_checkpoint_name(*, base_name: str, run_id: str) -> str:
    normalized_base = str(base_name).strip() or "execution_risk_control_online_buffer"
    normalized_run_id = str(run_id).strip()
    if not normalized_run_id:
        return normalized_base
    return f"{normalized_base}:{normalized_run_id}"


def _hoeffding_ucb_rate(empirical_rate: float, sample_count: int, delta: float) -> float:
    if sample_count <= 0:
        return 1.0
    rate = min(max(float(empirical_rate), 0.0), 1.0)
    bonus = math.sqrt(max(math.log(1.0 / max(float(delta), 1e-12)), 0.0) / (2.0 * float(sample_count)))
    return min(rate + bonus, 1.0)


def _safe_optional_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_optional_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _merge_online_risk_states(*, base_state: dict[str, Any], martingale_state: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base_state)
    merged.update(dict(martingale_state))
    base_halt = bool(base_state.get("halt_triggered"))
    martingale_halt = bool(martingale_state.get("martingale_halt_triggered"))
    merged["halt_triggered"] = bool(base_halt or martingale_halt)
    halt_reason_codes: list[str] = []
    if martingale_halt:
        martingale_reason = str(martingale_state.get("martingale_halt_reason_code", "")).strip()
        if martingale_reason:
            halt_reason_codes.append(martingale_reason)
    if base_halt:
        base_reason = str(base_state.get("halt_reason_code", "")).strip()
        if base_reason and base_reason not in halt_reason_codes:
            halt_reason_codes.append(base_reason)
    if martingale_halt:
        merged["halt_reason_code"] = str(martingale_state.get("martingale_halt_reason_code", "")).strip()
        merged["halt_action"] = str(martingale_state.get("martingale_halt_action", "")).strip() or "HALT_NEW_INTENTS"
    elif base_halt:
        merged["halt_reason_code"] = str(base_state.get("halt_reason_code", "")).strip()
        merged["halt_action"] = str(base_state.get("halt_action", "")).strip() or "HALT_NEW_INTENTS"
    else:
        merged["halt_reason_code"] = str(base_state.get("halt_reason_code", "")).strip()
        merged["halt_action"] = str(base_state.get("halt_action", "")).strip()
    merged["halt_reason_codes"] = halt_reason_codes
    merged["clear_halt"] = bool(base_state.get("clear_halt")) or bool(martingale_state.get("martingale_clear_halt"))
    clear_reason_codes: list[str] = []
    for value in list(base_state.get("clear_reason_codes") or []) + list(
        martingale_state.get("martingale_clear_reason_codes") or []
    ):
        reason_code = str(value).strip()
        if reason_code and reason_code not in clear_reason_codes:
            clear_reason_codes.append(reason_code)
    merged["clear_reason_codes"] = clear_reason_codes
    return merged


def resolve_live_strategy_execution(
    *,
    market: str,
    side: str,
    settings: Any,
    model_alpha_settings: Any,
    strategy_intent: Any,
    snapshot: Any,
    latest_trade_price: float,
    store: Any,
    exchange_view: Any,
    ts_ms: int,
    micro_snapshot_provider: Any,
    micro_order_policy: Any,
    trade_gate: Any,
    risk_control_payload: dict[str, Any] | None,
    resolution_cls: type,
    safe_optional_float_fn: Callable[[object], float | None],
    safe_float_fn: Callable[..., float],
    resolve_execution_risk_control_size_decision_fn: Callable[..., dict[str, Any]],
    strategy_live_exec_profile_fn: Callable[..., Any],
    entry_notional_quote_for_strategy_fn: Callable[..., float],
    apply_canary_notional_cap_fn: Callable[..., float],
    resolve_operational_execution_overlay_fn: Callable[..., Any],
    compute_micro_quality_composite_fn: Callable[..., Any],
    order_exec_profile_to_dict_fn: Callable[[Any], dict[str, Any]],
    round_price_to_tick_fn: Callable[..., float],
    build_limit_price_from_mode_fn: Callable[..., float],
    derive_volume_from_target_notional_fn: Callable[..., Any],
    sizing_envelope_to_payload_fn: Callable[[Any], dict[str, Any]],
) -> Any:
    strategy_meta = dict(strategy_intent.meta or {})
    trade_action_payload = (
        dict(strategy_meta.get("trade_action") or {})
        if isinstance(strategy_meta.get("trade_action"), dict)
        else {}
    )
    size_ladder_decision = resolve_execution_risk_control_size_decision_fn(
        risk_control_payload=risk_control_payload,
        trade_action=trade_action_payload,
        requested_multiplier=safe_optional_float_fn(strategy_meta.get("notional_multiplier")),
    )
    if bool(size_ladder_decision.get("enabled")):
        resolved_multiplier = safe_optional_float_fn(size_ladder_decision.get("resolved_multiplier"))
        if resolved_multiplier is None or resolved_multiplier <= 0.0:
            return resolution_cls(
                allowed=False,
                skip_reason=str(size_ladder_decision.get("reason_code", "SIZE_LADDER_NO_ADMISSIBLE_MULTIPLIER")),
                requested_price=float(strategy_intent.ref_price),
                requested_volume=safe_optional_float_fn(strategy_intent.volume),
                sizing_payload=None,
                meta_payload={
                    "strategy": {
                        "market": market,
                        "side": side,
                        "reason_code": str(strategy_intent.reason_code),
                        "score": strategy_intent.score,
                        "prob": strategy_intent.prob,
                        "meta": strategy_meta,
                    },
                    "size_ladder": size_ladder_decision,
                },
            )
        strategy_meta["notional_multiplier"] = float(resolved_multiplier)
        strategy_meta["notional_multiplier_source"] = "risk_control_size_ladder"
        if trade_action_payload:
            trade_action_payload["recommended_notional_multiplier"] = float(resolved_multiplier)
            strategy_meta["trade_action"] = trade_action_payload
    initial_ref_price = max(float(strategy_intent.ref_price), 1e-12)
    effective_ref_price = max(initial_ref_price, float(latest_trade_price), 1e-12)
    snapshot_for_policy = micro_snapshot_provider.get(market, int(ts_ms))
    exec_profile = strategy_live_exec_profile_fn(
        settings=settings,
        model_alpha_settings=model_alpha_settings,
    )
    operational_payload: dict[str, Any] = {}
    trade_gate_payload: dict[str, Any] = {"enabled": True}
    forced_volume = safe_optional_float_fn(strategy_meta.get("force_volume"))
    local_position = store.position_by_market(market=market) if side == "ask" else None
    entry_notional_quote = (
        entry_notional_quote_for_strategy_fn(
            strategy_mode="model_alpha_v1",
            per_trade_krw=float(settings.per_trade_krw),
            min_total_krw=max(float(snapshot.min_total), float(settings.min_order_krw)),
            model_alpha_settings=model_alpha_settings,
            candidate_meta=strategy_meta,
        )
        if side == "bid" and (forced_volume is None or forced_volume <= 0)
        else None
    )
    if entry_notional_quote is not None:
        entry_notional_quote = apply_canary_notional_cap_fn(
            store=store,
            settings=settings,
            target_notional_quote=float(entry_notional_quote),
        )

    if bool(model_alpha_settings.operational.enabled):
        operational_decision = resolve_operational_execution_overlay_fn(
            base_profile=exec_profile,
            settings=model_alpha_settings.operational,
            micro_quality=compute_micro_quality_composite_fn(
                micro_snapshot=snapshot_for_policy,
                now_ts_ms=ts_ms,
                settings=model_alpha_settings.operational,
            ),
            ts_ms=ts_ms,
        )
        operational_payload = {
            "runtime_risk_multiplier": float(operational_decision.risk_multiplier),
            "exec_overlay_mode": str(operational_decision.diagnostics.get("mode", "neutral")),
            "micro_quality_score": (
                float(operational_decision.micro_quality.score)
                if operational_decision.micro_quality is not None
                else None
            ),
            "diagnostics": dict(operational_decision.diagnostics),
        }
        if operational_decision.abort_reason is not None:
            return resolution_cls(
                allowed=False,
                skip_reason=str(operational_decision.abort_reason),
                requested_price=effective_ref_price,
                requested_volume=safe_optional_float_fn(strategy_intent.volume),
                sizing_payload=None,
                meta_payload={
                    "strategy": {
                        "market": market,
                        "side": side,
                        "reason_code": str(strategy_intent.reason_code),
                        "score": strategy_intent.score,
                        "prob": strategy_intent.prob,
                        "meta": strategy_meta,
                    },
                    "size_ladder": size_ladder_decision,
                    "execution": {
                        "initial_ref_price": float(initial_ref_price),
                        "latest_trade_price": float(latest_trade_price),
                        "effective_ref_price": float(effective_ref_price),
                        "exec_profile": order_exec_profile_to_dict_fn(exec_profile),
                    },
                    "operational_overlay": operational_payload,
                },
            )
        exec_profile = operational_decision.exec_profile
        if entry_notional_quote is not None:
            entry_notional_quote *= max(float(operational_decision.risk_multiplier), 0.0)
    exec_profile = apply_canary_entry_timeout_cap(
        store=store,
        settings=settings,
        side=side,
        exec_profile=exec_profile,
    )

    if side == "ask":
        if local_position is None:
            return resolution_cls(
                allowed=False,
                skip_reason="NO_LOCAL_POSITION",
                requested_price=effective_ref_price,
                requested_volume=None,
                sizing_payload=None,
                meta_payload={
                    "strategy": {"market": market, "side": side, "meta": strategy_meta},
                    "size_ladder": size_ladder_decision,
                },
            )
        live_exit_plan_present = any(
            str(item.get("state", "")).strip().upper() == "EXITING"
            or bool(
                str(item.get("current_exit_order_uuid") or "").strip()
                or str(item.get("current_exit_order_identifier") or "").strip()
            )
            for item in store.list_risk_plans(market=market, states=("ACTIVE", "TRIGGERED", "EXITING"))
        )
        if live_exit_plan_present:
            return resolution_cls(
                allowed=False,
                skip_reason="DUPLICATE_EXIT_ORDER",
                requested_price=effective_ref_price,
                requested_volume=None,
                sizing_payload=None,
                meta_payload={
                    "strategy": {"market": market, "side": side, "meta": strategy_meta},
                    "size_ladder": size_ladder_decision,
                    "trade_gate": {
                        **trade_gate_payload,
                        "reason_code": "DUPLICATE_EXIT_ORDER",
                        "severity": "BLOCK",
                        "gate_reasons": ["DUPLICATE_EXIT_ORDER"],
                        "diagnostics": {"source": "risk_plan"},
                    },
                },
            )
        if exchange_view.has_open_order(market, side="ask"):
            return resolution_cls(
                allowed=False,
                skip_reason="DUPLICATE_EXIT_ORDER",
                requested_price=effective_ref_price,
                requested_volume=None,
                sizing_payload=None,
                meta_payload={
                    "strategy": {"market": market, "side": side, "meta": strategy_meta},
                    "size_ladder": size_ladder_decision,
                    "trade_gate": {
                        **trade_gate_payload,
                        "reason_code": "DUPLICATE_EXIT_ORDER",
                        "severity": "BLOCK",
                        "gate_reasons": ["DUPLICATE_EXIT_ORDER"],
                        "diagnostics": {},
                    },
                },
            )
        if forced_volume is None or forced_volume <= 0:
            forced_volume = max(safe_float_fn(local_position.get("base_amount"), default=0.0), 0.0)

    gate_price = round_price_to_tick_fn(
        price=effective_ref_price,
        tick_size=float(snapshot.tick_size),
        side=side,
    )
    gate_volume = (
        float(forced_volume)
        if forced_volume is not None and forced_volume > 0
        else max(float(entry_notional_quote or settings.per_trade_krw), 1.0) / max(float(gate_price), 1e-12)
    )
    if gate_volume <= 0:
        return resolution_cls(
            allowed=False,
            skip_reason="ZERO_VOLUME",
            requested_price=gate_price,
            requested_volume=gate_volume,
            sizing_payload=None,
            meta_payload={
                "strategy": {"market": market, "side": side, "meta": strategy_meta},
                "size_ladder": size_ladder_decision,
                "trade_gate": {
                    **trade_gate_payload,
                    "reason_code": "ZERO_VOLUME",
                    "severity": "BLOCK",
                    "gate_reasons": ["ZERO_VOLUME"],
                    "diagnostics": {},
                },
            },
        )
    fee_rate = float(snapshot.bid_fee if side == "bid" else snapshot.ask_fee)
    trade_gate_decision = trade_gate.evaluate(
        ts_ms=ts_ms,
        market=market,
        side=side,
        price=gate_price,
        volume=gate_volume,
        fee_rate=fee_rate,
        exchange=exchange_view,
        min_total_krw=float(snapshot.min_total),
    )
    trade_gate_payload = {
        **trade_gate_payload,
        "reason_code": str(trade_gate_decision.reason_code),
        "severity": str(trade_gate_decision.severity),
        "gate_reasons": list(trade_gate_decision.gate_reasons),
        "diagnostics": dict(trade_gate_decision.diagnostics or {}),
        "gate_price": float(gate_price),
        "gate_volume": float(gate_volume),
    }
    if not trade_gate_decision.allowed:
        return resolution_cls(
            allowed=False,
            skip_reason=str(trade_gate_decision.reason_code),
            requested_price=gate_price,
            requested_volume=gate_volume,
            sizing_payload=None,
            meta_payload={
                "strategy": {
                    "market": market,
                    "side": side,
                    "reason_code": str(strategy_intent.reason_code),
                    "score": strategy_intent.score,
                    "prob": strategy_intent.prob,
                    "meta": strategy_meta,
                },
                "size_ladder": size_ladder_decision,
                "execution": {
                    "initial_ref_price": float(initial_ref_price),
                    "latest_trade_price": float(latest_trade_price),
                    "effective_ref_price": float(effective_ref_price),
                    "exec_profile": order_exec_profile_to_dict_fn(exec_profile),
                },
                "trade_gate": trade_gate_payload,
                "operational_overlay": operational_payload,
            },
        )

    policy_diagnostics: dict[str, Any] = {}
    policy_payload = {
        "enabled": bool(micro_order_policy is not None),
        "tier": None,
        "reason_code": "POLICY_DISABLED",
    }
    if micro_order_policy is not None:
        policy_decision = micro_order_policy.evaluate(
            micro_snapshot=snapshot_for_policy,
            base_profile=exec_profile,
            market=market,
            ref_price=effective_ref_price,
            tick_size=float(snapshot.tick_size),
            replace_attempt=0,
            model_prob=safe_optional_float_fn(strategy_meta.get("model_prob")),
            now_ts_ms=ts_ms,
        )
        policy_diagnostics = dict(policy_decision.diagnostics or {})
        policy_payload = {
            "enabled": True,
            "tier": str(policy_decision.tier) if policy_decision.tier is not None else None,
            "reason_code": str(policy_decision.reason_code),
        }
        if not policy_decision.allow:
            return resolution_cls(
                allowed=False,
                skip_reason=str(policy_decision.reason_code),
                requested_price=effective_ref_price,
                requested_volume=safe_optional_float_fn(strategy_intent.volume),
                sizing_payload=None,
                meta_payload={
                    "strategy": {
                        "market": market,
                        "side": side,
                        "reason_code": str(strategy_intent.reason_code),
                        "score": strategy_intent.score,
                        "prob": strategy_intent.prob,
                        "meta": strategy_meta,
                    },
                    "size_ladder": size_ladder_decision,
                    "execution": {
                        "initial_ref_price": float(initial_ref_price),
                        "latest_trade_price": float(latest_trade_price),
                        "effective_ref_price": float(effective_ref_price),
                        "exec_profile": order_exec_profile_to_dict_fn(exec_profile),
                    },
                    "micro_order_policy": policy_payload,
                    "micro_diagnostics": policy_diagnostics,
                    "operational_overlay": operational_payload,
                },
            )
        if policy_decision.profile is not None:
            exec_profile = policy_decision.profile

    requested_price = build_limit_price_from_mode_fn(
        side=side,
        ref_price=effective_ref_price,
        tick_size=float(snapshot.tick_size),
        price_mode=exec_profile.price_mode,
    )
    sizing_payload: dict[str, Any] | None = None
    requested_volume = safe_optional_float_fn(strategy_intent.volume)
    if side == "bid":
        target_notional_quote = float(entry_notional_quote or settings.per_trade_krw)
        sizing = derive_volume_from_target_notional_fn(
            side="bid",
            price=requested_price,
            target_notional_quote=float(target_notional_quote),
            fee_rate=max(float(snapshot.bid_fee), 0.0),
        )
        sizing_payload = sizing_envelope_to_payload_fn(sizing)
        requested_volume = max(float(sizing.admissible_volume), 1e-12)
    else:
        requested_volume = max(safe_float_fn(local_position.get("base_amount"), default=0.0), 0.0)
        if requested_volume <= 0:
            return resolution_cls(
                allowed=False,
                skip_reason="ZERO_LOCAL_POSITION",
                requested_price=requested_price,
                requested_volume=requested_volume,
                sizing_payload=None,
                meta_payload={
                    "strategy": {"market": market, "side": side, "meta": strategy_meta},
                    "size_ladder": size_ladder_decision,
                },
            )

    return resolution_cls(
        allowed=True,
        skip_reason=None,
        requested_price=requested_price,
        requested_volume=requested_volume,
        sizing_payload=sizing_payload,
        meta_payload={
            "strategy": {
                "market": market,
                "side": side,
                "reason_code": str(strategy_intent.reason_code),
                "score": strategy_intent.score,
                "prob": strategy_intent.prob,
                "meta": strategy_meta,
            },
            "size_ladder": size_ladder_decision,
            "execution": {
                "initial_ref_price": float(initial_ref_price),
                "latest_trade_price": float(latest_trade_price),
                "effective_ref_price": float(effective_ref_price),
                "requested_price": float(requested_price),
                "exec_profile": order_exec_profile_to_dict_fn(exec_profile),
            },
            "micro_order_policy": policy_payload,
            "micro_diagnostics": policy_diagnostics,
            "trade_gate": trade_gate_payload,
            "operational_overlay": operational_payload,
        },
    )


def handle_strategy_intent(
    *,
    store: Any,
    client: Any,
    public_client: Any,
    executor_gateway: Any | None,
    settings: Any,
    predictor: Any,
    model_alpha_settings: Any,
    strategy_intent: Any,
    instrument_cache: dict[str, dict[str, Any]],
    latest_prices: dict[str, float],
    micro_snapshot_provider: Any,
    micro_order_policy: Any,
    trade_gate: Any,
    ts_ms: int,
    canary_entry_guard_reason_fn: Callable[..., str | None],
    record_strategy_intent_fn: Callable[..., str],
    safe_optional_float_fn: Callable[[object], float | None],
    safe_float_fn: Callable[..., float],
    build_live_order_admissibility_snapshot_fn: Callable[..., Any],
    exchange_view_cls: type,
    resolve_live_strategy_execution_fn: Callable[..., Any],
    evaluate_live_limit_order_fn: Callable[..., Any],
    resolve_live_expected_edge_bps_fn: Callable[..., float | None],
    resolve_live_trade_action_fn: Callable[[dict[str, Any] | None], dict[str, Any]],
    resolve_execution_risk_control_decision_fn: Callable[..., dict[str, Any]],
    resolve_execution_risk_control_online_threshold_fn: Callable[..., dict[str, Any]],
    resolve_execution_risk_control_size_decision_fn: Callable[..., dict[str, Any]],
    arm_breaker_fn: Callable[..., Any],
    clear_breaker_reasons_fn: Callable[..., Any],
    action_halt_new_intents: str,
    action_halt_and_cancel_bot_orders: str,
    record_small_account_decision_fn: Callable[..., Any],
    build_live_admissibility_report_fn: Callable[..., dict[str, Any]],
    new_order_intent_fn: Callable[..., Any],
    order_emission_allowed_fn: Callable[[Any], bool],
    new_order_identifier_fn: Callable[..., str],
    as_optional_str_fn: Callable[[object], str | None],
    attach_exit_order_to_risk_plan_fn: Callable[..., str | None],
    order_record_cls: type,
    reset_counter_fn: Callable[..., Any],
    record_entry_submission_fn: Callable[..., Any],
    handle_submit_reject_fn: Callable[..., None],
) -> str:
    market = str(strategy_intent.market).strip().upper()
    side = str(strategy_intent.side).strip().lower()
    if not market or side not in {"bid", "ask"}:
        return "skipped"
    try:
        accounts_payload = client.accounts()
    except Exception as exc:
        record_strategy_intent_fn(
            store=store,
            market=market,
            side=side,
            price=safe_optional_float_fn(getattr(strategy_intent, "ref_price", None)),
            volume=safe_optional_float_fn(getattr(strategy_intent, "volume", None)),
            reason_code=str(strategy_intent.reason_code),
            meta_payload={"skip_reason": "ACCOUNTS_LOOKUP_FAILED", "error": str(exc)},
            status="SKIPPED",
            ts_ms=ts_ms,
        )
        return "skipped"
    canary_guard_reason = canary_entry_guard_reason_fn(
        store=store,
        settings=settings,
        market=market,
        side=side,
        accounts_payload=accounts_payload,
    )
    if canary_guard_reason:
        record_strategy_intent_fn(
            store=store,
            market=market,
            side=side,
            price=safe_optional_float_fn(getattr(strategy_intent, "ref_price", None)),
            volume=safe_optional_float_fn(getattr(strategy_intent, "volume", None)),
            reason_code=str(strategy_intent.reason_code),
            meta_payload={"skip_reason": canary_guard_reason},
            status="SKIPPED",
            ts_ms=ts_ms,
        )
        return "skipped"
    try:
        chance_payload = client.chance(market=market)
    except Exception as exc:
        record_strategy_intent_fn(
            store=store,
            market=market,
            side=side,
            price=safe_optional_float_fn(getattr(strategy_intent, "ref_price", None)),
            volume=safe_optional_float_fn(getattr(strategy_intent, "volume", None)),
            reason_code=str(strategy_intent.reason_code),
            meta_payload={"skip_reason": "CHANCE_LOOKUP_FAILED", "error": str(exc)},
            status="SKIPPED",
            ts_ms=ts_ms,
        )
        return "skipped"
    instruments_payload = instrument_cache.get(market)
    if instruments_payload is None:
        try:
            loaded = public_client.orderbook_instruments([market])
        except Exception as exc:
            record_strategy_intent_fn(
                store=store,
                market=market,
                side=side,
                price=safe_optional_float_fn(getattr(strategy_intent, "ref_price", None)),
                volume=safe_optional_float_fn(getattr(strategy_intent, "volume", None)),
                reason_code=str(strategy_intent.reason_code),
                meta_payload={"skip_reason": "INSTRUMENTS_LOOKUP_FAILED", "error": str(exc)},
                status="SKIPPED",
                ts_ms=ts_ms,
            )
            return "skipped"
        if isinstance(loaded, list):
            for item in loaded:
                if isinstance(item, dict):
                    item_market = str(item.get("market", "")).strip().upper()
                    if item_market:
                        instrument_cache[item_market] = dict(item)
        instruments_payload = instrument_cache.get(market)
    if instruments_payload is None:
        record_strategy_intent_fn(
            store=store,
            market=market,
            side=side,
            price=strategy_intent.ref_price,
            volume=strategy_intent.volume,
            reason_code=str(strategy_intent.reason_code),
            meta_payload={"skip_reason": "MISSING_INSTRUMENTS"},
            status="SKIPPED",
            ts_ms=ts_ms,
        )
        return "skipped"

    try:
        snapshot = build_live_order_admissibility_snapshot_fn(
            market=market,
            side=side,
            chance_payload=chance_payload if isinstance(chance_payload, dict) else {},
            instruments_payload=[instruments_payload],
            accounts_payload=accounts_payload,
            ts_ms=ts_ms,
        )
    except Exception as exc:
        record_strategy_intent_fn(
            store=store,
            market=market,
            side=side,
            price=safe_optional_float_fn(getattr(strategy_intent, "ref_price", None)),
            volume=safe_optional_float_fn(getattr(strategy_intent, "volume", None)),
            reason_code=str(strategy_intent.reason_code),
            meta_payload={"skip_reason": "ADMISSIBILITY_SNAPSHOT_BUILD_FAILED", "error": str(exc)},
            status="SKIPPED",
            ts_ms=ts_ms,
        )
        return "skipped"
    exchange_view = exchange_view_cls(
        store=store,
        accounts_payload=accounts_payload,
        quote_currency=snapshot.quote_currency,
    )
    execution_resolution = resolve_live_strategy_execution_fn(
        market=market,
        side=side,
        settings=settings,
        model_alpha_settings=model_alpha_settings,
        strategy_intent=strategy_intent,
        snapshot=snapshot,
        latest_trade_price=max(safe_float_fn(latest_prices.get(market), default=0.0), 0.0),
        store=store,
        exchange_view=exchange_view,
        ts_ms=ts_ms,
        micro_snapshot_provider=micro_snapshot_provider,
        micro_order_policy=micro_order_policy,
        trade_gate=trade_gate,
        risk_control_payload=(getattr(predictor, "runtime_recommendations", {}) or {}).get("risk_control", {}),
    )
    if not execution_resolution.allowed:
        record_strategy_intent_fn(
            store=store,
            market=market,
            side=side,
            price=execution_resolution.requested_price,
            volume=execution_resolution.requested_volume,
            reason_code=str(strategy_intent.reason_code),
            meta_payload={**execution_resolution.meta_payload, "skip_reason": execution_resolution.skip_reason},
            status="SKIPPED",
            ts_ms=ts_ms,
        )
        return "skipped"

    decision = evaluate_live_limit_order_fn(
        snapshot=snapshot,
        price=execution_resolution.requested_price,
        volume=max(float(execution_resolution.requested_volume), 1e-12),
        expected_edge_bps=resolve_live_expected_edge_bps_fn(execution_resolution.meta_payload),
    )
    record_small_account_decision_fn(
        store=store,
        decision=decision,
        source="live_model_alpha_runtime",
        ts_ms=ts_ms,
        market=market,
    )
    admissibility_report = build_live_admissibility_report_fn(
        snapshot=snapshot,
        decision=decision,
        sizing_payload=execution_resolution.sizing_payload,
    )
    meta_payload = dict(execution_resolution.meta_payload)
    runtime_recommendations = getattr(predictor, "runtime_recommendations", {}) or {}
    selection_score_value = safe_optional_float_fn(getattr(strategy_intent, "prob", None))
    if selection_score_value is None:
        selection_score_value = safe_optional_float_fn(getattr(strategy_intent, "score", None))
    if selection_score_value is None:
        selection_score_value = safe_optional_float_fn(
            ((dict(strategy_intent.meta or {})).get("model_prob"))
        )
    online_threshold = resolve_execution_risk_control_online_threshold_fn(
        store=store,
        run_id=str(getattr(predictor.run_dir, "name", "")),
        risk_control_payload=runtime_recommendations.get("risk_control") if isinstance(runtime_recommendations, dict) else {},
    )
    risk_control_decision = resolve_execution_risk_control_decision_fn(
        risk_control_payload=runtime_recommendations.get("risk_control") if isinstance(runtime_recommendations, dict) else {},
        selection_score=selection_score_value,
        trade_action=resolve_live_trade_action_fn(meta_payload),
        threshold_override=online_threshold.get("adaptive_threshold"),
    )
    meta_payload["risk_control"] = risk_control_decision
    meta_payload["risk_control_online"] = online_threshold
    if bool(online_threshold.get("enabled")) and hasattr(store, "set_checkpoint"):
        checkpoint_name = str(
            (((runtime_recommendations.get("risk_control") or {}).get("online_adaptation") or {}).get("checkpoint_name"))
            or "execution_risk_control_online_buffer"
        ).strip()
        resolved_checkpoint_name = _execution_risk_control_checkpoint_name(
            base_name=checkpoint_name,
            run_id=str(getattr(predictor.run_dir, "name", "")),
        )
        if resolved_checkpoint_name:
            store.set_checkpoint(name=resolved_checkpoint_name, payload=online_threshold, ts_ms=ts_ms)
    if bool(online_threshold.get("clear_halt")):
        clear_reason_codes = [
            str(item).strip() for item in (online_threshold.get("clear_reason_codes") or []) if str(item).strip()
        ]
        if not clear_reason_codes:
            fallback_clear_reason = str(online_threshold.get("halt_reason_code", "")).strip()
            if fallback_clear_reason:
                clear_reason_codes = [fallback_clear_reason]
        clear_breaker_reasons_fn(
            store,
            reason_codes=clear_reason_codes,
            source="execution_risk_control_online_recovery",
            ts_ms=ts_ms,
            details=dict(online_threshold),
        )
    elif bool(online_threshold.get("halt_triggered")):
        halt_action = str(online_threshold.get("halt_action", "")).strip() or action_halt_new_intents
        if halt_action not in {action_halt_new_intents, action_halt_and_cancel_bot_orders}:
            halt_action = action_halt_new_intents
        arm_breaker_fn(
            store,
            reason_codes=[str(online_threshold.get("halt_reason_code", ""))],
            source="execution_risk_control_online_halt",
            ts_ms=ts_ms,
            action=halt_action,
            details=dict(online_threshold),
        )
    if bool(online_threshold.get("halt_triggered")):
        record_strategy_intent_fn(
            store=store,
            market=market,
            side=side,
            price=execution_resolution.requested_price,
            volume=execution_resolution.requested_volume,
            reason_code=str(strategy_intent.reason_code),
            meta_payload={**meta_payload, "skip_reason": str(online_threshold.get("halt_reason_code", ""))},
            status="SKIPPED",
            ts_ms=ts_ms,
        )
        return "skipped"
    if bool(risk_control_decision.get("enabled")) and not bool(risk_control_decision.get("allowed")):
        record_strategy_intent_fn(
            store=store,
            market=market,
            side=side,
            price=execution_resolution.requested_price,
            volume=execution_resolution.requested_volume,
            reason_code=str(strategy_intent.reason_code),
            meta_payload={**meta_payload, "skip_reason": str(risk_control_decision.get("reason_code", ""))},
            status="SKIPPED",
            ts_ms=ts_ms,
        )
        return "skipped"
    meta_payload["runtime"] = {
        "live_runtime_model_run_id": predictor.run_dir.name,
        "model_family": settings.daemon.runtime_model_family,
    }
    meta_payload["admissibility"] = admissibility_report
    intent = new_order_intent_fn(
        market=market,
        side=side,
        price=float(decision.adjusted_price),
        volume=float(decision.adjusted_volume),
        reason_code=str(strategy_intent.reason_code),
        ord_type="limit",
        time_in_force="gtc",
        meta=meta_payload,
        ts_ms=ts_ms,
    )
    if not decision.admissible:
        record_strategy_intent_fn(
            store=store,
            market=market,
            side=side,
            price=float(decision.adjusted_price),
            volume=float(decision.adjusted_volume),
            reason_code=str(strategy_intent.reason_code),
            meta_payload=meta_payload,
            status="REJECTED_ADMISSIBILITY",
            ts_ms=ts_ms,
            intent_id=intent.intent_id,
        )
        return "skipped"

    if not order_emission_allowed_fn(store):
        record_strategy_intent_fn(
            store=store,
            market=market,
            side=side,
            price=float(decision.adjusted_price),
            volume=float(decision.adjusted_volume),
            reason_code=str(strategy_intent.reason_code),
            meta_payload=meta_payload,
            status="SHADOW",
            ts_ms=ts_ms,
            intent_id=intent.intent_id,
        )
        store.set_checkpoint(
            name="live_model_alpha_last_shadow_intent",
            payload={"intent_id": intent.intent_id, "meta": meta_payload},
            ts_ms=ts_ms,
        )
        return "shadow"

    if executor_gateway is None:
        record_strategy_intent_fn(
            store=store,
            market=market,
            side=side,
            price=float(decision.adjusted_price),
            volume=float(decision.adjusted_volume),
            reason_code=str(strategy_intent.reason_code),
            meta_payload={**meta_payload, "skip_reason": "MISSING_EXECUTOR_GATEWAY"},
            status="SKIPPED",
            ts_ms=ts_ms,
            intent_id=intent.intent_id,
        )
        return "skipped"

    identifier = new_order_identifier_fn(
        prefix=str(settings.daemon.identifier_prefix),
        bot_id=str(settings.daemon.bot_id),
        intent_id=intent.intent_id,
        run_token=str(predictor.run_dir.name),
        ts_ms=ts_ms,
    )
    record_strategy_intent_fn(
        store=store,
        market=market,
        side=side,
        price=float(decision.adjusted_price),
        volume=float(decision.adjusted_volume),
        reason_code=str(strategy_intent.reason_code),
        meta_payload=meta_payload,
        status="SUBMITTING",
        ts_ms=ts_ms,
        intent_id=intent.intent_id,
    )
    result = executor_gateway.submit_intent(
        intent=intent,
        identifier=identifier,
        meta_json=json.dumps(meta_payload, ensure_ascii=False, sort_keys=True),
    )
    if bool(getattr(result, "accepted", False)):
        trade_gate.record_success(market)
        reset_counter_fn(store, counter_name="rate_limit_error", source="live_model_alpha_submit_ok", ts_ms=ts_ms)
        reset_counter_fn(store, counter_name="auth_error", source="live_model_alpha_submit_ok", ts_ms=ts_ms)
        reset_counter_fn(store, counter_name="nonce_error", source="live_model_alpha_submit_ok", ts_ms=ts_ms)
        reset_counter_fn(store, counter_name="replace_reject", source="live_model_alpha_submit_ok", ts_ms=ts_ms)
        order_uuid = as_optional_str_fn(getattr(result, "upbit_uuid", None)) or f"pending-{intent.intent_id}"
        linked_plan_id = None
        if side == "ask":
            linked_plan_id = attach_exit_order_to_risk_plan_fn(
                store=store,
                market=market,
                order_uuid=order_uuid,
                order_identifier=as_optional_str_fn(getattr(result, "identifier", None)) or identifier,
                ts_ms=ts_ms,
            )
        store.upsert_order(
            order_record_cls(
                uuid=order_uuid,
                identifier=as_optional_str_fn(getattr(result, "identifier", None)) or identifier,
                market=market,
                side=side,
                ord_type="limit",
                price=float(decision.adjusted_price),
                volume_req=float(decision.adjusted_volume),
                volume_filled=0.0,
                state="wait",
                created_ts=ts_ms,
                updated_ts=ts_ms,
                intent_id=intent.intent_id,
                tp_sl_link=linked_plan_id,
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="SUBMIT_ACCEPTED",
                event_source="live_model_alpha_runtime",
                replace_seq=0,
                root_order_uuid=order_uuid,
                prev_order_uuid=None,
                prev_order_identifier=None,
            )
        )
        accepted_meta = {**meta_payload, "submit_result": {"accepted": True, "order_uuid": order_uuid}}
        record_strategy_intent_fn(
            store=store,
            market=market,
            side=side,
            price=float(decision.adjusted_price),
            volume=float(decision.adjusted_volume),
            reason_code=str(strategy_intent.reason_code),
            meta_payload=accepted_meta,
            status="SUBMITTED",
            ts_ms=ts_ms,
            intent_id=intent.intent_id,
        )
        if side == "bid":
            record_entry_submission_fn(
                store=store,
                market=market,
                intent_id=str(intent.intent_id),
                requested_price=float(decision.adjusted_price),
                requested_volume=float(decision.adjusted_volume),
                reason_code=str(strategy_intent.reason_code),
                meta_payload=accepted_meta,
                ts_ms=ts_ms,
                order_uuid=order_uuid,
                plan_id=linked_plan_id,
            )
        return "submitted"

    trade_gate.record_failure(market, ts_ms=ts_ms)
    handle_submit_reject_fn(
        store=store,
        intent=intent,
        ts_ms=ts_ms,
        market=market,
        side=side,
        reason_code=str(strategy_intent.reason_code),
        meta_payload=meta_payload,
        reject_reason=str(getattr(result, "reason", "") or ""),
    )
    return "skipped"
