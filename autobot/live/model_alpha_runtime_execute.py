"""Execution and submission helpers for live model_alpha runtime."""

from __future__ import annotations

import json
from typing import Any, Callable


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
    rollout_contract = store.live_rollout_contract() or {}
    rollout_status = store.live_rollout_status() or {}
    rollout_mode = (
        str(rollout_status.get("mode") or rollout_contract.get("mode") or settings.daemon.rollout_mode)
        .strip()
        .lower()
    )
    if rollout_mode != "canary":
        return resolved_target
    contract_target_unit = str(rollout_contract.get("target_unit") or "").strip()
    if contract_target_unit and contract_target_unit != str(settings.daemon.rollout_target_unit).strip():
        return resolved_target
    cap_value = safe_optional_float_fn(rollout_contract.get("canary_max_notional_quote"))
    if cap_value is None or cap_value <= 0.0:
        return resolved_target
    return min(resolved_target, float(cap_value))


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
    resolution_cls: type,
    safe_optional_float_fn: Callable[[object], float | None],
    safe_float_fn: Callable[..., float],
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

    if side == "ask":
        if local_position is None:
            return resolution_cls(
                allowed=False,
                skip_reason="NO_LOCAL_POSITION",
                requested_price=effective_ref_price,
                requested_volume=None,
                sizing_payload=None,
                meta_payload={"strategy": {"market": market, "side": side, "meta": strategy_meta}},
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
                meta_payload={"strategy": {"market": market, "side": side, "meta": strategy_meta}},
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
    canary_guard_reason = canary_entry_guard_reason_fn(
        store=store,
        settings=settings,
        market=market,
        side=side,
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

    accounts_payload = client.accounts()
    chance_payload = client.chance(market=market)
    instruments_payload = instrument_cache.get(market)
    if instruments_payload is None:
        loaded = public_client.orderbook_instruments([market])
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

    snapshot = build_live_order_admissibility_snapshot_fn(
        market=market,
        side=side,
        chance_payload=chance_payload if isinstance(chance_payload, dict) else {},
        instruments_payload=[instruments_payload],
        accounts_payload=accounts_payload,
        ts_ms=ts_ms,
    )
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
