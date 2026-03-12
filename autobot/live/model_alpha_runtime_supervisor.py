from __future__ import annotations

import json
from typing import Any, Callable

from autobot.execution.order_supervisor import (
    REASON_MAX_REPLACES_REACHED,
    SUPERVISOR_ACTION_ABORT,
    SUPERVISOR_ACTION_REPLACE,
    OrderExecProfile,
    SupervisorAction,
    evaluate_supervisor_action,
    make_legacy_exec_profile,
    order_exec_profile_from_dict,
)
from autobot.live.order_state import normalize_order_state

from .state_store import IntentRecord, LiveStateStore, OrderLineageRecord, OrderRecord, RiskPlanRecord


def supervise_open_strategy_orders(
    *,
    store: LiveStateStore,
    client: Any,
    public_client: Any,
    executor_gateway: Any | None,
    instrument_cache: dict[str, dict[str, Any]],
    latest_prices: dict[str, float],
    micro_snapshot_provider: Any,
    micro_order_policy: Any,
    ts_ms: int,
) -> dict[str, Any]:
    report: dict[str, Any] = {
        "evaluated": 0,
        "waited": 0,
        "replaced": 0,
        "aborted": 0,
        "failed": 0,
        "reason_counts": {},
        "results": [],
    }
    if executor_gateway is None:
        report["skipped_reason"] = "MISSING_EXECUTOR_GATEWAY"
        return report

    for order in sorted(store.list_orders(open_only=True), key=lambda item: int(item.get("created_ts") or 0)):
        intent_id = _as_optional_str(order.get("intent_id"))
        if intent_id is None:
            continue
        intent = store.intent_by_id(intent_id=intent_id)
        if intent is None:
            continue
        intent_meta = dict(intent.get("meta") or {})
        execution_meta = dict(intent_meta.get("execution") or {})
        if not execution_meta:
            continue
        market = str(order.get("market", "")).strip().upper()
        side = str(order.get("side", "")).strip().lower()
        if not market or side not in {"bid", "ask"}:
            continue
        remaining_volume = max(
            _safe_float(order.get("volume_req"), default=0.0) - _safe_float(order.get("volume_filled"), default=0.0),
            0.0,
        )
        if remaining_volume <= 0.0:
            continue
        profile = _resolve_exec_profile(execution_meta)
        replace_count = max(int(order.get("replace_seq") or 0), 0)
        if _timeout_abort_due(
            profile=profile,
            created_ts_ms=int(order.get("created_ts") or intent.get("ts_ms") or ts_ms),
            now_ts_ms=int(ts_ms),
            replace_count=replace_count,
        ):
            report["evaluated"] = int(report["evaluated"]) + 1
            result = _abort_open_order(
                store=store,
                executor_gateway=executor_gateway,
                order=order,
                intent=intent,
                action=SupervisorAction(
                    action=SUPERVISOR_ACTION_ABORT,
                    reason_code=REASON_MAX_REPLACES_REACHED,
                ),
                ts_ms=int(ts_ms),
                policy_diagnostics={},
            )
            report["results"].append(result)
            if result["ok"]:
                report["aborted"] = int(report["aborted"]) + 1
                _inc_reason(report["reason_counts"], result["reason_code"])
            else:
                report["failed"] = int(report["failed"]) + 1
            continue
        market_rules = _resolve_market_rules(
            client=client,
            public_client=public_client,
            instrument_cache=instrument_cache,
            market=market,
            side=side,
        )
        if market_rules is None:
            continue
        current_ref_price = _resolve_ref_price(
            order=order,
            execution_meta=execution_meta,
            latest_prices=latest_prices,
        )
        initial_ref_price = max(
            _safe_optional_float(execution_meta.get("initial_ref_price"))
            or _safe_optional_float(execution_meta.get("effective_ref_price"))
            or _safe_optional_float(execution_meta.get("requested_price"))
            or _safe_optional_float(order.get("price"))
            or 0.0,
            0.0,
        )
        if current_ref_price <= 0.0 or initial_ref_price <= 0.0:
            continue
        effective_profile = profile
        policy_diagnostics: dict[str, Any] = {}
        policy_abort_reason: str | None = None
        if micro_order_policy is not None:
            snapshot = micro_snapshot_provider.get(market, int(ts_ms)) if micro_snapshot_provider is not None else None
            model_prob = _safe_optional_float(_dig(intent_meta, "strategy", "meta", "model_prob"))
            guard = micro_order_policy.resolve_guarded_profile(
                profile=profile,
                market=market,
                ref_price=current_ref_price,
                tick_size=float(market_rules["tick_size"]),
                replace_attempt=replace_count + 1,
                model_prob=model_prob,
                micro_snapshot=snapshot,
                now_ts_ms=int(ts_ms),
            )
            effective_profile = guard.profile
            policy_diagnostics = dict(guard.diagnostics or {})
            policy_abort_reason = _as_optional_str(guard.abort_reason)
        action = evaluate_supervisor_action(
            profile=effective_profile,
            side=side,
            now_ts_ms=int(ts_ms),
            created_ts_ms=int(order.get("created_ts") or intent.get("ts_ms") or ts_ms),
            last_action_ts_ms=int(order.get("created_ts") or intent.get("ts_ms") or ts_ms),
            last_replace_ts_ms=int(order.get("created_ts") or intent.get("ts_ms") or ts_ms),
            replace_count=replace_count,
            remaining_volume=remaining_volume,
            ref_price=current_ref_price,
            tick_size=float(market_rules["tick_size"]),
            initial_ref_price=initial_ref_price,
            min_total=float(market_rules["min_total"]),
            replaces_last_minute=_recent_replace_count(store=store, intent_id=intent_id, ts_ms=int(ts_ms)),
            max_replaces_per_min_per_market=3,
        )
        if policy_abort_reason is not None and action.action == SUPERVISOR_ACTION_REPLACE:
            action = SupervisorAction(
                action=SUPERVISOR_ACTION_ABORT,
                reason_code=policy_abort_reason,
            )
        report["evaluated"] = int(report["evaluated"]) + 1
        if action.action == SUPERVISOR_ACTION_REPLACE:
            result = _replace_open_order(
                store=store,
                client=client,
                executor_gateway=executor_gateway,
                order=order,
                intent=intent,
                action=action,
                profile=effective_profile,
                remaining_volume=remaining_volume,
                ts_ms=int(ts_ms),
                policy_diagnostics=policy_diagnostics,
            )
            report["results"].append(result)
            if result["ok"]:
                report["replaced"] = int(report["replaced"]) + 1
                _inc_reason(report["reason_counts"], result["reason_code"])
            else:
                report["failed"] = int(report["failed"]) + 1
            continue
        if action.action == SUPERVISOR_ACTION_ABORT:
            result = _abort_open_order(
                store=store,
                executor_gateway=executor_gateway,
                order=order,
                intent=intent,
                action=action,
                ts_ms=int(ts_ms),
                policy_diagnostics=policy_diagnostics,
            )
            report["results"].append(result)
            if result["ok"]:
                report["aborted"] = int(report["aborted"]) + 1
                _inc_reason(report["reason_counts"], result["reason_code"])
            else:
                report["failed"] = int(report["failed"]) + 1
            continue
        report["waited"] = int(report["waited"]) + 1

    if report["evaluated"] > 0:
        store.set_checkpoint(name="live_model_alpha_last_order_supervision", payload=report, ts_ms=int(ts_ms))
    return report


def _resolve_exec_profile(execution_meta: dict[str, Any]) -> OrderExecProfile:
    raw_profile = execution_meta.get("exec_profile")
    fallback = make_legacy_exec_profile(
        timeout_ms=max(int(_safe_optional_float(_dig(execution_meta, "exec_profile", "timeout_ms")) or 300_000), 1),
        replace_interval_ms=max(
            int(_safe_optional_float(_dig(execution_meta, "exec_profile", "replace_interval_ms")) or 300_000),
            1,
        ),
        max_replaces=max(int(_safe_optional_float(_dig(execution_meta, "exec_profile", "max_replaces")) or 0), 0),
        price_mode=str(_dig(execution_meta, "exec_profile", "price_mode") or "JOIN"),
        max_chase_bps=max(int(_safe_optional_float(_dig(execution_meta, "exec_profile", "max_chase_bps")) or 10_000), 0),
        min_replace_interval_ms_global=max(
            int(_safe_optional_float(_dig(execution_meta, "exec_profile", "min_replace_interval_ms_global")) or 1_500),
            1,
        ),
    )
    return order_exec_profile_from_dict(raw_profile, fallback=fallback)


def _resolve_market_rules(
    *,
    client: Any,
    public_client: Any,
    instrument_cache: dict[str, dict[str, Any]],
    market: str,
    side: str,
) -> dict[str, float] | None:
    try:
        chance_payload = client.chance(market=market)
    except Exception:
        return None
    instrument_payload = instrument_cache.get(market)
    if instrument_payload is None:
        try:
            loaded = public_client.orderbook_instruments([market])
        except Exception:
            loaded = None
        if isinstance(loaded, list):
            for item in loaded:
                if isinstance(item, dict):
                    item_market = str(item.get("market", "")).strip().upper()
                    if item_market:
                        instrument_cache[item_market] = dict(item)
        instrument_payload = instrument_cache.get(market)
    tick_size = _safe_optional_float((instrument_payload or {}).get("tick_size"))
    if tick_size is None or tick_size <= 0.0:
        return None
    market_payload = dict((chance_payload or {}).get("market") or {}) if isinstance(chance_payload, dict) else {}
    side_payload = dict(market_payload.get(side) or {}) if isinstance(market_payload.get(side), dict) else {}
    min_total = _safe_optional_float(side_payload.get("min_total"))
    if min_total is None:
        min_total = 0.0
    return {
        "tick_size": float(tick_size),
        "min_total": max(float(min_total), 0.0),
    }


def _resolve_ref_price(
    *,
    order: dict[str, Any],
    execution_meta: dict[str, Any],
    latest_prices: dict[str, float],
) -> float:
    market = str(order.get("market", "")).strip().upper()
    return max(
        _safe_float(latest_prices.get(market), default=0.0),
        _safe_optional_float(execution_meta.get("effective_ref_price")) or 0.0,
        _safe_optional_float(execution_meta.get("initial_ref_price")) or 0.0,
        _safe_optional_float(execution_meta.get("requested_price")) or 0.0,
        _safe_optional_float(order.get("price")) or 0.0,
    )


def _recent_replace_count(*, store: LiveStateStore, intent_id: str, ts_ms: int) -> int:
    lower_bound = int(ts_ms) - 60_000
    return len(
        [
            item
            for item in store.list_order_lineage(intent_id=intent_id)
            if int(item.get("ts_ms") or 0) >= lower_bound
        ]
    )


def _timeout_abort_due(
    *,
    profile: OrderExecProfile,
    created_ts_ms: int,
    now_ts_ms: int,
    replace_count: int,
) -> bool:
    if int(replace_count) < int(profile.max_replaces):
        return False
    age_ms = max(int(now_ts_ms) - int(created_ts_ms), 0)
    return age_ms >= max(int(profile.timeout_ms), int(profile.replace_interval_ms))


def _abort_open_order(
    *,
    store: LiveStateStore,
    executor_gateway: Any,
    order: dict[str, Any],
    intent: dict[str, Any],
    action: SupervisorAction,
    ts_ms: int,
    policy_diagnostics: dict[str, Any],
) -> dict[str, Any]:
    result = executor_gateway.cancel(
        upbit_uuid=_as_optional_str(order.get("uuid")),
        identifier=_as_optional_str(order.get("identifier")),
    )
    reason_code = _as_optional_str(action.reason_code) or "SUPERVISOR_ABORT"
    if not bool(getattr(result, "accepted", False)):
        return {
            "ok": False,
            "action": "ABORT",
            "market": order.get("market"),
            "side": order.get("side"),
            "reason_code": reason_code,
            "error": str(getattr(result, "reason", "") or ""),
        }
    _mark_order_cancelled(
        store=store,
        order=order,
        ts_ms=ts_ms,
        event_name="ORDER_TIMEOUT",
        event_source="live_order_supervisor",
    )
    _update_intent_status(
        store=store,
        intent=intent,
        status="CANCELLED",
        supervisor_payload={
            "action": "ABORT",
            "reason_code": reason_code,
            "ts_ms": int(ts_ms),
            "policy_diagnostics": dict(policy_diagnostics),
        },
    )
    _update_linked_plan_after_abort(
        store=store,
        order=order,
        ts_ms=ts_ms,
    )
    return {
        "ok": True,
        "action": "ABORT",
        "market": order.get("market"),
        "side": order.get("side"),
        "reason_code": reason_code,
        "order_uuid": order.get("uuid"),
    }


def _replace_open_order(
    *,
    store: LiveStateStore,
    client: Any,
    executor_gateway: Any,
    order: dict[str, Any],
    intent: dict[str, Any],
    action: SupervisorAction,
    profile: OrderExecProfile,
    remaining_volume: float,
    ts_ms: int,
    policy_diagnostics: dict[str, Any],
) -> dict[str, Any]:
    reason_code = _as_optional_str(action.reason_code) or "TIMEOUT_REPLACE"
    replace_step = max(int(order.get("replace_seq") or 0), 0) + 1
    new_identifier = f"AUTOBOT-SUPREP-{str(intent.get('intent_id') or '')[:12]}-{replace_step}-{int(ts_ms)}"
    result = executor_gateway.replace_order(
        intent_id=str(intent.get("intent_id") or ""),
        prev_order_uuid=_as_optional_str(order.get("uuid")),
        prev_order_identifier=_as_optional_str(order.get("identifier")),
        new_identifier=new_identifier,
        new_price_str=_format_decimal(float(action.target_price or order.get("price") or 0.0)),
        new_volume_str="remain_only",
        new_time_in_force="gtc",
    )
    if not bool(getattr(result, "accepted", False)):
        return {
            "ok": False,
            "action": "REPLACE",
            "market": order.get("market"),
            "side": order.get("side"),
            "reason_code": reason_code,
            "error": str(getattr(result, "reason", "") or ""),
        }
    new_uuid = _as_optional_str(getattr(result, "new_order_uuid", None))
    new_identifier_value = _as_optional_str(getattr(result, "new_identifier", None)) or new_identifier
    if new_uuid is None:
        try:
            lookup = client.order(uuid=None, identifier=new_identifier_value)
        except Exception:
            lookup = None
        if isinstance(lookup, dict):
            new_uuid = _as_optional_str(lookup.get("uuid"))
            new_identifier_value = _as_optional_str(lookup.get("identifier")) or new_identifier_value
    _mark_order_cancelled(
        store=store,
        order=order,
        ts_ms=ts_ms,
        event_name="ORDER_REPLACED",
        event_source="live_order_supervisor",
    )
    if new_uuid is not None:
        normalized = normalize_order_state(exchange_state="wait", event_name="ORDER_REPLACED")
        store.upsert_order(
            OrderRecord(
                uuid=new_uuid,
                identifier=new_identifier_value,
                market=str(order.get("market") or ""),
                side=_as_optional_str(order.get("side")),
                ord_type=_as_optional_str(order.get("ord_type")) or "limit",
                price=float(action.target_price or order.get("price") or 0.0),
                volume_req=max(float(remaining_volume), 0.0),
                volume_filled=0.0,
                state="wait",
                created_ts=int(ts_ms),
                updated_ts=int(ts_ms),
                intent_id=_as_optional_str(intent.get("intent_id")),
                tp_sl_link=_as_optional_str(order.get("tp_sl_link")),
                local_state=normalized.local_state,
                raw_exchange_state=normalized.exchange_state,
                last_event_name=normalized.event_name,
                event_source="live_order_supervisor",
                replace_seq=replace_step,
                root_order_uuid=_as_optional_str(order.get("root_order_uuid")) or _as_optional_str(order.get("uuid")) or new_uuid,
                prev_order_uuid=_as_optional_str(order.get("uuid")),
                prev_order_identifier=_as_optional_str(order.get("identifier")),
            )
        )
    try:
        store.append_order_lineage(
            OrderLineageRecord(
                ts_ms=int(ts_ms),
                event_source="live_order_supervisor",
                intent_id=_as_optional_str(intent.get("intent_id")),
                prev_uuid=_as_optional_str(order.get("uuid")),
                prev_identifier=_as_optional_str(order.get("identifier")),
                new_uuid=new_uuid,
                new_identifier=new_identifier_value,
                replace_seq=replace_step,
            )
        )
    except Exception:
        pass
    _update_intent_status(
        store=store,
        intent=intent,
        status=str(intent.get("status") or "SUBMITTED"),
        supervisor_payload={
            "action": "REPLACE",
            "reason_code": reason_code,
            "ts_ms": int(ts_ms),
            "replace_step": replace_step,
            "new_order_uuid": new_uuid,
            "new_identifier": new_identifier_value,
            "effective_profile": {
                "timeout_ms": int(profile.timeout_ms),
                "replace_interval_ms": int(profile.replace_interval_ms),
                "max_replaces": int(profile.max_replaces),
                "price_mode": str(profile.price_mode),
                "max_chase_bps": int(profile.max_chase_bps),
                "min_replace_interval_ms_global": int(profile.min_replace_interval_ms_global),
                "post_only": bool(profile.post_only),
            },
            "policy_diagnostics": dict(policy_diagnostics),
        },
    )
    _update_linked_plan_after_replace(
        store=store,
        order=order,
        new_uuid=new_uuid,
        new_identifier=new_identifier_value,
        replace_step=replace_step,
        ts_ms=ts_ms,
    )
    return {
        "ok": True,
        "action": "REPLACE",
        "market": order.get("market"),
        "side": order.get("side"),
        "reason_code": reason_code,
        "prev_order_uuid": order.get("uuid"),
        "new_order_uuid": new_uuid,
    }


def _mark_order_cancelled(
    *,
    store: LiveStateStore,
    order: dict[str, Any],
    ts_ms: int,
    event_name: str,
    event_source: str,
) -> None:
    store.upsert_order(
        OrderRecord(
            uuid=str(order.get("uuid") or ""),
            identifier=_as_optional_str(order.get("identifier")),
            market=str(order.get("market") or ""),
            side=_as_optional_str(order.get("side")),
            ord_type=_as_optional_str(order.get("ord_type")),
            price=_safe_optional_float(order.get("price")),
            volume_req=_safe_optional_float(order.get("volume_req")),
            volume_filled=float(order.get("volume_filled") or 0.0),
            state="cancel",
            created_ts=int(order.get("created_ts") or ts_ms),
            updated_ts=int(ts_ms),
            intent_id=_as_optional_str(order.get("intent_id")),
            tp_sl_link=_as_optional_str(order.get("tp_sl_link")),
            local_state="CANCELLED",
            raw_exchange_state="cancel",
            last_event_name=event_name,
            event_source=event_source,
            replace_seq=int(order.get("replace_seq") or 0),
            root_order_uuid=_as_optional_str(order.get("root_order_uuid")) or _as_optional_str(order.get("uuid")),
            prev_order_uuid=_as_optional_str(order.get("prev_order_uuid")),
            prev_order_identifier=_as_optional_str(order.get("prev_order_identifier")),
            executed_funds=_safe_optional_float(order.get("executed_funds")),
            paid_fee=_safe_optional_float(order.get("paid_fee")),
            reserved_fee=_safe_optional_float(order.get("reserved_fee")),
            remaining_fee=_safe_optional_float(order.get("remaining_fee")),
            exchange_payload_json=json.dumps(order.get("exchange_payload"), ensure_ascii=False, sort_keys=True)
            if isinstance(order.get("exchange_payload"), dict)
            else str(order.get("exchange_payload_json") or "{}"),
        )
    )


def _update_linked_plan_after_abort(*, store: LiveStateStore, order: dict[str, Any], ts_ms: int) -> None:
    plan_id = _as_optional_str(order.get("tp_sl_link"))
    if plan_id is None:
        return
    row = store.risk_plan_by_id(plan_id=plan_id)
    if row is None:
        return
    _upsert_plan_from_row(
        store=store,
        row=row,
        ts_ms=ts_ms,
        state="TRIGGERED",
        current_exit_order_uuid=None,
        current_exit_order_identifier=None,
    )


def _update_linked_plan_after_replace(
    *,
    store: LiveStateStore,
    order: dict[str, Any],
    new_uuid: str | None,
    new_identifier: str | None,
    replace_step: int,
    ts_ms: int,
) -> None:
    plan_id = _as_optional_str(order.get("tp_sl_link"))
    if plan_id is None:
        return
    row = store.risk_plan_by_id(plan_id=plan_id)
    if row is None:
        return
    _upsert_plan_from_row(
        store=store,
        row=row,
        ts_ms=ts_ms,
        state="EXITING",
        current_exit_order_uuid=new_uuid,
        current_exit_order_identifier=new_identifier,
        replace_attempt=replace_step,
        last_action_ts_ms=ts_ms,
    )


def _upsert_plan_from_row(
    *,
    store: LiveStateStore,
    row: dict[str, Any],
    ts_ms: int,
    state: str,
    current_exit_order_uuid: str | None,
    current_exit_order_identifier: str | None,
    replace_attempt: int | None = None,
    last_action_ts_ms: int | None = None,
) -> None:
    tp = dict(row.get("tp") or {})
    sl = dict(row.get("sl") or {})
    trailing = dict(row.get("trailing") or {})
    store.upsert_risk_plan(
        RiskPlanRecord(
            plan_id=str(row.get("plan_id") or ""),
            market=str(row.get("market") or ""),
            side=str(row.get("side") or "long"),
            entry_price_str=str(row.get("entry_price_str") or ""),
            qty_str=str(row.get("qty_str") or ""),
            tp_enabled=bool(tp.get("enabled")),
            tp_price_str=_as_optional_str(tp.get("tp_price_str")),
            tp_pct=_safe_optional_float(tp.get("tp_pct")),
            sl_enabled=bool(sl.get("enabled")),
            sl_price_str=_as_optional_str(sl.get("sl_price_str")),
            sl_pct=_safe_optional_float(sl.get("sl_pct")),
            trailing_enabled=bool(trailing.get("enabled")),
            trail_pct=_safe_optional_float(trailing.get("trail_pct")),
            high_watermark_price_str=_as_optional_str(trailing.get("high_watermark_price_str")),
            armed_ts_ms=_safe_optional_int(trailing.get("armed_ts_ms")),
            timeout_ts_ms=_safe_optional_int(row.get("timeout_ts_ms")),
            state=state,
            last_eval_ts_ms=int(row.get("last_eval_ts_ms") or 0),
            last_action_ts_ms=int(last_action_ts_ms if last_action_ts_ms is not None else row.get("last_action_ts_ms") or 0),
            current_exit_order_uuid=current_exit_order_uuid,
            current_exit_order_identifier=current_exit_order_identifier,
            replace_attempt=int(replace_attempt if replace_attempt is not None else row.get("replace_attempt") or 0),
            created_ts=int(row.get("created_ts") or 0),
            updated_ts=int(ts_ms),
            plan_source=_as_optional_str(row.get("plan_source")),
            source_intent_id=_as_optional_str(row.get("source_intent_id")),
        )
    )


def _update_intent_status(
    *,
    store: LiveStateStore,
    intent: dict[str, Any],
    status: str,
    supervisor_payload: dict[str, Any],
) -> None:
    meta = dict(intent.get("meta") or {})
    meta["supervisor_result"] = supervisor_payload
    store.upsert_intent(
        IntentRecord(
            intent_id=str(intent.get("intent_id") or ""),
            ts_ms=int(intent.get("ts_ms") or 0),
            market=str(intent.get("market") or ""),
            side=str(intent.get("side") or ""),
            price=_safe_optional_float(intent.get("price")),
            volume=_safe_optional_float(intent.get("volume")),
            reason_code=_as_optional_str(intent.get("reason_code")),
            meta_json=json.dumps(meta, ensure_ascii=False, sort_keys=True),
            status=str(status).strip().upper(),
        )
    )


def _inc_reason(target: dict[str, int], reason: str | None) -> None:
    key = _as_optional_str(reason)
    if key is None:
        return
    target[key] = int(target.get(key, 0)) + 1


def _dig(payload: dict[str, Any] | None, *path: str) -> Any:
    current: Any = payload or {}
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _as_optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _safe_optional_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_optional_int(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_float(value: object, *, default: float) -> float:
    resolved = _safe_optional_float(value)
    if resolved is None:
        return float(default)
    return float(resolved)


def _format_decimal(value: float) -> str:
    text = f"{float(value):.12f}"
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"
