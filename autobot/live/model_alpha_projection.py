"""Risk-plan and strategy projection helpers for live model_alpha runtime."""

from __future__ import annotations

from typing import Any

from .breakers import clear_recovered_risk_exit_stuck_breaker
from autobot.backtest.strategy_adapter import StrategyFillEvent
from autobot.risk.live_risk_manager import LiveRiskManager

from .model_risk_plan import build_model_derived_risk_records, build_model_exit_plan_from_position, extract_model_exit_plan
from .state_store import LiveStateStore, RiskPlanRecord


def strategy_bid_fill(
    *,
    strategy: Any,
    market: str,
    position: dict[str, Any],
    ts_ms: int,
) -> None:
    strategy.on_fill(
        StrategyFillEvent(
            ts_ms=int(ts_ms),
            market=str(market).strip().upper(),
            side="bid",
            price=max(_safe_float(position.get("avg_entry_price"), default=0.0), 1e-12),
            volume=max(_safe_float(position.get("base_amount"), default=0.0), 1e-12),
            fee_quote=0.0,
            meta={"model_exit_plan": build_model_exit_plan_from_position(position)},
        )
    )


def strategy_ask_fill(
    *,
    strategy: Any,
    market: str,
    position: dict[str, Any],
    exit_price: float,
    ts_ms: int,
) -> None:
    strategy.on_fill(
        StrategyFillEvent(
            ts_ms=int(ts_ms),
            market=str(market).strip().upper(),
            side="ask",
            price=max(float(exit_price), 1e-12),
            volume=max(_safe_float(position.get("base_amount"), default=0.0), 1e-12),
            fee_quote=0.0,
            meta={},
        )
    )


def resolve_strategy_entry_ts_ms(
    *,
    store: LiveStateStore,
    market: str,
    position: dict[str, Any],
    default_ts_ms: int,
) -> int:
    live_plans = store.list_risk_plans(market=market, states=("ACTIVE", "TRIGGERED", "EXITING"))
    created_candidates = [int(item.get("created_ts") or 0) for item in live_plans if int(item.get("created_ts") or 0) > 0]
    if created_candidates:
        return max(created_candidates)
    updated_ts = _safe_int(position.get("updated_ts"), default=0)
    if updated_ts > 0:
        return updated_ts
    return int(default_ts_ms)


def attach_exit_order_to_risk_plan(
    *,
    store: LiveStateStore,
    market: str,
    order_uuid: str,
    order_identifier: str,
    ts_ms: int,
) -> str | None:
    live_plans = store.list_risk_plans(market=market, states=("ACTIVE", "TRIGGERED", "EXITING"))
    if not live_plans:
        return None
    selected = max(
        live_plans,
        key=lambda item: (
            int(item.get("created_ts") or 0),
            int(item.get("updated_ts") or 0),
            str(item.get("plan_id") or ""),
        ),
    )
    plan_id = str(selected.get("plan_id") or "").strip()
    if not plan_id:
        return None
    selected_tp = dict(selected.get("tp") or {})
    selected_sl = dict(selected.get("sl") or {})
    selected_trailing = dict(selected.get("trailing") or {})
    store.upsert_risk_plan(
        RiskPlanRecord(
            plan_id=plan_id,
            market=str(selected.get("market", market) or market),
            side=str(selected.get("side", "long") or "long"),
            entry_price_str=str(selected.get("entry_price_str", "")),
            qty_str=str(selected.get("qty_str", "")),
            tp_enabled=bool(selected_tp.get("enabled")),
            tp_price_str=_as_optional_str(selected_tp.get("tp_price_str")),
            tp_pct=_safe_optional_float(selected_tp.get("tp_pct")),
            sl_enabled=bool(selected_sl.get("enabled")),
            sl_price_str=_as_optional_str(selected_sl.get("sl_price_str")),
            sl_pct=_safe_optional_float(selected_sl.get("sl_pct")),
            trailing_enabled=bool(selected_trailing.get("enabled")),
            trail_pct=_safe_optional_float(selected_trailing.get("trail_pct")),
            high_watermark_price_str=_as_optional_str(selected_trailing.get("high_watermark_price_str")),
            armed_ts_ms=_safe_optional_int(selected_trailing.get("armed_ts_ms")),
            timeout_ts_ms=_safe_optional_int(selected.get("timeout_ts_ms")),
            state="EXITING",
            last_eval_ts_ms=int(selected.get("last_eval_ts_ms", 0) or 0),
            last_action_ts_ms=int(ts_ms),
            current_exit_order_uuid=str(order_uuid).strip(),
            current_exit_order_identifier=str(order_identifier).strip(),
            replace_attempt=int(selected.get("replace_attempt", 0) or 0),
            created_ts=int(selected.get("created_ts", ts_ms) or ts_ms),
            updated_ts=int(ts_ms),
            plan_source=_as_optional_str(selected.get("plan_source")),
            source_intent_id=_as_optional_str(selected.get("source_intent_id")),
        )
    )
    return plan_id


def ensure_live_risk_plan(
    *,
    store: LiveStateStore,
    risk_manager: LiveRiskManager | None,
    market: str,
    position: dict[str, Any],
    ts_ms: int,
) -> None:
    if risk_manager is None:
        return
    live_plans = store.list_risk_plans(market=market, states=("ACTIVE", "TRIGGERED", "EXITING"))
    qty = max(_safe_float(position.get("base_amount"), default=0.0), 0.0)
    entry_price = max(_safe_float(position.get("avg_entry_price"), default=0.0), 0.0)
    if qty <= 0 or entry_price <= 0:
        return
    entry_intent: dict[str, Any] | None = None
    if not live_plans:
        entry_intent = find_latest_model_entry_intent(store=store, market=market, position=position)
        if entry_intent is not None:
            _, risk_plan_record = build_model_derived_risk_records(
                market=market,
                base_currency=str(market).split("-")[-1],
                base_amount=qty,
                avg_entry_price=entry_price,
                plan_payload=entry_intent["plan_payload"],
                created_ts=int(entry_intent["created_ts"]),
                updated_ts=int(ts_ms),
                intent_id=entry_intent["intent_id"],
            )
            store.upsert_risk_plan(risk_plan_record)
            return
        risk_manager.attach_default_risk(
            market=market,
            entry_price=entry_price,
            qty=qty,
            ts_ms=ts_ms,
            plan_id=f"default-risk-{market}",
        )
        return
    for plan in live_plans:
        if str(plan.get("state", "")).strip().upper() != "ACTIVE":
            continue
        current_exit_uuid = str(plan.get("current_exit_order_uuid") or "").strip()
        if current_exit_uuid:
            continue
        derived_plan_record = None
        derived_position_record = None
        plan_tp = dict(plan.get("tp") or {})
        plan_sl = dict(plan.get("sl") or {})
        plan_trailing = dict(plan.get("trailing") or {})
        plan_source = _as_optional_str(plan.get("plan_source"))
        source_intent_id = _as_optional_str(plan.get("source_intent_id"))
        timeout_ts_ms = _safe_optional_int(plan.get("timeout_ts_ms"))
        needs_model_backfill = (not plan_source) or (not source_intent_id) or (timeout_ts_ms is None)
        if not needs_model_backfill and str(plan_source).strip().lower() == "model_alpha_v1":
            if entry_intent is None:
                entry_intent = find_latest_model_entry_intent(store=store, market=market, position=position)
            if entry_intent is not None:
                candidate_position_record, candidate_plan_record = build_model_derived_risk_records(
                    market=market,
                    base_currency=str(market).split("-")[-1],
                    base_amount=qty,
                    avg_entry_price=entry_price,
                    plan_payload=entry_intent["plan_payload"],
                    created_ts=int(entry_intent["created_ts"]),
                    updated_ts=int(ts_ms),
                    intent_id=entry_intent["intent_id"],
                )
                needs_model_backfill = _risk_plan_differs_from_record(plan=plan, record=candidate_plan_record)
                if needs_model_backfill:
                    derived_position_record = candidate_position_record
                    derived_plan_record = candidate_plan_record
        elif needs_model_backfill:
            if entry_intent is None:
                entry_intent = find_latest_model_entry_intent(store=store, market=market, position=position)
            if entry_intent is not None:
                derived_position_record, derived_plan_record = build_model_derived_risk_records(
                    market=market,
                    base_currency=str(market).split("-")[-1],
                    base_amount=qty,
                    avg_entry_price=entry_price,
                    plan_payload=entry_intent["plan_payload"],
                    created_ts=int(entry_intent["created_ts"]),
                    updated_ts=int(ts_ms),
                    intent_id=entry_intent["intent_id"],
                )
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id=str(plan.get("plan_id")),
                market=market,
                side=str(plan.get("side", "long") or "long"),
                entry_price_str=str(entry_price),
                qty_str=str(qty),
                tp_enabled=bool(derived_plan_record.tp_enabled) if derived_plan_record is not None else bool(plan_tp.get("enabled")),
                tp_price_str=derived_plan_record.tp_price_str if derived_plan_record is not None else _as_optional_str(plan_tp.get("tp_price_str")),
                tp_pct=derived_plan_record.tp_pct if derived_plan_record is not None else _safe_optional_float(plan_tp.get("tp_pct")),
                sl_enabled=bool(derived_plan_record.sl_enabled) if derived_plan_record is not None else bool(plan_sl.get("enabled")),
                sl_price_str=derived_plan_record.sl_price_str if derived_plan_record is not None else _as_optional_str(plan_sl.get("sl_price_str")),
                sl_pct=derived_plan_record.sl_pct if derived_plan_record is not None else _safe_optional_float(plan_sl.get("sl_pct")),
                trailing_enabled=bool(derived_plan_record.trailing_enabled) if derived_plan_record is not None else bool(plan_trailing.get("enabled")),
                trail_pct=derived_plan_record.trail_pct if derived_plan_record is not None else _safe_optional_float(plan_trailing.get("trail_pct")),
                high_watermark_price_str=_as_optional_str(plan_trailing.get("high_watermark_price_str")) or (
                    derived_plan_record.high_watermark_price_str if derived_plan_record is not None else None
                ),
                armed_ts_ms=_safe_optional_int(plan_trailing.get("armed_ts_ms")) if _safe_optional_int(plan_trailing.get("armed_ts_ms")) is not None else (
                    derived_plan_record.armed_ts_ms if derived_plan_record is not None else None
                ),
                timeout_ts_ms=derived_plan_record.timeout_ts_ms if derived_plan_record is not None else timeout_ts_ms,
                state=str(plan.get("state", "ACTIVE") or "ACTIVE"),
                last_eval_ts_ms=int(plan.get("last_eval_ts_ms", 0) or 0),
                last_action_ts_ms=int(plan.get("last_action_ts_ms", 0) or 0),
                current_exit_order_uuid=_as_optional_str(plan.get("current_exit_order_uuid")),
                current_exit_order_identifier=_as_optional_str(plan.get("current_exit_order_identifier")),
                replace_attempt=int(plan.get("replace_attempt", 0) or 0),
                created_ts=int(plan.get("created_ts", ts_ms) or ts_ms),
                updated_ts=int(ts_ms),
                plan_source=derived_plan_record.plan_source if derived_plan_record is not None else plan_source,
                source_intent_id=derived_plan_record.source_intent_id if derived_plan_record is not None else source_intent_id,
            )
        )
        if derived_position_record is not None:
            store.upsert_position(derived_position_record)


def close_market_risk_plans(*, store: LiveStateStore, market: str, ts_ms: int) -> None:
    for plan in store.list_risk_plans(market=market):
        if str(plan.get("state", "")).strip().upper() == "CLOSED":
            continue
        plan_tp = dict(plan.get("tp") or {})
        plan_sl = dict(plan.get("sl") or {})
        plan_trailing = dict(plan.get("trailing") or {})
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id=str(plan.get("plan_id")),
                market=market,
                side=str(plan.get("side", "long") or "long"),
                entry_price_str=str(plan.get("entry_price_str", "")),
                qty_str=str(plan.get("qty_str", "")),
                tp_enabled=bool(plan_tp.get("enabled")),
                tp_price_str=_as_optional_str(plan_tp.get("tp_price_str")),
                tp_pct=_safe_optional_float(plan_tp.get("tp_pct")),
                sl_enabled=bool(plan_sl.get("enabled")),
                sl_price_str=_as_optional_str(plan_sl.get("sl_price_str")),
                sl_pct=_safe_optional_float(plan_sl.get("sl_pct")),
                trailing_enabled=bool(plan_trailing.get("enabled")),
                trail_pct=_safe_optional_float(plan_trailing.get("trail_pct")),
                high_watermark_price_str=_as_optional_str(plan_trailing.get("high_watermark_price_str")),
                armed_ts_ms=_safe_optional_int(plan_trailing.get("armed_ts_ms")),
                timeout_ts_ms=_safe_optional_int(plan.get("timeout_ts_ms")),
                state="CLOSED",
                last_eval_ts_ms=int(plan.get("last_eval_ts_ms", 0) or 0),
                last_action_ts_ms=int(ts_ms),
                current_exit_order_uuid=_as_optional_str(plan.get("current_exit_order_uuid")),
                current_exit_order_identifier=_as_optional_str(plan.get("current_exit_order_identifier")),
                replace_attempt=int(plan.get("replace_attempt", 0) or 0),
                created_ts=int(plan.get("created_ts", ts_ms) or ts_ms),
                updated_ts=int(ts_ms),
                plan_source=_as_optional_str(plan.get("plan_source")),
                source_intent_id=_as_optional_str(plan.get("source_intent_id")),
            )
        )
    clear_recovered_risk_exit_stuck_breaker(
        store,
        source="close_market_risk_plans_recovery",
        ts_ms=ts_ms,
        details={"market": str(market).strip().upper()},
    )


def find_latest_model_entry_intent(
    *,
    store: LiveStateStore,
    market: str,
    position: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    market_value = str(market).strip().upper()
    accepted_statuses = {"SUBMITTED", "UPDATED_FROM_WS", "UPDATED_FROM_CLOSED_ORDERS"}
    intents_by_id: dict[str, dict[str, Any]] = {}
    for item in store.list_intents():
        if str(item.get("market", "")).strip().upper() != market_value:
            continue
        if str(item.get("side", "")).strip().lower() != "bid":
            continue
        if str(item.get("status", "")).strip().upper() not in accepted_statuses:
            continue
        meta = item.get("meta")
        if not isinstance(meta, dict):
            continue
        submit_result = meta.get("submit_result")
        if not isinstance(submit_result, dict) or not bool(submit_result.get("accepted")):
            continue
        plan_payload = extract_model_exit_plan(meta)
        if plan_payload is None:
            continue
        intent_id = str(item.get("intent_id") or "").strip()
        if not intent_id:
            continue
        intents_by_id[intent_id] = {
            "intent_id": intent_id,
            "created_ts": int(item.get("ts_ms") or 0),
            "plan_payload": plan_payload,
            "meta": dict(meta),
            "reason_code": str(item.get("reason_code") or ""),
            "submitted_price": _safe_optional_float(item.get("price")),
            "submitted_volume": _safe_optional_float(item.get("volume")),
        }
    if not intents_by_id:
        return None
    target_price = _safe_float((position or {}).get("avg_entry_price"), default=0.0)
    target_qty = _safe_float((position or {}).get("base_amount"), default=0.0)
    saw_market_bid_order = False
    best: tuple[tuple[int, float, float, int], dict[str, Any]] | None = None
    for order in store.list_orders(open_only=False):
        if str(order.get("market", "")).strip().upper() != market_value:
            continue
        if str(order.get("side", "")).strip().lower() != "bid":
            continue
        saw_market_bid_order = True
        intent_id = str(order.get("intent_id") or "").strip()
        candidate = intents_by_id.get(intent_id)
        if candidate is None:
            continue
        volume_filled = _safe_float(order.get("volume_filled"), default=0.0)
        local_state = str(order.get("local_state") or "").strip().upper()
        state = str(order.get("state") or "").strip().lower()
        has_fill = int(volume_filled > 0.0 or local_state in {"DONE", "PARTIAL"} or state == "done")
        if has_fill <= 0:
            continue
        matched_qty = volume_filled if volume_filled > 0.0 else (_safe_float(order.get("volume_req"), default=0.0) or 0.0)
        matched_price = _safe_float(order.get("price"), default=0.0) or 0.0
        price_delta = abs(matched_price - target_price) if target_price > 0.0 else 0.0
        qty_delta = abs(matched_qty - target_qty) if target_qty > 0.0 else 0.0
        sort_key = (
            has_fill,
            -qty_delta,
            -price_delta,
            int(order.get("updated_ts") or candidate["created_ts"] or 0),
        )
        if best is None or sort_key > best[0]:
            best = (
                sort_key,
                {
                    **candidate,
                    "order_uuid": str(order.get("uuid") or "").strip() or None,
                    "order_identifier": str(order.get("identifier") or "").strip() or None,
                    "matched_entry_price": matched_price,
                    "matched_qty": matched_qty,
                },
            )
    if best is not None:
        return best[1]
    if position is not None:
        if saw_market_bid_order:
            return None
        if len(intents_by_id) != 1:
            return None
    return max(intents_by_id.values(), key=lambda item: int(item.get("created_ts") or 0))


def _risk_plan_differs_from_record(*, plan: dict[str, Any], record: RiskPlanRecord) -> bool:
    plan_tp = dict(plan.get("tp") or {})
    plan_sl = dict(plan.get("sl") or {})
    plan_trailing = dict(plan.get("trailing") or {})
    if _as_optional_str(plan.get("plan_source")) != record.plan_source:
        return True
    if _as_optional_str(plan.get("source_intent_id")) != record.source_intent_id:
        return True
    if _safe_optional_int(plan.get("timeout_ts_ms")) != record.timeout_ts_ms:
        return True
    if bool(plan_tp.get("enabled")) != bool(record.tp_enabled):
        return True
    if _safe_optional_float(plan_tp.get("tp_pct")) != _safe_optional_float(record.tp_pct):
        return True
    if bool(plan_sl.get("enabled")) != bool(record.sl_enabled):
        return True
    if _safe_optional_float(plan_sl.get("sl_pct")) != _safe_optional_float(record.sl_pct):
        return True
    if bool(plan_trailing.get("enabled")) != bool(record.trailing_enabled):
        return True
    if _safe_optional_float(plan_trailing.get("trail_pct")) != _safe_optional_float(record.trail_pct):
        return True
    return False


def _as_optional_str(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _safe_float(value: object, *, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _safe_int(value: object, *, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


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
