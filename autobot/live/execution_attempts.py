from __future__ import annotations

import json
from typing import Any

from autobot.execution.order_supervisor import slippage_bps

from .state_store import ExecutionAttemptRecord, LiveStateStore


def record_execution_attempt_submission(
    *,
    store: LiveStateStore,
    journal_id: str | None,
    intent_id: str,
    order_uuid: str | None,
    order_identifier: str | None,
    market: str,
    side: str,
    ord_type: str,
    time_in_force: str | None,
    meta_payload: dict[str, Any] | None,
    ts_ms: int,
) -> str:
    payload = dict(meta_payload or {})
    strategy = dict(payload.get("strategy") or {}) if isinstance(payload.get("strategy"), dict) else {}
    strategy_meta = dict(strategy.get("meta") or {}) if isinstance(strategy.get("meta"), dict) else {}
    trade_action = dict(strategy_meta.get("trade_action") or {}) if isinstance(strategy_meta.get("trade_action"), dict) else {}
    execution = dict(payload.get("execution") or {}) if isinstance(payload.get("execution"), dict) else {}
    execution_policy = (
        dict(payload.get("execution_policy") or {}) if isinstance(payload.get("execution_policy"), dict) else {}
    )
    micro_state = dict(payload.get("micro_state") or {}) if isinstance(payload.get("micro_state"), dict) else {}
    admissibility = dict(payload.get("admissibility") or {}) if isinstance(payload.get("admissibility"), dict) else {}
    decision = dict(admissibility.get("decision") or {}) if isinstance(admissibility.get("decision"), dict) else {}
    sizing = dict(admissibility.get("sizing") or {}) if isinstance(admissibility.get("sizing"), dict) else {}

    attempt_id = _build_attempt_id(
        intent_id=intent_id,
        order_uuid=order_uuid,
        ts_ms=ts_ms,
    )
    requested_price = _safe_optional_float(execution.get("requested_price"))
    requested_volume = _safe_optional_float(execution.get("requested_volume"))
    requested_notional_quote = _safe_optional_float(
        sizing.get("admissible_notional_quote", sizing.get("target_notional_quote"))
    )
    if requested_notional_quote is None and requested_price is not None and requested_volume is not None:
        requested_notional_quote = float(requested_price) * float(requested_volume)
    snapshot_age_ms = _safe_optional_int(micro_state.get("snapshot_age_ms"))
    record = ExecutionAttemptRecord(
        attempt_id=attempt_id,
        journal_id=_as_optional_str(journal_id),
        intent_id=_as_optional_str(intent_id),
        order_uuid=_as_optional_str(order_uuid),
        order_identifier=_as_optional_str(order_identifier),
        market=str(market).strip().upper(),
        side=str(side).strip().lower(),
        ord_type=str(ord_type).strip().lower() or "limit",
        time_in_force=_as_optional_str(time_in_force),
        action_code=_as_optional_str(execution_policy.get("selected_action_code")),
        price_mode=_as_optional_str((execution.get("exec_profile") or {}).get("price_mode")),
        requested_price=requested_price,
        requested_volume=requested_volume,
        requested_notional_quote=requested_notional_quote,
        reference_price=_safe_optional_float(execution.get("effective_ref_price")),
        tick_size=_safe_optional_float(((admissibility.get("snapshot") or {}).get("tick_size"))),
        spread_bps=_safe_optional_float(micro_state.get("spread_bps")),
        depth_top5_notional_krw=_safe_optional_float(micro_state.get("depth_top5_notional_krw")),
        trade_coverage_ms=_safe_optional_int(micro_state.get("trade_coverage_ms")),
        book_coverage_ms=_safe_optional_int(micro_state.get("book_coverage_ms")),
        snapshot_age_ms=snapshot_age_ms,
        micro_quality_score=_safe_optional_float(micro_state.get("micro_quality_score")),
        model_prob=_safe_optional_float(strategy_meta.get("model_prob")),
        expected_edge_bps=_safe_optional_float(
            decision.get("expected_edge_bps", _edge_bps_from_trade_action(trade_action))
        ),
        expected_net_edge_bps=_safe_optional_float(decision.get("expected_net_edge_bps")),
        expected_es_bps=_es_bps_from_trade_action(trade_action),
        submitted_ts_ms=int(ts_ms),
        outcome_json=json.dumps(
            {
                "status": "submitted",
                "execution_policy": execution_policy,
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
        updated_ts=int(ts_ms),
    )
    store.upsert_execution_attempt(record)
    return attempt_id


def update_execution_attempt_from_order(
    *,
    store: LiveStateStore,
    order: dict[str, Any],
    intent_id: str | None,
    ts_ms: int,
) -> str | None:
    attempt = _resolve_attempt(store=store, order=order, intent_id=intent_id)
    if not isinstance(attempt, dict):
        return None
    requested_volume = _safe_optional_float(attempt.get("requested_volume"))
    if requested_volume is None or requested_volume <= 0.0:
        requested_volume = _safe_optional_float(order.get("volume_req"))
    filled_volume = max(_safe_optional_float(order.get("volume_filled")) or 0.0, 0.0)
    fill_fraction = (
        min(max(float(filled_volume) / float(requested_volume), 0.0), 1.0)
        if requested_volume is not None and requested_volume > 0.0
        else None
    )
    first_fill_ts_ms = attempt.get("first_fill_ts_ms")
    if first_fill_ts_ms is None and filled_volume > 0.0:
        first_fill_ts_ms = int(ts_ms)
    local_state = str(order.get("local_state") or "").strip().upper()
    raw_state = str(order.get("state") or "").strip().lower()
    full_fill = bool(
        (fill_fraction is not None and fill_fraction >= 0.999999)
        or local_state == "FILLED"
        or raw_state == "done"
    )
    partial_fill = bool((filled_volume > 0.0) and not full_fill)
    final_state: str | None = None
    full_fill_ts_ms = attempt.get("full_fill_ts_ms")
    cancelled_ts_ms = attempt.get("cancelled_ts_ms")
    final_ts_ms = attempt.get("final_ts_ms")
    if full_fill:
        final_state = "FILLED"
        full_fill_ts_ms = int(ts_ms)
        final_ts_ms = int(ts_ms)
    elif local_state == "CANCELLED" or raw_state in {"cancel", "cancelled"}:
        final_state = "PARTIAL_CANCELLED" if partial_fill else "MISSED"
        cancelled_ts_ms = int(ts_ms)
        final_ts_ms = int(ts_ms)
    store.upsert_execution_attempt(
        ExecutionAttemptRecord(
            attempt_id=str(attempt.get("attempt_id") or ""),
            journal_id=_as_optional_str(attempt.get("journal_id")),
            intent_id=_as_optional_str(attempt.get("intent_id")),
            order_uuid=_as_optional_str(order.get("uuid")) or _as_optional_str(attempt.get("order_uuid")),
            order_identifier=_as_optional_str(order.get("identifier")) or _as_optional_str(attempt.get("order_identifier")),
            market=str(attempt.get("market") or order.get("market") or "").strip().upper(),
            side=str(attempt.get("side") or order.get("side") or "").strip().lower(),
            ord_type=str(attempt.get("ord_type") or order.get("ord_type") or "limit").strip().lower(),
            time_in_force=_as_optional_str(attempt.get("time_in_force")) or _as_optional_str(order.get("time_in_force")),
            action_code=_as_optional_str(attempt.get("action_code")),
            price_mode=_as_optional_str(attempt.get("price_mode")),
            requested_price=_safe_optional_float(attempt.get("requested_price")),
            requested_volume=requested_volume,
            requested_notional_quote=_safe_optional_float(attempt.get("requested_notional_quote")),
            reference_price=_safe_optional_float(attempt.get("reference_price")),
            tick_size=_safe_optional_float(attempt.get("tick_size")),
            spread_bps=_safe_optional_float(attempt.get("spread_bps")),
            depth_top5_notional_krw=_safe_optional_float(attempt.get("depth_top5_notional_krw")),
            trade_coverage_ms=_safe_optional_int(attempt.get("trade_coverage_ms")),
            book_coverage_ms=_safe_optional_int(attempt.get("book_coverage_ms")),
            snapshot_age_ms=_safe_optional_int(attempt.get("snapshot_age_ms")),
            micro_quality_score=_safe_optional_float(attempt.get("micro_quality_score")),
            model_prob=_safe_optional_float(attempt.get("model_prob")),
            expected_edge_bps=_safe_optional_float(attempt.get("expected_edge_bps")),
            expected_net_edge_bps=_safe_optional_float(attempt.get("expected_net_edge_bps")),
            expected_es_bps=_safe_optional_float(attempt.get("expected_es_bps")),
            submitted_ts_ms=int(attempt.get("submitted_ts_ms") or ts_ms),
            acknowledged_ts_ms=int(attempt.get("acknowledged_ts_ms") or ts_ms),
            first_fill_ts_ms=_safe_optional_int(first_fill_ts_ms),
            full_fill_ts_ms=_safe_optional_int(full_fill_ts_ms),
            cancelled_ts_ms=_safe_optional_int(cancelled_ts_ms),
            final_ts_ms=_safe_optional_int(final_ts_ms),
            final_state=final_state or _as_optional_str(attempt.get("final_state")),
            filled_price=_safe_optional_float(attempt.get("filled_price")),
            shortfall_bps=_safe_optional_float(attempt.get("shortfall_bps")),
            filled_volume=float(filled_volume),
            fill_fraction=fill_fraction,
            partial_fill=partial_fill,
            full_fill=full_fill,
            outcome_json=json.dumps(
                {
                    "status": "ws_update",
                    "raw_state": raw_state,
                    "local_state": local_state,
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            updated_ts=int(ts_ms),
        )
    )
    return str(attempt.get("attempt_id") or "")


def update_execution_attempt_fill_from_position(
    *,
    store: LiveStateStore,
    intent_id: str | None,
    journal_id: str | None,
    fill_price: float | None,
    filled_volume: float | None,
    ts_ms: int,
) -> str | None:
    attempt = None
    if _as_optional_str(intent_id) is not None:
        attempt = store.latest_execution_attempt_by_intent(intent_id=str(intent_id))
    if attempt is None and _as_optional_str(journal_id) is not None:
        candidates = [item for item in store.list_execution_attempts(limit=50) if str(item.get("journal_id") or "") == str(journal_id)]
        attempt = candidates[0] if candidates else None
    if not isinstance(attempt, dict):
        return None
    reference_price = _safe_optional_float(attempt.get("reference_price"))
    filled_price_value = _safe_optional_float(fill_price)
    side = str(attempt.get("side") or "").strip().lower()
    shortfall = None
    if filled_price_value is not None and reference_price is not None and reference_price > 0.0 and side in {"bid", "ask"}:
        shortfall = slippage_bps(side=side, fill_price=float(filled_price_value), ref_price=float(reference_price))
    submitted_ts_ms = int(attempt.get("submitted_ts_ms") or ts_ms)
    store.upsert_execution_attempt(
        ExecutionAttemptRecord(
            attempt_id=str(attempt.get("attempt_id") or ""),
            journal_id=_as_optional_str(attempt.get("journal_id")),
            intent_id=_as_optional_str(attempt.get("intent_id")),
            order_uuid=_as_optional_str(attempt.get("order_uuid")),
            order_identifier=_as_optional_str(attempt.get("order_identifier")),
            market=str(attempt.get("market") or "").strip().upper(),
            side=side,
            ord_type=str(attempt.get("ord_type") or "limit").strip().lower(),
            time_in_force=_as_optional_str(attempt.get("time_in_force")),
            action_code=_as_optional_str(attempt.get("action_code")),
            price_mode=_as_optional_str(attempt.get("price_mode")),
            requested_price=_safe_optional_float(attempt.get("requested_price")),
            requested_volume=_safe_optional_float(attempt.get("requested_volume")),
            requested_notional_quote=_safe_optional_float(attempt.get("requested_notional_quote")),
            reference_price=reference_price,
            tick_size=_safe_optional_float(attempt.get("tick_size")),
            spread_bps=_safe_optional_float(attempt.get("spread_bps")),
            depth_top5_notional_krw=_safe_optional_float(attempt.get("depth_top5_notional_krw")),
            trade_coverage_ms=_safe_optional_int(attempt.get("trade_coverage_ms")),
            book_coverage_ms=_safe_optional_int(attempt.get("book_coverage_ms")),
            snapshot_age_ms=_safe_optional_int(attempt.get("snapshot_age_ms")),
            micro_quality_score=_safe_optional_float(attempt.get("micro_quality_score")),
            model_prob=_safe_optional_float(attempt.get("model_prob")),
            expected_edge_bps=_safe_optional_float(attempt.get("expected_edge_bps")),
            expected_net_edge_bps=_safe_optional_float(attempt.get("expected_net_edge_bps")),
            expected_es_bps=_safe_optional_float(attempt.get("expected_es_bps")),
            submitted_ts_ms=submitted_ts_ms,
            acknowledged_ts_ms=_safe_optional_int(attempt.get("acknowledged_ts_ms")),
            first_fill_ts_ms=_safe_optional_int(attempt.get("first_fill_ts_ms")) or int(ts_ms),
            full_fill_ts_ms=_safe_optional_int(attempt.get("full_fill_ts_ms")) or int(ts_ms),
            cancelled_ts_ms=_safe_optional_int(attempt.get("cancelled_ts_ms")),
            final_ts_ms=_safe_optional_int(attempt.get("final_ts_ms")) or int(ts_ms),
            final_state=_as_optional_str(attempt.get("final_state")) or "FILLED",
            filled_price=filled_price_value,
            shortfall_bps=_safe_optional_float(shortfall),
            filled_volume=_safe_optional_float(filled_volume),
            fill_fraction=_resolve_fill_fraction(
                requested_volume=_safe_optional_float(attempt.get("requested_volume")),
                filled_volume=_safe_optional_float(filled_volume),
            ),
            partial_fill=bool(_resolve_fill_fraction(
                requested_volume=_safe_optional_float(attempt.get("requested_volume")),
                filled_volume=_safe_optional_float(filled_volume),
            ) not in {None, 0.0, 1.0}),
            full_fill=True,
            outcome_json=json.dumps({"status": "position_fill"}, ensure_ascii=False, sort_keys=True),
            updated_ts=int(ts_ms),
        )
    )
    return str(attempt.get("attempt_id") or "")


def mark_execution_attempt_cancelled(
    *,
    store: LiveStateStore,
    intent_id: str | None,
    order_uuid: str | None,
    ts_ms: int,
    final_state: str,
    outcome_payload: dict[str, Any] | None = None,
) -> str | None:
    attempt = None
    if _as_optional_str(order_uuid) is not None:
        attempt = store.execution_attempt_by_order_uuid(order_uuid=str(order_uuid))
    if attempt is None and _as_optional_str(intent_id) is not None:
        attempt = store.latest_execution_attempt_by_intent(intent_id=str(intent_id))
    if not isinstance(attempt, dict):
        return None
    store.upsert_execution_attempt(
        ExecutionAttemptRecord(
            attempt_id=str(attempt.get("attempt_id") or ""),
            journal_id=_as_optional_str(attempt.get("journal_id")),
            intent_id=_as_optional_str(attempt.get("intent_id")),
            order_uuid=_as_optional_str(order_uuid) or _as_optional_str(attempt.get("order_uuid")),
            order_identifier=_as_optional_str(attempt.get("order_identifier")),
            market=str(attempt.get("market") or "").strip().upper(),
            side=str(attempt.get("side") or "").strip().lower(),
            ord_type=str(attempt.get("ord_type") or "limit").strip().lower(),
            time_in_force=_as_optional_str(attempt.get("time_in_force")),
            action_code=_as_optional_str(attempt.get("action_code")),
            price_mode=_as_optional_str(attempt.get("price_mode")),
            requested_price=_safe_optional_float(attempt.get("requested_price")),
            requested_volume=_safe_optional_float(attempt.get("requested_volume")),
            requested_notional_quote=_safe_optional_float(attempt.get("requested_notional_quote")),
            reference_price=_safe_optional_float(attempt.get("reference_price")),
            tick_size=_safe_optional_float(attempt.get("tick_size")),
            spread_bps=_safe_optional_float(attempt.get("spread_bps")),
            depth_top5_notional_krw=_safe_optional_float(attempt.get("depth_top5_notional_krw")),
            trade_coverage_ms=_safe_optional_int(attempt.get("trade_coverage_ms")),
            book_coverage_ms=_safe_optional_int(attempt.get("book_coverage_ms")),
            snapshot_age_ms=_safe_optional_int(attempt.get("snapshot_age_ms")),
            micro_quality_score=_safe_optional_float(attempt.get("micro_quality_score")),
            model_prob=_safe_optional_float(attempt.get("model_prob")),
            expected_edge_bps=_safe_optional_float(attempt.get("expected_edge_bps")),
            expected_net_edge_bps=_safe_optional_float(attempt.get("expected_net_edge_bps")),
            expected_es_bps=_safe_optional_float(attempt.get("expected_es_bps")),
            submitted_ts_ms=int(attempt.get("submitted_ts_ms") or ts_ms),
            acknowledged_ts_ms=_safe_optional_int(attempt.get("acknowledged_ts_ms")),
            first_fill_ts_ms=_safe_optional_int(attempt.get("first_fill_ts_ms")),
            full_fill_ts_ms=_safe_optional_int(attempt.get("full_fill_ts_ms")),
            cancelled_ts_ms=int(ts_ms),
            final_ts_ms=int(ts_ms),
            final_state=str(final_state).strip().upper(),
            filled_price=_safe_optional_float(attempt.get("filled_price")),
            shortfall_bps=_safe_optional_float(attempt.get("shortfall_bps")),
            filled_volume=_safe_optional_float(attempt.get("filled_volume")),
            fill_fraction=_safe_optional_float(attempt.get("fill_fraction")),
            partial_fill=bool(attempt.get("partial_fill", False)),
            full_fill=bool(attempt.get("full_fill", False)),
            outcome_json=json.dumps(dict(outcome_payload or {}), ensure_ascii=False, sort_keys=True),
            updated_ts=int(ts_ms),
        )
    )
    return str(attempt.get("attempt_id") or "")


def rebind_execution_attempt_order(
    *,
    store: LiveStateStore,
    intent_id: str | None,
    previous_order_uuid: str | None,
    new_order_uuid: str | None,
    new_order_identifier: str | None,
    ts_ms: int,
) -> str | None:
    attempt = None
    if _as_optional_str(previous_order_uuid) is not None:
        attempt = store.execution_attempt_by_order_uuid(order_uuid=str(previous_order_uuid))
    if attempt is None and _as_optional_str(intent_id) is not None:
        attempt = store.latest_execution_attempt_by_intent(intent_id=str(intent_id))
    if not isinstance(attempt, dict):
        return None
    store.upsert_execution_attempt(
        ExecutionAttemptRecord(
            attempt_id=str(attempt.get("attempt_id") or ""),
            journal_id=_as_optional_str(attempt.get("journal_id")),
            intent_id=_as_optional_str(attempt.get("intent_id")),
            order_uuid=_as_optional_str(new_order_uuid),
            order_identifier=_as_optional_str(new_order_identifier),
            market=str(attempt.get("market") or "").strip().upper(),
            side=str(attempt.get("side") or "").strip().lower(),
            ord_type=str(attempt.get("ord_type") or "limit").strip().lower(),
            time_in_force=_as_optional_str(attempt.get("time_in_force")),
            action_code=_as_optional_str(attempt.get("action_code")),
            price_mode=_as_optional_str(attempt.get("price_mode")),
            requested_price=_safe_optional_float(attempt.get("requested_price")),
            requested_volume=_safe_optional_float(attempt.get("requested_volume")),
            requested_notional_quote=_safe_optional_float(attempt.get("requested_notional_quote")),
            reference_price=_safe_optional_float(attempt.get("reference_price")),
            tick_size=_safe_optional_float(attempt.get("tick_size")),
            spread_bps=_safe_optional_float(attempt.get("spread_bps")),
            depth_top5_notional_krw=_safe_optional_float(attempt.get("depth_top5_notional_krw")),
            trade_coverage_ms=_safe_optional_int(attempt.get("trade_coverage_ms")),
            book_coverage_ms=_safe_optional_int(attempt.get("book_coverage_ms")),
            snapshot_age_ms=_safe_optional_int(attempt.get("snapshot_age_ms")),
            micro_quality_score=_safe_optional_float(attempt.get("micro_quality_score")),
            model_prob=_safe_optional_float(attempt.get("model_prob")),
            expected_edge_bps=_safe_optional_float(attempt.get("expected_edge_bps")),
            expected_net_edge_bps=_safe_optional_float(attempt.get("expected_net_edge_bps")),
            expected_es_bps=_safe_optional_float(attempt.get("expected_es_bps")),
            submitted_ts_ms=int(attempt.get("submitted_ts_ms") or ts_ms),
            acknowledged_ts_ms=_safe_optional_int(attempt.get("acknowledged_ts_ms")),
            first_fill_ts_ms=_safe_optional_int(attempt.get("first_fill_ts_ms")),
            full_fill_ts_ms=_safe_optional_int(attempt.get("full_fill_ts_ms")),
            cancelled_ts_ms=_safe_optional_int(attempt.get("cancelled_ts_ms")),
            final_ts_ms=_safe_optional_int(attempt.get("final_ts_ms")),
            final_state=_as_optional_str(attempt.get("final_state")),
            filled_price=_safe_optional_float(attempt.get("filled_price")),
            shortfall_bps=_safe_optional_float(attempt.get("shortfall_bps")),
            filled_volume=_safe_optional_float(attempt.get("filled_volume")),
            fill_fraction=_safe_optional_float(attempt.get("fill_fraction")),
            partial_fill=bool(attempt.get("partial_fill", False)),
            full_fill=bool(attempt.get("full_fill", False)),
            outcome_json=json.dumps({"status": "rebound"}, ensure_ascii=False, sort_keys=True),
            updated_ts=int(ts_ms),
        )
    )
    return str(attempt.get("attempt_id") or "")


def _resolve_attempt(
    *,
    store: LiveStateStore,
    order: dict[str, Any],
    intent_id: str | None,
) -> dict[str, Any] | None:
    order_uuid = _as_optional_str(order.get("uuid"))
    if order_uuid is not None:
        attempt = store.execution_attempt_by_order_uuid(order_uuid=order_uuid)
        if attempt is not None:
            return attempt
    if _as_optional_str(intent_id) is not None:
        return store.latest_execution_attempt_by_intent(intent_id=str(intent_id))
    return None


def _build_attempt_id(*, intent_id: str, order_uuid: str | None, ts_ms: int) -> str:
    uuid_part = _as_optional_str(order_uuid) or f"submitted-{int(ts_ms)}"
    return f"exec-{str(intent_id).strip()}-{uuid_part}"


def _edge_bps_from_trade_action(trade_action: dict[str, Any]) -> float | None:
    value = _safe_optional_float(trade_action.get("expected_edge"))
    if value is None:
        return None
    return float(value) * 10_000.0


def _es_bps_from_trade_action(trade_action: dict[str, Any]) -> float | None:
    value = _safe_optional_float(
        trade_action.get("expected_es", trade_action.get("expected_downside_deviation"))
    )
    if value is None:
        return None
    return float(value) * 10_000.0


def _resolve_fill_fraction(*, requested_volume: float | None, filled_volume: float | None) -> float | None:
    if requested_volume is None or requested_volume <= 0.0 or filled_volume is None:
        return None
    return min(max(float(filled_volume) / float(requested_volume), 0.0), 1.0)


def _as_optional_str(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _safe_optional_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_optional_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None
