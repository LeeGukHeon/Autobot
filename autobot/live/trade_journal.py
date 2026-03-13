from __future__ import annotations

import json
from typing import Any

from autobot.common.model_exit_contract import normalize_model_exit_plan_payload
from autobot.execution.order_supervisor import slippage_bps

from .state_store import LiveStateStore, OrderRecord, TradeJournalRecord

TRADE_JOURNAL_STATUS_PENDING = "PENDING_ENTRY"
TRADE_JOURNAL_STATUS_OPEN = "OPEN"
TRADE_JOURNAL_STATUS_CLOSED = "CLOSED"
TRADE_JOURNAL_STATUS_CANCELLED = "CANCELLED_ENTRY"


def record_entry_submission(
    *,
    store: LiveStateStore,
    market: str,
    intent_id: str,
    requested_price: float | None,
    requested_volume: float | None,
    reason_code: str | None,
    meta_payload: dict[str, Any] | None,
    ts_ms: int,
    order_uuid: str | None = None,
    plan_id: str | None = None,
) -> str:
    intent_id_value = _as_optional_str(intent_id)
    market_value = str(market).strip().upper()
    if intent_id_value is None or not market_value:
        raise ValueError("intent_id and market are required")
    meta_dict = dict(meta_payload or {})
    meta_summary = _build_entry_meta_summary(meta_dict)
    existing = store.trade_journal_by_entry_intent(entry_intent_id=intent_id_value)
    entry_details = _extract_entry_details(meta_summary)
    journal_id = _resolve_journal_id(existing=existing, entry_intent_id=intent_id_value, market=market_value, ts_ms=ts_ms)
    requested_price_value = _as_optional_float(requested_price)
    requested_volume_value = _as_optional_float(requested_volume)
    entry_notional_quote = (
        requested_price_value * requested_volume_value
        if requested_price_value is not None and requested_volume_value is not None
        else None
    )
    store.upsert_trade_journal(
        TradeJournalRecord(
            journal_id=journal_id,
            market=market_value,
            status=_coalesce_str(_as_optional_str((existing or {}).get("status")), TRADE_JOURNAL_STATUS_PENDING),
            entry_intent_id=intent_id_value,
            entry_order_uuid=_coalesce_str(_as_optional_str(order_uuid), _as_optional_str((existing or {}).get("entry_order_uuid"))),
            exit_order_uuid=_as_optional_str((existing or {}).get("exit_order_uuid")),
            plan_id=_coalesce_str(_as_optional_str(plan_id), _as_optional_str((existing or {}).get("plan_id"))),
            entry_submitted_ts_ms=_coalesce_int(_as_optional_int((existing or {}).get("entry_submitted_ts_ms")), int(ts_ms)),
            entry_filled_ts_ms=_as_optional_int((existing or {}).get("entry_filled_ts_ms")),
            exit_ts_ms=_as_optional_int((existing or {}).get("exit_ts_ms")),
            entry_price=_coalesce_float(_as_optional_float((existing or {}).get("entry_price")), requested_price_value),
            exit_price=_as_optional_float((existing or {}).get("exit_price")),
            qty=_coalesce_float(_as_optional_float((existing or {}).get("qty")), requested_volume_value),
            entry_notional_quote=_coalesce_float(_as_optional_float((existing or {}).get("entry_notional_quote")), entry_notional_quote),
            exit_notional_quote=_as_optional_float((existing or {}).get("exit_notional_quote")),
            realized_pnl_quote=_as_optional_float((existing or {}).get("realized_pnl_quote")),
            realized_pnl_pct=_as_optional_float((existing or {}).get("realized_pnl_pct")),
            entry_reason_code=_coalesce_str(_as_optional_str((existing or {}).get("entry_reason_code")), _as_optional_str(reason_code)),
            close_reason_code=_as_optional_str((existing or {}).get("close_reason_code")),
            close_mode=_as_optional_str((existing or {}).get("close_mode")),
            model_prob=_coalesce_float(_as_optional_float((existing or {}).get("model_prob")), entry_details["model_prob"]),
            selection_policy_mode=_coalesce_str(
                _as_optional_str((existing or {}).get("selection_policy_mode")),
                entry_details["selection_policy_mode"],
            ),
            trade_action=_coalesce_str(_as_optional_str((existing or {}).get("trade_action")), entry_details["trade_action"]),
            expected_edge_bps=_coalesce_float(
                _as_optional_float((existing or {}).get("expected_edge_bps")),
                entry_details["expected_edge_bps"],
            ),
            expected_downside_bps=_coalesce_float(
                _as_optional_float((existing or {}).get("expected_downside_bps")),
                entry_details["expected_downside_bps"],
            ),
            expected_net_edge_bps=_coalesce_float(
                _as_optional_float((existing or {}).get("expected_net_edge_bps")),
                entry_details["expected_net_edge_bps"],
            ),
            notional_multiplier=_coalesce_float(
                _as_optional_float((existing or {}).get("notional_multiplier")),
                entry_details["notional_multiplier"],
            ),
            entry_meta_json=_json_dumps(meta_summary) if meta_summary else _json_dumps((existing or {}).get("entry_meta")),
            exit_meta_json=_json_dumps((existing or {}).get("exit_meta")),
            updated_ts=int(ts_ms),
        )
    )
    return journal_id


def activate_trade_journal_for_position(
    *,
    store: LiveStateStore,
    market: str,
    position: dict[str, Any],
    ts_ms: int,
    entry_intent: dict[str, Any] | None = None,
    plan_id: str | None = None,
) -> str:
    market_value = str(market).strip().upper()
    position_qty = _as_optional_float(position.get("base_amount"))
    position_entry_price = _as_optional_float(position.get("avg_entry_price"))
    entry_intent_id = _as_optional_str((entry_intent or {}).get("intent_id"))
    intent_record = store.intent_by_id(intent_id=entry_intent_id) if entry_intent_id else None
    plan_id_value = _coalesce_str(_as_optional_str(plan_id), _as_optional_str((entry_intent or {}).get("plan_id")))
    entry_order_uuid_value = _as_optional_str((entry_intent or {}).get("order_uuid"))
    existing = store.trade_journal_by_entry_intent(entry_intent_id=entry_intent_id) if entry_intent_id else None
    if existing is None and plan_id_value is not None:
        existing = store.trade_journal_by_plan_id(plan_id=plan_id_value)
    if existing is None and entry_order_uuid_value is not None:
        existing = store.trade_journal_by_entry_order_uuid(entry_order_uuid=entry_order_uuid_value)
    meta_dict = dict((intent_record or {}).get("meta") or {})
    meta_summary = _build_entry_meta_summary(meta_dict)
    entry_details = _extract_entry_details(meta_summary)
    entry_order_uuid = _coalesce_str(
        entry_order_uuid_value,
        _as_optional_str((existing or {}).get("entry_order_uuid")),
    )
    submitted_ts = _coalesce_int(
        _as_optional_int((existing or {}).get("entry_submitted_ts_ms")),
        _as_optional_int((intent_record or {}).get("ts_ms")),
        _as_optional_int((entry_intent or {}).get("created_ts")),
    )
    filled_ts = _coalesce_int(_as_optional_int(position.get("updated_ts")), int(ts_ms))
    journal_id = _resolve_journal_id(existing=existing, entry_intent_id=entry_intent_id, market=market_value, ts_ms=filled_ts)
    store.upsert_trade_journal(
        TradeJournalRecord(
            journal_id=journal_id,
            market=market_value,
            status=TRADE_JOURNAL_STATUS_OPEN,
            entry_intent_id=entry_intent_id,
            entry_order_uuid=entry_order_uuid,
            exit_order_uuid=_as_optional_str((existing or {}).get("exit_order_uuid")),
            plan_id=_coalesce_str(
                plan_id_value,
                _as_optional_str((existing or {}).get("plan_id")),
            ),
            entry_submitted_ts_ms=submitted_ts,
            entry_filled_ts_ms=filled_ts,
            exit_ts_ms=_as_optional_int((existing or {}).get("exit_ts_ms")),
            entry_price=_coalesce_float(position_entry_price, _as_optional_float((existing or {}).get("entry_price"))),
            exit_price=_as_optional_float((existing or {}).get("exit_price")),
            qty=_coalesce_float(position_qty, _as_optional_float((existing or {}).get("qty"))),
            entry_notional_quote=(
                position_entry_price * position_qty
                if position_entry_price is not None and position_qty is not None
                else _as_optional_float((existing or {}).get("entry_notional_quote"))
            ),
            exit_notional_quote=_as_optional_float((existing or {}).get("exit_notional_quote")),
            realized_pnl_quote=_as_optional_float((existing or {}).get("realized_pnl_quote")),
            realized_pnl_pct=_as_optional_float((existing or {}).get("realized_pnl_pct")),
            entry_reason_code=_coalesce_str(
                _as_optional_str((existing or {}).get("entry_reason_code")),
                _as_optional_str((intent_record or {}).get("reason_code")),
            ),
            close_reason_code=_as_optional_str((existing or {}).get("close_reason_code")),
            close_mode=_as_optional_str((existing or {}).get("close_mode")),
            model_prob=_coalesce_float(_as_optional_float((existing or {}).get("model_prob")), entry_details["model_prob"]),
            selection_policy_mode=_coalesce_str(
                _as_optional_str((existing or {}).get("selection_policy_mode")),
                entry_details["selection_policy_mode"],
            ),
            trade_action=_coalesce_str(_as_optional_str((existing or {}).get("trade_action")), entry_details["trade_action"]),
            expected_edge_bps=_coalesce_float(
                _as_optional_float((existing or {}).get("expected_edge_bps")),
                entry_details["expected_edge_bps"],
            ),
            expected_downside_bps=_coalesce_float(
                _as_optional_float((existing or {}).get("expected_downside_bps")),
                entry_details["expected_downside_bps"],
            ),
            expected_net_edge_bps=_coalesce_float(
                _as_optional_float((existing or {}).get("expected_net_edge_bps")),
                entry_details["expected_net_edge_bps"],
            ),
            notional_multiplier=_coalesce_float(
                _as_optional_float((existing or {}).get("notional_multiplier")),
                entry_details["notional_multiplier"],
            ),
            entry_meta_json=_json_dumps(meta_summary) if meta_summary else _json_dumps((existing or {}).get("entry_meta")),
            exit_meta_json=_json_dumps((existing or {}).get("exit_meta")),
            updated_ts=int(ts_ms),
        )
    )
    return journal_id


def rebind_pending_entry_journal_order(
    *,
    store: LiveStateStore,
    entry_intent_id: str | None = None,
    previous_entry_order_uuid: str | None = None,
    new_entry_order_uuid: str | None,
    ts_ms: int,
) -> str | None:
    existing = (
        store.trade_journal_by_entry_intent(entry_intent_id=entry_intent_id)
        if _as_optional_str(entry_intent_id) is not None
        else None
    )
    if existing is None and _as_optional_str(previous_entry_order_uuid) is not None:
        existing = store.trade_journal_by_entry_order_uuid(entry_order_uuid=str(previous_entry_order_uuid))
    if existing is None:
        return None
    if str(existing.get("status") or "").strip().upper() != TRADE_JOURNAL_STATUS_PENDING:
        return _as_optional_str(existing.get("journal_id"))
    store.upsert_trade_journal(
        TradeJournalRecord(
            journal_id=str(existing.get("journal_id") or ""),
            market=str(existing.get("market") or ""),
            status=TRADE_JOURNAL_STATUS_PENDING,
            entry_intent_id=_coalesce_str(_as_optional_str(entry_intent_id), _as_optional_str(existing.get("entry_intent_id"))),
            entry_order_uuid=_coalesce_str(_as_optional_str(new_entry_order_uuid), _as_optional_str(existing.get("entry_order_uuid"))),
            exit_order_uuid=_as_optional_str(existing.get("exit_order_uuid")),
            plan_id=_as_optional_str(existing.get("plan_id")),
            entry_submitted_ts_ms=_as_optional_int(existing.get("entry_submitted_ts_ms")),
            entry_filled_ts_ms=_as_optional_int(existing.get("entry_filled_ts_ms")),
            exit_ts_ms=_as_optional_int(existing.get("exit_ts_ms")),
            entry_price=_as_optional_float(existing.get("entry_price")),
            exit_price=_as_optional_float(existing.get("exit_price")),
            qty=_as_optional_float(existing.get("qty")),
            entry_notional_quote=_as_optional_float(existing.get("entry_notional_quote")),
            exit_notional_quote=_as_optional_float(existing.get("exit_notional_quote")),
            realized_pnl_quote=_as_optional_float(existing.get("realized_pnl_quote")),
            realized_pnl_pct=_as_optional_float(existing.get("realized_pnl_pct")),
            entry_reason_code=_as_optional_str(existing.get("entry_reason_code")),
            close_reason_code=_as_optional_str(existing.get("close_reason_code")),
            close_mode=_as_optional_str(existing.get("close_mode")),
            model_prob=_as_optional_float(existing.get("model_prob")),
            selection_policy_mode=_as_optional_str(existing.get("selection_policy_mode")),
            trade_action=_as_optional_str(existing.get("trade_action")),
            expected_edge_bps=_as_optional_float(existing.get("expected_edge_bps")),
            expected_downside_bps=_as_optional_float(existing.get("expected_downside_bps")),
            expected_net_edge_bps=_as_optional_float(existing.get("expected_net_edge_bps")),
            notional_multiplier=_as_optional_float(existing.get("notional_multiplier")),
            entry_meta_json=_json_dumps(existing.get("entry_meta")),
            exit_meta_json=_json_dumps(existing.get("exit_meta")),
            updated_ts=int(ts_ms),
        )
    )
    return _as_optional_str(existing.get("journal_id"))


def cancel_pending_entry_journal(
    *,
    store: LiveStateStore,
    market: str,
    ts_ms: int,
    entry_intent_id: str | None = None,
    entry_order_uuid: str | None = None,
    close_reason_code: str | None = None,
    close_mode: str | None = None,
    exit_meta: dict[str, Any] | None = None,
) -> str | None:
    market_value = str(market).strip().upper()
    existing = (
        store.trade_journal_by_entry_intent(entry_intent_id=entry_intent_id)
        if _as_optional_str(entry_intent_id) is not None
        else None
    )
    if existing is None and _as_optional_str(entry_order_uuid) is not None:
        existing = store.trade_journal_by_entry_order_uuid(entry_order_uuid=str(entry_order_uuid))
    if existing is None:
        existing = _latest_live_trade_journal(store=store, market=market_value)
    if existing is None:
        return None
    if str(existing.get("status") or "").strip().upper() != TRADE_JOURNAL_STATUS_PENDING:
        return _as_optional_str(existing.get("journal_id"))
    exit_meta_payload = dict(exit_meta or {})
    exit_meta_payload.setdefault("close_mode", _coalesce_str(_as_optional_str(close_mode), "entry_order_timeout"))
    exit_meta_payload.setdefault(
        "close_reason_code",
        _coalesce_str(_as_optional_str(close_reason_code), "ENTRY_ORDER_TIMEOUT"),
    )
    exit_meta_payload.setdefault("entry_cancelled", True)
    store.upsert_trade_journal(
        TradeJournalRecord(
            journal_id=str(existing.get("journal_id") or ""),
            market=market_value,
            status=TRADE_JOURNAL_STATUS_CANCELLED,
            entry_intent_id=_as_optional_str(existing.get("entry_intent_id")),
            entry_order_uuid=_coalesce_str(_as_optional_str(entry_order_uuid), _as_optional_str(existing.get("entry_order_uuid"))),
            exit_order_uuid=None,
            plan_id=_as_optional_str(existing.get("plan_id")),
            entry_submitted_ts_ms=_as_optional_int(existing.get("entry_submitted_ts_ms")),
            entry_filled_ts_ms=None,
            exit_ts_ms=int(ts_ms),
            entry_price=_as_optional_float(existing.get("entry_price")),
            exit_price=None,
            qty=_as_optional_float(existing.get("qty")),
            entry_notional_quote=_as_optional_float(existing.get("entry_notional_quote")),
            exit_notional_quote=None,
            realized_pnl_quote=None,
            realized_pnl_pct=None,
            entry_reason_code=_as_optional_str(existing.get("entry_reason_code")),
            close_reason_code=_coalesce_str(_as_optional_str(close_reason_code), "ENTRY_ORDER_TIMEOUT"),
            close_mode=_coalesce_str(_as_optional_str(close_mode), "entry_order_timeout"),
            model_prob=_as_optional_float(existing.get("model_prob")),
            selection_policy_mode=_as_optional_str(existing.get("selection_policy_mode")),
            trade_action=_as_optional_str(existing.get("trade_action")),
            expected_edge_bps=_as_optional_float(existing.get("expected_edge_bps")),
            expected_downside_bps=_as_optional_float(existing.get("expected_downside_bps")),
            expected_net_edge_bps=_as_optional_float(existing.get("expected_net_edge_bps")),
            notional_multiplier=_as_optional_float(existing.get("notional_multiplier")),
            entry_meta_json=_json_dumps(existing.get("entry_meta")),
            exit_meta_json=_json_dumps(exit_meta_payload),
            updated_ts=int(ts_ms),
        )
    )
    return _as_optional_str(existing.get("journal_id"))


def close_trade_journal_for_market(
    *,
    store: LiveStateStore,
    market: str,
    position: dict[str, Any],
    ts_ms: int,
    exit_price: float | None = None,
    close_mode: str | None = None,
    close_reason_code: str | None = None,
    exit_order_uuid: str | None = None,
    plan_id: str | None = None,
    exit_meta: dict[str, Any] | None = None,
) -> str | None:
    market_value = str(market).strip().upper()
    resolved_plan = _as_optional_str(plan_id)
    existing = store.trade_journal_by_plan_id(plan_id=resolved_plan) if resolved_plan is not None else None
    if existing is None and exit_order_uuid is not None:
        exit_order_direct = next(
            (item for item in store.list_orders(open_only=False) if str(item.get("uuid") or "").strip() == str(exit_order_uuid).strip()),
            None,
        )
        plan_from_order = _as_optional_str((exit_order_direct or {}).get("tp_sl_link"))
        if plan_from_order is not None:
            resolved_plan = resolved_plan or plan_from_order
            existing = store.trade_journal_by_plan_id(plan_id=plan_from_order)
    min_exit_order_ts = _coalesce_int(
        _as_optional_int((existing or {}).get("entry_filled_ts_ms")),
        _as_optional_int((existing or {}).get("entry_submitted_ts_ms")),
    )
    latest_done_exit = _resolve_exit_order_for_journal(
        store=store,
        market=market_value,
        plan_id=_as_optional_str((existing or {}).get("plan_id")),
        exit_order_uuid=_as_optional_str((existing or {}).get("exit_order_uuid")),
        min_updated_ts=min_exit_order_ts,
        target_exit_ts=_as_optional_int((existing or {}).get("exit_ts_ms")),
    )
    if existing is None:
        lookup_exit_uuid = _coalesce_str(
            _as_optional_str(exit_order_uuid),
            _as_optional_str((latest_done_exit or {}).get("uuid")),
        )
        if lookup_exit_uuid is not None:
            existing = store.trade_journal_by_exit_order_uuid(exit_order_uuid=lookup_exit_uuid)
    if existing is None:
        risk_plan = _risk_plan_for_close(store=store, market=market_value, plan_id=resolved_plan)
        fallback_intent_id = _as_optional_str((risk_plan or {}).get("source_intent_id"))
        if resolved_plan is None:
            resolved_plan = _as_optional_str((risk_plan or {}).get("plan_id"))
        if fallback_intent_id:
            existing = store.trade_journal_by_entry_intent(entry_intent_id=fallback_intent_id)
        if existing is None:
            existing = {
                "journal_id": f"imported-{market_value}-{int(ts_ms)}",
                "market": market_value,
                "status": TRADE_JOURNAL_STATUS_OPEN,
                "entry_price": _as_optional_float(position.get("avg_entry_price")),
                "qty": _as_optional_float(position.get("base_amount")),
                "entry_notional_quote": (
                    _as_optional_float(position.get("avg_entry_price")) * _as_optional_float(position.get("base_amount"))
                    if _as_optional_float(position.get("avg_entry_price")) is not None
                    and _as_optional_float(position.get("base_amount")) is not None
                    else None
                ),
                "entry_meta": {"source": "position_sync_import"},
            }
    if existing is None:
        return None

    entry_order = _resolve_entry_order_for_journal(
        store=store,
        market=market_value,
        entry_order_uuid=_as_optional_str((existing or {}).get("entry_order_uuid")),
        entry_intent_id=_as_optional_str((existing or {}).get("entry_intent_id")),
        target_entry_ts=_coalesce_int(
            _as_optional_int((existing or {}).get("entry_filled_ts_ms")),
            _as_optional_int((existing or {}).get("entry_submitted_ts_ms")),
        ),
    )
    qty = _coalesce_float(
        _filled_qty_from_order(entry_order),
        _as_optional_float((existing or {}).get("qty")),
        _as_optional_float(position.get("base_amount")),
    )
    entry_price = _coalesce_float(
        _filled_price_from_order(entry_order),
        _as_optional_float((existing or {}).get("entry_price")),
        _as_optional_float(position.get("avg_entry_price")),
    )
    resolved_exit_price = _coalesce_float(
        _filled_price_from_order(latest_done_exit),
        _as_optional_float(exit_price),
        _as_optional_float((latest_done_exit or {}).get("price")),
        entry_price,
    )
    resolved_exit_order_uuid = _coalesce_str(
        _as_optional_str(exit_order_uuid),
        _as_optional_str((latest_done_exit or {}).get("uuid")),
        _as_optional_str((existing or {}).get("exit_order_uuid")),
    )
    exit_ts = _coalesce_int(
        _as_optional_int((latest_done_exit or {}).get("updated_ts")),
        int(ts_ms),
    )
    resolved_close_reason = _coalesce_str(
        _as_optional_str(close_reason_code),
        _as_optional_str((latest_done_exit or {}).get("last_event_name")),
        _as_optional_str((latest_done_exit or {}).get("raw_exchange_state")),
        "POSITION_CLOSED",
    )
    resolved_close_mode = _coalesce_str(
        _as_optional_str(close_mode),
        _derive_close_mode(order=latest_done_exit),
        "position_sync",
    )
    resolved_plan = _coalesce_str(
        resolved_plan,
        _as_optional_str((existing or {}).get("plan_id")),
        _as_optional_str((_risk_plan_for_close(store=store, market=market_value, plan_id=resolved_plan) or {}).get("plan_id")),
    )
    realized_pnl_quote = (
        (resolved_exit_price - entry_price) * qty
        if resolved_exit_price is not None and entry_price is not None and qty is not None
        else None
    )
    realized_pnl_pct = (
        ((resolved_exit_price / entry_price) - 1.0) * 100.0
        if resolved_exit_price is not None and entry_price is not None and entry_price > 0.0
        else None
    )
    exit_meta_payload = dict(exit_meta or {})
    if latest_done_exit is not None:
        exit_meta_payload.setdefault(
            "exit_order",
            {
                "uuid": latest_done_exit.get("uuid"),
                "identifier": latest_done_exit.get("identifier"),
                "price": latest_done_exit.get("price"),
                "state": latest_done_exit.get("state"),
                "local_state": latest_done_exit.get("local_state"),
                "last_event_name": latest_done_exit.get("last_event_name"),
                "updated_ts": latest_done_exit.get("updated_ts"),
            },
        )
    exit_meta_payload.setdefault("close_mode", resolved_close_mode)
    exit_meta_payload.setdefault("close_reason_code", resolved_close_reason)
    entry_meta_summary = _build_entry_meta_summary((existing or {}).get("entry_meta"))
    cost_metrics = _compute_cost_metrics(
        entry_meta=entry_meta_summary,
        entry_order=entry_order,
        exit_order=latest_done_exit,
        entry_price=entry_price,
        exit_price=resolved_exit_price,
        qty=qty,
    )
    exit_meta_payload.update(
        {
            "gross_pnl_quote": cost_metrics["gross_pnl_quote"],
            "gross_pnl_pct": cost_metrics["gross_pnl_pct"],
            "entry_fee_quote": cost_metrics["entry_fee_quote"],
            "exit_fee_quote": cost_metrics["exit_fee_quote"],
            "total_fee_quote": cost_metrics["total_fee_quote"],
            "entry_fee_bps": cost_metrics["entry_fee_bps"],
            "exit_fee_bps": cost_metrics["exit_fee_bps"],
            "entry_realized_slippage_bps": cost_metrics["entry_realized_slippage_bps"],
            "exit_expected_slippage_bps": cost_metrics["exit_expected_slippage_bps"],
            "pnl_basis": "net_after_fees__slippage_embedded_in_fill_prices",
        }
    )
    store.upsert_trade_journal(
        TradeJournalRecord(
            journal_id=str((existing or {}).get("journal_id") or f"imported-{market_value}-{exit_ts}"),
            market=market_value,
            status=TRADE_JOURNAL_STATUS_CLOSED,
            entry_intent_id=_as_optional_str((existing or {}).get("entry_intent_id")),
            entry_order_uuid=_as_optional_str((existing or {}).get("entry_order_uuid")),
            exit_order_uuid=resolved_exit_order_uuid,
            plan_id=resolved_plan,
            entry_submitted_ts_ms=_as_optional_int((existing or {}).get("entry_submitted_ts_ms")),
            entry_filled_ts_ms=_as_optional_int((existing or {}).get("entry_filled_ts_ms")),
            exit_ts_ms=exit_ts,
            entry_price=entry_price,
            exit_price=resolved_exit_price,
            qty=qty,
            entry_notional_quote=cost_metrics["entry_total_quote"],
            exit_notional_quote=cost_metrics["exit_total_quote"],
            realized_pnl_quote=cost_metrics["net_pnl_quote"],
            realized_pnl_pct=cost_metrics["net_pnl_pct"],
            entry_reason_code=_as_optional_str((existing or {}).get("entry_reason_code")),
            close_reason_code=resolved_close_reason,
            close_mode=resolved_close_mode,
            model_prob=_as_optional_float((existing or {}).get("model_prob")),
            selection_policy_mode=_as_optional_str((existing or {}).get("selection_policy_mode")),
            trade_action=_as_optional_str((existing or {}).get("trade_action")),
            expected_edge_bps=_as_optional_float((existing or {}).get("expected_edge_bps")),
            expected_downside_bps=_as_optional_float((existing or {}).get("expected_downside_bps")),
            expected_net_edge_bps=_as_optional_float((existing or {}).get("expected_net_edge_bps")),
            notional_multiplier=_as_optional_float((existing or {}).get("notional_multiplier")),
            entry_meta_json=_json_dumps(entry_meta_summary),
            exit_meta_json=_json_dumps(exit_meta_payload),
            updated_ts=exit_ts,
        )
    )
    return str((existing or {}).get("journal_id") or f"imported-{market_value}-{exit_ts}")


def recompute_trade_journal_records(*, store: LiveStateStore) -> dict[str, Any]:
    rows = store.list_trade_journal()
    updated = 0
    compacted = 0
    for row in rows:
        market = str(row.get("market") or "").strip().upper()
        if not market:
            continue
        entry_intent_id = _as_optional_str(row.get("entry_intent_id"))
        intent_record = store.intent_by_id(intent_id=entry_intent_id) if entry_intent_id else None
        entry_meta_raw = (intent_record or {}).get("meta") if isinstance((intent_record or {}).get("meta"), dict) else row.get("entry_meta")
        entry_meta_summary = _build_entry_meta_summary(entry_meta_raw)
        exit_meta_payload = _build_exit_meta_summary(row.get("exit_meta"))
        if str(row.get("status") or "").strip().upper() == TRADE_JOURNAL_STATUS_CLOSED:
            entry_ts = _coalesce_int(_as_optional_int(row.get("entry_filled_ts_ms")), _as_optional_int(row.get("entry_submitted_ts_ms")))
            entry_order = _resolve_entry_order_for_journal(
                store=store,
                market=market,
                entry_order_uuid=_as_optional_str(row.get("entry_order_uuid")),
                entry_intent_id=entry_intent_id,
                target_entry_ts=entry_ts,
            )
            exit_order = _resolve_exit_order_for_journal(
                store=store,
                market=market,
                plan_id=_as_optional_str(row.get("plan_id")),
                exit_order_uuid=_as_optional_str(row.get("exit_order_uuid")),
                min_updated_ts=entry_ts,
                target_exit_ts=_as_optional_int(row.get("exit_ts_ms")),
            )
            qty = _coalesce_float(
                _filled_qty_from_order(entry_order),
                _as_optional_float(row.get("qty")),
            )
            entry_price = _coalesce_float(
                _filled_price_from_order(entry_order),
                _as_optional_float(row.get("entry_price")),
            )
            exit_price = _coalesce_float(
                _filled_price_from_order(exit_order),
                _as_optional_float((exit_order or {}).get("price")),
                _as_optional_float(row.get("exit_price")),
            )
            exit_ts = _coalesce_int(_as_optional_int((exit_order or {}).get("updated_ts")), _as_optional_int(row.get("exit_ts_ms")))
            cost_metrics = _compute_cost_metrics(
                entry_meta=entry_meta_summary,
                entry_order=entry_order,
                exit_order=exit_order,
                entry_price=entry_price,
                exit_price=exit_price,
                qty=qty,
            )
            exit_meta_payload.update(
                {
                    "gross_pnl_quote": cost_metrics["gross_pnl_quote"],
                    "gross_pnl_pct": cost_metrics["gross_pnl_pct"],
                    "entry_fee_quote": cost_metrics["entry_fee_quote"],
                    "exit_fee_quote": cost_metrics["exit_fee_quote"],
                    "total_fee_quote": cost_metrics["total_fee_quote"],
                    "entry_fee_bps": cost_metrics["entry_fee_bps"],
                    "exit_fee_bps": cost_metrics["exit_fee_bps"],
                    "entry_realized_slippage_bps": cost_metrics["entry_realized_slippage_bps"],
                    "exit_expected_slippage_bps": cost_metrics["exit_expected_slippage_bps"],
                    "pnl_basis": "net_after_fees__slippage_embedded_in_fill_prices",
                }
            )
            store.upsert_trade_journal(
                TradeJournalRecord(
                    journal_id=str(row.get("journal_id")),
                    market=market,
                    status=str(row.get("status")),
                    entry_intent_id=entry_intent_id,
                    entry_order_uuid=_as_optional_str(row.get("entry_order_uuid")),
                    exit_order_uuid=_coalesce_str(_as_optional_str((exit_order or {}).get("uuid")), _as_optional_str(row.get("exit_order_uuid"))),
                    plan_id=_as_optional_str(row.get("plan_id")),
                    entry_submitted_ts_ms=_as_optional_int(row.get("entry_submitted_ts_ms")),
                    entry_filled_ts_ms=_as_optional_int(row.get("entry_filled_ts_ms")),
                    exit_ts_ms=exit_ts,
                    entry_price=entry_price,
                    exit_price=exit_price,
                    qty=qty,
                    entry_notional_quote=cost_metrics["entry_total_quote"],
                    exit_notional_quote=cost_metrics["exit_total_quote"],
                    realized_pnl_quote=cost_metrics["net_pnl_quote"],
                    realized_pnl_pct=cost_metrics["net_pnl_pct"],
                    entry_reason_code=_as_optional_str(row.get("entry_reason_code")),
                    close_reason_code=_coalesce_str(_as_optional_str(row.get("close_reason_code")), _as_optional_str((exit_order or {}).get("last_event_name"))),
                    close_mode=_coalesce_str(_as_optional_str(row.get("close_mode")), _derive_close_mode(order=exit_order)),
                    model_prob=_as_optional_float(row.get("model_prob")),
                    selection_policy_mode=_as_optional_str(row.get("selection_policy_mode")),
                    trade_action=_as_optional_str(row.get("trade_action")),
                    expected_edge_bps=_as_optional_float(row.get("expected_edge_bps")),
                    expected_downside_bps=_as_optional_float(row.get("expected_downside_bps")),
                    expected_net_edge_bps=_as_optional_float(row.get("expected_net_edge_bps")),
                    notional_multiplier=_as_optional_float(row.get("notional_multiplier")),
                    entry_meta_json=_json_dumps(entry_meta_summary),
                    exit_meta_json=_json_dumps(exit_meta_payload),
                    updated_ts=_coalesce_int(exit_ts, _as_optional_int(row.get("updated_ts")), 0) or 0,
                )
            )
            updated += 1
        else:
            status_value = str(row.get("status") or "")
            entry_order = _resolve_entry_order_for_journal(
                store=store,
                market=market,
                entry_order_uuid=_as_optional_str(row.get("entry_order_uuid")),
                entry_intent_id=entry_intent_id,
                target_entry_ts=_coalesce_int(
                    _as_optional_int(row.get("entry_filled_ts_ms")),
                    _as_optional_int(row.get("entry_submitted_ts_ms")),
                ),
            )
            if (
                status_value.strip().upper() == TRADE_JOURNAL_STATUS_PENDING
                and isinstance(entry_order, dict)
                and str(entry_order.get("local_state") or "").strip().upper() == "CANCELLED"
                and _filled_qty_from_order(entry_order) in (None, 0.0)
            ):
                exit_meta_payload["close_mode"] = _coalesce_str(
                    _as_optional_str(exit_meta_payload.get("close_mode")),
                    "entry_order_timeout",
                )
                exit_meta_payload["close_reason_code"] = _coalesce_str(
                    _as_optional_str(exit_meta_payload.get("close_reason_code")),
                    _as_optional_str(entry_order.get("last_event_name")),
                    "ENTRY_ORDER_TIMEOUT",
                )
                exit_meta_payload["entry_cancelled"] = True
                status_value = TRADE_JOURNAL_STATUS_CANCELLED
            store.upsert_trade_journal(
                TradeJournalRecord(
                    journal_id=str(row.get("journal_id")),
                    market=market,
                    status=status_value,
                    entry_intent_id=entry_intent_id,
                    entry_order_uuid=_as_optional_str(row.get("entry_order_uuid")),
                    exit_order_uuid=_as_optional_str(row.get("exit_order_uuid")),
                    plan_id=_as_optional_str(row.get("plan_id")),
                    entry_submitted_ts_ms=_as_optional_int(row.get("entry_submitted_ts_ms")),
                    entry_filled_ts_ms=_as_optional_int(row.get("entry_filled_ts_ms")),
                    exit_ts_ms=_coalesce_int(
                        _as_optional_int(row.get("exit_ts_ms")),
                        _as_optional_int((entry_order or {}).get("updated_ts")),
                    ),
                    entry_price=_as_optional_float(row.get("entry_price")),
                    exit_price=_as_optional_float(row.get("exit_price")),
                    qty=_as_optional_float(row.get("qty")),
                    entry_notional_quote=_as_optional_float(row.get("entry_notional_quote")),
                    exit_notional_quote=_as_optional_float(row.get("exit_notional_quote")),
                    realized_pnl_quote=_as_optional_float(row.get("realized_pnl_quote")),
                    realized_pnl_pct=_as_optional_float(row.get("realized_pnl_pct")),
                    entry_reason_code=_as_optional_str(row.get("entry_reason_code")),
                    close_reason_code=_coalesce_str(
                        _as_optional_str(row.get("close_reason_code")),
                        _as_optional_str(exit_meta_payload.get("close_reason_code")),
                    ),
                    close_mode=_coalesce_str(
                        _as_optional_str(row.get("close_mode")),
                        _as_optional_str(exit_meta_payload.get("close_mode")),
                    ),
                    model_prob=_as_optional_float(row.get("model_prob")),
                    selection_policy_mode=_as_optional_str(row.get("selection_policy_mode")),
                    trade_action=_as_optional_str(row.get("trade_action")),
                    expected_edge_bps=_as_optional_float(row.get("expected_edge_bps")),
                    expected_downside_bps=_as_optional_float(row.get("expected_downside_bps")),
                    expected_net_edge_bps=_as_optional_float(row.get("expected_net_edge_bps")),
                    notional_multiplier=_as_optional_float(row.get("notional_multiplier")),
                    entry_meta_json=_json_dumps(entry_meta_summary),
                    exit_meta_json=_json_dumps(exit_meta_payload),
                    updated_ts=_as_optional_int(row.get("updated_ts")) or 0,
                )
            )
        compacted += 1
    return {"rows_total": len(rows), "rows_updated": updated, "rows_compacted": compacted}


def backfill_order_execution_details(
    *,
    store: LiveStateStore,
    client: Any,
    max_orders: int | None = 64,
    target_markets: set[str] | None = None,
    target_order_uuids: set[str] | None = None,
) -> dict[str, Any]:
    scanned = 0
    updated = 0
    failed = 0
    for order in store.list_orders(open_only=False):
        if str(order.get("local_state") or "").strip().upper() != "DONE":
            continue
        if target_markets is not None and str(order.get("market") or "").strip().upper() not in target_markets:
            continue
        if target_order_uuids is not None and str(order.get("uuid") or "").strip() not in target_order_uuids:
            continue
        if max_orders is not None and scanned >= int(max_orders):
            break
        if _as_optional_float(order.get("executed_funds")) is not None and _as_optional_float(order.get("paid_fee")) is not None:
            continue
        uuid = _as_optional_str(order.get("uuid"))
        identifier = _as_optional_str(order.get("identifier"))
        if uuid is None and identifier is None:
            continue
        scanned += 1
        try:
            payload = client.order(uuid=uuid, identifier=identifier)
        except Exception:
            failed += 1
            continue
        if not isinstance(payload, dict):
            failed += 1
            continue
        store.upsert_order(_order_record_with_execution_details(order=order, payload=payload))
        updated += 1
    return {"orders_scanned": scanned, "orders_updated": updated, "orders_failed": failed}


def _latest_live_trade_journal(*, store: LiveStateStore, market: str) -> dict[str, Any] | None:
    rows = store.list_trade_journal(
        market=market,
        statuses=(TRADE_JOURNAL_STATUS_OPEN, TRADE_JOURNAL_STATUS_PENDING),
        limit=1,
    )
    return rows[0] if rows else None


def _risk_plan_for_close(
    *,
    store: LiveStateStore,
    market: str,
    plan_id: str | None,
) -> dict[str, Any] | None:
    if plan_id:
        direct = store.risk_plan_by_id(plan_id=plan_id)
        if direct is not None:
            return direct
    active_rows = store.list_risk_plans(market=market, states=("ACTIVE", "TRIGGERED", "EXITING"))
    if len(active_rows) == 1:
        return active_rows[0]
    return None


def _resolve_entry_order_for_journal(
    *,
    store: LiveStateStore,
    market: str,
    entry_order_uuid: str | None = None,
    entry_intent_id: str | None = None,
    target_entry_ts: int | None = None,
) -> dict[str, Any] | None:
    if entry_order_uuid:
        direct = next((item for item in store.list_orders(open_only=False) if str(item.get("uuid") or "") == str(entry_order_uuid)), None)
        if direct is not None and str(direct.get("side") or "").strip().lower() == "bid":
            return direct
    candidates: list[dict[str, Any]] = []
    for order in store.list_orders(open_only=False):
        if str(order.get("market") or "").strip().upper() != market:
            continue
        if str(order.get("side") or "").strip().lower() != "bid":
            continue
        if entry_intent_id and str(order.get("intent_id") or "").strip() != entry_intent_id:
            continue
        candidates.append(order)
    if not candidates:
        return None
    if target_entry_ts is not None:
        return min(
            candidates,
            key=lambda item: (
                abs(int(item.get("updated_ts") or 0) - int(target_entry_ts)),
                abs(int(item.get("created_ts") or 0) - int(target_entry_ts)),
            ),
        )
    return max(candidates, key=lambda item: (int(item.get("updated_ts") or 0), int(item.get("created_ts") or 0)))


def _resolve_exit_order_for_journal(
    *,
    store: LiveStateStore,
    market: str,
    plan_id: str | None = None,
    exit_order_uuid: str | None = None,
    min_updated_ts: int | None = None,
    target_exit_ts: int | None = None,
) -> dict[str, Any] | None:
    if exit_order_uuid:
        direct = next((item for item in store.list_orders(open_only=False) if str(item.get("uuid") or "") == str(exit_order_uuid)), None)
        if direct is not None and str(direct.get("local_state") or "").strip().upper() == "DONE":
            if min_updated_ts is None or int(direct.get("updated_ts") or 0) >= int(min_updated_ts):
                return direct
    candidates: list[dict[str, Any]] = []
    for order in store.list_orders(open_only=False):
        if str(order.get("market") or "").strip().upper() != market:
            continue
        if str(order.get("side") or "").strip().lower() != "ask":
            continue
        if str(order.get("local_state") or "").strip().upper() != "DONE":
            continue
        if min_updated_ts is not None and int(order.get("updated_ts") or 0) < int(min_updated_ts):
            continue
        candidates.append(order)
    if not candidates:
        return None
    preferred = [item for item in candidates if plan_id and _as_optional_str(item.get("tp_sl_link")) == plan_id]
    pool = preferred or candidates
    if target_exit_ts is not None:
        return min(
            pool,
            key=lambda item: (
                abs(int(item.get("updated_ts") or 0) - int(target_exit_ts)),
                int(item.get("updated_ts") or 0),
            ),
        )
    return min(pool, key=lambda item: int(item.get("updated_ts") or 0))


def _latest_plan_for_market(*, store: LiveStateStore, market: str) -> dict[str, Any] | None:
    rows = store.list_risk_plans(market=market)
    if not rows:
        return None
    return max(rows, key=lambda item: (int(item.get("updated_ts") or 0), int(item.get("created_ts") or 0), str(item.get("plan_id") or "")))


def _extract_entry_details(meta_payload: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(meta_payload or {})
    strategy_meta = ((payload.get("strategy") or {}).get("meta")) if isinstance(payload.get("strategy"), dict) else {}
    strategy_meta = dict(strategy_meta or {}) if isinstance(strategy_meta, dict) else {}
    trade_action = dict(strategy_meta.get("trade_action") or {}) if isinstance(strategy_meta.get("trade_action"), dict) else {}
    admissibility = ((payload.get("admissibility") or {}).get("decision")) if isinstance(payload.get("admissibility"), dict) else {}
    admissibility = dict(admissibility or {}) if isinstance(admissibility, dict) else {}
    expected_edge = _as_optional_float(trade_action.get("expected_edge"))
    expected_downside = _as_optional_float(trade_action.get("expected_downside_deviation"))
    return {
        "model_prob": _as_optional_float(strategy_meta.get("model_prob")),
        "selection_policy_mode": _as_optional_str(strategy_meta.get("selection_policy_mode")),
        "trade_action": _as_optional_str(trade_action.get("recommended_action")),
        "expected_edge_bps": expected_edge * 10_000.0 if expected_edge is not None else None,
        "expected_downside_bps": expected_downside * 10_000.0 if expected_downside is not None else None,
        "expected_net_edge_bps": _as_optional_float(admissibility.get("expected_net_edge_bps")),
        "notional_multiplier": _coalesce_float(
            _as_optional_float(strategy_meta.get("notional_multiplier")),
            _as_optional_float(trade_action.get("recommended_notional_multiplier")),
        ),
    }


def _build_entry_meta_summary(meta_payload: Any) -> dict[str, Any]:
    payload = dict(meta_payload or {}) if isinstance(meta_payload, dict) else {}
    strategy = dict(payload.get("strategy") or {}) if isinstance(payload.get("strategy"), dict) else {}
    strategy_meta = dict(strategy.get("meta") or {}) if isinstance(strategy.get("meta"), dict) else {}
    trade_action = dict(strategy_meta.get("trade_action") or {}) if isinstance(strategy_meta.get("trade_action"), dict) else {}
    exit_recommendation = (
        dict(strategy_meta.get("exit_recommendation") or {})
        if isinstance(strategy_meta.get("exit_recommendation"), dict)
        else {}
    )
    model_exit_plan = (
        normalize_model_exit_plan_payload(strategy_meta.get("model_exit_plan"))
        if isinstance(strategy_meta.get("model_exit_plan"), dict)
        else {}
    )
    admissibility = dict(payload.get("admissibility") or {}) if isinstance(payload.get("admissibility"), dict) else {}
    decision = dict(admissibility.get("decision") or {}) if isinstance(admissibility.get("decision"), dict) else {}
    snapshot = dict(admissibility.get("snapshot") or {}) if isinstance(admissibility.get("snapshot"), dict) else {}
    sizing = dict(admissibility.get("sizing") or {}) if isinstance(admissibility.get("sizing"), dict) else {}
    execution = dict(payload.get("execution") or {}) if isinstance(payload.get("execution"), dict) else {}
    runtime = dict(payload.get("runtime") or {}) if isinstance(payload.get("runtime"), dict) else {}
    return {
        "strategy": {
            "market": strategy.get("market"),
            "side": strategy.get("side"),
            "reason_code": strategy.get("reason_code"),
            "prob": strategy.get("prob"),
            "score": strategy.get("score"),
            "meta": {
                "model_prob": strategy_meta.get("model_prob"),
                "model_prob_raw": strategy_meta.get("model_prob_raw"),
                "selection_policy_mode": strategy_meta.get("selection_policy_mode"),
                "notional_multiplier": strategy_meta.get("notional_multiplier"),
                "notional_multiplier_source": strategy_meta.get("notional_multiplier_source"),
                "trade_action": {
                    "recommended_action": trade_action.get("recommended_action"),
                    "expected_edge": trade_action.get("expected_edge"),
                    "expected_downside_deviation": trade_action.get("expected_downside_deviation"),
                    "expected_objective_score": trade_action.get("expected_objective_score"),
                    "expected_action_value": (
                        trade_action.get("expected_action_value")
                        if trade_action.get("expected_action_value") is not None
                        else trade_action.get("expected_objective_score")
                    ),
                    "expected_es": trade_action.get("expected_es"),
                    "expected_ctm": (
                        trade_action.get("expected_ctm")
                        if trade_action.get("expected_ctm") is not None
                        else trade_action.get("expected_ctm2")
                    ),
                    "expected_ctm_order": trade_action.get("expected_ctm_order"),
                    "expected_tail_probability": trade_action.get("expected_tail_probability"),
                    "decision_source": (
                        trade_action.get("decision_source")
                        if trade_action.get("decision_source") is not None
                        else trade_action.get("chosen_action_source")
                    ),
                    "recommended_notional_multiplier": trade_action.get("recommended_notional_multiplier"),
                },
                "exit_recommendation": {
                    "recommended_exit_mode": exit_recommendation.get("recommended_exit_mode"),
                    "recommended_exit_mode_source": exit_recommendation.get("recommended_exit_mode_source"),
                    "recommended_exit_mode_reason_code": exit_recommendation.get("recommended_exit_mode_reason_code"),
                    "recommended_hold_bars": exit_recommendation.get("recommended_hold_bars"),
                    "chosen_family": exit_recommendation.get("chosen_family"),
                    "chosen_rule_id": exit_recommendation.get("chosen_rule_id"),
                    "hold_family_status": exit_recommendation.get("hold_family_status"),
                    "risk_family_status": exit_recommendation.get("risk_family_status"),
                    "family_compare_status": exit_recommendation.get("family_compare_status"),
                    "family_compare_reason_codes": list(exit_recommendation.get("family_compare_reason_codes") or []),
                },
                "model_exit_plan": {
                    "mode": model_exit_plan.get("mode"),
                    "hold_bars": model_exit_plan.get("hold_bars"),
                    "bar_interval_ms": model_exit_plan.get("bar_interval_ms"),
                    "timeout_delta_ms": model_exit_plan.get("timeout_delta_ms"),
                    "tp_ratio": model_exit_plan.get("tp_ratio"),
                    "sl_ratio": model_exit_plan.get("sl_ratio"),
                    "trailing_ratio": model_exit_plan.get("trailing_ratio"),
                    "tp_pct": model_exit_plan.get("tp_pct"),
                    "sl_pct": model_exit_plan.get("sl_pct"),
                    "trailing_pct": model_exit_plan.get("trailing_pct"),
                    "expected_exit_fee_ratio": model_exit_plan.get("expected_exit_fee_ratio"),
                    "expected_exit_fee_rate": model_exit_plan.get("expected_exit_fee_rate"),
                    "expected_exit_slippage_bps": model_exit_plan.get("expected_exit_slippage_bps"),
                },
            },
        },
        "admissibility": {
            "decision": {
                "adjusted_price": decision.get("adjusted_price"),
                "adjusted_volume": decision.get("adjusted_volume"),
                "adjusted_notional": decision.get("adjusted_notional"),
                "fee_reserve_quote": decision.get("fee_reserve_quote"),
                "fee_cost_bps": decision.get("fee_cost_bps"),
                "tick_proxy_bps": decision.get("tick_proxy_bps"),
                "replace_risk_budget_bps": decision.get("replace_risk_budget_bps"),
                "estimated_total_cost_bps": decision.get("estimated_total_cost_bps"),
                "expected_edge_bps": decision.get("expected_edge_bps"),
                "expected_net_edge_bps": decision.get("expected_net_edge_bps"),
            },
            "snapshot": {
                "bid_fee": snapshot.get("bid_fee"),
                "ask_fee": snapshot.get("ask_fee"),
                "tick_size": snapshot.get("tick_size"),
                "min_total": snapshot.get("min_total"),
            },
            "sizing": {
                "target_notional_quote": sizing.get("target_notional_quote"),
                "admissible_notional_quote": sizing.get("admissible_notional_quote"),
                "fee_rate": sizing.get("fee_rate"),
            },
        },
        "execution": {
            "initial_ref_price": execution.get("initial_ref_price"),
            "effective_ref_price": execution.get("effective_ref_price"),
            "requested_price": execution.get("requested_price"),
            "latest_trade_price": execution.get("latest_trade_price"),
            "exec_profile": dict(execution.get("exec_profile") or {}) if isinstance(execution.get("exec_profile"), dict) else {},
        },
        "runtime": {
            "live_runtime_model_run_id": runtime.get("live_runtime_model_run_id"),
            "model_family": runtime.get("model_family"),
        },
    }


def _build_exit_meta_summary(exit_meta: Any) -> dict[str, Any]:
    payload = dict(exit_meta or {}) if isinstance(exit_meta, dict) else {}
    exit_order = dict(payload.get("exit_order") or {}) if isinstance(payload.get("exit_order"), dict) else {}
    return {
        "close_mode": payload.get("close_mode"),
        "close_reason_code": payload.get("close_reason_code"),
        "pnl_basis": payload.get("pnl_basis"),
        "gross_pnl_quote": payload.get("gross_pnl_quote"),
        "gross_pnl_pct": payload.get("gross_pnl_pct"),
        "entry_fee_quote": payload.get("entry_fee_quote"),
        "exit_fee_quote": payload.get("exit_fee_quote"),
        "total_fee_quote": payload.get("total_fee_quote"),
        "entry_fee_bps": payload.get("entry_fee_bps"),
        "exit_fee_bps": payload.get("exit_fee_bps"),
        "entry_realized_slippage_bps": payload.get("entry_realized_slippage_bps"),
        "exit_expected_slippage_bps": payload.get("exit_expected_slippage_bps"),
        "exit_order": {
            "uuid": exit_order.get("uuid"),
            "identifier": exit_order.get("identifier"),
            "price": exit_order.get("price"),
            "state": exit_order.get("state"),
            "local_state": exit_order.get("local_state"),
            "last_event_name": exit_order.get("last_event_name"),
            "updated_ts": exit_order.get("updated_ts"),
        },
    }


def _compute_cost_metrics(
    *,
    entry_meta: dict[str, Any] | None,
    entry_order: dict[str, Any] | None,
    exit_order: dict[str, Any] | None,
    entry_price: float | None,
    exit_price: float | None,
    qty: float | None,
) -> dict[str, float | None]:
    qty_value = _coalesce_float(_filled_qty_from_order(entry_order), _as_optional_float(qty))
    entry_price_value = _coalesce_float(_filled_price_from_order(entry_order), _as_optional_float(entry_price))
    exit_price_value = _coalesce_float(_filled_price_from_order(exit_order), _as_optional_float(exit_price))
    entry_notional = entry_price_value * qty_value if entry_price_value is not None and qty_value is not None else None
    exit_notional = exit_price_value * qty_value if exit_price_value is not None and qty_value is not None else None
    entry_fee_rate = _resolve_entry_fee_rate(entry_meta)
    exit_fee_rate = _resolve_exit_fee_rate(entry_meta, entry_fee_rate=entry_fee_rate)
    entry_gross_quote = _coalesce_float(_as_optional_float((entry_order or {}).get("executed_funds")), entry_notional)
    exit_gross_quote = _coalesce_float(_as_optional_float((exit_order or {}).get("executed_funds")), exit_notional)
    entry_fee_quote = _coalesce_float(
        _as_optional_float((entry_order or {}).get("paid_fee")),
        entry_notional * entry_fee_rate if entry_notional is not None else None,
    )
    exit_fee_quote = _coalesce_float(
        _as_optional_float((exit_order or {}).get("paid_fee")),
        exit_notional * exit_fee_rate if exit_notional is not None else None,
    )
    gross_pnl_quote = (
        exit_gross_quote - entry_gross_quote
        if entry_gross_quote is not None and exit_gross_quote is not None
        else ((exit_price_value - entry_price_value) * qty_value if entry_price_value is not None and exit_price_value is not None and qty_value is not None else None)
    )
    gross_pnl_pct = (
        ((exit_price_value / entry_price_value) - 1.0) * 100.0
        if entry_price_value is not None and exit_price_value is not None and entry_price_value > 0.0
        else None
    )
    total_fee_quote = (
        (entry_fee_quote or 0.0) + (exit_fee_quote or 0.0)
        if entry_fee_quote is not None or exit_fee_quote is not None
        else None
    )
    entry_total_quote = (
        entry_gross_quote + entry_fee_quote
        if entry_gross_quote is not None and entry_fee_quote is not None
        else entry_notional
    )
    exit_total_quote = (
        exit_gross_quote - exit_fee_quote
        if exit_gross_quote is not None and exit_fee_quote is not None
        else exit_notional
    )
    net_pnl_quote = (
        exit_total_quote - entry_total_quote
        if exit_total_quote is not None and entry_total_quote is not None
        else (gross_pnl_quote - total_fee_quote if gross_pnl_quote is not None and total_fee_quote is not None else gross_pnl_quote)
    )
    net_pnl_pct = (
        ((exit_total_quote / entry_total_quote) - 1.0) * 100.0
        if entry_total_quote is not None and exit_total_quote is not None and entry_total_quote > 0.0
        else gross_pnl_pct
    )
    entry_ref_price = _coalesce_float(
        _dig_float(entry_meta, "execution", "initial_ref_price"),
        _dig_float(entry_meta, "execution", "effective_ref_price"),
        _dig_float(entry_meta, "execution", "requested_price"),
    )
    entry_realized_slippage = (
        slippage_bps(side="bid", fill_price=entry_price_value, ref_price=entry_ref_price)
        if entry_price_value is not None and entry_ref_price is not None
        else None
    )
    exit_expected_slippage = _dig_float(entry_meta, "strategy", "meta", "model_exit_plan", "expected_exit_slippage_bps")
    return {
        "entry_fee_rate": entry_fee_rate,
        "exit_fee_rate": exit_fee_rate,
        "entry_fee_bps": entry_fee_rate * 10_000.0 if entry_fee_rate is not None else None,
        "exit_fee_bps": exit_fee_rate * 10_000.0 if exit_fee_rate is not None else None,
        "entry_fee_quote": entry_fee_quote,
        "exit_fee_quote": exit_fee_quote,
        "total_fee_quote": total_fee_quote,
        "entry_total_quote": entry_total_quote,
        "exit_total_quote": exit_total_quote,
        "gross_pnl_quote": gross_pnl_quote,
        "gross_pnl_pct": gross_pnl_pct,
        "net_pnl_quote": net_pnl_quote,
        "net_pnl_pct": net_pnl_pct,
        "entry_realized_slippage_bps": entry_realized_slippage,
        "exit_expected_slippage_bps": exit_expected_slippage,
    }


def _resolve_entry_fee_rate(entry_meta: dict[str, Any] | None) -> float:
    fee_rate = _coalesce_float(
        _dig_float(entry_meta, "admissibility", "sizing", "fee_rate"),
        _dig_float(entry_meta, "admissibility", "snapshot", "bid_fee"),
    )
    return max(float(fee_rate or 0.0), 0.0)


def _resolve_exit_fee_rate(entry_meta: dict[str, Any] | None, *, entry_fee_rate: float) -> float:
    configured = _dig_float(entry_meta, "strategy", "meta", "model_exit_plan", "expected_exit_fee_rate")
    snapshot_fee = _dig_float(entry_meta, "admissibility", "snapshot", "ask_fee")
    fee_rate = configured if configured is not None and configured > 0.0 else _coalesce_float(snapshot_fee, entry_fee_rate)
    return max(float(fee_rate or 0.0), 0.0)


def _dig_float(payload: dict[str, Any] | None, *keys: str) -> float | None:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return _as_optional_float(current)


def _filled_qty_from_order(order: dict[str, Any] | None) -> float | None:
    if not isinstance(order, dict):
        return None
    return _coalesce_float(_as_optional_float(order.get("volume_filled")), _as_optional_float(order.get("volume_req")))


def _extract_executed_funds_from_payload(payload: dict[str, Any] | None) -> float | None:
    if not isinstance(payload, dict):
        return None
    direct = _as_optional_float(payload.get("executed_funds"))
    if direct is not None:
        return direct
    trades = payload.get("trades")
    if isinstance(trades, list):
        funds_values = [_as_optional_float(item.get("funds")) for item in trades if isinstance(item, dict)]
        funds_values = [value for value in funds_values if value is not None]
        if funds_values:
            return float(sum(funds_values))
    return None


def _filled_price_from_order(order: dict[str, Any] | None) -> float | None:
    if not isinstance(order, dict):
        return None
    executed_funds = _as_optional_float(order.get("executed_funds"))
    executed_volume = _as_optional_float(order.get("volume_filled"))
    if executed_funds is not None and executed_volume is not None and executed_volume > 0.0:
        return executed_funds / executed_volume
    return _as_optional_float(order.get("price"))


def _order_record_with_execution_details(*, order: dict[str, Any], payload: dict[str, Any]) -> OrderRecord:
    return OrderRecord(
        uuid=str(order.get("uuid") or ""),
        identifier=_as_optional_str(order.get("identifier")),
        market=str(order.get("market") or ""),
        side=_as_optional_str(order.get("side")),
        ord_type=_as_optional_str(order.get("ord_type")),
        price=_coalesce_float(_as_optional_float(payload.get("price")), _as_optional_float(order.get("price"))),
        volume_req=_coalesce_float(_as_optional_float(payload.get("volume")), _as_optional_float(order.get("volume_req"))),
        volume_filled=float(
            _coalesce_float(_as_optional_float(payload.get("executed_volume")), _as_optional_float(order.get("volume_filled")), 0.0)
            or 0.0
        ),
        state=str(payload.get("state") or order.get("state") or ""),
        created_ts=int(order.get("created_ts") or 0),
        updated_ts=int(order.get("updated_ts") or 0),
        intent_id=_as_optional_str(order.get("intent_id")),
        tp_sl_link=_as_optional_str(order.get("tp_sl_link")),
        local_state=_as_optional_str(order.get("local_state")),
        raw_exchange_state=_coalesce_str(_as_optional_str(payload.get("state")), _as_optional_str(order.get("raw_exchange_state"))),
        last_event_name=_as_optional_str(order.get("last_event_name")),
        event_source=_coalesce_str(_as_optional_str(order.get("event_source")), "order_detail_backfill"),
        replace_seq=int(order.get("replace_seq") or 0),
        root_order_uuid=_as_optional_str(order.get("root_order_uuid")),
        prev_order_uuid=_as_optional_str(order.get("prev_order_uuid")),
        prev_order_identifier=_as_optional_str(order.get("prev_order_identifier")),
        executed_funds=_coalesce_float(_extract_executed_funds_from_payload(payload), _as_optional_float(order.get("executed_funds"))),
        paid_fee=_coalesce_float(_as_optional_float(payload.get("paid_fee")), _as_optional_float(order.get("paid_fee"))),
        reserved_fee=_coalesce_float(_as_optional_float(payload.get("reserved_fee")), _as_optional_float(order.get("reserved_fee"))),
        remaining_fee=_coalesce_float(_as_optional_float(payload.get("remaining_fee")), _as_optional_float(order.get("remaining_fee"))),
        exchange_payload_json=json.dumps(payload, ensure_ascii=False, sort_keys=True),
    )


def _derive_close_mode(*, order: dict[str, Any] | None) -> str | None:
    if not isinstance(order, dict):
        return None
    if _as_optional_str(order.get("tp_sl_link")):
        return "managed_exit_order"
    return "done_ask_order"


def _resolve_journal_id(
    *,
    existing: dict[str, Any] | None,
    entry_intent_id: str | None,
    market: str,
    ts_ms: int,
) -> str:
    if existing is not None and _as_optional_str(existing.get("journal_id")):
        return str(existing.get("journal_id"))
    if entry_intent_id:
        return entry_intent_id
    return f"trade-{market}-{int(ts_ms)}"


def _json_dumps(value: Any) -> str:
    payload = value if isinstance(value, dict) else {}
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _as_optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _as_optional_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_optional_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coalesce_str(*values: str | None) -> str | None:
    for value in values:
        text = _as_optional_str(value)
        if text is not None:
            return text
    return None


def _coalesce_float(*values: float | None) -> float | None:
    for value in values:
        candidate = _as_optional_float(value)
        if candidate is not None:
            return candidate
    return None


def _coalesce_int(*values: int | None) -> int | None:
    for value in values:
        candidate = _as_optional_int(value)
        if candidate is not None:
            return candidate
    return None
