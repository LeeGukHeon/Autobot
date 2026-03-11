from __future__ import annotations

import json
from typing import Any

from .state_store import LiveStateStore, TradeJournalRecord

TRADE_JOURNAL_STATUS_PENDING = "PENDING_ENTRY"
TRADE_JOURNAL_STATUS_OPEN = "OPEN"
TRADE_JOURNAL_STATUS_CLOSED = "CLOSED"


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
    existing = store.trade_journal_by_entry_intent(entry_intent_id=intent_id_value)
    entry_details = _extract_entry_details(meta_dict)
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
            entry_meta_json=_json_dumps(meta_dict) if meta_dict else _json_dumps((existing or {}).get("entry_meta")),
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
    existing = store.trade_journal_by_entry_intent(entry_intent_id=entry_intent_id) if entry_intent_id else None
    if existing is None:
        existing = _latest_live_trade_journal(store=store, market=market_value)
    meta_dict = dict((intent_record or {}).get("meta") or {})
    entry_details = _extract_entry_details(meta_dict)
    entry_order_uuid = _coalesce_str(
        _as_optional_str((entry_intent or {}).get("order_uuid")),
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
                _as_optional_str(plan_id),
                _as_optional_str((entry_intent or {}).get("plan_id")),
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
            entry_meta_json=_json_dumps(meta_dict) if meta_dict else _json_dumps((existing or {}).get("entry_meta")),
            exit_meta_json=_json_dumps((existing or {}).get("exit_meta")),
            updated_ts=int(ts_ms),
        )
    )
    return journal_id


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
    existing = _latest_live_trade_journal(store=store, market=market_value)
    min_exit_order_ts = _coalesce_int(
        _as_optional_int((existing or {}).get("entry_filled_ts_ms")),
        _as_optional_int((existing or {}).get("entry_submitted_ts_ms")),
    )
    latest_done_exit = _latest_done_exit_order(store=store, market=market_value, min_updated_ts=min_exit_order_ts)
    if existing is None:
        risk_plan = _latest_plan_for_market(store=store, market=market_value)
        fallback_intent_id = _as_optional_str((risk_plan or {}).get("source_intent_id"))
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

    entry_price = _coalesce_float(
        _as_optional_float((existing or {}).get("entry_price")),
        _as_optional_float(position.get("avg_entry_price")),
    )
    qty = _coalesce_float(
        _as_optional_float((existing or {}).get("qty")),
        _as_optional_float(position.get("base_amount")),
    )
    resolved_exit_price = _coalesce_float(
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
        _as_optional_str(plan_id),
        _as_optional_str((existing or {}).get("plan_id")),
        _as_optional_str((_latest_plan_for_market(store=store, market=market_value) or {}).get("plan_id")),
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
    exit_meta_payload.setdefault("pnl_basis", "gross_no_fee")
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
            entry_notional_quote=_as_optional_float((existing or {}).get("entry_notional_quote")),
            exit_notional_quote=resolved_exit_price * qty if resolved_exit_price is not None and qty is not None else None,
            realized_pnl_quote=realized_pnl_quote,
            realized_pnl_pct=realized_pnl_pct,
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
            entry_meta_json=_json_dumps((existing or {}).get("entry_meta")),
            exit_meta_json=_json_dumps(exit_meta_payload),
            updated_ts=exit_ts,
        )
    )
    return str((existing or {}).get("journal_id") or f"imported-{market_value}-{exit_ts}")


def _latest_live_trade_journal(*, store: LiveStateStore, market: str) -> dict[str, Any] | None:
    rows = store.list_trade_journal(
        market=market,
        statuses=(TRADE_JOURNAL_STATUS_OPEN, TRADE_JOURNAL_STATUS_PENDING),
        limit=1,
    )
    return rows[0] if rows else None


def _latest_done_exit_order(
    *,
    store: LiveStateStore,
    market: str,
    min_updated_ts: int | None = None,
) -> dict[str, Any] | None:
    best: dict[str, Any] | None = None
    for order in store.list_orders(open_only=False):
        if str(order.get("market") or "").strip().upper() != market:
            continue
        if str(order.get("side") or "").strip().lower() != "ask":
            continue
        if str(order.get("local_state") or "").strip().upper() != "DONE":
            continue
        if min_updated_ts is not None and int(order.get("updated_ts") or 0) < int(min_updated_ts):
            continue
        if best is None or int(order.get("updated_ts") or 0) > int(best.get("updated_ts") or 0):
            best = order
    return best


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
