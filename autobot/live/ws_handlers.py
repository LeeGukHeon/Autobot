"""Private websocket event handlers that mutate live state store."""

from __future__ import annotations

import json
from typing import Any

from autobot.upbit.ws import MyAssetEvent, MyOrderEvent

from .identifier import extract_intent_id_from_identifier, extract_run_token_from_identifier, is_bot_identifier
from .model_alpha_projection import find_latest_model_entry_intent
from .model_risk_plan import build_model_derived_risk_records
from .order_state import normalize_order_state
from .state_store import IntentRecord, LiveStateStore, OrderRecord, PositionRecord, RiskPlanRecord
from .trade_journal import rebind_pending_entry_journal_order


def apply_private_ws_event(
    *,
    store: LiveStateStore,
    event: MyOrderEvent | MyAssetEvent,
    bot_id: str,
    identifier_prefix: str,
    quote_currency: str,
) -> dict[str, Any]:
    if isinstance(event, MyOrderEvent):
        return _apply_my_order_event(
            store=store,
            event=event,
            bot_id=bot_id,
            identifier_prefix=identifier_prefix,
        )
    return _apply_my_asset_event(store=store, event=event, quote_currency=quote_currency)


def _apply_my_order_event(
    *,
    store: LiveStateStore,
    event: MyOrderEvent,
    bot_id: str,
    identifier_prefix: str,
) -> dict[str, Any]:
    if not event.uuid or not event.market:
        return {"type": "ws_order_skip", "reason": "missing_uuid_or_market"}

    existing = store.order_by_uuid(uuid=event.uuid)
    identifier_value = event.identifier or (_as_optional_str(existing.get("identifier")) if existing else None)
    if existing is None and not is_bot_identifier(identifier_value, prefix=identifier_prefix, bot_id=bot_id):
        return {"type": "ws_order_skip", "reason": "external_order", "uuid": event.uuid, "identifier": identifier_value}

    lineage, previous_order = _recover_order_lineage_context(
        store=store,
        uuid=event.uuid,
        identifier=identifier_value,
    )
    intent_id = _as_optional_str(existing.get("intent_id")) if existing else None
    if not intent_id:
        intent_id = extract_intent_id_from_identifier(
            identifier_value,
            prefix=identifier_prefix,
            bot_id=bot_id,
        )
    if not intent_id:
        intent_id = _as_optional_str((lineage or {}).get("intent_id"))
    if not intent_id:
        intent_id = f"inferred-{event.uuid}"
    existing_intent = store.intent_by_id(intent_id=intent_id)
    raw = dict(event.raw) if isinstance(event.raw, dict) else {}
    event_name = _as_optional_str(raw.get("event_name")) or "PRIVATE_WS_ORDER_EVENT"
    normalized = normalize_order_state(
        exchange_state=event.state,
        event_name=event_name,
        executed_volume=event.executed_volume,
    )
    executed_funds = _as_optional_float(raw.get("executed_funds") or raw.get("ef"))
    if executed_funds is None and isinstance(raw.get("trades"), list):
        funds = [_as_optional_float(item.get("funds")) for item in raw.get("trades") if isinstance(item, dict)]
        funds = [value for value in funds if value is not None]
        if funds:
            executed_funds = float(sum(funds))

    order_record = OrderRecord(
        uuid=event.uuid,
        identifier=identifier_value,
        market=event.market,
        side=event.side,
        ord_type=event.ord_type,
        price=event.price,
        volume_req=event.volume,
        volume_filled=float(event.executed_volume or 0.0),
        state=str(event.state or "wait").strip().lower(),
        created_ts=(
            int(existing.get("created_ts"))
            if existing and existing.get("created_ts")
            else int((previous_order or {}).get("created_ts") or event.ts_ms)
        ),
        updated_ts=int(event.ts_ms),
        intent_id=intent_id,
        tp_sl_link=(
            _as_optional_str(existing.get("tp_sl_link"))
            if existing
            else _as_optional_str((previous_order or {}).get("tp_sl_link"))
        ),
        local_state=normalized.local_state,
        raw_exchange_state=normalized.exchange_state,
        last_event_name=normalized.event_name,
        event_source="private_ws",
        replace_seq=(
            int(existing.get("replace_seq") or 0)
            if existing
            else int((lineage or {}).get("replace_seq") or (previous_order or {}).get("replace_seq") or 0)
        ),
        root_order_uuid=(
            _as_optional_str(existing.get("root_order_uuid"))
            if existing
            else _as_optional_str((previous_order or {}).get("root_order_uuid"))
            or _as_optional_str((lineage or {}).get("prev_uuid"))
            or event.uuid
        ),
        prev_order_uuid=(
            _as_optional_str(existing.get("prev_order_uuid"))
            if existing
            else _as_optional_str((lineage or {}).get("prev_uuid"))
        ),
        prev_order_identifier=(
            _as_optional_str(existing.get("prev_order_identifier"))
            if existing
            else _as_optional_str((lineage or {}).get("prev_identifier"))
        ),
        executed_funds=executed_funds,
        paid_fee=_as_optional_float(raw.get("paid_fee") or raw.get("pf")),
        reserved_fee=_as_optional_float(raw.get("reserved_fee") or raw.get("rf")),
        remaining_fee=_as_optional_float(raw.get("remaining_fee") or raw.get("rmf")),
        exchange_payload_json=json.dumps(raw, ensure_ascii=False, sort_keys=True) if raw else "{}",
    )
    store.upsert_order(order_record)
    if (
        previous_order is not None
        and str(event.side or "").strip().lower() == "bid"
        and _as_optional_str((lineage or {}).get("prev_uuid")) != str(event.uuid)
    ):
        rebind_pending_entry_journal_order(
            store=store,
            entry_intent_id=intent_id,
            previous_entry_order_uuid=_as_optional_str((lineage or {}).get("prev_uuid")),
            new_entry_order_uuid=event.uuid,
            ts_ms=int(event.ts_ms),
        )

    status = "UPDATED_FROM_WS"
    if existing is None and existing_intent is None:
        status = "INFERRED_FROM_EXCHANGE"
    intent_meta = (
        dict(existing_intent.get("meta") or {})
        if existing_intent is not None and isinstance(existing_intent.get("meta"), dict)
        else {}
    )
    intent_meta.update(
        {
        "source": "private_ws",
        "stream_type": event.stream_type,
        "order_uuid": event.uuid,
        "identifier": event.identifier,
        "bot_id": bot_id,
        "identifier_prefix": identifier_prefix,
        "runtime_model_run_id": extract_run_token_from_identifier(
            identifier_value,
            prefix=identifier_prefix,
            bot_id=bot_id,
        ),
        }
    )
    store.upsert_intent(
        IntentRecord(
            intent_id=intent_id,
            ts_ms=_coalesce_int(_as_optional_int((existing_intent or {}).get("ts_ms")), int(event.ts_ms)) or int(event.ts_ms),
            market=str((existing_intent or {}).get("market") or event.market or "").strip().upper(),
            side=str(event.side or (existing_intent or {}).get("side") or "bid"),
            price=(
                event.price
                if event.price is not None
                else _as_optional_float((existing_intent or {}).get("price"))
            ),
            volume=(
                event.volume
                if event.volume is not None
                else _as_optional_float((existing_intent or {}).get("volume"))
            ),
            reason_code=_as_optional_str(existing_intent.get("reason_code")) if existing_intent is not None else "PRIVATE_WS_ORDER_EVENT",
            meta_json=json.dumps(intent_meta, ensure_ascii=False, sort_keys=True),
            status=status,
        )
    )
    return {
        "type": "ws_order_upsert",
        "uuid": event.uuid,
        "state": order_record.state,
        "local_state": order_record.local_state,
        "intent_id": intent_id,
        "intent_status": status,
    }


def _apply_my_asset_event(
    *,
    store: LiveStateStore,
    event: MyAssetEvent,
    quote_currency: str,
) -> dict[str, Any]:
    currency = str(event.currency or "").strip().upper()
    if not currency:
        return {"type": "ws_asset_skip", "reason": "missing_currency"}
    quote = quote_currency.strip().upper()
    if currency == quote:
        return {
            "type": "ws_asset_quote_balance",
            "currency": currency,
            "balance": float(event.balance or 0.0),
            "locked": float(event.locked or 0.0),
        }

    market = f"{quote}-{currency}"
    total = float(event.balance or 0.0) + float(event.locked or 0.0)
    existing = store.position_by_market(market=market)
    if total <= 0.0:
        if existing is not None:
            store.delete_position(market=market)
        return {"type": "ws_asset_position_delete", "market": market}

    managed_record, managed_plan = _resolve_model_managed_asset_position(
        store=store,
        market=market,
        currency=currency,
        base_amount=total,
        avg_entry_price=float(event.avg_buy_price or 0.0),
        ts_ms=int(event.ts_ms),
    )
    if managed_record is not None:
        store.upsert_position(managed_record)
        if managed_plan is not None and not store.list_risk_plans(market=market, states=("ACTIVE", "TRIGGERED", "EXITING")):
            store.upsert_risk_plan(managed_plan)
        managed = True
    else:
        managed = bool(existing.get("managed")) if existing is not None else False
        tp_json = json.dumps(existing.get("tp") if existing is not None else {}, ensure_ascii=False, sort_keys=True)
        sl_json = json.dumps(existing.get("sl") if existing is not None else {}, ensure_ascii=False, sort_keys=True)
        trailing_json = json.dumps(
            existing.get("trailing") if existing is not None else {},
            ensure_ascii=False,
            sort_keys=True,
        )
        store.upsert_position(
            PositionRecord(
                market=market,
                base_currency=currency,
                base_amount=total,
                avg_entry_price=float(event.avg_buy_price or 0.0),
                updated_ts=int(event.ts_ms),
                tp_json=tp_json,
                sl_json=sl_json,
                trailing_json=trailing_json,
                managed=managed,
            )
        )
    return {
        "type": "ws_asset_position_upsert",
        "market": market,
        "base_amount": total,
        "avg_entry_price": float(event.avg_buy_price or 0.0),
        "managed": managed,
        "plan_id": managed_plan.plan_id if managed_plan is not None else None,
    }


def _resolve_model_managed_asset_position(
    *,
    store: LiveStateStore,
    market: str,
    currency: str,
    base_amount: float,
    avg_entry_price: float,
    ts_ms: int,
) -> tuple[PositionRecord | None, RiskPlanRecord | None]:
    active_plans = store.list_risk_plans(market=market, states=("ACTIVE", "TRIGGERED", "EXITING"))
    if active_plans:
        return None, None
    entry_intent = find_latest_model_entry_intent(
        store=store,
        market=market,
        position={"market": market, "base_amount": base_amount, "avg_entry_price": avg_entry_price},
    )
    if entry_intent is None:
        return None, None
    return build_model_derived_risk_records(
        market=market,
        base_currency=currency,
        base_amount=base_amount,
        avg_entry_price=avg_entry_price,
        plan_payload=entry_intent["plan_payload"],
        created_ts=int(entry_intent["created_ts"]),
        updated_ts=int(ts_ms),
        intent_id=entry_intent["intent_id"],
    )


def _as_optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _recover_order_lineage_context(
    *,
    store: LiveStateStore,
    uuid: str | None,
    identifier: str | None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    lineage = (
        store.latest_order_lineage_for_identifier(identifier=identifier)
        if _as_optional_str(identifier) is not None
        else None
    )
    if lineage is None and _as_optional_str(uuid) is not None:
        lineage = store.latest_order_lineage_for_uuid(uuid=str(uuid))
    if lineage is None:
        return None, None
    previous_order = None
    prev_uuid = _as_optional_str(lineage.get("prev_uuid"))
    prev_identifier = _as_optional_str(lineage.get("prev_identifier"))
    if prev_uuid is not None:
        previous_order = store.order_by_uuid(uuid=prev_uuid)
    if previous_order is None and prev_identifier is not None:
        previous_order = store.order_by_identifier(identifier=prev_identifier)
    return lineage, previous_order


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


def _coalesce_int(*values: int | None) -> int | None:
    for value in values:
        resolved = _as_optional_int(value)
        if resolved is not None:
            return resolved
    return None
