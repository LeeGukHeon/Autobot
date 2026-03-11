"""Private websocket event handlers that mutate live state store."""

from __future__ import annotations

import json
from typing import Any

from autobot.upbit.ws import MyAssetEvent, MyOrderEvent

from .order_state import normalize_order_state
from .state_store import IntentRecord, LiveStateStore, OrderRecord, PositionRecord


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
    intent_id = _as_optional_str(existing.get("intent_id")) if existing else None
    if not intent_id:
        intent_id = f"inferred-{event.uuid}"
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
        identifier=(event.identifier or _as_optional_str(existing.get("identifier"))) if existing else event.identifier,
        market=event.market,
        side=event.side,
        ord_type=event.ord_type,
        price=event.price,
        volume_req=event.volume,
        volume_filled=float(event.executed_volume or 0.0),
        state=str(event.state or "wait").strip().lower(),
        created_ts=int(existing.get("created_ts")) if existing and existing.get("created_ts") else int(event.ts_ms),
        updated_ts=int(event.ts_ms),
        intent_id=intent_id,
        tp_sl_link=_as_optional_str(existing.get("tp_sl_link")) if existing else None,
        local_state=normalized.local_state,
        raw_exchange_state=normalized.exchange_state,
        last_event_name=normalized.event_name,
        event_source="private_ws",
        replace_seq=int(existing.get("replace_seq") or 0) if existing else 0,
        root_order_uuid=_as_optional_str(existing.get("root_order_uuid")) if existing else event.uuid,
        prev_order_uuid=_as_optional_str(existing.get("prev_order_uuid")) if existing else None,
        prev_order_identifier=_as_optional_str(existing.get("prev_order_identifier")) if existing else None,
        executed_funds=executed_funds,
        paid_fee=_as_optional_float(raw.get("paid_fee") or raw.get("pf")),
        reserved_fee=_as_optional_float(raw.get("reserved_fee") or raw.get("rf")),
        remaining_fee=_as_optional_float(raw.get("remaining_fee") or raw.get("rmf")),
        exchange_payload_json=json.dumps(raw, ensure_ascii=False, sort_keys=True) if raw else "{}",
    )
    store.upsert_order(order_record)

    status = "UPDATED_FROM_WS"
    if existing is None or not _as_optional_str(existing.get("intent_id")):
        status = "INFERRED_FROM_EXCHANGE"
    intent_meta = {
        "source": "private_ws",
        "stream_type": event.stream_type,
        "order_uuid": event.uuid,
        "identifier": event.identifier,
        "bot_id": bot_id,
        "identifier_prefix": identifier_prefix,
    }
    store.upsert_intent(
        IntentRecord(
            intent_id=intent_id,
            ts_ms=int(event.ts_ms),
            market=event.market,
            side=str(event.side or "bid"),
            price=event.price,
            volume=event.volume,
            reason_code="PRIVATE_WS_ORDER_EVENT",
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
    }


def _as_optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
