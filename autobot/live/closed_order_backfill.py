from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from typing import Any

from .identifier import extract_intent_id_from_identifier, extract_run_token_from_identifier, is_bot_identifier
from .order_state import normalize_order_state
from .state_store import IntentRecord, LiveStateStore, OrderRecord
from .trade_journal import recompute_trade_journal_records


def backfill_recent_bot_closed_orders(
    *,
    store: LiveStateStore,
    client: Any,
    bot_id: str,
    identifier_prefix: str,
    now_ts_ms: int,
    lookback_hours: int = 24,
    limit: int = 100,
) -> dict[str, Any]:
    if not hasattr(client, "closed_orders"):
        return {"supported": False, "orders_seen": 0, "orders_upserted": 0, "orders_skipped": 0, "recompute": None}

    end_dt = datetime.fromtimestamp(now_ts_ms / 1000.0, tz=timezone.utc)
    start_dt = end_dt - timedelta(hours=max(int(lookback_hours), 1))
    payload = client.closed_orders(
        states=("done", "cancel"),
        start_time=start_dt.isoformat().replace("+00:00", "Z"),
        end_time=end_dt.isoformat().replace("+00:00", "Z"),
        limit=max(int(limit), 1),
        order_by="desc",
    )
    rows = payload if isinstance(payload, list) else []
    seen = 0
    upserted = 0
    skipped = 0
    tracked_exit_order_uuids = {
        str(item.get("current_exit_order_uuid") or "").strip()
        for item in store.list_risk_plans()
        if str(item.get("current_exit_order_uuid") or "").strip()
    }
    tracked_exit_order_uuids.update(
        {
            str(item.get("exit_order_uuid") or "").strip()
            for item in store.list_trade_journal(statuses=("CLOSED", "OPEN", "PENDING_ENTRY", "CANCELLED_ENTRY"))
            if str(item.get("exit_order_uuid") or "").strip()
        }
    )
    tracked_exit_order_identifiers = {
        str(item.get("current_exit_order_identifier") or "").strip()
        for item in store.list_risk_plans()
        if str(item.get("current_exit_order_identifier") or "").strip()
    }
    for item in rows:
        if not isinstance(item, dict):
            continue
        seen += 1
        identifier = _as_optional_str(item.get("identifier"))
        uuid = _as_optional_str(item.get("uuid"))
        existing = None
        if uuid:
            existing = store.order_by_uuid(uuid=uuid)
        if existing is None and identifier:
            existing = store.order_by_identifier(identifier=identifier)
        tracked_order = existing is not None
        tracked_plan = (uuid in tracked_exit_order_uuids if uuid else False) or (
            identifier in tracked_exit_order_identifiers if identifier else False
        )
        tracked_identifier = is_bot_identifier(identifier, prefix=identifier_prefix, bot_id=bot_id)
        if not (tracked_identifier or tracked_order or tracked_plan):
            skipped += 1
            continue
        intent_id = _as_optional_str((existing or {}).get("intent_id")) or extract_intent_id_from_identifier(
            identifier,
            prefix=identifier_prefix,
            bot_id=bot_id,
        )
        if intent_id is None and uuid is not None:
            intent_id = f"inferred-{uuid}"
        if intent_id:
            store.upsert_intent(
                IntentRecord(
                    intent_id=intent_id,
                    ts_ms=_parse_created_ts(item.get("created_at"), fallback_ts=now_ts_ms),
                    market=str(item.get("market") or "").strip().upper(),
                    side=str(item.get("side") or "bid").strip().lower(),
                    price=_as_optional_float(item.get("price")),
                    volume=_as_optional_float(item.get("volume")),
                    reason_code="CLOSED_ORDERS_BACKFILL",
                    meta_json=json.dumps(
                        {
                            "source": "closed_orders_backfill",
                            "identifier": identifier,
                            "order_uuid": uuid,
                            "runtime_model_run_id": extract_run_token_from_identifier(
                                identifier,
                                prefix=identifier_prefix,
                                bot_id=bot_id,
                            ),
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    status="UPDATED_FROM_CLOSED_ORDERS",
                )
            )
        normalized = normalize_order_state(
            exchange_state=str(item.get("state") or ""),
            event_name="CLOSED_ORDERS_BACKFILL",
            executed_volume=_as_optional_float(item.get("executed_volume")),
        )
        executed_funds = _extract_executed_funds(item)
        store.upsert_order(
            OrderRecord(
                uuid=uuid or f"closed-backfill-{intent_id or 'unknown'}-{now_ts_ms}",
                identifier=identifier,
                market=str(item.get("market") or "").strip().upper(),
                side=_as_optional_str(item.get("side")),
                ord_type=_as_optional_str(item.get("ord_type")),
                price=_as_optional_float(item.get("price")),
                volume_req=_as_optional_float(item.get("volume")),
                volume_filled=float(_as_optional_float(item.get("executed_volume")) or 0.0),
                state=str(item.get("state") or "").strip().lower() or "done",
                created_ts=_parse_created_ts(item.get("created_at"), fallback_ts=now_ts_ms),
                updated_ts=_parse_created_ts(
                    item.get("done_at") or item.get("updated_at") or item.get("created_at"),
                    fallback_ts=now_ts_ms,
                ),
                intent_id=intent_id,
                tp_sl_link=_as_optional_str((existing or {}).get("tp_sl_link")),
                local_state=normalized.local_state,
                raw_exchange_state=normalized.exchange_state,
                last_event_name=normalized.event_name,
                event_source="closed_orders_backfill",
                replace_seq=int((existing or {}).get("replace_seq") or 0),
                root_order_uuid=_as_optional_str((existing or {}).get("root_order_uuid")) or uuid,
                prev_order_uuid=_as_optional_str((existing or {}).get("prev_order_uuid")),
                prev_order_identifier=_as_optional_str((existing or {}).get("prev_order_identifier")),
                executed_funds=executed_funds,
                paid_fee=_as_optional_float(item.get("paid_fee")),
                reserved_fee=_as_optional_float(item.get("reserved_fee")),
                remaining_fee=_as_optional_float(item.get("remaining_fee")),
                exchange_payload_json=json.dumps(item, ensure_ascii=False, sort_keys=True),
            )
        )
        upserted += 1

    recompute = recompute_trade_journal_records(store=store) if upserted > 0 else None
    return {
        "supported": True,
        "orders_seen": seen,
        "orders_upserted": upserted,
        "orders_skipped": skipped,
        "recompute": recompute,
    }


def _extract_executed_funds(payload: dict[str, Any]) -> float | None:
    direct = _as_optional_float(payload.get("executed_funds"))
    if direct is not None:
        return direct
    trades = payload.get("trades")
    if isinstance(trades, list):
        funds = [_as_optional_float(item.get("funds")) for item in trades if isinstance(item, dict)]
        funds = [value for value in funds if value is not None]
        if funds:
            return float(sum(funds))
    return None


def _parse_created_ts(raw: object, *, fallback_ts: int) -> int:
    value = _as_optional_str(raw)
    if not value:
        return int(fallback_ts)
    try:
        return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)
    except ValueError:
        return int(fallback_ts)


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
