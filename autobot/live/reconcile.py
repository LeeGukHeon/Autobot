"""Exchange-vs-local reconciliation helpers for live runtime startup."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from datetime import datetime
import json
from typing import Any, Literal
import time

from .identifier import is_bot_identifier
from .state_store import IntentRecord, LiveStateStore, OrderRecord, PositionRecord, RiskPlanRecord

UnknownOpenOrdersPolicy = Literal["halt", "ignore", "cancel"]
UnknownPositionsPolicy = Literal["halt", "import_as_unmanaged", "attach_default_risk"]


def reconcile_exchange_snapshot(
    *,
    store: LiveStateStore,
    bot_id: str,
    identifier_prefix: str,
    accounts_payload: Any,
    open_orders_payload: Any,
    fetch_order_detail: Callable[[str, str | None], Any] | None = None,
    unknown_open_orders_policy: UnknownOpenOrdersPolicy = "halt",
    unknown_positions_policy: UnknownPositionsPolicy = "halt",
    allow_cancel_external_orders: bool = False,
    default_risk_sl_pct: float = 2.0,
    default_risk_tp_pct: float = 3.0,
    default_risk_trailing_enabled: bool = False,
    quote_currency: str = "KRW",
    dry_run: bool = True,
    ts_ms: int | None = None,
) -> dict[str, Any]:
    now_ts = int(ts_ms if ts_ms is not None else time.time() * 1000)
    actions: list[dict[str, Any]] = []
    warnings: list[str] = []
    halted_reasons: list[str] = []

    local_orders = {item["uuid"]: item for item in store.list_orders(open_only=False)}
    local_open_orders = {uuid: item for uuid, item in local_orders.items() if str(item.get("state", "")).lower() in {"wait", "watch", "open", "partial"}}
    exchange_open_orders = [item for item in _as_dict_list(open_orders_payload) if item.get("uuid")]

    exchange_bot_open_orders: list[dict[str, Any]] = []
    external_open_orders: list[dict[str, Any]] = []
    for order in exchange_open_orders:
        order_uuid = str(order.get("uuid"))
        identifier = _as_optional_str(order.get("identifier"))
        if is_bot_identifier(identifier, prefix=identifier_prefix, bot_id=bot_id) or order_uuid in local_open_orders:
            exchange_bot_open_orders.append(order)
        else:
            external_open_orders.append(order)

    if unknown_open_orders_policy == "cancel":
        for item in exchange_bot_open_orders:
            actions.append(
                {
                    "type": "cancel_bot_open_order",
                    "uuid": item.get("uuid"),
                    "identifier": item.get("identifier"),
                    "market": item.get("market"),
                }
            )
        if external_open_orders and not allow_cancel_external_orders:
            halted_reasons.append("EXTERNAL_OPEN_ORDERS_CANCEL_BLOCKED")
        if external_open_orders and allow_cancel_external_orders:
            for item in external_open_orders:
                actions.append(
                    {
                        "type": "cancel_external_open_order",
                        "uuid": item.get("uuid"),
                        "identifier": item.get("identifier"),
                        "market": item.get("market"),
                    }
                )
    elif external_open_orders and unknown_open_orders_policy == "halt":
        halted_reasons.append("UNKNOWN_OPEN_ORDERS_DETECTED")

    for order in exchange_bot_open_orders:
        record = _order_record_from_payload(order, ts_ms=now_ts)
        if record is None:
            continue
        local_existing = local_orders.get(record.uuid)
        existing_intent_id = _as_optional_str(local_existing.get("intent_id")) if local_existing else None
        if not existing_intent_id:
            existing_intent_id = f"inferred-{record.uuid}"
        record = replace(record, intent_id=existing_intent_id)
        if not dry_run:
            store.upsert_order(record)
        inferred_intent = local_existing is None or not _as_optional_str(local_existing.get("intent_id"))
        if inferred_intent:
            intent_payload = {
                "source": "exchange",
                "order_uuid": record.uuid,
                "identifier": record.identifier,
            }
            if not dry_run:
                store.upsert_intent(
                    IntentRecord(
                        intent_id=existing_intent_id,
                        ts_ms=now_ts,
                        market=record.market,
                        side=str(record.side or "bid"),
                        price=record.price,
                        volume=record.volume_req,
                        reason_code="EXCHANGE_RECONCILE",
                        meta_json=json.dumps(intent_payload, ensure_ascii=False, sort_keys=True),
                        status="INFERRED_FROM_EXCHANGE",
                    )
                )
            actions.append(
                {
                    "type": "inferred_intent_upsert",
                    "intent_id": existing_intent_id,
                    "order_uuid": record.uuid,
                }
            )
        actions.append({"type": "upsert_bot_order", "uuid": record.uuid, "state": record.state})

    exchange_open_uuids = {str(item.get("uuid")) for item in exchange_bot_open_orders if item.get("uuid")}
    local_only_open_uuids = sorted(set(local_open_orders) - exchange_open_uuids)
    for local_uuid in local_only_open_uuids:
        local_item = local_open_orders[local_uuid]
        detail_payload: Any = None
        if fetch_order_detail is not None:
            try:
                detail_payload = fetch_order_detail(local_uuid, _as_optional_str(local_item.get("identifier")))
            except Exception as exc:  # pragma: no cover - defensive path
                warnings.append(f"order detail lookup failed uuid={local_uuid}: {exc}")

        detail_record = _order_record_from_payload(detail_payload, ts_ms=now_ts) if isinstance(detail_payload, dict) else None
        if detail_record is not None:
            if not dry_run:
                store.upsert_order(detail_record)
            actions.append(
                {
                    "type": "sync_local_order_from_detail",
                    "uuid": detail_record.uuid,
                    "state": detail_record.state,
                }
            )
            continue

        if not dry_run:
            store.mark_order_state(uuid=local_uuid, state="cancel", updated_ts=now_ts)
        actions.append({"type": "mark_local_order_closed", "uuid": local_uuid, "state": "cancel"})

    exchange_positions = _extract_exchange_positions(accounts_payload, quote_currency=quote_currency, ts_ms=now_ts)
    local_positions = {item["market"]: item for item in store.list_positions()}
    unknown_position_markets = sorted(set(exchange_positions) - set(local_positions))

    if unknown_position_markets:
        if unknown_positions_policy == "halt":
            halted_reasons.append("UNKNOWN_POSITIONS_DETECTED")
        else:
            managed = unknown_positions_policy == "attach_default_risk"
            tp_json = "{}"
            sl_json = "{}"
            trailing_json = "{}"
            if unknown_positions_policy == "attach_default_risk":
                tp_json = json.dumps({"mode": "default_pct", "tp_pct": float(default_risk_tp_pct)}, ensure_ascii=False)
                sl_json = json.dumps({"mode": "default_pct", "sl_pct": float(default_risk_sl_pct)}, ensure_ascii=False)
                trailing_json = json.dumps(
                    {"enabled": bool(default_risk_trailing_enabled), "mode": "default"},
                    ensure_ascii=False,
                )
            for market in unknown_position_markets:
                position = exchange_positions[market]
                record = PositionRecord(
                    market=market,
                    base_currency=str(position["base_currency"]),
                    base_amount=float(position["base_amount"]),
                    avg_entry_price=float(position["avg_entry_price"]),
                    updated_ts=now_ts,
                    tp_json=tp_json,
                    sl_json=sl_json,
                    trailing_json=trailing_json,
                    managed=managed,
                )
                if not dry_run:
                    store.upsert_position(record)
                    if unknown_positions_policy == "attach_default_risk":
                        plan_id = f"default-risk-{market}"
                        store.upsert_risk_plan(
                            RiskPlanRecord(
                                plan_id=plan_id,
                                market=market,
                                side="long",
                                entry_price_str=str(float(position["avg_entry_price"])),
                                qty_str=str(float(position["base_amount"])),
                                tp_enabled=float(default_risk_tp_pct) > 0,
                                tp_price_str=None,
                                tp_pct=float(default_risk_tp_pct) if float(default_risk_tp_pct) > 0 else None,
                                sl_enabled=float(default_risk_sl_pct) > 0,
                                sl_price_str=None,
                                sl_pct=float(default_risk_sl_pct) if float(default_risk_sl_pct) > 0 else None,
                                trailing_enabled=bool(default_risk_trailing_enabled),
                                trail_pct=0.01 if bool(default_risk_trailing_enabled) else None,
                                high_watermark_price_str=None,
                                armed_ts_ms=None,
                                state="ACTIVE",
                                last_eval_ts_ms=now_ts,
                                last_action_ts_ms=0,
                                current_exit_order_uuid=None,
                                current_exit_order_identifier=None,
                                replace_attempt=0,
                                created_ts=now_ts,
                                updated_ts=now_ts,
                            )
                        )
                actions.append(
                    {
                        "type": "upsert_unknown_position",
                        "market": market,
                        "managed": managed,
                    }
                )
                if unknown_positions_policy == "attach_default_risk":
                    actions.append(
                        {
                            "type": "upsert_default_risk_plan",
                            "market": market,
                            "plan_id": f"default-risk-{market}",
                        }
                    )

    report = {
        "halted": bool(halted_reasons),
        "halted_reasons": halted_reasons,
        "dry_run": bool(dry_run),
        "bot_id": bot_id,
        "identifier_prefix": identifier_prefix,
        "policies": {
            "unknown_open_orders_policy": unknown_open_orders_policy,
            "unknown_positions_policy": unknown_positions_policy,
            "allow_cancel_external_orders": bool(allow_cancel_external_orders),
        },
        "counts": {
            "exchange_open_orders": len(exchange_open_orders),
            "exchange_bot_open_orders": len(exchange_bot_open_orders),
            "external_open_orders": len(external_open_orders),
            "local_open_orders": len(local_open_orders),
            "local_only_open_orders": len(local_only_open_uuids),
            "exchange_positions": len(exchange_positions),
            "local_positions": len(local_positions),
            "unknown_positions": len(unknown_position_markets),
        },
        "external_open_orders": [
            {
                "uuid": item.get("uuid"),
                "identifier": item.get("identifier"),
                "market": item.get("market"),
                "state": item.get("state"),
            }
            for item in external_open_orders
        ],
        "unknown_positions": [exchange_positions[market] for market in unknown_position_markets],
        "actions": actions,
        "warnings": warnings,
        "ts_ms": now_ts,
    }
    return report


def apply_cancel_actions(
    *,
    report: dict[str, Any],
    cancel_order: Callable[[str | None, str | None], Any],
    apply: bool,
    allow_cancel_external_cli: bool,
    allow_cancel_external_config: bool,
) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "apply": bool(apply),
        "attempted": 0,
        "executed": 0,
        "failed": 0,
        "skipped": 0,
        "results": [],
    }
    actions = report.get("actions", [])
    if not isinstance(actions, list):
        return summary

    for action in actions:
        if not isinstance(action, dict):
            continue
        action_type = str(action.get("type", ""))
        if action_type not in {"cancel_bot_open_order", "cancel_external_open_order"}:
            continue
        summary["attempted"] += 1
        is_external = action_type == "cancel_external_open_order"
        uuid = _as_optional_str(action.get("uuid"))
        identifier = _as_optional_str(action.get("identifier"))
        result_item = {"type": action_type, "uuid": uuid, "identifier": identifier}

        if is_external and not (allow_cancel_external_cli and allow_cancel_external_config):
            summary["skipped"] += 1
            result_item["status"] = "skipped"
            result_item["reason"] = "external_cancel_opt_in_required"
            summary["results"].append(result_item)
            continue
        if not apply:
            summary["skipped"] += 1
            result_item["status"] = "skipped"
            result_item["reason"] = "dry_run"
            summary["results"].append(result_item)
            continue

        try:
            payload = cancel_order(uuid, identifier)
            summary["executed"] += 1
            result_item["status"] = "executed"
            result_item["payload"] = payload
        except Exception as exc:  # pragma: no cover - defensive path
            summary["failed"] += 1
            result_item["status"] = "failed"
            result_item["error"] = str(exc)
        summary["results"].append(result_item)
    return summary


def _order_record_from_payload(payload: Any, *, ts_ms: int) -> OrderRecord | None:
    if not isinstance(payload, dict):
        return None
    uuid = _as_optional_str(payload.get("uuid"))
    market = _as_optional_str(payload.get("market"))
    if not uuid or not market:
        return None
    created_ts = _parse_created_ts(payload.get("created_at"), fallback_ts=ts_ms)
    return OrderRecord(
        uuid=uuid,
        identifier=_as_optional_str(payload.get("identifier")),
        market=market.upper(),
        side=_as_optional_str(payload.get("side")),
        ord_type=_as_optional_str(payload.get("ord_type")),
        price=_as_optional_float(payload.get("price")),
        volume_req=_as_optional_float(payload.get("volume")),
        volume_filled=float(_as_optional_float(payload.get("executed_volume")) or 0.0),
        state=str(payload.get("state") or "wait").strip().lower(),
        created_ts=created_ts,
        updated_ts=ts_ms,
    )


def _extract_exchange_positions(accounts_payload: Any, *, quote_currency: str, ts_ms: int) -> dict[str, dict[str, Any]]:
    positions: dict[str, dict[str, Any]] = {}
    quote_upper = quote_currency.strip().upper()
    for account in _as_dict_list(accounts_payload):
        currency = str(account.get("currency", "")).strip().upper()
        if not currency or currency == quote_upper:
            continue
        balance = float(_as_optional_float(account.get("balance")) or 0.0)
        locked = float(_as_optional_float(account.get("locked")) or 0.0)
        total = balance + locked
        if total <= 0.0:
            continue
        market = f"{quote_upper}-{currency}"
        positions[market] = {
            "market": market,
            "base_currency": currency,
            "base_amount": total,
            "avg_entry_price": float(_as_optional_float(account.get("avg_buy_price")) or 0.0),
            "updated_ts": ts_ms,
        }
    return positions


def _parse_created_ts(raw: object, *, fallback_ts: int) -> int:
    value = _as_optional_str(raw)
    if not value:
        return fallback_ts
    try:
        return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)
    except ValueError:
        return fallback_ts


def _as_dict_list(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, list):
        return []
    return [item for item in payload if isinstance(item, dict)]


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
