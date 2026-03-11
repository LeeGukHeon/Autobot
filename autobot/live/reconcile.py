"""Exchange-vs-local reconciliation helpers for live runtime startup."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from datetime import datetime
import json
from pathlib import Path
from typing import Any, Literal
import time

from .identifier import extract_intent_id_from_identifier, is_bot_identifier
from .admissibility import extract_min_total
from .order_state import is_open_local_state, normalize_order_state
from .model_risk_plan import build_model_derived_risk_records, extract_model_exit_plan
from .state_store import IntentRecord, LiveStateStore, OrderRecord, PositionRecord, RiskPlanRecord
from .trade_journal import activate_trade_journal_for_position, close_trade_journal_for_market

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
    fetch_market_chance: Callable[[str], Any] | None = None,
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
    local_open_orders = {uuid: item for uuid, item in local_orders.items() if is_open_local_state(item.get("local_state"))}
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
    exchange_bot_open_orders_by_market: dict[str, list[dict[str, Any]]] = {}
    for item in exchange_bot_open_orders:
        market_key = _as_optional_str(item.get("market"))
        if not market_key:
            continue
        exchange_bot_open_orders_by_market.setdefault(market_key.upper(), []).append(item)

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
            recovered_intent_id = _as_optional_str(local_item.get("intent_id")) or extract_intent_id_from_identifier(
                _as_optional_str(detail_record.identifier) or _as_optional_str(local_item.get("identifier")),
                prefix=identifier_prefix,
                bot_id=bot_id,
            )
            detail_record = replace(
                detail_record,
                intent_id=recovered_intent_id,
                tp_sl_link=_as_optional_str(local_item.get("tp_sl_link")),
            )
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
    intents_by_id = {str(item.get("intent_id")): item for item in store.list_intents() if str(item.get("intent_id") or "").strip()}
    bid_orders_by_market = _latest_bot_bid_orders_by_market(
        store=store,
        bot_id=bot_id,
        identifier_prefix=identifier_prefix,
    )
    done_ask_orders_by_market = _latest_bot_done_ask_orders_by_market(
        store=store,
        bot_id=bot_id,
        identifier_prefix=identifier_prefix,
    )
    unknown_position_markets = sorted(set(exchange_positions) - set(local_positions))
    local_positions_missing_on_exchange = sorted(set(local_positions) - set(exchange_positions))
    ignored_dust_positions: list[dict[str, Any]] = []
    retained_unknown_markets: list[str] = []

    for market in sorted(set(exchange_positions) & set(local_positions)):
        position = exchange_positions[market]
        local_position = local_positions[market]
        dust_detail = _build_ignored_dust_position_detail(
            position=position,
            fetch_market_chance=fetch_market_chance,
        )
        if dust_detail is None:
            continue
        ignored_dust_positions.append(dust_detail)
        if not dry_run:
            store.delete_position(market=market)
            for row in store.list_risk_plans(market=market):
                store.upsert_risk_plan(
                    _risk_plan_record_from_row(
                        row,
                        state="CLOSED",
                        current_exit_order_uuid=_as_optional_str(row.get("current_exit_order_uuid")),
                        current_exit_order_identifier=_as_optional_str(row.get("current_exit_order_identifier")),
                        updated_ts=now_ts,
                        last_eval_ts_ms=max(int(row.get("last_eval_ts_ms") or 0), now_ts),
                    )
                )
        actions.append(
            {
                "type": "drop_managed_dust_position",
                "market": market,
                "reference_notional_quote": dust_detail["reference_notional_quote"],
                "min_total_quote": dust_detail["min_total_quote"],
            }
        )
        del local_positions[market]
        del exchange_positions[market]

    for market in unknown_position_markets:
        position = exchange_positions[market]
        dust_detail = _build_ignored_dust_position_detail(
            position=position,
            fetch_market_chance=fetch_market_chance,
        )
        if dust_detail is not None:
            ignored_dust_positions.append(dust_detail)
            actions.append(
                {
                    "type": "ignore_unknown_dust_position",
                    "market": market,
                    "reference_notional_quote": dust_detail["reference_notional_quote"],
                    "min_total_quote": dust_detail["min_total_quote"],
                }
            )
            continue
        matched_import = _match_model_managed_position_import(
            market=market,
            position=position,
            latest_bid_order=bid_orders_by_market.get(market),
            exchange_bot_open_orders=exchange_bot_open_orders_by_market.get(market, []),
            intents_by_id=intents_by_id,
            ts_ms=now_ts,
        )
        if matched_import is not None:
            if not dry_run:
                risk_plan_record = matched_import["risk_plan_record"]
                existing_live_plans = store.list_risk_plans(
                    market=market,
                    states=("ACTIVE", "TRIGGERED", "EXITING"),
                )
                if existing_live_plans:
                    selected_existing = max(
                        existing_live_plans,
                        key=lambda item: (
                            int(
                                bool(
                                    _as_optional_str(item.get("current_exit_order_uuid"))
                                    or _as_optional_str(item.get("current_exit_order_identifier"))
                                )
                            ),
                            int(item.get("updated_ts") or 0),
                            int(item.get("created_ts") or 0),
                            str(item.get("plan_id") or ""),
                        ),
                    )
                    recovered_exit_uuid = _as_optional_str(risk_plan_record.current_exit_order_uuid)
                    recovered_exit_identifier = _as_optional_str(risk_plan_record.current_exit_order_identifier)
                    current_exit_uuid = _as_optional_str(selected_existing.get("current_exit_order_uuid")) or recovered_exit_uuid
                    current_exit_identifier = (
                        _as_optional_str(selected_existing.get("current_exit_order_identifier")) or recovered_exit_identifier
                    )
                    preserved_last_action_ts_ms = int(selected_existing.get("last_action_ts_ms") or 0)
                    matched_exit_order = None
                    if current_exit_uuid:
                        matched_exit_order = store.order_by_uuid(uuid=current_exit_uuid)
                    if matched_exit_order is None and current_exit_identifier:
                        matched_exit_order = store.order_by_identifier(identifier=current_exit_identifier)
                    if matched_exit_order is not None and preserved_last_action_ts_ms <= 0:
                        preserved_last_action_ts_ms = max(
                            int(matched_exit_order.get("updated_ts") or 0),
                            int(matched_exit_order.get("created_ts") or 0),
                            now_ts,
                        )
                    merged_state = str(selected_existing.get("state") or risk_plan_record.state or "ACTIVE")
                    if (recovered_exit_uuid or recovered_exit_identifier) and str(risk_plan_record.state or "").strip().upper() == "EXITING":
                        merged_state = "EXITING"
                    risk_plan_record = replace(
                        risk_plan_record,
                        plan_id=str(selected_existing.get("plan_id") or risk_plan_record.plan_id),
                        state=merged_state,
                        last_eval_ts_ms=max(
                            int(selected_existing.get("last_eval_ts_ms") or 0),
                            int(risk_plan_record.last_eval_ts_ms),
                        ),
                        last_action_ts_ms=max(
                            preserved_last_action_ts_ms,
                            int(risk_plan_record.last_action_ts_ms),
                        ),
                        current_exit_order_uuid=current_exit_uuid,
                        current_exit_order_identifier=current_exit_identifier,
                        replace_attempt=max(
                            int(selected_existing.get("replace_attempt") or 0),
                            int(risk_plan_record.replace_attempt),
                        ),
                        created_ts=(
                            min(
                                value
                                for value in (
                                    int(selected_existing.get("created_ts") or 0),
                                    int(risk_plan_record.created_ts or 0),
                                )
                                if value > 0
                            )
                            if any(
                                value > 0
                                for value in (
                                    int(selected_existing.get("created_ts") or 0),
                                    int(risk_plan_record.created_ts or 0),
                                )
                            )
                            else int(now_ts)
                        ),
                        updated_ts=int(now_ts),
                        plan_source=_as_optional_str(selected_existing.get("plan_source")) or risk_plan_record.plan_source,
                        source_intent_id=_as_optional_str(selected_existing.get("source_intent_id")) or risk_plan_record.source_intent_id,
                    )
                    if matched_exit_order is not None and _as_optional_str(matched_exit_order.get("tp_sl_link")) != risk_plan_record.plan_id:
                        store.upsert_order(
                            _order_record_from_row_dict(
                                matched_exit_order,
                                tp_sl_link=risk_plan_record.plan_id,
                            )
                        )
                store.upsert_position(matched_import["position_record"])
                store.upsert_risk_plan(risk_plan_record)
                activate_trade_journal_for_position(
                    store=store,
                    market=market,
                    position={
                        "market": matched_import["position_record"].market,
                        "base_amount": matched_import["position_record"].base_amount,
                        "avg_entry_price": matched_import["position_record"].avg_entry_price,
                        "updated_ts": matched_import["position_record"].updated_ts,
                    },
                    ts_ms=now_ts,
                    entry_intent=intents_by_id.get(matched_import["intent_id"]),
                    plan_id=risk_plan_record.plan_id,
                )
                order_uuid = matched_import.get("order_uuid")
                if isinstance(order_uuid, str) and order_uuid.strip():
                    store.mark_order_state(uuid=order_uuid, state="done", updated_ts=now_ts)
                exit_order_record = matched_import.get("exit_order_record")
                if isinstance(exit_order_record, OrderRecord):
                    store.upsert_order(replace(exit_order_record, tp_sl_link=risk_plan_record.plan_id))
            actions.append(
                {
                    "type": "import_managed_position_from_bot_intent",
                    "market": market,
                    "intent_id": matched_import["intent_id"],
                    "plan_id": risk_plan_record.plan_id if not dry_run else matched_import["risk_plan_record"].plan_id,
                }
            )
            continue
        retained_unknown_markets.append(market)
    unknown_position_markets = retained_unknown_markets

    retained_local_missing_markets: list[str] = []
    for market in local_positions_missing_on_exchange:
        local_position = local_positions[market]
        active_live_plans = store.list_risk_plans(
            market=market,
            states=("ACTIVE", "TRIGGERED", "EXITING"),
        )
        matched_close = _match_model_managed_position_close(
            market=market,
            local_position=local_position,
            latest_done_ask_order=done_ask_orders_by_market.get(market),
            active_live_plans=active_live_plans,
            exchange_bot_open_orders=exchange_bot_open_orders_by_market.get(market, []),
        )
        if matched_close is not None:
            if not dry_run:
                close_trade_journal_for_market(
                    store=store,
                    market=market,
                    position=local_position,
                    ts_ms=now_ts,
                    close_mode=_as_optional_str(matched_close.get("close_mode")),
                    exit_order_uuid=_as_optional_str(matched_close.get("order_uuid")),
                    exit_meta={
                        "order_identifier": matched_close.get("order_identifier"),
                        "close_mode": matched_close.get("close_mode"),
                        "source": "reconcile_exchange_snapshot",
                    },
                )
                store.delete_position(market=market)
                for row in store.list_risk_plans(market=market):
                    store.upsert_risk_plan(
                        _risk_plan_record_from_row(
                            row,
                            state="CLOSED",
                            current_exit_order_uuid=matched_close.get("order_uuid"),
                            current_exit_order_identifier=matched_close.get("order_identifier"),
                            updated_ts=now_ts,
                            last_eval_ts_ms=max(int(row.get("last_eval_ts_ms") or 0), now_ts),
                        )
                    )
                if matched_close.get("order_uuid"):
                    store.mark_order_state(uuid=str(matched_close["order_uuid"]), state="done", updated_ts=now_ts)
            actions.append(
                {
                    "type": "close_managed_position_from_bot_exit",
                    "market": market,
                    "order_uuid": matched_close.get("order_uuid"),
                    "order_identifier": matched_close.get("order_identifier"),
                    "close_mode": matched_close.get("close_mode"),
                }
            )
            continue
        retained_local_missing_markets.append(market)
    local_positions_missing_on_exchange = retained_local_missing_markets

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
            "ignored_dust_positions": len(ignored_dust_positions),
            "local_positions_missing_on_exchange": len(local_positions_missing_on_exchange),
        },
        "exchange_bot_open_orders": [
            {
                "uuid": item.get("uuid"),
                "identifier": item.get("identifier"),
                "market": item.get("market"),
                "state": item.get("state"),
            }
            for item in exchange_bot_open_orders
        ],
        "external_open_orders": [
            {
                "uuid": item.get("uuid"),
                "identifier": item.get("identifier"),
                "market": item.get("market"),
                "state": item.get("state"),
            }
            for item in external_open_orders
        ],
        "local_only_open_orders": [
            {
                "uuid": local_uuid,
                "identifier": local_open_orders[local_uuid].get("identifier"),
                "market": local_open_orders[local_uuid].get("market"),
                "local_state": local_open_orders[local_uuid].get("local_state"),
            }
            for local_uuid in local_only_open_uuids
        ],
        "unknown_positions": [exchange_positions[market] for market in unknown_position_markets],
        "ignored_dust_positions": ignored_dust_positions,
        "local_positions_missing_on_exchange": [local_positions[market] for market in local_positions_missing_on_exchange],
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


def resume_risk_plans_after_reconcile(
    *,
    store: LiveStateStore,
    ts_ms: int | None = None,
) -> dict[str, Any]:
    now_ts = int(ts_ms if ts_ms is not None else time.time() * 1000)
    positions = {item["market"]: item for item in store.list_positions()}
    open_orders = store.list_orders(open_only=True)
    open_orders_by_uuid = {str(item.get("uuid")): item for item in open_orders if item.get("uuid")}
    open_orders_by_identifier = {
        str(item.get("identifier")): item
        for item in open_orders
        if item.get("identifier")
    }
    all_risk_plans = store.list_risk_plans()
    primary_active_plan_by_market = _primary_active_plan_ids_by_market(
        risk_plans=all_risk_plans,
        positions=positions,
    )

    report: dict[str, Any] = {
        "ts_ms": now_ts,
        "halted": False,
        "halted_plan_ids": [],
        "counts": {
            "positions": len(positions),
            "open_orders": len(open_orders),
            "risk_plans": 0,
            "plans_resumed_exiting": 0,
            "plans_retriggered": 0,
            "plans_closed": 0,
            "plans_kept_active": 0,
            "plans_halted_for_review": 0,
        },
        "plans": [],
    }

    for row in all_risk_plans:
        report["counts"]["risk_plans"] += 1
        plan_id = str(row.get("plan_id") or "")
        market = str(row.get("market") or "")
        previous_state = str(row.get("state") or "")
        current_exit_uuid = _as_optional_str(row.get("current_exit_order_uuid"))
        current_exit_identifier = _as_optional_str(row.get("current_exit_order_identifier"))
        position = positions.get(market)
        primary_active_plan_id = primary_active_plan_by_market.get(market)

        if (
            position is not None
            and previous_state == "CLOSED"
            and primary_active_plan_id is not None
            and plan_id != primary_active_plan_id
        ):
            report["plans"].append(
                {
                    "plan_id": plan_id,
                    "market": market,
                    "previous_state": previous_state,
                    "next_state": previous_state,
                    "position_present": True,
                    "matched_open_exit_order_uuid": None,
                    "matched_open_exit_order_identifier": None,
                    "resumed_from_restart": True,
                    "halted_for_review": False,
                    "action": "KEEP_CLOSED_HISTORY",
                }
            )
            continue

        if (
            position is not None
            and previous_state in {"ACTIVE", "TRIGGERED", "EXITING"}
            and primary_active_plan_id is not None
            and plan_id != primary_active_plan_id
        ):
            report["halted"] = True
            report["halted_plan_ids"].append(plan_id)
            report["counts"]["plans_halted_for_review"] += 1
            report["plans"].append(
                {
                    "plan_id": plan_id,
                    "market": market,
                    "previous_state": previous_state,
                    "next_state": previous_state,
                    "position_present": True,
                    "matched_open_exit_order_uuid": None,
                    "matched_open_exit_order_identifier": None,
                    "resumed_from_restart": False,
                    "halted_for_review": True,
                    "action": "HALT_DUPLICATE_ACTIVE_PLAN",
                }
            )
            continue
        matching_open_order = None
        if current_exit_uuid:
            matching_open_order = open_orders_by_uuid.get(current_exit_uuid)
        if matching_open_order is None and current_exit_identifier:
            matching_open_order = open_orders_by_identifier.get(current_exit_identifier)
        if matching_open_order is None and position is not None and not current_exit_uuid and not current_exit_identifier:
            market_open_asks = [
                item
                for item in open_orders
                if str(item.get("market") or "") == market and str(item.get("side") or "").lower() == "ask"
            ]
            if len(market_open_asks) == 1:
                matching_open_order = market_open_asks[0]
        ambiguous_market_orders = [
            item
            for item in open_orders
            if str(item.get("market") or "") == market
            and str(item.get("side") or "").lower() == "ask"
            and item is not matching_open_order
        ]

        action = "KEEP"
        next_state = previous_state
        next_exit_uuid = current_exit_uuid
        next_exit_identifier = current_exit_identifier
        next_last_action_ts_ms = int(row.get("last_action_ts_ms") or 0)
        halted_for_review = False

        if matching_open_order is not None:
            next_state = "EXITING"
            next_exit_uuid = _as_optional_str(matching_open_order.get("uuid"))
            next_exit_identifier = _as_optional_str(matching_open_order.get("identifier"))
            if next_last_action_ts_ms <= 0:
                next_last_action_ts_ms = max(
                    int(matching_open_order.get("updated_ts") or 0),
                    int(matching_open_order.get("created_ts") or 0),
                    now_ts,
                )
            action = "RESUME_EXITING"
            report["counts"]["plans_resumed_exiting"] += 1
        elif position is None:
            next_state = "CLOSED"
            next_exit_uuid = None
            next_exit_identifier = None
            action = "CLOSE_NO_POSITION"
            report["counts"]["plans_closed"] += 1
        else:
            if ambiguous_market_orders and (current_exit_uuid or current_exit_identifier or previous_state == "EXITING"):
                halted_for_review = True
                action = "HALT_AMBIGUOUS_EXIT"
                report["counts"]["plans_halted_for_review"] += 1
                report["halted"] = True
                report["halted_plan_ids"].append(plan_id)
            elif current_exit_uuid or current_exit_identifier or previous_state == "EXITING":
                next_state = "TRIGGERED"
                next_exit_uuid = None
                next_exit_identifier = None
                action = "RETRIGGER_MISSING_EXIT"
                report["counts"]["plans_retriggered"] += 1
            else:
                next_state = "ACTIVE" if previous_state not in {"ACTIVE", "TRIGGERED"} else previous_state
                action = "KEEP_ACTIVE"
                report["counts"]["plans_kept_active"] += 1

        if not halted_for_review:
            if matching_open_order is not None and _as_optional_str(matching_open_order.get("tp_sl_link")) != plan_id:
                store.upsert_order(
                    _order_record_from_row_dict(
                        matching_open_order,
                        tp_sl_link=plan_id,
                    )
                )
            updated = _risk_plan_record_from_row(
                row,
                state=next_state,
                current_exit_order_uuid=next_exit_uuid,
                current_exit_order_identifier=next_exit_identifier,
                updated_ts=now_ts,
                last_eval_ts_ms=max(int(row.get("last_eval_ts_ms") or 0), now_ts if next_state == "CLOSED" else int(row.get("last_eval_ts_ms") or 0)),
                last_action_ts_ms=next_last_action_ts_ms,
            )
            store.upsert_risk_plan(updated)

        report["plans"].append(
            {
                "plan_id": plan_id,
                "market": market,
                "previous_state": previous_state,
                "next_state": next_state,
                "position_present": position is not None,
                "matched_open_exit_order_uuid": _as_optional_str(matching_open_order.get("uuid")) if matching_open_order else None,
                "matched_open_exit_order_identifier": _as_optional_str(matching_open_order.get("identifier")) if matching_open_order else None,
                "resumed_from_restart": not halted_for_review,
                "halted_for_review": halted_for_review,
                "action": action,
            }
        )

    store.set_checkpoint(name="last_resume", payload=report, ts_ms=now_ts)
    _write_resume_report(path=store.db_path.parent / "live_resume_report.json", payload=report)
    return report


def _primary_active_plan_ids_by_market(
    *,
    risk_plans: list[dict[str, Any]],
    positions: dict[str, dict[str, Any]],
) -> dict[str, str]:
    result: dict[str, str] = {}
    for market in positions:
        active_rows = [
            item
            for item in risk_plans
            if str(item.get("market") or "") == market
            and str(item.get("state") or "") in {"ACTIVE", "TRIGGERED", "EXITING"}
        ]
        if not active_rows:
            continue

        def _sort_key(row: dict[str, Any]) -> tuple[int, int, int, str]:
            has_exit_binding = int(
                bool(
                    _as_optional_str(row.get("current_exit_order_uuid"))
                    or _as_optional_str(row.get("current_exit_order_identifier"))
                )
            )
            return (
                has_exit_binding,
                int(row.get("updated_ts") or 0),
                int(row.get("created_ts") or 0),
                str(row.get("plan_id") or ""),
            )

        primary = max(active_rows, key=_sort_key)
        result[market] = str(primary.get("plan_id") or "")
    return result


def _order_record_from_payload(payload: Any, *, ts_ms: int) -> OrderRecord | None:
    if not isinstance(payload, dict):
        return None
    uuid = _as_optional_str(payload.get("uuid"))
    market = _as_optional_str(payload.get("market"))
    if not uuid or not market:
        return None
    created_ts = _parse_created_ts(payload.get("created_at"), fallback_ts=ts_ms)
    normalized = normalize_order_state(
        exchange_state=_as_optional_str(payload.get("state")),
        event_name="EXCHANGE_SNAPSHOT",
        executed_volume=_as_optional_float(payload.get("executed_volume")),
    )
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
        local_state=normalized.local_state,
        raw_exchange_state=normalized.exchange_state,
        last_event_name=normalized.event_name,
        event_source="reconcile_snapshot",
        root_order_uuid=uuid,
    )


def _risk_plan_record_from_row(
    row: dict[str, Any],
    *,
    state: str,
    current_exit_order_uuid: str | None,
    current_exit_order_identifier: str | None,
    updated_ts: int,
    last_eval_ts_ms: int,
    last_action_ts_ms: int | None = None,
) -> RiskPlanRecord:
    tp = row.get("tp") if isinstance(row.get("tp"), dict) else {}
    sl = row.get("sl") if isinstance(row.get("sl"), dict) else {}
    trailing = row.get("trailing") if isinstance(row.get("trailing"), dict) else {}
    return RiskPlanRecord(
        plan_id=str(row.get("plan_id") or ""),
        market=str(row.get("market") or ""),
        side=str(row.get("side") or ""),
        entry_price_str=str(row.get("entry_price_str") or ""),
        qty_str=str(row.get("qty_str") or ""),
        tp_enabled=bool(tp.get("enabled")),
        tp_price_str=_as_optional_str(tp.get("tp_price_str")),
        tp_pct=_as_optional_float(tp.get("tp_pct")),
        sl_enabled=bool(sl.get("enabled")),
        sl_price_str=_as_optional_str(sl.get("sl_price_str")),
        sl_pct=_as_optional_float(sl.get("sl_pct")),
        trailing_enabled=bool(trailing.get("enabled")),
        trail_pct=_as_optional_float(trailing.get("trail_pct")),
        high_watermark_price_str=_as_optional_str(trailing.get("high_watermark_price_str")),
        armed_ts_ms=_as_optional_int(trailing.get("armed_ts_ms")),
        timeout_ts_ms=_as_optional_int(row.get("timeout_ts_ms")),
        state=state,
        last_eval_ts_ms=int(last_eval_ts_ms),
        last_action_ts_ms=int(last_action_ts_ms if last_action_ts_ms is not None else row.get("last_action_ts_ms") or 0),
        current_exit_order_uuid=current_exit_order_uuid,
        current_exit_order_identifier=current_exit_order_identifier,
        replace_attempt=int(row.get("replace_attempt") or 0),
        created_ts=int(row.get("created_ts") or updated_ts),
        updated_ts=int(updated_ts),
        plan_source=_as_optional_str(row.get("plan_source")),
        source_intent_id=_as_optional_str(row.get("source_intent_id")),
    )


def _order_record_from_row_dict(
    row: dict[str, Any],
    *,
    tp_sl_link: str | None = None,
) -> OrderRecord:
    return OrderRecord(
        uuid=str(row.get("uuid") or ""),
        identifier=_as_optional_str(row.get("identifier")),
        market=str(row.get("market") or ""),
        side=_as_optional_str(row.get("side")),
        ord_type=_as_optional_str(row.get("ord_type")),
        price=_as_optional_float(row.get("price")),
        volume_req=_as_optional_float(row.get("volume_req")),
        volume_filled=float(_as_optional_float(row.get("volume_filled")) or 0.0),
        state=str(row.get("state") or ""),
        created_ts=int(row.get("created_ts") or 0),
        updated_ts=int(row.get("updated_ts") or 0),
        intent_id=_as_optional_str(row.get("intent_id")),
        tp_sl_link=tp_sl_link if tp_sl_link is not None else _as_optional_str(row.get("tp_sl_link")),
        local_state=_as_optional_str(row.get("local_state")),
        raw_exchange_state=_as_optional_str(row.get("raw_exchange_state")),
        last_event_name=_as_optional_str(row.get("last_event_name")),
        event_source=_as_optional_str(row.get("event_source")),
        replace_seq=int(row.get("replace_seq") or 0),
        root_order_uuid=_as_optional_str(row.get("root_order_uuid")),
        prev_order_uuid=_as_optional_str(row.get("prev_order_uuid")),
        prev_order_identifier=_as_optional_str(row.get("prev_order_identifier")),
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


def _latest_bot_bid_orders_by_market(
    *,
    store: LiveStateStore,
    bot_id: str,
    identifier_prefix: str,
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for item in store.list_orders(open_only=False):
        market = str(item.get("market", "")).strip().upper()
        side = str(item.get("side", "")).strip().lower()
        identifier = _as_optional_str(item.get("identifier"))
        if not market or side != "bid":
            continue
        if not is_bot_identifier(identifier, prefix=identifier_prefix, bot_id=bot_id):
            continue
        item_intent_id = _as_optional_str(item.get("intent_id")) or extract_intent_id_from_identifier(
            identifier,
            prefix=identifier_prefix,
            bot_id=bot_id,
        )
        candidate = dict(item)
        candidate["intent_id"] = item_intent_id
        existing = result.get(market)
        candidate_has_fill = int(
            float(candidate.get("volume_filled") or 0.0) > 0.0
            or str(candidate.get("local_state") or "").strip().upper() in {"DONE", "PARTIAL"}
            or str(candidate.get("state") or "").strip().lower() == "done"
        )
        existing_has_fill = int(
            existing is not None
            and (
                float(existing.get("volume_filled") or 0.0) > 0.0
                or str(existing.get("local_state") or "").strip().upper() in {"DONE", "PARTIAL"}
                or str(existing.get("state") or "").strip().lower() == "done"
            )
        )
        if (
            existing is None
            or candidate_has_fill > existing_has_fill
            or (
                candidate_has_fill == existing_has_fill
                and int(candidate.get("updated_ts") or 0) > int(existing.get("updated_ts") or 0)
            )
        ):
            result[market] = candidate
    return result


def _latest_bot_done_ask_orders_by_market(
    *,
    store: LiveStateStore,
    bot_id: str,
    identifier_prefix: str,
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for item in store.list_orders(open_only=False):
        market = str(item.get("market", "")).strip().upper()
        side = str(item.get("side", "")).strip().lower()
        identifier = _as_optional_str(item.get("identifier"))
        if not market or side != "ask":
            continue
        if not is_bot_identifier(identifier, prefix=identifier_prefix, bot_id=bot_id):
            continue
        if str(item.get("local_state") or "").strip().upper() != "DONE":
            continue
        existing = result.get(market)
        if existing is None or int(item.get("updated_ts") or 0) > int(existing.get("updated_ts") or 0):
            result[market] = item
    return result


def _match_model_managed_position_import(
    *,
    market: str,
    position: dict[str, Any],
    latest_bid_order: dict[str, Any] | None,
    exchange_bot_open_orders: list[dict[str, Any]],
    intents_by_id: dict[str, dict[str, Any]],
    ts_ms: int,
) -> dict[str, Any] | None:
    if latest_bid_order is None:
        return None
    intent_id = _as_optional_str(latest_bid_order.get("intent_id"))
    if intent_id is None:
        return None
    intent = intents_by_id.get(intent_id)
    if intent is None:
        return None
    meta = intent.get("meta")
    if not isinstance(meta, dict):
        return None
    submit_result = meta.get("submit_result")
    if not isinstance(submit_result, dict) or not bool(submit_result.get("accepted")):
        return None
    plan_payload = extract_model_exit_plan(meta)
    if plan_payload is None:
        return None
    created_ts = max(int(intent.get("ts_ms") or 0), 0) or int(ts_ms)
    position_record, risk_plan_record = build_model_derived_risk_records(
        market=market,
        base_currency=str(position.get("base_currency", "")).strip().upper(),
        base_amount=float(position.get("base_amount") or 0.0),
        avg_entry_price=float(position.get("avg_entry_price") or 0.0),
        plan_payload=plan_payload,
        created_ts=created_ts,
        updated_ts=int(ts_ms),
        intent_id=intent_id,
    )
    if position_record.base_amount <= 0 or position_record.avg_entry_price <= 0:
        return None
    exit_order_record: OrderRecord | None = None
    open_exit_order = next(
        (
            item
            for item in exchange_bot_open_orders
            if str(item.get("market") or "").strip().upper() == str(market).strip().upper()
            and str(item.get("side") or "").strip().lower() == "ask"
        ),
        None,
    )
    if open_exit_order is not None:
        exit_order_record = _order_record_from_payload(open_exit_order, ts_ms=ts_ms)
        if exit_order_record is not None:
            last_action_ts_ms = max(
                int(exit_order_record.updated_ts or 0),
                int(exit_order_record.created_ts or 0),
                int(ts_ms),
            )
            risk_plan_record = replace(
                risk_plan_record,
                state="EXITING",
                last_action_ts_ms=last_action_ts_ms,
                current_exit_order_uuid=exit_order_record.uuid,
                current_exit_order_identifier=exit_order_record.identifier,
            )
    return {
        "intent_id": intent_id,
        "order_uuid": _as_optional_str(latest_bid_order.get("uuid")),
        "position_record": position_record,
        "risk_plan_record": risk_plan_record,
        "exit_order_record": exit_order_record,
    }


def _match_model_managed_position_close(
    *,
    market: str,
    local_position: dict[str, Any],
    latest_done_ask_order: dict[str, Any] | None,
    active_live_plans: list[dict[str, Any]] | None,
    exchange_bot_open_orders: list[dict[str, Any]] | None,
) -> dict[str, Any] | None:
    if latest_done_ask_order is None:
        pass
    else:
        if int(latest_done_ask_order.get("updated_ts") or 0) >= int(local_position.get("updated_ts") or 0):
            return {
                "market": market,
                "order_uuid": _as_optional_str(latest_done_ask_order.get("uuid")),
                "order_identifier": _as_optional_str(latest_done_ask_order.get("identifier")),
                "close_mode": "done_ask_order",
            }

    if exchange_bot_open_orders:
        return None
    if not active_live_plans:
        return None

    def _plan_rank(item: dict[str, Any]) -> tuple[int, int, int, str]:
        state_value = str(item.get("state") or "").strip().upper()
        if state_value == "EXITING":
            state_rank = 2
        elif state_value == "TRIGGERED":
            state_rank = 1
        else:
            state_rank = 0
        return (
            state_rank,
            int(bool(_as_optional_str(item.get("current_exit_order_uuid")) or _as_optional_str(item.get("current_exit_order_identifier")))),
            int(item.get("updated_ts") or 0),
            str(item.get("plan_id") or ""),
        )

    selected_plan = max(active_live_plans, key=_plan_rank)
    selected_state = str(selected_plan.get("state") or "").strip().upper()
    if selected_state not in {"TRIGGERED", "EXITING"}:
        return None
    if _as_optional_str(selected_plan.get("plan_source")) != "model_alpha_v1":
        return None
    if (
        int(selected_plan.get("last_action_ts_ms") or 0) <= 0
        and int(selected_plan.get("replace_attempt") or 0) <= 0
        and not _as_optional_str(selected_plan.get("current_exit_order_uuid"))
        and not _as_optional_str(selected_plan.get("current_exit_order_identifier"))
    ):
        return None
    return {
        "market": market,
        "order_uuid": _as_optional_str(selected_plan.get("current_exit_order_uuid")),
        "order_identifier": _as_optional_str(selected_plan.get("current_exit_order_identifier")),
        "close_mode": "missing_on_exchange_after_exit_plan",
    }


def _build_ignored_dust_position_detail(
    *,
    position: dict[str, Any],
    fetch_market_chance: Callable[[str], Any] | None,
) -> dict[str, Any] | None:
    if fetch_market_chance is None:
        return None
    market = str(position.get("market") or "").strip().upper()
    if not market:
        return None
    base_amount = float(_as_optional_float(position.get("base_amount")) or 0.0)
    avg_entry_price = float(_as_optional_float(position.get("avg_entry_price")) or 0.0)
    if base_amount <= 0.0 or avg_entry_price <= 0.0:
        return None
    try:
        chance_payload = fetch_market_chance(market)
        min_total_quote = float(extract_min_total(chance_payload, side="ask", market=market))
    except Exception:
        return None
    reference_notional_quote = base_amount * avg_entry_price
    if reference_notional_quote + 1e-12 >= min_total_quote:
        return None
    return {
        "market": market,
        "base_currency": str(position.get("base_currency") or ""),
        "base_amount": base_amount,
        "avg_entry_price": avg_entry_price,
        "reference_price_source": "avg_entry_price",
        "reference_notional_quote": reference_notional_quote,
        "min_total_quote": min_total_quote,
    }


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


def _as_optional_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _write_resume_report(*, path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")
