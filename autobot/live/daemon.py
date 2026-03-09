"""Polling-based live sync daemon for restart-safe runtime operations."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import json
import queue
import threading
import time
from typing import Any

from autobot.upbit.ws import MyAssetEvent, MyOrderEvent, parse_private_event

from .order_state import normalize_order_state
from .reconcile import apply_cancel_actions, reconcile_exchange_snapshot, resume_risk_plans_after_reconcile
from .state_store import LiveStateStore, OrderLineageRecord, OrderRecord
from .ws_handlers import apply_private_ws_event


@dataclass(frozen=True)
class LiveDaemonSettings:
    bot_id: str
    identifier_prefix: str
    unknown_open_orders_policy: str
    unknown_positions_policy: str
    allow_cancel_external_orders: bool
    poll_interval_sec: int
    quote_currency: str = "KRW"
    startup_reconcile: bool = True
    default_risk_sl_pct: float = 2.0
    default_risk_tp_pct: float = 3.0
    default_risk_trailing_enabled: bool = False
    allow_cancel_external_cli: bool = False
    use_private_ws: bool = False
    use_executor_ws: bool = False
    duration_sec: int | None = None
    max_cycles: int | None = None

    def __post_init__(self) -> None:
        if self.use_private_ws and self.use_executor_ws:
            raise ValueError("use_private_ws and use_executor_ws cannot both be true")


def run_live_sync_daemon(
    *,
    store: LiveStateStore,
    client: Any,
    settings: LiveDaemonSettings,
) -> dict[str, Any]:
    started_ts_ms = int(time.time() * 1000)
    started_monotonic = time.monotonic()
    cycles = 0
    summary: dict[str, Any] = {
        "started_ts_ms": started_ts_ms,
        "ended_ts_ms": started_ts_ms,
        "cycles": 0,
        "halted": False,
        "halted_reasons": [],
        "last_report": None,
        "last_cancel_summary": None,
        "resume_report": None,
    }

    if settings.startup_reconcile:
        cycle_result = _run_sync_cycle(store=store, client=client, settings=settings, ts_ms=int(time.time() * 1000))
        cycles += 1
        summary["last_report"] = cycle_result["report"]
        summary["last_cancel_summary"] = cycle_result["cancel_summary"]
        if bool(cycle_result["report"].get("halted")):
            summary["halted"] = True
            summary["halted_reasons"] = list(cycle_result["report"].get("halted_reasons", []))
            summary["cycles"] = cycles
            summary["ended_ts_ms"] = int(time.time() * 1000)
            return summary
        resume_report = resume_risk_plans_after_reconcile(store=store, ts_ms=int(time.time() * 1000))
        summary["resume_report"] = resume_report
        if bool(resume_report.get("halted")):
            summary["halted"] = True
            summary["halted_reasons"] = ["RESUME_REVIEW_REQUIRED"]
            summary["cycles"] = cycles
            summary["ended_ts_ms"] = int(time.time() * 1000)
            return summary

    while True:
        if settings.max_cycles is not None and cycles >= settings.max_cycles:
            break
        if settings.duration_sec is not None and settings.duration_sec > 0:
            elapsed = time.monotonic() - started_monotonic
            if elapsed >= settings.duration_sec:
                break

        time.sleep(max(int(settings.poll_interval_sec), 1))
        cycle_result = _run_sync_cycle(store=store, client=client, settings=settings, ts_ms=int(time.time() * 1000))
        cycles += 1
        summary["last_report"] = cycle_result["report"]
        summary["last_cancel_summary"] = cycle_result["cancel_summary"]
        if bool(cycle_result["report"].get("halted")):
            summary["halted"] = True
            summary["halted_reasons"] = list(cycle_result["report"].get("halted_reasons", []))
            break

    summary["cycles"] = cycles
    summary["ended_ts_ms"] = int(time.time() * 1000)
    return summary


def run_live_sync_daemon_with_executor_events(
    *,
    store: LiveStateStore,
    client: Any,
    executor_gateway: Any,
    settings: LiveDaemonSettings,
) -> dict[str, Any]:
    started_ts_ms = int(time.time() * 1000)
    started_monotonic = time.monotonic()
    cycles = 0
    executor_events = 0
    executor_last_event_ts_ms: int | None = None
    executor_last_event_latency_ms: int | None = None
    next_poll_monotonic = time.monotonic()
    poll_interval_sec = max(int(settings.poll_interval_sec), 60)
    stream_errors: list[str] = []

    summary: dict[str, Any] = {
        "started_ts_ms": started_ts_ms,
        "ended_ts_ms": started_ts_ms,
        "cycles": 0,
        "executor_events": 0,
        "executor_last_event_ts_ms": None,
        "executor_last_event_latency_ms": None,
        "halted": False,
        "halted_reasons": [],
        "last_report": None,
        "last_cancel_summary": None,
        "resume_report": None,
        "stream_errors": [],
    }

    if settings.startup_reconcile:
        cycle_result = _run_sync_cycle(store=store, client=client, settings=settings, ts_ms=int(time.time() * 1000))
        cycles += 1
        summary["last_report"] = cycle_result["report"]
        summary["last_cancel_summary"] = cycle_result["cancel_summary"]
        if bool(cycle_result["report"].get("halted")):
            summary["halted"] = True
            summary["halted_reasons"] = list(cycle_result["report"].get("halted_reasons", []))
            summary["cycles"] = cycles
            summary["ended_ts_ms"] = int(time.time() * 1000)
            return summary
        resume_report = resume_risk_plans_after_reconcile(store=store, ts_ms=int(time.time() * 1000))
        summary["resume_report"] = resume_report
        if bool(resume_report.get("halted")):
            summary["halted"] = True
            summary["halted_reasons"] = ["RESUME_REVIEW_REQUIRED"]
            summary["cycles"] = cycles
            summary["ended_ts_ms"] = int(time.time() * 1000)
            return summary
        next_poll_monotonic = time.monotonic() + poll_interval_sec

    event_queue: queue.Queue[Any] = queue.Queue()
    stop_event = threading.Event()

    def _executor_pump() -> None:
        try:
            for executor_event in executor_gateway.stream_events():
                if stop_event.is_set():
                    break
                event_queue.put(executor_event)
        except Exception as exc:  # pragma: no cover - protective runtime path
            stream_errors.append(str(exc))
            event_queue.put(
                {
                    "event_type": "ERROR",
                    "ts_ms": int(time.time() * 1000),
                    "payload": {"message": str(exc)},
                }
            )

    executor_thread = threading.Thread(target=_executor_pump, name="executor-event-pump", daemon=True)
    executor_thread.start()
    try:
        while True:
            if settings.max_cycles is not None and cycles >= settings.max_cycles:
                break
            if settings.duration_sec is not None and settings.duration_sec > 0:
                elapsed = time.monotonic() - started_monotonic
                if elapsed >= settings.duration_sec:
                    break

            now_monotonic = time.monotonic()
            timeout_sec = max(min(next_poll_monotonic - now_monotonic, 1.0), 0.0)
            try:
                executor_event = event_queue.get(timeout=timeout_sec)
            except queue.Empty:
                executor_event = None

            if executor_event is not None:
                action = _apply_executor_event(
                    store=store,
                    event=executor_event,
                    bot_id=settings.bot_id,
                    identifier_prefix=settings.identifier_prefix,
                    quote_currency=settings.quote_currency,
                )
                executor_events += 1
                event_ts_ms = _event_ts_ms(executor_event)
                executor_last_event_ts_ms = event_ts_ms
                executor_last_event_latency_ms = max(int(time.time() * 1000) - int(event_ts_ms), 0)
                store.set_checkpoint(
                    name="last_executor_event",
                    payload={
                        "action": action,
                        "event_type": _event_type(executor_event),
                        "event_ts_ms": executor_last_event_ts_ms,
                        "latency_ms": executor_last_event_latency_ms,
                    },
                    ts_ms=int(time.time() * 1000),
                )

            if time.monotonic() >= next_poll_monotonic:
                cycle_result = _run_sync_cycle(store=store, client=client, settings=settings, ts_ms=int(time.time() * 1000))
                cycles += 1
                summary["last_report"] = cycle_result["report"]
                summary["last_cancel_summary"] = cycle_result["cancel_summary"]
                if bool(cycle_result["report"].get("halted")):
                    summary["halted"] = True
                    summary["halted_reasons"] = list(cycle_result["report"].get("halted_reasons", []))
                    break
                next_poll_monotonic = time.monotonic() + poll_interval_sec
    finally:
        stop_event.set()
        executor_thread.join(timeout=2.0)

    summary["cycles"] = cycles
    summary["executor_events"] = executor_events
    summary["executor_last_event_ts_ms"] = executor_last_event_ts_ms
    summary["executor_last_event_latency_ms"] = executor_last_event_latency_ms
    summary["ended_ts_ms"] = int(time.time() * 1000)
    summary["stream_errors"] = list(stream_errors)
    return summary


async def run_live_sync_daemon_with_private_ws(
    *,
    store: LiveStateStore,
    client: Any,
    ws_client: Any,
    settings: LiveDaemonSettings,
) -> dict[str, Any]:
    started_ts_ms = int(time.time() * 1000)
    started_monotonic = time.monotonic()
    cycles = 0
    ws_events = 0
    ws_last_event_ts_ms: int | None = None
    ws_last_event_latency_ms: int | None = None
    next_poll_monotonic = time.monotonic()
    poll_interval_sec = max(int(settings.poll_interval_sec), 60)

    summary: dict[str, Any] = {
        "started_ts_ms": started_ts_ms,
        "ended_ts_ms": started_ts_ms,
        "cycles": 0,
        "ws_events": 0,
        "ws_last_event_ts_ms": None,
        "ws_last_event_latency_ms": None,
        "halted": False,
        "halted_reasons": [],
        "last_report": None,
        "last_cancel_summary": None,
        "resume_report": None,
        "ws_stats": {},
    }

    if settings.startup_reconcile:
        cycle_result = _run_sync_cycle(store=store, client=client, settings=settings, ts_ms=int(time.time() * 1000))
        cycles += 1
        summary["last_report"] = cycle_result["report"]
        summary["last_cancel_summary"] = cycle_result["cancel_summary"]
        if bool(cycle_result["report"].get("halted")):
            summary["halted"] = True
            summary["halted_reasons"] = list(cycle_result["report"].get("halted_reasons", []))
            summary["cycles"] = cycles
            summary["ended_ts_ms"] = int(time.time() * 1000)
            summary["ws_stats"] = ws_client.stats if hasattr(ws_client, "stats") else {}
            return summary
        resume_report = resume_risk_plans_after_reconcile(store=store, ts_ms=int(time.time() * 1000))
        summary["resume_report"] = resume_report
        if bool(resume_report.get("halted")):
            summary["halted"] = True
            summary["halted_reasons"] = ["RESUME_REVIEW_REQUIRED"]
            summary["cycles"] = cycles
            summary["ended_ts_ms"] = int(time.time() * 1000)
            summary["ws_stats"] = ws_client.stats if hasattr(ws_client, "stats") else {}
            return summary
        next_poll_monotonic = time.monotonic() + poll_interval_sec

    event_queue: asyncio.Queue[MyOrderEvent | MyAssetEvent] = asyncio.Queue()
    stop_event = asyncio.Event()

    async def _ws_pump() -> None:
        async for ws_event in ws_client.stream_private(channels=("myOrder", "myAsset")):
            if stop_event.is_set():
                break
            await event_queue.put(ws_event)

    ws_task = asyncio.create_task(_ws_pump())
    try:
        while True:
            if settings.max_cycles is not None and cycles >= settings.max_cycles:
                break
            if settings.duration_sec is not None and settings.duration_sec > 0:
                elapsed = time.monotonic() - started_monotonic
                if elapsed >= settings.duration_sec:
                    break

            now_monotonic = time.monotonic()
            timeout_sec = max(min(next_poll_monotonic - now_monotonic, 1.0), 0.0)
            try:
                ws_event = await asyncio.wait_for(event_queue.get(), timeout=timeout_sec)
            except asyncio.TimeoutError:
                ws_event = None

            if ws_event is not None:
                action = apply_private_ws_event(
                    store=store,
                    event=ws_event,
                    bot_id=settings.bot_id,
                    identifier_prefix=settings.identifier_prefix,
                    quote_currency=settings.quote_currency,
                )
                ws_events += 1
                ws_last_event_ts_ms = int(ws_event.ts_ms)
                ws_last_event_latency_ms = max(int(time.time() * 1000) - int(ws_event.ts_ms), 0)
                store.set_checkpoint(
                    name="last_ws_event",
                    payload={
                        "action": action,
                        "event_type": ws_event.stream_type,
                        "event_ts_ms": ws_last_event_ts_ms,
                        "latency_ms": ws_last_event_latency_ms,
                    },
                    ts_ms=int(time.time() * 1000),
                )

            if time.monotonic() >= next_poll_monotonic:
                cycle_result = _run_sync_cycle(store=store, client=client, settings=settings, ts_ms=int(time.time() * 1000))
                cycles += 1
                summary["last_report"] = cycle_result["report"]
                summary["last_cancel_summary"] = cycle_result["cancel_summary"]
                if bool(cycle_result["report"].get("halted")):
                    summary["halted"] = True
                    summary["halted_reasons"] = list(cycle_result["report"].get("halted_reasons", []))
                    break
                next_poll_monotonic = time.monotonic() + poll_interval_sec
    finally:
        stop_event.set()
        ws_task.cancel()
        await asyncio.gather(ws_task, return_exceptions=True)

    summary["cycles"] = cycles
    summary["ws_events"] = ws_events
    summary["ws_last_event_ts_ms"] = ws_last_event_ts_ms
    summary["ws_last_event_latency_ms"] = ws_last_event_latency_ms
    summary["ended_ts_ms"] = int(time.time() * 1000)
    summary["ws_stats"] = ws_client.stats if hasattr(ws_client, "stats") else {}
    return summary


def _apply_executor_event(
    *,
    store: LiveStateStore,
    event: Any,
    bot_id: str,
    identifier_prefix: str,
    quote_currency: str,
) -> dict[str, Any]:
    event_type = _event_type(event)
    payload = _event_payload(event)
    ts_ms = _event_ts_ms(event)
    normalized_type = event_type.strip().upper()
    event_name = str(payload.get("event_name", "")).strip().upper()

    if event_name == "ORDER_TIMEOUT":
        return {"type": "executor_order_timeout", "payload": payload}
    if event_name == "ORDER_REPLACED":
        prev_uuid = _as_optional_str(payload.get("prev_uuid"))
        prev_identifier = _as_optional_str(payload.get("prev_identifier"))
        new_uuid = _as_optional_str(payload.get("new_uuid"))
        new_identifier = _as_optional_str(payload.get("new_identifier"))
        replace_seq = _as_optional_int(payload.get("replace_attempt_count")) or 0
        previous_order = None
        if store is not None:
            if prev_uuid:
                previous_order = store.order_by_uuid(uuid=prev_uuid)
            if previous_order is None and prev_identifier:
                previous_order = store.order_by_identifier(identifier=prev_identifier)
            try:
                store.append_order_lineage(
                    OrderLineageRecord(
                        ts_ms=ts_ms,
                        event_source="executor_ws",
                        intent_id=_as_optional_str(payload.get("intent_id"))
                        or (_as_optional_str(previous_order.get("intent_id")) if previous_order else None),
                        prev_uuid=prev_uuid,
                        prev_identifier=prev_identifier,
                        new_uuid=new_uuid,
                        new_identifier=new_identifier,
                        replace_seq=replace_seq,
                    )
                )
            except Exception:
                pass
            if previous_order is not None and prev_uuid:
                try:
                    store.upsert_order(
                        OrderRecord(
                            uuid=prev_uuid,
                            identifier=prev_identifier or _as_optional_str(previous_order.get("identifier")),
                            market=str(previous_order.get("market") or ""),
                            side=_as_optional_str(previous_order.get("side")),
                            ord_type=_as_optional_str(previous_order.get("ord_type")),
                            price=previous_order.get("price"),
                            volume_req=previous_order.get("volume_req"),
                            volume_filled=float(previous_order.get("volume_filled") or 0.0),
                            state="cancel",
                            created_ts=int(previous_order.get("created_ts") or ts_ms),
                            updated_ts=ts_ms,
                            intent_id=_as_optional_str(previous_order.get("intent_id")),
                            tp_sl_link=_as_optional_str(previous_order.get("tp_sl_link")),
                            local_state="CANCELLED",
                            raw_exchange_state="cancel",
                            last_event_name="ORDER_REPLACED",
                            event_source="executor_ws",
                            replace_seq=int(previous_order.get("replace_seq") or 0),
                            root_order_uuid=_as_optional_str(previous_order.get("root_order_uuid")) or prev_uuid,
                            prev_order_uuid=_as_optional_str(previous_order.get("prev_order_uuid")),
                            prev_order_identifier=_as_optional_str(previous_order.get("prev_order_identifier")),
                        )
                    )
                except Exception:
                    pass
            if new_uuid:
                normalized = normalize_order_state(exchange_state="wait", event_name="ORDER_REPLACED")
                try:
                    store.upsert_order(
                        OrderRecord(
                            uuid=new_uuid,
                            identifier=new_identifier,
                            market=str((previous_order or {}).get("market") or payload.get("market") or "").strip().upper(),
                            side=_as_optional_str((previous_order or {}).get("side") or payload.get("side")),
                            ord_type=_as_optional_str((previous_order or {}).get("ord_type") or payload.get("ord_type")),
                            price=_as_optional_float(payload.get("new_price_str"))
                            or _as_optional_float(payload.get("price"))
                            or _as_optional_float((previous_order or {}).get("price")),
                            volume_req=_resolve_replaced_volume(payload=payload, previous_order=previous_order),
                            volume_filled=0.0,
                            state="wait",
                            created_ts=ts_ms,
                            updated_ts=ts_ms,
                            intent_id=_as_optional_str(payload.get("intent_id"))
                            or (_as_optional_str(previous_order.get("intent_id")) if previous_order else None),
                            tp_sl_link=_as_optional_str(previous_order.get("tp_sl_link")) if previous_order else None,
                            local_state=normalized.local_state,
                            raw_exchange_state=normalized.exchange_state,
                            last_event_name=normalized.event_name,
                            event_source="executor_ws",
                            replace_seq=replace_seq,
                            root_order_uuid=_as_optional_str(previous_order.get("root_order_uuid")) if previous_order else prev_uuid or new_uuid,
                            prev_order_uuid=prev_uuid,
                            prev_order_identifier=prev_identifier,
                        )
                    )
                except Exception:
                    pass
        if hasattr(store, "set_checkpoint"):
            try:
                store.set_checkpoint(
                    name="last_replace_chain",
                    payload={
                        "prev_uuid": prev_uuid,
                        "prev_identifier": prev_identifier,
                        "new_uuid": new_uuid,
                        "new_identifier": new_identifier,
                        "replace_attempt_count": replace_seq,
                    },
                    ts_ms=ts_ms,
                )
            except Exception:
                pass
        return {"type": "executor_order_replaced", "payload": payload}

    if normalized_type in {"ORDER_UPDATE", "FILL"} or event_name in {
        "ORDER_ACCEPTED",
        "ORDER_STATE",
        "CANCEL_RESULT",
        "FILL",
    }:
        ws_event = _to_private_ws_event(payload=payload, stream_type="myOrder", ts_ms=ts_ms)
        if isinstance(ws_event, MyOrderEvent):
            return apply_private_ws_event(
                store=store,
                event=ws_event,
                bot_id=bot_id,
                identifier_prefix=identifier_prefix,
                quote_currency=quote_currency,
            )
        return {"type": "executor_order_skip", "reason": "invalid_payload", "event_type": normalized_type}

    if normalized_type == "ASSET":
        ws_event = _to_private_ws_event(payload=payload, stream_type="myAsset", ts_ms=ts_ms)
        if isinstance(ws_event, MyAssetEvent):
            return apply_private_ws_event(
                store=store,
                event=ws_event,
                bot_id=bot_id,
                identifier_prefix=identifier_prefix,
                quote_currency=quote_currency,
            )
        return {"type": "executor_asset_skip", "reason": "invalid_payload"}

    if normalized_type == "HEALTH":
        return {"type": "executor_health", "payload": payload}

    if normalized_type == "ERROR":
        return {"type": "executor_error", "payload": payload}

    return {"type": "executor_event_ignored", "event_type": normalized_type}


def _event_type(event: Any) -> str:
    if isinstance(event, dict):
        return str(event.get("event_type", "EVENT_UNSPECIFIED"))
    return str(getattr(event, "event_type", "EVENT_UNSPECIFIED"))


def _event_ts_ms(event: Any) -> int:
    if isinstance(event, dict):
        value = event.get("ts_ms")
    else:
        value = getattr(event, "ts_ms", None)
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(time.time() * 1000)


def _event_payload(event: Any) -> dict[str, Any]:
    if isinstance(event, dict):
        payload = event.get("payload")
        payload_json = event.get("payload_json")
    else:
        payload = getattr(event, "payload", None)
        payload_json = getattr(event, "payload_json", None)

    if isinstance(payload, dict):
        return payload
    if isinstance(payload_json, str) and payload_json.strip():
        try:
            parsed = json.loads(payload_json)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return parsed
    return {}


def _to_private_ws_event(*, payload: dict[str, Any], stream_type: str, ts_ms: int) -> MyOrderEvent | MyAssetEvent | None:
    message = dict(payload)
    if "type" not in message and "ty" not in message:
        message["type"] = stream_type
    if "timestamp" not in message and "tms" not in message:
        message["timestamp"] = int(ts_ms)

    parsed = parse_private_event(message)
    if isinstance(parsed, (MyOrderEvent, MyAssetEvent)):
        return parsed

    if stream_type == "myOrder":
        market = _as_optional_str(message.get("market") or message.get("code") or message.get("cd"))
        return MyOrderEvent(
            ts_ms=int(ts_ms),
            uuid=_as_optional_str(message.get("uuid") or message.get("upbit_uuid") or message.get("uid")),
            identifier=_as_optional_str(message.get("identifier") or message.get("i")),
            market=market.upper() if market else None,
            side=_as_optional_str(message.get("side") or message.get("sd")),
            ord_type=_as_optional_str(message.get("ord_type") or message.get("ot")),
            state=_as_optional_str(message.get("state") or message.get("status") or message.get("st")),
            price=_as_optional_float(message.get("price") or message.get("p")),
            volume=_as_optional_float(message.get("volume") or message.get("v")),
            executed_volume=_as_optional_float(
                message.get("executed_volume") or message.get("volume_filled") or message.get("ev")
            ),
            stream_type="myOrder",
            raw=message,
        )

    currency = _as_optional_str(message.get("currency") or message.get("cy"))
    return MyAssetEvent(
        ts_ms=int(ts_ms),
        currency=currency.upper() if currency else None,
        balance=_as_optional_float(message.get("balance") or message.get("bl")),
        locked=_as_optional_float(message.get("locked") or message.get("lk")),
        avg_buy_price=_as_optional_float(message.get("avg_buy_price") or message.get("abp")),
        stream_type="myAsset",
        raw=message,
    )


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


def _resolve_replaced_volume(*, payload: dict[str, Any], previous_order: dict[str, Any] | None) -> float | None:
    raw = _as_optional_str(payload.get("new_volume_str")) or _as_optional_str(payload.get("volume"))
    if raw and raw.lower() == "remain_only":
        if previous_order is None:
            return None
        requested = _as_optional_float(previous_order.get("volume_req")) or 0.0
        filled = _as_optional_float(previous_order.get("volume_filled")) or 0.0
        remain = max(requested - filled, 0.0)
        return remain if remain > 0.0 else None
    parsed = _as_optional_float(raw)
    if parsed is not None:
        return parsed
    if previous_order is None:
        return None
    return _as_optional_float(previous_order.get("volume_req"))


def _run_sync_cycle(
    *,
    store: LiveStateStore,
    client: Any,
    settings: LiveDaemonSettings,
    ts_ms: int,
) -> dict[str, Any]:
    accounts = client.accounts()
    open_orders = client.open_orders(states=("wait", "watch"))
    report = reconcile_exchange_snapshot(
        store=store,
        bot_id=settings.bot_id,
        identifier_prefix=settings.identifier_prefix,
        accounts_payload=accounts,
        open_orders_payload=open_orders,
        fetch_order_detail=lambda uuid, identifier: client.order(uuid=uuid, identifier=identifier),
        unknown_open_orders_policy=settings.unknown_open_orders_policy,  # type: ignore[arg-type]
        unknown_positions_policy=settings.unknown_positions_policy,  # type: ignore[arg-type]
        allow_cancel_external_orders=bool(settings.allow_cancel_external_orders),
        default_risk_sl_pct=float(settings.default_risk_sl_pct),
        default_risk_tp_pct=float(settings.default_risk_tp_pct),
        default_risk_trailing_enabled=bool(settings.default_risk_trailing_enabled),
        quote_currency=settings.quote_currency,
        dry_run=False,
        ts_ms=ts_ms,
    )
    cancel_summary = apply_cancel_actions(
        report=report,
        cancel_order=lambda uuid, identifier: client.cancel_order(uuid=uuid, identifier=identifier),
        apply=True,
        allow_cancel_external_cli=bool(settings.allow_cancel_external_cli),
        allow_cancel_external_config=bool(settings.allow_cancel_external_orders),
    )
    checkpoint_payload = {
        "report": report,
        "cancel_summary": cancel_summary,
    }
    store.set_checkpoint(name="last_sync", payload=checkpoint_payload, ts_ms=ts_ms)
    return {
        "report": report,
        "cancel_summary": cancel_summary,
    }
