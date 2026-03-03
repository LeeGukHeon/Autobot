"""Polling-based live sync daemon for restart-safe runtime operations."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import time
from typing import Any

from autobot.upbit.ws import MyAssetEvent, MyOrderEvent

from .reconcile import apply_cancel_actions, reconcile_exchange_snapshot
from .state_store import LiveStateStore
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
    duration_sec: int | None = None
    max_cycles: int | None = None


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
