"""Polling-based live sync daemon for restart-safe runtime operations."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import json
from pathlib import Path
import queue
import threading
import time
from typing import Any

from autobot.upbit.ws import MyAssetEvent, MyOrderEvent, parse_private_event
from autobot.upbit.exceptions import UpbitError

from .breakers import (
    ACTION_FULL_KILL_SWITCH,
    ACTION_HALT_NEW_INTENTS,
    active_breaker_decision,
    arm_breaker,
    breaker_status,
    classify_executor_reject_reason,
    classify_identifier_collision,
    classify_upbit_exception,
    clear_breaker,
    evaluate_cycle_contracts,
    record_counter_failure,
    reset_counter,
    should_cancel_bot_orders,
)
from .model_handoff import (
    build_live_runtime_sync_status,
    load_ws_public_runtime_contract,
    resolve_live_runtime_model_contract,
)
from .order_state import normalize_order_state
from .reconcile import apply_cancel_actions, reconcile_exchange_snapshot, resume_risk_plans_after_reconcile
from .small_account import build_small_account_runtime_report
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
    breaker_cancel_reject_limit: int = 3
    breaker_replace_reject_limit: int = 3
    breaker_rate_limit_error_limit: int = 3
    breaker_auth_error_limit: int = 2
    breaker_nonce_error_limit: int = 2
    small_account_canary_enabled: bool = False
    small_account_max_positions: int = 1
    small_account_max_open_orders_per_market: int = 1
    registry_root: str = "models/registry"
    runtime_model_ref_source: str = "champion_v4"
    runtime_model_family: str = "train_v4_crypto_cs"
    ws_public_raw_root: str = "data/raw_ws/upbit/public"
    ws_public_meta_dir: str = "data/raw_ws/upbit/_meta"
    ws_public_stale_threshold_sec: int = 180
    micro_aggregate_report_path: str = "data/parquet/micro_v1/_meta/aggregate_report.json"

    def __post_init__(self) -> None:
        if self.use_private_ws and self.use_executor_ws:
            raise ValueError("use_private_ws and use_executor_ws cannot both be true")
        if int(self.small_account_max_positions) <= 0:
            raise ValueError("small_account_max_positions must be positive")
        if int(self.small_account_max_open_orders_per_market) <= 0:
            raise ValueError("small_account_max_open_orders_per_market must be positive")
        if not str(self.runtime_model_ref_source).strip():
            raise ValueError("runtime_model_ref_source must not be blank")
        if not str(self.runtime_model_family).strip():
            raise ValueError("runtime_model_family must not be blank")
        if int(self.ws_public_stale_threshold_sec) <= 0:
            raise ValueError("ws_public_stale_threshold_sec must be positive")


def _runtime_model_binding_after_resume(
    *,
    store: LiveStateStore,
    settings: LiveDaemonSettings,
    ts_ms: int,
) -> dict[str, Any]:
    previous_contract = store.runtime_contract() or {}
    current_contract = resolve_live_runtime_model_contract(
        registry_root=Path(str(settings.registry_root)),
        model_ref=str(settings.runtime_model_ref_source),
        model_family=str(settings.runtime_model_family),
        ts_ms=ts_ms,
    )
    previous_pinned_run_id = str(previous_contract.get("live_runtime_model_run_id", "")).strip()
    champion_pointer_run_id = str(current_contract.get("champion_pointer_run_id", "")).strip()
    pinned_contract = dict(current_contract)
    pinned_contract.update(
        {
            "previous_pinned_run_id": previous_pinned_run_id or None,
            "promote_happened_while_down": bool(
                previous_pinned_run_id and champion_pointer_run_id and previous_pinned_run_id != champion_pointer_run_id
            ),
            "bound_after_resume_ts_ms": int(ts_ms),
        }
    )
    store.set_runtime_contract(payload=pinned_contract, ts_ms=ts_ms)
    ws_public_contract = load_ws_public_runtime_contract(
        meta_dir=Path(str(settings.ws_public_meta_dir)),
        raw_root=Path(str(settings.ws_public_raw_root)),
        stale_threshold_sec=int(settings.ws_public_stale_threshold_sec),
        micro_aggregate_report_path=Path(str(settings.micro_aggregate_report_path)),
        ts_ms=ts_ms,
    )
    store.set_ws_public_contract(payload=ws_public_contract, ts_ms=ts_ms)
    runtime_status = build_live_runtime_sync_status(
        pinned_contract=pinned_contract,
        current_contract=current_contract,
        ws_public_contract=ws_public_contract,
    )
    if bool(runtime_status.get("ws_public_stale")):
        arm_breaker(
            store,
            reason_codes=["WS_PUBLIC_STALE"],
            source="ws_public",
            ts_ms=ts_ms,
            action=ACTION_HALT_NEW_INTENTS,
            details=runtime_status,
        )
    store.set_live_runtime_health(payload=runtime_status, ts_ms=ts_ms)
    return runtime_status


def _refresh_runtime_contract_health(
    *,
    store: LiveStateStore,
    settings: LiveDaemonSettings,
    ts_ms: int,
) -> dict[str, Any]:
    try:
        current_contract = resolve_live_runtime_model_contract(
            registry_root=Path(str(settings.registry_root)),
            model_ref=str(settings.runtime_model_ref_source),
            model_family=str(settings.runtime_model_family),
            ts_ms=ts_ms,
        )
    except Exception as exc:
        breaker_report = arm_breaker(
            store,
            reason_codes=["MODEL_POINTER_UNRESOLVED"],
            source="live_model_handoff",
            ts_ms=ts_ms,
            action=ACTION_HALT_NEW_INTENTS,
            details={"error": str(exc)},
        )
        runtime_status = {
            "live_runtime_model_run_id": str((store.runtime_contract() or {}).get("live_runtime_model_run_id", "")).strip() or None,
            "champion_pointer_run_id": None,
            "current_resolved_model_run_id": None,
            "model_pointer_divergence": False,
            "model_pointer_divergence_reason": "MODEL_POINTER_UNRESOLVED",
            "previous_pinned_run_id": None,
            "promote_happened_while_down": False,
            "ws_public_last_checkpoint_ts_ms": None,
            "ws_public_staleness_sec": None,
            "ws_public_stale": False,
            "ws_public_last_checkpoint_source": None,
            "ws_public_run_id": None,
            "ws_public_validate_run_id": None,
            "micro_aggregate_run_id": None,
            "pinned_contract": store.runtime_contract() or {},
            "current_contract": {},
            "ws_public_contract": store.ws_public_contract() or {},
            "breaker_report": breaker_report,
        }
        store.set_live_runtime_health(payload=runtime_status, ts_ms=ts_ms)
        return runtime_status

    ws_public_contract = load_ws_public_runtime_contract(
        meta_dir=Path(str(settings.ws_public_meta_dir)),
        raw_root=Path(str(settings.ws_public_raw_root)),
        stale_threshold_sec=int(settings.ws_public_stale_threshold_sec),
        micro_aggregate_report_path=Path(str(settings.micro_aggregate_report_path)),
        ts_ms=ts_ms,
    )
    store.set_ws_public_contract(payload=ws_public_contract, ts_ms=ts_ms)
    pinned_contract = store.runtime_contract() or current_contract
    runtime_status = build_live_runtime_sync_status(
        pinned_contract=pinned_contract,
        current_contract=current_contract,
        ws_public_contract=ws_public_contract,
    )
    if bool(runtime_status.get("model_pointer_divergence")):
        arm_breaker(
            store,
            reason_codes=["MODEL_POINTER_DIVERGENCE"],
            source="live_model_handoff",
            ts_ms=ts_ms,
            action=ACTION_HALT_NEW_INTENTS,
            details=runtime_status,
        )
    if bool(runtime_status.get("ws_public_stale")):
        arm_breaker(
            store,
            reason_codes=["WS_PUBLIC_STALE"],
            source="ws_public",
            ts_ms=ts_ms,
            action=ACTION_HALT_NEW_INTENTS,
            details=runtime_status,
        )
    store.set_live_runtime_health(payload=runtime_status, ts_ms=ts_ms)
    return runtime_status


def _apply_runtime_status_to_summary(summary: dict[str, Any], runtime_status: dict[str, Any] | None) -> None:
    payload = dict(runtime_status or {})
    summary["runtime_handoff"] = payload
    summary["live_runtime_model_run_id"] = payload.get("live_runtime_model_run_id")
    summary["champion_pointer_run_id"] = payload.get("champion_pointer_run_id")
    summary["ws_public_last_checkpoint_ts_ms"] = payload.get("ws_public_last_checkpoint_ts_ms")
    summary["ws_public_staleness_sec"] = payload.get("ws_public_staleness_sec")
    summary["model_pointer_divergence"] = bool(payload.get("model_pointer_divergence", False))


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
        "last_breaker_cancel_summary": None,
        "resume_report": None,
        "small_account_report": None,
        "breaker_report": breaker_status(store),
        "last_sync_error": None,
        "runtime_handoff": None,
        "live_runtime_model_run_id": None,
        "champion_pointer_run_id": None,
        "ws_public_last_checkpoint_ts_ms": None,
        "ws_public_staleness_sec": None,
        "model_pointer_divergence": False,
    }

    if settings.startup_reconcile:
        cycle_result = _run_sync_cycle_with_breakers(
            store=store,
            client=client,
            settings=settings,
            ts_ms=int(time.time() * 1000),
        )
        cycles += 1
        summary["last_report"] = cycle_result["report"]
        summary["last_cancel_summary"] = cycle_result["cancel_summary"]
        summary["breaker_report"] = cycle_result.get("breaker_report")
        summary["last_sync_error"] = cycle_result.get("sync_error")
        summary["small_account_report"] = cycle_result.get("small_account_report")
        breaker_cancel_summary = _maybe_enforce_breaker(
            store=store,
            client=client,
            settings=settings,
            report=cycle_result["report"],
            prior_cancel_summary=cycle_result["cancel_summary"],
            ts_ms=int(time.time() * 1000),
        )
        summary["last_breaker_cancel_summary"] = breaker_cancel_summary
        if bool(cycle_result["report"].get("halted")) or bool(active_breaker_decision(store).active):
            summary["halted"] = True
            summary["halted_reasons"] = list(active_breaker_decision(store).reason_codes or cycle_result["report"].get("halted_reasons", []))
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
        runtime_status = _runtime_model_binding_after_resume(
            store=store,
            settings=settings,
            ts_ms=int(time.time() * 1000),
        )
        _apply_runtime_status_to_summary(summary, runtime_status)
        summary["breaker_report"] = breaker_status(store)
        if bool(active_breaker_decision(store).active):
            summary["halted"] = True
            summary["halted_reasons"] = list(active_breaker_decision(store).reason_codes)
            summary["cycles"] = cycles
            summary["ended_ts_ms"] = int(time.time() * 1000)
            return summary
    else:
        runtime_status = _runtime_model_binding_after_resume(
            store=store,
            settings=settings,
            ts_ms=int(time.time() * 1000),
        )
        _apply_runtime_status_to_summary(summary, runtime_status)
        summary["breaker_report"] = breaker_status(store)
        if bool(active_breaker_decision(store).active):
            summary["halted"] = True
            summary["halted_reasons"] = list(active_breaker_decision(store).reason_codes)
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
        cycle_result = _run_sync_cycle_with_breakers(
            store=store,
            client=client,
            settings=settings,
            ts_ms=int(time.time() * 1000),
        )
        cycles += 1
        summary["last_report"] = cycle_result["report"]
        summary["last_cancel_summary"] = cycle_result["cancel_summary"]
        summary["breaker_report"] = cycle_result.get("breaker_report")
        summary["last_sync_error"] = cycle_result.get("sync_error")
        summary["small_account_report"] = cycle_result.get("small_account_report")
        _apply_runtime_status_to_summary(summary, cycle_result.get("runtime_handoff"))
        summary["last_breaker_cancel_summary"] = _maybe_enforce_breaker(
            store=store,
            client=client,
            settings=settings,
            report=cycle_result["report"],
            prior_cancel_summary=cycle_result["cancel_summary"],
            ts_ms=int(time.time() * 1000),
        )
        if bool(cycle_result["report"].get("halted")) or bool(active_breaker_decision(store).active):
            summary["halted"] = True
            summary["halted_reasons"] = list(active_breaker_decision(store).reason_codes or cycle_result["report"].get("halted_reasons", []))
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
        "last_breaker_cancel_summary": None,
        "resume_report": None,
        "stream_errors": [],
        "small_account_report": None,
        "breaker_report": breaker_status(store),
        "last_sync_error": None,
        "runtime_handoff": None,
        "live_runtime_model_run_id": None,
        "champion_pointer_run_id": None,
        "ws_public_last_checkpoint_ts_ms": None,
        "ws_public_staleness_sec": None,
        "model_pointer_divergence": False,
    }

    if settings.startup_reconcile:
        cycle_result = _run_sync_cycle_with_breakers(
            store=store,
            client=client,
            settings=settings,
            ts_ms=int(time.time() * 1000),
        )
        cycles += 1
        summary["last_report"] = cycle_result["report"]
        summary["last_cancel_summary"] = cycle_result["cancel_summary"]
        summary["breaker_report"] = cycle_result.get("breaker_report")
        summary["last_sync_error"] = cycle_result.get("sync_error")
        summary["small_account_report"] = cycle_result.get("small_account_report")
        summary["last_breaker_cancel_summary"] = _maybe_enforce_breaker(
            store=store,
            client=client,
            settings=settings,
            report=cycle_result["report"],
            prior_cancel_summary=cycle_result["cancel_summary"],
            ts_ms=int(time.time() * 1000),
        )
        if bool(cycle_result["report"].get("halted")) or bool(active_breaker_decision(store).active):
            summary["halted"] = True
            summary["halted_reasons"] = list(active_breaker_decision(store).reason_codes or cycle_result["report"].get("halted_reasons", []))
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
        runtime_status = _runtime_model_binding_after_resume(
            store=store,
            settings=settings,
            ts_ms=int(time.time() * 1000),
        )
        _apply_runtime_status_to_summary(summary, runtime_status)
        summary["breaker_report"] = breaker_status(store)
        if bool(active_breaker_decision(store).active):
            summary["halted"] = True
            summary["halted_reasons"] = list(active_breaker_decision(store).reason_codes)
            summary["cycles"] = cycles
            summary["ended_ts_ms"] = int(time.time() * 1000)
            return summary
        next_poll_monotonic = time.monotonic() + poll_interval_sec
    else:
        runtime_status = _runtime_model_binding_after_resume(
            store=store,
            settings=settings,
            ts_ms=int(time.time() * 1000),
        )
        _apply_runtime_status_to_summary(summary, runtime_status)
        summary["breaker_report"] = breaker_status(store)
        if bool(active_breaker_decision(store).active):
            summary["halted"] = True
            summary["halted_reasons"] = list(active_breaker_decision(store).reason_codes)
            summary["ended_ts_ms"] = int(time.time() * 1000)
            return summary

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
                action = _apply_executor_event_with_breakers(
                    store=store,
                    event=executor_event,
                    bot_id=settings.bot_id,
                    identifier_prefix=settings.identifier_prefix,
                    quote_currency=settings.quote_currency,
                    settings=settings,
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
                summary["breaker_report"] = breaker_status(store)

            if not executor_thread.is_alive() and event_queue.empty() and not stop_event.is_set():
                summary["breaker_report"] = arm_breaker(
                    store,
                    reason_codes=["STALE_EXECUTOR_STREAM"],
                    source="executor_stream",
                    ts_ms=int(time.time() * 1000),
                    details={"stream_errors": list(stream_errors)},
                )
                summary["halted"] = True
                summary["halted_reasons"] = list(active_breaker_decision(store).reason_codes)
                break

            if time.monotonic() >= next_poll_monotonic:
                cycle_result = _run_sync_cycle_with_breakers(
                    store=store,
                    client=client,
                    settings=settings,
                    ts_ms=int(time.time() * 1000),
                )
                cycles += 1
                summary["last_report"] = cycle_result["report"]
                summary["last_cancel_summary"] = cycle_result["cancel_summary"]
                summary["breaker_report"] = cycle_result.get("breaker_report")
                summary["last_sync_error"] = cycle_result.get("sync_error")
                summary["small_account_report"] = cycle_result.get("small_account_report")
                _apply_runtime_status_to_summary(summary, cycle_result.get("runtime_handoff"))
                summary["last_breaker_cancel_summary"] = _maybe_enforce_breaker(
                    store=store,
                    client=client,
                    settings=settings,
                    report=cycle_result["report"],
                    prior_cancel_summary=cycle_result["cancel_summary"],
                    ts_ms=int(time.time() * 1000),
                )
                if bool(cycle_result["report"].get("halted")) or bool(active_breaker_decision(store).active):
                    summary["halted"] = True
                    summary["halted_reasons"] = list(active_breaker_decision(store).reason_codes or cycle_result["report"].get("halted_reasons", []))
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
        "last_breaker_cancel_summary": None,
        "resume_report": None,
        "ws_stats": {},
        "small_account_report": None,
        "breaker_report": breaker_status(store),
        "last_sync_error": None,
        "runtime_handoff": None,
        "live_runtime_model_run_id": None,
        "champion_pointer_run_id": None,
        "ws_public_last_checkpoint_ts_ms": None,
        "ws_public_staleness_sec": None,
        "model_pointer_divergence": False,
    }

    if settings.startup_reconcile:
        cycle_result = _run_sync_cycle_with_breakers(
            store=store,
            client=client,
            settings=settings,
            ts_ms=int(time.time() * 1000),
        )
        cycles += 1
        summary["last_report"] = cycle_result["report"]
        summary["last_cancel_summary"] = cycle_result["cancel_summary"]
        summary["breaker_report"] = cycle_result.get("breaker_report")
        summary["last_sync_error"] = cycle_result.get("sync_error")
        summary["small_account_report"] = cycle_result.get("small_account_report")
        summary["last_breaker_cancel_summary"] = _maybe_enforce_breaker(
            store=store,
            client=client,
            settings=settings,
            report=cycle_result["report"],
            prior_cancel_summary=cycle_result["cancel_summary"],
            ts_ms=int(time.time() * 1000),
        )
        if bool(cycle_result["report"].get("halted")) or bool(active_breaker_decision(store).active):
            summary["halted"] = True
            summary["halted_reasons"] = list(active_breaker_decision(store).reason_codes or cycle_result["report"].get("halted_reasons", []))
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
        runtime_status = _runtime_model_binding_after_resume(
            store=store,
            settings=settings,
            ts_ms=int(time.time() * 1000),
        )
        _apply_runtime_status_to_summary(summary, runtime_status)
        summary["breaker_report"] = breaker_status(store)
        if bool(active_breaker_decision(store).active):
            summary["halted"] = True
            summary["halted_reasons"] = list(active_breaker_decision(store).reason_codes)
            summary["cycles"] = cycles
            summary["ended_ts_ms"] = int(time.time() * 1000)
            summary["ws_stats"] = ws_client.stats if hasattr(ws_client, "stats") else {}
            return summary
        next_poll_monotonic = time.monotonic() + poll_interval_sec
    else:
        runtime_status = _runtime_model_binding_after_resume(
            store=store,
            settings=settings,
            ts_ms=int(time.time() * 1000),
        )
        _apply_runtime_status_to_summary(summary, runtime_status)
        summary["breaker_report"] = breaker_status(store)
        if bool(active_breaker_decision(store).active):
            summary["halted"] = True
            summary["halted_reasons"] = list(active_breaker_decision(store).reason_codes)
            summary["ended_ts_ms"] = int(time.time() * 1000)
            summary["ws_stats"] = ws_client.stats if hasattr(ws_client, "stats") else {}
            return summary

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
                action = _apply_private_ws_event_with_breakers(
                    store=store,
                    event=ws_event,
                    bot_id=settings.bot_id,
                    identifier_prefix=settings.identifier_prefix,
                    quote_currency=settings.quote_currency,
                    settings=settings,
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
                summary["breaker_report"] = breaker_status(store)

            if ws_task.done() and event_queue.empty() and not stop_event.is_set():
                exc = None
                if not ws_task.cancelled():
                    try:
                        exc = ws_task.exception()
                    except Exception:
                        exc = None
                summary["breaker_report"] = arm_breaker(
                    store,
                    reason_codes=["STALE_PRIVATE_WS_STREAM"],
                    source="private_ws",
                    ts_ms=int(time.time() * 1000),
                    details={"task_error": str(exc) if exc is not None else None},
                )
                summary["halted"] = True
                summary["halted_reasons"] = list(active_breaker_decision(store).reason_codes)
                break

            if time.monotonic() >= next_poll_monotonic:
                cycle_result = _run_sync_cycle_with_breakers(
                    store=store,
                    client=client,
                    settings=settings,
                    ts_ms=int(time.time() * 1000),
                )
                cycles += 1
                summary["last_report"] = cycle_result["report"]
                summary["last_cancel_summary"] = cycle_result["cancel_summary"]
                summary["breaker_report"] = cycle_result.get("breaker_report")
                summary["last_sync_error"] = cycle_result.get("sync_error")
                summary["small_account_report"] = cycle_result.get("small_account_report")
                _apply_runtime_status_to_summary(summary, cycle_result.get("runtime_handoff"))
                summary["last_breaker_cancel_summary"] = _maybe_enforce_breaker(
                    store=store,
                    client=client,
                    settings=settings,
                    report=cycle_result["report"],
                    prior_cancel_summary=cycle_result["cancel_summary"],
                    ts_ms=int(time.time() * 1000),
                )
                if bool(cycle_result["report"].get("halted")) or bool(active_breaker_decision(store).active):
                    summary["halted"] = True
                    summary["halted_reasons"] = list(active_breaker_decision(store).reason_codes or cycle_result["report"].get("halted_reasons", []))
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


def _apply_executor_event_with_breakers(
    *,
    store: LiveStateStore,
    event: Any,
    bot_id: str,
    identifier_prefix: str,
    quote_currency: str,
    settings: LiveDaemonSettings,
) -> dict[str, Any]:
    try:
        action = _apply_executor_event(
            store=store,
            event=event,
            bot_id=bot_id,
            identifier_prefix=identifier_prefix,
            quote_currency=quote_currency,
        )
    except Exception as exc:
        if classify_identifier_collision(exc):
            arm_breaker(
                store,
                reason_codes=["IDENTIFIER_COLLISION"],
                source="executor_event",
                ts_ms=_event_ts_ms(event),
                action=ACTION_FULL_KILL_SWITCH,
                details={"error": str(exc), "event_type": _event_type(event)},
            )
            return {"type": "executor_breaker_identifier_collision", "error": str(exc)}
        raise

    payload = _event_payload(event)
    state = str(payload.get("state", "")).strip().lower()
    event_name = str(payload.get("event_name", "")).strip().upper()
    ts_ms = _event_ts_ms(event)
    if state == "cancel_reject":
        record_counter_failure(
            store,
            counter_name="cancel_reject",
            limit=settings.breaker_cancel_reject_limit,
            source="executor_event",
            ts_ms=ts_ms,
            details={"event_name": event_name, "payload": payload},
        )
    elif event_name == "CANCEL_RESULT" or state in {"cancel", "cancelled", "done"}:
        reset_counter(store, counter_name="cancel_reject", source="executor_event", ts_ms=ts_ms)

    if state == "replace_reject":
        record_counter_failure(
            store,
            counter_name="replace_reject",
            limit=settings.breaker_replace_reject_limit,
            source="executor_event",
            ts_ms=ts_ms,
            details={"event_name": event_name, "payload": payload},
        )
    elif event_name == "ORDER_REPLACED":
        reset_counter(store, counter_name="replace_reject", source="executor_event", ts_ms=ts_ms)
    return action


def _apply_private_ws_event_with_breakers(
    *,
    store: LiveStateStore,
    event: MyOrderEvent | MyAssetEvent,
    bot_id: str,
    identifier_prefix: str,
    quote_currency: str,
    settings: LiveDaemonSettings,
) -> dict[str, Any]:
    try:
        action = apply_private_ws_event(
            store=store,
            event=event,
            bot_id=bot_id,
            identifier_prefix=identifier_prefix,
            quote_currency=quote_currency,
        )
    except Exception as exc:
        if classify_identifier_collision(exc):
            arm_breaker(
                store,
                reason_codes=["IDENTIFIER_COLLISION"],
                source="private_ws",
                ts_ms=int(getattr(event, "ts_ms", int(time.time() * 1000))),
                action=ACTION_FULL_KILL_SWITCH,
                details={"error": str(exc), "stream_type": getattr(event, "stream_type", None)},
            )
            return {"type": "ws_breaker_identifier_collision", "error": str(exc)}
        raise

    if isinstance(event, MyOrderEvent):
        state = str(event.state or "").strip().lower()
        ts_ms = int(event.ts_ms)
        if state == "cancel_reject":
            record_counter_failure(
                store,
                counter_name="cancel_reject",
                limit=settings.breaker_cancel_reject_limit,
                source="private_ws",
                ts_ms=ts_ms,
                details={"uuid": event.uuid, "identifier": event.identifier},
            )
        elif state in {"cancel", "cancelled", "done"}:
            reset_counter(store, counter_name="cancel_reject", source="private_ws", ts_ms=ts_ms)
        if state == "replace_reject":
            record_counter_failure(
                store,
                counter_name="replace_reject",
                limit=settings.breaker_replace_reject_limit,
                source="private_ws",
                ts_ms=ts_ms,
                details={"uuid": event.uuid, "identifier": event.identifier},
            )
    return action


def _run_sync_cycle_with_breakers(
    *,
    store: LiveStateStore,
    client: Any,
    settings: LiveDaemonSettings,
    ts_ms: int,
) -> dict[str, Any]:
    try:
        cycle_result = _run_sync_cycle(store=store, client=client, settings=settings, ts_ms=ts_ms)
    except UpbitError as exc:
        reason_code = classify_upbit_exception(exc)
        sync_error = {
            "error": str(exc),
            "error_name": str(getattr(exc, "error_name", "") or "").strip() or None,
            "status_code": getattr(exc, "status_code", None),
            "endpoint": getattr(exc, "endpoint", None),
            "method": getattr(exc, "method", None),
        }
        breaker_report = breaker_status(store)
        if reason_code == "REPEATED_RATE_LIMIT_ERRORS":
            breaker_report = record_counter_failure(
                store,
                counter_name="rate_limit_error",
                limit=settings.breaker_rate_limit_error_limit,
                source="sync_cycle",
                ts_ms=ts_ms,
                details=sync_error,
            )
        elif reason_code == "REPEATED_AUTH_ERRORS":
            breaker_report = record_counter_failure(
                store,
                counter_name="auth_error",
                limit=settings.breaker_auth_error_limit,
                source="sync_cycle",
                ts_ms=ts_ms,
                details=sync_error,
            )
        elif reason_code == "REPEATED_NONCE_ERRORS":
            breaker_report = record_counter_failure(
                store,
                counter_name="nonce_error",
                limit=settings.breaker_nonce_error_limit,
                source="sync_cycle",
                ts_ms=ts_ms,
                details=sync_error,
            )
        else:
            raise
        store.set_checkpoint(name="last_sync_error", payload=sync_error, ts_ms=ts_ms)
        active = active_breaker_decision(store)
        runtime_status = store.live_runtime_health() or {}
        return {
            "report": {
                "halted": active.active,
                "halted_reasons": list(active.reason_codes),
                "sync_error": sync_error,
                "counts": {},
            },
            "cancel_summary": None,
            "breaker_report": breaker_report,
            "sync_error": sync_error,
            "runtime_handoff": runtime_status,
            "small_account_report": build_small_account_runtime_report(
                store=store,
                canary_enabled=bool(settings.small_account_canary_enabled),
                max_positions=int(settings.small_account_max_positions),
                max_open_orders_per_market=int(settings.small_account_max_open_orders_per_market),
                local_positions=store.list_positions(),
                exchange_bot_open_orders=[],
                ts_ms=ts_ms,
            ),
        }
    except Exception as exc:
        if not classify_identifier_collision(exc):
            raise
        breaker_report = arm_breaker(
            store,
            reason_codes=["IDENTIFIER_COLLISION"],
            source="sync_cycle",
            ts_ms=ts_ms,
            action=ACTION_FULL_KILL_SWITCH,
            details={"error": str(exc)},
        )
        sync_error = {"error": str(exc)}
        store.set_checkpoint(name="last_sync_error", payload=sync_error, ts_ms=ts_ms)
        runtime_status = store.live_runtime_health() or {}
        return {
            "report": {
                "halted": True,
                "halted_reasons": ["IDENTIFIER_COLLISION"],
                "sync_error": sync_error,
                "counts": {},
            },
            "cancel_summary": None,
            "breaker_report": breaker_report,
            "sync_error": sync_error,
            "runtime_handoff": runtime_status,
            "small_account_report": build_small_account_runtime_report(
                store=store,
                canary_enabled=bool(settings.small_account_canary_enabled),
                max_positions=int(settings.small_account_max_positions),
                max_open_orders_per_market=int(settings.small_account_max_open_orders_per_market),
                local_positions=store.list_positions(),
                exchange_bot_open_orders=[],
                ts_ms=ts_ms,
            ),
        }

    reset_counter(store, counter_name="rate_limit_error", source="sync_cycle_success", ts_ms=ts_ms)
    reset_counter(store, counter_name="auth_error", source="sync_cycle_success", ts_ms=ts_ms)
    reset_counter(store, counter_name="nonce_error", source="sync_cycle_success", ts_ms=ts_ms)
    small_account_report = build_small_account_runtime_report(
        store=store,
        canary_enabled=bool(settings.small_account_canary_enabled),
        max_positions=int(settings.small_account_max_positions),
        max_open_orders_per_market=int(settings.small_account_max_open_orders_per_market),
        local_positions=store.list_positions(),
        exchange_bot_open_orders=list(cycle_result["report"].get("exchange_bot_open_orders", [])),
        ts_ms=ts_ms,
    )
    breaker_report = evaluate_cycle_contracts(store, report=cycle_result["report"], source="sync_cycle", ts_ms=ts_ms)
    if bool(settings.small_account_canary_enabled) and small_account_report["canary"]["violations"]:
        if not active_breaker_decision(store).active:
            breaker_report = arm_breaker(
                store,
                reason_codes=list(small_account_report["canary"]["violations"]),
                source="small_account_canary",
                ts_ms=ts_ms,
                action=ACTION_HALT_NEW_INTENTS,
                details=small_account_report,
            )
    runtime_status = _refresh_runtime_contract_health(
        store=store,
        settings=settings,
        ts_ms=ts_ms,
    )
    cycle_result["breaker_report"] = breaker_status(store)
    cycle_result["sync_error"] = None
    cycle_result["small_account_report"] = small_account_report
    cycle_result["runtime_handoff"] = runtime_status
    cycle_result["report"]["runtime_handoff"] = runtime_status
    return cycle_result


def _maybe_enforce_breaker(
    *,
    store: LiveStateStore,
    client: Any,
    settings: LiveDaemonSettings,
    report: dict[str, Any] | None,
    prior_cancel_summary: dict[str, Any] | None,
    ts_ms: int,
) -> dict[str, Any] | None:
    decision = active_breaker_decision(store)
    if not decision.active or not should_cancel_bot_orders(decision.action):
        return None
    exchange_bot_open_orders = report.get("exchange_bot_open_orders") if isinstance(report, dict) else None
    if not isinstance(exchange_bot_open_orders, list):
        return None
    already_attempted: set[tuple[str | None, str | None]] = set()
    if isinstance(prior_cancel_summary, dict):
        for item in prior_cancel_summary.get("results", []):
            if not isinstance(item, dict):
                continue
            already_attempted.add((_as_optional_str(item.get("uuid")), _as_optional_str(item.get("identifier"))))
    synthetic_report = {
        "actions": [
            {
                "type": "cancel_bot_open_order",
                "uuid": item.get("uuid"),
                "identifier": item.get("identifier"),
                "market": item.get("market"),
            }
            for item in exchange_bot_open_orders
            if (_as_optional_str(item.get("uuid")), _as_optional_str(item.get("identifier"))) not in already_attempted
        ]
    }
    cancel_summary = apply_cancel_actions(
        report=synthetic_report,
        cancel_order=lambda uuid, identifier: client.cancel_order(uuid=uuid, identifier=identifier),
        apply=True,
        allow_cancel_external_cli=bool(settings.allow_cancel_external_cli),
        allow_cancel_external_config=bool(settings.allow_cancel_external_orders),
    )
    store.set_checkpoint(
        name="last_breaker_cancel",
        payload={"decision": decision.__dict__, "cancel_summary": cancel_summary},
        ts_ms=ts_ms,
    )
    return cancel_summary


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
