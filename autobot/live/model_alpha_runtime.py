"""Live model_alpha runtime sharing the same public data contract as paper."""

from __future__ import annotations

import json
import time
import asyncio
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Sequence

from autobot.backtest.strategy_adapter import StrategyOrderIntent
from autobot.execution.order_supervisor import (
    OrderExecProfile,
    build_limit_price_from_mode,
    make_legacy_exec_profile,
    order_exec_profile_to_dict,
)
from autobot.execution.intent import new_order_intent
from autobot.models.predictor import ModelPredictor, load_predictor_from_registry
from autobot.models.registry import load_json
from autobot.models.runtime_recommendation_contract import normalize_runtime_recommendations_payload
from autobot.paper.engine import (
    MarketDataHub,
    UniverseProviderTop20,
    _entry_notional_quote_for_strategy,
    _interval_ms_from_tf,
)
from autobot.paper.live_features_v3 import LiveFeatureProviderV3
from autobot.paper.live_features_v4 import LiveFeatureProviderV4
from autobot.paper.live_features_v4_native import LiveFeatureProviderV4Native
from autobot.risk.live_risk_manager import LiveRiskManager
from autobot.risk.models import RiskManagerConfig
from autobot.strategy.micro_gate_v1 import MicroGateSettings, MicroGateV1
from autobot.strategy.micro_order_policy import MicroOrderPolicySettings, MicroOrderPolicyV1
from autobot.strategy.micro_snapshot import LiveWsMicroSnapshotProvider, LiveWsProviderSettings
from autobot.strategy.model_alpha_v1 import (
    ModelAlphaSettings,
    ModelAlphaStrategyV1,
    resolve_model_alpha_runtime_row_columns,
    resolve_runtime_model_alpha_settings,
)
from autobot.strategy.operational_overlay_v1 import (
    compute_micro_quality_composite,
    resolve_operational_execution_overlay,
)
from autobot.strategy.trade_gate_v1 import GateSettings, TradeGateV1
from autobot.upbit.ws.models import OrderbookEvent, TickerEvent, TradeEvent

from .admissibility import (
    AccountBalanceSnapshot,
    build_live_admissibility_report,
    build_live_order_admissibility_snapshot,
    evaluate_live_limit_order,
    round_price_to_tick,
)
from .breakers import (
    ACTION_FULL_KILL_SWITCH,
    ACTION_HALT_AND_CANCEL_BOT_ORDERS,
    ACTION_HALT_NEW_INTENTS,
    active_breaker_decision,
    arm_breaker,
    breaker_status,
    clear_breaker_reasons,
    classify_executor_reject_reason,
    new_intents_allowed,
    protective_orders_allowed,
    record_counter_failure,
    reset_counter,
)
from .daemon import (
    PRIVATE_WS_STALE_RETRY_LIMIT,
    LiveDaemonSettings,
    _consume_stream_retry_budget,
    _apply_private_ws_event_with_breakers,
    _apply_rollout_status_to_summary,
    _apply_runtime_status_to_summary,
    _evaluate_rollout_gate,
    _maybe_enforce_breaker,
    _reset_stream_retry_budget,
    _run_sync_cycle_with_breakers,
    _runtime_model_binding_after_resume,
)
from .identifier import new_order_identifier
from . import model_alpha_runtime_bootstrap as _runtime_bootstrap
from . import model_alpha_runtime_execute as _runtime_execute
from . import model_alpha_projection as _model_alpha_projection
from . import model_alpha_runtime_supervisor as _runtime_supervisor
from .reconcile import resume_risk_plans_after_reconcile
from .risk_loop import apply_ticker_event
from .small_account import (
    build_small_account_runtime_report,
    derive_volume_from_target_notional,
    record_small_account_decision,
    sizing_envelope_to_payload,
)
from .state_store import IntentRecord, LiveStateStore, OrderRecord
from .trade_journal import (
    activate_trade_journal_for_position,
    backfill_order_execution_details,
    close_trade_journal_for_market,
    recompute_trade_journal_records,
    record_entry_submission,
)
from .closed_order_backfill import backfill_recent_bot_closed_orders
from autobot.upbit.ws import MyOrderEvent, MyAssetEvent


@dataclass(frozen=True)
class LiveModelAlphaRuntimeSettings:
    daemon: LiveDaemonSettings
    quote: str = "KRW"
    top_n: int = 20
    tf: str = "5m"
    decision_interval_sec: float = 1.0
    universe_refresh_sec: float = 60.0
    universe_hold_sec: float = 120.0
    per_trade_krw: float = 10_000.0
    max_positions: int = 2
    min_order_krw: float = 5_000.0
    max_consecutive_failures: int = 3
    cooldown_sec_after_fail: int = 60
    model_alpha: ModelAlphaSettings = field(default_factory=ModelAlphaSettings)
    paper_live_parquet_root: str = "data/parquet"
    paper_live_candles_dataset: str = "candles_api_v1"
    paper_live_bootstrap_1m_bars: int = 2_000
    paper_live_micro_max_age_ms: int = 300_000
    micro_gate: MicroGateSettings = field(default_factory=MicroGateSettings)
    micro_order_policy: MicroOrderPolicySettings = field(default_factory=MicroOrderPolicySettings)
    risk_enabled: bool = False
    risk_exit_aggress_bps: float = 8.0
    risk_timeout_sec: int = 20
    risk_replace_max: int = 2
    risk_default_trail_pct: float = 1.0

    def __post_init__(self) -> None:
        if self.daemon.use_executor_ws:
            raise ValueError("strategy-runtime currently supports poll sync or private ws only")


async def run_live_model_alpha_runtime(
    *,
    store: LiveStateStore,
    client: Any,
    public_client: Any,
    public_ws_client: Any,
    settings: LiveModelAlphaRuntimeSettings,
    executor_gateway: Any | None = None,
    private_ws_client: Any | None = None,
) -> dict[str, Any]:
    daemon_settings = settings.daemon
    started_ts_ms = int(time.time() * 1000)
    started_monotonic = time.monotonic()
    summary: dict[str, Any] = {
        "started_ts_ms": started_ts_ms,
        "ended_ts_ms": started_ts_ms,
        "cycles": 0,
        "ticker_events": 0,
        "universe_updates": 0,
        "decisions": 0,
        "shadow_intents_total": 0,
        "submitted_intents_total": 0,
        "skipped_intents_total": 0,
        "risk_actions_total": 0,
        "halted": False,
        "halted_reasons": [],
        "resume_report": None,
        "last_report": None,
        "last_cancel_summary": None,
        "last_breaker_cancel_summary": None,
        "small_account_report": None,
        "breaker_report": breaker_status(store),
        "runtime_handoff": None,
        "live_runtime_model_run_id": None,
        "champion_pointer_run_id": None,
        "ws_public_last_checkpoint_ts_ms": None,
        "ws_public_staleness_sec": None,
        "model_pointer_divergence": False,
        "rollout": None,
        "rollout_mode": None,
        "rollout_target_unit": None,
        "rollout_start_allowed": False,
        "rollout_order_emission_allowed": False,
        "rollout_reason_codes": [],
        "strategy_feature_provider": "",
        "strategy_predictor_run_id": None,
        "stream_stop_reason": None,
        "last_order_supervision_report": None,
        "order_supervision_actions_total": 0,
        "private_ws_events_total": 0,
        "private_ws_last_event_ts_ms": None,
        "private_ws_last_event_latency_ms": None,
        "private_ws_stats": {},
        "public_ws_stats": {},
        "stream_stop_traceback": None,
        "closed_orders_backfill": None,
    }

    if daemon_settings.startup_reconcile:
        if not _startup_sync(store=store, client=client, settings=daemon_settings, summary=summary):
            summary["ended_ts_ms"] = int(time.time() * 1000)
            return summary
    else:
        runtime_status = _runtime_model_binding_after_resume(
            store=store,
            settings=daemon_settings,
            ts_ms=int(time.time() * 1000),
        )
        _apply_runtime_status_to_summary(summary, runtime_status)
        rollout_status = _evaluate_rollout_gate(
            store=store,
            client=client,
            settings=daemon_settings,
            ts_ms=int(time.time() * 1000),
        )
        _apply_rollout_status_to_summary(summary, rollout_status)
        summary["breaker_report"] = breaker_status(store)
        if not _runtime_loop_allowed(store):
            summary["halted"] = True
            summary["halted_reasons"] = list(active_breaker_decision(store).reason_codes)
            summary["ended_ts_ms"] = int(time.time() * 1000)
            return summary

    predictor = _load_predictor_for_runtime(store=store, settings=settings)
    summary["strategy_predictor_run_id"] = predictor.run_dir.name
    summary["order_execution_backfill"] = backfill_order_execution_details(store=store, client=client)
    resolved_model_alpha_settings, _ = resolve_runtime_model_alpha_settings(
        predictor=predictor,
        settings=settings.model_alpha,
    )
    markets = _load_quote_markets(public_client=public_client, quote=str(settings.quote))
    if not markets:
        raise RuntimeError(f"no quote markets available for quote={settings.quote}")
    instrument_cache = _load_market_instruments(public_client=public_client, markets=markets)
    micro_provider = LiveWsMicroSnapshotProvider(
        LiveWsProviderSettings(enabled=True, max_markets=max(len(markets), int(settings.top_n)))
    )
    micro_gate = MicroGateV1(settings.micro_gate) if settings.micro_gate.enabled else None
    trade_gate = TradeGateV1(
        GateSettings(
            per_trade_krw=float(settings.per_trade_krw),
            max_positions=max(_effective_live_trade_gate_max_positions(settings), 1),
            min_order_krw=max(float(settings.min_order_krw), 0.0),
            max_consecutive_failures=max(int(settings.max_consecutive_failures), 1),
            cooldown_sec_after_fail=max(int(settings.cooldown_sec_after_fail), 0),
        ),
        micro_gate=micro_gate,
        micro_snapshot_provider=micro_provider,
    )
    micro_order_policy = (
        MicroOrderPolicyV1(settings.micro_order_policy) if settings.micro_order_policy.enabled else None
    )
    feature_provider = _build_live_feature_provider(
        predictor=predictor,
        settings=settings,
        micro_snapshot_provider=micro_provider,
    )
    summary["strategy_feature_provider"] = feature_provider.__class__.__name__
    strategy = _build_live_strategy(
        predictor=predictor,
        settings=settings,
        feature_provider=feature_provider,
    )
    risk_manager = _build_risk_manager(
        store=store,
        settings=settings,
        executor_gateway=executor_gateway,
        public_client=public_client,
        instrument_cache=instrument_cache,
    )
    market_data = MarketDataHub(history_sec=300)
    universe = UniverseProviderTop20(
        quote=str(settings.quote).strip().upper(),
        top_n=max(int(settings.top_n), 1),
        refresh_sec=max(float(settings.universe_refresh_sec), 1.0),
        hold_sec=max(float(settings.universe_hold_sec), 0.0),
    )
    known_positions = _snapshot_position_state(store)
    summary["trade_journal_backfill"] = recompute_trade_journal_records(store=store)
    summary["closed_orders_backfill"] = backfill_recent_bot_closed_orders(
        store=store,
        client=client,
        bot_id=daemon_settings.bot_id,
        identifier_prefix=daemon_settings.identifier_prefix,
        now_ts_ms=int(time.time() * 1000),
    )
    _bootstrap_strategy_positions(
        store=store,
        strategy=strategy,
        risk_manager=risk_manager,
        known_positions=known_positions,
        ts_ms=int(time.time() * 1000),
    )
    initial_order_supervision = _supervise_open_strategy_orders(
        store=store,
        client=client,
        public_client=public_client,
        executor_gateway=executor_gateway,
        latest_prices=market_data.latest_prices(),
        micro_snapshot_provider=micro_provider,
        micro_order_policy=micro_order_policy,
        instrument_cache=instrument_cache,
        identifier_prefix=str(daemon_settings.identifier_prefix),
        bot_id=str(daemon_settings.bot_id),
        ts_ms=int(time.time() * 1000),
    )
    summary["last_order_supervision_report"] = initial_order_supervision
    summary["order_supervision_actions_total"] = int(summary["order_supervision_actions_total"]) + int(
        initial_order_supervision.get("replaced", 0)
    ) + int(initial_order_supervision.get("aborted", 0))

    interval_ms = _interval_ms_from_tf(settings.tf)
    last_model_decision_ts_ms: int | None = None
    next_sync_monotonic = time.monotonic() + max(int(daemon_settings.poll_interval_sec), 1)
    next_decision_at = time.monotonic()
    private_ws_queue: asyncio.Queue[MyOrderEvent | MyAssetEvent] | None = None
    private_ws_task: asyncio.Task[None] | None = None
    private_ws_stop_event: asyncio.Event | None = None
    public_trade_queue: asyncio.Queue[TradeEvent] | None = None
    public_orderbook_queue: asyncio.Queue[OrderbookEvent] | None = None
    public_micro_tasks: list[asyncio.Task[None]] = []
    public_micro_stop_event: asyncio.Event | None = None
    if private_ws_client is not None:
        private_ws_queue = asyncio.Queue()
        private_ws_stop_event = asyncio.Event()

        async def _private_ws_pump() -> None:
            assert private_ws_queue is not None
            assert private_ws_stop_event is not None
            async for ws_event in private_ws_client.stream_private(duration_sec=daemon_settings.duration_sec):
                if private_ws_stop_event.is_set():
                    break
                await private_ws_queue.put(ws_event)

        def _spawn_private_ws_task() -> asyncio.Task[None]:
            return asyncio.create_task(_private_ws_pump())

        private_ws_task = _spawn_private_ws_task()
        await asyncio.sleep(0)
    else:
        _spawn_private_ws_task = None
    if isinstance(micro_provider, LiveWsMicroSnapshotProvider):
        public_micro_stop_event = asyncio.Event()
        if hasattr(public_ws_client, "stream_trade"):
            public_trade_queue = asyncio.Queue()

            async def _public_trade_pump() -> None:
                assert public_trade_queue is not None
                assert public_micro_stop_event is not None
                async for trade_event in public_ws_client.stream_trade(markets, duration_sec=daemon_settings.duration_sec):
                    if public_micro_stop_event.is_set():
                        break
                    await public_trade_queue.put(trade_event)

            public_micro_tasks.append(asyncio.create_task(_public_trade_pump()))
        if hasattr(public_ws_client, "stream_orderbook"):
            public_orderbook_queue = asyncio.Queue()

            async def _public_orderbook_pump() -> None:
                assert public_orderbook_queue is not None
                assert public_micro_stop_event is not None
                async for orderbook_event in public_ws_client.stream_orderbook(
                    markets,
                    duration_sec=daemon_settings.duration_sec,
                    level=micro_provider.settings.orderbook_level,
                ):
                    if public_micro_stop_event.is_set():
                        break
                    await public_orderbook_queue.put(orderbook_event)

            public_micro_tasks.append(asyncio.create_task(_public_orderbook_pump()))
        if public_micro_tasks:
            await asyncio.sleep(0)
    try:
        async for ticker in public_ws_client.stream_ticker(markets, duration_sec=daemon_settings.duration_sec):
            if private_ws_queue is not None:
                private_ws_task = _drain_private_ws_events(
                    store=store,
                    private_ws_queue=private_ws_queue,
                    private_ws_task=private_ws_task,
                    private_ws_client=private_ws_client,
                    risk_manager=risk_manager,
                    daemon_settings=daemon_settings,
                    summary=summary,
                    spawn_private_ws_task_fn=_spawn_private_ws_task,
                )
            _drain_public_micro_events(
                trade_queue=public_trade_queue,
                orderbook_queue=public_orderbook_queue,
                provider=micro_provider,
            )
            summary["ticker_events"] = int(summary["ticker_events"]) + 1
            market_data.update(ticker)
            universe.update_ticker(ticker)
            feature_provider.ingest_ticker(ticker)
            if not public_micro_tasks:
                _ingest_live_micro_from_ticker(provider=micro_provider, ticker=ticker)

            if (
                settings.risk_enabled
                and risk_manager is not None
                and _protective_order_emission_allowed(store)
            ):
                risk_actions = apply_ticker_event(
                    risk_manager=risk_manager,
                    event=ticker,
                    micro_snapshot_provider=micro_provider,
                )
                summary["risk_actions_total"] = int(summary["risk_actions_total"]) + int(len(risk_actions))

            now_monotonic = time.monotonic()
            if universe.maybe_refresh(now_monotonic=now_monotonic, market_data=market_data):
                summary["universe_updates"] = int(summary["universe_updates"]) + 1

            if now_monotonic >= next_decision_at:
                decision_ts_ms = (int(ticker.ts_ms) // interval_ms) * interval_ms
                if last_model_decision_ts_ms is None or decision_ts_ms > last_model_decision_ts_ms:
                    result = strategy.on_ts(
                        ts_ms=decision_ts_ms,
                        active_markets=universe.markets(),
                        latest_prices=market_data.latest_prices(),
                        open_markets=_snapshot_open_markets(store),
                    )
                    summary["decisions"] = int(summary["decisions"]) + 1
                    store.set_checkpoint(
                        name="live_model_alpha_last_selection",
                        payload={
                            "ts_ms": int(decision_ts_ms),
                            "scored_rows": int(result.scored_rows),
                            "eligible_rows": int(result.eligible_rows),
                            "selected_rows": int(result.selected_rows),
                            "intents": int(len(result.intents)),
                            "skipped_reasons": dict(result.skipped_reasons),
                            "feature_provider_stats": getattr(feature_provider, "last_build_stats", lambda: {})(),
                        },
                        ts_ms=int(ticker.ts_ms),
                    )
                    for strategy_intent in result.intents:
                        submit_result = _handle_strategy_intent(
                            store=store,
                            client=client,
                            public_client=public_client,
                            executor_gateway=executor_gateway,
                            settings=settings,
                            predictor=predictor,
                            model_alpha_settings=resolved_model_alpha_settings,
                            strategy_intent=strategy_intent,
                            instrument_cache=instrument_cache,
                            latest_prices=market_data.latest_prices(),
                            micro_snapshot_provider=micro_provider,
                            micro_order_policy=micro_order_policy,
                            trade_gate=trade_gate,
                            ts_ms=int(decision_ts_ms),
                        )
                        if submit_result == "shadow":
                            summary["shadow_intents_total"] = int(summary["shadow_intents_total"]) + 1
                        elif submit_result == "submitted":
                            summary["submitted_intents_total"] = int(summary["submitted_intents_total"]) + 1
                        else:
                            summary["skipped_intents_total"] = int(summary["skipped_intents_total"]) + 1
                    last_model_decision_ts_ms = decision_ts_ms
                next_decision_at = now_monotonic + max(float(settings.decision_interval_sec), 0.2)

            if now_monotonic >= next_sync_monotonic:
                previous_positions = dict(known_positions)
                summary["closed_orders_backfill"] = backfill_recent_bot_closed_orders(
                    store=store,
                    client=client,
                    bot_id=daemon_settings.bot_id,
                    identifier_prefix=daemon_settings.identifier_prefix,
                    now_ts_ms=int(time.time() * 1000),
                )
                cycle_result = _run_sync_cycle_with_breakers(
                    store=store,
                    client=client,
                    settings=daemon_settings,
                    ts_ms=int(time.time() * 1000),
                )
                summary["cycles"] = int(summary["cycles"]) + 1
                summary["last_report"] = cycle_result["report"]
                summary["last_cancel_summary"] = cycle_result["cancel_summary"]
                summary["breaker_report"] = cycle_result.get("breaker_report")
                summary["small_account_report"] = cycle_result.get("small_account_report")
                _apply_runtime_status_to_summary(summary, cycle_result.get("runtime_handoff"))
                _apply_rollout_status_to_summary(summary, cycle_result.get("rollout"))
                order_supervision = _supervise_open_strategy_orders(
                    store=store,
                    client=client,
                    public_client=public_client,
                    executor_gateway=executor_gateway,
                    latest_prices=market_data.latest_prices(),
                    micro_snapshot_provider=micro_provider,
                    micro_order_policy=micro_order_policy,
                    instrument_cache=instrument_cache,
                    identifier_prefix=str(daemon_settings.identifier_prefix),
                    bot_id=str(daemon_settings.bot_id),
                    ts_ms=int(time.time() * 1000),
                )
                summary["last_order_supervision_report"] = order_supervision
                summary["order_supervision_actions_total"] = int(summary["order_supervision_actions_total"]) + int(
                    order_supervision.get("replaced", 0)
                ) + int(order_supervision.get("aborted", 0))
                summary["last_breaker_cancel_summary"] = _maybe_enforce_breaker(
                    store=store,
                    client=client,
                    settings=daemon_settings,
                    report=cycle_result["report"],
                    prior_cancel_summary=cycle_result["cancel_summary"],
                    ts_ms=int(time.time() * 1000),
                )
                known_positions = _apply_position_sync_to_strategy(
                    store=store,
                    client=client,
                    strategy=strategy,
                    risk_manager=risk_manager,
                    previous_positions=previous_positions,
                    latest_prices=market_data.latest_prices(),
                    ts_ms=int(ticker.ts_ms),
                )
                if private_ws_queue is not None:
                    private_ws_task = _drain_private_ws_events(
                        store=store,
                        private_ws_queue=private_ws_queue,
                        private_ws_task=private_ws_task,
                        private_ws_client=private_ws_client,
                        risk_manager=risk_manager,
                        daemon_settings=daemon_settings,
                        summary=summary,
                        spawn_private_ws_task_fn=_spawn_private_ws_task,
                    )
                _drain_public_micro_events(
                    trade_queue=public_trade_queue,
                    orderbook_queue=public_orderbook_queue,
                    provider=micro_provider,
                )
                if bool(cycle_result["report"].get("halted")) or not _runtime_loop_allowed(store):
                    summary["halted"] = True
                    summary["halted_reasons"] = list(
                        active_breaker_decision(store).reason_codes
                        or cycle_result["report"].get("halted_reasons", [])
                    )
                    break
                next_sync_monotonic = time.monotonic() + max(int(daemon_settings.poll_interval_sec), 1)
    except Exception as exc:
        public_ws_stats = dict(public_ws_client.stats) if hasattr(public_ws_client, "stats") else {}
        traceback_text = traceback.format_exc(limit=20)
        reason_code = (
            "LIVE_PUBLIC_WS_STREAM_FAILED"
            if "/autobot/upbit/ws/" in traceback_text.replace("\\", "/")
            else "LIVE_RUNTIME_LOOP_FAILED"
        )
        arm_breaker(
            store,
            reason_codes=[reason_code],
            source="live_model_alpha_runtime",
            ts_ms=int(time.time() * 1000),
            action=ACTION_HALT_NEW_INTENTS,
            details={
                "error": str(exc),
                "public_ws_stats": public_ws_stats,
                "traceback": traceback_text,
            },
        )
        summary["halted"] = True
        summary["halted_reasons"] = list(active_breaker_decision(store).reason_codes)
        summary["stream_stop_reason"] = str(exc)
        summary["stream_stop_traceback"] = traceback_text
        summary["public_ws_stats"] = public_ws_stats
    else:
        if private_ws_queue is not None:
            private_ws_task = _drain_private_ws_events(
                store=store,
                private_ws_queue=private_ws_queue,
                private_ws_task=private_ws_task,
                private_ws_client=private_ws_client,
                risk_manager=risk_manager,
                daemon_settings=daemon_settings,
                summary=summary,
                spawn_private_ws_task_fn=_spawn_private_ws_task,
            )
        _drain_public_micro_events(
            trade_queue=public_trade_queue,
            orderbook_queue=public_orderbook_queue,
            provider=micro_provider,
        )
        if not _runtime_loop_allowed(store):
            summary["halted"] = True
            summary["halted_reasons"] = list(active_breaker_decision(store).reason_codes)
        else:
            summary["stream_stop_reason"] = "STREAM_COMPLETED"
    finally:
        if private_ws_stop_event is not None:
            private_ws_stop_event.set()
        if private_ws_task is not None:
            private_ws_task.cancel()
            await asyncio.gather(private_ws_task, return_exceptions=True)
        if public_micro_stop_event is not None:
            public_micro_stop_event.set()
        if public_micro_tasks:
            for task in public_micro_tasks:
                task.cancel()
            await asyncio.gather(*public_micro_tasks, return_exceptions=True)
        if private_ws_client is not None and hasattr(private_ws_client, "stats"):
            summary["private_ws_stats"] = private_ws_client.stats
        if hasattr(public_ws_client, "stats"):
            summary["public_ws_stats"] = public_ws_client.stats

    summary["small_account_report"] = build_small_account_runtime_report(
        store=store,
        canary_enabled=bool(daemon_settings.small_account_canary_enabled),
        max_positions=int(daemon_settings.small_account_max_positions),
        max_open_orders_per_market=int(daemon_settings.small_account_max_open_orders_per_market),
        local_positions=store.list_positions(),
        exchange_bot_open_orders=list((summary.get("last_report") or {}).get("exchange_bot_open_orders", [])),
        ts_ms=int(time.time() * 1000),
    )
    summary["breaker_report"] = breaker_status(store)
    summary["ended_ts_ms"] = int(time.time() * 1000)
    summary["elapsed_sec"] = max(time.monotonic() - started_monotonic, 0.0)
    store.set_checkpoint(name="live_model_alpha_last_run", payload=summary, ts_ms=summary["ended_ts_ms"])
    return summary


def _startup_sync(
    *,
    store: LiveStateStore,
    client: Any,
    settings: LiveDaemonSettings,
    summary: dict[str, Any],
) -> bool:
    summary["closed_orders_backfill"] = backfill_recent_bot_closed_orders(
        store=store,
        client=client,
        bot_id=settings.bot_id,
        identifier_prefix=settings.identifier_prefix,
        now_ts_ms=int(time.time() * 1000),
    )
    cycle_result = _run_sync_cycle_with_breakers(
        store=store,
        client=client,
        settings=settings,
        ts_ms=int(time.time() * 1000),
    )
    summary["cycles"] = 1
    summary["last_report"] = cycle_result["report"]
    summary["last_cancel_summary"] = cycle_result["cancel_summary"]
    summary["breaker_report"] = cycle_result.get("breaker_report")
    summary["small_account_report"] = cycle_result.get("small_account_report")
    summary["last_breaker_cancel_summary"] = _maybe_enforce_breaker(
        store=store,
        client=client,
        settings=settings,
        report=cycle_result["report"],
        prior_cancel_summary=cycle_result["cancel_summary"],
        ts_ms=int(time.time() * 1000),
    )
    startup_online_only_halt = _startup_has_only_online_risk_halt(store)
    if (bool(cycle_result["report"].get("halted")) or not _runtime_loop_allowed(store)) and not startup_online_only_halt:
        summary["halted"] = True
        summary["halted_reasons"] = list(
            active_breaker_decision(store).reason_codes or cycle_result["report"].get("halted_reasons", [])
        )
        return False
    resume_report = resume_risk_plans_after_reconcile(store=store, ts_ms=int(time.time() * 1000))
    summary["resume_report"] = resume_report
    if bool(resume_report.get("halted")):
        summary["halted"] = True
        summary["halted_reasons"] = ["RESUME_REVIEW_REQUIRED"]
        return False
    runtime_status = _runtime_model_binding_after_resume(
        store=store,
        settings=settings,
        ts_ms=int(time.time() * 1000),
    )
    _apply_runtime_status_to_summary(summary, runtime_status)
    summary["startup_online_risk_recovery"] = _startup_online_risk_recovery(
        store=store,
        runtime_status=runtime_status,
        ts_ms=int(time.time() * 1000),
    )
    rollout_status = _evaluate_rollout_gate(
        store=store,
        client=client,
        settings=settings,
        ts_ms=int(time.time() * 1000),
    )
    _apply_rollout_status_to_summary(summary, rollout_status)
    summary["breaker_report"] = breaker_status(store)
    if not _runtime_loop_allowed(store):
        summary["halted"] = True
        summary["halted_reasons"] = list(active_breaker_decision(store).reason_codes)
        return False
    return True


_STARTUP_ONLINE_RISK_REASON_CODES = {
    "RISK_CONTROL_ONLINE_BREACH_STREAK",
    "RISK_CONTROL_MARTINGALE_EVIDENCE",
    "RISK_CONTROL_MARTINGALE_CRITICAL_EVIDENCE",
}


def _startup_has_only_online_risk_halt(store: LiveStateStore) -> bool:
    decision = active_breaker_decision(store)
    if not bool(decision.active):
        return False
    reason_codes = [str(item).strip().upper() for item in (decision.reason_codes or ()) if str(item).strip()]
    if not reason_codes:
        return False
    return all(code in _STARTUP_ONLINE_RISK_REASON_CODES for code in reason_codes)


def _startup_online_risk_recovery(
    *,
    store: LiveStateStore,
    runtime_status: dict[str, Any] | None,
    ts_ms: int,
) -> dict[str, Any]:
    decision = active_breaker_decision(store)
    active_reason_codes = [str(item).strip().upper() for item in (decision.reason_codes or ()) if str(item).strip()]
    active_online_reason_codes = [code for code in active_reason_codes if code in _STARTUP_ONLINE_RISK_REASON_CODES]
    if not active_online_reason_codes:
        return {
            "attempted": False,
            "reason": "NO_ACTIVE_ONLINE_RISK_BREAKER",
            "cleared_reason_codes": [],
        }
    current_contract = dict((runtime_status or {}).get("current_contract") or {})
    run_id = str(current_contract.get("live_runtime_model_run_id") or (runtime_status or {}).get("live_runtime_model_run_id") or "").strip()
    run_dir = str(current_contract.get("live_runtime_model_run_dir") or "").strip()
    if not run_id or not run_dir:
        return {
            "attempted": False,
            "reason": "RUNTIME_MODEL_CONTRACT_MISSING",
            "active_online_reason_codes": active_online_reason_codes,
            "cleared_reason_codes": [],
        }
    runtime_recommendations = normalize_runtime_recommendations_payload(
        load_json(Path(run_dir) / "runtime_recommendations.json")
    )
    risk_control_payload = dict(runtime_recommendations.get("risk_control") or {})
    if not risk_control_payload:
        return {
            "attempted": False,
            "reason": "RISK_CONTROL_PAYLOAD_MISSING",
            "run_id": run_id,
            "run_dir": run_dir,
            "active_online_reason_codes": active_online_reason_codes,
            "cleared_reason_codes": [],
        }
    online_threshold = _runtime_execute.resolve_execution_risk_control_online_threshold(
        store=store,
        run_id=run_id,
        risk_control_payload=risk_control_payload,
    )
    cleared_reason_codes: list[str] = []
    for value in list(online_threshold.get("clear_reason_codes") or []):
        reason_code = str(value).strip().upper()
        if reason_code and reason_code not in cleared_reason_codes:
            cleared_reason_codes.append(reason_code)
    if not bool(online_threshold.get("halt_triggered")):
        for reason_code in active_online_reason_codes:
            if reason_code not in cleared_reason_codes:
                cleared_reason_codes.append(reason_code)
    if cleared_reason_codes:
        clear_breaker_reasons(
            store,
            reason_codes=cleared_reason_codes,
            source="startup_online_risk_recovery",
            ts_ms=ts_ms,
            details={
                "run_id": run_id,
                "run_dir": run_dir,
                "active_online_reason_codes": active_online_reason_codes,
                "online_threshold": online_threshold,
            },
        )
    return {
        "attempted": True,
        "run_id": run_id,
        "run_dir": run_dir,
        "active_online_reason_codes": active_online_reason_codes,
        "cleared_reason_codes": cleared_reason_codes,
        "online_threshold": online_threshold,
    }


def _load_predictor_for_runtime(*, store: LiveStateStore, settings: LiveModelAlphaRuntimeSettings) -> ModelPredictor:
    return _runtime_bootstrap.load_predictor_for_runtime(
        store=store,
        settings=settings,
        load_predictor_from_registry_fn=load_predictor_from_registry,
    )


def _load_quote_markets(*, public_client: Any, quote: str) -> list[str]:
    return _runtime_bootstrap.load_quote_markets(public_client=public_client, quote=quote)


def _load_market_instruments(*, public_client: Any, markets: Sequence[str]) -> dict[str, dict[str, Any]]:
    return _runtime_bootstrap.load_market_instruments(public_client=public_client, markets=markets)


def _build_live_feature_provider(
    *,
    predictor: ModelPredictor,
    settings: LiveModelAlphaRuntimeSettings,
    micro_snapshot_provider: LiveWsMicroSnapshotProvider,
) -> Any:
    return _runtime_bootstrap.build_live_feature_provider(
        predictor=predictor,
        settings=settings,
        micro_snapshot_provider=micro_snapshot_provider,
        resolve_model_alpha_runtime_row_columns_fn=resolve_model_alpha_runtime_row_columns,
        live_feature_provider_v3_cls=LiveFeatureProviderV3,
        live_feature_provider_v4_cls=LiveFeatureProviderV4,
        live_feature_provider_v4_native_cls=LiveFeatureProviderV4Native,
    )


def _build_live_strategy(
    *,
    predictor: ModelPredictor,
    settings: LiveModelAlphaRuntimeSettings,
    feature_provider: Any,
) -> ModelAlphaStrategyV1:
    return _runtime_bootstrap.build_live_strategy(
        predictor=predictor,
        settings=settings,
        feature_provider=feature_provider,
        interval_ms_from_tf_fn=_interval_ms_from_tf,
        model_alpha_strategy_cls=ModelAlphaStrategyV1,
    )


def _build_risk_manager(
    *,
    store: LiveStateStore,
    settings: LiveModelAlphaRuntimeSettings,
    executor_gateway: Any | None,
    public_client: Any,
    instrument_cache: dict[str, dict[str, Any]],
) -> LiveRiskManager | None:
    if not bool(settings.risk_enabled):
        return None

    def _resolve_tick_size(market: str) -> float | None:
        market_value = str(market).strip().upper()
        if not market_value:
            return None
        payload = instrument_cache.get(market_value)
        if payload is None:
            loaded = public_client.orderbook_instruments([market_value])
            if isinstance(loaded, list):
                for item in loaded:
                    if isinstance(item, dict):
                        item_market = str(item.get("market", "")).strip().upper()
                        if item_market:
                            instrument_cache[item_market] = dict(item)
                payload = instrument_cache.get(market_value)
        if not isinstance(payload, dict):
            return None
        try:
            tick_size = float(payload.get("tick_size") or 0.0)
        except (TypeError, ValueError):
            return None
        return tick_size if tick_size > 0 else None

    return LiveRiskManager(
        store=store,
        executor_gateway=executor_gateway,
        config=RiskManagerConfig(
            exit_aggress_bps=float(settings.risk_exit_aggress_bps),
            order_timeout_sec=max(int(settings.risk_timeout_sec), 1),
            replace_max=max(int(settings.risk_replace_max), 0),
            default_sl_pct=float(settings.daemon.default_risk_sl_pct),
            default_tp_pct=float(settings.daemon.default_risk_tp_pct),
            default_trailing_enabled=bool(settings.daemon.default_risk_trailing_enabled),
            default_trail_pct=float(settings.risk_default_trail_pct),
        ),
        identifier_prefix=str(settings.daemon.identifier_prefix),
        bot_id=str(settings.daemon.bot_id),
        tick_size_resolver=_resolve_tick_size,
        micro_overlay_settings=settings.model_alpha.operational,
    )


def _snapshot_position_state(store: LiveStateStore) -> dict[str, dict[str, Any]]:
    return _runtime_bootstrap.snapshot_position_state(store, safe_float_fn=_safe_float)


def _snapshot_open_order_markets(store: LiveStateStore) -> set[str]:
    return _runtime_bootstrap.snapshot_open_order_markets(store)


def _snapshot_open_markets(store: LiveStateStore) -> set[str]:
    return _runtime_bootstrap.snapshot_open_markets(
        store,
        snapshot_position_state_fn=_snapshot_position_state,
        snapshot_open_order_markets_fn=_snapshot_open_order_markets,
    )


def _canary_entry_guard_reason(
    *,
    store: LiveStateStore,
    settings: LiveModelAlphaRuntimeSettings,
    market: str,
    side: str,
    accounts_payload: Any | None = None,
) -> str | None:
    if str(side).strip().lower() != "bid":
        return None
    if not bool(settings.daemon.small_account_canary_enabled):
        return None
    market_value = str(market).strip().upper()
    active_markets = _snapshot_open_markets(store)
    active_markets.update(
        _active_markets_from_accounts_payload(
            accounts_payload=accounts_payload,
            quote=str(settings.quote),
            min_order_krw=float(settings.min_order_krw),
        )
    )
    if market_value in active_markets:
        return "CANARY_MARKET_ALREADY_ACTIVE"
    max_positions = max(int(settings.daemon.small_account_max_positions), 1)
    if len(active_markets) >= max_positions:
        return "CANARY_SLOT_UNAVAILABLE"
    return None


def _active_markets_from_accounts_payload(
    *,
    accounts_payload: Any,
    quote: str,
    min_order_krw: float,
) -> set[str]:
    active_markets: set[str] = set()
    quote_currency = str(quote).strip().upper() or "KRW"
    min_total = max(float(min_order_krw), 0.0)
    if not isinstance(accounts_payload, list):
        return active_markets
    for item in accounts_payload:
        if not isinstance(item, dict):
            continue
        currency = str(item.get("currency", "")).strip().upper()
        if not currency or currency == quote_currency:
            continue
        balance = _safe_optional_float(item.get("balance")) or 0.0
        locked = _safe_optional_float(item.get("locked")) or 0.0
        total = max(float(balance) + float(locked), 0.0)
        if total <= 0.0:
            continue
        avg_buy_price = _safe_optional_float(item.get("avg_buy_price"))
        if avg_buy_price is not None and avg_buy_price > 0.0 and (total * float(avg_buy_price)) < min_total:
            continue
        active_markets.add(f"{quote_currency}-{currency}")
    return active_markets


def _bootstrap_strategy_positions(
    *,
    store: LiveStateStore,
    strategy: ModelAlphaStrategyV1,
    risk_manager: LiveRiskManager | None,
    known_positions: dict[str, dict[str, Any]],
    ts_ms: int,
) -> None:
    _runtime_bootstrap.bootstrap_strategy_positions(
        store=store,
        strategy=strategy,
        risk_manager=risk_manager,
        known_positions=known_positions,
        ts_ms=ts_ms,
        resolve_strategy_entry_ts_ms_fn=_resolve_strategy_entry_ts_ms,
        strategy_bid_fill_fn=_strategy_bid_fill,
        ensure_live_risk_plan_fn=_ensure_live_risk_plan,
        find_latest_model_entry_intent_fn=_find_latest_model_entry_intent,
        as_optional_str_fn=_as_optional_str,
        activate_trade_journal_for_position_fn=activate_trade_journal_for_position,
    )


def _apply_position_sync_to_strategy(
    *,
    store: LiveStateStore,
    client: Any,
    strategy: ModelAlphaStrategyV1,
    risk_manager: LiveRiskManager | None,
    previous_positions: dict[str, dict[str, Any]],
    latest_prices: dict[str, float],
    ts_ms: int,
) -> dict[str, dict[str, Any]]:
    current_positions = _snapshot_position_state(store)
    previous_markets = set(previous_positions)
    current_markets = set(current_positions)

    for market in sorted(current_markets - previous_markets):
        payload = current_positions[market]
        entry_ts_ms = _resolve_strategy_entry_ts_ms(store=store, market=market, position=payload, default_ts_ms=ts_ms)
        _strategy_bid_fill(strategy=strategy, market=market, position=payload, ts_ms=entry_ts_ms)
        _ensure_live_risk_plan(store=store, risk_manager=risk_manager, market=market, position=payload, ts_ms=ts_ms)
        entry_intent = _find_latest_model_entry_intent(store=store, market=market, position=payload)
        active_plan = max(
            store.list_risk_plans(market=market, states=("ACTIVE", "TRIGGERED", "EXITING")),
            key=lambda item: (
                int(item.get("updated_ts") or 0),
                int(item.get("created_ts") or 0),
                str(item.get("plan_id") or ""),
            ),
            default=None,
        )
        activate_trade_journal_for_position(
            store=store,
            market=market,
            position=payload,
            ts_ms=ts_ms,
            entry_intent=entry_intent,
            plan_id=_as_optional_str((active_plan or {}).get("plan_id")),
        )

    for market in sorted(previous_markets - current_markets):
        previous = previous_positions[market]
        active_plan = max(
            store.list_risk_plans(market=market, states=("ACTIVE", "TRIGGERED", "EXITING")),
            key=lambda item: (
                int(item.get("updated_ts") or 0),
                int(item.get("created_ts") or 0),
                str(item.get("plan_id") or ""),
            ),
            default=None,
        )
        exit_price = _safe_float(latest_prices.get(market), default=0.0)
        if exit_price <= 0:
            exit_price = _safe_float(previous.get("avg_entry_price"), default=1.0)
        backfill_order_execution_details(
            store=store,
            client=client,
            max_orders=16,
            target_markets={market},
        )
        close_trade_journal_for_market(
            store=store,
            market=market,
            position=previous,
            exit_price=exit_price,
            ts_ms=ts_ms,
            plan_id=_as_optional_str((active_plan or {}).get("plan_id")),
        )
        _strategy_ask_fill(strategy=strategy, market=market, position=previous, exit_price=exit_price, ts_ms=ts_ms)
        _close_market_risk_plans(store=store, market=market, ts_ms=ts_ms)

    for market in sorted(current_markets & previous_markets):
        _ensure_live_risk_plan(
            store=store,
            risk_manager=risk_manager,
            market=market,
            position=current_positions[market],
            ts_ms=ts_ms,
        )

    return current_positions


def _strategy_bid_fill(
    *,
    strategy: ModelAlphaStrategyV1,
    market: str,
    position: dict[str, Any],
    ts_ms: int,
) -> None:
    _model_alpha_projection.strategy_bid_fill(
        strategy=strategy,
        market=market,
        position=position,
        ts_ms=ts_ms,
    )


def _strategy_ask_fill(
    *,
    strategy: ModelAlphaStrategyV1,
    market: str,
    position: dict[str, Any],
    exit_price: float,
    ts_ms: int,
) -> None:
    _model_alpha_projection.strategy_ask_fill(
        strategy=strategy,
        market=market,
        position=position,
        exit_price=exit_price,
        ts_ms=ts_ms,
    )


def _resolve_strategy_entry_ts_ms(
    *,
    store: LiveStateStore,
    market: str,
    position: dict[str, Any],
    default_ts_ms: int,
) -> int:
    return _model_alpha_projection.resolve_strategy_entry_ts_ms(
        store=store,
        market=market,
        position=position,
        default_ts_ms=default_ts_ms,
    )


def _attach_exit_order_to_risk_plan(
    *,
    store: LiveStateStore,
    market: str,
    order_uuid: str,
    order_identifier: str,
    ts_ms: int,
) -> str | None:
    return _model_alpha_projection.attach_exit_order_to_risk_plan(
        store=store,
        market=market,
        order_uuid=order_uuid,
        order_identifier=order_identifier,
        ts_ms=ts_ms,
    )


def _ensure_live_risk_plan(
    *,
    store: LiveStateStore,
    risk_manager: LiveRiskManager | None,
    market: str,
    position: dict[str, Any],
    ts_ms: int,
) -> None:
    _model_alpha_projection.ensure_live_risk_plan(
        store=store,
        risk_manager=risk_manager,
        market=market,
        position=position,
        ts_ms=ts_ms,
    )


def _close_market_risk_plans(*, store: LiveStateStore, market: str, ts_ms: int) -> None:
    _model_alpha_projection.close_market_risk_plans(store=store, market=market, ts_ms=ts_ms)


def _find_latest_model_entry_intent(
    *,
    store: LiveStateStore,
    market: str,
    position: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    return _model_alpha_projection.find_latest_model_entry_intent(
        store=store,
        market=market,
        position=position,
    )


def _supervise_open_strategy_orders(
    *,
    store: LiveStateStore,
    client: Any,
    public_client: Any,
    executor_gateway: Any | None,
    latest_prices: dict[str, float],
    micro_snapshot_provider: LiveWsMicroSnapshotProvider,
    micro_order_policy: MicroOrderPolicyV1 | None,
    instrument_cache: dict[str, dict[str, Any]],
    ts_ms: int,
    identifier_prefix: str = "AUTOBOT",
    bot_id: str = "autobot-001",
) -> dict[str, Any]:
    return _runtime_supervisor.supervise_open_strategy_orders(
        store=store,
        client=client,
        public_client=public_client,
        executor_gateway=executor_gateway,
        instrument_cache=instrument_cache,
        latest_prices=latest_prices,
        micro_snapshot_provider=micro_snapshot_provider,
        micro_order_policy=micro_order_policy,
        identifier_prefix=identifier_prefix,
        bot_id=bot_id,
        ts_ms=ts_ms,
    )


@dataclass(frozen=True)
class _LiveTradeGateBalanceView:
    free: float


class _LiveTradeGateExchangeView:
    def __init__(
        self,
        *,
        store: LiveStateStore,
        accounts_payload: Any,
        quote_currency: str,
    ) -> None:
        self._store = store
        self._quote_currency = str(quote_currency).strip().upper()
        self._balances: dict[str, AccountBalanceSnapshot] = {}
        if isinstance(accounts_payload, list):
            for item in accounts_payload:
                if not isinstance(item, dict):
                    continue
                currency = str(item.get("currency", "")).strip().upper()
                if not currency:
                    continue
                self._balances[currency] = AccountBalanceSnapshot(
                    currency=currency,
                    free=_safe_float(item.get("balance"), default=0.0),
                    locked=_safe_float(item.get("locked"), default=0.0),
                    avg_buy_price=_safe_optional_float(item.get("avg_buy_price")),
                )

    def quote_balance(self) -> object:
        payload = self._balances.get(self._quote_currency)
        return _LiveTradeGateBalanceView(free=(float(payload.free) if payload is not None else 0.0))

    def coin_balance(self, currency: str) -> object:
        payload = self._balances.get(str(currency).strip().upper())
        return _LiveTradeGateBalanceView(free=(float(payload.free) if payload is not None else 0.0))

    def active_position_count(self) -> int:
        return len(
            [
                item
                for item in self._store.list_positions()
                if _safe_float(item.get("base_amount"), default=0.0) > 0.0
            ]
        )

    def has_position(self, market: str) -> bool:
        position = self._store.position_by_market(market=str(market).strip().upper())
        return position is not None and _safe_float(position.get("base_amount"), default=0.0) > 0.0

    def has_open_order(self, market: str, side: str | None = None) -> bool:
        market_value = str(market).strip().upper()
        side_value = str(side).strip().lower() if side is not None else None
        for item in self._store.list_orders(open_only=True):
            if str(item.get("market", "")).strip().upper() != market_value:
                continue
            if side_value is None:
                return True
            if str(item.get("side", "")).strip().lower() == side_value:
                return True
        return False


def _handle_strategy_intent(
    *,
    store: LiveStateStore,
    client: Any,
    public_client: Any,
    executor_gateway: Any | None,
    settings: LiveModelAlphaRuntimeSettings,
    predictor: ModelPredictor,
    model_alpha_settings: ModelAlphaSettings,
    strategy_intent: StrategyOrderIntent,
    instrument_cache: dict[str, dict[str, Any]],
    latest_prices: dict[str, float],
    micro_snapshot_provider: LiveWsMicroSnapshotProvider,
    micro_order_policy: MicroOrderPolicyV1 | None,
    trade_gate: TradeGateV1,
    ts_ms: int,
) -> str:
    return _runtime_execute.handle_strategy_intent(
        store=store,
        client=client,
        public_client=public_client,
        executor_gateway=executor_gateway,
        settings=settings,
        predictor=predictor,
        model_alpha_settings=model_alpha_settings,
        strategy_intent=strategy_intent,
        instrument_cache=instrument_cache,
        latest_prices=latest_prices,
        micro_snapshot_provider=micro_snapshot_provider,
        micro_order_policy=micro_order_policy,
        trade_gate=trade_gate,
        ts_ms=ts_ms,
        canary_entry_guard_reason_fn=_canary_entry_guard_reason,
        record_strategy_intent_fn=_record_strategy_intent,
        safe_optional_float_fn=_safe_optional_float,
        safe_float_fn=_safe_float,
        build_live_order_admissibility_snapshot_fn=build_live_order_admissibility_snapshot,
        exchange_view_cls=_LiveTradeGateExchangeView,
        resolve_live_strategy_execution_fn=_resolve_live_strategy_execution,
        evaluate_live_limit_order_fn=evaluate_live_limit_order,
        resolve_live_expected_edge_bps_fn=_resolve_live_expected_edge_bps,
        resolve_live_trade_action_fn=_runtime_execute.resolve_live_trade_action,
        resolve_execution_risk_control_decision_fn=_runtime_execute.resolve_execution_risk_control_decision,
        resolve_execution_risk_control_online_threshold_fn=_runtime_execute.resolve_execution_risk_control_online_threshold,
        resolve_execution_risk_control_size_decision_fn=_runtime_execute.resolve_execution_risk_control_size_decision,
        arm_breaker_fn=arm_breaker,
        clear_breaker_reasons_fn=clear_breaker_reasons,
        action_halt_new_intents=ACTION_HALT_NEW_INTENTS,
        action_halt_and_cancel_bot_orders=ACTION_HALT_AND_CANCEL_BOT_ORDERS,
        record_small_account_decision_fn=record_small_account_decision,
        build_live_admissibility_report_fn=build_live_admissibility_report,
        new_order_intent_fn=new_order_intent,
        order_emission_allowed_fn=_order_emission_allowed,
        new_order_identifier_fn=new_order_identifier,
        as_optional_str_fn=_as_optional_str,
        attach_exit_order_to_risk_plan_fn=_attach_exit_order_to_risk_plan,
        order_record_cls=OrderRecord,
        reset_counter_fn=reset_counter,
        record_entry_submission_fn=record_entry_submission,
        handle_submit_reject_fn=_handle_submit_reject,
    )


def _record_strategy_intent(
    *,
    store: LiveStateStore,
    market: str,
    side: str,
    price: float | None,
    volume: float | None,
    reason_code: str,
    meta_payload: dict[str, Any],
    status: str,
    ts_ms: int,
    intent_id: str | None = None,
) -> str:
    return _runtime_execute.record_strategy_intent(
        store=store,
        market=market,
        side=side,
        price=price,
        volume=volume,
        reason_code=reason_code,
        meta_payload=meta_payload,
        status=status,
        ts_ms=ts_ms,
        intent_id=intent_id,
        intent_record_cls=IntentRecord,
    )


def _handle_submit_reject(
    *,
    store: LiveStateStore,
    intent: Any,
    ts_ms: int,
    market: str,
    side: str,
    reason_code: str,
    meta_payload: dict[str, Any],
    reject_reason: str,
) -> None:
    _runtime_execute.handle_submit_reject(
        store=store,
        intent=intent,
        ts_ms=ts_ms,
        market=market,
        side=side,
        reason_code=reason_code,
        meta_payload=meta_payload,
        reject_reason=reject_reason,
        classify_executor_reject_reason_fn=classify_executor_reject_reason,
        record_counter_failure_fn=record_counter_failure,
        arm_breaker_fn=arm_breaker,
        action_full_kill_switch=ACTION_FULL_KILL_SWITCH,
        record_strategy_intent_fn=_record_strategy_intent,
    )


def _apply_canary_notional_cap(
    *,
    store: LiveStateStore,
    settings: LiveModelAlphaRuntimeSettings,
    target_notional_quote: float,
) -> float:
    return _runtime_execute.apply_canary_notional_cap(
        store=store,
        settings=settings,
        target_notional_quote=target_notional_quote,
        safe_optional_float_fn=_safe_optional_float,
    )


@dataclass(frozen=True)
class _LiveStrategyExecutionResolution:
    allowed: bool
    skip_reason: str | None
    requested_price: float
    requested_volume: float | None
    sizing_payload: dict[str, Any] | None
    meta_payload: dict[str, Any]


def _resolve_live_strategy_execution(
    *,
    market: str,
    side: str,
    settings: LiveModelAlphaRuntimeSettings,
    model_alpha_settings: ModelAlphaSettings,
    strategy_intent: StrategyOrderIntent,
    snapshot: Any,
    latest_trade_price: float,
    store: LiveStateStore,
    exchange_view: _LiveTradeGateExchangeView,
    ts_ms: int,
    micro_snapshot_provider: LiveWsMicroSnapshotProvider,
    micro_order_policy: MicroOrderPolicyV1 | None,
    trade_gate: TradeGateV1,
    risk_control_payload: dict[str, Any] | None,
) -> _LiveStrategyExecutionResolution:
    return _runtime_execute.resolve_live_strategy_execution(
        market=market,
        side=side,
        settings=settings,
        model_alpha_settings=model_alpha_settings,
        strategy_intent=strategy_intent,
        snapshot=snapshot,
        latest_trade_price=latest_trade_price,
        store=store,
        exchange_view=exchange_view,
        ts_ms=ts_ms,
        micro_snapshot_provider=micro_snapshot_provider,
        micro_order_policy=micro_order_policy,
        trade_gate=trade_gate,
        risk_control_payload=risk_control_payload,
        resolution_cls=_LiveStrategyExecutionResolution,
        safe_optional_float_fn=_safe_optional_float,
        safe_float_fn=_safe_float,
        resolve_execution_risk_control_size_decision_fn=_runtime_execute.resolve_execution_risk_control_size_decision,
        strategy_live_exec_profile_fn=_strategy_live_exec_profile,
        entry_notional_quote_for_strategy_fn=_entry_notional_quote_for_strategy,
        apply_canary_notional_cap_fn=_apply_canary_notional_cap,
        resolve_operational_execution_overlay_fn=resolve_operational_execution_overlay,
        compute_micro_quality_composite_fn=compute_micro_quality_composite,
        order_exec_profile_to_dict_fn=order_exec_profile_to_dict,
        round_price_to_tick_fn=round_price_to_tick,
        build_limit_price_from_mode_fn=build_limit_price_from_mode,
        derive_volume_from_target_notional_fn=derive_volume_from_target_notional,
        sizing_envelope_to_payload_fn=sizing_envelope_to_payload,
    )


def _strategy_live_exec_profile(
    *,
    settings: LiveModelAlphaRuntimeSettings,
    model_alpha_settings: ModelAlphaSettings,
) -> OrderExecProfile:
    return _runtime_execute.strategy_live_exec_profile(
        settings=settings,
        model_alpha_settings=model_alpha_settings,
        interval_ms_from_tf_fn=_interval_ms_from_tf,
        make_legacy_exec_profile_fn=make_legacy_exec_profile,
    )


def _effective_live_trade_gate_max_positions(settings: LiveModelAlphaRuntimeSettings) -> int:
    return _runtime_execute.effective_live_trade_gate_max_positions(settings)


def _order_emission_allowed(store: LiveStateStore) -> bool:
    return _runtime_execute.order_emission_allowed(store, new_intents_allowed_fn=new_intents_allowed)


def _protective_order_emission_allowed(store: LiveStateStore) -> bool:
    if not protective_orders_allowed(store):
        return False
    rollout_status = store.live_rollout_status() or {}
    if str(rollout_status.get("mode") or "").strip().lower() == "shadow":
        return False
    return True


def _runtime_loop_allowed(store: LiveStateStore) -> bool:
    return protective_orders_allowed(store)


def _drain_private_ws_events(
    *,
    store: LiveStateStore,
    private_ws_queue: asyncio.Queue[MyOrderEvent | MyAssetEvent],
    private_ws_task: asyncio.Task[None] | None,
    private_ws_client: Any | None,
    risk_manager: LiveRiskManager | None,
    daemon_settings: LiveDaemonSettings,
    summary: dict[str, Any],
    spawn_private_ws_task_fn: Any | None = None,
) -> asyncio.Task[None] | None:
    while True:
        try:
            ws_event = private_ws_queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        _reset_stream_retry_budget(
            store=store,
            stream_name="private_ws",
            ts_ms=int(time.time() * 1000),
        )
        action = _apply_private_ws_event_with_breakers(
            store=store,
            event=ws_event,
            bot_id=daemon_settings.bot_id,
            identifier_prefix=daemon_settings.identifier_prefix,
            quote_currency=daemon_settings.quote_currency,
            settings=daemon_settings,
        )
        summary["private_ws_events_total"] = int(summary["private_ws_events_total"]) + 1
        summary["private_ws_last_event_ts_ms"] = int(ws_event.ts_ms)
        summary["private_ws_last_event_latency_ms"] = max(int(time.time() * 1000) - int(ws_event.ts_ms), 0)
        if risk_manager is not None and isinstance(ws_event, MyOrderEvent):
            risk_action = risk_manager.handle_executor_event(
                {
                    "payload": {
                        "uuid": ws_event.uuid,
                        "identifier": ws_event.identifier,
                        "state": ws_event.state,
                        "event_name": "ORDER_STATE",
                    }
                }
            )
            if risk_action is not None:
                summary["risk_actions_total"] = int(summary["risk_actions_total"]) + 1
            state_value = str(ws_event.state or "").strip().lower()
            if state_value in {"done", "cancel", "cancelled"}:
                summary["trade_journal_backfill"] = recompute_trade_journal_records(store=store)
        summary["breaker_report"] = breaker_status(store)
    if private_ws_task is not None and private_ws_task.done():
        if private_ws_client is not None and hasattr(private_ws_client, "stats"):
            summary["private_ws_stats"] = private_ws_client.stats
        exc = None
        if not private_ws_task.cancelled():
            try:
                exc = private_ws_task.exception()
            except Exception:
                exc = None
        can_retry, retry_payload = _consume_stream_retry_budget(
            store=store,
            stream_name="private_ws",
            limit=PRIVATE_WS_STALE_RETRY_LIMIT,
            ts_ms=int(time.time() * 1000),
            details={"task_error": str(exc) if exc is not None else None},
        )
        if can_retry and callable(spawn_private_ws_task_fn):
            private_ws_task = spawn_private_ws_task_fn()
            summary["private_ws_restart_attempts"] = int(summary.get("private_ws_restart_attempts", 0)) + 1
        else:
            arm_breaker(
                store,
                reason_codes=["STALE_PRIVATE_WS_STREAM"],
                source="strategy_private_ws",
                ts_ms=int(time.time() * 1000),
                action=ACTION_HALT_NEW_INTENTS,
                details={"task_error": str(exc) if exc is not None else None, "retry_state": retry_payload},
            )
            summary["breaker_report"] = breaker_status(store)
    return private_ws_task


def _resolve_live_expected_edge_bps(meta_payload: dict[str, Any] | None) -> float | None:
    return _runtime_execute.resolve_live_expected_edge_bps(
        meta_payload,
        safe_optional_float_fn=_safe_optional_float,
    )


def _ingest_live_micro_from_ticker(
    *,
    provider: LiveWsMicroSnapshotProvider,
    ticker: TickerEvent,
) -> None:
    market = str(ticker.market).strip().upper()
    trade_price = _safe_optional_float(ticker.trade_price)
    acc_trade_price_24h = _safe_optional_float(ticker.acc_trade_price_24h)
    if not market or trade_price is None or trade_price <= 0:
        return
    acc_by_market = getattr(provider, "_autobot_acc_notional_by_market", None)
    if not isinstance(acc_by_market, dict):
        acc_by_market = {}
        setattr(provider, "_autobot_acc_notional_by_market", acc_by_market)
    current_acc = max(float(acc_trade_price_24h or 0.0), 0.0)
    prev_acc = _safe_optional_float(acc_by_market.get(market))
    if prev_acc is None:
        notional_delta = max(float(trade_price) * 1e-8, 1e-8)
    elif current_acc >= prev_acc:
        notional_delta = max(current_acc - prev_acc, 0.0)
        if notional_delta <= 0:
            notional_delta = max(float(trade_price) * 1e-8, 1e-8)
    else:
        notional_delta = max(float(trade_price) * 1e-8, 1e-8)
    acc_by_market[market] = float(current_acc)
    volume = max(notional_delta / max(float(trade_price), 1e-8), 1e-12)
    provider.ingest_trade(
        {
            "market": market,
            "ts_ms": int(ticker.ts_ms),
            "trade_ts_ms": int(ticker.ts_ms),
            "price": float(trade_price),
            "volume": float(volume),
            "ask_bid": "BID",
        }
    )


def _ingest_live_micro_trade_event(
    *,
    provider: LiveWsMicroSnapshotProvider,
    event: TradeEvent,
) -> None:
    market = str(event.market).strip().upper()
    trade_price = _safe_optional_float(event.trade_price)
    trade_volume = _safe_optional_float(event.trade_volume)
    if not market or trade_price is None or trade_price <= 0 or trade_volume is None or trade_volume <= 0:
        return
    provider.ingest_trade(
        {
            "market": market,
            "ts_ms": int(event.ts_ms),
            "trade_ts_ms": int(event.ts_ms),
            "price": float(trade_price),
            "volume": float(trade_volume),
            "ask_bid": str(event.ask_bid).strip().upper(),
        }
    )


def _ingest_live_micro_orderbook_event(
    *,
    provider: LiveWsMicroSnapshotProvider,
    event: OrderbookEvent,
) -> None:
    market = str(event.market).strip().upper()
    if not market:
        return
    payload: dict[str, Any] = {
        "market": market,
        "ts_ms": int(event.ts_ms),
    }
    for idx, unit in enumerate(event.units[: max(int(provider.settings.orderbook_topk), 1)], start=1):
        payload[f"ask{idx}_price"] = unit.ask_price
        payload[f"ask{idx}_size"] = unit.ask_size
        payload[f"bid{idx}_price"] = unit.bid_price
        payload[f"bid{idx}_size"] = unit.bid_size
    provider.ingest_orderbook(payload)


def _drain_public_micro_events(
    *,
    trade_queue: asyncio.Queue[TradeEvent] | None,
    orderbook_queue: asyncio.Queue[OrderbookEvent] | None,
    provider: LiveWsMicroSnapshotProvider,
) -> None:
    if trade_queue is not None:
        while True:
            try:
                trade_event = trade_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            _ingest_live_micro_trade_event(provider=provider, event=trade_event)
    if orderbook_queue is not None:
        while True:
            try:
                orderbook_event = orderbook_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            _ingest_live_micro_orderbook_event(provider=provider, event=orderbook_event)


def _as_optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _safe_float(value: object, *, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _safe_optional_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_optional_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: object, *, default: int) -> int:
    resolved = _safe_optional_int(value)
    if resolved is None:
        return int(default)
    return int(resolved)
