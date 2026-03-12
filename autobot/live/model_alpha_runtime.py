"""Live model_alpha runtime sharing the same public data contract as paper."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Sequence

from autobot.backtest.strategy_adapter import StrategyFillEvent, StrategyOrderIntent
from autobot.execution.order_supervisor import (
    OrderExecProfile,
    build_limit_price_from_mode,
    make_legacy_exec_profile,
    order_exec_profile_to_dict,
)
from autobot.execution.intent import new_order_intent
from autobot.models.predictor import ModelPredictor, load_predictor_from_registry
from autobot.paper.engine import (
    MarketDataHub,
    UniverseProviderTop20,
    _entry_notional_quote_for_strategy,
    _interval_ms_from_tf,
)
from autobot.paper.live_features_v3 import LiveFeatureProviderV3
from autobot.paper.live_features_v4 import LiveFeatureProviderV4
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
from autobot.upbit.ws.models import TickerEvent

from .admissibility import (
    AccountBalanceSnapshot,
    build_live_admissibility_report,
    build_live_order_admissibility_snapshot,
    evaluate_live_limit_order,
    round_price_to_tick,
)
from .breakers import (
    ACTION_FULL_KILL_SWITCH,
    ACTION_HALT_NEW_INTENTS,
    active_breaker_decision,
    arm_breaker,
    breaker_status,
    classify_executor_reject_reason,
    new_intents_allowed,
    record_counter_failure,
    reset_counter,
)
from .daemon import (
    LiveDaemonSettings,
    _apply_rollout_status_to_summary,
    _apply_runtime_status_to_summary,
    _evaluate_rollout_gate,
    _maybe_enforce_breaker,
    _run_sync_cycle_with_breakers,
    _runtime_model_binding_after_resume,
)
from .identifier import new_order_identifier
from .model_risk_plan import build_model_derived_risk_records, build_model_exit_plan_from_position, extract_model_exit_plan
from .reconcile import resume_risk_plans_after_reconcile
from .risk_loop import apply_ticker_event
from .small_account import (
    build_small_account_runtime_report,
    derive_volume_from_target_notional,
    record_small_account_decision,
    sizing_envelope_to_payload,
)
from .state_store import IntentRecord, LiveStateStore, OrderRecord, RiskPlanRecord
from .trade_journal import (
    activate_trade_journal_for_position,
    backfill_order_execution_details,
    close_trade_journal_for_market,
    recompute_trade_journal_records,
    record_entry_submission,
)


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
        if self.daemon.use_private_ws or self.daemon.use_executor_ws:
            raise ValueError("strategy-runtime currently supports poll sync only")


async def run_live_model_alpha_runtime(
    *,
    store: LiveStateStore,
    client: Any,
    public_client: Any,
    public_ws_client: Any,
    settings: LiveModelAlphaRuntimeSettings,
    executor_gateway: Any | None = None,
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
            settings=daemon_settings,
            ts_ms=int(time.time() * 1000),
        )
        _apply_rollout_status_to_summary(summary, rollout_status)
        summary["breaker_report"] = breaker_status(store)
        if bool(active_breaker_decision(store).active):
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
    _bootstrap_strategy_positions(
        store=store,
        strategy=strategy,
        risk_manager=risk_manager,
        known_positions=known_positions,
        ts_ms=int(time.time() * 1000),
    )

    interval_ms = _interval_ms_from_tf(settings.tf)
    last_model_decision_ts_ms: int | None = None
    next_sync_monotonic = time.monotonic() + max(int(daemon_settings.poll_interval_sec), 1)
    next_decision_at = time.monotonic()
    try:
        async for ticker in public_ws_client.stream_ticker(markets, duration_sec=daemon_settings.duration_sec):
            summary["ticker_events"] = int(summary["ticker_events"]) + 1
            market_data.update(ticker)
            universe.update_ticker(ticker)
            feature_provider.ingest_ticker(ticker)
            _ingest_live_micro_from_ticker(provider=micro_provider, ticker=ticker)

            if settings.risk_enabled and risk_manager is not None and _order_emission_allowed(store):
                risk_actions = apply_ticker_event(risk_manager=risk_manager, event=ticker)
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
                if bool(cycle_result["report"].get("halted")) or bool(active_breaker_decision(store).active):
                    summary["halted"] = True
                    summary["halted_reasons"] = list(
                        active_breaker_decision(store).reason_codes
                        or cycle_result["report"].get("halted_reasons", [])
                    )
                    break
                next_sync_monotonic = time.monotonic() + max(int(daemon_settings.poll_interval_sec), 1)
    except Exception as exc:
        arm_breaker(
            store,
            reason_codes=["LIVE_PUBLIC_WS_STREAM_FAILED"],
            source="live_model_alpha_runtime",
            ts_ms=int(time.time() * 1000),
            action=ACTION_HALT_NEW_INTENTS,
            details={"error": str(exc)},
        )
        summary["halted"] = True
        summary["halted_reasons"] = list(active_breaker_decision(store).reason_codes)
        summary["stream_stop_reason"] = str(exc)
    else:
        if bool(active_breaker_decision(store).active):
            summary["halted"] = True
            summary["halted_reasons"] = list(active_breaker_decision(store).reason_codes)
        else:
            summary["stream_stop_reason"] = "STREAM_COMPLETED"

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
    if bool(cycle_result["report"].get("halted")) or bool(active_breaker_decision(store).active):
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
    rollout_status = _evaluate_rollout_gate(
        store=store,
        settings=settings,
        ts_ms=int(time.time() * 1000),
    )
    _apply_rollout_status_to_summary(summary, rollout_status)
    summary["breaker_report"] = breaker_status(store)
    if bool(active_breaker_decision(store).active):
        summary["halted"] = True
        summary["halted_reasons"] = list(active_breaker_decision(store).reason_codes)
        return False
    return True


def _load_predictor_for_runtime(*, store: LiveStateStore, settings: LiveModelAlphaRuntimeSettings) -> ModelPredictor:
    runtime_contract = store.runtime_contract() or {}
    run_id = str(runtime_contract.get("live_runtime_model_run_id", "")).strip()
    if not run_id:
        raise ValueError("runtime contract missing live_runtime_model_run_id")
    model_family = (
        str(runtime_contract.get("model_family_resolved", "")).strip() or settings.daemon.runtime_model_family
    )
    return load_predictor_from_registry(
        registry_root=Path(str(settings.daemon.registry_root)),
        model_ref=run_id,
        model_family=model_family,
    )


def _load_quote_markets(*, public_client: Any, quote: str) -> list[str]:
    payload = public_client.markets(is_details=True)
    if not isinstance(payload, list):
        return []
    prefix = f"{str(quote).strip().upper()}-"
    markets: list[str] = []
    seen: set[str] = set()
    for item in payload:
        if not isinstance(item, dict):
            continue
        market = str(item.get("market", "")).strip().upper()
        if not market.startswith(prefix) or market in seen:
            continue
        seen.add(market)
        markets.append(market)
    return markets


def _load_market_instruments(*, public_client: Any, markets: Sequence[str]) -> dict[str, dict[str, Any]]:
    payload = public_client.orderbook_instruments(list(markets))
    if not isinstance(payload, list):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for item in payload:
        if not isinstance(item, dict):
            continue
        market = str(item.get("market", "")).strip().upper()
        if market:
            out[market] = dict(item)
    return out


def _build_live_feature_provider(
    *,
    predictor: ModelPredictor,
    settings: LiveModelAlphaRuntimeSettings,
    micro_snapshot_provider: LiveWsMicroSnapshotProvider,
) -> Any:
    feature_set = str(settings.model_alpha.feature_set).strip().lower() or "v4"
    common_kwargs = {
        "feature_columns": predictor.feature_columns,
        "extra_columns": resolve_model_alpha_runtime_row_columns(predictor=predictor),
        "tf": str(settings.tf).strip().lower() or "5m",
        "quote": str(settings.quote).strip().upper() or "KRW",
        "micro_snapshot_provider": micro_snapshot_provider,
        "micro_max_age_ms": int(settings.paper_live_micro_max_age_ms),
        "parquet_root": str(settings.paper_live_parquet_root),
        "candles_dataset_name": str(settings.paper_live_candles_dataset),
        "bootstrap_1m_bars": int(settings.paper_live_bootstrap_1m_bars),
    }
    if feature_set == "v4":
        return LiveFeatureProviderV4(**common_kwargs)
    return LiveFeatureProviderV3(**common_kwargs)


def _build_live_strategy(
    *,
    predictor: ModelPredictor,
    settings: LiveModelAlphaRuntimeSettings,
    feature_provider: Any,
) -> ModelAlphaStrategyV1:
    interval_ms = _interval_ms_from_tf(settings.tf)
    return ModelAlphaStrategyV1(
        predictor=predictor,
        feature_groups=None,
        settings=settings.model_alpha,
        interval_ms=interval_ms,
        live_frame_provider=lambda ts_ms, markets: feature_provider.build_frame(ts_ms=ts_ms, markets=markets),
        enable_operational_overlay=True,
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
        tick_size_resolver=_resolve_tick_size,
    )


def _snapshot_position_state(store: LiveStateStore) -> dict[str, dict[str, Any]]:
    positions: dict[str, dict[str, Any]] = {}
    for item in store.list_positions():
        market = str(item.get("market", "")).strip().upper()
        if not market:
            continue
        if _safe_float(item.get("base_amount"), default=0.0) <= 0:
            continue
        positions[market] = dict(item)
    return positions


def _snapshot_open_order_markets(store: LiveStateStore) -> set[str]:
    markets: set[str] = set()
    for item in store.list_orders(open_only=True):
        market = str(item.get("market", "")).strip().upper()
        if market:
            markets.add(market)
    return markets


def _snapshot_open_markets(store: LiveStateStore) -> set[str]:
    return set(_snapshot_position_state(store).keys()) | _snapshot_open_order_markets(store)


def _canary_entry_guard_reason(
    *,
    store: LiveStateStore,
    settings: LiveModelAlphaRuntimeSettings,
    market: str,
    side: str,
) -> str | None:
    if str(side).strip().lower() != "bid":
        return None
    if not bool(settings.daemon.small_account_canary_enabled):
        return None
    market_value = str(market).strip().upper()
    active_markets = _snapshot_open_markets(store)
    if market_value in active_markets:
        return "CANARY_MARKET_ALREADY_ACTIVE"
    max_positions = max(int(settings.daemon.small_account_max_positions), 1)
    if len(active_markets) >= max_positions:
        return "CANARY_SLOT_UNAVAILABLE"
    return None


def _bootstrap_strategy_positions(
    *,
    store: LiveStateStore,
    strategy: ModelAlphaStrategyV1,
    risk_manager: LiveRiskManager | None,
    known_positions: dict[str, dict[str, Any]],
    ts_ms: int,
) -> None:
    for market, payload in known_positions.items():
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
    strategy.on_fill(
        StrategyFillEvent(
            ts_ms=int(ts_ms),
            market=str(market).strip().upper(),
            side="bid",
            price=max(_safe_float(position.get("avg_entry_price"), default=0.0), 1e-12),
            volume=max(_safe_float(position.get("base_amount"), default=0.0), 1e-12),
            fee_quote=0.0,
            meta={"model_exit_plan": build_model_exit_plan_from_position(position)},
        )
    )


def _strategy_ask_fill(
    *,
    strategy: ModelAlphaStrategyV1,
    market: str,
    position: dict[str, Any],
    exit_price: float,
    ts_ms: int,
) -> None:
    strategy.on_fill(
        StrategyFillEvent(
            ts_ms=int(ts_ms),
            market=str(market).strip().upper(),
            side="ask",
            price=max(float(exit_price), 1e-12),
            volume=max(_safe_float(position.get("base_amount"), default=0.0), 1e-12),
            fee_quote=0.0,
            meta={},
        )
    )


def _resolve_strategy_entry_ts_ms(
    *,
    store: LiveStateStore,
    market: str,
    position: dict[str, Any],
    default_ts_ms: int,
) -> int:
    live_plans = store.list_risk_plans(market=market, states=("ACTIVE", "TRIGGERED", "EXITING"))
    created_candidates = [int(item.get("created_ts") or 0) for item in live_plans if int(item.get("created_ts") or 0) > 0]
    if created_candidates:
        return max(created_candidates)
    updated_ts = _safe_int(position.get("updated_ts"), default=0)
    if updated_ts > 0:
        return updated_ts
    return int(default_ts_ms)


def _attach_exit_order_to_risk_plan(
    *,
    store: LiveStateStore,
    market: str,
    order_uuid: str,
    order_identifier: str,
    ts_ms: int,
) -> str | None:
    live_plans = store.list_risk_plans(market=market, states=("ACTIVE", "TRIGGERED", "EXITING"))
    if not live_plans:
        return None
    selected = max(
        live_plans,
        key=lambda item: (
            int(item.get("created_ts") or 0),
            int(item.get("updated_ts") or 0),
            str(item.get("plan_id") or ""),
        ),
    )
    plan_id = str(selected.get("plan_id") or "").strip()
    if not plan_id:
        return None
    selected_tp = dict(selected.get("tp") or {})
    selected_sl = dict(selected.get("sl") or {})
    selected_trailing = dict(selected.get("trailing") or {})
    store.upsert_risk_plan(
        RiskPlanRecord(
            plan_id=plan_id,
            market=str(selected.get("market", market) or market),
            side=str(selected.get("side", "long") or "long"),
            entry_price_str=str(selected.get("entry_price_str", "")),
            qty_str=str(selected.get("qty_str", "")),
            tp_enabled=bool(selected_tp.get("enabled")),
            tp_price_str=_as_optional_str(selected_tp.get("tp_price_str")),
            tp_pct=_safe_optional_float(selected_tp.get("tp_pct")),
            sl_enabled=bool(selected_sl.get("enabled")),
            sl_price_str=_as_optional_str(selected_sl.get("sl_price_str")),
            sl_pct=_safe_optional_float(selected_sl.get("sl_pct")),
            trailing_enabled=bool(selected_trailing.get("enabled")),
            trail_pct=_safe_optional_float(selected_trailing.get("trail_pct")),
            high_watermark_price_str=_as_optional_str(selected_trailing.get("high_watermark_price_str")),
            armed_ts_ms=_safe_optional_int(selected_trailing.get("armed_ts_ms")),
            timeout_ts_ms=_safe_optional_int(selected.get("timeout_ts_ms")),
            state="EXITING",
            last_eval_ts_ms=int(selected.get("last_eval_ts_ms", 0) or 0),
            last_action_ts_ms=int(ts_ms),
            current_exit_order_uuid=str(order_uuid).strip(),
            current_exit_order_identifier=str(order_identifier).strip(),
            replace_attempt=int(selected.get("replace_attempt", 0) or 0),
            created_ts=int(selected.get("created_ts", ts_ms) or ts_ms),
            updated_ts=int(ts_ms),
            plan_source=_as_optional_str(selected.get("plan_source")),
            source_intent_id=_as_optional_str(selected.get("source_intent_id")),
        )
    )
    return plan_id


def _ensure_live_risk_plan(
    *,
    store: LiveStateStore,
    risk_manager: LiveRiskManager | None,
    market: str,
    position: dict[str, Any],
    ts_ms: int,
) -> None:
    if risk_manager is None:
        return
    live_plans = store.list_risk_plans(market=market, states=("ACTIVE", "TRIGGERED", "EXITING"))
    qty = max(_safe_float(position.get("base_amount"), default=0.0), 0.0)
    entry_price = max(_safe_float(position.get("avg_entry_price"), default=0.0), 0.0)
    if qty <= 0 or entry_price <= 0:
        return
    entry_intent: dict[str, Any] | None = None
    if not live_plans:
        entry_intent = _find_latest_model_entry_intent(store=store, market=market, position=position)
        if entry_intent is not None:
            _, risk_plan_record = build_model_derived_risk_records(
                market=market,
                base_currency=str(market).split("-")[-1],
                base_amount=qty,
                avg_entry_price=entry_price,
                plan_payload=entry_intent["plan_payload"],
                created_ts=int(entry_intent["created_ts"]),
                updated_ts=int(ts_ms),
                intent_id=entry_intent["intent_id"],
            )
            store.upsert_risk_plan(risk_plan_record)
            return
        risk_manager.attach_default_risk(
            market=market,
            entry_price=entry_price,
            qty=qty,
            ts_ms=ts_ms,
            plan_id=f"default-risk-{market}",
        )
        return
    for plan in live_plans:
        if str(plan.get("state", "")).strip().upper() != "ACTIVE":
            continue
        current_exit_uuid = str(plan.get("current_exit_order_uuid") or "").strip()
        if current_exit_uuid:
            continue
        derived_plan_record = None
        derived_position_record = None
        plan_tp = dict(plan.get("tp") or {})
        plan_sl = dict(plan.get("sl") or {})
        plan_trailing = dict(plan.get("trailing") or {})
        plan_source = _as_optional_str(plan.get("plan_source"))
        source_intent_id = _as_optional_str(plan.get("source_intent_id"))
        timeout_ts_ms = _safe_optional_int(plan.get("timeout_ts_ms"))
        needs_model_backfill = (not plan_source) or (not source_intent_id) or (timeout_ts_ms is None)
        if not needs_model_backfill and str(plan_source).strip().lower() == "model_alpha_v1":
            if entry_intent is None:
                entry_intent = _find_latest_model_entry_intent(store=store, market=market, position=position)
            if entry_intent is not None:
                candidate_position_record, candidate_plan_record = build_model_derived_risk_records(
                    market=market,
                    base_currency=str(market).split("-")[-1],
                    base_amount=qty,
                    avg_entry_price=entry_price,
                    plan_payload=entry_intent["plan_payload"],
                    created_ts=int(entry_intent["created_ts"]),
                    updated_ts=int(ts_ms),
                    intent_id=entry_intent["intent_id"],
                )
                needs_model_backfill = _risk_plan_differs_from_record(plan=plan, record=candidate_plan_record)
                if needs_model_backfill:
                    derived_position_record = candidate_position_record
                    derived_plan_record = candidate_plan_record
        elif needs_model_backfill:
            if entry_intent is None:
                entry_intent = _find_latest_model_entry_intent(store=store, market=market, position=position)
            if entry_intent is not None:
                derived_position_record, derived_plan_record = build_model_derived_risk_records(
                    market=market,
                    base_currency=str(market).split("-")[-1],
                    base_amount=qty,
                    avg_entry_price=entry_price,
                    plan_payload=entry_intent["plan_payload"],
                    created_ts=int(entry_intent["created_ts"]),
                    updated_ts=int(ts_ms),
                    intent_id=entry_intent["intent_id"],
                )
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id=str(plan.get("plan_id")),
                market=market,
                side=str(plan.get("side", "long") or "long"),
                entry_price_str=str(entry_price),
                qty_str=str(qty),
                tp_enabled=bool(derived_plan_record.tp_enabled) if derived_plan_record is not None else bool(plan_tp.get("enabled")),
                tp_price_str=derived_plan_record.tp_price_str if derived_plan_record is not None else _as_optional_str(plan_tp.get("tp_price_str")),
                tp_pct=derived_plan_record.tp_pct if derived_plan_record is not None else _safe_optional_float(plan_tp.get("tp_pct")),
                sl_enabled=bool(derived_plan_record.sl_enabled) if derived_plan_record is not None else bool(plan_sl.get("enabled")),
                sl_price_str=derived_plan_record.sl_price_str if derived_plan_record is not None else _as_optional_str(plan_sl.get("sl_price_str")),
                sl_pct=derived_plan_record.sl_pct if derived_plan_record is not None else _safe_optional_float(plan_sl.get("sl_pct")),
                trailing_enabled=bool(derived_plan_record.trailing_enabled) if derived_plan_record is not None else bool(plan_trailing.get("enabled")),
                trail_pct=derived_plan_record.trail_pct if derived_plan_record is not None else _safe_optional_float(plan_trailing.get("trail_pct")),
                high_watermark_price_str=_as_optional_str(plan_trailing.get("high_watermark_price_str")) or (
                    derived_plan_record.high_watermark_price_str if derived_plan_record is not None else None
                ),
                armed_ts_ms=_safe_optional_int(plan_trailing.get("armed_ts_ms")) if _safe_optional_int(plan_trailing.get("armed_ts_ms")) is not None else (
                    derived_plan_record.armed_ts_ms if derived_plan_record is not None else None
                ),
                timeout_ts_ms=derived_plan_record.timeout_ts_ms if derived_plan_record is not None else timeout_ts_ms,
                state=str(plan.get("state", "ACTIVE") or "ACTIVE"),
                last_eval_ts_ms=int(plan.get("last_eval_ts_ms", 0) or 0),
                last_action_ts_ms=int(plan.get("last_action_ts_ms", 0) or 0),
                current_exit_order_uuid=_as_optional_str(plan.get("current_exit_order_uuid")),
                current_exit_order_identifier=_as_optional_str(plan.get("current_exit_order_identifier")),
                replace_attempt=int(plan.get("replace_attempt", 0) or 0),
                created_ts=int(plan.get("created_ts", ts_ms) or ts_ms),
                updated_ts=int(ts_ms),
                plan_source=derived_plan_record.plan_source if derived_plan_record is not None else plan_source,
                source_intent_id=derived_plan_record.source_intent_id if derived_plan_record is not None else source_intent_id,
            )
        )
        if derived_position_record is not None:
            store.upsert_position(derived_position_record)


def _risk_plan_differs_from_record(*, plan: dict[str, Any], record: RiskPlanRecord) -> bool:
    plan_tp = dict(plan.get("tp") or {})
    plan_sl = dict(plan.get("sl") or {})
    plan_trailing = dict(plan.get("trailing") or {})
    if _as_optional_str(plan.get("plan_source")) != record.plan_source:
        return True
    if _as_optional_str(plan.get("source_intent_id")) != record.source_intent_id:
        return True
    if _safe_optional_int(plan.get("timeout_ts_ms")) != record.timeout_ts_ms:
        return True
    if bool(plan_tp.get("enabled")) != bool(record.tp_enabled):
        return True
    if _safe_optional_float(plan_tp.get("tp_pct")) != _safe_optional_float(record.tp_pct):
        return True
    if bool(plan_sl.get("enabled")) != bool(record.sl_enabled):
        return True
    if _safe_optional_float(plan_sl.get("sl_pct")) != _safe_optional_float(record.sl_pct):
        return True
    if bool(plan_trailing.get("enabled")) != bool(record.trailing_enabled):
        return True
    if _safe_optional_float(plan_trailing.get("trail_pct")) != _safe_optional_float(record.trail_pct):
        return True
    return False


def _close_market_risk_plans(*, store: LiveStateStore, market: str, ts_ms: int) -> None:
    for plan in store.list_risk_plans(market=market):
        if str(plan.get("state", "")).strip().upper() == "CLOSED":
            continue
        plan_tp = dict(plan.get("tp") or {})
        plan_sl = dict(plan.get("sl") or {})
        plan_trailing = dict(plan.get("trailing") or {})
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id=str(plan.get("plan_id")),
                market=market,
                side=str(plan.get("side", "long") or "long"),
                entry_price_str=str(plan.get("entry_price_str", "")),
                qty_str=str(plan.get("qty_str", "")),
                tp_enabled=bool(plan_tp.get("enabled")),
                tp_price_str=_as_optional_str(plan_tp.get("tp_price_str")),
                tp_pct=_safe_optional_float(plan_tp.get("tp_pct")),
                sl_enabled=bool(plan_sl.get("enabled")),
                sl_price_str=_as_optional_str(plan_sl.get("sl_price_str")),
                sl_pct=_safe_optional_float(plan_sl.get("sl_pct")),
                trailing_enabled=bool(plan_trailing.get("enabled")),
                trail_pct=_safe_optional_float(plan_trailing.get("trail_pct")),
                high_watermark_price_str=_as_optional_str(plan_trailing.get("high_watermark_price_str")),
                armed_ts_ms=_safe_optional_int(plan_trailing.get("armed_ts_ms")),
                timeout_ts_ms=_safe_optional_int(plan.get("timeout_ts_ms")),
                state="CLOSED",
                last_eval_ts_ms=int(plan.get("last_eval_ts_ms", 0) or 0),
                last_action_ts_ms=int(ts_ms),
                current_exit_order_uuid=_as_optional_str(plan.get("current_exit_order_uuid")),
                current_exit_order_identifier=_as_optional_str(plan.get("current_exit_order_identifier")),
                replace_attempt=int(plan.get("replace_attempt", 0) or 0),
                created_ts=int(plan.get("created_ts", ts_ms) or ts_ms),
                updated_ts=int(ts_ms),
                plan_source=_as_optional_str(plan.get("plan_source")),
                source_intent_id=_as_optional_str(plan.get("source_intent_id")),
            )
        )


def _find_latest_model_entry_intent(
    *,
    store: LiveStateStore,
    market: str,
    position: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    market_value = str(market).strip().upper()
    intents_by_id: dict[str, dict[str, Any]] = {}
    for item in store.list_intents():
        if str(item.get("market", "")).strip().upper() != market_value:
            continue
        if str(item.get("side", "")).strip().lower() != "bid":
            continue
        if str(item.get("status", "")).strip().upper() != "SUBMITTED":
            continue
        meta = item.get("meta")
        if not isinstance(meta, dict):
            continue
        submit_result = meta.get("submit_result")
        if not isinstance(submit_result, dict) or not bool(submit_result.get("accepted")):
            continue
        plan_payload = extract_model_exit_plan(meta)
        if plan_payload is None:
            continue
        intent_id = str(item.get("intent_id") or "").strip()
        if not intent_id:
            continue
        intents_by_id[intent_id] = {
            "intent_id": intent_id,
            "created_ts": int(item.get("ts_ms") or 0),
            "plan_payload": plan_payload,
            "meta": dict(meta),
            "reason_code": str(item.get("reason_code") or ""),
            "submitted_price": _safe_optional_float(item.get("price")),
            "submitted_volume": _safe_optional_float(item.get("volume")),
        }
    if not intents_by_id:
        return None
    target_price = _safe_float((position or {}).get("avg_entry_price"), default=0.0)
    target_qty = _safe_float((position or {}).get("base_amount"), default=0.0)
    saw_market_bid_order = False
    best: tuple[tuple[int, float, float, int], dict[str, Any]] | None = None
    for order in store.list_orders(open_only=False):
        if str(order.get("market", "")).strip().upper() != market_value:
            continue
        if str(order.get("side", "")).strip().lower() != "bid":
            continue
        saw_market_bid_order = True
        intent_id = str(order.get("intent_id") or "").strip()
        candidate = intents_by_id.get(intent_id)
        if candidate is None:
            continue
        volume_filled = _safe_float(order.get("volume_filled"), default=0.0)
        local_state = str(order.get("local_state") or "").strip().upper()
        state = str(order.get("state") or "").strip().lower()
        has_fill = int(volume_filled > 0.0 or local_state in {"DONE", "PARTIAL"} or state == "done")
        if has_fill <= 0:
            continue
        matched_qty = volume_filled if volume_filled > 0.0 else (_safe_float(order.get("volume_req"), default=0.0) or 0.0)
        matched_price = _safe_float(order.get("price"), default=0.0) or 0.0
        price_delta = abs(matched_price - target_price) if target_price > 0.0 else 0.0
        qty_delta = abs(matched_qty - target_qty) if target_qty > 0.0 else 0.0
        sort_key = (
            has_fill,
            -qty_delta,
            -price_delta,
            int(order.get("updated_ts") or candidate["created_ts"] or 0),
        )
        if best is None or sort_key > best[0]:
            best = (
                sort_key,
                {
                    **candidate,
                    "order_uuid": str(order.get("uuid") or "").strip() or None,
                    "order_identifier": str(order.get("identifier") or "").strip() or None,
                    "matched_entry_price": matched_price,
                    "matched_qty": matched_qty,
                },
            )
    if best is not None:
        return best[1]
    if position is not None:
        if saw_market_bid_order:
            return None
        if len(intents_by_id) != 1:
            return None
    return max(intents_by_id.values(), key=lambda item: int(item.get("created_ts") or 0))


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
    market = str(strategy_intent.market).strip().upper()
    side = str(strategy_intent.side).strip().lower()
    if not market or side not in {"bid", "ask"}:
        return "skipped"
    canary_guard_reason = _canary_entry_guard_reason(
        store=store,
        settings=settings,
        market=market,
        side=side,
    )
    if canary_guard_reason:
        _record_strategy_intent(
            store=store,
            market=market,
            side=side,
            price=_safe_optional_float(getattr(strategy_intent, "ref_price", None)),
            volume=_safe_optional_float(getattr(strategy_intent, "volume", None)),
            reason_code=str(strategy_intent.reason_code),
            meta_payload={"skip_reason": canary_guard_reason},
            status="SKIPPED",
            ts_ms=ts_ms,
        )
        return "skipped"

    accounts_payload = client.accounts()
    chance_payload = client.chance(market=market)
    instruments_payload = instrument_cache.get(market)
    if instruments_payload is None:
        loaded = public_client.orderbook_instruments([market])
        if isinstance(loaded, list):
            for item in loaded:
                if isinstance(item, dict):
                    item_market = str(item.get("market", "")).strip().upper()
                    if item_market:
                        instrument_cache[item_market] = dict(item)
        instruments_payload = instrument_cache.get(market)
    if instruments_payload is None:
        _record_strategy_intent(
            store=store,
            market=market,
            side=side,
            price=strategy_intent.ref_price,
            volume=strategy_intent.volume,
            reason_code=str(strategy_intent.reason_code),
            meta_payload={"skip_reason": "MISSING_INSTRUMENTS"},
            status="SKIPPED",
            ts_ms=ts_ms,
        )
        return "skipped"

    snapshot = build_live_order_admissibility_snapshot(
        market=market,
        side=side,
        chance_payload=chance_payload if isinstance(chance_payload, dict) else {},
        instruments_payload=[instruments_payload],
        accounts_payload=accounts_payload,
        ts_ms=ts_ms,
    )
    exchange_view = _LiveTradeGateExchangeView(
        store=store,
        accounts_payload=accounts_payload,
        quote_currency=snapshot.quote_currency,
    )
    execution_resolution = _resolve_live_strategy_execution(
        market=market,
        side=side,
        settings=settings,
        model_alpha_settings=model_alpha_settings,
        strategy_intent=strategy_intent,
        snapshot=snapshot,
        latest_trade_price=max(_safe_float(latest_prices.get(market), default=0.0), 0.0),
        store=store,
        exchange_view=exchange_view,
        ts_ms=ts_ms,
        micro_snapshot_provider=micro_snapshot_provider,
        micro_order_policy=micro_order_policy,
        trade_gate=trade_gate,
    )
    if not execution_resolution.allowed:
        _record_strategy_intent(
            store=store,
            market=market,
            side=side,
            price=execution_resolution.requested_price,
            volume=execution_resolution.requested_volume,
            reason_code=str(strategy_intent.reason_code),
            meta_payload={**execution_resolution.meta_payload, "skip_reason": execution_resolution.skip_reason},
            status="SKIPPED",
            ts_ms=ts_ms,
        )
        return "skipped"
    decision = evaluate_live_limit_order(
        snapshot=snapshot,
        price=execution_resolution.requested_price,
        volume=max(float(execution_resolution.requested_volume), 1e-12),
        expected_edge_bps=_resolve_live_expected_edge_bps(execution_resolution.meta_payload),
    )
    record_small_account_decision(
        store=store,
        decision=decision,
        source="live_model_alpha_runtime",
        ts_ms=ts_ms,
        market=market,
    )
    admissibility_report = build_live_admissibility_report(
        snapshot=snapshot,
        decision=decision,
        sizing_payload=execution_resolution.sizing_payload,
    )
    meta_payload = dict(execution_resolution.meta_payload)
    meta_payload["runtime"] = {
        "live_runtime_model_run_id": predictor.run_dir.name,
        "model_family": settings.daemon.runtime_model_family,
    }
    meta_payload["admissibility"] = admissibility_report
    intent = new_order_intent(
        market=market,
        side=side,
        price=float(decision.adjusted_price),
        volume=float(decision.adjusted_volume),
        reason_code=str(strategy_intent.reason_code),
        ord_type="limit",
        time_in_force="gtc",
        meta=meta_payload,
        ts_ms=ts_ms,
    )
    if not decision.admissible:
        _record_strategy_intent(
            store=store,
            market=market,
            side=side,
            price=float(decision.adjusted_price),
            volume=float(decision.adjusted_volume),
            reason_code=str(strategy_intent.reason_code),
            meta_payload=meta_payload,
            status="REJECTED_ADMISSIBILITY",
            ts_ms=ts_ms,
            intent_id=intent.intent_id,
        )
        return "skipped"

    if not _order_emission_allowed(store):
        _record_strategy_intent(
            store=store,
            market=market,
            side=side,
            price=float(decision.adjusted_price),
            volume=float(decision.adjusted_volume),
            reason_code=str(strategy_intent.reason_code),
            meta_payload=meta_payload,
            status="SHADOW",
            ts_ms=ts_ms,
            intent_id=intent.intent_id,
        )
        store.set_checkpoint(
            name="live_model_alpha_last_shadow_intent",
            payload={"intent_id": intent.intent_id, "meta": meta_payload},
            ts_ms=ts_ms,
        )
        return "shadow"

    if executor_gateway is None:
        _record_strategy_intent(
            store=store,
            market=market,
            side=side,
            price=float(decision.adjusted_price),
            volume=float(decision.adjusted_volume),
            reason_code=str(strategy_intent.reason_code),
            meta_payload={**meta_payload, "skip_reason": "MISSING_EXECUTOR_GATEWAY"},
            status="SKIPPED",
            ts_ms=ts_ms,
            intent_id=intent.intent_id,
        )
        return "skipped"

    identifier = new_order_identifier(
        prefix=str(settings.daemon.identifier_prefix),
        bot_id=str(settings.daemon.bot_id),
        intent_id=intent.intent_id,
        ts_ms=ts_ms,
    )
    _record_strategy_intent(
        store=store,
        market=market,
        side=side,
        price=float(decision.adjusted_price),
        volume=float(decision.adjusted_volume),
        reason_code=str(strategy_intent.reason_code),
        meta_payload=meta_payload,
        status="SUBMITTING",
        ts_ms=ts_ms,
        intent_id=intent.intent_id,
    )
    result = executor_gateway.submit_intent(
        intent=intent,
        identifier=identifier,
        meta_json=json.dumps(meta_payload, ensure_ascii=False, sort_keys=True),
    )
    if bool(getattr(result, "accepted", False)):
        trade_gate.record_success(market)
        reset_counter(store, counter_name="rate_limit_error", source="live_model_alpha_submit_ok", ts_ms=ts_ms)
        reset_counter(store, counter_name="auth_error", source="live_model_alpha_submit_ok", ts_ms=ts_ms)
        reset_counter(store, counter_name="nonce_error", source="live_model_alpha_submit_ok", ts_ms=ts_ms)
        reset_counter(store, counter_name="replace_reject", source="live_model_alpha_submit_ok", ts_ms=ts_ms)
        order_uuid = _as_optional_str(getattr(result, "upbit_uuid", None)) or f"pending-{intent.intent_id}"
        linked_plan_id = None
        if side == "ask":
            linked_plan_id = _attach_exit_order_to_risk_plan(
                store=store,
                market=market,
                order_uuid=order_uuid,
                order_identifier=_as_optional_str(getattr(result, "identifier", None)) or identifier,
                ts_ms=ts_ms,
            )
        store.upsert_order(
            OrderRecord(
                uuid=order_uuid,
                identifier=_as_optional_str(getattr(result, "identifier", None)) or identifier,
                market=market,
                side=side,
                ord_type="limit",
                price=float(decision.adjusted_price),
                volume_req=float(decision.adjusted_volume),
                volume_filled=0.0,
                state="wait",
                created_ts=ts_ms,
                updated_ts=ts_ms,
                intent_id=intent.intent_id,
                tp_sl_link=linked_plan_id,
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="SUBMIT_ACCEPTED",
                event_source="live_model_alpha_runtime",
                replace_seq=0,
                root_order_uuid=order_uuid,
                prev_order_uuid=None,
                prev_order_identifier=None,
            )
        )
        _record_strategy_intent(
            store=store,
            market=market,
            side=side,
            price=float(decision.adjusted_price),
            volume=float(decision.adjusted_volume),
            reason_code=str(strategy_intent.reason_code),
            meta_payload={**meta_payload, "submit_result": {"accepted": True, "order_uuid": order_uuid}},
            status="SUBMITTED",
            ts_ms=ts_ms,
            intent_id=intent.intent_id,
        )
        if side == "bid":
            record_entry_submission(
                store=store,
                market=market,
                intent_id=str(intent.intent_id),
                requested_price=float(decision.adjusted_price),
                requested_volume=float(decision.adjusted_volume),
                reason_code=str(strategy_intent.reason_code),
                meta_payload={**meta_payload, "submit_result": {"accepted": True, "order_uuid": order_uuid}},
                ts_ms=ts_ms,
                order_uuid=order_uuid,
                plan_id=linked_plan_id,
            )
        return "submitted"

    trade_gate.record_failure(market, ts_ms=ts_ms)
    _handle_submit_reject(
        store=store,
        intent=intent,
        ts_ms=ts_ms,
        market=market,
        side=side,
        reason_code=str(strategy_intent.reason_code),
        meta_payload=meta_payload,
        reject_reason=str(getattr(result, "reason", "") or ""),
    )
    return "skipped"


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
    resolved_intent_id = str(intent_id or f"live-{market}-{side}-{ts_ms}").strip()
    store.upsert_intent(
        IntentRecord(
            intent_id=resolved_intent_id,
            ts_ms=int(ts_ms),
            market=market,
            side=side,
            price=price,
            volume=volume,
            reason_code=reason_code,
            meta_json=json.dumps(meta_payload, ensure_ascii=False, sort_keys=True),
            status=str(status).strip().upper(),
        )
    )
    return resolved_intent_id


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
    classified = classify_executor_reject_reason(reject_reason)
    if classified == "REPEATED_RATE_LIMIT_ERRORS":
        record_counter_failure(
            store,
            counter_name="rate_limit_error",
            limit=3,
            source="live_model_alpha_submit",
            ts_ms=ts_ms,
            details={"market": market, "reason": reject_reason},
        )
    elif classified == "REPEATED_AUTH_ERRORS":
        record_counter_failure(
            store,
            counter_name="auth_error",
            limit=2,
            source="live_model_alpha_submit",
            ts_ms=ts_ms,
            details={"market": market, "reason": reject_reason},
        )
    elif classified == "REPEATED_NONCE_ERRORS":
        record_counter_failure(
            store,
            counter_name="nonce_error",
            limit=2,
            source="live_model_alpha_submit",
            ts_ms=ts_ms,
            details={"market": market, "reason": reject_reason},
        )
    elif classified == "IDENTIFIER_COLLISION":
        arm_breaker(
            store,
            reason_codes=["IDENTIFIER_COLLISION"],
            source="live_model_alpha_submit",
            ts_ms=ts_ms,
            action=ACTION_FULL_KILL_SWITCH,
            details={"market": market, "reason": reject_reason},
        )
    _record_strategy_intent(
        store=store,
        market=market,
        side=side,
        price=float(intent.price),
        volume=float(intent.volume),
        reason_code=reason_code,
        meta_payload={**meta_payload, "submit_result": {"accepted": False, "reason": reject_reason}},
        status="REJECTED",
        ts_ms=ts_ms,
        intent_id=str(intent.intent_id),
    )
    store.set_checkpoint(
        name="live_model_alpha_last_reject",
        payload={
            "intent_id": str(intent.intent_id),
            "market": market,
            "side": side,
            "reason": reject_reason,
            "classified_reject": classified,
        },
        ts_ms=ts_ms,
    )


def _apply_canary_notional_cap(
    *,
    store: LiveStateStore,
    settings: LiveModelAlphaRuntimeSettings,
    target_notional_quote: float,
) -> float:
    resolved_target = max(float(target_notional_quote), 0.0)
    rollout_contract = store.live_rollout_contract() or {}
    rollout_status = store.live_rollout_status() or {}
    rollout_mode = (
        str(rollout_status.get("mode") or rollout_contract.get("mode") or settings.daemon.rollout_mode)
        .strip()
        .lower()
    )
    if rollout_mode != "canary":
        return resolved_target
    contract_target_unit = str(rollout_contract.get("target_unit") or "").strip()
    if contract_target_unit and contract_target_unit != str(settings.daemon.rollout_target_unit).strip():
        return resolved_target
    cap_value = _safe_optional_float(rollout_contract.get("canary_max_notional_quote"))
    if cap_value is None or cap_value <= 0.0:
        return resolved_target
    return min(resolved_target, float(cap_value))


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
) -> _LiveStrategyExecutionResolution:
    strategy_meta = dict(strategy_intent.meta or {})
    initial_ref_price = max(float(strategy_intent.ref_price), 1e-12)
    effective_ref_price = max(initial_ref_price, float(latest_trade_price), 1e-12)
    snapshot_for_policy = micro_snapshot_provider.get(market, int(ts_ms))
    exec_profile = _strategy_live_exec_profile(
        settings=settings,
        model_alpha_settings=model_alpha_settings,
    )
    operational_payload: dict[str, Any] = {}
    trade_gate_payload: dict[str, Any] = {"enabled": True}
    forced_volume = _safe_optional_float(strategy_meta.get("force_volume"))
    local_position = store.position_by_market(market=market) if side == "ask" else None
    entry_notional_quote = (
        _entry_notional_quote_for_strategy(
            strategy_mode="model_alpha_v1",
            per_trade_krw=float(settings.per_trade_krw),
            min_total_krw=max(float(snapshot.min_total), float(settings.min_order_krw)),
            model_alpha_settings=model_alpha_settings,
            candidate_meta=strategy_meta,
        )
        if side == "bid" and (forced_volume is None or forced_volume <= 0)
        else None
    )
    if entry_notional_quote is not None:
        entry_notional_quote = _apply_canary_notional_cap(
            store=store,
            settings=settings,
            target_notional_quote=float(entry_notional_quote),
        )

    if bool(model_alpha_settings.operational.enabled):
        operational_decision = resolve_operational_execution_overlay(
            base_profile=exec_profile,
            settings=model_alpha_settings.operational,
            micro_quality=compute_micro_quality_composite(
                micro_snapshot=snapshot_for_policy,
                now_ts_ms=ts_ms,
                settings=model_alpha_settings.operational,
            ),
            ts_ms=ts_ms,
        )
        operational_payload = {
            "runtime_risk_multiplier": float(operational_decision.risk_multiplier),
            "exec_overlay_mode": str(operational_decision.diagnostics.get("mode", "neutral")),
            "micro_quality_score": (
                float(operational_decision.micro_quality.score)
                if operational_decision.micro_quality is not None
                else None
            ),
            "diagnostics": dict(operational_decision.diagnostics),
        }
        if operational_decision.abort_reason is not None:
            return _LiveStrategyExecutionResolution(
                allowed=False,
                skip_reason=str(operational_decision.abort_reason),
                requested_price=effective_ref_price,
                requested_volume=_safe_optional_float(strategy_intent.volume),
                sizing_payload=None,
                meta_payload={
                    "strategy": {
                        "market": market,
                        "side": side,
                        "reason_code": str(strategy_intent.reason_code),
                        "score": strategy_intent.score,
                        "prob": strategy_intent.prob,
                        "meta": strategy_meta,
                    },
                    "execution": {
                        "initial_ref_price": float(initial_ref_price),
                        "latest_trade_price": float(latest_trade_price),
                        "effective_ref_price": float(effective_ref_price),
                        "exec_profile": order_exec_profile_to_dict(exec_profile),
                    },
                    "operational_overlay": operational_payload,
                },
            )
        exec_profile = operational_decision.exec_profile
        if entry_notional_quote is not None:
            entry_notional_quote *= max(float(operational_decision.risk_multiplier), 0.0)

    if side == "ask":
        if local_position is None:
            return _LiveStrategyExecutionResolution(
                allowed=False,
                skip_reason="NO_LOCAL_POSITION",
                requested_price=effective_ref_price,
                requested_volume=None,
                sizing_payload=None,
                meta_payload={"strategy": {"market": market, "side": side, "meta": strategy_meta}},
            )
        if exchange_view.has_open_order(market, side="ask"):
            return _LiveStrategyExecutionResolution(
                allowed=False,
                skip_reason="DUPLICATE_EXIT_ORDER",
                requested_price=effective_ref_price,
                requested_volume=None,
                sizing_payload=None,
                meta_payload={
                    "strategy": {"market": market, "side": side, "meta": strategy_meta},
                    "trade_gate": {
                        **trade_gate_payload,
                        "reason_code": "DUPLICATE_EXIT_ORDER",
                        "severity": "BLOCK",
                        "gate_reasons": ["DUPLICATE_EXIT_ORDER"],
                        "diagnostics": {},
                    },
                },
            )
        if forced_volume is None or forced_volume <= 0:
            forced_volume = max(_safe_float(local_position.get("base_amount"), default=0.0), 0.0)

    gate_price = round_price_to_tick(
        price=effective_ref_price,
        tick_size=float(snapshot.tick_size),
        side=side,
    )
    gate_volume = (
        float(forced_volume)
        if forced_volume is not None and forced_volume > 0
        else max(float(entry_notional_quote or settings.per_trade_krw), 1.0) / max(float(gate_price), 1e-12)
    )
    if gate_volume <= 0:
        return _LiveStrategyExecutionResolution(
            allowed=False,
            skip_reason="ZERO_VOLUME",
            requested_price=gate_price,
            requested_volume=gate_volume,
            sizing_payload=None,
            meta_payload={
                "strategy": {"market": market, "side": side, "meta": strategy_meta},
                "trade_gate": {
                    **trade_gate_payload,
                    "reason_code": "ZERO_VOLUME",
                    "severity": "BLOCK",
                    "gate_reasons": ["ZERO_VOLUME"],
                    "diagnostics": {},
                },
            },
        )
    fee_rate = float(snapshot.bid_fee if side == "bid" else snapshot.ask_fee)
    trade_gate_decision = trade_gate.evaluate(
        ts_ms=ts_ms,
        market=market,
        side=side,
        price=gate_price,
        volume=gate_volume,
        fee_rate=fee_rate,
        exchange=exchange_view,
        min_total_krw=float(snapshot.min_total),
    )
    trade_gate_payload = {
        **trade_gate_payload,
        "reason_code": str(trade_gate_decision.reason_code),
        "severity": str(trade_gate_decision.severity),
        "gate_reasons": list(trade_gate_decision.gate_reasons),
        "diagnostics": dict(trade_gate_decision.diagnostics or {}),
        "gate_price": float(gate_price),
        "gate_volume": float(gate_volume),
    }
    if not trade_gate_decision.allowed:
        return _LiveStrategyExecutionResolution(
            allowed=False,
            skip_reason=str(trade_gate_decision.reason_code),
            requested_price=gate_price,
            requested_volume=gate_volume,
            sizing_payload=None,
            meta_payload={
                "strategy": {
                    "market": market,
                    "side": side,
                    "reason_code": str(strategy_intent.reason_code),
                    "score": strategy_intent.score,
                    "prob": strategy_intent.prob,
                    "meta": strategy_meta,
                },
                "execution": {
                    "initial_ref_price": float(initial_ref_price),
                    "latest_trade_price": float(latest_trade_price),
                    "effective_ref_price": float(effective_ref_price),
                    "exec_profile": order_exec_profile_to_dict(exec_profile),
                },
                "trade_gate": trade_gate_payload,
                "operational_overlay": operational_payload,
            },
        )

    policy_diagnostics: dict[str, Any] = {}
    policy_payload = {
        "enabled": bool(micro_order_policy is not None),
        "tier": None,
        "reason_code": "POLICY_DISABLED",
    }
    if micro_order_policy is not None:
        policy_decision = micro_order_policy.evaluate(
            micro_snapshot=snapshot_for_policy,
            base_profile=exec_profile,
            market=market,
            ref_price=effective_ref_price,
            tick_size=float(snapshot.tick_size),
            replace_attempt=0,
            model_prob=_safe_optional_float(strategy_meta.get("model_prob")),
            now_ts_ms=ts_ms,
        )
        policy_diagnostics = dict(policy_decision.diagnostics or {})
        policy_payload = {
            "enabled": True,
            "tier": str(policy_decision.tier) if policy_decision.tier is not None else None,
            "reason_code": str(policy_decision.reason_code),
        }
        if not policy_decision.allow:
            return _LiveStrategyExecutionResolution(
                allowed=False,
                skip_reason=str(policy_decision.reason_code),
                requested_price=effective_ref_price,
                requested_volume=_safe_optional_float(strategy_intent.volume),
                sizing_payload=None,
                meta_payload={
                    "strategy": {
                        "market": market,
                        "side": side,
                        "reason_code": str(strategy_intent.reason_code),
                        "score": strategy_intent.score,
                        "prob": strategy_intent.prob,
                        "meta": strategy_meta,
                    },
                    "execution": {
                        "initial_ref_price": float(initial_ref_price),
                        "latest_trade_price": float(latest_trade_price),
                        "effective_ref_price": float(effective_ref_price),
                        "exec_profile": order_exec_profile_to_dict(exec_profile),
                    },
                    "micro_order_policy": policy_payload,
                    "micro_diagnostics": policy_diagnostics,
                    "operational_overlay": operational_payload,
                },
            )
        if policy_decision.profile is not None:
            exec_profile = policy_decision.profile

    requested_price = build_limit_price_from_mode(
        side=side,
        ref_price=effective_ref_price,
        tick_size=float(snapshot.tick_size),
        price_mode=exec_profile.price_mode,
    )
    sizing_payload: dict[str, Any] | None = None
    requested_volume = _safe_optional_float(strategy_intent.volume)
    if side == "bid":
        target_notional_quote = float(entry_notional_quote or settings.per_trade_krw)
        sizing = derive_volume_from_target_notional(
            side="bid",
            price=requested_price,
            target_notional_quote=float(target_notional_quote),
            fee_rate=max(float(snapshot.bid_fee), 0.0),
        )
        sizing_payload = sizing_envelope_to_payload(sizing)
        requested_volume = max(float(sizing.admissible_volume), 1e-12)
    else:
        requested_volume = max(_safe_float(local_position.get("base_amount"), default=0.0), 0.0)
        if requested_volume <= 0:
            return _LiveStrategyExecutionResolution(
                allowed=False,
                skip_reason="ZERO_LOCAL_POSITION",
                requested_price=requested_price,
                requested_volume=requested_volume,
                sizing_payload=None,
                meta_payload={"strategy": {"market": market, "side": side, "meta": strategy_meta}},
            )

    return _LiveStrategyExecutionResolution(
        allowed=True,
        skip_reason=None,
        requested_price=requested_price,
        requested_volume=requested_volume,
        sizing_payload=sizing_payload,
        meta_payload={
            "strategy": {
                "market": market,
                "side": side,
                "reason_code": str(strategy_intent.reason_code),
                "score": strategy_intent.score,
                "prob": strategy_intent.prob,
                "meta": strategy_meta,
            },
            "execution": {
                "initial_ref_price": float(initial_ref_price),
                "latest_trade_price": float(latest_trade_price),
                "effective_ref_price": float(effective_ref_price),
                "requested_price": float(requested_price),
                "exec_profile": order_exec_profile_to_dict(exec_profile),
            },
            "micro_order_policy": policy_payload,
            "micro_diagnostics": policy_diagnostics,
            "trade_gate": trade_gate_payload,
            "operational_overlay": operational_payload,
        },
    )


def _strategy_live_exec_profile(
    *,
    settings: LiveModelAlphaRuntimeSettings,
    model_alpha_settings: ModelAlphaSettings,
) -> OrderExecProfile:
    interval_ms = _interval_ms_from_tf(settings.tf)
    timeout_ms = max(int(model_alpha_settings.execution.timeout_bars), 1) * interval_ms
    return make_legacy_exec_profile(
        timeout_ms=timeout_ms,
        replace_interval_ms=timeout_ms,
        max_replaces=max(int(model_alpha_settings.execution.replace_max), 0),
        price_mode=str(model_alpha_settings.execution.price_mode),
        max_chase_bps=10_000,
        min_replace_interval_ms_global=1_500,
    )


def _effective_live_trade_gate_max_positions(settings: LiveModelAlphaRuntimeSettings) -> int:
    max_positions = max(int(settings.max_positions), 1)
    if bool(settings.daemon.small_account_canary_enabled):
        max_positions = min(max_positions, max(int(settings.daemon.small_account_max_positions), 1))
    return max_positions


def _order_emission_allowed(store: LiveStateStore) -> bool:
    rollout_status = store.live_rollout_status() or {}
    return bool(rollout_status.get("order_emission_allowed")) and bool(new_intents_allowed(store))


def _resolve_live_expected_edge_bps(meta_payload: dict[str, Any] | None) -> float | None:
    if not isinstance(meta_payload, dict):
        return None
    strategy_meta = (
        ((meta_payload.get("strategy") or {}).get("meta"))
        if isinstance(meta_payload.get("strategy"), dict)
        else None
    )
    if not isinstance(strategy_meta, dict):
        return None
    trade_action = strategy_meta.get("trade_action")
    if not isinstance(trade_action, dict):
        return None
    raw_edge = _safe_optional_float(trade_action.get("expected_edge"))
    if raw_edge is None:
        return None
    return float(raw_edge) * 10_000.0


def _ingest_live_micro_from_ticker(
    *,
    provider: LiveWsMicroSnapshotProvider,
    ticker: TickerEvent,
) -> None:
    market = str(ticker.market).strip().upper()
    if not market or float(ticker.trade_price) <= 0:
        return
    volume = max(float(ticker.acc_trade_price_24h) / max(float(ticker.trade_price), 1e-8) * 1e-12, 1e-12)
    provider.ingest_trade(
        {
            "market": market,
            "ts_ms": int(ticker.ts_ms),
            "trade_ts_ms": int(ticker.ts_ms),
            "price": float(ticker.trade_price),
            "volume": float(volume),
            "ask_bid": "BID",
        }
    )


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
