"""Live model_alpha runtime sharing the same public data contract as paper."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Sequence

from autobot.backtest.strategy_adapter import StrategyFillEvent, StrategyOrderIntent
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
from autobot.strategy.micro_snapshot import LiveWsMicroSnapshotProvider, LiveWsProviderSettings
from autobot.strategy.model_alpha_v1 import ModelAlphaSettings, ModelAlphaStrategyV1
from autobot.upbit.ws.models import TickerEvent

from .admissibility import (
    build_live_admissibility_report,
    build_live_order_admissibility_snapshot,
    evaluate_live_limit_order,
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
from .reconcile import resume_risk_plans_after_reconcile
from .risk_loop import apply_ticker_event
from .small_account import (
    build_small_account_runtime_report,
    derive_volume_from_target_notional,
    record_small_account_decision,
    sizing_envelope_to_payload,
)
from .state_store import IntentRecord, LiveStateStore, OrderRecord, RiskPlanRecord


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
    min_order_krw: float = 5_000.0
    model_alpha: ModelAlphaSettings = field(default_factory=ModelAlphaSettings)
    paper_live_parquet_root: str = "data/parquet"
    paper_live_candles_dataset: str = "candles_api_v1"
    paper_live_bootstrap_1m_bars: int = 2_000
    paper_live_micro_max_age_ms: int = 300_000
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
    markets = _load_quote_markets(public_client=public_client, quote=str(settings.quote))
    if not markets:
        raise RuntimeError(f"no quote markets available for quote={settings.quote}")
    instrument_cache = _load_market_instruments(public_client=public_client, markets=markets)

    micro_provider = LiveWsMicroSnapshotProvider(
        LiveWsProviderSettings(enabled=True, max_markets=max(len(markets), int(settings.top_n)))
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
    risk_manager = _build_risk_manager(store=store, settings=settings, executor_gateway=executor_gateway)
    market_data = MarketDataHub(history_sec=300)
    universe = UniverseProviderTop20(
        quote=str(settings.quote).strip().upper(),
        top_n=max(int(settings.top_n), 1),
        refresh_sec=max(float(settings.universe_refresh_sec), 1.0),
        hold_sec=max(float(settings.universe_hold_sec), 0.0),
    )
    known_positions = _snapshot_position_state(store)
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
                        open_markets=set(_snapshot_position_state(store).keys()),
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
                            strategy_intent=strategy_intent,
                            instrument_cache=instrument_cache,
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
) -> LiveRiskManager | None:
    if not bool(settings.risk_enabled):
        return None
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


def _bootstrap_strategy_positions(
    *,
    store: LiveStateStore,
    strategy: ModelAlphaStrategyV1,
    risk_manager: LiveRiskManager | None,
    known_positions: dict[str, dict[str, Any]],
    ts_ms: int,
) -> None:
    for market, payload in known_positions.items():
        _strategy_bid_fill(strategy=strategy, market=market, position=payload, ts_ms=ts_ms)
        _ensure_default_risk_plan(store=store, risk_manager=risk_manager, market=market, position=payload, ts_ms=ts_ms)


def _apply_position_sync_to_strategy(
    *,
    store: LiveStateStore,
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
        _strategy_bid_fill(strategy=strategy, market=market, position=payload, ts_ms=ts_ms)
        _ensure_default_risk_plan(store=store, risk_manager=risk_manager, market=market, position=payload, ts_ms=ts_ms)

    for market in sorted(previous_markets - current_markets):
        previous = previous_positions[market]
        exit_price = _safe_float(latest_prices.get(market), default=0.0)
        if exit_price <= 0:
            exit_price = _safe_float(previous.get("avg_entry_price"), default=1.0)
        _strategy_ask_fill(strategy=strategy, market=market, position=previous, exit_price=exit_price, ts_ms=ts_ms)
        _close_market_risk_plans(store=store, market=market, ts_ms=ts_ms)

    for market in sorted(current_markets & previous_markets):
        _ensure_default_risk_plan(
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
        )
    )


def _ensure_default_risk_plan(
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
    if not live_plans:
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
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id=str(plan.get("plan_id")),
                market=market,
                side=str(plan.get("side", "long") or "long"),
                entry_price_str=str(entry_price),
                qty_str=str(qty),
                tp_enabled=bool(plan.get("tp_enabled")),
                tp_price_str=_as_optional_str(plan.get("tp_price_str")),
                tp_pct=_safe_optional_float(plan.get("tp_pct")),
                sl_enabled=bool(plan.get("sl_enabled")),
                sl_price_str=_as_optional_str(plan.get("sl_price_str")),
                sl_pct=_safe_optional_float(plan.get("sl_pct")),
                trailing_enabled=bool(plan.get("trailing_enabled")),
                trail_pct=_safe_optional_float(plan.get("trail_pct")),
                high_watermark_price_str=_as_optional_str(plan.get("high_watermark_price_str")),
                armed_ts_ms=_safe_optional_int(plan.get("armed_ts_ms")),
                state=str(plan.get("state", "ACTIVE") or "ACTIVE"),
                last_eval_ts_ms=int(plan.get("last_eval_ts_ms", 0) or 0),
                last_action_ts_ms=int(plan.get("last_action_ts_ms", 0) or 0),
                current_exit_order_uuid=_as_optional_str(plan.get("current_exit_order_uuid")),
                current_exit_order_identifier=_as_optional_str(plan.get("current_exit_order_identifier")),
                replace_attempt=int(plan.get("replace_attempt", 0) or 0),
                created_ts=int(plan.get("created_ts", ts_ms) or ts_ms),
                updated_ts=int(ts_ms),
            )
        )


def _close_market_risk_plans(*, store: LiveStateStore, market: str, ts_ms: int) -> None:
    for plan in store.list_risk_plans(market=market):
        if str(plan.get("state", "")).strip().upper() == "CLOSED":
            continue
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id=str(plan.get("plan_id")),
                market=market,
                side=str(plan.get("side", "long") or "long"),
                entry_price_str=str(plan.get("entry_price_str", "")),
                qty_str=str(plan.get("qty_str", "")),
                tp_enabled=bool(plan.get("tp_enabled")),
                tp_price_str=_as_optional_str(plan.get("tp_price_str")),
                tp_pct=_safe_optional_float(plan.get("tp_pct")),
                sl_enabled=bool(plan.get("sl_enabled")),
                sl_price_str=_as_optional_str(plan.get("sl_price_str")),
                sl_pct=_safe_optional_float(plan.get("sl_pct")),
                trailing_enabled=bool(plan.get("trailing_enabled")),
                trail_pct=_safe_optional_float(plan.get("trail_pct")),
                high_watermark_price_str=_as_optional_str(plan.get("high_watermark_price_str")),
                armed_ts_ms=_safe_optional_int(plan.get("armed_ts_ms")),
                state="CLOSED",
                last_eval_ts_ms=int(plan.get("last_eval_ts_ms", 0) or 0),
                last_action_ts_ms=int(ts_ms),
                current_exit_order_uuid=_as_optional_str(plan.get("current_exit_order_uuid")),
                current_exit_order_identifier=_as_optional_str(plan.get("current_exit_order_identifier")),
                replace_attempt=int(plan.get("replace_attempt", 0) or 0),
                created_ts=int(plan.get("created_ts", ts_ms) or ts_ms),
                updated_ts=int(ts_ms),
            )
        )


def _handle_strategy_intent(
    *,
    store: LiveStateStore,
    client: Any,
    public_client: Any,
    executor_gateway: Any | None,
    settings: LiveModelAlphaRuntimeSettings,
    predictor: ModelPredictor,
    strategy_intent: StrategyOrderIntent,
    instrument_cache: dict[str, dict[str, Any]],
    ts_ms: int,
) -> str:
    market = str(strategy_intent.market).strip().upper()
    side = str(strategy_intent.side).strip().lower()
    if not market or side not in {"bid", "ask"}:
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

    requested_price = max(float(strategy_intent.ref_price), 1e-12)
    sizing_payload: dict[str, Any] | None = None
    requested_volume = _safe_optional_float(strategy_intent.volume)
    if side == "bid":
        fee_rate = _safe_optional_float((chance_payload or {}).get("bid_fee")) or 0.0
        target_notional_quote = _entry_notional_quote_for_strategy(
            strategy_mode="model_alpha_v1",
            per_trade_krw=float(settings.per_trade_krw),
            min_total_krw=float(settings.min_order_krw),
            model_alpha_settings=settings.model_alpha,
            candidate_meta=dict(strategy_intent.meta or {}),
        )
        sizing = derive_volume_from_target_notional(
            side="bid",
            price=requested_price,
            target_notional_quote=float(target_notional_quote),
            fee_rate=fee_rate,
        )
        sizing_payload = sizing_envelope_to_payload(sizing)
        requested_volume = max(float(sizing.admissible_volume), 1e-12)
    else:
        local_position = store.position_by_market(market=market)
        if local_position is None:
            _record_strategy_intent(
                store=store,
                market=market,
                side=side,
                price=requested_price,
                volume=None,
                reason_code=str(strategy_intent.reason_code),
                meta_payload={"skip_reason": "NO_LOCAL_POSITION"},
                status="SKIPPED",
                ts_ms=ts_ms,
            )
            return "skipped"
        requested_volume = max(_safe_float(local_position.get("base_amount"), default=0.0), 0.0)
        if requested_volume <= 0:
            _record_strategy_intent(
                store=store,
                market=market,
                side=side,
                price=requested_price,
                volume=requested_volume,
                reason_code=str(strategy_intent.reason_code),
                meta_payload={"skip_reason": "ZERO_LOCAL_POSITION"},
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
    decision = evaluate_live_limit_order(
        snapshot=snapshot,
        price=requested_price,
        volume=max(float(requested_volume), 1e-12),
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
        sizing_payload=sizing_payload,
    )
    meta_payload = {
        "strategy": {
            "market": market,
            "side": side,
            "reason_code": str(strategy_intent.reason_code),
            "score": strategy_intent.score,
            "prob": strategy_intent.prob,
            "meta": dict(strategy_intent.meta or {}),
        },
        "runtime": {
            "live_runtime_model_run_id": predictor.run_dir.name,
            "model_family": settings.daemon.runtime_model_family,
        },
        "admissibility": admissibility_report,
    }
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
        reset_counter(store, counter_name="rate_limit_error", source="live_model_alpha_submit_ok", ts_ms=ts_ms)
        reset_counter(store, counter_name="auth_error", source="live_model_alpha_submit_ok", ts_ms=ts_ms)
        reset_counter(store, counter_name="nonce_error", source="live_model_alpha_submit_ok", ts_ms=ts_ms)
        reset_counter(store, counter_name="replace_reject", source="live_model_alpha_submit_ok", ts_ms=ts_ms)
        order_uuid = _as_optional_str(getattr(result, "upbit_uuid", None)) or f"pending-{intent.intent_id}"
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
                tp_sl_link=None,
                local_state="OPEN_WORKING",
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
        return "submitted"

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


def _order_emission_allowed(store: LiveStateStore) -> bool:
    rollout_status = store.live_rollout_status() or {}
    return bool(rollout_status.get("order_emission_allowed")) and bool(new_intents_allowed(store))


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
