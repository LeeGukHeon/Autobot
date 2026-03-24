"""Live paper-run engine (WS Top20 -> Candidate -> TradeGate -> PaperExecution)."""

from __future__ import annotations

import asyncio
from collections import deque
import csv
from dataclasses import asdict, dataclass, field
import json
import os
from pathlib import Path
import time
from typing import Any, Callable, Sequence

from autobot.backtest.strategy_adapter import StrategyFillEvent, StrategyOrderIntent
from autobot.common.execution_structure import summarize_fill_records
from autobot.common.event_store import JsonlEventStore
from autobot.execution.intent import OrderIntent, new_order_intent
from autobot.execution.order_supervisor import (
    PRICE_MODE_CROSS_1T,
    PRICE_MODE_JOIN,
    REASON_CHASE_LIMIT_EXCEEDED,
    REASON_MAX_REPLACES_REACHED,
    REASON_MIN_NOTIONAL_DUST_ABORT,
    REASON_TIMEOUT_REPLACE,
    SUPERVISOR_ACTION_ABORT,
    SUPERVISOR_ACTION_REPLACE,
    OrderExecProfile,
    SupervisorAction,
    build_limit_price_from_mode,
    evaluate_supervisor_action,
    make_legacy_exec_profile,
    mean,
    normalize_order_exec_profile,
    order_exec_profile_from_dict,
    order_exec_profile_to_dict,
    percentile,
    slippage_bps,
)
from autobot.models.dataset_loader import DatasetRequest, iter_feature_rows_grouped_by_ts
from autobot.models.live_execution_policy import (
    DEFAULT_EXECUTION_CONTRACT_ARTIFACT_PATH,
    build_execution_policy_state,
    candidate_action_codes_for_price_mode,
    load_live_execution_contract_artifact,
    select_live_execution_action,
)
from autobot.models.predictor import load_predictor_from_registry
from autobot.strategy.candidates_v1 import Candidate, CandidateGeneratorV1, CandidateSettings
from autobot.strategy.micro_gate_v1 import MicroGateSettings, MicroGateV1
from autobot.strategy.micro_order_policy import (
    REASON_MICRO_MISSING_FALLBACK,
    MicroOrderPolicySettings,
    MicroOrderPolicyV1,
)
from autobot.strategy.micro_snapshot import (
    LiveWsMicroSnapshotProvider,
    MicroSnapshotProvider,
    OfflineMicroSnapshotProvider,
)
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
from autobot.strategy.top20_scanner import TopTradeValueScanner
from autobot.strategy.trade_gate_v1 import GateSettings, TradeGateV1
from autobot.upbit import (
    UpbitCredentials,
    UpbitError,
    UpbitHttpClient,
    UpbitPrivateClient,
    UpbitPublicClient,
    UpbitSettings,
    load_upbit_credentials,
)
from autobot.upbit.ws import UpbitWebSocketPublicClient
from autobot.upbit.ws.models import OrderbookEvent, TickerEvent, TradeEvent

from .live_features_v3 import LiveFeatureProviderV3
from .live_features_v4 import LiveFeatureProviderV4
from .live_features_v4_native import LiveFeatureProviderV4Native
from .rolling_evidence import compute_rolling_paper_evidence
from .run_id import build_paper_run_id
from .sim_exchange import (
    FillEvent,
    MarketRules,
    PaperOrder,
    PaperSimExchange,
    order_volume_from_notional,
    round_price_to_tick,
)


@dataclass(frozen=True)
class PaperRunSettings:
    duration_sec: int = 600  # 0 means run until externally stopped
    quote: str = "KRW"
    top_n: int = 20
    tf: str = "5m"
    print_every_sec: float = 5.0
    decision_interval_sec: float = 1.0
    universe_refresh_sec: float = 60.0
    universe_hold_sec: float = 120.0
    momentum_window_sec: int = 60
    min_momentum_pct: float = 0.2
    starting_krw: float = 50000.0
    per_trade_krw: float = 10000.0
    max_positions: int = 2
    min_order_krw: float = 5000.0
    order_timeout_sec: float = 20.0
    reprice_max_attempts: int = 2
    reprice_tick_steps: int = 1
    cooldown_sec_after_fail: int = 60
    max_consecutive_failures: int = 3
    rules_ttl_sec: int = 86400
    out_root_dir: str = "data/paper"
    strategy: str = "candidates_v1"  # candidates_v1 | model_alpha_v1
    model_ref: str | None = None
    model_family: str | None = None
    feature_set: str = "v3"
    model_registry_root: str = "models/registry"
    model_feature_dataset_root: str | None = None
    model_alpha: ModelAlphaSettings = field(default_factory=ModelAlphaSettings)
    micro_gate: MicroGateSettings = field(default_factory=MicroGateSettings)
    micro_order_policy: MicroOrderPolicySettings = field(default_factory=MicroOrderPolicySettings)
    paper_micro_provider: str = "offline_parquet"
    paper_micro_warmup_sec: int = 60
    paper_micro_warmup_min_trade_events_per_market: int = 1
    paper_micro_auto_health_path: str = "data/raw_ws/upbit/_meta/ws_public_health.json"
    paper_micro_auto_health_stale_sec: int = 180
    paper_feature_provider: str = "offline_parquet"
    paper_live_parquet_root: str = "data/parquet"
    paper_live_candles_dataset: str = "candles_api_v1"
    paper_live_bootstrap_1m_bars: int = 2000
    paper_live_micro_max_age_ms: int = 300_000
    execution_contract_artifact_path: str = DEFAULT_EXECUTION_CONTRACT_ARTIFACT_PATH


@dataclass(frozen=True)
class PaperRunSummary:
    run_id: str
    run_dir: str
    duration_sec: float
    events: int
    orders_submitted: int
    orders_filled: int
    orders_canceled: int
    intents_failed: int
    candidates_total: int
    candidates_blocked_by_micro: int
    candidates_aborted_by_policy: int
    micro_blocked_ratio: float
    micro_blocked_reasons: dict[str, int]
    replaces_total: int
    cancels_total: int
    aborted_timeout_total: int
    dust_abort_total: int
    avg_time_to_fill_ms: float
    p50_time_to_fill_ms: float
    p90_time_to_fill_ms: float
    slippage_bps_mean: float
    slippage_bps_p50: float
    slippage_bps_p90: float
    fill_ratio: float
    fill_rate: float
    realized_pnl_quote: float
    unrealized_pnl_quote: float
    max_drawdown_pct: float
    win_rate: float
    micro_quality_score_mean: float
    micro_quality_score_p50: float
    runtime_risk_multiplier_mean: float
    operational_regime_score_mean: float
    operational_breadth_ratio_mean: float
    operational_max_positions_mean: float
    rolling_window_minutes: int
    rolling_windows_total: int
    rolling_active_windows: int
    rolling_nonnegative_active_window_ratio: float
    rolling_positive_active_window_ratio: float
    rolling_max_fill_concentration_ratio: float
    rolling_max_window_drawdown_pct: float
    rolling_worst_window_realized_pnl_quote: float
    orders_partially_filled: int = 0
    orders_completed: int = 0
    fill_events_total: int = 0
    avg_time_to_first_fill_ms: float = 0.0
    p50_time_to_first_fill_ms: float = 0.0
    p90_time_to_first_fill_ms: float = 0.0
    avg_time_to_complete_fill_ms: float = 0.0
    p50_time_to_complete_fill_ms: float = 0.0
    p90_time_to_complete_fill_ms: float = 0.0


@dataclass(frozen=True)
class TickerSnapshot:
    market: str
    ts_ms: int
    trade_price: float
    acc_trade_price_24h: float


class MarketDataHub:
    def __init__(self, *, history_sec: int = 300) -> None:
        self._latest: dict[str, TickerSnapshot] = {}
        self._history: dict[str, deque[TickerSnapshot]] = {}
        self._max_history_ms = max(int(history_sec), 1) * 1000

    def update(self, event: TickerEvent) -> None:
        trade_price = _safe_optional_float(event.trade_price)
        acc_trade_price_24h = _safe_optional_float(event.acc_trade_price_24h)
        if trade_price is None or trade_price <= 0 or acc_trade_price_24h is None or acc_trade_price_24h < 0:
            return
        snapshot = TickerSnapshot(
            market=event.market,
            ts_ms=event.ts_ms,
            trade_price=float(trade_price),
            acc_trade_price_24h=float(acc_trade_price_24h),
        )
        self._latest[event.market] = snapshot

        history = self._history.get(event.market)
        if history is None:
            history = deque()
            self._history[event.market] = history
        history.append(snapshot)

        cutoff = snapshot.ts_ms - self._max_history_ms
        while history and history[0].ts_ms < cutoff:
            history.popleft()

    def get_latest_ticker(self, market: str) -> TickerSnapshot | None:
        return self._latest.get(market.strip().upper())

    def get_universe_snapshot(self, *, quote: str = "KRW") -> dict[str, TickerSnapshot]:
        prefix = f"{quote.strip().upper()}-"
        return {market: value for market, value in self._latest.items() if market.startswith(prefix)}

    def get_momentum_pct(self, market: str, *, window_sec: int) -> float | None:
        points = self._history.get(market.strip().upper())
        if points is None or len(points) < 2:
            return None

        latest = points[-1]
        baseline = _find_baseline(points, target_ts_ms=latest.ts_ms - max(int(window_sec), 1) * 1000)
        if baseline is None or baseline.trade_price <= 0:
            return None
        return (latest.trade_price - baseline.trade_price) / baseline.trade_price * 100.0

    def get_acc_trade_delta(self, market: str, *, window_sec: int) -> float | None:
        points = self._history.get(market.strip().upper())
        if points is None or len(points) < 2:
            return None

        latest = points[-1]
        baseline = _find_baseline(points, target_ts_ms=latest.ts_ms - max(int(window_sec), 1) * 1000)
        if baseline is None:
            return None
        return latest.acc_trade_price_24h - baseline.acc_trade_price_24h

    def latest_prices(self) -> dict[str, float]:
        return {market: snapshot.trade_price for market, snapshot in self._latest.items()}


class UniverseProviderTop20:
    def __init__(
        self,
        *,
        quote: str,
        top_n: int,
        refresh_sec: float,
        hold_sec: float,
    ) -> None:
        self._quote = quote.strip().upper()
        self._top_n = max(int(top_n), 1)
        self._refresh_sec = max(float(refresh_sec), 1.0)
        self._hold_sec = max(float(hold_sec), 0.0)
        self._scanner = TopTradeValueScanner()
        self._markets: list[str] = []
        self._pending_removal_started: dict[str, float] = {}
        self._next_refresh_at_monotonic = 0.0

    def update_ticker(self, event: TickerEvent) -> None:
        self._scanner.update(event)

    def maybe_refresh(self, *, now_monotonic: float, market_data: MarketDataHub) -> bool:
        if now_monotonic < self._next_refresh_at_monotonic and self._markets:
            return False
        self._next_refresh_at_monotonic = now_monotonic + self._refresh_sec

        ranked = self._scanner.top_n(n=self._top_n, quote=self._quote)
        desired_markets = [item.market for item in ranked]
        desired_set = set(desired_markets)

        next_markets: list[str] = []
        for market in self._markets:
            if market in desired_set:
                self._pending_removal_started.pop(market, None)
                if market not in next_markets:
                    next_markets.append(market)
                continue

            pending_started = self._pending_removal_started.get(market)
            if pending_started is None:
                self._pending_removal_started[market] = now_monotonic
                if market not in next_markets:
                    next_markets.append(market)
                continue

            if now_monotonic - pending_started < self._hold_sec:
                if market not in next_markets:
                    next_markets.append(market)
                continue

            self._pending_removal_started.pop(market, None)

        for market in desired_markets:
            if market not in next_markets:
                next_markets.append(market)

        next_markets.sort(
            key=lambda market: (
                market_data.get_latest_ticker(market).acc_trade_price_24h if market_data.get_latest_ticker(market) else 0.0
            ),
            reverse=True,
        )
        next_markets = next_markets[: self._top_n]

        changed = next_markets != self._markets
        self._markets = next_markets
        return changed

    def markets(self) -> list[str]:
        return list(self._markets)


class RulesProvider:
    def __init__(
        self,
        *,
        settings: UpbitSettings,
        credentials: UpbitCredentials | None,
        ttl_sec: int,
    ) -> None:
        self._settings = settings
        self._credentials = credentials
        self._ttl_ms = max(int(ttl_sec), 1) * 1000
        self._cache: dict[str, tuple[int, MarketRules]] = {}

    def get_rules(self, *, market: str, reference_price: float, ts_ms: int) -> MarketRules:
        market_value = market.strip().upper()
        cached = self._cache.get(market_value)
        if cached is not None and ts_ms < cached[0]:
            return cached[1]

        rules = MarketRules(
            bid_fee=0.0005,
            ask_fee=0.0005,
            maker_bid_fee=None,
            maker_ask_fee=None,
            min_total=5000.0,
            tick_size=_infer_tick_size(price=max(reference_price, 1e-8), quote=market_value.split("-", 1)[0]),
        )

        chance_payload = self._fetch_chance_payload(market_value)
        if isinstance(chance_payload, dict):
            rules = MarketRules(
                bid_fee=_safe_float(chance_payload.get("bid_fee"), default=rules.bid_fee),
                ask_fee=_safe_float(chance_payload.get("ask_fee"), default=rules.ask_fee),
                maker_bid_fee=_safe_optional_float(chance_payload.get("maker_bid_fee")),
                maker_ask_fee=_safe_optional_float(chance_payload.get("maker_ask_fee")),
                min_total=_extract_min_total(chance_payload, default=rules.min_total),
                tick_size=rules.tick_size,
            )

        tick_size = self._fetch_tick_size(market_value)
        if tick_size is not None:
            rules = MarketRules(
                bid_fee=rules.bid_fee,
                ask_fee=rules.ask_fee,
                maker_bid_fee=rules.maker_bid_fee,
                maker_ask_fee=rules.maker_ask_fee,
                min_total=rules.min_total,
                tick_size=tick_size,
            )

        self._cache[market_value] = (ts_ms + self._ttl_ms, rules)
        return rules

    def _fetch_chance_payload(self, market: str) -> dict[str, Any] | None:
        if self._credentials is None:
            return None

        try:
            with UpbitHttpClient(self._settings, credentials=self._credentials) as http_client:
                payload = UpbitPrivateClient(http_client).chance(market=market)
        except UpbitError:
            return None

        return payload if isinstance(payload, dict) else None

    def _fetch_tick_size(self, market: str) -> float | None:
        try:
            with UpbitHttpClient(self._settings) as http_client:
                payload = UpbitPublicClient(http_client).orderbook_instruments(markets=[market])
        except UpbitError:
            return None

        if not isinstance(payload, list):
            return None

        for item in payload:
            if not isinstance(item, dict):
                continue
            if str(item.get("market", "")).strip().upper() != market:
                continue
            tick = _safe_optional_float(item.get("tick_size"))
            if tick is not None and tick > 0:
                return tick
        return None

@dataclass
class ExecutionUpdate:
    orders_submitted: list[PaperOrder] = field(default_factory=list)
    orders_canceled: list[PaperOrder] = field(default_factory=list)
    orders_filled: list[PaperOrder] = field(default_factory=list)
    orders_with_fill: list[PaperOrder] = field(default_factory=list)
    fills: list[FillEvent] = field(default_factory=list)
    order_states_after_fill: dict[str, PaperOrder] = field(default_factory=dict)
    failed_markets: list[str] = field(default_factory=list)
    success_markets: list[str] = field(default_factory=list)
    supervisor_events: list[dict[str, Any]] = field(default_factory=list)
    counter_deltas: dict[str, int] = field(default_factory=dict)
    reason_counts: dict[str, int] = field(default_factory=dict)
    order_exec_profiles: dict[str, dict[str, Any]] = field(default_factory=dict)
    order_policy_diagnostics: dict[str, dict[str, Any]] = field(default_factory=dict)


@dataclass
class _PendingIntent:
    intent: OrderIntent
    order_id: str
    replace_count: int
    created_ts_ms: int
    last_action_ts_ms: int
    last_replace_ts_ms: int
    initial_ref_price: float
    profile: OrderExecProfile
    market: str


class PaperExecutionGateway:
    def __init__(
        self,
        *,
        exchange: PaperSimExchange,
        rules_provider: RulesProvider,
        order_timeout_sec: float,
        reprice_max_attempts: int,
        reprice_tick_steps: int,
        default_profile: OrderExecProfile | None = None,
        max_replaces_per_min_per_market: int = 10,
        micro_order_policy: MicroOrderPolicyV1 | None = None,
        micro_snapshot_provider: MicroSnapshotProvider | None = None,
    ) -> None:
        self._exchange = exchange
        self._rules_provider = rules_provider
        self._legacy_default_profile = make_legacy_exec_profile(
            timeout_ms=max(int(float(order_timeout_sec) * 1000), 1_000),
            replace_interval_ms=max(int(float(order_timeout_sec) * 1000), 1_000),
            max_replaces=max(int(reprice_max_attempts), 0),
            price_mode=PRICE_MODE_JOIN,
            max_chase_bps=max(int(reprice_tick_steps), 1) * 10_000,
            min_replace_interval_ms_global=1_500,
        )
        self._default_profile = (
            order_exec_profile_from_dict(order_exec_profile_to_dict(default_profile), fallback=self._legacy_default_profile)
            if default_profile is not None
            else self._legacy_default_profile
        )
        self._max_replaces_per_min_per_market = max(int(max_replaces_per_min_per_market), 1)
        self._pending_by_intent: dict[str, _PendingIntent] = {}
        self._intent_by_order_id: dict[str, str] = {}
        self._replace_window_by_market: dict[str, deque[int]] = {}
        self._micro_order_policy = micro_order_policy
        self._micro_snapshot_provider = micro_snapshot_provider

    def submit_intent(self, *, intent: OrderIntent, latest_trade_price: float, ts_ms: int) -> ExecutionUpdate:
        update = ExecutionUpdate()
        rules = self._rules_provider.get_rules(market=intent.market, reference_price=intent.price, ts_ms=ts_ms)
        snapshot = (
            self._micro_snapshot_provider.get(intent.market, int(ts_ms))
            if self._micro_snapshot_provider is not None
            else None
        )
        profile = order_exec_profile_from_dict(
            intent.meta.get("exec_profile"),
            fallback=self._default_profile,
        )
        profile_payload = order_exec_profile_to_dict(profile)
        policy_diagnostics = (
            dict(intent.meta.get("micro_diagnostics"))
            if isinstance(intent.meta.get("micro_diagnostics"), dict)
            else {}
        )
        order, fill = self._exchange.submit_order(
            intent=intent,
            rules=rules,
            latest_trade_price=latest_trade_price,
            micro_snapshot=snapshot,
            ts_ms=ts_ms,
            reprice_attempt=0,
        )
        update.orders_submitted.append(order)
        update.order_exec_profiles[order.order_id] = profile_payload
        if policy_diagnostics:
            update.order_policy_diagnostics[order.order_id] = dict(policy_diagnostics)

        if order.state in {"OPEN", "PARTIAL"}:
            self._activate_pending(
                intent=intent,
                order_id=order.order_id,
                replace_count=0,
                ts_ms=ts_ms,
                created_ts_ms=ts_ms,
                profile=profile,
            )

        if order.state == "FAILED":
            update.failed_markets.append(intent.market)

        if fill is not None:
            update.fills.append(fill)
            latest = self._exchange.get_order(order.order_id)
            if latest is not None:
                update.order_states_after_fill[latest.order_id] = latest
                update.orders_with_fill.append(latest)
                if latest.state == "FILLED":
                    update.orders_filled.append(latest)
                    update.success_markets.append(intent.market)
                    self._clear_pending(intent.intent_id)

        return update

    def on_ticker(self, *, event: TickerEvent) -> ExecutionUpdate:
        update = ExecutionUpdate()
        rules = self._rules_provider.get_rules(
            market=event.market,
            reference_price=event.trade_price,
            ts_ms=event.ts_ms,
        )
        market_value = event.market.strip().upper()
        self._trim_replace_window(market=market_value, now_ts_ms=event.ts_ms)
        market_window = self._replace_window_by_market.setdefault(market_value, deque())

        fills = self._exchange.process_ticker(
            market=event.market,
            trade_price=event.trade_price,
            ts_ms=event.ts_ms,
            rules=rules,
        )
        for fill in fills:
            update.fills.append(fill)
            current = self._exchange.get_order(fill.order_id)
            if current is not None:
                update.order_states_after_fill[current.order_id] = current
                update.orders_with_fill.append(current)
                if current.state == "FILLED":
                    update.orders_filled.append(current)
                    intent_id = self._intent_by_order_id.pop(fill.order_id, None)
                    if intent_id is not None:
                        pending = self._pending_by_intent.pop(intent_id, None)
                        if pending is not None:
                            update.success_markets.append(pending.intent.market)

        for intent_id, pending in list(self._pending_by_intent.items()):
            if pending.market != market_value:
                continue

            current_order = self._exchange.get_order(pending.order_id)
            if current_order is None or current_order.state not in {"OPEN", "PARTIAL"}:
                self._clear_pending(intent_id)
                continue

            remaining_volume = max(current_order.volume_req - current_order.volume_filled, 0.0)
            effective_profile = pending.profile
            policy_diagnostics: dict[str, Any] = {}
            policy_abort_reason: str | None = None
            if self._micro_order_policy is not None:
                snapshot = (
                    self._micro_snapshot_provider.get(market_value, int(event.ts_ms))
                    if self._micro_snapshot_provider is not None
                    else None
                )
                model_prob = (
                    _safe_optional_float(pending.intent.meta.get("model_prob"))
                    if isinstance(pending.intent.meta, dict)
                    else None
                )
                guard = self._micro_order_policy.resolve_guarded_profile(
                    profile=pending.profile,
                    market=market_value,
                    ref_price=event.trade_price,
                    tick_size=rules.tick_size,
                    replace_attempt=pending.replace_count + 1,
                    model_prob=model_prob,
                    micro_snapshot=snapshot,
                    now_ts_ms=event.ts_ms,
                )
                effective_profile = guard.profile
                policy_diagnostics = dict(guard.diagnostics or {})
                policy_abort_reason = guard.abort_reason
            action = evaluate_supervisor_action(
                profile=effective_profile,
                side=current_order.side,
                now_ts_ms=event.ts_ms,
                created_ts_ms=pending.created_ts_ms,
                last_action_ts_ms=pending.last_action_ts_ms,
                last_replace_ts_ms=pending.last_replace_ts_ms,
                replace_count=pending.replace_count,
                remaining_volume=remaining_volume,
                ref_price=event.trade_price,
                tick_size=rules.tick_size,
                initial_ref_price=pending.initial_ref_price,
                min_total=rules.min_total,
                replaces_last_minute=len(market_window),
                max_replaces_per_min_per_market=self._max_replaces_per_min_per_market,
            )
            if policy_abort_reason is not None and action.action == SUPERVISOR_ACTION_REPLACE:
                action = SupervisorAction(
                    action=SUPERVISOR_ACTION_ABORT,
                    reason_code=policy_abort_reason,
                )
            if action.action not in {SUPERVISOR_ACTION_ABORT, SUPERVISOR_ACTION_REPLACE}:
                continue

            reason_code = action.reason_code or REASON_TIMEOUT_REPLACE
            canceled = self._exchange.cancel_order(
                pending.order_id,
                ts_ms=event.ts_ms,
                reason=reason_code,
            )
            if canceled is None:
                update.failed_markets.append(pending.intent.market)
                self._clear_pending(intent_id)
                continue

            update.orders_canceled.append(canceled)
            update.supervisor_events.append(
                {
                    "event_type": "ORDER_TIMEOUT",
                    "ts_ms": int(event.ts_ms),
                    "payload": {
                        "market": current_order.market,
                        "side": current_order.side,
                        "order_id": current_order.order_id,
                        "intent_id": pending.intent.intent_id,
                        "replace_count": pending.replace_count,
                        "reason_code": reason_code,
                        "micro_policy": dict(policy_diagnostics),
                    },
                }
            )
            update.supervisor_events.append(
                {
                    "event_type": "CANCEL_RESULT",
                    "ts_ms": int(event.ts_ms),
                    "payload": {
                        "market": canceled.market,
                        "side": canceled.side,
                        "order_id": canceled.order_id,
                        "state": canceled.state,
                        "reason_code": reason_code,
                    },
                }
            )

            self._intent_by_order_id.pop(pending.order_id, None)

            _reason_inc(update.reason_counts, reason_code, 1)
            if action.action == SUPERVISOR_ACTION_ABORT:
                _counter_inc(update.counter_deltas, "aborted_timeout_total", 1)
                if reason_code == REASON_MIN_NOTIONAL_DUST_ABORT:
                    _counter_inc(update.counter_deltas, "dust_abort_total", 1)
                update.failed_markets.append(pending.intent.market)
                self._clear_pending(intent_id)
                continue

            new_price = float(action.target_price if action.target_price is not None else current_order.price)
            reprice_intent = OrderIntent(
                intent_id=pending.intent.intent_id,
                ts_ms=event.ts_ms,
                market=pending.intent.market,
                side=pending.intent.side,
                ord_type="limit",
                price=new_price,
                volume=remaining_volume,
                time_in_force=pending.intent.time_in_force,
                reason_code=pending.intent.reason_code,
                meta={
                    **pending.intent.meta,
                    "reprice_attempt": pending.replace_count + 1,
                    "exec_profile": order_exec_profile_to_dict(effective_profile),
                    "micro_diagnostics": dict(policy_diagnostics),
                },
            )
            new_order, new_fill = self._exchange.submit_order(
                intent=reprice_intent,
                rules=rules,
                latest_trade_price=event.trade_price,
                micro_snapshot=snapshot,
                ts_ms=event.ts_ms,
                reprice_attempt=pending.replace_count + 1,
            )
            update.orders_submitted.append(new_order)
            _counter_inc(update.counter_deltas, "replaces_total", 1)
            update.supervisor_events.append(
                {
                    "event_type": "ORDER_REPLACED",
                    "ts_ms": int(event.ts_ms),
                    "payload": {
                        "market": current_order.market,
                        "side": current_order.side,
                        "prev_order_id": current_order.order_id,
                        "new_order_id": new_order.order_id,
                        "intent_id": pending.intent.intent_id,
                        "replace_count": pending.replace_count + 1,
                        "reason_code": reason_code,
                        "new_price": float(new_price),
                        "new_volume": float(remaining_volume),
                        "price_mode": str(effective_profile.price_mode),
                        "micro_policy": dict(policy_diagnostics),
                    },
                }
            )
            market_window.append(int(event.ts_ms))

            effective_profile_payload = order_exec_profile_to_dict(effective_profile)
            if new_order.state in {"OPEN", "PARTIAL"}:
                self._activate_pending(
                    intent=reprice_intent,
                    order_id=new_order.order_id,
                    replace_count=pending.replace_count + 1,
                    ts_ms=event.ts_ms,
                    created_ts_ms=pending.created_ts_ms,
                    profile=effective_profile,
                )
            elif new_order.state == "FAILED":
                update.failed_markets.append(reprice_intent.market)
                self._clear_pending(intent_id)
            else:
                self._clear_pending(intent_id)

            update.order_exec_profiles[new_order.order_id] = effective_profile_payload
            if policy_diagnostics:
                update.order_policy_diagnostics[new_order.order_id] = dict(policy_diagnostics)

            if new_fill is not None:
                update.fills.append(new_fill)
                latest = self._exchange.get_order(new_fill.order_id)
                if latest is not None:
                    update.order_states_after_fill[latest.order_id] = latest
                    update.orders_with_fill.append(latest)
                    if latest.state == "FILLED":
                        update.orders_filled.append(latest)
                        update.success_markets.append(reprice_intent.market)
                        self._clear_pending(intent_id)

        return update

    def _activate_pending(
        self,
        *,
        intent: OrderIntent,
        order_id: str,
        replace_count: int,
        ts_ms: int,
        created_ts_ms: int,
        profile: OrderExecProfile,
    ) -> None:
        initial_ref_price = _safe_optional_float(intent.meta.get("initial_ref_price")) if isinstance(intent.meta, dict) else None
        self._pending_by_intent[intent.intent_id] = _PendingIntent(
            intent=intent,
            order_id=order_id,
            replace_count=max(int(replace_count), 0),
            created_ts_ms=int(created_ts_ms),
            last_action_ts_ms=int(ts_ms),
            last_replace_ts_ms=int(ts_ms),
            initial_ref_price=float(initial_ref_price if initial_ref_price is not None else intent.price),
            profile=profile,
            market=intent.market.strip().upper(),
        )
        self._intent_by_order_id[order_id] = intent.intent_id

    def _clear_pending(self, intent_id: str) -> None:
        pending = self._pending_by_intent.pop(intent_id, None)
        if pending is not None:
            self._intent_by_order_id.pop(pending.order_id, None)

    def _trim_replace_window(self, *, market: str, now_ts_ms: int) -> None:
        queue = self._replace_window_by_market.setdefault(market, deque())
        cutoff = int(now_ts_ms) - 60_000
        while queue and int(queue[0]) < cutoff:
            queue.popleft()

class PaperRunEngine:
    def __init__(
        self,
        *,
        upbit_settings: UpbitSettings,
        run_settings: PaperRunSettings,
        ws_client: UpbitWebSocketPublicClient | None = None,
        market_loader: Callable[[str], list[str]] | None = None,
        rules_provider: RulesProvider | None = None,
        micro_snapshot_provider: MicroSnapshotProvider | None = None,
    ) -> None:
        self._upbit_settings = upbit_settings
        self._run_settings = run_settings
        self._ws_client = ws_client or UpbitWebSocketPublicClient(upbit_settings.websocket)
        self._market_loader = market_loader or self._load_quote_markets
        self._micro_snapshot_provider = micro_snapshot_provider

        credentials = load_upbit_credentials(upbit_settings)
        self._rules_provider = rules_provider or RulesProvider(
            settings=upbit_settings,
            credentials=credentials,
            ttl_sec=run_settings.rules_ttl_sec,
        )
        self._runtime_counters = {
            "orders_submitted": 0,
            "orders_filled": 0,
            "orders_canceled": 0,
            "cancels_total": 0,
            "replaces_total": 0,
            "aborted_timeout_total": 0,
            "dust_abort_total": 0,
            "intents_failed": 0,
            "candidates_total": 0,
            "candidates_blocked_by_micro": 0,
            "candidates_aborted_by_policy": 0,
            "micro_blocked_reasons": {},
            "micro_policy_tier_counts": {},
            "micro_policy_fallback_counts": {},
            "micro_policy_cross_allowed_count": 0,
            "micro_policy_cross_used_count": 0,
            "micro_policy_cross_block_reasons": {},
            "micro_policy_resolver_failed_fallback_used": 0,
            "order_supervisor_reasons": {},
            "orders_partially_filled": 0,
            "orders_completed": 0,
            "fill_events_total": 0,
            "scored_rows": 0,
            "eligible_rows": 0,
            "selected_rows": 0,
            "skipped_missing_features_rows": 0,
            "dropped_min_prob_rows": 0,
            "dropped_top_pct_rows": 0,
            "blocked_min_candidates_ts": 0,
            "debug_mismatch_reasons": {},
            "exit_intents_total": 0,
        }
        self._runtime_state: dict[str, Any] = {}

    async def run(self) -> PaperRunSummary:
        quote = self._run_settings.quote.strip().upper()
        run_id = build_paper_run_id()
        run_root = Path(self._run_settings.out_root_dir) / "runs" / run_id
        self._runtime_counters = {
            "orders_submitted": 0,
            "orders_filled": 0,
            "orders_canceled": 0,
            "cancels_total": 0,
            "replaces_total": 0,
            "aborted_timeout_total": 0,
            "dust_abort_total": 0,
            "intents_failed": 0,
            "candidates_total": 0,
            "candidates_blocked_by_micro": 0,
            "candidates_aborted_by_policy": 0,
            "micro_blocked_reasons": {},
            "micro_policy_tier_counts": {},
            "micro_policy_fallback_counts": {},
            "micro_policy_cross_allowed_count": 0,
            "micro_policy_cross_used_count": 0,
            "micro_policy_cross_block_reasons": {},
            "micro_policy_resolver_failed_fallback_used": 0,
            "order_supervisor_reasons": {},
            "orders_partially_filled": 0,
            "orders_completed": 0,
            "fill_events_total": 0,
            "scored_rows": 0,
            "eligible_rows": 0,
            "selected_rows": 0,
            "skipped_missing_features_rows": 0,
            "dropped_min_prob_rows": 0,
            "dropped_top_pct_rows": 0,
            "blocked_min_candidates_ts": 0,
            "debug_mismatch_reasons": {},
            "exit_intents_total": 0,
        }
        self._runtime_state = {
            "intent_context": {},
            "first_filled_intents": set(),
            "completed_intents": set(),
            "filled_order_ids": set(),
            "completed_order_ids": set(),
            "partially_filled_order_ids": set(),
            "time_to_fill_ms": [],
            "time_to_first_fill_ms": [],
            "time_to_complete_fill_ms": [],
            "slippage_bps": [],
            "policy_tick_bps_values": [],
            "operational_micro_quality_scores": [],
            "operational_runtime_risk_multipliers": [],
            "operational_regime_scores": [],
            "operational_breadth_ratios": [],
            "operational_max_positions_values": [],
            "fill_records": [],
            "equity_samples": [],
            "order_exec_profile_by_order_id": {},
            "order_policy_diag_by_order_id": {},
            "live_ws_trade_events_by_market": {},
            "live_ws_acc_notional_by_market": {},
            "live_ws_last_event_ts_ms_by_market": {},
            "live_ws_warmup": {},
            "live_feature_provider": None,
            "execution_contract": load_live_execution_contract_artifact(
                project_root=Path.cwd(),
                artifact_path=self._run_settings.execution_contract_artifact_path,
            ),
        }

        markets = self._market_loader(quote)
        if not markets:
            raise RuntimeError(f"no markets available for quote={quote}")

        market_data = MarketDataHub(history_sec=max(self._run_settings.momentum_window_sec * 2, 300))
        strategy_mode = str(self._run_settings.strategy).strip().lower() or "candidates_v1"
        if strategy_mode not in {"candidates_v1", "model_alpha_v1"}:
            raise ValueError(f"unsupported paper strategy: {self._run_settings.strategy}")
        universe = UniverseProviderTop20(
            quote=quote,
            top_n=self._run_settings.top_n,
            refresh_sec=self._run_settings.universe_refresh_sec,
            hold_sec=self._run_settings.universe_hold_sec,
        )
        candidate_generator = CandidateGeneratorV1(
            CandidateSettings(
                momentum_window_sec=self._run_settings.momentum_window_sec,
                min_momentum_pct=self._run_settings.min_momentum_pct,
            )
        )
        micro_gate = MicroGateV1(self._run_settings.micro_gate) if self._run_settings.micro_gate.enabled else None
        micro_order_policy = (
            MicroOrderPolicyV1(self._run_settings.micro_order_policy)
            if self._run_settings.micro_order_policy.enabled
            else None
        )
        micro_snapshot_provider = self._resolve_micro_snapshot_provider(markets=markets)
        self._runtime_state["micro_snapshot_provider_for_features"] = micro_snapshot_provider
        micro_provider_info = dict(
            _describe_micro_snapshot_provider(
                provider=micro_snapshot_provider,
                micro_gate=self._run_settings.micro_gate,
            )
        )
        provider_decision = self._runtime_state.get("micro_provider_decision")
        if isinstance(provider_decision, dict):
            micro_provider_info.update(provider_decision)
        live_ws_provider = micro_snapshot_provider if isinstance(micro_snapshot_provider, LiveWsMicroSnapshotProvider) else None
        warmup_enabled = live_ws_provider is not None
        warmup_sec = max(int(self._run_settings.paper_micro_warmup_sec), 0) if warmup_enabled else 0
        warmup_min_trade_events = (
            max(int(self._run_settings.paper_micro_warmup_min_trade_events_per_market), 1) if warmup_enabled else 0
        )
        warmup_started_monotonic = time.monotonic()
        warmup_deadline_monotonic = warmup_started_monotonic + float(warmup_sec)
        warmup_elapsed_sec = 0.0
        warmup_satisfied = not warmup_enabled
        warmup_completed = not warmup_enabled
        warmup_trade_events_total = 0
        warmup_markets_with_samples = 0
        warmup_last_wait_emit = 0.0
        self._runtime_state["live_ws_warmup"] = {
            "enabled": bool(warmup_enabled),
            "sec": int(warmup_sec),
            "min_trade_events_per_market": int(warmup_min_trade_events),
            "started_monotonic": float(warmup_started_monotonic),
        }
        micro_provider_info.update(
            {
                "warmup_enabled": bool(warmup_enabled),
                "warmup_sec": int(warmup_sec),
                "warmup_min_trade_events_per_market": int(warmup_min_trade_events),
            }
        )
        if warmup_completed:
            self._runtime_state["live_ws_warmup"]["satisfied"] = True
            self._runtime_state["live_ws_warmup"]["elapsed_sec"] = 0.0
        model_strategy: ModelAlphaStrategyV1 | None = None
        if strategy_mode == "model_alpha_v1":
            model_interval_ms = _interval_ms_from_tf(self._run_settings.tf)
            now_ms = int(time.time() * 1000)
            feature_provider_mode = _normalize_paper_feature_provider(self._run_settings.paper_feature_provider)
            if feature_provider_mode in {"LIVE_V3", "LIVE_V4", "LIVE_V4_NATIVE"}:
                anchor = (now_ms // model_interval_ms) * model_interval_ms
                start_ts_ms = int(anchor - model_interval_ms * 4)
                end_ts_ms = int(anchor + max(int(self._run_settings.duration_sec) * 1000, model_interval_ms * 4))
            else:
                start_ts_ms = now_ms - max(
                    int(self._run_settings.duration_sec) * 1000 + 86_400_000,
                    model_interval_ms * 4,
                )
                end_ts_ms = now_ms + max(int(self._run_settings.duration_sec) * 1000, model_interval_ms * 2)
            model_strategy = self._build_model_alpha_strategy(
                active_markets=markets,
                decision_start_ts_ms=start_ts_ms,
                decision_end_ts_ms=end_ts_ms,
            )
        self._runtime_state["strategy_adapter"] = model_strategy
        self._runtime_state["model_last_decision_ts_ms"] = None
        trade_gate = TradeGateV1(
            GateSettings(
                per_trade_krw=self._run_settings.per_trade_krw,
                max_positions=(
                    max(int(self._run_settings.model_alpha.position.max_positions_total), 1)
                    if strategy_mode == "model_alpha_v1"
                    else self._run_settings.max_positions
                ),
                min_order_krw=self._run_settings.min_order_krw,
                max_consecutive_failures=self._run_settings.max_consecutive_failures,
                cooldown_sec_after_fail=self._run_settings.cooldown_sec_after_fail,
            ),
            micro_gate=micro_gate,
            micro_snapshot_provider=micro_snapshot_provider,
        )
        exchange = PaperSimExchange(
            quote_currency=quote,
            starting_cash_quote=self._run_settings.starting_krw,
        )
        execution = PaperExecutionGateway(
            exchange=exchange,
            rules_provider=self._rules_provider,
            order_timeout_sec=self._run_settings.order_timeout_sec,
            reprice_max_attempts=self._run_settings.reprice_max_attempts,
            reprice_tick_steps=self._run_settings.reprice_tick_steps,
            default_profile=_strategy_paper_exec_profile(self._run_settings),
            max_replaces_per_min_per_market=(
                self._run_settings.micro_order_policy.safety.max_replaces_per_min_per_market
                if self._run_settings.micro_order_policy.enabled
                else 10
            ),
            micro_order_policy=micro_order_policy,
            micro_snapshot_provider=micro_snapshot_provider,
        )

        events_count = 0
        orders_submitted = 0
        orders_filled = 0
        orders_canceled = 0
        intents_failed = 0
        candidates_total = 0
        candidates_blocked_by_micro = 0
        candidates_aborted_by_policy = 0
        equity_curve: list[float] = []
        realized_trade_pnls: list[float] = []
        received_events = 0

        run_started_monotonic = time.monotonic()
        next_decision_at = run_started_monotonic
        next_report_at = run_started_monotonic + max(self._run_settings.print_every_sec, 1.0)
        deadline_monotonic = (
            run_started_monotonic + float(self._run_settings.duration_sec)
            if int(self._run_settings.duration_sec) > 0
            else None
        )

        with JsonlEventStore(run_root) as store:
            runtime_metadata = _resolve_paper_runtime_metadata(self._run_settings)
            if strategy_mode == "model_alpha_v1" and model_strategy is not None:
                predictor_run_id = str(getattr(model_strategy, "predictor_run_id", "")).strip()
                if predictor_run_id:
                    runtime_metadata["paper_runtime_model_run_id"] = predictor_run_id

            def append_event(event_type: str, ts_ms: int, payload: dict[str, Any] | None = None) -> None:
                nonlocal events_count
                store.append_event(event_type=event_type, ts_ms=ts_ms, payload=payload)
                events_count += 1

            append_event(
                "RUN_STARTED",
                ts_ms=int(time.time() * 1000),
                payload={
                    "run_id": run_id,
                    "quote": quote,
                    "top_n": self._run_settings.top_n,
                    "strategy": strategy_mode,
                    "duration_sec": self._run_settings.duration_sec,
                    "markets_subscribed": len(markets),
                    "micro_order_policy_enabled": bool(self._run_settings.micro_order_policy.enabled),
                    "micro_order_policy_mode": str(self._run_settings.micro_order_policy.mode),
                    "micro_order_policy_on_missing": str(self._run_settings.micro_order_policy.on_missing),
                    "micro_provider": str(micro_provider_info.get("provider") or "NONE"),
                    "micro_provider_info": dict(micro_provider_info),
                    "feature_provider": _normalize_paper_feature_provider(self._run_settings.paper_feature_provider),
                    "execution_contract_artifact_path": str(self._run_settings.execution_contract_artifact_path),
                    "execution_contract_rows_total": int(
                        ((self._runtime_state.get("execution_contract") or {}).get("rows_total", 0) or 0)
                    ),
                    "warmup_sec": int(warmup_sec),
                    "warmup_min_trade_events_per_market": int(warmup_min_trade_events),
                    **runtime_metadata,
                },
            )

            stream_duration_sec = (
                float(self._run_settings.duration_sec)
                if int(self._run_settings.duration_sec) > 0
                else None
            )
            public_trade_queue: asyncio.Queue[TradeEvent] | None = None
            public_orderbook_queue: asyncio.Queue[OrderbookEvent] | None = None
            public_micro_tasks: list[asyncio.Task[None]] = []
            public_micro_stop_event: asyncio.Event | None = None
            if live_ws_provider is not None:
                public_micro_stop_event = asyncio.Event()
                if hasattr(self._ws_client, "stream_trade"):
                    public_trade_queue = asyncio.Queue()

                    async def _public_trade_pump() -> None:
                        assert public_trade_queue is not None
                        assert public_micro_stop_event is not None
                        async for trade_event in self._ws_client.stream_trade(markets, duration_sec=stream_duration_sec):
                            if public_micro_stop_event.is_set():
                                break
                            await public_trade_queue.put(trade_event)

                    public_micro_tasks.append(asyncio.create_task(_public_trade_pump()))
                if hasattr(self._ws_client, "stream_orderbook"):
                    public_orderbook_queue = asyncio.Queue()

                    async def _public_orderbook_pump() -> None:
                        assert public_orderbook_queue is not None
                        assert public_micro_stop_event is not None
                        async for orderbook_event in self._ws_client.stream_orderbook(
                            markets,
                            duration_sec=stream_duration_sec,
                            level=live_ws_provider.settings.orderbook_level,
                        ):
                            if public_micro_stop_event.is_set():
                                break
                            await public_orderbook_queue.put(orderbook_event)

                    public_micro_tasks.append(asyncio.create_task(_public_orderbook_pump()))
                if public_micro_tasks:
                    await asyncio.sleep(0)
            async for ticker in self._ws_client.stream_ticker(markets, duration_sec=stream_duration_sec):
                if deadline_monotonic is not None and time.monotonic() >= deadline_monotonic:
                    break
                received_events += 1
                market_data.update(ticker)
                universe.update_ticker(ticker)
                live_feature_provider = self._runtime_state.get("live_feature_provider")
                if _is_live_feature_provider(live_feature_provider):
                    live_feature_provider.ingest_ticker(ticker)
                if live_ws_provider is not None:
                    self._drain_live_public_micro_events(
                        provider=live_ws_provider,
                        trade_queue=public_trade_queue,
                        orderbook_queue=public_orderbook_queue,
                    )
                    if not public_micro_tasks:
                        self._ingest_live_micro_from_ticker(provider=live_ws_provider, ticker=ticker)
                    trade_counts = self._runtime_state.get("live_ws_trade_events_by_market", {})
                    if isinstance(trade_counts, dict):
                        warmup_trade_events_total = int(
                            sum(max(int(value), 0) for value in trade_counts.values() if value is not None)
                        )
                        warmup_markets_with_samples = int(
                            sum(1 for value in trade_counts.values() if value is not None and int(value) > 0)
                        )
                        has_ready_market = any(
                            value is not None and int(value) >= int(warmup_min_trade_events)
                            for value in trade_counts.values()
                        )
                    else:
                        warmup_trade_events_total = 0
                        warmup_markets_with_samples = 0
                        has_ready_market = False

                    if warmup_enabled and not warmup_completed:
                        now_for_warmup = time.monotonic()
                        elapsed_for_warmup = max(now_for_warmup - warmup_started_monotonic, 0.0)
                        if has_ready_market:
                            warmup_completed = True
                            warmup_satisfied = True
                            warmup_elapsed_sec = elapsed_for_warmup
                            self._runtime_state["live_ws_warmup"]["satisfied"] = True
                            self._runtime_state["live_ws_warmup"]["elapsed_sec"] = float(warmup_elapsed_sec)
                            append_event(
                                "MICRO_WARMUP_COMPLETED",
                                ts_ms=ticker.ts_ms,
                                payload={
                                    "warmup_satisfied": True,
                                    "warmup_elapsed_sec": float(round(warmup_elapsed_sec, 3)),
                                    "warmup_trade_events_total": int(warmup_trade_events_total),
                                    "markets_with_samples": int(warmup_markets_with_samples),
                                    "reason": "MIN_TRADE_EVENTS_REACHED",
                                },
                            )
                        elif now_for_warmup >= warmup_deadline_monotonic:
                            warmup_completed = True
                            warmup_satisfied = False
                            warmup_elapsed_sec = float(warmup_sec)
                            self._runtime_state["live_ws_warmup"]["satisfied"] = False
                            self._runtime_state["live_ws_warmup"]["elapsed_sec"] = float(warmup_elapsed_sec)
                            append_event(
                                "MICRO_WARMUP_COMPLETED",
                                ts_ms=ticker.ts_ms,
                                payload={
                                    "warmup_satisfied": False,
                                    "warmup_elapsed_sec": float(round(warmup_elapsed_sec, 3)),
                                    "warmup_trade_events_total": int(warmup_trade_events_total),
                                    "markets_with_samples": int(warmup_markets_with_samples),
                                    "reason": "WARMUP_TIMEOUT",
                                },
                            )
                        elif (elapsed_for_warmup - warmup_last_wait_emit) >= 5.0:
                            warmup_last_wait_emit = elapsed_for_warmup
                            append_event(
                                "MICRO_WARMUP_WAIT",
                                ts_ms=ticker.ts_ms,
                                payload={
                                    "warmup_elapsed_sec": float(round(elapsed_for_warmup, 3)),
                                    "warmup_target_sec": int(warmup_sec),
                                    "warmup_trade_events_total": int(warmup_trade_events_total),
                                    "markets_with_samples": int(warmup_markets_with_samples),
                                },
                            )

                realized_before = exchange.total_realized_pnl()
                market_has_ask_fill = self._apply_execution_update(
                    update=execution.on_ticker(event=ticker),
                    trade_gate=trade_gate,
                    event_store=store,
                    append_event=append_event,
                    ts_ms=ticker.ts_ms,
                    counters=self._runtime_counters,
                    strategy_adapter=model_strategy,
                )
                orders_submitted = int(self._runtime_counters["orders_submitted"])
                orders_filled = int(self._runtime_counters["orders_filled"])
                orders_canceled = int(self._runtime_counters["orders_canceled"])
                intents_failed = int(self._runtime_counters["intents_failed"])
                candidates_total = int(self._runtime_counters["candidates_total"])
                candidates_blocked_by_micro = int(self._runtime_counters["candidates_blocked_by_micro"])
                candidates_aborted_by_policy = int(self._runtime_counters["candidates_aborted_by_policy"])

                if market_has_ask_fill:
                    realized_after = exchange.total_realized_pnl()
                    realized_delta = realized_after - realized_before
                    if abs(realized_delta) > 1e-12:
                        realized_trade_pnls.append(realized_delta)

                now_monotonic = time.monotonic()
                if universe.maybe_refresh(now_monotonic=now_monotonic, market_data=market_data):
                    append_event(
                        "UNIVERSE_UPDATE",
                        ts_ms=ticker.ts_ms,
                        payload={
                            "markets": universe.markets(),
                            "count": len(universe.markets()),
                        },
                    )

                if now_monotonic >= next_decision_at:
                    if warmup_completed:
                        if strategy_mode == "model_alpha_v1" and model_strategy is not None:
                            interval_ms = _interval_ms_from_tf(self._run_settings.tf)
                            decision_ts_ms = (int(ticker.ts_ms) // interval_ms) * interval_ms
                            last_model_decision_ts = _safe_int(self._runtime_state.get("model_last_decision_ts_ms"))
                            if last_model_decision_ts is None or decision_ts_ms > last_model_decision_ts:
                                self._run_model_alpha_cycle(
                                    ts_ms=decision_ts_ms,
                                    latest_trade_price=ticker.trade_price,
                                    universe=universe,
                                    market_data=market_data,
                                    trade_gate=trade_gate,
                                    micro_order_policy=micro_order_policy,
                                    micro_snapshot_provider=micro_snapshot_provider,
                                    exchange=exchange,
                                    execution=execution,
                                    event_store=store,
                                    append_event=append_event,
                                    strategy=model_strategy,
                                )
                                self._runtime_state["model_last_decision_ts_ms"] = decision_ts_ms
                        else:
                            self._run_candidate_cycle(
                                ts_ms=ticker.ts_ms,
                                latest_trade_price=ticker.trade_price,
                                universe=universe,
                                market_data=market_data,
                                candidate_generator=candidate_generator,
                                trade_gate=trade_gate,
                                micro_order_policy=micro_order_policy,
                                micro_snapshot_provider=micro_snapshot_provider,
                                exchange=exchange,
                                execution=execution,
                                event_store=store,
                                append_event=append_event,
                            )
                        orders_submitted = int(self._runtime_counters["orders_submitted"])
                        orders_filled = int(self._runtime_counters["orders_filled"])
                        orders_canceled = int(self._runtime_counters["orders_canceled"])
                        intents_failed = int(self._runtime_counters["intents_failed"])
                        candidates_total = int(self._runtime_counters["candidates_total"])
                        candidates_blocked_by_micro = int(self._runtime_counters["candidates_blocked_by_micro"])
                        candidates_aborted_by_policy = int(self._runtime_counters["candidates_aborted_by_policy"])
                    next_decision_at = now_monotonic + max(self._run_settings.decision_interval_sec, 0.2)

                if now_monotonic >= next_report_at:
                    snapshot = exchange.portfolio_snapshot(ts_ms=ticker.ts_ms, latest_prices=market_data.latest_prices())
                    store.append_equity(snapshot)
                    append_event("PORTFOLIO_SNAPSHOT", ts_ms=ticker.ts_ms, payload=asdict(snapshot))
                    equity_samples = self._runtime_state.setdefault("equity_samples", [])
                    if isinstance(equity_samples, list):
                        equity_samples.append(
                            {
                                "ts_ms": int(snapshot.ts_ms),
                                "equity_quote": float(snapshot.equity_quote),
                                "realized_pnl_quote": float(snapshot.realized_pnl_quote),
                            }
                        )
                    equity_curve.append(snapshot.equity_quote)
                    print(
                        "[paper][status] "
                        f"ts_ms={ticker.ts_ms} events={received_events} "
                        f"universe={len(universe.markets())} open_orders={len(exchange.list_open_orders())} "
                        f"equity={snapshot.equity_quote:.2f}"
                    )
                    next_report_at = now_monotonic + max(self._run_settings.print_every_sec, 1.0)

            final_ts_ms = int(time.time() * 1000)
            if live_ws_provider is not None:
                self._drain_live_public_micro_events(
                    provider=live_ws_provider,
                    trade_queue=public_trade_queue,
                    orderbook_queue=public_orderbook_queue,
                )
            if warmup_enabled and not warmup_completed:
                warmup_elapsed_sec = min(
                    max(time.monotonic() - warmup_started_monotonic, 0.0),
                    float(warmup_sec),
                )
                warmup_satisfied = False
                warmup_completed = True
                self._runtime_state["live_ws_warmup"]["satisfied"] = False
                self._runtime_state["live_ws_warmup"]["elapsed_sec"] = float(warmup_elapsed_sec)

            micro_provider_info["warmup_elapsed_sec"] = float(round(warmup_elapsed_sec, 3))
            micro_provider_info["warmup_satisfied"] = bool(warmup_satisfied)
            micro_provider_info["warmup_trade_events_total"] = int(warmup_trade_events_total)
            micro_provider_info["micro_cache_markets_with_samples"] = int(warmup_markets_with_samples)
            last_event_by_market = self._runtime_state.get("live_ws_last_event_ts_ms_by_market", {})
            if isinstance(last_event_by_market, dict):
                last_values = [int(value) for value in last_event_by_market.values() if value is not None]
                last_event_ts_ms = max(last_values) if last_values else None
            else:
                last_event_ts_ms = None
            micro_provider_info["last_event_ts_ms"] = int(last_event_ts_ms) if last_event_ts_ms is not None else None
            if last_event_ts_ms is not None and int(last_event_ts_ms) > 0:
                micro_provider_info["micro_snapshot_age_ms"] = max(final_ts_ms - int(last_event_ts_ms), 0)
            else:
                micro_provider_info["micro_snapshot_age_ms"] = None

            final_snapshot = exchange.portfolio_snapshot(ts_ms=final_ts_ms, latest_prices=market_data.latest_prices())
            store.append_equity(final_snapshot)
            append_event("PORTFOLIO_SNAPSHOT", ts_ms=final_ts_ms, payload=asdict(final_snapshot))
            equity_samples = self._runtime_state.setdefault("equity_samples", [])
            if isinstance(equity_samples, list):
                equity_samples.append(
                    {
                        "ts_ms": int(final_snapshot.ts_ms),
                        "equity_quote": float(final_snapshot.equity_quote),
                        "realized_pnl_quote": float(final_snapshot.realized_pnl_quote),
                    }
                )
            append_event(
                "RUN_COMPLETED",
                ts_ms=final_ts_ms,
                payload={
                    "received_ticker_events": received_events,
                    "orders_submitted": orders_submitted,
                    "orders_filled": orders_filled,
                    "orders_canceled": orders_canceled,
                    "intents_failed": intents_failed,
                    "candidates_total": candidates_total,
                    "candidates_blocked_by_micro": candidates_blocked_by_micro,
                    "candidates_aborted_by_policy": candidates_aborted_by_policy,
                    "micro_blocked_reasons": dict(self._runtime_counters.get("micro_blocked_reasons", {})),
                    "micro_policy_tier_counts": dict(self._runtime_counters.get("micro_policy_tier_counts", {})),
                    "micro_policy_fallback_counts": dict(
                        self._runtime_counters.get("micro_policy_fallback_counts", {})
                    ),
                    "order_supervisor_reasons": dict(self._runtime_counters.get("order_supervisor_reasons", {})),
                    "micro_provider": str(micro_provider_info.get("provider") or "NONE"),
                    "micro_provider_info": dict(micro_provider_info),
                    "warmup_elapsed_sec": float(round(warmup_elapsed_sec, 3)),
                    "warmup_satisfied": bool(warmup_satisfied),
                    "warmup_trade_events_total": int(warmup_trade_events_total),
                    "micro_cache_markets_with_samples": int(warmup_markets_with_samples),
                },
            )
            if public_micro_stop_event is not None:
                public_micro_stop_event.set()
            if public_micro_tasks:
                for task in public_micro_tasks:
                    task.cancel()
                await asyncio.gather(*public_micro_tasks, return_exceptions=True)

        fill_ratio = (orders_filled / orders_submitted) if orders_submitted > 0 else 0.0
        fill_rate = fill_ratio
        micro_blocked_reasons = _normalize_reason_counts(self._runtime_counters.get("micro_blocked_reasons", {}))
        micro_blocked_ratio = (candidates_blocked_by_micro / candidates_total) if candidates_total > 0 else 0.0
        max_drawdown_pct = _max_drawdown_pct(equity_curve)
        wins = sum(1 for pnl in realized_trade_pnls if pnl > 0)
        win_rate = (wins / len(realized_trade_pnls)) if realized_trade_pnls else 0.0
        ttf_values = [float(value) for value in self._runtime_state.get("time_to_fill_ms", [])]
        first_fill_values = [float(value) for value in self._runtime_state.get("time_to_first_fill_ms", [])]
        complete_fill_values = [float(value) for value in self._runtime_state.get("time_to_complete_fill_ms", [])]
        slippage_values = [float(value) for value in self._runtime_state.get("slippage_bps", [])]
        policy_tick_bps_values = [float(value) for value in self._runtime_state.get("policy_tick_bps_values", [])]
        operational_quality_scores = [
            float(value) for value in self._runtime_state.get("operational_micro_quality_scores", [])
        ]
        operational_risk_values = [
            float(value) for value in self._runtime_state.get("operational_runtime_risk_multipliers", [])
        ]
        operational_regime_scores = [
            float(value) for value in self._runtime_state.get("operational_regime_scores", [])
        ]
        operational_breadth_ratios = [
            float(value) for value in self._runtime_state.get("operational_breadth_ratios", [])
        ]
        operational_max_positions_values = [
            float(value) for value in self._runtime_state.get("operational_max_positions_values", [])
        ]
        rolling_evidence = compute_rolling_paper_evidence(
            equity_samples=self._runtime_state.get("equity_samples", []),
            fill_records=self._runtime_state.get("fill_records", []),
        )
        replaces_total = int(self._runtime_counters.get("replaces_total", 0))
        cancels_total = int(self._runtime_counters.get("cancels_total", 0))
        aborted_timeout_total = int(self._runtime_counters.get("aborted_timeout_total", 0))
        dust_abort_total = int(self._runtime_counters.get("dust_abort_total", 0))
        orders_partially_filled = int(self._runtime_counters.get("orders_partially_filled", 0))
        orders_completed = int(self._runtime_counters.get("orders_completed", 0))
        fill_events_total = int(self._runtime_counters.get("fill_events_total", 0))

        actual_duration_sec = max(time.monotonic() - run_started_monotonic, 0.0)
        summary = PaperRunSummary(
            run_id=run_id,
            run_dir=str(run_root),
            duration_sec=float(actual_duration_sec),
            events=events_count,
            orders_submitted=orders_submitted,
            orders_filled=orders_filled,
            orders_canceled=orders_canceled,
            intents_failed=intents_failed,
            candidates_total=candidates_total,
            candidates_blocked_by_micro=candidates_blocked_by_micro,
            candidates_aborted_by_policy=candidates_aborted_by_policy,
            micro_blocked_ratio=micro_blocked_ratio,
            micro_blocked_reasons=micro_blocked_reasons,
            replaces_total=replaces_total,
            cancels_total=cancels_total,
            aborted_timeout_total=aborted_timeout_total,
            dust_abort_total=dust_abort_total,
            avg_time_to_fill_ms=mean(ttf_values),
            p50_time_to_fill_ms=percentile(ttf_values, 0.50),
            p90_time_to_fill_ms=percentile(ttf_values, 0.90),
            slippage_bps_mean=mean(slippage_values),
            slippage_bps_p50=percentile(slippage_values, 0.50),
            slippage_bps_p90=percentile(slippage_values, 0.90),
            fill_ratio=fill_ratio,
            fill_rate=fill_rate,
            realized_pnl_quote=final_snapshot.realized_pnl_quote,
            unrealized_pnl_quote=final_snapshot.unrealized_pnl_quote,
            max_drawdown_pct=max_drawdown_pct,
            win_rate=win_rate,
            micro_quality_score_mean=mean(operational_quality_scores),
            micro_quality_score_p50=percentile(operational_quality_scores, 0.50),
            runtime_risk_multiplier_mean=mean(operational_risk_values),
            operational_regime_score_mean=mean(operational_regime_scores),
            operational_breadth_ratio_mean=mean(operational_breadth_ratios),
            operational_max_positions_mean=mean(operational_max_positions_values),
            rolling_window_minutes=int(rolling_evidence.get("window_minutes", 60)),
            rolling_windows_total=int(rolling_evidence.get("windows_total", 0)),
            rolling_active_windows=int(rolling_evidence.get("active_windows", 0)),
            rolling_nonnegative_active_window_ratio=float(
                rolling_evidence.get("nonnegative_active_window_ratio", 0.0)
            ),
            rolling_positive_active_window_ratio=float(
                rolling_evidence.get("positive_active_window_ratio", 0.0)
            ),
            rolling_max_fill_concentration_ratio=float(
                rolling_evidence.get("max_fill_concentration_ratio", 0.0)
            ),
            rolling_max_window_drawdown_pct=float(rolling_evidence.get("max_window_drawdown_pct", 0.0)),
            rolling_worst_window_realized_pnl_quote=float(
                rolling_evidence.get("worst_window_realized_pnl_quote", 0.0)
            ),
            orders_partially_filled=orders_partially_filled,
            orders_completed=orders_completed,
            fill_events_total=fill_events_total,
            avg_time_to_first_fill_ms=mean(first_fill_values),
            p50_time_to_first_fill_ms=percentile(first_fill_values, 0.50),
            p90_time_to_first_fill_ms=percentile(first_fill_values, 0.90),
            avg_time_to_complete_fill_ms=mean(complete_fill_values),
            p50_time_to_complete_fill_ms=percentile(complete_fill_values, 0.50),
            p90_time_to_complete_fill_ms=percentile(complete_fill_values, 0.90),
        )
        summary_payload = asdict(summary)
        summary_payload.update(runtime_metadata)
        summary_payload["run_completed_ts_ms"] = int(final_ts_ms)
        summary_payload["rolling_evidence"] = dict(rolling_evidence)
        summary_payload["model_alpha_min_prob_used"] = float(self._runtime_state.get("model_alpha_min_prob_used", 0.0))
        summary_payload["model_alpha_min_prob_source"] = str(self._runtime_state.get("model_alpha_min_prob_source", "manual"))
        summary_payload["model_alpha_top_pct_used"] = float(self._runtime_state.get("model_alpha_top_pct_used", 0.0))
        summary_payload["model_alpha_top_pct_source"] = str(self._runtime_state.get("model_alpha_top_pct_source", "manual"))
        summary_payload["model_alpha_min_candidates_used"] = int(self._runtime_state.get("model_alpha_min_candidates_used", 0))
        summary_payload["model_alpha_min_candidates_source"] = str(
            self._runtime_state.get("model_alpha_min_candidates_source", "manual")
        )
        summary_payload["model_alpha_operational_regime_score"] = float(
            self._runtime_state.get("model_alpha_operational_regime_score", 0.0)
        )
        summary_payload["model_alpha_operational_risk_multiplier"] = float(
            self._runtime_state.get("model_alpha_operational_risk_multiplier", 1.0)
        )
        summary_payload["model_alpha_operational_max_positions"] = int(
            self._runtime_state.get("model_alpha_operational_max_positions", 0)
        )
        summary_payload["micro_provider"] = str(micro_provider_info.get("provider") or "NONE")
        summary_payload["micro_provider_info"] = dict(micro_provider_info)
        summary_payload["feature_provider"] = _normalize_paper_feature_provider(self._run_settings.paper_feature_provider)
        summary_payload["warmup_elapsed_sec"] = float(round(warmup_elapsed_sec, 3))
        summary_payload["warmup_satisfied"] = bool(warmup_satisfied)
        summary_payload["warmup_trade_events_total"] = int(warmup_trade_events_total)
        summary_payload["micro_cache_markets_with_samples"] = int(warmup_markets_with_samples)
        summary_payload["execution_structure"] = summarize_fill_records(self._runtime_state.get("fill_records", []))
        _write_json(path=run_root / "summary.json", payload=summary_payload)
        _write_json(
            path=run_root / "micro_gate_blocked.json",
            payload={
                "candidates_total": candidates_total,
                "candidates_blocked_by_micro": candidates_blocked_by_micro,
                "blocked_ratio": micro_blocked_ratio,
                "reasons": micro_blocked_reasons,
            },
        )
        _write_json(
            path=run_root / "micro_order_policy_report.json",
            payload={
                "enabled": bool(self._run_settings.micro_order_policy.enabled),
                "mode": str(self._run_settings.micro_order_policy.mode),
                "on_missing": str(self._run_settings.micro_order_policy.on_missing),
                "tiers": _normalize_reason_counts(self._runtime_counters.get("micro_policy_tier_counts", {})),
                "fallback_reasons": _normalize_reason_counts(
                    self._runtime_counters.get("micro_policy_fallback_counts", {})
                ),
                "replace_reasons": _normalize_reason_counts(self._runtime_counters.get("order_supervisor_reasons", {})),
                "replaces_total": replaces_total,
                "cancels_total": cancels_total,
                "aborted_timeout_total": aborted_timeout_total,
                "dust_abort_total": dust_abort_total,
                "avg_time_to_fill_ms": mean(ttf_values),
                "p50_time_to_fill_ms": percentile(ttf_values, 0.50),
                "p90_time_to_fill_ms": percentile(ttf_values, 0.90),
                "avg_time_to_first_fill_ms": mean(first_fill_values),
                "p50_time_to_first_fill_ms": percentile(first_fill_values, 0.50),
                "p90_time_to_first_fill_ms": percentile(first_fill_values, 0.90),
                "avg_time_to_complete_fill_ms": mean(complete_fill_values),
                "p50_time_to_complete_fill_ms": percentile(complete_fill_values, 0.50),
                "p90_time_to_complete_fill_ms": percentile(complete_fill_values, 0.90),
                "slippage_bps_mean": mean(slippage_values),
                "slippage_bps_p50": percentile(slippage_values, 0.50),
                "slippage_bps_p90": percentile(slippage_values, 0.90),
                "tick_bps_stats": {
                    "mean": mean(policy_tick_bps_values),
                    "p90": percentile(policy_tick_bps_values, 0.90),
                    "max": max(policy_tick_bps_values) if policy_tick_bps_values else 0.0,
                },
                "cross_block_reasons": _normalize_reason_counts(
                    self._runtime_counters.get("micro_policy_cross_block_reasons", {})
                ),
                "cross_allowed_count": int(self._runtime_counters.get("micro_policy_cross_allowed_count", 0)),
                "cross_used_count": int(self._runtime_counters.get("micro_policy_cross_used_count", 0)),
                "resolver_failed_fallback_used_count": int(
                    self._runtime_counters.get("micro_policy_resolver_failed_fallback_used", 0)
                ),
            },
        )
        _write_trade_artifacts(
            run_root=run_root,
            fill_records=self._runtime_state.get("fill_records", []),
        )
        return summary

    def _run_model_alpha_cycle(
        self,
        *,
        ts_ms: int,
        latest_trade_price: float,
        universe: UniverseProviderTop20,
        market_data: MarketDataHub,
        trade_gate: TradeGateV1,
        micro_order_policy: MicroOrderPolicyV1 | None,
        micro_snapshot_provider: MicroSnapshotProvider | None,
        exchange: PaperSimExchange,
        execution: PaperExecutionGateway,
        event_store: JsonlEventStore,
        append_event: Callable[[str, int, dict[str, Any] | None], None],
        strategy: ModelAlphaStrategyV1,
    ) -> None:
        markets = universe.markets()
        live_feature_provider = self._runtime_state.get("live_feature_provider")
        if _is_live_feature_provider(live_feature_provider):
            append_event(
                "FEATURE_PROVIDER_STATUS",
                ts_ms=ts_ms,
                payload=live_feature_provider.status(now_ts_ms=ts_ms, requested_ts_ms=ts_ms),
            )
        open_markets = exchange.open_position_markets()
        result = strategy.on_ts(
            ts_ms=ts_ms,
            active_markets=markets,
            latest_prices=market_data.latest_prices(),
            open_markets=open_markets,
        )
        if _is_live_feature_provider(live_feature_provider):
            live_payload = dict(live_feature_provider.last_build_stats())
            live_payload["model_selection_scored_rows"] = int(result.scored_rows)
            live_payload["model_selection_eligible_rows"] = int(result.eligible_rows)
            live_payload["model_selection_selected_rows"] = int(result.selected_rows)
            live_payload["model_selection_blocked_min_candidates_ts"] = int(result.blocked_min_candidates_ts)
            live_payload["model_selection_min_prob_used"] = float(result.min_prob_used)
            live_payload["model_selection_min_prob_source"] = str(result.min_prob_source)
            live_payload["model_selection_top_pct_used"] = float(result.top_pct_used)
            live_payload["model_selection_top_pct_source"] = str(result.top_pct_source)
            live_payload["model_selection_min_candidates_used"] = int(result.min_candidates_used)
            live_payload["model_selection_min_candidates_source"] = str(result.min_candidates_source)
            live_payload["operational_regime_score"] = float(result.operational_regime_score)
            live_payload["operational_risk_multiplier"] = float(result.operational_risk_multiplier)
            live_payload["operational_max_positions"] = int(result.operational_max_positions)
            live_payload["operational_state"] = dict(result.operational_state)
            append_event(
                "LIVE_FEATURES_BUILT",
                ts_ms=ts_ms,
                payload=live_payload,
            )
        self._runtime_counters["scored_rows"] = int(self._runtime_counters.get("scored_rows", 0)) + int(
            result.scored_rows
        )
        self._runtime_counters["eligible_rows"] = int(self._runtime_counters.get("eligible_rows", 0)) + int(
            result.eligible_rows
        )
        self._runtime_counters["selected_rows"] = int(self._runtime_counters.get("selected_rows", 0)) + int(
            result.selected_rows
        )
        self._runtime_counters["skipped_missing_features_rows"] = int(
            self._runtime_counters.get("skipped_missing_features_rows", 0)
        ) + int(result.skipped_missing_features_rows)
        self._runtime_counters["dropped_min_prob_rows"] = int(self._runtime_counters.get("dropped_min_prob_rows", 0)) + int(
            result.dropped_min_prob_rows
        )
        self._runtime_counters["dropped_top_pct_rows"] = int(self._runtime_counters.get("dropped_top_pct_rows", 0)) + int(
            result.dropped_top_pct_rows
        )
        self._runtime_counters["blocked_min_candidates_ts"] = int(
            self._runtime_counters.get("blocked_min_candidates_ts", 0)
        ) + int(result.blocked_min_candidates_ts)
        debug_reasons = self._runtime_counters.setdefault("debug_mismatch_reasons", {})
        if isinstance(debug_reasons, dict):
            for reason, count in result.skipped_reasons.items():
                _note_reason_count(reason_code=reason, reason_counts=debug_reasons, delta=int(count))
        self._runtime_state["model_alpha_min_prob_used"] = float(result.min_prob_used)
        self._runtime_state["model_alpha_min_prob_source"] = str(result.min_prob_source)
        self._runtime_state["model_alpha_top_pct_used"] = float(result.top_pct_used)
        self._runtime_state["model_alpha_top_pct_source"] = str(result.top_pct_source)
        self._runtime_state["model_alpha_min_candidates_used"] = int(result.min_candidates_used)
        self._runtime_state["model_alpha_min_candidates_source"] = str(result.min_candidates_source)
        self._runtime_state["model_alpha_operational_regime_score"] = float(result.operational_regime_score)
        self._runtime_state["model_alpha_operational_risk_multiplier"] = float(result.operational_risk_multiplier)
        self._runtime_state["model_alpha_operational_max_positions"] = int(result.operational_max_positions)
        regime_scores = self._runtime_state.setdefault("operational_regime_scores", [])
        if isinstance(regime_scores, list):
            regime_scores.append(float(result.operational_regime_score))
        operational_state = dict(result.operational_state)
        breadth_ratios = self._runtime_state.setdefault("operational_breadth_ratios", [])
        if isinstance(breadth_ratios, list):
            try:
                breadth_ratios.append(float(operational_state.get("breadth_ratio", 0.0)))
            except (TypeError, ValueError):
                pass
        max_positions_values = self._runtime_state.setdefault("operational_max_positions_values", [])
        if isinstance(max_positions_values, list):
            max_positions_values.append(float(result.operational_max_positions))
        selection_payload = {
            "scored_rows": int(result.scored_rows),
            "eligible_rows": int(result.eligible_rows),
            "selected_rows": int(result.selected_rows),
            "intents": int(len(result.intents)),
            "skipped_missing_features_rows": int(result.skipped_missing_features_rows),
            "dropped_min_prob_rows": int(result.dropped_min_prob_rows),
            "dropped_top_pct_rows": int(result.dropped_top_pct_rows),
            "blocked_min_candidates_ts": int(result.blocked_min_candidates_ts),
            "min_prob_used": float(result.min_prob_used),
            "min_prob_source": str(result.min_prob_source),
            "top_pct_used": float(result.top_pct_used),
            "top_pct_source": str(result.top_pct_source),
            "min_candidates_used": int(result.min_candidates_used),
            "min_candidates_source": str(result.min_candidates_source),
            "operational_regime_score": float(result.operational_regime_score),
            "operational_risk_multiplier": float(result.operational_risk_multiplier),
            "operational_max_positions": int(result.operational_max_positions),
            "operational_state": operational_state,
            "reasons": dict(result.skipped_reasons),
        }
        if _is_live_feature_provider(live_feature_provider):
            live_stats = live_feature_provider.last_build_stats()
            selection_payload["live_feature_skip_reasons"] = dict(live_stats.get("skip_reasons", {}))
            selection_payload["live_feature_skipped_markets"] = int(live_stats.get("skipped_markets", 0))
        append_event(
            "MODEL_ALPHA_SELECTION",
            ts_ms=ts_ms,
            payload=selection_payload,
        )
        for intent in result.intents:
            if str(intent.side).strip().lower() == "ask":
                self._runtime_counters["exit_intents_total"] = int(self._runtime_counters.get("exit_intents_total", 0)) + 1
            candidate = _intent_to_candidate(intent)
            self._try_submit_candidate(
                ts_ms=ts_ms,
                candidate=candidate,
                latest_trade_price=latest_trade_price,
                market_data=market_data,
                trade_gate=trade_gate,
                micro_order_policy=micro_order_policy,
                micro_snapshot_provider=micro_snapshot_provider,
                exchange=exchange,
                execution=execution,
                event_store=event_store,
                append_event=append_event,
            )

    def _build_model_alpha_strategy(
        self,
        *,
        active_markets: list[str],
        decision_start_ts_ms: int,
        decision_end_ts_ms: int,
    ) -> ModelAlphaStrategyV1:
        settings = self._run_settings
        feature_set = str(settings.feature_set).strip().lower() or "v3"
        if feature_set not in {"v3", "v4"}:
            raise ValueError("paper model_alpha_v1 currently requires --feature-set v3 or v4")
        model_ref = str(settings.model_ref or settings.model_alpha.model_ref).strip()
        if not model_ref:
            raise ValueError("paper model_alpha_v1 requires model_ref")
        model_family = str(settings.model_family).strip() if settings.model_family else settings.model_alpha.model_family
        predictor = load_predictor_from_registry(
            registry_root=Path(settings.model_registry_root),
            model_ref=model_ref,
            model_family=model_family,
        )
        resolved_model_alpha, runtime_recommendation_state = resolve_runtime_model_alpha_settings(
            predictor=predictor,
            settings=settings.model_alpha,
        )
        self._runtime_state["resolved_model_alpha_settings"] = resolved_model_alpha
        self._runtime_state["model_alpha_runtime_recommendation_state"] = runtime_recommendation_state
        feature_provider_mode = _normalize_paper_feature_provider(settings.paper_feature_provider)
        if feature_provider_mode == "LIVE_V3":
            if feature_set != "v3":
                raise ValueError("paper LIVE_V3 provider requires --feature-set v3")
            live_feature_provider = LiveFeatureProviderV3(
                feature_columns=predictor.feature_columns,
                extra_columns=resolve_model_alpha_runtime_row_columns(predictor=predictor),
                tf=str(settings.tf).strip().lower(),
                micro_snapshot_provider=self._runtime_state.get("micro_snapshot_provider_for_features"),
                micro_max_age_ms=max(int(settings.paper_live_micro_max_age_ms), 0),
                parquet_root=str(settings.paper_live_parquet_root),
                candles_dataset_name=str(settings.paper_live_candles_dataset),
                bootstrap_1m_bars=max(int(settings.paper_live_bootstrap_1m_bars), 256),
            )
            self._runtime_state["live_feature_provider"] = live_feature_provider
            interval_ms = _interval_ms_from_tf(settings.tf)
            return ModelAlphaStrategyV1(
                predictor=predictor,
                feature_groups=(),
                settings=resolved_model_alpha,
                interval_ms=interval_ms,
                enable_operational_overlay=True,
                live_frame_provider=lambda ts_ms, markets: live_feature_provider.build_frame(
                    ts_ms=int(ts_ms),
                    markets=markets,
                ),
            )

        if feature_provider_mode == "LIVE_V4":
            if feature_set != "v4":
                raise ValueError("paper LIVE_V4 provider requires --feature-set v4")
            live_feature_provider = LiveFeatureProviderV4(
                feature_columns=predictor.feature_columns,
                extra_columns=resolve_model_alpha_runtime_row_columns(predictor=predictor),
                tf=str(settings.tf).strip().lower(),
                quote=str(settings.quote).strip().upper(),
                micro_snapshot_provider=self._runtime_state.get("micro_snapshot_provider_for_features"),
                micro_max_age_ms=max(int(settings.paper_live_micro_max_age_ms), 0),
                parquet_root=str(settings.paper_live_parquet_root),
                candles_dataset_name=str(settings.paper_live_candles_dataset),
                bootstrap_1m_bars=max(int(settings.paper_live_bootstrap_1m_bars), 256),
            )
            self._runtime_state["live_feature_provider"] = live_feature_provider
            interval_ms = _interval_ms_from_tf(settings.tf)
            return ModelAlphaStrategyV1(
                predictor=predictor,
                feature_groups=(),
                settings=resolved_model_alpha,
                interval_ms=interval_ms,
                enable_operational_overlay=True,
                live_frame_provider=lambda ts_ms, markets: live_feature_provider.build_frame(
                    ts_ms=int(ts_ms),
                    markets=markets,
                ),
            )

        if feature_provider_mode == "LIVE_V4_NATIVE":
            if feature_set != "v4":
                raise ValueError("paper LIVE_V4_NATIVE provider requires --feature-set v4")
            live_feature_provider = LiveFeatureProviderV4Native(
                feature_columns=predictor.feature_columns,
                extra_columns=resolve_model_alpha_runtime_row_columns(predictor=predictor),
                tf=str(settings.tf).strip().lower(),
                quote=str(settings.quote).strip().upper(),
                micro_snapshot_provider=self._runtime_state.get("micro_snapshot_provider_for_features"),
                micro_max_age_ms=max(int(settings.paper_live_micro_max_age_ms), 0),
                parquet_root=str(settings.paper_live_parquet_root),
                candles_dataset_name=str(settings.paper_live_candles_dataset),
                bootstrap_1m_bars=max(int(settings.paper_live_bootstrap_1m_bars), 256),
            )
            self._runtime_state["live_feature_provider"] = live_feature_provider
            interval_ms = _interval_ms_from_tf(settings.tf)
            return ModelAlphaStrategyV1(
                predictor=predictor,
                feature_groups=(),
                settings=resolved_model_alpha,
                interval_ms=interval_ms,
                enable_operational_overlay=True,
                live_frame_provider=lambda ts_ms, markets: live_feature_provider.build_frame(
                    ts_ms=int(ts_ms),
                    markets=markets,
                ),
            )

        dataset_root = (
            Path(str(settings.model_feature_dataset_root))
            if settings.model_feature_dataset_root
            else predictor.dataset_root
        )
        if dataset_root is None:
            raise ValueError("unable to resolve feature dataset root for paper model_alpha_v1")
        if not dataset_root.exists():
            raise FileNotFoundError(f"feature dataset root not found: {dataset_root}")
        request = DatasetRequest(
            dataset_root=dataset_root,
            tf=str(settings.tf).strip().lower(),
            quote=str(settings.quote).strip().upper(),
            top_n=max(int(settings.top_n), 1),
            start_ts_ms=int(decision_start_ts_ms),
            end_ts_ms=int(decision_end_ts_ms),
            markets=tuple(active_markets),
            batch_rows=200_000,
        )
        groups = iter_feature_rows_grouped_by_ts(
            request,
            feature_columns=predictor.feature_columns,
            extra_columns=resolve_model_alpha_runtime_row_columns(predictor=predictor),
        )
        self._runtime_state["live_feature_provider"] = None
        interval_ms = _interval_ms_from_tf(settings.tf)
        return ModelAlphaStrategyV1(
            predictor=predictor,
            feature_groups=groups,
            settings=resolved_model_alpha,
            interval_ms=interval_ms,
            enable_operational_overlay=True,
        )

    def _run_candidate_cycle(
        self,
        *,
        ts_ms: int,
        latest_trade_price: float,
        universe: UniverseProviderTop20,
        market_data: MarketDataHub,
        candidate_generator: CandidateGeneratorV1,
        trade_gate: TradeGateV1,
        micro_order_policy: MicroOrderPolicyV1 | None,
        micro_snapshot_provider: MicroSnapshotProvider | None,
        exchange: PaperSimExchange,
        execution: PaperExecutionGateway,
        event_store: JsonlEventStore,
        append_event: Callable[[str, int, dict[str, Any] | None], None],
    ) -> None:
        markets = universe.markets()
        append_event(
            "MARKET_SNAPSHOT",
            ts_ms=ts_ms,
            payload={
                "count": len(markets),
                "markets": markets,
            },
        )

        candidates = candidate_generator.generate(markets=markets, market_data=market_data)
        if not candidates and self._runtime_counters["orders_submitted"] == 0:
            for market in markets:
                ticker = market_data.get_latest_ticker(market)
                if ticker is None:
                    continue
                candidates.append(
                    Candidate(
                        market=market,
                        score=0.0,
                        proposed_side="bid",
                        ref_price=ticker.trade_price,
                        meta={"fallback": True, "reason": "NO_MOMENTUM_CANDIDATE"},
                    )
                )
                break
        append_event(
            "CANDIDATES",
            ts_ms=ts_ms,
            payload={
                "count": len(candidates),
                "top": [
                    {
                        "market": item.market,
                        "score": item.score,
                        "proposed_side": item.proposed_side,
                        "ref_price": item.ref_price,
                    }
                    for item in candidates[:5]
                ],
            },
        )

        for candidate in candidates:
            if self._try_submit_candidate(
                ts_ms=ts_ms,
                candidate=candidate,
                latest_trade_price=latest_trade_price,
                market_data=market_data,
                trade_gate=trade_gate,
                micro_order_policy=micro_order_policy,
                micro_snapshot_provider=micro_snapshot_provider,
                exchange=exchange,
                execution=execution,
                event_store=event_store,
                append_event=append_event,
            ):
                break

    def _try_submit_candidate(
        self,
        *,
        ts_ms: int,
        candidate: Candidate,
        latest_trade_price: float,
        market_data: MarketDataHub,
        trade_gate: TradeGateV1,
        micro_order_policy: MicroOrderPolicyV1 | None,
        micro_snapshot_provider: MicroSnapshotProvider | None,
        exchange: PaperSimExchange,
        execution: PaperExecutionGateway,
        event_store: JsonlEventStore,
        append_event: Callable[[str, int, dict[str, Any] | None], None],
    ) -> bool:
        self._runtime_counters["candidates_total"] = int(self._runtime_counters.get("candidates_total", 0)) + 1
        ticker = market_data.get_latest_ticker(candidate.market)
        if ticker is None:
            return False

        rules = self._rules_provider.get_rules(
            market=candidate.market,
            reference_price=candidate.ref_price,
            ts_ms=ts_ms,
        )
        side_value = str(candidate.proposed_side).strip().lower()
        strategy_mode = str(self._run_settings.strategy).strip().lower() or "candidates_v1"
        ref_price = max(float(candidate.ref_price), float(ticker.trade_price), 1e-8)
        model_prob = (
            _safe_optional_float(candidate.meta.get("model_prob"))
            if isinstance(candidate.meta, dict)
            else None
        )
        snapshot = (
            micro_snapshot_provider.get(candidate.market, int(ts_ms))
            if micro_snapshot_provider is not None
            else None
        )
        resolved_model_alpha_settings = self._runtime_state.get("resolved_model_alpha_settings")
        exec_profile = _strategy_paper_exec_profile(
            self._run_settings,
            model_alpha_settings=(
                resolved_model_alpha_settings
                if isinstance(resolved_model_alpha_settings, ModelAlphaSettings)
                else None
            ),
        )
        if strategy_mode == "model_alpha_v1" and isinstance(candidate.meta, dict):
            strategy_exec_profile = candidate.meta.get("exec_profile")
            if isinstance(strategy_exec_profile, dict) and strategy_exec_profile:
                exec_profile = order_exec_profile_from_dict(strategy_exec_profile, fallback=exec_profile)
        execution_recommendations_enabled = bool(
            strategy_mode == "model_alpha_v1"
            and bool(self._run_settings.model_alpha.execution.use_learned_recommendations)
        )
        operational_decision = None
        if strategy_mode == "model_alpha_v1" and bool(self._run_settings.model_alpha.operational.enabled):
            operational_decision = resolve_operational_execution_overlay(
                base_profile=exec_profile,
                settings=self._run_settings.model_alpha.operational,
                micro_quality=compute_micro_quality_composite(
                    micro_snapshot=snapshot,
                    now_ts_ms=ts_ms,
                    settings=self._run_settings.model_alpha.operational,
                ),
                ts_ms=ts_ms,
            )
            if operational_decision.abort_reason is not None:
                append_event(
                    "OPERATIONAL_MICRO_QUALITY_ABORT",
                    ts_ms=ts_ms,
                    payload={
                        "market": candidate.market,
                        "side": side_value,
                        "reason_code": str(operational_decision.abort_reason),
                        "diagnostics": dict(operational_decision.diagnostics),
                    },
                )
                return False
            exec_profile = operational_decision.exec_profile
        forced_volume = None
        if isinstance(candidate.meta, dict):
            forced_volume = _safe_optional_float(candidate.meta.get("force_volume"))
        entry_notional_quote = (
            _entry_notional_quote_for_strategy(
                strategy_mode=strategy_mode,
                per_trade_krw=float(self._run_settings.per_trade_krw),
                min_total_krw=max(float(rules.min_total), float(self._run_settings.min_order_krw)),
                model_alpha_settings=self._run_settings.model_alpha,
                candidate_meta=(candidate.meta if isinstance(candidate.meta, dict) else None),
            )
            if side_value == "bid" and (forced_volume is None or forced_volume <= 0)
            else None
        )
        if entry_notional_quote is not None and operational_decision is not None:
            entry_notional_quote *= max(float(operational_decision.risk_multiplier), 0.0)
        if side_value == "ask" and (forced_volume is None or forced_volume <= 0):
            base_currency = _base_currency(candidate.market)
            if base_currency:
                forced_volume = _safe_optional_float(getattr(exchange.coin_balance(base_currency), "free", 0.0))
        if side_value == "ask" and exchange.has_open_order(candidate.market, side="ask"):
            append_event(
                "TRADE_GATE_BLOCKED",
                ts_ms=ts_ms,
                payload={
                    "market": candidate.market,
                    "side": side_value,
                    "reason_code": "DUPLICATE_EXIT_ORDER",
                    "detail": "ask order already open",
                    "severity": "BLOCK",
                    "gate_reasons": ["DUPLICATE_EXIT_ORDER"],
                    "diagnostics": {},
                },
            )
            return False
        gate_price = round_price_to_tick(
            price=max(candidate.ref_price, ticker.trade_price),
            tick_size=rules.tick_size,
            side=side_value,
        )
        gate_volume = (
            float(forced_volume)
            if forced_volume is not None and forced_volume > 0
            else order_volume_from_notional(
                notional_quote=max(float(entry_notional_quote or self._run_settings.per_trade_krw), 1.0),
                price=gate_price,
            )
        )
        if gate_volume <= 0:
            append_event(
                "TRADE_GATE_BLOCKED",
                ts_ms=ts_ms,
                payload={
                    "market": candidate.market,
                    "side": side_value,
                    "reason_code": "ZERO_VOLUME",
                    "detail": "computed order volume is zero",
                    "severity": "BLOCK",
                    "gate_reasons": ["ZERO_VOLUME"],
                    "diagnostics": {},
                },
            )
            return False
        fee_rate = rules.fee_rate(side=side_value, maker_or_taker="taker")
        decision = trade_gate.evaluate(
            ts_ms=ts_ms,
            market=candidate.market,
            side=side_value,
            price=gate_price,
            volume=gate_volume,
            fee_rate=fee_rate,
            exchange=exchange,
            min_total_krw=rules.min_total,
        )
        if not decision.allowed:
            if _is_micro_decision_block(decision):
                self._runtime_counters["candidates_blocked_by_micro"] = int(
                    self._runtime_counters.get("candidates_blocked_by_micro", 0)
                ) + 1
                _note_micro_block(
                    reasons=decision.gate_reasons,
                    reason_counts=self._runtime_counters.setdefault("micro_blocked_reasons", {}),
                )
            append_event(
                "TRADE_GATE_BLOCKED",
                ts_ms=ts_ms,
                payload={
                    "market": candidate.market,
                    "side": side_value,
                    "reason_code": decision.reason_code,
                    "detail": decision.detail,
                    "severity": decision.severity,
                    "gate_reasons": list(decision.gate_reasons),
                    "diagnostics": decision.diagnostics or {},
                },
            )
            return False

        if decision.severity == "WARN":
            append_event(
                "TRADE_GATE_WARN_ALLOW",
                ts_ms=ts_ms,
                payload={
                    "market": candidate.market,
                    "side": side_value,
                    "reason_code": decision.reason_code,
                    "detail": decision.detail,
                    "severity": decision.severity,
                    "gate_reasons": list(decision.gate_reasons),
                    "diagnostics": decision.diagnostics or {},
                },
            )

        policy_decision = None
        policy_diagnostics: dict[str, Any] = {}
        if micro_order_policy is not None:
            policy_decision = micro_order_policy.evaluate(
                micro_snapshot=snapshot,
                base_profile=(exec_profile if strategy_mode == "model_alpha_v1" else None),
                market=candidate.market,
                ref_price=ref_price,
                tick_size=rules.tick_size,
                replace_attempt=0,
                model_prob=model_prob,
                now_ts_ms=ts_ms,
            )
            policy_diagnostics = dict(policy_decision.diagnostics or {})
            if not policy_decision.allow:
                self._runtime_counters["candidates_aborted_by_policy"] = int(
                    self._runtime_counters.get("candidates_aborted_by_policy", 0)
                ) + 1
                _note_reason_count(
                    reason_code=str(policy_decision.reason_code),
                    reason_counts=self._runtime_counters.setdefault("micro_policy_fallback_counts", {}),
                )
                append_event(
                    "MICRO_ORDER_POLICY_ABORT",
                    ts_ms=ts_ms,
                    payload={
                        "market": candidate.market,
                        "side": side_value,
                        "reason_code": policy_decision.reason_code,
                        "detail": policy_decision.detail,
                        "diagnostics": policy_diagnostics,
                    },
                )
                return False
            if policy_decision.profile is not None:
                exec_profile = policy_decision.profile
            _record_micro_policy_diagnostics(
                counters=self._runtime_counters,
                runtime_state=self._runtime_state,
                diagnostics=policy_diagnostics,
                count_cross_used=str(exec_profile.price_mode).strip().upper() == PRICE_MODE_CROSS_1T,
            )
            tier_code = str(policy_decision.tier or "NONE").strip().upper() or "NONE"
            _note_reason_count(
                reason_code=tier_code,
                reason_counts=self._runtime_counters.setdefault("micro_policy_tier_counts", {}),
            )
            if str(policy_decision.reason_code).strip().upper() == REASON_MICRO_MISSING_FALLBACK:
                _note_reason_count(
                    reason_code=REASON_MICRO_MISSING_FALLBACK,
                    reason_counts=self._runtime_counters.setdefault("micro_policy_fallback_counts", {}),
                )
        execution_policy: dict[str, Any] | None = None
        if strategy_mode == "model_alpha_v1" and side_value == "bid" and execution_recommendations_enabled:
            execution_contract = dict(self._runtime_state.get("execution_contract") or {})
            if int(execution_contract.get("rows_total", 0) or 0) > 0:
                expected_edge_bps = _candidate_expected_edge_bps(candidate.meta if isinstance(candidate.meta, dict) else None)
                execution_policy = select_live_execution_action(
                    model_payload=execution_contract,
                    current_state=build_execution_policy_state(
                        micro_state=_execution_contract_micro_state(snapshot=snapshot, now_ts_ms=ts_ms),
                        expected_edge_bps=expected_edge_bps,
                    ),
                    expected_edge_bps=expected_edge_bps,
                    candidate_actions=candidate_action_codes_for_price_mode(price_mode=str(exec_profile.price_mode)),
                    deadline_ms=max(int(exec_profile.timeout_ms), 1),
                )
                if str(execution_policy.get("status", "")).strip().lower() == "skip":
                    self._runtime_counters["candidates_aborted_by_policy"] = int(
                        self._runtime_counters.get("candidates_aborted_by_policy", 0)
                    ) + 1
                    append_event(
                        "EXECUTION_POLICY_ABORT",
                        ts_ms=ts_ms,
                        payload={
                            "market": candidate.market,
                            "side": side_value,
                            "reason_code": str(
                                execution_policy.get("skip_reason_code", "LIVE_EXECUTION_NO_POSITIVE_UTILITY")
                            ),
                            "execution_policy": execution_policy,
                        },
                    )
                    return False
                selected_price_mode = str(
                    execution_policy.get("selected_price_mode", exec_profile.price_mode)
                ).strip().upper() or str(exec_profile.price_mode).strip().upper()
                if selected_price_mode != str(exec_profile.price_mode).strip().upper():
                    exec_profile = normalize_order_exec_profile(
                        OrderExecProfile(
                            timeout_ms=int(exec_profile.timeout_ms),
                            replace_interval_ms=int(exec_profile.replace_interval_ms),
                            max_replaces=int(exec_profile.max_replaces),
                            price_mode=selected_price_mode,
                            max_chase_bps=int(exec_profile.max_chase_bps),
                            min_replace_interval_ms_global=int(exec_profile.min_replace_interval_ms_global),
                            post_only=bool(exec_profile.post_only),
                        )
                    )

        limit_price = build_limit_price_from_mode(
            side=side_value,
            ref_price=ref_price,
            tick_size=rules.tick_size,
            price_mode=exec_profile.price_mode,
        )
        volume = (
            float(forced_volume)
            if forced_volume is not None and forced_volume > 0
            else order_volume_from_notional(
                notional_quote=max(float(entry_notional_quote or self._run_settings.per_trade_krw), 1.0),
                price=limit_price,
            )
        )
        profile_payload = order_exec_profile_to_dict(exec_profile)
        selected_ord_type = (
            str((execution_policy or {}).get("selected_ord_type", "limit")).strip().lower() or "limit"
        )
        selected_time_in_force = (
            str((execution_policy or {}).get("selected_time_in_force", "gtc")).strip().lower() or "gtc"
        )
        simulated_ord_type = selected_ord_type
        policy_payload = {
            "enabled": bool(micro_order_policy is not None),
            "tier": str(policy_decision.tier) if policy_decision is not None and policy_decision.tier is not None else None,
            "reason_code": (
                str(policy_decision.reason_code)
                if policy_decision is not None
                else "POLICY_DISABLED"
            ),
        }
        operational_payload = {}
        if operational_decision is not None:
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
            quality_scores = self._runtime_state.setdefault("operational_micro_quality_scores", [])
            if isinstance(quality_scores, list) and operational_payload.get("micro_quality_score") is not None:
                quality_scores.append(float(operational_payload["micro_quality_score"]))
            runtime_risk_values = self._runtime_state.setdefault("operational_runtime_risk_multipliers", [])
            if isinstance(runtime_risk_values, list):
                runtime_risk_values.append(float(operational_decision.risk_multiplier))
        reason_code_value = "CANDIDATE_V1"
        if isinstance(candidate.meta, dict):
            raw_reason_code = candidate.meta.get("reason_code")
            if isinstance(raw_reason_code, str) and raw_reason_code.strip():
                reason_code_value = raw_reason_code.strip().upper()

        intent = new_order_intent(
            market=candidate.market,
            side=side_value,
            ord_type=simulated_ord_type,
            time_in_force=selected_time_in_force,
            price=limit_price,
            volume=volume,
            reason_code=reason_code_value,
            meta={
                **candidate.meta,
                "candidate_score": candidate.score,
                "target_notional_quote": (float(entry_notional_quote) if entry_notional_quote is not None else None),
                "tick_size": rules.tick_size,
                "min_total": rules.min_total,
                "gate_severity": decision.severity,
                "gate_reasons": list(decision.gate_reasons),
                "exec_profile": profile_payload,
                "initial_ref_price": ref_price,
                "execution_policy": (
                    {
                        **execution_policy,
                        "selected_ord_type_runtime": selected_ord_type,
                        "selected_ord_type_simulated": simulated_ord_type,
                    }
                    if isinstance(execution_policy, dict)
                    else {}
                ),
                "micro_order_policy": policy_payload,
                "micro_diagnostics": policy_diagnostics,
                "operational_overlay": operational_payload,
            },
            ts_ms=ts_ms,
        )
        intent_context = self._runtime_state.setdefault("intent_context", {})
        if isinstance(intent_context, dict) and intent.intent_id not in intent_context:
            intent_context[intent.intent_id] = {
                "first_submit_ts_ms": int(ts_ms),
                "initial_ref_price": float(ref_price),
                "side": intent.side,
                "reason_code": intent.reason_code,
                "strategy_meta": dict(intent.meta) if isinstance(intent.meta, dict) else {},
                "exec_profile": profile_payload,
                "micro_diagnostics": policy_diagnostics,
                "micro_order_policy": policy_payload,
                "operational_overlay": operational_payload,
            }
        append_event("INTENT_CREATED", ts_ms=ts_ms, payload=asdict(intent))

        update = execution.submit_intent(intent=intent, latest_trade_price=latest_trade_price, ts_ms=ts_ms)
        self._apply_execution_update(
            update=update,
            trade_gate=trade_gate,
            event_store=event_store,
            append_event=append_event,
            ts_ms=ts_ms,
            counters=self._runtime_counters,
            strategy_adapter=self._runtime_state.get("strategy_adapter"),
        )
        return True

    def _apply_execution_update(
        self,
        *,
        update: ExecutionUpdate,
        trade_gate: TradeGateV1,
        event_store: JsonlEventStore,
        append_event: Callable[[str, int, dict[str, Any] | None], None],
        ts_ms: int,
        counters: dict[str, Any],
        strategy_adapter: ModelAlphaStrategyV1 | None = None,
    ) -> bool:
        has_ask_fill = False
        intent_context = self._runtime_state.setdefault("intent_context", {})
        first_filled_intents = self._runtime_state.setdefault("first_filled_intents", set())
        completed_intents = self._runtime_state.setdefault("completed_intents", set())
        filled_order_ids = self._runtime_state.setdefault("filled_order_ids", set())
        completed_order_ids = self._runtime_state.setdefault("completed_order_ids", set())
        partially_filled_order_ids = self._runtime_state.setdefault("partially_filled_order_ids", set())
        time_to_fill_ms = self._runtime_state.setdefault("time_to_fill_ms", [])
        time_to_first_fill_ms = self._runtime_state.setdefault("time_to_first_fill_ms", [])
        time_to_complete_fill_ms = self._runtime_state.setdefault("time_to_complete_fill_ms", [])
        slippage_values = self._runtime_state.setdefault("slippage_bps", [])
        fill_records = self._runtime_state.setdefault("fill_records", [])
        order_exec_profile_by_order_id = self._runtime_state.setdefault("order_exec_profile_by_order_id", {})
        order_policy_diag_by_order_id = self._runtime_state.setdefault("order_policy_diag_by_order_id", {})
        order_by_id = {
            str(order_id): order
            for order_id, order in update.order_states_after_fill.items()
            if isinstance(order, PaperOrder)
        }

        if isinstance(order_exec_profile_by_order_id, dict):
            for order_id, payload in update.order_exec_profiles.items():
                if isinstance(payload, dict):
                    order_exec_profile_by_order_id[str(order_id)] = dict(payload)
        if isinstance(order_policy_diag_by_order_id, dict):
            for order_id, payload in update.order_policy_diagnostics.items():
                if isinstance(payload, dict):
                    order_policy_diag_by_order_id[str(order_id)] = dict(payload)

        for order in update.orders_submitted:
            event_store.append_order(order)
            payload = asdict(order)
            if isinstance(intent_context, dict):
                context = intent_context.get(order.intent_id)
                if isinstance(context, dict):
                    exec_profile = (
                        order_exec_profile_by_order_id.get(order.order_id)
                        if isinstance(order_exec_profile_by_order_id, dict)
                        else None
                    )
                    if not isinstance(exec_profile, dict):
                        exec_profile = context.get("exec_profile")
                    diagnostics = (
                        order_policy_diag_by_order_id.get(order.order_id)
                        if isinstance(order_policy_diag_by_order_id, dict)
                        else None
                    )
                    if not isinstance(diagnostics, dict):
                        diagnostics = context.get("micro_diagnostics")
                    policy = context.get("micro_order_policy")
                    payload["exec_profile"] = dict(exec_profile) if isinstance(exec_profile, dict) else {}
                    payload["micro_diagnostics"] = dict(diagnostics) if isinstance(diagnostics, dict) else {}
                    payload["micro_order_policy"] = dict(policy) if isinstance(policy, dict) else {}
                    if isinstance(exec_profile, dict):
                        context["exec_profile"] = dict(exec_profile)
                    if isinstance(diagnostics, dict):
                        context["micro_diagnostics"] = dict(diagnostics)
            append_event(
                "ORDER_SUBMITTED",
                ts_ms=order.updated_ts_ms,
                payload=payload,
            )
            counters["orders_submitted"] += 1

        for order in update.orders_canceled:
            event_store.append_order(order)
            append_event(
                "ORDER_CANCELED",
                ts_ms=order.updated_ts_ms,
                payload=asdict(order),
            )
            counters["orders_canceled"] += 1
            counters["cancels_total"] = int(counters.get("cancels_total", 0)) + 1

        for item in update.supervisor_events:
            event_type = str(item.get("event_type", "")).strip().upper()
            ts_value = int(item.get("ts_ms", ts_ms))
            payload = item.get("payload")
            payload_dict = payload if isinstance(payload, dict) else {}
            append_event(event_type, ts_ms=ts_value, payload=payload_dict)
            policy_payload = payload_dict.get("micro_policy")
            if not isinstance(policy_payload, dict):
                continue
            if event_type == "ORDER_TIMEOUT":
                _record_micro_policy_diagnostics(
                    counters=counters,
                    runtime_state=self._runtime_state,
                    diagnostics=policy_payload,
                    count_cross_used=False,
                )
            elif event_type == "ORDER_REPLACED":
                mode_value = str(payload_dict.get("price_mode", policy_payload.get("selected_price_mode", ""))).strip().upper()
                _record_micro_policy_diagnostics(
                    counters=counters,
                    runtime_state=self._runtime_state,
                    diagnostics=policy_payload,
                    count_cross_used=(mode_value == PRICE_MODE_CROSS_1T),
                    include_tick_sample=False,
                    include_cross_gate=False,
                )

        for key, delta in update.counter_deltas.items():
            counters[key] = int(counters.get(key, 0)) + int(delta)

        reason_counts = counters.setdefault("order_supervisor_reasons", {})
        if isinstance(reason_counts, dict):
            for reason, count in update.reason_counts.items():
                _note_reason_count(reason_code=reason, reason_counts=reason_counts, delta=int(count))

        for fill in update.fills:
            event_store.append_fill(fill)
            counters["fill_events_total"] = int(counters.get("fill_events_total", 0)) + 1
            order = order_by_id.get(fill.order_id)
            fill_payload = asdict(fill)
            if order is not None:
                fill_payload["order_state"] = str(order.state).strip().upper()
                fill_payload["ord_type"] = str(order.ord_type).strip().lower()
                fill_payload["time_in_force"] = str(order.time_in_force).strip().lower()
                fill_payload["volume_req"] = float(order.volume_req)
                fill_payload["volume_filled_cumulative"] = float(order.volume_filled)
                fill_payload["remaining_volume"] = max(float(order.volume_req) - float(order.volume_filled), 0.0)
            append_event("ORDER_FILLED", ts_ms=fill.ts_ms, payload=fill_payload)
            reason_code = ""
            ref_price_value: float | None = None
            slippage_value: float | None = None
            price_mode_value = ""
            tick_bps_value: float | None = None
            if order is not None and isinstance(intent_context, dict):
                context = intent_context.get(order.intent_id)
                if isinstance(context, dict):
                    ref_price = _safe_optional_float(context.get("initial_ref_price"))
                    ref_price_value = float(ref_price) if ref_price is not None else None
                    reason_code = str(context.get("reason_code", "")).strip()
                    exec_profile_payload = (
                        order_exec_profile_by_order_id.get(order.order_id)
                        if isinstance(order_exec_profile_by_order_id, dict)
                        else None
                    )
                    if not isinstance(exec_profile_payload, dict):
                        exec_profile_payload = context.get("exec_profile")
                    if isinstance(exec_profile_payload, dict):
                        price_mode_value = str(exec_profile_payload.get("price_mode", "")).strip().upper()
                    policy_diag_payload = (
                        order_policy_diag_by_order_id.get(order.order_id)
                        if isinstance(order_policy_diag_by_order_id, dict)
                        else None
                    )
                    if isinstance(policy_diag_payload, dict):
                        tick_bps_raw = policy_diag_payload.get("tick_bps")
                        try:
                            tick_bps_value = float(tick_bps_raw) if tick_bps_raw is not None else None
                        except (TypeError, ValueError):
                            tick_bps_value = None
                    if ref_price is not None and ref_price > 0:
                        slip = slippage_bps(
                            side=order.side,
                            fill_price=fill.price,
                            ref_price=ref_price,
                        )
                        if slip is not None:
                            slippage_value = float(slip)
                            slippage_values.append(slippage_value)
            if isinstance(fill_records, list):
                fill_records.append(
                    {
                        "ts_ms": int(fill.ts_ms),
                        "market": str(fill.market).strip().upper(),
                        "side": (str(order.side).strip().lower() if order is not None else ""),
                        "ref_price": ref_price_value,
                        "tick_bps": tick_bps_value,
                        "order_price": (float(order.price) if order is not None else None),
                        "fill_price": float(fill.price),
                        "slippage_bps": slippage_value,
                        "price_mode": price_mode_value,
                        "price": float(fill.price),
                        "volume": float(fill.volume),
                        "notional_quote": float(fill.price) * float(fill.volume),
                        "fee_quote": float(fill.fee_quote),
                        "order_id": str(fill.order_id),
                        "intent_id": (str(order.intent_id) if order is not None else ""),
                        "reason_code": reason_code,
                        "order_state": (str(order.state).strip().upper() if order is not None else ""),
                        "ord_type": (str(order.ord_type).strip().lower() if order is not None else ""),
                        "time_in_force": (str(order.time_in_force).strip().lower() if order is not None else ""),
                        "volume_req": (float(order.volume_req) if order is not None else None),
                        "volume_filled_cumulative": (float(order.volume_filled) if order is not None else None),
                        "remaining_volume": (
                            max(float(order.volume_req) - float(order.volume_filled), 0.0)
                            if order is not None
                            else None
                        ),
                        "filled_fraction": (
                            float(order.volume_filled) / max(float(order.volume_req), 1e-12)
                            if order is not None and float(order.volume_req) > 0.0
                            else None
                        ),
                    }
                )
            if order is not None and strategy_adapter is not None:
                try:
                    fill_meta = {}
                    if isinstance(intent_context, dict):
                        context = intent_context.get(order.intent_id)
                        if isinstance(context, dict) and isinstance(context.get("strategy_meta"), dict):
                            fill_meta = dict(context.get("strategy_meta") or {})
                    strategy_adapter.on_fill(
                        StrategyFillEvent(
                            ts_ms=int(fill.ts_ms),
                            market=str(fill.market).strip().upper(),
                            side=str(order.side).strip().lower(),
                            price=float(fill.price),
                            volume=float(fill.volume),
                            fee_quote=float(fill.fee_quote),
                            meta=fill_meta,
                        )
                    )
                except Exception:
                    # keep paper loop resilient even if strategy bookkeeping fails
                    pass

        for order in update.orders_with_fill:
            event_store.append_order(order)
            if order.side == "ask" and float(order.volume_filled) > 0.0:
                has_ask_fill = True
            if isinstance(filled_order_ids, set) and order.order_id not in filled_order_ids:
                counters["orders_filled"] = int(counters.get("orders_filled", 0)) + 1
                filled_order_ids.add(order.order_id)
            if (
                str(order.state).strip().upper() != "FILLED"
                and float(order.volume_filled) > 0.0
                and isinstance(partially_filled_order_ids, set)
                and order.order_id not in partially_filled_order_ids
            ):
                counters["orders_partially_filled"] = int(counters.get("orders_partially_filled", 0)) + 1
                partially_filled_order_ids.add(order.order_id)
                append_event("ORDER_PARTIAL", ts_ms=order.updated_ts_ms, payload=asdict(order))
            if (
                float(order.volume_filled) > 0.0
                and isinstance(first_filled_intents, set)
                and order.intent_id not in first_filled_intents
                and isinstance(intent_context, dict)
            ):
                context = intent_context.get(order.intent_id)
                if isinstance(context, dict):
                    first_submit_ts = int(context.get("first_submit_ts_ms", order.created_ts_ms))
                else:
                    first_submit_ts = int(order.created_ts_ms)
                elapsed_ms = max(int(order.updated_ts_ms) - first_submit_ts, 0)
                if isinstance(time_to_first_fill_ms, list):
                    time_to_first_fill_ms.append(float(elapsed_ms))
                first_filled_intents.add(order.intent_id)
            if (
                str(order.state).strip().upper() == "FILLED"
                and isinstance(completed_intents, set)
                and order.intent_id not in completed_intents
                and isinstance(intent_context, dict)
            ):
                context = intent_context.get(order.intent_id)
                if isinstance(context, dict):
                    first_submit_ts = int(context.get("first_submit_ts_ms", order.created_ts_ms))
                else:
                    first_submit_ts = int(order.created_ts_ms)
                elapsed_ms = max(int(order.updated_ts_ms) - first_submit_ts, 0)
                if isinstance(time_to_fill_ms, list):
                    time_to_fill_ms.append(float(elapsed_ms))
                if isinstance(time_to_complete_fill_ms, list):
                    time_to_complete_fill_ms.append(float(elapsed_ms))
                if isinstance(completed_order_ids, set) and order.order_id not in completed_order_ids:
                    counters["orders_completed"] = int(counters.get("orders_completed", 0)) + 1
                    completed_order_ids.add(order.order_id)
                completed_intents.add(order.intent_id)

        for market in update.success_markets:
            trade_gate.record_success(market)

        for market in update.failed_markets:
            trade_gate.record_failure(market, ts_ms=ts_ms)
            counters["intents_failed"] += 1

        return has_ask_fill

    def _ingest_live_micro_from_ticker(
        self,
        *,
        provider: LiveWsMicroSnapshotProvider,
        ticker: TickerEvent,
    ) -> None:
        market = str(ticker.market).strip().upper()
        if not market:
            return
        if ticker.trade_price <= 0:
            return

        acc_by_market = self._runtime_state.setdefault("live_ws_acc_notional_by_market", {})
        trade_counts = self._runtime_state.setdefault("live_ws_trade_events_by_market", {})
        last_event_by_market = self._runtime_state.setdefault("live_ws_last_event_ts_ms_by_market", {})
        if not isinstance(acc_by_market, dict) or not isinstance(trade_counts, dict) or not isinstance(last_event_by_market, dict):
            return

        current_acc = max(float(ticker.acc_trade_price_24h), 0.0)
        prev_acc = _safe_optional_float(acc_by_market.get(market))
        if prev_acc is None:
            notional_delta = max(float(ticker.trade_price) * 1e-8, 1e-8)
        elif current_acc >= prev_acc:
            notional_delta = max(current_acc - prev_acc, 0.0)
            if notional_delta <= 0:
                notional_delta = max(float(ticker.trade_price) * 1e-8, 1e-8)
        else:
            notional_delta = max(float(ticker.trade_price) * 1e-8, 1e-8)

        acc_by_market[market] = float(current_acc)
        volume = max(notional_delta / max(float(ticker.trade_price), 1e-8), 1e-12)
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
        trade_counts[market] = int(trade_counts.get(market, 0)) + 1
        last_event_by_market[market] = int(ticker.ts_ms)

    def _ingest_live_micro_trade_event(
        self,
        *,
        provider: LiveWsMicroSnapshotProvider,
        event: TradeEvent,
    ) -> None:
        market = str(event.market).strip().upper()
        if not market or float(event.trade_price) <= 0 or float(event.trade_volume) <= 0:
            return
        provider.ingest_trade(
            {
                "market": market,
                "ts_ms": int(event.ts_ms),
                "trade_ts_ms": int(event.ts_ms),
                "price": float(event.trade_price),
                "volume": float(event.trade_volume),
                "ask_bid": str(event.ask_bid).strip().upper(),
            }
        )
        trade_counts = self._runtime_state.setdefault("live_ws_trade_events_by_market", {})
        last_event_by_market = self._runtime_state.setdefault("live_ws_last_event_ts_ms_by_market", {})
        if isinstance(trade_counts, dict):
            trade_counts[market] = int(trade_counts.get(market, 0)) + 1
        if isinstance(last_event_by_market, dict):
            last_event_by_market[market] = int(event.ts_ms)

    def _ingest_live_micro_orderbook_event(
        self,
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
        last_event_by_market = self._runtime_state.setdefault("live_ws_last_event_ts_ms_by_market", {})
        if isinstance(last_event_by_market, dict):
            last_event_by_market[market] = int(event.ts_ms)

    def _drain_live_public_micro_events(
        self,
        *,
        provider: LiveWsMicroSnapshotProvider,
        trade_queue: asyncio.Queue[TradeEvent] | None,
        orderbook_queue: asyncio.Queue[OrderbookEvent] | None,
    ) -> None:
        if trade_queue is not None:
            while True:
                try:
                    trade_event = trade_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                self._ingest_live_micro_trade_event(provider=provider, event=trade_event)
        if orderbook_queue is not None:
            while True:
                try:
                    orderbook_event = orderbook_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                self._ingest_live_micro_orderbook_event(provider=provider, event=orderbook_event)

    def _load_quote_markets(self, quote: str) -> list[str]:
        quote_prefix = f"{quote.strip().upper()}-"
        with UpbitHttpClient(self._upbit_settings) as http_client:
            payload = UpbitPublicClient(http_client).markets(is_details=True)

        if not isinstance(payload, list):
            return []

        markets: list[str] = []
        seen: set[str] = set()
        for item in payload:
            if not isinstance(item, dict):
                continue
            market = str(item.get("market", "")).strip().upper()
            if not market.startswith(quote_prefix):
                continue
            if market in seen:
                continue
            seen.add(market)
            markets.append(market)
        return markets

    def _resolve_micro_snapshot_provider(self, *, markets: list[str]) -> MicroSnapshotProvider | None:
        cfg = self._run_settings.micro_gate
        policy_enabled = bool(self._run_settings.micro_order_policy.enabled)
        gate_enabled = bool(cfg.enabled)
        strategy_mode = str(self._run_settings.strategy).strip().lower() or "candidates_v1"
        execution_snapshot_needed = bool(
            strategy_mode == "model_alpha_v1"
            and bool(self._run_settings.model_alpha.execution.use_learned_recommendations)
        )
        requested_provider = _normalize_paper_micro_provider(self._run_settings.paper_micro_provider)
        decision: dict[str, Any] = {
            "requested_provider": requested_provider,
            "effective_provider": "NONE",
            "policy_enabled": bool(policy_enabled),
            "micro_gate_enabled": bool(gate_enabled),
            "execution_snapshot_needed": bool(execution_snapshot_needed),
            "live_ws_config_enabled": bool(cfg.live_ws.enabled),
            "provider_decision": "UNSET",
        }
        if not (gate_enabled or policy_enabled or execution_snapshot_needed):
            decision["provider_decision"] = "MICRO_DISABLED"
            self._runtime_state["micro_provider_decision"] = decision
            return None

        if self._micro_snapshot_provider is not None:
            decision["provider_decision"] = "INJECTED_PROVIDER"
            decision["effective_provider"] = "INJECTED"
            self._runtime_state["micro_provider_decision"] = decision
            return self._micro_snapshot_provider

        auto_live_ws = False
        if requested_provider == "AUTO":
            auto_live_ws = _is_ws_public_daemon_running(
                health_path=Path(self._run_settings.paper_micro_auto_health_path),
                stale_sec=max(int(self._run_settings.paper_micro_auto_health_stale_sec), 1),
            )
            decision["auto_ws_public_running"] = bool(auto_live_ws)

        live_ws_consumer_enabled = bool(policy_enabled or gate_enabled or execution_snapshot_needed)
        use_live_ws = False
        if requested_provider == "LIVE_WS":
            if live_ws_consumer_enabled:
                use_live_ws = True
                decision["provider_decision"] = "LIVE_WS_FORCED"
            else:
                decision["provider_decision"] = "LIVE_WS_REJECTED_MICRO_DISABLED"
        elif requested_provider == "AUTO":
            if live_ws_consumer_enabled and auto_live_ws:
                use_live_ws = True
                decision["provider_decision"] = "LIVE_WS_AUTO"
            else:
                decision["provider_decision"] = "AUTO_FALLBACK_OFFLINE"
        else:
            decision["provider_decision"] = "OFFLINE_DEFAULT"

        if use_live_ws:
            live_provider = LiveWsMicroSnapshotProvider(settings=cfg.live_ws)
            live_provider.track_markets(markets)
            self._micro_snapshot_provider = live_provider
            decision["effective_provider"] = "LIVE_WS"
            tracked_markets = getattr(live_provider, "_tracked_markets", ())
            decision["subscribed_markets_count"] = (
                len(tuple(tracked_markets)) if isinstance(tracked_markets, tuple) else 0
            )
            self._runtime_state["micro_provider_decision"] = decision
            return self._micro_snapshot_provider

        micro_root = _resolve_micro_root(dataset_name=cfg.dataset_name)
        decision["offline_micro_root"] = str(micro_root)
        if not micro_root.exists():
            decision["provider_decision"] = f"{decision['provider_decision']}_MISSING_MICRO_ROOT"
            decision["effective_provider"] = "NONE"
            self._runtime_state["micro_provider_decision"] = decision
            return None
        provider = OfflineMicroSnapshotProvider(
            micro_root=micro_root,
            tf=(cfg.tf or "5m"),
            cache_entries=cfg.cache_entries,
        )
        self._micro_snapshot_provider = provider
        decision["effective_provider"] = "OFFLINE_PARQUET"
        self._runtime_state["micro_provider_decision"] = decision
        return self._micro_snapshot_provider


def _describe_micro_snapshot_provider(
    *,
    provider: MicroSnapshotProvider | None,
    micro_gate: MicroGateSettings,
) -> dict[str, Any]:
    if provider is None:
        return {
            "provider": "NONE",
            "class_name": None,
            "live_ws_enabled": bool(micro_gate.live_ws.enabled),
        }

    if isinstance(provider, LiveWsMicroSnapshotProvider):
        tracked_markets = getattr(provider, "_tracked_markets", ())
        subscribed_markets_count = len(tuple(tracked_markets)) if isinstance(tracked_markets, tuple) else 0
        return {
            "provider": "LIVE_WS",
            "class_name": provider.__class__.__name__,
            "live_ws_enabled": True,
            "subscribed_markets_count": int(subscribed_markets_count),
            "window_sec": int(getattr(provider.settings, "window_sec", 0)),
            "orderbook_topk": int(getattr(provider.settings, "orderbook_topk", 0)),
        }

    if isinstance(provider, OfflineMicroSnapshotProvider):
        micro_root = getattr(provider, "_micro_root", None)
        tf = getattr(provider, "_tf", None)
        return {
            "provider": "OFFLINE_PARQUET",
            "class_name": provider.__class__.__name__,
            "live_ws_enabled": bool(micro_gate.live_ws.enabled),
            "micro_root": str(micro_root) if micro_root is not None else None,
            "tf": str(tf) if tf is not None else None,
        }

    return {
        "provider": "CUSTOM",
        "class_name": provider.__class__.__name__,
        "live_ws_enabled": bool(micro_gate.live_ws.enabled),
    }


def _intent_to_candidate(intent: StrategyOrderIntent) -> Candidate:
    side = str(intent.side).strip().lower()
    if side not in {"bid", "ask"}:
        raise ValueError(f"invalid strategy intent side: {intent.side}")
    meta = dict(intent.meta) if isinstance(intent.meta, dict) else {}
    if intent.volume is not None:
        meta["force_volume"] = float(intent.volume)
    if intent.reason_code:
        meta["reason_code"] = str(intent.reason_code)
    if intent.prob is not None:
        meta["model_prob"] = float(intent.prob)
    score = float(intent.score if intent.score is not None else (intent.prob if intent.prob is not None else 0.0))
    return Candidate(
        market=str(intent.market).strip().upper(),
        score=score,
        proposed_side=side,
        ref_price=float(intent.ref_price),
        meta=meta,
    )


def _base_currency(market: str) -> str:
    value = str(market).strip().upper()
    if "-" not in value:
        return ""
    return value.split("-", 1)[1].strip().upper()


def _interval_ms_from_tf(tf: str) -> int:
    value = str(tf).strip().lower()
    if value.endswith("m"):
        try:
            return max(int(value[:-1]), 1) * 60_000
        except ValueError:
            return 60_000
    if value.endswith("h"):
        try:
            return max(int(value[:-1]), 1) * 3_600_000
        except ValueError:
            return 60_000
    return 60_000


def _is_micro_reason(reason: str) -> bool:
    value = str(reason).strip().upper()
    if value.startswith("MICRO_"):
        return True
    return value in {
        "LOW_LIQUIDITY_TRADE",
        "STALE_MICRO",
        "WIDE_SPREAD",
        "THIN_BOOK",
        "BOOK_MISSING",
    }


def _is_micro_decision_block(decision: Any) -> bool:
    if str(getattr(decision, "severity", "")).strip().upper() != "BLOCK":
        return False
    reasons = getattr(decision, "gate_reasons", ()) or ()
    return any(_is_micro_reason(reason) for reason in reasons)


def _note_micro_block(*, reasons: tuple[str, ...], reason_counts: dict[str, Any]) -> None:
    for raw_reason in reasons:
        reason = str(raw_reason).strip().upper()
        if not _is_micro_reason(reason):
            continue
        reason_counts[reason] = int(reason_counts.get(reason, 0)) + 1


def _counter_inc(counter: dict[str, int], key: str, delta: int = 1) -> None:
    counter[str(key)] = int(counter.get(str(key), 0)) + int(delta)


def _reason_inc(reason_counts: dict[str, int], reason: str, delta: int = 1) -> None:
    _note_reason_count(reason_code=reason, reason_counts=reason_counts, delta=delta)


def _note_reason_count(*, reason_code: str, reason_counts: dict[str, Any], delta: int = 1) -> None:
    reason = str(reason_code).strip().upper()
    if not reason:
        return
    reason_counts[reason] = int(reason_counts.get(reason, 0)) + int(delta)


def _record_micro_policy_diagnostics(
    *,
    counters: dict[str, Any],
    runtime_state: dict[str, Any],
    diagnostics: dict[str, Any],
    count_cross_used: bool,
    include_tick_sample: bool = True,
    include_cross_gate: bool = True,
) -> None:
    if not isinstance(diagnostics, dict):
        return

    if include_tick_sample:
        tick_raw = diagnostics.get("tick_bps")
        try:
            tick_value = float(tick_raw) if tick_raw is not None else None
        except (TypeError, ValueError):
            tick_value = None
        if tick_value is not None:
            samples = runtime_state.setdefault("policy_tick_bps_values", [])
            if isinstance(samples, list):
                samples.append(float(tick_value))

    if bool(diagnostics.get("resolver_failed_fallback_used", False)):
        counters["micro_policy_resolver_failed_fallback_used"] = int(
            counters.get("micro_policy_resolver_failed_fallback_used", 0)
        ) + 1

    if include_cross_gate:
        cross_candidate = bool(diagnostics.get("cross_candidate", False))
        cross_allowed = bool(diagnostics.get("cross_allowed", False))
        if cross_candidate and cross_allowed:
            counters["micro_policy_cross_allowed_count"] = int(counters.get("micro_policy_cross_allowed_count", 0)) + 1

        block_reason = str(diagnostics.get("cross_block_reason", "")).strip().upper()
        if cross_candidate and block_reason:
            _note_reason_count(
                reason_code=block_reason,
                reason_counts=counters.setdefault("micro_policy_cross_block_reasons", {}),
            )

    if count_cross_used:
        counters["micro_policy_cross_used_count"] = int(counters.get("micro_policy_cross_used_count", 0)) + 1


def _write_trade_artifacts(*, run_root: Path, fill_records: Any) -> None:
    trades_path = run_root / "trades.csv"
    per_market_path = run_root / "per_market.csv"
    slippage_by_market_path = run_root / "slippage_by_market.csv"
    price_mode_by_market_path = run_root / "price_mode_by_market.csv"
    rows = [item for item in fill_records if isinstance(item, dict)] if isinstance(fill_records, list) else []
    rows.sort(key=lambda item: (int(item.get("ts_ms", 0)), str(item.get("market", "")), str(item.get("side", ""))))

    trade_fields = [
        "ts_ms",
        "market",
        "side",
        "ref_price",
        "tick_bps",
        "order_price",
        "fill_price",
        "slippage_bps",
        "price_mode",
        "price",
        "volume",
        "notional_quote",
        "fee_quote",
        "order_id",
        "intent_id",
        "reason_code",
    ]
    trades_path.parent.mkdir(parents=True, exist_ok=True)
    with trades_path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=trade_fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in trade_fields})

    aggregates: dict[str, dict[str, float]] = {}
    for row in rows:
        market = str(row.get("market", "")).strip().upper()
        if not market:
            continue
        item = aggregates.setdefault(
            market,
            {
                "market": market,
                "fills_total": 0.0,
                "entry_fills": 0.0,
                "exit_fills": 0.0,
                "notional_bid": 0.0,
                "notional_ask": 0.0,
                "fees_quote": 0.0,
                "net_flow_quote": 0.0,
            },
        )
        side = str(row.get("side", "")).strip().lower()
        notional = float(row.get("notional_quote", 0.0) or 0.0)
        fees = float(row.get("fee_quote", 0.0) or 0.0)
        item["fills_total"] += 1.0
        item["fees_quote"] += fees
        if side == "bid":
            item["entry_fills"] += 1.0
            item["notional_bid"] += notional
            item["net_flow_quote"] -= notional + fees
        elif side == "ask":
            item["exit_fills"] += 1.0
            item["notional_ask"] += notional
            item["net_flow_quote"] += notional - fees

    per_market_fields = [
        "market",
        "fills_total",
        "entry_fills",
        "exit_fills",
        "notional_bid",
        "notional_ask",
        "fees_quote",
        "net_flow_quote",
    ]
    with per_market_path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=per_market_fields)
        writer.writeheader()
        for market in sorted(aggregates):
            item = aggregates[market]
            writer.writerow(
                {
                    "market": market,
                    "fills_total": int(item["fills_total"]),
                    "entry_fills": int(item["entry_fills"]),
                    "exit_fills": int(item["exit_fills"]),
                    "notional_bid": float(item["notional_bid"]),
                    "notional_ask": float(item["notional_ask"]),
                    "fees_quote": float(item["fees_quote"]),
                    "net_flow_quote": float(item["net_flow_quote"]),
                }
            )

    mode_aggregates: dict[str, dict[str, Any]] = {}
    for row in rows:
        market = str(row.get("market", "")).strip().upper()
        if not market:
            continue
        item = mode_aggregates.setdefault(
            market,
            {
                "fills": 0,
                "cross": 0,
                "join": 0,
                "passive": 0,
                "other": 0,
                "slippages": [],
            },
        )
        item["fills"] = int(item["fills"]) + 1
        mode = str(row.get("price_mode", "")).strip().upper()
        if mode == PRICE_MODE_CROSS_1T:
            item["cross"] = int(item["cross"]) + 1
        elif mode == PRICE_MODE_JOIN:
            item["join"] = int(item["join"]) + 1
        elif mode.startswith("PASSIVE"):
            item["passive"] = int(item["passive"]) + 1
        else:
            item["other"] = int(item["other"]) + 1
        slip_raw = row.get("slippage_bps")
        try:
            slip_value = float(slip_raw) if slip_raw is not None else None
        except (TypeError, ValueError):
            slip_value = None
        if slip_value is not None and isinstance(item.get("slippages"), list):
            item["slippages"].append(slip_value)

    slippage_by_market_fields = [
        "market",
        "fills",
        "mean_bps",
        "p50_bps",
        "p90_bps",
        "max_bps",
        "cross_ratio",
    ]
    with slippage_by_market_path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=slippage_by_market_fields)
        writer.writeheader()
        for market in sorted(mode_aggregates):
            item = mode_aggregates[market]
            fills = int(item["fills"])
            slippages = [float(value) for value in item.get("slippages", []) if isinstance(value, (int, float))]
            writer.writerow(
                {
                    "market": market,
                    "fills": fills,
                    "mean_bps": mean(slippages),
                    "p50_bps": percentile(slippages, 0.50),
                    "p90_bps": percentile(slippages, 0.90),
                    "max_bps": max(slippages) if slippages else 0.0,
                    "cross_ratio": (int(item["cross"]) / fills) if fills > 0 else 0.0,
                }
            )

    price_mode_by_market_fields = [
        "market",
        "JOIN_count",
        "CROSS_1T_count",
        "PASSIVE_count",
        "OTHER_count",
        "total_fills",
    ]
    with price_mode_by_market_path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=price_mode_by_market_fields)
        writer.writeheader()
        for market in sorted(mode_aggregates):
            item = mode_aggregates[market]
            writer.writerow(
                {
                    "market": market,
                    "JOIN_count": int(item["join"]),
                    "CROSS_1T_count": int(item["cross"]),
                    "PASSIVE_count": int(item["passive"]),
                    "OTHER_count": int(item["other"]),
                    "total_fills": int(item["fills"]),
                }
            )


def _model_alpha_paper_exec_profile(
    settings: PaperRunSettings,
    *,
    model_alpha_settings: ModelAlphaSettings | None = None,
) -> OrderExecProfile:
    interval_ms = _interval_ms_from_tf(settings.tf)
    effective_settings = model_alpha_settings or settings.model_alpha
    timeout_ms = max(int(effective_settings.execution.timeout_bars), 1) * interval_ms
    return make_legacy_exec_profile(
        timeout_ms=timeout_ms,
        replace_interval_ms=timeout_ms,
        max_replaces=max(int(effective_settings.execution.replace_max), 0),
        price_mode=str(effective_settings.execution.price_mode),
        max_chase_bps=10_000,
        min_replace_interval_ms_global=1_500,
    )


def _entry_notional_quote_for_strategy(
    *,
    strategy_mode: str,
    per_trade_krw: float,
    min_total_krw: float,
    model_alpha_settings: ModelAlphaSettings,
    candidate_meta: dict[str, Any] | None = None,
) -> float:
    target_notional = max(float(per_trade_krw), 1.0)
    if str(strategy_mode).strip().lower() != "model_alpha_v1":
        return target_notional
    target_notional *= _resolve_candidate_notional_multiplier(candidate_meta)
    buffer_bps = max(float(model_alpha_settings.position.entry_min_notional_buffer_bps), 0.0)
    min_total_with_buffer = max(float(min_total_krw), 0.0) * (1.0 + (buffer_bps / 10_000.0))
    return max(target_notional, min_total_with_buffer)


def _resolve_candidate_notional_multiplier(candidate_meta: dict[str, Any] | None) -> float:
    if not isinstance(candidate_meta, dict):
        return 1.0
    value = _safe_optional_float(candidate_meta.get("notional_multiplier"))
    if value is None or value <= 0:
        return 1.0
    return float(value)


def _strategy_paper_exec_profile(
    settings: PaperRunSettings,
    *,
    model_alpha_settings: ModelAlphaSettings | None = None,
) -> OrderExecProfile:
    strategy_mode = str(settings.strategy).strip().lower() or "candidates_v1"
    if strategy_mode == "model_alpha_v1":
        return _model_alpha_paper_exec_profile(settings, model_alpha_settings=model_alpha_settings)
    return _legacy_paper_exec_profile(settings)


def _legacy_paper_exec_profile(settings: PaperRunSettings) -> OrderExecProfile:
    timeout_ms = max(int(float(settings.order_timeout_sec) * 1000), 1_000)
    return make_legacy_exec_profile(
        timeout_ms=timeout_ms,
        replace_interval_ms=timeout_ms,
        max_replaces=max(int(settings.reprice_max_attempts), 0),
        price_mode=PRICE_MODE_JOIN,
        max_chase_bps=10_000,
        min_replace_interval_ms_global=1_500,
    )


def _normalize_reason_counts(value: Any) -> dict[str, int]:
    if not isinstance(value, dict):
        return {}
    normalized: dict[str, int] = {}
    for key, count in value.items():
        reason = str(key).strip().upper()
        if not reason:
            continue
        normalized[reason] = int(count or 0)
    return normalized


def _resolve_micro_root(*, dataset_name: str) -> Path:
    raw = str(dataset_name).strip() or "micro_v1"
    candidate = Path(raw)
    if candidate.exists() or candidate.is_absolute():
        return candidate
    return Path("data/parquet") / raw


def _normalize_paper_micro_provider(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text in {"LIVE_WS", "LIVE", "WS"}:
        return "LIVE_WS"
    if text in {"AUTO"}:
        return "AUTO"
    if text in {"OFFLINE", "OFFLINE_PARQUET", "PARQUET"}:
        return "OFFLINE_PARQUET"
    return "OFFLINE_PARQUET"


def _normalize_paper_feature_provider(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text in {"LIVE_V4_NATIVE", "V4_NATIVE", "NATIVE_V4"}:
        return "LIVE_V4_NATIVE"
    if text in {"LIVE_V4", "V4"}:
        return "LIVE_V4"
    if text in {"LIVE_V3", "LIVE", "V3"}:
        return "LIVE_V3"
    if text in {"OFFLINE", "OFFLINE_PARQUET", "PARQUET"}:
        return "OFFLINE_PARQUET"
    return "OFFLINE_PARQUET"


def _is_live_feature_provider(provider: Any) -> bool:
    return isinstance(provider, (LiveFeatureProviderV3, LiveFeatureProviderV4, LiveFeatureProviderV4Native))


def _is_ws_public_daemon_running(*, health_path: Path, stale_sec: int) -> bool:
    if not health_path.exists():
        return False
    try:
        payload = json.loads(health_path.read_text(encoding="utf-8"))
    except Exception:
        return False
    if not isinstance(payload, dict):
        return False
    connected_raw = payload.get("connected")
    if isinstance(connected_raw, bool):
        connected = connected_raw
    elif isinstance(connected_raw, (int, float)):
        connected = int(connected_raw) != 0
    elif isinstance(connected_raw, str):
        connected = connected_raw.strip().lower() in {"1", "true", "yes", "y", "on"}
    else:
        connected = False
    if not connected:
        return False
    updated_at_ms = _safe_int(payload.get("updated_at_ms"))
    if updated_at_ms is None or updated_at_ms <= 0:
        return True
    age_ms = int(time.time() * 1000) - int(updated_at_ms)
    return age_ms <= max(int(stale_sec), 1) * 1000


def _write_json(*, path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _resolve_paper_runtime_metadata(settings: PaperRunSettings) -> dict[str, Any]:
    unit_name = str(os.getenv("AUTOBOT_PAPER_UNIT_NAME", "")).strip()
    runtime_role = str(os.getenv("AUTOBOT_PAPER_RUNTIME_ROLE", "")).strip().lower() or "unspecified"
    paper_lane = str(os.getenv("AUTOBOT_PAPER_LANE", "")).strip().lower()
    pinned_model_ref = str(os.getenv("AUTOBOT_PAPER_MODEL_REF_PINNED", "")).strip()
    effective_model_ref = str(settings.model_ref or settings.model_alpha.model_ref or "").strip()
    effective_model_family = str(settings.model_family or settings.model_alpha.model_family or "").strip()
    effective_feature_set = str(settings.feature_set or settings.model_alpha.feature_set or "").strip().lower()
    return {
        "paper_unit_name": unit_name,
        "paper_runtime_role": runtime_role,
        "paper_lane": paper_lane,
        "paper_runtime_model_ref": effective_model_ref,
        "paper_runtime_model_family": effective_model_family,
        "paper_runtime_feature_set": effective_feature_set,
        "paper_runtime_model_ref_pinned": pinned_model_ref,
        "paper_runtime_model_run_id": "",
        "run_started_ts_ms": int(time.time() * 1000),
    }


def _find_baseline(points: deque[TickerSnapshot], *, target_ts_ms: int) -> TickerSnapshot | None:
    baseline: TickerSnapshot | None = None
    for snapshot in points:
        if snapshot.ts_ms <= target_ts_ms:
            baseline = snapshot
            continue
        if baseline is None:
            baseline = snapshot
        break
    return baseline


def _extract_min_total(payload: dict[str, Any], *, default: float) -> float:
    market = payload.get("market")
    if not isinstance(market, dict):
        return default

    values: list[float] = []
    for side in ("bid", "ask"):
        side_payload = market.get(side)
        if not isinstance(side_payload, dict):
            continue
        value = _safe_optional_float(side_payload.get("min_total"))
        if value is not None and value > 0:
            values.append(value)
    if not values:
        return default
    return max(values)


def _safe_float(value: Any, *, default: float) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if number < 0:
        return default
    return number


def _safe_optional_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number < 0:
        return None
    return number


def _candidate_expected_edge_bps(meta_payload: dict[str, Any] | None) -> float | None:
    if not isinstance(meta_payload, dict):
        return None
    trade_action = meta_payload.get("trade_action")
    if isinstance(trade_action, dict):
        edge = _safe_optional_float(trade_action.get("expected_edge"))
        if edge is not None:
            return float(edge) * 10_000.0
    return _safe_optional_float(meta_payload.get("expected_edge_bps"))


def _execution_contract_micro_state(*, snapshot: MicroSnapshot | None, now_ts_ms: int) -> dict[str, Any]:
    if snapshot is None:
        return {}
    last_event_ts_ms = getattr(snapshot, "last_event_ts_ms", None)
    snapshot_age_ms = None
    if last_event_ts_ms is not None:
        snapshot_age_ms = max(int(now_ts_ms) - int(last_event_ts_ms), 0)
    return {
        "spread_bps": _safe_optional_float(getattr(snapshot, "spread_bps_mean", None)),
        "depth_top5_notional_krw": _safe_optional_float(getattr(snapshot, "depth_top5_notional_krw", None)),
        "trade_coverage_ms": getattr(snapshot, "trade_coverage_ms", None),
        "book_coverage_ms": getattr(snapshot, "book_coverage_ms", None),
        "snapshot_age_ms": snapshot_age_ms,
        "micro_quality_score": None,
    }


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            return int(float(text))
        return int(value)
    except (TypeError, ValueError):
        return None


def _infer_tick_size(*, price: float, quote: str) -> float:
    quote_value = quote.strip().upper()
    if quote_value != "KRW":
        return 0.00000001

    px = max(float(price), 0.0)
    if px >= 2_000_000:
        return 1000.0
    if px >= 1_000_000:
        return 500.0
    if px >= 500_000:
        return 100.0
    if px >= 100_000:
        return 50.0
    if px >= 10_000:
        return 10.0
    if px >= 1_000:
        return 1.0
    if px >= 100:
        return 0.1
    if px >= 10:
        return 0.01
    if px >= 1:
        return 0.001
    return 0.0001


def _reprice_limit_price(*, price: float, tick_size: float, side: str, ticks: int) -> float:
    step = max(float(tick_size), 1e-12) * max(int(ticks), 1)
    side_value = side.strip().lower()
    if side_value == "bid":
        target = price + step
    elif side_value == "ask":
        target = max(price - step, step)
    else:
        target = price
    return round_price_to_tick(price=target, tick_size=tick_size, side=side_value)


def _max_drawdown_pct(equity_curve: Sequence[float]) -> float:
    max_dd = 0.0
    peak = 0.0
    for equity in equity_curve:
        if equity > peak:
            peak = equity
        if peak <= 0:
            continue
        drawdown = (peak - equity) / peak * 100.0
        if drawdown > max_dd:
            max_dd = drawdown
    return max_dd


async def run_live_paper(*, upbit_settings: UpbitSettings, run_settings: PaperRunSettings) -> PaperRunSummary:
    engine = PaperRunEngine(upbit_settings=upbit_settings, run_settings=run_settings)
    return await engine.run()


def run_live_paper_sync(*, upbit_settings: UpbitSettings, run_settings: PaperRunSettings) -> PaperRunSummary:
    return asyncio.run(run_live_paper(upbit_settings=upbit_settings, run_settings=run_settings))
