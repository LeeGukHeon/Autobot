"""Backtest engine v1 for candle-based simulation."""

from __future__ import annotations

from collections import deque
import csv
from dataclasses import asdict, dataclass, field
import heapq
import json
from pathlib import Path
import time
from typing import Any, Callable, Sequence

from autobot.common.event_store import JsonlEventStore
from autobot.common.execution_structure import summarize_fill_records
from autobot.execution.intent import OrderIntent, new_order_intent
from autobot.execution.order_supervisor import (
    PRICE_MODE_CROSS_1T,
    PRICE_MODE_JOIN,
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
from autobot.paper.sim_exchange import (
    FillEvent,
    MarketRules,
    PaperOrder,
    order_volume_from_notional,
    round_price_to_tick,
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
from autobot.strategy.model_alpha_v1 import (
    ModelAlphaSettings,
    ModelAlphaStrategyV1,
    resolve_model_alpha_runtime_row_columns,
    resolve_runtime_model_alpha_settings,
)
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

from .exchange import BacktestSimExchange
from .loader import CandleDataLoader
from .metrics import max_drawdown_pct, win_rate
from .reporting import write_summary_json
from .run_id import build_backtest_run_id
from .strategy_adapter import StrategyFillEvent, StrategyOrderIntent
from .types import CandleBar
from .universe import StaticUniverseProvider, build_static_start_universe


@dataclass(frozen=True)
class BacktestRunSettings:
    dataset_name: str = "candles_v1"
    parquet_root: str = "data/parquet"
    tf: str = "1m"
    from_ts_ms: int | None = None
    to_ts_ms: int | None = None
    duration_days: int | None = None
    market: str | None = None
    markets: tuple[str, ...] = ()
    universe_mode: str = "static_start"  # static_start | fixed_list
    quote: str = "KRW"
    top_n: int = 20
    dense_grid: bool = False
    starting_krw: float = 50_000.0
    per_trade_krw: float = 10_000.0
    max_positions: int = 2
    min_order_krw: float = 5_000.0
    order_timeout_bars: int = 5
    reprice_max_attempts: int = 1
    reprice_tick_steps: int = 1
    rules_ttl_sec: int = 86_400
    momentum_window_sec: int = 60
    min_momentum_pct: float = 0.2
    strategy: str = "candidates_v1"  # candidates_v1 | model_alpha_v1
    model_ref: str | None = None
    model_family: str | None = None
    feature_set: str = "v3"
    model_registry_root: str = "models/registry"
    model_feature_dataset_root: str | None = None
    model_alpha: ModelAlphaSettings = field(default_factory=ModelAlphaSettings)
    output_root_dir: str = "data/backtest"
    seed: int = 0
    micro_gate: MicroGateSettings = field(default_factory=MicroGateSettings)
    micro_order_policy: MicroOrderPolicySettings = field(default_factory=MicroOrderPolicySettings)
    execution_contract_artifact_path: str = DEFAULT_EXECUTION_CONTRACT_ARTIFACT_PATH


@dataclass(frozen=True)
class BacktestRunSummary:
    run_id: str
    run_dir: str
    tf: str
    from_ts_ms: int
    to_ts_ms: int
    bars_processed: int
    markets: list[str]
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
    strategy: str = "candidates_v1"
    scored_rows: int = 0
    selected_rows: int = 0
    skipped_missing_features_rows: int = 0
    selection_ratio: float = 0.0
    exposure_avg_open_positions: float = 0.0
    exposure_max_open_positions: int = 0


@dataclass(frozen=True)
class _BarTickerSnapshot:
    market: str
    ts_ms: int
    trade_price: float
    acc_trade_price_24h: float


class _BarDataHub:
    def __init__(self, *, history_sec: int = 300) -> None:
        self._latest: dict[str, _BarTickerSnapshot] = {}
        self._history: dict[str, deque[_BarTickerSnapshot]] = {}
        self._max_history_ms = max(int(history_sec), 1) * 1000
        self._acc_trade_value: dict[str, float] = {}

    def update(self, bar: CandleBar) -> None:
        market = bar.market.strip().upper()
        quote_volume = float(bar.volume_quote) if bar.volume_quote is not None else float(bar.close) * float(bar.volume_base)
        cumulative = self._acc_trade_value.get(market, 0.0) + max(quote_volume, 0.0)
        self._acc_trade_value[market] = cumulative

        snapshot = _BarTickerSnapshot(
            market=market,
            ts_ms=int(bar.ts_ms),
            trade_price=float(bar.close),
            acc_trade_price_24h=float(cumulative),
        )
        self._latest[market] = snapshot
        points = self._history.get(market)
        if points is None:
            points = deque()
            self._history[market] = points
        points.append(snapshot)

        cutoff = snapshot.ts_ms - self._max_history_ms
        while points and points[0].ts_ms < cutoff:
            points.popleft()

    def get_latest_ticker(self, market: str) -> _BarTickerSnapshot | None:
        return self._latest.get(market.strip().upper())

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
        return {market: item.trade_price for market, item in self._latest.items()}


class BacktestRulesProvider:
    def __init__(
        self,
        *,
        settings: UpbitSettings | None,
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
        if cached is not None and int(ts_ms) < cached[0]:
            return cached[1]

        quote = market_value.split("-", 1)[0] if "-" in market_value else "KRW"
        rules = MarketRules(
            bid_fee=0.0005,
            ask_fee=0.0005,
            maker_bid_fee=None,
            maker_ask_fee=None,
            min_total=5000.0,
            tick_size=_infer_tick_size(price=max(float(reference_price), 1e-8), quote=quote),
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
        if tick_size is not None and tick_size > 0:
            rules = MarketRules(
                bid_fee=rules.bid_fee,
                ask_fee=rules.ask_fee,
                maker_bid_fee=rules.maker_bid_fee,
                maker_ask_fee=rules.maker_ask_fee,
                min_total=rules.min_total,
                tick_size=tick_size,
            )

        self._cache[market_value] = (int(ts_ms) + self._ttl_ms, rules)
        return rules

    def _fetch_chance_payload(self, market: str) -> dict[str, Any] | None:
        if self._settings is None or self._credentials is None:
            return None
        if UpbitHttpClient is None or UpbitPrivateClient is None:
            return None
        try:
            with UpbitHttpClient(self._settings, credentials=self._credentials) as http_client:
                payload = UpbitPrivateClient(http_client).chance(market=market)
        except UpbitError:
            return None
        return payload if isinstance(payload, dict) else None

    def _fetch_tick_size(self, market: str) -> float | None:
        if self._settings is None:
            return None
        if UpbitHttpClient is None or UpbitPublicClient is None:
            return None
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
            tick_size = _safe_optional_float(item.get("tick_size"))
            if tick_size is not None and tick_size > 0:
                return tick_size
        return None


@dataclass
class ExecutionUpdate:
    orders_submitted: list[PaperOrder] = field(default_factory=list)
    orders_canceled: list[PaperOrder] = field(default_factory=list)
    orders_filled: list[PaperOrder] = field(default_factory=list)
    fills: list[FillEvent] = field(default_factory=list)
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


class BacktestExecutionGateway:
    def __init__(
        self,
        *,
        exchange: BacktestSimExchange,
        order_timeout_bars: int,
        reprice_max_attempts: int,
        reprice_tick_steps: int,
        default_profile: OrderExecProfile | None = None,
        max_replaces_per_min_per_market: int = 10,
        micro_order_policy: MicroOrderPolicyV1 | None = None,
        micro_snapshot_provider: MicroSnapshotProvider | None = None,
    ) -> None:
        self._exchange = exchange
        self._legacy_default_profile = make_legacy_exec_profile(
            timeout_ms=max(int(order_timeout_bars), 1) * 60_000,
            replace_interval_ms=max(int(order_timeout_bars), 1) * 60_000,
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

    def submit_intent(
        self,
        *,
        intent: OrderIntent,
        rules: MarketRules,
        bar_index: int,
        ts_ms: int,
    ) -> ExecutionUpdate:
        update = ExecutionUpdate()
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
        order, fill = self._exchange.submit_limit_order_deferred(
            intent=intent,
            rules=rules,
            ts_ms=ts_ms,
            activate_on_index=bar_index + 1,
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
                update.orders_filled.append(latest)
            update.success_markets.append(intent.market)
            self._clear_pending(intent.intent_id)
        return update

    def on_bar(self, *, bar: CandleBar, bar_index: int, rules: MarketRules) -> ExecutionUpdate:
        update = ExecutionUpdate()
        market_value = bar.market.strip().upper()
        self._trim_replace_window(market=market_value, now_ts_ms=bar.ts_ms)
        market_window = self._replace_window_by_market.setdefault(market_value, deque())

        fills = self._exchange.match_orders_on_bar(bar=bar, bar_index=bar_index, rules=rules)
        for fill in fills:
            update.fills.append(fill)
            current = self._exchange.get_order(fill.order_id)
            if current is not None:
                update.orders_filled.append(current)
            intent_id = self._intent_by_order_id.pop(fill.order_id, None)
            if intent_id is None:
                continue
            pending = self._pending_by_intent.pop(intent_id, None)
            if pending is not None:
                update.success_markets.append(pending.market)

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
                    self._micro_snapshot_provider.get(market_value, int(bar.ts_ms))
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
                    ref_price=bar.close,
                    tick_size=rules.tick_size,
                    replace_attempt=pending.replace_count + 1,
                    model_prob=model_prob,
                    micro_snapshot=snapshot,
                    now_ts_ms=bar.ts_ms,
                )
                effective_profile = guard.profile
                policy_diagnostics = dict(guard.diagnostics or {})
                policy_abort_reason = guard.abort_reason
            action = evaluate_supervisor_action(
                profile=effective_profile,
                side=current_order.side,
                now_ts_ms=bar.ts_ms,
                created_ts_ms=pending.created_ts_ms,
                last_action_ts_ms=pending.last_action_ts_ms,
                last_replace_ts_ms=pending.last_replace_ts_ms,
                replace_count=pending.replace_count,
                remaining_volume=remaining_volume,
                ref_price=bar.close,
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
            canceled = self._exchange.cancel_order(current_order.order_id, ts_ms=bar.ts_ms, reason=reason_code)
            if canceled is None:
                update.failed_markets.append(pending.market)
                self._clear_pending(intent_id)
                continue

            update.orders_canceled.append(canceled)
            update.supervisor_events.append(
                {
                    "event_type": "ORDER_TIMEOUT",
                    "ts_ms": int(bar.ts_ms),
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
                    "ts_ms": int(bar.ts_ms),
                    "payload": {
                        "market": canceled.market,
                        "side": canceled.side,
                        "order_id": canceled.order_id,
                        "state": canceled.state,
                        "reason_code": reason_code,
                    },
                }
            )
            self._intent_by_order_id.pop(current_order.order_id, None)
            _reason_inc(update.reason_counts, reason_code, 1)

            if action.action == SUPERVISOR_ACTION_ABORT:
                _counter_inc(update.counter_deltas, "aborted_timeout_total", 1)
                if reason_code == REASON_MIN_NOTIONAL_DUST_ABORT:
                    _counter_inc(update.counter_deltas, "dust_abort_total", 1)
                update.failed_markets.append(pending.market)
                self._clear_pending(intent_id)
                continue

            new_price = float(action.target_price if action.target_price is not None else current_order.price)
            reprice_intent = OrderIntent(
                intent_id=pending.intent.intent_id,
                ts_ms=bar.ts_ms,
                market=pending.intent.market,
                side=pending.intent.side,
                ord_type="limit",
                price=float(new_price),
                volume=float(remaining_volume),
                time_in_force=pending.intent.time_in_force,
                reason_code=pending.intent.reason_code,
                meta={
                    **pending.intent.meta,
                    "reprice_attempt": pending.replace_count + 1,
                    "exec_profile": order_exec_profile_to_dict(effective_profile),
                    "micro_diagnostics": dict(policy_diagnostics),
                },
            )
            new_order, new_fill = self._exchange.submit_limit_order_deferred(
                intent=reprice_intent,
                rules=rules,
                ts_ms=bar.ts_ms,
                activate_on_index=bar_index + 1,
                reprice_attempt=pending.replace_count + 1,
            )
            update.orders_submitted.append(new_order)
            _counter_inc(update.counter_deltas, "replaces_total", 1)
            update.supervisor_events.append(
                {
                    "event_type": "ORDER_REPLACED",
                    "ts_ms": int(bar.ts_ms),
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
            market_window.append(int(bar.ts_ms))

            effective_profile_payload = order_exec_profile_to_dict(effective_profile)
            if new_order.state in {"OPEN", "PARTIAL"}:
                self._activate_pending(
                    intent=reprice_intent,
                    order_id=new_order.order_id,
                    replace_count=pending.replace_count + 1,
                    ts_ms=bar.ts_ms,
                    created_ts_ms=pending.created_ts_ms,
                    profile=effective_profile,
                )
            elif new_order.state == "FAILED":
                update.failed_markets.append(pending.market)
                self._clear_pending(intent_id)
            else:
                self._clear_pending(intent_id)

            update.order_exec_profiles[new_order.order_id] = effective_profile_payload
            if policy_diagnostics:
                update.order_policy_diagnostics[new_order.order_id] = dict(policy_diagnostics)

            if new_fill is not None:
                update.fills.append(new_fill)
                latest = self._exchange.get_order(new_order.order_id)
                if latest is not None:
                    update.orders_filled.append(latest)
                update.success_markets.append(pending.market)
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


class BacktestRunEngine:
    def __init__(
        self,
        *,
        run_settings: BacktestRunSettings,
        upbit_settings: UpbitSettings | None = None,
        loader: CandleDataLoader | None = None,
        rules_provider: BacktestRulesProvider | None = None,
        micro_snapshot_provider: MicroSnapshotProvider | None = None,
    ) -> None:
        self._run_settings = run_settings
        self._loader = loader or CandleDataLoader(
            parquet_root=run_settings.parquet_root,
            dataset_name=run_settings.dataset_name,
            tf=run_settings.tf,
            dense_grid=run_settings.dense_grid,
        )
        self._micro_snapshot_provider = micro_snapshot_provider
        credentials = load_upbit_credentials(upbit_settings) if upbit_settings is not None else None
        self._rules_provider = rules_provider or BacktestRulesProvider(
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

    def run(self) -> BacktestRunSummary:
        settings = self._run_settings
        markets = self._resolve_markets()
        if not markets:
            raise RuntimeError("no markets selected for backtest")

        from_ts_ms, to_ts_ms = self._resolve_time_window(markets=markets)
        market_bars = self._load_market_bars(markets=markets, from_ts_ms=from_ts_ms, to_ts_ms=to_ts_ms)
        if not market_bars:
            raise RuntimeError("no candle bars found in the selected range")

        active_markets = sorted(market_bars.keys())
        run_id = build_backtest_run_id(
            tf=settings.tf,
            markets=active_markets,
            from_ts_ms=from_ts_ms,
            to_ts_ms=to_ts_ms,
            seed=settings.seed,
        )
        run_root = Path(settings.output_root_dir) / "runs" / run_id
        universe = StaticUniverseProvider(active_markets)
        market_data = _BarDataHub(history_sec=max(settings.momentum_window_sec * 2, 300))
        strategy_mode = str(settings.strategy).strip().lower() or "candidates_v1"
        candidate_generator = CandidateGeneratorV1(
            CandidateSettings(
                momentum_window_sec=max(settings.momentum_window_sec, 1),
                min_momentum_pct=float(settings.min_momentum_pct),
            )
        )
        micro_snapshot_provider = self._resolve_micro_snapshot_provider()
        micro_gate = (
            MicroGateV1(settings.micro_gate)
            if settings.micro_gate.enabled and micro_snapshot_provider is not None
            else None
        )
        micro_order_policy = (
            MicroOrderPolicyV1(settings.micro_order_policy) if settings.micro_order_policy.enabled else None
        )
        trade_gate = TradeGateV1(
            GateSettings(
                per_trade_krw=float(settings.per_trade_krw),
                max_positions=(
                    max(int(settings.model_alpha.position.max_positions_total), 1)
                    if strategy_mode == "model_alpha_v1"
                    else max(int(settings.max_positions), 1)
                ),
                min_order_krw=max(float(settings.min_order_krw), 0.0),
                max_consecutive_failures=3,
                cooldown_sec_after_fail=60,
            ),
            micro_gate=micro_gate,
            micro_snapshot_provider=micro_snapshot_provider,
        )
        exchange = BacktestSimExchange(
            quote_currency=settings.quote.strip().upper(),
            starting_cash_quote=max(float(settings.starting_krw), 0.0),
        )
        execution = BacktestExecutionGateway(
            exchange=exchange,
            order_timeout_bars=settings.order_timeout_bars,
            reprice_max_attempts=settings.reprice_max_attempts,
            reprice_tick_steps=settings.reprice_tick_steps,
            default_profile=_strategy_backtest_exec_profile(settings),
            max_replaces_per_min_per_market=(
                settings.micro_order_policy.safety.max_replaces_per_min_per_market
                if settings.micro_order_policy.enabled
                else 10
            ),
            micro_order_policy=micro_order_policy,
            micro_snapshot_provider=micro_snapshot_provider,
        )

        model_strategy = (
            self._build_model_alpha_strategy(
                active_markets=active_markets,
                from_ts_ms=from_ts_ms,
                to_ts_ms=to_ts_ms,
            )
            if strategy_mode == "model_alpha_v1"
            else None
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
            "filled_intents": set(),
            "time_to_fill_ms": [],
            "slippage_bps": [],
            "policy_tick_bps_values": [],
            "fill_records": [],
            "order_exec_profile_by_order_id": {},
            "order_policy_diag_by_order_id": {},
            "selection_per_ts": [],
            "strategy_adapter": model_strategy,
            "execution_contract": load_live_execution_contract_artifact(
                project_root=Path.cwd(),
                artifact_path=settings.execution_contract_artifact_path,
            ),
        }
        bars_processed = 0
        equity_curve: list[float] = []
        open_positions_curve: list[float] = []
        realized_trade_pnls: list[float] = []
        latest_bar_index_by_market: dict[str, int] = {}
        max_event_ts_ms = from_ts_ms

        queue: list[tuple[int, int, str, int]] = []
        queue_seq = 0
        for market in active_markets:
            bars = market_bars[market]
            if not bars:
                continue
            heapq.heappush(queue, (bars[0].ts_ms, queue_seq, market, 0))
            queue_seq += 1

        with JsonlEventStore(run_root) as store:
            event_count = 0

            def append_event(event_type: str, ts_ms: int, payload: dict[str, Any] | None = None) -> None:
                nonlocal event_count
                store.append_event(event_type=event_type, ts_ms=ts_ms, payload=payload)
                event_count += 1

            append_event(
                "RUN_STARTED",
                ts_ms=int(time.time() * 1000),
                payload={
                    "run_id": run_id,
                    "tf": settings.tf,
                    "from_ts_ms": from_ts_ms,
                    "to_ts_ms": to_ts_ms,
                    "markets": active_markets,
                    "dense_grid": bool(settings.dense_grid),
                    "strategy": strategy_mode,
                    "model_ref": str(settings.model_ref or settings.model_alpha.model_ref or ""),
                    "micro_order_policy_enabled": bool(settings.micro_order_policy.enabled),
                    "micro_order_policy_mode": str(settings.micro_order_policy.mode),
                    "micro_order_policy_on_missing": str(settings.micro_order_policy.on_missing),
                },
            )

            while queue:
                ts_batch = int(queue[0][0])
                batch_items: list[tuple[str, int]] = []
                while queue and int(queue[0][0]) == ts_batch:
                    _, _, market, bar_index = heapq.heappop(queue)
                    batch_items.append((market, bar_index))

                for market, bar_index in batch_items:
                    bar = market_bars[market][bar_index]
                    bars_processed += 1
                    max_event_ts_ms = max(max_event_ts_ms, bar.ts_ms)
                    latest_bar_index_by_market[market] = bar_index

                    rules = self._rules_provider.get_rules(
                        market=bar.market,
                        reference_price=bar.close,
                        ts_ms=bar.ts_ms,
                    )
                    realized_before = exchange.total_realized_pnl()
                    has_ask_fill = self._apply_execution_update(
                        update=execution.on_bar(bar=bar, bar_index=bar_index, rules=rules),
                        trade_gate=trade_gate,
                        event_store=store,
                        append_event=append_event,
                        ts_ms=bar.ts_ms,
                        counters=self._runtime_counters,
                    )
                    if has_ask_fill:
                        realized_after = exchange.total_realized_pnl()
                        realized_delta = realized_after - realized_before
                        if abs(realized_delta) > 1e-12:
                            realized_trade_pnls.append(realized_delta)

                    market_data.update(bar)
                    universe.update_if_needed(bar.ts_ms)

                    if strategy_mode != "model_alpha_v1":
                        self._run_candidate_cycle(
                            ts_ms=bar.ts_ms,
                            current_market=market,
                            latest_bar_index_by_market=latest_bar_index_by_market,
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

                    snapshot = exchange.portfolio_snapshot(ts_ms=bar.ts_ms, latest_prices=market_data.latest_prices())
                    store.append_equity(snapshot)
                    append_event("PORTFOLIO_SNAPSHOT", ts_ms=bar.ts_ms, payload=asdict(snapshot))
                    equity_curve.append(snapshot.equity_quote)
                    open_positions_curve.append(float(len(snapshot.positions)))

                    next_index = bar_index + 1
                    if next_index < len(market_bars[market]):
                        heapq.heappush(queue, (market_bars[market][next_index].ts_ms, queue_seq, market, next_index))
                        queue_seq += 1

                if strategy_mode == "model_alpha_v1" and model_strategy is not None:
                    self._run_model_alpha_cycle(
                        ts_ms=ts_batch,
                        latest_bar_index_by_market=latest_bar_index_by_market,
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

            final_snapshot = exchange.portfolio_snapshot(ts_ms=max_event_ts_ms, latest_prices=market_data.latest_prices())
            store.append_equity(final_snapshot)
            append_event("PORTFOLIO_SNAPSHOT", ts_ms=max_event_ts_ms, payload=asdict(final_snapshot))
            open_positions_curve.append(float(len(final_snapshot.positions)))
            append_event(
                "RUN_COMPLETED",
                ts_ms=max_event_ts_ms,
                payload={
                    "bars_processed": bars_processed,
                    "orders_submitted": int(self._runtime_counters["orders_submitted"]),
                    "orders_filled": int(self._runtime_counters["orders_filled"]),
                    "orders_canceled": int(self._runtime_counters["orders_canceled"]),
                    "intents_failed": int(self._runtime_counters["intents_failed"]),
                    "candidates_total": int(self._runtime_counters["candidates_total"]),
                    "candidates_blocked_by_micro": int(self._runtime_counters["candidates_blocked_by_micro"]),
                    "candidates_aborted_by_policy": int(self._runtime_counters["candidates_aborted_by_policy"]),
                    "micro_blocked_reasons": dict(self._runtime_counters.get("micro_blocked_reasons", {})),
                    "micro_policy_tier_counts": dict(self._runtime_counters.get("micro_policy_tier_counts", {})),
                    "micro_policy_fallback_counts": dict(
                        self._runtime_counters.get("micro_policy_fallback_counts", {})
                    ),
                    "order_supervisor_reasons": dict(self._runtime_counters.get("order_supervisor_reasons", {})),
                    "scored_rows": int(self._runtime_counters.get("scored_rows", 0)),
                    "selected_rows": int(self._runtime_counters.get("selected_rows", 0)),
                    "skipped_missing_features_rows": int(
                        self._runtime_counters.get("skipped_missing_features_rows", 0)
                    ),
                    "debug_mismatch_reasons": dict(self._runtime_counters.get("debug_mismatch_reasons", {})),
                    "exposure_avg_open_positions": mean(open_positions_curve),
                    "exposure_max_open_positions": int(max(open_positions_curve) if open_positions_curve else 0),
                },
            )

        orders_submitted = int(self._runtime_counters["orders_submitted"])
        orders_filled = int(self._runtime_counters["orders_filled"])
        orders_canceled = int(self._runtime_counters["orders_canceled"])
        intents_failed = int(self._runtime_counters["intents_failed"])
        candidates_total = int(self._runtime_counters["candidates_total"])
        candidates_blocked_by_micro = int(self._runtime_counters["candidates_blocked_by_micro"])
        candidates_aborted_by_policy = int(self._runtime_counters.get("candidates_aborted_by_policy", 0))
        micro_blocked_reasons = _normalize_reason_counts(self._runtime_counters.get("micro_blocked_reasons", {}))
        micro_blocked_ratio = (candidates_blocked_by_micro / candidates_total) if candidates_total > 0 else 0.0
        fill_ratio = (orders_filled / orders_submitted) if orders_submitted > 0 else 0.0
        fill_rate = fill_ratio
        ttf_values = [float(value) for value in self._runtime_state.get("time_to_fill_ms", [])]
        slippage_values = [float(value) for value in self._runtime_state.get("slippage_bps", [])]
        policy_tick_bps_values = [float(value) for value in self._runtime_state.get("policy_tick_bps_values", [])]
        replaces_total = int(self._runtime_counters.get("replaces_total", 0))
        cancels_total = int(self._runtime_counters.get("cancels_total", 0))
        aborted_timeout_total = int(self._runtime_counters.get("aborted_timeout_total", 0))
        dust_abort_total = int(self._runtime_counters.get("dust_abort_total", 0))
        scored_rows = int(self._runtime_counters.get("scored_rows", 0))
        selected_rows = int(self._runtime_counters.get("selected_rows", 0))
        skipped_missing_features_rows = int(self._runtime_counters.get("skipped_missing_features_rows", 0))
        selection_ratio = (selected_rows / scored_rows) if scored_rows > 0 else 0.0
        summary = BacktestRunSummary(
            run_id=run_id,
            run_dir=str(run_root),
            tf=settings.tf,
            from_ts_ms=from_ts_ms,
            to_ts_ms=to_ts_ms,
            bars_processed=bars_processed,
            markets=active_markets,
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
            max_drawdown_pct=max_drawdown_pct(equity_curve),
            win_rate=win_rate(realized_trade_pnls),
            strategy=str(strategy_mode),
            scored_rows=scored_rows,
            selected_rows=selected_rows,
            skipped_missing_features_rows=skipped_missing_features_rows,
            selection_ratio=selection_ratio,
            exposure_avg_open_positions=mean(open_positions_curve),
            exposure_max_open_positions=int(max(open_positions_curve) if open_positions_curve else 0),
        )
        summary_payload = asdict(summary)
        summary_payload["execution_structure"] = summarize_fill_records(self._runtime_state.get("fill_records", []))
        write_summary_json(run_root, summary_payload)
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
                "enabled": bool(settings.micro_order_policy.enabled),
                "mode": str(settings.micro_order_policy.mode),
                "on_missing": str(settings.micro_order_policy.on_missing),
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
        _write_json(
            path=run_root / "selection_stats.json",
            payload={
                "strategy": str(strategy_mode),
                "scored_rows": scored_rows,
                "eligible_rows": int(self._runtime_counters.get("eligible_rows", 0)),
                "selected_rows": selected_rows,
                "selection_ratio": selection_ratio,
                "min_prob_used": float(self._runtime_state.get("model_alpha_min_prob_used", 0.0)),
                "min_prob_source": str(self._runtime_state.get("model_alpha_min_prob_source", "manual")),
                "top_pct_used": float(self._runtime_state.get("model_alpha_top_pct_used", 0.0)),
                "top_pct_source": str(self._runtime_state.get("model_alpha_top_pct_source", "manual")),
                "min_candidates_used": int(self._runtime_state.get("model_alpha_min_candidates_used", 0)),
                "min_candidates_source": str(
                    self._runtime_state.get("model_alpha_min_candidates_source", "manual")
                ),
                "dropped_min_prob_rows": int(self._runtime_counters.get("dropped_min_prob_rows", 0)),
                "dropped_min_prob_ratio": (
                    int(self._runtime_counters.get("dropped_min_prob_rows", 0)) / scored_rows if scored_rows > 0 else 0.0
                ),
                "dropped_top_pct_rows": int(self._runtime_counters.get("dropped_top_pct_rows", 0)),
                "dropped_top_pct_ratio": (
                    int(self._runtime_counters.get("dropped_top_pct_rows", 0)) / scored_rows if scored_rows > 0 else 0.0
                ),
                "blocked_min_candidates_ts": int(self._runtime_counters.get("blocked_min_candidates_ts", 0)),
                "exit_intents_total": int(self._runtime_counters.get("exit_intents_total", 0)),
                "per_ts": list(self._runtime_state.get("selection_per_ts", [])),
            },
        )
        _write_json(
            path=run_root / "debug_mismatch.json",
            payload={
                "strategy": str(strategy_mode),
                "skipped_missing_features_rows": skipped_missing_features_rows,
                "reasons": _normalize_reason_counts(self._runtime_counters.get("debug_mismatch_reasons", {})),
            },
        )
        _write_trade_artifacts(
            run_root=run_root,
            fill_records=self._runtime_state.get("fill_records", []),
        )
        return summary

    def _run_candidate_cycle(
        self,
        *,
        ts_ms: int,
        current_market: str,
        latest_bar_index_by_market: dict[str, int],
        universe: StaticUniverseProvider,
        market_data: _BarDataHub,
        candidate_generator: CandidateGeneratorV1,
        trade_gate: TradeGateV1,
        micro_order_policy: MicroOrderPolicyV1 | None,
        micro_snapshot_provider: MicroSnapshotProvider | None,
        exchange: BacktestSimExchange,
        execution: BacktestExecutionGateway,
        event_store: JsonlEventStore,
        append_event: Callable[[str, int, dict[str, Any] | None], None],
    ) -> None:
        markets = universe.markets()
        append_event(
            "MARKET_SNAPSHOT",
            ts_ms=ts_ms,
            payload={"count": len(markets), "markets": markets},
        )
        candidates = candidate_generator.generate(markets=markets, market_data=market_data)
        if not candidates and self._runtime_counters["orders_submitted"] == 0:
            ticker = market_data.get_latest_ticker(current_market)
            if ticker is not None:
                candidates.append(
                    Candidate(
                        market=current_market,
                        score=0.0,
                        proposed_side="bid",
                        ref_price=ticker.trade_price,
                        meta={"fallback": True, "reason": "NO_MOMENTUM_CANDIDATE"},
                    )
                )
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
                latest_bar_index_by_market=latest_bar_index_by_market,
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

    def _run_model_alpha_cycle(
        self,
        *,
        ts_ms: int,
        latest_bar_index_by_market: dict[str, int],
        universe: StaticUniverseProvider,
        market_data: _BarDataHub,
        trade_gate: TradeGateV1,
        micro_order_policy: MicroOrderPolicyV1 | None,
        micro_snapshot_provider: MicroSnapshotProvider | None,
        exchange: BacktestSimExchange,
        execution: BacktestExecutionGateway,
        event_store: JsonlEventStore,
        append_event: Callable[[str, int, dict[str, Any] | None], None],
        strategy: ModelAlphaStrategyV1,
    ) -> None:
        markets = universe.markets()
        open_markets = exchange.open_position_markets()
        result = strategy.on_ts(
            ts_ms=ts_ms,
            active_markets=markets,
            latest_prices=market_data.latest_prices(),
            open_markets=open_markets,
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

        selection_rows = self._runtime_state.setdefault("selection_per_ts", [])
        if isinstance(selection_rows, list):
            scored_rows_ts = int(result.scored_rows)
            eligible_rows_ts = int(result.eligible_rows)
            selected_rows_ts = int(result.selected_rows)
            selection_rows.append(
                {
                    "ts_ms": int(ts_ms),
                    "scored_rows": scored_rows_ts,
                    "eligible_rows": eligible_rows_ts,
                    "selected_rows": selected_rows_ts,
                    "selected_ratio": (selected_rows_ts / scored_rows_ts) if scored_rows_ts > 0 else 0.0,
                    "intents_created": int(len(result.intents)),
                    "missing_rows": int(result.skipped_missing_features_rows),
                    "min_prob_used": float(result.min_prob_used),
                    "min_prob_source": str(result.min_prob_source),
                    "top_pct_used": float(result.top_pct_used),
                    "top_pct_source": str(result.top_pct_source),
                    "min_candidates_used": int(result.min_candidates_used),
                    "min_candidates_source": str(result.min_candidates_source),
                }
            )

        append_event(
            "MODEL_ALPHA_SELECTION",
            ts_ms=ts_ms,
            payload={
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
                "reasons": dict(result.skipped_reasons),
            },
        )

        for intent in result.intents:
            if str(intent.side).strip().lower() == "ask":
                self._runtime_counters["exit_intents_total"] = int(self._runtime_counters.get("exit_intents_total", 0)) + 1
            candidate = _intent_to_candidate(intent)
            self._try_submit_candidate(
                ts_ms=ts_ms,
                candidate=candidate,
                latest_bar_index_by_market=latest_bar_index_by_market,
                market_data=market_data,
                trade_gate=trade_gate,
                micro_order_policy=micro_order_policy,
                micro_snapshot_provider=micro_snapshot_provider,
                exchange=exchange,
                execution=execution,
                event_store=event_store,
                append_event=append_event,
            )

    def _try_submit_candidate(
        self,
        *,
        ts_ms: int,
        candidate: Candidate,
        latest_bar_index_by_market: dict[str, int],
        market_data: _BarDataHub,
        trade_gate: TradeGateV1,
        micro_order_policy: MicroOrderPolicyV1 | None,
        micro_snapshot_provider: MicroSnapshotProvider | None,
        exchange: BacktestSimExchange,
        execution: BacktestExecutionGateway,
        event_store: JsonlEventStore,
        append_event: Callable[[str, int, dict[str, Any] | None], None],
    ) -> bool:
        self._runtime_counters["candidates_total"] = int(self._runtime_counters.get("candidates_total", 0)) + 1
        bar_index = latest_bar_index_by_market.get(candidate.market.strip().upper())
        if bar_index is None:
            return False

        ticker = market_data.get_latest_ticker(candidate.market)
        if ticker is None:
            return False

        rules = self._rules_provider.get_rules(
            market=candidate.market,
            reference_price=candidate.ref_price,
            ts_ms=ts_ms,
        )
        side_value = str(candidate.proposed_side).strip().lower()
        forced_volume = None
        if isinstance(candidate.meta, dict):
            forced_volume = _safe_optional_float(candidate.meta.get("force_volume"))
        entry_notional_quote = (
            _entry_notional_quote_for_strategy(
                strategy_mode=str(self._run_settings.strategy).strip().lower() or "candidates_v1",
                per_trade_krw=float(self._run_settings.per_trade_krw),
                min_total_krw=max(float(rules.min_total), float(self._run_settings.min_order_krw)),
                model_alpha_settings=self._run_settings.model_alpha,
                candidate_meta=(candidate.meta if isinstance(candidate.meta, dict) else None),
            )
            if side_value == "bid" and (forced_volume is None or forced_volume <= 0)
            else None
        )
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
            price=max(float(candidate.ref_price), float(ticker.trade_price)),
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
            min_total_krw=max(float(rules.min_total), float(self._run_settings.min_order_krw)),
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

        ref_price = max(float(candidate.ref_price), float(ticker.trade_price), 1e-8)
        model_prob = (
            _safe_optional_float(candidate.meta.get("model_prob"))
            if isinstance(candidate.meta, dict)
            else None
        )
        strategy_mode = str(self._run_settings.strategy).strip().lower() or "candidates_v1"
        policy_decision = None
        resolved_model_alpha_settings = self._runtime_state.get("resolved_model_alpha_settings")
        exec_profile = _strategy_backtest_exec_profile(
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
        policy_diagnostics: dict[str, Any] = {}
        snapshot = (
            micro_snapshot_provider.get(candidate.market, int(ts_ms))
            if micro_snapshot_provider is not None
            else None
        )
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
        if strategy_mode == "model_alpha_v1" and side_value == "bid":
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
        if volume <= 0:
            return False
        profile_payload = order_exec_profile_to_dict(exec_profile)
        selected_ord_type = (
            str((execution_policy or {}).get("selected_ord_type", "limit")).strip().lower() or "limit"
        )
        selected_time_in_force = (
            str((execution_policy or {}).get("selected_time_in_force", "gtc")).strip().lower() or "gtc"
        )
        simulated_ord_type = "limit" if selected_ord_type == "best" else selected_ord_type
        policy_payload = {
            "enabled": bool(micro_order_policy is not None),
            "tier": str(policy_decision.tier) if policy_decision is not None and policy_decision.tier is not None else None,
            "reason_code": (
                str(policy_decision.reason_code)
                if policy_decision is not None
                else "POLICY_DISABLED"
            ),
        }

        intent = new_order_intent(
            market=candidate.market,
            side=side_value,
            ord_type=simulated_ord_type,
            time_in_force=selected_time_in_force,
            price=limit_price,
            volume=volume,
            reason_code=(
                str(candidate.meta.get("reason_code"))
                if isinstance(candidate.meta, dict) and str(candidate.meta.get("reason_code", "")).strip()
                else "BACKTEST_CANDIDATE_V1"
            ),
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
            }
        append_event("INTENT_CREATED", ts_ms=ts_ms, payload=asdict(intent))

        update = execution.submit_intent(
            intent=intent,
            rules=rules,
            bar_index=bar_index,
            ts_ms=ts_ms,
        )
        self._apply_execution_update(
            update=update,
            trade_gate=trade_gate,
            event_store=event_store,
            append_event=append_event,
            ts_ms=ts_ms,
            counters=self._runtime_counters,
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
    ) -> bool:
        has_ask_fill = False
        intent_context = self._runtime_state.setdefault("intent_context", {})
        filled_intents = self._runtime_state.setdefault("filled_intents", set())
        time_to_fill_ms = self._runtime_state.setdefault("time_to_fill_ms", [])
        slippage_values = self._runtime_state.setdefault("slippage_bps", [])
        fill_records = self._runtime_state.setdefault("fill_records", [])
        order_exec_profile_by_order_id = self._runtime_state.setdefault("order_exec_profile_by_order_id", {})
        order_policy_diag_by_order_id = self._runtime_state.setdefault("order_policy_diag_by_order_id", {})
        strategy_adapter = self._runtime_state.get("strategy_adapter")
        order_by_id = {order.order_id: order for order in update.orders_filled}

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
            append_event("ORDER_SUBMITTED", ts_ms=order.updated_ts_ms, payload=payload)
            counters["orders_submitted"] += 1

        for order in update.orders_canceled:
            event_store.append_order(order)
            append_event("ORDER_CANCELED", ts_ms=order.updated_ts_ms, payload=asdict(order))
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
            append_event("ORDER_FILLED", ts_ms=fill.ts_ms, payload=asdict(fill))
            counters["orders_filled"] += 1
            order = order_by_id.get(fill.order_id)
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
                    }
                )
            if order is not None and strategy_adapter is not None and hasattr(strategy_adapter, "on_fill"):
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
                    # keep backtest resilient even if strategy bookkeeping fails
                    pass

        for order in update.orders_filled:
            event_store.append_order(order)
            if order.side == "ask":
                has_ask_fill = True
            if (
                str(order.state).strip().upper() == "FILLED"
                and isinstance(filled_intents, set)
                and order.intent_id not in filled_intents
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
                filled_intents.add(order.intent_id)

        for market in update.success_markets:
            trade_gate.record_success(market)

        for market in update.failed_markets:
            trade_gate.record_failure(market, ts_ms=ts_ms)
            counters["intents_failed"] += 1

        return has_ask_fill

    def _resolve_micro_snapshot_provider(self) -> MicroSnapshotProvider | None:
        cfg = self._run_settings.micro_gate
        if not (cfg.enabled or self._run_settings.micro_order_policy.enabled):
            return None
        if self._micro_snapshot_provider is not None:
            return self._micro_snapshot_provider

        if cfg.live_ws.enabled:
            # Backtest is offline; live WS provider can still be injected for parity tests.
            self._micro_snapshot_provider = LiveWsMicroSnapshotProvider(settings=cfg.live_ws)
            return self._micro_snapshot_provider

        micro_root = _resolve_micro_root(
            dataset_name=cfg.dataset_name,
            parquet_root=Path(self._run_settings.parquet_root),
        )
        if not micro_root.exists():
            return None
        self._micro_snapshot_provider = OfflineMicroSnapshotProvider(
            micro_root=micro_root,
            tf=(cfg.tf or self._run_settings.tf),
            cache_entries=cfg.cache_entries,
        )
        return self._micro_snapshot_provider

    def _build_model_alpha_strategy(
        self,
        *,
        active_markets: list[str],
        from_ts_ms: int,
        to_ts_ms: int,
    ) -> ModelAlphaStrategyV1:
        settings = self._run_settings
        feature_set = str(settings.feature_set).strip().lower() or "v3"
        if feature_set not in {"v3", "v4"}:
            raise ValueError("model_alpha_v1 currently requires --feature-set v3 or v4")
        model_ref = str(settings.model_ref or settings.model_alpha.model_ref).strip()
        if not model_ref:
            raise ValueError("model_alpha_v1 requires model_ref")
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
        dataset_root = (
            Path(str(settings.model_feature_dataset_root))
            if settings.model_feature_dataset_root
            else predictor.dataset_root
        )
        if dataset_root is None:
            raise ValueError("unable to resolve feature dataset root for model_alpha_v1")
        if not dataset_root.exists():
            raise FileNotFoundError(f"feature dataset root not found: {dataset_root}")

        request = DatasetRequest(
            dataset_root=dataset_root,
            tf=str(settings.tf).strip().lower(),
            quote=str(settings.quote).strip().upper(),
            top_n=max(int(settings.top_n), 1),
            start_ts_ms=int(from_ts_ms),
            end_ts_ms=int(to_ts_ms),
            markets=tuple(active_markets),
            batch_rows=200_000,
        )
        groups = iter_feature_rows_grouped_by_ts(
            request,
            feature_columns=predictor.feature_columns,
            extra_columns=resolve_model_alpha_runtime_row_columns(predictor=predictor),
        )
        interval_ms = _interval_ms_from_tf(settings.tf)
        return ModelAlphaStrategyV1(
            predictor=predictor,
            feature_groups=groups,
            settings=resolved_model_alpha,
            interval_ms=interval_ms,
        )

    def _resolve_markets(self) -> list[str]:
        explicit = _normalize_markets(market=self._run_settings.market, markets=self._run_settings.markets)
        if explicit:
            return explicit

        mode = str(self._run_settings.universe_mode).strip().lower()
        if mode == "fixed_list":
            items = self._loader.list_markets(quote=self._run_settings.quote)
            return items[: max(int(self._run_settings.top_n), 1)]

        if mode == "static_start":
            markets = build_static_start_universe(
                parquet_root=self._run_settings.parquet_root,
                dataset_name=self._run_settings.dataset_name,
                tf=self._run_settings.tf,
                quote=self._run_settings.quote,
                top_n=self._run_settings.top_n,
                from_ts_ms=self._run_settings.from_ts_ms,
                to_ts_ms=self._run_settings.to_ts_ms,
            )
            if markets:
                return markets
            items = self._loader.list_markets(quote=self._run_settings.quote)
            return items[: max(int(self._run_settings.top_n), 1)]

        raise ValueError(f"unsupported universe mode: {self._run_settings.universe_mode}")

    def _resolve_time_window(self, *, markets: list[str]) -> tuple[int, int]:
        bounds: list[tuple[int, int]] = []
        for market in markets:
            bound = self._loader.market_time_bounds(market=market)
            if bound is not None:
                bounds.append(bound)
        if not bounds:
            raise RuntimeError("unable to infer timeframe bounds; no market data found")

        dataset_min_ts = min(item[0] for item in bounds)
        dataset_max_ts = max(item[1] for item in bounds)
        from_ts_ms = self._run_settings.from_ts_ms
        to_ts_ms = self._run_settings.to_ts_ms
        duration_ms = (
            max(int(self._run_settings.duration_days), 1) * 86_400_000
            if self._run_settings.duration_days is not None
            else None
        )

        if from_ts_ms is None and to_ts_ms is None:
            if duration_ms is None:
                from_ts_ms = dataset_min_ts
                to_ts_ms = dataset_max_ts
            else:
                to_ts_ms = dataset_max_ts
                from_ts_ms = max(dataset_min_ts, to_ts_ms - duration_ms)
        elif from_ts_ms is not None and to_ts_ms is None:
            if duration_ms is not None:
                to_ts_ms = from_ts_ms + duration_ms
            else:
                to_ts_ms = dataset_max_ts
        elif from_ts_ms is None and to_ts_ms is not None:
            if duration_ms is not None:
                from_ts_ms = to_ts_ms - duration_ms
            else:
                from_ts_ms = dataset_min_ts

        assert from_ts_ms is not None
        assert to_ts_ms is not None

        if from_ts_ms > to_ts_ms:
            raise ValueError(f"invalid time window: from_ts_ms={from_ts_ms} > to_ts_ms={to_ts_ms}")
        return (int(from_ts_ms), int(to_ts_ms))

    def _load_market_bars(
        self,
        *,
        markets: list[str],
        from_ts_ms: int,
        to_ts_ms: int,
    ) -> dict[str, list[CandleBar]]:
        loaded: dict[str, list[CandleBar]] = {}
        for market in markets:
            bars = self._loader.load_market_bars(
                market=market,
                from_ts_ms=from_ts_ms,
                to_ts_ms=to_ts_ms,
            )
            if not bars:
                continue
            loaded[market] = bars
        return loaded


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


def _legacy_backtest_exec_profile(settings: BacktestRunSettings) -> OrderExecProfile:
    interval_ms = _interval_ms_from_tf(settings.tf)
    timeout_ms = max(int(settings.order_timeout_bars), 1) * interval_ms
    return make_legacy_exec_profile(
        timeout_ms=timeout_ms,
        replace_interval_ms=timeout_ms,
        max_replaces=max(int(settings.reprice_max_attempts), 0),
        price_mode=PRICE_MODE_JOIN,
        max_chase_bps=10_000,
        min_replace_interval_ms_global=1_500,
    )


def _model_alpha_backtest_exec_profile(
    settings: BacktestRunSettings,
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


def _strategy_backtest_exec_profile(
    settings: BacktestRunSettings,
    *,
    model_alpha_settings: ModelAlphaSettings | None = None,
) -> OrderExecProfile:
    strategy_mode = str(settings.strategy).strip().lower() or "candidates_v1"
    if strategy_mode == "model_alpha_v1":
        return _model_alpha_backtest_exec_profile(settings, model_alpha_settings=model_alpha_settings)
    return _legacy_backtest_exec_profile(settings)


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


def _resolve_micro_root(*, dataset_name: str, parquet_root: Path) -> Path:
    raw = str(dataset_name).strip() or "micro_v1"
    candidate = Path(raw)
    if candidate.exists() or candidate.is_absolute():
        return candidate
    return parquet_root / raw


def _write_json(*, path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


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


def run_backtest_sync(
    *,
    run_settings: BacktestRunSettings,
    upbit_settings: UpbitSettings | None = None,
) -> BacktestRunSummary:
    engine = BacktestRunEngine(run_settings=run_settings, upbit_settings=upbit_settings)
    return engine.run()


def _normalize_markets(*, market: str | None, markets: Sequence[str]) -> list[str]:
    values: list[str] = []
    if market:
        values.append(str(market).strip().upper())
    values.extend(str(item).strip().upper() for item in markets if str(item).strip())
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def _find_baseline(points: deque[_BarTickerSnapshot], *, target_ts_ms: int) -> _BarTickerSnapshot | None:
    baseline: _BarTickerSnapshot | None = None
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


def _base_currency(market: str) -> str:
    value = str(market).strip().upper()
    if "-" not in value:
        return ""
    return value.split("-", 1)[1]


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
