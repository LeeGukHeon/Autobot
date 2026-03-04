"""Live paper-run engine (WS Top20 -> Candidate -> TradeGate -> PaperExecution)."""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import asdict, dataclass, field
import json
from pathlib import Path
import time
from typing import Any, Callable, Sequence

from autobot.common.event_store import JsonlEventStore
from autobot.execution.intent import OrderIntent, new_order_intent
from autobot.execution.order_supervisor import (
    PRICE_MODE_JOIN,
    REASON_CHASE_LIMIT_EXCEEDED,
    REASON_MAX_REPLACES_REACHED,
    REASON_MIN_NOTIONAL_DUST_ABORT,
    REASON_TIMEOUT_REPLACE,
    SUPERVISOR_ACTION_ABORT,
    SUPERVISOR_ACTION_REPLACE,
    OrderExecProfile,
    build_limit_price_from_mode,
    evaluate_supervisor_action,
    make_legacy_exec_profile,
    mean,
    order_exec_profile_from_dict,
    order_exec_profile_to_dict,
    percentile,
    slippage_bps,
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
from autobot.upbit.ws.models import TickerEvent

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
    duration_sec: int = 600
    quote: str = "KRW"
    top_n: int = 20
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
    micro_gate: MicroGateSettings = field(default_factory=MicroGateSettings)
    micro_order_policy: MicroOrderPolicySettings = field(default_factory=MicroOrderPolicySettings)


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
        snapshot = TickerSnapshot(
            market=event.market,
            ts_ms=event.ts_ms,
            trade_price=event.trade_price,
            acc_trade_price_24h=event.acc_trade_price_24h,
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
    fills: list[FillEvent] = field(default_factory=list)
    failed_markets: list[str] = field(default_factory=list)
    success_markets: list[str] = field(default_factory=list)
    supervisor_events: list[dict[str, Any]] = field(default_factory=list)
    counter_deltas: dict[str, int] = field(default_factory=dict)
    reason_counts: dict[str, int] = field(default_factory=dict)


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

    def submit_intent(self, *, intent: OrderIntent, latest_trade_price: float, ts_ms: int) -> ExecutionUpdate:
        update = ExecutionUpdate()
        rules = self._rules_provider.get_rules(market=intent.market, reference_price=intent.price, ts_ms=ts_ms)
        profile = order_exec_profile_from_dict(
            intent.meta.get("exec_profile"),
            fallback=self._default_profile,
        )
        order, fill = self._exchange.submit_limit_order(
            intent=intent,
            rules=rules,
            latest_trade_price=latest_trade_price,
            ts_ms=ts_ms,
            reprice_attempt=0,
        )
        update.orders_submitted.append(order)

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
            action = evaluate_supervisor_action(
                profile=pending.profile,
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
                meta={**pending.intent.meta, "reprice_attempt": pending.replace_count + 1},
            )
            new_order, new_fill = self._exchange.submit_limit_order(
                intent=reprice_intent,
                rules=rules,
                latest_trade_price=event.trade_price,
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
                    },
                }
            )
            market_window.append(int(event.ts_ms))

            if new_order.state in {"OPEN", "PARTIAL"}:
                self._activate_pending(
                    intent=reprice_intent,
                    order_id=new_order.order_id,
                    replace_count=pending.replace_count + 1,
                    ts_ms=event.ts_ms,
                    created_ts_ms=pending.created_ts_ms,
                    profile=pending.profile,
                )
            elif new_order.state == "FAILED":
                update.failed_markets.append(reprice_intent.market)
                self._clear_pending(intent_id)
            else:
                self._clear_pending(intent_id)

            if new_fill is not None:
                update.fills.append(new_fill)
                latest = self._exchange.get_order(new_fill.order_id)
                if latest is not None:
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
            "order_supervisor_reasons": {},
        }
        self._runtime_state: dict[str, Any] = {}

    async def run(self) -> PaperRunSummary:
        quote = self._run_settings.quote.strip().upper()
        run_id = f"paper-{time.strftime('%Y%m%d-%H%M%S')}"
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
            "order_supervisor_reasons": {},
        }
        self._runtime_state = {
            "intent_context": {},
            "filled_intents": set(),
            "time_to_fill_ms": [],
            "slippage_bps": [],
        }

        markets = self._market_loader(quote)
        if not markets:
            raise RuntimeError(f"no markets available for quote={quote}")

        market_data = MarketDataHub(history_sec=max(self._run_settings.momentum_window_sec * 2, 300))
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
        trade_gate = TradeGateV1(
            GateSettings(
                per_trade_krw=self._run_settings.per_trade_krw,
                max_positions=self._run_settings.max_positions,
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
            default_profile=_legacy_paper_exec_profile(self._run_settings),
            max_replaces_per_min_per_market=(
                self._run_settings.micro_order_policy.safety.max_replaces_per_min_per_market
                if self._run_settings.micro_order_policy.enabled
                else 10
            ),
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

        next_decision_at = time.monotonic()
        next_report_at = time.monotonic() + max(self._run_settings.print_every_sec, 1.0)
        deadline_monotonic = time.monotonic() + float(self._run_settings.duration_sec)

        with JsonlEventStore(run_root) as store:

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
                    "duration_sec": self._run_settings.duration_sec,
                    "markets_subscribed": len(markets),
                    "micro_order_policy_enabled": bool(self._run_settings.micro_order_policy.enabled),
                    "micro_order_policy_mode": str(self._run_settings.micro_order_policy.mode),
                    "micro_order_policy_on_missing": str(self._run_settings.micro_order_policy.on_missing),
                },
            )

            async for ticker in self._ws_client.stream_ticker(markets, duration_sec=self._run_settings.duration_sec):
                if time.monotonic() >= deadline_monotonic:
                    break
                received_events += 1
                market_data.update(ticker)
                universe.update_ticker(ticker)

                realized_before = exchange.total_realized_pnl()
                market_has_ask_fill = self._apply_execution_update(
                    update=execution.on_ticker(event=ticker),
                    trade_gate=trade_gate,
                    event_store=store,
                    append_event=append_event,
                    ts_ms=ticker.ts_ms,
                    counters=self._runtime_counters,
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
                    equity_curve.append(snapshot.equity_quote)
                    print(
                        "[paper][status] "
                        f"ts_ms={ticker.ts_ms} events={received_events} "
                        f"universe={len(universe.markets())} open_orders={len(exchange.list_open_orders())} "
                        f"equity={snapshot.equity_quote:.2f}"
                    )
                    next_report_at = now_monotonic + max(self._run_settings.print_every_sec, 1.0)

            final_ts_ms = int(time.time() * 1000)
            final_snapshot = exchange.portfolio_snapshot(ts_ms=final_ts_ms, latest_prices=market_data.latest_prices())
            store.append_equity(final_snapshot)
            append_event("PORTFOLIO_SNAPSHOT", ts_ms=final_ts_ms, payload=asdict(final_snapshot))
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
                },
            )

        fill_ratio = (orders_filled / orders_submitted) if orders_submitted > 0 else 0.0
        fill_rate = fill_ratio
        micro_blocked_reasons = _normalize_reason_counts(self._runtime_counters.get("micro_blocked_reasons", {}))
        micro_blocked_ratio = (candidates_blocked_by_micro / candidates_total) if candidates_total > 0 else 0.0
        max_drawdown_pct = _max_drawdown_pct(equity_curve)
        wins = sum(1 for pnl in realized_trade_pnls if pnl > 0)
        win_rate = (wins / len(realized_trade_pnls)) if realized_trade_pnls else 0.0
        ttf_values = [float(value) for value in self._runtime_state.get("time_to_fill_ms", [])]
        slippage_values = [float(value) for value in self._runtime_state.get("slippage_bps", [])]
        replaces_total = int(self._runtime_counters.get("replaces_total", 0))
        cancels_total = int(self._runtime_counters.get("cancels_total", 0))
        aborted_timeout_total = int(self._runtime_counters.get("aborted_timeout_total", 0))
        dust_abort_total = int(self._runtime_counters.get("dust_abort_total", 0))

        summary = PaperRunSummary(
            run_id=run_id,
            run_dir=str(run_root),
            duration_sec=float(self._run_settings.duration_sec),
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
        )
        _write_json(path=run_root / "summary.json", payload=asdict(summary))
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
                "slippage_bps_mean": mean(slippage_values),
                "slippage_bps_p50": percentile(slippage_values, 0.50),
                "slippage_bps_p90": percentile(slippage_values, 0.90),
            },
        )
        return summary

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
        gate_price = round_price_to_tick(
            price=max(candidate.ref_price, ticker.trade_price),
            tick_size=rules.tick_size,
            side=candidate.proposed_side,
        )
        gate_volume = order_volume_from_notional(
            notional_quote=self._run_settings.per_trade_krw,
            price=gate_price,
        )
        fee_rate = rules.fee_rate(side=candidate.proposed_side, maker_or_taker="taker")
        decision = trade_gate.evaluate(
            ts_ms=ts_ms,
            market=candidate.market,
            side=candidate.proposed_side,
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
                    "side": candidate.proposed_side,
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
                    "side": candidate.proposed_side,
                    "reason_code": decision.reason_code,
                    "detail": decision.detail,
                    "severity": decision.severity,
                    "gate_reasons": list(decision.gate_reasons),
                    "diagnostics": decision.diagnostics or {},
                },
            )

        policy_decision = None
        exec_profile = _legacy_paper_exec_profile(self._run_settings)
        policy_diagnostics: dict[str, Any] = {}
        if micro_order_policy is not None:
            snapshot = (
                micro_snapshot_provider.get(candidate.market, int(ts_ms))
                if micro_snapshot_provider is not None
                else None
            )
            policy_decision = micro_order_policy.evaluate(micro_snapshot=snapshot)
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
                        "side": candidate.proposed_side,
                        "reason_code": policy_decision.reason_code,
                        "detail": policy_decision.detail,
                        "diagnostics": policy_diagnostics,
                    },
                )
                return False
            if policy_decision.profile is not None:
                exec_profile = policy_decision.profile
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

        ref_price = max(float(candidate.ref_price), float(ticker.trade_price), 1e-8)
        limit_price = build_limit_price_from_mode(
            side=candidate.proposed_side,
            ref_price=ref_price,
            tick_size=rules.tick_size,
            price_mode=exec_profile.price_mode,
        )
        volume = order_volume_from_notional(
            notional_quote=self._run_settings.per_trade_krw,
            price=limit_price,
        )
        profile_payload = order_exec_profile_to_dict(exec_profile)
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
            side=candidate.proposed_side,
            ord_type="limit",
            time_in_force="gtc",
            price=limit_price,
            volume=volume,
            reason_code="CANDIDATE_V1",
            meta={
                **candidate.meta,
                "candidate_score": candidate.score,
                "tick_size": rules.tick_size,
                "min_total": rules.min_total,
                "gate_severity": decision.severity,
                "gate_reasons": list(decision.gate_reasons),
                "exec_profile": profile_payload,
                "initial_ref_price": ref_price,
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
                "exec_profile": profile_payload,
                "micro_diagnostics": policy_diagnostics,
                "micro_order_policy": policy_payload,
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
        order_by_id = {order.order_id: order for order in update.orders_filled}

        for order in update.orders_submitted:
            event_store.append_order(order)
            payload = asdict(order)
            if isinstance(intent_context, dict):
                context = intent_context.get(order.intent_id)
                if isinstance(context, dict):
                    exec_profile = context.get("exec_profile")
                    diagnostics = context.get("micro_diagnostics")
                    policy = context.get("micro_order_policy")
                    payload["exec_profile"] = dict(exec_profile) if isinstance(exec_profile, dict) else {}
                    payload["micro_diagnostics"] = dict(diagnostics) if isinstance(diagnostics, dict) else {}
                    payload["micro_order_policy"] = dict(policy) if isinstance(policy, dict) else {}
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
            append_event(event_type, ts_ms=ts_value, payload=payload if isinstance(payload, dict) else {})

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
            if order is not None and isinstance(intent_context, dict):
                context = intent_context.get(order.intent_id)
                if isinstance(context, dict):
                    ref_price = _safe_optional_float(context.get("initial_ref_price"))
                    if ref_price is not None and ref_price > 0:
                        slip = slippage_bps(
                            side=order.side,
                            fill_price=fill.price,
                            ref_price=ref_price,
                        )
                        if slip is not None:
                            slippage_values.append(float(slip))

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
        if not (cfg.enabled or self._run_settings.micro_order_policy.enabled):
            return None
        if self._micro_snapshot_provider is not None:
            return self._micro_snapshot_provider

        if cfg.live_ws.enabled:
            live_provider = LiveWsMicroSnapshotProvider(settings=cfg.live_ws)
            live_provider.track_markets(markets)
            self._micro_snapshot_provider = live_provider
            return self._micro_snapshot_provider

        micro_root = _resolve_micro_root(dataset_name=cfg.dataset_name)
        if not micro_root.exists():
            return None
        provider = OfflineMicroSnapshotProvider(
            micro_root=micro_root,
            tf=(cfg.tf or "5m"),
            cache_entries=cfg.cache_entries,
        )
        self._micro_snapshot_provider = provider
        return self._micro_snapshot_provider


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


def _write_json(*, path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


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
