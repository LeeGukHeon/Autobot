"""Backtest engine v1 for candle-based simulation."""

from __future__ import annotations

from collections import deque
from dataclasses import asdict, dataclass, field
import heapq
from pathlib import Path
import time
from typing import Any, Callable, Sequence

from autobot.common.event_store import JsonlEventStore
from autobot.execution.intent import OrderIntent, new_order_intent
from autobot.paper.sim_exchange import (
    FillEvent,
    MarketRules,
    PaperOrder,
    order_volume_from_notional,
    round_price_to_tick,
)
from autobot.strategy.candidates_v1 import Candidate, CandidateGeneratorV1, CandidateSettings
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
    output_root_dir: str = "data/backtest"
    seed: int = 0


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
    fill_rate: float
    realized_pnl_quote: float
    unrealized_pnl_quote: float
    max_drawdown_pct: float
    win_rate: float


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


@dataclass
class _PendingIntent:
    intent: OrderIntent
    order_id: str
    attempts: int
    timeout_bar_index: int
    market: str


class BacktestExecutionGateway:
    def __init__(
        self,
        *,
        exchange: BacktestSimExchange,
        order_timeout_bars: int,
        reprice_max_attempts: int,
        reprice_tick_steps: int,
    ) -> None:
        self._exchange = exchange
        self._order_timeout_bars = max(int(order_timeout_bars), 1)
        self._reprice_max_attempts = max(int(reprice_max_attempts), 0)
        self._reprice_tick_steps = max(int(reprice_tick_steps), 1)
        self._pending_by_intent: dict[str, _PendingIntent] = {}
        self._intent_by_order_id: dict[str, str] = {}

    def submit_intent(
        self,
        *,
        intent: OrderIntent,
        rules: MarketRules,
        bar_index: int,
        ts_ms: int,
    ) -> ExecutionUpdate:
        update = ExecutionUpdate()
        order, fill = self._exchange.submit_limit_order_deferred(
            intent=intent,
            rules=rules,
            ts_ms=ts_ms,
            activate_on_index=bar_index + 1,
            reprice_attempt=0,
        )
        update.orders_submitted.append(order)
        if order.state in {"OPEN", "PARTIAL"}:
            self._activate_pending(
                intent=intent,
                order_id=order.order_id,
                attempts=0,
                timeout_bar_index=bar_index + self._order_timeout_bars,
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

        market_value = bar.market.strip().upper()
        for intent_id, pending in list(self._pending_by_intent.items()):
            if pending.market != market_value:
                continue
            if bar_index < pending.timeout_bar_index:
                continue

            current_order = self._exchange.get_order(pending.order_id)
            if current_order is None or current_order.state not in {"OPEN", "PARTIAL"}:
                self._clear_pending(intent_id)
                continue

            canceled = self._exchange.cancel_order(current_order.order_id, ts_ms=bar.ts_ms, reason="ORDER_TIMEOUT")
            if canceled is not None:
                update.orders_canceled.append(canceled)
            self._intent_by_order_id.pop(current_order.order_id, None)

            remaining_volume = max(current_order.volume_req - current_order.volume_filled, 0.0)
            if pending.attempts >= self._reprice_max_attempts or remaining_volume <= 0:
                update.failed_markets.append(pending.market)
                self._clear_pending(intent_id)
                continue

            new_price = _reprice_limit_price(
                price=current_order.price,
                tick_size=rules.tick_size,
                side=current_order.side,
                ticks=self._reprice_tick_steps,
            )
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
                meta={**pending.intent.meta, "reprice_attempt": pending.attempts + 1},
            )
            new_order, new_fill = self._exchange.submit_limit_order_deferred(
                intent=reprice_intent,
                rules=rules,
                ts_ms=bar.ts_ms,
                activate_on_index=bar_index + 1,
                reprice_attempt=pending.attempts + 1,
            )
            update.orders_submitted.append(new_order)

            if new_order.state in {"OPEN", "PARTIAL"}:
                self._activate_pending(
                    intent=reprice_intent,
                    order_id=new_order.order_id,
                    attempts=pending.attempts + 1,
                    timeout_bar_index=bar_index + self._order_timeout_bars,
                )
            elif new_order.state == "FAILED":
                update.failed_markets.append(pending.market)
                self._clear_pending(intent_id)
            else:
                self._clear_pending(intent_id)

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
        attempts: int,
        timeout_bar_index: int,
    ) -> None:
        self._pending_by_intent[intent.intent_id] = _PendingIntent(
            intent=intent,
            order_id=order_id,
            attempts=attempts,
            timeout_bar_index=timeout_bar_index,
            market=intent.market.strip().upper(),
        )
        self._intent_by_order_id[order_id] = intent.intent_id

    def _clear_pending(self, intent_id: str) -> None:
        pending = self._pending_by_intent.pop(intent_id, None)
        if pending is not None:
            self._intent_by_order_id.pop(pending.order_id, None)


class BacktestRunEngine:
    def __init__(
        self,
        *,
        run_settings: BacktestRunSettings,
        upbit_settings: UpbitSettings | None = None,
        loader: CandleDataLoader | None = None,
        rules_provider: BacktestRulesProvider | None = None,
    ) -> None:
        self._run_settings = run_settings
        self._loader = loader or CandleDataLoader(
            parquet_root=run_settings.parquet_root,
            dataset_name=run_settings.dataset_name,
            tf=run_settings.tf,
            dense_grid=run_settings.dense_grid,
        )
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
            "intents_failed": 0,
        }

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
        candidate_generator = CandidateGeneratorV1(
            CandidateSettings(
                momentum_window_sec=max(settings.momentum_window_sec, 1),
                min_momentum_pct=float(settings.min_momentum_pct),
            )
        )
        trade_gate = TradeGateV1(
            GateSettings(
                per_trade_krw=float(settings.per_trade_krw),
                max_positions=max(int(settings.max_positions), 1),
                min_order_krw=max(float(settings.min_order_krw), 0.0),
                max_consecutive_failures=3,
                cooldown_sec_after_fail=60,
            )
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
        )

        self._runtime_counters = {
            "orders_submitted": 0,
            "orders_filled": 0,
            "orders_canceled": 0,
            "intents_failed": 0,
        }
        bars_processed = 0
        equity_curve: list[float] = []
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
                },
            )

            while queue:
                _, _, market, bar_index = heapq.heappop(queue)
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

                self._run_candidate_cycle(
                    ts_ms=bar.ts_ms,
                    current_market=market,
                    latest_bar_index_by_market=latest_bar_index_by_market,
                    universe=universe,
                    market_data=market_data,
                    candidate_generator=candidate_generator,
                    trade_gate=trade_gate,
                    exchange=exchange,
                    execution=execution,
                    event_store=store,
                    append_event=append_event,
                )

                snapshot = exchange.portfolio_snapshot(ts_ms=bar.ts_ms, latest_prices=market_data.latest_prices())
                store.append_equity(snapshot)
                append_event("PORTFOLIO_SNAPSHOT", ts_ms=bar.ts_ms, payload=asdict(snapshot))
                equity_curve.append(snapshot.equity_quote)

                next_index = bar_index + 1
                if next_index < len(market_bars[market]):
                    heapq.heappush(queue, (market_bars[market][next_index].ts_ms, queue_seq, market, next_index))
                    queue_seq += 1

            final_snapshot = exchange.portfolio_snapshot(ts_ms=max_event_ts_ms, latest_prices=market_data.latest_prices())
            store.append_equity(final_snapshot)
            append_event("PORTFOLIO_SNAPSHOT", ts_ms=max_event_ts_ms, payload=asdict(final_snapshot))
            append_event(
                "RUN_COMPLETED",
                ts_ms=max_event_ts_ms,
                payload={
                    "bars_processed": bars_processed,
                    "orders_submitted": int(self._runtime_counters["orders_submitted"]),
                    "orders_filled": int(self._runtime_counters["orders_filled"]),
                    "orders_canceled": int(self._runtime_counters["orders_canceled"]),
                    "intents_failed": int(self._runtime_counters["intents_failed"]),
                },
            )

        orders_submitted = int(self._runtime_counters["orders_submitted"])
        orders_filled = int(self._runtime_counters["orders_filled"])
        orders_canceled = int(self._runtime_counters["orders_canceled"])
        intents_failed = int(self._runtime_counters["intents_failed"])
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
            fill_rate=(orders_filled / orders_submitted) if orders_submitted > 0 else 0.0,
            realized_pnl_quote=final_snapshot.realized_pnl_quote,
            unrealized_pnl_quote=final_snapshot.unrealized_pnl_quote,
            max_drawdown_pct=max_drawdown_pct(equity_curve),
            win_rate=win_rate(realized_trade_pnls),
        )
        write_summary_json(run_root, asdict(summary))
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
        latest_bar_index_by_market: dict[str, int],
        market_data: _BarDataHub,
        trade_gate: TradeGateV1,
        exchange: BacktestSimExchange,
        execution: BacktestExecutionGateway,
        event_store: JsonlEventStore,
        append_event: Callable[[str, int, dict[str, Any] | None], None],
    ) -> bool:
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
        limit_price = round_price_to_tick(
            price=max(float(candidate.ref_price), float(ticker.trade_price)),
            tick_size=rules.tick_size,
            side=candidate.proposed_side,
        )
        volume = order_volume_from_notional(
            notional_quote=max(float(self._run_settings.per_trade_krw), 1.0),
            price=limit_price,
        )
        fee_rate = rules.fee_rate(side=candidate.proposed_side, maker_or_taker="taker")
        decision = trade_gate.evaluate(
            ts_ms=ts_ms,
            market=candidate.market,
            side=candidate.proposed_side,
            price=limit_price,
            volume=volume,
            fee_rate=fee_rate,
            exchange=exchange,
            min_total_krw=max(float(rules.min_total), float(self._run_settings.min_order_krw)),
        )
        if not decision.allowed:
            append_event(
                "TRADE_GATE_BLOCKED",
                ts_ms=ts_ms,
                payload={
                    "market": candidate.market,
                    "side": candidate.proposed_side,
                    "reason_code": decision.reason_code,
                    "detail": decision.detail,
                },
            )
            return False

        intent = new_order_intent(
            market=candidate.market,
            side=candidate.proposed_side,
            ord_type="limit",
            time_in_force="gtc",
            price=limit_price,
            volume=volume,
            reason_code="BACKTEST_CANDIDATE_V1",
            meta={
                **candidate.meta,
                "candidate_score": candidate.score,
                "tick_size": rules.tick_size,
                "min_total": rules.min_total,
            },
            ts_ms=ts_ms,
        )
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

    @staticmethod
    def _apply_execution_update(
        *,
        update: ExecutionUpdate,
        trade_gate: TradeGateV1,
        event_store: JsonlEventStore,
        append_event: Callable[[str, int, dict[str, Any] | None], None],
        ts_ms: int,
        counters: dict[str, int],
    ) -> bool:
        has_ask_fill = False
        for order in update.orders_submitted:
            event_store.append_order(order)
            append_event("ORDER_SUBMITTED", ts_ms=order.updated_ts_ms, payload=asdict(order))
            counters["orders_submitted"] += 1

        for order in update.orders_canceled:
            event_store.append_order(order)
            append_event("ORDER_CANCELED", ts_ms=order.updated_ts_ms, payload=asdict(order))
            counters["orders_canceled"] += 1

        for fill in update.fills:
            event_store.append_fill(fill)
            append_event("ORDER_FILLED", ts_ms=fill.ts_ms, payload=asdict(fill))
            counters["orders_filled"] += 1

        for order in update.orders_filled:
            event_store.append_order(order)
            if order.side == "ask":
                has_ask_fill = True

        for market in update.success_markets:
            trade_gate.record_success(market)

        for market in update.failed_markets:
            trade_gate.record_failure(market, ts_ms=ts_ms)
            counters["intents_failed"] += 1

        return has_ask_fill

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
