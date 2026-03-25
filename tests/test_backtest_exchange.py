from __future__ import annotations

from autobot.backtest.engine import BacktestExecutionGateway
from autobot.backtest.exchange import BacktestSimExchange
from autobot.backtest.types import CandleBar
from autobot.execution.intent import new_order_intent
from autobot.paper.sim_exchange import MarketRules, PaperOrder, round_price_to_tick
from autobot.strategy.micro_snapshot import MicroSnapshot


def _bar(*, ts_ms: int, low: float, high: float, close: float = 100.0) -> CandleBar:
    return CandleBar(
        market="KRW-BTC",
        ts_ms=ts_ms,
        open=close,
        high=high,
        low=low,
        close=close,
        volume_base=1.0,
        volume_quote=close,
        volume_quote_est=False,
    )


def test_lookahead_guard_matches_from_next_bar_only() -> None:
    exchange = BacktestSimExchange(quote_currency="KRW", starting_cash_quote=50_000.0)
    gateway = BacktestExecutionGateway(
        exchange=exchange,
        order_timeout_bars=5,
        reprice_max_attempts=0,
        reprice_tick_steps=1,
    )
    rules = MarketRules(min_total=5_000.0, tick_size=1.0)
    intent = new_order_intent(
        market="KRW-BTC",
        side="bid",
        price=100.0,
        volume=100.0,
        reason_code="TEST",
        ts_ms=0,
    )

    submit = gateway.submit_intent(intent=intent, rules=rules, latest_trade_price=100.0, bar_index=0, ts_ms=0)
    assert len(submit.orders_submitted) == 1

    same_bar = gateway.on_bar(bar=_bar(ts_ms=0, low=90.0, high=110.0), bar_index=0, rules=rules)
    assert len(same_bar.fills) == 0

    next_bar = gateway.on_bar(bar=_bar(ts_ms=60_000, low=99.0, high=105.0), bar_index=1, rules=rules)
    assert len(next_bar.fills) == 1


def test_tick_rounding_and_fee_never_make_quote_negative() -> None:
    assert round_price_to_tick(price=1234.56, tick_size=10.0, side="bid") == 1230.0
    assert round_price_to_tick(price=1234.56, tick_size=10.0, side="ask") == 1240.0

    exchange = BacktestSimExchange(quote_currency="KRW", starting_cash_quote=10_000.0)
    gateway = BacktestExecutionGateway(
        exchange=exchange,
        order_timeout_bars=5,
        reprice_max_attempts=0,
        reprice_tick_steps=1,
    )
    rules = MarketRules(bid_fee=0.001, ask_fee=0.001, min_total=5_000.0, tick_size=1.0)
    intent = new_order_intent(
        market="KRW-BTC",
        side="bid",
        price=100.0,
        volume=50.0,
        reason_code="FEE_TEST",
        ts_ms=0,
    )
    gateway.submit_intent(intent=intent, rules=rules, latest_trade_price=100.0, bar_index=0, ts_ms=0)
    gateway.on_bar(bar=_bar(ts_ms=60_000, low=99.0, high=101.0), bar_index=1, rules=rules)

    quote = exchange.quote_balance()
    assert quote.free >= 0.0
    assert quote.locked >= 0.0


def test_resting_queue_requires_visible_queue_to_clear_before_fill() -> None:
    exchange = BacktestSimExchange(quote_currency="KRW", starting_cash_quote=50_000.0)
    gateway = BacktestExecutionGateway(
        exchange=exchange,
        order_timeout_bars=5,
        reprice_max_attempts=0,
        reprice_tick_steps=1,
    )
    rules = MarketRules(min_total=5_000.0, tick_size=1.0)
    intent = new_order_intent(
        market="KRW-BTC",
        side="bid",
        price=100.0,
        volume=100.0,
        reason_code="QUEUE_TEST",
        ts_ms=0,
    )
    submit_snapshot = MicroSnapshot(
        market="KRW-BTC",
        snapshot_ts_ms=0,
        last_event_ts_ms=0,
        bid_levels=((100.0, 5.0),),
        ask_levels=((101.0, 5.0),),
        best_bid_price=100.0,
        best_ask_price=101.0,
        book_events=1,
        book_available=True,
    )
    submit = gateway.submit_intent(intent=intent, rules=rules, latest_trade_price=100.0, bar_index=0, ts_ms=0)
    order = submit.orders_submitted[0]
    exchange._initialize_resting_queue_state(order=order, micro_snapshot=submit_snapshot)  # noqa: SLF001

    blocked = exchange.match_orders_on_bar(
        bar=_bar(ts_ms=60_000, low=100.0, high=101.0),
        bar_index=1,
        rules=rules,
        micro_snapshot=MicroSnapshot(
            market="KRW-BTC",
            snapshot_ts_ms=60_000,
            last_event_ts_ms=60_000,
            bid_levels=((100.0, 2.0),),
            ask_levels=((101.0, 5.0),),
            best_bid_price=100.0,
            best_ask_price=101.0,
            book_events=1,
            book_available=True,
        ),
    )
    assert blocked == []

    filled = exchange.match_orders_on_bar(
        bar=_bar(ts_ms=120_000, low=99.0, high=101.0),
        bar_index=2,
        rules=rules,
        micro_snapshot=MicroSnapshot(
            market="KRW-BTC",
            snapshot_ts_ms=120_000,
            last_event_ts_ms=120_000,
            bid_levels=(),
            ask_levels=((101.0, 5.0),),
            best_bid_price=99.0,
            best_ask_price=100.0,
            book_events=1,
            book_available=True,
        ),
    )
    assert len(filled) == 1


def test_resting_queue_trade_at_level_consumes_queue_ahead_before_fill() -> None:
    exchange = BacktestSimExchange(quote_currency="KRW", starting_cash_quote=50_000.0)
    gateway = BacktestExecutionGateway(
        exchange=exchange,
        order_timeout_bars=5,
        reprice_max_attempts=0,
        reprice_tick_steps=1,
    )
    rules = MarketRules(min_total=5_000.0, tick_size=1.0)
    intent = new_order_intent(
        market="KRW-BTC",
        side="bid",
        price=100.0,
        volume=100.0,
        reason_code="QUEUE_TRADE_TEST",
        ts_ms=0,
    )

    submit = gateway.submit_intent(intent=intent, rules=rules, latest_trade_price=100.0, bar_index=0, ts_ms=0)
    order = submit.orders_submitted[0]
    exchange._initialize_resting_queue_state(  # noqa: SLF001
        order=order,
        micro_snapshot=MicroSnapshot(
            market="KRW-BTC",
            snapshot_ts_ms=0,
            last_event_ts_ms=0,
            bid_levels=((100.0, 5.0),),
            ask_levels=((101.0, 5.0),),
            recent_trade_ticks=(),
            best_bid_price=100.0,
            best_ask_price=101.0,
            book_events=1,
            book_available=True,
        ),
    )

    blocked = exchange.match_orders_on_bar(
        bar=_bar(ts_ms=60_000, low=100.0, high=101.0),
        bar_index=1,
        rules=rules,
        micro_snapshot=MicroSnapshot(
            market="KRW-BTC",
            snapshot_ts_ms=60_000,
            last_event_ts_ms=60_000,
            bid_levels=((100.0, 5.0),),
            ask_levels=((101.0, 5.0),),
            recent_trade_ticks=((60_000, 100.0, 3.0, "sell"),),
            best_bid_price=100.0,
            best_ask_price=101.0,
            book_events=1,
            book_available=True,
        ),
    )
    assert blocked == []

    filled = exchange.match_orders_on_bar(
        bar=_bar(ts_ms=120_000, low=100.0, high=101.0),
        bar_index=2,
        rules=rules,
        micro_snapshot=MicroSnapshot(
            market="KRW-BTC",
            snapshot_ts_ms=120_000,
            last_event_ts_ms=120_000,
            bid_levels=((100.0, 5.0),),
            ask_levels=((101.0, 5.0),),
            recent_trade_ticks=((120_000, 100.0, 2.0, "sell"),),
            best_bid_price=100.0,
            best_ask_price=101.0,
            book_events=1,
            book_available=True,
        ),
    )
    assert len(filled) == 1


def test_resting_queue_same_level_size_expansion_is_treated_as_behind_us_arrival() -> None:
    exchange = BacktestSimExchange(quote_currency="KRW", starting_cash_quote=50_000.0)
    gateway = BacktestExecutionGateway(
        exchange=exchange,
        order_timeout_bars=5,
        reprice_max_attempts=0,
        reprice_tick_steps=1,
    )
    rules = MarketRules(min_total=5_000.0, tick_size=1.0)
    intent = new_order_intent(
        market="KRW-BTC",
        side="bid",
        price=100.0,
        volume=100.0,
        reason_code="QUEUE_ARRIVAL_TEST",
        ts_ms=0,
    )

    submit = gateway.submit_intent(intent=intent, rules=rules, latest_trade_price=100.0, bar_index=0, ts_ms=0)
    order = submit.orders_submitted[0]
    exchange._initialize_resting_queue_state(  # noqa: SLF001
        order=order,
        micro_snapshot=MicroSnapshot(
            market="KRW-BTC",
            snapshot_ts_ms=0,
            last_event_ts_ms=0,
            bid_levels=((100.0, 5.0),),
            ask_levels=((101.0, 5.0),),
            recent_trade_ticks=(),
            best_bid_price=100.0,
            best_ask_price=101.0,
            book_events=1,
            book_available=True,
        ),
    )

    blocked = exchange.match_orders_on_bar(
        bar=_bar(ts_ms=60_000, low=100.0, high=101.0),
        bar_index=1,
        rules=rules,
        micro_snapshot=MicroSnapshot(
            market="KRW-BTC",
            snapshot_ts_ms=60_000,
            last_event_ts_ms=60_000,
            bid_levels=((100.0, 7.0),),
            ask_levels=((101.0, 5.0),),
            recent_trade_ticks=(),
            best_bid_price=100.0,
            best_ask_price=101.0,
            book_events=1,
            book_available=True,
        ),
    )
    assert blocked == []

    filled = exchange.match_orders_on_bar(
        bar=_bar(ts_ms=120_000, low=99.0, high=101.0),
        bar_index=2,
        rules=rules,
        micro_snapshot=MicroSnapshot(
            market="KRW-BTC",
            snapshot_ts_ms=120_000,
            last_event_ts_ms=120_000,
            bid_levels=((100.0, 0.0),),
            ask_levels=((101.0, 5.0),),
            recent_trade_ticks=(),
            best_bid_price=99.0,
            best_ask_price=100.0,
            book_events=1,
            book_available=True,
        ),
    )
    assert len(filled) == 1


def test_resting_queue_uses_orderbook_event_sequence_within_snapshot_window() -> None:
    exchange = BacktestSimExchange(quote_currency="KRW", starting_cash_quote=50_000.0)
    gateway = BacktestExecutionGateway(
        exchange=exchange,
        order_timeout_bars=5,
        reprice_max_attempts=0,
        reprice_tick_steps=1,
    )
    rules = MarketRules(min_total=5_000.0, tick_size=1.0)
    intent = new_order_intent(
        market="KRW-BTC",
        side="bid",
        price=100.0,
        volume=100.0,
        reason_code="QUEUE_BOOK_EVENT_TEST",
        ts_ms=0,
    )

    submit = gateway.submit_intent(intent=intent, rules=rules, latest_trade_price=100.0, bar_index=0, ts_ms=0)
    order = submit.orders_submitted[0]
    exchange._initialize_resting_queue_state(  # noqa: SLF001
        order=order,
        micro_snapshot=MicroSnapshot(
            market="KRW-BTC",
            snapshot_ts_ms=0,
            last_event_ts_ms=0,
            bid_levels=((100.0, 5.0),),
            ask_levels=((101.0, 5.0),),
            recent_orderbook_events=(),
            best_bid_price=100.0,
            best_ask_price=101.0,
            book_events=1,
            book_available=True,
        ),
    )

    blocked = exchange.match_orders_on_bar(
        bar=_bar(ts_ms=60_000, low=100.0, high=101.0),
        bar_index=1,
        rules=rules,
        micro_snapshot=MicroSnapshot(
            market="KRW-BTC",
            snapshot_ts_ms=60_000,
            last_event_ts_ms=60_000,
            bid_levels=((100.0, 2.0),),
            ask_levels=((101.0, 5.0),),
            recent_orderbook_events=(
                (50_000, ((100.0, 4.0),), ((101.0, 5.0),), 100.0, 101.0),
                (60_000, ((100.0, 2.0),), ((101.0, 5.0),), 100.0, 101.0),
            ),
            best_bid_price=100.0,
            best_ask_price=101.0,
            book_events=2,
            book_available=True,
        ),
    )
    assert blocked == []

    filled = exchange.match_orders_on_bar(
        bar=_bar(ts_ms=120_000, low=99.0, high=101.0),
        bar_index=2,
        rules=rules,
        micro_snapshot=MicroSnapshot(
            market="KRW-BTC",
            snapshot_ts_ms=120_000,
            last_event_ts_ms=120_000,
            bid_levels=(),
            ask_levels=((101.0, 5.0),),
            recent_orderbook_events=((120_000, (), ((101.0, 5.0),), 99.0, 100.0),),
            best_bid_price=99.0,
            best_ask_price=100.0,
            book_events=1,
            book_available=True,
        ),
    )
    assert len(filled) == 1


class _ReplaceOnlyExchange:
    def __init__(self) -> None:
        self.orders: dict[str, PaperOrder] = {}
        self.calls: list[dict[str, object]] = []

    def submit_order_deferred(
        self,
        *,
        intent,
        rules,
        ts_ms,
        activate_on_index,
        latest_trade_price=None,
        micro_snapshot=None,
        reprice_attempt=0,
    ):
        _ = (rules, activate_on_index, micro_snapshot)
        order_id = f"order-{len(self.calls) + 1}"
        order = PaperOrder(
            order_id=order_id,
            intent_id=intent.intent_id,
            state="OPEN",
            created_ts_ms=int(ts_ms),
            updated_ts_ms=int(ts_ms),
            market=intent.market,
            side=intent.side,
            ord_type=intent.ord_type,
            time_in_force=intent.time_in_force,
            price=float(intent.price),
            volume_req=float(intent.volume),
            volume_filled=0.0,
            avg_fill_price=0.0,
            fee_paid_quote=0.0,
            maker_or_taker="maker",
            reprice_attempt=int(reprice_attempt),
        )
        self.orders[order_id] = order
        self.calls.append(
            {
                "order_id": order_id,
                "latest_trade_price": latest_trade_price,
                "reprice_attempt": int(reprice_attempt),
            }
        )
        return order, None

    def get_order(self, order_id: str):
        return self.orders.get(order_id)

    def match_orders_on_bar(self, *, bar, bar_index, rules, micro_snapshot=None):
        _ = (bar, bar_index, rules, micro_snapshot)
        return []

    def cancel_order(self, order_id: str, *, ts_ms: int, reason: str | None = None):
        order = self.orders.get(order_id)
        if order is None:
            return None
        order.state = "CANCELED"
        order.updated_ts_ms = int(ts_ms)
        order.failure_reason = reason
        return order


def test_backtest_gateway_reprice_path_uses_submit_order_deferred() -> None:
    exchange = _ReplaceOnlyExchange()
    gateway = BacktestExecutionGateway(
        exchange=exchange,  # type: ignore[arg-type]
        order_timeout_bars=1,
        reprice_max_attempts=1,
        reprice_tick_steps=1,
    )
    rules = MarketRules(min_total=5_000.0, tick_size=1.0)
    intent = new_order_intent(
        market="KRW-BTC",
        side="bid",
        price=100.0,
        volume=100.0,
        reason_code="REPRICE_TEST",
        ts_ms=0,
    )

    submit = gateway.submit_intent(intent=intent, rules=rules, latest_trade_price=100.0, bar_index=0, ts_ms=0)
    assert len(submit.orders_submitted) == 1

    update = gateway.on_bar(bar=_bar(ts_ms=60_000, low=99.0, high=101.0, close=100.0), bar_index=1, rules=rules)

    assert len(update.orders_submitted) == 1
    assert update.orders_submitted[0].reprice_attempt == 1
    assert exchange.calls[-1]["reprice_attempt"] == 1
    assert exchange.calls[-1]["latest_trade_price"] == 100.0
