from __future__ import annotations

from autobot.backtest.engine import BacktestExecutionGateway
from autobot.backtest.exchange import BacktestSimExchange
from autobot.backtest.types import CandleBar
from autobot.execution.intent import new_order_intent
from autobot.paper.sim_exchange import MarketRules, round_price_to_tick


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

    submit = gateway.submit_intent(intent=intent, rules=rules, bar_index=0, ts_ms=0)
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
    gateway.submit_intent(intent=intent, rules=rules, bar_index=0, ts_ms=0)
    gateway.on_bar(bar=_bar(ts_ms=60_000, low=99.0, high=101.0), bar_index=1, rules=rules)

    quote = exchange.quote_balance()
    assert quote.free >= 0.0
    assert quote.locked >= 0.0
