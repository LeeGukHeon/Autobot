from __future__ import annotations

from autobot.execution.intent import new_order_intent
from autobot.paper.sim_exchange import MarketRules, PaperSimExchange, order_volume_from_notional, round_price_to_tick


def test_order_volume_from_notional() -> None:
    volume = order_volume_from_notional(notional_quote=10_000.0, price=2_000.0)
    assert volume == 5.0


def test_round_price_to_tick_bid_and_ask() -> None:
    assert round_price_to_tick(price=1234.56, tick_size=10.0, side="bid") == 1230.0
    assert round_price_to_tick(price=1234.56, tick_size=10.0, side="ask") == 1240.0


def test_sim_exchange_rejects_order_below_min_total() -> None:
    exchange = PaperSimExchange(quote_currency="KRW", starting_cash_quote=50_000.0)
    rules = MarketRules(min_total=5_000.0, tick_size=1.0)

    intent = new_order_intent(
        market="KRW-BTC",
        side="bid",
        price=1_000.0,
        volume=1.0,
        reason_code="TEST",
    )

    order, fill = exchange.submit_limit_order(
        intent=intent,
        rules=rules,
        latest_trade_price=1_000.0,
        ts_ms=1,
    )

    assert fill is None
    assert order.state == "FAILED"
    assert order.failure_reason == "BELOW_MIN_TOTAL"


def test_sim_exchange_passive_maker_does_not_immediately_fill() -> None:
    exchange = PaperSimExchange(quote_currency="KRW", starting_cash_quote=50_000.0)
    rules = MarketRules(min_total=5_000.0, tick_size=1.0)

    intent = new_order_intent(
        market="KRW-BTC",
        side="bid",
        price=1_000.0,
        volume=5.0,
        reason_code="TEST",
        time_in_force="gtc",
        meta={"exec_profile": {"price_mode": "PASSIVE_MAKER"}},
    )

    order, fill = exchange.submit_limit_order(
        intent=intent,
        rules=rules,
        latest_trade_price=999.0,
        ts_ms=1,
    )

    assert fill is None
    assert order.state == "OPEN"


def test_sim_exchange_ioc_no_touch_releases_reserved_quote() -> None:
    exchange = PaperSimExchange(quote_currency="KRW", starting_cash_quote=50_000.0)
    rules = MarketRules(min_total=5_000.0, tick_size=1.0)

    intent = new_order_intent(
        market="KRW-BTC",
        side="bid",
        price=1_000.0,
        volume=5.0,
        reason_code="TEST",
        time_in_force="ioc",
        meta={"exec_profile": {"price_mode": "CROSS_1T"}},
    )

    order, fill = exchange.submit_limit_order(
        intent=intent,
        rules=rules,
        latest_trade_price=1_001.0,
        ts_ms=1,
    )

    snapshot = exchange.portfolio_snapshot(ts_ms=1, latest_prices={"KRW-BTC": 1_001.0})

    assert fill is None
    assert order.state == "CANCELED"
    assert order.failure_reason == "IOC_FOK_NO_TOUCH"
    assert snapshot.cash_locked == 0.0
    assert snapshot.cash_free == 50_000.0
