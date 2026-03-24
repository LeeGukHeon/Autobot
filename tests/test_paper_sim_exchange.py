from __future__ import annotations

import pytest

from autobot.execution.intent import new_order_intent
from autobot.paper.sim_exchange import MarketRules, PaperSimExchange, order_volume_from_notional, round_price_to_tick
from autobot.strategy.micro_snapshot import MicroSnapshot


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


def test_sim_exchange_best_bid_fills_immediately_at_latest_trade_price() -> None:
    exchange = PaperSimExchange(quote_currency="KRW", starting_cash_quote=50_000.0)
    rules = MarketRules(min_total=5_000.0, tick_size=1.0)

    intent = new_order_intent(
        market="KRW-BTC",
        side="bid",
        price=10_000.0,
        volume=1.0,
        reason_code="TEST",
        ord_type="best",
        time_in_force="ioc",
    )

    order, fill = exchange.submit_order(
        intent=intent,
        rules=rules,
        latest_trade_price=1_000.0,
        ts_ms=1,
    )

    snapshot = exchange.portfolio_snapshot(ts_ms=1, latest_prices={"KRW-BTC": 1_000.0})

    assert fill is not None
    assert order.ord_type == "best"
    assert order.state == "FILLED"
    assert order.maker_or_taker == "taker"
    assert order.avg_fill_price == 1_000.0
    assert order.volume_req == 10.0
    assert snapshot.cash_locked == 0.0
    assert snapshot.cash_free < 50_000.0


def test_sim_exchange_best_bid_uses_spread_proxy_for_immediate_fill_price() -> None:
    exchange = PaperSimExchange(quote_currency="KRW", starting_cash_quote=50_000.0)
    rules = MarketRules(min_total=5_000.0, tick_size=1.0)
    snapshot = MicroSnapshot(
        market="KRW-BTC",
        snapshot_ts_ms=1,
        last_event_ts_ms=1,
        trade_events=1,
        trade_coverage_ms=1000,
        trade_notional_krw=10000.0,
        spread_bps_mean=100.0,
        depth_top5_notional_krw=1_000_000.0,
        book_events=1,
        book_coverage_ms=1000,
        book_available=True,
    )

    intent = new_order_intent(
        market="KRW-BTC",
        side="bid",
        price=10_000.0,
        volume=1.0,
        reason_code="TEST",
        ord_type="best",
        time_in_force="ioc",
    )

    order, fill = exchange.submit_order(
        intent=intent,
        rules=rules,
        latest_trade_price=1_000.0,
        micro_snapshot=snapshot,
        ts_ms=1,
    )

    assert fill is not None
    assert order.avg_fill_price == pytest.approx(1_005.0)


def test_sim_exchange_best_ioc_can_partially_fill_and_cancel_remainder() -> None:
    exchange = PaperSimExchange(quote_currency="KRW", starting_cash_quote=50_000.0)
    rules = MarketRules(min_total=5_000.0, tick_size=1.0)
    snapshot = MicroSnapshot(
        market="KRW-BTC",
        snapshot_ts_ms=1,
        last_event_ts_ms=1,
        trade_events=1,
        trade_coverage_ms=1000,
        trade_notional_krw=10000.0,
        spread_bps_mean=0.0,
        depth_top5_notional_krw=12_000.0,
        depth_bid_top5_notional_krw=50_000.0,
        depth_ask_top5_notional_krw=6_000.0,
        book_events=1,
        book_coverage_ms=1000,
        book_available=True,
    )

    intent = new_order_intent(
        market="KRW-BTC",
        side="bid",
        price=10_000.0,
        volume=1.0,
        reason_code="TEST",
        ord_type="best",
        time_in_force="ioc",
    )

    order, fill = exchange.submit_order(
        intent=intent,
        rules=rules,
        latest_trade_price=1_000.0,
        micro_snapshot=snapshot,
        ts_ms=1,
    )
    snapshot_after = exchange.portfolio_snapshot(ts_ms=1, latest_prices={"KRW-BTC": 1_000.0})

    assert fill is not None
    assert order.state == "CANCELED"
    assert order.failure_reason == "IOC_PARTIAL_CANCELLED_REMAINDER"
    assert order.volume_filled == pytest.approx(6.0)
    assert snapshot_after.cash_locked == 0.0


def test_sim_exchange_best_ioc_uses_side_specific_depth_proxy_when_available() -> None:
    exchange = PaperSimExchange(quote_currency="KRW", starting_cash_quote=50_000.0)
    rules = MarketRules(min_total=5_000.0, tick_size=1.0)
    snapshot = MicroSnapshot(
        market="KRW-BTC",
        snapshot_ts_ms=1,
        last_event_ts_ms=1,
        trade_events=1,
        trade_coverage_ms=1000,
        trade_notional_krw=10000.0,
        spread_bps_mean=0.0,
        depth_top5_notional_krw=100_000.0,
        depth_bid_top5_notional_krw=50_000.0,
        depth_ask_top5_notional_krw=8_000.0,
        book_events=1,
        book_coverage_ms=1000,
        book_available=True,
    )

    intent = new_order_intent(
        market="KRW-BTC",
        side="bid",
        price=10_000.0,
        volume=1.0,
        reason_code="TEST",
        ord_type="best",
        time_in_force="ioc",
    )

    order, fill = exchange.submit_order(
        intent=intent,
        rules=rules,
        latest_trade_price=1_000.0,
        micro_snapshot=snapshot,
        ts_ms=1,
    )

    assert fill is not None
    assert order.state == "CANCELED"
    assert order.volume_filled == pytest.approx(8.0)


def test_sim_exchange_best_fok_cancels_when_depth_is_insufficient() -> None:
    exchange = PaperSimExchange(quote_currency="KRW", starting_cash_quote=50_000.0)
    rules = MarketRules(min_total=5_000.0, tick_size=1.0)
    snapshot = MicroSnapshot(
        market="KRW-BTC",
        snapshot_ts_ms=1,
        last_event_ts_ms=1,
        trade_events=1,
        trade_coverage_ms=1000,
        trade_notional_krw=10000.0,
        spread_bps_mean=0.0,
        depth_top5_notional_krw=12_000.0,
        book_events=1,
        book_coverage_ms=1000,
        book_available=True,
    )

    intent = new_order_intent(
        market="KRW-BTC",
        side="bid",
        price=10_000.0,
        volume=1.0,
        reason_code="TEST",
        ord_type="best",
        time_in_force="fok",
    )

    order, fill = exchange.submit_order(
        intent=intent,
        rules=rules,
        latest_trade_price=1_000.0,
        micro_snapshot=snapshot,
        ts_ms=1,
    )
    snapshot_after = exchange.portfolio_snapshot(ts_ms=1, latest_prices={"KRW-BTC": 1_000.0})

    assert fill is None
    assert order.state == "CANCELED"
    assert order.failure_reason == "FOK_NOT_FULLY_FILLED"
    assert snapshot_after.cash_locked == 0.0
    assert snapshot_after.unrealized_pnl_quote == 0.0


def test_sim_exchange_limit_ioc_join_uses_marketable_executable_price_proxy() -> None:
    exchange = PaperSimExchange(quote_currency="KRW", starting_cash_quote=50_000.0)
    rules = MarketRules(min_total=5_000.0, tick_size=1.0)
    snapshot = MicroSnapshot(
        market="KRW-BTC",
        snapshot_ts_ms=1,
        last_event_ts_ms=1,
        trade_events=1,
        trade_coverage_ms=1000,
        trade_notional_krw=10000.0,
        spread_bps_mean=100.0,
        depth_top5_notional_krw=30_000.0,
        book_events=1,
        book_coverage_ms=1000,
        book_available=True,
    )

    intent = new_order_intent(
        market="KRW-BTC",
        side="bid",
        price=1_010.0,
        volume=10.0,
        reason_code="TEST",
        ord_type="limit",
        time_in_force="ioc",
        meta={"exec_profile": {"price_mode": "JOIN"}},
    )

    order, fill = exchange.submit_limit_order(
        intent=intent,
        rules=rules,
        latest_trade_price=1_000.0,
        micro_snapshot=snapshot,
        ts_ms=1,
    )

    assert fill is not None
    assert order.state == "FILLED"
    assert order.avg_fill_price == pytest.approx(1005.0)


def test_sim_exchange_partial_bid_fill_preserves_remaining_quote_lock_and_fee_basis() -> None:
    exchange = PaperSimExchange(quote_currency="KRW", starting_cash_quote=50_000.0)
    rules = MarketRules(bid_fee=0.001, ask_fee=0.001, min_total=5_000.0, tick_size=1.0)

    intent = new_order_intent(
        market="KRW-BTC",
        side="bid",
        price=1_000.0,
        volume=10.0,
        reason_code="TEST",
        time_in_force="gtc",
        meta={"exec_profile": {"price_mode": "PASSIVE_MAKER"}},
    )

    order, fill = exchange.submit_limit_order(
        intent=intent,
        rules=rules,
        latest_trade_price=1_001.0,
        ts_ms=1,
    )

    assert fill is None
    assert order.state == "OPEN"

    current = exchange._orders[order.order_id]  # noqa: SLF001
    assert current is not None
    partial_fill = exchange._fill_order(  # noqa: SLF001
        order=current,
        fill_price=1_000.0,
        fill_volume=4.0,
        maker_or_taker="maker",
        ts_ms=2,
        rules=rules,
    )
    snapshot = exchange.portfolio_snapshot(ts_ms=2, latest_prices={"KRW-BTC": 1_000.0})
    latest = exchange.get_order(order.order_id)

    assert partial_fill is not None
    assert latest is not None
    assert latest.state == "PARTIAL"
    assert latest.volume_filled == 4.0
    assert latest.locked_quote > 0.0
    assert snapshot.cash_locked == latest.locked_quote
    assert snapshot.positions[0].avg_entry_price == 1001.0


def test_sim_exchange_partial_ask_fill_preserves_remaining_base_lock() -> None:
    exchange = PaperSimExchange(quote_currency="KRW", starting_cash_quote=50_000.0)
    rules = MarketRules(bid_fee=0.001, ask_fee=0.001, min_total=5_000.0, tick_size=1.0)

    bid_intent = new_order_intent(
        market="KRW-BTC",
        side="bid",
        price=10_000.0,
        volume=1.0,
        reason_code="TEST",
        ord_type="best",
        time_in_force="ioc",
    )
    order, fill = exchange.submit_order(
        intent=bid_intent,
        rules=rules,
        latest_trade_price=1_000.0,
        ts_ms=1,
    )
    assert fill is not None
    assert order.state == "FILLED"

    ask_intent = new_order_intent(
        market="KRW-BTC",
        side="ask",
        price=1_050.0,
        volume=6.0,
        reason_code="TEST",
        time_in_force="gtc",
        meta={"exec_profile": {"price_mode": "PASSIVE_MAKER"}},
    )
    ask_order, ask_fill = exchange.submit_limit_order(
        intent=ask_intent,
        rules=rules,
        latest_trade_price=1_000.0,
        ts_ms=2,
    )

    assert ask_fill is None
    current = exchange._orders[ask_order.order_id]  # noqa: SLF001
    assert current is not None
    partial_fill = exchange._fill_order(  # noqa: SLF001
        order=current,
        fill_price=1_050.0,
        fill_volume=4.0,
        maker_or_taker="maker",
        ts_ms=3,
        rules=rules,
    )
    latest = exchange.get_order(ask_order.order_id)
    balance = exchange.coin_balance("BTC")

    assert partial_fill is not None
    assert latest is not None
    assert latest.state == "PARTIAL"
    assert latest.locked_base == 2.0
    assert balance.locked == 2.0


def test_sim_exchange_flat_round_trip_reconciles_realized_pnl_with_equity() -> None:
    exchange = PaperSimExchange(quote_currency="KRW", starting_cash_quote=10_020.0)
    rules = MarketRules(bid_fee=0.001, ask_fee=0.001, min_total=5_000.0, tick_size=1.0)

    bid_intent = new_order_intent(
        market="KRW-BTC",
        side="bid",
        price=10_000.0,
        volume=1.0,
        reason_code="TEST",
        ord_type="best",
        time_in_force="ioc",
    )
    bid_order, bid_fill = exchange.submit_order(
        intent=bid_intent,
        rules=rules,
        latest_trade_price=1_000.0,
        ts_ms=1,
    )
    assert bid_fill is not None
    assert bid_order.state == "FILLED"

    ask_intent = new_order_intent(
        market="KRW-BTC",
        side="ask",
        price=1_000.0,
        volume=10.0,
        reason_code="TEST",
        ord_type="best",
        time_in_force="ioc",
    )
    ask_order, ask_fill = exchange.submit_order(
        intent=ask_intent,
        rules=rules,
        latest_trade_price=1_000.0,
        ts_ms=2,
    )
    snapshot = exchange.portfolio_snapshot(ts_ms=2, latest_prices={"KRW-BTC": 1_000.0})

    assert ask_fill is not None
    assert ask_order.state == "FILLED"
    assert snapshot.cash_locked == 0.0
    assert snapshot.unrealized_pnl_quote == 0.0
    assert snapshot.realized_pnl_quote == pytest.approx(snapshot.equity_quote - 10_020.0)
