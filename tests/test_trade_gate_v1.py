from __future__ import annotations

from autobot.execution.intent import new_order_intent
from autobot.paper.sim_exchange import MarketRules, PaperSimExchange
from autobot.strategy.trade_gate_v1 import GateSettings, TradeGateV1


def test_trade_gate_blocks_min_total_and_insufficient_quote() -> None:
    gate = TradeGateV1(
        GateSettings(
            per_trade_krw=10_000.0,
            max_positions=2,
            min_order_krw=5_000.0,
            max_consecutive_failures=3,
            cooldown_sec_after_fail=60,
        )
    )
    exchange = PaperSimExchange(quote_currency="KRW", starting_cash_quote=4_000.0)

    min_total_block = gate.evaluate(
        ts_ms=1,
        market="KRW-BTC",
        side="bid",
        price=1_000.0,
        volume=1.0,
        fee_rate=0.0005,
        exchange=exchange,
        min_total_krw=5_000.0,
    )
    assert not min_total_block.allowed
    assert min_total_block.reason_code == "MIN_TOTAL"

    quote_block = gate.evaluate(
        ts_ms=2,
        market="KRW-BTC",
        side="bid",
        price=10_000.0,
        volume=1.0,
        fee_rate=0.0005,
        exchange=exchange,
        min_total_krw=5_000.0,
    )
    assert not quote_block.allowed
    assert quote_block.reason_code == "INSUFFICIENT_QUOTE"


def test_trade_gate_blocks_duplicate_entry() -> None:
    gate = TradeGateV1(
        GateSettings(
            per_trade_krw=10_000.0,
            max_positions=2,
            min_order_krw=5_000.0,
            max_consecutive_failures=3,
            cooldown_sec_after_fail=60,
        )
    )
    exchange = PaperSimExchange(quote_currency="KRW", starting_cash_quote=100_000.0)
    rules = MarketRules(min_total=5_000.0, tick_size=1.0)

    first_intent = new_order_intent(
        market="KRW-BTC",
        side="bid",
        price=10_000.0,
        volume=1.0,
        reason_code="TEST",
    )
    order, fill = exchange.submit_limit_order(
        intent=first_intent,
        rules=rules,
        latest_trade_price=10_000.0,
        ts_ms=1,
    )
    assert order.state == "FILLED"
    assert fill is not None

    decision = gate.evaluate(
        ts_ms=2,
        market="KRW-BTC",
        side="bid",
        price=10_000.0,
        volume=1.0,
        fee_rate=0.0005,
        exchange=exchange,
        min_total_krw=5_000.0,
    )
    assert not decision.allowed
    assert decision.reason_code == "DUPLICATE_ENTRY"
