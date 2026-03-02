from __future__ import annotations

from autobot.strategy.top20_scanner import TopTradeValueScanner
from autobot.upbit.ws.models import TickerEvent


def test_top_trade_value_scanner_orders_by_acc_trade_price_24h() -> None:
    scanner = TopTradeValueScanner()
    scanner.update(
        TickerEvent(
            market="KRW-BTC",
            ts_ms=1,
            trade_price=100.0,
            acc_trade_price_24h=1_000_000.0,
        )
    )
    scanner.update(
        TickerEvent(
            market="KRW-ETH",
            ts_ms=2,
            trade_price=200.0,
            acc_trade_price_24h=2_000_000.0,
        )
    )

    top = scanner.top_n(n=2, quote="KRW")
    assert [item.market for item in top] == ["KRW-ETH", "KRW-BTC"]


def test_top_trade_value_scanner_can_filter_caution() -> None:
    scanner = TopTradeValueScanner()
    scanner.update(
        TickerEvent(
            market="KRW-AAA",
            ts_ms=1,
            trade_price=100.0,
            acc_trade_price_24h=2_000_000.0,
            market_warning="CAUTION",
        )
    )
    scanner.update(
        TickerEvent(
            market="KRW-BBB",
            ts_ms=2,
            trade_price=90.0,
            acc_trade_price_24h=1_000_000.0,
            market_warning="NONE",
        )
    )

    top = scanner.top_n(n=5, quote="KRW", include_caution=False)
    assert [item.market for item in top] == ["KRW-BBB"]

