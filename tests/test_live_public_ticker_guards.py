from __future__ import annotations

from types import SimpleNamespace

from autobot.paper.engine import MarketDataHub
from autobot.strategy.candidates_v1 import CandidateGeneratorV1, CandidateSettings
from autobot.strategy.top20_scanner import TopTradeValueScanner
from autobot.upbit.ws.models import TickerEvent


class _MarketData:
    def __init__(self, latest: object | None) -> None:
        self._latest = latest

    def get_latest_ticker(self, market: str) -> object | None:
        _ = market
        return self._latest

    def get_momentum_pct(self, market: str, *, window_sec: int) -> float | None:
        _ = market, window_sec
        return 1.0

    def get_acc_trade_delta(self, market: str, *, window_sec: int) -> float | None:
        _ = market, window_sec
        return 1000.0


def test_candidate_generator_skips_ticker_with_none_prices() -> None:
    latest = SimpleNamespace(trade_price=None, acc_trade_price_24h=None)
    generator = CandidateGeneratorV1(CandidateSettings(momentum_window_sec=60, min_momentum_pct=0.2))

    out = generator.generate(markets=["KRW-BTC"], market_data=_MarketData(latest))

    assert out == []


def test_top_trade_value_scanner_ignores_malformed_ticker() -> None:
    scanner = TopTradeValueScanner()
    scanner.update(
        TickerEvent(
            market="KRW-BTC",
            ts_ms=1,
            trade_price=None,  # type: ignore[arg-type]
            acc_trade_price_24h=None,  # type: ignore[arg-type]
        )
    )
    assert scanner.size() == 0


def test_market_data_hub_ignores_malformed_ticker() -> None:
    hub = MarketDataHub()
    hub.update(
        TickerEvent(
            market="KRW-BTC",
            ts_ms=1,
            trade_price=None,  # type: ignore[arg-type]
            acc_trade_price_24h=None,  # type: ignore[arg-type]
        )
    )
    assert hub.get_latest_ticker("KRW-BTC") is None
