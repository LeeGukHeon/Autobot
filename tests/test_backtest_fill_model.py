from __future__ import annotations

from autobot.backtest.fill_model import CandleFillModel
from autobot.backtest.types import CandleBar


def _bar(*, high: float, low: float) -> CandleBar:
    return CandleBar(
        market="KRW-BTC",
        ts_ms=1_000,
        open=100.0,
        high=high,
        low=low,
        close=100.0,
        volume_base=1.0,
        volume_quote=100.0,
        volume_quote_est=False,
    )


def test_candle_fill_model_bid_uses_next_low_touch() -> None:
    model = CandleFillModel()
    assert model.should_fill(side="bid", limit_price=100.0, bar=_bar(high=105.0, low=99.0))
    assert not model.should_fill(side="bid", limit_price=100.0, bar=_bar(high=105.0, low=101.0))


def test_candle_fill_model_ask_uses_next_high_touch() -> None:
    model = CandleFillModel()
    assert model.should_fill(side="ask", limit_price=100.0, bar=_bar(high=101.0, low=95.0))
    assert not model.should_fill(side="ask", limit_price=100.0, bar=_bar(high=99.0, low=95.0))
