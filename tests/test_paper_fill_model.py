from __future__ import annotations

from autobot.paper.fill_model import TouchFillModel


def test_touch_fill_model_bid_and_ask_rules() -> None:
    model = TouchFillModel()

    assert model.should_fill(side="bid", limit_price=100.0, trade_price=100.0)
    assert model.should_fill(side="bid", limit_price=100.0, trade_price=99.9)
    assert not model.should_fill(side="bid", limit_price=100.0, trade_price=100.1)

    assert model.should_fill(side="ask", limit_price=100.0, trade_price=100.0)
    assert model.should_fill(side="ask", limit_price=100.0, trade_price=100.1)
    assert not model.should_fill(side="ask", limit_price=100.0, trade_price=99.9)
