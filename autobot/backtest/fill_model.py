"""Candle-based fill model for backtest runtime."""

from __future__ import annotations

from dataclasses import dataclass

from .types import CandleBar


@dataclass(frozen=True)
class FillDecision:
    should_fill: bool
    fill_price: float | None
    maker_or_taker: str


class CandleFillModel:
    """Fill a resting limit order if the next bar touches the limit."""

    def should_fill(self, *, side: str, limit_price: float, bar: CandleBar) -> bool:
        side_value = side.strip().lower()
        if side_value == "bid":
            return float(bar.low) <= float(limit_price)
        if side_value == "ask":
            return float(bar.high) >= float(limit_price)
        raise ValueError(f"unsupported side: {side}")

    def decide(self, *, side: str, limit_price: float, bar: CandleBar) -> FillDecision:
        can_fill = self.should_fill(side=side, limit_price=limit_price, bar=bar)
        if not can_fill:
            return FillDecision(should_fill=False, fill_price=None, maker_or_taker="unknown")
        return FillDecision(
            should_fill=True,
            fill_price=float(limit_price),
            maker_or_taker="maker",
        )
