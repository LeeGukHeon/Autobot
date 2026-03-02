"""Simplified ticker-touch fill model for paper execution."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FillDecision:
    should_fill: bool
    maker_or_taker: str


class TouchFillModel:
    """Fill when trade price touches the submitted limit price."""

    def should_fill(self, *, side: str, limit_price: float, trade_price: float) -> bool:
        side_value = side.lower()
        if side_value == "bid":
            return trade_price <= limit_price
        if side_value == "ask":
            return trade_price >= limit_price
        raise ValueError(f"unsupported side: {side}")

    def decide(
        self,
        *,
        side: str,
        limit_price: float,
        trade_price: float,
        immediate: bool,
    ) -> FillDecision:
        can_fill = self.should_fill(side=side, limit_price=limit_price, trade_price=trade_price)
        if not can_fill:
            return FillDecision(should_fill=False, maker_or_taker="unknown")
        return FillDecision(
            should_fill=True,
            maker_or_taker="taker" if immediate else "maker",
        )