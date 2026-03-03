"""Backtest exchange adapter built on PaperSimExchange contracts."""

from __future__ import annotations

from autobot.execution.intent import OrderIntent
from autobot.paper.sim_exchange import FillEvent, MarketRules, PaperOrder, PaperSimExchange

from .fill_model import CandleFillModel
from .types import CandleBar


class BacktestSimExchange(PaperSimExchange):
    def __init__(
        self,
        *,
        quote_currency: str,
        starting_cash_quote: float,
        fill_model: CandleFillModel | None = None,
    ) -> None:
        super().__init__(quote_currency=quote_currency, starting_cash_quote=starting_cash_quote)
        self._candle_fill_model = fill_model or CandleFillModel()
        self._activate_on_index_by_order_id: dict[str, int] = {}

    def submit_limit_order_deferred(
        self,
        *,
        intent: OrderIntent,
        rules: MarketRules,
        ts_ms: int,
        activate_on_index: int,
        reprice_attempt: int = 0,
    ) -> tuple[PaperOrder, FillEvent | None]:
        # Ensure no same-bar immediate fill. Matching is handled only in `match_orders_on_bar`.
        non_touch_trade_price = (
            float(intent.price) * 10.0 + max(float(rules.tick_size), 1.0)
            if str(intent.side).strip().lower() == "bid"
            else 0.0
        )
        order, fill = super().submit_limit_order(
            intent=intent,
            rules=rules,
            latest_trade_price=non_touch_trade_price,
            ts_ms=ts_ms,
            reprice_attempt=reprice_attempt,
        )
        if order.state in {"OPEN", "PARTIAL"}:
            self._activate_on_index_by_order_id[order.order_id] = max(int(activate_on_index), 0)
        return (order, fill)

    def match_orders_on_bar(
        self,
        *,
        bar: CandleBar,
        bar_index: int,
        rules: MarketRules,
    ) -> list[FillEvent]:
        market_value = str(bar.market).strip().upper()
        fills: list[FillEvent] = []
        target_orders = [order for order in self._open_orders.values() if order.market == market_value]
        for order in target_orders:
            activate_on_index = self._activate_on_index_by_order_id.get(order.order_id, 0)
            if bar_index < activate_on_index:
                continue

            decision = self._candle_fill_model.decide(side=order.side, limit_price=order.price, bar=bar)
            if not decision.should_fill or decision.fill_price is None:
                continue

            fill_volume = max(order.volume_req - order.volume_filled, 0.0)
            if fill_volume <= 0:
                continue
            fill_event = self._fill_order(
                order=order,
                fill_price=decision.fill_price,
                fill_volume=fill_volume,
                maker_or_taker=decision.maker_or_taker,
                ts_ms=bar.ts_ms,
                rules=rules,
            )
            fills.append(fill_event)
            if order.state == "FILLED":
                self._activate_on_index_by_order_id.pop(order.order_id, None)
        return fills

    def cancel_order(self, order_id: str, *, ts_ms: int, reason: str | None = None) -> PaperOrder | None:
        canceled = super().cancel_order(order_id, ts_ms=ts_ms, reason=reason)
        if canceled is not None:
            self._activate_on_index_by_order_id.pop(order_id, None)
        return canceled
