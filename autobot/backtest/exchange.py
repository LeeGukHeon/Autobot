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
        self._resting_queue_state_by_order_id: dict[str, dict[str, float | int | str]] = {}

    def submit_order_deferred(
        self,
        *,
        intent: OrderIntent,
        rules: MarketRules,
        ts_ms: int,
        activate_on_index: int,
        latest_trade_price: float | None = None,
        micro_snapshot: object | None = None,
        reprice_attempt: int = 0,
    ) -> tuple[PaperOrder, FillEvent | None]:
        if str(intent.time_in_force).strip().lower() in {"ioc", "fok"} or str(intent.ord_type).strip().lower() == "best":
            effective_trade_price = (
                float(latest_trade_price)
                if latest_trade_price is not None
                else float(intent.price if intent.price is not None else 0.0)
            )
            return super().submit_order(
                intent=intent,
                rules=rules,
                latest_trade_price=effective_trade_price,
                micro_snapshot=micro_snapshot,
                ts_ms=ts_ms,
                reprice_attempt=reprice_attempt,
            )
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
            self._initialize_resting_queue_state(order=order, micro_snapshot=micro_snapshot)
        return (order, fill)

    def match_orders_on_bar(
        self,
        *,
        bar: CandleBar,
        bar_index: int,
        rules: MarketRules,
        micro_snapshot: object | None = None,
    ) -> list[FillEvent]:
        market_value = str(bar.market).strip().upper()
        fills: list[FillEvent] = []
        target_orders = [order for order in self._open_orders.values() if order.market == market_value]
        for order in target_orders:
            activate_on_index = self._activate_on_index_by_order_id.get(order.order_id, 0)
            if bar_index < activate_on_index:
                continue

            queue_ready = self._update_resting_queue_state(order=order, micro_snapshot=micro_snapshot, bar=bar)
            decision = self._candle_fill_model.decide(side=order.side, limit_price=order.price, bar=bar)
            if not decision.should_fill or decision.fill_price is None:
                continue
            if not queue_ready:
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
                self._resting_queue_state_by_order_id.pop(order.order_id, None)
        return fills

    def cancel_order(self, order_id: str, *, ts_ms: int, reason: str | None = None) -> PaperOrder | None:
        canceled = super().cancel_order(order_id, ts_ms=ts_ms, reason=reason)
        if canceled is not None:
            self._activate_on_index_by_order_id.pop(order_id, None)
            self._resting_queue_state_by_order_id.pop(order_id, None)
        return canceled

    def _initialize_resting_queue_state(self, *, order: PaperOrder, micro_snapshot: object | None) -> None:
        visible_size = _visible_size_at_order_price(
            micro_snapshot=micro_snapshot,
            side=str(order.side).strip().lower(),
            price=float(order.price),
        )
        if visible_size is None:
            return
        snapshot_ts_ms = _snapshot_ts_ms(micro_snapshot)
        self._resting_queue_state_by_order_id[order.order_id] = {
            "queue_ahead_volume": float(max(visible_size, 0.0)),
            "queue_behind_volume": 0.0,
            "last_visible_size": float(max(visible_size, 0.0)),
            "queue_price": float(order.price),
            "snapshot_ts_ms": int(snapshot_ts_ms) if snapshot_ts_ms is not None else int(order.created_ts_ms),
            "last_processed_book_ts_ms": int(snapshot_ts_ms) if snapshot_ts_ms is not None else int(order.created_ts_ms),
            "last_processed_trade_ts_ms": int(snapshot_ts_ms) if snapshot_ts_ms is not None else int(order.created_ts_ms),
            "side": str(order.side).strip().lower(),
        }

    def _update_resting_queue_state(
        self,
        *,
        order: PaperOrder,
        micro_snapshot: object | None,
        bar: CandleBar,
    ) -> bool:
        state = self._resting_queue_state_by_order_id.get(order.order_id)
        if state is None:
            return True

        current_visible_size = _visible_size_at_order_price(
            micro_snapshot=micro_snapshot,
            side=str(order.side).strip().lower(),
            price=float(order.price),
        )
        queue_ahead_volume = float(state.get("queue_ahead_volume", 0.0) or 0.0)
        queue_behind_volume = float(state.get("queue_behind_volume", 0.0) or 0.0)
        last_visible_size = state.get("last_visible_size")
        processed_book_ts_ms = int(state.get("last_processed_book_ts_ms", 0) or 0)
        orderbook_events = getattr(micro_snapshot, "recent_orderbook_events", ())
        if isinstance(orderbook_events, tuple) and orderbook_events:
            (
                queue_ahead_volume,
                queue_behind_volume,
                last_visible_size,
                processed_book_ts_ms,
            ) = _apply_orderbook_events_to_queue_state(
                order=order,
                orderbook_events=orderbook_events,
                queue_ahead_volume=queue_ahead_volume,
                queue_behind_volume=queue_behind_volume,
                last_visible_size=(float(last_visible_size) if last_visible_size is not None else current_visible_size),
                last_processed_book_ts_ms=processed_book_ts_ms,
            )
        elif current_visible_size is not None and last_visible_size is not None:
            delta_visible = float(current_visible_size) - float(last_visible_size)
            if delta_visible < 0.0:
                removal = abs(float(delta_visible))
                ahead_reduction = min(queue_ahead_volume, removal)
                queue_ahead_volume = max(queue_ahead_volume - ahead_reduction, 0.0)
                removal -= ahead_reduction
                if removal > 0.0:
                    queue_behind_volume = max(queue_behind_volume - removal, 0.0)
            elif delta_visible > 0.0:
                queue_behind_volume += float(delta_visible)
        elif current_visible_size is not None:
            queue_ahead_volume = min(queue_ahead_volume, float(current_visible_size))

        queue_ahead_volume = _consume_queue_from_trade_ticks(
            order=order,
            micro_snapshot=micro_snapshot,
            queue_ahead_volume=queue_ahead_volume,
            last_processed_trade_ts_ms=int(state.get("last_processed_trade_ts_ms", 0) or 0),
        )

        if current_visible_size is not None and not (isinstance(orderbook_events, tuple) and orderbook_events):
            state["last_visible_size"] = float(current_visible_size)
        elif last_visible_size is not None:
            state["last_visible_size"] = float(last_visible_size)

        if current_visible_size is None and _price_through_order(order=order, bar=bar, micro_snapshot=micro_snapshot):
            queue_ahead_volume = 0.0

        state["queue_ahead_volume"] = float(max(queue_ahead_volume, 0.0))
        state["queue_behind_volume"] = float(max(queue_behind_volume, 0.0))
        snapshot_ts_ms = _snapshot_ts_ms(micro_snapshot)
        if snapshot_ts_ms is not None:
            state["snapshot_ts_ms"] = int(snapshot_ts_ms)
            state["last_processed_trade_ts_ms"] = int(snapshot_ts_ms)
        state["last_processed_book_ts_ms"] = int(processed_book_ts_ms)
        self._resting_queue_state_by_order_id[order.order_id] = state
        return float(state.get("queue_ahead_volume", 0.0) or 0.0) <= 0.0


def _snapshot_ts_ms(micro_snapshot: object | None) -> int | None:
    try:
        value = getattr(micro_snapshot, "snapshot_ts_ms", None)
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _visible_size_at_order_price(*, micro_snapshot: object | None, side: str, price: float) -> float | None:
    side_value = str(side).strip().lower()
    levels = getattr(micro_snapshot, "bid_levels" if side_value == "bid" else "ask_levels", ())
    if not isinstance(levels, tuple):
        return None
    for raw in levels:
        if not isinstance(raw, tuple) or len(raw) != 2:
            continue
        try:
            level_price = float(raw[0])
            level_size = float(raw[1])
        except (TypeError, ValueError):
            continue
        if abs(level_price - float(price)) <= 1e-12:
            return max(level_size, 0.0)
    return None


def _price_through_order(*, order: PaperOrder, bar: CandleBar, micro_snapshot: object | None) -> bool:
    side_value = str(order.side).strip().lower()
    if side_value == "bid":
        best_ask = getattr(micro_snapshot, "best_ask_price", None)
        try:
            if best_ask is not None and float(best_ask) <= float(order.price) + 1e-12:
                return True
        except (TypeError, ValueError):
            pass
        return float(bar.low) < float(order.price) - 1e-12
    best_bid = getattr(micro_snapshot, "best_bid_price", None)
    try:
        if best_bid is not None and float(best_bid) >= float(order.price) - 1e-12:
            return True
    except (TypeError, ValueError):
        pass
    return float(bar.high) > float(order.price) + 1e-12


def _consume_queue_from_trade_ticks(
    *,
    order: PaperOrder,
    micro_snapshot: object | None,
    queue_ahead_volume: float,
    last_processed_trade_ts_ms: int,
) -> float:
    ticks = getattr(micro_snapshot, "recent_trade_ticks", ())
    if not isinstance(ticks, tuple):
        return float(max(queue_ahead_volume, 0.0))
    remaining_queue = float(max(queue_ahead_volume, 0.0))
    side_value = str(order.side).strip().lower()
    for raw in ticks:
        if not isinstance(raw, tuple) or len(raw) != 4:
            continue
        try:
            ts_ms = int(raw[0])
            price = float(raw[1])
            volume = float(raw[2])
            trade_side = str(raw[3]).strip().lower()
        except (TypeError, ValueError):
            continue
        if ts_ms <= int(last_processed_trade_ts_ms):
            continue
        if side_value == "bid":
            if price < float(order.price) - 1e-12 and trade_side == "sell":
                return 0.0
            if abs(price - float(order.price)) <= 1e-12 and trade_side == "sell":
                remaining_queue = max(remaining_queue - float(volume), 0.0)
        else:
            if price > float(order.price) + 1e-12 and trade_side == "buy":
                return 0.0
            if abs(price - float(order.price)) <= 1e-12 and trade_side == "buy":
                remaining_queue = max(remaining_queue - float(volume), 0.0)
    return remaining_queue


def _apply_orderbook_events_to_queue_state(
    *,
    order: PaperOrder,
    orderbook_events: tuple[tuple[int, tuple[tuple[float, float], ...], tuple[tuple[float, float], ...], float | None, float | None], ...],
    queue_ahead_volume: float,
    queue_behind_volume: float,
    last_visible_size: float | None,
    last_processed_book_ts_ms: int,
) -> tuple[float, float, float | None, int]:
    side_value = str(order.side).strip().lower()
    visible_value = last_visible_size
    processed_ts = int(last_processed_book_ts_ms)
    for raw in orderbook_events:
        if not isinstance(raw, tuple) or len(raw) != 5:
            continue
        try:
            event_ts_ms = int(raw[0])
        except (TypeError, ValueError):
            continue
        if event_ts_ms <= processed_ts:
            continue
        levels = raw[1] if side_value == "bid" else raw[2]
        current_visible = _visible_size_at_price_from_levels(levels=levels, price=float(order.price))
        if current_visible is not None and visible_value is not None:
            delta_visible = float(current_visible) - float(visible_value)
            if delta_visible < 0.0:
                removal = abs(float(delta_visible))
                ahead_reduction = min(queue_ahead_volume, removal)
                queue_ahead_volume = max(queue_ahead_volume - ahead_reduction, 0.0)
                removal -= ahead_reduction
                if removal > 0.0:
                    queue_behind_volume = max(queue_behind_volume - removal, 0.0)
            elif delta_visible > 0.0:
                queue_behind_volume += float(delta_visible)
        elif current_visible is not None:
            queue_ahead_volume = min(queue_ahead_volume, float(current_visible))
        visible_value = current_visible
        processed_ts = int(event_ts_ms)
    return (
        float(max(queue_ahead_volume, 0.0)),
        float(max(queue_behind_volume, 0.0)),
        (float(visible_value) if visible_value is not None else None),
        int(processed_ts),
    )


def _visible_size_at_price_from_levels(*, levels: object, price: float) -> float | None:
    if not isinstance(levels, tuple):
        return None
    for raw in levels:
        if not isinstance(raw, tuple) or len(raw) != 2:
            continue
        try:
            level_price = float(raw[0])
            level_size = float(raw[1])
        except (TypeError, ValueError):
            continue
        if abs(level_price - float(price)) <= 1e-12:
            return max(level_size, 0.0)
    return None
