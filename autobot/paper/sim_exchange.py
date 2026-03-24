"""Paper exchange simulator with balances, orders, and spot positions."""

from __future__ import annotations

from dataclasses import dataclass
import math
import time
import uuid
from typing import Any

from autobot.execution.intent import OrderIntent

from .fill_model import TouchFillModel

EPSILON = 1e-12


@dataclass(frozen=True)
class MarketRules:
    bid_fee: float = 0.0005
    ask_fee: float = 0.0005
    maker_bid_fee: float | None = None
    maker_ask_fee: float | None = None
    min_total: float = 5000.0
    tick_size: float = 1.0

    def fee_rate(self, *, side: str, maker_or_taker: str) -> float:
        side_value = side.lower()
        maker_or_taker_value = maker_or_taker.lower()

        if side_value == "bid":
            if maker_or_taker_value == "maker" and self.maker_bid_fee is not None:
                return max(float(self.maker_bid_fee), 0.0)
            return max(float(self.bid_fee), 0.0)

        if side_value == "ask":
            if maker_or_taker_value == "maker" and self.maker_ask_fee is not None:
                return max(float(self.maker_ask_fee), 0.0)
            return max(float(self.ask_fee), 0.0)

        raise ValueError(f"unsupported side: {side}")

    def max_bid_fee(self) -> float:
        maker_fee = self.maker_bid_fee if self.maker_bid_fee is not None else self.bid_fee
        return max(float(self.bid_fee), float(maker_fee), 0.0)


@dataclass
class PaperOrder:
    order_id: str
    intent_id: str
    state: str
    created_ts_ms: int
    updated_ts_ms: int
    market: str
    side: str
    ord_type: str
    time_in_force: str
    price: float
    volume_req: float
    volume_filled: float
    avg_fill_price: float
    fee_paid_quote: float
    maker_or_taker: str
    reprice_attempt: int = 0
    failure_reason: str | None = None
    locked_quote: float = 0.0
    locked_base: float = 0.0


@dataclass(frozen=True)
class FillEvent:
    order_id: str
    market: str
    ts_ms: int
    price: float
    volume: float
    fee_quote: float


@dataclass
class Position:
    market: str
    base_currency: str
    quote_currency: str
    base_amount: float = 0.0
    avg_entry_price: float = 0.0
    realized_pnl_quote: float = 0.0
    unrealized_pnl_quote: float = 0.0


@dataclass
class AssetBalance:
    free: float = 0.0
    locked: float = 0.0


@dataclass(frozen=True)
class PortfolioSnapshot:
    ts_ms: int
    quote_currency: str
    cash_free: float
    cash_locked: float
    equity_quote: float
    realized_pnl_quote: float
    unrealized_pnl_quote: float
    positions: list[Position]


class PaperSimExchange:
    def __init__(
        self,
        *,
        quote_currency: str,
        starting_cash_quote: float,
        fill_model: TouchFillModel | None = None,
    ) -> None:
        self.quote_currency = quote_currency.strip().upper()
        if not self.quote_currency:
            raise ValueError("quote_currency is required")

        if starting_cash_quote < 0:
            raise ValueError("starting_cash_quote must be non-negative")

        self._fill_model = fill_model or TouchFillModel()
        self._cash = AssetBalance(free=float(starting_cash_quote), locked=0.0)
        self._coin_balances: dict[str, AssetBalance] = {}
        self._positions: dict[str, Position] = {}
        self._orders: dict[str, PaperOrder] = {}
        self._open_orders: dict[str, PaperOrder] = {}

    def quote_balance(self) -> AssetBalance:
        return AssetBalance(free=self._cash.free, locked=self._cash.locked)

    def coin_balance(self, currency: str) -> AssetBalance:
        key = currency.strip().upper()
        balance = self._coin_balances.get(key)
        if balance is None:
            return AssetBalance()
        return AssetBalance(free=balance.free, locked=balance.locked)

    def active_position_count(self) -> int:
        return sum(1 for position in self._positions.values() if position.base_amount > EPSILON)

    def has_position(self, market: str) -> bool:
        position = self._positions.get(market.strip().upper())
        return bool(position and position.base_amount > EPSILON)

    def open_position_markets(self) -> set[str]:
        return {
            str(market).strip().upper()
            for market, position in self._positions.items()
            if position.base_amount > EPSILON
        }

    def has_open_order(self, market: str, side: str | None = None) -> bool:
        market_value = market.strip().upper()
        side_value = side.strip().lower() if side else None
        for order in self._open_orders.values():
            if order.market != market_value:
                continue
            if side_value and order.side != side_value:
                continue
            return True
        return False

    def list_open_orders(self) -> list[PaperOrder]:
        return [self._clone_order(order) for order in self._open_orders.values()]

    def list_orders(self) -> list[PaperOrder]:
        return [self._clone_order(order) for order in self._orders.values()]

    def get_order(self, order_id: str) -> PaperOrder | None:
        order = self._orders.get(order_id)
        if order is None:
            return None
        return self._clone_order(order)

    def total_realized_pnl(self) -> float:
        return sum(position.realized_pnl_quote for position in self._positions.values())

    def submit_limit_order(
        self,
        *,
        intent: OrderIntent,
        rules: MarketRules,
        latest_trade_price: float,
        micro_snapshot: Any | None = None,
        ts_ms: int | None = None,
        reprice_attempt: int = 0,
    ) -> tuple[PaperOrder, FillEvent | None]:
        now_ts_ms = int(ts_ms if ts_ms is not None else time.time() * 1000)
        if intent.ord_type != "limit":
            failed = self._build_failed_order(intent=intent, ts_ms=now_ts_ms, reason="ORD_TYPE_NOT_ALLOWED")
            return (self._clone_order(failed), None)

        order = PaperOrder(
            order_id=f"paper-{uuid.uuid4().hex}",
            intent_id=intent.intent_id,
            state="OPEN",
            created_ts_ms=now_ts_ms,
            updated_ts_ms=now_ts_ms,
            market=intent.market,
            side=intent.side,
            ord_type="limit",
            time_in_force=intent.time_in_force,
            price=float(intent.price),
            volume_req=float(intent.volume),
            volume_filled=0.0,
            avg_fill_price=0.0,
            fee_paid_quote=0.0,
            maker_or_taker="unknown",
            reprice_attempt=max(int(reprice_attempt), 0),
        )

        if order.volume_req <= 0 or order.price <= 0:
            order.state = "FAILED"
            order.failure_reason = "INVALID_ORDER_PARAMS"
            self._orders[order.order_id] = order
            return (self._clone_order(order), None)

        notional_quote = order.price * order.volume_req
        if notional_quote + EPSILON < max(float(rules.min_total), 0.0):
            order.state = "FAILED"
            order.failure_reason = "BELOW_MIN_TOTAL"
            self._orders[order.order_id] = order
            return (self._clone_order(order), None)

        reserved = self._reserve_for_order(order, rules)
        if not reserved:
            order.state = "FAILED"
            order.failure_reason = "INSUFFICIENT_BALANCE"
            self._orders[order.order_id] = order
            return (self._clone_order(order), None)

        self._orders[order.order_id] = order

        if _allow_immediate_fill(intent=intent) and order.time_in_force in {"ioc", "fok"}:
            immediate_fill = self._resolve_immediate_taker_fill(
                order=order,
                latest_trade_price=latest_trade_price,
                micro_snapshot=micro_snapshot,
                require_marketable=bool(order.time_in_force in {"ioc", "fok"}),
            )
            if order.time_in_force == "fok" and immediate_fill is not None and float(immediate_fill["fill_volume"]) + EPSILON < order.volume_req:
                self._release_order_reserve(order)
                order.state = "CANCELED"
                order.updated_ts_ms = now_ts_ms
                order.failure_reason = "FOK_NOT_FULLY_FILLED"
                return (self._clone_order(order), None)
            if immediate_fill is not None:
                fill_event = self._fill_order(
                    order=order,
                    fill_price=float(immediate_fill["fill_price"]),
                    fill_volume=float(immediate_fill["fill_volume"]),
                    maker_or_taker=str(immediate_fill.get("maker_or_taker", "taker")),
                    ts_ms=now_ts_ms,
                    rules=rules,
                )
                if order.time_in_force == "fok" and order.state != "FILLED":
                    self._release_order_reserve(order)
                    order.state = "CANCELED"
                    order.updated_ts_ms = now_ts_ms
                    order.failure_reason = "FOK_NOT_FULLY_FILLED"
                elif order.time_in_force == "ioc" and order.state != "FILLED":
                    self._release_order_reserve(order)
                    order.state = "CANCELED"
                    order.updated_ts_ms = now_ts_ms
                    order.failure_reason = "IOC_PARTIAL_CANCELLED_REMAINDER"
                return (self._clone_order(order), fill_event)

        if _allow_immediate_fill(intent=intent):
            immediate_decision = self._fill_model.decide(
                side=order.side,
                limit_price=order.price,
                trade_price=latest_trade_price,
                immediate=True,
            )
            if immediate_decision.should_fill:
                fill_event = self._fill_order(
                    order=order,
                    fill_price=order.price,
                    fill_volume=order.volume_req,
                    maker_or_taker=immediate_decision.maker_or_taker,
                    ts_ms=now_ts_ms,
                    rules=rules,
                )
                return (self._clone_order(order), fill_event)

        if order.time_in_force in {"ioc", "fok"}:
            canceled = self.cancel_order(order.order_id, ts_ms=now_ts_ms, reason="IOC_FOK_NO_TOUCH")
            if canceled is None:
                self._release_order_reserve(order)
                order.state = "CANCELED"
                order.updated_ts_ms = now_ts_ms
                order.failure_reason = "IOC_FOK_NO_TOUCH"
            return (self._clone_order(order), None)

        self._open_orders[order.order_id] = order
        return (self._clone_order(order), None)

    def submit_order(
        self,
        *,
        intent: OrderIntent,
        rules: MarketRules,
        latest_trade_price: float,
        micro_snapshot: Any | None = None,
        ts_ms: int | None = None,
        reprice_attempt: int = 0,
    ) -> tuple[PaperOrder, FillEvent | None]:
        if str(intent.ord_type).strip().lower() == "best":
            return self.submit_best_order(
                intent=intent,
                rules=rules,
                latest_trade_price=latest_trade_price,
                micro_snapshot=micro_snapshot,
                ts_ms=ts_ms,
                reprice_attempt=reprice_attempt,
            )
        return self.submit_limit_order(
            intent=intent,
            rules=rules,
            latest_trade_price=latest_trade_price,
            micro_snapshot=micro_snapshot,
            ts_ms=ts_ms,
            reprice_attempt=reprice_attempt,
        )

    def submit_best_order(
        self,
        *,
        intent: OrderIntent,
        rules: MarketRules,
        latest_trade_price: float,
        micro_snapshot: Any | None = None,
        ts_ms: int | None = None,
        reprice_attempt: int = 0,
    ) -> tuple[PaperOrder, FillEvent | None]:
        now_ts_ms = int(ts_ms if ts_ms is not None else time.time() * 1000)
        side_value = str(intent.side).strip().lower()
        executable_prices = _resolve_executable_price_proxy(
            latest_trade_price=latest_trade_price,
            micro_snapshot=micro_snapshot,
        )
        market_price = (
            executable_prices["ask_price"]
            if side_value == "bid"
            else executable_prices["bid_price"]
            if side_value == "ask"
            else 0.0
        )
        if float(market_price) <= 0.0:
            failed = self._build_failed_order(intent=intent, ts_ms=now_ts_ms, reason="BEST_PRICE_UNAVAILABLE")
            return (self._clone_order(failed), None)

        if side_value == "bid":
            requested_notional = float(intent.price) if intent.price is not None else 0.0
            resolved_volume = order_volume_from_notional(
                notional_quote=max(requested_notional, 0.0),
                price=market_price,
            )
        elif side_value == "ask":
            resolved_volume = float(intent.volume) if intent.volume is not None else 0.0
            requested_notional = market_price * resolved_volume
        else:
            failed = self._build_failed_order(intent=intent, ts_ms=now_ts_ms, reason="INVALID_SIDE")
            return (self._clone_order(failed), None)

        order = PaperOrder(
            order_id=f"paper-{uuid.uuid4().hex}",
            intent_id=intent.intent_id,
            state="OPEN",
            created_ts_ms=now_ts_ms,
            updated_ts_ms=now_ts_ms,
            market=intent.market,
            side=side_value,
            ord_type="best",
            time_in_force=intent.time_in_force,
            price=float(market_price),
            volume_req=float(resolved_volume),
            volume_filled=0.0,
            avg_fill_price=0.0,
            fee_paid_quote=0.0,
            maker_or_taker="unknown",
            reprice_attempt=max(int(reprice_attempt), 0),
        )

        if order.volume_req <= 0.0 or requested_notional + EPSILON < max(float(rules.min_total), 0.0):
            order.state = "FAILED"
            order.failure_reason = "BELOW_MIN_TOTAL"
            self._orders[order.order_id] = order
            return (self._clone_order(order), None)

        reserved = self._reserve_for_order(order, rules)
        if not reserved:
            order.state = "FAILED"
            order.failure_reason = "INSUFFICIENT_BALANCE"
            self._orders[order.order_id] = order
            return (self._clone_order(order), None)

        self._orders[order.order_id] = order
        immediate_fill = self._resolve_immediate_taker_fill(
            order=order,
            latest_trade_price=latest_trade_price,
            micro_snapshot=micro_snapshot,
            require_marketable=False,
        )
        if immediate_fill is None:
            self._release_order_reserve(order)
            order.state = "CANCELED"
            order.updated_ts_ms = now_ts_ms
            order.failure_reason = "BEST_PRICE_UNAVAILABLE"
            return (self._clone_order(order), None)
        if order.time_in_force == "fok" and float(immediate_fill["fill_volume"]) + EPSILON < order.volume_req:
            self._release_order_reserve(order)
            order.state = "CANCELED"
            order.updated_ts_ms = now_ts_ms
            order.failure_reason = "FOK_NOT_FULLY_FILLED"
            return (self._clone_order(order), None)
        fill_event = self._fill_order(
            order=order,
            fill_price=float(immediate_fill["fill_price"]),
            fill_volume=float(immediate_fill["fill_volume"]),
            maker_or_taker=str(immediate_fill.get("maker_or_taker", "taker")),
            ts_ms=now_ts_ms,
            rules=rules,
        )
        if order.time_in_force == "fok" and order.state != "FILLED":
            self._release_order_reserve(order)
            order.state = "CANCELED"
            order.updated_ts_ms = now_ts_ms
            order.failure_reason = "FOK_NOT_FULLY_FILLED"
        elif order.time_in_force == "ioc" and order.state != "FILLED":
            self._release_order_reserve(order)
            order.state = "CANCELED"
            order.updated_ts_ms = now_ts_ms
            order.failure_reason = "IOC_PARTIAL_CANCELLED_REMAINDER"
        return (self._clone_order(order), fill_event)

    def process_ticker(
        self,
        *,
        market: str,
        trade_price: float,
        ts_ms: int,
        rules: MarketRules,
    ) -> list[FillEvent]:
        market_value = market.strip().upper()
        target_orders = [order for order in self._open_orders.values() if order.market == market_value]
        fills: list[FillEvent] = []
        for order in target_orders:
            decision = self._fill_model.decide(
                side=order.side,
                limit_price=order.price,
                trade_price=trade_price,
                immediate=False,
            )
            if not decision.should_fill:
                continue
            fill_event = self._fill_order(
                order=order,
                fill_price=order.price,
                fill_volume=max(order.volume_req - order.volume_filled, 0.0),
                maker_or_taker=decision.maker_or_taker,
                ts_ms=ts_ms,
                rules=rules,
            )
            fills.append(fill_event)
        return fills

    def cancel_order(self, order_id: str, *, ts_ms: int, reason: str | None = None) -> PaperOrder | None:
        order = self._open_orders.get(order_id)
        if order is None:
            return None

        self._release_order_reserve(order)
        order.state = "CANCELED"
        order.updated_ts_ms = int(ts_ms)
        if reason:
            order.failure_reason = str(reason)
        self._open_orders.pop(order_id, None)
        return self._clone_order(order)

    def _release_order_reserve(self, order: PaperOrder) -> None:
        if order.side == "bid":
            release_quote = max(order.locked_quote, 0.0)
            self._cash.locked = max(self._cash.locked - release_quote, 0.0)
            self._cash.free += release_quote
            order.locked_quote = 0.0
        else:
            base_currency = _base_currency(order.market)
            balance = self._coin_balance_mut(base_currency)
            release_base = max(order.locked_base, 0.0)
            balance.locked = max(balance.locked - release_base, 0.0)
            balance.free += release_base
            order.locked_base = 0.0

    def _resolve_immediate_taker_fill(
        self,
        *,
        order: PaperOrder,
        latest_trade_price: float,
        micro_snapshot: Any | None,
        require_marketable: bool,
    ) -> dict[str, float | str] | None:
        executable = _resolve_executable_price_proxy(
            latest_trade_price=latest_trade_price,
            micro_snapshot=micro_snapshot,
        )
        side_depth_quote = _resolve_executable_side_depth_quote(
            micro_snapshot=micro_snapshot,
            side=order.side,
        )
        if order.side == "bid":
            executable_price = float(executable["ask_price"])
            if require_marketable and float(order.price) + EPSILON < executable_price:
                return None
            requested_quote = float(order.price) * float(order.volume_req)
            fill_quote = requested_quote if side_depth_quote is None else min(float(requested_quote), float(side_depth_quote))
            fill_volume = fill_quote / max(executable_price, EPSILON)
        else:
            executable_price = float(executable["bid_price"])
            if require_marketable and float(order.price) - EPSILON > executable_price:
                return None
            executable_volume = float(order.volume_req) if side_depth_quote is None else float(side_depth_quote) / max(executable_price, EPSILON)
            fill_volume = min(float(order.volume_req), float(executable_volume))
        if fill_volume <= EPSILON:
            return None
        return {
            "fill_price": float(executable_price),
            "fill_volume": float(min(fill_volume, order.volume_req)),
            "maker_or_taker": "taker",
        }

    def portfolio_snapshot(self, *, ts_ms: int, latest_prices: dict[str, float]) -> PortfolioSnapshot:
        total_equity = self._cash.free + self._cash.locked
        total_realized = 0.0
        total_unrealized = 0.0
        positions: list[Position] = []

        for market, position in self._positions.items():
            if position.base_amount <= EPSILON and abs(position.realized_pnl_quote) <= EPSILON:
                continue
            mark_price = float(latest_prices.get(market, position.avg_entry_price or 0.0))
            unrealized = (mark_price - position.avg_entry_price) * position.base_amount
            total_unrealized += unrealized
            total_realized += position.realized_pnl_quote
            total_equity += mark_price * position.base_amount
            positions.append(
                Position(
                    market=position.market,
                    base_currency=position.base_currency,
                    quote_currency=position.quote_currency,
                    base_amount=position.base_amount,
                    avg_entry_price=position.avg_entry_price,
                    realized_pnl_quote=position.realized_pnl_quote,
                    unrealized_pnl_quote=unrealized,
                )
            )

        positions.sort(key=lambda item: item.market)
        return PortfolioSnapshot(
            ts_ms=int(ts_ms),
            quote_currency=self.quote_currency,
            cash_free=self._cash.free,
            cash_locked=self._cash.locked,
            equity_quote=total_equity,
            realized_pnl_quote=total_realized,
            unrealized_pnl_quote=total_unrealized,
            positions=positions,
        )

    def _reserve_for_order(self, order: PaperOrder, rules: MarketRules) -> bool:
        if order.side == "bid":
            reserve_quote = order.price * order.volume_req * (1.0 + rules.max_bid_fee())
            if self._cash.free + EPSILON < reserve_quote:
                return False
            self._cash.free -= reserve_quote
            self._cash.locked += reserve_quote
            order.locked_quote = reserve_quote
            return True

        if order.side == "ask":
            base_currency = _base_currency(order.market)
            balance = self._coin_balance_mut(base_currency)
            if balance.free + EPSILON < order.volume_req:
                return False
            balance.free -= order.volume_req
            balance.locked += order.volume_req
            order.locked_base = order.volume_req
            return True

        return False

    def _fill_order(
        self,
        *,
        order: PaperOrder,
        fill_price: float,
        fill_volume: float,
        maker_or_taker: str,
        ts_ms: int,
        rules: MarketRules,
    ) -> FillEvent:
        fill_volume_value = max(fill_volume, 0.0)
        fee_rate = rules.fee_rate(side=order.side, maker_or_taker=maker_or_taker)
        fee_quote = fill_price * fill_volume_value * fee_rate

        previous_volume_filled = float(order.volume_filled)
        remaining_before_fill = max(order.volume_req - previous_volume_filled, 0.0)
        order.volume_filled = min(order.volume_req, order.volume_filled + fill_volume_value)
        if order.avg_fill_price <= 0 or previous_volume_filled <= EPSILON:
            order.avg_fill_price = fill_price
        else:
            total_quote_before = float(order.avg_fill_price) * float(previous_volume_filled)
            total_quote_after = total_quote_before + (float(fill_price) * float(fill_volume_value))
            order.avg_fill_price = total_quote_after / max(float(order.volume_filled), EPSILON)
        order.fee_paid_quote += fee_quote
        order.maker_or_taker = maker_or_taker
        order.updated_ts_ms = int(ts_ms)
        order.state = "FILLED" if order.volume_filled + EPSILON >= order.volume_req else "PARTIAL"

        if order.side == "bid":
            total_cost = fill_price * fill_volume_value + fee_quote
            reserved_before = max(order.locked_quote, 0.0)
            release_quote = reserved_before
            if order.state == "PARTIAL" and remaining_before_fill > EPSILON:
                release_quote = reserved_before * min(max(fill_volume_value / remaining_before_fill, 0.0), 1.0)
            self._cash.locked = max(self._cash.locked - release_quote, 0.0)
            refund = max(release_quote - total_cost, 0.0)
            self._cash.free += refund
            order.locked_quote = max(reserved_before - release_quote, 0.0)

            base_currency = _base_currency(order.market)
            coin_balance = self._coin_balance_mut(base_currency)
            coin_balance.free += fill_volume_value

            position = self._position_mut(order.market)
            total_base_before = max(position.base_amount, 0.0)
            total_cost_before = total_base_before * position.avg_entry_price
            total_base_after = total_base_before + fill_volume_value
            position.base_amount = total_base_after
            position.avg_entry_price = (
                0.0 if total_base_after <= EPSILON else (total_cost_before + total_cost) / total_base_after
            )

        else:
            base_currency = _base_currency(order.market)
            coin_balance = self._coin_balance_mut(base_currency)
            reserved_base_before = max(order.locked_base, 0.0)
            release_base = reserved_base_before
            if order.state == "PARTIAL" and remaining_before_fill > EPSILON:
                release_base = reserved_base_before * min(max(fill_volume_value / remaining_before_fill, 0.0), 1.0)
            coin_balance.locked = max(coin_balance.locked - release_base, 0.0)
            order.locked_base = max(reserved_base_before - release_base, 0.0)

            quote_gain = fill_price * fill_volume_value - fee_quote
            self._cash.free += quote_gain

            position = self._position_mut(order.market)
            matched_volume = min(position.base_amount, fill_volume_value)
            if matched_volume > EPSILON:
                position.realized_pnl_quote += (fill_price - position.avg_entry_price) * matched_volume
            position.realized_pnl_quote -= fee_quote
            position.base_amount = max(position.base_amount - fill_volume_value, 0.0)
            if position.base_amount <= EPSILON:
                position.base_amount = 0.0
                position.avg_entry_price = 0.0

        if order.state == "FILLED":
            self._open_orders.pop(order.order_id, None)

        return FillEvent(
            order_id=order.order_id,
            market=order.market,
            ts_ms=int(ts_ms),
            price=float(fill_price),
            volume=float(fill_volume_value),
            fee_quote=float(fee_quote),
        )

    def _build_failed_order(self, *, intent: OrderIntent, ts_ms: int, reason: str) -> PaperOrder:
        order = PaperOrder(
            order_id=f"paper-{uuid.uuid4().hex}",
            intent_id=intent.intent_id,
            state="FAILED",
            created_ts_ms=ts_ms,
            updated_ts_ms=ts_ms,
            market=intent.market,
            side=intent.side,
            ord_type=intent.ord_type,
            time_in_force=intent.time_in_force,
            price=float(intent.price) if intent.price is not None else 0.0,
            volume_req=float(intent.volume) if intent.volume is not None else 0.0,
            volume_filled=0.0,
            avg_fill_price=0.0,
            fee_paid_quote=0.0,
            maker_or_taker="unknown",
            failure_reason=reason,
        )
        self._orders[order.order_id] = order
        return order

    def _position_mut(self, market: str) -> Position:
        market_value = market.strip().upper()
        position = self._positions.get(market_value)
        if position is not None:
            return position

        quote_currency, base_currency = parse_market(market_value)
        position = Position(
            market=market_value,
            base_currency=base_currency,
            quote_currency=quote_currency,
        )
        self._positions[market_value] = position
        return position

    def _coin_balance_mut(self, currency: str) -> AssetBalance:
        key = currency.strip().upper()
        balance = self._coin_balances.get(key)
        if balance is not None:
            return balance
        balance = AssetBalance()
        self._coin_balances[key] = balance
        return balance

    @staticmethod
    def _clone_order(order: PaperOrder) -> PaperOrder:
        return PaperOrder(
            order_id=order.order_id,
            intent_id=order.intent_id,
            state=order.state,
            created_ts_ms=order.created_ts_ms,
            updated_ts_ms=order.updated_ts_ms,
            market=order.market,
            side=order.side,
            ord_type=order.ord_type,
            time_in_force=order.time_in_force,
            price=order.price,
            volume_req=order.volume_req,
            volume_filled=order.volume_filled,
            avg_fill_price=order.avg_fill_price,
            fee_paid_quote=order.fee_paid_quote,
            maker_or_taker=order.maker_or_taker,
            reprice_attempt=order.reprice_attempt,
            failure_reason=order.failure_reason,
            locked_quote=order.locked_quote,
            locked_base=order.locked_base,
        )


def parse_market(market: str) -> tuple[str, str]:
    raw = market.strip().upper()
    if "-" not in raw:
        raise ValueError(f"invalid market format: {market}")
    quote_currency, base_currency = raw.split("-", 1)
    if not quote_currency or not base_currency:
        raise ValueError(f"invalid market format: {market}")
    return (quote_currency, base_currency)


def _base_currency(market: str) -> str:
    return parse_market(market)[1]


def _allow_immediate_fill(*, intent: OrderIntent) -> bool:
    meta = dict(intent.meta or {})
    execution_policy = dict(meta.get("execution_policy") or {}) if isinstance(meta.get("execution_policy"), dict) else {}
    exec_profile = dict(meta.get("exec_profile") or {}) if isinstance(meta.get("exec_profile"), dict) else {}
    price_mode = str(execution_policy.get("selected_price_mode", exec_profile.get("price_mode", ""))).strip().upper()
    time_in_force = str(getattr(intent, "time_in_force", "") or "").strip().lower()
    if price_mode == "PASSIVE_MAKER":
        return False
    if time_in_force == "post_only":
        return False
    return True


def _resolve_executable_price_proxy(*, latest_trade_price: float, micro_snapshot: Any | None = None) -> dict[str, float]:
    trade_price = max(float(latest_trade_price), EPSILON)
    spread_bps = _safe_optional_float(getattr(micro_snapshot, "spread_bps_mean", None))
    if spread_bps is None or float(spread_bps) <= 0.0:
        return {
            "bid_price": float(trade_price),
            "ask_price": float(trade_price),
        }
    half_spread_ratio = max(float(spread_bps), 0.0) / 20_000.0
    ask_price = max(float(trade_price) * (1.0 + half_spread_ratio), EPSILON)
    bid_price = max(float(trade_price) * (1.0 - half_spread_ratio), EPSILON)
    return {
        "bid_price": float(bid_price),
        "ask_price": float(ask_price),
    }


def _resolve_executable_side_depth_quote(*, micro_snapshot: Any | None, side: str) -> float | None:
    side_value = str(side).strip().lower()
    side_specific = _safe_optional_float(
        getattr(
            micro_snapshot,
            "depth_ask_top5_notional_krw" if side_value == "bid" else "depth_bid_top5_notional_krw",
            None,
        )
    )
    if side_specific is not None and float(side_specific) > 0.0:
        return max(float(side_specific), 0.0)

    depth_total = _safe_optional_float(getattr(micro_snapshot, "depth_top5_notional_krw", None))
    if depth_total is None or float(depth_total) <= 0.0:
        return None
    # `depth_top5_notional_krw` is a combined top-of-book proxy, so use half as
    # a conservative first-pass estimate for one-side immediately executable depth.
    return max(float(depth_total) * 0.5, 0.0)


def _safe_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def order_volume_from_notional(*, notional_quote: float, price: float) -> float:
    if price <= 0:
        raise ValueError("price must be positive")
    return max(float(notional_quote) / float(price), 0.0)


def round_price_to_tick(*, price: float, tick_size: float, side: str) -> float:
    if price <= 0:
        raise ValueError("price must be positive")

    tick_value = max(float(tick_size), EPSILON)
    side_value = side.strip().lower()

    scaled = float(price) / tick_value
    if side_value == "bid":
        rounded_ticks = math.floor(scaled + EPSILON)
    elif side_value == "ask":
        rounded_ticks = math.ceil(scaled - EPSILON)
    else:
        raise ValueError("side must be bid or ask")

    rounded = max(rounded_ticks * tick_value, tick_value)
    digits = _decimal_places(tick_value)
    return round(rounded, digits)


def _decimal_places(value: float) -> int:
    text = f"{value:.16f}".rstrip("0")
    if "." not in text:
        return 0
    return len(text.split(".", 1)[1])
