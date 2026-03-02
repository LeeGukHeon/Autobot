"""Minimal rule gate for paper-run order intents."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class GateSettings:
    per_trade_krw: float
    max_positions: int
    min_order_krw: float
    max_consecutive_failures: int
    cooldown_sec_after_fail: int


@dataclass(frozen=True)
class GateDecision:
    allowed: bool
    reason_code: str
    detail: str


class ExchangeView(Protocol):
    def quote_balance(self) -> object: ...

    def coin_balance(self, currency: str) -> object: ...

    def active_position_count(self) -> int: ...

    def has_position(self, market: str) -> bool: ...

    def has_open_order(self, market: str, side: str | None = None) -> bool: ...


class TradeGateV1:
    def __init__(self, settings: GateSettings) -> None:
        self._settings = settings
        self._consecutive_failures: dict[str, int] = {}
        self._cooldown_until_ms: dict[str, int] = {}

    def evaluate(
        self,
        *,
        ts_ms: int,
        market: str,
        side: str,
        price: float,
        volume: float,
        fee_rate: float,
        exchange: ExchangeView,
        min_total_krw: float,
    ) -> GateDecision:
        market_value = market.strip().upper()
        side_value = side.strip().lower()
        if side_value not in {"bid", "ask"}:
            return GateDecision(False, "INVALID_SIDE", f"unsupported side={side}")

        cooldown_until = self._cooldown_until_ms.get(market_value, 0)
        if ts_ms < cooldown_until:
            return GateDecision(False, "COOLDOWN_ACTIVE", f"cooldown_until_ms={cooldown_until}")

        notional = float(price) * float(volume)
        required_min_total = max(float(self._settings.min_order_krw), float(min_total_krw), 5000.0)
        if notional < required_min_total:
            return GateDecision(False, "MIN_TOTAL", f"notional={notional:.2f} min_total={required_min_total:.2f}")

        if side_value == "bid":
            if exchange.has_position(market_value) or exchange.has_open_order(market_value, side="bid"):
                return GateDecision(False, "DUPLICATE_ENTRY", "position/open order already exists")
            if exchange.active_position_count() >= int(self._settings.max_positions):
                return GateDecision(False, "MAX_POSITIONS", f"max_positions={self._settings.max_positions}")

            quote_balance = exchange.quote_balance()
            free_quote = float(getattr(quote_balance, "free", 0.0))
            required_quote = notional * (1.0 + max(float(fee_rate), 0.0))
            if free_quote < required_quote:
                return GateDecision(False, "INSUFFICIENT_QUOTE", f"required={required_quote:.2f} free={free_quote:.2f}")

        if side_value == "ask":
            base_currency = market_value.split("-", 1)[1] if "-" in market_value else ""
            if not base_currency:
                return GateDecision(False, "INVALID_MARKET", f"market={market_value}")
            base_balance = exchange.coin_balance(base_currency)
            free_base = float(getattr(base_balance, "free", 0.0))
            if free_base < float(volume):
                return GateDecision(False, "INSUFFICIENT_BASE", f"required={volume:.8f} free={free_base:.8f}")

        return GateDecision(True, "ALLOW", "passed")

    def record_failure(self, market: str, *, ts_ms: int) -> None:
        market_value = market.strip().upper()
        new_fail_count = self._consecutive_failures.get(market_value, 0) + 1
        self._consecutive_failures[market_value] = new_fail_count

        if new_fail_count >= max(int(self._settings.max_consecutive_failures), 1):
            cooldown_ms = max(int(self._settings.cooldown_sec_after_fail), 0) * 1000
            self._cooldown_until_ms[market_value] = int(ts_ms) + cooldown_ms
            self._consecutive_failures[market_value] = 0

    def record_success(self, market: str) -> None:
        market_value = market.strip().upper()
        self._consecutive_failures[market_value] = 0
        self._cooldown_until_ms.pop(market_value, None)