"""Minimal rule gate for paper-run/backtest order intents."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from .micro_gate_v1 import MicroGateV1
from .micro_snapshot import MicroSnapshotProvider


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
    gate_reasons: tuple[str, ...] = ()
    severity: str = "OK"  # OK | WARN | BLOCK
    diagnostics: dict[str, Any] | None = None


class ExchangeView(Protocol):
    def quote_balance(self) -> object: ...

    def coin_balance(self, currency: str) -> object: ...

    def active_position_count(self) -> int: ...

    def has_position(self, market: str) -> bool: ...

    def has_open_order(self, market: str, side: str | None = None) -> bool: ...


class TradeGateV1:
    def __init__(
        self,
        settings: GateSettings,
        *,
        micro_gate: MicroGateV1 | None = None,
        micro_snapshot_provider: MicroSnapshotProvider | None = None,
    ) -> None:
        self._settings = settings
        self._consecutive_failures: dict[str, int] = {}
        self._cooldown_until_ms: dict[str, int] = {}
        self._micro_gate = micro_gate
        self._micro_snapshot_provider = micro_snapshot_provider

    def set_micro_snapshot_provider(self, provider: MicroSnapshotProvider | None) -> None:
        self._micro_snapshot_provider = provider

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
            return _decision(False, "INVALID_SIDE", f"unsupported side={side}", severity="BLOCK")

        cooldown_until = self._cooldown_until_ms.get(market_value, 0)
        if ts_ms < cooldown_until:
            return _decision(False, "COOLDOWN_ACTIVE", f"cooldown_until_ms={cooldown_until}", severity="BLOCK")

        notional = float(price) * float(volume)
        required_min_total = max(float(self._settings.min_order_krw), float(min_total_krw), 5000.0)
        if notional < required_min_total:
            return _decision(
                False,
                "MIN_TOTAL",
                f"notional={notional:.2f} min_total={required_min_total:.2f}",
                severity="BLOCK",
            )

        if side_value == "bid":
            if exchange.has_position(market_value) or exchange.has_open_order(market_value, side="bid"):
                return _decision(False, "DUPLICATE_ENTRY", "position/open order already exists", severity="BLOCK")
            if exchange.active_position_count() >= int(self._settings.max_positions):
                return _decision(
                    False,
                    "MAX_POSITIONS",
                    f"max_positions={self._settings.max_positions}",
                    severity="BLOCK",
                )

            quote_balance = exchange.quote_balance()
            free_quote = float(getattr(quote_balance, "free", 0.0))
            required_quote = notional * (1.0 + max(float(fee_rate), 0.0))
            if free_quote < required_quote:
                return _decision(
                    False,
                    "INSUFFICIENT_QUOTE",
                    f"required={required_quote:.2f} free={free_quote:.2f}",
                    severity="BLOCK",
                )

        if side_value == "ask":
            base_currency = market_value.split("-", 1)[1] if "-" in market_value else ""
            if not base_currency:
                return _decision(False, "INVALID_MARKET", f"market={market_value}", severity="BLOCK")
            base_balance = exchange.coin_balance(base_currency)
            free_base = float(getattr(base_balance, "free", 0.0))
            if free_base < float(volume):
                return _decision(
                    False,
                    "INSUFFICIENT_BASE",
                    f"required={volume:.8f} free={free_base:.8f}",
                    severity="BLOCK",
                )

        if self._micro_gate is not None:
            snapshot = (
                self._micro_snapshot_provider.get(market_value, int(ts_ms))
                if self._micro_snapshot_provider is not None
                else None
            )
            micro_decision = self._micro_gate.evaluate(
                candidate=None,
                micro_snapshot=snapshot,
                now_ts_ms=int(ts_ms),
            )
            reason = (
                micro_decision.reasons[0]
                if micro_decision.reasons
                else ("MICRO_BLOCK" if micro_decision.severity == "BLOCK" else "ALLOW")
            )
            if not micro_decision.allow:
                return _decision(
                    False,
                    reason,
                    f"micro_gate_blocked reasons={','.join(micro_decision.reasons)}",
                    severity="BLOCK",
                    gate_reasons=micro_decision.reasons,
                    diagnostics=micro_decision.diagnostics,
                )
            if micro_decision.severity == "WARN":
                return _decision(
                    True,
                    reason,
                    f"micro_gate_warn reasons={','.join(micro_decision.reasons)}",
                    severity="WARN",
                    gate_reasons=micro_decision.reasons,
                    diagnostics=micro_decision.diagnostics,
                )

        return _decision(True, "ALLOW", "passed", severity="OK")

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


def _decision(
    allowed: bool,
    reason_code: str,
    detail: str,
    *,
    severity: str,
    gate_reasons: tuple[str, ...] | list[str] | None = None,
    diagnostics: dict[str, Any] | None = None,
) -> GateDecision:
    if gate_reasons is None:
        normalized_reasons = (str(reason_code).strip().upper(),)
    else:
        normalized_reasons = tuple(str(item).strip().upper() for item in gate_reasons if str(item).strip())
        if not normalized_reasons:
            normalized_reasons = (str(reason_code).strip().upper(),)
    return GateDecision(
        allowed=bool(allowed),
        reason_code=str(reason_code).strip().upper(),
        detail=str(detail),
        gate_reasons=normalized_reasons,
        severity=str(severity).strip().upper(),
        diagnostics=diagnostics if isinstance(diagnostics, dict) else None,
    )
