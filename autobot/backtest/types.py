"""Shared data contracts for backtest runtime."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CandleBar:
    market: str
    ts_ms: int
    open: float
    high: float
    low: float
    close: float
    volume_base: float
    volume_quote: float | None
    volume_quote_est: bool
