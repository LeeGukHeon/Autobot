"""Strategy adapter contracts for backtest runtime."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, Sequence


@dataclass(frozen=True)
class StrategyOrderIntent:
    market: str
    side: str
    ref_price: float
    reason_code: str
    score: float | None = None
    prob: float | None = None
    volume: float | None = None
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StrategyStepResult:
    intents: tuple[StrategyOrderIntent, ...] = ()
    scored_rows: int = 0
    selected_rows: int = 0
    skipped_missing_features_rows: int = 0
    dropped_min_prob_rows: int = 0
    dropped_top_pct_rows: int = 0
    blocked_min_candidates_ts: int = 0
    skipped_reasons: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class StrategyFillEvent:
    ts_ms: int
    market: str
    side: str
    price: float
    volume: float


class BacktestStrategyAdapter(Protocol):
    def on_ts(
        self,
        *,
        ts_ms: int,
        active_markets: Sequence[str],
        latest_prices: dict[str, float],
        open_markets: set[str],
    ) -> StrategyStepResult: ...

    def on_fill(self, event: StrategyFillEvent) -> None: ...
