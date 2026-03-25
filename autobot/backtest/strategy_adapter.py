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
class StrategyOpportunityRecord:
    opportunity_id: str
    ts_ms: int
    market: str
    side: str
    selection_score: float | None = None
    selection_score_raw: float | None = None
    feature_hash: str = ""
    chosen_action: str = ""
    reason_code: str = ""
    skip_reason_code: str | None = None
    intent_id: str | None = None
    expected_edge_bps: float | None = None
    uncertainty: float | None = None
    run_id: str | None = None
    candidate_actions_json: tuple[dict[str, Any], ...] = ()
    chosen_action_propensity: float | None = None
    realized_outcome_json: dict[str, Any] = field(default_factory=dict)
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StrategyStepResult:
    intents: tuple[StrategyOrderIntent, ...] = ()
    opportunities: tuple[StrategyOpportunityRecord, ...] = ()
    scored_rows: int = 0
    eligible_rows: int = 0
    selected_rows: int = 0
    skipped_missing_features_rows: int = 0
    dropped_min_prob_rows: int = 0
    dropped_top_pct_rows: int = 0
    blocked_min_candidates_ts: int = 0
    min_prob_used: float = 0.0
    min_prob_source: str = "manual"
    top_pct_used: float = 0.0
    top_pct_source: str = "manual"
    min_candidates_used: int = 0
    min_candidates_source: str = "manual"
    selection_policy_mode: str = "raw_threshold"
    selection_policy_source: str = "manual"
    operational_regime_score: float = 0.0
    operational_risk_multiplier: float = 1.0
    operational_max_positions: int = 0
    operational_state: dict[str, Any] = field(default_factory=dict)
    skipped_reasons: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class StrategyFillEvent:
    ts_ms: int
    market: str
    side: str
    price: float
    volume: float
    fee_quote: float = 0.0
    meta: dict[str, Any] = field(default_factory=dict)


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
