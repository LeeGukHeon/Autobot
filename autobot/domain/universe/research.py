"""Stable research-universe contract used by training and historical evaluation."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ResearchUniverseSpec:
    universe_id: str
    markets: tuple[str, ...]
    source_policy: str = "stable_research_universe_v1"
    selection_basis: str = "broad_liquidity_history"
    tags: tuple[str, ...] = field(default_factory=tuple)

