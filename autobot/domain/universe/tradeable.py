"""Tradeable-universe contract resolved from live scan and runtime/model eligibility."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class TradeableUniverse:
    markets: tuple[str, ...]
    live_scan_markets: tuple[str, ...]
    runtime_allowed_markets: tuple[str, ...]
    policy: str = "tradeable_universe_intersection_v1"


def resolve_tradeable_markets(
    *,
    live_scan_markets: Iterable[str],
    runtime_allowed_markets: Iterable[str],
) -> TradeableUniverse:
    ordered_live = tuple(str(item).strip().upper() for item in live_scan_markets if str(item).strip())
    allowed = {str(item).strip().upper() for item in runtime_allowed_markets if str(item).strip()}
    markets = tuple(item for item in ordered_live if item in allowed) if allowed else ordered_live
    return TradeableUniverse(
        markets=markets,
        live_scan_markets=ordered_live,
        runtime_allowed_markets=tuple(sorted(allowed)),
    )

