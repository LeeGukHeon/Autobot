"""Dynamic live-scan universe contract for current liquid markets."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class LiveScanUniverse:
    quote: str
    markets: tuple[str, ...]
    selected_at_ts_ms: int
    selection_policy: str = "top_trade_value_scanner_v1"
    tags: tuple[str, ...] = field(default_factory=tuple)

