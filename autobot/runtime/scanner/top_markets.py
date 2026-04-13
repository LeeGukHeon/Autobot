"""Bridge into the current top-trade-value live scanner."""

from __future__ import annotations

from autobot.strategy.top20_scanner import MarketTopItem, TopTradeValueScanner


def scan_top_markets(
    scanner: TopTradeValueScanner,
    *,
    n: int,
    quote: str,
    include_caution: bool = True,
    include_inactive: bool = True,
) -> list[MarketTopItem]:
    return scanner.top_n(
        n=n,
        quote=quote,
        include_caution=include_caution,
        include_inactive=include_inactive,
    )

