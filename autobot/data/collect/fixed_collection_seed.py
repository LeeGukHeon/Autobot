"""Helpers for curating the fixed first-tier collection market layer."""

from __future__ import annotations

from collections.abc import Iterable, Mapping


DEFAULT_EXCLUDED_SYMBOLS: tuple[str, ...] = (
    "USDT",
    "USDC",
    "USDS",
    "USDE",
    "USD1",
    "XAUT",
)


def select_market_cap_ranked_fixed_markets(
    *,
    available_markets: Iterable[str],
    market_cap_rows: Iterable[Mapping[str, object]],
    quote: str = "KRW",
    limit: int = 30,
    excluded_symbols: Iterable[str] = DEFAULT_EXCLUDED_SYMBOLS,
) -> list[str]:
    """Select quote markets by descending external market-cap ranking.

    `market_cap_rows` are expected to already be sorted by market-cap rank
    ascending (largest cap first). Each row must expose a `symbol` field.
    """

    quote_value = str(quote).strip().upper() or "KRW"
    quote_prefix = f"{quote_value}-"
    normalized_excluded = {
        str(symbol).strip().upper()
        for symbol in excluded_symbols
        if str(symbol).strip()
    }

    market_by_symbol: dict[str, str] = {}
    for raw_market in available_markets:
        market = str(raw_market).strip().upper()
        if not market.startswith(quote_prefix):
            continue
        symbol = market.split("-", 1)[1].strip().upper()
        if not symbol or symbol in market_by_symbol:
            continue
        market_by_symbol[symbol] = market

    selected: list[str] = []
    seen_markets: set[str] = set()
    effective_limit = max(int(limit), 1)
    for row in market_cap_rows:
        symbol = str(row.get("symbol", "")).strip().upper()
        if not symbol or symbol in normalized_excluded:
            continue
        market = market_by_symbol.get(symbol)
        if not market or market in seen_markets:
            continue
        seen_markets.add(market)
        selected.append(market)
        if len(selected) >= effective_limit:
            break
    return selected
