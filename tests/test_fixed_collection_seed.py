from __future__ import annotations

from autobot.data.collect.fixed_collection_seed import (
    DEFAULT_EXCLUDED_SYMBOLS,
    select_market_cap_ranked_fixed_markets,
)


def test_select_market_cap_ranked_fixed_markets_uses_rank_order_and_quote_filter() -> None:
    markets = (
        "KRW-BTC",
        "KRW-ETH",
        "KRW-XRP",
        "BTC-ETH",
        "USDT-BTC",
    )
    market_cap_rows = (
        {"symbol": "btc"},
        {"symbol": "eth"},
        {"symbol": "xrp"},
    )

    selected = select_market_cap_ranked_fixed_markets(
        available_markets=markets,
        market_cap_rows=market_cap_rows,
        quote="KRW",
        limit=2,
    )

    assert selected == ["KRW-BTC", "KRW-ETH"]


def test_select_market_cap_ranked_fixed_markets_skips_excluded_symbols() -> None:
    markets = (
        "KRW-USDT",
        "KRW-BTC",
        "KRW-USDC",
        "KRW-ETH",
    )
    market_cap_rows = (
        {"symbol": "usdt"},
        {"symbol": "btc"},
        {"symbol": "usdc"},
        {"symbol": "eth"},
    )

    selected = select_market_cap_ranked_fixed_markets(
        available_markets=markets,
        market_cap_rows=market_cap_rows,
        quote="KRW",
        limit=4,
    )

    assert "KRW-USDT" not in selected
    assert "KRW-USDC" not in selected
    assert selected == ["KRW-BTC", "KRW-ETH"]


def test_default_excluded_symbols_keep_stable_like_assets_out_of_fixed_layer() -> None:
    assert {"USDT", "USDC", "USDS", "USDE", "USD1", "XAUT"} <= set(DEFAULT_EXCLUDED_SYMBOLS)
