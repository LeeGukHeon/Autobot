"""Helpers for filtering planner market universes to currently active Upbit markets."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ...upbit import UpbitHttpClient, UpbitPublicClient, load_upbit_settings


def resolve_active_quote_markets(
    *,
    quote: str,
    config_dir: Path,
    active_markets_override: tuple[str, ...] | None = None,
    enabled: bool = False,
) -> tuple[set[str] | None, dict[str, Any]]:
    quote_prefix = f"{str(quote).strip().upper()}-"
    if active_markets_override is not None:
        selected = {
            str(item).strip().upper()
            for item in active_markets_override
            if str(item).strip().upper().startswith(quote_prefix)
        }
        return selected, {
            "status": "override",
            "source": "active_markets_override",
            "count": len(selected),
        }

    if not enabled:
        return None, {
            "status": "disabled",
            "source": "none",
            "count": None,
        }

    try:
        settings = load_upbit_settings(config_dir)
        with UpbitHttpClient(settings) as http_client:
            payload = UpbitPublicClient(http_client).markets(is_details=True)
    except Exception as exc:
        return None, {
            "status": "unavailable",
            "source": "upbit_market_all",
            "count": None,
            "error": str(exc),
        }

    if not isinstance(payload, list):
        return None, {
            "status": "invalid_payload",
            "source": "upbit_market_all",
            "count": None,
        }

    markets: set[str] = set()
    for item in payload:
        if not isinstance(item, dict):
            continue
        market = str(item.get("market", "")).strip().upper()
        if market.startswith(quote_prefix):
            markets.add(market)
    return markets, {
        "status": "resolved",
        "source": "upbit_market_all",
        "count": len(markets),
    }


def filter_markets_by_active_set(
    *,
    markets: list[str],
    active_markets: set[str] | None,
    top_n: int | None = None,
) -> tuple[list[str], list[str]]:
    if not active_markets:
        limited = list(markets[: max(int(top_n), 0)]) if top_n is not None and int(top_n) >= 0 else list(markets)
        return limited, []

    kept: list[str] = []
    dropped: list[str] = []
    limit = max(int(top_n), 0) if top_n is not None else None
    for market in markets:
        normalized = str(market).strip().upper()
        if normalized in active_markets:
            kept.append(normalized)
            if limit is not None and len(kept) >= limit:
                break
        else:
            dropped.append(normalized)
    return kept, dropped
