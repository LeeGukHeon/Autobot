"""Runtime-universe helpers combining live scan inputs, runtime eligibility, and live-input completeness."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Sequence

from autobot.live.model_alpha_runtime_bootstrap import load_quote_markets, resolve_runtime_allowed_markets
from autobot.strategy.micro_snapshot import MicroSnapshotProvider
from autobot.strategy.top20_scanner import MarketTopItem


@dataclass(frozen=True)
class RuntimeUniverseSnapshot:
    quote: str
    live_markets: tuple[str, ...]
    runtime_allowed_markets: tuple[str, ...]
    tradeable_markets: tuple[str, ...]
    missing_live_inputs_by_market: dict[str, tuple[str, ...]] = field(default_factory=dict)


def determine_missing_live_inputs(
    *,
    market_items: Sequence[MarketTopItem],
    micro_snapshot_provider: MicroSnapshotProvider | None,
) -> dict[str, tuple[str, ...]]:
    results: dict[str, tuple[str, ...]] = {}
    for item in market_items:
        market = str(item.market).strip().upper()
        if not market:
            continue
        missing: list[str] = []
        if float(item.trade_price or 0.0) <= 0.0:
            missing.append("ticker")
        if micro_snapshot_provider is None:
            missing.extend(["trade", "orderbook"])
        else:
            snapshot = micro_snapshot_provider.get(market, int(item.ts_ms))
            trade_ready = (
                snapshot is not None
                and int(getattr(snapshot, "trade_events", 0) or 0) > 0
                and int(getattr(snapshot, "trade_coverage_ms", 0) or 0) > 0
            )
            orderbook_ready = (
                snapshot is not None
                and int(getattr(snapshot, "book_events", 0) or 0) > 0
                and int(getattr(snapshot, "book_coverage_ms", 0) or 0) > 0
            )
            if not trade_ready:
                missing.append("trade")
            if not orderbook_ready:
                missing.append("orderbook")
        results[market] = tuple(missing)
    return results


def intersect_runtime_markets(
    *,
    live_markets: Iterable[str],
    runtime_allowed_markets: Iterable[str],
) -> tuple[str, ...]:
    ordered_live = tuple(str(item).strip().upper() for item in live_markets if str(item).strip())
    allowed = {str(item).strip().upper() for item in runtime_allowed_markets if str(item).strip()}
    if not allowed:
        return ordered_live
    return tuple(item for item in ordered_live if item in allowed)


def filter_markets_with_live_inputs(
    *,
    market_items: Sequence[MarketTopItem],
    micro_snapshot_provider: MicroSnapshotProvider | None,
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    missing_live_inputs = determine_missing_live_inputs(
        market_items=market_items,
        micro_snapshot_provider=micro_snapshot_provider,
    )
    tradeable = tuple(
        str(item.market).strip().upper()
        for item in market_items
        if not missing_live_inputs.get(str(item.market).strip().upper())
    )
    return tradeable, missing_live_inputs


def build_runtime_universe_snapshot(
    *,
    public_client: Any,
    quote: str,
    predictor: Any | None = None,
    allowed_markets: Sequence[str] | None = None,
    market_items: Sequence[MarketTopItem] | None = None,
    micro_snapshot_provider: MicroSnapshotProvider | None = None,
) -> RuntimeUniverseSnapshot:
    predictor_allowed = resolve_runtime_allowed_markets(predictor=predictor) if predictor is not None else []
    explicit_allowed = [str(item).strip().upper() for item in (allowed_markets or []) if str(item).strip()]
    resolved_allowed = tuple(explicit_allowed or predictor_allowed)
    if market_items is not None:
        ordered_items = tuple(market_items)
        live_markets = tuple(str(item.market).strip().upper() for item in ordered_items if str(item.market).strip())
        live_input_markets, missing_live_inputs = filter_markets_with_live_inputs(
            market_items=ordered_items,
            micro_snapshot_provider=micro_snapshot_provider,
        )
    else:
        live_markets = tuple(load_quote_markets(public_client=public_client, quote=quote, allowed_markets=resolved_allowed))
        live_input_markets = live_markets
        missing_live_inputs = {
            market: tuple()
            for market in live_markets
        }
    tradeable_markets = intersect_runtime_markets(
        live_markets=live_input_markets,
        runtime_allowed_markets=resolved_allowed,
    )
    return RuntimeUniverseSnapshot(
        quote=str(quote).strip().upper(),
        live_markets=live_markets,
        runtime_allowed_markets=resolved_allowed,
        tradeable_markets=tradeable_markets,
        missing_live_inputs_by_market=missing_live_inputs,
    )
