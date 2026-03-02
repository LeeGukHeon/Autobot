"""Ticker-based scanner for top-N markets by 24h accumulated trade price."""

from __future__ import annotations

from dataclasses import dataclass

from autobot.upbit.ws.models import TickerEvent


@dataclass(frozen=True)
class MarketTopItem:
    market: str
    ts_ms: int
    trade_price: float
    acc_trade_price_24h: float
    market_state: str | None
    market_warning: str | None


class TopTradeValueScanner:
    def __init__(self) -> None:
        self._state: dict[str, MarketTopItem] = {}

    def update(self, event: TickerEvent) -> None:
        self._state[event.market] = MarketTopItem(
            market=event.market,
            ts_ms=event.ts_ms,
            trade_price=event.trade_price,
            acc_trade_price_24h=event.acc_trade_price_24h,
            market_state=event.market_state,
            market_warning=event.market_warning,
        )

    def top_n(
        self,
        *,
        n: int = 20,
        quote: str | None = None,
        include_caution: bool = True,
        include_inactive: bool = True,
    ) -> list[MarketTopItem]:
        quote_prefix = f"{quote.strip().upper()}-" if quote else None
        items: list[MarketTopItem] = []
        for item in self._state.values():
            if quote_prefix and not item.market.startswith(quote_prefix):
                continue
            if not include_caution and (item.market_warning or "").upper() == "CAUTION":
                continue
            if not include_inactive and (item.market_state or "").upper() not in {"ACTIVE"}:
                continue
            items.append(item)

        items.sort(key=lambda value: value.acc_trade_price_24h, reverse=True)
        return items[: max(int(n), 0)]

    def size(self) -> int:
        return len(self._state)

