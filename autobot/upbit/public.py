"""Public (Quotation) Upbit REST endpoints."""

from __future__ import annotations

from typing import Sequence

from .exceptions import ValidationError
from .http_client import UpbitHttpClient
from .types import JSONValue


class UpbitPublicClient:
    def __init__(self, http_client: UpbitHttpClient) -> None:
        self._http = http_client

    def markets(self, *, is_details: bool = False) -> JSONValue:
        params = [("isDetails", str(is_details).lower())]
        return self._http.request_json(
            "GET",
            "/v1/market/all",
            params=params,
            auth=False,
            rate_limit_group="market",
        )

    def ticker(self, markets: Sequence[str]) -> JSONValue:
        normalized = [market.strip().upper() for market in markets if market.strip()]
        if not normalized:
            raise ValidationError("markets is required")
        return self._http.request_json(
            "GET",
            "/v1/ticker",
            params=[("markets", ",".join(normalized))],
            auth=False,
            rate_limit_group="ticker",
        )

    def candles_minutes(
        self,
        *,
        market: str,
        tf_min: int,
        count: int = 10,
        to: str | None = None,
    ) -> JSONValue:
        if tf_min <= 0:
            raise ValidationError("tf_min must be positive")
        market_value = market.strip().upper()
        if not market_value:
            raise ValidationError("market is required")

        params: list[tuple[str, str | int]] = [("market", market_value), ("count", max(count, 1))]
        if to:
            params.append(("to", to))

        return self._http.request_json(
            "GET",
            f"/v1/candles/minutes/{tf_min}",
            params=params,
            auth=False,
            rate_limit_group="candle",
        )

    def trades_ticks(
        self,
        *,
        market: str,
        count: int = 200,
        cursor: int | str | None = None,
        days_ago: int | None = None,
    ) -> JSONValue:
        market_value = market.strip().upper()
        if not market_value:
            raise ValidationError("market is required")

        count_value = max(min(int(count), 200), 1)
        params: list[tuple[str, str | int]] = [("market", market_value), ("count", count_value)]

        if cursor is not None:
            cursor_value = str(cursor).strip()
            if not cursor_value:
                raise ValidationError("cursor must not be empty")
            params.append(("cursor", cursor_value))

        if days_ago is not None:
            days_ago_value = int(days_ago)
            if days_ago_value < 1 or days_ago_value > 7:
                raise ValidationError("days_ago must be between 1 and 7")
            params.append(("daysAgo", days_ago_value))

        return self._http.request_json(
            "GET",
            "/v1/trades/ticks",
            params=params,
            auth=False,
            rate_limit_group="trade",
        )

    def orderbook_instruments(self, markets: Sequence[str]) -> JSONValue:
        normalized = [market.strip().upper() for market in markets if market.strip()]
        if not normalized:
            raise ValidationError("markets is required")

        return self._http.request_json(
            "GET",
            "/v1/orderbook/instruments",
            params=[("markets", ",".join(normalized))],
            auth=False,
            rate_limit_group="orderbook",
        )
