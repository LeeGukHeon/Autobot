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
