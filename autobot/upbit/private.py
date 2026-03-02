"""Private (Exchange) Upbit REST endpoints."""

from __future__ import annotations

from .exceptions import ValidationError
from .http_client import UpbitHttpClient
from .types import JSONValue


class UpbitPrivateClient:
    def __init__(self, http_client: UpbitHttpClient) -> None:
        self._http = http_client

    def accounts(self) -> JSONValue:
        return self._http.request_json(
            "GET",
            "/v1/accounts",
            auth=True,
            rate_limit_group="default",
        )

    def chance(self, *, market: str) -> JSONValue:
        market_value = market.strip().upper()
        if not market_value:
            raise ValidationError("market is required")

        return self._http.request_json(
            "GET",
            "/v1/orders/chance",
            params=[("market", market_value)],
            auth=True,
            rate_limit_group="default",
        )

    def order_test(
        self,
        *,
        market: str,
        side: str,
        ord_type: str,
        price: str | None = None,
        volume: str | None = None,
        time_in_force: str | None = None,
        identifier: str | None = None,
    ) -> JSONValue:
        market_value = market.strip().upper()
        side_value = side.strip().lower()
        ord_type_value = ord_type.strip().lower()
        if not market_value:
            raise ValidationError("market is required")
        if side_value not in {"bid", "ask"}:
            raise ValidationError("side must be one of: bid, ask")
        if not ord_type_value:
            raise ValidationError("ord_type is required")

        body: dict[str, str] = {
            "market": market_value,
            "side": side_value,
            "ord_type": ord_type_value,
        }
        if price is not None:
            body["price"] = str(price)
        if volume is not None:
            body["volume"] = str(volume)
        if time_in_force is not None:
            body["time_in_force"] = str(time_in_force)
        if identifier is not None:
            body["identifier"] = str(identifier)

        return self._http.request_json(
            "POST",
            "/v1/orders/test",
            json_body=body,
            auth=True,
            rate_limit_group="order-test",
        )
