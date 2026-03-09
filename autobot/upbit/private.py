"""Private (Exchange) Upbit REST endpoints."""

from __future__ import annotations

from collections.abc import Sequence

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

    def create_order(
        self,
        *,
        market: str,
        side: str,
        ord_type: str,
        price: str | None = None,
        volume: str | None = None,
        time_in_force: str | None = None,
        identifier: str | None = None,
        test_mode: bool = False,
    ) -> JSONValue:
        market_value = market.strip().upper()
        side_value = side.strip().lower()
        ord_type_value = ord_type.strip().lower()
        tif_value = str(time_in_force).strip().lower() if time_in_force is not None else None
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
        if tif_value:
            body["time_in_force"] = tif_value
        if identifier is not None:
            identifier_value = str(identifier).strip()
            if identifier_value:
                body["identifier"] = identifier_value

        return self._http.request_json(
            "POST",
            "/v1/orders/test" if bool(test_mode) else "/v1/orders",
            json_body=body,
            auth=True,
            rate_limit_group="order-test" if bool(test_mode) else "order",
        )

    def open_orders(
        self,
        *,
        market: str | None = None,
        states: Sequence[str] | None = None,
    ) -> JSONValue:
        params: list[tuple[str, str]] = []
        if market is not None:
            market_value = market.strip().upper()
            if not market_value:
                raise ValidationError("market must not be blank")
            params.append(("market", market_value))

        raw_states = ("wait", "watch") if states is None else states
        states_value = tuple(str(state).strip().lower() for state in raw_states if str(state).strip())
        if not states_value:
            raise ValidationError("states must include at least one value")
        for state in states_value:
            params.append(("states[]", state))

        return self._http.request_json(
            "GET",
            "/v1/orders/open",
            params=params,
            auth=True,
            rate_limit_group="default",
        )

    def order(
        self,
        *,
        uuid: str | None = None,
        identifier: str | None = None,
    ) -> JSONValue:
        uuid_value = str(uuid or "").strip()
        identifier_value = str(identifier or "").strip()
        if not uuid_value and not identifier_value:
            raise ValidationError("uuid or identifier is required")

        params: list[tuple[str, str]] = []
        if uuid_value:
            params.append(("uuid", uuid_value))
        if identifier_value:
            params.append(("identifier", identifier_value))

        return self._http.request_json(
            "GET",
            "/v1/order",
            params=params,
            auth=True,
            rate_limit_group="default",
        )

    def cancel_order(
        self,
        *,
        uuid: str | None = None,
        identifier: str | None = None,
    ) -> JSONValue:
        uuid_value = str(uuid or "").strip()
        identifier_value = str(identifier or "").strip()
        if not uuid_value and not identifier_value:
            raise ValidationError("uuid or identifier is required")

        params: list[tuple[str, str]] = []
        if uuid_value:
            params.append(("uuid", uuid_value))
        if identifier_value:
            params.append(("identifier", identifier_value))

        return self._http.request_json(
            "DELETE",
            "/v1/order",
            params=params,
            auth=True,
            rate_limit_group="order",
        )

    def cancel_and_new_order(
        self,
        *,
        prev_order_uuid: str | None = None,
        prev_order_identifier: str | None = None,
        new_identifier: str,
        new_price: str,
        new_volume: str,
        new_time_in_force: str | None = None,
    ) -> JSONValue:
        prev_uuid_value = str(prev_order_uuid or "").strip()
        prev_identifier_value = str(prev_order_identifier or "").strip()
        new_identifier_value = str(new_identifier).strip()
        new_price_value = str(new_price).strip()
        new_volume_value = str(new_volume).strip()
        new_tif_value = str(new_time_in_force or "").strip().lower()
        if not prev_uuid_value and not prev_identifier_value:
            raise ValidationError("prev_order_uuid or prev_order_identifier is required")
        if not new_identifier_value:
            raise ValidationError("new_identifier is required")
        if not new_price_value:
            raise ValidationError("new_price is required")
        if not new_volume_value:
            raise ValidationError("new_volume is required")

        body: dict[str, str] = {
            "new_identifier": new_identifier_value,
            "new_ord_type": "limit",
            "new_price": new_price_value,
            "new_volume": new_volume_value,
        }
        if prev_uuid_value:
            body["prev_order_uuid"] = prev_uuid_value
        else:
            body["prev_order_identifier"] = prev_identifier_value
        if new_tif_value:
            body["new_time_in_force"] = new_tif_value

        return self._http.request_json(
            "POST",
            "/v1/orders/cancel_and_new",
            json_body=body,
            auth=True,
            rate_limit_group="order",
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
