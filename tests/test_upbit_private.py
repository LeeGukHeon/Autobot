from __future__ import annotations

import pytest

from autobot.upbit.exceptions import ValidationError
from autobot.upbit.private import UpbitPrivateClient


class _StubHttpClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def request_json(self, method: str, endpoint: str, **kwargs: object) -> dict[str, object]:
        self.calls.append(
            {
                "method": method,
                "endpoint": endpoint,
                "kwargs": kwargs,
            }
        )
        return {"ok": True}


def test_open_orders_defaults_to_wait_watch() -> None:
    stub = _StubHttpClient()
    client = UpbitPrivateClient(stub)  # type: ignore[arg-type]

    payload = client.open_orders()

    assert payload == {"ok": True}
    call = stub.calls[-1]
    assert call["method"] == "GET"
    assert call["endpoint"] == "/v1/orders/open"
    assert call["kwargs"]["params"] == [("states[]", "wait"), ("states[]", "watch")]


def test_open_orders_uses_market_and_custom_states() -> None:
    stub = _StubHttpClient()
    client = UpbitPrivateClient(stub)  # type: ignore[arg-type]

    client.open_orders(market="krw-btc", states=("wait", "watch"))

    call = stub.calls[-1]
    assert call["kwargs"]["params"] == [("market", "KRW-BTC"), ("states[]", "wait"), ("states[]", "watch")]


def test_open_orders_requires_non_empty_states() -> None:
    stub = _StubHttpClient()
    client = UpbitPrivateClient(stub)  # type: ignore[arg-type]

    with pytest.raises(ValidationError):
        client.open_orders(states=())


def test_order_accepts_identifier_only() -> None:
    stub = _StubHttpClient()
    client = UpbitPrivateClient(stub)  # type: ignore[arg-type]

    client.order(identifier="AUTOBOT-1")

    call = stub.calls[-1]
    assert call["endpoint"] == "/v1/order"
    assert call["kwargs"]["params"] == [("identifier", "AUTOBOT-1")]


def test_order_requires_uuid_or_identifier() -> None:
    stub = _StubHttpClient()
    client = UpbitPrivateClient(stub)  # type: ignore[arg-type]

    with pytest.raises(ValidationError):
        client.order()


def test_cancel_order_prefers_uuid_when_both_present() -> None:
    stub = _StubHttpClient()
    client = UpbitPrivateClient(stub)  # type: ignore[arg-type]

    client.cancel_order(uuid="uuid-1", identifier="AUTOBOT-1")

    call = stub.calls[-1]
    assert call["method"] == "DELETE"
    assert call["endpoint"] == "/v1/order"
    assert call["kwargs"]["params"] == [("uuid", "uuid-1"), ("identifier", "AUTOBOT-1")]
    assert call["kwargs"]["rate_limit_group"] == "order"


def test_create_order_uses_live_orders_endpoint() -> None:
    stub = _StubHttpClient()
    client = UpbitPrivateClient(stub)  # type: ignore[arg-type]

    client.create_order(
        market="krw-btc",
        side="bid",
        ord_type="limit",
        price="1000",
        volume="0.01",
        time_in_force="ioc",
        identifier="AUTOBOT-1",
    )

    call = stub.calls[-1]
    assert call["method"] == "POST"
    assert call["endpoint"] == "/v1/orders"
    assert call["kwargs"]["json_body"] == {
        "market": "KRW-BTC",
        "side": "bid",
        "ord_type": "limit",
        "price": "1000",
        "volume": "0.01",
        "time_in_force": "ioc",
        "identifier": "AUTOBOT-1",
    }
    assert call["kwargs"]["rate_limit_group"] == "order"


def test_create_order_omits_gtc_for_limit_orders() -> None:
    stub = _StubHttpClient()
    client = UpbitPrivateClient(stub)  # type: ignore[arg-type]

    client.create_order(
        market="krw-btc",
        side="bid",
        ord_type="limit",
        price="1000",
        volume="0.01",
        time_in_force="gtc",
        identifier="AUTOBOT-1",
    )

    call = stub.calls[-1]
    assert call["kwargs"]["json_body"] == {
        "market": "KRW-BTC",
        "side": "bid",
        "ord_type": "limit",
        "price": "1000",
        "volume": "0.01",
        "identifier": "AUTOBOT-1",
    }


def test_create_order_test_mode_uses_test_endpoint() -> None:
    stub = _StubHttpClient()
    client = UpbitPrivateClient(stub)  # type: ignore[arg-type]

    client.create_order(
        market="KRW-BTC",
        side="ask",
        ord_type="limit",
        price="1000",
        volume="0.01",
        test_mode=True,
    )

    call = stub.calls[-1]
    assert call["endpoint"] == "/v1/orders/test"
    assert call["kwargs"]["rate_limit_group"] == "order-test"


def test_order_test_omits_gtc_for_limit_orders() -> None:
    stub = _StubHttpClient()
    client = UpbitPrivateClient(stub)  # type: ignore[arg-type]

    client.order_test(
        market="KRW-BTC",
        side="bid",
        ord_type="limit",
        price="1000",
        volume="0.01",
        time_in_force="gtc",
        identifier="AUTOBOT-1",
    )

    call = stub.calls[-1]
    assert call["endpoint"] == "/v1/orders/test"
    assert call["kwargs"]["json_body"] == {
        "market": "KRW-BTC",
        "side": "bid",
        "ord_type": "limit",
        "price": "1000",
        "volume": "0.01",
        "identifier": "AUTOBOT-1",
    }


def test_cancel_and_new_order_requires_prev_key() -> None:
    stub = _StubHttpClient()
    client = UpbitPrivateClient(stub)  # type: ignore[arg-type]

    with pytest.raises(ValidationError):
        client.cancel_and_new_order(
            new_identifier="AUTOBOT-NEW",
            new_price="1000",
            new_volume="0.01",
        )


def test_cancel_and_new_order_uses_expected_payload() -> None:
    stub = _StubHttpClient()
    client = UpbitPrivateClient(stub)  # type: ignore[arg-type]

    client.cancel_and_new_order(
        prev_order_uuid="prev-uuid",
        new_identifier="AUTOBOT-NEW",
        new_price="1000",
        new_volume="remain_only",
        new_time_in_force="ioc",
    )

    call = stub.calls[-1]
    assert call["method"] == "POST"
    assert call["endpoint"] == "/v1/orders/cancel_and_new"
    assert call["kwargs"]["json_body"] == {
        "prev_order_uuid": "prev-uuid",
        "new_identifier": "AUTOBOT-NEW",
        "new_ord_type": "limit",
        "new_price": "1000",
        "new_volume": "remain_only",
        "new_time_in_force": "ioc",
    }
    assert call["kwargs"]["rate_limit_group"] == "order"


def test_cancel_and_new_order_omits_gtc_for_limit_orders() -> None:
    stub = _StubHttpClient()
    client = UpbitPrivateClient(stub)  # type: ignore[arg-type]

    client.cancel_and_new_order(
        prev_order_uuid="prev-uuid",
        new_identifier="AUTOBOT-NEW",
        new_price="1000",
        new_volume="0.01",
        new_time_in_force="gtc",
    )

    call = stub.calls[-1]
    assert call["kwargs"]["json_body"] == {
        "prev_order_uuid": "prev-uuid",
        "new_identifier": "AUTOBOT-NEW",
        "new_ord_type": "limit",
        "new_price": "1000",
        "new_volume": "0.01",
    }
