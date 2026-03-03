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
