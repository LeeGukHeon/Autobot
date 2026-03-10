from __future__ import annotations

from autobot.execution.direct_gateway import DirectRestExecutionGateway
from autobot.execution.intent import new_order_intent
from autobot.upbit.exceptions import ClientRequestError


class _DummyPrivateClient:
    def __init__(self) -> None:
        self.created = []
        self.cancelled = []
        self.replaced = []
        self.tested = []
        self.orders = []

    def accounts(self):
        return [{"currency": "KRW"}]

    def create_order(self, **kwargs):
        self.created.append(kwargs)
        return {"uuid": "u-1", "identifier": kwargs.get("identifier")}

    def order_test(self, **kwargs):
        self.tested.append(kwargs)
        return {"uuid": "u-test", "identifier": kwargs.get("identifier")}

    def cancel_order(self, **kwargs):
        self.cancelled.append(kwargs)
        return {"uuid": kwargs.get("uuid"), "identifier": kwargs.get("identifier")}

    def cancel_and_new_order(self, **kwargs):
        self.replaced.append(kwargs)
        return {
            "cancelled_order_uuid": kwargs.get("prev_order_uuid"),
            "new_order_uuid": "u-2",
            "new_identifier": kwargs.get("new_identifier"),
        }

    def order(self, **kwargs):
        self.orders.append(kwargs)
        return {
            "uuid": kwargs.get("uuid") or "u-1",
            "identifier": kwargs.get("identifier"),
            "remaining_volume": "0.1",
        }


def test_direct_gateway_submit_intent_success() -> None:
    client = _DummyPrivateClient()
    gateway = DirectRestExecutionGateway(client=client)
    intent = new_order_intent(
        market="KRW-BTC",
        side="bid",
        price=12345.0,
        volume=0.001,
        reason_code="TEST",
    )

    result = gateway.submit_intent(intent=intent, identifier="AUTOBOT-1")

    assert result.accepted is True
    assert result.upbit_uuid == "u-1"
    assert client.created[0]["identifier"] == "AUTOBOT-1"


def test_direct_gateway_submit_intent_reject_on_upbit_error() -> None:
    class _RejectingClient(_DummyPrivateClient):
        def create_order(self, **kwargs):
            raise ClientRequestError("bad request", status_code=400, error_name="validation_error")

    gateway = DirectRestExecutionGateway(client=_RejectingClient())
    intent = new_order_intent(
        market="KRW-BTC",
        side="bid",
        price=12345.0,
        volume=0.001,
        reason_code="TEST",
    )

    result = gateway.submit_intent(intent=intent, identifier="AUTOBOT-1")

    assert result.accepted is False
    assert "bad request" in result.reason


def test_direct_gateway_replace_order_success() -> None:
    client = _DummyPrivateClient()
    gateway = DirectRestExecutionGateway(client=client)

    result = gateway.replace_order(
        intent_id="intent-1",
        prev_order_uuid="u-1",
        prev_order_identifier=None,
        new_identifier="AUTOBOT-2",
        new_price_str="1000",
        new_volume_str="0.1",
        new_time_in_force="gtc",
    )

    assert result.accepted is True
    assert result.new_order_uuid == "u-2"
    assert client.replaced[0]["new_identifier"] == "AUTOBOT-2"


def test_direct_gateway_replace_order_resolves_remain_only_and_nested_lookup() -> None:
    class _NestedReplaceClient(_DummyPrivateClient):
        def cancel_and_new_order(self, **kwargs):
            self.replaced.append(kwargs)
            return {
                "cancelled_order_uuid": kwargs.get("prev_order_uuid"),
                "result": {
                    "uuid": "u-3",
                    "identifier": kwargs.get("new_identifier"),
                },
            }

    client = _NestedReplaceClient()
    gateway = DirectRestExecutionGateway(client=client)

    result = gateway.replace_order(
        intent_id="intent-2",
        prev_order_uuid="u-1",
        prev_order_identifier=None,
        new_identifier="AUTOBOT-3",
        new_price_str="1001",
        new_volume_str="remain_only",
        new_time_in_force="gtc",
    )

    assert result.accepted is True
    assert result.new_order_uuid == "u-3"
    assert client.orders[0]["uuid"] == "u-1"
    assert client.replaced[0]["new_volume"] == "0.1"
