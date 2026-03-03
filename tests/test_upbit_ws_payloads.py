from __future__ import annotations

import pytest

from autobot.upbit.ws.models import Subscription
from autobot.upbit.ws.payloads import build_private_subscribe_payload, build_subscribe_payload


def test_build_subscribe_payload_normalizes_codes_and_format() -> None:
    payload = build_subscribe_payload(
        ticket="ticket-1",
        subscriptions=[
            Subscription(
                type="ticker",
                codes=("krw-btc", "KRW-eth", "KRW-BTC"),
                is_only_realtime=True,
            )
        ],
        fmt="simple_list",
    )

    assert payload == [
        {"ticket": "ticket-1"},
        {"type": "ticker", "codes": ["KRW-BTC", "KRW-ETH"], "is_only_realtime": True},
        {"format": "SIMPLE_LIST"},
    ]


def test_build_subscribe_payload_rejects_invalid_format() -> None:
    with pytest.raises(ValueError):
        build_subscribe_payload(
            ticket="ticket-1",
            subscriptions=[{"type": "ticker", "codes": ["KRW-BTC"]}],
            fmt="UNKNOWN",
        )


def test_build_private_subscribe_payload() -> None:
    payload = build_private_subscribe_payload(
        ticket="ticket-2",
        types=("myorder", "myasset", "myOrder"),
        fmt="simple_list",
    )

    assert payload == [
        {"ticket": "ticket-2"},
        {"type": "myOrder"},
        {"type": "myAsset"},
        {"format": "SIMPLE_LIST"},
    ]
