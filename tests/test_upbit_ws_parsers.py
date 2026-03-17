from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

from autobot.upbit.ws.models import MyAssetEvent, MyOrderEvent
from autobot.upbit.ws.parsers import decode_ws_message, parse_private_event, parse_private_events, parse_ticker_event
from autobot.upbit.ws.private_client import UpbitWebSocketPrivateClient


def test_parse_ticker_event_default_payload() -> None:
    payload = {
        "type": "ticker",
        "code": "KRW-BTC",
        "timestamp": 1700000000000,
        "trade_price": 100.5,
        "acc_trade_price_24h": 123456789.0,
    }
    event = parse_ticker_event(payload)
    assert event is not None
    assert event.market == "KRW-BTC"
    assert event.ts_ms == 1700000000000
    assert event.trade_price == 100.5
    assert event.acc_trade_price_24h == 123456789.0
    assert event.stream_type == "ticker"


def test_parse_ticker_event_simple_list_payload() -> None:
    payload = [
        {
            "ty": "ticker",
            "cd": "KRW-ETH",
            "tms": 1700000005000,
            "tp": 200.25,
            "atp24h": 987654321.0,
            "ms": "ACTIVE",
            "mw": "NONE",
        }
    ]
    event = parse_ticker_event(payload)
    assert event is not None
    assert event.market == "KRW-ETH"
    assert event.ts_ms == 1700000005000
    assert event.trade_price == 200.25
    assert event.acc_trade_price_24h == 987654321.0
    assert event.market_state == "ACTIVE"
    assert event.market_warning == "NONE"


def test_decode_ws_message_accepts_bytes() -> None:
    raw = json.dumps({"code": "KRW-BTC", "timestamp": 1, "trade_price": 1.0, "acc_trade_price_24h": 2.0}).encode(
        "utf-8"
    )
    decoded = decode_ws_message(raw)
    assert isinstance(decoded, dict)
    assert decoded["code"] == "KRW-BTC"


def test_parse_private_order_event_simple_list_payload() -> None:
    payload = [
        {
            "ty": "myOrder",
            "cd": "KRW-BTC",
            "uid": "order-1",
            "id": "AUTOBOT-1",
            "ab": "BID",
            "ot": "limit",
            "s": "wait",
            "p": "100000000",
            "v": "0.01",
            "ev": "0.005",
            "ttms": 1700000007000,
            "st": "REALTIME",
        }
    ]
    event = parse_private_event(payload)
    assert event is not None
    assert event.stream_type == "REALTIME"
    assert event.market == "KRW-BTC"
    assert event.uuid == "order-1"
    assert event.identifier == "AUTOBOT-1"
    assert event.side == "BID"
    assert event.state == "wait"
    assert event.ts_ms == 1700000007000
    assert event.executed_volume == 0.005


def test_parse_private_asset_event_default_payload() -> None:
    payload = {
        "type": "myAsset",
        "currency": "eth",
        "balance": "0.10000000",
        "locked": "0.02000000",
        "avg_buy_price": "3000000",
        "timestamp": 1700000009000,
    }
    event = parse_private_event(payload)
    assert event is not None
    assert event.stream_type == "myAsset"
    assert event.currency == "ETH"
    assert event.balance == 0.1
    assert event.locked == 0.02


def test_parse_private_asset_event_supports_official_assets_array_payload() -> None:
    payload = {
        "type": "myAsset",
        "asset_timestamp": 1700000010000,
        "stream_type": "REALTIME",
        "assets": [
            {
                "currency": "KRW",
                "balance": "120000.0",
                "locked": "5000.0",
                "avg_buy_price": "0",
            },
            {
                "currency": "btc",
                "balance": "0.01000000",
                "locked": "0.00200000",
                "avg_buy_price": "100000000",
            },
        ],
    }
    events = parse_private_events(payload)

    assert len(events) == 2
    assert all(isinstance(item, MyAssetEvent) for item in events)
    assert events[0].currency == "KRW"
    assert events[0].balance == 120000.0
    assert events[0].locked == 5000.0
    assert events[0].ts_ms == 1700000010000
    assert events[1].currency == "BTC"
    assert events[1].avg_buy_price == 100000000.0
    assert events[1].stream_type == "REALTIME"


def test_parse_private_order_event_supports_official_named_fields() -> None:
    payload = {
        "type": "myOrder",
        "code": "KRW-XRP",
        "uuid": "order-2",
        "identifier": "AUTOBOT-2",
        "ask_bid": "ASK",
        "order_type": "limit",
        "state": "done",
        "price": "3200",
        "volume": "12.5",
        "executed_volume": "12.5",
        "order_timestamp": 1700000020000,
        "stream_type": "SNAPSHOT",
    }
    event = parse_private_event(payload)

    assert isinstance(event, MyOrderEvent)
    assert event.market == "KRW-XRP"
    assert event.identifier == "AUTOBOT-2"
    assert event.side == "ASK"
    assert event.ord_type == "limit"
    assert event.state == "done"
    assert event.ts_ms == 1700000020000
    assert event.stream_type == "SNAPSHOT"


def test_private_ws_client_expands_official_myasset_payload_into_multiple_events(monkeypatch) -> None:
    class _FakeWebSocket:
        def __init__(self) -> None:
            self._payloads = [
                json.dumps(
                    {
                        "type": "myAsset",
                        "asset_timestamp": 1700000010000,
                        "stream_type": "REALTIME",
                        "assets": [
                            {"currency": "KRW", "balance": "120000.0", "locked": "5000.0", "avg_buy_price": "0"},
                            {"currency": "BTC", "balance": "0.01", "locked": "0.002", "avg_buy_price": "100000000"},
                        ],
                    }
                )
            ]

        async def recv(self):  # noqa: ANN201
            if self._payloads:
                return self._payloads.pop(0)
            raise asyncio.CancelledError

        async def send(self, payload):  # noqa: ANN001, ANN201
            _ = payload

        def ping(self):  # noqa: ANN201
            async def _pong():
                return None

            return _pong()

    class _FakeConnectCtx:
        async def __aenter__(self):  # noqa: ANN201
            return _FakeWebSocket()

        async def __aexit__(self, exc_type, exc, tb):  # noqa: ANN001, ANN201
            return False

    monkeypatch.setattr("autobot.upbit.ws.private_client._connect_private_websocket", lambda *args, **kwargs: _FakeConnectCtx())

    settings = SimpleNamespace(
        private_url="wss://example.invalid",
        format="DEFAULT",
        ratelimit=SimpleNamespace(connect_rps=10, message_rps=10, message_rpm=600),
        reconnect=SimpleNamespace(enabled=False, base_delay_ms=10, max_delay_ms=10, jitter_ms=0),
        keepalive=SimpleNamespace(ping_interval_sec=30.0, ping_timeout_sec=5.0, allow_text_ping=True),
    )
    credentials = SimpleNamespace(access_key="a", secret_key="b")
    client = UpbitWebSocketPrivateClient(settings, credentials)

    async def _collect() -> list[MyOrderEvent | MyAssetEvent]:
        results: list[MyOrderEvent | MyAssetEvent] = []
        async for event in client.stream_private(channels=("myAsset",), duration_sec=0.01):
            results.append(event)
            if len(results) >= 2:
                break
        return results

    events = asyncio.run(_collect())

    assert len(events) == 2
    assert all(isinstance(item, MyAssetEvent) for item in events)
    assert [item.currency for item in events] == ["KRW", "BTC"]
