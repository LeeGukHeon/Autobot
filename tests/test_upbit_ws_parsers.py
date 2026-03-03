from __future__ import annotations

import json

from autobot.upbit.ws.parsers import decode_ws_message, parse_private_event, parse_ticker_event


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
            "i": "AUTOBOT-1",
            "sd": "bid",
            "ot": "limit",
            "st": "wait",
            "p": "100000000",
            "v": "0.01",
            "ev": "0.005",
            "tms": 1700000007000,
        }
    ]
    event = parse_private_event(payload)
    assert event is not None
    assert event.stream_type == "myOrder"
    assert event.market == "KRW-BTC"
    assert event.uuid == "order-1"
    assert event.identifier == "AUTOBOT-1"
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
