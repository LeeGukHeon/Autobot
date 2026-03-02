from __future__ import annotations

import json

from autobot.upbit.ws.parsers import decode_ws_message, parse_ticker_event


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

