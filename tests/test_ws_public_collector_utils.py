from __future__ import annotations

import random

from autobot.data.collect.ws_public_collector import (
    _normalize_public_ws_row,
    _orderbook_state_snapshot,
    _reconnect_delay_sec,
    _should_write_orderbook,
)


def test_normalize_trade_default_payload() -> None:
    row = _normalize_public_ws_row(
        message={
            "type": "trade",
            "code": "KRW-BTC",
            "trade_timestamp": 1_700_000_000_000,
            "timestamp": 1_700_000_000_100,
            "trade_price": 100.5,
            "trade_volume": 0.01,
            "ask_bid": "BID",
            "sequential_id": 123,
        },
        orderbook_topk=5,
        orderbook_level=0,
        collected_at_ms=1_700_000_000_200,
    )
    assert row is not None
    assert row["channel"] == "trade"
    assert row["market"] == "KRW-BTC"
    assert row["trade_ts_ms"] == 1_700_000_000_000
    assert row["price"] == 100.5
    assert row["ask_bid"] == "BID"


def test_normalize_ticker_default_payload() -> None:
    row = _normalize_public_ws_row(
        message={
            "type": "ticker",
            "code": "KRW-BTC",
            "timestamp": 1_700_000_000_000,
            "trade_price": 100.5,
            "acc_trade_price_24h": 123456789.0,
            "market_state": "ACTIVE",
            "market_warning": "NONE",
        },
        orderbook_topk=5,
        orderbook_level=0,
        collected_at_ms=1_700_000_000_200,
    )
    assert row is not None
    assert row["channel"] == "ticker"
    assert row["market"] == "KRW-BTC"
    assert row["ts_ms"] == 1_700_000_000_000
    assert row["trade_price"] == 100.5
    assert row["acc_trade_price_24h"] == 123456789.0


def test_normalize_orderbook_simple_payload() -> None:
    row = _normalize_public_ws_row(
        message={
            "ty": "orderbook",
            "cd": "KRW-ETH",
            "tms": 1_700_000_010_000,
            "tas": 10.0,
            "tbs": 9.0,
            "obu": [
                {"ap": 200.1, "as": 1.1, "bp": 199.9, "bs": 1.2},
                {"ap": 200.2, "as": 1.3, "bp": 199.8, "bs": 1.4},
            ],
        },
        orderbook_topk=5,
        orderbook_level=0,
        collected_at_ms=1_700_000_010_500,
    )
    assert row is not None
    assert row["channel"] == "orderbook"
    assert row["market"] == "KRW-ETH"
    assert row["topk"] == 5
    assert row["ask1_price"] == 200.1
    assert row["bid1_price"] == 199.9
    assert row["ask5_price"] is None
    assert row["bid5_price"] is None


def test_orderbook_downsample_writes_on_price_change() -> None:
    record = {
        "channel": "orderbook",
        "market": "KRW-BTC",
        "ts_ms": 1_700_000_020_000,
        "ask1_price": 101.0,
        "ask1_size": 1.0,
        "bid1_price": 100.0,
        "bid1_size": 2.0,
    }
    assert _should_write_orderbook(
        record=record,
        state=None,
        min_write_interval_ms=200,
        spread_bps_threshold=0.5,
        top1_size_change_threshold=0.2,
    )

    state = _orderbook_state_snapshot(record)
    same_record = dict(record)
    same_record["ts_ms"] = record["ts_ms"] + 100
    assert not _should_write_orderbook(
        record=same_record,
        state=state,
        min_write_interval_ms=200,
        spread_bps_threshold=0.5,
        top1_size_change_threshold=0.2,
    )

    changed_price = dict(same_record)
    changed_price["bid1_price"] = 100.1
    assert _should_write_orderbook(
        record=changed_price,
        state=state,
        min_write_interval_ms=200,
        spread_bps_threshold=0.5,
        top1_size_change_threshold=0.2,
    )


def test_reconnect_delay_backoff_with_jitter() -> None:
    rng = random.Random(0)
    d1 = _reconnect_delay_sec(0, rng=rng)
    d2 = _reconnect_delay_sec(1, rng=rng)
    d6 = _reconnect_delay_sec(6, rng=rng)

    assert 1.0 <= d1 <= 1.5
    assert 2.0 <= d2 <= 2.5
    assert 32.0 <= d6 <= 32.5
