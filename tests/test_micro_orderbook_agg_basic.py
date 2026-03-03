from __future__ import annotations

from math import isclose

import polars as pl

from autobot.data.micro.orderbook_aggregator_v1 import aggregate_orderbook_events_to_1m


def test_orderbook_aggregation_basic() -> None:
    events = [
        {
            "channel": "orderbook",
            "market": "KRW-BTC",
            "ts_ms": 61_000,
            "ask1_price": 101.0,
            "bid1_price": 99.0,
            "ask1_size": 1.0,
            "bid1_size": 2.0,
            "ask2_size": 1.0,
            "bid2_size": 1.0,
            "ask3_size": 0.0,
            "bid3_size": 0.0,
            "ask4_size": 0.0,
            "bid4_size": 0.0,
            "ask5_size": 0.0,
            "bid5_size": 0.0,
        },
        {
            "channel": "orderbook",
            "market": "KRW-BTC",
            "ts_ms": 62_000,
            "ask1_price": 102.0,
            "bid1_price": 100.0,
            "ask1_size": 2.0,
            "bid1_size": 1.0,
            "ask2_size": 2.0,
            "bid2_size": 1.0,
            "ask3_size": 0.0,
            "bid3_size": 0.0,
            "ask4_size": 0.0,
            "bid4_size": 0.0,
            "ask5_size": 0.0,
            "bid5_size": 0.0,
        },
    ]

    frame = aggregate_orderbook_events_to_1m(events, topk=5)
    row = frame.to_dicts()[0]

    assert row["book_events"] == 2
    assert isclose(float(row["mid_mean"]), 100.5, rel_tol=1e-8)
    assert isclose(float(row["depth_bid_top5_mean"]), 2.5, rel_tol=1e-8)
    assert isclose(float(row["depth_ask_top5_mean"]), 3.0, rel_tol=1e-8)
    assert int(row["book_min_ts_ms"]) == 61_000
    assert int(row["book_max_ts_ms"]) == 62_000
