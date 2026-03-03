from __future__ import annotations

import polars as pl

from autobot.data.micro.trade_aggregator_v1 import merge_trade_bars_with_precedence


def test_trade_merge_uses_ws_when_both_exist() -> None:
    ws = pl.DataFrame(
        {
            "market": ["KRW-BTC"],
            "ts_ms": [60_000],
            "trade_count": [2],
            "buy_count": [2],
            "sell_count": [0],
            "trade_volume_total": [3.0],
            "buy_volume": [3.0],
            "sell_volume": [0.0],
            "trade_imbalance": [1.0],
            "vwap": [100.0],
            "avg_trade_size": [1.5],
            "max_trade_size": [2.0],
            "last_trade_price": [101.0],
            "trade_events": [2],
            "trade_min_ts_ms": [60_010],
            "trade_max_ts_ms": [60_020],
        }
    )
    rest = pl.DataFrame(
        {
            "market": ["KRW-BTC", "KRW-ETH"],
            "ts_ms": [60_000, 60_000],
            "trade_count": [4, 1],
            "buy_count": [0, 0],
            "sell_count": [4, 1],
            "trade_volume_total": [8.0, 2.0],
            "buy_volume": [0.0, 0.0],
            "sell_volume": [8.0, 2.0],
            "trade_imbalance": [-1.0, -1.0],
            "vwap": [99.0, 80.0],
            "avg_trade_size": [2.0, 2.0],
            "max_trade_size": [4.0, 2.0],
            "last_trade_price": [98.0, 80.0],
            "trade_events": [4, 1],
            "trade_min_ts_ms": [60_011, 60_010],
            "trade_max_ts_ms": [60_099, 60_010],
        }
    )

    merged = merge_trade_bars_with_precedence(ws_frame=ws, rest_frame=rest)

    btc = merged.filter(pl.col("market") == "KRW-BTC").to_dicts()[0]
    eth = merged.filter(pl.col("market") == "KRW-ETH").to_dicts()[0]

    assert btc["trade_source"] == "ws"
    assert btc["trade_count"] == 2
    assert btc["trade_volume_total"] == 3.0

    assert eth["trade_source"] == "rest"
    assert eth["trade_count"] == 1
