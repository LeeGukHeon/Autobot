from __future__ import annotations

from math import isclose

import polars as pl

from autobot.data.micro.resample_v1 import resample_micro_1m_to_5m, resample_micro_1m_to_base


def test_resample_micro_1m_to_5m() -> None:
    rows = []
    for idx in range(5):
        ts_ms = idx * 60_000
        rows.append(
            {
                "market": "KRW-BTC",
                "tf": "1m",
                "ts_ms": ts_ms,
                "trade_source": "ws" if idx == 1 else ("rest" if idx == 0 else "none"),
                "trade_events": 1 if idx in {0, 1} else 0,
                "book_events": 1 if idx in {0, 1} else 0,
                "trade_min_ts_ms": ts_ms + 10 if idx in {0, 1} else None,
                "trade_max_ts_ms": ts_ms + 20 if idx in {0, 1} else None,
                "book_min_ts_ms": ts_ms + 5 if idx in {0, 1} else None,
                "book_max_ts_ms": ts_ms + 25 if idx in {0, 1} else None,
                "trade_coverage_ms": 10 if idx in {0, 1} else 0,
                "book_coverage_ms": 20 if idx in {0, 1} else 0,
                "micro_trade_available": idx in {0, 1},
                "micro_book_available": idx in {0, 1},
                "micro_available": idx in {0, 1},
                "trade_count": 1 if idx in {0, 1} else 0,
                "buy_count": 1 if idx == 1 else 0,
                "sell_count": 1 if idx == 0 else 0,
                "trade_volume_total": 2.0 if idx in {0, 1} else 0.0,
                "buy_volume": 2.0 if idx == 1 else 0.0,
                "sell_volume": 2.0 if idx == 0 else 0.0,
                "trade_imbalance": 1.0 if idx == 1 else (-1.0 if idx == 0 else 0.0),
                "vwap": 100.0 + idx if idx in {0, 1} else None,
                "avg_trade_size": 2.0 if idx in {0, 1} else None,
                "max_trade_size": 2.0 if idx in {0, 1} else None,
                "last_trade_price": 100.0 + idx if idx in {0, 1} else None,
                "mid_mean": 100.0 + idx if idx in {0, 1} else None,
                "spread_bps_mean": 10.0 if idx in {0, 1} else None,
                "depth_bid_top5_mean": 5.0 if idx in {0, 1} else None,
                "depth_ask_top5_mean": 4.0 if idx in {0, 1} else None,
                "imbalance_top5_mean": 0.1 if idx in {0, 1} else None,
                "microprice_bias_bps_mean": 0.2 if idx in {0, 1} else None,
                "book_update_count": 1 if idx in {0, 1} else 0,
            }
        )

    frame_1m = pl.DataFrame(rows)
    frame_5m = resample_micro_1m_to_5m(frame_1m)

    assert frame_5m.height == 1
    row = frame_5m.to_dicts()[0]
    assert row["trade_source"] == "ws"
    assert row["trade_count"] == 2
    assert row["trade_events"] == 2
    assert row["book_events"] == 2
    assert isclose(float(row["trade_volume_total"]), 4.0, rel_tol=1e-8)
    assert isclose(float(row["vwap"]), 100.5, rel_tol=1e-8)


def test_resample_micro_1m_to_5m_keeps_schema_when_first_rows_are_null() -> None:
    frame_1m = pl.DataFrame(
        {
            "market": ["KRW-BTC", "KRW-BTC"],
            "tf": ["1m", "1m"],
            "ts_ms": [0, 60_000],
            "trade_source": ["none", "ws"],
            "trade_events": [0, 1],
            "book_events": [1, 1],
            "trade_min_ts_ms": [None, 60_010],
            "trade_max_ts_ms": [None, 60_020],
            "book_min_ts_ms": [5, 60_005],
            "book_max_ts_ms": [25, 60_025],
            "trade_coverage_ms": [0, 10],
            "book_coverage_ms": [20, 20],
            "micro_trade_available": [False, True],
            "micro_book_available": [True, True],
            "micro_available": [True, True],
            "trade_count": [0, 1],
            "buy_count": [0, 1],
            "sell_count": [0, 0],
            "trade_volume_total": [0.0, 1.0],
            "buy_volume": [0.0, 1.0],
            "sell_volume": [0.0, 0.0],
            "trade_imbalance": [0.0, 1.0],
            "vwap": [None, 100.0],
            "avg_trade_size": [None, 1.0],
            "max_trade_size": [None, 1.0],
            "last_trade_price": [None, 100.0],
            "mid_mean": [100.0, 101.0],
            "spread_bps_mean": [10.0, 11.0],
            "depth_bid_top5_mean": [5.0, 6.0],
            "depth_ask_top5_mean": [4.0, 5.0],
            "imbalance_top5_mean": [0.1, 0.09],
            "microprice_bias_bps_mean": [0.2, 0.1],
            "book_update_count": [1, 1],
        }
    )

    frame_5m = resample_micro_1m_to_5m(frame_1m)

    assert frame_5m.height == 1
    assert frame_5m.schema["trade_min_ts_ms"] == pl.Int64
    row = frame_5m.row(0, named=True)
    assert row["trade_min_ts_ms"] == 60_010


def test_resample_micro_1m_to_base_supports_one_minute_passthrough() -> None:
    frame_1m = pl.DataFrame(
        {
            "market": ["KRW-BTC"],
            "tf": ["1m"],
            "ts_ms": [60_000],
            "trade_source": ["ws"],
            "trade_events": [1],
            "book_events": [1],
            "trade_min_ts_ms": [60_010],
            "trade_max_ts_ms": [60_020],
            "book_min_ts_ms": [60_005],
            "book_max_ts_ms": [60_025],
            "trade_coverage_ms": [10],
            "book_coverage_ms": [20],
            "micro_trade_available": [True],
            "micro_book_available": [True],
            "micro_available": [True],
            "trade_count": [1],
            "buy_count": [1],
            "sell_count": [0],
            "trade_volume_total": [1.0],
            "buy_volume": [1.0],
            "sell_volume": [0.0],
            "trade_imbalance": [1.0],
            "vwap": [100.0],
            "avg_trade_size": [1.0],
            "max_trade_size": [1.0],
            "last_trade_price": [100.0],
            "mid_mean": [101.0],
            "spread_bps_mean": [11.0],
            "depth_bid_top5_mean": [6.0],
            "depth_ask_top5_mean": [5.0],
            "imbalance_top5_mean": [0.09],
            "microprice_bias_bps_mean": [0.1],
            "book_update_count": [1],
        }
    )

    frame_out = resample_micro_1m_to_base(frame_1m, base_tf="1m")

    assert frame_out.height == 1
    row = frame_out.row(0, named=True)
    assert row["tf"] == "1m"
    assert row["ts_ms"] == 60_000


def test_resample_micro_1m_to_base_supports_fifteen_minute_rollup() -> None:
    rows = []
    for idx in range(15):
        ts_ms = idx * 60_000
        rows.append(
            {
                "market": "KRW-BTC",
                "tf": "1m",
                "ts_ms": ts_ms,
                "trade_source": "ws",
                "trade_events": 1,
                "book_events": 1,
                "trade_min_ts_ms": ts_ms + 10,
                "trade_max_ts_ms": ts_ms + 20,
                "book_min_ts_ms": ts_ms + 5,
                "book_max_ts_ms": ts_ms + 25,
                "trade_coverage_ms": 10,
                "book_coverage_ms": 20,
                "micro_trade_available": True,
                "micro_book_available": True,
                "micro_available": True,
                "trade_count": 1,
                "buy_count": 1,
                "sell_count": 0,
                "trade_volume_total": 2.0,
                "buy_volume": 2.0,
                "sell_volume": 0.0,
                "trade_imbalance": 1.0,
                "vwap": 100.0 + idx,
                "avg_trade_size": 2.0,
                "max_trade_size": 2.0,
                "last_trade_price": 100.0 + idx,
                "mid_mean": 100.0 + idx,
                "spread_bps_mean": 10.0,
                "depth_bid_top5_mean": 5.0,
                "depth_ask_top5_mean": 4.0,
                "imbalance_top5_mean": 0.1,
                "microprice_bias_bps_mean": 0.2,
                "book_update_count": 1,
            }
        )
    frame_1m = pl.DataFrame(rows)

    frame_15m = resample_micro_1m_to_base(frame_1m, base_tf="15m")

    assert frame_15m.height == 1
    row = frame_15m.row(0, named=True)
    assert row["tf"] == "15m"
    assert row["ts_ms"] == 0
    assert row["trade_count"] == 15
