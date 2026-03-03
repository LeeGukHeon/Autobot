from __future__ import annotations

from math import isclose

import polars as pl

from autobot.data.micro.resample_v1 import resample_micro_1m_to_5m


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
