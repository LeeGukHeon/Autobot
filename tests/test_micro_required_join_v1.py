from __future__ import annotations

from datetime import datetime, timezone

import polars as pl

from autobot.features.micro_required_join_v1 import join_micro_required, load_market_micro_for_base


def test_micro_required_join_drops_rows_without_micro() -> None:
    base = pl.DataFrame(
        {
            "ts_ms": [0, 300_000, 600_000],
            "x": [1.0, 2.0, 3.0],
        }
    )
    micro = pl.DataFrame(
        {
            "market": ["KRW-BTC", "KRW-BTC"],
            "tf": ["5m", "5m"],
            "ts_ms": [0, 600_000],
            "trade_source": ["ws", "rest"],
            "trade_events": [3, 2],
            "book_events": [4, 1],
            "trade_coverage_ms": [240_000, 200_000],
            "book_coverage_ms": [250_000, 180_000],
            "micro_trade_available": [True, True],
            "micro_book_available": [True, True],
            "micro_available": [True, True],
            "trade_volume_total": [10.0, 5.0],
            "buy_volume": [6.0, 1.0],
            "sell_volume": [4.0, 4.0],
            "spread_bps_mean": [1.2, 2.4],
        }
    )

    result = join_micro_required(base_frame=base, micro_frame=micro, micro_tf_used="5m")

    assert result.rows_before == 3
    assert result.rows_after == 2
    assert result.rows_dropped_no_micro == 1
    assert "m_spread_proxy" in result.frame.columns
    assert "m_trade_buy_ratio" in result.frame.columns
    assert "m_source_ws" in result.frame.columns


def test_load_market_micro_for_base_resamples_one_minute_to_requested_base_tf(tmp_path) -> None:
    micro_root = tmp_path / "micro_v1"
    part_dir = micro_root / "tf=1m" / "market=KRW-BTC" / "date=2026-01-01"
    part_dir.mkdir(parents=True, exist_ok=True)
    start_ts_ms = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    rows = []
    for idx in range(15):
        ts_ms = start_ts_ms + (idx * 60_000)
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
    pl.DataFrame(rows).write_parquet(part_dir / "part-000.parquet")

    frame, micro_tf_used = load_market_micro_for_base(
        micro_root=micro_root,
        market="KRW-BTC",
        base_tf="15m",
        from_ts_ms=start_ts_ms,
        to_ts_ms=start_ts_ms + (14 * 60_000),
    )

    assert micro_tf_used == "1m_resampled"
    assert frame.height == 1
    row = frame.row(0, named=True)
    assert row["tf"] == "15m"
    assert row["ts_ms"] == start_ts_ms
