from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from autobot.data.micro.validate_micro_v1 import validate_micro_dataset_v1


def test_validate_micro_generates_report(tmp_path: Path) -> None:
    out_root = tmp_path / "micro_v1"
    part_dir = out_root / "tf=1m" / "market=KRW-BTC" / "date=2026-03-03"
    part_dir.mkdir(parents=True, exist_ok=True)

    frame = pl.DataFrame(
        {
            "market": ["KRW-BTC", "KRW-BTC"],
            "tf": ["1m", "1m"],
            "ts_ms": [1_020_000, 1_080_000],
            "trade_source": ["ws", "none"],
            "trade_events": [1, 0],
            "book_events": [1, 1],
            "trade_min_ts_ms": [1_020_010, None],
            "trade_max_ts_ms": [1_020_020, None],
            "book_min_ts_ms": [1_020_005, 1_080_005],
            "book_max_ts_ms": [1_020_025, 1_080_025],
            "trade_coverage_ms": [10, 0],
            "book_coverage_ms": [20, 20],
            "micro_trade_available": [True, False],
            "micro_book_available": [True, True],
            "micro_available": [True, True],
            "trade_count": [1, 0],
            "buy_count": [1, 0],
            "sell_count": [0, 0],
            "trade_volume_total": [1.0, 0.0],
            "buy_volume": [1.0, 0.0],
            "sell_volume": [0.0, 0.0],
            "trade_imbalance": [1.0, 0.0],
            "vwap": [100.0, None],
            "avg_trade_size": [1.0, None],
            "max_trade_size": [1.0, None],
            "last_trade_price": [100.0, None],
            "mid_mean": [100.0, 101.0],
            "spread_bps_mean": [10.0, 10.5],
            "depth_bid_top5_mean": [5.0, 5.0],
            "depth_ask_top5_mean": [4.0, 4.5],
            "imbalance_top5_mean": [0.1, 0.05],
            "microprice_bias_bps_mean": [0.2, 0.1],
            "book_update_count": [1, 1],
        }
    )
    frame.write_parquet(part_dir / "part-test.parquet")

    aggregate_report = {
        "parse_ok_ratio": 1.0,
        "alignment_mode": "start",
    }
    meta_dir = out_root / "_meta"
    meta_dir.mkdir(parents=True, exist_ok=True)
    (meta_dir / "aggregate_report.json").write_text(json.dumps(aggregate_report), encoding="utf-8")

    candles_root = tmp_path / "candles_v1"
    candle_dir = candles_root / "tf=1m" / "market=KRW-BTC"
    candle_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"ts_ms": [1_020_000, 1_080_000]}).write_parquet(candle_dir / "part.parquet")

    summary = validate_micro_dataset_v1(
        out_root=out_root,
        tf_set=("1m",),
        base_candles_root=candles_root,
    )

    assert summary.fail_files == 0
    assert summary.checked_files == 1
    assert summary.validate_report_file.exists()
