from __future__ import annotations

import polars as pl

from autobot.data.sources.completeness import TradeCoverageRequest, summarize_trade_coverage
from autobot.data.sources.trades.build_raw_trade_v1 import RawTradeBuildOptions, build_raw_trade_v1_dataset
from autobot.data.sources.trades.writer import read_raw_trade_part_file
from autobot.data.collect.ticks_writer import write_ticks_partitions
from autobot.data.collect.ws_public_writer import WsRawRotatingWriter


def test_build_raw_trade_v1_dataset_merges_ws_and_rest_preferring_ws(tmp_path) -> None:
    raw_ws_root = tmp_path / "raw_ws"
    raw_ticks_root = tmp_path / "raw_ticks"
    out_root = tmp_path / "raw_trade_v1"

    writer = WsRawRotatingWriter(raw_root=raw_ws_root, run_id="ws-run", rotate_sec=3600)
    writer.write(
        channel="trade",
        row={
            "channel": "trade",
            "market": "KRW-BTC",
            "trade_ts_ms": 1_712_713_200_000,
            "recv_ts_ms": 1_712_713_200_010,
            "price": 101.0,
            "volume": 0.2,
            "ask_bid": "BID",
            "sequential_id": 1001,
            "source": "ws",
            "collected_at_ms": 1_712_713_200_020,
        },
        event_ts_ms=1_712_713_200_000,
    )
    writer.close()

    write_ticks_partitions(
        raw_root=raw_ticks_root,
        ticks=[
            {
                "market": "KRW-BTC",
                "timestamp_ms": 1_712_713_200_000,
                "trade_price": 101.0,
                "trade_volume": 0.2,
                "ask_bid": "BID",
                "sequential_id": 1001,
                "days_ago": 1,
                "collected_at_ms": 1_712_713_200_100,
            },
            {
                "market": "KRW-BTC",
                "timestamp_ms": 1_712_713_201_000,
                "trade_price": 102.0,
                "trade_volume": 0.3,
                "ask_bid": "ASK",
                "sequential_id": 1002,
                "days_ago": 1,
                "collected_at_ms": 1_712_713_201_100,
            },
        ],
        run_id="rest-run",
    )

    summary = build_raw_trade_v1_dataset(
        RawTradeBuildOptions(
            raw_ws_root=raw_ws_root,
            raw_ticks_root=raw_ticks_root,
            out_root=out_root,
            meta_dir=out_root / "_meta",
            start="2024-04-10",
            end="2024-04-10",
            markets=("KRW-BTC",),
        )
    )

    assert summary.built_pairs == 1
    assert summary.ws_rows_total == 1
    assert summary.rest_rows_total == 2
    assert summary.merged_rows_total == 2

    manifest = pl.read_parquet(summary.manifest_file)
    assert manifest.height == 1
    assert manifest["source_ws_rows"][0] == 1
    assert manifest["source_rest_rows"][0] == 2
    assert manifest["source_merged_rows"][0] == 2

    part_file = out_root / "date=2024-04-10" / "market=KRW-BTC"
    part_paths = sorted(part_file.glob("*.jsonl.zst"))
    rows = read_raw_trade_part_file(part_paths[0])
    assert len(rows) == 2
    assert rows[0]["source"] == "ws"
    assert rows[0]["recv_ts_ms"] == 1_712_713_200_010
    assert rows[1]["source"] == "rest"


def test_trade_coverage_reads_raw_trade_manifest(tmp_path) -> None:
    out_root = tmp_path / "raw_trade_v1"
    meta_root = out_root / "_meta"
    meta_root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "run_id": ["run-1", "run-1"],
            "date": ["2026-04-12", "2026-04-12"],
            "market": ["KRW-BTC", "KRW-ETH"],
            "rows": [10, 10],
            "min_ts_ms": [1_000, 1_000],
            "max_ts_ms": [5_000, 3_000],
            "source_ws_rows": [10, 10],
            "source_rest_rows": [0, 0],
            "source_merged_rows": [10, 10],
            "status": ["OK", "OK"],
            "reasons_json": ["[]", "[]"],
            "part_file": ["a", "b"],
            "built_at_ms": [1, 1],
        }
    ).write_parquet(meta_root / "manifest.parquet")

    result = summarize_trade_coverage(
        TradeCoverageRequest(
            out_root=out_root,
            markets=("KRW-BTC", "KRW-ETH", "KRW-XRP"),
            required_end_ts_ms=4_000,
        )
    )

    assert result.pass_ is False
    assert result.stale_markets == ("KRW-ETH",)
    assert result.missing_markets == ("KRW-XRP",)
