from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import polars as pl

from autobot.data.collect.sequence_tensor_store import (
    LOB_GLOBAL_CHANNELS,
    LOB_PER_LEVEL_CHANNELS,
    MICRO_FEATURE_NAMES,
    ONE_MIN_FEATURE_NAMES,
    SECOND_FEATURE_NAMES,
    SequenceTensorBuildOptions,
    build_sequence_tensor_store,
    validate_sequence_tensor_store,
)


def test_build_sequence_tensor_store_writes_cache_and_contracts(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    market = "KRW-BTC"
    date_value = "2026-03-27"
    anchor_ts_ms = 1_774_569_660_000  # 2026-03-27T00:01:00+00:00

    _write_second_candles(
        parquet_root / "candles_second_v1" / "tf=1s" / f"market={market}" / "part-000.parquet",
        start_ts_ms=anchor_ts_ms - 3_000,
        count=4,
    )
    _write_minute_candles(
        parquet_root / "ws_candle_v1" / "tf=1m" / f"market={market}" / "part-000.parquet",
        ts_values=[anchor_ts_ms],
    )
    _write_micro_rows(
        parquet_root / "micro_v1" / "tf=1m" / f"market={market}" / f"date={date_value}" / "part-000.parquet",
        ts_values=[anchor_ts_ms],
    )
    _write_lob_rows(
        parquet_root / "lob30_v1" / f"market={market}" / f"date={date_value}" / "part-000.parquet",
        ts_values=[anchor_ts_ms - 2_000, anchor_ts_ms - 1_000, anchor_ts_ms],
    )

    options = SequenceTensorBuildOptions(
        parquet_root=parquet_root,
        out_dataset="sequence_v1",
        markets=(market,),
        date=date_value,
        max_anchors_per_market=1,
        second_lookback_steps=4,
        minute_lookback_steps=1,
        micro_lookback_steps=1,
        lob_lookback_steps=3,
    )

    summary = build_sequence_tensor_store(options)
    assert summary.built_anchors == 1
    assert summary.fail_anchors == 0

    manifest = pl.read_parquet(summary.manifest_file)
    assert manifest.height == 1
    cache_file = Path(manifest.item(0, "cache_file"))
    assert cache_file.exists()

    payload = np.load(cache_file)
    assert payload["second_tensor"].shape == (4, len(SECOND_FEATURE_NAMES))
    assert payload["minute_tensor"].shape == (1, len(ONE_MIN_FEATURE_NAMES))
    assert payload["micro_tensor"].shape == (1, len(MICRO_FEATURE_NAMES))
    assert payload["lob_tensor"].shape == (3, 30, len(LOB_PER_LEVEL_CHANNELS))
    assert payload["lob_global_tensor"].shape == (3, len(LOB_GLOBAL_CHANNELS))
    assert int(payload["second_mask"].sum()) == 4
    assert int(payload["lob_mask"].sum()) == 3

    sequence_contract = json.loads(summary.sequence_contract_file.read_text(encoding="utf-8"))
    lob_contract = json.loads(summary.lob_contract_file.read_text(encoding="utf-8"))
    assert sequence_contract["policy"] == "sequence_tensor_contract_v1"
    assert lob_contract["policy"] == "lob_tensor_contract_v1"
    assert lob_contract["shape"]["levels"] == 30

    validate_summary = validate_sequence_tensor_store(options=options)
    assert validate_summary.fail_files == 0
    assert validate_summary.ok_files == 1


def test_validate_sequence_tensor_store_treats_blank_cache_path_as_missing(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    out_root = parquet_root / "sequence_v1"
    meta_root = out_root / "_meta"
    meta_root.mkdir(parents=True, exist_ok=True)
    manifest = pl.DataFrame(
        [
            {
                "market": "KRW-BTC",
                "date": "2026-03-27",
                "anchor_ts_ms": 1_774_569_660_000,
                "anchor_utc": "2026-03-27T00:01:00+00:00",
                "status": "FAIL",
                "reasons_json": json.dumps(["BUILD_EXCEPTION"], ensure_ascii=False),
                "error_message": "boom",
                "cache_file": "",
                "second_coverage_ratio": 0.0,
                "minute_coverage_ratio": 0.0,
                "micro_coverage_ratio": 0.0,
                "lob_coverage_ratio": 0.0,
                "built_at_ms": 1_774_569_660_000,
            }
        ]
    )
    manifest.write_parquet(meta_root / "manifest.parquet")

    options = SequenceTensorBuildOptions(
        parquet_root=parquet_root,
        out_dataset="sequence_v1",
        markets=("KRW-BTC",),
        date="2026-03-27",
    )

    validate_summary = validate_sequence_tensor_store(options=options)

    assert validate_summary.fail_files == 1
    assert validate_summary.details[0]["reasons"] == ["CACHE_FILE_MISSING"]


def test_build_sequence_tensor_store_merges_manifest_rows_across_dates(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    market = "KRW-BTC"
    first_date = "2026-03-27"
    second_date = "2026-03-28"
    first_anchor_ts_ms = 1_774_569_660_000
    second_anchor_ts_ms = 1_774_656_060_000

    _write_second_candles(
        parquet_root / "candles_second_v1" / "tf=1s" / f"market={market}" / "part-000.parquet",
        start_ts_ms=first_anchor_ts_ms - 3_000,
        count=90_000,
    )
    _write_minute_candles(
        parquet_root / "ws_candle_v1" / "tf=1m" / f"market={market}" / "part-000.parquet",
        ts_values=[first_anchor_ts_ms, second_anchor_ts_ms],
    )
    _write_micro_rows(
        parquet_root / "micro_v1" / "tf=1m" / f"market={market}" / f"date={first_date}" / "part-000.parquet",
        ts_values=[first_anchor_ts_ms],
    )
    _write_micro_rows(
        parquet_root / "micro_v1" / "tf=1m" / f"market={market}" / f"date={second_date}" / "part-000.parquet",
        ts_values=[second_anchor_ts_ms],
    )
    _write_lob_rows(
        parquet_root / "lob30_v1" / f"market={market}" / f"date={first_date}" / "part-000.parquet",
        ts_values=[first_anchor_ts_ms - 2_000, first_anchor_ts_ms - 1_000, first_anchor_ts_ms],
    )
    _write_lob_rows(
        parquet_root / "lob30_v1" / f"market={market}" / f"date={second_date}" / "part-000.parquet",
        ts_values=[second_anchor_ts_ms - 2_000, second_anchor_ts_ms - 1_000, second_anchor_ts_ms],
    )

    first_options = SequenceTensorBuildOptions(
        parquet_root=parquet_root,
        out_dataset="sequence_v1",
        markets=(market,),
        date=first_date,
        max_anchors_per_market=10,
        second_lookback_steps=4,
        minute_lookback_steps=1,
        micro_lookback_steps=1,
        lob_lookback_steps=3,
    )
    second_options = SequenceTensorBuildOptions(
        parquet_root=parquet_root,
        out_dataset="sequence_v1",
        markets=(market,),
        date=second_date,
        max_anchors_per_market=10,
        second_lookback_steps=4,
        minute_lookback_steps=1,
        micro_lookback_steps=1,
        lob_lookback_steps=3,
    )

    build_sequence_tensor_store(first_options)
    build_sequence_tensor_store(second_options)

    manifest = pl.read_parquet(parquet_root / "sequence_v1" / "_meta" / "manifest.parquet").sort("anchor_ts_ms")
    assert manifest.height == 2
    assert manifest.get_column("date").to_list() == [first_date, second_date]


def test_build_sequence_tensor_store_skips_existing_ready_anchors(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    market = "KRW-BTC"
    date_value = "2026-03-27"
    anchor_ts_ms = 1_774_569_660_000

    _write_second_candles(
        parquet_root / "candles_second_v1" / "tf=1s" / f"market={market}" / "part-000.parquet",
        start_ts_ms=anchor_ts_ms - 3_000,
        count=4,
    )
    _write_minute_candles(
        parquet_root / "ws_candle_v1" / "tf=1m" / f"market={market}" / "part-000.parquet",
        ts_values=[anchor_ts_ms],
    )
    _write_micro_rows(
        parquet_root / "micro_v1" / "tf=1m" / f"market={market}" / f"date={date_value}" / "part-000.parquet",
        ts_values=[anchor_ts_ms],
    )
    _write_lob_rows(
        parquet_root / "lob30_v1" / f"market={market}" / f"date={date_value}" / "part-000.parquet",
        ts_values=[anchor_ts_ms - 2_000, anchor_ts_ms - 1_000, anchor_ts_ms],
    )

    options = SequenceTensorBuildOptions(
        parquet_root=parquet_root,
        out_dataset="sequence_v1",
        markets=(market,),
        date=date_value,
        max_anchors_per_market=1,
        second_lookback_steps=4,
        minute_lookback_steps=1,
        micro_lookback_steps=1,
        lob_lookback_steps=3,
    )

    first_summary = build_sequence_tensor_store(options)
    second_summary = build_sequence_tensor_store(options)

    assert first_summary.built_anchors == 1
    assert second_summary.built_anchors == 0
    manifest = pl.read_parquet(parquet_root / "sequence_v1" / "_meta" / "manifest.parquet")
    assert manifest.height == 1


def _write_second_candles(path: Path, *, start_ts_ms: int, count: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pl.DataFrame(
        {
            "ts_ms": [start_ts_ms + idx * 1_000 for idx in range(count)],
            "open": [100.0 + idx for idx in range(count)],
            "high": [101.0 + idx for idx in range(count)],
            "low": [99.0 + idx for idx in range(count)],
            "close": [100.5 + idx for idx in range(count)],
            "volume_base": [1.0 + idx for idx in range(count)],
            "volume_quote": [100.0 + idx for idx in range(count)],
            "volume_quote_est": [False for _ in range(count)],
        }
    )
    frame.write_parquet(path)


def _write_minute_candles(path: Path, *, ts_values: list[int]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pl.DataFrame(
        {
            "ts_ms": ts_values,
            "open": [200.0 + idx for idx, _ in enumerate(ts_values)],
            "high": [201.0 + idx for idx, _ in enumerate(ts_values)],
            "low": [199.0 + idx for idx, _ in enumerate(ts_values)],
            "close": [200.5 + idx for idx, _ in enumerate(ts_values)],
            "volume_base": [2.0 + idx for idx, _ in enumerate(ts_values)],
            "volume_quote": [200.0 + idx for idx, _ in enumerate(ts_values)],
            "volume_quote_est": [False for _ in ts_values],
        }
    )
    frame.write_parquet(path)


def _write_micro_rows(path: Path, *, ts_values: list[int]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pl.DataFrame(
        {
            "market": ["KRW-BTC" for _ in ts_values],
            "tf": ["1m" for _ in ts_values],
            "ts_ms": ts_values,
            "trade_source": ["ws" for _ in ts_values],
            "trade_events": [5 for _ in ts_values],
            "book_events": [5 for _ in ts_values],
            "trade_min_ts_ms": ts_values,
            "trade_max_ts_ms": ts_values,
            "book_min_ts_ms": ts_values,
            "book_max_ts_ms": ts_values,
            "trade_coverage_ms": [60_000 for _ in ts_values],
            "book_coverage_ms": [60_000 for _ in ts_values],
            "micro_trade_available": [True for _ in ts_values],
            "micro_book_available": [True for _ in ts_values],
            "micro_available": [True for _ in ts_values],
            "trade_count": [5 for _ in ts_values],
            "buy_count": [3 for _ in ts_values],
            "sell_count": [2 for _ in ts_values],
            "trade_volume_total": [10.0 for _ in ts_values],
            "buy_volume": [6.0 for _ in ts_values],
            "sell_volume": [4.0 for _ in ts_values],
            "trade_imbalance": [0.2 for _ in ts_values],
            "vwap": [200.0 for _ in ts_values],
            "avg_trade_size": [2.0 for _ in ts_values],
            "max_trade_size": [4.0 for _ in ts_values],
            "last_trade_price": [200.0 for _ in ts_values],
            "mid_mean": [200.0 for _ in ts_values],
            "spread_bps_mean": [2.0 for _ in ts_values],
            "depth_bid_top5_mean": [1000.0 for _ in ts_values],
            "depth_ask_top5_mean": [1001.0 for _ in ts_values],
            "imbalance_top5_mean": [0.1 for _ in ts_values],
            "microprice_bias_bps_mean": [0.3 for _ in ts_values],
            "book_update_count": [5 for _ in ts_values],
        }
    )
    frame.write_parquet(path)


def _write_lob_rows(path: Path, *, ts_values: list[int]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for offset, ts_ms in enumerate(ts_values):
        row: dict[str, object] = {
            "ts_ms": ts_ms,
            "collected_at_ms": ts_ms + 10,
            "stream_type": "REALTIME",
            "market": "KRW-BTC",
            "source": "upbit_ws_orderbook_30",
            "level": 0.0,
            "requested_depth": 30,
            "levels_present": 30,
            "total_ask_size": 300.0 + offset,
            "total_bid_size": 310.0 + offset,
        }
        for idx in range(1, 31):
            row[f"ask{idx}_price"] = 201.0 + idx
            row[f"ask{idx}_size"] = float(idx + offset)
            row[f"bid{idx}_price"] = 200.0 - idx
            row[f"bid{idx}_size"] = float(idx + 10 + offset)
        rows.append(row)
    pl.DataFrame(rows).write_parquet(path)
