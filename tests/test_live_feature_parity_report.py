from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from autobot.ops.live_feature_parity_report import build_live_feature_parity_report
from autobot.paper.live_features_v4 import LiveFeatureProviderV4


def _write_one_m_candles(
    *,
    dataset_root: Path,
    market: str,
    start_ts_ms: int = 60_000,
    count: int = 599,
    base_close: float = 100.0,
    slope: float = 0.05,
) -> None:
    part_dir = dataset_root / "tf=1m" / f"market={market}" / "date=2026-01-01"
    part_dir.mkdir(parents=True, exist_ok=True)
    ts_values = [int(start_ts_ms) + (i * 60_000) for i in range(int(count))]
    close_values = [float(base_close) + (i * float(slope)) for i in range(len(ts_values))]
    frame = pl.DataFrame(
        {
            "ts_ms": ts_values,
            "open": [value - 0.02 for value in close_values],
            "high": [value + 0.05 for value in close_values],
            "low": [value - 0.05 for value in close_values],
            "close": close_values,
            "volume_base": [10.0 + (i % 5) for i in range(len(ts_values))],
        }
    )
    frame.write_parquet(part_dir / "part-000.parquet")


def test_build_live_feature_parity_report_passes_for_matching_offline_and_live_rows(tmp_path: Path) -> None:
    project_root = tmp_path
    parquet_root = project_root / "data" / "parquet"
    candles_root = parquet_root / "candles_api_v1"
    _write_one_m_candles(dataset_root=candles_root, market="KRW-BTC")

    feature_columns = ("logret_1", "btc_ret_12", "hour_sin")
    provider = LiveFeatureProviderV4(
        feature_columns=feature_columns,
        tf="5m",
        quote="KRW",
        parquet_root=parquet_root,
        candles_dataset_name="candles_api_v1",
        bootstrap_1m_bars=2000,
        bootstrap_end_ts_ms=300_000,
    )
    offline_frame = provider.build_frame(ts_ms=300_000, markets=["KRW-BTC"])

    dataset_root = project_root / "data" / "features" / "features_v4"
    part_dir = dataset_root / "tf=5m" / "market=KRW-BTC" / "date=1970-01-01"
    part_dir.mkdir(parents=True, exist_ok=True)
    offline_frame.write_parquet(part_dir / "part-000.parquet")
    meta_root = dataset_root / "_meta"
    meta_root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "tf": ["5m"],
            "market": ["KRW-BTC"],
            "status": ["OK"],
            "rows_final": [1],
        }
    ).write_parquet(meta_root / "manifest.parquet")
    (meta_root / "feature_spec.json").write_text(
        json.dumps(
            {
                "feature_columns": list(feature_columns),
                "base_candles_root": str(candles_root),
                "micro_root": str(project_root / "data" / "parquet" / "micro_v1"),
            }
        ),
        encoding="utf-8",
    )

    report = build_live_feature_parity_report(project_root=project_root, top_n=1, samples_per_market=1)

    assert report["status"] == "PASS"
    assert report["acceptable"] is True
    assert report["sampled_pairs"] == 1
    assert report["passing_pairs"] == 1
    assert report["missing_feature_columns_total"] == 0
    assert report["details"][0]["pass"] is True


def test_build_live_feature_parity_report_ignores_stale_partitions_outside_latest_build_window(tmp_path: Path) -> None:
    project_root = tmp_path
    parquet_root = project_root / "data" / "parquet"
    candles_root = parquet_root / "candles_api_v1"
    _write_one_m_candles(dataset_root=candles_root, market="KRW-BTC")

    feature_columns = ("logret_1", "btc_ret_12", "hour_sin")
    provider = LiveFeatureProviderV4(
        feature_columns=feature_columns,
        tf="5m",
        quote="KRW",
        parquet_root=parquet_root,
        candles_dataset_name="candles_api_v1",
        bootstrap_1m_bars=2000,
        bootstrap_end_ts_ms=300_000,
    )
    offline_frame = provider.build_frame(ts_ms=300_000, markets=["KRW-BTC"])

    dataset_root = project_root / "data" / "features" / "features_v4"
    part_dir = dataset_root / "tf=5m" / "market=KRW-BTC" / "date=1970-01-01"
    part_dir.mkdir(parents=True, exist_ok=True)
    offline_frame.write_parquet(part_dir / "part-000.parquet")
    stale_dir = dataset_root / "tf=5m" / "market=KRW-BTC" / "date=1970-01-02"
    stale_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "ts_ms": [86_700_000],
            "market": ["KRW-BTC"],
            "close": [9999.0],
            "logret_1": [123.0],
            "btc_ret_12": [456.0],
            "hour_sin": [789.0],
            "y_reg_net_12": [1.0],
        }
    ).write_parquet(stale_dir / "part-000.parquet")

    meta_root = dataset_root / "_meta"
    meta_root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "tf": ["5m"],
            "market": ["KRW-BTC"],
            "status": ["OK"],
            "rows_final": [2],
        }
    ).write_parquet(meta_root / "manifest.parquet")
    (meta_root / "feature_spec.json").write_text(
        json.dumps(
            {
                "feature_columns": list(feature_columns),
                "base_candles_root": str(candles_root),
                "micro_root": str(project_root / "data" / "parquet" / "micro_v1"),
            }
        ),
        encoding="utf-8",
    )
    (meta_root / "build_report.json").write_text(
        json.dumps(
            {
                "effective_start": "1970-01-01",
                "effective_end": "1970-01-01",
            }
        ),
        encoding="utf-8",
    )

    report = build_live_feature_parity_report(project_root=project_root, top_n=1, samples_per_market=1)

    assert report["status"] == "PASS"
    assert report["sampling_window"]["effective_start"] == "1970-01-01"
    assert report["sampling_window"]["effective_end"] == "1970-01-01"
    assert report["sampled_pairs"] == 1
    assert report["details"][0]["ts_ms"] == 300_000


def test_build_live_feature_parity_report_normalizes_trade_source_like_runtime_loader(tmp_path: Path) -> None:
    project_root = tmp_path
    parquet_root = project_root / "data" / "parquet"
    candles_root = parquet_root / "candles_api_v1"
    _write_one_m_candles(dataset_root=candles_root, market="KRW-BTC")
    micro_root = project_root / "data" / "parquet" / "micro_v1" / "tf=5m" / "market=KRW-BTC" / "date=1970-01-01"
    micro_root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "market": ["KRW-BTC"],
            "tf": ["5m"],
            "ts_ms": [300_000],
            "trade_source": ["ws"],
            "trade_events": [1],
            "book_events": [1],
            "trade_min_ts_ms": [240_000],
            "trade_max_ts_ms": [300_000],
            "book_min_ts_ms": [240_000],
            "book_max_ts_ms": [300_000],
            "trade_coverage_ms": [60_000],
            "book_coverage_ms": [60_000],
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
            "mid_mean": [100.0],
            "spread_bps_mean": [1.0],
            "depth_bid_top5_mean": [1000.0],
            "depth_ask_top5_mean": [1000.0],
            "imbalance_top5_mean": [0.0],
            "microprice_bias_bps_mean": [0.0],
            "book_update_count": [1],
        }
    ).write_parquet(micro_root / "part-000.parquet")
    provider = LiveFeatureProviderV4(
        feature_columns=("m_trade_source",),
        tf="5m",
        quote="KRW",
        parquet_root=parquet_root,
        candles_dataset_name="candles_api_v1",
        bootstrap_1m_bars=2000,
    )
    live_row = provider.build_frame(ts_ms=300_000, markets=["KRW-BTC"]).row(0, named=True)

    dataset_root = project_root / "data" / "features" / "features_v4"
    part_dir = dataset_root / "tf=5m" / "market=KRW-BTC" / "date=1970-01-01"
    part_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "ts_ms": [300_000],
            "market": ["KRW-BTC"],
            "close": [float(live_row["close"])],
            "m_trade_source": ["ws"],
        }
    ).write_parquet(part_dir / "part-000.parquet")
    meta_root = dataset_root / "_meta"
    meta_root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "tf": ["5m"],
            "market": ["KRW-BTC"],
            "status": ["OK"],
            "rows_final": [1],
        }
    ).write_parquet(meta_root / "manifest.parquet")
    (meta_root / "feature_spec.json").write_text(
        json.dumps(
                {
                    "feature_columns": ["m_trade_source"],
                    "base_candles_root": str(candles_root),
                    "micro_root": str(project_root / "data" / "parquet" / "micro_v1"),
                }
            ),
            encoding="utf-8",
        )
    (meta_root / "build_report.json").write_text(
        json.dumps(
            {
                "effective_start": "1970-01-01",
                "effective_end": "1970-01-01",
            }
        ),
        encoding="utf-8",
    )

    report = build_live_feature_parity_report(project_root=project_root, top_n=1, samples_per_market=1)

    assert report["status"] == "PASS"
    assert report["passing_pairs"] == 1


def test_live_feature_parity_report_builds_live_context_from_full_manifest_universe(tmp_path: Path) -> None:
    project_root = tmp_path
    parquet_root = project_root / "data" / "parquet"
    candles_root = parquet_root / "candles_api_v1"
    _write_one_m_candles(dataset_root=candles_root, market="KRW-BTC", base_close=100.0, slope=0.08)
    _write_one_m_candles(dataset_root=candles_root, market="KRW-ETH", base_close=200.0, slope=-0.06)

    feature_columns = ("market_breadth_pos_12",)
    target_ts_ms = 3_900_000
    provider = LiveFeatureProviderV4(
        feature_columns=feature_columns,
        tf="5m",
        quote="KRW",
        parquet_root=parquet_root,
        candles_dataset_name="candles_api_v1",
        bootstrap_1m_bars=2000,
        bootstrap_end_ts_ms=target_ts_ms,
    )
    offline_frame = provider.build_frame(ts_ms=target_ts_ms, markets=["KRW-BTC", "KRW-ETH"])
    offline_btc = offline_frame.filter(pl.col("market") == "KRW-BTC")

    dataset_root = project_root / "data" / "features" / "features_v4"
    part_dir = dataset_root / "tf=5m" / "market=KRW-BTC" / "date=1970-01-01"
    part_dir.mkdir(parents=True, exist_ok=True)
    offline_btc.write_parquet(part_dir / "part-000.parquet")
    meta_root = dataset_root / "_meta"
    meta_root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "tf": ["5m", "5m"],
            "market": ["KRW-BTC", "KRW-ETH"],
            "status": ["OK", "OK"],
            "rows_final": [1, 1],
        }
    ).write_parquet(meta_root / "manifest.parquet")
    (meta_root / "feature_spec.json").write_text(
        json.dumps(
            {
                "feature_columns": ["market_breadth_pos_12"],
                "base_candles_root": str(candles_root),
                "micro_root": str(project_root / "data" / "parquet" / "micro_v1"),
            }
        ),
        encoding="utf-8",
    )
    (meta_root / "build_report.json").write_text(
        json.dumps(
            {
                "effective_start": "1970-01-01",
                "effective_end": "1970-01-01",
            }
        ),
        encoding="utf-8",
    )

    report = build_live_feature_parity_report(project_root=project_root, top_n=1, samples_per_market=1)

    assert report["status"] == "PASS"
    assert report["sampled_pairs"] == 1
    assert report["passing_pairs"] == 1
