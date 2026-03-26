from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from autobot.ops.live_feature_parity_report import build_live_feature_parity_report
from autobot.paper.live_features_v4 import LiveFeatureProviderV4
from autobot.upbit.ws.models import TickerEvent


def _write_one_m_candles(*, dataset_root: Path, market: str, start_ts_ms: int = 60_000, count: int = 599) -> None:
    part_dir = dataset_root / "tf=1m" / f"market={market}" / "date=2026-01-01"
    part_dir.mkdir(parents=True, exist_ok=True)
    ts_values = [int(start_ts_ms) + (i * 60_000) for i in range(int(count))]
    close_values = [100.0 + (i * 0.05) for i in range(len(ts_values))]
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
    )
    provider.ingest_ticker(
        TickerEvent(
            market="KRW-BTC",
            ts_ms=301_000,
            trade_price=121.0,
            acc_trade_price_24h=1_000_100_000.0,
        )
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
