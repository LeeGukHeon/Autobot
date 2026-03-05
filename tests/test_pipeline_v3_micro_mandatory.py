from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from autobot.features.feature_spec import FeatureSetV1Config, FeatureWindows, LabelV1Config, TimeRangeConfig, UniverseConfig
from autobot.features.pipeline_v3 import (
    FeatureBuildV3Options,
    FeaturesV3BuildConfig,
    FeaturesV3Config,
    FeaturesV3ValidateConfig,
    build_features_dataset_v3,
)


def test_pipeline_v3_drops_rows_when_micro_missing(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    features_root = tmp_path / "features"
    _write_candles(parquet_root / "candles_api_v1")
    _write_micro(parquet_root / "micro_v1")

    config = FeaturesV3Config(
        build=FeaturesV3BuildConfig(
            output_dataset="features_v3_test",
            tf="5m",
            base_candles_dataset="candles_api_v1",
            micro_dataset="micro_v1",
            high_tfs=("15m", "60m", "240m"),
            high_tf_staleness_multiplier=2.0,
            one_m_required_bars=5,
            one_m_max_missing_ratio=0.2,
            sample_weight_half_life_days=60.0,
            min_rows_for_train=1,
            require_micro_validate_pass=True,
        ),
        parquet_root=parquet_root,
        features_root=features_root,
        universe=UniverseConfig(quote="KRW", mode="static_start", top_n=1, lookback_days=7, fixed_list=()),
        time_range=TimeRangeConfig(start="2026-01-01", end="2026-01-03"),
        feature_set_v1=FeatureSetV1Config(
            windows=FeatureWindows(ret=(1, 3, 6, 12), rv=(12, 36), ema=(12, 36), rsi=14, atr=14, vol_z=36),
            enable_factor_features=False,
            factor_markets=(),
            enable_liquidity_rank=False,
        ),
        label_v1=LabelV1Config(horizon_bars=2, thr_bps=15.0, neutral_policy="drop", fee_bps_est=10.0, safety_bps=5.0),
        validation=FeaturesV3ValidateConfig(leakage_fail_on_future_ts=True),
        float_dtype="float32",
    )

    summary = build_features_dataset_v3(
        config,
        FeatureBuildV3Options(
            tf="5m",
            quote="KRW",
            top_n=1,
            start="2026-01-01",
            end="2026-01-03",
            feature_set="v3",
            label_set="v1",
            dry_run=False,
        ),
    )

    assert summary.rows_base_total > 0
    assert summary.rows_dropped_no_micro > 0
    assert summary.rows_final > 0
    assert summary.rows_dropped_one_m_before_densify >= summary.rows_dropped_one_m
    assert summary.rows_rescued_by_one_m_densify >= 0
    assert summary.effective_end is not None
    assert summary.effective_end <= "2026-01-02"

    report = json.loads((features_root / "features_v3_test" / "_meta" / "build_report.json").read_text(encoding="utf-8"))
    assert "rows_dropped_one_m_before_densify" in report
    assert "rows_rescued_by_one_m_densify" in report
    assert "one_m_synth_ratio_p50" in report


def _write_candles(dataset_root: Path) -> None:
    start_ts = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    market = "KRW-BTC"
    _write_tf(dataset_root, tf="1m", market=market, start_ts=start_ts, count=3_200, interval_ms=60_000)
    _write_tf(dataset_root, tf="5m", market=market, start_ts=start_ts, count=620, interval_ms=300_000)
    _write_tf(dataset_root, tf="15m", market=market, start_ts=start_ts, count=230, interval_ms=900_000)
    _write_tf(dataset_root, tf="60m", market=market, start_ts=start_ts, count=80, interval_ms=3_600_000)
    _write_tf(dataset_root, tf="240m", market=market, start_ts=start_ts, count=25, interval_ms=14_400_000)


def _write_tf(dataset_root: Path, *, tf: str, market: str, start_ts: int, count: int, interval_ms: int) -> None:
    part_dir = dataset_root / f"tf={tf}" / f"market={market}"
    part_dir.mkdir(parents=True, exist_ok=True)
    ts = [start_ts + i * interval_ms for i in range(count)]
    close = [100.0 + (i * 0.2) for i in range(count)]
    pl.DataFrame(
        {
            "ts_ms": ts,
            "open": [value - 0.02 for value in close],
            "high": [value + 0.05 for value in close],
            "low": [value - 0.05 for value in close],
            "close": close,
            "volume_base": [100.0 + i * 0.1 for i in range(count)],
            "volume_quote": [close[i] * (100.0 + i * 0.1) for i in range(count)],
            "volume_quote_est": [False for _ in range(count)],
        }
    ).write_parquet(part_dir / "part.parquet")


def _write_micro(dataset_root: Path) -> None:
    start_ts = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    ts = [start_ts + i * 300_000 for i in range(520)]
    part_dir = dataset_root / "tf=5m" / "market=KRW-BTC" / "date=2026-01-01"
    part_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "market": ["KRW-BTC" for _ in ts],
            "tf": ["5m" for _ in ts],
            "ts_ms": ts,
            "trade_source": ["ws" for _ in ts],
            "trade_events": [5 for _ in ts],
            "book_events": [8 for _ in ts],
            "trade_coverage_ms": [250_000 for _ in ts],
            "book_coverage_ms": [260_000 for _ in ts],
            "micro_trade_available": [True for _ in ts],
            "micro_book_available": [True for _ in ts],
            "micro_available": [True for _ in ts],
            "trade_volume_total": [20.0 for _ in ts],
            "buy_volume": [12.0 for _ in ts],
            "sell_volume": [8.0 for _ in ts],
            "spread_bps_mean": [1.0 for _ in ts],
        }
    ).write_parquet(part_dir / "part-000.parquet")

    meta_dir = dataset_root / "_meta"
    meta_dir.mkdir(parents=True, exist_ok=True)
    (meta_dir / "validate_report.json").write_text(
        json.dumps({"fail_files": 0, "warn_files": 0, "ok_files": 1}, ensure_ascii=False),
        encoding="utf-8",
    )
