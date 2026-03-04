from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest

from autobot.features.feature_spec import FeatureSetV1Config, FeatureWindows, LabelV1Config, TimeRangeConfig, UniverseConfig
from autobot.features.feature_set_v2 import MicroFilterPolicy
from autobot.features.pipeline_v2 import (
    FeatureBuildV2Options,
    FeaturesV2BuildConfig,
    FeaturesV2Config,
    FeaturesV2ValidateConfig,
    build_features_dataset_v2,
)


def test_features_v2_preflight_fails_when_no_candle_overlap(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    micro_dir = parquet_root / "micro_v1" / "tf=5m" / "market=KRW-BTC" / "date=2026-03-03"
    micro_dir.mkdir(parents=True, exist_ok=True)

    t0 = int(datetime(2026, 3, 3, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
    pl.DataFrame(
        {
            "market": ["KRW-BTC", "KRW-BTC"],
            "tf": ["5m", "5m"],
            "ts_ms": [t0, t0 + 300_000],
            "trade_source": ["ws", "ws"],
            "trade_events": [2, 3],
            "book_events": [4, 5],
            "trade_coverage_ms": [120_000, 110_000],
            "book_coverage_ms": [120_000, 120_000],
            "micro_trade_available": [True, True],
            "micro_book_available": [True, True],
            "micro_available": [True, True],
        }
    ).write_parquet(micro_dir / "part-000.parquet")

    candles_dir = parquet_root / "candles_api_v1" / "tf=5m" / "market=KRW-BTC"
    candles_dir.mkdir(parents=True, exist_ok=True)
    old_ts = int(datetime(2026, 2, 1, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
    pl.DataFrame(
        {
            "ts_ms": [old_ts, old_ts + 300_000],
            "open": [100.0, 101.0],
            "high": [101.0, 102.0],
            "low": [99.0, 100.0],
            "close": [100.5, 101.5],
            "volume_base": [10.0, 11.0],
        }
    ).write_parquet(candles_dir / "part-000.parquet")

    config = FeaturesV2Config(
        build=FeaturesV2BuildConfig(
            output_dataset="features_v2_test",
            tf="5m",
            base_candles_dataset="candles_api_v1",
            micro_dataset="micro_v1",
            alignment_mode="auto",
            use_precomputed_features_v1=False,
            precomputed_features_v1_dataset="features_v1",
            min_rows_for_train=10,
        ),
        parquet_root=parquet_root,
        features_root=tmp_path / "features",
        universe=UniverseConfig(quote="KRW", mode="static_start", top_n=1, lookback_days=1, fixed_list=()),
        time_range=TimeRangeConfig(start="2026-03-03", end="2026-03-03"),
        feature_set_v1=FeatureSetV1Config(
            windows=FeatureWindows(ret=(1, 3), rv=(3,), ema=(3, 6), rsi=3, atr=3, vol_z=3),
            enable_factor_features=False,
            factor_markets=(),
            enable_liquidity_rank=False,
        ),
        label_v1=LabelV1Config(horizon_bars=2, thr_bps=15.0, neutral_policy="drop", fee_bps_est=10.0, safety_bps=5.0),
        micro_filter=MicroFilterPolicy(
            require_micro_available=True,
            min_trade_events=1,
            min_trade_coverage_ms=60_000,
            min_book_events=1,
            min_book_coverage_ms=60_000,
        ),
        validation=FeaturesV2ValidateConfig(join_match_warn=0.98, join_match_fail=0.90),
        float_dtype="float32",
    )

    with pytest.raises(ValueError, match="PRECONDITION_FAILED"):
        build_features_dataset_v2(
            config,
            FeatureBuildV2Options(
                tf="5m",
                quote="KRW",
                top_n=1,
                start="2026-03-03",
                end="2026-03-03",
                feature_set="v2",
                label_set="v1",
                dry_run=True,
            ),
        )
