from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl

from autobot.features.feature_set_v1 import compute_feature_set_v1
from autobot.features.feature_spec import (
    FeatureBuildConfig,
    FeatureSetV1Config,
    FeatureWindows,
    FeaturesConfig,
    LabelV1Config,
    TimeRangeConfig,
    UniverseConfig,
)
from autobot.features.pipeline import (
    FeatureBuildOptions,
    FeatureValidateOptions,
    build_features_dataset,
    validate_features_dataset,
)


def test_feature_set_v1_rsi_ema_atr_sanity() -> None:
    frame = _make_market_frame(
        market_bias=0.2,
        start_dt=datetime(2024, 1, 1, tzinfo=timezone.utc),
        bars=200,
    )
    config = FeatureSetV1Config(
        windows=FeatureWindows(ret=(1, 3, 6, 12), rv=(12, 36), ema=(12, 36), rsi=14, atr=14, vol_z=36),
        enable_factor_features=False,
        factor_markets=(),
        enable_liquidity_rank=False,
    )
    featured = compute_feature_set_v1(frame, tf="5m", config=config, float_dtype="float64")

    assert "ema_12" in featured.columns
    assert "ema_36" in featured.columns
    assert "ema_ratio" in featured.columns
    assert "rsi_14" in featured.columns
    assert "atr_14" in featured.columns

    non_null_rsi = featured.get_column("rsi_14").drop_nulls()
    assert non_null_rsi.len() > 0
    assert float(non_null_rsi.min()) >= 0.0
    assert float(non_null_rsi.max()) <= 100.0

    non_null_atr = featured.get_column("atr_14").drop_nulls()
    assert non_null_atr.len() > 0
    assert float(non_null_atr.min()) >= 0.0


def test_features_pipeline_build_then_validate_passes(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    features_root = tmp_path / "features"
    dataset_root = parquet_root / "candles_v1" / "tf=5m"

    bars = 1_200
    start_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
    _write_market_parquet(dataset_root, market="KRW-BTC", frame=_make_market_frame(2.0, start_dt, bars))
    _write_market_parquet(dataset_root, market="KRW-ETH", frame=_make_market_frame(1.5, start_dt, bars))
    _write_market_parquet(dataset_root, market="KRW-AAA", frame=_make_market_frame(4.0, start_dt, bars))
    _write_market_parquet(dataset_root, market="KRW-BBB", frame=_make_market_frame(0.2, start_dt, bars))

    config = FeaturesConfig(
        build=FeatureBuildConfig(dataset_name="features_v1", input_dataset="candles_v1", float_dtype="float32"),
        parquet_root=parquet_root,
        features_root=features_root,
        universe=UniverseConfig(quote="KRW", mode="static_start", top_n=2, lookback_days=1, fixed_list=()),
        time_range=TimeRangeConfig(start="2024-01-02", end="2024-01-03"),
        feature_set_v1=FeatureSetV1Config(
            windows=FeatureWindows(ret=(1, 3, 6, 12), rv=(12, 36), ema=(12, 36), rsi=14, atr=14, vol_z=36),
            enable_factor_features=True,
            factor_markets=("KRW-BTC", "KRW-ETH"),
            enable_liquidity_rank=False,
        ),
        label_v1=LabelV1Config(
            horizon_bars=12,
            thr_bps=15.0,
            neutral_policy="drop",
            fee_bps_est=10.0,
            safety_bps=5.0,
        ),
    )

    build_summary = build_features_dataset(
        config,
        FeatureBuildOptions(
            tf="5m",
            quote="KRW",
            top_n=2,
            start="2024-01-02",
            end="2024-01-03",
            workers=1,
            fail_on_warn=False,
        ),
    )
    assert build_summary.fail_markets == 0
    assert build_summary.processed_markets == 2
    assert build_summary.rows_total > 0
    assert (features_root / "features_v1" / "_meta" / "feature_spec.json").exists()
    assert (features_root / "features_v1" / "_meta" / "label_spec.json").exists()
    assert (features_root / "features_v1" / "_meta" / "build_report.json").exists()

    validate_summary = validate_features_dataset(
        config,
        FeatureValidateOptions(tf="5m", quote="KRW", top_n=2),
    )
    assert validate_summary.checked_files == 2
    assert validate_summary.fail_files == 0
    assert validate_summary.leakage_smoke == "PASS"
    assert (features_root / "features_v1" / "_meta" / "validate_report.json").exists()


def _write_market_parquet(root: Path, *, market: str, frame: pl.DataFrame) -> None:
    path = root / f"market={market}"
    path.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(path / "part.parquet")


def _make_market_frame(market_bias: float, start_dt: datetime, bars: int) -> pl.DataFrame:
    ts_ms: list[int] = []
    close: list[float] = []
    open_: list[float] = []
    high: list[float] = []
    low: list[float] = []
    volume_base: list[float] = []
    for idx in range(bars):
        ts = start_dt + timedelta(minutes=5 * idx)
        ts_ms.append(int(ts.timestamp() * 1000))
        base_price = 100.0 + (idx * 0.04) + ((idx % 9) - 4) * 0.02 + market_bias
        open_price = base_price - 0.05
        high_price = base_price + 0.15
        low_price = base_price - 0.15
        close_price = base_price + ((idx % 3) - 1) * 0.01
        volume = 200.0 + market_bias * 80.0 + (idx % 17) * 3.0

        open_.append(open_price)
        high.append(high_price)
        low.append(low_price)
        close.append(close_price)
        volume_base.append(volume)

    volume_quote = [close[idx] * volume_base[idx] for idx in range(bars)]
    return pl.DataFrame(
        {
            "ts_ms": ts_ms,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume_base": volume_base,
            "volume_quote": volume_quote,
            "volume_quote_est": [False for _ in range(bars)],
        }
    )
