from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from autobot.features.feature_spec import FeatureSetV1Config, FeatureWindows, TimeRangeConfig, UniverseConfig
from autobot.features.labeling_v2_crypto_cs import LabelV2CryptoCsConfig
from autobot.features.pipeline_v4 import (
    FeatureBuildV4Options,
    FeatureValidateV4Options,
    FeaturesV4BuildConfig,
    FeaturesV4Config,
    FeaturesV4ValidateConfig,
    build_features_dataset_v4,
    features_stats_v4,
    validate_features_dataset_v4,
)


def test_pipeline_v4_builds_cross_sectional_labels(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    features_root = tmp_path / "features"
    _write_candles(parquet_root / "candles_api_v1")
    _write_micro(parquet_root / "micro_v1")

    config = FeaturesV4Config(
        build=FeaturesV4BuildConfig(
            output_dataset="features_v4_test",
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
        universe=UniverseConfig(quote="KRW", mode="static_start", top_n=2, lookback_days=7, fixed_list=()),
        time_range=TimeRangeConfig(start="2026-01-01", end="2026-01-03"),
        feature_set_v1=FeatureSetV1Config(
            windows=FeatureWindows(ret=(1, 3, 6, 12), rv=(12, 36), ema=(12, 36), rsi=14, atr=14, vol_z=36),
            enable_factor_features=False,
            factor_markets=(),
            enable_liquidity_rank=False,
        ),
        label_v2=LabelV2CryptoCsConfig(
            horizon_bars=2,
            fee_bps_est=10.0,
            safety_bps=5.0,
            top_quantile=0.49,
            bottom_quantile=0.49,
            neutral_policy="drop",
        ),
        validation=FeaturesV4ValidateConfig(leakage_fail_on_future_ts=True),
        float_dtype="float32",
    )

    summary = build_features_dataset_v4(
        config,
        FeatureBuildV4Options(
            tf="5m",
            quote="KRW",
            top_n=2,
            start="2026-01-01",
            end="2026-01-03",
            feature_set="v4",
            label_set="v2",
            dry_run=False,
        ),
    )

    assert summary.rows_base_total > 0
    assert summary.rows_final > 0
    assert summary.fail_markets == 0

    validate_summary = validate_features_dataset_v4(
        config,
        FeatureValidateV4Options(
            tf="5m",
            quote="KRW",
            top_n=2,
            start="2026-01-01",
            end="2026-01-03",
        ),
    )
    assert validate_summary.fail_files == 0
    assert validate_summary.leakage_smoke == "PASS"

    stats = features_stats_v4(config, tf="5m", quote="KRW", top_n=2)
    assert stats["rows_final"] > 0
    assert stats["label_pos_rows"] > 0
    assert stats["label_neg_rows"] > 0

    label_spec = json.loads((features_root / "features_v4_test" / "_meta" / "label_spec.json").read_text(encoding="utf-8"))
    assert label_spec["label_set_version"] == "v2_crypto_cs"
    assert label_spec["label_columns"] == ["y_reg_net_12", "y_rank_cs_12", "y_cls_topq_12"]

    files = sorted((features_root / "features_v4_test").glob("tf=5m/market=*/date=*/*.parquet"))
    assert files
    frame = pl.concat([pl.read_parquet(path) for path in files], how="vertical_relaxed")
    assert set(("market", "sample_weight", "y_reg_net_12", "y_rank_cs_12", "y_cls_topq_12")).issubset(frame.columns)
    assert set(frame.get_column("market").unique().to_list()) == {"KRW-BTC", "KRW-ETH"}
    assert set(frame.get_column("y_cls_topq_12").drop_nulls().unique().to_list()) == {0, 1}
    assert frame.filter(pl.col("y_rank_cs_12").is_between(0.0, 1.0, closed="both")).height == frame.height


def _write_candles(dataset_root: Path) -> None:
    start_ts = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    _write_tf(dataset_root, tf="1m", market="KRW-BTC", start_ts=start_ts, count=3_200, interval_ms=60_000, slope=0.22)
    _write_tf(dataset_root, tf="5m", market="KRW-BTC", start_ts=start_ts, count=620, interval_ms=300_000, slope=0.22)
    _write_tf(dataset_root, tf="15m", market="KRW-BTC", start_ts=start_ts, count=230, interval_ms=900_000, slope=0.22)
    _write_tf(dataset_root, tf="60m", market="KRW-BTC", start_ts=start_ts, count=80, interval_ms=3_600_000, slope=0.22)
    _write_tf(dataset_root, tf="240m", market="KRW-BTC", start_ts=start_ts, count=25, interval_ms=14_400_000, slope=0.22)

    _write_tf(dataset_root, tf="1m", market="KRW-ETH", start_ts=start_ts, count=3_200, interval_ms=60_000, slope=0.06)
    _write_tf(dataset_root, tf="5m", market="KRW-ETH", start_ts=start_ts, count=620, interval_ms=300_000, slope=0.06)
    _write_tf(dataset_root, tf="15m", market="KRW-ETH", start_ts=start_ts, count=230, interval_ms=900_000, slope=0.06)
    _write_tf(dataset_root, tf="60m", market="KRW-ETH", start_ts=start_ts, count=80, interval_ms=3_600_000, slope=0.06)
    _write_tf(dataset_root, tf="240m", market="KRW-ETH", start_ts=start_ts, count=25, interval_ms=14_400_000, slope=0.06)


def _write_tf(dataset_root: Path, *, tf: str, market: str, start_ts: int, count: int, interval_ms: int, slope: float) -> None:
    part_dir = dataset_root / f"tf={tf}" / f"market={market}"
    part_dir.mkdir(parents=True, exist_ok=True)
    ts = [start_ts + i * interval_ms for i in range(count)]
    close = [100.0 + (i * slope) for i in range(count)]
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
    for market in ("KRW-BTC", "KRW-ETH"):
        part_dir = dataset_root / "tf=5m" / f"market={market}" / "date=2026-01-01"
        part_dir.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            {
                "market": [market for _ in ts],
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
        json.dumps({"fail_files": 0, "warn_files": 0, "ok_files": 2}, ensure_ascii=False),
        encoding="utf-8",
    )
