from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest

from autobot.features.feature_spec import FeatureSetV1Config, FeatureWindows, LabelV1Config, TimeRangeConfig, UniverseConfig
from autobot.features.feature_set_v3 import build_feature_set_v3_from_candles
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
        label_v1=LabelV1Config(horizon_bars=2, thr_bps=0.0, neutral_policy="keep_as_class", fee_bps_est=0.0, safety_bps=0.0),
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


def test_pipeline_v3_requires_micro_validate_report(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    features_root = tmp_path / "features"
    _write_candles(parquet_root / "candles_api_v1")
    _write_micro(parquet_root / "micro_v1", write_validate_report=False)

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
        label_v1=LabelV1Config(horizon_bars=2, thr_bps=0.0, neutral_policy="keep_as_class", fee_bps_est=0.0, safety_bps=0.0),
        validation=FeaturesV3ValidateConfig(leakage_fail_on_future_ts=True),
        float_dtype="float32",
    )

    with pytest.raises(ValueError, match="micro validate report missing"):
        build_features_dataset_v3(
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


def test_pipeline_v3_supports_one_minute_feature_build_dry_run(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    features_root = tmp_path / "features"
    _write_candles(parquet_root / "candles_api_v1", count_1m=5_000)
    _write_micro(parquet_root / "micro_v1", tf="1m", count=5_000)

    config = FeaturesV3Config(
        build=FeaturesV3BuildConfig(
            output_dataset="features_v3_test_1m",
            tf="1m",
            base_candles_dataset="candles_api_v1",
            micro_dataset="micro_v1",
            high_tfs=("15m", "60m", "240m"),
            high_tf_staleness_multiplier=2.0,
            one_m_required_bars=1,
            one_m_max_missing_ratio=0.0,
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
        label_v1=LabelV1Config(horizon_bars=2, thr_bps=0.0, neutral_policy="keep_as_class", fee_bps_est=0.0, safety_bps=0.0),
        validation=FeaturesV3ValidateConfig(leakage_fail_on_future_ts=True),
        float_dtype="float32",
    )

    summary = build_features_dataset_v3(
        config,
        FeatureBuildV3Options(
            tf="1m",
            quote="KRW",
            top_n=1,
                start="2026-01-01",
                end="2026-01-03",
                feature_set="v3",
                label_set="v1",
                dry_run=True,
            ),
        )

    assert summary.preflight_ok is True
    report = json.loads((features_root / "features_v3_test_1m" / "_meta" / "build_report.json").read_text(encoding="utf-8"))
    assert report["tf"] == "1m"
    assert report["status"] == "PASS"
    assert report["dry_run"] is True


def test_pipeline_v3_adapts_one_m_required_bars_for_one_minute_base_tf(tmp_path: Path) -> None:
    ts_values = [60_000 * (idx + 1) for idx in range(40)]
    close_values = [100.0 + (idx * 0.1) for idx in range(40)]
    base = pl.DataFrame(
        {
            "ts_ms": ts_values,
            "open": [value - 0.02 for value in close_values],
            "high": [value + 0.05 for value in close_values],
            "low": [value - 0.05 for value in close_values],
            "close": close_values,
            "volume_base": [10.0 + (idx % 5) for idx in range(40)],
        }
    )
    one_m = base.with_columns(pl.lit(False).alias("is_synth_1m"))
    high_frames = {
        "15m": pl.DataFrame({"ts_ms": [0, 900_000, 1_800_000, 2_700_000], "close": [99.0, 100.0, 101.0, 102.0]}),
        "60m": pl.DataFrame({"ts_ms": [0, 3_600_000], "close": [99.5, 100.5]}),
        "240m": pl.DataFrame({"ts_ms": [0], "close": [98.5]}),
    }
    micro = pl.DataFrame(
        {
            "market": ["KRW-BTC" for _ in ts_values],
            "tf": ["1m" for _ in ts_values],
            "ts_ms": ts_values,
            "trade_source": ["ws" for _ in ts_values],
            "trade_events": [1 for _ in ts_values],
            "book_events": [1 for _ in ts_values],
            "trade_min_ts_ms": [value for value in ts_values],
            "trade_max_ts_ms": [value for value in ts_values],
            "book_min_ts_ms": [value for value in ts_values],
            "book_max_ts_ms": [value for value in ts_values],
            "trade_coverage_ms": [60_000 for _ in ts_values],
            "book_coverage_ms": [60_000 for _ in ts_values],
            "micro_trade_available": [True for _ in ts_values],
            "micro_book_available": [True for _ in ts_values],
            "micro_available": [True for _ in ts_values],
            "trade_count": [1 for _ in ts_values],
            "buy_count": [1 for _ in ts_values],
            "sell_count": [0 for _ in ts_values],
            "trade_volume_total": [1.0 for _ in ts_values],
            "buy_volume": [1.0 for _ in ts_values],
            "sell_volume": [0.0 for _ in ts_values],
            "trade_imbalance": [1.0 for _ in ts_values],
            "vwap": close_values,
            "avg_trade_size": [1.0 for _ in ts_values],
            "max_trade_size": [1.0 for _ in ts_values],
            "last_trade_price": close_values,
            "mid_mean": close_values,
            "spread_bps_mean": [1.0 for _ in ts_values],
            "depth_bid_top5_mean": [1000.0 for _ in ts_values],
            "depth_ask_top5_mean": [1000.0 for _ in ts_values],
            "imbalance_top5_mean": [0.0 for _ in ts_values],
            "microprice_bias_bps_mean": [0.0 for _ in ts_values],
            "book_update_count": [1 for _ in ts_values],
        }
    )

    result = build_feature_set_v3_from_candles(
        base_candles_frame=base,
        one_m_candles_frame=one_m,
        high_tf_candles=high_frames,
        micro_frame=micro,
        micro_tf_used="1m",
        tf="1m",
        from_ts_ms=ts_values[0],
        to_ts_ms=ts_values[-1],
        label_config=LabelV1Config(horizon_bars=2, thr_bps=0.0, neutral_policy="keep_as_class", fee_bps_est=0.0, safety_bps=0.0),
        high_tfs=("15m", "60m", "240m"),
        high_tf_staleness_multiplier=2.0,
        one_m_required_bars=5,
        one_m_max_missing_ratio=0.2,
        one_m_drop_if_real_count_zero=True,
        sample_weight_half_life_days=60.0,
        one_m_synth_weight_floor=0.2,
        one_m_synth_weight_power=2.0,
        float_dtype="float32",
    )

    assert result.rows_after_multitf > 0
    assert result.one_m_stats.required_bars == 1


def _write_candles(dataset_root: Path, *, count_1m: int = 3_200) -> None:
    start_ts = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    market = "KRW-BTC"
    _write_tf(dataset_root, tf="1m", market=market, start_ts=start_ts, count=count_1m, interval_ms=60_000)
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


def _write_micro(dataset_root: Path, *, write_validate_report: bool = True, tf: str = "5m", count: int = 520) -> None:
    start_ts = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    interval_ms = 60_000 if str(tf).strip().lower() == "1m" else 300_000
    ts = [start_ts + i * interval_ms for i in range(count)]
    tf_value = str(tf).strip().lower()
    part_dir = dataset_root / f"tf={tf_value}" / "market=KRW-BTC" / "date=2026-01-01"
    part_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "market": ["KRW-BTC" for _ in ts],
            "tf": [tf_value for _ in ts],
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

    if write_validate_report:
        meta_dir = dataset_root / "_meta"
        meta_dir.mkdir(parents=True, exist_ok=True)
        (meta_dir / "validate_report.json").write_text(
            json.dumps(
                {
                    "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    "checked_files": 1,
                    "fail_files": 0,
                    "warn_files": 0,
                    "ok_files": 1,
                    "details": [
                        {
                            "file": str(part_dir / "part-000.parquet"),
                            "tf": tf_value,
                            "market": "KRW-BTC",
                            "date": "2026-01-01",
                            "rows": len(ts),
                        }
                    ],
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
