from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest

from autobot.features.feature_set_v4_live_base import build_feature_set_v4_live_base_from_candles
from autobot.features.feature_spec import FeatureSetV1Config, FeatureWindows, LabelV1Config, TimeRangeConfig, UniverseConfig
from autobot.features.labeling_v2_crypto_cs import LabelV2CryptoCsConfig
from autobot.features.labeling_v3_crypto_cs import LabelV3CryptoCsConfig
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
    _write_candles_history(parquet_root / "candles_v1")
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
            horizon_bars=12,
            horizons_bars=(3, 6, 12, 24),
            primary_horizon_bars=12,
            fee_bps_est=10.0,
            safety_bps=5.0,
            top_quantile=0.49,
            bottom_quantile=0.49,
            neutral_policy="drop",
        ),
        label_v3=LabelV3CryptoCsConfig(
            horizons_bars=(3, 6, 12, 24),
            primary_horizon_bars=12,
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
    build_report = json.loads(
        (features_root / "features_v4_test" / "_meta" / "build_report.json").read_text(encoding="utf-8")
    )
    assert build_report["min_rows_for_train"] == 1

    label_spec = json.loads((features_root / "features_v4_test" / "_meta" / "label_spec.json").read_text(encoding="utf-8"))
    assert label_spec["label_set_version"] == "v2_crypto_cs"
    assert label_spec["label_bundle_version"] == "multi_horizon_v1"
    assert label_spec["multi_horizon_bars"] == [3, 6, 12, 24]
    assert label_spec["training_default_columns"] == {
        "y_reg": "y_reg_net_12",
        "y_rank": "y_rank_cs_12",
        "y_cls": "y_cls_topq_12",
    }
    assert set(
        [
            "y_reg_net_12",
            "y_rank_cs_12",
            "y_cls_topq_12",
            "y_reg_net_h3",
            "y_reg_net_h6",
            "y_reg_net_h12",
            "y_reg_net_h24",
            "y_rank_cs_h3",
            "y_rank_cs_h6",
            "y_rank_cs_h12",
            "y_rank_cs_h24",
        ]
    ).issubset(set(label_spec["label_columns"]))
    feature_spec = json.loads(
        (features_root / "features_v4_test" / "_meta" / "feature_spec.json").read_text(encoding="utf-8")
    )
    assert "btc_ret_12" in feature_spec["feature_columns"]
    assert "market_breadth_pos_12" in feature_spec["feature_columns"]
    assert "turnover_concentration_hhi" in feature_spec["feature_columns"]
    assert "hour_sin" in feature_spec["feature_columns"]
    assert "asia_us_overlap_flag" in feature_spec["feature_columns"]
    assert "utc_session_bucket" in feature_spec["feature_columns"]
    assert "price_trend_short" in feature_spec["feature_columns"]
    assert "volume_trend_long" in feature_spec["feature_columns"]
    assert "trend_consensus" in feature_spec["feature_columns"]
    assert "oflow_v1_signed_volume_imbalance_1" in feature_spec["feature_columns"]
    assert "oflow_v1_depth_conditioned_flow_1" in feature_spec["feature_columns"]
    assert "oflow_v1_microprice_conditioned_flow_1" in feature_spec["feature_columns"]
    assert "ctrend_v1_rsi_14" not in feature_spec["feature_columns"]
    assert "ctrend_v1_cci_20" not in feature_spec["feature_columns"]
    assert "ctrend_v1_ma_gap_200" not in feature_spec["feature_columns"]
    assert "ctrend_v1_vol_ma_gap_200" not in feature_spec["feature_columns"]
    assert "ctrend_v1_macd_hist_12_26_9" not in feature_spec["feature_columns"]
    assert "ctrend_v1_vol_macd_hist_12_26_9" not in feature_spec["feature_columns"]
    assert "ctrend_v1_boll_width_20_2" not in feature_spec["feature_columns"]
    assert "mom_x_illiq" in feature_spec["feature_columns"]
    assert "one_m_pressure_x_spread" in feature_spec["feature_columns"]
    assert "volume_z_x_trend" in feature_spec["feature_columns"]
    assert feature_spec["active_factor_contracts"] == []
    assert feature_spec["factor_contracts"] == {}
    assert feature_spec["active_micro_panel_contracts"] == ["order_flow_panel_v1"]
    assert feature_spec["micro_panel_contracts"]["order_flow_panel_v1"]["version"] == "order_flow_panel_v1"
    assert feature_spec["order_flow_diagnostics"]["rows"] > 0

    files = sorted((features_root / "features_v4_test").glob("tf=5m/market=*/date=*/*.parquet"))
    assert files
    frame = pl.concat([pl.read_parquet(path) for path in files], how="vertical_relaxed")
    assert set(
        (
            "market",
            "sample_weight",
            "y_reg_net_12",
            "y_rank_cs_12",
            "y_cls_topq_12",
            "y_reg_net_h3",
            "y_reg_net_h6",
            "y_reg_net_h12",
            "y_reg_net_h24",
            "y_rank_cs_h3",
            "y_rank_cs_h6",
            "y_rank_cs_h12",
            "y_rank_cs_h24",
        )
    ).issubset(frame.columns)
    assert set(
        (
            "btc_ret_12",
            "eth_ret_12",
            "leader_basket_ret_12",
            "market_breadth_pos_12",
            "market_dispersion_12",
            "turnover_concentration_hhi",
            "rel_strength_vs_btc_12",
            "hour_sin",
            "hour_cos",
            "dow_sin",
            "dow_cos",
            "weekend_flag",
            "asia_us_overlap_flag",
            "utc_session_bucket",
            "price_trend_short",
            "price_trend_med",
            "price_trend_long",
            "volume_trend_long",
            "trend_consensus",
            "trend_vs_market",
            "oflow_v1_signed_volume_imbalance_1",
            "oflow_v1_signed_count_imbalance_1",
            "oflow_v1_signed_volume_imbalance_3",
            "oflow_v1_signed_volume_imbalance_12",
            "oflow_v1_flow_sign_persistence_12",
            "oflow_v1_depth_conditioned_flow_1",
            "oflow_v1_trade_book_imbalance_gap_1",
            "oflow_v1_spread_conditioned_flow_1",
            "oflow_v1_microprice_conditioned_flow_1",
            "mom_x_illiq",
            "mom_x_spread",
            "spread_x_vol",
            "rel_strength_x_btc_regime",
            "one_m_pressure_x_spread",
            "volume_z_x_trend",
        )
    ).issubset(frame.columns)
    assert set(frame.get_column("market").unique().to_list()) == {"KRW-BTC", "KRW-ETH"}
    assert set(frame.get_column("y_cls_topq_12").drop_nulls().unique().to_list()) == {0, 1}
    assert frame.filter(pl.col("y_rank_cs_12").is_between(0.0, 1.0, closed="both")).height == frame.height
    assert frame.filter(pl.col("market_breadth_pos_12").is_between(0.0, 1.0, closed="both")).height == frame.height
    assert frame.get_column("turnover_concentration_hhi").null_count() == 0
    assert frame.filter(pl.col("hour_sin").is_between(-1.0, 1.0, closed="both")).height == frame.height
    assert frame.filter(pl.col("dow_cos").is_between(-1.0, 1.0, closed="both")).height == frame.height
    assert frame.filter(pl.col("weekend_flag").is_in([0.0, 1.0])).height == frame.height
    assert frame.filter(pl.col("asia_us_overlap_flag").is_in([0.0, 1.0])).height == frame.height
    assert frame.filter(pl.col("utc_session_bucket").is_in([0.0, 1.0, 2.0, 3.0])).height == frame.height
    assert frame.get_column("price_trend_short").null_count() == 0
    assert frame.get_column("volume_trend_long").null_count() == 0
    assert frame.filter(pl.col("trend_consensus").is_between(-1.0, 1.0, closed="both")).height == frame.height
    assert frame.get_column("oflow_v1_signed_volume_imbalance_1").null_count() == 0
    assert frame.get_column("oflow_v1_depth_conditioned_flow_1").null_count() == 0
    assert frame.get_column("oflow_v1_microprice_conditioned_flow_1").null_count() == 0
    assert "ctrend_v1_rsi_14" not in frame.columns
    assert "ctrend_v1_cci_20" not in frame.columns
    assert "ctrend_v1_macd_hist_12_26_9" not in frame.columns
    assert "ctrend_v1_ma_gap_200" not in frame.columns
    assert "ctrend_v1_vol_ma_gap_200" not in frame.columns
    assert "ctrend_v1_boll_width_20_2" not in frame.columns
    assert frame.get_column("mom_x_illiq").null_count() == 0
    assert frame.get_column("one_m_pressure_x_spread").null_count() == 0
    assert frame.get_column("volume_z_x_trend").null_count() == 0


def test_validate_features_v4_ignores_stale_partitions_outside_requested_window(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    features_root = tmp_path / "features"
    _write_candles(parquet_root / "candles_api_v1")
    _write_candles_history(parquet_root / "candles_v1")
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
            horizon_bars=12,
            horizons_bars=(3, 6, 12, 24),
            primary_horizon_bars=12,
            fee_bps_est=10.0,
            safety_bps=5.0,
            top_quantile=0.49,
            bottom_quantile=0.49,
            neutral_policy="drop",
        ),
        label_v3=LabelV3CryptoCsConfig(
            horizons_bars=(3, 6, 12, 24),
            primary_horizon_bars=12,
            fee_bps_est=10.0,
            safety_bps=5.0,
            top_quantile=0.49,
            bottom_quantile=0.49,
            neutral_policy="drop",
        ),
        validation=FeaturesV4ValidateConfig(leakage_fail_on_future_ts=True),
        float_dtype="float32",
    )

    build_features_dataset_v4(
        config,
        FeatureBuildV4Options(
            tf="5m",
            quote="KRW",
            top_n=2,
            start="2026-01-01",
            end="2026-01-03",
            feature_set="v4",
            label_set="v3",
            dry_run=False,
        ),
    )

    stale_dir = features_root / "features_v4_test" / "tf=5m" / "market=KRW-BTC" / "date=2026-01-04"
    stale_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "ts_ms": [1_704_326_400_000],
            "market": ["KRW-BTC"],
            "sample_weight": [1.0],
            "close": [9999.0],
            "y_reg_net_12": [1.0],
            "y_cls_topq_12": [1],
            "legacy_only_extra": [123.0],
        }
    ).write_parquet(stale_dir / "part-000.parquet")

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


def test_pipeline_v4_requires_micro_validate_report_overlap(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    features_root = tmp_path / "features"
    _write_candles(parquet_root / "candles_api_v1")
    _write_candles_history(parquet_root / "candles_v1")
    _write_micro(parquet_root / "micro_v1", report_date="2025-12-31")

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
            horizon_bars=12,
            horizons_bars=(3, 6, 12, 24),
            primary_horizon_bars=12,
            fee_bps_est=10.0,
            safety_bps=5.0,
            top_quantile=0.49,
            bottom_quantile=0.49,
            neutral_policy="drop",
        ),
        label_v3=LabelV3CryptoCsConfig(
            horizons_bars=(3, 6, 12, 24),
            primary_horizon_bars=12,
            fee_bps_est=10.0,
            safety_bps=5.0,
            top_quantile=0.49,
            bottom_quantile=0.49,
            neutral_policy="drop",
        ),
        validation=FeaturesV4ValidateConfig(leakage_fail_on_future_ts=True),
        float_dtype="float32",
    )

    with pytest.raises(ValueError, match="micro validate report does not overlap"):
        build_features_dataset_v4(
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


def test_pipeline_v4_builds_label_v3_residualized_bundle(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    features_root = tmp_path / "features"
    _write_candles(parquet_root / "candles_api_v1")
    _write_candles_history(parquet_root / "candles_v1")
    _write_micro(parquet_root / "micro_v1")

    config = FeaturesV4Config(
        build=FeaturesV4BuildConfig(
            output_dataset="features_v4_v3_test",
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
            horizon_bars=12,
            horizons_bars=(3, 6, 12, 24),
            primary_horizon_bars=12,
            fee_bps_est=10.0,
            safety_bps=5.0,
            top_quantile=0.49,
            bottom_quantile=0.49,
            neutral_policy="drop",
        ),
        label_v3=LabelV3CryptoCsConfig(
            horizons_bars=(3, 6, 12, 24),
            primary_horizon_bars=12,
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
            label_set="v3",
            dry_run=False,
        ),
    )

    assert summary.rows_final > 0
    label_spec = json.loads(
        (features_root / "features_v4_v3_test" / "_meta" / "label_spec.json").read_text(encoding="utf-8")
    )
    assert label_spec["label_set_version"] == "v3_crypto_cs_residualized"
    assert label_spec["training_default_columns"]["y_reg"] == "y_reg_resid_leader_h12"
    assert "y_reg_resid_btc_h3" in label_spec["label_columns"]
    assert "y_reg_resid_eth_h24" in label_spec["label_columns"]
    assert "y_rank_resid_leader_h12" in label_spec["label_columns"]
    assert "y_cls_resid_leader_topq_h12" in label_spec["label_columns"]

    files = sorted((features_root / "features_v4_v3_test").glob("tf=5m/market=*/date=*/*.parquet"))
    assert files
    frame = pl.concat([pl.read_parquet(path) for path in files], how="vertical_relaxed")
    assert set(
        (
            "y_reg_net_h3",
            "y_reg_resid_btc_h3",
            "y_reg_resid_eth_h3",
            "y_reg_resid_leader_h3",
            "y_rank_resid_leader_h12",
            "y_cls_resid_leader_topq_h12",
        )
    ).issubset(frame.columns)


def test_pipeline_v4_supports_one_minute_feature_build_dry_run(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    features_root = tmp_path / "features"
    _write_candles(parquet_root / "candles_api_v1", count_1m=5_000)
    _write_candles_history(parquet_root / "candles_v1")
    _write_micro(parquet_root / "micro_v1", tf="1m", count=5_000)

    config = FeaturesV4Config(
        build=FeaturesV4BuildConfig(
            output_dataset="features_v4_test_1m",
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
        universe=UniverseConfig(quote="KRW", mode="static_start", top_n=2, lookback_days=7, fixed_list=()),
        time_range=TimeRangeConfig(start="2026-01-01", end="2026-01-03"),
        feature_set_v1=FeatureSetV1Config(
            windows=FeatureWindows(ret=(1, 3, 6, 12), rv=(12, 36), ema=(12, 36), rsi=14, atr=14, vol_z=36),
            enable_factor_features=False,
            factor_markets=(),
            enable_liquidity_rank=False,
        ),
        label_v2=LabelV2CryptoCsConfig(
            horizon_bars=3,
            horizons_bars=(1, 2, 3, 6),
            primary_horizon_bars=3,
            fee_bps_est=0.0,
            safety_bps=0.0,
            top_quantile=0.5,
            bottom_quantile=0.5,
            neutral_policy="drop",
        ),
        label_v3=LabelV3CryptoCsConfig(
            horizons_bars=(1, 2, 3, 6),
            primary_horizon_bars=3,
            fee_bps_est=0.0,
            safety_bps=0.0,
            top_quantile=0.5,
            bottom_quantile=0.5,
            neutral_policy="drop",
        ),
        validation=FeaturesV4ValidateConfig(leakage_fail_on_future_ts=True),
        float_dtype="float32",
    )

    summary = build_features_dataset_v4(
        config,
        FeatureBuildV4Options(
            tf="1m",
            quote="KRW",
            top_n=2,
                start="2026-01-01",
                end="2026-01-03",
                feature_set="v4",
                label_set="v2",
                dry_run=True,
            ),
        )

    assert summary.preflight_ok is True
    report = json.loads((features_root / "features_v4_test_1m" / "_meta" / "build_report.json").read_text(encoding="utf-8"))
    assert report["tf"] == "1m"
    assert report["status"] == "PASS"
    assert report["dry_run"] is True


def test_pipeline_v4_adapts_one_m_required_bars_for_one_minute_base_tf(tmp_path: Path) -> None:
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

    result = build_feature_set_v4_live_base_from_candles(
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
    _write_tf(dataset_root, tf="1m", market="KRW-BTC", start_ts=start_ts, count=count_1m, interval_ms=60_000, slope=0.22)
    _write_tf(dataset_root, tf="5m", market="KRW-BTC", start_ts=start_ts, count=620, interval_ms=300_000, slope=0.22)
    _write_tf(dataset_root, tf="15m", market="KRW-BTC", start_ts=start_ts, count=230, interval_ms=900_000, slope=0.22)
    _write_tf(dataset_root, tf="60m", market="KRW-BTC", start_ts=start_ts, count=80, interval_ms=3_600_000, slope=0.22)
    _write_tf(dataset_root, tf="240m", market="KRW-BTC", start_ts=start_ts, count=25, interval_ms=14_400_000, slope=0.22)

    _write_tf(dataset_root, tf="1m", market="KRW-ETH", start_ts=start_ts, count=count_1m, interval_ms=60_000, slope=0.06)
    _write_tf(dataset_root, tf="5m", market="KRW-ETH", start_ts=start_ts, count=620, interval_ms=300_000, slope=0.06)
    _write_tf(dataset_root, tf="15m", market="KRW-ETH", start_ts=start_ts, count=230, interval_ms=900_000, slope=0.06)
    _write_tf(dataset_root, tf="60m", market="KRW-ETH", start_ts=start_ts, count=80, interval_ms=3_600_000, slope=0.06)
    _write_tf(dataset_root, tf="240m", market="KRW-ETH", start_ts=start_ts, count=25, interval_ms=14_400_000, slope=0.06)


def _write_candles_history(dataset_root: Path) -> None:
    start_ts = int(datetime(2025, 4, 1, tzinfo=timezone.utc).timestamp() * 1000)
    _write_tf(dataset_root, tf="5m", market="KRW-BTC", start_ts=start_ts, count=82_000, interval_ms=300_000, slope=0.04)
    _write_tf(dataset_root, tf="5m", market="KRW-ETH", start_ts=start_ts, count=82_000, interval_ms=300_000, slope=0.02)


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


def _write_micro(
    dataset_root: Path,
    *,
    data_date: str = "2026-01-01",
    report_date: str | None = None,
    tf: str = "5m",
    count: int = 520,
) -> None:
    start_ts = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    tf_value = str(tf).strip().lower()
    interval_ms = 60_000 if tf_value == "1m" else 300_000
    ts = [start_ts + i * interval_ms for i in range(count)]
    report_date_value = report_date or data_date
    for market in ("KRW-BTC", "KRW-ETH"):
        part_dir = dataset_root / f"tf={tf_value}" / f"market={market}" / f"date={data_date}"
        part_dir.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            {
                "market": [market for _ in ts],
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
                "trade_count": [20 for _ in ts],
                "buy_count": [12 for _ in ts],
                "sell_count": [8 for _ in ts],
                "trade_volume_total": [20.0 for _ in ts],
                "buy_volume": [12.0 for _ in ts],
                "sell_volume": [8.0 for _ in ts],
                "microprice_bias_bps_mean": [0.2 for _ in ts],
                "depth_bid_top5_mean": [5.0 for _ in ts],
                "depth_ask_top5_mean": [4.0 for _ in ts],
                "spread_bps_mean": [1.0 for _ in ts],
            }
        ).write_parquet(part_dir / "part-000.parquet")

    meta_dir = dataset_root / "_meta"
    meta_dir.mkdir(parents=True, exist_ok=True)
    (meta_dir / "validate_report.json").write_text(
        json.dumps(
                {
                    "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    "checked_files": 2,
                    "fail_files": 0,
                    "warn_files": 0,
                    "ok_files": 2,
                    "details": [
                        {
                            "file": str(dataset_root / f"tf={tf_value}" / "market=KRW-BTC" / f"date={report_date_value}" / "part-000.parquet"),
                            "tf": tf_value,
                            "market": "KRW-BTC",
                            "date": report_date_value,
                            "rows": len(ts),
                        },
                        {
                            "file": str(dataset_root / f"tf={tf_value}" / "market=KRW-ETH" / f"date={report_date_value}" / "part-000.parquet"),
                            "tf": tf_value,
                            "market": "KRW-ETH",
                            "date": report_date_value,
                            "rows": len(ts),
                        },
                    ],
                },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
