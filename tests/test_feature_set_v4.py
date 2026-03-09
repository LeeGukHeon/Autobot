from __future__ import annotations

import polars as pl

from autobot.features.ctrend_v1 import attach_ctrend_v1_features, ctrend_v1_factor_contract
from autobot.features.feature_set_v4 import (
    attach_interaction_features_v4,
    attach_order_flow_panel_v1,
    attach_periodicity_features_v4,
    attach_spillover_breadth_features_v4,
    attach_trend_volume_features_v4,
    feature_columns_v4,
)


def test_attach_spillover_breadth_features_v4_adds_cross_sectional_columns() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [1000, 1000, 2000, 2000],
            "market": ["KRW-BTC", "KRW-ETH", "KRW-BTC", "KRW-ETH"],
            "close": [100.0, 50.0, 101.0, 49.5],
            "volume_base": [10.0, 20.0, 11.0, 18.0],
            "logret_1": [0.0100, -0.0200, 0.0150, 0.0050],
            "logret_3": [0.0200, -0.0100, 0.0180, 0.0070],
            "logret_12": [0.0300, -0.0400, 0.0250, 0.0040],
        }
    )

    out = attach_spillover_breadth_features_v4(frame, quote="KRW", float_dtype="float32")

    for column in (
        "btc_ret_1",
        "eth_ret_1",
        "leader_basket_ret_12",
        "market_breadth_pos_1",
        "market_breadth_pos_12",
        "market_dispersion_12",
        "turnover_concentration_hhi",
        "rel_strength_vs_btc_12",
    ):
        assert column in out.columns
        assert out.get_column(column).null_count() == 0

    btc_row = out.filter((pl.col("ts_ms") == 1000) & (pl.col("market") == "KRW-ETH"))
    assert round(float(btc_row.item(0, "btc_ret_1")), 4) == 0.01
    assert round(float(btc_row.item(0, "rel_strength_vs_btc_12")), 4) == -0.07
    assert 0.0 <= float(btc_row.item(0, "market_breadth_pos_1")) <= 1.0
    assert float(btc_row.item(0, "turnover_concentration_hhi")) > 0.0


def test_feature_columns_v4_extends_v3_contract() -> None:
    columns = feature_columns_v4()

    assert "m_spread_proxy" in columns
    assert "btc_ret_12" in columns
    assert "market_dispersion_12" in columns
    assert "hour_sin" in columns
    assert "utc_session_bucket" in columns
    assert "price_trend_short" in columns
    assert "trend_consensus" in columns
    assert "oflow_v1_signed_volume_imbalance_1" in columns
    assert "oflow_v1_depth_conditioned_flow_1" in columns
    assert "ctrend_v1_boll_mid_gap_20" in columns
    assert "ctrend_v1_macd_hist_12_26_9" in columns
    assert "mom_x_illiq" in columns
    assert "volume_z_x_trend" in columns


def test_attach_periodicity_features_v4_adds_utc_time_columns() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [
                1_704_067_200_000,  # 2024-01-01 00:00:00 UTC
                1_704_114_000_000,  # 2024-01-01 13:00:00 UTC
            ],
            "market": ["KRW-BTC", "KRW-BTC"],
        }
    )

    out = attach_periodicity_features_v4(frame, float_dtype="float32")

    for column in ("hour_sin", "hour_cos", "dow_sin", "dow_cos", "weekend_flag", "asia_us_overlap_flag", "utc_session_bucket"):
        assert column in out.columns
        assert out.get_column(column).null_count() == 0

    first = out.row(0, named=True)
    second = out.row(1, named=True)
    assert round(float(first["hour_sin"]), 6) == 0.0
    assert round(float(first["hour_cos"]), 6) == 1.0
    assert float(first["utc_session_bucket"]) == 0.0
    assert float(second["asia_us_overlap_flag"]) == 1.0
    assert float(second["utc_session_bucket"]) == 2.0


def test_attach_trend_volume_features_v4_adds_aggregate_trend_columns() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [1000, 2000, 3000, 1000, 2000, 3000],
            "market": ["KRW-BTC", "KRW-BTC", "KRW-BTC", "KRW-ETH", "KRW-ETH", "KRW-ETH"],
            "volume_base": [10.0, 11.0, 12.0, 20.0, 21.0, 22.0],
            "one_m_real_volume_sum": [8.0, 9.0, 10.0, 16.0, 18.0, 20.0],
            "one_m_volume_sum": [8.0, 9.0, 10.0, 16.0, 18.0, 20.0],
            "volume_z": [0.4, 0.5, 0.6, -0.1, 0.0, 0.1],
            "logret_1": [0.01, 0.015, 0.02, -0.01, 0.0, 0.01],
            "logret_3": [0.02, 0.018, 0.017, -0.02, -0.01, 0.005],
            "logret_12": [0.03, 0.025, 0.022, -0.03, -0.02, 0.0],
            "logret_36": [0.05, 0.045, 0.04, -0.04, -0.03, -0.01],
            "h15m_ret_1": [0.01, 0.012, 0.013, -0.01, -0.005, 0.0],
            "h60m_ret_1": [0.02, 0.021, 0.022, -0.015, -0.01, -0.005],
            "h60m_ret_3": [0.03, 0.031, 0.032, -0.02, -0.015, -0.01],
            "h240m_ret_1": [0.04, 0.041, 0.042, -0.025, -0.02, -0.015],
            "h240m_ret_3": [0.05, 0.051, 0.052, -0.03, -0.025, -0.02],
            "leader_basket_ret_12": [0.015, 0.016, 0.017, 0.015, 0.016, 0.017],
        }
    )

    out = attach_trend_volume_features_v4(frame, float_dtype="float32")

    for column in (
        "price_trend_short",
        "price_trend_med",
        "price_trend_long",
        "volume_trend_long",
        "trend_consensus",
        "trend_vs_market",
    ):
        assert column in out.columns
        assert out.get_column(column).null_count() == 0

    row = out.filter((pl.col("market") == "KRW-BTC") & (pl.col("ts_ms") == 3000)).row(0, named=True)
    assert float(row["price_trend_short"]) > 0.0
    assert -1.0 <= float(row["trend_consensus"]) <= 1.0
    assert round(float(row["trend_vs_market"]), 6) == round(float(row["price_trend_med"] - row["leader_basket_ret_12"]), 6)


def test_attach_interaction_features_v4_adds_expected_cross_terms() -> None:
    frame = pl.DataFrame(
        {
            "price_trend_short": [0.5, -0.2],
            "price_trend_med": [0.3, -0.1],
            "volume_z": [0.4, -0.5],
            "m_spread_proxy": [2.0, 3.0],
            "vol_12": [0.1, 0.2],
            "rel_strength_vs_btc_12": [0.08, -0.04],
            "btc_ret_12": [0.03, -0.02],
            "m_signed_volume": [5.0, -4.0],
            "m_trade_volume_base": [10.0, 8.0],
            "one_m_ret_mean": [0.01, -0.02],
            "one_m_real_volume_sum": [9.0, 7.0],
            "volume_base": [12.0, 9.0],
        }
    )

    out = attach_interaction_features_v4(frame, float_dtype="float32")

    for column in (
        "mom_x_illiq",
        "mom_x_spread",
        "spread_x_vol",
        "rel_strength_x_btc_regime",
        "one_m_pressure_x_spread",
        "volume_z_x_trend",
    ):
        assert column in out.columns
        assert out.get_column(column).null_count() == 0

    first = out.row(0, named=True)
    assert round(float(first["mom_x_spread"]), 6) == 1.0
    assert round(float(first["spread_x_vol"]), 6) == 0.2
    assert round(float(first["rel_strength_x_btc_regime"]), 6) == 0.08
    assert round(float(first["one_m_pressure_x_spread"]), 6) == 1.0
    assert round(float(first["volume_z_x_trend"]), 6) == 0.12


def test_attach_order_flow_panel_v1_adds_expected_state_features() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [0, 300_000, 600_000],
            "market": ["KRW-BTC", "KRW-BTC", "KRW-BTC"],
            "m_buy_volume": [6.0, 7.0, 8.0],
            "m_sell_volume": [4.0, 3.0, 2.0],
            "m_trade_volume_base": [10.0, 10.0, 10.0],
            "m_buy_count": [6.0, 7.0, 8.0],
            "m_sell_count": [4.0, 3.0, 2.0],
            "m_trade_count": [10.0, 10.0, 10.0],
            "m_depth_bid_top5_mean": [12.0, 12.0, 13.0],
            "m_depth_ask_top5_mean": [8.0, 8.0, 7.0],
            "m_spread_proxy": [2.0, 2.0, 2.0],
            "m_microprice_bias_bps_mean": [5.0, 6.0, 7.0],
        }
    )

    out = attach_order_flow_panel_v1(frame, float_dtype="float32")

    for column in (
        "oflow_v1_signed_volume_imbalance_1",
        "oflow_v1_signed_count_imbalance_1",
        "oflow_v1_signed_volume_imbalance_3",
        "oflow_v1_signed_volume_imbalance_12",
        "oflow_v1_depth_conditioned_flow_1",
        "oflow_v1_spread_conditioned_flow_1",
    ):
        assert column in out.columns
        assert out.get_column(column).null_count() == 0

    row = out.tail(1).row(0, named=True)
    assert round(float(row["oflow_v1_signed_volume_imbalance_1"]), 6) == 0.6
    assert float(row["oflow_v1_signed_volume_imbalance_3"]) > 0.0
    assert float(row["oflow_v1_depth_conditioned_flow_1"]) > 0.0


def test_attach_ctrend_v1_features_adds_compact_literature_aligned_columns() -> None:
    ts = list(range(260))
    frame = pl.DataFrame(
        {
            "ts_ms": [1_700_000_000_000 + (value * 86_400_000) for value in ts],
            "market": ["KRW-BTC" for _ in ts],
            "open": [100.0 + value * 0.3 for value in ts],
            "high": [100.5 + value * 0.3 for value in ts],
            "low": [99.5 + value * 0.3 for value in ts],
            "close": [100.2 + value * 0.3 for value in ts],
            "volume_base": [1000.0 + value * 2.0 for value in ts],
        }
    )

    out = attach_ctrend_v1_features(frame, float_dtype="float32")

    for column in (
        "ctrend_v1_rsi_14",
        "ctrend_v1_stochrsi_14",
        "ctrend_v1_stoch_k_14_3",
        "ctrend_v1_stoch_d_14_3_3",
        "ctrend_v1_cci_20",
        "ctrend_v1_macd_line_12_26",
        "ctrend_v1_macd_hist_12_26_9",
        "ctrend_v1_ma_gap_200",
        "ctrend_v1_vol_ma_gap_200",
        "ctrend_v1_chaikin_mf_20",
        "ctrend_v1_boll_width_20_2",
    ):
        assert column in out.columns

    last = out.tail(1).row(0, named=True)
    assert float(last["ctrend_v1_rsi_14"]) > 0.0
    assert float(last["ctrend_v1_macd_line_12_26"]) > 0.0
    assert float(last["ctrend_v1_cci_20"]) > 0.0
    assert last["ctrend_v1_ma_gap_200"] is not None
    assert last["ctrend_v1_vol_ma_gap_200"] is not None


def test_ctrend_v1_factor_contract_persists_paper_and_adaptation_metadata() -> None:
    contract = ctrend_v1_factor_contract()

    assert contract["version"] == "ctrend_v1"
    assert contract["paper_original_definition"]["aggregation_model"] == "cross-sectional combined elastic net (CS-C-ENet)"
    assert contract["deployment_adaptation"]["hardcoded_factor_weights"] is False
    assert contract["paper_original_definition"]["input_panel_size"] == 28
    assert any(item["feature_name"] == "ctrend_v1_cci_20" for item in contract["indicator_definitions"])
    assert any(item["feature_name"] == "ctrend_v1_vol_ma_gap_200" for item in contract["indicator_definitions"])
