from __future__ import annotations

import polars as pl

from autobot.features.feature_set_v4 import (
    attach_interaction_features_v4,
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
