from __future__ import annotations

import polars as pl

from autobot.features.feature_set_v4 import attach_spillover_breadth_features_v4, feature_columns_v4


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
