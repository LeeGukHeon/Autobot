from __future__ import annotations

import polars as pl

from autobot.features.order_flow_panel_v1 import (
    attach_order_flow_panel_v1,
    order_flow_panel_v1_contract,
    order_flow_panel_v1_diagnostics,
)


def test_attach_order_flow_panel_v1_adds_expected_columns() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [0, 300_000, 600_000, 0, 300_000, 600_000],
            "market": ["KRW-BTC", "KRW-BTC", "KRW-BTC", "KRW-ETH", "KRW-ETH", "KRW-ETH"],
            "m_buy_volume": [6.0, 7.0, 8.0, 3.0, 2.0, 1.0],
            "m_sell_volume": [4.0, 3.0, 2.0, 5.0, 4.0, 3.0],
            "m_trade_volume_base": [10.0, 10.0, 10.0, 8.0, 6.0, 4.0],
            "m_buy_count": [6.0, 7.0, 8.0, 3.0, 2.0, 1.0],
            "m_sell_count": [4.0, 3.0, 2.0, 5.0, 4.0, 3.0],
            "m_trade_count": [10.0, 10.0, 10.0, 8.0, 6.0, 4.0],
            "m_depth_bid_top5_mean": [12.0, 12.0, 13.0, 6.0, 5.0, 4.0],
            "m_depth_ask_top5_mean": [8.0, 8.0, 7.0, 7.0, 7.0, 6.0],
            "m_spread_proxy": [2.0, 2.0, 2.0, 3.0, 4.0, 5.0],
            "m_microprice_bias_bps_mean": [5.0, 6.0, 7.0, -4.0, -5.0, -6.0],
            "m_micro_available": [True, True, True, True, True, True],
            "m_trade_coverage_ms": [250_000, 250_000, 250_000, 240_000, 240_000, 240_000],
            "m_book_coverage_ms": [260_000, 260_000, 260_000, 250_000, 250_000, 250_000],
        }
    )

    out = attach_order_flow_panel_v1(frame, float_dtype="float32")

    for column in (
        "oflow_v1_signed_volume_imbalance_1",
        "oflow_v1_signed_count_imbalance_1",
        "oflow_v1_signed_volume_imbalance_3",
        "oflow_v1_signed_volume_imbalance_12",
        "oflow_v1_flow_sign_persistence_12",
        "oflow_v1_depth_conditioned_flow_1",
        "oflow_v1_trade_book_imbalance_gap_1",
        "oflow_v1_spread_conditioned_flow_1",
        "oflow_v1_microprice_conditioned_flow_1",
    ):
        assert column in out.columns
        assert out.get_column(column).null_count() == 0

    btc_last = out.filter((pl.col("market") == "KRW-BTC") & (pl.col("ts_ms") == 600_000)).row(0, named=True)
    assert round(float(btc_last["oflow_v1_signed_volume_imbalance_1"]), 6) == 0.6
    assert float(btc_last["oflow_v1_signed_volume_imbalance_3"]) > 0.0
    assert float(btc_last["oflow_v1_depth_conditioned_flow_1"]) > 0.0


def test_order_flow_panel_v1_contract_and_diagnostics_are_explicit() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [0, 300_000, 600_000],
            "market": ["KRW-BTC", "KRW-BTC", "KRW-BTC"],
            "m_buy_volume": [5.0, 4.0, 0.0],
            "m_sell_volume": [3.0, 2.0, 0.0],
            "m_trade_volume_base": [8.0, 6.0, 0.0],
            "m_buy_count": [5.0, 4.0, 0.0],
            "m_sell_count": [3.0, 2.0, 0.0],
            "m_trade_count": [8.0, 6.0, 0.0],
            "m_depth_bid_top5_mean": [10.0, 11.0, 0.0],
            "m_depth_ask_top5_mean": [9.0, 8.0, 0.0],
            "m_spread_proxy": [2.0, 2.0, 0.0],
            "m_microprice_bias_bps_mean": [2.0, 1.0, 0.0],
            "m_micro_available": [True, True, False],
            "m_trade_coverage_ms": [250_000, 240_000, 0],
            "m_book_coverage_ms": [260_000, 250_000, 0],
        }
    )

    diagnostics = order_flow_panel_v1_diagnostics(frame)
    contract = order_flow_panel_v1_contract()

    assert contract["version"] == "order_flow_panel_v1"
    assert contract["deployment_scope"]["venue_id"] == "upbit"
    assert contract["scaling_choices"]["horizons_bars"] == [1, 3, 12]
    assert any(item["feature_name"] == "oflow_v1_depth_conditioned_flow_1" for item in contract["field_definitions"])
    assert diagnostics["rows"] == 3
    assert diagnostics["micro_available_ratio"] < 1.0
    assert diagnostics["horizon_full_availability"]["1"] > 0.0
