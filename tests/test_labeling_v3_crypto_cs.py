from __future__ import annotations

import math

import polars as pl

from autobot.features.labeling_v3_crypto_cs import (
    LabelV3CryptoCsConfig,
    apply_labeling_v3_crypto_cs,
)


def test_apply_labeling_v3_crypto_cs_builds_residualized_bundle() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [1, 2, 1, 2, 1, 2],
            "market": ["KRW-BTC", "KRW-BTC", "KRW-ETH", "KRW-ETH", "KRW-XRP", "KRW-XRP"],
            "close": [100.0, 110.0, 100.0, 120.0, 100.0, 130.0],
        }
    )
    config = LabelV3CryptoCsConfig(
        horizons_bars=(1,),
        primary_horizon_bars=1,
        fee_bps_est=0.0,
        safety_bps=0.0,
        top_quantile=0.34,
        bottom_quantile=0.34,
        neutral_policy="drop",
    )

    labeled = apply_labeling_v3_crypto_cs(frame, config=config).sort(["ts_ms", "market"])
    at_t0 = labeled.filter(pl.col("ts_ms") == 1).sort("market")

    assert set(
        (
            "y_reg_net_h1",
            "y_reg_resid_btc_h1",
            "y_reg_resid_eth_h1",
            "y_reg_resid_leader_h1",
            "y_rank_resid_leader_h1",
            "y_cls_resid_leader_topq_h1",
        )
    ).issubset(set(labeled.columns))

    leader = (math.log(110.0 / 100.0) + math.log(120.0 / 100.0)) / 2.0
    xrp = at_t0.filter(pl.col("market") == "KRW-XRP").row(0, named=True)
    btc = at_t0.filter(pl.col("market") == "KRW-BTC").row(0, named=True)
    eth = at_t0.filter(pl.col("market") == "KRW-ETH").row(0, named=True)

    assert math.isclose(float(xrp["y_reg_resid_leader_h1"]), math.log(130.0 / 100.0) - leader, rel_tol=0.0, abs_tol=1e-9)
    assert math.isclose(float(btc["y_reg_resid_btc_h1"]), 0.0, rel_tol=0.0, abs_tol=1e-9)
    assert math.isclose(float(eth["y_reg_resid_eth_h1"]), 0.0, rel_tol=0.0, abs_tol=1e-9)
    assert at_t0.get_column("y_cls_resid_leader_topq_h1").to_list() == [0, None, 1]
