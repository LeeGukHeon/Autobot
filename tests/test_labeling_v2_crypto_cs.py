from __future__ import annotations

import math

import polars as pl

from autobot.features.labeling_v2_crypto_cs import (
    LabelV2CryptoCsConfig,
    apply_labeling_v2_crypto_cs,
    drop_neutral_rows_v2_crypto_cs,
    label_distribution_v2_crypto_cs,
)


def test_apply_labeling_v2_crypto_cs_builds_net_return_rank_and_classes() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [1, 2, 1, 2, 1, 2],
            "market": ["KRW-A", "KRW-A", "KRW-B", "KRW-B", "KRW-C", "KRW-C"],
            "close": [100.0, 110.0, 100.0, 102.0, 100.0, 99.0],
        }
    )
    config = LabelV2CryptoCsConfig(
        horizon_bars=1,
        fee_bps_est=0.0,
        safety_bps=0.0,
        top_quantile=0.34,
        bottom_quantile=0.34,
        neutral_policy="drop",
    )

    labeled = apply_labeling_v2_crypto_cs(frame, config=config).sort(["ts_ms", "market"])
    at_t0 = labeled.filter(pl.col("ts_ms") == 1).sort("market")

    assert set(("y_reg_net_12", "y_rank_cs_12", "y_cls_topq_12")).issubset(set(labeled.columns))
    assert math.isclose(float(at_t0.get_column("y_rank_cs_12")[0]), 1.0, rel_tol=0.0, abs_tol=1e-9)
    assert math.isclose(float(at_t0.get_column("y_rank_cs_12")[1]), 0.5, rel_tol=0.0, abs_tol=1e-9)
    assert math.isclose(float(at_t0.get_column("y_rank_cs_12")[2]), 0.0, rel_tol=0.0, abs_tol=1e-9)
    assert at_t0.get_column("y_cls_topq_12").to_list() == [1, None, 0]


def test_apply_labeling_v2_crypto_cs_applies_fee_and_safety_haircut() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [1, 2, 1, 2],
            "market": ["KRW-A", "KRW-A", "KRW-B", "KRW-B"],
            "close": [100.0, 101.0, 100.0, 99.0],
        }
    )
    config = LabelV2CryptoCsConfig(
        horizon_bars=1,
        fee_bps_est=10.0,
        safety_bps=5.0,
        top_quantile=0.49,
        bottom_quantile=0.49,
        neutral_policy="keep_as_class",
    )

    labeled = apply_labeling_v2_crypto_cs(frame, config=config).sort(["market", "ts_ms"])
    a_row = labeled.filter((pl.col("market") == "KRW-A") & (pl.col("ts_ms") == 1)).row(0, named=True)
    gross = math.log(101.0 / 100.0)
    assert math.isclose(float(a_row["y_reg_net_12"]), gross - 0.0015, rel_tol=0.0, abs_tol=1e-9)


def test_drop_neutral_rows_v2_crypto_cs_and_distribution() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [1, 2, 1, 2, 1, 2],
            "market": ["KRW-A", "KRW-A", "KRW-B", "KRW-B", "KRW-C", "KRW-C"],
            "close": [100.0, 110.0, 100.0, 102.0, 100.0, 99.0],
        }
    )
    config = LabelV2CryptoCsConfig(
        horizon_bars=1,
        fee_bps_est=0.0,
        safety_bps=0.0,
        top_quantile=0.34,
        bottom_quantile=0.34,
        neutral_policy="keep_as_class",
    )

    labeled = apply_labeling_v2_crypto_cs(frame, config=config)
    dist = label_distribution_v2_crypto_cs(labeled, config=config)
    assert dist["pos"] == 1
    assert dist["neg"] == 1
    assert dist["neutral"] == 1

    dropped = drop_neutral_rows_v2_crypto_cs(
        labeled,
        config=LabelV2CryptoCsConfig(
            horizon_bars=1,
            fee_bps_est=0.0,
            safety_bps=0.0,
            top_quantile=0.34,
            bottom_quantile=0.34,
            neutral_policy="drop",
        ),
    )
    assert dropped.filter(pl.col("ts_ms") == 1).height == 2
