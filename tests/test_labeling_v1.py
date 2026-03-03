from __future__ import annotations

import math

import polars as pl

from autobot.features.feature_spec import LabelV1Config
from autobot.features.labeling_v1 import apply_labeling_v1, drop_neutral_rows


def test_labeling_v1_shift_direction_uses_future_close() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [0, 1, 2, 3, 4],
            "close": [100.0, 101.0, 102.0, 103.0, 104.0],
        }
    )
    config = LabelV1Config(horizon_bars=2, thr_bps=1.0, neutral_policy="drop", fee_bps_est=10.0, safety_bps=5.0)
    labeled = apply_labeling_v1(frame, config=config)

    y_reg_0 = labeled.item(row=0, column="y_reg")
    expected = math.log(102.0 / 100.0)
    assert y_reg_0 is not None
    assert math.isclose(float(y_reg_0), expected, rel_tol=1e-9, abs_tol=1e-12)


def test_labeling_v1_neutral_policy_drop_and_keep() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [0, 1, 2, 3],
            "close": [100.0, 100.1, 100.2, 100.3],
        }
    )

    drop_cfg = LabelV1Config(horizon_bars=1, thr_bps=100.0, neutral_policy="drop", fee_bps_est=10.0, safety_bps=5.0)
    dropped = drop_neutral_rows(apply_labeling_v1(frame, config=drop_cfg), config=drop_cfg)
    assert dropped.height == 0

    keep_cfg = LabelV1Config(
        horizon_bars=1,
        thr_bps=100.0,
        neutral_policy="keep_as_class",
        fee_bps_est=10.0,
        safety_bps=5.0,
    )
    kept = apply_labeling_v1(frame, config=keep_cfg)
    assert int(kept.filter(pl.col("y_cls") == 2).height) >= 1
