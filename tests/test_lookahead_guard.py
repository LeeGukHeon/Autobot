from __future__ import annotations

import math

import polars as pl

from autobot.features.feature_set_v1 import compute_feature_set_v1
from autobot.features.feature_spec import FeatureSetV1Config, FeatureWindows, feature_columns


def test_feature_rows_do_not_change_when_future_close_changes() -> None:
    frame = _make_candles(rows=140)
    config = FeatureSetV1Config(
        windows=FeatureWindows(ret=(1, 3, 6, 12), rv=(12, 36), ema=(12, 36), rsi=14, atr=14, vol_z=36),
        enable_factor_features=False,
        factor_markets=(),
        enable_liquidity_rank=False,
    )

    baseline = compute_feature_set_v1(frame, tf="5m", config=config, float_dtype="float64")

    pivot_idx = 80
    mutated_source = (
        frame.with_row_index("__idx")
        .with_columns(
            pl.when(pl.col("__idx") >= pivot_idx)
            .then(pl.col("close") * 1.2)
            .otherwise(pl.col("close"))
            .alias("close")
        )
        .drop("__idx")
    )
    mutated = compute_feature_set_v1(mutated_source, tf="5m", config=config, float_dtype="float64")

    cols = [col for col in feature_columns(config) if col in baseline.columns and col in mutated.columns]
    left = baseline.head(pivot_idx).select(cols)
    right = mutated.head(pivot_idx).select(cols)

    assert left.height == right.height
    for col in cols:
        left_vals = left.get_column(col).to_list()
        right_vals = right.get_column(col).to_list()
        for idx, (lhs, rhs) in enumerate(zip(left_vals, right_vals, strict=False)):
            if lhs is None and rhs is None:
                continue
            assert (lhs is None) == (rhs is None), f"null mismatch in {col} at {idx}"
            if isinstance(lhs, bool) or isinstance(rhs, bool):
                assert bool(lhs) == bool(rhs), f"bool mismatch in {col} at {idx}"
            else:
                assert math.isclose(float(lhs), float(rhs), rel_tol=1e-9, abs_tol=1e-12), (
                    f"value mismatch in {col} at {idx}: {lhs} != {rhs}"
                )


def _make_candles(*, rows: int) -> pl.DataFrame:
    ts = [1_704_067_200_000 + i * 300_000 for i in range(rows)]
    close = [100.0 + (i * 0.03) + ((i % 7) - 3) * 0.01 for i in range(rows)]
    return pl.DataFrame(
        {
            "ts_ms": ts,
            "open": [value - 0.05 for value in close],
            "high": [value + 0.1 for value in close],
            "low": [value - 0.1 for value in close],
            "close": close,
            "volume_base": [150.0 + i for i in range(rows)],
            "volume_quote": [close[i] * (150.0 + i) for i in range(rows)],
            "volume_quote_est": [False for _ in range(rows)],
        }
    )
