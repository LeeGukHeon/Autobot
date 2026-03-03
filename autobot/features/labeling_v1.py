"""Lookahead-safe labeling helpers for feature store v1."""

from __future__ import annotations

from typing import Any

import polars as pl

from .feature_spec import LabelV1Config, effective_threshold_bps


def apply_labeling_v1(frame: pl.DataFrame, *, config: LabelV1Config) -> pl.DataFrame:
    """Attach regression/classification labels from future close."""

    threshold = effective_threshold_fraction(config)
    horizon = max(1, int(config.horizon_bars))
    neutral_policy = str(config.neutral_policy).strip().lower()

    working = frame.sort("ts_ms").with_columns(
        (
            pl.col("close").shift(-horizon).log() - pl.col("close").log()
        ).alias("y_reg")
    )

    if neutral_policy == "keep_as_class":
        cls_expr = (
            pl.when(pl.col("y_reg") > threshold)
            .then(pl.lit(1, dtype=pl.Int8))
            .when(pl.col("y_reg") < -threshold)
            .then(pl.lit(0, dtype=pl.Int8))
            .otherwise(pl.lit(2, dtype=pl.Int8))
            .alias("y_cls")
        )
    else:
        cls_expr = (
            pl.when(pl.col("y_reg") > threshold)
            .then(pl.lit(1, dtype=pl.Int8))
            .when(pl.col("y_reg") < -threshold)
            .then(pl.lit(0, dtype=pl.Int8))
            .otherwise(pl.lit(None, dtype=pl.Int8))
            .alias("y_cls")
        )

    return working.with_columns(
        [
            pl.col("y_reg").cast(pl.Float64).alias("y_reg"),
            cls_expr,
        ]
    )


def effective_threshold_fraction(config: LabelV1Config) -> float:
    return effective_threshold_bps(config) / 10_000.0


def drop_neutral_rows(frame: pl.DataFrame, *, config: LabelV1Config) -> pl.DataFrame:
    neutral_policy = str(config.neutral_policy).strip().lower()
    if neutral_policy == "drop":
        return frame.filter(pl.col("y_cls").is_not_null())
    return frame


def label_distribution(frame: pl.DataFrame, *, config: LabelV1Config) -> dict[str, Any]:
    if frame.height <= 0:
        return {"pos": 0, "neg": 0, "neutral": 0, "total": 0}

    pos = int(frame.filter(pl.col("y_cls") == 1).height)
    neg = int(frame.filter(pl.col("y_cls") == 0).height)
    if str(config.neutral_policy).strip().lower() == "keep_as_class":
        neutral = int(frame.filter(pl.col("y_cls") == 2).height)
    else:
        neutral = int(frame.filter(pl.col("y_cls").is_null() & pl.col("y_reg").is_not_null()).height)

    total = int(frame.height)
    return {"pos": pos, "neg": neg, "neutral": neutral, "total": total}
