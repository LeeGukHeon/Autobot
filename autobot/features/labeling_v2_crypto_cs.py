"""Cross-sectional crypto labels for the next alpha research lane."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import polars as pl


VALID_NEUTRAL_POLICIES_V2 = {"drop", "keep_as_class"}


@dataclass(frozen=True)
class LabelV2CryptoCsConfig:
    horizon_bars: int = 12
    fee_bps_est: float = 10.0
    safety_bps: float = 5.0
    top_quantile: float = 0.2
    bottom_quantile: float = 0.2
    neutral_policy: str = "drop"


def apply_labeling_v2_crypto_cs(
    frame: pl.DataFrame,
    *,
    config: LabelV2CryptoCsConfig,
    ts_col: str = "ts_ms",
    market_col: str = "market",
    close_col: str = "close",
) -> pl.DataFrame:
    if frame.height <= 0:
        return frame.with_columns(
            [
                pl.lit(None, dtype=pl.Float64).alias("y_reg_net_12"),
                pl.lit(None, dtype=pl.Float64).alias("y_rank_cs_12"),
                pl.lit(None, dtype=pl.Int8).alias("y_cls_topq_12"),
            ]
        )

    _validate_config(config)
    horizon = max(int(config.horizon_bars), 1)
    total_cost_fraction = max(float(config.fee_bps_est) + float(config.safety_bps), 0.0) / 10_000.0
    neutral_policy = str(config.neutral_policy).strip().lower()
    top_threshold = 1.0 - float(config.top_quantile)
    bottom_threshold = float(config.bottom_quantile)

    working = frame.sort([market_col, ts_col]).with_columns(
        (
            pl.col(close_col)
            .shift(-horizon)
            .over(market_col)
            .truediv(pl.col(close_col))
            .log()
            .sub(pl.lit(total_cost_fraction, dtype=pl.Float64))
        ).alias("y_reg_net_12")
    )

    working = working.with_columns(
        [
            pl.len().over(ts_col).cast(pl.Int64).alias("__cs_count"),
            pl.col("y_reg_net_12").rank(method="average").over(ts_col).cast(pl.Float64).alias("__cs_rank_asc"),
        ]
    )

    rank_pct = (
        pl.when(pl.col("y_reg_net_12").is_null())
        .then(pl.lit(None, dtype=pl.Float64))
        .when(pl.col("__cs_count") <= 1)
        .then(pl.lit(1.0, dtype=pl.Float64))
        .otherwise(
            (pl.col("__cs_rank_asc") - pl.lit(1.0, dtype=pl.Float64))
            .truediv(pl.col("__cs_count").cast(pl.Float64) - pl.lit(1.0, dtype=pl.Float64))
        )
        .alias("y_rank_cs_12")
    )
    working = working.with_columns(rank_pct)

    if neutral_policy == "keep_as_class":
        cls_expr = (
            pl.when(pl.col("y_rank_cs_12").is_null())
            .then(pl.lit(None, dtype=pl.Int8))
            .when(pl.col("y_rank_cs_12") >= pl.lit(top_threshold, dtype=pl.Float64))
            .then(pl.lit(1, dtype=pl.Int8))
            .when(pl.col("y_rank_cs_12") <= pl.lit(bottom_threshold, dtype=pl.Float64))
            .then(pl.lit(0, dtype=pl.Int8))
            .otherwise(pl.lit(2, dtype=pl.Int8))
            .alias("y_cls_topq_12")
        )
    else:
        cls_expr = (
            pl.when(pl.col("y_rank_cs_12").is_null())
            .then(pl.lit(None, dtype=pl.Int8))
            .when(pl.col("y_rank_cs_12") >= pl.lit(top_threshold, dtype=pl.Float64))
            .then(pl.lit(1, dtype=pl.Int8))
            .when(pl.col("y_rank_cs_12") <= pl.lit(bottom_threshold, dtype=pl.Float64))
            .then(pl.lit(0, dtype=pl.Int8))
            .otherwise(pl.lit(None, dtype=pl.Int8))
            .alias("y_cls_topq_12")
        )

    return working.with_columns(cls_expr).drop(["__cs_count", "__cs_rank_asc"])


def drop_neutral_rows_v2_crypto_cs(frame: pl.DataFrame, *, config: LabelV2CryptoCsConfig) -> pl.DataFrame:
    if str(config.neutral_policy).strip().lower() == "drop":
        return frame.filter(pl.col("y_cls_topq_12").is_not_null() & (pl.col("y_cls_topq_12") != 2))
    return frame


def label_distribution_v2_crypto_cs(frame: pl.DataFrame, *, config: LabelV2CryptoCsConfig) -> dict[str, Any]:
    if frame.height <= 0:
        return {"pos": 0, "neg": 0, "neutral": 0, "total": 0}

    pos = int(frame.filter(pl.col("y_cls_topq_12") == 1).height)
    neg = int(frame.filter(pl.col("y_cls_topq_12") == 0).height)
    if str(config.neutral_policy).strip().lower() == "keep_as_class":
        neutral = int(frame.filter(pl.col("y_cls_topq_12") == 2).height)
    else:
        neutral = int(frame.filter(pl.col("y_cls_topq_12").is_null() & pl.col("y_rank_cs_12").is_not_null()).height)
    return {"pos": pos, "neg": neg, "neutral": neutral, "total": int(frame.height)}


def _validate_config(config: LabelV2CryptoCsConfig) -> None:
    neutral_policy = str(config.neutral_policy).strip().lower()
    if neutral_policy not in VALID_NEUTRAL_POLICIES_V2:
        raise ValueError(f"neutral_policy must be one of: {', '.join(sorted(VALID_NEUTRAL_POLICIES_V2))}")
    top_quantile = float(config.top_quantile)
    bottom_quantile = float(config.bottom_quantile)
    if not 0.0 < top_quantile < 0.5:
        raise ValueError("top_quantile must be between 0 and 0.5")
    if not 0.0 < bottom_quantile < 0.5:
        raise ValueError("bottom_quantile must be between 0 and 0.5")
