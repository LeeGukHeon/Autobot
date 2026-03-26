"""Cross-sectional crypto labels for the next alpha research lane."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import polars as pl


VALID_NEUTRAL_POLICIES_V2 = {"drop", "keep_as_class"}


@dataclass(frozen=True)
class LabelV2CryptoCsConfig:
    horizon_bars: int = 12
    horizons_bars: tuple[int, ...] = ()
    primary_horizon_bars: int | None = None
    fee_bps_est: float = 10.0
    safety_bps: float = 5.0
    top_quantile: float = 0.2
    bottom_quantile: float = 0.2
    neutral_policy: str = "drop"


LEGACY_Y_REG_COLUMN_V2 = "y_reg_net_12"
LEGACY_Y_RANK_COLUMN_V2 = "y_rank_cs_12"
LEGACY_Y_CLS_COLUMN_V2 = "y_cls_topq_12"


def resolve_label_horizons_v2_crypto_cs(config: LabelV2CryptoCsConfig) -> tuple[tuple[int, ...], int]:
    configured: list[int] = []
    for value in getattr(config, "horizons_bars", ()) or ():
        try:
            horizon = int(value)
        except Exception:
            continue
        if horizon > 0 and horizon not in configured:
            configured.append(horizon)
    legacy_primary = max(int(getattr(config, "horizon_bars", 12)), 1)
    if not configured:
        configured = [legacy_primary]
    primary = getattr(config, "primary_horizon_bars", None)
    try:
        primary_horizon = int(primary) if primary is not None else legacy_primary
    except Exception:
        primary_horizon = legacy_primary
    if primary_horizon <= 0:
        primary_horizon = legacy_primary
    if primary_horizon not in configured:
        configured.append(primary_horizon)
    return tuple(configured), int(primary_horizon)


def build_label_column_contract_v2_crypto_cs(config: LabelV2CryptoCsConfig) -> dict[str, Any]:
    horizons, primary_horizon = resolve_label_horizons_v2_crypto_cs(config)
    reg_columns = tuple(f"y_reg_net_h{horizon}" for horizon in horizons)
    rank_columns = tuple(f"y_rank_cs_h{horizon}" for horizon in horizons)
    return {
        "horizons_bars": horizons,
        "primary_horizon_bars": primary_horizon,
        "reg_columns": reg_columns,
        "rank_columns": rank_columns,
        "primary": {
            "y_reg": f"y_reg_net_h{primary_horizon}",
            "y_rank": f"y_rank_cs_h{primary_horizon}",
        },
        "legacy": {
            "y_reg": LEGACY_Y_REG_COLUMN_V2,
            "y_rank": LEGACY_Y_RANK_COLUMN_V2,
            "y_cls": LEGACY_Y_CLS_COLUMN_V2,
        },
        "label_columns": (
            LEGACY_Y_REG_COLUMN_V2,
            LEGACY_Y_RANK_COLUMN_V2,
            LEGACY_Y_CLS_COLUMN_V2,
            *reg_columns,
            *rank_columns,
        ),
    }


def apply_labeling_v2_crypto_cs(
    frame: pl.DataFrame,
    *,
    config: LabelV2CryptoCsConfig,
    ts_col: str = "ts_ms",
    market_col: str = "market",
    close_col: str = "close",
) -> pl.DataFrame:
    contract = build_label_column_contract_v2_crypto_cs(config)
    if frame.height <= 0:
        empty_exprs: list[pl.Expr] = [
            pl.lit(None, dtype=pl.Float64).alias(contract["legacy"]["y_reg"]),
            pl.lit(None, dtype=pl.Float64).alias(contract["legacy"]["y_rank"]),
            pl.lit(None, dtype=pl.Int8).alias(contract["legacy"]["y_cls"]),
        ]
        empty_exprs.extend(pl.lit(None, dtype=pl.Float64).alias(name) for name in contract["reg_columns"])
        empty_exprs.extend(pl.lit(None, dtype=pl.Float64).alias(name) for name in contract["rank_columns"])
        return frame.with_columns(empty_exprs)

    _validate_config(config)
    horizons = contract["horizons_bars"]
    primary_reg_column = contract["primary"]["y_reg"]
    primary_rank_column = contract["primary"]["y_rank"]
    total_cost_fraction = max(float(config.fee_bps_est) + float(config.safety_bps), 0.0) / 10_000.0
    neutral_policy = str(config.neutral_policy).strip().lower()
    top_threshold = 1.0 - float(config.top_quantile)
    bottom_threshold = float(config.bottom_quantile)

    reg_exprs = [
        (
            pl.col(close_col)
            .shift(-int(horizon))
            .over(market_col)
            .truediv(pl.col(close_col))
            .log()
            .sub(pl.lit(total_cost_fraction, dtype=pl.Float64))
        ).alias(f"y_reg_net_h{int(horizon)}")
        for horizon in horizons
    ]
    working = frame.sort([market_col, ts_col]).with_columns(reg_exprs)

    rank_state_exprs: list[pl.Expr] = []
    for horizon in horizons:
        rank_state_exprs.append(pl.len().over(ts_col).cast(pl.Int64).alias(f"__cs_count_h{int(horizon)}"))
        rank_state_exprs.append(
            pl.col(f"y_reg_net_h{int(horizon)}")
            .rank(method="average")
            .over(ts_col)
            .cast(pl.Float64)
            .alias(f"__cs_rank_asc_h{int(horizon)}")
        )
    working = working.with_columns(rank_state_exprs)

    rank_pct_exprs: list[pl.Expr] = []
    for horizon in horizons:
        suffix = f"h{int(horizon)}"
        rank_pct_exprs.append(
            pl.when(pl.col(f"y_reg_net_{suffix}").is_null())
            .then(pl.lit(None, dtype=pl.Float64))
            .when(pl.col(f"__cs_count_{suffix}") <= 1)
            .then(pl.lit(1.0, dtype=pl.Float64))
            .otherwise(
                (pl.col(f"__cs_rank_asc_{suffix}") - pl.lit(1.0, dtype=pl.Float64))
                .truediv(pl.col(f"__cs_count_{suffix}").cast(pl.Float64) - pl.lit(1.0, dtype=pl.Float64))
            )
            .alias(f"y_rank_cs_{suffix}")
        )
    working = working.with_columns(rank_pct_exprs)
    working = working.with_columns(
        [
            pl.col(primary_reg_column).cast(pl.Float64).alias(contract["legacy"]["y_reg"]),
            pl.col(primary_rank_column).cast(pl.Float64).alias(contract["legacy"]["y_rank"]),
        ]
    )

    if neutral_policy == "keep_as_class":
        cls_expr = (
            pl.when(pl.col(contract["legacy"]["y_rank"]).is_null())
            .then(pl.lit(None, dtype=pl.Int8))
            .when(pl.col(contract["legacy"]["y_rank"]) >= pl.lit(top_threshold, dtype=pl.Float64))
            .then(pl.lit(1, dtype=pl.Int8))
            .when(pl.col(contract["legacy"]["y_rank"]) <= pl.lit(bottom_threshold, dtype=pl.Float64))
            .then(pl.lit(0, dtype=pl.Int8))
            .otherwise(pl.lit(2, dtype=pl.Int8))
            .alias(contract["legacy"]["y_cls"])
        )
    else:
        cls_expr = (
            pl.when(pl.col(contract["legacy"]["y_rank"]).is_null())
            .then(pl.lit(None, dtype=pl.Int8))
            .when(pl.col(contract["legacy"]["y_rank"]) >= pl.lit(top_threshold, dtype=pl.Float64))
            .then(pl.lit(1, dtype=pl.Int8))
            .when(pl.col(contract["legacy"]["y_rank"]) <= pl.lit(bottom_threshold, dtype=pl.Float64))
            .then(pl.lit(0, dtype=pl.Int8))
            .otherwise(pl.lit(None, dtype=pl.Int8))
            .alias(contract["legacy"]["y_cls"])
        )

    temp_columns = [f"__cs_count_h{int(horizon)}" for horizon in horizons]
    temp_columns.extend(f"__cs_rank_asc_h{int(horizon)}" for horizon in horizons)
    return working.with_columns(cls_expr).drop(temp_columns)


def drop_neutral_rows_v2_crypto_cs(frame: pl.DataFrame, *, config: LabelV2CryptoCsConfig) -> pl.DataFrame:
    if str(config.neutral_policy).strip().lower() == "drop":
        return frame.filter(pl.col(LEGACY_Y_CLS_COLUMN_V2).is_not_null() & (pl.col(LEGACY_Y_CLS_COLUMN_V2) != 2))
    return frame


def label_distribution_v2_crypto_cs(frame: pl.DataFrame, *, config: LabelV2CryptoCsConfig) -> dict[str, Any]:
    if frame.height <= 0:
        return {"pos": 0, "neg": 0, "neutral": 0, "total": 0}

    pos = int(frame.filter(pl.col(LEGACY_Y_CLS_COLUMN_V2) == 1).height)
    neg = int(frame.filter(pl.col(LEGACY_Y_CLS_COLUMN_V2) == 0).height)
    if str(config.neutral_policy).strip().lower() == "keep_as_class":
        neutral = int(frame.filter(pl.col(LEGACY_Y_CLS_COLUMN_V2) == 2).height)
    else:
        neutral = int(
            frame.filter(pl.col(LEGACY_Y_CLS_COLUMN_V2).is_null() & pl.col(LEGACY_Y_RANK_COLUMN_V2).is_not_null()).height
        )
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
    horizons, primary_horizon = resolve_label_horizons_v2_crypto_cs(config)
    if not horizons:
        raise ValueError("horizons_bars must contain at least one positive horizon")
    if primary_horizon not in horizons:
        raise ValueError("primary_horizon_bars must resolve to one of the configured horizons")
