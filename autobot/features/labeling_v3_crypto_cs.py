"""Residualized multi-horizon crypto label bundle for the stronger panel lane."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import polars as pl


VALID_NEUTRAL_POLICIES_V3 = {"drop", "keep_as_class"}


@dataclass(frozen=True)
class LabelV3CryptoCsConfig:
    horizons_bars: tuple[int, ...] = (3, 6, 12, 24)
    primary_horizon_bars: int = 12
    fee_bps_est: float = 10.0
    safety_bps: float = 5.0
    top_quantile: float = 0.2
    bottom_quantile: float = 0.2
    neutral_policy: str = "drop"


def resolve_label_horizons_v3_crypto_cs(config: LabelV3CryptoCsConfig) -> tuple[tuple[int, ...], int]:
    horizons: list[int] = []
    for value in getattr(config, "horizons_bars", ()) or ():
        try:
            horizon = int(value)
        except Exception:
            continue
        if horizon > 0 and horizon not in horizons:
            horizons.append(horizon)
    if not horizons:
        horizons = [12]
    primary_horizon = max(int(getattr(config, "primary_horizon_bars", 12)), 1)
    if primary_horizon not in horizons:
        horizons.append(primary_horizon)
    return tuple(horizons), primary_horizon


def build_label_column_contract_v3_crypto_cs(config: LabelV3CryptoCsConfig) -> dict[str, Any]:
    horizons, primary_horizon = resolve_label_horizons_v3_crypto_cs(config)
    raw_reg_columns = tuple(f"y_reg_net_h{horizon}" for horizon in horizons)
    resid_btc_columns = tuple(f"y_reg_resid_btc_h{horizon}" for horizon in horizons)
    resid_eth_columns = tuple(f"y_reg_resid_eth_h{horizon}" for horizon in horizons)
    resid_leader_columns = tuple(f"y_reg_resid_leader_h{horizon}" for horizon in horizons)
    rank_columns = tuple(f"y_rank_resid_leader_h{horizon}" for horizon in horizons)
    cls_columns = tuple(f"y_cls_resid_leader_topq_h{horizon}" for horizon in horizons)
    return {
        "horizons_bars": horizons,
        "primary_horizon_bars": primary_horizon,
        "raw_reg_columns": raw_reg_columns,
        "residual_reg_columns": {
            "btc": resid_btc_columns,
            "eth": resid_eth_columns,
            "leader": resid_leader_columns,
        },
        "rank_columns": rank_columns,
        "cls_columns": cls_columns,
        "training_default_columns": {
            "y_reg": f"y_reg_resid_leader_h{primary_horizon}",
            "y_rank": f"y_rank_resid_leader_h{primary_horizon}",
            "y_cls": f"y_cls_resid_leader_topq_h{primary_horizon}",
        },
        "label_columns": (
            *raw_reg_columns,
            *resid_btc_columns,
            *resid_eth_columns,
            *resid_leader_columns,
            *rank_columns,
            *cls_columns,
        ),
    }


def apply_labeling_v3_crypto_cs(
    frame: pl.DataFrame,
    *,
    config: LabelV3CryptoCsConfig,
    ts_col: str = "ts_ms",
    market_col: str = "market",
    close_col: str = "close",
) -> pl.DataFrame:
    contract = build_label_column_contract_v3_crypto_cs(config)
    if frame.height <= 0:
        empty_exprs: list[pl.Expr] = []
        for name in contract["label_columns"]:
            dtype = pl.Int8 if name.startswith("y_cls") else pl.Float64
            empty_exprs.append(pl.lit(None, dtype=dtype).alias(name))
        return frame.with_columns(empty_exprs)

    _validate_config(config)
    horizons = contract["horizons_bars"]
    total_cost_fraction = max(float(config.fee_bps_est) + float(config.safety_bps), 0.0) / 10_000.0
    neutral_policy = str(config.neutral_policy).strip().lower()
    top_threshold = 1.0 - float(config.top_quantile)
    bottom_threshold = float(config.bottom_quantile)
    btc_market, eth_market = _infer_leader_markets(frame=frame, market_col=market_col)

    gross_exprs = [
        (
            pl.col(close_col)
            .shift(-int(horizon))
            .over(market_col)
            .truediv(pl.col(close_col))
            .log()
        ).alias(f"__gross_logret_h{int(horizon)}")
        for horizon in horizons
    ]
    raw_net_exprs = [
        (
            pl.col(f"__gross_logret_h{int(horizon)}")
            .sub(pl.lit(total_cost_fraction, dtype=pl.Float64))
        ).alias(f"y_reg_net_h{int(horizon)}")
        for horizon in horizons
    ]
    working = frame.sort([market_col, ts_col]).with_columns(gross_exprs).with_columns(raw_net_exprs)

    market_reference_exprs: list[pl.Expr] = []
    for horizon in horizons:
        market_reference_exprs.extend(
            [
                pl.col(f"__gross_logret_h{int(horizon)}").mean().alias(f"__market_mean_h{int(horizon)}"),
                pl.col(f"__gross_logret_h{int(horizon)}")
                .filter(pl.col(market_col) == pl.lit(btc_market))
                .first()
                .alias(f"__btc_ret_h{int(horizon)}"),
                pl.col(f"__gross_logret_h{int(horizon)}")
                .filter(pl.col(market_col) == pl.lit(eth_market))
                .first()
                .alias(f"__eth_ret_h{int(horizon)}"),
            ]
        )
    references = working.group_by(ts_col).agg(market_reference_exprs)
    working = working.join(references, on=ts_col, how="left")

    residual_exprs: list[pl.Expr] = []
    fill_exprs: list[pl.Expr] = []
    leader_fill_exprs: list[pl.Expr] = []
    for horizon in horizons:
        market_mean_column = f"__market_mean_h{int(horizon)}"
        btc_column = f"__btc_ret_h{int(horizon)}"
        eth_column = f"__eth_ret_h{int(horizon)}"
        btc_filled = f"__btc_ret_filled_h{int(horizon)}"
        eth_filled = f"__eth_ret_filled_h{int(horizon)}"
        leader_filled = f"__leader_ret_filled_h{int(horizon)}"
        fill_exprs.extend(
            [
                pl.coalesce([pl.col(btc_column), pl.col(market_mean_column)]).alias(btc_filled),
                pl.coalesce([pl.col(eth_column), pl.col(market_mean_column)]).alias(eth_filled),
            ]
        )
        raw_reg_column = f"y_reg_net_h{int(horizon)}"
        residual_exprs.extend(
            [
                (
                    pl.col(raw_reg_column)
                    .sub(pl.col(btc_filled))
                ).alias(f"y_reg_resid_btc_h{int(horizon)}"),
                (
                    pl.col(raw_reg_column)
                    .sub(pl.col(eth_filled))
                ).alias(f"y_reg_resid_eth_h{int(horizon)}"),
            ]
        )
        leader_fill_exprs.append(
            pl.when(pl.col(btc_filled).is_not_null() & pl.col(eth_filled).is_not_null())
            .then((pl.col(btc_filled) + pl.col(eth_filled)) / 2.0)
            .when(pl.col(btc_filled).is_not_null())
            .then(pl.col(btc_filled))
            .when(pl.col(eth_filled).is_not_null())
            .then(pl.col(eth_filled))
            .otherwise(pl.col(market_mean_column))
            .alias(leader_filled)
        )
    working = working.with_columns(fill_exprs)
    working = working.with_columns(leader_fill_exprs)
    working = working.with_columns(residual_exprs)

    leader_residual_exprs = [
        (
            pl.col(f"y_reg_net_h{int(horizon)}")
            .sub(pl.col(f"__leader_ret_filled_h{int(horizon)}"))
        ).alias(f"y_reg_resid_leader_h{int(horizon)}")
        for horizon in horizons
    ]
    working = working.with_columns(leader_residual_exprs)

    rank_state_exprs: list[pl.Expr] = []
    for horizon in horizons:
        resid_column = f"y_reg_resid_leader_h{int(horizon)}"
        rank_state_exprs.extend(
            [
                pl.len().over(ts_col).cast(pl.Int64).alias(f"__cs_count_h{int(horizon)}"),
                pl.col(resid_column)
                .rank(method="average")
                .over(ts_col)
                .cast(pl.Float64)
                .alias(f"__cs_rank_asc_h{int(horizon)}"),
            ]
        )
    working = working.with_columns(rank_state_exprs)

    rank_exprs: list[pl.Expr] = []
    cls_exprs: list[pl.Expr] = []
    for horizon in horizons:
        rank_column = f"y_rank_resid_leader_h{int(horizon)}"
        rank_exprs.append(
            pl.when(pl.col(f"y_reg_resid_leader_h{int(horizon)}").is_null())
            .then(pl.lit(None, dtype=pl.Float64))
            .when(pl.col(f"__cs_count_h{int(horizon)}") <= 1)
            .then(pl.lit(1.0, dtype=pl.Float64))
            .otherwise(
                (pl.col(f"__cs_rank_asc_h{int(horizon)}") - pl.lit(1.0, dtype=pl.Float64))
                .truediv(pl.col(f"__cs_count_h{int(horizon)}").cast(pl.Float64) - pl.lit(1.0, dtype=pl.Float64))
            )
            .alias(rank_column)
        )
    working = working.with_columns(rank_exprs)

    for horizon in horizons:
        rank_column = f"y_rank_resid_leader_h{int(horizon)}"
        cls_name = f"y_cls_resid_leader_topq_h{int(horizon)}"
        if neutral_policy == "keep_as_class":
            cls_expr = (
                pl.when(pl.col(rank_column).is_null())
                .then(pl.lit(None, dtype=pl.Int8))
                .when(pl.col(rank_column) >= pl.lit(top_threshold, dtype=pl.Float64))
                .then(pl.lit(1, dtype=pl.Int8))
                .when(pl.col(rank_column) <= pl.lit(bottom_threshold, dtype=pl.Float64))
                .then(pl.lit(0, dtype=pl.Int8))
                .otherwise(pl.lit(2, dtype=pl.Int8))
                .alias(cls_name)
            )
        else:
            cls_expr = (
                pl.when(pl.col(rank_column).is_null())
                .then(pl.lit(None, dtype=pl.Int8))
                .when(pl.col(rank_column) >= pl.lit(top_threshold, dtype=pl.Float64))
                .then(pl.lit(1, dtype=pl.Int8))
                .when(pl.col(rank_column) <= pl.lit(bottom_threshold, dtype=pl.Float64))
                .then(pl.lit(0, dtype=pl.Int8))
                .otherwise(pl.lit(None, dtype=pl.Int8))
                .alias(cls_name)
            )
        cls_exprs.append(cls_expr)
    working = working.with_columns(cls_exprs)

    temp_columns = [f"__gross_logret_h{int(horizon)}" for horizon in horizons]
    temp_columns.extend(f"__market_mean_h{int(horizon)}" for horizon in horizons)
    temp_columns.extend(f"__btc_ret_h{int(horizon)}" for horizon in horizons)
    temp_columns.extend(f"__eth_ret_h{int(horizon)}" for horizon in horizons)
    temp_columns.extend(f"__btc_ret_filled_h{int(horizon)}" for horizon in horizons)
    temp_columns.extend(f"__eth_ret_filled_h{int(horizon)}" for horizon in horizons)
    temp_columns.extend(f"__leader_ret_filled_h{int(horizon)}" for horizon in horizons)
    temp_columns.extend(f"__cs_count_h{int(horizon)}" for horizon in horizons)
    temp_columns.extend(f"__cs_rank_asc_h{int(horizon)}" for horizon in horizons)
    return working.drop(temp_columns)


def drop_neutral_rows_v3_crypto_cs(frame: pl.DataFrame, *, config: LabelV3CryptoCsConfig) -> pl.DataFrame:
    contract = build_label_column_contract_v3_crypto_cs(config)
    y_cls_column = contract["training_default_columns"]["y_cls"]
    if str(config.neutral_policy).strip().lower() == "drop":
        return frame.filter(pl.col(y_cls_column).is_not_null() & (pl.col(y_cls_column) != 2))
    return frame


def label_distribution_v3_crypto_cs(frame: pl.DataFrame, *, config: LabelV3CryptoCsConfig) -> dict[str, Any]:
    if frame.height <= 0:
        return {"pos": 0, "neg": 0, "neutral": 0, "total": 0}
    contract = build_label_column_contract_v3_crypto_cs(config)
    y_cls_column = contract["training_default_columns"]["y_cls"]
    pos = int(frame.filter(pl.col(y_cls_column) == 1).height)
    neg = int(frame.filter(pl.col(y_cls_column) == 0).height)
    if str(config.neutral_policy).strip().lower() == "keep_as_class":
        neutral = int(frame.filter(pl.col(y_cls_column) == 2).height)
    else:
        neutral = int(frame.filter(pl.col(y_cls_column).is_null()).height)
    return {"pos": pos, "neg": neg, "neutral": neutral, "total": int(frame.height)}


def _infer_leader_markets(*, frame: pl.DataFrame, market_col: str) -> tuple[str, str]:
    markets = [str(value).strip().upper() for value in frame.get_column(market_col).drop_nulls().unique().to_list()]
    markets = [value for value in markets if value]
    quote_prefix = "KRW"
    for market in markets:
        if "-" in market:
            quote_prefix = market.split("-", 1)[0].strip().upper() or "KRW"
            break
    return f"{quote_prefix}-BTC", f"{quote_prefix}-ETH"


def _validate_config(config: LabelV3CryptoCsConfig) -> None:
    neutral_policy = str(config.neutral_policy).strip().lower()
    if neutral_policy not in VALID_NEUTRAL_POLICIES_V3:
        raise ValueError(f"neutral_policy must be one of: {', '.join(sorted(VALID_NEUTRAL_POLICIES_V3))}")
    top_quantile = float(config.top_quantile)
    bottom_quantile = float(config.bottom_quantile)
    if not 0.0 < top_quantile < 0.5:
        raise ValueError("top_quantile must be between 0 and 0.5")
    if not 0.0 < bottom_quantile < 0.5:
        raise ValueError("bottom_quantile must be between 0 and 0.5")
    horizons, primary_horizon = resolve_label_horizons_v3_crypto_cs(config)
    if not horizons:
        raise ValueError("horizons_bars must contain at least one positive horizon")
    if primary_horizon not in horizons:
        raise ValueError("primary_horizon_bars must resolve to one of the configured horizons")
