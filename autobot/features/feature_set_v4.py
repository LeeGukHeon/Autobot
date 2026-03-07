"""FeatureSet v4: v3 core + cross-sectional spillover/breadth pack."""

from __future__ import annotations

import polars as pl

from .feature_blocks_v3 import feature_columns_v3_contract, required_feature_columns_v3


def spillover_breadth_feature_columns_v4() -> tuple[str, ...]:
    return (
        "btc_ret_1",
        "btc_ret_3",
        "btc_ret_12",
        "eth_ret_1",
        "eth_ret_3",
        "eth_ret_12",
        "leader_basket_ret_1",
        "leader_basket_ret_3",
        "leader_basket_ret_12",
        "market_breadth_pos_1",
        "market_breadth_pos_12",
        "market_dispersion_12",
        "turnover_concentration_hhi",
        "rel_strength_vs_btc_12",
    )


def feature_columns_v4(*, high_tfs: tuple[str, ...] = ("15m", "60m", "240m")) -> tuple[str, ...]:
    return tuple(list(feature_columns_v3_contract(high_tfs=high_tfs)) + list(spillover_breadth_feature_columns_v4()))


def required_feature_columns_v4(*, high_tfs: tuple[str, ...] = ("15m", "60m", "240m")) -> tuple[str, ...]:
    return tuple(list(required_feature_columns_v3(high_tfs=high_tfs)) + list(spillover_breadth_feature_columns_v4()))


def attach_spillover_breadth_features_v4(
    frame: pl.DataFrame,
    *,
    quote: str = "KRW",
    float_dtype: str = "float32",
) -> pl.DataFrame:
    if frame.height <= 0:
        return frame
    if "market" not in frame.columns or "ts_ms" not in frame.columns:
        raise ValueError("v4 spillover/breadth features require market and ts_ms columns")

    leader_quote = str(quote).strip().upper() or "KRW"
    btc_market = f"{leader_quote}-BTC"
    eth_market = f"{leader_quote}-ETH"
    dtype = pl.Float64 if str(float_dtype).strip().lower() == "float64" else pl.Float32

    working = frame.sort(["ts_ms", "market"]).with_columns(
        (pl.col("close").cast(pl.Float64) * pl.col("volume_base").cast(pl.Float64))
        .clip(lower_bound=0.0)
        .alias("__quote_turnover")
    )

    market_stats = working.group_by("ts_ms").agg(
        [
            (pl.col("logret_1") > 0.0).cast(pl.Float64).mean().alias("market_breadth_pos_1"),
            (pl.col("logret_12") > 0.0).cast(pl.Float64).mean().alias("market_breadth_pos_12"),
            pl.col("logret_12").std().fill_null(0.0).alias("market_dispersion_12"),
            pl.col("logret_1").mean().alias("__market_mean_ret_1"),
            pl.col("logret_3").mean().alias("__market_mean_ret_3"),
            pl.col("logret_12").mean().alias("__market_mean_ret_12"),
        ]
    )
    turnover_stats = (
        working.with_columns(pl.col("__quote_turnover").sum().over("ts_ms").alias("__turnover_total"))
        .with_columns(
            pl.when(pl.col("__turnover_total") > 0.0)
            .then((pl.col("__quote_turnover") / pl.col("__turnover_total")).pow(2))
            .otherwise(0.0)
            .alias("__turnover_hhi_term")
        )
        .group_by("ts_ms")
        .agg(pl.col("__turnover_hhi_term").sum().alias("turnover_concentration_hhi"))
    )
    btc_frame = _leader_frame(working, market=btc_market, prefix="btc")
    eth_frame = _leader_frame(working, market=eth_market, prefix="eth")

    enriched = (
        working.join(market_stats, on="ts_ms", how="left")
        .join(turnover_stats, on="ts_ms", how="left")
        .join(btc_frame, on="ts_ms", how="left")
        .join(eth_frame, on="ts_ms", how="left")
        .with_columns(
            [
                pl.col("btc_ret_1").fill_null(pl.col("__market_mean_ret_1")).alias("btc_ret_1"),
                pl.col("btc_ret_3").fill_null(pl.col("__market_mean_ret_3")).alias("btc_ret_3"),
                pl.col("btc_ret_12").fill_null(pl.col("__market_mean_ret_12")).alias("btc_ret_12"),
                pl.col("eth_ret_1").fill_null(pl.col("__market_mean_ret_1")).alias("eth_ret_1"),
                pl.col("eth_ret_3").fill_null(pl.col("__market_mean_ret_3")).alias("eth_ret_3"),
                pl.col("eth_ret_12").fill_null(pl.col("__market_mean_ret_12")).alias("eth_ret_12"),
            ]
        )
        .with_columns(
            [
                _leader_mean_expr("btc_ret_1", "eth_ret_1", "__market_mean_ret_1").alias("leader_basket_ret_1"),
                _leader_mean_expr("btc_ret_3", "eth_ret_3", "__market_mean_ret_3").alias("leader_basket_ret_3"),
                _leader_mean_expr("btc_ret_12", "eth_ret_12", "__market_mean_ret_12").alias("leader_basket_ret_12"),
                (pl.col("logret_12") - pl.col("btc_ret_12")).alias("rel_strength_vs_btc_12"),
            ]
        )
    )

    exprs: list[pl.Expr] = []
    final_cols = set(spillover_breadth_feature_columns_v4())
    for name in enriched.columns:
        if name in final_cols:
            exprs.append(pl.col(name).cast(dtype).alias(name))
        else:
            exprs.append(pl.col(name).alias(name))
    return enriched.with_columns(exprs).drop(
        [
            "__quote_turnover",
            "__market_mean_ret_1",
            "__market_mean_ret_3",
            "__market_mean_ret_12",
        ]
    )


def _leader_frame(frame: pl.DataFrame, *, market: str, prefix: str) -> pl.DataFrame:
    leader = frame.filter(pl.col("market") == market)
    if leader.height <= 0:
        return pl.DataFrame(
            schema={
                "ts_ms": pl.Int64,
                f"{prefix}_ret_1": pl.Float64,
                f"{prefix}_ret_3": pl.Float64,
                f"{prefix}_ret_12": pl.Float64,
            }
        )
    return leader.select(
        [
            "ts_ms",
            pl.col("logret_1").cast(pl.Float64).alias(f"{prefix}_ret_1"),
            pl.col("logret_3").cast(pl.Float64).alias(f"{prefix}_ret_3"),
            pl.col("logret_12").cast(pl.Float64).alias(f"{prefix}_ret_12"),
        ]
    )


def _leader_mean_expr(a_col: str, b_col: str, fallback_col: str) -> pl.Expr:
    return (
        pl.when(pl.col(a_col).is_not_null() & pl.col(b_col).is_not_null())
        .then((pl.col(a_col) + pl.col(b_col)) / 2.0)
        .when(pl.col(a_col).is_not_null())
        .then(pl.col(a_col))
        .when(pl.col(b_col).is_not_null())
        .then(pl.col(b_col))
        .otherwise(pl.col(fallback_col))
    )
