"""Compact, versioned order-flow predictor panel from existing micro_v1 inputs."""

from __future__ import annotations

from typing import Any

import polars as pl


def order_flow_panel_v1_feature_columns() -> tuple[str, ...]:
    return (
        "oflow_v1_signed_volume_imbalance_1",
        "oflow_v1_signed_count_imbalance_1",
        "oflow_v1_signed_volume_imbalance_3",
        "oflow_v1_signed_volume_imbalance_12",
        "oflow_v1_flow_sign_persistence_12",
        "oflow_v1_depth_conditioned_flow_1",
        "oflow_v1_trade_book_imbalance_gap_1",
        "oflow_v1_spread_conditioned_flow_1",
        "oflow_v1_microprice_conditioned_flow_1",
    )


def order_flow_panel_v1_contract() -> dict[str, Any]:
    return {
        "version": "order_flow_panel_v1",
        "status": "single_venue_compact_panel",
        "source_paper": {
            "title": "Order Flow and Cryptocurrency Returns",
            "publisher": "Journal of Financial Markets",
            "url": "https://www.sciencedirect.com/science/article/pii/S1386418126000029",
        },
        "deployment_scope": {
            "venue_id": "upbit",
            "aggregation_scope": "single_venue_local",
            "base_tf": "5m",
            "note": (
                "This contract uses already aggregated micro_v1 rows from a single venue. "
                "Schema keeps venue_id and aggregation_scope explicit so later multi-venue expansion "
                "does not change field semantics."
            ),
        },
        "scaling_choices": {
            "spread_floor_bps": 1.0,
            "microprice_scale_bps": 25.0,
            "clip_rule": "signed imbalance and conditioned signals clipped to [-1, 1] unless otherwise stated",
            "missing_data_handling": {
                "offline_training": "rows without mandatory micro are dropped before the panel is attached",
                "live_runtime": "missing micro snapshot fields are zero-filled upstream and the panel resolves to neutral zeros",
            },
            "horizons_bars": [1, 3, 12],
        },
        "field_definitions": [
            {
                "feature_name": "oflow_v1_signed_volume_imbalance_1",
                "formula": "clip((m_buy_volume - m_sell_volume) / max(m_trade_volume_base, eps), -1, 1)",
                "family": "signed_flow",
                "denominator": "m_trade_volume_base",
            },
            {
                "feature_name": "oflow_v1_signed_count_imbalance_1",
                "formula": "clip((m_buy_count - m_sell_count) / max(m_trade_count, eps), -1, 1)",
                "family": "signed_flow",
                "denominator": "m_trade_count",
            },
            {
                "feature_name": "oflow_v1_signed_volume_imbalance_3",
                "formula": "rolling_mean(oflow_v1_signed_volume_imbalance_1, 3 bars, by market)",
                "family": "persistence",
                "horizon_bars": 3,
            },
            {
                "feature_name": "oflow_v1_signed_volume_imbalance_12",
                "formula": "rolling_mean(oflow_v1_signed_volume_imbalance_1, 12 bars, by market)",
                "family": "persistence",
                "horizon_bars": 12,
            },
            {
                "feature_name": "oflow_v1_flow_sign_persistence_12",
                "formula": "rolling_mean(sign(oflow_v1_signed_volume_imbalance_1), 12 bars, by market)",
                "family": "persistence",
                "horizon_bars": 12,
            },
            {
                "feature_name": "oflow_v1_depth_conditioned_flow_1",
                "formula": (
                    "oflow_v1_signed_volume_imbalance_1 * "
                    "clip((m_depth_bid_top5_mean - m_depth_ask_top5_mean) / max(m_depth_bid_top5_mean + m_depth_ask_top5_mean, eps), -1, 1)"
                ),
                "family": "state_conditioned",
                "conditioning": "top5 depth imbalance",
            },
            {
                "feature_name": "oflow_v1_trade_book_imbalance_gap_1",
                "formula": (
                    "oflow_v1_signed_volume_imbalance_1 - "
                    "clip((m_depth_bid_top5_mean - m_depth_ask_top5_mean) / max(m_depth_bid_top5_mean + m_depth_ask_top5_mean, eps), -1, 1)"
                ),
                "family": "state_conditioned",
                "conditioning": "trade vs book imbalance gap",
            },
            {
                "feature_name": "oflow_v1_spread_conditioned_flow_1",
                "formula": "oflow_v1_signed_volume_imbalance_1 / max(abs(m_spread_proxy), spread_floor_bps)",
                "family": "state_conditioned",
                "denominator": "spread_bps_mean",
            },
            {
                "feature_name": "oflow_v1_microprice_conditioned_flow_1",
                "formula": "oflow_v1_signed_volume_imbalance_1 * clip(m_microprice_bias_bps_mean / microprice_scale_bps, -1, 1)",
                "family": "state_conditioned",
                "conditioning": "microprice bias",
            },
        ],
    }


def attach_order_flow_panel_v1(frame: pl.DataFrame, *, float_dtype: str = "float32") -> pl.DataFrame:
    if frame.height <= 0:
        return frame
    if "market" not in frame.columns or "ts_ms" not in frame.columns:
        raise ValueError("order_flow_panel_v1 requires market and ts_ms columns")

    dtype = pl.Float64 if str(float_dtype).strip().lower() == "float64" else pl.Float32
    columns = frame.columns
    buy_volume = _col_or_zero(columns, "m_buy_volume").clip(lower_bound=0.0)
    sell_volume = _col_or_zero(columns, "m_sell_volume").clip(lower_bound=0.0)
    trade_volume = _col_or_zero(columns, "m_trade_volume_base").clip(lower_bound=0.0)
    buy_count = _col_or_zero(columns, "m_buy_count").clip(lower_bound=0.0)
    sell_count = _col_or_zero(columns, "m_sell_count").clip(lower_bound=0.0)
    trade_count = _col_or_zero(columns, "m_trade_count").clip(lower_bound=0.0)
    depth_bid = _col_or_zero(columns, "m_depth_bid_top5_mean").clip(lower_bound=0.0)
    depth_ask = _col_or_zero(columns, "m_depth_ask_top5_mean").clip(lower_bound=0.0)
    spread_bps = _col_or_zero(columns, "m_spread_proxy").abs()
    microprice_bias_bps = _col_or_zero(columns, "m_microprice_bias_bps_mean")

    working = frame.sort(["market", "ts_ms"]).with_columns(
        [
            _safe_ratio_expr(buy_volume - sell_volume, trade_volume).clip(lower_bound=-1.0, upper_bound=1.0).alias(
                "oflow_v1_signed_volume_imbalance_1"
            ),
            _safe_ratio_expr(buy_count - sell_count, trade_count).clip(lower_bound=-1.0, upper_bound=1.0).alias(
                "oflow_v1_signed_count_imbalance_1"
            ),
            _safe_ratio_expr(depth_bid - depth_ask, depth_bid + depth_ask).clip(lower_bound=-1.0, upper_bound=1.0).alias(
                "__oflow_depth_imbalance_1"
            ),
            _safe_ratio_expr(microprice_bias_bps, pl.lit(25.0, dtype=pl.Float64))
            .clip(lower_bound=-1.0, upper_bound=1.0)
            .alias("__oflow_microprice_scaled_1"),
        ]
    )
    working = working.with_columns(
        [
            pl.col("oflow_v1_signed_volume_imbalance_1")
            .rolling_mean(window_size=3, min_samples=1)
            .over("market")
            .alias("oflow_v1_signed_volume_imbalance_3"),
            pl.col("oflow_v1_signed_volume_imbalance_1")
            .rolling_mean(window_size=12, min_samples=1)
            .over("market")
            .alias("oflow_v1_signed_volume_imbalance_12"),
            _sign_expr(pl.col("oflow_v1_signed_volume_imbalance_1"))
            .rolling_mean(window_size=12, min_samples=1)
            .over("market")
            .alias("oflow_v1_flow_sign_persistence_12"),
            (pl.col("oflow_v1_signed_volume_imbalance_1") * pl.col("__oflow_depth_imbalance_1")).alias(
                "oflow_v1_depth_conditioned_flow_1"
            ),
            (pl.col("oflow_v1_signed_volume_imbalance_1") - pl.col("__oflow_depth_imbalance_1")).alias(
                "oflow_v1_trade_book_imbalance_gap_1"
            ),
            _safe_ratio_expr(
                pl.col("oflow_v1_signed_volume_imbalance_1"),
                pl.when(spread_bps > 1.0).then(spread_bps).otherwise(1.0),
            ).alias("oflow_v1_spread_conditioned_flow_1"),
            (pl.col("oflow_v1_signed_volume_imbalance_1") * pl.col("__oflow_microprice_scaled_1")).alias(
                "oflow_v1_microprice_conditioned_flow_1"
            ),
        ]
    )

    exprs: list[pl.Expr] = []
    final_cols = set(order_flow_panel_v1_feature_columns())
    for name in working.columns:
        if name in final_cols:
            exprs.append(pl.col(name).fill_null(0.0).cast(dtype).alias(name))
        else:
            exprs.append(pl.col(name).alias(name))
    return working.with_columns(exprs).drop(["__oflow_depth_imbalance_1", "__oflow_microprice_scaled_1"])


def order_flow_panel_v1_diagnostics(frame: pl.DataFrame) -> dict[str, Any]:
    if frame.height <= 0:
        return {
            "rows": 0,
            "micro_available_ratio": 0.0,
            "trade_flow_available_ratio": 0.0,
            "book_state_available_ratio": 0.0,
            "trade_coverage_ms_p50": None,
            "trade_coverage_ms_p90": None,
            "book_coverage_ms_p50": None,
            "book_coverage_ms_p90": None,
            "horizon_full_availability": {"1": 0.0, "3": 0.0, "12": 0.0},
        }

    working = frame.sort(["market", "ts_ms"])
    trade_available = (
        (_col_or_zero(working.columns, "m_trade_volume_base") > 0.0)
        | (_col_or_zero(working.columns, "m_trade_count") > 0.0)
    )
    book_available = (
        (_col_or_zero(working.columns, "m_depth_bid_top5_mean") + _col_or_zero(working.columns, "m_depth_ask_top5_mean")) > 0.0
    )
    available = (
        pl.col("m_micro_available").cast(pl.Boolean)
        if "m_micro_available" in working.columns
        else (trade_available | book_available)
    )
    diagnostic = working.with_columns(
        [
            available.cast(pl.Int64).alias("__oflow_micro_available_i"),
            trade_available.cast(pl.Int64).alias("__oflow_trade_available_i"),
            book_available.cast(pl.Int64).alias("__oflow_book_available_i"),
            trade_available.cast(pl.Int64).rolling_sum(window_size=1, min_samples=1).over("market").alias("__oflow_h1"),
            trade_available.cast(pl.Int64).rolling_sum(window_size=3, min_samples=1).over("market").alias("__oflow_h3"),
            trade_available.cast(pl.Int64).rolling_sum(window_size=12, min_samples=1).over("market").alias("__oflow_h12"),
        ]
    )
    rows = int(diagnostic.height)
    return {
        "rows": rows,
        "micro_available_ratio": _mean_int_column(diagnostic, "__oflow_micro_available_i"),
        "trade_flow_available_ratio": _mean_int_column(diagnostic, "__oflow_trade_available_i"),
        "book_state_available_ratio": _mean_int_column(diagnostic, "__oflow_book_available_i"),
        "trade_coverage_ms_p50": _quantile_int_column(diagnostic, "m_trade_coverage_ms", 0.50),
        "trade_coverage_ms_p90": _quantile_int_column(diagnostic, "m_trade_coverage_ms", 0.90),
        "book_coverage_ms_p50": _quantile_int_column(diagnostic, "m_book_coverage_ms", 0.50),
        "book_coverage_ms_p90": _quantile_int_column(diagnostic, "m_book_coverage_ms", 0.90),
        "horizon_full_availability": {
            "1": _availability_ratio(diagnostic, "__oflow_h1", 1),
            "3": _availability_ratio(diagnostic, "__oflow_h3", 3),
            "12": _availability_ratio(diagnostic, "__oflow_h12", 12),
        },
    }


def _col_or_zero(columns: list[str], name: str) -> pl.Expr:
    if name in columns:
        return pl.col(name).cast(pl.Float64)
    return pl.lit(0.0, dtype=pl.Float64)


def _safe_ratio_expr(numerator: pl.Expr, denominator: pl.Expr) -> pl.Expr:
    return pl.when(denominator.abs() > 1e-12).then(numerator / denominator).otherwise(0.0)


def _sign_expr(expr: pl.Expr) -> pl.Expr:
    return pl.when(expr > 0.0).then(1.0).when(expr < 0.0).then(-1.0).otherwise(0.0)


def _mean_int_column(frame: pl.DataFrame, column: str) -> float:
    if column not in frame.columns or frame.height <= 0:
        return 0.0
    return float(frame.get_column(column).mean() or 0.0)


def _availability_ratio(frame: pl.DataFrame, column: str, threshold: int) -> float:
    if column not in frame.columns or frame.height <= 0:
        return 0.0
    return float(frame.filter(pl.col(column) >= int(threshold)).height) / float(frame.height)


def _quantile_int_column(frame: pl.DataFrame, column: str, q: float) -> int | None:
    if column not in frame.columns or frame.height <= 0:
        return None
    values = [int(item) for item in frame.get_column(column).drop_nulls().to_list()]
    if not values:
        return None
    values.sort()
    if len(values) == 1:
        return values[0]
    position = min(max(float(q), 0.0), 1.0) * float(len(values) - 1)
    lower = int(position)
    upper = min(lower + 1, len(values) - 1)
    if lower == upper:
        return values[lower]
    weight = position - float(lower)
    return int(round((1.0 - weight) * float(values[lower]) + weight * float(values[upper])))
