"""Native v4 live-base feature blocks shared by training and runtime."""

from __future__ import annotations

import math
from typing import Iterable

import polars as pl

from autobot.common.data_quality_budget import quality_weight_expr_from_synth_ratio
from autobot.data import expected_interval_ms

from .micro_join import prefixed_micro_columns
from .multitf_join_v1 import high_tf_prefix


def base_feature_columns_v4_live_base() -> tuple[str, ...]:
    return (
        "logret_1",
        "logret_3",
        "logret_12",
        "logret_36",
        "vol_12",
        "vol_36",
        "range_pct",
        "body_pct",
        "volume_log",
        "volume_z",
    )


def one_m_feature_columns_v4_live_base() -> tuple[str, ...]:
    return (
        "one_m_count",
        "one_m_ret_mean",
        "one_m_ret_std",
        "one_m_volume_sum",
        "one_m_range_mean",
        "one_m_missing_ratio",
        "one_m_synth_ratio",
        "one_m_real_count",
        "one_m_real_volume_sum",
    )


def high_tf_feature_columns_v4_live_base(*, high_tfs: tuple[str, ...] = ("15m", "60m", "240m")) -> tuple[str, ...]:
    high: list[str] = []
    for tf in high_tfs:
        prefix = high_tf_prefix(tf)
        high.extend(
            [
                f"{prefix}_ret_1",
                f"{prefix}_ret_3",
                f"{prefix}_vol_3",
                f"{prefix}_trend_slope",
                f"{prefix}_regime_flag",
            ]
        )
    return tuple(_dedupe_preserve(high))


def micro_feature_columns_v4_live_base() -> tuple[str, ...]:
    micro = list(prefixed_micro_columns())
    micro.extend(
        [
            "m_spread_proxy",
            "m_trade_volume_base",
            "m_trade_buy_ratio",
            "m_signed_volume",
            "m_source_ws",
            "m_source_rest",
        ]
    )
    return tuple(_dedupe_preserve(micro))


def feature_columns_v4_live_base_contract(*, high_tfs: tuple[str, ...] = ("15m", "60m", "240m")) -> tuple[str, ...]:
    return tuple(
        _dedupe_preserve(
            list(base_feature_columns_v4_live_base())
            + list(one_m_feature_columns_v4_live_base())
            + list(high_tf_feature_columns_v4_live_base(high_tfs=high_tfs))
            + list(micro_feature_columns_v4_live_base())
        )
    )


def required_feature_columns_v4_live_base(*, high_tfs: tuple[str, ...] = ("15m", "60m", "240m")) -> tuple[str, ...]:
    micro = (
        "m_micro_available",
        "m_trade_events",
        "m_book_events",
        "m_trade_coverage_ms",
        "m_book_coverage_ms",
        "m_spread_proxy",
        "m_trade_volume_base",
        "m_source_ws",
        "m_source_rest",
    )
    return tuple(
        _dedupe_preserve(
            list(base_feature_columns_v4_live_base())
            + list(one_m_feature_columns_v4_live_base())
            + list(high_tf_feature_columns_v4_live_base(high_tfs=high_tfs))
            + list(micro)
        )
    )


def compute_base_features_v4_live_base(base_candles_frame: pl.DataFrame, *, tf: str, float_dtype: str) -> pl.DataFrame:
    required = {"ts_ms", "open", "high", "low", "close", "volume_base"}
    missing = [name for name in required if name not in base_candles_frame.columns]
    if missing:
        raise ValueError(f"base candles missing required columns: {missing}")

    frame = base_candles_frame.sort("ts_ms")
    frame = frame.with_columns(
        [
            (pl.col("close").log() - pl.col("close").shift(1).log()).alias("logret_1"),
            (pl.col("close").log() - pl.col("close").shift(3).log()).alias("logret_3"),
            (pl.col("close").log() - pl.col("close").shift(12).log()).alias("logret_12"),
            (pl.col("close").log() - pl.col("close").shift(36).log()).alias("logret_36"),
            pl.col("volume_base").log1p().alias("volume_log"),
            (pl.col("high") / pl.col("low") - 1.0).alias("range_pct"),
            ((pl.col("close") - pl.col("open")) / pl.col("open")).alias("body_pct"),
        ]
    )
    frame = frame.with_columns(
        [
            pl.col("logret_1").rolling_std(window_size=12, min_samples=12).alias("vol_12"),
            pl.col("logret_1").rolling_std(window_size=36, min_samples=36).alias("vol_36"),
        ]
    )
    volume_mean = pl.col("volume_log").rolling_mean(window_size=36, min_samples=36)
    volume_std = pl.col("volume_log").rolling_std(window_size=36, min_samples=36)
    interval_ms = expected_interval_ms(tf)
    frame = frame.with_columns(
        [
            ((pl.col("volume_log") - volume_mean) / volume_std).alias("volume_z"),
            (pl.col("ts_ms").diff() > int(interval_ms)).fill_null(False).alias("is_gap"),
            (
                (pl.col("high") >= pl.max_horizontal(["open", "close", "low"]))
                & (pl.col("low") <= pl.min_horizontal(["open", "close", "high"]))
            )
            .fill_null(False)
            .alias("candle_ok"),
        ]
    )

    keep = [
        "ts_ms",
        "open",
        "high",
        "low",
        "close",
        "volume_base",
        "logret_1",
        "logret_3",
        "logret_12",
        "logret_36",
        "vol_12",
        "vol_36",
        "range_pct",
        "body_pct",
        "volume_log",
        "volume_z",
        "is_gap",
        "candle_ok",
    ]
    _ = float_dtype  # preserved for call-site compatibility
    return frame.select([name for name in keep if name in frame.columns]).with_columns(
        [pl.col("ts_ms").cast(pl.Int64).alias("ts_ms")]
    )


def attach_sample_weight_v4_live_base(
    frame: pl.DataFrame,
    *,
    half_life_days: float,
    synth_weight_floor: float = 0.2,
    synth_weight_power: float = 2.0,
) -> pl.DataFrame:
    if frame.height <= 0:
        return frame.with_columns(pl.lit(None, dtype=pl.Float64).alias("sample_weight"))
    max_ts = int(frame.get_column("ts_ms").max())
    decay = math.log(2.0) / max(float(half_life_days), 1e-9)
    floor = min(max(float(synth_weight_floor), 0.0), 1.0)
    power = max(float(synth_weight_power), 0.0)

    age_weight = (
        ((pl.lit(max_ts, dtype=pl.Int64) - pl.col("ts_ms").cast(pl.Int64)).cast(pl.Float64) / 86_400_000.0)
        .mul(-decay)
        .exp()
        .cast(pl.Float64)
    )
    quality_weight: pl.Expr = pl.lit(1.0, dtype=pl.Float64)
    if "one_m_synth_ratio" in frame.columns:
        quality_weight = quality_weight_expr_from_synth_ratio(
            synth_ratio_expr=pl.col("one_m_synth_ratio"),
            floor=floor,
            power=power,
        )
    return frame.with_columns((age_weight * quality_weight).cast(pl.Float64).alias("sample_weight"))


def cast_feature_output_v4_live_base(
    frame: pl.DataFrame,
    *,
    float_dtype: str,
    high_tfs: tuple[str, ...] = ("15m", "60m", "240m"),
) -> pl.DataFrame:
    if frame.height <= 0:
        return frame
    dtype = pl.Float64 if str(float_dtype).strip().lower() == "float64" else pl.Float32
    bool_columns = {
        "is_gap",
        "candle_ok",
        "one_m_missing",
        "one_m_no_real",
        "one_m_fail",
        "m_micro_trade_available",
        "m_micro_book_available",
        "m_micro_available",
    }
    int_columns = {
        "ts_ms",
        "one_m_count",
        "one_m_last_ts",
        "one_m_synth_count",
        "one_m_real_count",
        "src_ts_micro",
        "y_cls",
        "m_trade_events",
        "m_trade_min_ts_ms",
        "m_trade_max_ts_ms",
        "m_book_events",
        "m_book_min_ts_ms",
        "m_book_max_ts_ms",
        "m_trade_coverage_ms",
        "m_book_coverage_ms",
        "m_trade_count",
        "m_buy_count",
        "m_sell_count",
        "m_book_update_count",
        "m_source_ws",
        "m_source_rest",
    }
    for tf in high_tfs:
        int_columns.add(f"src_ts_{tf}")
        int_columns.add(f"{high_tf_prefix(tf)}_regime_flag")

    exprs: list[pl.Expr] = []
    contract_cols = set(feature_columns_v4_live_base_contract(high_tfs=high_tfs))
    for name, col_dtype in frame.schema.items():
        if name in int_columns:
            cast = pl.Int8 if name == "y_cls" else pl.Int64
            exprs.append(pl.col(name).cast(cast).alias(name))
        elif name in bool_columns:
            exprs.append(pl.col(name).cast(pl.Boolean).alias(name))
        elif name == "m_trade_source":
            exprs.append(pl.col(name).fill_null("none").cast(pl.Utf8).alias(name))
        elif col_dtype in {pl.Float32, pl.Float64} or name in contract_cols:
            exprs.append(pl.col(name).cast(dtype).alias(name))
        else:
            exprs.append(pl.col(name).alias(name))
    return frame.with_columns(exprs)


def _dedupe_preserve(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in values:
        item = str(raw).strip()
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


base_feature_columns_v3 = base_feature_columns_v4_live_base
one_m_feature_columns_v3 = one_m_feature_columns_v4_live_base
high_tf_feature_columns_v3 = high_tf_feature_columns_v4_live_base
micro_feature_columns_v3 = micro_feature_columns_v4_live_base
feature_columns_v3_contract = feature_columns_v4_live_base_contract
required_feature_columns_v3 = required_feature_columns_v4_live_base
compute_base_features_v3 = compute_base_features_v4_live_base
attach_sample_weight_v3 = attach_sample_weight_v4_live_base
cast_feature_output_v3 = cast_feature_output_v4_live_base
