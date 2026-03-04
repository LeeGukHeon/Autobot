"""FeatureSet v3: base(5m) + multi-tf + mandatory micro + sample weights."""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Iterable

import polars as pl

from autobot.data import expected_interval_ms

from .feature_set_v2 import apply_label_tail_guard
from .labeling_v1 import apply_labeling_v1, drop_neutral_rows
from .micro_join import prefixed_micro_columns
from .micro_required_join_v1 import MicroRequiredJoinResult, join_micro_required
from .multitf_join_v1 import (
    HighTfJoinStats,
    OneMJoinStats,
    aggregate_1m_for_base,
    compute_high_tf_features,
    high_tf_prefix,
    join_1m_aggregate,
    join_high_tf_asof,
)
from .feature_spec import LabelV1Config


@dataclass(frozen=True)
class FeatureSetV3BuildResult:
    frame: pl.DataFrame
    feature_columns: tuple[str, ...]
    label_columns: tuple[str, ...]
    rows_base_total: int
    rows_after_multitf: int
    rows_after_label: int
    rows_after_micro: int
    rows_dropped_no_micro: int
    rows_dropped_stale: int
    rows_dropped_one_m: int
    tail_dropped_rows: int
    micro_tf_used: str
    high_tf_stats: tuple[HighTfJoinStats, ...]
    one_m_stats: OneMJoinStats


def feature_columns_v3(*, high_tfs: tuple[str, ...] = ("15m", "60m", "240m")) -> tuple[str, ...]:
    base = (
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
    one_m = (
        "one_m_count",
        "one_m_ret_mean",
        "one_m_ret_std",
        "one_m_volume_sum",
        "one_m_range_mean",
        "one_m_missing_ratio",
    )
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
    return tuple(_dedupe_preserve(list(base) + list(one_m) + high + micro))


def build_feature_set_v3_from_candles(
    *,
    base_candles_frame: pl.DataFrame,
    one_m_candles_frame: pl.DataFrame,
    high_tf_candles: dict[str, pl.DataFrame],
    micro_frame: pl.DataFrame,
    micro_tf_used: str,
    tf: str,
    from_ts_ms: int,
    to_ts_ms: int,
    label_config: LabelV1Config,
    high_tfs: tuple[str, ...] = ("15m", "60m", "240m"),
    high_tf_staleness_multiplier: float = 2.0,
    one_m_required_bars: int = 5,
    one_m_max_missing_ratio: float = 0.2,
    sample_weight_half_life_days: float = 60.0,
    float_dtype: str = "float32",
) -> FeatureSetV3BuildResult:
    if base_candles_frame.height <= 0:
        return FeatureSetV3BuildResult(
            frame=pl.DataFrame(),
            feature_columns=feature_columns_v3(high_tfs=high_tfs),
            label_columns=("y_reg", "y_cls"),
            rows_base_total=0,
            rows_after_multitf=0,
            rows_after_label=0,
            rows_after_micro=0,
            rows_dropped_no_micro=0,
            rows_dropped_stale=0,
            rows_dropped_one_m=0,
            tail_dropped_rows=0,
            micro_tf_used=micro_tf_used,
            high_tf_stats=tuple(),
            one_m_stats=OneMJoinStats(0, 0, 0, max(int(one_m_required_bars), 1), float(one_m_max_missing_ratio)),
        )

    working = _compute_base_features(base_candles_frame, tf=tf, float_dtype=float_dtype).sort("ts_ms")
    one_m_agg = aggregate_1m_for_base(one_m_candles_frame, base_tf=tf, float_dtype=float_dtype)
    working, one_m_stats = join_1m_aggregate(
        base_frame=working,
        one_m_agg=one_m_agg,
        required_bars=one_m_required_bars,
        max_missing_ratio=one_m_max_missing_ratio,
    )

    high_stats: list[HighTfJoinStats] = []
    for high_tf in high_tfs:
        raw = high_tf_candles.get(high_tf, pl.DataFrame())
        features = compute_high_tf_features(raw, tf=high_tf, float_dtype=float_dtype)
        staleness = int(round(expected_interval_ms(high_tf) * max(float(high_tf_staleness_multiplier), 0.0)))
        joined, stats = join_high_tf_asof(
            base_frame=working,
            high_tf_features=features,
            tf=high_tf,
            max_staleness_ms=staleness,
        )
        working = joined
        high_stats.append(stats)

    in_window = working.filter((pl.col("ts_ms") >= int(from_ts_ms)) & (pl.col("ts_ms") <= int(to_ts_ms))).sort("ts_ms")
    rows_base_total = int(in_window.height)
    if rows_base_total <= 0:
        return FeatureSetV3BuildResult(
            frame=pl.DataFrame(),
            feature_columns=feature_columns_v3(high_tfs=high_tfs),
            label_columns=("y_reg", "y_cls"),
            rows_base_total=0,
            rows_after_multitf=0,
            rows_after_label=0,
            rows_after_micro=0,
            rows_dropped_no_micro=0,
            rows_dropped_stale=0,
            rows_dropped_one_m=0,
            tail_dropped_rows=0,
            micro_tf_used=micro_tf_used,
            high_tf_stats=tuple(high_stats),
            one_m_stats=one_m_stats,
        )

    stale_exprs = [pl.col(f"{high_tf_prefix(item)}_stale") == True for item in high_tfs if f"{high_tf_prefix(item)}_stale" in in_window.columns]  # noqa: E712
    stale_mask = pl.any_horizontal(stale_exprs) if stale_exprs else pl.lit(False)
    rows_dropped_stale = int(in_window.filter(stale_mask).height) if stale_exprs else 0
    rows_dropped_one_m = int(in_window.filter(pl.col("one_m_fail") == True).height) if "one_m_fail" in in_window.columns else 0  # noqa: E712

    filtered = in_window
    if stale_exprs:
        filtered = filtered.filter(~stale_mask)
    if "one_m_fail" in filtered.columns:
        filtered = filtered.filter(pl.col("one_m_fail") == False)  # noqa: E712
    rows_after_multitf = int(filtered.height)

    labeled = apply_labeling_v1(frame=filtered, config=label_config)
    labeled, tail_dropped = apply_label_tail_guard(labeled, horizon_bars=label_config.horizon_bars)
    labeled = drop_neutral_rows(frame=labeled, config=label_config)
    required_non_null = [
        name
        for name in _required_feature_columns_for_filter(high_tfs=high_tfs)
        if name in labeled.columns and name != "m_trade_source"
    ]
    required_non_null.extend([name for name in ("y_reg", "y_cls") if name in labeled.columns])
    if required_non_null:
        labeled = labeled.filter(pl.all_horizontal([pl.col(name).is_not_null() for name in required_non_null]))
    rows_after_label = int(labeled.height)

    micro_join = join_micro_required(base_frame=labeled, micro_frame=micro_frame, micro_tf_used=micro_tf_used)
    output = micro_join.frame
    rows_after_micro = int(output.height)

    output = _attach_sample_weight(
        output,
        half_life_days=max(float(sample_weight_half_life_days), 1e-6),
    )
    output = _cast_output(output, float_dtype=float_dtype)

    return FeatureSetV3BuildResult(
        frame=output.sort("ts_ms"),
        feature_columns=feature_columns_v3(high_tfs=high_tfs),
        label_columns=("y_reg", "y_cls"),
        rows_base_total=rows_base_total,
        rows_after_multitf=rows_after_multitf,
        rows_after_label=rows_after_label,
        rows_after_micro=rows_after_micro,
        rows_dropped_no_micro=int(micro_join.rows_dropped_no_micro),
        rows_dropped_stale=rows_dropped_stale,
        rows_dropped_one_m=rows_dropped_one_m,
        tail_dropped_rows=int(tail_dropped),
        micro_tf_used=micro_join.micro_tf_used,
        high_tf_stats=tuple(high_stats),
        one_m_stats=one_m_stats,
    )


def _compute_base_features(base_candles_frame: pl.DataFrame, *, tf: str, float_dtype: str) -> pl.DataFrame:
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
    return frame.select([name for name in keep if name in frame.columns]).with_columns(
        [pl.col("ts_ms").cast(pl.Int64).alias("ts_ms")]
    )


def _attach_sample_weight(frame: pl.DataFrame, *, half_life_days: float) -> pl.DataFrame:
    if frame.height <= 0:
        return frame.with_columns(pl.lit(None, dtype=pl.Float64).alias("sample_weight"))
    max_ts = int(frame.get_column("ts_ms").max())
    decay = math.log(2.0) / max(float(half_life_days), 1e-9)
    return frame.with_columns(
        (
            ((pl.lit(max_ts, dtype=pl.Int64) - pl.col("ts_ms").cast(pl.Int64)).cast(pl.Float64) / 86_400_000.0)
            .mul(-decay)
            .exp()
            .cast(pl.Float64)
            .alias("sample_weight")
        )
    )


def _cast_output(frame: pl.DataFrame, *, float_dtype: str) -> pl.DataFrame:
    if frame.height <= 0:
        return frame
    dtype = pl.Float64 if str(float_dtype).strip().lower() == "float64" else pl.Float32
    bool_columns = {
        "is_gap",
        "candle_ok",
        "one_m_missing",
        "one_m_fail",
        "m_micro_trade_available",
        "m_micro_book_available",
        "m_micro_available",
    }
    int_columns = {
        "ts_ms",
        "one_m_count",
        "one_m_last_ts",
        "src_ts_micro",
        "y_cls",
        "m_trade_events",
        "m_book_events",
        "m_trade_coverage_ms",
        "m_book_coverage_ms",
        "m_trade_count",
        "m_buy_count",
        "m_sell_count",
        "m_book_update_count",
        "m_source_ws",
        "m_source_rest",
    }
    for tf in ("15m", "60m", "240m"):
        int_columns.add(f"src_ts_{tf}")
        int_columns.add(f"{high_tf_prefix(tf)}_regime_flag")

    exprs: list[pl.Expr] = []
    for name, col_dtype in frame.schema.items():
        if name in int_columns:
            cast = pl.Int8 if name == "y_cls" else pl.Int64
            exprs.append(pl.col(name).cast(cast).alias(name))
        elif name in bool_columns:
            exprs.append(pl.col(name).cast(pl.Boolean).alias(name))
        elif name == "m_trade_source":
            exprs.append(pl.col(name).fill_null("none").cast(pl.Utf8).alias(name))
        elif col_dtype in {pl.Float32, pl.Float64} or name in feature_columns_v3():
            exprs.append(pl.col(name).cast(dtype).alias(name))
        else:
            exprs.append(pl.col(name).alias(name))
    return frame.with_columns(exprs)


def _required_feature_columns_for_filter(*, high_tfs: tuple[str, ...]) -> tuple[str, ...]:
    base = (
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
        "one_m_count",
        "one_m_ret_mean",
        "one_m_ret_std",
        "one_m_volume_sum",
        "one_m_range_mean",
    )
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
    return tuple(_dedupe_preserve(list(base) + high + list(micro)))


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
