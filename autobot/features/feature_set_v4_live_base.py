"""FeatureSet v4 live-base: active base tf (1m/5m) + multi-tf + mandatory micro + sample weights."""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from autobot.data import expected_interval_ms

from .feature_blocks_v4_live_base import (
    attach_sample_weight_v4_live_base,
    cast_feature_output_v4_live_base,
    compute_base_features_v4_live_base,
    feature_columns_v4_live_base_contract,
    required_feature_columns_v4_live_base,
)
from .feature_set_v2 import apply_label_tail_guard
from .labeling_v1 import apply_labeling_v1, drop_neutral_rows
from .micro_required_join_v1 import MicroRequiredJoinResult, join_micro_required
from .multitf_join_v1 import (
    HighTfJoinStats,
    OneMJoinStats,
    aggregate_1m_for_base,
    compute_high_tf_features,
    densify_1m_candles,
    effective_one_m_required_bars,
    high_tf_prefix,
    join_1m_aggregate,
    join_high_tf_asof,
)
from .feature_spec import LabelV1Config


@dataclass(frozen=True)
class FeatureSetV4LiveBaseBuildResult:
    frame: pl.DataFrame
    feature_columns: tuple[str, ...]
    label_columns: tuple[str, ...]
    rows_base_total: int
    rows_after_multitf: int
    rows_after_label: int
    rows_after_micro: int
    rows_dropped_no_micro: int
    rows_dropped_stale: int
    rows_dropped_one_m_before_densify: int
    rows_dropped_one_m: int
    rows_rescued_by_one_m_densify: int
    tail_dropped_rows: int
    micro_tf_used: str
    one_m_synth_ratio_mean: float | None
    one_m_synth_ratio_p50: float | None
    one_m_synth_ratio_p90: float | None
    high_tf_stats: tuple[HighTfJoinStats, ...]
    one_m_stats: OneMJoinStats


def feature_columns_v4_live_base(*, high_tfs: tuple[str, ...] = ("15m", "60m", "240m")) -> tuple[str, ...]:
    return feature_columns_v4_live_base_contract(high_tfs=high_tfs)


def build_feature_set_v4_live_base_from_candles(
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
    one_m_drop_if_real_count_zero: bool = True,
    sample_weight_half_life_days: float = 60.0,
    one_m_synth_weight_floor: float = 0.2,
    one_m_synth_weight_power: float = 2.0,
    float_dtype: str = "float32",
) -> FeatureSetV4LiveBaseBuildResult:
    effective_required_bars = effective_one_m_required_bars(base_tf=tf, required_bars=one_m_required_bars)
    if base_candles_frame.height <= 0:
        return FeatureSetV4LiveBaseBuildResult(
            frame=pl.DataFrame(),
            feature_columns=feature_columns_v4_live_base(high_tfs=high_tfs),
            label_columns=("y_reg", "y_cls"),
            rows_base_total=0,
            rows_after_multitf=0,
            rows_after_label=0,
            rows_after_micro=0,
            rows_dropped_no_micro=0,
            rows_dropped_stale=0,
            rows_dropped_one_m_before_densify=0,
            rows_dropped_one_m=0,
            rows_rescued_by_one_m_densify=0,
            tail_dropped_rows=0,
            micro_tf_used=micro_tf_used,
            one_m_synth_ratio_mean=None,
            one_m_synth_ratio_p50=None,
            one_m_synth_ratio_p90=None,
            high_tf_stats=tuple(),
            one_m_stats=OneMJoinStats(0, 0, 0, effective_required_bars, float(one_m_max_missing_ratio)),
        )

    working = compute_base_features_v4_live_base(base_candles_frame, tf=tf, float_dtype=float_dtype).sort("ts_ms")
    dense_start_ts_ms = int(working.get_column("ts_ms").min())
    dense_end_ts_ms = int(working.get_column("ts_ms").max())

    one_m_agg_before = aggregate_1m_for_base(one_m_candles_frame, base_tf=tf, float_dtype=float_dtype)
    working_before, _ = join_1m_aggregate(
        base_frame=working,
        one_m_agg=one_m_agg_before,
        required_bars=effective_required_bars,
        max_missing_ratio=one_m_max_missing_ratio,
        drop_if_real_count_zero=False,
    )

    one_m_dense = densify_1m_candles(
        one_m_candles_frame,
        start_ts_ms=dense_start_ts_ms,
        end_ts_ms=dense_end_ts_ms,
    )
    one_m_agg = aggregate_1m_for_base(one_m_dense, base_tf=tf, float_dtype=float_dtype)
    working, one_m_stats = join_1m_aggregate(
        base_frame=working,
        one_m_agg=one_m_agg,
        required_bars=effective_required_bars,
        max_missing_ratio=one_m_max_missing_ratio,
        drop_if_real_count_zero=one_m_drop_if_real_count_zero,
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
    in_window_before = working_before.filter((pl.col("ts_ms") >= int(from_ts_ms)) & (pl.col("ts_ms") <= int(to_ts_ms))).sort("ts_ms")
    rows_base_total = int(in_window.height)
    if rows_base_total <= 0:
        return FeatureSetV4LiveBaseBuildResult(
            frame=pl.DataFrame(),
            feature_columns=feature_columns_v4_live_base(high_tfs=high_tfs),
            label_columns=("y_reg", "y_cls"),
            rows_base_total=0,
            rows_after_multitf=0,
            rows_after_label=0,
            rows_after_micro=0,
            rows_dropped_no_micro=0,
            rows_dropped_stale=0,
            rows_dropped_one_m_before_densify=0,
            rows_dropped_one_m=0,
            rows_rescued_by_one_m_densify=0,
            tail_dropped_rows=0,
            micro_tf_used=micro_tf_used,
            one_m_synth_ratio_mean=None,
            one_m_synth_ratio_p50=None,
            one_m_synth_ratio_p90=None,
            high_tf_stats=tuple(high_stats),
            one_m_stats=one_m_stats,
        )

    stale_exprs = [pl.col(f"{high_tf_prefix(item)}_stale") == True for item in high_tfs if f"{high_tf_prefix(item)}_stale" in in_window.columns]  # noqa: E712
    stale_mask = pl.any_horizontal(stale_exprs) if stale_exprs else pl.lit(False)
    rows_dropped_stale = int(in_window.filter(stale_mask).height) if stale_exprs else 0
    rows_dropped_one_m_before_densify = (
        int(in_window_before.filter(pl.col("one_m_fail") == True).height) if "one_m_fail" in in_window_before.columns else 0  # noqa: E712
    )
    rows_dropped_one_m = int(in_window.filter(pl.col("one_m_fail") == True).height) if "one_m_fail" in in_window.columns else 0  # noqa: E712
    rows_rescued_by_one_m_densify = 0
    if "one_m_fail" in in_window_before.columns and "one_m_fail" in in_window.columns:
        rescue = (
            in_window_before.select(["ts_ms", pl.col("one_m_fail").alias("before_fail")])
            .join(
                in_window.select(["ts_ms", pl.col("one_m_fail").alias("after_fail")]),
                on="ts_ms",
                how="inner",
            )
            .filter((pl.col("before_fail") == True) & (pl.col("after_fail") == False))  # noqa: E712
        )
        rows_rescued_by_one_m_densify = int(rescue.height)

    filtered = in_window
    if stale_exprs:
        filtered = filtered.filter(~stale_mask)
    if "one_m_fail" in filtered.columns:
        filtered = filtered.filter(pl.col("one_m_fail") == False)  # noqa: E712
    rows_after_multitf = int(filtered.height)
    one_m_synth_ratio_mean, one_m_synth_ratio_p50, one_m_synth_ratio_p90 = _one_m_synth_ratio_stats(filtered)

    micro_join = join_micro_required(base_frame=filtered, micro_frame=micro_frame, micro_tf_used=micro_tf_used)
    output = micro_join.frame
    rows_after_micro = int(output.height)

    bootstrap_labeled = apply_labeling_v1(frame=output, config=label_config)
    bootstrap_labeled, tail_dropped = apply_label_tail_guard(bootstrap_labeled, horizon_bars=label_config.horizon_bars)
    bootstrap_labeled = drop_neutral_rows(frame=bootstrap_labeled, config=label_config)
    required_non_null = [
        name
        for name in required_feature_columns_v4_live_base(high_tfs=high_tfs)
        if name in bootstrap_labeled.columns and name != "m_trade_source"
    ]
    required_non_null.extend([name for name in ("y_reg", "y_cls") if name in bootstrap_labeled.columns])
    if required_non_null:
        bootstrap_labeled = bootstrap_labeled.filter(pl.all_horizontal([pl.col(name).is_not_null() for name in required_non_null]))
    rows_after_label = int(bootstrap_labeled.height)

    output = attach_sample_weight_v4_live_base(
        output,
        half_life_days=max(float(sample_weight_half_life_days), 1e-6),
        synth_weight_floor=one_m_synth_weight_floor,
        synth_weight_power=one_m_synth_weight_power,
    )
    output = cast_feature_output_v4_live_base(output, float_dtype=float_dtype, high_tfs=high_tfs)

    return FeatureSetV4LiveBaseBuildResult(
        frame=output.sort("ts_ms"),
        feature_columns=feature_columns_v4_live_base(high_tfs=high_tfs),
        label_columns=("y_reg", "y_cls"),
        rows_base_total=rows_base_total,
        rows_after_multitf=rows_after_multitf,
        rows_after_label=rows_after_label,
        rows_after_micro=rows_after_micro,
        rows_dropped_no_micro=int(micro_join.rows_dropped_no_micro),
        rows_dropped_stale=rows_dropped_stale,
        rows_dropped_one_m_before_densify=rows_dropped_one_m_before_densify,
        rows_dropped_one_m=rows_dropped_one_m,
        rows_rescued_by_one_m_densify=rows_rescued_by_one_m_densify,
        tail_dropped_rows=int(tail_dropped),
        micro_tf_used=micro_join.micro_tf_used,
        one_m_synth_ratio_mean=one_m_synth_ratio_mean,
        one_m_synth_ratio_p50=one_m_synth_ratio_p50,
        one_m_synth_ratio_p90=one_m_synth_ratio_p90,
        high_tf_stats=tuple(high_stats),
        one_m_stats=one_m_stats,
    )

def _compute_base_features(base_candles_frame: pl.DataFrame, *, tf: str, float_dtype: str) -> pl.DataFrame:
    return compute_base_features_v4_live_base(base_candles_frame, tf=tf, float_dtype=float_dtype)


def _attach_sample_weight(
    frame: pl.DataFrame,
    *,
    half_life_days: float,
    synth_weight_floor: float = 0.2,
    synth_weight_power: float = 2.0,
) -> pl.DataFrame:
    return attach_sample_weight_v4_live_base(
        frame,
        half_life_days=half_life_days,
        synth_weight_floor=synth_weight_floor,
        synth_weight_power=synth_weight_power,
    )


def _cast_output(frame: pl.DataFrame, *, float_dtype: str) -> pl.DataFrame:
    return cast_feature_output_v4_live_base(frame, float_dtype=float_dtype)


def _required_feature_columns_for_filter(*, high_tfs: tuple[str, ...]) -> tuple[str, ...]:
    return required_feature_columns_v4_live_base(high_tfs=high_tfs)


def _one_m_synth_ratio_stats(frame: pl.DataFrame) -> tuple[float | None, float | None, float | None]:
    if frame.height <= 0 or "one_m_synth_ratio" not in frame.columns:
        return None, None, None
    values = [float(v) for v in frame.get_column("one_m_synth_ratio").drop_nulls().to_list()]
    if not values:
        return None, None, None
    values.sort()
    mean = float(sum(values) / len(values))
    p50 = _quantile_sorted(values, 0.50)
    p90 = _quantile_sorted(values, 0.90)
    return mean, p50, p90


def _quantile_sorted(values: list[float], q: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return float(values[0])
    q_clamped = min(max(float(q), 0.0), 1.0)
    idx = int(round((len(values) - 1) * q_clamped))
    return float(values[idx])
