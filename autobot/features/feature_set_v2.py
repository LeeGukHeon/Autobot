"""features_v2 builder: OHLC(v1-equivalent) + micro_v1 join + label guard."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import polars as pl

from autobot.data import expected_interval_ms

from .feature_set_v1 import add_liquidity_roll_features, compute_feature_set_v1
from .feature_spec import FeatureSetV1Config, LabelV1Config, feature_columns
from .labeling_v1 import apply_labeling_v1, drop_neutral_rows
from .micro_join import (
    MICRO_BOOL_COLUMNS,
    MICRO_INT_COLUMNS,
    MICRO_VALUE_COLUMNS,
    MicroJoinStats,
    join_market_micro,
    prefixed_micro_columns,
)


@dataclass(frozen=True)
class MicroFilterPolicy:
    require_micro_available: bool = True
    min_trade_events: int = 1
    min_trade_coverage_ms: int = 60_000
    min_book_events: int = 1
    min_book_coverage_ms: int = 60_000


@dataclass(frozen=True)
class FeatureSetV2BuildResult:
    frame: pl.DataFrame
    feature_columns: tuple[str, ...]
    label_columns: tuple[str, ...]
    join_stats: MicroJoinStats
    prefilter_rows: int
    postfilter_rows: int
    tail_dropped_rows: int


def build_feature_set_v2_from_candles(
    *,
    candles_frame: pl.DataFrame,
    micro_frame: pl.DataFrame,
    tf: str,
    from_ts_ms: int,
    to_ts_ms: int,
    feature_config: FeatureSetV1Config,
    label_config: LabelV1Config,
    micro_filter: MicroFilterPolicy,
    factor_frames: dict[str, pl.DataFrame] | None = None,
    float_dtype: str = "float32",
) -> FeatureSetV2BuildResult:
    featured = compute_feature_set_v1(
        frame=candles_frame,
        tf=tf,
        config=feature_config,
        float_dtype=float_dtype,
    )

    if factor_frames:
        for frame in factor_frames.values():
            if frame.height <= 0:
                continue
            featured = featured.join(frame, on="ts_ms", how="left")

    if feature_config.enable_liquidity_rank:
        interval_ms = expected_interval_ms(tf)
        bars_24h = max(1, int(86_400_000 / interval_ms))
        featured = add_liquidity_roll_features(frame=featured, window_bars=bars_24h, float_dtype=float_dtype)

    labeled = apply_labeling_v1(frame=featured, config=label_config).filter(
        (pl.col("ts_ms") >= int(from_ts_ms)) & (pl.col("ts_ms") <= int(to_ts_ms))
    )
    guarded, tail_dropped = apply_label_tail_guard(labeled, horizon_bars=label_config.horizon_bars)
    guarded = drop_neutral_rows(frame=guarded, config=label_config)

    base_feature_cols = feature_columns(feature_config)
    required_non_null = [name for name in base_feature_cols if name in guarded.columns and name != "vol_rank_at_ts"]
    if "y_reg" in guarded.columns:
        required_non_null.append("y_reg")
    if "y_cls" in guarded.columns:
        required_non_null.append("y_cls")
    if required_non_null:
        guarded = guarded.filter(pl.all_horizontal([pl.col(name).is_not_null() for name in required_non_null]))

    joined, join_stats = join_market_micro(base_frame=guarded, micro_frame=micro_frame)
    output, prefilter_rows, postfilter_rows = attach_micro_and_filter(
        base_labeled_frame=joined,
        feature_columns_base=base_feature_cols,
        policy=micro_filter,
        float_dtype=float_dtype,
    )

    return FeatureSetV2BuildResult(
        frame=output,
        feature_columns=tuple(_dedupe_preserve(base_feature_cols + prefixed_micro_columns())),
        label_columns=("y_reg", "y_cls"),
        join_stats=join_stats,
        prefilter_rows=prefilter_rows,
        postfilter_rows=postfilter_rows,
        tail_dropped_rows=tail_dropped,
    )


def attach_micro_and_filter(
    *,
    base_labeled_frame: pl.DataFrame,
    feature_columns_base: list[str],
    policy: MicroFilterPolicy,
    float_dtype: str = "float32",
) -> tuple[pl.DataFrame, int, int]:
    prefilter_rows = int(base_labeled_frame.height)
    filtered = apply_micro_filter(base_labeled_frame, policy=policy)
    postfilter_rows = int(filtered.height)

    final_feature_cols = _dedupe_preserve(list(feature_columns_base) + prefixed_micro_columns())
    output_cols = ["ts_ms"] + final_feature_cols + ["y_reg", "y_cls"]
    output = _ensure_columns(filtered, output_cols)
    output = _cast_output_dtypes(output.select(output_cols).sort("ts_ms"), float_dtype=float_dtype)
    return output, prefilter_rows, postfilter_rows


def apply_label_tail_guard(frame: pl.DataFrame, *, horizon_bars: int) -> tuple[pl.DataFrame, int]:
    horizon = max(int(horizon_bars), 1)
    if frame.height <= 0:
        return frame, 0
    if frame.height <= horizon:
        return frame, 0
    limit = int(frame.height) - horizon
    guarded = frame.with_row_index("__idx").filter(pl.col("__idx") < limit).drop("__idx")
    return guarded, int(frame.height - guarded.height)


def apply_micro_filter(frame: pl.DataFrame, *, policy: MicroFilterPolicy) -> pl.DataFrame:
    if frame.height <= 0:
        return frame

    conditions: list[pl.Expr] = []
    if policy.require_micro_available and "m_micro_available" in frame.columns:
        conditions.append(pl.col("m_micro_available") == True)  # noqa: E712
    if "m_trade_events" in frame.columns:
        conditions.append(pl.col("m_trade_events") >= max(int(policy.min_trade_events), 0))
    if "m_book_events" in frame.columns:
        conditions.append(pl.col("m_book_events") >= max(int(policy.min_book_events), 0))
    if "m_trade_coverage_ms" in frame.columns:
        conditions.append(pl.col("m_trade_coverage_ms") >= max(int(policy.min_trade_coverage_ms), 0))
    if "m_book_coverage_ms" in frame.columns:
        conditions.append(pl.col("m_book_coverage_ms") >= max(int(policy.min_book_coverage_ms), 0))

    if not conditions:
        return frame
    return frame.filter(pl.all_horizontal(conditions))


def _ensure_columns(frame: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    working = frame
    for name in columns:
        if name in working.columns:
            continue
        working = working.with_columns(pl.lit(None).alias(name))
    return working


def _cast_output_dtypes(frame: pl.DataFrame, *, float_dtype: str) -> pl.DataFrame:
    float_pl = pl.Float64 if str(float_dtype).strip().lower() == "float64" else pl.Float32
    micro_bool_cols = {f"m_{name}" for name in MICRO_BOOL_COLUMNS}
    micro_int_cols = {f"m_{name}" for name in MICRO_INT_COLUMNS}
    micro_trade_source = "m_trade_source"
    cast_exprs: list[pl.Expr] = []
    for name, dtype in frame.schema.items():
        if name == "ts_ms":
            cast_exprs.append(pl.col(name).cast(pl.Int64).alias(name))
        elif name in {"is_gap", "candle_ok", "volume_quote_est", *micro_bool_cols}:
            cast_exprs.append(pl.col(name).cast(pl.Boolean).alias(name))
        elif name == "y_cls":
            cast_exprs.append(pl.col(name).cast(pl.Int8).alias(name))
        elif name in micro_int_cols:
            cast_exprs.append(pl.col(name).cast(pl.Int64).alias(name))
        elif name == micro_trade_source:
            cast_exprs.append(pl.col(name).fill_null("none").cast(pl.Utf8).alias(name))
        elif name in {"y_reg"} or dtype in {pl.Float32, pl.Float64}:
            cast_exprs.append(pl.col(name).cast(float_pl).alias(name))
        else:
            cast_exprs.append(pl.col(name).alias(name))
    return frame.with_columns(cast_exprs)


def _dedupe_preserve(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for item in values:
        text = str(item).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        deduped.append(text)
    return deduped


def prefixed_micro_columns_v2() -> list[str]:
    return [f"m_{name}" for name in MICRO_VALUE_COLUMNS]


def micro_column_defaults() -> dict[str, Any]:
    defaults: dict[str, Any] = {}
    for name in MICRO_VALUE_COLUMNS:
        key = f"m_{name}"
        if name in MICRO_BOOL_COLUMNS:
            defaults[key] = False
        elif name in MICRO_INT_COLUMNS:
            defaults[key] = 0
        elif name == "trade_source":
            defaults[key] = "none"
        else:
            defaults[key] = None
    return defaults
