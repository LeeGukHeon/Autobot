"""Lookahead-safe multi-timeframe join helpers for FeatureSet v3."""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from autobot.data import expected_interval_ms


@dataclass(frozen=True)
class HighTfJoinStats:
    tf: str
    rows_total: int
    rows_missing: int
    rows_stale: int
    max_age_ms: int | None


@dataclass(frozen=True)
class OneMJoinStats:
    rows_total: int
    rows_missing: int
    rows_failed: int
    required_bars: int
    max_missing_ratio: float


def high_tf_prefix(tf: str) -> str:
    return f"tf{str(tf).strip().lower()}"


def compute_high_tf_features(
    candles_frame: pl.DataFrame,
    *,
    tf: str,
    float_dtype: str = "float32",
) -> pl.DataFrame:
    if candles_frame.height <= 0:
        return pl.DataFrame(
            schema={
                "ts_ms": pl.Int64,
                f"{high_tf_prefix(tf)}_ret_1": _float_dtype(float_dtype),
                f"{high_tf_prefix(tf)}_ret_3": _float_dtype(float_dtype),
                f"{high_tf_prefix(tf)}_vol_3": _float_dtype(float_dtype),
                f"{high_tf_prefix(tf)}_trend_slope": _float_dtype(float_dtype),
                f"{high_tf_prefix(tf)}_regime_flag": pl.Int8,
            }
        )

    required = {"ts_ms", "close"}
    missing = [name for name in required if name not in candles_frame.columns]
    if missing:
        raise ValueError(f"high-tf frame missing columns: {missing}")

    prefix = high_tf_prefix(tf)
    dtype = _float_dtype(float_dtype)
    frame = candles_frame.sort("ts_ms")
    frame = frame.with_columns(
        [
            (pl.col("close").log() - pl.col("close").shift(1).log()).alias("__ret_1"),
            (pl.col("close").log() - pl.col("close").shift(3).log()).alias("__ret_3"),
        ]
    )
    frame = frame.with_columns(
        [
            pl.col("__ret_1").rolling_std(window_size=3, min_samples=3).alias("__vol_3"),
            pl.col("close").rolling_mean(window_size=3, min_samples=3).alias("__ma_fast"),
            pl.col("close").rolling_mean(window_size=9, min_samples=9).alias("__ma_slow"),
        ]
    )
    frame = frame.with_columns(
        [
            ((pl.col("__ma_fast") - pl.col("__ma_slow")) / pl.col("__ma_slow")).alias("__trend_slope"),
            (pl.col("__ma_fast") > pl.col("__ma_slow")).cast(pl.Int8).alias("__regime_flag"),
        ]
    )
    return frame.select(
        [
            pl.col("ts_ms").cast(pl.Int64).alias("ts_ms"),
            pl.col("__ret_1").cast(dtype).alias(f"{prefix}_ret_1"),
            pl.col("__ret_3").cast(dtype).alias(f"{prefix}_ret_3"),
            pl.col("__vol_3").cast(dtype).alias(f"{prefix}_vol_3"),
            pl.col("__trend_slope").cast(dtype).alias(f"{prefix}_trend_slope"),
            pl.col("__regime_flag").cast(pl.Int8).alias(f"{prefix}_regime_flag"),
        ]
    )


def join_high_tf_asof(
    *,
    base_frame: pl.DataFrame,
    high_tf_features: pl.DataFrame,
    tf: str,
    max_staleness_ms: int,
) -> tuple[pl.DataFrame, HighTfJoinStats]:
    if "ts_ms" not in base_frame.columns:
        raise ValueError("base_frame must include ts_ms")

    rows_total = int(base_frame.height)
    if rows_total <= 0:
        return base_frame, HighTfJoinStats(
            tf=tf,
            rows_total=0,
            rows_missing=0,
            rows_stale=0,
            max_age_ms=None,
        )

    prefix = high_tf_prefix(tf)
    src_col = f"src_ts_{tf}"
    if high_tf_features.height <= 0:
        out = _ensure_high_tf_default_columns(base_frame, tf=tf)
        out = out.with_columns(
            [
                pl.lit(None, dtype=pl.Int64).alias(src_col),
                pl.lit(True, dtype=pl.Boolean).alias(f"{prefix}_stale"),
            ]
        )
        return out, HighTfJoinStats(
            tf=tf,
            rows_total=rows_total,
            rows_missing=rows_total,
            rows_stale=rows_total,
            max_age_ms=None,
        )

    right = high_tf_features.sort("ts_ms").rename({"ts_ms": src_col})
    joined = base_frame.sort("ts_ms").join_asof(
        right,
        left_on="ts_ms",
        right_on=src_col,
        strategy="backward",
    )
    joined = _ensure_high_tf_default_columns(joined, tf=tf)
    age_col = f"{prefix}_age_ms"
    stale_col = f"{prefix}_stale"
    joined = joined.with_columns(
        [
            (pl.col("ts_ms") - pl.col(src_col)).cast(pl.Int64).alias(age_col),
            (
                pl.col(src_col).is_null()
                | (pl.col("ts_ms") < pl.col(src_col))
                | (pl.col("ts_ms") - pl.col(src_col) > int(max_staleness_ms))
            )
            .fill_null(True)
            .alias(stale_col),
        ]
    )

    rows_missing = int(joined.get_column(src_col).null_count()) if src_col in joined.columns else rows_total
    rows_stale = int(joined.filter(pl.col(stale_col) == True).height)  # noqa: E712
    max_age = None
    if age_col in joined.columns:
        values = joined.get_column(age_col).drop_nulls()
        if values.len() > 0:
            max_age = int(values.max())

    return joined, HighTfJoinStats(
        tf=tf,
        rows_total=rows_total,
        rows_missing=rows_missing,
        rows_stale=rows_stale,
        max_age_ms=max_age,
    )


def aggregate_1m_for_base(
    one_m_candles: pl.DataFrame,
    *,
    base_tf: str,
    float_dtype: str = "float32",
) -> pl.DataFrame:
    if one_m_candles.height <= 0:
        return pl.DataFrame(
            schema={
                "ts_ms": pl.Int64,
                "one_m_count": pl.Int64,
                "one_m_last_ts": pl.Int64,
                "one_m_ret_mean": _float_dtype(float_dtype),
                "one_m_ret_std": _float_dtype(float_dtype),
                "one_m_volume_sum": _float_dtype(float_dtype),
                "one_m_range_mean": _float_dtype(float_dtype),
            }
        )

    required = {"ts_ms", "high", "low", "close", "volume_base"}
    missing = [name for name in required if name not in one_m_candles.columns]
    if missing:
        raise ValueError(f"1m frame missing columns: {missing}")

    dtype = _float_dtype(float_dtype)
    base_interval_ms = expected_interval_ms(base_tf)
    frame = one_m_candles.sort("ts_ms")
    frame = frame.with_columns(
        [
            (pl.col("close").log() - pl.col("close").shift(1).log()).alias("__ret_1m"),
            (pl.col("high") / pl.col("low") - 1.0).alias("__range_1m"),
            (((pl.col("ts_ms") // int(base_interval_ms)) * int(base_interval_ms)) + int(base_interval_ms))
            .cast(pl.Int64)
            .alias("__base_ts"),
        ]
    )
    grouped = frame.group_by("__base_ts").agg(
        [
            pl.len().cast(pl.Int64).alias("one_m_count"),
            pl.col("ts_ms").max().cast(pl.Int64).alias("one_m_last_ts"),
            pl.col("__ret_1m").mean().cast(dtype).alias("one_m_ret_mean"),
            pl.col("__ret_1m").std().cast(dtype).alias("one_m_ret_std"),
            pl.col("volume_base").sum().cast(dtype).alias("one_m_volume_sum"),
            pl.col("__range_1m").mean().cast(dtype).alias("one_m_range_mean"),
        ]
    )
    return grouped.rename({"__base_ts": "ts_ms"}).sort("ts_ms")


def join_1m_aggregate(
    *,
    base_frame: pl.DataFrame,
    one_m_agg: pl.DataFrame,
    required_bars: int = 5,
    max_missing_ratio: float = 0.2,
) -> tuple[pl.DataFrame, OneMJoinStats]:
    rows_total = int(base_frame.height)
    if rows_total <= 0:
        return base_frame, OneMJoinStats(
            rows_total=0,
            rows_missing=0,
            rows_failed=0,
            required_bars=max(int(required_bars), 1),
            max_missing_ratio=float(max_missing_ratio),
        )

    joined = base_frame.sort("ts_ms")
    if one_m_agg.height > 0:
        joined = joined.join(one_m_agg.sort("ts_ms"), on="ts_ms", how="left")

    required = max(int(required_bars), 1)
    joined = joined.with_columns(
        [
            pl.col("one_m_count").fill_null(0).cast(pl.Int64).alias("one_m_count"),
            pl.col("one_m_last_ts").cast(pl.Int64).alias("one_m_last_ts"),
            pl.col("one_m_ret_mean").cast(pl.Float64).alias("one_m_ret_mean"),
            pl.col("one_m_ret_std").cast(pl.Float64).alias("one_m_ret_std"),
            pl.col("one_m_volume_sum").cast(pl.Float64).alias("one_m_volume_sum"),
            pl.col("one_m_range_mean").cast(pl.Float64).alias("one_m_range_mean"),
        ]
    )
    joined = joined.with_columns(
        [
            ((pl.lit(required, dtype=pl.Int64) - pl.col("one_m_count").clip(0, required)) / float(required)).alias(
                "one_m_missing_ratio"
            ),
            (pl.col("one_m_count") < required).alias("one_m_missing"),
            (
                (pl.col("one_m_count") < required)
                & (
                    (pl.lit(required, dtype=pl.Int64) - pl.col("one_m_count").clip(0, required)) / float(required)
                    > float(max_missing_ratio)
                )
                | (pl.col("one_m_last_ts") > pl.col("ts_ms"))
            )
            .fill_null(True)
            .alias("one_m_fail"),
        ]
    )

    rows_missing = int(joined.filter(pl.col("one_m_missing") == True).height)  # noqa: E712
    rows_failed = int(joined.filter(pl.col("one_m_fail") == True).height)  # noqa: E712
    return joined, OneMJoinStats(
        rows_total=rows_total,
        rows_missing=rows_missing,
        rows_failed=rows_failed,
        required_bars=required,
        max_missing_ratio=float(max_missing_ratio),
    )


def _ensure_high_tf_default_columns(frame: pl.DataFrame, *, tf: str) -> pl.DataFrame:
    prefix = high_tf_prefix(tf)
    defaults: list[tuple[str, pl.Expr]] = [
        (f"{prefix}_ret_1", pl.lit(None, dtype=pl.Float64)),
        (f"{prefix}_ret_3", pl.lit(None, dtype=pl.Float64)),
        (f"{prefix}_vol_3", pl.lit(None, dtype=pl.Float64)),
        (f"{prefix}_trend_slope", pl.lit(None, dtype=pl.Float64)),
        (f"{prefix}_regime_flag", pl.lit(None, dtype=pl.Int8)),
    ]
    out = frame
    for name, expr in defaults:
        if name in out.columns:
            continue
        out = out.with_columns(expr.alias(name))
    return out


def _float_dtype(value: str) -> pl.DataType:
    if str(value).strip().lower() == "float64":
        return pl.Float64
    return pl.Float32
