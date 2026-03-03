"""Feature engineering for OHLCV-based feature set v1."""

from __future__ import annotations

import polars as pl

from autobot.data import expected_interval_ms

from .feature_spec import FeatureSetV1Config, factor_prefix, feature_columns


REQUIRED_CANDLE_COLUMNS = ("ts_ms", "open", "high", "low", "close", "volume_base")


def compute_feature_set_v1(
    frame: pl.DataFrame,
    *,
    tf: str,
    config: FeatureSetV1Config,
    float_dtype: str = "float32",
) -> pl.DataFrame:
    """Compute trailing-only OHLCV features for v1."""

    _validate_input_columns(frame)
    dtype = _float_dtype(float_dtype)
    working = _ensure_optional_columns(frame).sort("ts_ms")

    ret_exprs = [(pl.col("close").log() - pl.col("close").shift(win).log()).alias(f"log_ret_{win}") for win in config.windows.ret]
    vol_log_expr = pl.col("volume_base").log1p().alias("vol_log")
    working = working.with_columns(ret_exprs + [vol_log_expr])

    rv_exprs = [
        pl.col("log_ret_1").rolling_std(window_size=win, min_samples=win).alias(f"rv_{win}")
        for win in config.windows.rv
    ]
    ema_exprs = [
        pl.col("close").ewm_mean(alpha=(2.0 / (win + 1.0)), adjust=False, min_samples=win).alias(f"ema_{win}")
        for win in sorted(config.windows.ema)
    ]

    delta = pl.col("close").diff()
    gain = pl.when(delta > 0.0).then(delta).otherwise(0.0)
    loss = pl.when(delta < 0.0).then(-delta).otherwise(0.0)
    avg_gain = gain.ewm_mean(alpha=(1.0 / config.windows.rsi), adjust=False, min_samples=config.windows.rsi)
    avg_loss = loss.ewm_mean(alpha=(1.0 / config.windows.rsi), adjust=False, min_samples=config.windows.rsi)
    rs = avg_gain / avg_loss
    rsi_expr = (
        pl.when(avg_loss == 0.0).then(100.0).otherwise(100.0 - (100.0 / (1.0 + rs))).alias(f"rsi_{config.windows.rsi}")
    )

    prev_close = pl.col("close").shift(1)
    true_range = pl.max_horizontal(
        [
            pl.col("high") - pl.col("low"),
            (pl.col("high") - prev_close).abs(),
            (pl.col("low") - prev_close).abs(),
        ]
    )
    atr_expr = true_range.rolling_mean(window_size=config.windows.atr, min_samples=config.windows.atr).alias(
        f"atr_{config.windows.atr}"
    )

    vol_mean = pl.col("vol_log").rolling_mean(window_size=config.windows.vol_z, min_samples=config.windows.vol_z)
    vol_std = pl.col("vol_log").rolling_std(window_size=config.windows.vol_z, min_samples=config.windows.vol_z)
    vol_z_expr = ((pl.col("vol_log") - vol_mean) / vol_std).alias(f"vol_z_{config.windows.vol_z}")

    interval_ms = expected_interval_ms(tf)
    required_null = pl.any_horizontal(
        [
            pl.col("open").is_null(),
            pl.col("high").is_null(),
            pl.col("low").is_null(),
            pl.col("close").is_null(),
            pl.col("volume_base").is_null(),
        ]
    )
    ohlc_violation = (pl.col("high") < pl.max_horizontal("open", "close", "low")) | (
        pl.col("low") > pl.min_horizontal("open", "close", "high")
    )

    working = working.with_columns(
        rv_exprs
        + ema_exprs
        + [
            rsi_expr,
            atr_expr,
            (pl.col("high") / pl.col("low") - 1.0).alias("hl_pct"),
            vol_z_expr,
            (pl.col("ts_ms").diff() > interval_ms).fill_null(False).alias("is_gap"),
            (~required_null & ~ohlc_violation).fill_null(False).alias("candle_ok"),
        ]
    )

    ema_short = min(config.windows.ema)
    ema_long = max(config.windows.ema)
    working = working.with_columns(
        (pl.col(f"ema_{ema_short}") / pl.col(f"ema_{ema_long}") - 1.0).alias("ema_ratio")
    )

    output_columns = ["ts_ms", "open", "high", "low", "close", "volume_base", "volume_quote", "volume_quote_est"]
    output_columns.extend([col for col in feature_columns(config) if col in working.columns])
    working = working.select(output_columns)

    cast_exprs: list[pl.Expr] = []
    for name, col_dtype in working.schema.items():
        if name == "ts_ms":
            cast_exprs.append(pl.col(name).cast(pl.Int64).alias(name))
        elif name in {"is_gap", "candle_ok", "volume_quote_est"}:
            cast_exprs.append(pl.col(name).cast(pl.Boolean).alias(name))
        elif col_dtype in {pl.Float64, pl.Float32}:
            cast_exprs.append(pl.col(name).cast(dtype).alias(name))
        else:
            cast_exprs.append(pl.col(name).alias(name))
    return working.with_columns(cast_exprs).sort("ts_ms")


def compute_factor_feature_frame(
    frame: pl.DataFrame,
    *,
    market: str,
    rv_window: int,
    float_dtype: str = "float32",
) -> pl.DataFrame:
    """Build factor feature columns (return/volatility) from factor market candles."""

    dtype = _float_dtype(float_dtype)
    prefix = factor_prefix(market)
    if frame.height <= 0:
        return pl.DataFrame(
            schema={
                "ts_ms": pl.Int64,
                f"{prefix}_log_ret_1": dtype,
                f"{prefix}_rv_{rv_window}": dtype,
            }
        )

    working = frame.sort("ts_ms").select(["ts_ms", "close"]).with_columns(
        (pl.col("close").log() - pl.col("close").shift(1).log()).alias("log_ret_1")
    )
    working = working.with_columns(
        pl.col("log_ret_1").rolling_std(window_size=rv_window, min_samples=rv_window).alias(f"rv_{rv_window}")
    )
    return working.select(
        [
            pl.col("ts_ms").cast(pl.Int64),
            pl.col("log_ret_1").cast(dtype).alias(f"{prefix}_log_ret_1"),
            pl.col(f"rv_{rv_window}").cast(dtype).alias(f"{prefix}_rv_{rv_window}"),
        ]
    )


def add_liquidity_roll_features(
    frame: pl.DataFrame,
    *,
    window_bars: int,
    float_dtype: str = "float32",
) -> pl.DataFrame:
    """Add trailing liquidity features used for optional cross-sectional ranking."""

    dtype = _float_dtype(float_dtype)
    return frame.with_columns(
        [
            (pl.col("close") * pl.col("volume_base")).cast(dtype).alias("vol_quote_est"),
            (pl.col("close") * pl.col("volume_base"))
            .rolling_sum(window_size=window_bars, min_samples=window_bars)
            .cast(dtype)
            .alias("vol_quote_24h_roll"),
        ]
    )


def _validate_input_columns(frame: pl.DataFrame) -> None:
    missing = [name for name in REQUIRED_CANDLE_COLUMNS if name not in frame.columns]
    if missing:
        raise ValueError(f"missing required candle columns: {', '.join(missing)}")


def _ensure_optional_columns(frame: pl.DataFrame) -> pl.DataFrame:
    working = frame
    if "volume_quote" not in working.columns:
        working = working.with_columns(pl.lit(None, dtype=pl.Float64).alias("volume_quote"))
    if "volume_quote_est" not in working.columns:
        working = working.with_columns(pl.lit(False, dtype=pl.Boolean).alias("volume_quote_est"))
    return working


def _float_dtype(value: str) -> pl.DataType:
    if str(value).strip().lower() == "float64":
        return pl.Float64
    return pl.Float32
