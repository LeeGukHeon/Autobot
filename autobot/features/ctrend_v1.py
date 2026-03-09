"""Auditable CTREND v1 input-panel contract derived from the crypto trend-factor literature."""

from __future__ import annotations

from typing import Any

import polars as pl


def ctrend_v1_feature_columns() -> tuple[str, ...]:
    return (
        "ctrend_v1_rsi_14",
        "ctrend_v1_stochrsi_14",
        "ctrend_v1_stoch_k_14_3",
        "ctrend_v1_stoch_d_14_3_3",
        "ctrend_v1_cci_20",
        "ctrend_v1_ma_gap_3",
        "ctrend_v1_ma_gap_5",
        "ctrend_v1_ma_gap_10",
        "ctrend_v1_ma_gap_20",
        "ctrend_v1_ma_gap_50",
        "ctrend_v1_ma_gap_100",
        "ctrend_v1_ma_gap_200",
        "ctrend_v1_macd_line_12_26",
        "ctrend_v1_macd_hist_12_26_9",
        "ctrend_v1_vol_ma_gap_3",
        "ctrend_v1_vol_ma_gap_5",
        "ctrend_v1_vol_ma_gap_10",
        "ctrend_v1_vol_ma_gap_20",
        "ctrend_v1_vol_ma_gap_50",
        "ctrend_v1_vol_ma_gap_100",
        "ctrend_v1_vol_ma_gap_200",
        "ctrend_v1_vol_macd_line_12_26",
        "ctrend_v1_vol_macd_hist_12_26_9",
        "ctrend_v1_chaikin_mf_20",
        "ctrend_v1_boll_low_gap_20_2",
        "ctrend_v1_boll_mid_gap_20",
        "ctrend_v1_boll_high_gap_20_2",
        "ctrend_v1_boll_width_20_2",
    )


def ctrend_v1_history_lookback_days() -> int:
    return 240


def ctrend_v1_factor_contract() -> dict[str, Any]:
    return {
        "version": "ctrend_v1",
        "status": "full_input_panel_deployment_adaptation",
        "source_paper": {
            "title": "A Trend Factor for the Cross Section of Cryptocurrency Returns",
            "publisher": "Journal of Financial and Quantitative Analysis",
            "url": "https://www.cambridge.org/core/journals/journal-of-financial-and-quantitative-analysis/article/trend-factor-for-the-cross-section-of-cryptocurrency-returns/4C1509ACBA33D5DCAF0AC24379148178",
        },
        "paper_original_definition": {
            "input_frequency": "daily",
            "portfolio_rebalance_frequency": "weekly",
            "input_panel_size": 28,
            "input_panel_description": "daily technical-indicator panel over prices and volume",
            "aggregation_model": "cross-sectional combined elastic net (CS-C-ENet)",
            "note": (
                "The published factor is a learned weekly cross-sectional trend score built from the full daily "
                "technical-indicator panel. This deployment contract exposes the full indicator input panel and lets "
                "the existing v4 trainer learn the aggregation."
            ),
        },
        "deployment_adaptation": {
            "mode": "full_input_panel_without_sidecar_enet_model",
            "bar_frequency": "training_tf_with_daily_broadcast",
            "daily_rollup_timezone": "UTC",
            "lookahead_guard": "Daily indicators are computed on completed UTC days and shifted by one day before joining to intraday rows.",
            "aggregation_owner": "downstream_v4_trainer",
            "hardcoded_factor_weights": False,
            "history_source_policy": [
                "prefer primary base-candles dataset for recent rows",
                "backfill older warmup from candles_v1 when available",
            ],
        },
        "indicator_families": {
            "momentum_oscillators": ["RSI(14)", "StochRSI(14)", "Stochastic %K(14,3)", "Stochastic %D(14,3,3)", "CCI(20)"],
            "price_trend": ["MA3", "MA5", "MA10", "MA20", "MA50", "MA100", "MA200", "MACD(12,26)", "MACD hist(12,26,9)"],
            "volume_trend": [
                "VOL_MA3",
                "VOL_MA5",
                "VOL_MA10",
                "VOL_MA20",
                "VOL_MA50",
                "VOL_MA100",
                "VOL_MA200",
                "VOL_MACD(12,26)",
                "VOL_MACD hist(12,26,9)",
                "Chaikin Money Flow(20)",
            ],
            "volatility": ["BOLL_LOW(20,2)", "BOLL_MID(20)", "BOLL_HIGH(20,2)", "BOLL_WIDTH(20,2)"],
        },
        "indicator_definitions": [
            {"feature_name": "ctrend_v1_rsi_14", "formula": "RSI(close, 14)", "family": "momentum_oscillators"},
            {"feature_name": "ctrend_v1_stochrsi_14", "formula": "stochastic(RSI(close,14), 14)", "family": "momentum_oscillators"},
            {"feature_name": "ctrend_v1_stoch_k_14_3", "formula": "SMA(100 * (close-lowest(low,14)) / (highest(high,14)-lowest(low,14)), 3)", "family": "momentum_oscillators"},
            {"feature_name": "ctrend_v1_stoch_d_14_3_3", "formula": "SMA(stoch_k_14_3, 3)", "family": "momentum_oscillators"},
            {"feature_name": "ctrend_v1_cci_20", "formula": "(TP-SMA(TP,20))/(0.015*mean(abs(TP-SMA(TP,20)),20)); TP=(high+low+close)/3", "family": "momentum_oscillators"},
            {"feature_name": "ctrend_v1_ma_gap_3", "formula": "(close / SMA(close,3)) - 1", "family": "price_trend"},
            {"feature_name": "ctrend_v1_ma_gap_5", "formula": "(close / SMA(close,5)) - 1", "family": "price_trend"},
            {"feature_name": "ctrend_v1_ma_gap_10", "formula": "(close / SMA(close,10)) - 1", "family": "price_trend"},
            {"feature_name": "ctrend_v1_ma_gap_20", "formula": "(close / SMA(close,20)) - 1", "family": "price_trend"},
            {"feature_name": "ctrend_v1_ma_gap_50", "formula": "(close / SMA(close,50)) - 1", "family": "price_trend"},
            {"feature_name": "ctrend_v1_ma_gap_100", "formula": "(close / SMA(close,100)) - 1", "family": "price_trend"},
            {"feature_name": "ctrend_v1_ma_gap_200", "formula": "(close / SMA(close,200)) - 1", "family": "price_trend"},
            {"feature_name": "ctrend_v1_macd_line_12_26", "formula": "EMA(close,12)-EMA(close,26)", "family": "price_trend"},
            {"feature_name": "ctrend_v1_macd_hist_12_26_9", "formula": "MACD(close,12,26)-EMA(MACD(close,12,26),9)", "family": "price_trend"},
            {"feature_name": "ctrend_v1_vol_ma_gap_3", "formula": "(volume / SMA(volume,3)) - 1", "family": "volume_trend"},
            {"feature_name": "ctrend_v1_vol_ma_gap_5", "formula": "(volume / SMA(volume,5)) - 1", "family": "volume_trend"},
            {"feature_name": "ctrend_v1_vol_ma_gap_10", "formula": "(volume / SMA(volume,10)) - 1", "family": "volume_trend"},
            {"feature_name": "ctrend_v1_vol_ma_gap_20", "formula": "(volume / SMA(volume,20)) - 1", "family": "volume_trend"},
            {"feature_name": "ctrend_v1_vol_ma_gap_50", "formula": "(volume / SMA(volume,50)) - 1", "family": "volume_trend"},
            {"feature_name": "ctrend_v1_vol_ma_gap_100", "formula": "(volume / SMA(volume,100)) - 1", "family": "volume_trend"},
            {"feature_name": "ctrend_v1_vol_ma_gap_200", "formula": "(volume / SMA(volume,200)) - 1", "family": "volume_trend"},
            {"feature_name": "ctrend_v1_vol_macd_line_12_26", "formula": "EMA(volume,12)-EMA(volume,26)", "family": "volume_trend"},
            {"feature_name": "ctrend_v1_vol_macd_hist_12_26_9", "formula": "MACD(volume,12,26)-EMA(MACD(volume,12,26),9)", "family": "volume_trend"},
            {"feature_name": "ctrend_v1_chaikin_mf_20", "formula": "SUM(MFM*volume,20)/SUM(volume,20)", "family": "volume_trend"},
            {"feature_name": "ctrend_v1_boll_low_gap_20_2", "formula": "(lower_band(close,20,2) / close) - 1", "family": "volatility"},
            {"feature_name": "ctrend_v1_boll_mid_gap_20", "formula": "(middle_band(close,20) / close) - 1", "family": "volatility"},
            {"feature_name": "ctrend_v1_boll_high_gap_20_2", "formula": "(upper_band(close,20,2) / close) - 1", "family": "volatility"},
            {"feature_name": "ctrend_v1_boll_width_20_2", "formula": "(upper_band-lower_band)/middle_band", "family": "volatility"},
        ],
    }


def attach_ctrend_v1_features(
    frame: pl.DataFrame,
    *,
    history_frame: pl.DataFrame | None = None,
    float_dtype: str = "float32",
) -> pl.DataFrame:
    if frame.height <= 0:
        return frame
    _require_columns(frame, ("market", "ts_ms", "open", "high", "low", "close", "volume_base"))
    history = history_frame if history_frame is not None and history_frame.height > 0 else frame
    history = _normalize_history_frame(history=history, target_frame=frame)
    daily_features = build_ctrend_v1_daily_feature_frame(history, float_dtype=float_dtype)
    if daily_features.height <= 0:
        return frame
    target = frame.with_columns(pl.from_epoch(pl.col("ts_ms"), time_unit="ms").dt.date().alias("__ctrend_join_date"))
    joined = target.join(
        daily_features,
        left_on=["market", "__ctrend_join_date"],
        right_on=["market", "__broadcast_date"],
        how="left",
    )
    return joined.drop(["__ctrend_join_date"])


def build_ctrend_v1_daily_feature_frame(history_frame: pl.DataFrame, *, float_dtype: str = "float32") -> pl.DataFrame:
    if history_frame.height <= 0:
        schema = {"market": pl.Utf8, "__broadcast_date": pl.Date}
        for name in ctrend_v1_feature_columns():
            schema[name] = pl.Float32 if str(float_dtype).strip().lower() != "float64" else pl.Float64
        return pl.DataFrame(schema=schema)
    _require_columns(history_frame, ("market", "ts_ms", "open", "high", "low", "close", "volume_base"))
    dtype = pl.Float64 if str(float_dtype).strip().lower() == "float64" else pl.Float32
    history = history_frame.sort(["market", "ts_ms"]).with_columns(
        pl.from_epoch(pl.col("ts_ms"), time_unit="ms").dt.date().alias("__date")
    )
    daily = history.group_by(["market", "__date"]).agg(
        [
            pl.col("open").cast(pl.Float64).first().alias("__open_d"),
            pl.col("high").cast(pl.Float64).max().alias("__high_d"),
            pl.col("low").cast(pl.Float64).min().alias("__low_d"),
            pl.col("close").cast(pl.Float64).last().alias("__close_d"),
            pl.col("volume_base").cast(pl.Float64).sum().alias("__volume_d"),
        ]
    ).sort(["market", "__date"])
    daily = _attach_ctrend_indicators_to_daily(daily)
    daily = daily.with_columns(pl.col("__date").dt.offset_by("1d").alias("__broadcast_date"))

    exprs: list[pl.Expr] = [pl.col("market"), pl.col("__broadcast_date")]
    for name in ctrend_v1_feature_columns():
        exprs.append(pl.col(name).cast(dtype).alias(name))
    return daily.select(exprs).filter(pl.col("__broadcast_date").is_not_null())


def _attach_ctrend_indicators_to_daily(frame: pl.DataFrame) -> pl.DataFrame:
    close = pl.col("__close_d").cast(pl.Float64)
    high = pl.col("__high_d").cast(pl.Float64)
    low = pl.col("__low_d").cast(pl.Float64)
    volume = pl.col("__volume_d").cast(pl.Float64)
    typical_price = ((high + low + close) / 3.0).alias("__tp_d")

    daily = frame.with_columns([typical_price])
    delta = close.diff().over("market")
    gain = pl.when(delta > 0.0).then(delta).otherwise(0.0)
    loss = pl.when(delta < 0.0).then(-delta).otherwise(0.0)
    avg_gain = gain.ewm_mean(alpha=(1.0 / 14.0), adjust=False, min_samples=14).over("market")
    avg_loss = loss.ewm_mean(alpha=(1.0 / 14.0), adjust=False, min_samples=14).over("market")
    rsi = (
        pl.when(avg_loss == 0.0)
        .then(100.0)
        .otherwise(100.0 - (100.0 / (1.0 + (avg_gain / avg_loss))))
        .alias("ctrend_v1_rsi_14")
    )
    daily = daily.with_columns([rsi])

    daily = daily.with_columns(
        [
            pl.col("ctrend_v1_rsi_14").rolling_min(window_size=14, min_samples=14).over("market").alias("__rsi_min_14"),
            pl.col("ctrend_v1_rsi_14").rolling_max(window_size=14, min_samples=14).over("market").alias("__rsi_max_14"),
            low.rolling_min(window_size=14, min_samples=14).over("market").alias("__low_14"),
            high.rolling_max(window_size=14, min_samples=14).over("market").alias("__high_14"),
            pl.col("__tp_d").rolling_mean(window_size=20, min_samples=20).over("market").alias("__tp_sma_20"),
            close.rolling_std(window_size=20, min_samples=20).over("market").alias("__close_std_20"),
        ]
    )
    daily = daily.with_columns(
        [
            pl.when((pl.col("__rsi_max_14") - pl.col("__rsi_min_14")).abs() > 1e-12)
            .then((pl.col("ctrend_v1_rsi_14") - pl.col("__rsi_min_14")) / (pl.col("__rsi_max_14") - pl.col("__rsi_min_14")))
            .otherwise(None)
            .alias("ctrend_v1_stochrsi_14"),
            pl.when((pl.col("__high_14") - pl.col("__low_14")).abs() > 1e-12)
            .then(100.0 * (close - pl.col("__low_14")) / (pl.col("__high_14") - pl.col("__low_14")))
            .otherwise(None)
            .alias("__stoch_raw_14"),
            (pl.col("__tp_d") - pl.col("__tp_sma_20")).abs().alias("__tp_abs_dev_20"),
        ]
    )
    daily = daily.with_columns(
        [
            pl.col("__stoch_raw_14").rolling_mean(window_size=3, min_samples=3).over("market").alias("ctrend_v1_stoch_k_14_3"),
            pl.col("__tp_abs_dev_20").rolling_mean(window_size=20, min_samples=20).over("market").alias("__tp_mad_20"),
            close.rolling_mean(window_size=3, min_samples=3).over("market").alias("__close_sma_3"),
            close.rolling_mean(window_size=5, min_samples=5).over("market").alias("__close_sma_5"),
            close.rolling_mean(window_size=10, min_samples=10).over("market").alias("__close_sma_10"),
            close.rolling_mean(window_size=20, min_samples=20).over("market").alias("__close_sma_20"),
            close.rolling_mean(window_size=50, min_samples=50).over("market").alias("__close_sma_50"),
            close.rolling_mean(window_size=100, min_samples=100).over("market").alias("__close_sma_100"),
            close.rolling_mean(window_size=200, min_samples=200).over("market").alias("__close_sma_200"),
            volume.rolling_mean(window_size=3, min_samples=3).over("market").alias("__volume_sma_3"),
            volume.rolling_mean(window_size=5, min_samples=5).over("market").alias("__volume_sma_5"),
            volume.rolling_mean(window_size=10, min_samples=10).over("market").alias("__volume_sma_10"),
            volume.rolling_mean(window_size=20, min_samples=20).over("market").alias("__volume_sma_20"),
            volume.rolling_mean(window_size=50, min_samples=50).over("market").alias("__volume_sma_50"),
            volume.rolling_mean(window_size=100, min_samples=100).over("market").alias("__volume_sma_100"),
            volume.rolling_mean(window_size=200, min_samples=200).over("market").alias("__volume_sma_200"),
            close.ewm_mean(alpha=(2.0 / 13.0), adjust=False, min_samples=12).over("market").alias("__ema_close_12"),
            close.ewm_mean(alpha=(2.0 / 27.0), adjust=False, min_samples=26).over("market").alias("__ema_close_26"),
            volume.ewm_mean(alpha=(2.0 / 13.0), adjust=False, min_samples=12).over("market").alias("__ema_volume_12"),
            volume.ewm_mean(alpha=(2.0 / 27.0), adjust=False, min_samples=26).over("market").alias("__ema_volume_26"),
        ]
    )
    daily = daily.with_columns(
        [
            pl.col("ctrend_v1_stoch_k_14_3").rolling_mean(window_size=3, min_samples=3).over("market").alias("ctrend_v1_stoch_d_14_3_3"),
            pl.when(pl.col("__tp_mad_20").abs() > 1e-12)
            .then((pl.col("__tp_d") - pl.col("__tp_sma_20")) / (0.015 * pl.col("__tp_mad_20")))
            .otherwise(None)
            .alias("ctrend_v1_cci_20"),
            (pl.col("__ema_close_12") - pl.col("__ema_close_26")).alias("ctrend_v1_macd_line_12_26"),
            (pl.col("__ema_volume_12") - pl.col("__ema_volume_26")).alias("ctrend_v1_vol_macd_line_12_26"),
            (pl.col("__close_sma_20") - (2.0 * pl.col("__close_std_20"))).alias("__boll_low_20_2"),
            (pl.col("__close_sma_20") + (2.0 * pl.col("__close_std_20"))).alias("__boll_high_20_2"),
        ]
    )
    daily = daily.with_columns(
        [
            pl.col("ctrend_v1_macd_line_12_26")
            .ewm_mean(alpha=(2.0 / 10.0), adjust=False, min_samples=9)
            .over("market")
            .alias("__macd_signal_12_26_9"),
            pl.col("ctrend_v1_vol_macd_line_12_26")
            .ewm_mean(alpha=(2.0 / 10.0), adjust=False, min_samples=9)
            .over("market")
            .alias("__vol_macd_signal_12_26_9"),
            pl.when((high - low).abs() > 1e-12)
            .then((((close - low) - (high - close)) / (high - low)) * volume)
            .otherwise(0.0)
            .alias("__money_flow_volume"),
        ]
    )
    daily = daily.with_columns(
        [
            _ratio_gap_expr(close, pl.col("__close_sma_3")).alias("ctrend_v1_ma_gap_3"),
            _ratio_gap_expr(close, pl.col("__close_sma_5")).alias("ctrend_v1_ma_gap_5"),
            _ratio_gap_expr(close, pl.col("__close_sma_10")).alias("ctrend_v1_ma_gap_10"),
            _ratio_gap_expr(close, pl.col("__close_sma_20")).alias("ctrend_v1_ma_gap_20"),
            _ratio_gap_expr(close, pl.col("__close_sma_50")).alias("ctrend_v1_ma_gap_50"),
            _ratio_gap_expr(close, pl.col("__close_sma_100")).alias("ctrend_v1_ma_gap_100"),
            _ratio_gap_expr(close, pl.col("__close_sma_200")).alias("ctrend_v1_ma_gap_200"),
            _ratio_gap_expr(volume, pl.col("__volume_sma_3")).alias("ctrend_v1_vol_ma_gap_3"),
            _ratio_gap_expr(volume, pl.col("__volume_sma_5")).alias("ctrend_v1_vol_ma_gap_5"),
            _ratio_gap_expr(volume, pl.col("__volume_sma_10")).alias("ctrend_v1_vol_ma_gap_10"),
            _ratio_gap_expr(volume, pl.col("__volume_sma_20")).alias("ctrend_v1_vol_ma_gap_20"),
            _ratio_gap_expr(volume, pl.col("__volume_sma_50")).alias("ctrend_v1_vol_ma_gap_50"),
            _ratio_gap_expr(volume, pl.col("__volume_sma_100")).alias("ctrend_v1_vol_ma_gap_100"),
            _ratio_gap_expr(volume, pl.col("__volume_sma_200")).alias("ctrend_v1_vol_ma_gap_200"),
            (pl.col("ctrend_v1_macd_line_12_26") - pl.col("__macd_signal_12_26_9")).alias("ctrend_v1_macd_hist_12_26_9"),
            (pl.col("ctrend_v1_vol_macd_line_12_26") - pl.col("__vol_macd_signal_12_26_9")).alias("ctrend_v1_vol_macd_hist_12_26_9"),
            pl.when(volume.rolling_sum(window_size=20, min_samples=20).over("market").abs() > 1e-12)
            .then(
                pl.col("__money_flow_volume").rolling_sum(window_size=20, min_samples=20).over("market")
                / volume.rolling_sum(window_size=20, min_samples=20).over("market")
            )
            .otherwise(None)
            .alias("ctrend_v1_chaikin_mf_20"),
            _ratio_gap_expr(pl.col("__boll_low_20_2"), close).alias("ctrend_v1_boll_low_gap_20_2"),
            _ratio_gap_expr(pl.col("__close_sma_20"), close).alias("ctrend_v1_boll_mid_gap_20"),
            _ratio_gap_expr(pl.col("__boll_high_20_2"), close).alias("ctrend_v1_boll_high_gap_20_2"),
            pl.when(pl.col("__close_sma_20").abs() > 1e-12)
            .then((pl.col("__boll_high_20_2") - pl.col("__boll_low_20_2")) / pl.col("__close_sma_20"))
            .otherwise(None)
            .alias("ctrend_v1_boll_width_20_2"),
        ]
    )
    return daily


def _normalize_history_frame(*, history: pl.DataFrame, target_frame: pl.DataFrame) -> pl.DataFrame:
    if "market" in history.columns:
        return history
    unique_markets = target_frame.get_column("market").unique().drop_nulls().to_list() if "market" in target_frame.columns else []
    if len(unique_markets) != 1:
        raise ValueError("ctrend_v1 history_frame without market column requires a single-market target frame")
    return history.with_columns(pl.lit(str(unique_markets[0]), dtype=pl.Utf8).alias("market"))


def _require_columns(frame: pl.DataFrame, names: tuple[str, ...]) -> None:
    missing = [name for name in names if name not in frame.columns]
    if missing:
        raise ValueError(f"ctrend_v1 requires columns: {missing}")


def _ratio_gap_expr(numerator: pl.Expr, denominator: pl.Expr) -> pl.Expr:
    return pl.when(denominator.abs() > 1e-12).then((numerator / denominator) - 1.0).otherwise(None)
