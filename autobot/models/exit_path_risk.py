"""Pathwise exit-risk summaries built from out-of-sample trade paths."""

from __future__ import annotations

import math
from typing import Any

import numpy as np

from .selection_calibration import apply_selection_calibration


DEFAULT_EXIT_PATH_HORIZONS: tuple[int, ...] = (1, 3, 6, 9, 12)
EXIT_PATH_RISK_POLICY_ID = "pathwise_exit_risk_summary_v1"


def build_exit_path_risk_summary(
    *,
    oos_rows: list[dict[str, Any]] | None,
    selection_calibration: dict[str, Any] | None,
    risk_feature_name: str,
    horizons: tuple[int, ...] = DEFAULT_EXIT_PATH_HORIZONS,
    selection_bucket_count: int = 3,
    risk_bucket_count: int = 3,
    expected_exit_fee_rate: float = 0.0,
    expected_exit_slippage_bps: float = 0.0,
    recommended_hold_bars: int | None = None,
) -> dict[str, Any]:
    resolved_horizons = tuple(sorted({max(int(item), 1) for item in horizons if int(item) > 0}))
    samples = _build_exit_path_samples(
        oos_rows=list(oos_rows or []),
        selection_calibration=selection_calibration,
        risk_feature_name=str(risk_feature_name).strip(),
        horizons=resolved_horizons,
        expected_exit_fee_rate=max(float(expected_exit_fee_rate), 0.0),
        expected_exit_slippage_bps=max(float(expected_exit_slippage_bps), 0.0),
    )
    if not samples:
        return {
            "version": 1,
            "policy": EXIT_PATH_RISK_POLICY_ID,
            "status": "skipped",
            "reason": "INSUFFICIENT_OOS_ROWS",
            "risk_feature_name": str(risk_feature_name).strip(),
            "horizons": list(resolved_horizons),
            "sample_count": 0,
        }

    selection_scores = np.asarray([float(item["selection_score"]) for item in samples], dtype=np.float64)
    risk_values = np.asarray([float(item["risk_value"]) for item in samples], dtype=np.float64)
    selection_bounds = _quantile_bounds(selection_scores, bucket_count=max(int(selection_bucket_count), 1))
    risk_bounds = _quantile_bounds(risk_values, bucket_count=max(int(risk_bucket_count), 1))

    overall_by_horizon = [
        _summarize_sample_group(hold_bars=horizon, samples=[item for item in samples if int(item["hold_bars"]) == int(horizon)])
        for horizon in resolved_horizons
        if any(int(item["hold_bars"]) == int(horizon) for item in samples)
    ]

    grouped: dict[tuple[int, int, int], list[dict[str, Any]]] = {}
    for item in samples:
        edge_bucket = _resolve_bin_index(float(item["selection_score"]), selection_bounds)
        risk_bucket = _resolve_bin_index(float(item["risk_value"]), risk_bounds)
        key = (int(item["hold_bars"]), int(edge_bucket), int(risk_bucket))
        grouped.setdefault(key, []).append(item)

    by_bucket: list[dict[str, Any]] = []
    for (hold_bars, edge_bucket, risk_bucket), grouped_samples in sorted(grouped.items()):
        summary = _summarize_sample_group(hold_bars=hold_bars, samples=grouped_samples)
        summary.update(
            {
                "selection_bucket": int(edge_bucket),
                "risk_bucket": int(risk_bucket),
            }
        )
        by_bucket.append(summary)

    recommended_summary = {}
    if recommended_hold_bars is not None:
        matched = [item for item in overall_by_horizon if int(item["hold_bars"]) == int(recommended_hold_bars)]
        if matched:
            recommended_summary = dict(matched[0])

    return {
        "version": 1,
        "policy": EXIT_PATH_RISK_POLICY_ID,
        "status": "ready",
        "risk_feature_name": str(risk_feature_name).strip(),
        "horizons": list(resolved_horizons),
        "sample_count": int(len(samples)),
        "selection_bucket_bounds": [float(item) for item in selection_bounds.tolist()],
        "risk_bucket_bounds": [float(item) for item in risk_bounds.tolist()],
        "overall_by_horizon": overall_by_horizon,
        "by_bucket": by_bucket,
        "recommended_hold_bars": int(recommended_hold_bars) if recommended_hold_bars is not None else None,
        "recommended_summary": recommended_summary,
        "expected_exit_fee_rate": float(expected_exit_fee_rate),
        "expected_exit_slippage_bps": float(expected_exit_slippage_bps),
    }


def _build_exit_path_samples(
    *,
    oos_rows: list[dict[str, Any]],
    selection_calibration: dict[str, Any] | None,
    risk_feature_name: str,
    horizons: tuple[int, ...],
    expected_exit_fee_rate: float,
    expected_exit_slippage_bps: float,
) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    for window in oos_rows:
        samples.extend(
            _build_window_exit_path_samples(
                window=window,
                selection_calibration=selection_calibration,
                risk_feature_name=risk_feature_name,
                horizons=horizons,
                expected_exit_fee_rate=expected_exit_fee_rate,
                expected_exit_slippage_bps=expected_exit_slippage_bps,
            )
        )
    return samples


def _build_window_exit_path_samples(
    *,
    window: dict[str, Any],
    selection_calibration: dict[str, Any] | None,
    risk_feature_name: str,
    horizons: tuple[int, ...],
    expected_exit_fee_rate: float,
    expected_exit_slippage_bps: float,
) -> list[dict[str, Any]]:
    raw_scores = np.asarray(window.get("raw_scores") or [], dtype=np.float64)
    markets = np.asarray(window.get("markets") or [], dtype=object)
    ts_ms = np.asarray(window.get("ts_ms") or [], dtype=np.int64)
    close = np.asarray(window.get("close") or [], dtype=np.float64)
    rv_12 = np.asarray(window.get("rv_12") or [], dtype=np.float64)
    rv_36 = np.asarray(window.get("rv_36") or [], dtype=np.float64)
    atr_14 = np.asarray(window.get("atr_14") or [], dtype=np.float64)
    atr_pct_14 = np.asarray(window.get("atr_pct_14") or [], dtype=np.float64)
    size = raw_scores.size
    if size <= 0 or any(arr.size != size for arr in (markets, ts_ms, close, rv_12, rv_36, atr_14, atr_pct_14)):
        return []

    selection_scores = apply_selection_calibration(raw_scores, selection_calibration)
    rows: list[dict[str, Any]] = []
    by_market: dict[str, list[int]] = {}
    for index, market in enumerate(markets.tolist()):
        market_value = str(market).strip().upper()
        if market_value:
            by_market.setdefault(market_value, []).append(index)

    for market_value, market_indices in by_market.items():
        ordered = sorted(market_indices, key=lambda idx: int(ts_ms[idx]))
        market_close = close[ordered]
        market_rv_12 = rv_12[ordered]
        market_rv_36 = rv_36[ordered]
        market_atr_14 = atr_14[ordered]
        market_atr_pct_14 = atr_pct_14[ordered]
        market_scores = selection_scores[ordered]
        market_ts_ms = ts_ms[ordered]
        for offset in range(len(ordered)):
            entry_price = _safe_float(market_close[offset])
            if entry_price <= 0.0:
                continue
            row_payload = {
                "close": _safe_float(market_close[offset]),
                "rv_12": _safe_float(market_rv_12[offset]),
                "rv_36": _safe_float(market_rv_36[offset]),
                "atr_14": _safe_float(market_atr_14[offset]),
                "atr_pct_14": _safe_float(market_atr_pct_14[offset]),
            }
            risk_value = _resolve_row_risk_feature_value(row=row_payload, feature_name=risk_feature_name)
            if risk_value is None or not math.isfinite(float(risk_value)):
                continue
            for hold_bars in horizons:
                end_index = int(offset) + int(hold_bars)
                if end_index >= int(market_close.size):
                    continue
                path_prices = market_close[offset + 1 : end_index + 1]
                if path_prices.size <= 0:
                    continue
                path_returns = np.asarray(
                    [
                        _net_return_after_costs(
                            entry_price=float(entry_price),
                            exit_price=float(_safe_float(price)),
                            exit_fee_rate=float(expected_exit_fee_rate),
                            exit_slippage_bps=float(expected_exit_slippage_bps),
                        )
                        for price in path_prices.tolist()
                    ],
                    dtype=np.float64,
                )
                rows.append(
                    {
                        "window_index": int(window.get("window_index", -1)),
                        "market": market_value,
                        "entry_ts_ms": int(market_ts_ms[offset]),
                        "selection_score": float(market_scores[offset]),
                        "risk_value": float(risk_value),
                        "hold_bars": int(hold_bars),
                        "terminal_return": float(path_returns[-1]),
                        "mfe": float(np.max(path_returns)),
                        "mae": float(np.min(path_returns)),
                        "mae_abs": float(max(-float(np.min(path_returns)), 0.0)),
                    }
                )
    return rows


def _summarize_sample_group(*, hold_bars: int, samples: list[dict[str, Any]]) -> dict[str, Any]:
    terminal = np.asarray([float(item["terminal_return"]) for item in samples], dtype=np.float64)
    mfe = np.asarray([float(item["mfe"]) for item in samples], dtype=np.float64)
    mae_abs = np.asarray([float(item["mae_abs"]) for item in samples], dtype=np.float64)
    return {
        "hold_bars": int(hold_bars),
        "sample_count": int(len(samples)),
        "terminal_return_q50": _quantile(terminal, 0.50),
        "terminal_return_q75": _quantile(terminal, 0.75),
        "terminal_return_q90": _quantile(terminal, 0.90),
        "mfe_q50": _quantile(mfe, 0.50),
        "mfe_q75": _quantile(mfe, 0.75),
        "mfe_q90": _quantile(mfe, 0.90),
        "mae_abs_q50": _quantile(mae_abs, 0.50),
        "mae_abs_q75": _quantile(mae_abs, 0.75),
        "mae_abs_q90": _quantile(mae_abs, 0.90),
        "reachable_tp_q60": _quantile(mfe, 0.60),
        "bounded_sl_q80": _quantile(mae_abs, 0.80),
    }


def _quantile(values: np.ndarray, q: float) -> float:
    if values.size <= 0:
        return 0.0
    return float(np.quantile(values, float(q), method="linear"))


def _quantile_bounds(values: np.ndarray, *, bucket_count: int) -> np.ndarray:
    if values.size <= 1 or int(bucket_count) <= 1:
        return np.asarray([], dtype=np.float64)
    quantiles = np.linspace(0.0, 1.0, int(bucket_count) + 1, dtype=np.float64)[1:-1]
    if quantiles.size <= 0:
        return np.asarray([], dtype=np.float64)
    bounds = np.quantile(values, quantiles, method="linear")
    return np.unique(bounds.astype(np.float64))


def _resolve_bin_index(value: float, bounds: np.ndarray) -> int:
    if bounds.size <= 0:
        return 0
    return int(np.searchsorted(bounds, float(value), side="right"))


def _resolve_row_risk_feature_value(*, row: dict[str, Any] | None, feature_name: str) -> float | None:
    if not isinstance(row, dict):
        return None
    feature = str(feature_name).strip()
    if not feature:
        return None
    if feature == "rv_12":
        feature = "rv_12"
    elif feature == "rv_36":
        feature = "rv_36"
    if feature == "atr_pct_14":
        value = _safe_optional_float(row.get("atr_pct_14"))
        if value is not None:
            return max(float(value), 0.0)
        atr = _safe_optional_float(row.get("atr_14"))
        close = _safe_optional_float(row.get("close"))
        if atr is None or close is None or close <= 0.0:
            return None
        return max(float(atr) / float(close), 0.0)
    value = _safe_optional_float(row.get(feature))
    if value is None:
        return None
    return max(float(value), 0.0)


def _net_return_after_costs(
    *,
    entry_price: float,
    exit_price: float,
    exit_fee_rate: float,
    exit_slippage_bps: float,
) -> float:
    entry_value = max(float(entry_price), 1e-12)
    slippage_ratio = max(float(exit_slippage_bps), 0.0) / 10_000.0
    net_exit_price = float(exit_price) * (1.0 - slippage_ratio)
    gross_return = (net_exit_price / entry_value) - 1.0
    return float(gross_return - max(float(exit_fee_rate), 0.0))


def _safe_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
