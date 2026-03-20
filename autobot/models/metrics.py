"""Classification and trading-friendly metric helpers."""

from __future__ import annotations

from typing import Any

import numpy as np
from sklearn.metrics import average_precision_score, brier_score_loss, log_loss, roc_auc_score


def classification_metrics(
    y_true: np.ndarray,
    scores: np.ndarray,
    *,
    sample_weight: np.ndarray | None = None,
) -> dict[str, float | None]:
    y = np.asarray(y_true, dtype=np.int8)
    proba = np.asarray(scores, dtype=np.float64)
    clipped = np.clip(proba, 1e-7, 1.0 - 1e-7)
    weight = _resolve_sample_weight(sample_weight=sample_weight, size=y.size)

    roc_auc: float | None
    pr_auc: float | None
    try:
        roc_auc = float(roc_auc_score(y, clipped, sample_weight=weight))
    except ValueError:
        roc_auc = None
    try:
        pr_auc = float(average_precision_score(y, clipped, sample_weight=weight))
    except ValueError:
        pr_auc = None

    result: dict[str, float | None] = {
        "roc_auc": roc_auc,
        "pr_auc": pr_auc,
        "log_loss": float(log_loss(y, clipped, labels=[0, 1], sample_weight=weight)),
        "brier_score": float(brier_score_loss(y, clipped, sample_weight=weight)),
        "positive_rate": _weighted_boolean_mean(y == 1, weight),
    }
    return result


def precision_at_top_p(
    y_true: np.ndarray,
    scores: np.ndarray,
    top_p: float,
    *,
    sample_weight: np.ndarray | None = None,
) -> float:
    y = np.asarray(y_true, dtype=np.int8)
    proba = np.asarray(scores, dtype=np.float64)
    if y.size == 0:
        return 0.0
    idx = top_k_indices(proba, top_p=top_p)
    if idx.size == 0:
        return 0.0
    weight = _resolve_sample_weight(sample_weight=sample_weight, size=y.size)
    return _weighted_boolean_mean(y[idx] == 1, weight[idx])


def recall_at_top_p(
    y_true: np.ndarray,
    scores: np.ndarray,
    top_p: float,
    *,
    sample_weight: np.ndarray | None = None,
) -> float:
    y = np.asarray(y_true, dtype=np.int8)
    weight = _resolve_sample_weight(sample_weight=sample_weight, size=y.size)
    positives = float(np.sum(weight[y == 1]))
    if positives <= 0.0:
        return 0.0
    idx = top_k_indices(np.asarray(scores, dtype=np.float64), top_p=top_p)
    if idx.size == 0:
        return 0.0
    selected_positives = float(np.sum(weight[idx][y[idx] == 1]))
    return float(selected_positives) / float(positives)


def top_p_threshold(scores: np.ndarray, top_p: float) -> float:
    proba = np.asarray(scores, dtype=np.float64)
    if proba.size <= 0:
        return 1.0
    idx = top_k_indices(proba, top_p=top_p)
    if idx.size <= 0:
        return 1.0
    return float(np.min(proba[idx]))


def trading_metrics(
    y_true: np.ndarray,
    y_reg: np.ndarray,
    scores: np.ndarray,
    *,
    top_ps: tuple[float, ...] = (0.01, 0.05, 0.10),
    fee_bps_est: float = 10.0,
    safety_bps: float = 5.0,
    sample_weight: np.ndarray | None = None,
) -> dict[str, Any]:
    y = np.asarray(y_true, dtype=np.int8)
    reg = np.asarray(y_reg, dtype=np.float64)
    proba = np.asarray(scores, dtype=np.float64)
    weight = _resolve_sample_weight(sample_weight=sample_weight, size=y.size)
    fee_frac = float(fee_bps_est + safety_bps) / 10_000.0

    payload: dict[str, Any] = {}
    for top_p in top_ps:
        idx = top_k_indices(proba, top_p=float(top_p))
        label = _top_p_label(top_p)
        if idx.size <= 0:
            payload[label] = {
                "top_p": float(top_p),
                "threshold": 1.0,
                "selected_rows": 0,
                "precision": 0.0,
                "recall": 0.0,
                "mean_y_reg": 0.0,
                "ev_net": -fee_frac,
            }
            continue

        mean_y_reg = _weighted_nanmean(reg[idx], weight[idx]) if reg.size > 0 else 0.0
        payload[label] = {
            "top_p": float(top_p),
            "threshold": float(np.min(proba[idx])),
            "selected_rows": int(idx.size),
            "precision": _weighted_boolean_mean(y[idx] == 1, weight[idx]),
            "recall": recall_at_top_p(y, proba, float(top_p), sample_weight=weight),
            "mean_y_reg": mean_y_reg,
            "ev_net": mean_y_reg - fee_frac,
        }
    return payload


def ev_optimal_threshold(
    y_reg: np.ndarray,
    scores: np.ndarray,
    *,
    fee_bps_est: float = 10.0,
    safety_bps: float = 5.0,
    scan_steps: int = 200,
    min_selected: int = 100,
    sample_weight: np.ndarray | None = None,
) -> dict[str, float | int]:
    reg = np.asarray(y_reg, dtype=np.float64)
    proba = np.asarray(scores, dtype=np.float64)
    if reg.size <= 0 or proba.size <= 0 or reg.size != proba.size:
        return {"threshold": 1.0, "selected_rows": 0, "mean_y_reg": 0.0, "ev_net": 0.0}

    weight = _resolve_sample_weight(sample_weight=sample_weight, size=reg.size)
    fee_frac = float(fee_bps_est + safety_bps) / 10_000.0
    quantiles = np.linspace(0.50, 0.999, max(int(scan_steps), 10))

    best_threshold = 1.0
    best_selected = 0
    best_mean = 0.0
    best_ev = -1e18
    for quantile in quantiles:
        threshold = float(np.quantile(proba, quantile))
        selected = proba >= threshold
        count = int(np.sum(selected))
        if count < max(int(min_selected), 1):
            continue
        mean_y_reg = _weighted_nanmean(reg[selected], weight[selected])
        ev = mean_y_reg - fee_frac
        if ev > best_ev:
            best_ev = ev
            best_threshold = threshold
            best_selected = count
            best_mean = mean_y_reg

    if best_ev <= -1e17:
        return {"threshold": 1.0, "selected_rows": 0, "mean_y_reg": 0.0, "ev_net": -fee_frac}

    return {
        "threshold": float(best_threshold),
        "selected_rows": int(best_selected),
        "mean_y_reg": float(best_mean),
        "ev_net": float(best_ev),
    }


def grouped_trading_metrics(
    *,
    markets: np.ndarray,
    y_true: np.ndarray,
    y_reg: np.ndarray,
    scores: np.ndarray,
    top_ps: tuple[float, ...] = (0.01, 0.05, 0.10),
    fee_bps_est: float = 10.0,
    safety_bps: float = 5.0,
    sample_weight: np.ndarray | None = None,
) -> dict[str, Any]:
    market_values = np.asarray(markets, dtype=object)
    weight = _resolve_sample_weight(sample_weight=sample_weight, size=market_values.size)
    unique_markets = sorted({str(value) for value in market_values.tolist() if str(value)})
    per_market: dict[str, Any] = {}
    for market in unique_markets:
        mask = market_values == market
        if not np.any(mask):
            continue
        cls = classification_metrics(y_true[mask], scores[mask], sample_weight=weight[mask])
        trade = trading_metrics(
            y_true[mask],
            y_reg[mask],
            scores[mask],
            top_ps=top_ps,
            fee_bps_est=fee_bps_est,
            safety_bps=safety_bps,
            sample_weight=weight[mask],
        )
        per_market[market] = {
            "rows": int(np.sum(mask)),
            "classification": cls,
            "trading": trade,
        }
    return per_market


def summarize_grouped_top5(per_market: dict[str, Any]) -> dict[str, Any]:
    if not per_market:
        return {
            "market_count": 0,
            "precision_top5_mean": 0.0,
            "precision_top5_std": 0.0,
            "ev_net_top5_mean": 0.0,
            "ev_net_top5_std": 0.0,
            "positive_markets": 0,
            "negative_markets": 0,
            "max_precision_market": None,
            "min_precision_market": None,
        }

    precisions: list[tuple[str, float]] = []
    evs: list[tuple[str, float]] = []
    for market, row in per_market.items():
        trading_row = row.get("trading", {}) if isinstance(row, dict) else {}
        top5 = trading_row.get("top_5pct", {}) if isinstance(trading_row, dict) else {}
        precision = float(top5.get("precision", 0.0))
        ev_net = float(top5.get("ev_net", 0.0))
        precisions.append((market, precision))
        evs.append((market, ev_net))

    precision_values = np.array([item[1] for item in precisions], dtype=np.float64)
    ev_values = np.array([item[1] for item in evs], dtype=np.float64)
    max_precision_market = sorted(precisions, key=lambda item: (-item[1], item[0]))[0][0]
    min_precision_market = sorted(precisions, key=lambda item: (item[1], item[0]))[0][0]
    positive_markets = int(np.sum(ev_values > 0.0))

    return {
        "market_count": len(precisions),
        "precision_top5_mean": float(np.mean(precision_values)),
        "precision_top5_std": float(np.std(precision_values)),
        "ev_net_top5_mean": float(np.mean(ev_values)),
        "ev_net_top5_std": float(np.std(ev_values)),
        "positive_markets": positive_markets,
        "negative_markets": len(precisions) - positive_markets,
        "max_precision_market": max_precision_market,
        "min_precision_market": min_precision_market,
    }


def top_k_indices(scores: np.ndarray, *, top_p: float) -> np.ndarray:
    proba = np.asarray(scores, dtype=np.float64)
    if proba.size <= 0:
        return np.array([], dtype=np.int64)
    fraction = min(max(float(top_p), 0.0), 1.0)
    k = max(int(np.ceil(proba.size * fraction)), 1)
    if k >= proba.size:
        return np.arange(proba.size, dtype=np.int64)
    selected = np.argpartition(proba, -k)[-k:]
    return selected.astype(np.int64, copy=False)


def _top_p_label(value: float) -> str:
    pct = int(round(float(value) * 100))
    return f"top_{pct}pct"


def _resolve_sample_weight(*, sample_weight: np.ndarray | None, size: int) -> np.ndarray:
    if sample_weight is None:
        return np.ones(int(size), dtype=np.float64)
    weight = np.asarray(sample_weight, dtype=np.float64)
    if weight.size != int(size):
        return np.ones(int(size), dtype=np.float64)
    weight = np.where(np.isfinite(weight), weight, 0.0)
    weight = np.clip(weight, 0.0, None)
    if np.sum(weight) <= 0.0:
        return np.ones(int(size), dtype=np.float64)
    return weight


def _weighted_boolean_mean(mask: np.ndarray, weight: np.ndarray) -> float:
    mask_value = np.asarray(mask, dtype=np.float64)
    weight_value = np.asarray(weight, dtype=np.float64)
    total_weight = float(np.sum(weight_value))
    if total_weight <= 0.0:
        return 0.0
    return float(np.sum(mask_value * weight_value) / total_weight)


def _weighted_nanmean(values: np.ndarray, weight: np.ndarray) -> float:
    value_arr = np.asarray(values, dtype=np.float64)
    weight_arr = np.asarray(weight, dtype=np.float64)
    finite_mask = np.isfinite(value_arr) & np.isfinite(weight_arr) & (weight_arr > 0.0)
    if not np.any(finite_mask):
        return 0.0
    clipped_weight = np.clip(weight_arr[finite_mask], 0.0, None)
    total_weight = float(np.sum(clipped_weight))
    if total_weight <= 0.0:
        return 0.0
    return float(np.sum(value_arr[finite_mask] * clipped_weight) / total_weight)
