from __future__ import annotations

from typing import Any, Iterable

import numpy as np


DEFAULT_SELECTION_CALIBRATION_MODE = "identity_v1"
ISOTONIC_SELECTION_CALIBRATION_MODE = "isotonic_oos_v1"
DEFAULT_SELECTION_CALIBRATION_VERSION = 1


def build_selection_calibration_from_oos_rows(
    *,
    oos_rows: Iterable[dict[str, Any]] | None,
    min_rows: int = 200,
    min_positive_rows: int = 20,
    min_negative_rows: int = 20,
) -> dict[str, Any]:
    score_parts: list[np.ndarray] = []
    label_parts: list[np.ndarray] = []
    window_count = 0
    for row in oos_rows or ():
        if not isinstance(row, dict):
            continue
        scores = np.asarray(row.get("scores") or [], dtype=np.float64)
        labels = np.asarray(row.get("y_cls") or [], dtype=np.int64)
        if scores.size <= 0 or labels.size <= 0 or scores.size != labels.size:
            continue
        score_parts.append(np.clip(scores, 0.0, 1.0))
        label_parts.append((labels > 0).astype(np.int64, copy=False))
        window_count += 1
    if not score_parts:
        return _identity_calibration(reason="NO_OOS_ROWS")

    score_values = np.concatenate(score_parts, axis=0).astype(np.float64, copy=False)
    label_values = np.concatenate(label_parts, axis=0).astype(np.int64, copy=False)
    positive_rows = int(np.sum(label_values > 0))
    negative_rows = int(label_values.size - positive_rows)
    if score_values.size < int(min_rows):
        return _identity_calibration(
            reason="INSUFFICIENT_ROWS",
            fit_rows=int(score_values.size),
            positive_rows=positive_rows,
            negative_rows=negative_rows,
            windows_covered=window_count,
        )
    if positive_rows < int(min_positive_rows) or negative_rows < int(min_negative_rows):
        return _identity_calibration(
            reason="INSUFFICIENT_CLASS_SUPPORT",
            fit_rows=int(score_values.size),
            positive_rows=positive_rows,
            negative_rows=negative_rows,
            windows_covered=window_count,
        )

    try:
        from sklearn.isotonic import IsotonicRegression
    except Exception:
        return _identity_calibration(
            reason="SKLEARN_ISOTONIC_UNAVAILABLE",
            fit_rows=int(score_values.size),
            positive_rows=positive_rows,
            negative_rows=negative_rows,
            windows_covered=window_count,
        )

    fitter = IsotonicRegression(y_min=0.0, y_max=1.0, out_of_bounds="clip")
    fitter.fit(score_values, label_values)
    x_knots = np.asarray(getattr(fitter, "X_thresholds_", []), dtype=np.float64)
    y_knots = np.asarray(getattr(fitter, "y_thresholds_", []), dtype=np.float64)
    if x_knots.size < 2 or y_knots.size != x_knots.size:
        return _identity_calibration(
            reason="INVALID_ISOTONIC_KNOTS",
            fit_rows=int(score_values.size),
            positive_rows=positive_rows,
            negative_rows=negative_rows,
            windows_covered=window_count,
        )

    calibrated_scores = apply_selection_calibration(
        score_values,
        {
            "mode": ISOTONIC_SELECTION_CALIBRATION_MODE,
            "x_knots": x_knots.tolist(),
            "y_knots": y_knots.tolist(),
        },
    )
    return {
        "version": DEFAULT_SELECTION_CALIBRATION_VERSION,
        "mode": ISOTONIC_SELECTION_CALIBRATION_MODE,
        "comparable": True,
        "reason": "OK",
        "source": "walk_forward_oos",
        "fit_rows": int(score_values.size),
        "positive_rows": positive_rows,
        "negative_rows": negative_rows,
        "windows_covered": int(window_count),
        "class_balance": float(positive_rows / max(int(score_values.size), 1)),
        "score_min": float(np.min(score_values)),
        "score_max": float(np.max(score_values)),
        "calibrated_score_min": float(np.min(calibrated_scores)),
        "calibrated_score_max": float(np.max(calibrated_scores)),
        "metrics": {
            "brier_raw": float(np.mean(np.square(score_values - label_values))),
            "brier_calibrated": float(np.mean(np.square(calibrated_scores - label_values))),
            "log_loss_raw": _binary_log_loss(label_values, score_values),
            "log_loss_calibrated": _binary_log_loss(label_values, calibrated_scores),
        },
        "x_knots": x_knots.tolist(),
        "y_knots": y_knots.tolist(),
    }


def normalize_selection_calibration(calibration: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(calibration or {})
    mode = str(payload.get("mode", "")).strip().lower()
    if mode == ISOTONIC_SELECTION_CALIBRATION_MODE:
        x_knots = np.asarray(payload.get("x_knots") or [], dtype=np.float64)
        y_knots = np.asarray(payload.get("y_knots") or [], dtype=np.float64)
        if x_knots.size >= 2 and x_knots.size == y_knots.size:
            return {
                **payload,
                "version": int(payload.get("version") or DEFAULT_SELECTION_CALIBRATION_VERSION),
                "mode": ISOTONIC_SELECTION_CALIBRATION_MODE,
                "comparable": bool(payload.get("comparable", True)),
                "x_knots": x_knots.tolist(),
                "y_knots": np.clip(y_knots, 0.0, 1.0).tolist(),
            }
    return _identity_calibration(reason=str(payload.get("reason") or "MISSING_SELECTION_CALIBRATION"))


def apply_selection_calibration(scores: np.ndarray, calibration: dict[str, Any] | None) -> np.ndarray:
    score_values = np.clip(np.asarray(scores, dtype=np.float64), 0.0, 1.0)
    payload = normalize_selection_calibration(calibration)
    if str(payload.get("mode", "")).strip().lower() != ISOTONIC_SELECTION_CALIBRATION_MODE:
        return score_values
    x_knots = np.asarray(payload.get("x_knots") or [], dtype=np.float64)
    y_knots = np.asarray(payload.get("y_knots") or [], dtype=np.float64)
    if x_knots.size < 2 or x_knots.size != y_knots.size:
        return score_values
    return np.clip(
        np.interp(score_values, x_knots, y_knots, left=float(y_knots[0]), right=float(y_knots[-1])),
        0.0,
        1.0,
    )


def _identity_calibration(
    *,
    reason: str,
    fit_rows: int = 0,
    positive_rows: int = 0,
    negative_rows: int = 0,
    windows_covered: int = 0,
) -> dict[str, Any]:
    return {
        "version": DEFAULT_SELECTION_CALIBRATION_VERSION,
        "mode": DEFAULT_SELECTION_CALIBRATION_MODE,
        "comparable": False,
        "reason": str(reason).strip().upper() or "IDENTITY",
        "source": "identity",
        "fit_rows": int(fit_rows),
        "positive_rows": int(positive_rows),
        "negative_rows": int(negative_rows),
        "windows_covered": int(windows_covered),
        "class_balance": float(positive_rows / max(int(fit_rows), 1)) if int(fit_rows) > 0 else 0.0,
        "score_min": 0.0,
        "score_max": 1.0,
        "calibrated_score_min": 0.0,
        "calibrated_score_max": 1.0,
        "metrics": {},
        "x_knots": [0.0, 1.0],
        "y_knots": [0.0, 1.0],
    }


def _binary_log_loss(labels: np.ndarray, scores: np.ndarray) -> float:
    y_true = np.asarray(labels, dtype=np.float64)
    y_score = np.clip(np.asarray(scores, dtype=np.float64), 1e-6, 1.0 - 1e-6)
    return float(-np.mean((y_true * np.log(y_score)) + ((1.0 - y_true) * np.log(1.0 - y_score))))
