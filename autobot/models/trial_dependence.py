"""Helpers for estimating dependence-adjusted effective trial counts."""

from __future__ import annotations

import math
from typing import Any, Sequence

import numpy as np


def estimate_effective_trials_from_trial_records(
    trial_records: Sequence[dict[str, Any]] | None,
    *,
    fallback_trial_count: int = 1,
) -> dict[str, Any]:
    raw_fallback = max(int(fallback_trial_count), 1)
    records = [dict(item) for item in (trial_records or []) if isinstance(item, dict)]
    if not records:
        return {
            "raw_trial_count": raw_fallback,
            "effective_trials": raw_fallback,
            "effective_trials_estimate": float(raw_fallback),
            "source": "raw_trial_count_fallback",
            "comparable": False,
            "avg_pairwise_correlation": None,
            "metric_count": 0,
            "reasons": ["MISSING_TRIAL_RECORDS"],
        }

    outcome_matrix, metric_names = build_trial_outcome_matrix(records)
    raw_trial_count = max(int(outcome_matrix.shape[0]), raw_fallback, 1)
    if outcome_matrix.shape[0] < 2:
        return {
            "raw_trial_count": raw_trial_count,
            "effective_trials": raw_trial_count,
            "effective_trials_estimate": float(raw_trial_count),
            "source": "single_trial",
            "comparable": False,
            "avg_pairwise_correlation": None,
            "metric_count": int(outcome_matrix.shape[1]),
            "metric_names": metric_names,
            "reasons": ["INSUFFICIENT_TRIAL_ROWS"],
        }

    if outcome_matrix.shape[1] < 2:
        return {
            "raw_trial_count": raw_trial_count,
            "effective_trials": raw_trial_count,
            "effective_trials_estimate": float(raw_trial_count),
            "source": "raw_trial_count_fallback",
            "comparable": False,
            "avg_pairwise_correlation": None,
            "metric_count": int(outcome_matrix.shape[1]),
            "metric_names": metric_names,
            "reasons": ["INSUFFICIENT_COMMON_OUTCOME_METRICS"],
        }

    similarity = _pairwise_trial_similarity(outcome_matrix)
    if similarity is None:
        return {
            "raw_trial_count": raw_trial_count,
            "effective_trials": raw_trial_count,
            "effective_trials_estimate": float(raw_trial_count),
            "source": "raw_trial_count_fallback",
            "comparable": False,
            "avg_pairwise_correlation": None,
            "metric_count": int(outcome_matrix.shape[1]),
            "metric_names": metric_names,
            "reasons": ["UNSTABLE_TRIAL_SIMILARITY_MATRIX"],
        }

    off_diag = similarity[~np.eye(similarity.shape[0], dtype=bool)]
    avg_corr = float(np.mean(off_diag)) if off_diag.size > 0 else 0.0
    corr_for_effective = min(max(avg_corr, 0.0), 1.0)
    effective_estimate = 1.0 + (float(raw_trial_count - 1) * (1.0 - corr_for_effective))
    effective_estimate = min(max(effective_estimate, 1.0), float(raw_trial_count))
    effective_trials = max(1, min(raw_trial_count, int(round(effective_estimate))))
    return {
        "raw_trial_count": raw_trial_count,
        "effective_trials": effective_trials,
        "effective_trials_estimate": float(effective_estimate),
        "source": "avg_pairwise_outcome_correlation",
        "comparable": True,
        "avg_pairwise_correlation": float(avg_corr),
        "metric_count": int(outcome_matrix.shape[1]),
        "metric_names": metric_names,
        "reasons": [],
    }


def build_trial_outcome_matrix(trial_records: Sequence[dict[str, Any]]) -> tuple[np.ndarray, list[str]]:
    extracted: list[dict[str, float]] = []
    for record in trial_records:
        outcome = _extract_trial_outcome_metrics(record)
        if outcome:
            extracted.append(outcome)
    if not extracted:
        return np.empty((0, 0), dtype=np.float64), []

    common_keys = set(extracted[0].keys())
    for outcome in extracted[1:]:
        common_keys.intersection_update(outcome.keys())
    metric_names = sorted(str(key) for key in common_keys)
    if not metric_names:
        return np.empty((len(extracted), 0), dtype=np.float64), []

    matrix = np.asarray(
        [[float(outcome[name]) for name in metric_names] for outcome in extracted],
        dtype=np.float64,
    )
    if matrix.size <= 0:
        return np.empty((len(extracted), 0), dtype=np.float64), []

    finite_mask = np.all(np.isfinite(matrix), axis=0)
    std_mask = np.std(matrix, axis=0) > 1e-12
    keep_mask = finite_mask & std_mask
    if not np.any(keep_mask):
        return np.empty((matrix.shape[0], 0), dtype=np.float64), []
    kept_names = [name for name, keep in zip(metric_names, keep_mask.tolist(), strict=False) if keep]
    return matrix[:, keep_mask], kept_names


def _extract_trial_outcome_metrics(record: dict[str, Any]) -> dict[str, float]:
    for key in ("selection_key", "valid_selection_key"):
        payload = record.get(key)
        if isinstance(payload, dict):
            numeric = {
                str(name): float(value)
                for name, value in payload.items()
                if _is_finite_number(value)
            }
            if numeric:
                return numeric
    numeric = {
        str(name): float(value)
        for name, value in record.items()
        if _is_finite_number(value) and str(name) not in {"trial"}
    }
    return numeric


def _pairwise_trial_similarity(outcome_matrix: np.ndarray) -> np.ndarray | None:
    if outcome_matrix.ndim != 2 or outcome_matrix.shape[0] < 2 or outcome_matrix.shape[1] < 2:
        return None
    row_std = np.std(outcome_matrix, axis=1)
    if np.any(row_std <= 1e-12):
        return None
    similarity = np.corrcoef(outcome_matrix)
    if not np.all(np.isfinite(similarity)):
        return None
    similarity = np.clip(similarity, -1.0, 1.0)
    np.fill_diagonal(similarity, 1.0)
    return similarity


def _is_finite_number(value: Any) -> bool:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(numeric)
