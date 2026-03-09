"""Bounded CPCV-lite helpers for research-only validation."""

from __future__ import annotations

from dataclasses import dataclass
import itertools
import math
from typing import Any

import numpy as np

from .split import SPLIT_DROP, SPLIT_TEST, SPLIT_TRAIN
from .stat_validation import deflated_sharpe_ratio_estimate


@dataclass(frozen=True)
class CpcvLiteFoldSpec:
    fold_index: int
    labels: np.ndarray
    test_groups: tuple[int, ...]
    purged_groups: tuple[int, ...]
    counts: dict[str, int]


def build_cpcv_lite_plan(
    ts_ms: np.ndarray,
    *,
    group_count: int,
    test_group_count: int,
    max_combinations: int,
    embargo_bars: int,
    interval_ms: int,
) -> tuple[list[CpcvLiteFoldSpec], dict[str, Any]]:
    ts_values = np.asarray(ts_ms, dtype=np.int64)
    report: dict[str, Any] = {
        "policy": "cpcv_lite_research_v1",
        "enabled": True,
        "estimate_label": "lite",
        "group_count_requested": max(int(group_count), 1),
        "test_group_count_requested": max(int(test_group_count), 1),
        "max_combinations": max(int(max_combinations), 1),
        "embargo_bars": max(int(embargo_bars), 0),
        "interval_ms": max(int(interval_ms), 1),
        "group_count_effective": 0,
        "test_group_count_effective": 0,
        "total_combinations": 0,
        "budget_cut": False,
        "budget_reason": "UNSET",
        "group_definitions": [],
        "chosen_combinations": [],
        "skipped_combinations": [],
        "insufficiency_reasons": [],
    }
    if ts_values.size <= 0:
        report["budget_reason"] = "EMPTY_TS"
        report["insufficiency_reasons"] = ["EMPTY_TS"]
        return [], report

    unique_ts = np.unique(ts_values)
    if unique_ts.size < 6:
        report["budget_reason"] = "INSUFFICIENT_UNIQUE_TS"
        report["insufficiency_reasons"] = ["INSUFFICIENT_UNIQUE_TS"]
        return [], report

    effective_groups = min(max(int(group_count), 1), int(unique_ts.size))
    effective_groups = max(effective_groups, 3)
    group_chunks = [chunk.astype(np.int64, copy=False) for chunk in np.array_split(unique_ts, effective_groups) if chunk.size > 0]
    effective_groups = len(group_chunks)
    effective_test_groups = min(max(int(test_group_count), 1), max(effective_groups - 1, 1))
    if effective_groups < 3 or effective_test_groups <= 0:
        report["budget_reason"] = "INSUFFICIENT_GROUPS"
        report["insufficiency_reasons"] = ["INSUFFICIENT_GROUPS"]
        return [], report

    ts_to_group: dict[int, int] = {}
    group_definitions: list[dict[str, Any]] = []
    for idx, chunk in enumerate(group_chunks):
        for ts_value in chunk.tolist():
            ts_to_group[int(ts_value)] = idx
        group_definitions.append(
            {
                "group_index": int(idx),
                "start_ts": int(chunk[0]),
                "end_ts": int(chunk[-1]),
                "ts_count": int(chunk.size),
            }
        )

    group_ids = np.asarray([ts_to_group[int(value)] for value in ts_values.tolist()], dtype=np.int64)
    combinations_all = list(itertools.combinations(range(effective_groups), effective_test_groups))
    total_combinations = len(combinations_all)
    if total_combinations <= 0:
        report["budget_reason"] = "NO_COMBINATIONS"
        report["insufficiency_reasons"] = ["NO_COMBINATIONS"]
        return [], report

    chosen_indices = _select_combination_indices(
        total=total_combinations,
        max_combinations=max(int(max_combinations), 1),
    )
    chosen_combinations = [tuple(int(item) for item in combinations_all[idx]) for idx in chosen_indices]
    skipped_combinations = [
        tuple(int(item) for item in combinations_all[idx])
        for idx in range(total_combinations)
        if idx not in chosen_indices
    ]

    embargo_ms = max(int(embargo_bars), 0) * max(int(interval_ms), 1)
    specs: list[CpcvLiteFoldSpec] = []
    for fold_index, combo in enumerate(chosen_combinations):
        test_mask = np.isin(group_ids, np.asarray(combo, dtype=np.int64))
        drop_mask = np.zeros(ts_values.shape[0], dtype=bool)
        for group_idx in combo:
            group_def = group_definitions[int(group_idx)]
            start_ts = int(group_def["start_ts"])
            end_ts = int(group_def["end_ts"])
            if embargo_ms > 0:
                near_start = np.abs(ts_values - start_ts) <= embargo_ms
                near_end = np.abs(ts_values - end_ts) <= embargo_ms
                drop_mask |= near_start | near_end
        drop_mask &= ~test_mask
        train_mask = ~(test_mask | drop_mask)
        purged_groups = tuple(
            int(item)
            for item in sorted({int(group_ids[idx]) for idx in np.flatnonzero(drop_mask).tolist()})
            if int(item) not in combo
        )
        labels = np.full(ts_values.shape[0], SPLIT_DROP, dtype=object)
        labels[train_mask] = SPLIT_TRAIN
        labels[test_mask] = SPLIT_TEST
        specs.append(
            CpcvLiteFoldSpec(
                fold_index=int(fold_index),
                labels=labels,
                test_groups=tuple(int(item) for item in combo),
                purged_groups=purged_groups,
                counts={
                    SPLIT_TRAIN: int(np.sum(train_mask)),
                    SPLIT_TEST: int(np.sum(test_mask)),
                    SPLIT_DROP: int(np.sum(drop_mask)),
                },
            )
        )

    report.update(
        {
            "group_count_effective": int(effective_groups),
            "test_group_count_effective": int(effective_test_groups),
            "total_combinations": int(total_combinations),
            "budget_cut": bool(total_combinations > len(chosen_combinations)),
            "budget_reason": "MAX_COMBINATIONS_CAP" if total_combinations > len(chosen_combinations) else "FULL_ENUMERATION",
            "group_definitions": group_definitions,
            "chosen_combinations": [list(item) for item in chosen_combinations],
            "skipped_combinations": [list(item) for item in skipped_combinations],
        }
    )
    return specs, report


def summarize_cpcv_lite_fold_selection(
    *,
    train_selection: dict[str, Any],
    test_selection: dict[str, Any],
) -> dict[str, Any]:
    train_rows = _flatten_selection_rows(train_selection)
    test_rows = _flatten_selection_rows(test_selection)
    common_keys = sorted(set(train_rows.keys()) & set(test_rows.keys()))
    if len(common_keys) < 2:
        return {
            "comparable": False,
            "reason": "INSUFFICIENT_COMMON_CONFIGS",
            "common_config_count": int(len(common_keys)),
        }

    train_pairs = [(key, float(train_rows[key]["objective_score"])) for key in common_keys]
    train_pairs.sort(key=lambda item: (item[1], item[0]), reverse=True)
    selected_key = str(train_pairs[0][0])
    test_scores = np.asarray([float(test_rows[key]["objective_score"]) for key in common_keys], dtype=np.float64)
    selected_test_score = float(test_rows[selected_key]["objective_score"])
    rank_position = 1 + int(np.sum(test_scores > selected_test_score))
    percentile = 1.0 - ((rank_position - 1) / float(max(test_scores.size - 1, 1)))
    percentile = float(min(max(percentile, 1e-6), 1.0 - 1e-6))
    return {
        "comparable": True,
        "reason": "",
        "common_config_count": int(len(common_keys)),
        "selected_train_config_id": selected_key,
        "selected_train_objective": float(train_rows[selected_key]["objective_score"]),
        "selected_test_objective": selected_test_score,
        "selected_test_rank": int(rank_position),
        "selected_test_percentile": percentile,
        "selected_test_logit": float(math.log(percentile / max(1.0 - percentile, 1e-6))),
        "selected_threshold_key": str(train_rows[selected_key]["threshold_key"]),
        "selected_grid_point": {
            "top_pct": float(train_rows[selected_key]["top_pct"]),
            "min_candidates_per_ts": int(train_rows[selected_key]["min_candidates_per_ts"]),
        },
    }


def summarize_cpcv_lite_pbo(folds: list[dict[str, Any]]) -> dict[str, Any]:
    comparable_rows = [
        dict(fold.get("selection_summary", {}))
        for fold in folds
        if isinstance(fold, dict) and bool((fold.get("selection_summary") or {}).get("comparable"))
    ]
    if len(comparable_rows) < 2:
        return {
            "comparable": False,
            "reason": "INSUFFICIENT_COMPARABLE_FOLDS",
            "folds_considered": int(len(comparable_rows)),
            "pbo_estimate": 0.0,
            "overfit_fold_count": 0,
            "median_selected_test_percentile": 0.0,
            "mean_selected_test_logit": 0.0,
        }
    percentiles = np.asarray(
        [float(row.get("selected_test_percentile", 0.0) or 0.0) for row in comparable_rows],
        dtype=np.float64,
    )
    logits = np.asarray(
        [float(row.get("selected_test_logit", 0.0) or 0.0) for row in comparable_rows],
        dtype=np.float64,
    )
    overfit_mask = percentiles <= 0.5
    return {
        "comparable": True,
        "reason": "",
        "folds_considered": int(percentiles.size),
        "pbo_estimate": float(np.mean(overfit_mask.astype(np.float64))),
        "overfit_fold_count": int(np.sum(overfit_mask)),
        "median_selected_test_percentile": float(np.median(percentiles)),
        "mean_selected_test_logit": float(np.mean(logits)),
    }


def summarize_cpcv_lite_dsr(folds: list[dict[str, Any]]) -> dict[str, Any]:
    comparable_rows = [
        dict(fold.get("selection_summary", {}))
        for fold in folds
        if isinstance(fold, dict) and bool((fold.get("selection_summary") or {}).get("comparable"))
    ]
    if len(comparable_rows) < 2:
        return {
            "comparable": False,
            "reason": "INSUFFICIENT_COMPARABLE_FOLDS",
            "observations": int(len(comparable_rows)),
            "mean_selected_test_objective": 0.0,
            "std_selected_test_objective": 0.0,
            "score_series_sharpe": 0.0,
            "deflated_sharpe_ratio_est": 0.0,
            "benchmark_sharpe": 0.0,
            "effective_trials": 0,
            "effective_trials_source": "selection_config_count_max",
        }
    values = np.asarray(
        [float(row.get("selected_test_objective", 0.0) or 0.0) for row in comparable_rows],
        dtype=np.float64,
    )
    common_counts = np.asarray(
        [int(row.get("common_config_count", 0) or 0) for row in comparable_rows],
        dtype=np.int64,
    )
    std_value = _sample_std(values)
    if std_value <= 0.0:
        return {
            "comparable": False,
            "reason": "ZERO_SCORE_VOLATILITY",
            "observations": int(values.size),
            "mean_selected_test_objective": float(np.mean(values)) if values.size > 0 else 0.0,
            "std_selected_test_objective": float(std_value),
            "score_series_sharpe": 0.0,
            "deflated_sharpe_ratio_est": 0.0,
            "benchmark_sharpe": 0.0,
            "effective_trials": int(np.max(common_counts)) if common_counts.size > 0 else 0,
            "effective_trials_source": "selection_config_count_max",
        }
    sharpe_like = float(np.mean(values) / std_value)
    dsr = deflated_sharpe_ratio_estimate(
        observed_sharpe=sharpe_like,
        observations=int(values.size),
        skewness=_sample_skewness(values),
        kurtosis=_sample_kurtosis(values),
        trial_count=int(np.max(common_counts)) if common_counts.size > 0 else 1,
    )
    return {
        "comparable": True,
        "reason": "",
        "observations": int(values.size),
        "mean_selected_test_objective": float(np.mean(values)),
        "std_selected_test_objective": float(std_value),
        "score_series_sharpe": sharpe_like,
        "deflated_sharpe_ratio_est": float(dsr.get("deflated_sharpe_ratio_est", 0.0) or 0.0),
        "benchmark_sharpe": float(dsr.get("benchmark_sharpe", 0.0) or 0.0),
        "effective_trials": int(np.max(common_counts)) if common_counts.size > 0 else 0,
        "effective_trials_source": "selection_config_count_max",
    }


def _select_combination_indices(*, total: int, max_combinations: int) -> set[int]:
    total_count = max(int(total), 0)
    max_count = max(int(max_combinations), 1)
    if total_count <= max_count:
        return set(range(total_count))
    raw = np.linspace(0, total_count - 1, num=max_count, dtype=np.int64).tolist()
    return {int(item) for item in raw}


def _flatten_selection_rows(doc: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    payload = dict(doc or {})
    by_key = payload.get("by_threshold_key") if isinstance(payload.get("by_threshold_key"), dict) else {}
    rows: dict[str, dict[str, Any]] = {}
    for threshold_key, threshold_entry in by_key.items():
        if not isinstance(threshold_entry, dict):
            continue
        grid_results = threshold_entry.get("grid_results") if isinstance(threshold_entry.get("grid_results"), list) else []
        for row in grid_results:
            if not isinstance(row, dict) or not bool(row.get("feasible", False)):
                continue
            config_id = (
                f"{str(threshold_key)}|"
                f"{float(row.get('top_pct', 0.0) or 0.0):.6f}|"
                f"{int(row.get('min_candidates_per_ts', 0) or 0)}"
            )
            rows[config_id] = {
                "config_id": config_id,
                "threshold_key": str(threshold_key),
                "top_pct": float(row.get("top_pct", 0.0) or 0.0),
                "min_candidates_per_ts": int(row.get("min_candidates_per_ts", 0) or 0),
                "objective_score": float(row.get("ev_net", 0.0) or 0.0),
            }
    return rows


def _sample_std(values: np.ndarray) -> float:
    array = np.asarray(values, dtype=np.float64)
    if array.size <= 1:
        return 0.0
    return float(np.std(array, ddof=1))


def _sample_skewness(values: np.ndarray) -> float:
    array = np.asarray(values, dtype=np.float64)
    if array.size < 3:
        return 0.0
    mean_value = float(np.mean(array))
    std_value = _sample_std(array)
    if std_value <= 0.0:
        return 0.0
    centered = (array - mean_value) / std_value
    return float(np.mean(centered**3))


def _sample_kurtosis(values: np.ndarray) -> float:
    array = np.asarray(values, dtype=np.float64)
    if array.size < 4:
        return 3.0
    mean_value = float(np.mean(array))
    std_value = _sample_std(array)
    if std_value <= 0.0:
        return 3.0
    centered = (array - mean_value) / std_value
    return float(np.mean(centered**4))
