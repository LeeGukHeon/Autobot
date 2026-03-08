from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np


_DEFAULT_TOP_PCT_GRID = (0.05, 0.10, 0.20, 0.33, 0.50, 0.75, 1.00)
_DEFAULT_MIN_CANDIDATES_GRID = (1, 2, 3, 4, 5)


@dataclass(frozen=True)
class SelectionGridConfig:
    top_pct_grid: tuple[float, ...] = _DEFAULT_TOP_PCT_GRID
    min_candidates_grid: tuple[int, ...] = _DEFAULT_MIN_CANDIDATES_GRID
    min_active_ts_ratio: float = 0.10
    min_selected_rows: int = 3


def build_window_selection_objectives(
    *,
    scores: np.ndarray,
    y_reg: np.ndarray,
    ts_ms: np.ndarray,
    thresholds: dict[str, Any],
    fee_bps_est: float,
    safety_bps: float,
    config: SelectionGridConfig | None = None,
) -> dict[str, Any]:
    grid = config or SelectionGridConfig()
    score_values = np.asarray(scores, dtype=np.float64)
    reg_values = np.asarray(y_reg, dtype=np.float64)
    ts_values = np.asarray(ts_ms, dtype=np.int64)
    if score_values.size <= 0 or reg_values.size <= 0 or ts_values.size <= 0:
        return {
            "policy": "walk_forward_selection_objective",
            "comparable": False,
            "reason": "EMPTY_WINDOW",
            "by_threshold_key": {},
        }
    if not (score_values.size == reg_values.size == ts_values.size):
        return {
            "policy": "walk_forward_selection_objective",
            "comparable": False,
            "reason": "MISALIGNED_WINDOW_ARRAYS",
            "by_threshold_key": {},
        }

    fee_frac = float(fee_bps_est + safety_bps) / 10_000.0
    threshold_keys = ("top_1pct", "top_5pct", "top_10pct", "ev_opt")
    by_key: dict[str, Any] = {}
    for threshold_key in threshold_keys:
        threshold_value = _safe_optional_float(thresholds.get(threshold_key))
        if threshold_value is None:
            continue
        grid_results = _evaluate_grid_for_threshold(
            scores=score_values,
            y_reg=reg_values,
            ts_ms=ts_values,
            threshold=float(threshold_value),
            fee_frac=fee_frac,
            grid=grid,
        )
        by_key[threshold_key] = {
            "threshold": float(threshold_value),
            "objective": "walk_forward_mean_ev_net_selected",
            "grid_results": grid_results,
            "grid_results_count": int(len(grid_results)),
        }
    return {
        "policy": "walk_forward_selection_objective",
        "comparable": True,
        "config": {
            "top_pct_grid": [float(value) for value in grid.top_pct_grid],
            "min_candidates_grid": [int(value) for value in grid.min_candidates_grid],
            "min_active_ts_ratio": float(grid.min_active_ts_ratio),
            "min_selected_rows": int(grid.min_selected_rows),
        },
        "by_threshold_key": by_key,
    }


def build_selection_recommendations_from_walk_forward(
    *,
    windows: list[dict[str, Any]] | None,
    fallback_recommendations: dict[str, Any] | None,
) -> dict[str, Any]:
    fallback = dict(fallback_recommendations or {})
    result: dict[str, Any] = {
        "version": 2,
        "created_at_utc": fallback.get("created_at_utc"),
        "optimizer": {
            "method": "walk_forward_grid_search",
            "objective": "mean_ev_net_selected",
            "fallback_used": False,
        },
        "by_threshold_key": {},
    }
    fallback_by_key = fallback.get("by_threshold_key") if isinstance(fallback.get("by_threshold_key"), dict) else {}
    window_rows = list(windows or [])
    optimization_rows = [window.get("selection_optimization") for window in window_rows if isinstance(window, dict)]
    comparable_rows = [row for row in optimization_rows if isinstance(row, dict) and bool(row.get("comparable"))]

    threshold_keys = sorted(
        {
            str(key)
            for row in comparable_rows
            for key in ((row.get("by_threshold_key") or {}).keys() if isinstance(row.get("by_threshold_key"), dict) else [])
        }
        | {
            str(key)
            for key in (fallback_by_key.keys() if isinstance(fallback_by_key, dict) else [])
        }
    )

    used_fallback = False
    for threshold_key in threshold_keys:
        fallback_entry = dict(fallback_by_key.get(threshold_key) or {})
        aggregate = _aggregate_window_grid_results(
            comparable_rows=comparable_rows,
            threshold_key=threshold_key,
        )
        if not aggregate:
            if fallback_entry:
                used_fallback = True
                fallback_entry["objective"] = "fallback_heuristic"
                fallback_entry["selected_grid_point"] = {
                    "top_pct": _safe_optional_float(fallback_entry.get("recommended_top_pct")),
                    "min_candidates_per_ts": int(fallback_entry.get("recommended_min_candidates_per_ts", 1) or 1),
                }
                fallback_entry["constraint_reasons"] = ["NO_WALK_FORWARD_GRID_RESULTS"]
                fallback_entry["fallback_used"] = True
                fallback_entry["recommendation_source"] = "fallback_heuristic"
                result["by_threshold_key"][threshold_key] = fallback_entry
            continue

        chosen = _choose_best_grid_result(aggregate)
        if not chosen:
            if fallback_entry:
                used_fallback = True
                fallback_entry["objective"] = "fallback_heuristic"
                fallback_entry["selected_grid_point"] = {
                    "top_pct": _safe_optional_float(fallback_entry.get("recommended_top_pct")),
                    "min_candidates_per_ts": int(fallback_entry.get("recommended_min_candidates_per_ts", 1) or 1),
                }
                fallback_entry["constraint_reasons"] = ["NO_FEASIBLE_GRID_POINT"]
                fallback_entry["fallback_used"] = True
                fallback_entry["recommendation_source"] = "fallback_heuristic"
                result["by_threshold_key"][threshold_key] = fallback_entry
            continue

        entry = dict(fallback_entry)
        entry.update(
            {
                "recommended_top_pct": float(chosen["top_pct"]),
                "recommended_min_candidates_per_ts": int(chosen["min_candidates_per_ts"]),
                "recommended_min_candidates_coverage": float(chosen["active_ts_ratio_mean"]),
                "top_pct_source": "walk_forward_objective_optimizer",
                "min_candidates_source": "walk_forward_objective_optimizer",
                "objective": "walk_forward_mean_ev_net_selected",
                "objective_score": float(chosen["objective_score"]),
                "selected_grid_point": {
                    "top_pct": float(chosen["top_pct"]),
                    "min_candidates_per_ts": int(chosen["min_candidates_per_ts"]),
                },
                "constraint_reasons": list(chosen.get("constraint_reasons", [])),
                "fallback_used": False,
                "recommendation_source": "walk_forward_objective_optimizer",
                "windows_covered": int(chosen["windows_covered"]),
                "window_count": int(chosen["window_count"]),
                "feasible_window_ratio": float(chosen["feasible_window_ratio"]),
                "active_ts_ratio_mean": float(chosen["active_ts_ratio_mean"]),
                "positive_active_ts_ratio_mean": float(chosen["positive_active_ts_ratio_mean"]),
                "selected_rows_mean": float(chosen["selected_rows_mean"]),
                "grid_candidates_evaluated": int(chosen["grid_candidates_evaluated"]),
                "grid_summary": {
                    "top_pct_grid": sorted({float(row["top_pct"]) for row in aggregate}),
                    "min_candidates_grid": sorted({int(row["min_candidates_per_ts"]) for row in aggregate}),
                },
            }
        )
        result["by_threshold_key"][threshold_key] = entry

    result["optimizer"]["fallback_used"] = bool(used_fallback)
    return result


def _evaluate_grid_for_threshold(
    *,
    scores: np.ndarray,
    y_reg: np.ndarray,
    ts_ms: np.ndarray,
    threshold: float,
    fee_frac: float,
    grid: SelectionGridConfig,
) -> list[dict[str, Any]]:
    by_ts = _group_indices_by_ts(ts_ms)
    total_ts_count = max(len(by_ts), 1)
    max_eligible = 0
    grid_rows: list[dict[str, Any]] = []

    eligible_per_ts: list[int] = []
    for _, indices in by_ts:
        eligible_count = int(np.sum(scores[indices] >= threshold))
        max_eligible = max(max_eligible, eligible_count)
        eligible_per_ts.append(eligible_count)

    candidate_grid = [int(value) for value in grid.min_candidates_grid if int(value) > 0]
    if max_eligible > 0 and max_eligible not in candidate_grid:
        candidate_grid.append(max_eligible)
    candidate_grid = sorted(set(candidate_grid))

    for top_pct in sorted(set(float(value) for value in grid.top_pct_grid if float(value) > 0.0)):
        for min_candidates in candidate_grid:
            stats = _simulate_selection_runtime(
                scores=scores,
                y_reg=y_reg,
                by_ts=by_ts,
                threshold=threshold,
                top_pct=top_pct,
                min_candidates=min_candidates,
                fee_frac=fee_frac,
            )
            feasible = bool(
                stats["active_ts_ratio"] >= float(grid.min_active_ts_ratio)
                and stats["selected_rows"] >= int(grid.min_selected_rows)
            )
            reasons: list[str] = []
            if stats["active_ts_ratio"] < float(grid.min_active_ts_ratio):
                reasons.append("LOW_ACTIVE_TS_RATIO")
            if stats["selected_rows"] < int(grid.min_selected_rows):
                reasons.append("LOW_SELECTED_ROWS")
            grid_rows.append(
                {
                    "top_pct": float(top_pct),
                    "min_candidates_per_ts": int(min_candidates),
                    "selected_rows": int(stats["selected_rows"]),
                    "active_ts_count": int(stats["active_ts_count"]),
                    "active_ts_ratio": float(stats["active_ts_ratio"]),
                    "eligible_ts_nonzero": int(np.sum(np.asarray(eligible_per_ts, dtype=np.int64) > 0)),
                    "total_ts_count": int(total_ts_count),
                    "mean_y_reg_selected": float(stats["mean_y_reg_selected"]),
                    "ev_net": float(stats["ev_net"]),
                    "positive_active_ts_ratio": float(stats["positive_active_ts_ratio"]),
                    "feasible": bool(feasible),
                    "constraint_reasons": reasons,
                }
            )
    return grid_rows


def _simulate_selection_runtime(
    *,
    scores: np.ndarray,
    y_reg: np.ndarray,
    by_ts: list[tuple[int, np.ndarray]],
    threshold: float,
    top_pct: float,
    min_candidates: int,
    fee_frac: float,
) -> dict[str, Any]:
    selected_values: list[float] = []
    per_ts_ev: list[float] = []
    active_ts_count = 0
    selected_rows = 0

    for _, indices in by_ts:
        window_scores = scores[indices]
        eligible_local = np.flatnonzero(window_scores >= threshold)
        eligible_count = int(eligible_local.size)
        if eligible_count < int(min_candidates):
            continue
        select_count = int(np.floor(float(eligible_count) * float(top_pct)))
        if select_count <= 0:
            continue
        select_count = min(select_count, eligible_count)
        if select_count >= eligible_count:
            selected_local = eligible_local
        else:
            eligible_scores = window_scores[eligible_local]
            selected_slice = np.argpartition(eligible_scores, -select_count)[-select_count:]
            selected_local = eligible_local[selected_slice]
        selected_reg = np.asarray(y_reg[indices[selected_local]], dtype=np.float64)
        if selected_reg.size <= 0:
            continue
        active_ts_count += 1
        selected_rows += int(selected_reg.size)
        selected_values.extend(float(value) for value in selected_reg.tolist())
        per_ts_ev.append(float(np.mean(selected_reg)) - float(fee_frac))

    total_ts_count = max(len(by_ts), 1)
    mean_y_reg_selected = float(np.mean(selected_values)) if selected_values else 0.0
    ev_net = mean_y_reg_selected - float(fee_frac) if selected_values else -float(fee_frac)
    positive_active_ts_ratio = float(np.mean(np.asarray(per_ts_ev, dtype=np.float64) > 0.0)) if per_ts_ev else 0.0
    return {
        "selected_rows": int(selected_rows),
        "active_ts_count": int(active_ts_count),
        "active_ts_ratio": float(active_ts_count) / float(total_ts_count),
        "mean_y_reg_selected": float(mean_y_reg_selected),
        "ev_net": float(ev_net),
        "positive_active_ts_ratio": float(positive_active_ts_ratio),
    }


def _aggregate_window_grid_results(
    *,
    comparable_rows: list[dict[str, Any]],
    threshold_key: str,
) -> list[dict[str, Any]]:
    grouped: dict[tuple[float, int], list[dict[str, Any]]] = {}
    window_count = len(comparable_rows)
    for row in comparable_rows:
        by_key = row.get("by_threshold_key") if isinstance(row.get("by_threshold_key"), dict) else {}
        threshold_doc = by_key.get(threshold_key) if isinstance(by_key, dict) else None
        grid_results = threshold_doc.get("grid_results") if isinstance(threshold_doc, dict) else None
        if not isinstance(grid_results, list):
            continue
        for candidate in grid_results:
            try:
                grid_key = (
                    float(candidate.get("top_pct")),
                    int(candidate.get("min_candidates_per_ts")),
                )
            except Exception:
                continue
            grouped.setdefault(grid_key, []).append(dict(candidate))

    aggregate: list[dict[str, Any]] = []
    for (top_pct, min_candidates), rows in grouped.items():
        if not rows:
            continue
        feasible_rows = [row for row in rows if bool(row.get("feasible"))]
        objective_score = float(np.mean([float(row.get("ev_net", 0.0)) for row in rows]))
        aggregate.append(
            {
                "top_pct": float(top_pct),
                "min_candidates_per_ts": int(min_candidates),
                "window_count": int(window_count),
                "windows_covered": int(len(rows)),
                "feasible_window_ratio": float(len(feasible_rows)) / float(max(window_count, 1)),
                "active_ts_ratio_mean": float(np.mean([float(row.get("active_ts_ratio", 0.0)) for row in rows])),
                "positive_active_ts_ratio_mean": float(
                    np.mean([float(row.get("positive_active_ts_ratio", 0.0)) for row in rows])
                ),
                "selected_rows_mean": float(np.mean([float(row.get("selected_rows", 0.0)) for row in rows])),
                "objective_score": float(objective_score),
                "grid_candidates_evaluated": int(len(rows)),
                "all_feasible": bool(rows and len(feasible_rows) == len(rows)),
                "constraint_reasons": sorted(
                    {
                        str(reason)
                        for row in rows
                        for reason in (row.get("constraint_reasons") or [])
                        if str(reason)
                    }
                ),
            }
        )
    return aggregate


def _choose_best_grid_result(aggregate: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not aggregate:
        return None
    feasible = [row for row in aggregate if float(row.get("feasible_window_ratio", 0.0)) >= 0.5]
    candidate_rows = feasible or aggregate
    return sorted(
        candidate_rows,
        key=lambda row: (
            float(row.get("objective_score", 0.0)),
            float(row.get("feasible_window_ratio", 0.0)),
            float(row.get("active_ts_ratio_mean", 0.0)),
            float(row.get("selected_rows_mean", 0.0)),
            -float(row.get("top_pct", 1.0)),
        ),
        reverse=True,
    )[0]


def _group_indices_by_ts(ts_ms: np.ndarray) -> list[tuple[int, np.ndarray]]:
    values = np.asarray(ts_ms, dtype=np.int64)
    if values.size <= 0:
        return []
    unique_ts, inverse = np.unique(values, return_inverse=True)
    grouped: list[tuple[int, np.ndarray]] = []
    for ts_idx, ts_value in enumerate(unique_ts.tolist()):
        grouped.append((int(ts_value), np.flatnonzero(inverse == ts_idx).astype(np.int64, copy=False)))
    return grouped


def _safe_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
