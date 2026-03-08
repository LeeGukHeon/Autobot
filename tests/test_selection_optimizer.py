from __future__ import annotations

import numpy as np

from autobot.models.selection_optimizer import (
    SelectionGridConfig,
    build_selection_recommendations_from_walk_forward,
    build_window_selection_objectives,
)


def test_build_window_selection_objectives_evaluates_grid() -> None:
    scores = np.asarray([0.95, 0.80, 0.70, 0.65, 0.92, 0.60], dtype=np.float64)
    y_reg = np.asarray([0.020, 0.010, -0.005, 0.003, 0.018, -0.002], dtype=np.float64)
    ts_ms = np.asarray([1, 1, 1, 2, 2, 2], dtype=np.int64)
    thresholds = {"top_5pct": 0.60}

    result = build_window_selection_objectives(
        scores=scores,
        y_reg=y_reg,
        ts_ms=ts_ms,
        thresholds=thresholds,
        fee_bps_est=5.0,
        safety_bps=1.0,
        config=SelectionGridConfig(
            top_pct_grid=(0.5, 1.0),
            min_candidates_grid=(1, 2),
            min_active_ts_ratio=0.1,
            min_selected_rows=1,
        ),
    )

    assert result["comparable"] is True
    top5 = result["by_threshold_key"]["top_5pct"]
    assert top5["objective"] == "walk_forward_mean_ev_net_selected"
    assert top5["grid_results_count"] == 6
    grid_points = {(row["top_pct"], row["min_candidates_per_ts"]) for row in top5["grid_results"]}
    assert {(0.5, 1), (0.5, 2), (1.0, 1), (1.0, 2)}.issubset(grid_points)


def test_build_selection_recommendations_from_walk_forward_prefers_best_grid_point() -> None:
    fallback = {
        "version": 1,
        "by_threshold_key": {
            "top_5pct": {
                "threshold": 0.8,
                "recommended_top_pct": 0.2,
                "recommended_min_candidates_per_ts": 2,
            }
        },
    }
    windows = [
        {
            "selection_optimization": {
                "comparable": True,
                "by_threshold_key": {
                    "top_5pct": {
                        "grid_results": [
                            {
                                "top_pct": 0.5,
                                "min_candidates_per_ts": 1,
                                "selected_rows": 4,
                                "active_ts_ratio": 0.6,
                                "positive_active_ts_ratio": 0.7,
                                "ev_net": 0.004,
                                "feasible": True,
                                "constraint_reasons": [],
                            },
                            {
                                "top_pct": 1.0,
                                "min_candidates_per_ts": 1,
                                "selected_rows": 6,
                                "active_ts_ratio": 0.8,
                                "positive_active_ts_ratio": 0.6,
                                "ev_net": 0.002,
                                "feasible": True,
                                "constraint_reasons": [],
                            },
                        ]
                    }
                },
            }
        },
        {
            "selection_optimization": {
                "comparable": True,
                "by_threshold_key": {
                    "top_5pct": {
                        "grid_results": [
                            {
                                "top_pct": 0.5,
                                "min_candidates_per_ts": 1,
                                "selected_rows": 3,
                                "active_ts_ratio": 0.5,
                                "positive_active_ts_ratio": 0.8,
                                "ev_net": 0.003,
                                "feasible": True,
                                "constraint_reasons": [],
                            },
                            {
                                "top_pct": 1.0,
                                "min_candidates_per_ts": 1,
                                "selected_rows": 5,
                                "active_ts_ratio": 0.7,
                                "positive_active_ts_ratio": 0.5,
                                "ev_net": 0.001,
                                "feasible": True,
                                "constraint_reasons": [],
                            },
                        ]
                    }
                },
            }
        },
    ]

    doc = build_selection_recommendations_from_walk_forward(
        windows=windows,
        fallback_recommendations=fallback,
    )

    entry = doc["by_threshold_key"]["top_5pct"]
    assert doc["version"] == 2
    assert entry["recommended_top_pct"] == 0.5
    assert entry["recommended_min_candidates_per_ts"] == 1
    assert entry["recommendation_source"] == "walk_forward_objective_optimizer"
    assert entry["fallback_used"] is False


def test_build_selection_recommendations_from_walk_forward_falls_back_without_windows() -> None:
    fallback = {
        "version": 1,
        "by_threshold_key": {
            "top_5pct": {
                "threshold": 0.8,
                "recommended_top_pct": 0.33,
                "recommended_min_candidates_per_ts": 2,
            }
        },
    }

    doc = build_selection_recommendations_from_walk_forward(
        windows=[],
        fallback_recommendations=fallback,
    )

    entry = doc["by_threshold_key"]["top_5pct"]
    assert entry["recommended_top_pct"] == 0.33
    assert entry["recommended_min_candidates_per_ts"] == 2
    assert entry["fallback_used"] is True
    assert entry["recommendation_source"] == "fallback_heuristic"
