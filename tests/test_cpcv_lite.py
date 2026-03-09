from __future__ import annotations

import numpy as np

from autobot.models.cpcv_lite import (
    build_cpcv_lite_plan,
    summarize_cpcv_lite_dsr,
    summarize_cpcv_lite_fold_selection,
    summarize_cpcv_lite_pbo,
)


def test_build_cpcv_lite_plan_respects_combination_budget() -> None:
    base = np.array([1_700_000_000_000 + (i * 300_000) for i in range(120)], dtype=np.int64)
    ts_ms = np.repeat(base, 2)

    specs, report = build_cpcv_lite_plan(
        ts_ms,
        group_count=6,
        test_group_count=2,
        max_combinations=4,
        embargo_bars=1,
        interval_ms=300_000,
    )

    assert len(specs) == 4
    assert report["budget_cut"] is True
    assert report["budget_reason"] == "MAX_COMBINATIONS_CAP"
    assert report["group_count_effective"] == 6
    assert report["test_group_count_effective"] == 2
    assert len(report["chosen_combinations"]) == 4
    assert len(report["skipped_combinations"]) == report["total_combinations"] - 4
    assert all(spec.counts["train"] > 0 and spec.counts["test"] > 0 for spec in specs)


def test_cpcv_lite_fold_selection_summarizes_common_configs() -> None:
    train_selection = {
        "by_threshold_key": {
            "top_5pct": {
                "grid_results": [
                    {"top_pct": 0.5, "min_candidates_per_ts": 1, "ev_net": 0.004, "feasible": True},
                    {"top_pct": 1.0, "min_candidates_per_ts": 1, "ev_net": 0.002, "feasible": True},
                ]
            }
        }
    }
    test_selection = {
        "by_threshold_key": {
            "top_5pct": {
                "grid_results": [
                    {"top_pct": 0.5, "min_candidates_per_ts": 1, "ev_net": 0.003, "feasible": True},
                    {"top_pct": 1.0, "min_candidates_per_ts": 1, "ev_net": 0.001, "feasible": True},
                ]
            }
        }
    }

    summary = summarize_cpcv_lite_fold_selection(
        train_selection=train_selection,
        test_selection=test_selection,
    )

    assert summary["comparable"] is True
    assert summary["common_config_count"] == 2
    assert summary["selected_threshold_key"] == "top_5pct"
    assert summary["selected_test_rank"] == 1
    assert 0.5 < summary["selected_test_percentile"] < 1.0


def test_cpcv_lite_pbo_and_dsr_summarize_fold_rows() -> None:
    folds = [
        {
            "selection_summary": {
                "comparable": True,
                "common_config_count": 3,
                "selected_test_objective": 0.004,
                "selected_test_percentile": 0.9,
                "selected_test_logit": 2.197224577,
            }
        },
        {
            "selection_summary": {
                "comparable": True,
                "common_config_count": 3,
                "selected_test_objective": -0.001,
                "selected_test_percentile": 0.25,
                "selected_test_logit": -1.098612289,
            }
        },
    ]

    pbo = summarize_cpcv_lite_pbo(folds)
    dsr = summarize_cpcv_lite_dsr(folds)

    assert pbo["comparable"] is True
    assert pbo["folds_considered"] == 2
    assert 0.0 <= pbo["pbo_estimate"] <= 1.0
    assert dsr["observations"] == 2
    assert dsr["effective_trials"] == 3
