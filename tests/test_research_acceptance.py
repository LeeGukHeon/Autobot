from __future__ import annotations

from autobot.models.research_acceptance import (
    compare_balanced_pareto,
    compare_execution_balanced_pareto,
    compare_spa_like_window_test,
    summarize_walk_forward_windows,
)


def test_summarize_walk_forward_windows_aggregates_expected_fields() -> None:
    summary = summarize_walk_forward_windows(
        [
            {
                "metrics": {
                    "classification": {"pr_auc": 0.60, "roc_auc": 0.70, "log_loss": 0.45, "brier_score": 0.20},
                    "trading": {"top_5pct": {"precision": 0.62, "ev_net": 0.0012}},
                }
            },
            {
                "metrics": {
                    "classification": {"pr_auc": 0.64, "roc_auc": 0.74, "log_loss": 0.40, "brier_score": 0.18},
                    "trading": {"top_5pct": {"precision": 0.66, "ev_net": -0.0002}},
                }
            },
        ]
    )

    assert summary["windows_run"] == 2
    assert round(float(summary["precision_top5_mean"]), 4) == 0.64
    assert round(float(summary["positive_window_ratio"]), 4) == 0.5


def test_compare_balanced_pareto_prefers_candidate_on_utility_tie_break() -> None:
    candidate = {
        "ev_net_top5_mean": 0.0011,
        "precision_top5_mean": 0.63,
        "pr_auc_mean": 0.61,
        "positive_window_ratio": 0.66,
        "log_loss_mean": 0.39,
        "brier_score_mean": 0.18,
    }
    champion = {
        "ev_net_top5_mean": 0.0012,
        "precision_top5_mean": 0.62,
        "pr_auc_mean": 0.58,
        "positive_window_ratio": 0.50,
        "log_loss_mean": 0.42,
        "brier_score_mean": 0.19,
    }

    compare = compare_balanced_pareto(candidate, champion)

    assert compare["comparable"] is True
    assert compare["candidate_dominates"] is False
    assert compare["champion_dominates"] is False
    assert compare["decision"] == "candidate_edge"
    assert compare["economic_objective_profile_id"] == "v4_shared_economic_objective_v1"
    assert compare["economic_objective_context"] == "offline_compare"
    assert "UTILITY_TIE_BREAK_PASS" in compare["reasons"]


def test_compare_execution_balanced_pareto_prefers_candidate_on_utility_tie_break() -> None:
    candidate = {
        "orders_filled": 12,
        "realized_pnl_quote": 980.0,
        "fill_rate": 0.94,
        "max_drawdown_pct": 0.72,
        "slippage_bps_mean": 3.2,
    }
    champion = {
        "orders_filled": 11,
        "realized_pnl_quote": 1000.0,
        "fill_rate": 0.91,
        "max_drawdown_pct": 0.81,
        "slippage_bps_mean": 3.8,
    }

    compare = compare_execution_balanced_pareto(candidate, champion)

    assert compare["comparable"] is True
    assert compare["candidate_dominates"] is False
    assert compare["champion_dominates"] is False
    assert compare["decision"] == "candidate_edge"
    assert compare["economic_objective_profile_id"] == "v4_shared_economic_objective_v1"
    assert compare["economic_objective_context"] == "execution_compare"
    assert "UTILITY_TIE_BREAK_PASS" in compare["reasons"]


def test_compare_spa_like_window_test_prefers_candidate_when_all_window_edges_are_positive() -> None:
    candidate_windows = [
        {"window_index": 0, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0040}}}},
        {"window_index": 1, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0035}}}},
        {"window_index": 2, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0020}}}},
    ]
    champion_windows = [
        {"window_index": 0, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0010}}}},
        {"window_index": 1, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0015}}}},
        {"window_index": 2, "metrics": {"trading": {"top_5pct": {"ev_net": -0.0005}}}},
    ]

    compare = compare_spa_like_window_test(candidate_windows, champion_windows, alpha=0.20)

    assert compare["comparable"] is True
    assert compare["decision"] == "candidate_edge"
    assert compare["window_count"] == 3
    assert "SPA_LIKE_PASS" in compare["reasons"]


def test_compare_spa_like_window_test_requires_common_windows() -> None:
    compare = compare_spa_like_window_test(
        candidate_windows=[{"window_index": 0, "metrics": {"trading": {"top_5pct": {"ev_net": 0.001}}}}],
        champion_windows=[],
    )

    assert compare["comparable"] is False
    assert compare["decision"] == "insufficient_evidence"


def test_compare_spa_like_window_test_supports_distinct_threshold_keys() -> None:
    candidate_windows = [
        {"window_index": 0, "metrics": {"trading": {"ev_opt": {"ev_net": 0.0040}}}},
        {"window_index": 1, "metrics": {"trading": {"ev_opt": {"ev_net": 0.0035}}}},
    ]
    champion_windows = [
        {"window_index": 0, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0010}}}},
        {"window_index": 1, "metrics": {"trading": {"top_5pct": {"ev_net": 0.0015}}}},
    ]

    compare = compare_spa_like_window_test(
        candidate_windows,
        champion_windows,
        candidate_threshold_key="ev_opt",
        champion_threshold_key="top_5pct",
        alpha=0.20,
    )

    assert compare["comparable"] is True
    assert compare["candidate_threshold_key"] == "ev_opt"
    assert compare["champion_threshold_key"] == "top_5pct"
