from __future__ import annotations

from autobot.models.selection_policy import build_selection_policy_from_recommendations


def test_build_selection_policy_can_force_ev_opt_threshold_key() -> None:
    recommendations = {
        "recommended_threshold_key": "top_1pct",
        "recommended_threshold_key_source": "walk_forward_objective_optimizer",
        "by_threshold_key": {
            "top_1pct": {
                "threshold": 0.99,
                "recommended_top_pct": 0.5,
                "recommended_min_candidates_per_ts": 1,
                "eligible_ratio": 0.01,
                "recommendation_source": "walk_forward_objective_optimizer",
                "objective": "precision",
            },
            "ev_opt": {
                "threshold": 0.77,
                "recommended_top_pct": 1.0,
                "recommended_min_candidates_per_ts": 1,
                "eligible_ratio": 0.035,
                "recommendation_source": "walk_forward_objective_optimizer",
                "objective": "ev_net",
            },
        },
    }

    policy = build_selection_policy_from_recommendations(
        selection_recommendations=recommendations,
        fallback_threshold_key="top_5pct",
        forced_threshold_key="ev_opt",
    )

    assert policy["threshold_key"] == "ev_opt"
    assert policy["selection_fraction"] == 0.035
    assert policy["selection_recommendation_source"] == "forced_threshold_key:ev_opt"
    assert policy["forced_threshold_key"] == "ev_opt"


def test_build_selection_policy_uses_recommended_threshold_key_when_not_forced() -> None:
    recommendations = {
        "recommended_threshold_key": "top_1pct",
        "recommended_threshold_key_source": "walk_forward_objective_optimizer",
        "by_threshold_key": {
            "top_1pct": {
                "threshold": 0.99,
                "recommended_top_pct": 0.5,
                "recommended_min_candidates_per_ts": 1,
                "eligible_ratio": 0.01,
                "recommendation_source": "walk_forward_objective_optimizer",
                "objective": "precision",
            },
            "ev_opt": {
                "threshold": 0.77,
                "recommended_top_pct": 1.0,
                "recommended_min_candidates_per_ts": 1,
                "eligible_ratio": 0.035,
                "recommendation_source": "walk_forward_objective_optimizer",
                "objective": "ev_net",
            },
        },
    }

    policy = build_selection_policy_from_recommendations(
        selection_recommendations=recommendations,
        fallback_threshold_key="top_5pct",
    )

    assert policy["threshold_key"] == "top_1pct"
    assert policy["forced_threshold_key"] == ""
