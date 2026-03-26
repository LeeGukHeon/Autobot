from __future__ import annotations

import numpy as np

from autobot.models.selection_calibration import apply_selection_calibration, build_selection_calibration_by_score_source
from autobot.models.selection_policy import build_selection_policy_from_recommendations


def test_build_selection_policy_can_pin_score_lcb_source() -> None:
    recommendations = {
        "recommended_threshold_key": "top_5pct",
        "recommended_threshold_key_source": "walk_forward_objective_optimizer",
        "by_threshold_key": {
            "top_5pct": {
                "threshold": 0.55,
                "recommended_top_pct": 0.5,
                "recommended_min_candidates_per_ts": 1,
                "objective_score": 0.1,
                "recommendation_source": "walk_forward_objective_optimizer",
            }
        },
    }

    policy = build_selection_policy_from_recommendations(
        selection_recommendations=recommendations,
        score_source="score_lcb",
    )

    assert policy["score_source"] == "score_lcb"


def test_apply_selection_calibration_supports_score_source_specific_payload() -> None:
    calibration = build_selection_calibration_by_score_source(
        by_score_source={
            "score_mean": {
                "mode": "isotonic_oos_v1",
                "x_knots": [0.0, 1.0],
                "y_knots": [0.0, 1.0],
            },
            "score_lcb": {
                "mode": "isotonic_oos_v1",
                "x_knots": [0.0, 1.0],
                "y_knots": [0.1, 0.9],
            },
        },
        default_score_source="score_mean",
    )

    scores = np.asarray([0.0, 1.0], dtype=np.float64)
    resolved = apply_selection_calibration(scores, calibration, score_source="score_lcb")
    assert np.allclose(resolved, np.asarray([0.1, 0.9], dtype=np.float64))
