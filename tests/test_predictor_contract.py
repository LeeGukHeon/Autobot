from __future__ import annotations

from pathlib import Path

import numpy as np

from autobot.models.predictor import ModelPredictor


class DummyEstimator:
    def predict_proba(self, x: np.ndarray) -> np.ndarray:
        probs = np.clip(0.6 + (np.asarray(x, dtype=np.float64)[:, 0] * 0.1), 0.0, 1.0)
        return np.column_stack([1.0 - probs, probs])

    def predict_uncertainty(self, x: np.ndarray) -> np.ndarray:
        return np.full(np.asarray(x).shape[0], 0.05, dtype=np.float64)


def test_model_predictor_exports_uncertainty_aware_score_contract() -> None:
    predictor = ModelPredictor(
        run_dir=Path("."),
        model_bundle={"model_type": "v5_panel_ensemble", "scaler": None, "estimator": DummyEstimator()},
        model_ref="run_a",
        model_family="train_v5_panel_ensemble",
        feature_columns=("f1",),
        train_config={},
        thresholds={},
        selection_recommendations={},
    )

    payload = predictor.predict_score_contract(np.asarray([[0.0], [1.0]], dtype=np.float64))

    assert set(payload.keys()) == {"score_mean", "score_std", "score_lcb", "uncertainty_available"}
    assert np.allclose(payload["score_std"], np.asarray([0.05, 0.05], dtype=np.float64))
    assert np.all(payload["score_lcb"] <= payload["score_mean"])
