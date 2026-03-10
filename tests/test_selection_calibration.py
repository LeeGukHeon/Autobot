from __future__ import annotations

import numpy as np

from autobot.models.selection_calibration import (
    ISOTONIC_SELECTION_CALIBRATION_MODE,
    apply_selection_calibration,
    build_selection_calibration_from_oos_rows,
)


def test_build_selection_calibration_from_oos_rows_fits_isotonic_artifact() -> None:
    scores = np.linspace(0.05, 0.95, 240, dtype=np.float64)
    labels = (scores >= 0.55).astype(np.int64)
    artifact = build_selection_calibration_from_oos_rows(
        oos_rows=[
            {"window_index": 0, "scores": scores[:120].tolist(), "y_cls": labels[:120].tolist()},
            {"window_index": 1, "scores": scores[120:].tolist(), "y_cls": labels[120:].tolist()},
        ]
    )

    assert artifact["mode"] == ISOTONIC_SELECTION_CALIBRATION_MODE
    assert artifact["comparable"] is True
    calibrated = apply_selection_calibration(scores, artifact)
    assert calibrated.shape == scores.shape
    assert float(np.min(calibrated)) >= 0.0
    assert float(np.max(calibrated)) <= 1.0
    assert artifact["metrics"]["brier_calibrated"] <= artifact["metrics"]["brier_raw"]


def test_build_selection_calibration_from_oos_rows_falls_back_when_insufficient() -> None:
    artifact = build_selection_calibration_from_oos_rows(
        oos_rows=[{"window_index": 0, "scores": [0.8, 0.7], "y_cls": [1, 1]}]
    )
    assert artifact["mode"] == "identity_v1"
    assert artifact["comparable"] is False

