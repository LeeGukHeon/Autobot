from __future__ import annotations

import numpy as np

from autobot.models.metric_audit import audit_predictions


def test_audit_predictions_fail_on_non_binary_labels() -> None:
    result = audit_predictions(
        y_true=np.array([0, 1, 2], dtype=np.int8),
        y_pred_proba=np.array([0.1, 0.9, 0.8], dtype=np.float64),
    )

    issue_codes = {str(item.get("code")) for item in result.get("issues", [])}
    assert result["status"] == "FAIL"
    assert "NON_BINARY_LABELS" in issue_codes


def test_audit_predictions_fail_on_proba_range() -> None:
    result = audit_predictions(
        y_true=np.array([0, 1], dtype=np.int8),
        y_pred_proba=np.array([-0.1, 1.2], dtype=np.float64),
    )

    issue_codes = {str(item.get("code")) for item in result.get("issues", [])}
    assert result["status"] == "FAIL"
    assert "PROBA_OUT_OF_RANGE" in issue_codes


def test_audit_predictions_warn_on_metric_mismatch() -> None:
    y_true = np.array([0, 1, 0, 1, 1, 0], dtype=np.int8)
    y_pred = np.array([0.1, 0.8, 0.2, 0.9, 0.7, 0.3], dtype=np.float64)

    base = audit_predictions(y_true=y_true, y_pred_proba=y_pred)
    stored = dict(base["recomputed_classification"])
    stored["log_loss"] = float(stored["log_loss"] or 0.0) + 5e-4

    result = audit_predictions(
        y_true=y_true,
        y_pred_proba=y_pred,
        stored_classification=stored,
        tolerance_warn=1e-6,
        tolerance_fail=1e-3,
    )

    issue_severity = [str(item.get("severity")) for item in result.get("issues", [])]
    assert result["status"] == "WARN"
    assert "WARN" in issue_severity
