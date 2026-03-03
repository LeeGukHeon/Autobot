from __future__ import annotations

import numpy as np

from autobot.models.metrics import precision_at_top_p, recall_at_top_p, top_p_threshold, top_k_indices


def test_precision_and_recall_at_top_p() -> None:
    y_true = np.array([1, 0, 1, 0, 1], dtype=np.int8)
    scores = np.array([0.95, 0.80, 0.40, 0.20, 0.70], dtype=np.float64)

    precision = precision_at_top_p(y_true, scores, 0.40)
    recall = recall_at_top_p(y_true, scores, 0.40)
    threshold = top_p_threshold(scores, 0.40)

    assert precision == 0.5
    assert recall == (1.0 / 3.0)
    assert threshold == 0.80


def test_top_k_indices_returns_at_least_one_row() -> None:
    scores = np.array([0.1, 0.2, 0.3], dtype=np.float64)
    idx = top_k_indices(scores, top_p=0.0)
    assert idx.size == 1
