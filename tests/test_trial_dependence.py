from __future__ import annotations

from autobot.models.trial_dependence import estimate_effective_trials_from_trial_records


def test_estimate_effective_trials_falls_back_without_records() -> None:
    report = estimate_effective_trials_from_trial_records([], fallback_trial_count=7)

    assert report["raw_trial_count"] == 7
    assert report["effective_trials"] == 7
    assert report["source"] == "raw_trial_count_fallback"
    assert report["comparable"] is False


def test_estimate_effective_trials_detects_high_trial_dependence() -> None:
    records = [
        {"trial": 0, "selection_key": {"precision_top5": 0.51, "pr_auc": 0.61, "roc_auc": 0.71}},
        {"trial": 1, "selection_key": {"precision_top5": 0.52, "pr_auc": 0.62, "roc_auc": 0.72}},
        {"trial": 2, "selection_key": {"precision_top5": 0.53, "pr_auc": 0.63, "roc_auc": 0.73}},
        {"trial": 3, "selection_key": {"precision_top5": 0.54, "pr_auc": 0.64, "roc_auc": 0.74}},
    ]

    report = estimate_effective_trials_from_trial_records(records, fallback_trial_count=4)

    assert report["comparable"] is True
    assert report["raw_trial_count"] == 4
    assert report["effective_trials"] < 4
    assert report["metric_count"] == 3
