from __future__ import annotations

from types import SimpleNamespace

import numpy as np

from autobot.models.train_v4_models import build_group_level_sample_weight, fit_booster_sweep_ranker
from autobot.models.train_v4_walkforward_trials import fit_walk_forward_ranker_trials


class _RecordingRanker:
    last_fit_kwargs: dict | None = None
    last_fit_y: np.ndarray | None = None

    def __init__(self, **kwargs):
        self.init_kwargs = kwargs

    def fit(self, x, y, **kwargs):
        type(self).last_fit_kwargs = dict(kwargs)
        type(self).last_fit_y = np.asarray(y)
        return self

    def predict(self, x):
        return np.asarray(x[:, 0], dtype=np.float64)


def _fake_xgboost():
    return SimpleNamespace(XGBRanker=_RecordingRanker)


def _ranker_params():
    return {
        "learning_rate": 0.05,
        "max_depth": 4,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "min_child_weight": 1.0,
        "reg_lambda": 1.0,
        "reg_alpha": 0.0,
        "max_bin": 256,
    }


def _group_counts_by_ts(ts_ms: np.ndarray) -> np.ndarray:
    _, counts = np.unique(np.asarray(ts_ms, dtype=np.int64), return_counts=True)
    return counts.astype(np.int64, copy=False)


def test_build_group_level_sample_weight_averages_row_weights_by_query_group() -> None:
    weights = build_group_level_sample_weight(
        np.asarray([1.0, 1.0, 2.0, 2.0, 6.0], dtype=np.float64),
        np.asarray([2, 2, 1], dtype=np.int64),
    )

    assert np.allclose(weights, np.asarray([1.0, 2.0, 6.0], dtype=np.float64))


def test_build_rank_relevance_labels_converts_continuous_rank_scores_to_integers() -> None:
    from autobot.models.train_v4_models import build_rank_relevance_labels

    labels = build_rank_relevance_labels(np.asarray([0.0, 0.125, 0.5, 1.0], dtype=np.float64))

    assert labels.dtype == np.int32
    assert labels.tolist() == [0, 4, 16, 31]


def test_fit_booster_sweep_ranker_uses_group_level_sample_weight() -> None:
    _RecordingRanker.last_fit_kwargs = None
    _RecordingRanker.last_fit_y = None

    fit_booster_sweep_ranker(
        x_train=np.asarray([[0.1], [0.2], [0.3], [0.4]], dtype=np.float64),
        y_train_rank=np.asarray([0.0, 1.0, 0.5, 0.9], dtype=np.float64),
        ts_train_ms=np.asarray([1000, 1000, 2000, 2000], dtype=np.int64),
        w_train=np.asarray([1.0, 1.0, 2.0, 2.0], dtype=np.float64),
        x_valid=np.asarray([[0.5], [0.6]], dtype=np.float64),
        y_valid_cls=np.asarray([0, 1], dtype=np.int64),
        y_valid_reg=np.asarray([0.0, 0.1], dtype=np.float64),
        y_valid_rank=np.asarray([0.2, 0.8], dtype=np.float64),
        ts_valid_ms=np.asarray([3000, 3000], dtype=np.int64),
        w_valid=np.asarray([3.0, 3.0], dtype=np.float64),
        fee_bps_est=5.0,
        safety_bps=1.0,
        seed=7,
        nthread=1,
        trials=1,
        try_import_xgboost_fn=_fake_xgboost,
        sample_xgb_params_fn=lambda rng: dict(_ranker_params()),
        evaluate_split_fn=lambda **kwargs: {"trading": {"top_5pct": {"ev_net": 1.0, "precision": 1.0}}},
        attach_ranking_metrics_fn=lambda **kwargs: kwargs["metrics"],
        build_v4_trainer_sweep_sort_key_fn=lambda metrics, task: (1.0, 1.0, 1.0),
        build_v4_shared_economic_objective_profile_fn=lambda: {"profile_id": "profile-test"},
        group_counts_by_ts_fn=_group_counts_by_ts,
    )

    assert _RecordingRanker.last_fit_kwargs is not None
    assert _RecordingRanker.last_fit_kwargs["group"] == [2, 2]
    assert np.allclose(
        np.asarray(_RecordingRanker.last_fit_kwargs["sample_weight"], dtype=np.float64),
        np.asarray([1.0, 2.0], dtype=np.float64),
    )
    assert _RecordingRanker.last_fit_y is not None
    assert _RecordingRanker.last_fit_y.dtype == np.int32
    assert _RecordingRanker.last_fit_y.tolist() == [0, 31, 16, 28]


def test_fit_walk_forward_ranker_trials_uses_group_level_sample_weight() -> None:
    _RecordingRanker.last_fit_kwargs = None
    _RecordingRanker.last_fit_y = None

    result = fit_walk_forward_ranker_trials(
        options=SimpleNamespace(seed=11, nthread=1, fee_bps_est=5.0, safety_bps=1.0),
        sweep_trials=1,
        x_train=np.asarray([[0.1], [0.2], [0.3], [0.4]], dtype=np.float64),
        y_train_rank=np.asarray([0.0, 1.0, 0.5, 0.9], dtype=np.float64),
        w_train=np.asarray([1.0, 1.0, 2.0, 2.0], dtype=np.float64),
        ts_train_ms=np.asarray([1000, 1000, 2000, 2000], dtype=np.int64),
        x_valid=np.asarray([[0.5], [0.6]], dtype=np.float64),
        y_valid_cls=np.asarray([0, 1], dtype=np.int64),
        y_valid_reg=np.asarray([0.0, 0.1], dtype=np.float64),
        y_valid_rank=np.asarray([0.2, 0.8], dtype=np.float64),
        w_valid=np.asarray([3.0, 3.0], dtype=np.float64),
        ts_valid_ms=np.asarray([3000, 3000], dtype=np.int64),
        x_test=np.asarray([[0.7], [0.8]], dtype=np.float64),
        y_test_cls=np.asarray([0, 1], dtype=np.int64),
        y_test_reg=np.asarray([0.0, 0.1], dtype=np.float64),
        y_test_rank=np.asarray([0.1, 0.9], dtype=np.float64),
        market_test=np.asarray(["KRW-BTC", "KRW-ETH"], dtype=object),
        ts_test_ms=np.asarray([4000, 4000], dtype=np.int64),
        try_import_xgboost_fn=_fake_xgboost,
        sample_xgb_params_fn=lambda rng: dict(_ranker_params()),
        evaluate_split_fn=lambda **kwargs: {"trading": {"top_5pct": {"ev_net": 1.0, "precision": 1.0}}},
        attach_ranking_metrics_fn=lambda **kwargs: kwargs["metrics"],
        build_v4_trainer_sweep_sort_key_fn=lambda metrics, task: (1.0, 1.0, 1.0),
        compact_eval_metrics_fn=lambda metrics: metrics,
        build_oos_period_metrics_fn=lambda **kwargs: [],
        build_oos_slice_metrics_fn=lambda **kwargs: [],
        build_trial_selection_key_fn=lambda metrics: {"ev_net_top5": 1.0},
        group_counts_by_ts_fn=_group_counts_by_ts,
        build_v4_shared_economic_objective_profile_fn=lambda: {"profile_id": "profile-test"},
    )

    assert result["bundle"]["model_type"] == "xgboost_ranker"
    assert _RecordingRanker.last_fit_kwargs is not None
    assert _RecordingRanker.last_fit_kwargs["group"] == [2, 2]
    assert np.allclose(
        np.asarray(_RecordingRanker.last_fit_kwargs["sample_weight"], dtype=np.float64),
        np.asarray([1.0, 2.0], dtype=np.float64),
    )
    assert _RecordingRanker.last_fit_y is not None
    assert _RecordingRanker.last_fit_y.dtype == np.int32
