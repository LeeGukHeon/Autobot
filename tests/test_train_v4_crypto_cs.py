from __future__ import annotations

from types import SimpleNamespace

import numpy as np

from autobot.models.registry import load_json
from autobot.models.train_v1 import _predict_scores
from autobot.models.train_v4_crypto_cs import TrainV4CryptoCsOptions, train_and_register_v4_crypto_cs


class DummyClassifier:
    def predict_proba(self, x: np.ndarray) -> np.ndarray:
        probs = np.full(x.shape[0], 0.8, dtype=np.float64)
        return np.column_stack([1.0 - probs, probs])


class DummyRegressor:
    def predict(self, x: np.ndarray) -> np.ndarray:
        return x[:, 0].astype(np.float64)


def test_predict_scores_supports_predict_only_estimators() -> None:
    scores = _predict_scores(
        {"model_type": "xgboost_regressor", "scaler": None, "estimator": DummyRegressor()},
        np.array([[-2.0], [0.0], [2.0]], dtype=np.float64),
    )

    assert scores.shape == (3,)
    assert 0.0 < scores[0] < scores[1] < scores[2] < 1.0


def test_train_v4_cls_registers_candidate_without_auto_promotion(tmp_path, monkeypatch) -> None:
    dataset = SimpleNamespace(
        rows=3,
        X=np.array([[0.1], [0.2], [0.3]], dtype=np.float64),
        y_cls=np.array([0, 1, 1], dtype=np.int64),
        sample_weight=np.array([1.0, 1.0, 1.0], dtype=np.float64),
        y_reg=np.array([0.0, 0.1, 0.2], dtype=np.float64),
        markets=np.array(["KRW-BTC", "KRW-ETH", "KRW-XRP"], dtype=object),
        selected_markets=("KRW-BTC", "KRW-ETH", "KRW-XRP"),
        feature_names=("f1",),
        ts_ms=np.array([1_000, 2_000, 3_000], dtype=np.int64),
    )
    split_info = SimpleNamespace(valid_start_ts=2_000, test_start_ts=3_000, counts={"train": 1, "valid": 1, "test": 1})
    masks = {
        "train": np.array([True, False, False]),
        "valid": np.array([False, True, False]),
        "test": np.array([False, False, True]),
        "drop": np.array([False, False, False]),
    }

    monkeypatch.setattr("autobot.models.train_v4_crypto_cs._try_import_xgboost", lambda: object())
    monkeypatch.setattr(
        "autobot.models.train_v4_crypto_cs.build_dataset_request",
        lambda **kwargs: SimpleNamespace(**kwargs),
    )
    monkeypatch.setattr(
        "autobot.models.train_v4_crypto_cs.load_feature_spec",
        lambda dataset_root: {"feature_columns": ["f1"]},
    )
    monkeypatch.setattr(
        "autobot.models.train_v4_crypto_cs.load_label_spec",
        lambda dataset_root: {"label_columns": ["y_reg_net_12", "y_cls_topq_12"]},
    )
    monkeypatch.setattr("autobot.models.train_v4_crypto_cs.feature_columns_from_spec", lambda dataset_root: ("f1",))
    monkeypatch.setattr("autobot.models.train_v4_crypto_cs.load_feature_dataset", lambda *args, **kwargs: dataset)
    monkeypatch.setattr(
        "autobot.models.train_v4_crypto_cs.compute_time_splits",
        lambda *args, **kwargs: (np.array(["train", "valid", "test"], dtype=object), split_info),
    )
    monkeypatch.setattr("autobot.models.train_v4_crypto_cs.split_masks", lambda labels: masks)
    monkeypatch.setattr("autobot.models.train_v4_crypto_cs._validate_split_counts", lambda split_masks: None)
    monkeypatch.setattr(
        "autobot.models.train_v4_crypto_cs._fit_booster_sweep_weighted",
        lambda **kwargs: {
            "bundle": {"model_type": "xgboost", "scaler": None, "estimator": DummyClassifier()},
            "best_params": {"max_depth": 2},
            "trials": [],
        },
    )
    monkeypatch.setattr(
        "autobot.models.train_v4_crypto_cs._evaluate_split",
        lambda **kwargs: {
            "classification": {
                "roc_auc": 0.71,
                "pr_auc": 0.61,
                "log_loss": 0.4,
                "brier_score": 0.2,
            },
            "trading": {
                "top_5pct": {
                    "precision": 0.63,
                    "ev_net": 0.0012,
                }
            },
        },
    )
    monkeypatch.setattr("autobot.models.train_v4_crypto_cs._build_thresholds", lambda **kwargs: {"top_5pct": 0.7})
    monkeypatch.setattr(
        "autobot.models.train_v4_crypto_cs.build_data_fingerprint",
        lambda **kwargs: {"manifest_sha256": "abc"},
    )
    monkeypatch.setattr("autobot.models.train_v4_crypto_cs.render_model_card", lambda **kwargs: "# card")

    options = TrainV4CryptoCsOptions(
        dataset_root=tmp_path / "features_v4",
        registry_root=tmp_path / "registry",
        logs_root=tmp_path / "logs",
        model_family="train_v4_crypto_cs",
        tf="5m",
        quote="KRW",
        top_n=20,
        start="2026-03-01",
        end="2026-03-05",
        feature_set="v4",
        label_set="v2",
        task="cls",
        booster_sweep_trials=1,
        seed=7,
        nthread=1,
        batch_rows=128,
        train_ratio=0.6,
        valid_ratio=0.2,
        test_ratio=0.2,
        embargo_bars=0,
        fee_bps_est=5.0,
        safety_bps=1.0,
        ev_scan_steps=10,
        ev_min_selected=1,
        min_rows_for_train=1,
    )

    result = train_and_register_v4_crypto_cs(options)

    assert result.status == "candidate"
    assert result.walk_forward_report_path is not None
    assert result.walk_forward_report_path.exists()
    assert load_json(options.registry_root / options.model_family / "champion.json") == {}
    assert load_json(options.registry_root / options.model_family / "latest_candidate.json")["run_id"] == result.run_id
    assert load_json(options.registry_root / "latest_candidate.json")["run_id"] == result.run_id
    reasons = load_json(result.promotion_path)["reasons"]
    assert "MANUAL_PROMOTION_REQUIRED" in reasons
    assert "NO_EXISTING_CHAMPION" in reasons
    assert "NO_WALK_FORWARD_EVIDENCE" in reasons


def test_train_v4_reg_registers_candidate_without_auto_promotion(tmp_path, monkeypatch) -> None:
    dataset = SimpleNamespace(
        rows=3,
        X=np.array([[-0.3], [0.2], [0.8]], dtype=np.float64),
        y_cls=np.array([0, 1, 1], dtype=np.int64),
        sample_weight=np.array([1.0, 1.0, 1.0], dtype=np.float64),
        y_reg=np.array([-0.02, 0.01, 0.04], dtype=np.float64),
        markets=np.array(["KRW-BTC", "KRW-ETH", "KRW-XRP"], dtype=object),
        selected_markets=("KRW-BTC", "KRW-ETH", "KRW-XRP"),
        feature_names=("f1",),
        ts_ms=np.array([1_000, 2_000, 3_000], dtype=np.int64),
    )
    split_info = SimpleNamespace(valid_start_ts=2_000, test_start_ts=3_000, counts={"train": 1, "valid": 1, "test": 1})
    masks = {
        "train": np.array([True, False, False]),
        "valid": np.array([False, True, False]),
        "test": np.array([False, False, True]),
        "drop": np.array([False, False, False]),
    }

    monkeypatch.setattr("autobot.models.train_v4_crypto_cs._try_import_xgboost", lambda: object())
    monkeypatch.setattr(
        "autobot.models.train_v4_crypto_cs.build_dataset_request",
        lambda **kwargs: SimpleNamespace(**kwargs),
    )
    monkeypatch.setattr(
        "autobot.models.train_v4_crypto_cs.load_feature_spec",
        lambda dataset_root: {"feature_columns": ["f1"]},
    )
    monkeypatch.setattr(
        "autobot.models.train_v4_crypto_cs.load_label_spec",
        lambda dataset_root: {"label_columns": ["y_reg_net_12", "y_cls_topq_12"]},
    )
    monkeypatch.setattr("autobot.models.train_v4_crypto_cs.feature_columns_from_spec", lambda dataset_root: ("f1",))
    monkeypatch.setattr("autobot.models.train_v4_crypto_cs.load_feature_dataset", lambda *args, **kwargs: dataset)
    monkeypatch.setattr(
        "autobot.models.train_v4_crypto_cs.compute_time_splits",
        lambda *args, **kwargs: (np.array(["train", "valid", "test"], dtype=object), split_info),
    )
    monkeypatch.setattr("autobot.models.train_v4_crypto_cs.split_masks", lambda labels: masks)
    monkeypatch.setattr("autobot.models.train_v4_crypto_cs._validate_split_counts", lambda split_masks: None)
    monkeypatch.setattr(
        "autobot.models.train_v4_crypto_cs._fit_booster_sweep_regression",
        lambda **kwargs: {
            "bundle": {"model_type": "xgboost_regressor", "scaler": None, "estimator": DummyRegressor()},
            "best_params": {"max_depth": 2},
            "trials": [],
        },
    )
    monkeypatch.setattr(
        "autobot.models.train_v4_crypto_cs._evaluate_split",
        lambda **kwargs: {
            "classification": {
                "roc_auc": 0.69,
                "pr_auc": 0.58,
                "log_loss": 0.45,
                "brier_score": 0.22,
            },
            "trading": {
                "top_5pct": {
                    "precision": 0.61,
                    "ev_net": 0.0010,
                }
            },
        },
    )
    monkeypatch.setattr("autobot.models.train_v4_crypto_cs._build_thresholds", lambda **kwargs: {"top_5pct": 0.61})
    monkeypatch.setattr(
        "autobot.models.train_v4_crypto_cs.build_data_fingerprint",
        lambda **kwargs: {"manifest_sha256": "abc"},
    )
    monkeypatch.setattr("autobot.models.train_v4_crypto_cs.render_model_card", lambda **kwargs: "# card")

    options = TrainV4CryptoCsOptions(
        dataset_root=tmp_path / "features_v4",
        registry_root=tmp_path / "registry",
        logs_root=tmp_path / "logs",
        model_family="train_v4_crypto_cs",
        tf="5m",
        quote="KRW",
        top_n=20,
        start="2026-03-01",
        end="2026-03-05",
        feature_set="v4",
        label_set="v2",
        task="reg",
        booster_sweep_trials=1,
        seed=7,
        nthread=1,
        batch_rows=128,
        train_ratio=0.6,
        valid_ratio=0.2,
        test_ratio=0.2,
        embargo_bars=0,
        fee_bps_est=5.0,
        safety_bps=1.0,
        ev_scan_steps=10,
        ev_min_selected=1,
        min_rows_for_train=1,
    )

    result = train_and_register_v4_crypto_cs(options)

    assert result.status == "candidate"
    assert result.walk_forward_report_path is not None
    assert result.walk_forward_report_path.exists()
    assert result.leaderboard_row["task"] == "reg"
    assert load_json(options.registry_root / options.model_family / "latest_candidate.json")["run_id"] == result.run_id
    reasons = load_json(result.promotion_path)["reasons"]
    assert "NO_WALK_FORWARD_EVIDENCE" in reasons
