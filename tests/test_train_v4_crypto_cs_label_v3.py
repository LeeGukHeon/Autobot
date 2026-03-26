from __future__ import annotations

from types import SimpleNamespace

import numpy as np

from autobot.models.train_v4_crypto_cs import TrainV4CryptoCsOptions, train_and_register_v4_crypto_cs


class DummyClassifier:
    def predict_proba(self, x: np.ndarray) -> np.ndarray:
        probs = np.full(x.shape[0], 0.8, dtype=np.float64)
        return np.column_stack([1.0 - probs, probs])


def test_train_v4_crypto_cs_accepts_v3_label_contract_defaults(tmp_path, monkeypatch) -> None:
    captured: dict[str, object] = {}
    dataset = SimpleNamespace(
        rows=3,
        X=np.array([[0.1], [0.2], [0.3]], dtype=np.float64),
        y_cls=np.array([0, 1, 1], dtype=np.int64),
        y_reg=np.array([0.0, 0.1, 0.2], dtype=np.float64),
        y_rank=np.array([0.0, 0.5, 1.0], dtype=np.float64),
        sample_weight=np.array([1.0, 1.0, 1.0], dtype=np.float64),
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
    monkeypatch.setattr("autobot.models.train_v4_crypto_cs.build_dataset_request", lambda **kwargs: SimpleNamespace(**kwargs))
    monkeypatch.setattr("autobot.models.train_v4_crypto_cs.load_feature_spec", lambda dataset_root: {"feature_columns": ["f1"]})
    monkeypatch.setattr(
        "autobot.models.train_v4_crypto_cs.load_label_spec",
        lambda dataset_root: {
            "label_set_version": "v3_crypto_cs_residualized",
            "label_columns": [
                "y_reg_net_h3",
                "y_reg_resid_btc_h3",
                "y_reg_resid_eth_h3",
                "y_reg_resid_leader_h3",
                "y_rank_resid_leader_h3",
                "y_cls_resid_leader_topq_h3",
            ],
            "training_default_columns": {
                "y_reg": "y_reg_resid_leader_h3",
                "y_rank": "y_rank_resid_leader_h3",
                "y_cls": "y_cls_resid_leader_topq_h3",
            },
            "multi_horizon_bars": [3],
            "horizon_bars": 3,
        },
    )
    monkeypatch.setattr("autobot.models.train_v4_crypto_cs.feature_columns_from_spec", lambda dataset_root: ("f1",))

    def _fake_load_feature_dataset(request, *, feature_columns, **kwargs):
        captured["dataset_kwargs"] = dict(kwargs)
        return dataset

    monkeypatch.setattr("autobot.models.train_v4_crypto_cs.load_feature_dataset", _fake_load_feature_dataset)
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
            "classification": {"roc_auc": 0.7, "pr_auc": 0.6, "log_loss": 0.4, "brier_score": 0.2},
            "trading": {"top_5pct": {"precision": 0.6, "ev_net": 0.001}},
        },
    )
    monkeypatch.setattr("autobot.models.train_v4_crypto_cs._build_thresholds", lambda **kwargs: {"top_5pct": 0.7})
    monkeypatch.setattr("autobot.models.train_v4_crypto_cs.build_data_fingerprint", lambda **kwargs: {"manifest_sha256": "abc"})
    monkeypatch.setattr("autobot.models.train_v4_crypto_cs.render_model_card", lambda **kwargs: "# card")

    options = TrainV4CryptoCsOptions(
        dataset_root=tmp_path / "features_v4",
        registry_root=tmp_path / "registry",
        logs_root=tmp_path / "logs",
        execution_acceptance_output_root=tmp_path / "logs" / "train_v4_execution_backtest",
        model_family="train_v4_crypto_cs",
        tf="5m",
        quote="KRW",
        top_n=20,
        start="2026-03-01",
        end="2026-03-05",
        feature_set="v4",
        label_set="v3",
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

    train_and_register_v4_crypto_cs(options)

    assert captured["dataset_kwargs"] == {
        "y_cls_column": "y_cls_resid_leader_topq_h3",
        "y_reg_column": "y_reg_resid_leader_h3",
        "y_rank_column": "y_rank_resid_leader_h3",
    }
