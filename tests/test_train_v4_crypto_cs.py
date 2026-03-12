from __future__ import annotations

import json
from types import SimpleNamespace

import numpy as np

from autobot.models.factor_block_selector import FactorBlockDefinition
from autobot.models.registry import load_json
from autobot.models.train_v1 import _predict_scores
from autobot.models.train_v4_crypto_cs import (
    TrainV4CryptoCsOptions,
    _build_lane_governance_v4,
    _build_research_support_lane_v4,
    _evaluate_factor_block_refit_window_evidence,
    _evaluate_factor_block_refit_window_rows,
    _build_selection_search_trial_panel,
    _summarize_walk_forward_trial_panel,
    train_and_register_v4_crypto_cs,
)
from autobot.strategy.model_alpha_v1 import (
    ModelAlphaExitSettings,
    ModelAlphaSelectionSettings,
    ModelAlphaSettings,
)


class DummyClassifier:
    def predict_proba(self, x: np.ndarray) -> np.ndarray:
        probs = np.full(x.shape[0], 0.8, dtype=np.float64)
        return np.column_stack([1.0 - probs, probs])


class DummyRegressor:
    def predict(self, x: np.ndarray) -> np.ndarray:
        return x[:, 0].astype(np.float64)


class DummyRanker:
    def predict(self, x: np.ndarray) -> np.ndarray:
        return (x[:, 0].astype(np.float64) * 2.0) - 0.1


def test_predict_scores_supports_predict_only_estimators() -> None:
    scores = _predict_scores(
        {"model_type": "xgboost_regressor", "scaler": None, "estimator": DummyRegressor()},
        np.array([[-2.0], [0.0], [2.0]], dtype=np.float64),
    )

    assert scores.shape == (3,)
    assert 0.0 < scores[0] < scores[1] < scores[2] < 1.0


def test_predict_scores_supports_ranker_estimators() -> None:
    scores = _predict_scores(
        {"model_type": "xgboost_ranker", "scaler": None, "estimator": DummyRanker()},
        np.array([[-2.0], [0.0], [2.0]], dtype=np.float64),
    )

    assert scores.shape == (3,)
    assert 0.0 < scores[0] < scores[1] < scores[2] < 1.0


def test_evaluate_factor_block_refit_window_rows_marks_refit_evidence(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        "autobot.models.train_v4_crypto_cs._predict_scores",
        lambda bundle, x: np.clip(0.45 + (np.asarray(x, dtype=np.float64)[:, 0] * 0.2), 0.01, 0.99),
    )
    monkeypatch.setattr(
        "autobot.models.train_v4_crypto_cs._evaluate_split",
        lambda **kwargs: {
            "trading": {
                "top_5pct": {
                    "ev_net": float(np.mean(np.asarray(kwargs["scores"], dtype=np.float64))),
                    "precision": float(np.mean(np.asarray(kwargs["scores"], dtype=np.float64))),
                }
            }
        },
    )
    monkeypatch.setattr(
        "autobot.models.train_v4_crypto_cs._attach_ranking_metrics",
        lambda **kwargs: kwargs["metrics"],
    )
    monkeypatch.setattr(
        "autobot.models.train_v4_crypto_cs._fit_fixed_classifier_model",
        lambda **kwargs: {"model_type": "xgboost", "scaler": None, "estimator": DummyClassifier()},
    )

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

    rows = _evaluate_factor_block_refit_window_rows(
        window_index=2,
        task="cls",
        options=options,
        best_params={"max_depth": 2},
        full_bundle={"model_type": "xgboost", "scaler": None, "estimator": DummyClassifier()},
        feature_names=("f1", "f2"),
        x_train=np.asarray([[0.1, 0.2], [0.2, 0.4], [0.3, 0.6], [0.4, 0.8]], dtype=np.float32),
        y_cls_train=np.asarray([0, 1, 0, 1], dtype=np.int64),
        y_reg_train=np.asarray([0.0, 0.1, 0.0, 0.2], dtype=np.float32),
        y_rank_train=np.asarray([0.0, 0.1, 0.0, 0.2], dtype=np.float32),
        w_train=np.ones(4, dtype=np.float64),
        ts_train_ms=np.asarray([1_000, 2_000, 3_000, 4_000], dtype=np.int64),
        x_valid=np.asarray([[0.5, 1.0], [0.6, 1.2]], dtype=np.float32),
        y_valid_cls=np.asarray([1, 0], dtype=np.int64),
        y_valid_reg=np.asarray([0.2, -0.1], dtype=np.float32),
        y_valid_rank=np.asarray([0.2, -0.1], dtype=np.float32),
        w_valid=np.ones(2, dtype=np.float64),
        ts_valid_ms=np.asarray([5_000, 6_000], dtype=np.int64),
        x_test=np.asarray([[0.7, 1.4], [0.8, 1.6], [0.9, 1.8], [1.0, 2.0]], dtype=np.float32),
        y_test_cls=np.asarray([1, 0, 1, 0], dtype=np.int64),
        y_test_reg=np.asarray([0.1, -0.1, 0.2, -0.2], dtype=np.float32),
        y_test_rank=np.asarray([0.1, -0.1, 0.2, -0.2], dtype=np.float32),
        ts_test_ms=np.asarray([7_000, 7_000, 8_000, 8_000], dtype=np.int64),
        thresholds={"top_5pct": 0.5},
        block_registry=[
            FactorBlockDefinition(
                block_id="v4_optional_test",
                label="optional test",
                feature_columns=("f2",),
                protected=False,
                source_contracts=("tests.synthetic",),
            )
        ],
    )

    assert len(rows) == 1
    assert rows[0]["block_id"] == "v4_optional_test"
    assert rows[0]["evidence_mode"] == "refit_drop_block"
    assert rows[0]["diagnostic_only"] is False


def test_evaluate_factor_block_refit_window_evidence_records_nonfatal_refit_failure(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        "autobot.models.train_v4_crypto_cs._predict_scores",
        lambda bundle, x: np.clip(0.45 + (np.asarray(x, dtype=np.float64)[:, 0] * 0.2), 0.01, 0.99),
    )
    monkeypatch.setattr(
        "autobot.models.train_v4_crypto_cs._evaluate_split",
        lambda **kwargs: {
            "trading": {
                "top_5pct": {
                    "ev_net": float(np.mean(np.asarray(kwargs["scores"], dtype=np.float64))),
                    "precision": float(np.mean(np.asarray(kwargs["scores"], dtype=np.float64))),
                }
            }
        },
    )
    monkeypatch.setattr(
        "autobot.models.train_v4_crypto_cs._attach_ranking_metrics",
        lambda **kwargs: kwargs["metrics"],
    )
    monkeypatch.setattr(
        "autobot.models.train_v4_crypto_cs._fit_fixed_classifier_model",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("refit failed")),
    )

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

    evidence = _evaluate_factor_block_refit_window_evidence(
        window_index=2,
        task="cls",
        options=options,
        best_params={"max_depth": 2},
        full_bundle={"model_type": "xgboost", "scaler": None, "estimator": DummyClassifier()},
        feature_names=("f1", "f2"),
        x_train=np.asarray([[0.1, 0.2], [0.2, 0.4], [0.3, 0.6], [0.4, 0.8]], dtype=np.float32),
        y_cls_train=np.asarray([0, 1, 0, 1], dtype=np.int64),
        y_reg_train=np.asarray([0.0, 0.1, 0.0, 0.2], dtype=np.float32),
        y_rank_train=np.asarray([0.0, 0.1, 0.0, 0.2], dtype=np.float32),
        w_train=np.ones(4, dtype=np.float64),
        ts_train_ms=np.asarray([1_000, 2_000, 3_000, 4_000], dtype=np.int64),
        x_valid=np.asarray([[0.5, 1.0], [0.6, 1.2]], dtype=np.float32),
        y_valid_cls=np.asarray([1, 0], dtype=np.int64),
        y_valid_reg=np.asarray([0.2, -0.1], dtype=np.float32),
        y_valid_rank=np.asarray([0.2, -0.1], dtype=np.float32),
        w_valid=np.ones(2, dtype=np.float64),
        ts_valid_ms=np.asarray([5_000, 6_000], dtype=np.int64),
        x_test=np.asarray([[0.7, 1.4], [0.8, 1.6], [0.9, 1.8], [1.0, 2.0]], dtype=np.float32),
        y_test_cls=np.asarray([1, 0, 1, 0], dtype=np.int64),
        y_test_reg=np.asarray([0.1, -0.1, 0.2, -0.2], dtype=np.float32),
        y_test_rank=np.asarray([0.1, -0.1, 0.2, -0.2], dtype=np.float32),
        ts_test_ms=np.asarray([7_000, 7_000, 8_000, 8_000], dtype=np.int64),
        thresholds={"top_5pct": 0.5},
        block_registry=[
            FactorBlockDefinition(
                block_id="v4_optional_test",
                label="optional test",
                feature_columns=("f2",),
                protected=False,
                source_contracts=("tests.synthetic",),
            )
        ],
    )

    assert evidence["rows"] == []
    assert evidence["support"]["status"] == "insufficient"
    assert evidence["support"]["by_block"]["v4_optional_test"]["reason_codes"] == ["REFIT_MODEL_FAILED_RUNTIMEERROR"]


def test_build_research_support_lane_summarizes_multiple_testing_and_cpcv() -> None:
    support_lane = _build_research_support_lane_v4(
        walk_forward={
            "summary": {"windows_run": 4},
            "multiple_testing_panel_diagnostics": {
                "comparable": True,
                "common_panel_key_count": 3,
                "reasons": [],
            },
            "spa_like_window_test": {
                "policy": "spa_like_window_ev_net",
                "decision": "candidate_edge",
                "comparable": True,
                "reasons": ["SPA_LIKE_PASS"],
                "window_count": 4,
            },
            "white_reality_check": {
                "policy": "white_reality_check",
                "decision": "candidate_edge",
                "comparable": True,
                "reasons": ["WHITE_RC_PASS"],
                "panel_diagnostics": {"common_panel_key_count": 3, "reasons": []},
            },
            "hansen_spa": {
                "policy": "hansen_spa",
                "decision": "indeterminate",
                "comparable": True,
                "reasons": ["HANSEN_SPA_HOLD"],
                "panel_diagnostics": {"common_panel_key_count": 3, "reasons": []},
            },
        },
        cpcv_lite={
            "enabled": True,
            "trigger": "guarded_policy",
            "summary": {
                "status": "partial",
                "reasons": ["BUDGET_CUT", "INSUFFICIENT_COMPARABLE_FOLDS"],
            },
            "pbo": {"comparable": True},
            "dsr": {"comparable": True},
            "insufficiency_reasons": ["BUDGET_CUT"],
        },
    )

    assert support_lane["policy"] == "v4_certification_support_lane_v1"
    assert support_lane["summary"]["status"] == "supported"
    assert support_lane["summary"]["multiple_testing_supported"] is True
    assert support_lane["summary"]["cpcv_lite_status"] == "partial"
    assert support_lane["white_rc"]["panel_diagnostics"]["common_panel_key_count"] == 3
    assert "BUDGET_CUT" in support_lane["cpcv_lite"]["insufficiency_reasons"]


def test_build_lane_governance_allows_rank_governed_primary_after_shadow_pass() -> None:
    governance = _build_lane_governance_v4(
        task="rank",
        run_scope="scheduled_daily_rank_governed",
        economic_objective_profile={"profile_id": "v4_shared_economic_objective_v3"},
    )

    assert governance["lane_id"] == "rank_governed_primary"
    assert governance["shadow_only"] is False
    assert governance["promotion_allowed"] is True
    assert governance["governance_reasons"] == ["AUTO_GOVERNED_FROM_RANK_SHADOW_PASS"]


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
        execution_acceptance_output_root=tmp_path / "logs" / "train_v4_execution_backtest",
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
    selection_doc = load_json(result.run_dir / "selection_recommendations.json")
    selection_policy_doc = load_json(result.run_dir / "selection_policy.json")
    selection_calibration_doc = load_json(result.run_dir / "selection_calibration.json")
    train_config_doc = load_json(result.run_dir / "train_config.yaml")
    research_evidence_doc = load_json(result.run_dir / "trainer_research_evidence.json")
    decision_surface_doc = load_json(result.run_dir / "decision_surface.json")
    assert "by_threshold_key" in selection_doc
    assert selection_doc["version"] == 2
    assert selection_doc["optimizer"]["method"] == "walk_forward_grid_search"
    assert selection_doc["recommended_threshold_key"] == "top_5pct"
    assert selection_policy_doc["mode"] == "rank_effective_quantile"
    assert float(selection_policy_doc["selection_fraction"]) > 0.0
    assert int(selection_calibration_doc["version"]) == 1
    assert train_config_doc["selection_policy"]["mode"] == "rank_effective_quantile"
    assert int(train_config_doc["selection_calibration"]["version"]) == 1
    assert train_config_doc["factor_block_selection"]["refit_support"]["summary"]["status"] == "not_applicable"
    assert train_config_doc["research_support_lane"]["policy"] == "v4_certification_support_lane_v1"
    assert result.trainer_research_evidence_path is not None
    assert result.trainer_research_evidence_path.exists()
    assert research_evidence_doc["policy"] == "v4_trainer_research_evidence_v1"
    assert result.economic_objective_profile_path is not None
    assert result.economic_objective_profile_path.exists()
    assert result.decision_surface_path is not None
    assert result.decision_surface_path.exists()
    assert decision_surface_doc["policy"] == "v4_decision_surface_v1"
    assert decision_surface_doc["trainer_entrypoint"]["task"] == "cls"
    assert decision_surface_doc["selection_runtime_contract"]["selection_policy_mode"] == "rank_effective_quantile"
    assert decision_surface_doc["economic_objective_contract"]["profile_id"] == "v4_shared_economic_objective_v3"
    assert decision_surface_doc["search_budget_contract"]["lane_class_requested"] == "promotion_eligible"
    assert decision_surface_doc["search_budget_contract"]["lane_class_effective"] == "scout"
    assert decision_surface_doc["search_budget_contract"]["budget_contract_id"] == "v4_promotion_eligible_budget_v1"
    assert decision_surface_doc["search_budget_contract"]["promotion_eligible_satisfied"] is False
    assert decision_surface_doc["factor_block_contract"]["refit_support_status"] == "not_applicable"
    assert decision_surface_doc["research_support_contract"]["policy"] == "v4_certification_support_lane_v1"
    assert result.metrics["economic_objective"]["profile_id"] == "v4_shared_economic_objective_v3"
    assert "TRAINER_RESEARCH_PRIOR_IS_TRAIN_PRODUCED" in decision_surface_doc["known_methodology_warnings"]
    assert decision_surface_doc["promotion_contract"]["trainer_evidence_source"] == "certification_artifact.research_evidence"
    assert decision_surface_doc["promotion_contract"]["trainer_research_prior_path"] == "trainer_research_evidence.json"
    assert decision_surface_doc["promotion_contract"]["trainer_research_prior_role"] == "audit_only_prior"
    assert research_evidence_doc["support_lane"]["policy"] == "v4_certification_support_lane_v1"
    assert result.walk_forward_report_path is not None
    assert result.walk_forward_report_path.exists()
    assert load_json(options.registry_root / options.model_family / "champion.json") == {}
    assert load_json(options.registry_root / options.model_family / "latest_candidate.json")["run_id"] == result.run_id
    assert load_json(options.registry_root / "latest_candidate.json")["run_id"] == result.run_id
    reasons = load_json(result.promotion_path)["reasons"]
    assert "MANUAL_PROMOTION_REQUIRED" in reasons
    assert "NO_EXISTING_CHAMPION" in reasons
    assert "NO_WALK_FORWARD_EVIDENCE" in reasons


def test_train_v4_manual_scope_keeps_latest_candidate_pointers_clean(tmp_path, monkeypatch) -> None:
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
        execution_acceptance_output_root=tmp_path / "logs" / "train_v4_execution_backtest",
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
        run_scope="manual_daily",
    )

    result = train_and_register_v4_crypto_cs(options)

    assert result.status == "candidate"
    assert result.train_report_path.name == "train_v4_report.manual_daily.json"
    assert load_json(result.train_report_path)["run_scope"] == "manual_daily"
    assert result.experiment_ledger_path is not None
    assert result.experiment_ledger_path.name == "experiment_ledger.manual_daily.jsonl"
    assert result.experiment_ledger_summary_path is not None
    assert result.experiment_ledger_summary_path.name == "latest_experiment_ledger_summary.manual_daily.json"
    assert not (options.registry_root / options.model_family / "latest_candidate.json").exists()
    assert not (options.registry_root / "latest_candidate.json").exists()


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
        execution_acceptance_output_root=tmp_path / "logs" / "train_v4_execution_backtest",
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
    selection_doc = load_json(result.run_dir / "selection_recommendations.json")
    assert "by_threshold_key" in selection_doc
    assert selection_doc["version"] == 2
    assert selection_doc["optimizer"]["method"] == "walk_forward_grid_search"
    assert selection_doc["recommended_threshold_key"] == "top_5pct"
    assert result.walk_forward_report_path is not None
    assert result.walk_forward_report_path.exists()
    assert result.leaderboard_row["task"] == "reg"
    assert load_json(options.registry_root / options.model_family / "latest_candidate.json")["run_id"] == result.run_id
    reasons = load_json(result.promotion_path)["reasons"]
    assert "NO_WALK_FORWARD_EVIDENCE" in reasons


def test_train_v4_rank_registers_candidate_without_runtime_contract_change(tmp_path, monkeypatch) -> None:
    dataset = SimpleNamespace(
        rows=4,
        X=np.array([[0.1], [0.2], [0.4], [0.8]], dtype=np.float64),
        y_cls=np.array([0, 1, 0, 1], dtype=np.int64),
        sample_weight=np.array([1.0, 1.0, 1.0, 1.0], dtype=np.float64),
        y_reg=np.array([0.0, 0.02, -0.01, 0.05], dtype=np.float64),
        y_rank=np.array([0.0, 1.0, 0.5, 1.0], dtype=np.float64),
        markets=np.array(["KRW-BTC", "KRW-ETH", "KRW-BTC", "KRW-ETH"], dtype=object),
        selected_markets=("KRW-BTC", "KRW-ETH"),
        feature_names=("f1",),
        ts_ms=np.array([1_000, 1_000, 2_000, 2_000], dtype=np.int64),
    )
    split_info = SimpleNamespace(valid_start_ts=1_000, test_start_ts=2_000, counts={"train": 2, "valid": 0, "test": 2})
    masks = {
        "train": np.array([True, True, False, False]),
        "valid": np.array([False, False, True, False]),
        "test": np.array([False, False, False, True]),
        "drop": np.array([False, False, False, False]),
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
        lambda dataset_root: {"label_columns": ["y_reg_net_12", "y_rank_cs_12", "y_cls_topq_12"]},
    )
    monkeypatch.setattr("autobot.models.train_v4_crypto_cs.feature_columns_from_spec", lambda dataset_root: ("f1",))
    monkeypatch.setattr("autobot.models.train_v4_crypto_cs.load_feature_dataset", lambda *args, **kwargs: dataset)
    monkeypatch.setattr(
        "autobot.models.train_v4_crypto_cs.compute_time_splits",
        lambda *args, **kwargs: (np.array(["train", "train", "valid", "test"], dtype=object), split_info),
    )
    monkeypatch.setattr("autobot.models.train_v4_crypto_cs.split_masks", lambda labels: masks)
    monkeypatch.setattr("autobot.models.train_v4_crypto_cs._validate_split_counts", lambda split_masks: None)
    monkeypatch.setattr(
        "autobot.models.train_v4_crypto_cs._fit_booster_sweep_ranker",
        lambda **kwargs: {
            "bundle": {"model_type": "xgboost_ranker", "scaler": None, "estimator": DummyRanker()},
            "best_params": {"max_depth": 2},
            "objective": "rank:pairwise",
            "grouping": {"query_key": "ts_ms"},
            "trials": [],
        },
    )
    monkeypatch.setattr(
        "autobot.models.train_v4_crypto_cs._evaluate_split",
        lambda **kwargs: {
            "classification": {
                "roc_auc": 0.70,
                "pr_auc": 0.62,
                "log_loss": 0.39,
                "brier_score": 0.19,
            },
            "trading": {
                "top_5pct": {
                    "precision": 0.65,
                    "ev_net": 0.0013,
                }
            },
        },
    )
    monkeypatch.setattr("autobot.models.train_v4_crypto_cs._build_thresholds", lambda **kwargs: {"top_5pct": 0.66})
    monkeypatch.setattr(
        "autobot.models.train_v4_crypto_cs.build_data_fingerprint",
        lambda **kwargs: {"manifest_sha256": "abc"},
    )
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
        label_set="v2",
        task="rank",
        booster_sweep_trials=5,
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
    assert result.leaderboard_row["task"] == "rank"
    assert result.leaderboard_row["champion_backend"] == "xgboost_ranker"
    assert result.leaderboard_row["test_ndcg_at5"] is not None
    assert result.lane_governance_path is not None
    assert result.lane_governance_path.exists()
    selection_doc = load_json(result.run_dir / "selection_recommendations.json")
    lane_governance_doc = load_json(result.lane_governance_path)
    decision_surface_doc = load_json(result.run_dir / "decision_surface.json")
    assert selection_doc["recommended_threshold_key"] == "top_5pct"
    assert lane_governance_doc["lane_id"] == "rank_shadow"
    assert lane_governance_doc["shadow_only"] is True
    assert lane_governance_doc["promotion_allowed"] is False
    assert decision_surface_doc["lane_governance"]["lane_id"] == "rank_shadow"
    assert "LANE_GOVERNANCE_SHADOW_ONLY" in decision_surface_doc["known_methodology_warnings"]
    reasons = load_json(result.promotion_path)["reasons"]
    assert "MANUAL_PROMOTION_REQUIRED" in reasons


def test_train_v4_cls_writes_execution_acceptance_report_when_enabled(tmp_path, monkeypatch) -> None:
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
    captured: dict[str, object] = {}

    exec_runs_root = tmp_path / "logs" / "train_v4_execution_backtest" / "runs"
    candidate_exec_run = exec_runs_root / "backtest-candidate"
    champion_exec_run = exec_runs_root / "backtest-champion"
    hold_exec_run = exec_runs_root / "backtest-hold"
    risk_exec_run = exec_runs_root / "backtest-risk"
    execution_exec_run = exec_runs_root / "backtest-execution"
    for path in (candidate_exec_run, champion_exec_run, hold_exec_run, risk_exec_run, execution_exec_run):
        path.mkdir(parents=True, exist_ok=True)
        (path / "summary.json").write_text("{}", encoding="utf-8")

    def _fake_run_execution_acceptance(options):
        captured["options"] = options
        return {
            "policy": "balanced_pareto_execution",
            "enabled": True,
            "status": "compared",
            "compare_to_champion": {"decision": "candidate_edge", "comparable": True},
            "candidate_summary": {"orders_filled": 9, "run_dir": str(candidate_exec_run)},
            "champion_summary": {"orders_filled": 7, "run_dir": str(champion_exec_run)},
        }

    monkeypatch.setattr("autobot.models.train_v4_crypto_cs.run_execution_acceptance", _fake_run_execution_acceptance)
    monkeypatch.setattr(
        "autobot.models.train_v4_crypto_cs.optimize_runtime_recommendations",
        lambda **kwargs: {
            "version": 1,
            "status": "ready",
            "exit": {
                "recommended_exit_mode": "risk",
                "recommended_exit_mode_source": "execution_backtest_grid_search_compare",
                "recommended_exit_mode_reason_code": "RISK_EXECUTION_COMPARE_EDGE",
                "recommended_hold_bars": 12,
                "recommendation_source": "execution_backtest_grid_search",
                "summary": {"run_dir": str(hold_exec_run)},
                "grid_point": {"hold_bars": 12},
                "recommended_risk_scaling_mode": "volatility_scaled",
                "recommended_risk_vol_feature": "rv_36",
                "recommended_tp_vol_multiplier": 2.5,
                "recommended_sl_vol_multiplier": 1.5,
                "recommended_trailing_vol_multiplier": 0.75,
                "risk_summary": {"run_dir": str(risk_exec_run)},
                "risk_grid_point": {
                    "hold_bars": 12,
                    "risk_scaling_mode": "volatility_scaled",
                    "risk_vol_feature": "rv_36",
                    "tp_vol_multiplier": 2.5,
                    "sl_vol_multiplier": 1.5,
                    "trailing_vol_multiplier": 0.75,
                },
            },
            "execution": {
                "recommended_price_mode": "JOIN",
                "recommended_timeout_bars": 2,
                "recommended_replace_max": 1,
                "recommendation_source": "execution_backtest_grid_search",
                "summary": {"run_dir": str(execution_exec_run)},
            },
        },
    )
    monkeypatch.setattr(
        "autobot.models.train_v4_crypto_cs.build_trade_action_policy_from_oos_rows",
        lambda **kwargs: {
            "version": 1,
            "policy": "trade_level_hold_risk_oos_bins_v1",
            "status": "ready",
            "source": "walk_forward_oos_trade_replay",
            "risk_feature_name": "rv_36",
            "runtime_decision_source": "continuous_conditional_action_value",
            "state_feature_names": ["selection_score", "rv_12", "rv_36", "atr_pct_14"],
            "tail_confidence_level": 0.9,
            "ctm_order": 2,
            "tail_risk_contract": {"method": "conditional_linear_quantile_tail_v2"},
            "conditional_action_model": {
                "status": "ready",
                "model": "conditional_action_linear_quantile_tail_v2",
            },
            "by_bin": [],
        },
    )

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
        execution_acceptance_enabled=True,
        execution_acceptance_top_n=11,
        execution_acceptance_model_alpha=ModelAlphaSettings(
            selection=ModelAlphaSelectionSettings(top_pct=0.5, min_prob=0.0, min_candidates_per_ts=1),
            exit=ModelAlphaExitSettings(hold_bars=9),
        ),
    )

    result = train_and_register_v4_crypto_cs(options)
    promotion = load_json(result.promotion_path)
    execution_doc = load_json(result.run_dir / "execution_acceptance_report.json")
    runtime_doc = load_json(result.run_dir / "runtime_recommendations.json")
    research_evidence_doc = load_json(result.run_dir / "trainer_research_evidence.json")
    decision_surface_doc = load_json(result.run_dir / "decision_surface.json")

    assert result.execution_acceptance_report_path is not None
    assert result.execution_acceptance_report_path.exists()
    assert result.runtime_recommendations_path is not None
    assert result.runtime_recommendations_path.exists()
    assert result.trainer_research_evidence_path is not None
    assert result.trainer_research_evidence_path.exists()
    assert result.decision_surface_path is not None
    assert result.decision_surface_path.exists()
    assert execution_doc["status"] == "compared"
    assert execution_doc["artifacts_cleanup"]["removed_count"] == 5
    assert runtime_doc["artifacts_cleanup"]["removed_count"] == 5
    assert runtime_doc["exit"]["recommended_exit_mode"] == "risk"
    assert runtime_doc["exit"]["recommended_hold_bars"] == 12
    assert runtime_doc["exit"]["recommended_risk_scaling_mode"] == "volatility_scaled"
    assert runtime_doc["exit"]["recommended_risk_vol_feature"] == "rv_36"
    assert runtime_doc["exit"]["recommended_tp_vol_multiplier"] == 2.5
    assert runtime_doc["exit"]["recommended_sl_vol_multiplier"] == 1.5
    assert runtime_doc["exit"]["recommended_trailing_vol_multiplier"] == 0.75
    assert runtime_doc["trade_action"]["status"] == "ready"
    assert runtime_doc["trade_action"]["risk_feature_name"] == "rv_36"
    assert candidate_exec_run.exists() is False
    assert champion_exec_run.exists() is False
    assert hold_exec_run.exists() is False
    assert risk_exec_run.exists() is False
    assert execution_exec_run.exists() is False
    assert promotion["execution_acceptance"]["compare_to_champion"]["decision"] == "candidate_edge"
    assert "EXECUTION_BALANCED_PARETO_PASS" in promotion["reasons"]
    assert research_evidence_doc["execution"]["decision"] == "candidate_edge"
    assert "EXECUTION_ACCEPTANCE_REUSES_TRAIN_WINDOW" in decision_surface_doc["known_methodology_warnings"]
    assert "RUNTIME_RECOMMENDATIONS_REUSE_TRAIN_WINDOW" in decision_surface_doc["known_methodology_warnings"]
    assert decision_surface_doc["execution_acceptance_contract"]["selection_use_learned_recommendations"] is False
    assert decision_surface_doc["runtime_recommendation_contract"]["selection_use_learned_recommendations"] is True
    assert decision_surface_doc["runtime_recommendation_contract"]["trade_action_policy_status"] == "ready"
    assert decision_surface_doc["runtime_recommendation_contract"]["trade_action_risk_feature_name"] == "rv_36"
    assert decision_surface_doc["runtime_recommendation_contract"]["trade_action_runtime_decision_source"] == "continuous_conditional_action_value"
    assert decision_surface_doc["runtime_recommendation_contract"]["trade_action_tail_confidence_level"] == 0.9
    assert decision_surface_doc["runtime_recommendation_contract"]["trade_action_conditional_model"] == "conditional_action_linear_quantile_tail_v2"
    assert decision_surface_doc["promotion_contract"]["trainer_evidence_source"] == "certification_artifact.research_evidence"
    assert decision_surface_doc["promotion_contract"]["trainer_research_prior_path"] == "trainer_research_evidence.json"
    execution_options = captured["options"]
    assert execution_options.top_n == 11
    assert execution_options.model_alpha_settings.selection.top_pct == 0.5
    assert execution_options.model_alpha_settings.selection.min_prob == 0.0
    assert execution_options.model_alpha_settings.selection.min_candidates_per_ts == 1
    assert execution_options.model_alpha_settings.selection.use_learned_recommendations is False
    assert execution_options.model_alpha_settings.exit.hold_bars == 9


def test_summarize_walk_forward_trial_panel_flattens_oos_slices() -> None:
    windows = [
        {
            "window_index": 0,
            "time_window": {"test_start_ts": 1000, "test_end_ts": 2000},
            "trial_records": [
                {
                    "trial": 0,
                    "params": {"max_depth": 4},
                    "test_metrics": {"trading": {"top_5pct": {"ev_net": 0.01}}},
                    "test_oos_periods": [
                        {"period_index": 0, "ts_ms": 1000, "metrics": {"trading": {"top_5pct": {"ev_net": 0.004}}}},
                        {"period_index": 1, "ts_ms": 2000, "metrics": {"trading": {"top_5pct": {"ev_net": 0.006}}}},
                    ],
                    "test_oos_slices": [
                        {"slice_index": 0, "slice_key": "0:1000:1500", "metrics": {"trading": {"top_5pct": {"ev_net": 0.004}}}},
                        {"slice_index": 1, "slice_key": "1:1501:2000", "metrics": {"trading": {"top_5pct": {"ev_net": 0.006}}}},
                    ],
                }
            ],
        }
    ]

    panel = _summarize_walk_forward_trial_panel(windows)

    assert len(panel) == 1
    assert panel[0]["trial"] == 0
    assert panel[0]["oos_period_count"] == 2
    assert panel[0]["oos_slice_count"] == 2
    assert panel[0]["oos_periods"][0]["window_index"] == 0
    assert panel[0]["oos_periods"][1]["ts_ms"] == 2000
    assert panel[0]["oos_slices"][0]["window_index"] == 0
    assert panel[0]["oos_slices"][1]["slice_index"] == 1


def test_build_selection_search_trial_panel_keeps_all_grid_trials() -> None:
    windows = [
        {
            "window_index": 0,
            "oos_slices": [{"slice_index": 0}],
            "selection_optimization": {
                "by_threshold_key": {
                    "top_5pct": {
                        "grid_results": [
                            {
                                "top_pct": 0.10,
                                "min_candidates_per_ts": 1,
                                "ev_net": 0.010,
                                "selected_rows": 4,
                                "period_results": [{"period_index": 0, "ts_ms": 1000, "ev_net": 0.010, "selected_rows": 4}],
                            },
                            {
                                "top_pct": 0.20,
                                "min_candidates_per_ts": 2,
                                "ev_net": 0.015,
                                "selected_rows": 3,
                                "period_results": [{"period_index": 0, "ts_ms": 1000, "ev_net": 0.015, "selected_rows": 3}],
                            },
                        ]
                    }
                }
            },
        },
        {
            "window_index": 1,
            "oos_slices": [{"slice_index": 0}],
            "selection_optimization": {
                "by_threshold_key": {
                    "top_5pct": {
                        "grid_results": [
                            {
                                "top_pct": 0.10,
                                "min_candidates_per_ts": 1,
                                "ev_net": 0.020,
                                "selected_rows": 5,
                                "period_results": [{"period_index": 0, "ts_ms": 2000, "ev_net": 0.020, "selected_rows": 5}],
                            },
                            {
                                "top_pct": 0.20,
                                "min_candidates_per_ts": 2,
                                "ev_net": 0.030,
                                "selected_rows": 2,
                                "period_results": [{"period_index": 0, "ts_ms": 2000, "ev_net": 0.030, "selected_rows": 2}],
                            },
                        ]
                    }
                }
            },
        },
    ]

    panel = _build_selection_search_trial_panel(windows=windows, start_trial_id=7)

    assert len(panel) == 2
    assert [row["trial"] for row in panel] == [7, 8]
    assert [row["params"] for row in panel] == [
        {"threshold_key": "top_5pct", "top_pct": 0.1, "min_candidates_per_ts": 1},
        {"threshold_key": "top_5pct", "top_pct": 0.2, "min_candidates_per_ts": 2},
    ]
    assert all(row["windows_run"] == 2 for row in panel)
    assert panel[0]["windows"][0]["metrics"]["trading"]["top_5pct"]["ev_net"] == 0.01
    assert panel[0]["windows"][1]["metrics"]["trading"]["top_5pct"]["ev_net"] == 0.02
    assert panel[1]["windows"][0]["metrics"]["trading"]["top_5pct"]["ev_net"] == 0.015
    assert panel[1]["windows"][1]["metrics"]["trading"]["top_5pct"]["ev_net"] == 0.03
    assert panel[0]["windows"][0]["panel_source"] == "oos_periods"
    assert panel[0]["windows"][0]["oos_slices"] == []
    assert len(panel[0]["windows"][0]["oos_periods"]) == 1
    assert panel[0]["windows"][0]["oos_periods"][0]["metrics"]["trading"]["top_5pct"]["ev_net"] == 0.01
    assert "precision" not in panel[0]["windows"][0]["metrics"]["trading"]["top_5pct"]


def test_train_v4_cls_writes_cpcv_lite_report_when_enabled(tmp_path, monkeypatch) -> None:
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
    monkeypatch.setattr(
        "autobot.models.train_v4_crypto_cs._run_cpcv_lite_v4",
        lambda **kwargs: {
            "policy": "cpcv_lite_research_v1",
            "enabled": True,
            "estimate_label": "lite",
            "summary": {
                "status": "partial",
                "folds_requested": 4,
                "folds_run": 3,
                "skipped_folds": 1,
                "comparable_fold_count": 2,
                "budget_cut": True,
            },
            "folds": [],
            "skipped_folds": [],
            "pbo": {"comparable": True, "pbo_estimate": 0.5},
            "dsr": {"comparable": True, "deflated_sharpe_ratio_est": 0.2},
            "insufficiency_reasons": [],
        },
    )

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
        cpcv_lite_enabled=True,
    )

    result = train_and_register_v4_crypto_cs(options)

    assert result.cpcv_lite_report_path is not None
    assert result.cpcv_lite_report_path.exists()
    assert load_json(result.cpcv_lite_report_path)["summary"]["status"] == "partial"
    assert result.metrics["research_only"]["cpcv_lite"]["summary"]["status"] == "partial"


def test_train_v4_use_latest_factor_block_selection_subsets_feature_columns(tmp_path, monkeypatch) -> None:
    selected_feature_capture: dict[str, object] = {}
    dataset = SimpleNamespace(
        rows=3,
        X=np.array([[0.1, 0.2, 0.3], [0.2, 0.3, 0.4], [0.3, 0.4, 0.5]], dtype=np.float64),
        y_cls=np.array([0, 1, 1], dtype=np.int64),
        sample_weight=np.array([1.0, 1.0, 1.0], dtype=np.float64),
        y_reg=np.array([0.0, 0.1, 0.2], dtype=np.float64),
        markets=np.array(["KRW-BTC", "KRW-ETH", "KRW-XRP"], dtype=object),
        selected_markets=("KRW-BTC", "KRW-ETH", "KRW-XRP"),
        feature_names=("logret_1", "volume_z", "btc_ret_1"),
        ts_ms=np.array([1_000, 2_000, 3_000], dtype=np.int64),
    )
    split_info = SimpleNamespace(valid_start_ts=2_000, test_start_ts=3_000, counts={"train": 1, "valid": 1, "test": 1})
    masks = {
        "train": np.array([True, False, False]),
        "valid": np.array([False, True, False]),
        "test": np.array([False, False, True]),
        "drop": np.array([False, False, False]),
    }
    family_dir = tmp_path / "registry" / "train_v4_crypto_cs"
    family_dir.mkdir(parents=True, exist_ok=True)
    (family_dir / "latest_factor_block_selection.json").write_text(
        '{"run_id":"prior-run","accepted_blocks":["v3_base_core","v4_spillover_breadth"],"selected_feature_columns":["logret_1","volume_z","btc_ret_1"]}',
        encoding="utf-8",
    )

    monkeypatch.setattr("autobot.models.train_v4_crypto_cs._try_import_xgboost", lambda: object())
    monkeypatch.setattr(
        "autobot.models.train_v4_crypto_cs.build_dataset_request",
        lambda **kwargs: SimpleNamespace(**kwargs),
    )
    monkeypatch.setattr(
        "autobot.models.train_v4_crypto_cs.load_feature_spec",
        lambda dataset_root: {"feature_columns": ["logret_1", "volume_z", "one_m_count", "btc_ret_1", "hour_sin"], "high_tfs": ["15m", "60m", "240m"]},
    )
    monkeypatch.setattr(
        "autobot.models.train_v4_crypto_cs.load_label_spec",
        lambda dataset_root: {"label_columns": ["y_reg_net_12", "y_cls_topq_12"]},
    )
    monkeypatch.setattr(
        "autobot.models.train_v4_crypto_cs.feature_columns_from_spec",
        lambda dataset_root: ("logret_1", "volume_z", "one_m_count", "btc_ret_1", "hour_sin"),
    )

    def _fake_load_feature_dataset(request, *, feature_columns, **kwargs):
        selected_feature_capture["feature_columns"] = tuple(feature_columns)
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
        execution_acceptance_output_root=tmp_path / "logs" / "train_v4_execution_backtest",
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
        factor_block_selection_mode="use_latest",
    )

    result = train_and_register_v4_crypto_cs(options)

    assert selected_feature_capture["feature_columns"] == ("logret_1", "volume_z", "btc_ret_1")
    assert result.factor_block_selection_path is not None
    assert result.factor_block_selection_path.exists()
    assert result.search_budget_decision_path is not None
    assert result.search_budget_decision_path.exists()
    search_budget_doc = load_json(result.search_budget_decision_path)
    assert search_budget_doc["lane_class_requested"] == "promotion_eligible"
    assert search_budget_doc["lane_class_effective"] == "scout"
    assert search_budget_doc["promotion_eligible_contract"]["satisfied"] is False
    assert load_json(result.run_dir / "train_config.yaml")["factor_block_selection"]["resolution_context"]["applied"] is True


def test_train_v4_guarded_auto_policy_applies_pruned_features_and_triggers_cpcv(tmp_path, monkeypatch) -> None:
    selected_feature_capture: dict[str, object] = {}
    cpcv_capture: dict[str, object] = {}
    dataset = SimpleNamespace(
        rows=3,
        X=np.array([[0.1, 0.2, 0.3], [0.2, 0.3, 0.4], [0.3, 0.4, 0.5]], dtype=np.float64),
        y_cls=np.array([0, 1, 1], dtype=np.int64),
        sample_weight=np.array([1.0, 1.0, 1.0], dtype=np.float64),
        y_reg=np.array([0.0, 0.1, 0.2], dtype=np.float64),
        markets=np.array(["KRW-BTC", "KRW-ETH", "KRW-XRP"], dtype=object),
        selected_markets=("KRW-BTC", "KRW-ETH", "KRW-XRP"),
        feature_names=("logret_1", "one_m_count", "btc_ret_1"),
        ts_ms=np.array([1_000, 2_000, 3_000], dtype=np.int64),
    )
    split_info = SimpleNamespace(valid_start_ts=2_000, test_start_ts=3_000, counts={"train": 1, "valid": 1, "test": 1})
    masks = {
        "train": np.array([True, False, False]),
        "valid": np.array([False, True, False]),
        "test": np.array([False, False, True]),
        "drop": np.array([False, False, False]),
    }
    family_dir = tmp_path / "registry" / "train_v4_crypto_cs"
    family_dir.mkdir(parents=True, exist_ok=True)
    (family_dir / "latest_factor_block_policy.json").write_text(
        json.dumps(
            {
                "updated_by_run_id": "policy-run",
                "apply_pruned_feature_set": True,
                "accepted_blocks": ["v3_base_core", "v3_one_m_core", "v4_spillover_breadth"],
                "selected_feature_columns": ["logret_1", "one_m_count", "btc_ret_1"],
                "summary": {"status": "stable"},
                "policy_reasons": ["GUARDED_AUTO_POLICY_ACTIVE"],
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr("autobot.models.train_v4_crypto_cs._try_import_xgboost", lambda: object())
    monkeypatch.setattr(
        "autobot.models.train_v4_crypto_cs.build_dataset_request",
        lambda **kwargs: SimpleNamespace(**kwargs),
    )
    monkeypatch.setattr(
        "autobot.models.train_v4_crypto_cs.load_feature_spec",
        lambda dataset_root: {"feature_columns": ["logret_1", "volume_z", "one_m_count", "btc_ret_1", "hour_sin"], "high_tfs": ["15m", "60m", "240m"]},
    )
    monkeypatch.setattr(
        "autobot.models.train_v4_crypto_cs.load_label_spec",
        lambda dataset_root: {"label_columns": ["y_reg_net_12", "y_cls_topq_12"]},
    )
    monkeypatch.setattr(
        "autobot.models.train_v4_crypto_cs.feature_columns_from_spec",
        lambda dataset_root: ("logret_1", "volume_z", "one_m_count", "btc_ret_1", "hour_sin"),
    )

    def _fake_load_feature_dataset(request, *, feature_columns, **kwargs):
        selected_feature_capture["feature_columns"] = tuple(feature_columns)
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

    def _fake_cpcv(**kwargs):
        cpcv_capture["enabled"] = kwargs["enabled"]
        cpcv_capture["trigger"] = kwargs["trigger"]
        return {
            "policy": "cpcv_lite_research_v1",
            "enabled": bool(kwargs["enabled"]),
            "trigger": kwargs["trigger"],
            "estimate_label": "lite",
            "summary": {
                "status": "partial",
                "folds_requested": 4,
                "folds_run": 2,
                "skipped_folds": 2,
                "comparable_fold_count": 1,
                "budget_cut": False,
            },
            "folds": [],
            "skipped_folds": [],
            "pbo": {"comparable": False, "reason": "TOY"},
            "dsr": {"comparable": False, "reason": "TOY"},
            "insufficiency_reasons": ["TOY"],
        }

    monkeypatch.setattr("autobot.models.train_v4_crypto_cs._run_cpcv_lite_v4", _fake_cpcv)

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
        factor_block_selection_mode="guarded_auto",
    )

    result = train_and_register_v4_crypto_cs(options)

    assert selected_feature_capture["feature_columns"] == ("logret_1", "volume_z", "one_m_count", "btc_ret_1")
    assert cpcv_capture == {"enabled": True, "trigger": "guarded_policy"}
    train_config = load_json(result.run_dir / "train_config.yaml")
    assert train_config["factor_block_selection"]["resolution_context"]["applied"] is True
    assert train_config["cpcv_lite"]["enabled"] is True
    assert train_config["cpcv_lite"]["trigger"] == "guarded_policy"
    assert result.experiment_ledger_path is not None
    assert result.experiment_ledger_path.exists()
    assert result.experiment_ledger_summary_path is not None
    assert result.experiment_ledger_summary_path.exists()
    assert load_json(result.experiment_ledger_summary_path)["updated_by_run_id"] == result.run_id
