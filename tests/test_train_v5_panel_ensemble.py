from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np

from autobot.models.registry import load_json
from autobot.models.train_v5_panel_ensemble import (
    TrainV5PanelEnsembleOptions,
    materialize_v5_panel_ensemble_runtime_export,
    train_and_register_v5_panel_ensemble,
)


class DummyClassifier:
    def predict_proba(self, x: np.ndarray) -> np.ndarray:
        probs = np.clip(0.55 + (np.asarray(x, dtype=np.float64)[:, 0] * 0.1), 0.05, 0.95)
        return np.column_stack([1.0 - probs, probs])


class DummyRanker:
    def predict(self, x: np.ndarray) -> np.ndarray:
        return np.asarray(x, dtype=np.float64)[:, 0] * 0.25


class DummyRegressor:
    def __init__(self, bias: float) -> None:
        self.bias = float(bias)

    def predict(self, x: np.ndarray) -> np.ndarray:
        return np.asarray(x, dtype=np.float64)[:, 0] + self.bias


def test_train_v5_panel_ensemble_writes_core_contract_artifacts(tmp_path, monkeypatch) -> None:
    dataset = SimpleNamespace(
        rows=4,
        X=np.asarray([[0.1], [0.2], [0.3], [0.4]], dtype=np.float64),
        y_cls=np.asarray([0, 1, 0, 1], dtype=np.int64),
        y_reg=np.asarray([0.0, 0.1, 0.0, 0.2], dtype=np.float64),
        y_rank=np.asarray([0.0, 1.0, 0.0, 1.0], dtype=np.float64),
        sample_weight=np.asarray([1.0, 1.0, 1.0, 1.0], dtype=np.float64),
        markets=np.asarray(["KRW-BTC", "KRW-ETH", "KRW-BTC", "KRW-ETH"], dtype=object),
        selected_markets=("KRW-BTC", "KRW-ETH"),
        feature_names=("f1",),
        ts_ms=np.asarray([1_000, 2_000, 3_000, 4_000], dtype=np.int64),
    )
    prepared = {
        "dataset": dataset,
        "label_spec": {
            "canonical_multi_horizon_columns": {
                "y_reg_resid_leader": [
                    "y_reg_resid_leader_h3",
                    "y_reg_resid_leader_h6",
                    "y_reg_resid_leader_h12",
                    "y_reg_resid_leader_h24",
                ],
                "y_rank_resid_leader": [
                    "y_rank_resid_leader_h3",
                    "y_rank_resid_leader_h6",
                    "y_rank_resid_leader_h12",
                    "y_rank_resid_leader_h24",
                ],
                "y_cls_resid_leader": [
                    "y_cls_resid_leader_topq_h3",
                    "y_cls_resid_leader_topq_h6",
                    "y_cls_resid_leader_topq_h12",
                    "y_cls_resid_leader_topq_h24",
                ],
            }
        },
        "label_contract": {
            "y_cls_column": "y_cls_resid_leader_topq_h12",
            "y_reg_column": "y_reg_resid_leader_h12",
            "y_rank_column": "y_rank_resid_leader_h12",
            "label_columns": [
                "y_cls_resid_leader_topq_h12",
                "y_reg_resid_leader_h12",
                "y_rank_resid_leader_h12",
            ],
            "primary_horizon_bars": 12,
        },
        "request": SimpleNamespace(dataset_root=tmp_path / "features"),
        "feature_spec": {"feature_columns": ["f1"]},
        "train_mask": np.asarray([True, True, False, False]),
        "valid_mask": np.asarray([False, False, True, False]),
        "test_mask": np.asarray([False, False, False, True]),
        "rows": {"total": 4, "train": 2, "valid": 1, "test": 1, "drop": 0},
        "split_info": SimpleNamespace(valid_start_ts=3_000, test_start_ts=4_000, counts={"train": 2, "valid": 1, "test": 1}),
        "interval_ms": 300_000,
        "search_budget_decision": {},
        "factor_block_selection_context": {},
        "factor_block_registry": [],
        "cpcv_lite_runtime": {"enabled": False, "trigger": "disabled"},
        "action_aux_arrays": {
            "close": np.asarray([100.0, 101.0, 102.0, 103.0], dtype=np.float64),
            "rv_12": np.asarray([0.1, 0.1, 0.1, 0.1], dtype=np.float64),
            "rv_36": np.asarray([0.2, 0.2, 0.2, 0.2], dtype=np.float64),
            "atr_14": np.asarray([0.3, 0.3, 0.3, 0.3], dtype=np.float64),
            "atr_pct_14": np.asarray([0.01, 0.01, 0.01, 0.01], dtype=np.float64),
        },
        "effective_booster_sweep_trials": 1,
        "live_domain_reweighting": {},
    }

    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble.prepare_v4_training_inputs",
        lambda **kwargs: prepared,
    )
    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble._load_v5_regression_targets",
        lambda **kwargs: {
            "h3": np.asarray([0.0, 0.1, 0.0, 0.1], dtype=np.float64),
            "h6": np.asarray([0.0, 0.2, 0.0, 0.2], dtype=np.float64),
            "h12": np.asarray([0.0, 0.3, 0.0, 0.3], dtype=np.float64),
            "h24": np.asarray([0.0, 0.4, 0.0, 0.4], dtype=np.float64),
        },
    )
    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble._load_v5_auxiliary_panel_targets",
        lambda **kwargs: {
            "cls": {
                "h3": np.asarray([0.0, 1.0, 0.0, 1.0], dtype=np.float64),
                "h6": np.asarray([0.0, 1.0, 0.0, 1.0], dtype=np.float64),
                "h12": np.asarray([0.0, 1.0, 0.0, 1.0], dtype=np.float64),
                "h24": np.asarray([0.0, 1.0, 0.0, 1.0], dtype=np.float64),
            },
            "rank": {
                "h3": np.asarray([0.0, 1.0, 0.0, 1.0], dtype=np.float64),
                "h6": np.asarray([0.0, 1.0, 0.0, 1.0], dtype=np.float64),
                "h12": np.asarray([0.0, 1.0, 0.0, 1.0], dtype=np.float64),
                "h24": np.asarray([0.0, 1.0, 0.0, 1.0], dtype=np.float64),
            },
        },
    )
    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble.v4._fit_booster_sweep_weighted",
        lambda **kwargs: {"bundle": {"model_type": "xgboost", "scaler": None, "estimator": DummyClassifier()}, "best_params": {"max_depth": 2}},
    )
    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble.v4._fit_booster_sweep_ranker",
        lambda **kwargs: {"bundle": {"model_type": "xgboost_ranker", "scaler": None, "estimator": DummyRanker()}, "best_params": {"max_depth": 2}},
    )
    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble._fit_v5_regression_heads",
        lambda **kwargs: {
            "h3": {"bundle": {"model_type": "xgboost_regressor", "scaler": None, "estimator": DummyRegressor(0.1)}, "best_params": {"max_depth": 2}},
            "h6": {"bundle": {"model_type": "xgboost_regressor", "scaler": None, "estimator": DummyRegressor(0.2)}, "best_params": {"max_depth": 2}},
            "h12": {"bundle": {"model_type": "xgboost_regressor", "scaler": None, "estimator": DummyRegressor(0.3)}, "best_params": {"max_depth": 2}},
            "h24": {"bundle": {"model_type": "xgboost_regressor", "scaler": None, "estimator": DummyRegressor(0.4)}, "best_params": {"max_depth": 2}},
        },
    )
    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble._build_v5_oof_windows",
        lambda **kwargs: {
                "windows": [
                    {
                        "window_index": 0,
                        "time_window": {"valid_start_ts": 3_000, "test_start_ts": 4_000, "test_end_ts": 4_000},
                    "counts": {"train": 2, "valid": 1, "test": 1, "drop": 0},
                    "metrics": {"classification": {"roc_auc": 0.7}, "ranking": {}, "trading": {}},
                    "oos_periods": [],
                    "oos_slices": [],
                    "selection_optimization": {"comparable": False, "by_threshold_key": {}},
                    "trial_records": [],
                }
                ],
                "skipped_windows": [],
                "_selection_calibration_rows": [{"scores": [0.2, 0.8], "y_cls": [0, 1]}],
                "_trade_action_oos_rows": [],
                "meta_rows": [{"x": np.asarray([[0.2, 0.3, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.6, 0.62, 0.64, 0.66], [0.8, 0.7, 0.6, 0.58, 0.5, 0.48, 0.4, 0.38, 0.3, 0.28, 0.26, 0.24]], dtype=np.float64), "y": np.asarray([0, 1], dtype=np.int64)}],
                "sample_weight": np.asarray([1.0, 1.0], dtype=np.float64),
            },
        )
    monkeypatch.setattr("autobot.models.train_v5_panel_ensemble.build_data_fingerprint", lambda **kwargs: {"manifest_sha256": "abc"})
    monkeypatch.setattr("autobot.models.train_v5_panel_ensemble.render_model_card", lambda **kwargs: "# card")
    monkeypatch.setattr("autobot.models.train_v5_panel_ensemble.resolve_ready_snapshot_id", lambda **kwargs: "snapshot-main")
    monkeypatch.setattr("autobot.models.train_v5_panel_ensemble.v4._detect_duplicate_candidate_artifacts", lambda **kwargs: {"duplicate": False})
    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble.v4._run_execution_acceptance_v4",
        lambda **kwargs: {"version": 1, "status": "skipped", "reason": "TEST"},
    )
    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble.v4._build_runtime_recommendations_v4",
        lambda **kwargs: {"version": 1, "status": "skipped", "reason": "TEST"},
    )
    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble.v4._build_exit_path_risk_summary_v4",
        lambda **kwargs: {"status": "skipped"},
    )
    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble.v4._build_trade_action_policy_v4",
        lambda **kwargs: {"status": "skipped"},
    )
    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble.v4._build_execution_risk_control_v4",
        lambda **kwargs: {"status": "skipped"},
    )
    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble.v4._purge_execution_artifact_run_dirs",
        lambda **kwargs: {"evaluated": False},
    )
    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble.v4._manual_promotion_decision_v4",
        lambda **kwargs: {"status": "candidate", "promotion_mode": "candidate", "reasons": ["TEST"]},
    )
    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble.v4._build_trainer_research_evidence_from_promotion_v4",
        lambda **kwargs: {"available": True, "pass": True},
    )

    options = TrainV5PanelEnsembleOptions(
        dataset_root=tmp_path / "features",
        registry_root=tmp_path / "registry",
        logs_root=tmp_path / "logs",
        model_family="train_v5_panel_ensemble",
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
        run_scope="scheduled_daily",
    )

    result = train_and_register_v5_panel_ensemble(options)

    assert result.run_dir.exists()
    assert (result.run_dir / "panel_ensemble_contract.json").exists()
    assert (result.run_dir / "predictor_contract.json").exists()
    assert (result.run_dir / "expert_prediction_table.parquet").exists()
    assert result.promotion_path.exists()
    assert result.experiment_ledger_path is not None
    assert result.experiment_ledger_summary_path is not None
    assert (result.run_dir / "panel_tail_context.json").exists()
    assert load_json(result.run_dir / "train_config.yaml")["trainer"] == "v5_panel_ensemble"
    assert load_json(result.run_dir / "train_config.yaml")["data_platform_ready_snapshot_id"] == "snapshot-main"
    assert load_json(result.run_dir / "panel_ensemble_contract.json")["policy"] == "v5_panel_ensemble_v1"
    assert load_json(result.run_dir / "panel_ensemble_contract.json")["auxiliary_classifier_horizons"] == [3, 6, 24]
    assert load_json(result.run_dir / "panel_ensemble_contract.json")["auxiliary_rank_horizons"] == [3, 6, 24]
    assert load_json(result.run_dir / "predictor_contract.json")["score_lcb_field"] == "score_lcb"
    assert load_json(result.run_dir / "predictor_contract.json")["final_rank_score_field"] == "final_rank_score"
    assert load_json(result.run_dir / "predictor_contract.json")["final_expected_return_field"] == "final_expected_return"
    assert load_json(result.run_dir / "predictor_contract.json")["final_expected_es_field"] == "final_expected_es"
    assert load_json(result.run_dir / "predictor_contract.json")["final_tradability_field"] == "final_tradability"
    assert load_json(result.run_dir / "predictor_contract.json")["final_alpha_lcb_field"] == "final_alpha_lcb"
    assert load_json(result.run_dir / "predictor_contract.json")["distributional_contract"]["quantile_levels"] == [0.1, 0.5, 0.9]
    assert load_json(result.run_dir / "panel_ensemble_contract.json")["final_output_contract"]["score_aliases"]["final_rank_score"] == "score_mean"
    assert load_json(result.run_dir / "panel_ensemble_contract.json")["final_output_contract"]["expected_return_field"] == "final_expected_return"
    assert load_json(result.run_dir / "panel_ensemble_contract.json")["final_output_contract"]["expected_es_field"] == "final_expected_es"
    assert load_json(result.run_dir / "panel_ensemble_contract.json")["final_output_contract"]["tradability_field"] == "final_tradability"
    assert load_json(result.run_dir / "panel_ensemble_contract.json")["final_output_contract"]["alpha_lcb_field"] == "final_alpha_lcb"
    artifact_status = load_json(result.run_dir / "artifact_status.json")
    assert artifact_status["tail_context_written"] is True
    assert artifact_status["promotion_complete"] is True
    assert artifact_status["decision_surface_complete"] is True
    assert artifact_status["expert_prediction_table_complete"] is True
    assert load_json(result.train_report_path)["data_platform_ready_snapshot_id"] == "snapshot-main"
    assert load_json(result.train_report_path)["resumed"] is False
    assert float(load_json(result.train_report_path)["tail_duration_sec"]) >= 0.0
    assert (tmp_path / "registry" / "train_v5_panel_ensemble" / "latest.json").exists()
    assert not (tmp_path / "registry" / "latest.json").exists()


def test_train_v5_panel_ensemble_dependency_expert_only_skips_heavy_tail(tmp_path, monkeypatch) -> None:
    dataset = SimpleNamespace(
        rows=4,
        X=np.asarray([[0.1], [0.2], [0.3], [0.4]], dtype=np.float64),
        y_cls=np.asarray([0, 1, 0, 1], dtype=np.int64),
        y_reg=np.asarray([0.0, 0.1, 0.0, 0.2], dtype=np.float64),
        y_rank=np.asarray([0.0, 1.0, 0.0, 1.0], dtype=np.float64),
        sample_weight=np.asarray([1.0, 1.0, 1.0, 1.0], dtype=np.float64),
        markets=np.asarray(["KRW-BTC", "KRW-ETH", "KRW-BTC", "KRW-ETH"], dtype=object),
        selected_markets=("KRW-BTC", "KRW-ETH"),
        feature_names=("f1",),
        ts_ms=np.asarray([1_000, 2_000, 3_000, 4_000], dtype=np.int64),
    )
    prepared = {
        "dataset": dataset,
        "label_spec": {
            "canonical_multi_horizon_columns": {
                "y_reg_resid_leader": [
                    "y_reg_resid_leader_h3",
                    "y_reg_resid_leader_h6",
                    "y_reg_resid_leader_h12",
                    "y_reg_resid_leader_h24",
                ],
                "y_rank_resid_leader": [
                    "y_rank_resid_leader_h3",
                    "y_rank_resid_leader_h6",
                    "y_rank_resid_leader_h12",
                    "y_rank_resid_leader_h24",
                ],
                "y_cls_resid_leader": [
                    "y_cls_resid_leader_topq_h3",
                    "y_cls_resid_leader_topq_h6",
                    "y_cls_resid_leader_topq_h12",
                    "y_cls_resid_leader_topq_h24",
                ],
            }
        },
        "label_contract": {
            "y_cls_column": "y_cls_resid_leader_topq_h12",
            "y_reg_column": "y_reg_resid_leader_h12",
            "y_rank_column": "y_rank_resid_leader_h12",
            "label_columns": [
                "y_cls_resid_leader_topq_h12",
                "y_reg_resid_leader_h12",
                "y_rank_resid_leader_h12",
            ],
            "primary_horizon_bars": 12,
        },
        "request": SimpleNamespace(dataset_root=tmp_path / "features"),
        "feature_spec": {"feature_columns": ["f1"]},
        "train_mask": np.asarray([True, True, False, False]),
        "valid_mask": np.asarray([False, False, True, False]),
        "test_mask": np.asarray([False, False, False, True]),
        "rows": {"total": 4, "train": 2, "valid": 1, "test": 1, "drop": 0},
        "split_info": SimpleNamespace(valid_start_ts=3_000, test_start_ts=4_000, counts={"train": 2, "valid": 1, "test": 1}),
        "interval_ms": 300_000,
        "search_budget_decision": {"applied": {"runtime_recommendation_profile": "compact"}},
        "factor_block_selection_context": {},
        "factor_block_registry": [],
        "cpcv_lite_runtime": {"enabled": False, "trigger": "disabled"},
        "action_aux_arrays": {
            "close": np.asarray([100.0, 101.0, 102.0, 103.0], dtype=np.float64),
            "rv_12": np.asarray([0.1, 0.1, 0.1, 0.1], dtype=np.float64),
            "rv_36": np.asarray([0.2, 0.2, 0.2, 0.2], dtype=np.float64),
            "atr_14": np.asarray([0.3, 0.3, 0.3, 0.3], dtype=np.float64),
            "atr_pct_14": np.asarray([0.01, 0.01, 0.01, 0.01], dtype=np.float64),
        },
        "effective_booster_sweep_trials": 1,
        "live_domain_reweighting": {},
    }

    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble.prepare_v4_training_inputs",
        lambda **kwargs: prepared,
    )
    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble._load_v5_regression_targets",
        lambda **kwargs: {
            "h3": np.asarray([0.0, 0.1, 0.0, 0.1], dtype=np.float64),
            "h6": np.asarray([0.0, 0.2, 0.0, 0.2], dtype=np.float64),
            "h12": np.asarray([0.0, 0.3, 0.0, 0.3], dtype=np.float64),
            "h24": np.asarray([0.0, 0.4, 0.0, 0.4], dtype=np.float64),
        },
    )
    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble._load_v5_auxiliary_panel_targets",
        lambda **kwargs: {
            "cls": {
                "h3": np.asarray([0.0, 1.0, 0.0, 1.0], dtype=np.float64),
                "h6": np.asarray([0.0, 1.0, 0.0, 1.0], dtype=np.float64),
                "h12": np.asarray([0.0, 1.0, 0.0, 1.0], dtype=np.float64),
                "h24": np.asarray([0.0, 1.0, 0.0, 1.0], dtype=np.float64),
            },
            "rank": {
                "h3": np.asarray([0.0, 1.0, 0.0, 1.0], dtype=np.float64),
                "h6": np.asarray([0.0, 1.0, 0.0, 1.0], dtype=np.float64),
                "h12": np.asarray([0.0, 1.0, 0.0, 1.0], dtype=np.float64),
                "h24": np.asarray([0.0, 1.0, 0.0, 1.0], dtype=np.float64),
            },
        },
    )
    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble.v4._fit_booster_sweep_weighted",
        lambda **kwargs: {"bundle": {"model_type": "xgboost", "scaler": None, "estimator": DummyClassifier()}, "best_params": {"max_depth": 2}},
    )
    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble.v4._fit_booster_sweep_ranker",
        lambda **kwargs: {"bundle": {"model_type": "xgboost_ranker", "scaler": None, "estimator": DummyRanker()}, "best_params": {"max_depth": 2}},
    )
    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble._fit_v5_regression_heads",
        lambda **kwargs: {
            "h3": {"bundle": {"model_type": "xgboost_regressor", "scaler": None, "estimator": DummyRegressor(0.1)}, "best_params": {"max_depth": 2}},
            "h6": {"bundle": {"model_type": "xgboost_regressor", "scaler": None, "estimator": DummyRegressor(0.2)}, "best_params": {"max_depth": 2}},
            "h12": {"bundle": {"model_type": "xgboost_regressor", "scaler": None, "estimator": DummyRegressor(0.3)}, "best_params": {"max_depth": 2}},
            "h24": {"bundle": {"model_type": "xgboost_regressor", "scaler": None, "estimator": DummyRegressor(0.4)}, "best_params": {"max_depth": 2}},
        },
    )
    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble._build_v5_oof_windows",
        lambda **kwargs: {
                "windows": [
                    {
                        "window_index": 0,
                        "time_window": {"valid_start_ts": 3_000, "test_start_ts": 4_000, "test_end_ts": 4_000},
                    "counts": {"train": 2, "valid": 1, "test": 1, "drop": 0},
                    "metrics": {"classification": {"roc_auc": 0.7}, "ranking": {}, "trading": {}},
                    "oos_periods": [],
                    "oos_slices": [],
                    "selection_optimization": {"comparable": False, "by_threshold_key": {}},
                    "trial_records": [],
                }
                ],
                "skipped_windows": [],
                "_selection_calibration_rows": [{"scores": [0.2, 0.8], "y_cls": [0, 1]}],
                "_trade_action_oos_rows": [],
                "meta_rows": [{"x": np.asarray([[0.2, 0.3, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.6, 0.62, 0.64, 0.66], [0.8, 0.7, 0.6, 0.58, 0.5, 0.48, 0.4, 0.38, 0.3, 0.28, 0.26, 0.24]], dtype=np.float64), "y": np.asarray([0, 1], dtype=np.int64)}],
                "sample_weight": np.asarray([1.0, 1.0], dtype=np.float64),
            },
        )
    monkeypatch.setattr("autobot.models.train_v5_panel_ensemble.build_data_fingerprint", lambda **kwargs: {"manifest_sha256": "abc"})
    monkeypatch.setattr("autobot.models.train_v5_panel_ensemble.render_model_card", lambda **kwargs: "# card")
    monkeypatch.setattr("autobot.models.train_v5_panel_ensemble.resolve_ready_snapshot_id", lambda **kwargs: "snapshot-dependency")
    monkeypatch.setattr("autobot.models.train_v5_panel_ensemble.v4._detect_duplicate_candidate_artifacts", lambda **kwargs: {"duplicate": False})

    def _fail_heavy(**kwargs):
        raise AssertionError("heavy panel tail should not run in dependency expert-only mode")

    monkeypatch.setattr("autobot.models.train_v5_panel_ensemble.v4._run_execution_acceptance_v4", _fail_heavy)
    monkeypatch.setattr("autobot.models.train_v5_panel_ensemble.v4._build_runtime_recommendations_v4", _fail_heavy)
    monkeypatch.setattr("autobot.models.train_v5_panel_ensemble.v4._manual_promotion_decision_v4", _fail_heavy)

    options = TrainV5PanelEnsembleOptions(
        dataset_root=tmp_path / "features",
        registry_root=tmp_path / "registry",
        logs_root=tmp_path / "logs",
        model_family="train_v5_panel_ensemble",
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
        run_scope="scheduled_daily_dependency_v5_panel_ensemble",
        dependency_expert_only=True,
    )

    result = train_and_register_v5_panel_ensemble(options)

    report = load_json(result.train_report_path)
    assert report["dependency_expert_only"] is True
    assert report["tail_mode"] == "dependency_expert_only"
    assert report["data_platform_ready_snapshot_id"] == "snapshot-dependency"
    assert (result.run_dir / "execution_acceptance_report.json").exists()
    assert (result.run_dir / "runtime_recommendations.json").exists()
    assert (result.run_dir / "promotion_decision.json").exists()
    assert (result.run_dir / "expert_prediction_table.parquet").exists()
    assert load_json(result.run_dir / "train_config.yaml")["dependency_expert_only"] is True
    assert load_json(result.run_dir / "panel_tail_context.json")["dependency_expert_only"] is True
    assert load_json(result.run_dir / "panel_tail_context.json")["tail_mode"] == "dependency_expert_only"


def test_materialize_v5_panel_ensemble_runtime_export_writes_window_artifacts(tmp_path, monkeypatch) -> None:
    class _RuntimeEstimator:
        def predict_panel_contract(self, x: np.ndarray) -> dict[str, np.ndarray]:
            rows = np.asarray(x).shape[0]
            return {
                "final_rank_score": np.full(rows, 0.7, dtype=np.float64),
                "final_uncertainty": np.full(rows, 0.1, dtype=np.float64),
                "score_mean": np.full(rows, 0.7, dtype=np.float64),
                "score_std": np.full(rows, 0.1, dtype=np.float64),
                "score_lcb": np.full(rows, 0.6, dtype=np.float64),
                "final_expected_return": np.full(rows, 0.02, dtype=np.float64),
                "final_expected_es": np.full(rows, 0.01, dtype=np.float64),
                "final_tradability": np.full(rows, 0.8, dtype=np.float64),
                "final_alpha_lcb": np.full(rows, -0.09, dtype=np.float64),
            }

    dataset = SimpleNamespace(
        rows=2,
        X=np.asarray([[0.1], [0.2]], dtype=np.float64),
        y_cls=np.asarray([0, 1], dtype=np.int64),
        y_reg=np.asarray([0.0, 0.1], dtype=np.float64),
        y_rank=np.asarray([0.0, 0.1], dtype=np.float64),
        sample_weight=np.asarray([1.0, 1.0], dtype=np.float64),
        ts_ms=np.asarray([1_000, 2_000], dtype=np.int64),
        markets=np.asarray(["KRW-BTC", "KRW-ETH"], dtype=object),
        selected_markets=("KRW-BTC", "KRW-ETH"),
    )
    train_config = {
        "trainer": "v5_panel_ensemble",
        "model_family": "train_v5_panel_ensemble",
        "data_platform_ready_snapshot_id": "snapshot-export",
    }
    run_dir = tmp_path / "registry" / "train_v5_panel_ensemble" / "panel-export-run"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "train_config.yaml").write_text(json.dumps(train_config), encoding="utf-8")
    estimator = _RuntimeEstimator()
    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble._load_panel_inference_dataset_window",
        lambda **kwargs: (
            dataset,
            TrainV5PanelEnsembleOptions(
                dataset_root=tmp_path / "features",
                registry_root=tmp_path / "registry",
                logs_root=tmp_path / "logs",
                model_family="train_v5_panel_ensemble",
                tf="5m",
                quote="KRW",
                top_n=20,
                start="2026-03-27",
                end="2026-03-27",
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
            ),
            {**train_config, "selected_markets": ["KRW-BTC", "KRW-ETH"]},
        ),
    )
    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble.load_model_bundle",
        lambda path: {"estimator": estimator},
    )
    export = materialize_v5_panel_ensemble_runtime_export(run_dir=run_dir, start="2026-03-27", end="2026-03-27")
    assert Path(export["export_path"]).exists()
    assert Path(export["metadata_path"]).exists()
    assert export["rows"] == 2
    assert export["reused"] is False


def test_materialize_v5_panel_ensemble_runtime_export_resolves_common_runtime_universe(tmp_path, monkeypatch) -> None:
    dataset = SimpleNamespace(
        rows=1,
        X=np.asarray([[0.2]], dtype=np.float64),
        y_cls=np.asarray([1], dtype=np.int64),
        y_reg=np.asarray([0.1], dtype=np.float64),
        y_rank=np.asarray([0.1], dtype=np.float64),
        sample_weight=np.asarray([1.0], dtype=np.float64),
        ts_ms=np.asarray([2_000], dtype=np.int64),
        markets=np.asarray(["KRW-ETH"], dtype=object),
        selected_markets=("KRW-ETH",),
    )
    train_config = {
        "trainer": "v5_panel_ensemble",
        "model_family": "train_v5_panel_ensemble",
        "data_platform_ready_snapshot_id": "snapshot-export",
        "selected_markets": ["KRW-BTC", "KRW-ETH"],
    }
    run_dir = tmp_path / "registry" / "train_v5_panel_ensemble" / "panel-export-run-common"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "train_config.yaml").write_text(json.dumps(train_config), encoding="utf-8")

    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble._load_panel_inference_dataset_window",
        lambda **kwargs: (
            dataset,
            TrainV5PanelEnsembleOptions(
                dataset_root=tmp_path / "features",
                registry_root=tmp_path / "registry",
                logs_root=tmp_path / "logs",
                model_family="train_v5_panel_ensemble",
                tf="5m",
                quote="KRW",
                top_n=20,
                start="2026-03-27",
                end="2026-03-27",
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
            ),
            train_config,
        ),
    )

    export = materialize_v5_panel_ensemble_runtime_export(
        run_dir=run_dir,
        start="2026-03-27",
        end="2026-03-27",
        selected_markets_override=("KRW-ETH",),
        resolve_markets_only=True,
    )

    assert export["requested_selected_markets"] == ["KRW-ETH"]
    assert export["selected_markets"] == ["KRW-ETH"]
    assert export["selected_markets_source"] == "acceptance_common_runtime_universe"
    assert export["fallback_reason"] == ""
    assert export["source_mode"] == "resolve_markets_only"
