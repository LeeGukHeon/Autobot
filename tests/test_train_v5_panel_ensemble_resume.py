from __future__ import annotations

from dataclasses import asdict
import json
from pathlib import Path

import joblib
import numpy as np

from autobot.models import train_v4_crypto_cs as v4
from autobot.models.train_v5_panel_ensemble import (
    TrainV5PanelEnsembleOptions,
    resume_v5_panel_ensemble_tail,
)


class _DummyEstimator:
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


def test_resume_v5_panel_ensemble_tail_writes_missing_runtime_artifacts(tmp_path: Path, monkeypatch) -> None:
    registry_root = tmp_path / "registry"
    logs_root = tmp_path / "logs"
    run_id = "resume-run"
    run_dir = registry_root / "train_v5_panel_ensemble" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    options = TrainV5PanelEnsembleOptions(
        dataset_root=tmp_path / "features",
        registry_root=registry_root,
        logs_root=logs_root,
        model_family="train_v5_panel_ensemble",
        tf="5m",
        quote="KRW",
        top_n=10,
        start="2026-03-20",
        end="2026-03-29",
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
        execution_acceptance_enabled=True,
        execution_acceptance_parquet_root=tmp_path / "parquet",
        execution_acceptance_output_root=tmp_path / "backtest",
        execution_acceptance_eval_start="2026-03-27",
        execution_acceptance_eval_end="2026-03-29",
    )
    (options.logs_root).mkdir(parents=True, exist_ok=True)
    (options.execution_acceptance_output_root).mkdir(parents=True, exist_ok=True)

    train_config = asdict(options)
    train_config["data_platform_ready_snapshot_id"] = "snapshot-resume"
    (run_dir / "train_config.yaml").write_text(json.dumps(train_config, default=str), encoding="utf-8")
    (run_dir / "metrics.json").write_text(json.dumps({"panel_ensemble": {"policy": "v5_panel_ensemble_v1"}}), encoding="utf-8")
    (run_dir / "thresholds.json").write_text(json.dumps({"top_5pct": 0.5}), encoding="utf-8")
    (run_dir / "leaderboard_row.json").write_text(json.dumps({"run_id": run_id}), encoding="utf-8")
    (run_dir / "walk_forward_report.json").write_text(json.dumps({"summary": {"windows_run": 1}, "_trade_action_oos_rows": []}), encoding="utf-8")
    (run_dir / "cpcv_lite_report.json").write_text(json.dumps({"summary": {"status": "disabled"}}), encoding="utf-8")
    (run_dir / "factor_block_selection.json").write_text(json.dumps({"summary": {"status": "ok"}, "selection_mode": "off"}), encoding="utf-8")
    (run_dir / "selection_recommendations.json").write_text(json.dumps({"top_5pct": {"threshold": 0.5}}), encoding="utf-8")
    (run_dir / "selection_policy.json").write_text(json.dumps({"mode": "raw_threshold"}), encoding="utf-8")
    (run_dir / "selection_calibration.json").write_text(json.dumps({"default_score_source": "score_mean"}), encoding="utf-8")
    (run_dir / "search_budget_decision.json").write_text(json.dumps({"status": "default"}), encoding="utf-8")
    joblib.dump({"model_type": "v5_panel_ensemble", "estimator": _DummyEstimator()}, run_dir / "model.bin")

    dataset = type(
        "Dataset",
        (),
        {
            "rows": 3,
            "X": np.asarray([[0.1], [0.2], [0.3]], dtype=np.float64),
            "y_cls": np.asarray([0, 1, 1], dtype=np.int64),
            "y_reg": np.asarray([0.0, 0.1, 0.2], dtype=np.float64),
            "y_rank": np.asarray([0.0, 0.1, 0.2], dtype=np.float64),
            "sample_weight": np.asarray([1.0, 1.0, 1.0], dtype=np.float64),
            "markets": np.asarray(["KRW-BTC", "KRW-ETH", "KRW-BTC"], dtype=object),
            "selected_markets": ("KRW-BTC", "KRW-ETH"),
            "feature_names": ("f1",),
            "ts_ms": np.asarray([1_000, 2_000, 3_000], dtype=np.int64),
        },
    )()
    prepared = {
        "dataset": dataset,
        "label_spec": {"canonical_multi_horizon_columns": {"y_reg_resid_leader": ["y_reg_resid_leader_h12"]}},
        "label_contract": {
            "y_cls_column": "y_cls_resid_leader_topq_h12",
            "y_reg_column": "y_reg_resid_leader_h12",
            "y_rank_column": "y_rank_resid_leader_h12",
            "primary_horizon_bars": 12,
        },
        "request": object(),
        "train_mask": np.asarray([True, True, False]),
        "valid_mask": np.asarray([False, False, True]),
        "test_mask": np.asarray([False, False, True]),
        "rows": {"total": 3, "train": 2, "valid": 1, "test": 1, "drop": 0},
        "split_info": type("SplitInfo", (), {"valid_start_ts": 2_000, "test_start_ts": 3_000})(),
        "interval_ms": 300_000,
        "search_budget_decision": {"status": "default"},
        "cpcv_lite_runtime": {"enabled": False, "trigger": "disabled"},
    }

    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble.prepare_v4_training_inputs",
        lambda **kwargs: prepared,
    )
    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble._load_v5_regression_targets",
        lambda **kwargs: {"h12": np.asarray([0.0, 0.1, 0.2], dtype=np.float64)},
    )
    monkeypatch.setattr("autobot.models.train_v5_panel_ensemble.v4.build_v4_shared_economic_objective_profile", lambda: {"profile_id": "test"})
    monkeypatch.setattr("autobot.models.train_v5_panel_ensemble.v4._build_lane_governance_v4", lambda **kwargs: {"lane_id": "cls_primary"})
    monkeypatch.setattr("autobot.models.train_v5_panel_ensemble.v4._build_research_support_lane_v4", lambda **kwargs: {"summary": {"status": "ok"}})
    monkeypatch.setattr("autobot.models.train_v5_panel_ensemble.v4._detect_duplicate_candidate_artifacts", lambda **kwargs: {"duplicate": False})
    calls = {
        "execution_acceptance": 0,
        "runtime_recommendations": 0,
        "promotion": 0,
        "decision_surface": 0,
    }

    def _fake_execution_acceptance(**kwargs):
        _ = kwargs
        calls["execution_acceptance"] += 1
        return {"status": "ok"}

    def _fake_runtime_recommendations(**kwargs):
        _ = kwargs
        calls["runtime_recommendations"] += 1
        return {"status": "ready", "exit": {}}

    def _fake_promotion(**kwargs):
        _ = kwargs
        calls["promotion"] += 1
        return {"status": "candidate", "reasons": ["ok"]}

    def _fake_decision_surface(**kwargs):
        _ = kwargs
        calls["decision_surface"] += 1
        return {"status": "ok"}

    monkeypatch.setattr("autobot.models.train_v5_panel_ensemble.v4._run_execution_acceptance_v4", _fake_execution_acceptance)
    monkeypatch.setattr("autobot.models.train_v5_panel_ensemble.v4._build_runtime_recommendations_v4", _fake_runtime_recommendations)
    monkeypatch.setattr("autobot.models.train_v5_panel_ensemble.v4._build_exit_path_risk_summary_v4", lambda **kwargs: {"status": "ok"})
    monkeypatch.setattr("autobot.models.train_v5_panel_ensemble.v4._build_trade_action_policy_v4", lambda **kwargs: {"status": "ok"})
    monkeypatch.setattr("autobot.models.train_v5_panel_ensemble.v4._build_execution_risk_control_v4", lambda **kwargs: {"status": "ok"})
    monkeypatch.setattr("autobot.models.train_v5_panel_ensemble.v4._purge_execution_artifact_run_dirs", lambda **kwargs: {"evaluated": False})
    monkeypatch.setattr("autobot.models.train_v5_panel_ensemble.v4._manual_promotion_decision_v4", _fake_promotion)
    monkeypatch.setattr("autobot.models.train_v5_panel_ensemble.v4._build_trainer_research_evidence_from_promotion_v4", lambda **kwargs: {"available": True})
    monkeypatch.setattr("autobot.models.train_v5_panel_ensemble.build_decision_surface_v4", _fake_decision_surface)
    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble.v4.build_experiment_ledger_record",
        lambda **kwargs: {"run_id": run_id},
    )
    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble.v4.append_experiment_ledger_record",
        lambda **kwargs: tmp_path / "ledger.jsonl",
    )
    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble.v4.load_experiment_ledger",
        lambda **kwargs: [{"run_id": run_id}],
    )
    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble.v4.build_recent_experiment_ledger_summary",
        lambda **kwargs: {"records_considered": 1},
    )
    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble.v4.write_latest_experiment_ledger_summary",
        lambda **kwargs: tmp_path / "ledger_summary.json",
    )

    result = resume_v5_panel_ensemble_tail(run_dir=run_dir)
    calls_after_first = dict(calls)

    assert result.run_id == run_id
    assert (run_dir / "execution_acceptance_report.json").exists()
    assert (run_dir / "runtime_recommendations.json").exists()
    assert (run_dir / "promotion_decision.json").exists()
    assert (run_dir / "expert_prediction_table.parquet").exists()
    assert (run_dir / "panel_tail_context.json").exists()
    artifact_status = json.loads((run_dir / "artifact_status.json").read_text(encoding="utf-8"))
    assert artifact_status["tail_context_written"] is True
    assert artifact_status["execution_acceptance_complete"] is True
    assert artifact_status["runtime_recommendations_complete"] is True
    assert artifact_status["governance_artifacts_complete"] is True
    assert artifact_status["promotion_complete"] is True
    assert artifact_status["decision_surface_complete"] is True
    assert artifact_status["expert_prediction_table_complete"] is True
    report = json.loads(result.train_report_path.read_text(encoding="utf-8"))
    assert report["data_platform_ready_snapshot_id"] == "snapshot-resume"
    assert report["resumed"] is True
    assert float(report["tail_duration_sec"]) >= 0.0

    resumed_result = resume_v5_panel_ensemble_tail(run_dir=run_dir)

    assert resumed_result.run_id == run_id
    assert calls == calls_after_first


def test_resume_v5_panel_ensemble_tail_dependency_expert_only_skips_heavy_tail(tmp_path: Path, monkeypatch) -> None:
    registry_root = tmp_path / "registry"
    logs_root = tmp_path / "logs"
    run_id = "resume-dependency-run"
    run_dir = registry_root / "train_v5_panel_ensemble" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    options = TrainV5PanelEnsembleOptions(
        dataset_root=tmp_path / "features",
        registry_root=registry_root,
        logs_root=logs_root,
        model_family="train_v5_panel_ensemble",
        tf="5m",
        quote="KRW",
        top_n=10,
        start="2026-03-20",
        end="2026-03-29",
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
        execution_acceptance_enabled=True,
        execution_acceptance_parquet_root=tmp_path / "parquet",
        execution_acceptance_output_root=tmp_path / "backtest",
        execution_acceptance_eval_start="2026-03-27",
        execution_acceptance_eval_end="2026-03-29",
        run_scope="scheduled_daily_dependency_v5_panel_ensemble",
        dependency_expert_only=True,
    )
    options.logs_root.mkdir(parents=True, exist_ok=True)
    options.execution_acceptance_output_root.mkdir(parents=True, exist_ok=True)

    train_config = asdict(options)
    train_config["data_platform_ready_snapshot_id"] = "snapshot-resume-dependency"
    (run_dir / "train_config.yaml").write_text(json.dumps(train_config, default=str), encoding="utf-8")
    (run_dir / "metrics.json").write_text(json.dumps({"panel_ensemble": {"policy": "v5_panel_ensemble_v1"}}), encoding="utf-8")
    (run_dir / "thresholds.json").write_text(json.dumps({"top_5pct": 0.5}), encoding="utf-8")
    (run_dir / "leaderboard_row.json").write_text(json.dumps({"run_id": run_id}), encoding="utf-8")
    (run_dir / "walk_forward_report.json").write_text(json.dumps({"summary": {"windows_run": 1}, "_trade_action_oos_rows": []}), encoding="utf-8")
    (run_dir / "cpcv_lite_report.json").write_text(json.dumps({"summary": {"status": "disabled"}}), encoding="utf-8")
    (run_dir / "factor_block_selection.json").write_text(json.dumps({"summary": {"status": "ok"}, "selection_mode": "off"}), encoding="utf-8")
    (run_dir / "selection_recommendations.json").write_text(json.dumps({"top_5pct": {"threshold": 0.5}}), encoding="utf-8")
    (run_dir / "selection_policy.json").write_text(json.dumps({"mode": "raw_threshold"}), encoding="utf-8")
    (run_dir / "selection_calibration.json").write_text(json.dumps({"default_score_source": "score_mean"}), encoding="utf-8")
    (run_dir / "search_budget_decision.json").write_text(json.dumps({"status": "default"}), encoding="utf-8")
    joblib.dump({"model_type": "v5_panel_ensemble", "estimator": _DummyEstimator()}, run_dir / "model.bin")

    dataset = type(
        "Dataset",
        (),
        {
            "rows": 3,
            "X": np.asarray([[0.1], [0.2], [0.3]], dtype=np.float64),
            "y_cls": np.asarray([0, 1, 1], dtype=np.int64),
            "y_reg": np.asarray([0.0, 0.1, 0.2], dtype=np.float64),
            "y_rank": np.asarray([0.0, 0.1, 0.2], dtype=np.float64),
            "sample_weight": np.asarray([1.0, 1.0, 1.0], dtype=np.float64),
            "markets": np.asarray(["KRW-BTC", "KRW-ETH", "KRW-BTC"], dtype=object),
            "selected_markets": ("KRW-BTC", "KRW-ETH"),
            "feature_names": ("f1",),
            "ts_ms": np.asarray([1_000, 2_000, 3_000], dtype=np.int64),
        },
    )()
    prepared = {
        "dataset": dataset,
        "label_spec": {"canonical_multi_horizon_columns": {"y_reg_resid_leader": ["y_reg_resid_leader_h12"]}},
        "label_contract": {
            "y_cls_column": "y_cls_resid_leader_topq_h12",
            "y_reg_column": "y_reg_resid_leader_h12",
            "y_rank_column": "y_rank_resid_leader_h12",
            "primary_horizon_bars": 12,
        },
        "request": object(),
        "train_mask": np.asarray([True, True, False]),
        "valid_mask": np.asarray([False, False, True]),
        "test_mask": np.asarray([False, False, True]),
        "rows": {"total": 3, "train": 2, "valid": 1, "test": 1, "drop": 0},
        "split_info": type("SplitInfo", (), {"valid_start_ts": 2_000, "test_start_ts": 3_000})(),
        "interval_ms": 300_000,
        "search_budget_decision": {"status": "default"},
        "cpcv_lite_runtime": {"enabled": False, "trigger": "disabled"},
    }

    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble.prepare_v4_training_inputs",
        lambda **kwargs: prepared,
    )
    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble._load_v5_regression_targets",
        lambda **kwargs: {"h12": np.asarray([0.0, 0.1, 0.2], dtype=np.float64)},
    )
    monkeypatch.setattr("autobot.models.train_v5_panel_ensemble.v4.build_v4_shared_economic_objective_profile", lambda: {"profile_id": "test"})
    monkeypatch.setattr("autobot.models.train_v5_panel_ensemble.v4._build_lane_governance_v4", lambda **kwargs: {"lane_id": "cls_primary"})
    monkeypatch.setattr("autobot.models.train_v5_panel_ensemble.v4._build_research_support_lane_v4", lambda **kwargs: {"summary": {"status": "ok"}})
    monkeypatch.setattr("autobot.models.train_v5_panel_ensemble.v4._detect_duplicate_candidate_artifacts", lambda **kwargs: {"duplicate": False})

    def _fail_heavy(**kwargs):
        raise AssertionError("heavy panel tail should not run in dependency expert-only mode")

    monkeypatch.setattr("autobot.models.train_v5_panel_ensemble.v4._run_execution_acceptance_v4", _fail_heavy)
    monkeypatch.setattr("autobot.models.train_v5_panel_ensemble.v4._build_runtime_recommendations_v4", _fail_heavy)
    monkeypatch.setattr("autobot.models.train_v5_panel_ensemble.v4._manual_promotion_decision_v4", _fail_heavy)
    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble.v4.build_experiment_ledger_record",
        lambda **kwargs: {"run_id": run_id},
    )
    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble.v4.append_experiment_ledger_record",
        lambda **kwargs: tmp_path / "ledger.jsonl",
    )
    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble.v4.load_experiment_ledger",
        lambda **kwargs: [{"run_id": run_id}],
    )
    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble.v4.build_recent_experiment_ledger_summary",
        lambda **kwargs: {"records_considered": 1},
    )
    monkeypatch.setattr(
        "autobot.models.train_v5_panel_ensemble.v4.write_latest_experiment_ledger_summary",
        lambda **kwargs: tmp_path / "ledger_summary.json",
    )

    result = resume_v5_panel_ensemble_tail(run_dir=run_dir)
    report = json.loads(result.train_report_path.read_text(encoding="utf-8"))
    assert report["dependency_expert_only"] is True
    assert report["tail_mode"] == "dependency_expert_only"
    assert report["resumed"] is True
    assert (run_dir / "execution_acceptance_report.json").exists()
    assert (run_dir / "runtime_recommendations.json").exists()
    assert (run_dir / "promotion_decision.json").exists()
    assert (run_dir / "expert_prediction_table.parquet").exists()
