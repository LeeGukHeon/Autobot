from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from autobot.models.train_v5_fusion import TrainV5FusionOptions
from autobot.models.train_v5_lob import TrainV5LobOptions
from autobot.models.train_v5_sequence import TrainV5SequenceOptions
from autobot.models.v5_variant_selection import (
    run_v5_fusion_variant_matrix,
    run_v5_lob_variant_matrix,
    run_v5_sequence_variant_matrix,
)


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _make_common_run_artifacts(
    run_dir: Path,
    *,
    train_config: dict,
    runtime_recommendations: dict,
    leaderboard_row: dict,
    walk_forward_report: dict,
    runtime_viability: dict | None = None,
) -> None:
    _write_json(run_dir / "train_config.yaml", train_config)
    _write_json(run_dir / "runtime_recommendations.json", runtime_recommendations)
    _write_json(run_dir / "leaderboard_row.json", leaderboard_row)
    _write_json(run_dir / "walk_forward_report.json", walk_forward_report)
    _write_json(
        run_dir / "artifact_status.json",
        {
            "core_saved": True,
            "support_artifacts_written": True,
            "expert_prediction_table_complete": True,
        },
    )
    _write_json(run_dir / "promotion_decision.json", {"status": "candidate"})
    (run_dir / "expert_prediction_table.parquet").write_bytes(b"PAR1")
    if str(runtime_recommendations.get("source_family") or "").strip() == "train_v5_fusion":
        _write_json(
            run_dir / "runtime_viability_report.json",
            runtime_viability
            or {
                "policy": "v5_runtime_viability_report_v1",
                "pass": True,
                "alpha_lcb_floor": -0.01,
                "runtime_rows_total": 100,
                "alpha_lcb_positive_count": 10,
                "rows_above_alpha_floor": 10,
                "rows_above_alpha_floor_ratio": 0.1,
                "expected_return_positive_count": 12,
                "entry_gate_allowed_count": 8,
                "entry_gate_allowed_ratio": 0.08,
                "estimated_intent_candidate_count": 8,
                "primary_reason_code": "PASS",
            },
        )


def test_sequence_variant_matrix_keeps_baseline_when_no_clear_edge(tmp_path: Path, monkeypatch) -> None:
    registry_root = tmp_path / "models" / "registry"
    logs_root = tmp_path / "logs"

    def fake_train(options: TrainV5SequenceOptions):
        variant_name = f"{options.backbone_family}__{options.pretrain_method}"
        run_id = f"sequence-{variant_name}"
        run_dir = registry_root / options.model_family / run_id
        score_map = {
            "patchtst_v1__none": (0.10, 0.60, 0.60, 0.40, 0.20),
            "patchtst_v1__ts2vec_v1": (0.10, 0.59, 0.60, 0.40, 0.20),
            "patchtst_v1__timemae_v1": (0.09, 0.58, 0.58, 0.42, 0.21),
            "timemixer_v1__ts2vec_v1": (0.11, 0.58, 0.58, 0.41, 0.22),
        }
        ev, precision, pr_auc, log_loss, brier = score_map[variant_name]
        _make_common_run_artifacts(
            run_dir,
            train_config={
                "trainer": "v5_sequence",
                "model_family": options.model_family,
                "start": options.start,
                "end": options.end,
                "quote": options.quote,
                "top_n": options.top_n,
                "run_scope": options.run_scope,
                "backbone_family": options.backbone_family,
                "pretrain_method": options.pretrain_method,
                "sequence_variant_name": variant_name,
            },
            runtime_recommendations={
                "source_family": "train_v5_sequence",
                "sequence_variant_name": variant_name,
                "sequence_backbone_name": options.backbone_family,
                "sequence_pretrain_method": options.pretrain_method,
            },
            leaderboard_row={
                "test_ev_net_top5": ev,
                "test_precision_top5": precision,
                "test_pr_auc": pr_auc,
                "test_log_loss": log_loss,
                "test_brier_score": brier,
            },
            walk_forward_report={"realized_pnl_quote": ev * 100.0},
        )
        _write_json(
            run_dir / "sequence_pretrain_contract.json",
            {
                "policy": "sequence_pretrain_contract_v1",
                "backbone_family": options.backbone_family,
                "pretrain_method": options.pretrain_method,
                "pretrain_impl_method": "none" if options.pretrain_method == "none" else ("ts2vec_like" if options.pretrain_method == "ts2vec_v1" else "timemae_like"),
                "pretrain_ready": options.pretrain_method != "none",
                "encoder_artifact_path": str(run_dir / "sequence_pretrain_encoder.pt") if options.pretrain_method != "none" else "",
            },
        )
        if options.pretrain_method != "none":
            (run_dir / "sequence_pretrain_encoder.pt").write_bytes(b"PTSEQ")
        _write_json(
            run_dir / "sequence_pretrain_report.json",
            {
                "policy": "sequence_pretrain_report_v1",
                "objective_name": "none" if options.pretrain_method == "none" else "ts2vec_alignment_variance_v1",
                "status": "disabled" if options.pretrain_method == "none" else "enabled",
                "best_epoch": 0 if options.pretrain_method == "none" else 1,
                "encoder_dim": 16,
                "final_loss": None if options.pretrain_method == "none" else 0.1,
                "final_component_values": {} if options.pretrain_method == "none" else {"alignment_loss": 0.1},
                "encoder_norm_summary": {"parameter_tensor_count": 0 if options.pretrain_method == "none" else 4},
            },
        )
        _write_json(
            run_dir / "domain_weighting_report.json",
            {"policy": "v5_domain_weighting_v1", "effective_sample_weight_summary": {"mean": 1.0}},
        )
        _write_json(
            run_dir / "ood_generalization_report.json",
            {
                "status": "informative_ready",
                "source_kind": "regime_inverse_frequency_v1",
                "invariant_penalty_enabled": False,
                "train_vs_future_domain_gap_summary": {"future_to_train_ratio": 0.5},
            },
        )
        return SimpleNamespace(run_dir=run_dir)

    monkeypatch.setattr("autobot.models.v5_variant_selection.train_and_register_v5_sequence", fake_train)

    payload = run_v5_sequence_variant_matrix(
        TrainV5SequenceOptions(
            dataset_root=tmp_path / "sequence_v1",
            registry_root=registry_root,
            logs_root=logs_root,
            model_family="train_v5_sequence",
            quote="KRW",
            top_n=20,
            start="2026-03-01",
            end="2026-03-07",
            seed=42,
            run_scope="scheduled_daily_dependency_v5_sequence",
        )
    )

    assert payload["chosen_variant_name"] == "patchtst_v1__none"
    report = json.loads(Path(payload["variant_report_path"]).read_text(encoding="utf-8"))
    assert report["chosen_variant_name"] == "patchtst_v1__none"
    assert report["chosen_reason_code"] == "BASELINE_RETAINED_NO_CLEAR_EDGE"
    assert report["evaluated_variant_count"] == 4
    assert report["pretrain_summary_by_variant"]["patchtst_v1__none"]["pretrain_ready"] is False
    assert report["selection_evidence"]["pretrain_stability"] is True
    assert report["selection_evidence"]["ood_status"] == "informative_ready"
    assert report["selection_evidence"]["ood_source_kind"] == "regime_inverse_frequency_v1"


def test_lob_variant_matrix_keeps_deeplob_when_bdlob_uncertainty_edge_is_weak(tmp_path: Path, monkeypatch) -> None:
    registry_root = tmp_path / "models" / "registry"
    logs_root = tmp_path / "logs"

    def fake_train(options: TrainV5LobOptions):
        variant_name = options.backbone_family
        run_id = f"lob-{variant_name}"
        run_dir = registry_root / options.model_family / run_id
        score_map = {
            "deeplob_v1": (0.10, 0.60, 0.60, 0.40, 0.100),
            "bdlob_v1": (0.12, 0.62, 0.61, 0.39, 0.099),
            "hlob_v1": (0.09, 0.58, 0.57, 0.43, 0.120),
        }
        ev, precision, pr_auc, log_loss, brier = score_map[variant_name]
        _make_common_run_artifacts(
            run_dir,
            train_config={
                "trainer": "v5_lob",
                "model_family": options.model_family,
                "start": options.start,
                "end": options.end,
                "quote": options.quote,
                "top_n": options.top_n,
                "run_scope": options.run_scope,
                "backbone_family": options.backbone_family,
                "lob_variant_name": variant_name,
            },
            runtime_recommendations={
                "source_family": "train_v5_lob",
                "lob_variant_name": variant_name,
                "lob_backbone_name": options.backbone_family,
            },
            leaderboard_row={
                "test_ev_net_top5": ev,
                "test_precision_top5": precision,
                "test_pr_auc": pr_auc,
                "test_log_loss": log_loss,
                "test_brier_score": brier,
            },
            walk_forward_report={"realized_pnl_quote": ev * 100.0, "max_drawdown_pct": 0.10},
        )
        _write_json(
            run_dir / "lob_backbone_contract.json",
            {"policy": "lob_backbone_contract_v1", "backbone_family": options.backbone_family, "uncertainty_head": "softplus_scalar"},
        )
        _write_json(
            run_dir / "lob_target_contract.json",
            {
                "policy": "lob_target_contract_v1",
                "primary_horizon_seconds": 30,
                "auxiliary_targets": ["five_min_alpha"],
                "uncertainty_target": "micro_uncertainty",
                "support_level_quality_summary": {
                    "strict_full_ratio": 0.7 if variant_name == "deeplob_v1" else (0.6 if variant_name == "bdlob_v1" else 0.8),
                    "structural_invalid_ratio": 0.02 if variant_name == "deeplob_v1" else (0.03 if variant_name == "bdlob_v1" else 0.01),
                },
            },
        )
        _write_json(
            run_dir / "walk_forward_report.json",
            {
                "realized_pnl_quote": ev * 100.0,
                "max_drawdown_pct": 0.10,
            },
        )
        _write_json(run_dir / "domain_weighting_report.json", {"policy": "v5_domain_weighting_v1", "effective_sample_weight_summary": {"mean": 1.0}})
        return SimpleNamespace(run_dir=run_dir)

    monkeypatch.setattr("autobot.models.v5_variant_selection.train_and_register_v5_lob", fake_train)

    payload = run_v5_lob_variant_matrix(
        TrainV5LobOptions(
            dataset_root=tmp_path / "sequence_v1",
            registry_root=registry_root,
            logs_root=logs_root,
            model_family="train_v5_lob",
            quote="KRW",
            top_n=20,
            start="2026-03-01",
            end="2026-03-07",
            seed=42,
            run_scope="scheduled_daily_dependency_v5_lob",
        )
    )

    assert payload["chosen_variant_name"] == "deeplob_v1"
    report = json.loads(Path(payload["variant_report_path"]).read_text(encoding="utf-8"))
    assert report["chosen_variant_name"] == "deeplob_v1"
    assert report["chosen_reason_code"] == "BASELINE_RETAINED_NO_CLEAR_EDGE"
    assert report["selection_evidence"]["support_level_coverage_summary"]["strict_full_ratio"] == 0.7


def test_fusion_variant_matrix_keeps_linear_when_regime_moe_regresses_execution_structure(tmp_path: Path, monkeypatch) -> None:
    registry_root = tmp_path / "models" / "registry"
    logs_root = tmp_path / "logs"
    seq_dir = registry_root / "train_v5_sequence" / "sequence-winner"
    lob_dir = registry_root / "train_v5_lob" / "lob-winner"
    panel_dir = registry_root / "train_v5_panel_ensemble" / "panel-run"
    trad_dir = registry_root / "train_v5_tradability" / "trad-run"
    for run_dir, train_config, runtime_recommendations in (
        (
            seq_dir,
            {"sequence_variant_name": "patchtst_v1__none"},
            {"sequence_variant_name": "patchtst_v1__none", "sequence_backbone_name": "patchtst_v1", "sequence_pretrain_status": "disabled", "sequence_pretrain_objective": "none"},
        ),
        (
            lob_dir,
            {"lob_variant_name": "deeplob_v1"},
            {"lob_variant_name": "deeplob_v1", "lob_backbone_name": "deeplob_v1"},
        ),
        (panel_dir, {}, {}),
        (trad_dir, {}, {}),
    ):
        _make_common_run_artifacts(
            run_dir,
            train_config={"trainer": "seed", "model_family": run_dir.parent.name, **train_config},
            runtime_recommendations=runtime_recommendations,
            leaderboard_row={"test_ev_net_top5": 0.1},
            walk_forward_report={"realized_pnl_quote": 10.0},
        )

    def fake_train(options: TrainV5FusionOptions):
        variant_name = options.stacker_family
        run_id = f"fusion-{variant_name}"
        run_dir = registry_root / options.model_family / run_id
        score_map = {
            "linear": (0.10, 0.60, 0.60, 0.40),
            "monotone_gbdt": (0.11, 0.60, 0.61, 0.39),
            "regime_moe": (0.13, 0.58, 0.62, 0.43),
        }
        ev, precision, pr_auc, log_loss = score_map[variant_name]
        _make_common_run_artifacts(
            run_dir,
            train_config={
                "trainer": "v5_fusion",
                "model_family": options.model_family,
                "start": options.start,
                "end": options.end,
                "quote": options.quote,
                "run_scope": options.run_scope,
                "stacker_family": options.stacker_family,
                "fusion_variant_name": variant_name,
            },
            runtime_recommendations={
                "source_family": "train_v5_fusion",
                "fusion_variant_name": variant_name,
                "fusion_stacker_family": variant_name,
                "fusion_gating_policy": "sequence_regime_embedding_nearest_centroid_v1" if variant_name == "regime_moe" else "single_expert_v1",
                "sequence_variant_name": "patchtst_v1__none",
                "lob_variant_name": "deeplob_v1",
            },
            leaderboard_row={
                "test_ev_net_top5": ev,
                "test_precision_top5": precision,
                "test_pr_auc": pr_auc,
                "test_log_loss": log_loss,
            },
            walk_forward_report={"realized_pnl_quote": ev * 100.0},
        )
        _write_json(run_dir / "fusion_model_contract.json", {"policy": "v5_fusion_v1"})
        _write_json(run_dir / "fusion_runtime_input_contract.json", {"policy": "v5_fusion_runtime_input_contract_v1"})
        _write_json(run_dir / "domain_weighting_report.json", {"policy": "v5_domain_weighting_v1", "effective_sample_weight_summary": {"mean": 1.0}})
        return SimpleNamespace(run_dir=run_dir)

    monkeypatch.setattr("autobot.models.v5_variant_selection.train_and_register_v5_fusion", fake_train)

    payload = run_v5_fusion_variant_matrix(
        TrainV5FusionOptions(
            panel_input_path=panel_dir / "expert_prediction_table.parquet",
            sequence_input_path=seq_dir / "expert_prediction_table.parquet",
            lob_input_path=lob_dir / "expert_prediction_table.parquet",
            tradability_input_path=trad_dir / "expert_prediction_table.parquet",
            registry_root=registry_root,
            logs_root=logs_root,
            model_family="train_v5_fusion",
            quote="KRW",
            start="2026-03-01",
            end="2026-03-07",
            seed=42,
            run_scope="scheduled_daily",
        )
    )

    assert payload["chosen_variant_name"] == "linear"
    report = json.loads(Path(payload["variant_report_path"]).read_text(encoding="utf-8"))
    assert report["chosen_variant_name"] == "linear"
    assert report["selected_sequence_variant_name"] == "patchtst_v1__none"
    assert report["selected_lob_variant_name"] == "deeplob_v1"
    assert report["selected_fusion_stacker"] == "linear"
    assert report["offline_winner_variant_name"] == "linear"
    assert report["default_eligible_variant_name"] == "linear"


def test_fusion_variant_matrix_rejects_zero_runtime_viability_candidate(tmp_path: Path, monkeypatch) -> None:
    registry_root = tmp_path / "models" / "registry"
    logs_root = tmp_path / "logs"
    seq_dir = registry_root / "train_v5_sequence" / "sequence-winner"
    lob_dir = registry_root / "train_v5_lob" / "lob-winner"
    panel_dir = registry_root / "train_v5_panel_ensemble" / "panel-run"
    trad_dir = registry_root / "train_v5_tradability" / "trad-run"
    for run_dir, train_config, runtime_recommendations in (
        (
            seq_dir,
            {"sequence_variant_name": "patchtst_v1__none"},
            {"sequence_variant_name": "patchtst_v1__none", "sequence_backbone_name": "patchtst_v1", "sequence_pretrain_status": "disabled", "sequence_pretrain_objective": "none"},
        ),
        (
            lob_dir,
            {"lob_variant_name": "deeplob_v1"},
            {"lob_variant_name": "deeplob_v1", "lob_backbone_name": "deeplob_v1"},
        ),
        (panel_dir, {}, {}),
        (trad_dir, {}, {}),
    ):
        _make_common_run_artifacts(
            run_dir,
            train_config={"trainer": "seed", "model_family": run_dir.parent.name, **train_config},
            runtime_recommendations=runtime_recommendations,
            leaderboard_row={"test_ev_net_top5": 0.1},
            walk_forward_report={"realized_pnl_quote": 10.0},
        )

    def fake_train(options: TrainV5FusionOptions):
        variant_name = options.stacker_family
        run_id = f"fusion-{variant_name}"
        run_dir = registry_root / options.model_family / run_id
        score_map = {
            "linear": (0.10, 0.60, 0.60, 0.40),
            "monotone_gbdt": (0.20, 0.80, 0.80, 0.20),
            "regime_moe": (0.10, 0.59, 0.60, 0.41),
        }
        ev, precision, pr_auc, log_loss = score_map[variant_name]
        viability = None
        if variant_name == "monotone_gbdt":
            viability = {
                "policy": "v5_runtime_viability_report_v1",
                "pass": False,
                "alpha_lcb_floor": 0.0,
                "runtime_rows_total": 100,
                "alpha_lcb_positive_count": 0,
                "rows_above_alpha_floor": 0,
                "rows_above_alpha_floor_ratio": 0.0,
                "expected_return_positive_count": 0,
                "entry_gate_allowed_count": 0,
                "entry_gate_allowed_ratio": 0.0,
                "estimated_intent_candidate_count": 0,
                "primary_reason_code": "FUSION_RUNTIME_ALPHA_LCB_ZERO_VIABILITY",
            }
        _make_common_run_artifacts(
            run_dir,
            train_config={
                "trainer": "v5_fusion",
                "model_family": options.model_family,
                "start": options.start,
                "end": options.end,
                "quote": options.quote,
                "run_scope": options.run_scope,
                "stacker_family": options.stacker_family,
                "fusion_variant_name": variant_name,
            },
            runtime_recommendations={
                "source_family": "train_v5_fusion",
                "fusion_variant_name": variant_name,
                "fusion_stacker_family": variant_name,
                "fusion_gating_policy": "single_expert_v1",
                "sequence_variant_name": "patchtst_v1__none",
                "lob_variant_name": "deeplob_v1",
            },
            leaderboard_row={
                "test_ev_net_top5": ev,
                "test_precision_top5": precision,
                "test_pr_auc": pr_auc,
                "test_log_loss": log_loss,
            },
            walk_forward_report={"realized_pnl_quote": ev * 100.0},
            runtime_viability=viability,
        )
        _write_json(run_dir / "fusion_model_contract.json", {"policy": "v5_fusion_v1"})
        _write_json(run_dir / "fusion_runtime_input_contract.json", {"policy": "v5_fusion_runtime_input_contract_v1"})
        _write_json(run_dir / "domain_weighting_report.json", {"policy": "v5_domain_weighting_v1", "effective_sample_weight_summary": {"mean": 1.0}})
        return SimpleNamespace(run_dir=run_dir)

    monkeypatch.setattr("autobot.models.v5_variant_selection.train_and_register_v5_fusion", fake_train)

    payload = run_v5_fusion_variant_matrix(
        TrainV5FusionOptions(
            panel_input_path=panel_dir / "expert_prediction_table.parquet",
            sequence_input_path=seq_dir / "expert_prediction_table.parquet",
            lob_input_path=lob_dir / "expert_prediction_table.parquet",
            tradability_input_path=trad_dir / "expert_prediction_table.parquet",
            registry_root=registry_root,
            logs_root=logs_root,
            model_family="train_v5_fusion",
            quote="KRW",
            start="2026-03-01",
            end="2026-03-07",
            seed=42,
            run_scope="scheduled_daily",
        )
    )

    assert payload["chosen_variant_name"] == "linear"
    report = json.loads(Path(payload["variant_report_path"]).read_text(encoding="utf-8"))
    assert "FUSION_RUNTIME_ALPHA_LCB_ZERO_VIABILITY" in report["rejection_reasons"]["monotone_gbdt"]
    assert report["runtime_viability_pass"] is True
