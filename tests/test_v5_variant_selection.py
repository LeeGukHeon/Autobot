from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from autobot.models.train_v5_fusion import TrainV5FusionOptions
from autobot.models.train_v5_lob import TrainV5LobOptions
from autobot.models.train_v5_sequence import TrainV5SequenceOptions
from autobot.models.v5_variant_selection import (
    run_v5_fusion_input_ablation_matrix,
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
    runtime_deploy_contract_readiness: dict | None = None,
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
        _write_json(
            run_dir / "runtime_deploy_contract_readiness.json",
            runtime_deploy_contract_readiness
            or {
                "policy": "v5_runtime_deploy_contract_readiness_v1",
                "evaluation_contract_id": "runtime_deploy_contract_v1",
                "evaluation_contract_role": "deploy_runtime",
                "decision_contract_version": "v5_post_model_contract_v1",
                "pass": True,
                "primary_reason_code": "PASS",
                "required_components": ["exit", "execution"],
                "advisory_components": ["trade_action", "risk_control"],
                "component_readiness": {
                    "exit": {"required": True, "ready": True, "reason_codes": []},
                    "execution": {"required": True, "ready": True, "reason_codes": []},
                    "trade_action": {"required": False, "ready": True, "reason_codes": []},
                    "risk_control": {"required": False, "ready": True, "reason_codes": []},
                },
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
    assert report["offline_winner_variant_name"] == "regime_moe"
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
    latest_payload = json.loads((registry_root / "train_v5_fusion" / "latest.json").read_text(encoding="utf-8"))
    assert latest_payload["run_id"] == "fusion-linear"


def test_fusion_variant_matrix_never_reverts_to_invalid_linear_baseline(tmp_path: Path, monkeypatch) -> None:
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
            "linear": (0.30, 0.90, 0.90, 0.10),
            "monotone_gbdt": (0.20, 0.70, 0.70, 0.20),
            "regime_moe": (0.21, 0.71, 0.71, 0.19),
        }
        ev, precision, pr_auc, log_loss = score_map[variant_name]
        viability = {
            "policy": "v5_runtime_viability_report_v1",
            "pass": variant_name != "linear",
            "alpha_lcb_floor": 0.0,
            "runtime_rows_total": 100,
            "alpha_lcb_positive_count": 0 if variant_name == "linear" else 10,
            "rows_above_alpha_floor": 0 if variant_name == "linear" else 10,
            "rows_above_alpha_floor_ratio": 0.0 if variant_name == "linear" else 0.1,
            "expected_return_positive_count": 0 if variant_name == "linear" else 12,
            "entry_gate_allowed_count": 0 if variant_name == "linear" else 10,
            "entry_gate_allowed_ratio": 0.0 if variant_name == "linear" else 0.1,
            "estimated_intent_candidate_count": 0 if variant_name == "linear" else 10,
            "primary_reason_code": "FUSION_RUNTIME_ALPHA_LCB_ZERO_VIABILITY" if variant_name == "linear" else "PASS",
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

    assert payload["chosen_variant_name"] == "regime_moe"
    assert payload["default_eligible_variant_name"] == "regime_moe"
    assert payload["offline_winner_variant_name"] == "linear"
    report = json.loads(Path(payload["variant_report_path"]).read_text(encoding="utf-8"))
    assert report["chosen_variant_name"] == "regime_moe"
    assert report["offline_winner_variant_name"] == "linear"
    assert report["default_eligible_variant_name"] == "regime_moe"
    latest_payload = json.loads((registry_root / "train_v5_fusion" / "latest.json").read_text(encoding="utf-8"))
    assert latest_payload["run_id"] == "fusion-regime_moe"


def test_fusion_variant_matrix_rejects_runtime_deploy_contract_not_ready_candidate(tmp_path: Path, monkeypatch) -> None:
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
            "regime_moe": (0.19, 0.79, 0.79, 0.21),
        }
        ev, precision, pr_auc, log_loss = score_map[variant_name]
        readiness = {
            "policy": "v5_runtime_deploy_contract_readiness_v1",
            "evaluation_contract_id": "runtime_deploy_contract_v1",
            "evaluation_contract_role": "deploy_runtime",
            "decision_contract_version": "v5_post_model_contract_v1",
            "pass": variant_name != "monotone_gbdt",
            "primary_reason_code": (
                "PASS"
                if variant_name != "monotone_gbdt"
                else "FUSION_RUNTIME_DEPLOY_CONTRACT_EXECUTION_NOT_READY"
            ),
            "required_components": ["exit", "execution"],
            "advisory_components": ["trade_action", "risk_control"],
            "component_readiness": {
                "exit": {"required": True, "ready": True, "reason_codes": []},
                "execution": {
                    "required": True,
                    "ready": variant_name != "monotone_gbdt",
                    "reason_codes": ([] if variant_name != "monotone_gbdt" else ["FUSION_RUNTIME_DEPLOY_CONTRACT_EXECUTION_NOT_READY"]),
                },
                "trade_action": {"required": False, "ready": True, "reason_codes": []},
                "risk_control": {"required": False, "ready": True, "reason_codes": []},
            },
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
            runtime_deploy_contract_readiness=readiness,
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

    assert payload["chosen_variant_name"] == "regime_moe"
    assert payload["default_eligible_variant_name"] == "regime_moe"
    assert payload["runtime_deploy_contract_ready"] is True
    report = json.loads(Path(payload["variant_report_path"]).read_text(encoding="utf-8"))
    assert "FUSION_RUNTIME_DEPLOY_CONTRACT_EXECUTION_NOT_READY" in report["rejection_reasons"]["monotone_gbdt"]
    assert report["runtime_deploy_contract_ready"] is True


def test_fusion_variant_matrix_applies_input_quality_brake_to_small_nonbaseline_edge(tmp_path: Path, monkeypatch) -> None:
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
            {"sequence_variant_name": "patchtst_v1__none", "sequence_backbone_name": "patchtst_v1"},
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

    degraded_input_quality = {
        "policy": "v5_fusion_input_quality_summary_v1",
        "overall_quality_status": "degraded",
        "reason_codes": ["SEQUENCE_REDUCED_CONTEXT_HEAVY", "TRADABILITY_EVIDENCE_THIN"],
        "experts": {
            "sequence": {"quality_status": "reduced_context_heavy"},
            "lob": {"quality_status": "healthy"},
            "tradability": {"quality_status": "thin_training_evidence"},
        },
        "tradability_provenance": {
            "source_kind": "dedicated_tradability_expert",
            "evidence_strength": "thin",
            "quality_status": "thin_training_evidence",
            "reason_codes": ["TRADABILITY_EVIDENCE_THIN"],
        },
    }

    def fake_train(options: TrainV5FusionOptions):
        variant_name = options.stacker_family
        run_id = f"fusion-{variant_name}"
        run_dir = registry_root / options.model_family / run_id
        score_map = {
            "linear": (0.100, 0.60, 0.60, 0.40),
            "monotone_gbdt": (0.105, 0.60, 0.60, 0.39),
            "regime_moe": (0.090, 0.59, 0.59, 0.41),
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
            runtime_viability={
                "policy": "v5_runtime_viability_report_v1",
                "pass": True,
                "alpha_lcb_floor": -0.01,
                "runtime_rows_total": 100,
                "alpha_lcb_positive_count": 12,
                "rows_above_alpha_floor": 12,
                "rows_above_alpha_floor_ratio": 0.12,
                "expected_return_positive_count": 15,
                "entry_gate_allowed_count": 10,
                "entry_gate_allowed_ratio": 0.10,
                "estimated_intent_candidate_count": 10,
                "primary_reason_code": "PASS",
                "input_quality_summary": degraded_input_quality,
                "tradability_provenance": dict(degraded_input_quality["tradability_provenance"]),
            },
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
    assert report["offline_winner_variant_name"] == "monotone_gbdt"
    assert report["chosen_variant_name"] == "linear"
    assert report["selection_evidence"]["input_quality_brake_applied"] is True
    assert report["selection_evidence"]["input_quality_brake_severity"] == "degraded"
    assert report["selection_evidence"]["minimum_utility_edge_vs_linear_required"] == 0.01
    assert report["selection_evidence"]["tradability_evidence_strength"] == "thin"
    assert report["selection_evidence"]["entry_boundary_summary"] == {}
    assert report["selection_evidence"]["sell_side_quality_summary"] == {}
    assert report["selection_evidence"]["boundary_quality_non_regression"] is None
    assert report["selection_evidence"]["sell_side_quality_non_regression"] is None


def test_fusion_variant_matrix_rejects_nonbaseline_when_paired_evidence_regresses(tmp_path: Path, monkeypatch) -> None:
    registry_root = tmp_path / "models" / "registry"
    logs_root = tmp_path / "logs"
    seq_dir = registry_root / "train_v5_sequence" / "sequence-winner"
    lob_dir = registry_root / "train_v5_lob" / "lob-winner"
    panel_dir = registry_root / "train_v5_panel_ensemble" / "panel-run"
    trad_dir = registry_root / "train_v5_tradability" / "trad-run"
    for run_dir, train_config, runtime_recommendations in (
        (seq_dir, {"sequence_variant_name": "patchtst_v1__none"}, {"sequence_variant_name": "patchtst_v1__none"}),
        (lob_dir, {"lob_variant_name": "deeplob_v1"}, {"lob_variant_name": "deeplob_v1"}),
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
            "monotone_gbdt": (0.12, 0.61, 0.61, 0.39),
            "regime_moe": (0.09, 0.59, 0.59, 0.41),
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
        )
        if variant_name == "monotone_gbdt":
            _write_json(
                run_dir / "promotion_decision.json",
                {
                    "comparison_mode": "paired_paper_runtime_decision_v1",
                    "paired_gate": {"pass": False, "reason": "PAIRED_PAPER_NOT_READY"},
                    "decision": {
                        "promote": False,
                        "decision": "keep_champion",
                        "hard_failures": ["PAIRED_PAPER_NOT_READY"],
                        "matched_evidence_checks": {"matched_pnl_not_worse": False},
                    },
                    "paired_report_excerpt": {"decision_language_counts": {"challenger_safety_veto_only": 1}},
                },
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
    assert report["offline_winner_variant_name"] == "monotone_gbdt"
    assert report["baseline_kept_reason_code"] == "PAIRED_NON_REGRESSION_FAILED"
    assessment = report["selection_evidence"]["offline_winner_clear_edge_assessment"]
    assert assessment["reason_code"] == "PAIRED_NON_REGRESSION_FAILED"
    assert assessment["paired_evidence"]["available"] is True
    assert "PAIRED_PAPER_NOT_READY" in assessment["paired_evidence"]["hard_failures"]
    chosen_runtime = json.loads((registry_root / "train_v5_fusion" / "fusion-linear" / "runtime_recommendations.json").read_text(encoding="utf-8"))
    assert chosen_runtime["fusion_evidence_reason_code"] == "PAIRED_NON_REGRESSION_FAILED"
    assert chosen_runtime["fusion_non_regression_summary"]["paired_non_regression"] is False
    assert "PAIRED_PAPER_NOT_READY" in chosen_runtime["fusion_non_regression_summary"]["paired_evidence_summary"]["hard_failures"]


def test_fusion_variant_matrix_rejects_nonbaseline_when_canary_evidence_regresses(tmp_path: Path, monkeypatch) -> None:
    registry_root = tmp_path / "models" / "registry"
    logs_root = tmp_path / "logs"
    seq_dir = registry_root / "train_v5_sequence" / "sequence-winner"
    lob_dir = registry_root / "train_v5_lob" / "lob-winner"
    panel_dir = registry_root / "train_v5_panel_ensemble" / "panel-run"
    trad_dir = registry_root / "train_v5_tradability" / "trad-run"
    for run_dir, train_config, runtime_recommendations in (
        (seq_dir, {"sequence_variant_name": "patchtst_v1__none"}, {"sequence_variant_name": "patchtst_v1__none"}),
        (lob_dir, {"lob_variant_name": "deeplob_v1"}, {"lob_variant_name": "deeplob_v1"}),
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

    canary_root = tmp_path / "logs" / "canary_confidence_sequence" / "autobot_live_alpha_candidate_service"
    canary_root.mkdir(parents=True, exist_ok=True)

    def fake_train(options: TrainV5FusionOptions):
        variant_name = options.stacker_family
        run_id = f"fusion-{variant_name}"
        run_dir = registry_root / options.model_family / run_id
        score_map = {
            "linear": (0.10, 0.60, 0.60, 0.40),
            "monotone_gbdt": (0.12, 0.61, 0.61, 0.39),
            "regime_moe": (0.09, 0.59, 0.59, 0.41),
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
        )
        if variant_name == "monotone_gbdt":
            _write_json(
                canary_root / "latest.json",
                {
                    "policy": "canary_confidence_sequence_v1",
                    "run_id": run_id,
                    "decision": {
                        "status": "abort",
                        "promote_eligible": False,
                        "abort": True,
                        "abort_reason_codes": ["FEATURE_DIVERGENCE_CS_BREACH"],
                        "blocking_reason_codes": ["CANARY_DIVERGENCE_INSUFFICIENT_EVIDENCE"],
                        "execution_liquidation_summary": {"exit_decision_reasons_top": []},
                    },
                },
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
    assert report["offline_winner_variant_name"] == "monotone_gbdt"
    assert report["baseline_kept_reason_code"] == "CANARY_NON_REGRESSION_FAILED"
    assessment = report["selection_evidence"]["offline_winner_clear_edge_assessment"]
    assert assessment["reason_code"] == "CANARY_NON_REGRESSION_FAILED"
    assert assessment["canary_evidence"]["available"] is True
    assert assessment["canary_evidence"]["abort"] is True
    chosen_promotion = json.loads((registry_root / "train_v5_fusion" / "fusion-linear" / "promotion_decision.json").read_text(encoding="utf-8"))
    assert chosen_promotion["fusion_evidence_reason_code"] == "CANARY_NON_REGRESSION_FAILED"
    assert chosen_promotion["fusion_non_regression_summary"]["canary_non_regression"] is False
    assert chosen_promotion["fusion_non_regression_summary"]["canary_evidence_summary"]["abort"] is True


def test_fusion_variant_matrix_records_forced_input_variant_provenance(tmp_path: Path, monkeypatch) -> None:
    registry_root = tmp_path / "models" / "registry"
    logs_root = tmp_path / "logs"
    seq_dir = registry_root / "train_v5_sequence" / "sequence-winner"
    lob_dir = registry_root / "train_v5_lob" / "lob-winner"
    panel_dir = registry_root / "train_v5_panel_ensemble" / "panel-run"
    trad_dir = registry_root / "train_v5_tradability" / "trad-run"
    for run_dir, train_config, runtime_recommendations in (
        (seq_dir, {"sequence_variant_name": "patchtst_v1__none"}, {"sequence_variant_name": "patchtst_v1__none"}),
        (lob_dir, {"lob_variant_name": "deeplob_v1"}, {"lob_variant_name": "deeplob_v1"}),
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
                "input_variant_name": options.input_variant_name,
                "include_sequence": False,
                "include_lob": False,
                "include_tradability": False,
                "fusion_variant_name": variant_name,
            },
            runtime_recommendations={
                "source_family": "train_v5_fusion",
                "fusion_variant_name": variant_name,
                "fusion_stacker_family": variant_name,
                "fusion_input_variant_name": options.input_variant_name,
                "fusion_included_experts": ["panel"],
                "fusion_excluded_experts": ["sequence", "lob", "tradability"],
            },
            leaderboard_row={
                "test_ev_net_top5": 0.10,
                "test_precision_top5": 0.60,
                "test_pr_auc": 0.60,
                "test_log_loss": 0.40,
            },
            walk_forward_report={"realized_pnl_quote": 10.0},
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
            input_variant_name="panel_only",
        )
    )

    report = json.loads(Path(payload["variant_report_path"]).read_text(encoding="utf-8"))
    assert report["input_provenance"]["input_variant_name"] == "panel_only"
    assert report["input_provenance"]["included_experts"] == ["panel"]
    assert set(report["input_provenance"]["excluded_experts"]) == {"sequence", "lob", "tradability"}


def test_fusion_input_ablation_matrix_selects_clear_edge_variant(tmp_path: Path, monkeypatch) -> None:
    registry_root = tmp_path / "models" / "registry"
    logs_root = tmp_path / "logs"

    def fake_run(options: TrainV5FusionOptions, publish_latest: bool = True):  # noqa: ARG001
        input_variant_name = str(options.input_variant_name)
        run_id = f"fusion-{input_variant_name}"
        run_dir = registry_root / options.model_family / run_id
        included_map = {
            "full_fusion": ["panel", "sequence", "lob", "tradability"],
            "full_without_tradability": ["panel", "sequence", "lob"],
            "panel_plus_sequence": ["panel", "sequence"],
            "panel_plus_lob": ["panel", "lob"],
            "panel_only": ["panel"],
        }
        included_experts = included_map[input_variant_name]
        excluded_experts = [name for name in ("sequence", "lob", "tradability") if name not in included_experts]
        metric_map = {
            "full_fusion": (10.0, 0.10, 0.60, 0.60, 0.40),
            "full_without_tradability": (9.5, 0.095, 0.59, 0.59, 0.41),
            "panel_plus_sequence": (10.5, 0.101, 0.60, 0.60, 0.40),
            "panel_plus_lob": (9.8, 0.099, 0.60, 0.60, 0.40),
            "panel_only": (12.0, 0.12, 0.62, 0.62, 0.38),
        }
        pnl, ev, precision, pr_auc, log_loss = metric_map[input_variant_name]
        _make_common_run_artifacts(
            run_dir,
            train_config={
                "trainer": "v5_fusion",
                "model_family": options.model_family,
                "start": options.start,
                "end": options.end,
                "quote": options.quote,
                "run_scope": options.run_scope,
                "stacker_family": "linear",
                "input_variant_name": input_variant_name,
                "include_sequence": str("sequence" in included_experts),
                "include_lob": str("lob" in included_experts),
                "include_tradability": str("tradability" in included_experts),
            },
            runtime_recommendations={
                "source_family": "train_v5_fusion",
                "fusion_variant_name": "linear",
                "fusion_stacker_family": "linear",
                "fusion_input_variant_name": input_variant_name,
                "fusion_included_experts": included_experts,
                "fusion_excluded_experts": excluded_experts,
            },
            leaderboard_row={
                "test_ev_net_top5": ev,
                "test_precision_top5": precision,
                "test_pr_auc": pr_auc,
                "test_log_loss": log_loss,
            },
            walk_forward_report={"realized_pnl_quote": pnl},
        )
        _write_json(
            run_dir / "fusion_variant_report.json",
            {
                "policy": "v5_fusion_variant_report_v1",
                "chosen_variant_name": "linear",
                "chosen_reason_code": "BASELINE_RETAINED_NO_CLEAR_EDGE",
                "baseline_kept_reason_code": "NO_CLEAR_EDGE",
                "selected_variant_summary": {
                    "variant_name": "linear",
                    "contract_pass": True,
                    "utility_summary": {
                        "test_ev_net_top5": ev,
                        "test_precision_top5": precision,
                        "test_pr_auc": pr_auc,
                        "test_log_loss": log_loss,
                    },
                    "pnl_summary": {
                        "walk_forward_realized_pnl_quote": pnl,
                    },
                    "sell_side_quality_summary": {
                        "quality_status": "healthy",
                        "selected": {"timeout_exit_share": 0.20, "payoff_ratio": 0.90},
                    },
                    "entry_boundary_summary": {
                        "support_quality_policy": {
                            "support_score_threshold": 1.0,
                            "reduced_context_severe_loss_risk_multiplier": 1.10,
                        }
                    },
                },
                "input_provenance": {
                    "input_variant_name": input_variant_name,
                    "included_experts": included_experts,
                    "excluded_experts": excluded_experts,
                },
            },
        )
        return {
            "trainer": "v5_fusion",
            "run_id": run_id,
            "run_dir": str(run_dir),
            "chosen_variant_name": "linear",
            "variant_report_path": str(run_dir / "fusion_variant_report.json"),
            "evaluated_variant_count": 3,
            "reused": False,
            "source_mode": "fresh_train",
            "chosen_reason_code": "BASELINE_RETAINED_NO_CLEAR_EDGE",
            "baseline_kept_reason_code": "NO_CLEAR_EDGE",
            "default_eligible": True,
            "runtime_viability_pass": True,
            "runtime_deploy_contract_ready": True,
            "input_provenance": {
                "input_variant_name": input_variant_name,
                "included_experts": included_experts,
                "excluded_experts": excluded_experts,
            },
        }

    monkeypatch.setattr("autobot.models.v5_variant_selection._run_v5_fusion_variant_matrix", fake_run)

    payload = run_v5_fusion_input_ablation_matrix(
        TrainV5FusionOptions(
            panel_input_path=tmp_path / "panel.parquet",
            sequence_input_path=tmp_path / "sequence.parquet",
            lob_input_path=tmp_path / "lob.parquet",
            tradability_input_path=tmp_path / "tradability.parquet",
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

    assert payload["chosen_input_variant_name"] == "panel_only"
    assert payload["chosen_variant_name"] == "linear"
    assert payload["offline_winner_input_variant_name"] == "panel_only"
    report = json.loads(Path(payload["input_ablation_report_path"]).read_text(encoding="utf-8"))
    assert report["chosen_input_variant_name"] == "panel_only"
    assert report["selection_evidence"]["clear_edge_assessment"]["reason_code"] == "INPUT_ABLATION_CLEAR_EDGE"
    chosen_runtime = json.loads((registry_root / "train_v5_fusion" / "fusion-panel_only" / "runtime_recommendations.json").read_text(encoding="utf-8"))
    assert chosen_runtime["fusion_input_ablation_selected_variant_name"] == "panel_only"
    assert chosen_runtime["fusion_input_ablation_report_path"].endswith("fusion_input_ablation_report.json")
