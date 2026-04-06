"""Variant matrix runners for v5 performance-deepening automation."""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

import numpy as np

from .registry import load_artifact_status, load_json, update_latest_pointer
from .train_v5_fusion import TrainV5FusionOptions, TrainV5FusionResult, train_and_register_v5_fusion
from .train_v5_lob import TrainV5LobOptions, TrainV5LobResult, train_and_register_v5_lob
from .train_v5_sequence import TrainV5SequenceOptions, TrainV5SequenceResult, train_and_register_v5_sequence


@dataclass(frozen=True)
class VariantRunRecord:
    trainer: str
    model_family: str
    variant_name: str
    baseline: bool
    run_id: str
    run_dir: Path
    reusable: bool
    source_mode: str
    leaderboard_row: dict[str, Any]
    walk_forward_report: dict[str, Any]
    contract_artifacts: dict[str, str]
    contract_pass: bool
    rejection_reasons: tuple[str, ...]
    selection_key: dict[str, float | None]
    runtime_viability_report_path: str
    runtime_viability: dict[str, Any]
    runtime_viability_pass: bool
    runtime_deploy_contract_readiness_path: str
    runtime_deploy_contract_readiness: dict[str, Any]
    runtime_deploy_contract_ready: bool


SEQUENCE_VARIANTS: tuple[dict[str, Any], ...] = (
    {"variant_name": "patchtst_v1__none", "backbone_family": "patchtst_v1", "pretrain_method": "none", "baseline": True},
    {"variant_name": "patchtst_v1__ts2vec_v1", "backbone_family": "patchtst_v1", "pretrain_method": "ts2vec_v1", "baseline": False},
    {"variant_name": "patchtst_v1__timemae_v1", "backbone_family": "patchtst_v1", "pretrain_method": "timemae_v1", "baseline": False},
    {"variant_name": "timemixer_v1__ts2vec_v1", "backbone_family": "timemixer_v1", "pretrain_method": "ts2vec_v1", "baseline": False},
)

LOB_VARIANTS: tuple[dict[str, Any], ...] = (
    {"variant_name": "deeplob_v1", "backbone_family": "deeplob_v1", "baseline": True},
    {"variant_name": "bdlob_v1", "backbone_family": "bdlob_v1", "baseline": False},
    {"variant_name": "hlob_v1", "backbone_family": "hlob_v1", "baseline": False},
)

FUSION_VARIANTS: tuple[dict[str, Any], ...] = (
    {"variant_name": "linear", "stacker_family": "linear", "baseline": True},
    {"variant_name": "monotone_gbdt", "stacker_family": "monotone_gbdt", "baseline": False},
    {"variant_name": "regime_moe", "stacker_family": "regime_moe", "baseline": False},
)


def run_v5_sequence_variant_matrix(options: TrainV5SequenceOptions) -> dict[str, Any]:
    records = [
        _run_or_reuse_sequence_variant(options=replace(options, backbone_family=str(spec["backbone_family"]), pretrain_method=str(spec["pretrain_method"])), spec=spec)
        for spec in SEQUENCE_VARIANTS
    ]
    selected = _select_sequence_winner(records)
    report_path = _write_variant_report(
        run_dir=selected.run_dir,
        filename="sequence_variant_report.json",
        payload=_build_variant_report_payload(
            trainer="v5_sequence",
            model_family=options.model_family,
            records=records,
            selected=selected,
            baseline_variant_name="patchtst_v1__none",
            selection_policy="sequence_variant_selection_v1",
        ),
    )
    _annotate_chosen_run(
        run_dir=selected.run_dir,
        metadata={
            "sequence_variant_name": selected.variant_name,
            "sequence_backbone_name": str(selected.run_dir.joinpath("runtime_recommendations.json") and (load_json(selected.run_dir / "runtime_recommendations.json").get("sequence_backbone_name") or "")).strip() or str(selected.variant_name.split("__")[0]),
            "sequence_variant_report_path": str(report_path),
            "baseline_kept_reason_code": str(load_json(report_path).get("baseline_kept_reason_code") or ""),
            "chosen_reason_code": str(load_json(report_path).get("chosen_reason_code") or ""),
        },
    )
    update_latest_pointer(options.registry_root, options.model_family, selected.run_id)
    return _matrix_result_payload(
        trainer="v5_sequence",
        selected=selected,
        report_path=report_path,
        records=records,
    )


def run_v5_lob_variant_matrix(options: TrainV5LobOptions) -> dict[str, Any]:
    records = [
        _run_or_reuse_lob_variant(options=replace(options, backbone_family=str(spec["backbone_family"])), spec=spec)
        for spec in LOB_VARIANTS
    ]
    selected = _select_lob_winner(records)
    report_path = _write_variant_report(
        run_dir=selected.run_dir,
        filename="lob_variant_report.json",
        payload=_build_variant_report_payload(
            trainer="v5_lob",
            model_family=options.model_family,
            records=records,
            selected=selected,
            baseline_variant_name="deeplob_v1",
            selection_policy="lob_variant_selection_v1",
        ),
    )
    _annotate_chosen_run(
        run_dir=selected.run_dir,
        metadata={
            "lob_variant_name": selected.variant_name,
            "lob_variant_report_path": str(report_path),
            "baseline_kept_reason_code": str(load_json(report_path).get("baseline_kept_reason_code") or ""),
            "chosen_reason_code": str(load_json(report_path).get("chosen_reason_code") or ""),
        },
    )
    update_latest_pointer(options.registry_root, options.model_family, selected.run_id)
    return _matrix_result_payload(
        trainer="v5_lob",
        selected=selected,
        report_path=report_path,
        records=records,
    )


def run_v5_fusion_variant_matrix(options: TrainV5FusionOptions) -> dict[str, Any]:
    records = [
        _run_or_reuse_fusion_variant(options=replace(options, stacker_family=str(spec["stacker_family"])), spec=spec)
        for spec in FUSION_VARIANTS
    ]
    selected = _select_fusion_winner(records)
    input_provenance = _resolve_fusion_input_variant_provenance(options=options)
    report_path = _write_variant_report(
        run_dir=selected.run_dir,
        filename="fusion_variant_report.json",
        payload=(
            _build_variant_report_payload(
                trainer="v5_fusion",
                model_family=options.model_family,
                records=records,
                selected=selected,
                baseline_variant_name="linear",
                selection_policy="fusion_variant_selection_v1",
            )
            | {
                "input_provenance": input_provenance,
                "selected_sequence_variant_name": str(input_provenance.get("sequence_variant_name") or ""),
                "selected_lob_variant_name": str(input_provenance.get("lob_variant_name") or ""),
                "selected_fusion_stacker": selected.variant_name,
            }
        ),
    )
    _annotate_chosen_run(
        run_dir=selected.run_dir,
        metadata={
            "fusion_variant_name": selected.variant_name,
            "fusion_stacker_family": selected.variant_name,
            "fusion_variant_report_path": str(report_path),
            "sequence_variant_name": str(input_provenance.get("sequence_variant_name") or ""),
            "lob_variant_name": str(input_provenance.get("lob_variant_name") or ""),
            "selected_sequence_variant_name": str(input_provenance.get("sequence_variant_name") or ""),
            "selected_lob_variant_name": str(input_provenance.get("lob_variant_name") or ""),
            "selected_fusion_stacker": selected.variant_name,
            "baseline_kept_reason_code": str(load_json(report_path).get("baseline_kept_reason_code") or ""),
            "chosen_reason_code": str(load_json(report_path).get("chosen_reason_code") or ""),
        },
    )
    if selected.contract_pass:
        update_latest_pointer(options.registry_root, options.model_family, selected.run_id)
    return _matrix_result_payload(
        trainer="v5_fusion",
        selected=selected,
        report_path=report_path,
        records=records,
        extra={"input_provenance": input_provenance},
    )


def _run_or_reuse_sequence_variant(*, options: TrainV5SequenceOptions, spec: dict[str, Any]) -> VariantRunRecord:
    required_fields = {
        "trainer": "v5_sequence",
        "model_family": options.model_family,
        "start": options.start,
        "end": options.end,
        "quote": options.quote,
        "top_n": int(options.top_n),
        "run_scope": options.run_scope,
        "backbone_family": options.backbone_family,
        "pretrain_method": options.pretrain_method,
    }
    run_dir = _find_matching_run_dir(
        registry_root=options.registry_root,
        model_family=options.model_family,
        required_fields=required_fields,
        required_artifacts=("sequence_pretrain_contract.json", "sequence_pretrain_report.json", "domain_weighting_report.json", "expert_prediction_table.parquet", "walk_forward_report.json"),
    )
    reusable = run_dir is not None
    if run_dir is None:
        result = train_and_register_v5_sequence(options)
        run_dir = result.run_dir
    return _collect_variant_run_record(
        trainer="v5_sequence",
        model_family=options.model_family,
        variant_name=str(spec["variant_name"]),
        baseline=bool(spec.get("baseline", False)),
        run_dir=run_dir,
        reusable=reusable,
    )


def _run_or_reuse_lob_variant(*, options: TrainV5LobOptions, spec: dict[str, Any]) -> VariantRunRecord:
    required_fields = {
        "trainer": "v5_lob",
        "model_family": options.model_family,
        "start": options.start,
        "end": options.end,
        "quote": options.quote,
        "top_n": int(options.top_n),
        "run_scope": options.run_scope,
        "backbone_family": options.backbone_family,
    }
    run_dir = _find_matching_run_dir(
        registry_root=options.registry_root,
        model_family=options.model_family,
        required_fields=required_fields,
        required_artifacts=("lob_backbone_contract.json", "lob_target_contract.json", "domain_weighting_report.json", "expert_prediction_table.parquet", "walk_forward_report.json"),
    )
    reusable = run_dir is not None
    if run_dir is None:
        result = train_and_register_v5_lob(options)
        run_dir = result.run_dir
    return _collect_variant_run_record(
        trainer="v5_lob",
        model_family=options.model_family,
        variant_name=str(spec["variant_name"]),
        baseline=bool(spec.get("baseline", False)),
        run_dir=run_dir,
        reusable=reusable,
    )


def _run_or_reuse_fusion_variant(*, options: TrainV5FusionOptions, spec: dict[str, Any]) -> VariantRunRecord:
    required_fields = {
        "trainer": "v5_fusion",
        "model_family": options.model_family,
        "start": options.start,
        "end": options.end,
        "quote": options.quote,
        "run_scope": options.run_scope,
        "stacker_family": options.stacker_family,
        "panel_input_path": str(options.panel_input_path),
        "sequence_input_path": str(options.sequence_input_path),
        "lob_input_path": str(options.lob_input_path),
        "tradability_input_path": str(options.tradability_input_path),
        "runtime_start": str(options.runtime_start or options.start),
        "runtime_end": str(options.runtime_end or options.end),
        "panel_runtime_input_path": str(options.panel_runtime_input_path or ""),
        "sequence_runtime_input_path": str(options.sequence_runtime_input_path or ""),
        "lob_runtime_input_path": str(options.lob_runtime_input_path or ""),
        "tradability_runtime_input_path": str(options.tradability_runtime_input_path or ""),
    }
    run_dir = _find_matching_run_dir(
        registry_root=options.registry_root,
        model_family=options.model_family,
        required_fields=required_fields,
        required_artifacts=("fusion_model_contract.json", "fusion_runtime_input_contract.json", "domain_weighting_report.json", "walk_forward_report.json"),
    )
    reusable = run_dir is not None
    if run_dir is None:
        result = train_and_register_v5_fusion(options)
        run_dir = result.run_dir
    return _collect_variant_run_record(
        trainer="v5_fusion",
        model_family=options.model_family,
        variant_name=str(spec["variant_name"]),
        baseline=bool(spec.get("baseline", False)),
        run_dir=run_dir,
        reusable=reusable,
    )


def _find_matching_run_dir(
    *,
    registry_root: Path,
    model_family: str,
    required_fields: dict[str, Any],
    required_artifacts: tuple[str, ...],
) -> Path | None:
    family_root = Path(registry_root) / str(model_family)
    if not family_root.exists():
        return None
    candidate_dirs = sorted((path for path in family_root.iterdir() if path.is_dir()), key=lambda item: item.name, reverse=True)
    for run_dir in candidate_dirs:
        train_config = load_json(run_dir / "train_config.yaml")
        if not train_config:
            continue
        if any(str(train_config.get(key) or "").strip() != str(value).strip() for key, value in required_fields.items()):
            continue
        if any(not (run_dir / artifact_name).exists() for artifact_name in required_artifacts):
            continue
        artifact_status = load_artifact_status(run_dir)
        if not bool(artifact_status.get("expert_prediction_table_complete", False)):
            continue
        return run_dir
    return None


def _collect_variant_run_record(
    *,
    trainer: str,
    model_family: str,
    variant_name: str,
    baseline: bool,
    run_dir: Path,
    reusable: bool,
) -> VariantRunRecord:
    resolved_run_dir = Path(run_dir).resolve()
    walk_forward_report = load_json(resolved_run_dir / "walk_forward_report.json")
    leaderboard_row = load_json(resolved_run_dir / "leaderboard_row.json")
    contract_artifacts = _collect_contract_artifacts(run_dir=resolved_run_dir, trainer=trainer)
    runtime_viability = load_json(resolved_run_dir / "runtime_viability_report.json")
    runtime_deploy_contract_readiness = load_json(resolved_run_dir / "runtime_deploy_contract_readiness.json")
    contract_pass, rejection_reasons = _validate_variant_contracts(
        trainer=trainer,
        run_dir=resolved_run_dir,
        contract_artifacts=contract_artifacts,
    )
    return VariantRunRecord(
        trainer=trainer,
        model_family=model_family,
        variant_name=variant_name,
        baseline=baseline,
        run_id=resolved_run_dir.name,
        run_dir=resolved_run_dir,
        reusable=bool(reusable),
        source_mode="existing_run" if reusable else "fresh_train",
        leaderboard_row=leaderboard_row,
        walk_forward_report=walk_forward_report,
        contract_artifacts=contract_artifacts,
        contract_pass=contract_pass,
        rejection_reasons=tuple(rejection_reasons),
        selection_key=_selection_key_from_leaderboard(leaderboard_row),
        runtime_viability_report_path=str(resolved_run_dir / "runtime_viability_report.json"),
        runtime_viability=(runtime_viability if isinstance(runtime_viability, dict) else {}),
        runtime_viability_pass=bool((runtime_viability or {}).get("pass", False)),
        runtime_deploy_contract_readiness_path=str(resolved_run_dir / "runtime_deploy_contract_readiness.json"),
        runtime_deploy_contract_readiness=(
            runtime_deploy_contract_readiness if isinstance(runtime_deploy_contract_readiness, dict) else {}
        ),
        runtime_deploy_contract_ready=bool((runtime_deploy_contract_readiness or {}).get("pass", False)),
    )


def _collect_contract_artifacts(*, run_dir: Path, trainer: str) -> dict[str, str]:
    artifacts: dict[str, str] = {}
    for artifact_name in {
        "v5_sequence": ("sequence_pretrain_contract.json", "sequence_pretrain_report.json", "domain_weighting_report.json"),
        "v5_lob": ("lob_backbone_contract.json", "lob_target_contract.json", "domain_weighting_report.json"),
        "v5_fusion": (
            "fusion_model_contract.json",
            "runtime_recommendations.json",
            "domain_weighting_report.json",
            "runtime_viability_report.json",
            "runtime_deploy_contract_readiness.json",
        ),
    }.get(trainer, ()):
        artifacts[artifact_name] = str(run_dir / artifact_name)
    return artifacts


def _validate_variant_contracts(*, trainer: str, run_dir: Path, contract_artifacts: dict[str, str]) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    for artifact_path in contract_artifacts.values():
        if not Path(artifact_path).exists():
            reasons.append("ARTIFACT_MISSING")
    if trainer == "v5_sequence":
        contract = load_json(run_dir / "sequence_pretrain_contract.json")
        report = load_json(run_dir / "sequence_pretrain_report.json")
        domain = load_json(run_dir / "domain_weighting_report.json")
        if str(contract.get("policy") or "") != "sequence_pretrain_contract_v1":
            reasons.append("SEQUENCE_PRETRAIN_CONTRACT_INVALID")
        if str(report.get("policy") or "") != "sequence_pretrain_report_v1":
            reasons.append("SEQUENCE_PRETRAIN_REPORT_INVALID")
        if str(domain.get("policy") or "") != "v5_domain_weighting_v1":
            reasons.append("DOMAIN_WEIGHTING_INVALID")
        if not str(contract.get("pretrain_method") or "").strip():
            reasons.append("SEQUENCE_PRETRAIN_METHOD_MISSING")
        if not str(contract.get("pretrain_impl_method") or "").strip():
            reasons.append("SEQUENCE_PRETRAIN_IMPL_METHOD_MISSING")
        pretrain_ready = bool(contract.get("pretrain_ready", False))
        pretrain_method = str(contract.get("pretrain_method") or "").strip().lower()
        encoder_artifact_path = str(contract.get("encoder_artifact_path") or "").strip()
        if pretrain_method == "none":
            if pretrain_ready:
                reasons.append("SEQUENCE_PRETRAIN_READY_INVALID")
        else:
            if not pretrain_ready:
                reasons.append("SEQUENCE_PRETRAIN_NOT_READY")
            if not encoder_artifact_path or not Path(encoder_artifact_path).exists():
                reasons.append("SEQUENCE_PRETRAIN_ENCODER_ARTIFACT_MISSING")
            if str(report.get("status") or "").strip().lower() != "enabled":
                reasons.append("SEQUENCE_PRETRAIN_STATUS_INVALID")
            if _safe_float(report.get("final_loss")) is None:
                reasons.append("SEQUENCE_PRETRAIN_FINAL_LOSS_MISSING")
            if _safe_int(report.get("best_epoch")) <= 0:
                reasons.append("SEQUENCE_PRETRAIN_BEST_EPOCH_INVALID")
            if _safe_int(report.get("encoder_dim")) <= 0:
                reasons.append("SEQUENCE_PRETRAIN_ENCODER_DIM_INVALID")
            if not dict(report.get("final_component_values") or {}):
                reasons.append("SEQUENCE_PRETRAIN_COMPONENT_VALUES_MISSING")
    elif trainer == "v5_lob":
        backbone = load_json(run_dir / "lob_backbone_contract.json")
        target = load_json(run_dir / "lob_target_contract.json")
        domain = load_json(run_dir / "domain_weighting_report.json")
        if str(backbone.get("policy") or "") != "lob_backbone_contract_v1":
            reasons.append("LOB_BACKBONE_CONTRACT_INVALID")
        if str(target.get("policy") or "") != "lob_target_contract_v1":
            reasons.append("LOB_TARGET_CONTRACT_INVALID")
        if str(domain.get("policy") or "") != "v5_domain_weighting_v1":
            reasons.append("DOMAIN_WEIGHTING_INVALID")
    elif trainer == "v5_fusion":
        fusion_contract = load_json(run_dir / "fusion_model_contract.json")
        runtime_doc = load_json(run_dir / "runtime_recommendations.json")
        domain = load_json(run_dir / "domain_weighting_report.json")
        viability = load_json(run_dir / "runtime_viability_report.json")
        if str(fusion_contract.get("policy") or "") != "v5_fusion_v1":
            reasons.append("FUSION_CONTRACT_INVALID")
        if str(runtime_doc.get("source_family") or "") != "train_v5_fusion":
            reasons.append("FUSION_RUNTIME_RECOMMENDATIONS_INVALID")
        if str(domain.get("policy") or "") != "v5_domain_weighting_v1":
            reasons.append("DOMAIN_WEIGHTING_INVALID")
        if str(viability.get("policy") or "") != "v5_runtime_viability_report_v1":
            reasons.append("FUSION_RUNTIME_VIABILITY_REPORT_INVALID")
        readiness = load_json(run_dir / "runtime_deploy_contract_readiness.json")
        if str(readiness.get("policy") or "") != "v5_runtime_deploy_contract_readiness_v1":
            reasons.append("FUSION_RUNTIME_DEPLOY_CONTRACT_READINESS_INVALID")
        if _safe_int(viability.get("rows_above_alpha_floor")) <= 0:
            reasons.append("FUSION_RUNTIME_ALPHA_LCB_ZERO_VIABILITY")
        if _safe_int(viability.get("entry_gate_allowed_count")) <= 0:
            reasons.append("FUSION_RUNTIME_ENTRY_GATE_ZERO_VIABILITY")
        if not bool(readiness.get("pass", False)):
            reasons.append(
                str(readiness.get("primary_reason_code") or "FUSION_RUNTIME_DEPLOY_CONTRACT_NOT_READY")
            )
    return len(reasons) == 0, reasons


def _selection_key_from_leaderboard(payload: dict[str, Any]) -> dict[str, float | None]:
    return {
        "test_ev_net_top5": _safe_float(payload.get("test_ev_net_top5")),
        "test_precision_top5": _safe_float(payload.get("test_precision_top5")),
        "test_pr_auc": _safe_float(payload.get("test_pr_auc")),
        "test_log_loss": _safe_float(payload.get("test_log_loss")),
        "test_brier_score": _safe_float(payload.get("test_brier_score")),
    }


def _select_sequence_winner(records: list[VariantRunRecord]) -> VariantRunRecord:
    valid_records = [item for item in records if item.contract_pass]
    baseline = next((item for item in valid_records if item.variant_name == "patchtst_v1__none"), None)
    if not valid_records:
        baseline_fallback = next((item for item in records if item.variant_name == "patchtst_v1__none"), None)
        if baseline_fallback is None:
            raise ValueError("no valid sequence variants available")
        return baseline_fallback
    ranked = sorted(valid_records, key=_variant_sort_key, reverse=True)
    candidate = ranked[0]
    if baseline is None or candidate.variant_name == baseline.variant_name:
        return candidate if baseline is None else baseline
    candidate_ev = _safe_float(candidate.selection_key.get("test_ev_net_top5")) or float("-inf")
    baseline_ev = _safe_float(baseline.selection_key.get("test_ev_net_top5")) or float("-inf")
    if candidate_ev <= baseline_ev + 1e-6:
        return baseline
    candidate_pr = _safe_float(candidate.selection_key.get("test_pr_auc"))
    baseline_pr = _safe_float(baseline.selection_key.get("test_pr_auc"))
    if candidate_pr is not None and baseline_pr is not None and candidate_pr < (baseline_pr - 0.01):
        return baseline
    candidate_pretrain = _build_sequence_pretrain_summary(candidate)
    if bool(candidate_pretrain.get("pretrain_enabled", False)):
        if not bool(candidate_pretrain.get("pretrain_stability_pass", False)):
            return baseline
        if candidate_ev < (baseline_ev + 0.005):
            return baseline
    return candidate


def _select_lob_winner(records: list[VariantRunRecord]) -> VariantRunRecord:
    baseline = next((item for item in records if item.variant_name == "deeplob_v1"), None)
    chosen = _select_with_baseline(records, baseline_variant_name="deeplob_v1")
    if baseline is not None:
        candidate_ev = _safe_float(chosen.selection_key.get("test_ev_net_top5")) or float("-inf")
        baseline_ev = _safe_float(baseline.selection_key.get("test_ev_net_top5")) or float("-inf")
        if candidate_ev < (baseline_ev + 0.005):
            return baseline
    if baseline is None or chosen.variant_name == "deeplob_v1":
        return chosen
    baseline_summary = _build_variant_summary(record=baseline)
    candidate_summary = _build_variant_summary(record=chosen)
    baseline_brier = _safe_float(((baseline_summary.get("uncertainty_quality_summary") or {}).get("brier_score")))
    candidate_brier = _safe_float(((candidate_summary.get("uncertainty_quality_summary") or {}).get("brier_score")))
    support_not_worse = _lob_support_level_not_worse(
        candidate_summary.get("support_level_coverage_summary"),
        baseline_summary.get("support_level_coverage_summary"),
    )
    target_consistency_pass = bool((candidate_summary.get("primary_vs_aux_target_consistency") or {}).get("pass", False))
    uncertainty_edge_positive = (
        baseline_brier is not None and candidate_brier is not None and candidate_brier < baseline_brier
    )
    if chosen.variant_name == "bdlob_v1":
        if (not uncertainty_edge_positive) or (not target_consistency_pass) or (not support_not_worse):
            return baseline
    if chosen.variant_name == "hlob_v1":
        if (not target_consistency_pass) or (not support_not_worse):
            return baseline
    return chosen


def _select_fusion_winner(records: list[VariantRunRecord]) -> VariantRunRecord:
    baseline = next((item for item in records if item.variant_name == "linear"), None)
    chosen = _select_with_baseline(records, baseline_variant_name="linear")
    valid_baseline = baseline if (baseline is not None and baseline.contract_pass) else None
    if chosen.variant_name == "regime_moe" and valid_baseline is not None:
        if not _has_clear_fusion_edge(candidate=chosen, baseline=valid_baseline):
            return baseline
    return chosen


def _select_with_baseline(records: list[VariantRunRecord], *, baseline_variant_name: str) -> VariantRunRecord:
    valid_records = [item for item in records if item.contract_pass]
    baseline = next((item for item in valid_records if item.variant_name == baseline_variant_name), None)
    if not valid_records:
        baseline_fallback = next((item for item in records if item.variant_name == baseline_variant_name), None)
        if baseline_fallback is None:
            raise ValueError("no valid variants available")
        return baseline_fallback
    ranked = sorted(valid_records, key=_variant_sort_key, reverse=True)
    candidate = ranked[0]
    if baseline is None:
        return candidate
    if candidate.variant_name == baseline.variant_name:
        return baseline
    candidate_ev = _safe_float(candidate.selection_key.get("test_ev_net_top5")) or float("-inf")
    baseline_ev = _safe_float(baseline.selection_key.get("test_ev_net_top5")) or float("-inf")
    if candidate_ev <= baseline_ev + 1e-6:
        return baseline
    candidate_pr = _safe_float(candidate.selection_key.get("test_pr_auc"))
    baseline_pr = _safe_float(baseline.selection_key.get("test_pr_auc"))
    if candidate_pr is not None and baseline_pr is not None and candidate_pr < (baseline_pr - 0.01):
        return baseline
    return candidate


def _variant_sort_key(record: VariantRunRecord) -> tuple[float, float, float, float]:
    key = record.selection_key
    return (
        _safe_float(key.get("test_ev_net_top5")) or float("-inf"),
        _safe_float(key.get("test_precision_top5")) or float("-inf"),
        _safe_float(key.get("test_pr_auc")) or float("-inf"),
        -(_safe_float(key.get("test_log_loss")) or float("inf")),
    )


def _has_clear_fusion_edge(*, candidate: VariantRunRecord, baseline: VariantRunRecord) -> bool:
    candidate_ev = _safe_float(candidate.selection_key.get("test_ev_net_top5")) or float("-inf")
    baseline_ev = _safe_float(baseline.selection_key.get("test_ev_net_top5")) or float("-inf")
    candidate_precision = _safe_float(candidate.selection_key.get("test_precision_top5"))
    baseline_precision = _safe_float(baseline.selection_key.get("test_precision_top5"))
    candidate_log_loss = _safe_float(candidate.selection_key.get("test_log_loss"))
    baseline_log_loss = _safe_float(baseline.selection_key.get("test_log_loss"))
    if candidate_ev <= baseline_ev + 1e-6:
        return False
    if candidate_precision is not None and baseline_precision is not None and candidate_precision < (baseline_precision - 0.01):
        return False
    if candidate_log_loss is not None and baseline_log_loss is not None and candidate_log_loss > (baseline_log_loss + 0.02):
        return False
    return True


def _build_variant_report_payload(
    *,
    trainer: str,
    model_family: str,
    records: list[VariantRunRecord],
    selected: VariantRunRecord,
    baseline_variant_name: str,
    selection_policy: str,
) -> dict[str, Any]:
    baseline = next((item for item in records if item.variant_name == baseline_variant_name), None)
    offline_winner = sorted(records, key=_variant_sort_key, reverse=True)[0] if records else selected
    kept_baseline = bool(baseline and baseline.run_id == selected.run_id)
    if kept_baseline:
        chosen_reason_code = "BASELINE_RETAINED_NO_CLEAR_EDGE"
        baseline_kept_reason_code = "NO_CLEAR_EDGE"
    else:
        chosen_reason_code = "NONBASELINE_CLEAR_EDGE"
        baseline_kept_reason_code = ""
    selected_summary = _build_variant_summary(record=selected)
    payload = {
        "policy": f"{trainer}_variant_report_v1",
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "trainer": trainer,
        "model_family": model_family,
        "selection_policy": selection_policy,
        "baseline_variant_name": baseline_variant_name,
        "chosen_variant_name": selected.variant_name,
        "chosen_run_id": selected.run_id,
        "chosen_run_dir": str(selected.run_dir),
        "evaluated_variant_count": len(records),
        "kept_baseline": kept_baseline,
        "chosen_reason_code": chosen_reason_code,
        "baseline_kept_reason_code": baseline_kept_reason_code,
        "selected_sequence_variant_name": selected.variant_name if trainer == "v5_sequence" else None,
        "selected_lob_variant_name": selected.variant_name if trainer == "v5_lob" else None,
        "selected_fusion_stacker": selected.variant_name if trainer == "v5_fusion" else None,
        "rejection_reasons": {
            item.variant_name: list(item.rejection_reasons)
            for item in records
            if item.rejection_reasons
        },
        "evaluated_variants": [_build_variant_summary(record=item) for item in records],
        "variants": [_build_variant_summary(record=item) for item in records],
        "selected_variant_summary": selected_summary,
    }
    if trainer == "v5_sequence":
        baseline = next((item for item in records if item.variant_name == baseline_variant_name), None)
        selected_pretrain = _build_sequence_pretrain_summary(selected)
        baseline_ev = _safe_float((baseline.selection_key if baseline is not None else {}).get("test_ev_net_top5")) or float("-inf")
        selected_ev = _safe_float(selected.selection_key.get("test_ev_net_top5")) or float("-inf")
        selected_ood = _build_ood_summary(selected)
        payload["pretrain_summary_by_variant"] = {
            item.variant_name: _build_sequence_pretrain_summary(item)
            for item in records
        }
        payload["candidate_pretrain_ready"] = bool(selected_pretrain.get("pretrain_ready", False))
        payload["selection_evidence"] = {
            "pretrain_stability": bool(selected_pretrain.get("pretrain_stability_pass", False)),
            "utility_edge_vs_baseline": float(selected_ev - baseline_ev) if np.isfinite(selected_ev - baseline_ev) else None,
            "ood_status": selected_ood.get("status"),
            "ood_source_kind": selected_ood.get("source_kind"),
            "ood_penalty_enabled": selected_ood.get("ood_penalty_enabled"),
            "future_to_train_ratio": selected_ood.get("future_to_train_ratio"),
        }
    if trainer == "v5_lob":
        baseline = next((item for item in records if item.variant_name == baseline_variant_name), None)
        baseline_summary = _build_variant_summary(record=baseline) if baseline is not None else {}
        baseline_uncertainty = _safe_float(((baseline_summary.get("uncertainty_quality_summary") or {}).get("brier_score"))) if baseline is not None else None
        selected_uncertainty = _safe_float(((selected_summary.get("uncertainty_quality_summary") or {}).get("brier_score")))
        baseline_ev = _safe_float((baseline.selection_key if baseline is not None else {}).get("test_ev_net_top5")) or float("-inf")
        selected_ev = _safe_float(selected.selection_key.get("test_ev_net_top5")) or float("-inf")
        payload["selection_evidence"] = {
            "utility_edge_vs_baseline": float(selected_ev - baseline_ev) if np.isfinite(selected_ev - baseline_ev) else None,
            "uncertainty_quality_edge_vs_baseline": (
                float((baseline_uncertainty or 0.0) - (selected_uncertainty or 0.0))
                if (baseline_uncertainty is not None and selected_uncertainty is not None)
                else None
            ),
            "support_level_coverage_summary": _build_lob_support_level_summary(selected),
            "primary_vs_aux_target_consistency": _build_lob_target_consistency(selected),
        }
    if trainer == "v5_fusion":
        payload["offline_winner_variant_name"] = offline_winner.variant_name
        payload["default_eligible_variant_name"] = selected.variant_name
        payload["default_eligible"] = bool(selected.runtime_viability_pass and selected.runtime_deploy_contract_ready)
        payload["runtime_viability_pass"] = bool(selected.runtime_viability_pass)
        payload["runtime_viability_report_path"] = selected.runtime_viability_report_path
        payload["runtime_viability_summary"] = dict(selected.runtime_viability or {})
        payload["runtime_deploy_contract_ready"] = bool(selected.runtime_deploy_contract_ready)
        payload["runtime_deploy_contract_readiness_path"] = selected.runtime_deploy_contract_readiness_path
        payload["runtime_deploy_contract_summary"] = dict(selected.runtime_deploy_contract_readiness or {})
        payload["selection_evidence"] = {
            "utility_edge_vs_linear": 0.0,
            "execution_structure_non_regression": None,
            "paper_non_regression": None,
            "paired_non_regression": None,
            "canary_non_regression": None,
            "promotion_safe": bool(selected.runtime_viability_pass and selected.runtime_deploy_contract_ready),
            "rows_above_alpha_floor": _safe_int((selected.runtime_viability or {}).get("rows_above_alpha_floor")),
            "entry_gate_allowed_count": _safe_int((selected.runtime_viability or {}).get("entry_gate_allowed_count")),
            "runtime_viability_pass": bool(selected.runtime_viability_pass),
            "runtime_deploy_contract_ready": bool(selected.runtime_deploy_contract_ready),
        }
    return payload


def _write_variant_report(*, run_dir: Path, filename: str, payload: dict[str, Any]) -> Path:
    path = Path(run_dir) / str(filename)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _annotate_chosen_run(*, run_dir: Path, metadata: dict[str, Any]) -> None:
    run_path = Path(run_dir)
    for filename in ("train_config.yaml", "runtime_recommendations.json", "promotion_decision.json"):
        path = run_path / filename
        payload = load_json(path)
        if not payload:
            continue
        payload.update(dict(metadata))
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _matrix_result_payload(
    *,
    trainer: str,
    selected: VariantRunRecord,
    report_path: Path,
    records: list[VariantRunRecord],
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "trainer": trainer,
        "run_id": selected.run_id,
        "run_dir": str(selected.run_dir),
        "chosen_variant_name": selected.variant_name,
        "variant_report_path": str(report_path),
        "evaluated_variant_count": len(records),
        "reused": bool(selected.reusable),
        "source_mode": str(selected.source_mode),
        "chosen_reason_code": str(load_json(report_path).get("chosen_reason_code") or ""),
        "baseline_kept_reason_code": str(load_json(report_path).get("baseline_kept_reason_code") or ""),
    }
    if trainer == "v5_fusion":
        report_payload = load_json(report_path)
        payload["offline_winner_variant_name"] = str(report_payload.get("offline_winner_variant_name") or selected.variant_name)
        payload["default_eligible_variant_name"] = str(report_payload.get("default_eligible_variant_name") or selected.variant_name)
        payload["default_eligible"] = bool(report_payload.get("default_eligible", True))
        payload["runtime_viability_pass"] = bool(report_payload.get("runtime_viability_pass", False))
        payload["runtime_viability_report_path"] = str(report_payload.get("runtime_viability_report_path") or selected.runtime_viability_report_path)
        payload["runtime_deploy_contract_ready"] = bool(report_payload.get("runtime_deploy_contract_ready", False))
        payload["runtime_deploy_contract_readiness_path"] = str(
            report_payload.get("runtime_deploy_contract_readiness_path") or selected.runtime_deploy_contract_readiness_path
        )
    if extra:
        payload.update(dict(extra))
    return payload


def _resolve_fusion_input_variant_provenance(*, options: TrainV5FusionOptions) -> dict[str, Any]:
    def _load(path: Path | None) -> dict[str, Any]:
        if path is None:
            return {}
        run_dir = Path(path).resolve().parent
        train_config = load_json(run_dir / "train_config.yaml")
        runtime_recommendations = load_json(run_dir / "runtime_recommendations.json")
        return {
            "run_id": run_dir.name,
            "run_dir": str(run_dir),
            "variant_name": str(train_config.get("sequence_variant_name") or train_config.get("lob_variant_name") or runtime_recommendations.get("sequence_variant_name") or runtime_recommendations.get("lob_variant_name") or "").strip() or None,
            "backbone_name": str(runtime_recommendations.get("sequence_backbone_name") or runtime_recommendations.get("lob_backbone_name") or "").strip() or None,
            "sequence_pretrain_method": str(runtime_recommendations.get("sequence_pretrain_method") or train_config.get("pretrain_method") or "").strip() or None,
            "sequence_pretrain_ready": bool(runtime_recommendations.get("sequence_pretrain_ready", False)),
            "sequence_pretrain_status": str(runtime_recommendations.get("sequence_pretrain_status") or "").strip() or None,
            "sequence_pretrain_objective": str(runtime_recommendations.get("sequence_pretrain_objective") or "").strip() or None,
            "sequence_pretrain_best_epoch": _safe_int(runtime_recommendations.get("sequence_pretrain_best_epoch")),
            "sequence_pretrain_encoder_present": bool(runtime_recommendations.get("sequence_pretrain_encoder_present", False)),
        }

    sequence_info = _load(options.sequence_input_path)
    lob_info = _load(options.lob_input_path)
    return {
        "sequence_run_id": sequence_info.get("run_id"),
        "sequence_variant_name": sequence_info.get("variant_name"),
        "sequence_pretrain_method": sequence_info.get("sequence_pretrain_method"),
        "sequence_pretrain_ready": sequence_info.get("sequence_pretrain_ready"),
        "sequence_pretrain_status": sequence_info.get("sequence_pretrain_status"),
        "sequence_pretrain_objective": sequence_info.get("sequence_pretrain_objective"),
        "sequence_pretrain_best_epoch": sequence_info.get("sequence_pretrain_best_epoch"),
        "sequence_pretrain_encoder_present": sequence_info.get("sequence_pretrain_encoder_present"),
        "lob_run_id": lob_info.get("run_id"),
        "lob_variant_name": lob_info.get("variant_name"),
    }


def _build_variant_summary(*, record: VariantRunRecord) -> dict[str, Any]:
    leaderboard_row = dict(record.leaderboard_row or {})
    walk_forward_report = dict(record.walk_forward_report or {})
    uncertainty_quality = (
        walk_forward_report.get("uncertainty_quality")
        or leaderboard_row.get("uncertainty_quality")
        or leaderboard_row.get("test_brier_score")
    )
    payload = {
        "variant_name": record.variant_name,
        "baseline": record.baseline,
        "run_id": record.run_id,
        "run_dir": str(record.run_dir),
        "reusable": record.reusable,
        "source_mode": record.source_mode,
        "contract_pass": record.contract_pass,
        "rejection_reasons": list(record.rejection_reasons),
        "selection_key": dict(record.selection_key),
        "runtime_viability_report_path": record.runtime_viability_report_path,
        "runtime_viability_pass": record.runtime_viability_pass,
        "runtime_viability_summary": dict(record.runtime_viability or {}),
        "runtime_deploy_contract_readiness_path": record.runtime_deploy_contract_readiness_path,
        "runtime_deploy_contract_ready": record.runtime_deploy_contract_ready,
        "runtime_deploy_contract_summary": dict(record.runtime_deploy_contract_readiness or {}),
        "utility_summary": {
            "test_ev_net_top5": _safe_float(leaderboard_row.get("test_ev_net_top5")),
            "test_precision_top5": _safe_float(leaderboard_row.get("test_precision_top5")),
            "test_pr_auc": _safe_float(leaderboard_row.get("test_pr_auc")),
            "test_log_loss": _safe_float(leaderboard_row.get("test_log_loss")),
        },
        "pnl_summary": {
            "walk_forward_realized_pnl_quote": _safe_float(
                walk_forward_report.get("realized_pnl_quote")
                or walk_forward_report.get("walk_forward_realized_pnl_quote")
                or leaderboard_row.get("walk_forward_realized_pnl_quote")
            ),
            "test_realized_pnl_quote": _safe_float(
                leaderboard_row.get("test_realized_pnl_quote")
                or walk_forward_report.get("test_realized_pnl_quote")
            ),
        },
        "drawdown_summary": {
            "max_drawdown_quote": _safe_float(
                walk_forward_report.get("max_drawdown_quote")
                or leaderboard_row.get("max_drawdown_quote")
                or walk_forward_report.get("max_drawdown")
            ),
            "max_drawdown_pct": _safe_float(
                walk_forward_report.get("max_drawdown_pct")
                or leaderboard_row.get("max_drawdown_pct")
            ),
        },
        "uncertainty_quality_summary": {
            "brier_score": _safe_float(leaderboard_row.get("test_brier_score")),
            "uncertainty_quality": _safe_float(uncertainty_quality),
        },
    }
    if record.trainer == "v5_sequence":
        payload["pretrain_summary"] = _build_sequence_pretrain_summary(record)
        payload["ood_summary"] = _build_ood_summary(record)
    if record.trainer == "v5_lob":
        payload["support_level_coverage_summary"] = _build_lob_support_level_summary(record)
        payload["primary_vs_aux_target_consistency"] = _build_lob_target_consistency(record)
    return payload


def _build_sequence_pretrain_summary(record: VariantRunRecord) -> dict[str, Any]:
    contract = load_json(record.run_dir / "sequence_pretrain_contract.json")
    report = load_json(record.run_dir / "sequence_pretrain_report.json")
    pretrain_method = str(contract.get("pretrain_method") or "").strip()
    pretrain_ready = bool(contract.get("pretrain_ready", False))
    encoder_artifact_path = str(contract.get("encoder_artifact_path") or "").strip()
    encoder_present = bool(encoder_artifact_path) and Path(encoder_artifact_path).exists()
    final_loss = _safe_float(report.get("final_loss"))
    best_epoch = _safe_int(report.get("best_epoch"))
    encoder_dim = _safe_int(report.get("encoder_dim"))
    final_component_values = dict(report.get("final_component_values") or {})
    finite_component_values = all(_safe_float(value) is not None for value in final_component_values.values()) if final_component_values else False
    pretrain_enabled = pretrain_method != "none"
    stability_pass = (
        (not pretrain_enabled)
        or (
            pretrain_ready
            and encoder_present
            and str(report.get("status") or "").strip().lower() == "enabled"
            and final_loss is not None
            and best_epoch > 0
            and encoder_dim > 0
            and finite_component_values
        )
    )
    return {
        "pretrain_method": pretrain_method,
        "pretrain_ready": pretrain_ready,
        "pretrain_enabled": pretrain_enabled,
        "status": str(report.get("status") or contract.get("status") or "").strip(),
        "objective_name": str(report.get("objective_name") or contract.get("objective_name") or "").strip(),
        "final_loss": final_loss,
        "best_epoch": best_epoch,
        "encoder_present": encoder_present,
        "encoder_dim": encoder_dim,
        "pretrain_stability_pass": bool(stability_pass),
        "mask_ratio_schedule": list(report.get("mask_ratio_schedule") or []),
        "augmentation_policy": list(report.get("augmentation_policy") or []),
        "final_component_values": final_component_values,
    }


def _build_lob_support_level_summary(record: VariantRunRecord) -> dict[str, Any]:
    contract = load_json(record.run_dir / "lob_target_contract.json")
    return dict(contract.get("support_level_quality_summary") or {})


def _build_lob_target_consistency(record: VariantRunRecord) -> dict[str, Any]:
    contract = load_json(record.run_dir / "lob_target_contract.json")
    primary_horizon = _safe_int(contract.get("primary_horizon_seconds"))
    auxiliary_targets = list(contract.get("auxiliary_targets") or [])
    uncertainty_target = str(contract.get("uncertainty_target") or "").strip()
    uncertainty_quality_target = str(contract.get("uncertainty_quality_target") or "").strip()
    return {
        "primary_horizon_seconds": primary_horizon,
        "auxiliary_target_count": len(auxiliary_targets),
        "uncertainty_target": uncertainty_target or None,
        "uncertainty_quality_target": uncertainty_quality_target or None,
        "pass": (primary_horizon == 30) and ("five_min_alpha" in auxiliary_targets) and bool(uncertainty_target),
    }


def _lob_support_level_not_worse(candidate: Any, baseline: Any) -> bool:
    candidate_summary = dict(candidate or {})
    baseline_summary = dict(baseline or {})
    candidate_strict = _safe_float(candidate_summary.get("strict_full_ratio"))
    baseline_strict = _safe_float(baseline_summary.get("strict_full_ratio"))
    candidate_structural = _safe_float(candidate_summary.get("structural_invalid_ratio"))
    baseline_structural = _safe_float(baseline_summary.get("structural_invalid_ratio"))
    strict_ok = (
        candidate_strict is None
        or baseline_strict is None
        or candidate_strict >= baseline_strict
    )
    structural_ok = (
        candidate_structural is None
        or baseline_structural is None
        or candidate_structural <= baseline_structural
    )
    return bool(strict_ok and structural_ok)


def _build_ood_summary(record: VariantRunRecord) -> dict[str, Any]:
    payload = load_json(record.run_dir / "ood_generalization_report.json")
    train_future = dict(payload.get("train_vs_future_domain_gap_summary") or {})
    return {
        "status": str(payload.get("status") or "").strip() or None,
        "source_kind": str(payload.get("source_kind") or "").strip() or None,
        "ood_penalty_enabled": bool(payload.get("invariant_penalty_enabled", False)),
        "future_to_train_ratio": _safe_float(train_future.get("future_to_train_ratio")),
    }


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int:
    try:
        if value in (None, ""):
            return 0
        return int(value)
    except (TypeError, ValueError):
        return 0
