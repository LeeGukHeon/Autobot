"""Variant matrix runners for v5 performance-deepening automation."""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

import numpy as np

from .registry import load_artifact_status, load_json, update_latest_pointer
from .train_v5_fusion import (
    TrainV5FusionOptions,
    TrainV5FusionResult,
    _resolve_fusion_input_variant,
    train_and_register_v5_fusion,
)
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

FUSION_INPUT_ABLATION_VARIANTS: tuple[dict[str, Any], ...] = (
    {"input_variant_name": "full_fusion", "baseline": True},
    {"input_variant_name": "full_without_tradability", "baseline": False},
    {"input_variant_name": "panel_plus_sequence", "baseline": False},
    {"input_variant_name": "panel_plus_lob", "baseline": False},
    {"input_variant_name": "panel_only", "baseline": False},
)


@dataclass(frozen=True)
class FusionInputAblationRecord:
    input_variant_name: str
    baseline: bool
    run_id: str
    run_dir: Path
    chosen_variant_name: str
    variant_report_path: str
    reusable: bool
    source_mode: str
    chosen_reason_code: str
    baseline_kept_reason_code: str
    default_eligible: bool
    runtime_viability_pass: bool
    runtime_deploy_contract_ready: bool
    selected_variant_summary: dict[str, Any]
    selection_key: dict[str, float | None]
    input_provenance: dict[str, Any]
    fusion_variant_report: dict[str, Any]
    matrix_payload: dict[str, Any]


def _fusion_variant_specs_for_input_variant(*, options: TrainV5FusionOptions) -> tuple[dict[str, Any], ...]:
    input_variant = _resolve_fusion_input_variant(options)
    include_sequence = bool(input_variant.get("include_sequence", True))
    if include_sequence:
        return FUSION_VARIANTS
    return tuple(
        spec
        for spec in FUSION_VARIANTS
        if str(spec.get("stacker_family") or "").strip().lower() != "regime_moe"
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


def _run_v5_fusion_variant_matrix(options: TrainV5FusionOptions, *, publish_latest: bool = True) -> dict[str, Any]:
    variant_specs = _fusion_variant_specs_for_input_variant(options=options)
    records = [
        _run_or_reuse_fusion_variant(options=replace(options, stacker_family=str(spec["stacker_family"])), spec=spec)
        for spec in variant_specs
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
    report_payload = load_json(report_path)
    selected_evidence = dict(report_payload.get("selection_evidence") or {})
    offline_winner_assessment = dict(selected_evidence.get("offline_winner_clear_edge_assessment") or {})
    effective_paper_non_regression = (
        offline_winner_assessment.get("paper_evidence", {}).get("non_regression")
        if offline_winner_assessment
        else selected_evidence.get("paper_non_regression")
    )
    effective_paired_non_regression = (
        offline_winner_assessment.get("paired_evidence", {}).get("non_regression")
        if offline_winner_assessment
        else selected_evidence.get("paired_non_regression")
    )
    effective_canary_non_regression = (
        offline_winner_assessment.get("canary_evidence", {}).get("non_regression")
        if offline_winner_assessment
        else selected_evidence.get("canary_non_regression")
    )
    effective_paper_evidence = (
        dict(offline_winner_assessment.get("paper_evidence") or {})
        if offline_winner_assessment
        else dict(selected_evidence.get("paper_evidence_summary") or {})
    )
    effective_paired_evidence = (
        dict(offline_winner_assessment.get("paired_evidence") or {})
        if offline_winner_assessment
        else dict(selected_evidence.get("paired_evidence_summary") or {})
    )
    effective_canary_evidence = (
        dict(offline_winner_assessment.get("canary_evidence") or {})
        if offline_winner_assessment
        else dict(selected_evidence.get("canary_evidence_summary") or {})
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
            "baseline_kept_reason_code": str(report_payload.get("baseline_kept_reason_code") or ""),
            "chosen_reason_code": str(report_payload.get("chosen_reason_code") or ""),
            "fusion_evidence_reason_code": str(report_payload.get("chosen_reason_code") or ""),
            "fusion_candidate_default_eligible": bool(report_payload.get("default_eligible", False)),
            "fusion_evidence_winner": str(report_payload.get("chosen_variant_name") or selected.variant_name),
            "fusion_offline_winner": str(report_payload.get("offline_winner_variant_name") or selected.variant_name),
            "fusion_default_eligible_winner": str(report_payload.get("default_eligible_variant_name") or selected.variant_name),
            "fusion_non_regression_summary": {
                "paper_non_regression": effective_paper_non_regression,
                "paired_non_regression": effective_paired_non_regression,
                "canary_non_regression": effective_canary_non_regression,
                "paper_evidence_summary": effective_paper_evidence,
                "paired_evidence_summary": effective_paired_evidence,
                "canary_evidence_summary": effective_canary_evidence,
            },
        },
    )
    if publish_latest and selected.contract_pass:
        update_latest_pointer(options.registry_root, options.model_family, selected.run_id)
    return _matrix_result_payload(
        trainer="v5_fusion",
        selected=selected,
        report_path=report_path,
        records=records,
        extra={"input_provenance": input_provenance},
    )


def run_v5_fusion_variant_matrix(options: TrainV5FusionOptions) -> dict[str, Any]:
    return _run_v5_fusion_variant_matrix(options, publish_latest=True)


def run_v5_fusion_input_ablation_matrix(options: TrainV5FusionOptions) -> dict[str, Any]:
    records = [
        _collect_fusion_input_ablation_record(
            payload=_run_v5_fusion_variant_matrix(
                replace(options, input_variant_name=str(spec["input_variant_name"])),
                publish_latest=False,
            ),
            baseline=bool(spec.get("baseline", False)),
        )
        for spec in FUSION_INPUT_ABLATION_VARIANTS
    ]
    selected = _select_fusion_input_ablation_winner(records)
    baseline = next((item for item in records if item.baseline), None)
    offline_winner = sorted(records, key=_fusion_input_ablation_sort_key, reverse=True)[0] if records else selected
    clear_edge_assessment = (
        _build_fusion_input_ablation_clear_edge_assessment(candidate=offline_winner, baseline=baseline)
        if baseline is not None and offline_winner.input_variant_name != baseline.input_variant_name
        else {}
    )
    kept_baseline = bool(baseline and baseline.run_id == selected.run_id)
    chosen_reason_code = "BASELINE_RETAINED_NO_CLEAR_EDGE" if kept_baseline else "NONBASELINE_CLEAR_EDGE"
    baseline_kept_reason_code = "NO_CLEAR_EDGE" if kept_baseline else ""
    if kept_baseline and clear_edge_assessment:
        chosen_reason_code = str(clear_edge_assessment.get("reason_code") or chosen_reason_code)
        baseline_kept_reason_code = str(clear_edge_assessment.get("reason_code") or baseline_kept_reason_code)
    report_payload = {
        "policy": "v5_fusion_input_ablation_report_v1",
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "trainer": "v5_fusion",
        "model_family": options.model_family,
        "selection_policy": "fusion_input_ablation_selection_v1",
        "baseline_input_variant_name": "full_fusion",
        "chosen_input_variant_name": selected.input_variant_name,
        "chosen_run_id": selected.run_id,
        "chosen_run_dir": str(selected.run_dir),
        "chosen_fusion_variant_name": selected.chosen_variant_name,
        "evaluated_input_variant_count": len(records),
        "kept_baseline": kept_baseline,
        "chosen_reason_code": chosen_reason_code,
        "baseline_kept_reason_code": baseline_kept_reason_code,
        "offline_winner_input_variant_name": offline_winner.input_variant_name,
        "selected_input_variant_summary": _build_fusion_input_ablation_summary(record=selected),
        "evaluated_input_variants": [_build_fusion_input_ablation_summary(record=item) for item in records],
        "selection_evidence": {
            "walk_forward_pnl_edge_vs_full_fusion": _fusion_input_ablation_metric_delta(
                candidate=selected,
                baseline=baseline,
                key="walk_forward_realized_pnl_quote",
            ),
            "utility_edge_vs_full_fusion": _fusion_input_ablation_metric_delta(
                candidate=selected,
                baseline=baseline,
                key="test_ev_net_top5",
            ),
            "sell_side_quality_non_regression": (
                _fusion_input_ablation_sell_side_not_worse(candidate=selected, baseline=baseline)
                if baseline is not None
                else None
            ),
            "boundary_quality_non_regression": (
                _fusion_input_ablation_boundary_not_worse(candidate=selected, baseline=baseline)
                if baseline is not None
                else None
            ),
            "clear_edge_assessment": clear_edge_assessment,
        },
    }
    input_ablation_report_path = _write_variant_report(
        run_dir=selected.run_dir,
        filename="fusion_input_ablation_report.json",
        payload=report_payload,
    )
    _annotate_chosen_run(
        run_dir=selected.run_dir,
        metadata={
            "fusion_input_variant_name": selected.input_variant_name,
            "fusion_input_ablation_report_path": str(input_ablation_report_path),
            "fusion_input_ablation_selected_variant_name": selected.input_variant_name,
            "fusion_input_ablation_chosen_reason_code": chosen_reason_code,
        },
    )
    if selected.default_eligible:
        update_latest_pointer(options.registry_root, options.model_family, selected.run_id)
    payload = dict(selected.matrix_payload if isinstance(selected.matrix_payload, dict) else {})
    payload["chosen_input_variant_name"] = selected.input_variant_name
    payload["input_ablation_report_path"] = str(input_ablation_report_path)
    payload["input_ablation_evaluated_variant_count"] = len(records)
    payload["input_ablation_baseline_kept_reason_code"] = baseline_kept_reason_code
    payload["input_ablation_chosen_reason_code"] = chosen_reason_code
    payload["offline_winner_input_variant_name"] = offline_winner.input_variant_name
    payload["input_provenance"] = dict(selected.input_provenance or {})
    return payload


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
    input_variant = _resolve_fusion_input_variant(options)
    required_fields = {
        "trainer": "v5_fusion",
        "model_family": options.model_family,
        "start": options.start,
        "end": options.end,
        "quote": options.quote,
        "run_scope": options.run_scope,
        "stacker_family": options.stacker_family,
        "input_variant_name": str(input_variant.get("input_variant_name") or "full_fusion"),
        "include_sequence": str(bool(input_variant.get("include_sequence", True))),
        "include_lob": str(bool(input_variant.get("include_lob", True))),
        "include_tradability": str(bool(input_variant.get("include_tradability", True))),
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
        if any(
            (
                ("" if (train_config.get(key) if key in train_config else "") is None else str(train_config.get(key) if key in train_config else "").strip())
                != ("" if value is None else str(value).strip())
            )
            for key, value in required_fields.items()
        ):
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
    if chosen.variant_name != "linear" and valid_baseline is not None:
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
    return bool(_build_fusion_clear_edge_assessment(candidate=candidate, baseline=baseline).get("pass", False))


def _build_fusion_clear_edge_assessment(*, candidate: VariantRunRecord, baseline: VariantRunRecord) -> dict[str, Any]:
    candidate_ev = _safe_float(candidate.selection_key.get("test_ev_net_top5")) or float("-inf")
    baseline_ev = _safe_float(baseline.selection_key.get("test_ev_net_top5")) or float("-inf")
    candidate_precision = _safe_float(candidate.selection_key.get("test_precision_top5"))
    baseline_precision = _safe_float(baseline.selection_key.get("test_precision_top5"))
    candidate_log_loss = _safe_float(candidate.selection_key.get("test_log_loss"))
    baseline_log_loss = _safe_float(baseline.selection_key.get("test_log_loss"))
    input_quality_brake = _build_fusion_input_quality_brake(candidate)
    minimum_edge = max(float(input_quality_brake.get("minimum_utility_edge_vs_linear") or 0.0), 1e-6)
    if candidate_ev <= baseline_ev + minimum_edge:
        return {
            "pass": False,
            "reason_code": "NONBASELINE_EDGE_BELOW_REQUIRED_MARGIN",
            "paper_evidence": _build_fusion_paper_non_regression(candidate),
            "paired_evidence": _build_fusion_paired_non_regression(candidate),
            "canary_evidence": _build_fusion_canary_non_regression(candidate),
            "minimum_utility_edge_vs_linear": float(minimum_edge),
        }
    if candidate_precision is not None and baseline_precision is not None and candidate_precision < (baseline_precision - 0.01):
        return {
            "pass": False,
            "reason_code": "NONBASELINE_PRECISION_REGRESSION",
            "paper_evidence": _build_fusion_paper_non_regression(candidate),
            "paired_evidence": _build_fusion_paired_non_regression(candidate),
            "canary_evidence": _build_fusion_canary_non_regression(candidate),
            "minimum_utility_edge_vs_linear": float(minimum_edge),
        }
    if candidate_log_loss is not None and baseline_log_loss is not None and candidate_log_loss > (baseline_log_loss + 0.02):
        return {
            "pass": False,
            "reason_code": "NONBASELINE_LOGLOSS_REGRESSION",
            "paper_evidence": _build_fusion_paper_non_regression(candidate),
            "paired_evidence": _build_fusion_paired_non_regression(candidate),
            "canary_evidence": _build_fusion_canary_non_regression(candidate),
            "minimum_utility_edge_vs_linear": float(minimum_edge),
        }
    paper_evidence = _build_fusion_paper_non_regression(candidate)
    if paper_evidence.get("available") and not paper_evidence.get("non_regression", True):
        return {
            "pass": False,
            "reason_code": "PAPER_NON_REGRESSION_FAILED",
            "paper_evidence": paper_evidence,
            "paired_evidence": _build_fusion_paired_non_regression(candidate),
            "canary_evidence": _build_fusion_canary_non_regression(candidate),
            "minimum_utility_edge_vs_linear": float(minimum_edge),
        }
    paired_evidence = _build_fusion_paired_non_regression(candidate)
    if paired_evidence.get("available") and not paired_evidence.get("non_regression", True):
        return {
            "pass": False,
            "reason_code": "PAIRED_NON_REGRESSION_FAILED",
            "paper_evidence": paper_evidence,
            "paired_evidence": paired_evidence,
            "canary_evidence": _build_fusion_canary_non_regression(candidate),
            "minimum_utility_edge_vs_linear": float(minimum_edge),
        }
    canary_evidence = _build_fusion_canary_non_regression(candidate)
    if canary_evidence.get("available") and not canary_evidence.get("non_regression", True):
        return {
            "pass": False,
            "reason_code": "CANARY_NON_REGRESSION_FAILED",
            "paper_evidence": paper_evidence,
            "paired_evidence": paired_evidence,
            "canary_evidence": canary_evidence,
            "minimum_utility_edge_vs_linear": float(minimum_edge),
        }
    return {
        "pass": True,
        "reason_code": "NONBASELINE_CLEAR_EDGE",
        "paper_evidence": paper_evidence,
        "paired_evidence": paired_evidence,
        "canary_evidence": canary_evidence,
        "minimum_utility_edge_vs_linear": float(minimum_edge),
    }


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
    offline_winner_assessment = (
        _build_fusion_clear_edge_assessment(candidate=offline_winner, baseline=baseline)
        if trainer == "v5_fusion" and baseline is not None and offline_winner.variant_name != baseline.variant_name
        else {}
    )
    kept_baseline = bool(baseline and baseline.run_id == selected.run_id)
    if kept_baseline:
        chosen_reason_code = "BASELINE_RETAINED_NO_CLEAR_EDGE"
        baseline_kept_reason_code = "NO_CLEAR_EDGE"
        if trainer == "v5_fusion" and offline_winner_assessment:
            chosen_reason_code = str(offline_winner_assessment.get("reason_code") or chosen_reason_code)
            baseline_kept_reason_code = str(offline_winner_assessment.get("reason_code") or baseline_kept_reason_code)
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
        baseline_summary = _build_variant_summary(record=baseline) if baseline is not None else {}
        selected_input_quality = _build_fusion_input_quality_summary(selected)
        baseline_input_quality = _build_fusion_input_quality_summary(baseline) if baseline is not None else {}
        selected_boundary = _build_fusion_entry_boundary_summary(selected)
        baseline_boundary = _build_fusion_entry_boundary_summary(baseline) if baseline is not None else {}
        selected_tradability = _build_fusion_tradability_provenance(selected)
        selected_sell_side = _build_fusion_sell_side_quality_summary(selected)
        baseline_sell_side = _build_fusion_sell_side_quality_summary(baseline) if baseline is not None else {}
        paper_evidence = _build_fusion_paper_non_regression(selected)
        paired_evidence = _build_fusion_paired_non_regression(selected)
        canary_evidence = _build_fusion_canary_non_regression(selected)
        input_quality_brake = _build_fusion_input_quality_brake(selected)
        selected_ev = _safe_float(selected.selection_key.get("test_ev_net_top5"))
        baseline_ev = _safe_float((baseline.selection_key if baseline is not None else {}).get("test_ev_net_top5"))
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
            "utility_edge_vs_linear": (
                float((selected_ev or 0.0) - (baseline_ev or 0.0))
                if (selected_ev is not None and baseline_ev is not None)
                else None
            ),
            "execution_structure_non_regression": _fusion_sell_side_quality_not_worse(candidate=selected, baseline=baseline),
            "paper_non_regression": (
                bool(paper_evidence.get("non_regression", False))
                if paper_evidence.get("available")
                else None
            ),
            "paired_non_regression": (
                bool(paired_evidence.get("non_regression", False))
                if paired_evidence.get("available")
                else None
            ),
            "canary_non_regression": (
                bool(canary_evidence.get("non_regression", False))
                if canary_evidence.get("available")
                else None
            ),
            "promotion_safe": bool(selected.runtime_viability_pass and selected.runtime_deploy_contract_ready),
            "rows_above_alpha_floor": _safe_int((selected.runtime_viability or {}).get("rows_above_alpha_floor")),
            "entry_gate_allowed_count": _safe_int((selected.runtime_viability or {}).get("entry_gate_allowed_count")),
            "runtime_viability_pass": bool(selected.runtime_viability_pass),
            "runtime_deploy_contract_ready": bool(selected.runtime_deploy_contract_ready),
            "input_quality_summary": selected_input_quality,
            "baseline_input_quality_summary": baseline_input_quality,
            "entry_boundary_summary": selected_boundary,
            "baseline_entry_boundary_summary": baseline_boundary,
            "tradability_provenance": selected_tradability,
            "sell_side_quality_summary": selected_sell_side,
            "baseline_sell_side_quality_summary": baseline_sell_side,
            "paper_evidence_summary": paper_evidence,
            "paired_evidence_summary": paired_evidence,
            "canary_evidence_summary": canary_evidence,
            "input_quality_non_regression": _fusion_input_quality_not_worse(candidate=selected, baseline=baseline),
            "boundary_quality_non_regression": _fusion_boundary_quality_not_worse(candidate=selected, baseline=baseline),
            "sell_side_quality_non_regression": _fusion_sell_side_quality_not_worse(candidate=selected, baseline=baseline),
            "input_quality_brake_applied": bool(input_quality_brake.get("applied", False)),
            "input_quality_brake_severity": str(input_quality_brake.get("severity") or "none"),
            "input_quality_reason_codes": list(input_quality_brake.get("reason_codes") or []),
            "minimum_utility_edge_vs_linear_required": float(
                input_quality_brake.get("minimum_utility_edge_vs_linear", 0.0) or 0.0
            ),
            "tradability_source_kind": str(selected_tradability.get("source_kind") or ""),
            "tradability_evidence_strength": str(selected_tradability.get("evidence_strength") or ""),
            "baseline_variant_name": str((baseline_summary.get("variant_name") or baseline_variant_name)),
            "offline_winner_clear_edge_assessment": offline_winner_assessment,
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


def _collect_fusion_input_ablation_record(*, payload: dict[str, Any], baseline: bool) -> FusionInputAblationRecord:
    matrix_payload = dict(payload or {})
    report_path = Path(str(matrix_payload.get("variant_report_path") or "")).resolve()
    report_doc = load_json(report_path) if report_path.exists() else {}
    selected_summary = dict(report_doc.get("selected_variant_summary") or {})
    utility_summary = dict(selected_summary.get("utility_summary") or {})
    pnl_summary = dict(selected_summary.get("pnl_summary") or {})
    input_provenance = dict(
        matrix_payload.get("input_provenance")
        or report_doc.get("input_provenance")
        or {}
    )
    return FusionInputAblationRecord(
        input_variant_name=str(input_provenance.get("input_variant_name") or "full_fusion"),
        baseline=bool(baseline),
        run_id=str(matrix_payload.get("run_id") or ""),
        run_dir=Path(str(matrix_payload.get("run_dir") or ".")),
        chosen_variant_name=str(matrix_payload.get("chosen_variant_name") or report_doc.get("chosen_variant_name") or ""),
        variant_report_path=str(report_path),
        reusable=bool(matrix_payload.get("reused", False)),
        source_mode=str(matrix_payload.get("source_mode") or ""),
        chosen_reason_code=str(matrix_payload.get("chosen_reason_code") or report_doc.get("chosen_reason_code") or ""),
        baseline_kept_reason_code=str(
            matrix_payload.get("baseline_kept_reason_code") or report_doc.get("baseline_kept_reason_code") or ""
        ),
        default_eligible=bool(matrix_payload.get("default_eligible", False)),
        runtime_viability_pass=bool(matrix_payload.get("runtime_viability_pass", False)),
        runtime_deploy_contract_ready=bool(matrix_payload.get("runtime_deploy_contract_ready", False)),
        selected_variant_summary=selected_summary,
        selection_key={
            "walk_forward_realized_pnl_quote": _safe_float(pnl_summary.get("walk_forward_realized_pnl_quote")),
            "test_ev_net_top5": _safe_float(utility_summary.get("test_ev_net_top5")),
            "test_precision_top5": _safe_float(utility_summary.get("test_precision_top5")),
            "test_pr_auc": _safe_float(utility_summary.get("test_pr_auc")),
            "test_log_loss": _safe_float(utility_summary.get("test_log_loss")),
        },
        input_provenance=input_provenance,
        fusion_variant_report=report_doc,
        matrix_payload=matrix_payload,
    )


def _fusion_input_ablation_sort_key(record: FusionInputAblationRecord) -> tuple[float, float, float, float, float]:
    key = record.selection_key
    return (
        _safe_float(key.get("walk_forward_realized_pnl_quote")) or float("-inf"),
        _safe_float(key.get("test_ev_net_top5")) or float("-inf"),
        _safe_float(key.get("test_precision_top5")) or float("-inf"),
        _safe_float(key.get("test_pr_auc")) or float("-inf"),
        -(_safe_float(key.get("test_log_loss")) or float("inf")),
    )


def _fusion_input_ablation_metric_delta(
    *,
    candidate: FusionInputAblationRecord,
    baseline: FusionInputAblationRecord | None,
    key: str,
) -> float | None:
    if baseline is None:
        return None
    candidate_value = _safe_float(candidate.selection_key.get(key))
    baseline_value = _safe_float(baseline.selection_key.get(key))
    if candidate_value is None or baseline_value is None:
        return None
    return float(candidate_value - baseline_value)


def _fusion_input_ablation_boundary_not_worse(
    *,
    candidate: FusionInputAblationRecord,
    baseline: FusionInputAblationRecord | None,
) -> bool | None:
    if baseline is None:
        return None
    candidate_summary = dict((candidate.selected_variant_summary.get("entry_boundary_summary") or {}))
    baseline_summary = dict((baseline.selected_variant_summary.get("entry_boundary_summary") or {}))
    if not candidate_summary and not baseline_summary:
        return None
    candidate_policy = dict(candidate_summary.get("support_quality_policy") or {})
    baseline_policy = dict(baseline_summary.get("support_quality_policy") or {})
    candidate_threshold = _safe_float(candidate_policy.get("support_score_threshold"))
    baseline_threshold = _safe_float(baseline_policy.get("support_score_threshold"))
    candidate_risk_multiplier = _safe_float(candidate_policy.get("reduced_context_severe_loss_risk_multiplier"))
    baseline_risk_multiplier = _safe_float(baseline_policy.get("reduced_context_severe_loss_risk_multiplier"))
    threshold_not_looser = (
        candidate_threshold is None
        or baseline_threshold is None
        or candidate_threshold >= baseline_threshold
    )
    risk_not_looser = (
        candidate_risk_multiplier is None
        or baseline_risk_multiplier is None
        or candidate_risk_multiplier >= baseline_risk_multiplier
    )
    return bool(threshold_not_looser and risk_not_looser)


def _fusion_input_ablation_sell_side_not_worse(
    *,
    candidate: FusionInputAblationRecord,
    baseline: FusionInputAblationRecord | None,
) -> bool | None:
    if baseline is None:
        return None
    candidate_summary = dict((candidate.selected_variant_summary.get("sell_side_quality_summary") or {}))
    baseline_summary = dict((baseline.selected_variant_summary.get("sell_side_quality_summary") or {}))
    if not candidate_summary and not baseline_summary:
        return None
    candidate_rank = _sell_side_quality_rank(candidate_summary)
    baseline_rank = _sell_side_quality_rank(baseline_summary)
    candidate_timeout = _safe_float(candidate_summary.get("selected", {}).get("timeout_exit_share"))
    baseline_timeout = _safe_float(baseline_summary.get("selected", {}).get("timeout_exit_share"))
    candidate_payoff = _safe_float(candidate_summary.get("selected", {}).get("payoff_ratio"))
    baseline_payoff = _safe_float(baseline_summary.get("selected", {}).get("payoff_ratio"))
    timeout_not_worse = (
        candidate_timeout is None
        or baseline_timeout is None
        or candidate_timeout <= (baseline_timeout + 0.05)
    )
    payoff_not_worse = (
        candidate_payoff is None
        or baseline_payoff is None
        or candidate_payoff >= (baseline_payoff - 0.05)
    )
    return bool(candidate_rank <= baseline_rank and timeout_not_worse and payoff_not_worse)


def _build_fusion_input_ablation_clear_edge_assessment(
    *,
    candidate: FusionInputAblationRecord,
    baseline: FusionInputAblationRecord,
) -> dict[str, Any]:
    candidate_pnl = _safe_float(candidate.selection_key.get("walk_forward_realized_pnl_quote"))
    baseline_pnl = _safe_float(baseline.selection_key.get("walk_forward_realized_pnl_quote"))
    candidate_ev = _safe_float(candidate.selection_key.get("test_ev_net_top5")) or float("-inf")
    baseline_ev = _safe_float(baseline.selection_key.get("test_ev_net_top5")) or float("-inf")
    candidate_precision = _safe_float(candidate.selection_key.get("test_precision_top5"))
    baseline_precision = _safe_float(baseline.selection_key.get("test_precision_top5"))
    candidate_log_loss = _safe_float(candidate.selection_key.get("test_log_loss"))
    baseline_log_loss = _safe_float(baseline.selection_key.get("test_log_loss"))
    if not candidate.default_eligible:
        return {
            "pass": False,
            "reason_code": "INPUT_ABLATION_NOT_DEFAULT_ELIGIBLE",
            "walk_forward_pnl_edge_vs_full_fusion": _fusion_input_ablation_metric_delta(
                candidate=candidate,
                baseline=baseline,
                key="walk_forward_realized_pnl_quote",
            ),
            "utility_edge_vs_full_fusion": _fusion_input_ablation_metric_delta(
                candidate=candidate,
                baseline=baseline,
                key="test_ev_net_top5",
            ),
        }
    if candidate_pnl is not None and baseline_pnl is not None:
        if candidate_pnl <= baseline_pnl + 1e-6:
            return {
                "pass": False,
                "reason_code": "INPUT_ABLATION_ECONOMIC_EDGE_BELOW_BASELINE",
                "walk_forward_pnl_edge_vs_full_fusion": float(candidate_pnl - baseline_pnl),
                "utility_edge_vs_full_fusion": _fusion_input_ablation_metric_delta(
                    candidate=candidate,
                    baseline=baseline,
                    key="test_ev_net_top5",
                ),
            }
    elif candidate_ev <= baseline_ev + 1e-6:
        return {
            "pass": False,
            "reason_code": "INPUT_ABLATION_UTILITY_EDGE_BELOW_BASELINE",
            "walk_forward_pnl_edge_vs_full_fusion": _fusion_input_ablation_metric_delta(
                candidate=candidate,
                baseline=baseline,
                key="walk_forward_realized_pnl_quote",
            ),
            "utility_edge_vs_full_fusion": float(candidate_ev - baseline_ev),
        }
    if candidate_precision is not None and baseline_precision is not None and candidate_precision < (baseline_precision - 0.01):
        return {
            "pass": False,
            "reason_code": "INPUT_ABLATION_PRECISION_REGRESSION",
            "walk_forward_pnl_edge_vs_full_fusion": _fusion_input_ablation_metric_delta(
                candidate=candidate,
                baseline=baseline,
                key="walk_forward_realized_pnl_quote",
            ),
            "utility_edge_vs_full_fusion": _fusion_input_ablation_metric_delta(
                candidate=candidate,
                baseline=baseline,
                key="test_ev_net_top5",
            ),
        }
    if candidate_log_loss is not None and baseline_log_loss is not None and candidate_log_loss > (baseline_log_loss + 0.02):
        return {
            "pass": False,
            "reason_code": "INPUT_ABLATION_LOGLOSS_REGRESSION",
            "walk_forward_pnl_edge_vs_full_fusion": _fusion_input_ablation_metric_delta(
                candidate=candidate,
                baseline=baseline,
                key="walk_forward_realized_pnl_quote",
            ),
            "utility_edge_vs_full_fusion": _fusion_input_ablation_metric_delta(
                candidate=candidate,
                baseline=baseline,
                key="test_ev_net_top5",
            ),
        }
    if not (_fusion_input_ablation_sell_side_not_worse(candidate=candidate, baseline=baseline) in {True, None}):
        return {
            "pass": False,
            "reason_code": "INPUT_ABLATION_SELL_SIDE_REGRESSION",
            "walk_forward_pnl_edge_vs_full_fusion": _fusion_input_ablation_metric_delta(
                candidate=candidate,
                baseline=baseline,
                key="walk_forward_realized_pnl_quote",
            ),
            "utility_edge_vs_full_fusion": _fusion_input_ablation_metric_delta(
                candidate=candidate,
                baseline=baseline,
                key="test_ev_net_top5",
            ),
        }
    if not (_fusion_input_ablation_boundary_not_worse(candidate=candidate, baseline=baseline) in {True, None}):
        return {
            "pass": False,
            "reason_code": "INPUT_ABLATION_BOUNDARY_REGRESSION",
            "walk_forward_pnl_edge_vs_full_fusion": _fusion_input_ablation_metric_delta(
                candidate=candidate,
                baseline=baseline,
                key="walk_forward_realized_pnl_quote",
            ),
            "utility_edge_vs_full_fusion": _fusion_input_ablation_metric_delta(
                candidate=candidate,
                baseline=baseline,
                key="test_ev_net_top5",
            ),
        }
    return {
        "pass": True,
        "reason_code": "INPUT_ABLATION_CLEAR_EDGE",
        "walk_forward_pnl_edge_vs_full_fusion": _fusion_input_ablation_metric_delta(
            candidate=candidate,
            baseline=baseline,
            key="walk_forward_realized_pnl_quote",
        ),
        "utility_edge_vs_full_fusion": _fusion_input_ablation_metric_delta(
            candidate=candidate,
            baseline=baseline,
            key="test_ev_net_top5",
        ),
    }


def _select_fusion_input_ablation_winner(records: list[FusionInputAblationRecord]) -> FusionInputAblationRecord:
    baseline = next((item for item in records if item.input_variant_name == "full_fusion"), None)
    valid_records = [
        item
        for item in records
        if item.default_eligible and item.runtime_viability_pass and item.runtime_deploy_contract_ready
    ]
    baseline_valid = (
        baseline
        if baseline is not None and baseline.default_eligible and baseline.runtime_viability_pass and baseline.runtime_deploy_contract_ready
        else None
    )
    if not valid_records:
        if baseline is not None:
            return baseline
        raise ValueError("no valid fusion input ablation variants available")
    chosen = sorted(valid_records, key=_fusion_input_ablation_sort_key, reverse=True)[0]
    if baseline_valid is None or chosen.input_variant_name == baseline_valid.input_variant_name:
        return chosen if baseline_valid is None else baseline_valid
    if not _build_fusion_input_ablation_clear_edge_assessment(candidate=chosen, baseline=baseline_valid).get("pass", False):
        return baseline_valid
    return chosen


def _build_fusion_input_ablation_summary(*, record: FusionInputAblationRecord) -> dict[str, Any]:
    return {
        "input_variant_name": record.input_variant_name,
        "baseline": record.baseline,
        "run_id": record.run_id,
        "run_dir": str(record.run_dir),
        "chosen_fusion_variant_name": record.chosen_variant_name,
        "variant_report_path": record.variant_report_path,
        "reused": record.reusable,
        "source_mode": record.source_mode,
        "chosen_reason_code": record.chosen_reason_code,
        "baseline_kept_reason_code": record.baseline_kept_reason_code,
        "default_eligible": record.default_eligible,
        "runtime_viability_pass": record.runtime_viability_pass,
        "runtime_deploy_contract_ready": record.runtime_deploy_contract_ready,
        "selected_variant_summary": dict(record.selected_variant_summary or {}),
        "input_provenance": dict(record.input_provenance or {}),
    }


def _resolve_fusion_input_variant_provenance(*, options: TrainV5FusionOptions) -> dict[str, Any]:
    input_variant = _resolve_fusion_input_variant(options)
    input_variant_name = str(input_variant.get("input_variant_name") or "full_fusion")
    include_sequence = bool(input_variant.get("include_sequence", True))
    include_lob = bool(input_variant.get("include_lob", True))
    include_tradability = bool(input_variant.get("include_tradability", True))

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
        "input_variant_name": input_variant_name,
        "include_sequence": bool(include_sequence),
        "include_lob": bool(include_lob),
        "include_tradability": bool(include_tradability),
        "included_experts": [
            expert_name
            for expert_name, included in (
                ("panel", True),
                ("sequence", include_sequence),
                ("lob", include_lob),
                ("tradability", include_tradability),
            )
            if included
        ],
        "excluded_experts": [
            expert_name
            for expert_name, included in (
                ("sequence", include_sequence),
                ("lob", include_lob),
                ("tradability", include_tradability),
            )
            if not included
        ],
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
    if record.trainer == "v5_fusion":
        payload["input_quality_summary"] = _build_fusion_input_quality_summary(record)
        payload["entry_boundary_summary"] = _build_fusion_entry_boundary_summary(record)
        payload["tradability_provenance"] = _build_fusion_tradability_provenance(record)
        payload["sell_side_quality_summary"] = _build_fusion_sell_side_quality_summary(record)
    if record.trainer == "v5_sequence":
        payload["pretrain_summary"] = _build_sequence_pretrain_summary(record)
        payload["ood_summary"] = _build_ood_summary(record)
    if record.trainer == "v5_lob":
        payload["support_level_coverage_summary"] = _build_lob_support_level_summary(record)
        payload["primary_vs_aux_target_consistency"] = _build_lob_target_consistency(record)
    return payload


def _build_fusion_input_quality_summary(record: VariantRunRecord | None) -> dict[str, Any]:
    if record is None:
        return {}
    runtime_viability = dict(record.runtime_viability or {})
    runtime_deploy = dict(record.runtime_deploy_contract_readiness or {})
    return (
        dict(runtime_viability.get("input_quality_summary") or {})
        or dict(runtime_deploy.get("input_quality_summary") or {})
    )


def _build_fusion_entry_boundary_summary(record: VariantRunRecord | None) -> dict[str, Any]:
    if record is None:
        return {}
    runtime_viability = dict(record.runtime_viability or {})
    runtime_deploy = dict(record.runtime_deploy_contract_readiness or {})
    return (
        dict(runtime_viability.get("entry_boundary_summary") or {})
        or dict(runtime_deploy.get("entry_boundary_summary") or {})
    )


def _build_fusion_tradability_provenance(record: VariantRunRecord | None) -> dict[str, Any]:
    if record is None:
        return {}
    runtime_viability = dict(record.runtime_viability or {})
    runtime_deploy = dict(record.runtime_deploy_contract_readiness or {})
    return (
        dict(runtime_viability.get("tradability_provenance") or {})
        or dict(runtime_deploy.get("tradability_provenance") or {})
    )


def _build_fusion_sell_side_quality_summary(record: VariantRunRecord | None) -> dict[str, Any]:
    if record is None:
        return {}
    runtime_doc = load_json(record.run_dir / "runtime_recommendations.json")
    return dict(runtime_doc.get("sell_side_quality_summary") or {})


def _load_fusion_promotion_decision(record: VariantRunRecord | None) -> dict[str, Any]:
    if record is None:
        return {}
    return load_json(record.run_dir / "promotion_decision.json")


def _infer_project_root_from_run_dir(run_dir: Path) -> Path | None:
    resolved = Path(run_dir).resolve()
    for parent in [resolved, *resolved.parents]:
        if (parent / "models" / "registry").exists():
            return parent
    return None


def _build_fusion_paper_non_regression(record: VariantRunRecord | None) -> dict[str, Any]:
    promotion = _load_fusion_promotion_decision(record)
    decision = dict(promotion.get("decision") or {})
    pairwise_checks = dict(decision.get("pairwise_checks") or {})
    if not pairwise_checks:
        return {"available": False, "non_regression": None}
    non_regression = all(bool(value) for value in pairwise_checks.values())
    return {
        "available": True,
        "non_regression": bool(non_regression),
        "decision": str(decision.get("decision") or "").strip(),
        "promote": bool(decision.get("promote", False)),
        "hard_failures": list(decision.get("hard_failures") or []),
        "evidence_score": _safe_float(decision.get("evidence_score")),
        "pairwise_checks": pairwise_checks,
        "execution_liquidation_summary": dict(decision.get("execution_liquidation_summary") or {}),
    }


def _build_fusion_paired_non_regression(record: VariantRunRecord | None) -> dict[str, Any]:
    promotion = _load_fusion_promotion_decision(record)
    if str(promotion.get("comparison_mode") or "").strip() != "paired_paper_runtime_decision_v1":
        return {"available": False, "non_regression": None}
    decision = dict(promotion.get("decision") or {})
    paired_gate = dict(promotion.get("paired_gate") or {})
    matched_evidence_checks = dict(decision.get("matched_evidence_checks") or {})
    non_regression = bool(paired_gate.get("pass", False))
    if matched_evidence_checks:
        non_regression = non_regression and all(bool(value) for value in matched_evidence_checks.values())
    if list(decision.get("hard_failures") or []):
        non_regression = False
    return {
        "available": True,
        "non_regression": bool(non_regression),
        "decision": str(decision.get("decision") or "").strip(),
        "promote": bool(decision.get("promote", False)),
        "hard_failures": list(decision.get("hard_failures") or []),
        "paired_gate": paired_gate,
        "matched_evidence_checks": matched_evidence_checks,
        "paired_report_excerpt": dict(promotion.get("paired_report_excerpt") or {}),
    }


def _build_fusion_canary_non_regression(record: VariantRunRecord | None) -> dict[str, Any]:
    if record is None:
        return {"available": False, "non_regression": None}
    project_root = _infer_project_root_from_run_dir(record.run_dir)
    if project_root is None:
        return {"available": False, "non_regression": None}
    canary_root = project_root / "logs" / "canary_confidence_sequence"
    if not canary_root.exists():
        return {"available": False, "non_regression": None}
    for latest_path in canary_root.glob("*/latest.json"):
        payload = load_json(latest_path)
        if str(payload.get("run_id") or "").strip() != record.run_id:
            continue
        decision = dict(payload.get("decision") or {})
        non_regression = (not bool(decision.get("abort", False))) and (
            bool(decision.get("promote_eligible", False))
            or len(list(decision.get("blocking_reason_codes") or [])) == 0
        )
        return {
            "available": True,
            "non_regression": bool(non_regression),
            "status": str(decision.get("status") or "").strip(),
            "promote_eligible": bool(decision.get("promote_eligible", False)),
            "abort": bool(decision.get("abort", False)),
            "abort_reason_codes": list(decision.get("abort_reason_codes") or []),
            "blocking_reason_codes": list(decision.get("blocking_reason_codes") or []),
            "execution_liquidation_summary": dict(decision.get("execution_liquidation_summary") or {}),
            "path": str(latest_path),
        }
    return {"available": False, "non_regression": None}


def _quality_status_rank(status: str | None) -> int:
    value = str(status or "").strip().lower()
    if value in {"", "unknown", "ready", "healthy", "anchor_ready", "excluded_by_ablation", "not_applicable"}:
        return 0
    if value in {"caution", "moderate_training_evidence", "reduced_context_mixed"}:
        return 1
    if value in {"degraded", "thin_training_evidence", "proxy_backed", "reduced_context_heavy"}:
        return 2
    if value in {"missing", "structural_invalid_heavy"}:
        return 3
    return 1


def _build_fusion_input_quality_brake(record: VariantRunRecord | None) -> dict[str, Any]:
    input_quality_summary = _build_fusion_input_quality_summary(record)
    tradability_provenance = _build_fusion_tradability_provenance(record)
    overall_status = str(input_quality_summary.get("overall_quality_status") or "").strip()
    tradability_status = str(tradability_provenance.get("quality_status") or "").strip()
    severity = "none"
    minimum_edge = 0.0
    if _quality_status_rank(overall_status) >= 2 or _quality_status_rank(tradability_status) >= 2:
        severity = "degraded"
        minimum_edge = 0.01
    elif _quality_status_rank(overall_status) == 1 or _quality_status_rank(tradability_status) == 1:
        severity = "caution"
        minimum_edge = 0.005
    reason_codes: list[str] = []
    for code in list(input_quality_summary.get("reason_codes") or []) + list(tradability_provenance.get("reason_codes") or []):
        normalized = str(code).strip()
        if normalized and normalized not in reason_codes:
            reason_codes.append(normalized)
    return {
        "applied": minimum_edge > 0.0,
        "severity": severity,
        "minimum_utility_edge_vs_linear": float(minimum_edge),
        "reason_codes": reason_codes,
        "overall_quality_status": overall_status,
        "tradability_quality_status": tradability_status,
    }


def _fusion_input_quality_not_worse(*, candidate: VariantRunRecord, baseline: VariantRunRecord | None) -> bool | None:
    if baseline is None:
        return None
    candidate_summary = _build_fusion_input_quality_summary(candidate)
    baseline_summary = _build_fusion_input_quality_summary(baseline)
    candidate_tradability = _build_fusion_tradability_provenance(candidate)
    baseline_tradability = _build_fusion_tradability_provenance(baseline)
    if not candidate_summary and not baseline_summary and not candidate_tradability and not baseline_tradability:
        return None
    candidate_rank = max(
        _quality_status_rank(candidate_summary.get("overall_quality_status")),
        _quality_status_rank(candidate_tradability.get("quality_status")),
    )
    baseline_rank = max(
        _quality_status_rank(baseline_summary.get("overall_quality_status")),
        _quality_status_rank(baseline_tradability.get("quality_status")),
    )
    return candidate_rank <= baseline_rank


def _fusion_boundary_quality_not_worse(*, candidate: VariantRunRecord, baseline: VariantRunRecord | None) -> bool | None:
    if baseline is None:
        return None
    candidate_summary = _build_fusion_entry_boundary_summary(candidate)
    baseline_summary = _build_fusion_entry_boundary_summary(baseline)
    if not candidate_summary and not baseline_summary:
        return None
    candidate_policy = dict(candidate_summary.get("support_quality_policy") or {})
    baseline_policy = dict(baseline_summary.get("support_quality_policy") or {})
    candidate_threshold = _safe_float(candidate_policy.get("support_score_threshold"))
    baseline_threshold = _safe_float(baseline_policy.get("support_score_threshold"))
    candidate_risk_multiplier = _safe_float(candidate_policy.get("reduced_context_severe_loss_risk_multiplier"))
    baseline_risk_multiplier = _safe_float(baseline_policy.get("reduced_context_severe_loss_risk_multiplier"))
    threshold_not_looser = (
        candidate_threshold is None
        or baseline_threshold is None
        or candidate_threshold >= baseline_threshold
    )
    risk_not_looser = (
        candidate_risk_multiplier is None
        or baseline_risk_multiplier is None
        or candidate_risk_multiplier >= baseline_risk_multiplier
    )
    return bool(threshold_not_looser and risk_not_looser)


def _sell_side_quality_rank(summary: dict[str, Any] | None) -> int:
    payload = dict(summary or {})
    status = str(payload.get("quality_status") or "").strip().lower()
    if status in {"", "healthy"}:
        return 0
    if status in {"payoff_weak"}:
        return 1
    if status in {"timeout_heavy"}:
        return 2
    if status in {"timeout_heavy_no_tp", "loss_concentration_high"}:
        return 3
    if status in {"insufficient_trade_count"}:
        return 4
    return 1


def _fusion_sell_side_quality_not_worse(*, candidate: VariantRunRecord, baseline: VariantRunRecord | None) -> bool | None:
    if baseline is None:
        return None
    candidate_summary = _build_fusion_sell_side_quality_summary(candidate)
    baseline_summary = _build_fusion_sell_side_quality_summary(baseline)
    if not candidate_summary and not baseline_summary:
        return None
    candidate_rank = _sell_side_quality_rank(candidate_summary)
    baseline_rank = _sell_side_quality_rank(baseline_summary)
    candidate_timeout = _safe_float(candidate_summary.get("selected", {}).get("timeout_exit_share"))
    baseline_timeout = _safe_float(baseline_summary.get("selected", {}).get("timeout_exit_share"))
    candidate_payoff = _safe_float(candidate_summary.get("selected", {}).get("payoff_ratio"))
    baseline_payoff = _safe_float(baseline_summary.get("selected", {}).get("payoff_ratio"))
    timeout_not_worse = (
        candidate_timeout is None
        or baseline_timeout is None
        or candidate_timeout <= (baseline_timeout + 0.05)
    )
    payoff_not_worse = (
        candidate_payoff is None
        or baseline_payoff is None
        or candidate_payoff >= (baseline_payoff - 0.10)
    )
    return bool(candidate_rank <= baseline_rank and timeout_not_worse and payoff_not_worse)


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
