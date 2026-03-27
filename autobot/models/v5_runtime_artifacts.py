"""Common runtime/governance artifacts for v5 expert-family trainers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .economic_objective import build_v4_shared_economic_objective_profile
from .factor_block_selector import normalize_run_scope
from .train_v4_governance import build_trainer_research_evidence_from_promotion_v4
from .train_v4_persistence import persist_v4_runtime_and_governance_artifacts
from .train_v4_postprocess import build_lane_governance_v4


def build_v5_support_lane(*, status: str = "ok", reasons: list[str] | None = None) -> dict[str, Any]:
    return {
        "policy": "v5_expert_support_lane_v1",
        "source": "trainer_runtime_contract",
        "support_only": True,
        "summary": {
            "status": str(status).strip() or "ok",
            "windows_run": 1,
            "multiple_testing_supported": False,
            "cpcv_lite_status": "disabled",
            "reasons": list(reasons or []),
        },
        "multiple_testing_panel_diagnostics": {},
        "spa_like": {},
        "white_rc": {},
        "hansen_spa": {},
        "cpcv_lite": {},
    }


def build_v5_execution_acceptance_stub(
    *,
    run_id: str,
    model_family: str,
    trainer_name: str,
    metrics: dict[str, Any],
    reasons: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "version": 1,
        "policy": "v5_trainer_execution_acceptance_stub_v1",
        "run_id": str(run_id).strip(),
        "model_family": str(model_family).strip(),
        "trainer": str(trainer_name).strip(),
        "status": "trainer_runtime_contract_ready",
        "evaluated": True,
        "pass": True,
        "reason_codes": list(reasons or []),
        "compare_to_champion": {},
        "summary": dict(metrics.get("champion_metrics") or {}),
    }


def build_v5_primary_lane_governance(*, run_scope: str) -> tuple[dict[str, Any], dict[str, Any]]:
    economic_objective_profile = build_v4_shared_economic_objective_profile()
    lane_governance = build_lane_governance_v4(
        task="cls",
        run_scope=str(run_scope).strip(),
        economic_objective_profile=economic_objective_profile,
        normalize_run_scope_fn=normalize_run_scope,
    )
    return economic_objective_profile, lane_governance


def build_v5_trainer_research_evidence(
    *,
    walk_forward_summary: dict[str, Any],
    promotion: dict[str, Any],
    support_lane: dict[str, Any],
) -> dict[str, Any]:
    promotion_payload = dict(promotion or {})
    checks = dict(promotion_payload.get("checks") or {})
    checks.setdefault("existing_champion_present", False)
    checks.setdefault("walk_forward_present", True)
    checks.setdefault("walk_forward_windows_run", 1)
    checks.setdefault("execution_acceptance_enabled", False)
    checks.setdefault("execution_acceptance_present", False)
    checks.setdefault("risk_control_required", False)
    promotion_payload["checks"] = checks
    research_acceptance = dict(promotion_payload.get("research_acceptance") or {})
    research_acceptance.setdefault("walk_forward_summary", dict(walk_forward_summary or {}))
    promotion_payload["research_acceptance"] = research_acceptance
    promotion_payload.setdefault("execution_acceptance", {"status": "not_required"})
    return build_trainer_research_evidence_from_promotion_v4(
        promotion=promotion_payload,
        support_lane=support_lane,
    )


def build_v5_decision_surface(
    *,
    trainer_name: str,
    model_family: str,
    run_scope: str,
    runtime_recommendations: dict[str, Any],
    promotion: dict[str, Any],
    economic_objective_profile: dict[str, Any],
    lane_governance: dict[str, Any],
    metrics: dict[str, Any],
) -> dict[str, Any]:
    return {
        "version": 1,
        "policy": "v5_decision_surface_v1",
        "trainer_entrypoint": {
            "trainer": str(trainer_name).strip(),
            "model_family": str(model_family).strip(),
            "run_scope": normalize_run_scope(run_scope),
        },
        "lane_governance": dict(lane_governance or {}),
        "economic_objective_contract": dict(economic_objective_profile or {}),
        "runtime_recommendations": dict(runtime_recommendations or {}),
        "promotion": dict(promotion or {}),
        "metrics": dict(metrics or {}),
    }


def persist_v5_runtime_governance_artifacts(
    *,
    run_dir: Path,
    trainer_name: str,
    model_family: str,
    run_scope: str,
    metrics: dict[str, Any],
    runtime_recommendations: dict[str, Any],
    promotion: dict[str, Any],
    trainer_research_reasons: list[str] | None = None,
) -> dict[str, Any]:
    economic_objective_profile, lane_governance = build_v5_primary_lane_governance(run_scope=run_scope)
    support_lane = build_v5_support_lane(status="ok", reasons=list(trainer_research_reasons or []))
    trainer_research_evidence = build_v5_trainer_research_evidence(
        walk_forward_summary=dict(metrics.get("valid_metrics") or {}),
        promotion=promotion,
        support_lane=support_lane,
    )
    execution_acceptance = build_v5_execution_acceptance_stub(
        run_id=run_dir.name,
        model_family=model_family,
        trainer_name=trainer_name,
        metrics=metrics,
        reasons=list(trainer_research_reasons or []),
    )
    decision_surface = build_v5_decision_surface(
        trainer_name=trainer_name,
        model_family=model_family,
        run_scope=run_scope,
        runtime_recommendations=runtime_recommendations,
        promotion=promotion,
        economic_objective_profile=economic_objective_profile,
        lane_governance=lane_governance,
        metrics=metrics,
    )
    runtime_paths = persist_v4_runtime_and_governance_artifacts(
        run_dir=run_dir,
        execution_acceptance=execution_acceptance,
        runtime_recommendations=runtime_recommendations,
        promotion=promotion,
        trainer_research_evidence=trainer_research_evidence,
        economic_objective_profile=economic_objective_profile,
        lane_governance=lane_governance,
        decision_surface=decision_surface,
    )
    return {
        **runtime_paths,
        "economic_objective_profile": economic_objective_profile,
        "lane_governance": lane_governance,
        "trainer_research_evidence": trainer_research_evidence,
        "execution_acceptance": execution_acceptance,
        "decision_surface": decision_surface,
    }
