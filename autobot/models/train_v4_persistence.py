"""Persistence helpers for trainer=v4_crypto_cs artifacts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .factor_block_selector import (
    append_factor_block_selection_history,
    build_guarded_factor_block_policy,
    load_factor_block_selection_history,
    write_latest_factor_block_selection_pointer,
    write_latest_guarded_factor_block_policy,
)
from .search_budget import write_search_budget_decision


def persist_v4_support_artifacts(
    *,
    run_dir: Path,
    options: Any,
    run_id: str,
    factor_block_registry: Any,
    walk_forward: dict[str, Any],
    cpcv_lite: dict[str, Any],
    factor_block_selection: dict[str, Any],
    search_budget_decision: dict[str, Any],
) -> dict[str, Any]:
    walk_forward_report_path = _write_json(
        run_dir / "walk_forward_report.json",
        walk_forward,
    )

    cpcv_lite_report_path: Path | None = None
    if bool(cpcv_lite):
        cpcv_lite_report_path = _write_json(
            run_dir / "cpcv_lite_report.json",
            cpcv_lite,
        )

    factor_block_selection_pointer_path = write_latest_factor_block_selection_pointer(
        registry_root=options.registry_root,
        model_family=options.model_family,
        run_id=run_id,
        report=factor_block_selection,
        run_scope=options.run_scope,
    )
    if factor_block_selection_pointer_path is not None:
        factor_block_selection["latest_pointer_path"] = str(factor_block_selection_pointer_path)

    factor_block_selection_path = _write_json(
        run_dir / "factor_block_selection.json",
        factor_block_selection,
    )
    factor_block_history_path = append_factor_block_selection_history(
        registry_root=options.registry_root,
        model_family=options.model_family,
        report=factor_block_selection,
        run_scope=options.run_scope,
    )
    factor_block_history = load_factor_block_selection_history(
        registry_root=options.registry_root,
        model_family=options.model_family,
        run_scope=options.run_scope,
    )
    factor_block_policy = build_guarded_factor_block_policy(
        block_registry=factor_block_registry,
        history_records=factor_block_history,
    )
    factor_block_policy_path = write_latest_guarded_factor_block_policy(
        registry_root=options.registry_root,
        model_family=options.model_family,
        run_id=run_id,
        policy=factor_block_policy,
        run_scope=options.run_scope,
    )
    if factor_block_history_path is not None:
        factor_block_selection["history_path"] = str(factor_block_history_path)
    if factor_block_policy_path is not None:
        factor_block_selection["guarded_policy_path"] = str(factor_block_policy_path)
    factor_block_selection["guarded_policy"] = factor_block_policy
    factor_block_selection_path = _write_json(
        factor_block_selection_path,
        factor_block_selection,
    )

    search_budget_decision_path = write_search_budget_decision(
        run_dir=run_dir,
        decision=search_budget_decision,
    )
    return {
        "walk_forward_report_path": walk_forward_report_path,
        "cpcv_lite_report_path": cpcv_lite_report_path,
        "factor_block_selection_path": factor_block_selection_path,
        "factor_block_history_path": factor_block_history_path,
        "factor_block_policy_path": factor_block_policy_path,
        "search_budget_decision_path": search_budget_decision_path,
        "factor_block_selection": factor_block_selection,
        "factor_block_policy": factor_block_policy,
    }


def persist_v4_runtime_and_governance_artifacts(
    *,
    run_dir: Path,
    execution_acceptance: dict[str, Any],
    runtime_recommendations: dict[str, Any],
    promotion: dict[str, Any],
    trainer_research_evidence: dict[str, Any],
    economic_objective_profile: dict[str, Any],
    lane_governance: dict[str, Any],
    decision_surface: dict[str, Any],
) -> dict[str, Path]:
    return {
        "execution_acceptance_report_path": _write_json(
            run_dir / "execution_acceptance_report.json",
            execution_acceptance,
        ),
        "runtime_recommendations_path": _write_json(
            run_dir / "runtime_recommendations.json",
            runtime_recommendations,
        ),
        "promotion_path": _write_json(
            run_dir / "promotion_decision.json",
            promotion,
        ),
        "trainer_research_evidence_path": _write_json(
            run_dir / "trainer_research_evidence.json",
            trainer_research_evidence,
        ),
        "economic_objective_profile_path": _write_json(
            run_dir / "economic_objective_profile.json",
            economic_objective_profile,
        ),
        "lane_governance_path": _write_json(
            run_dir / "lane_governance.json",
            lane_governance,
        ),
        "decision_surface_path": _write_json(
            run_dir / "decision_surface.json",
            decision_surface,
        ),
    }


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path
