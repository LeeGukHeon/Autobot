from __future__ import annotations

import argparse
import json
from dataclasses import fields
from pathlib import Path
from typing import Any

from autobot.models.registry import load_json
from autobot.models.train_v4_crypto_cs import (
    ModelAlphaExecutionSettings,
    ModelAlphaExitSettings,
    ModelAlphaSelectionSettings,
    ModelAlphaSettings,
    TrainV4CryptoCsOptions,
    _build_decision_surface_v4,
    _build_execution_risk_control_v4,
    _build_research_support_lane_v4,
    _build_runtime_recommendations_v4,
    _build_trade_action_policy_v4,
    _build_trainer_research_evidence_from_promotion_v4,
    _manual_promotion_decision_v4,
    _purge_execution_artifact_run_dirs,
)
from autobot.models.train_v4_persistence import persist_v4_runtime_and_governance_artifacts


def _model_alpha_settings_from_doc(payload: dict[str, Any] | None) -> ModelAlphaSettings:
    doc = dict(payload or {})
    selection = dict(doc.get("selection") or {})
    position = dict(doc.get("position") or {})
    exit_doc = dict(doc.get("exit") or {})
    execution = dict(doc.get("execution") or {})
    operational = dict(doc.get("operational") or {})
    return ModelAlphaSettings(
        model_ref=str(doc.get("model_ref", "candidate_v4")).strip() or "candidate_v4",
        model_family=str(doc.get("model_family", "train_v4_crypto_cs")).strip() or "train_v4_crypto_cs",
        feature_set=str(doc.get("feature_set", "v4")).strip().lower() or "v4",
        selection=ModelAlphaSelectionSettings(
            top_pct=float(selection.get("top_pct", 1.0) or 1.0),
            min_prob=(float(selection["min_prob"]) if selection.get("min_prob") is not None else None),
            min_candidates_per_ts=int(selection.get("min_candidates_per_ts", 1) or 1),
            registry_threshold_key=str(selection.get("registry_threshold_key", "top_5pct")).strip() or "top_5pct",
            use_learned_recommendations=bool(selection.get("use_learned_recommendations", False)),
            selection_policy_mode=str(selection.get("selection_policy_mode", "auto")).strip().lower() or "auto",
        ),
        position=ModelAlphaSettings().position.__class__(
            max_positions_total=int(position.get("max_positions_total", 3) or 3),
            cooldown_bars=int(position.get("cooldown_bars", 0) or 0),
            entry_min_notional_buffer_bps=float(position.get("entry_min_notional_buffer_bps", 0.0) or 0.0),
            sizing_mode=str(position.get("sizing_mode", "prob_ramp")).strip().lower() or "prob_ramp",
            size_multiplier_min=float(position.get("size_multiplier_min", 0.5) or 0.5),
            size_multiplier_max=float(position.get("size_multiplier_max", 1.5) or 1.5),
        ),
        exit=ModelAlphaExitSettings(
            mode=str(exit_doc.get("mode", "hold")).strip().lower() or "hold",
            hold_bars=int(exit_doc.get("hold_bars", 6) or 6),
            use_learned_exit_mode=bool(exit_doc.get("use_learned_exit_mode", False)),
            use_learned_hold_bars=bool(exit_doc.get("use_learned_hold_bars", False)),
            use_learned_risk_recommendations=bool(exit_doc.get("use_learned_risk_recommendations", False)),
            use_trade_level_action_policy=bool(exit_doc.get("use_trade_level_action_policy", False)),
            risk_scaling_mode=str(exit_doc.get("risk_scaling_mode", "fixed")).strip().lower() or "fixed",
            risk_vol_feature=str(exit_doc.get("risk_vol_feature", "rv_12")).strip() or "rv_12",
            tp_vol_multiplier=(float(exit_doc["tp_vol_multiplier"]) if exit_doc.get("tp_vol_multiplier") is not None else None),
            sl_vol_multiplier=(float(exit_doc["sl_vol_multiplier"]) if exit_doc.get("sl_vol_multiplier") is not None else None),
            trailing_vol_multiplier=(float(exit_doc["trailing_vol_multiplier"]) if exit_doc.get("trailing_vol_multiplier") is not None else None),
            tp_pct=float(exit_doc.get("tp_pct", 0.02) or 0.02),
            sl_pct=float(exit_doc.get("sl_pct", 0.01) or 0.01),
            trailing_pct=float(exit_doc.get("trailing_pct", 0.0) or 0.0),
            expected_exit_slippage_bps=(float(exit_doc["expected_exit_slippage_bps"]) if exit_doc.get("expected_exit_slippage_bps") is not None else None),
            expected_exit_fee_bps=(float(exit_doc["expected_exit_fee_bps"]) if exit_doc.get("expected_exit_fee_bps") is not None else None),
        ),
        execution=ModelAlphaExecutionSettings(
            price_mode=str(execution.get("price_mode", "JOIN")).strip().upper() or "JOIN",
            timeout_bars=int(execution.get("timeout_bars", 2) or 2),
            replace_max=int(execution.get("replace_max", 2) or 2),
            use_learned_recommendations=bool(execution.get("use_learned_recommendations", False)),
        ),
        operational=ModelAlphaSettings().operational.__class__(**operational) if operational else ModelAlphaSettings().operational,
    )


def _options_from_train_config(train_config: dict[str, Any]) -> TrainV4CryptoCsOptions:
    payload: dict[str, Any] = {}
    for field in fields(TrainV4CryptoCsOptions):
        if field.name not in train_config:
            continue
        payload[field.name] = train_config[field.name]
    for key in (
        "dataset_root",
        "registry_root",
        "logs_root",
        "execution_acceptance_parquet_root",
        "execution_acceptance_output_root",
        "live_domain_reweighting_db_path",
    ):
        if key in payload and payload[key] is not None:
            payload[key] = Path(str(payload[key]))
    payload["execution_acceptance_model_alpha"] = _model_alpha_settings_from_doc(
        train_config.get("execution_acceptance_model_alpha")
    )
    return TrainV4CryptoCsOptions(**payload)


def rebuild_runtime_artifacts_for_run(*, run_dir: Path) -> dict[str, Any]:
    train_config = load_json(run_dir / "train_config.yaml")
    if not train_config:
        raise FileNotFoundError(f"missing train_config.yaml: {run_dir}")
    options = _options_from_train_config(train_config)
    run_id = run_dir.name
    walk_forward = load_json(run_dir / "walk_forward_report.json")
    selection_calibration = load_json(run_dir / "selection_calibration.json")
    selection_policy = load_json(run_dir / "selection_policy.json")
    factor_block_selection = load_json(run_dir / "factor_block_selection.json")
    cpcv_lite = load_json(run_dir / "cpcv_lite_report.json")
    execution_acceptance = load_json(run_dir / "execution_acceptance_report.json")
    search_budget_decision = load_json(run_dir / "search_budget_decision.json")
    economic_objective_profile = load_json(run_dir / "economic_objective_profile.json")
    lane_governance = load_json(run_dir / "lane_governance.json")

    factor_block_selection_context = dict(
        ((train_config.get("factor_block_selection") or {}).get("resolution_context")) or {}
    )
    cpcv_lite_runtime = dict(train_config.get("cpcv_lite") or {})
    research_support_lane = _build_research_support_lane_v4(
        walk_forward=walk_forward,
        cpcv_lite=cpcv_lite,
    )
    trade_action_oos_rows = list(walk_forward.get("_trade_action_oos_rows") or [])

    runtime_recommendations = _build_runtime_recommendations_v4(
        options=options,
        run_id=run_id,
        search_budget_decision=search_budget_decision,
    )
    runtime_recommendations["trade_action"] = _build_trade_action_policy_v4(
        options=options,
        runtime_recommendations=runtime_recommendations,
        selection_calibration=selection_calibration,
        oos_rows=trade_action_oos_rows,
    )
    runtime_recommendations["risk_control"] = _build_execution_risk_control_v4(
        options=options,
        runtime_recommendations=runtime_recommendations,
        selection_calibration=selection_calibration,
        oos_rows=trade_action_oos_rows,
    )

    cleanup = _purge_execution_artifact_run_dirs(
        output_root=options.execution_acceptance_output_root,
        execution_acceptance=execution_acceptance,
        runtime_recommendations=runtime_recommendations,
    )
    if cleanup.get("evaluated"):
        execution_acceptance["artifacts_cleanup"] = cleanup
        runtime_recommendations["artifacts_cleanup"] = cleanup

    promotion = _manual_promotion_decision_v4(
        options=options,
        run_id=run_id,
        walk_forward=walk_forward,
        execution_acceptance=execution_acceptance,
        runtime_recommendations=runtime_recommendations,
    )
    trainer_research_evidence = _build_trainer_research_evidence_from_promotion_v4(
        promotion=promotion,
        support_lane=research_support_lane,
    )
    decision_surface = _build_decision_surface_v4(
        options=options,
        task=str(train_config.get("task", "cls")).strip().lower() or "cls",
        selection_policy=selection_policy,
        selection_calibration=selection_calibration,
        factor_block_selection=factor_block_selection,
        research_support_lane=research_support_lane,
        factor_block_selection_context=factor_block_selection_context,
        cpcv_lite_runtime=cpcv_lite_runtime,
        search_budget_decision=search_budget_decision,
        execution_acceptance=execution_acceptance,
        runtime_recommendations=runtime_recommendations,
        promotion=promotion,
        economic_objective_profile=economic_objective_profile,
        lane_governance=lane_governance,
    )
    written = persist_v4_runtime_and_governance_artifacts(
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
        "run_id": run_id,
        "run_dir": str(run_dir),
        "runtime_status": str(runtime_recommendations.get("status", "")).strip() or "unknown",
        "runtime_reason": str(runtime_recommendations.get("reason", "")).strip(),
        "trade_action_status": str(((runtime_recommendations.get("trade_action") or {}).get("status", ""))).strip() or "unknown",
        "trade_action_reason": str(((runtime_recommendations.get("trade_action") or {}).get("reason", ""))).strip(),
        "risk_control_status": str(((runtime_recommendations.get("risk_control") or {}).get("status", ""))).strip() or "unknown",
        "promotion_status": str(promotion.get("status", "")).strip() or "unknown",
        "trainer_research_evidence_pass": bool(trainer_research_evidence.get("pass", False)),
        "written_paths": {key: str(value) for key, value in written.items()},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Rebuild runtime/trade-action artifacts for an existing v4 run.")
    parser.add_argument("--run-dir", required=True)
    args = parser.parse_args()
    payload = rebuild_runtime_artifacts_for_run(run_dir=Path(str(args.run_dir)).resolve())
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
