"""Contract bundle assembly helpers for trainer=v4_crypto_cs."""

from __future__ import annotations

from typing import Any, Callable

from .factor_block_selector import build_factor_block_refit_support_summary, build_factor_block_selection_report
from .selection_calibration import build_selection_calibration_from_oos_rows
from .selection_optimizer import build_selection_recommendations_from_walk_forward
from .selection_policy import build_selection_policy_from_recommendations
from .train_v1 import build_selection_recommendations


def build_v4_contract_bundle(
    *,
    options: Any,
    task: str,
    dataset: Any,
    label_contract: dict[str, Any],
    action_aux_arrays: dict[str, Any] | None,
    interval_ms: int,
    thresholds: dict[str, Any],
    factor_block_registry: Any,
    effective_booster_sweep_trials: int,
    factor_block_selection_context: dict[str, Any],
    cpcv_lite_runtime: dict[str, Any],
    search_budget_decision: dict[str, Any],
    ranker_budget_profile: dict[str, Any],
    run_id: str,
    split_info: Any,
    rows: dict[str, int],
    valid_scores: Any,
    valid_ts_ms: Any,
    valid_metrics: dict[str, Any],
    test_metrics: dict[str, Any],
    booster: dict[str, Any],
    economic_objective_profile: dict[str, Any],
    lane_governance: dict[str, Any],
    run_walk_forward_fn: Callable[..., dict[str, Any]],
    finalize_walk_forward_report_fn: Callable[..., dict[str, Any]],
    run_cpcv_lite_fn: Callable[..., dict[str, Any]],
    build_research_support_lane_fn: Callable[..., dict[str, Any]],
    build_v4_metrics_doc_fn: Callable[..., dict[str, Any]],
    make_v4_leaderboard_row_fn: Callable[..., dict[str, Any]],
    train_config_snapshot_fn: Callable[..., dict[str, Any]],
) -> dict[str, Any]:
    fallback_selection_recommendations = build_selection_recommendations(
        valid_scores=valid_scores,
        valid_ts_ms=valid_ts_ms,
        thresholds=thresholds,
    )
    walk_forward = run_walk_forward_fn(
        options=options,
        task=task,
        dataset=dataset,
        action_aux_arrays=action_aux_arrays,
        interval_ms=interval_ms,
        thresholds=thresholds,
        feature_names=dataset.feature_names,
        factor_block_registry=factor_block_registry,
        effective_booster_sweep_trials=effective_booster_sweep_trials,
    )
    selection_recommendations = build_selection_recommendations_from_walk_forward(
        windows=walk_forward.get("windows", []),
        fallback_recommendations=fallback_selection_recommendations,
    )
    selection_policy = build_selection_policy_from_recommendations(
        selection_recommendations=selection_recommendations,
        fallback_threshold_key="top_5pct",
        forced_threshold_key=getattr(options, "selection_threshold_key_override", None),
    )
    selection_calibration = build_selection_calibration_from_oos_rows(
        oos_rows=walk_forward.pop("_selection_calibration_rows", []),
    )
    walk_forward = finalize_walk_forward_report_fn(
        walk_forward=walk_forward,
        selection_recommendations=selection_recommendations,
        options=options,
    )
    walk_forward["factor_block_refit_support"] = build_factor_block_refit_support_summary(
        block_registry=factor_block_registry,
        window_support=walk_forward.get("factor_block_refit_windows", []),
    )
    cpcv_lite = run_cpcv_lite_fn(
        options=options,
        task=task,
        dataset=dataset,
        interval_ms=interval_ms,
        thresholds=thresholds,
        best_params=dict(booster.get("best_params", {})),
        enabled=bool(cpcv_lite_runtime["enabled"]),
        trigger=str(cpcv_lite_runtime["trigger"]),
    )
    research_support_lane = build_research_support_lane_fn(
        walk_forward=walk_forward,
        cpcv_lite=cpcv_lite,
    )
    factor_block_selection = build_factor_block_selection_report(
        block_registry=factor_block_registry,
        window_rows=walk_forward.pop("_factor_block_window_rows", []),
        selection_mode=options.factor_block_selection_mode,
        feature_columns=dataset.feature_names,
        run_id=run_id,
        refit_support=walk_forward.get("factor_block_refit_support"),
    )
    metrics = build_v4_metrics_doc_fn(
        run_id=run_id,
        options=options,
        task=task,
        split_info=split_info,
        interval_ms=interval_ms,
        rows=rows,
        valid_metrics=valid_metrics,
        test_metrics=test_metrics,
        walk_forward_summary=walk_forward.get("summary", {}),
        cpcv_lite_summary=cpcv_lite.get("summary", {}),
        factor_block_selection_summary=factor_block_selection.get("summary", {}),
        best_params=dict(booster.get("best_params", {})),
        sweep_records=list(booster.get("trials", [])),
        ranker_budget_profile=ranker_budget_profile,
        cpcv_lite_runtime=cpcv_lite_runtime,
        search_budget_decision=search_budget_decision,
        economic_objective_profile=economic_objective_profile,
        lane_governance=lane_governance,
    )
    leaderboard_row = make_v4_leaderboard_row_fn(
        run_id=run_id,
        options=options,
        task=task,
        rows=rows,
        test_metrics=test_metrics,
    )
    train_config = train_config_snapshot_fn(
        options=options,
        task=task,
        feature_cols=dataset.feature_names,
        markets=dataset.selected_markets,
        label_contract=label_contract,
        selection_recommendations=selection_recommendations,
        selection_policy=selection_policy,
        selection_calibration=selection_calibration,
        research_support_lane=research_support_lane,
        ranker_budget_profile=ranker_budget_profile,
        cpcv_lite_summary=cpcv_lite.get("summary", {}),
        factor_block_selection=factor_block_selection,
        factor_block_selection_context=factor_block_selection_context,
        cpcv_lite_runtime=cpcv_lite_runtime,
        search_budget_decision=search_budget_decision,
        lane_governance=lane_governance,
    )
    return {
        "walk_forward": walk_forward,
        "selection_recommendations": selection_recommendations,
        "selection_policy": selection_policy,
        "selection_calibration": selection_calibration,
        "cpcv_lite": cpcv_lite,
        "research_support_lane": research_support_lane,
        "factor_block_selection": factor_block_selection,
        "metrics": metrics,
        "leaderboard_row": leaderboard_row,
        "train_config": train_config,
    }
