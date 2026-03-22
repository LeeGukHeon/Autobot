"""Training orchestration for trainer=v4_crypto_cs."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import time
from typing import Any, Sequence

import numpy as np

from autobot import __version__ as autobot_version
from autobot.data import expected_interval_ms
from autobot.strategy.model_alpha_v1 import (
    ModelAlphaExecutionSettings,
    ModelAlphaExitSettings,
    ModelAlphaSelectionSettings,
    ModelAlphaSettings,
)

from .dataset_loader import (
    build_data_fingerprint,
    build_dataset_request,
    feature_columns_from_spec,
    load_feature_aux_frame,
    load_feature_dataset,
    load_feature_spec,
    load_label_spec,
)
from .economic_objective import (
    build_v4_shared_economic_objective_profile,
    build_v4_trainer_sweep_sort_key,
    resolve_v4_execution_compare_contract,
)
from .cpcv_lite import (
    build_cpcv_lite_plan,
    summarize_cpcv_lite_dsr,
    summarize_cpcv_lite_fold_selection,
    summarize_cpcv_lite_pbo,
)
from .experiment_ledger import (
    append_experiment_ledger_record,
    build_experiment_ledger_record,
    build_recent_experiment_ledger_summary,
    load_experiment_ledger,
    write_latest_experiment_ledger_summary,
)
from .factor_block_selector import (
    build_factor_block_selection_signature,
    build_factor_block_window_baseline,
    build_factor_block_window_row,
    evaluate_factor_block_window_rows,
    normalize_factor_block_selection_mode,
    normalize_run_scope as normalize_factor_block_run_scope,
    resolve_selected_feature_columns_from_latest,
    v4_factor_block_registry,
)
from .multiple_testing import (
    build_trial_window_differential_diagnostics,
    build_trial_window_differential_matrix,
    run_hansen_spa,
    run_white_reality_check,
)
from .execution_acceptance import run_execution_acceptance
from .runtime_recommendations import optimize_runtime_recommendations, runtime_recommendation_grid_for_profile
from .model_card import render_model_card
from .search_budget import resolve_v4_search_budget
from .research_acceptance import (
    compare_balanced_pareto,
    compare_spa_like_window_test,
    summarize_walk_forward_windows,
)
from .selection_optimizer import (
    SelectionGridConfig,
    build_window_selection_objectives,
)
from .trade_action_policy import build_trade_action_policy_from_oos_rows
from . import train_v4_artifacts as _train_v4_artifacts
from . import train_v4_cpcv as _train_v4_cpcv
from . import train_v4_contract_bundle as _train_v4_contract_bundle
from . import train_v4_core as _train_v4_core
from . import train_v4_execution as _train_v4_execution
from . import train_v4_governance as _train_v4_governance
from . import train_v4_models as _train_v4_models
from . import train_v4_persistence as _train_v4_persistence
from . import train_v4_postprocess as _train_v4_postprocess
from . import train_v4_walkforward as _train_v4_walkforward
from . import train_v4_walkforward_support as _train_v4_walkforward_support
from . import train_v4_walkforward_trials as _train_v4_walkforward_trials
from .registry import (
    RegistrySavePayload,
    load_json,
    make_run_id,
    save_run,
    update_latest_pointer,
    update_latest_candidate_pointer,
)
from .split import (
    SPLIT_DROP,
    SPLIT_TEST,
    SPLIT_TRAIN,
    SPLIT_VALID,
    compute_anchored_walk_forward_splits,
    compute_time_splits,
    split_masks,
)
from .train_v1 import (
    _group_counts_by_ts,
    _build_thresholds,
    _estimate_dataset_memory_mb,
    _evaluate_split,
    _predict_scores,
    _sample_xgb_params,
    _try_import_xgboost,
    _validate_split_counts,
)


@dataclass(frozen=True)
class TrainV4CryptoCsOptions:
    dataset_root: Path
    registry_root: Path
    logs_root: Path
    model_family: str
    tf: str
    quote: str
    top_n: int
    start: str
    end: str
    feature_set: str
    label_set: str
    task: str
    booster_sweep_trials: int
    seed: int
    nthread: int
    batch_rows: int
    train_ratio: float
    valid_ratio: float
    test_ratio: float
    embargo_bars: int
    fee_bps_est: float
    safety_bps: float
    ev_scan_steps: int
    ev_min_selected: int
    min_rows_for_train: int = 5000
    walk_forward_enabled: bool = True
    walk_forward_windows: int = 4
    walk_forward_sweep_trials: int = 3
    walk_forward_min_train_rows: int = 1_000
    walk_forward_min_test_rows: int = 200
    cpcv_lite_enabled: bool = False
    cpcv_lite_group_count: int = 6
    cpcv_lite_test_group_count: int = 2
    cpcv_lite_max_combinations: int = 6
    cpcv_lite_min_train_rows: int = 1_000
    cpcv_lite_min_test_rows: int = 200
    factor_block_selection_mode: str = "guarded_auto"
    selection_threshold_key_override: str | None = None
    multiple_testing_alpha: float = 0.20
    multiple_testing_bootstrap_iters: int = 500
    multiple_testing_block_length: int = 0
    execution_acceptance_enabled: bool = False
    execution_acceptance_dataset_name: str = "candles_v1"
    execution_acceptance_parquet_root: Path = Path("data/parquet")
    execution_acceptance_output_root: Path = Path("data/backtest")
    execution_acceptance_top_n: int = 0
    execution_acceptance_dense_grid: bool = False
    execution_acceptance_starting_krw: float = 50_000.0
    execution_acceptance_per_trade_krw: float = 10_000.0
    execution_acceptance_max_positions: int = 2
    execution_acceptance_min_order_krw: float = 5_000.0
    execution_acceptance_order_timeout_bars: int = 5
    execution_acceptance_reprice_max_attempts: int = 1
    execution_acceptance_reprice_tick_steps: int = 1
    execution_acceptance_rules_ttl_sec: int = 86_400
    run_scope: str = "scheduled_daily"
    execution_acceptance_model_alpha: ModelAlphaSettings = ModelAlphaSettings(
        selection=ModelAlphaSelectionSettings(use_learned_recommendations=False),
        exit=ModelAlphaExitSettings(use_learned_exit_mode=False, use_learned_hold_bars=False),
        execution=ModelAlphaExecutionSettings(use_learned_recommendations=False),
    )
    live_domain_reweighting_enabled: bool = False
    live_domain_reweighting_db_path: Path | None = None
    live_domain_reweighting_min_target_rows: int = 32
    live_domain_reweighting_max_target_rows: int = 1_024
    live_domain_reweighting_clip_min: float = 0.5
    live_domain_reweighting_clip_max: float = 3.0


@dataclass(frozen=True)
class TrainV4CryptoCsResult:
    run_id: str
    run_dir: Path
    status: str
    leaderboard_row: dict[str, Any]
    metrics: dict[str, Any]
    thresholds: dict[str, Any]
    train_report_path: Path
    promotion_path: Path
    walk_forward_report_path: Path | None = None
    cpcv_lite_report_path: Path | None = None
    factor_block_selection_path: Path | None = None
    factor_block_history_path: Path | None = None
    factor_block_policy_path: Path | None = None
    search_budget_decision_path: Path | None = None
    execution_acceptance_report_path: Path | None = None
    runtime_recommendations_path: Path | None = None
    trainer_research_evidence_path: Path | None = None
    economic_objective_profile_path: Path | None = None
    lane_governance_path: Path | None = None
    decision_surface_path: Path | None = None
    experiment_ledger_path: Path | None = None
    experiment_ledger_summary_path: Path | None = None
    live_domain_reweighting_path: Path | None = None


def _resolve_cpcv_lite_runtime_config(
    *,
    options: TrainV4CryptoCsOptions,
    factor_block_selection_context: dict[str, Any],
) -> dict[str, Any]:
    return _train_v4_cpcv.resolve_cpcv_lite_runtime_config(
        options=options,
        factor_block_selection_context=factor_block_selection_context,
    )


def _build_lane_governance_v4(
    *,
    task: str,
    run_scope: str,
    economic_objective_profile: dict[str, Any],
) -> dict[str, Any]:
    return _train_v4_postprocess.build_lane_governance_v4(
        task=task,
        run_scope=run_scope,
        economic_objective_profile=economic_objective_profile,
        normalize_run_scope_fn=normalize_factor_block_run_scope,
    )


def train_and_register_v4_crypto_cs(options: TrainV4CryptoCsOptions) -> TrainV4CryptoCsResult:
    task = str(options.task).strip().lower() or "cls"
    if task not in {"cls", "reg", "rank"}:
        raise ValueError("task currently supports only 'cls', 'reg', or 'rank'")
    if options.feature_set != "v4":
        raise ValueError("trainer v4_crypto_cs requires --feature-set v4")
    if options.label_set != "v2":
        raise ValueError("trainer v4_crypto_cs requires --label-set v2")
    if _try_import_xgboost() is None:
        raise RuntimeError("xgboost is required for trainer=v4_crypto_cs")

    started_at = time.time()
    economic_objective_profile = build_v4_shared_economic_objective_profile()
    lane_governance = _build_lane_governance_v4(
        task=task,
        run_scope=options.run_scope,
        economic_objective_profile=economic_objective_profile,
    )
    run_id = make_run_id(seed=options.seed)
    prepared = _train_v4_core.prepare_v4_training_inputs(
        options=options,
        task=task,
        build_dataset_request_fn=build_dataset_request,
        load_feature_spec_fn=load_feature_spec,
        load_label_spec_fn=load_label_spec,
        feature_columns_from_spec_fn=feature_columns_from_spec,
        resolve_selected_feature_columns_from_latest_fn=resolve_selected_feature_columns_from_latest,
        resolve_v4_search_budget_fn=resolve_v4_search_budget,
        load_feature_dataset_fn=load_feature_dataset,
        load_feature_aux_frame_fn=load_feature_aux_frame,
        expected_interval_ms_fn=expected_interval_ms,
        compute_time_splits_fn=compute_time_splits,
        split_masks_fn=split_masks,
        validate_split_counts_fn=_validate_split_counts,
        resolve_ranker_budget_profile_fn=_resolve_ranker_budget_profile,
        factor_block_registry_fn=v4_factor_block_registry,
        resolve_cpcv_lite_runtime_config_fn=_resolve_cpcv_lite_runtime_config,
    )
    request = prepared["request"]
    feature_spec = prepared["feature_spec"]
    label_spec = prepared["label_spec"]
    factor_block_selection_context = prepared["factor_block_selection_context"]
    search_budget_decision = prepared["search_budget_decision"]
    effective_booster_sweep_trials = int(prepared["effective_booster_sweep_trials"])
    dataset = prepared["dataset"]
    action_aux_arrays = prepared["action_aux_arrays"]
    interval_ms = int(prepared["interval_ms"])
    split_info = prepared["split_info"]
    rows = prepared["rows"]
    ranker_budget_profile = prepared["ranker_budget_profile"]
    factor_block_registry = prepared["factor_block_registry"]
    cpcv_lite_runtime = prepared["cpcv_lite_runtime"]
    live_domain_reweighting = dict(prepared.get("live_domain_reweighting") or {})

    fitted = _train_v4_core.fit_v4_primary_model_bundle(
        options=options,
        task=task,
        prepared=prepared,
        fit_weighted_fn=_fit_booster_sweep_weighted,
        fit_regression_fn=_fit_booster_sweep_regression,
        fit_ranker_fn=_fit_booster_sweep_ranker,
        predict_scores_fn=_predict_scores,
        evaluate_split_fn=_evaluate_split,
        attach_ranking_metrics_fn=_attach_ranking_metrics,
        build_thresholds_fn=_build_thresholds,
    )
    booster = fitted["booster"]
    valid_scores = fitted["valid_scores"]
    valid_metrics = fitted["valid_metrics"]
    test_metrics = fitted["test_metrics"]
    thresholds = fitted["thresholds"]
    contract_bundle = _train_v4_contract_bundle.build_v4_contract_bundle(
        options=options,
        task=task,
        dataset=dataset,
        action_aux_arrays=action_aux_arrays,
        interval_ms=interval_ms,
        thresholds=thresholds,
        factor_block_registry=factor_block_registry,
        effective_booster_sweep_trials=effective_booster_sweep_trials,
        factor_block_selection_context=factor_block_selection_context,
        cpcv_lite_runtime=cpcv_lite_runtime,
        search_budget_decision=search_budget_decision,
        ranker_budget_profile=ranker_budget_profile,
        run_id=run_id,
        split_info=split_info,
        rows=rows,
        valid_scores=valid_scores,
        valid_ts_ms=fitted["valid_ts_ms"],
        valid_metrics=valid_metrics,
        test_metrics=test_metrics,
        booster=booster,
        economic_objective_profile=economic_objective_profile,
        lane_governance=lane_governance,
        run_walk_forward_fn=_run_walk_forward_v4,
        finalize_walk_forward_report_fn=_finalize_walk_forward_report,
        run_cpcv_lite_fn=_run_cpcv_lite_v4,
        build_research_support_lane_fn=_build_research_support_lane_v4,
        build_v4_metrics_doc_fn=_build_v4_metrics_doc,
        make_v4_leaderboard_row_fn=_make_v4_leaderboard_row,
        train_config_snapshot_fn=_train_config_snapshot_v4,
    )
    walk_forward = contract_bundle["walk_forward"]
    selection_recommendations = contract_bundle["selection_recommendations"]
    selection_policy = contract_bundle["selection_policy"]
    selection_calibration = contract_bundle["selection_calibration"]
    cpcv_lite = contract_bundle["cpcv_lite"]
    research_support_lane = contract_bundle["research_support_lane"]
    factor_block_selection = contract_bundle["factor_block_selection"]
    metrics = contract_bundle["metrics"]
    leaderboard_row = contract_bundle["leaderboard_row"]

    data_fingerprint = build_data_fingerprint(
        request=request,
        selected_markets=dataset.selected_markets,
        total_rows=dataset.rows,
    )
    data_fingerprint["code_version"] = autobot_version
    data_fingerprint["sample_weight"] = {
        "enabled": True,
        "min": float(np.nanmin(dataset.sample_weight)) if dataset.sample_weight.size > 0 else 0.0,
        "max": float(np.nanmax(dataset.sample_weight)) if dataset.sample_weight.size > 0 else 0.0,
        "mean": float(np.nanmean(dataset.sample_weight)) if dataset.sample_weight.size > 0 else 0.0,
    }
    if live_domain_reweighting:
        data_fingerprint["live_domain_reweighting"] = live_domain_reweighting

    model_card = render_model_card(
        run_id=run_id,
        model_family=options.model_family,
        champion="booster",
        metrics=metrics,
        thresholds=thresholds,
        data_fingerprint=data_fingerprint,
    )
    train_config = contract_bundle["train_config"]

    run_dir = save_run(
        RegistrySavePayload(
            registry_root=options.registry_root,
            model_family=options.model_family,
            run_id=run_id,
            model_bundle=booster["bundle"],
            metrics=metrics,
            thresholds=thresholds,
            feature_spec=feature_spec,
            label_spec=label_spec,
            train_config=train_config,
            data_fingerprint=data_fingerprint,
            leaderboard_row=leaderboard_row,
            model_card_text=model_card,
            selection_recommendations=selection_recommendations,
            selection_policy=selection_policy,
            selection_calibration=selection_calibration,
        ),
        publish_pointers=False,
    )
    support_artifacts = _train_v4_persistence.persist_v4_support_artifacts(
        run_dir=run_dir,
        options=options,
        run_id=run_id,
        factor_block_registry=factor_block_registry,
        walk_forward=walk_forward,
        cpcv_lite=cpcv_lite,
        factor_block_selection=factor_block_selection,
        search_budget_decision=search_budget_decision,
    )
    walk_forward_report_path = support_artifacts["walk_forward_report_path"]
    cpcv_lite_report_path = support_artifacts["cpcv_lite_report_path"]
    factor_block_selection_path = support_artifacts["factor_block_selection_path"]
    factor_block_history_path = support_artifacts["factor_block_history_path"]
    factor_block_policy_path = support_artifacts["factor_block_policy_path"]
    search_budget_decision_path = support_artifacts["search_budget_decision_path"]
    factor_block_selection = support_artifacts["factor_block_selection"]
    factor_block_policy = support_artifacts["factor_block_policy"]
    live_domain_reweighting_path: Path | None = None
    if live_domain_reweighting:
        live_domain_reweighting_path = _train_v4_persistence._write_json(
            run_dir / "live_domain_reweighting.json",
            live_domain_reweighting,
        )
    duplicate_artifacts = _detect_duplicate_candidate_artifacts(
        options=options,
        run_id=run_id,
        run_dir=run_dir,
    )
    trade_action_oos_rows = walk_forward.pop("_trade_action_oos_rows", [])
    duplicate_candidate = bool(duplicate_artifacts.get("duplicate", False))
    if duplicate_candidate:
        execution_acceptance = _build_duplicate_candidate_execution_acceptance(
            run_id=run_id,
            duplicate_artifacts=duplicate_artifacts,
        )
    else:
        execution_acceptance = _run_execution_acceptance_v4(
            options=options,
            run_id=run_id,
        )
    if duplicate_candidate:
        runtime_recommendations = _build_duplicate_candidate_runtime_recommendations(
            run_id=run_id,
            duplicate_artifacts=duplicate_artifacts,
        )
    else:
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
    execution_artifact_cleanup = _purge_execution_artifact_run_dirs(
        output_root=options.execution_acceptance_output_root,
        execution_acceptance=execution_acceptance,
        runtime_recommendations=runtime_recommendations,
    )
    if execution_artifact_cleanup.get("evaluated"):
        execution_acceptance["artifacts_cleanup"] = execution_artifact_cleanup
        runtime_recommendations["artifacts_cleanup"] = execution_artifact_cleanup
    if duplicate_candidate:
        promotion = _build_duplicate_candidate_promotion_decision_v4(
            options=options,
            run_id=run_id,
            walk_forward=walk_forward,
            execution_acceptance=execution_acceptance,
            duplicate_artifacts=duplicate_artifacts,
            runtime_recommendations=runtime_recommendations,
        )
    else:
        promotion = _manual_promotion_decision_v4(
            options=options,
            run_id=run_id,
            walk_forward=walk_forward,
            execution_acceptance=execution_acceptance,
            runtime_recommendations=runtime_recommendations,
        )
    promotion_path = run_dir / "promotion_decision.json"
    promotion_path.write_text(
        json.dumps(promotion, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    trainer_research_evidence = _build_trainer_research_evidence_from_promotion_v4(
        promotion=promotion,
        support_lane=research_support_lane,
    )
    decision_surface = _build_decision_surface_v4(
        options=options,
        task=task,
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
    runtime_artifacts = _train_v4_persistence.persist_v4_runtime_and_governance_artifacts(
        run_dir=run_dir,
        execution_acceptance=execution_acceptance,
        runtime_recommendations=runtime_recommendations,
        promotion=promotion,
        trainer_research_evidence=trainer_research_evidence,
        economic_objective_profile=economic_objective_profile,
        lane_governance=lane_governance,
        decision_surface=decision_surface,
    )
    execution_acceptance_report_path = runtime_artifacts["execution_acceptance_report_path"]
    runtime_recommendations_path = runtime_artifacts["runtime_recommendations_path"]
    promotion_path = runtime_artifacts["promotion_path"]
    trainer_research_evidence_path = runtime_artifacts["trainer_research_evidence_path"]
    economic_objective_profile_path = runtime_artifacts["economic_objective_profile_path"]
    lane_governance_path = runtime_artifacts["lane_governance_path"]
    decision_surface_path = runtime_artifacts["decision_surface_path"]
    if normalize_factor_block_run_scope(options.run_scope) == "scheduled_daily":
        update_latest_pointer(options.registry_root, options.model_family, run_id)
        update_latest_pointer(
            options.registry_root,
            "_global",
            run_id,
            family=options.model_family,
        )
    status = str(promotion.get("status", "candidate")).strip() or "candidate"

    finished_at = time.time()
    duration_sec = round(finished_at - started_at, 3)
    experiment_ledger_record = build_experiment_ledger_record(
        run_id=run_id,
        task=task,
        status=status,
        duration_sec=duration_sec,
        run_dir=run_dir,
        search_budget_decision=search_budget_decision,
        walk_forward=walk_forward,
        cpcv_lite=cpcv_lite,
        factor_block_selection=factor_block_selection,
        factor_block_policy=factor_block_policy,
        factor_block_selection_context=factor_block_selection_context,
        execution_acceptance=execution_acceptance,
        runtime_recommendations=runtime_recommendations,
        promotion=promotion,
        duplicate_candidate=duplicate_candidate,
        run_scope=options.run_scope,
        economic_objective_profile=economic_objective_profile,
        lane_governance=lane_governance,
    )
    experiment_ledger_path = append_experiment_ledger_record(
        registry_root=options.registry_root,
        model_family=options.model_family,
        record=experiment_ledger_record,
        run_scope=options.run_scope,
    )
    experiment_ledger_history = load_experiment_ledger(
        registry_root=options.registry_root,
        model_family=options.model_family,
        run_scope=options.run_scope,
    )
    experiment_ledger_summary = build_recent_experiment_ledger_summary(
        history_records=experiment_ledger_history,
    )
    experiment_ledger_summary_path = write_latest_experiment_ledger_summary(
        registry_root=options.registry_root,
        model_family=options.model_family,
        run_id=run_id,
        summary=experiment_ledger_summary,
        run_scope=options.run_scope,
    )
    report_path = _write_train_report_v4(
        options.logs_root,
        options.run_scope,
        {
            "run_id": run_id,
            "status": status,
            "task": task,
            "run_scope": normalize_factor_block_run_scope(options.run_scope),
            "started_at_utc": datetime.fromtimestamp(started_at, tz=timezone.utc).isoformat(),
            "finished_at_utc": datetime.fromtimestamp(finished_at, tz=timezone.utc).isoformat(),
            "duration_sec": duration_sec,
            "rows": rows,
            "memory_estimate_mb": _estimate_dataset_memory_mb(dataset),
            "search_budget_decision": search_budget_decision,
            "ranker_budget_profile": ranker_budget_profile,
            "sweep_trials": booster.get("trials", []),
            "candidate": leaderboard_row,
            "live_domain_reweighting": live_domain_reweighting,
            "walk_forward": walk_forward,
            "cpcv_lite": cpcv_lite,
            "factor_block_selection": factor_block_selection,
            "factor_block_policy": factor_block_policy,
            "execution_acceptance": execution_acceptance,
            "runtime_recommendations": runtime_recommendations,
            "selection_recommendations": selection_recommendations,
            "promotion": promotion,
            "economic_objective_profile": economic_objective_profile,
            "experiment_ledger_record": experiment_ledger_record,
            "experiment_ledger_summary": experiment_ledger_summary,
        },
    )

    return TrainV4CryptoCsResult(
        run_id=run_id,
        run_dir=run_dir,
        status=status,
        leaderboard_row=leaderboard_row,
        metrics=metrics,
        thresholds=thresholds,
        train_report_path=report_path,
        promotion_path=promotion_path,
        walk_forward_report_path=walk_forward_report_path,
        cpcv_lite_report_path=cpcv_lite_report_path,
        factor_block_selection_path=factor_block_selection_path,
        factor_block_history_path=factor_block_history_path,
        factor_block_policy_path=factor_block_policy_path,
        search_budget_decision_path=search_budget_decision_path,
        execution_acceptance_report_path=execution_acceptance_report_path,
        runtime_recommendations_path=runtime_recommendations_path,
        trainer_research_evidence_path=trainer_research_evidence_path,
        economic_objective_profile_path=economic_objective_profile_path,
        lane_governance_path=lane_governance_path,
        decision_surface_path=decision_surface_path,
        experiment_ledger_path=experiment_ledger_path,
        experiment_ledger_summary_path=experiment_ledger_summary_path,
        live_domain_reweighting_path=live_domain_reweighting_path,
    )


def _run_walk_forward_v4(
    *,
    options: TrainV4CryptoCsOptions,
    task: str,
    dataset: Any,
    action_aux_arrays: dict[str, np.ndarray],
    interval_ms: int,
    thresholds: dict[str, Any],
    feature_names: Sequence[str],
    factor_block_registry: Sequence[Any],
    effective_booster_sweep_trials: int,
) -> dict[str, Any]:
    return _train_v4_walkforward.run_walk_forward_v4(
        options=options,
        task=task,
        dataset=dataset,
        action_aux_arrays=action_aux_arrays,
        interval_ms=interval_ms,
        thresholds=thresholds,
        feature_names=feature_names,
        factor_block_registry=factor_block_registry,
        effective_booster_sweep_trials=effective_booster_sweep_trials,
        summarize_walk_forward_windows_fn=summarize_walk_forward_windows,
        compare_balanced_pareto_fn=compare_balanced_pareto,
        compute_anchored_walk_forward_splits_fn=compute_anchored_walk_forward_splits,
        split_masks_fn=split_masks,
        resolve_ranker_budget_profile_fn=_resolve_ranker_budget_profile,
        fit_walk_forward_window_model_fn=_fit_walk_forward_window_model,
        predict_scores_fn=_predict_scores,
        evaluate_split_fn=_evaluate_split,
        attach_ranking_metrics_fn=_attach_ranking_metrics,
        build_oos_period_metrics_fn=_build_oos_period_metrics,
        build_oos_slice_metrics_fn=_build_oos_slice_metrics,
        compact_eval_metrics_fn=_compact_eval_metrics,
        build_window_selection_objectives_fn=build_window_selection_objectives,
        selection_grid_config_factory=SelectionGridConfig,
        evaluate_factor_block_window_rows_fn=evaluate_factor_block_window_rows,
        evaluate_factor_block_refit_window_evidence_fn=_evaluate_factor_block_refit_window_evidence,
        summarize_walk_forward_trial_panel_fn=_summarize_walk_forward_trial_panel,
        split_train_label=SPLIT_TRAIN,
        split_valid_label=SPLIT_VALID,
        split_test_label=SPLIT_TEST,
    )


def _run_cpcv_lite_v4(
    *,
    options: TrainV4CryptoCsOptions,
    task: str,
    dataset: Any,
    interval_ms: int,
    thresholds: dict[str, Any],
    best_params: dict[str, Any],
    enabled: bool,
    trigger: str,
) -> dict[str, Any]:
    return _train_v4_cpcv.run_cpcv_lite_v4(
        options=options,
        task=task,
        dataset=dataset,
        interval_ms=interval_ms,
        thresholds=thresholds,
        best_params=best_params,
        enabled=enabled,
        trigger=trigger,
        build_cpcv_lite_plan_fn=build_cpcv_lite_plan,
        build_inner_train_valid_masks_fn=_build_cpcv_inner_train_valid_masks,
        fit_cpcv_lite_fold_model_fn=_fit_cpcv_lite_fold_model,
        predict_scores_fn=_predict_scores,
        evaluate_split_fn=_evaluate_split,
        attach_ranking_metrics_fn=_attach_ranking_metrics,
        build_window_selection_objectives_fn=build_window_selection_objectives,
        selection_grid_config_factory=SelectionGridConfig,
        summarize_cpcv_lite_fold_selection_fn=summarize_cpcv_lite_fold_selection,
        summarize_cpcv_lite_pbo_fn=summarize_cpcv_lite_pbo,
        summarize_cpcv_lite_dsr_fn=summarize_cpcv_lite_dsr,
        split_train_label=SPLIT_TRAIN,
        split_test_label=SPLIT_TEST,
        split_drop_label=SPLIT_DROP,
    )


def _build_cpcv_inner_train_valid_masks(
    *,
    ts_ms: np.ndarray,
    valid_ratio: float,
    embargo_bars: int,
    interval_ms: int,
) -> dict[str, Any] | None:
    return _train_v4_cpcv.build_cpcv_inner_train_valid_masks(
        ts_ms=ts_ms,
        valid_ratio=valid_ratio,
        embargo_bars=embargo_bars,
        interval_ms=interval_ms,
    )


def _fit_cpcv_lite_fold_model(
    *,
    task: str,
    options: TrainV4CryptoCsOptions,
    best_params: dict[str, Any],
    x_train: np.ndarray,
    y_cls_train: np.ndarray,
    y_reg_train: np.ndarray,
    y_rank_train: np.ndarray,
    w_train: np.ndarray,
    ts_train_ms: np.ndarray,
    x_valid: np.ndarray,
    y_valid_cls: np.ndarray,
    y_valid_reg: np.ndarray,
    y_valid_rank: np.ndarray,
    w_valid: np.ndarray,
    ts_valid_ms: np.ndarray,
    fold_index: int,
) -> dict[str, Any]:
    return _train_v4_cpcv.fit_cpcv_lite_fold_model(
        task=task,
        options=options,
        best_params=best_params,
        x_train=x_train,
        y_cls_train=y_cls_train,
        y_reg_train=y_reg_train,
        y_rank_train=y_rank_train,
        w_train=w_train,
        ts_train_ms=ts_train_ms,
        x_valid=x_valid,
        y_valid_cls=y_valid_cls,
        y_valid_reg=y_valid_reg,
        y_valid_rank=y_valid_rank,
        w_valid=w_valid,
        ts_valid_ms=ts_valid_ms,
        fold_index=fold_index,
        fit_fixed_classifier_fn=_fit_fixed_classifier_model,
        fit_fixed_regression_fn=_fit_fixed_regression_model,
        fit_fixed_ranker_fn=_fit_fixed_ranker_model,
    )


def _fit_fixed_classifier_model(
    *,
    options: TrainV4CryptoCsOptions,
    best_params: dict[str, Any],
    x_train: np.ndarray,
    y_train: np.ndarray,
    w_train: np.ndarray,
    x_valid: np.ndarray,
    y_valid: np.ndarray,
    w_valid: np.ndarray,
    fold_index: int,
) -> dict[str, Any]:
    return _train_v4_models.fit_fixed_classifier_model(
        options=options,
        best_params=best_params,
        x_train=x_train,
        y_train=y_train,
        w_train=w_train,
        x_valid=x_valid,
        y_valid=y_valid,
        w_valid=w_valid,
        fold_index=fold_index,
        try_import_xgboost_fn=_try_import_xgboost,
        normalize_xgb_best_params_fn=_normalize_xgb_best_params,
    )


def _fit_fixed_regression_model(
    *,
    options: TrainV4CryptoCsOptions,
    best_params: dict[str, Any],
    x_train: np.ndarray,
    y_train: np.ndarray,
    w_train: np.ndarray,
    x_valid: np.ndarray,
    y_valid: np.ndarray,
    w_valid: np.ndarray,
    fold_index: int,
) -> dict[str, Any]:
    return _train_v4_models.fit_fixed_regression_model(
        options=options,
        best_params=best_params,
        x_train=x_train,
        y_train=y_train,
        w_train=w_train,
        x_valid=x_valid,
        y_valid=y_valid,
        w_valid=w_valid,
        fold_index=fold_index,
        try_import_xgboost_fn=_try_import_xgboost,
        normalize_xgb_best_params_fn=_normalize_xgb_best_params,
    )


def _fit_fixed_ranker_model(
    *,
    options: TrainV4CryptoCsOptions,
    best_params: dict[str, Any],
    x_train: np.ndarray,
    y_train: np.ndarray,
    ts_train_ms: np.ndarray,
    w_train: np.ndarray,
    x_valid: np.ndarray,
    y_valid: np.ndarray,
    ts_valid_ms: np.ndarray,
    fold_index: int,
) -> dict[str, Any]:
    return _train_v4_models.fit_fixed_ranker_model(
        options=options,
        best_params=best_params,
        x_train=x_train,
        y_train=y_train,
        ts_train_ms=ts_train_ms,
        w_train=w_train,
        x_valid=x_valid,
        y_valid=y_valid,
        ts_valid_ms=ts_valid_ms,
        fold_index=fold_index,
        try_import_xgboost_fn=_try_import_xgboost,
        normalize_xgb_best_params_fn=_normalize_xgb_best_params,
        group_counts_by_ts_fn=_group_counts_by_ts,
    )


def _normalize_xgb_best_params(best_params: dict[str, Any] | None) -> dict[str, Any]:
    return _train_v4_models.normalize_xgb_best_params(best_params)


def _fit_walk_forward_window_model(
    *,
    task: str,
    options: TrainV4CryptoCsOptions,
    sweep_trials: int,
    x_train: np.ndarray,
    y_cls_train: np.ndarray,
    y_reg_train: np.ndarray,
    y_rank_train: np.ndarray,
    w_train: np.ndarray,
    ts_train_ms: np.ndarray,
    x_valid: np.ndarray,
    y_valid_cls: np.ndarray,
    y_valid_reg: np.ndarray,
    y_valid_rank: np.ndarray,
    w_valid: np.ndarray,
    ts_valid_ms: np.ndarray,
    x_test: np.ndarray,
    y_test_cls: np.ndarray,
    y_test_reg: np.ndarray,
    y_test_rank: np.ndarray,
    market_test: np.ndarray,
    ts_test_ms: np.ndarray,
) -> dict[str, Any]:
    return _train_v4_walkforward.fit_walk_forward_window_model(
        task=task,
        options=options,
        sweep_trials=sweep_trials,
        x_train=x_train,
        y_train=y_cls_train if task == "cls" else y_reg_train,
        y_train_rank=y_rank_train,
        w_train=w_train,
        x_valid=x_valid,
        y_valid_cls=y_valid_cls,
        y_valid_reg=y_valid_reg,
        y_valid_rank=y_valid_rank,
        w_valid=w_valid,
        x_test=x_test,
        y_test_cls=y_test_cls,
        y_test_reg=y_test_reg,
        y_test_rank=y_test_rank,
        market_test=market_test,
        ts_test_ms=ts_test_ms,
        ts_train_ms=ts_train_ms,
        ts_valid_ms=ts_valid_ms,
        fit_walk_forward_weighted_trials_fn=_fit_walk_forward_weighted_trials,
        fit_walk_forward_regression_trials_fn=_fit_walk_forward_regression_trials,
        fit_walk_forward_ranker_trials_fn=_fit_walk_forward_ranker_trials,
    )


def _evaluate_factor_block_refit_window_evidence(
    *,
    window_index: int,
    task: str,
    options: TrainV4CryptoCsOptions,
    best_params: dict[str, Any] | None,
    full_bundle: dict[str, Any],
    feature_names: Sequence[str],
    x_train: np.ndarray,
    y_cls_train: np.ndarray,
    y_reg_train: np.ndarray,
    y_rank_train: np.ndarray,
    w_train: np.ndarray,
    ts_train_ms: np.ndarray,
    x_valid: np.ndarray,
    y_valid_cls: np.ndarray,
    y_valid_reg: np.ndarray,
    y_valid_rank: np.ndarray,
    w_valid: np.ndarray,
    ts_valid_ms: np.ndarray,
    x_test: np.ndarray,
    y_test_cls: np.ndarray,
    y_test_reg: np.ndarray,
    y_test_rank: np.ndarray,
    ts_test_ms: np.ndarray,
    thresholds: dict[str, Any],
    block_registry: Sequence[Any],
) -> dict[str, Any]:
    return _train_v4_walkforward_support.evaluate_factor_block_refit_window_evidence(
        window_index=window_index,
        task=task,
        options=options,
        best_params=best_params,
        full_bundle=full_bundle,
        feature_names=feature_names,
        x_train=x_train,
        y_cls_train=y_cls_train,
        y_reg_train=y_reg_train,
        y_rank_train=y_rank_train,
        w_train=w_train,
        ts_train_ms=ts_train_ms,
        x_valid=x_valid,
        y_valid_cls=y_valid_cls,
        y_valid_reg=y_valid_reg,
        y_valid_rank=y_valid_rank,
        w_valid=w_valid,
        ts_valid_ms=ts_valid_ms,
        x_test=x_test,
        y_test_cls=y_test_cls,
        y_test_reg=y_test_reg,
        y_test_rank=y_test_rank,
        ts_test_ms=ts_test_ms,
        thresholds=thresholds,
        block_registry=block_registry,
        predict_scores_fn=_predict_scores,
        evaluate_split_fn=_evaluate_split,
        attach_ranking_metrics_fn=_attach_ranking_metrics,
        fit_fixed_classifier_fn=_fit_fixed_classifier_model,
        fit_fixed_regression_fn=_fit_fixed_regression_model,
        fit_fixed_ranker_fn=_fit_fixed_ranker_model,
        build_factor_block_window_baseline_fn=build_factor_block_window_baseline,
        build_factor_block_selection_signature_fn=build_factor_block_selection_signature,
        build_factor_block_window_row_fn=build_factor_block_window_row,
    )


def _evaluate_factor_block_refit_window_rows(
    *,
    window_index: int,
    task: str,
    options: TrainV4CryptoCsOptions,
    best_params: dict[str, Any] | None,
    full_bundle: dict[str, Any],
    feature_names: Sequence[str],
    x_train: np.ndarray,
    y_cls_train: np.ndarray,
    y_reg_train: np.ndarray,
    y_rank_train: np.ndarray,
    w_train: np.ndarray,
    ts_train_ms: np.ndarray,
    x_valid: np.ndarray,
    y_valid_cls: np.ndarray,
    y_valid_reg: np.ndarray,
    y_valid_rank: np.ndarray,
    w_valid: np.ndarray,
    ts_valid_ms: np.ndarray,
    x_test: np.ndarray,
    y_test_cls: np.ndarray,
    y_test_reg: np.ndarray,
    y_test_rank: np.ndarray,
    ts_test_ms: np.ndarray,
    thresholds: dict[str, Any],
    block_registry: Sequence[Any],
) -> list[dict[str, Any]]:
    return _train_v4_walkforward_support.evaluate_factor_block_refit_window_rows(
        evaluate_factor_block_refit_window_evidence_fn=_evaluate_factor_block_refit_window_evidence,
        window_index=window_index,
        task=task,
        options=options,
        best_params=best_params,
        full_bundle=full_bundle,
        feature_names=feature_names,
        x_train=x_train,
        y_cls_train=y_cls_train,
        y_reg_train=y_reg_train,
        y_rank_train=y_rank_train,
        w_train=w_train,
        ts_train_ms=ts_train_ms,
        x_valid=x_valid,
        y_valid_cls=y_valid_cls,
        y_valid_reg=y_valid_reg,
        y_valid_rank=y_valid_rank,
        w_valid=w_valid,
        ts_valid_ms=ts_valid_ms,
        x_test=x_test,
        y_test_cls=y_test_cls,
        y_test_reg=y_test_reg,
        y_test_rank=y_test_rank,
        ts_test_ms=ts_test_ms,
        thresholds=thresholds,
        block_registry=block_registry,
    )


def _summarize_walk_forward_trial_panel(
    windows: list[dict[str, Any]],
    *,
    threshold_key: str = "top_5pct",
) -> list[dict[str, Any]]:
    return _train_v4_walkforward_support.summarize_walk_forward_trial_panel(
        windows,
        threshold_key=threshold_key,
        summarize_walk_forward_windows_fn=summarize_walk_forward_windows,
    )


def _fit_walk_forward_weighted_trials(
    *,
    options: TrainV4CryptoCsOptions,
    sweep_trials: int,
    x_train: np.ndarray,
    y_train: np.ndarray,
    w_train: np.ndarray,
    x_valid: np.ndarray,
    y_valid_cls: np.ndarray,
    y_valid_reg: np.ndarray,
    w_valid: np.ndarray,
    x_test: np.ndarray,
    y_test_cls: np.ndarray,
    y_test_reg: np.ndarray,
    market_test: np.ndarray,
    ts_test_ms: np.ndarray,
) -> dict[str, Any]:
    return _train_v4_walkforward_trials.fit_walk_forward_weighted_trials(
        options=options,
        sweep_trials=sweep_trials,
        x_train=x_train,
        y_train=y_train,
        w_train=w_train,
        x_valid=x_valid,
        y_valid_cls=y_valid_cls,
        y_valid_reg=y_valid_reg,
        w_valid=w_valid,
        x_test=x_test,
        y_test_cls=y_test_cls,
        y_test_reg=y_test_reg,
        market_test=market_test,
        ts_test_ms=ts_test_ms,
        try_import_xgboost_fn=_try_import_xgboost,
        sample_xgb_params_fn=_sample_xgb_params,
        evaluate_split_fn=_evaluate_split,
        build_v4_trainer_sweep_sort_key_fn=build_v4_trainer_sweep_sort_key,
        compact_eval_metrics_fn=_compact_eval_metrics,
        build_oos_period_metrics_fn=_build_oos_period_metrics,
        build_oos_slice_metrics_fn=_build_oos_slice_metrics,
        build_trial_selection_key_fn=_build_trial_selection_key,
        build_v4_shared_economic_objective_profile_fn=build_v4_shared_economic_objective_profile,
    )


def _fit_walk_forward_regression_trials(
    *,
    options: TrainV4CryptoCsOptions,
    sweep_trials: int,
    x_train: np.ndarray,
    y_train: np.ndarray,
    w_train: np.ndarray,
    x_valid: np.ndarray,
    y_valid_cls: np.ndarray,
    y_valid_reg: np.ndarray,
    w_valid: np.ndarray,
    x_test: np.ndarray,
    y_test_cls: np.ndarray,
    y_test_reg: np.ndarray,
    market_test: np.ndarray,
    ts_test_ms: np.ndarray,
) -> dict[str, Any]:
    return _train_v4_walkforward_trials.fit_walk_forward_regression_trials(
        options=options,
        sweep_trials=sweep_trials,
        x_train=x_train,
        y_train=y_train,
        w_train=w_train,
        x_valid=x_valid,
        y_valid_cls=y_valid_cls,
        y_valid_reg=y_valid_reg,
        w_valid=w_valid,
        x_test=x_test,
        y_test_cls=y_test_cls,
        y_test_reg=y_test_reg,
        market_test=market_test,
        ts_test_ms=ts_test_ms,
        try_import_xgboost_fn=_try_import_xgboost,
        sample_xgb_params_fn=_sample_xgb_params,
        evaluate_split_fn=_evaluate_split,
        build_v4_trainer_sweep_sort_key_fn=build_v4_trainer_sweep_sort_key,
        compact_eval_metrics_fn=_compact_eval_metrics,
        build_oos_period_metrics_fn=_build_oos_period_metrics,
        build_oos_slice_metrics_fn=_build_oos_slice_metrics,
        build_trial_selection_key_fn=_build_trial_selection_key,
        build_v4_shared_economic_objective_profile_fn=build_v4_shared_economic_objective_profile,
    )


def _fit_walk_forward_ranker_trials(
    *,
    options: TrainV4CryptoCsOptions,
    sweep_trials: int,
    x_train: np.ndarray,
    y_train_rank: np.ndarray,
    ts_train_ms: np.ndarray,
    w_train: np.ndarray,
    x_valid: np.ndarray,
    y_valid_cls: np.ndarray,
    y_valid_reg: np.ndarray,
    y_valid_rank: np.ndarray,
    ts_valid_ms: np.ndarray,
    w_valid: np.ndarray,
    x_test: np.ndarray,
    y_test_cls: np.ndarray,
    y_test_reg: np.ndarray,
    y_test_rank: np.ndarray,
    market_test: np.ndarray,
    ts_test_ms: np.ndarray,
) -> dict[str, Any]:
    return _train_v4_walkforward_trials.fit_walk_forward_ranker_trials(
        options=options,
        sweep_trials=sweep_trials,
        x_train=x_train,
        y_train_rank=y_train_rank,
        ts_train_ms=ts_train_ms,
        w_train=w_train,
        x_valid=x_valid,
        y_valid_cls=y_valid_cls,
        y_valid_reg=y_valid_reg,
        y_valid_rank=y_valid_rank,
        ts_valid_ms=ts_valid_ms,
        w_valid=w_valid,
        x_test=x_test,
        y_test_cls=y_test_cls,
        y_test_reg=y_test_reg,
        y_test_rank=y_test_rank,
        market_test=market_test,
        ts_test_ms=ts_test_ms,
        try_import_xgboost_fn=_try_import_xgboost,
        sample_xgb_params_fn=_sample_xgb_params,
        evaluate_split_fn=_evaluate_split,
        attach_ranking_metrics_fn=_attach_ranking_metrics,
        build_v4_trainer_sweep_sort_key_fn=build_v4_trainer_sweep_sort_key,
        compact_eval_metrics_fn=_compact_eval_metrics,
        build_oos_period_metrics_fn=_build_oos_period_metrics,
        build_oos_slice_metrics_fn=_build_oos_slice_metrics,
        build_trial_selection_key_fn=_build_trial_selection_key,
        group_counts_by_ts_fn=_group_counts_by_ts,
        build_v4_shared_economic_objective_profile_fn=build_v4_shared_economic_objective_profile,
    )


def _compact_eval_metrics(metrics: dict[str, Any]) -> dict[str, Any]:
    return _train_v4_walkforward_trials.compact_eval_metrics(metrics)


def _build_oos_slice_metrics(
    *,
    ts_ms: np.ndarray,
    y_cls: np.ndarray,
    y_reg: np.ndarray,
    scores: np.ndarray,
    markets: np.ndarray,
    fee_bps_est: float,
    safety_bps: float,
    max_slices: int = 8,
) -> list[dict[str, Any]]:
    return _train_v4_walkforward_trials.build_oos_slice_metrics(
        ts_ms=ts_ms,
        y_cls=y_cls,
        y_reg=y_reg,
        scores=scores,
        markets=markets,
        fee_bps_est=fee_bps_est,
        safety_bps=safety_bps,
        evaluate_split_fn=_evaluate_split,
        compact_eval_metrics_fn=_compact_eval_metrics,
        max_slices=max_slices,
    )


def _build_oos_period_metrics(
    *,
    ts_ms: np.ndarray,
    y_cls: np.ndarray,
    y_reg: np.ndarray,
    scores: np.ndarray,
    markets: np.ndarray,
    fee_bps_est: float,
    safety_bps: float,
) -> list[dict[str, Any]]:
    return _train_v4_walkforward_trials.build_oos_period_metrics(
        ts_ms=ts_ms,
        y_cls=y_cls,
        y_reg=y_reg,
        scores=scores,
        markets=markets,
        fee_bps_est=fee_bps_est,
        safety_bps=safety_bps,
        evaluate_split_fn=_evaluate_split,
        compact_eval_metrics_fn=_compact_eval_metrics,
    )


def _load_champion_walk_forward_report(*, options: TrainV4CryptoCsOptions) -> dict[str, Any] | None:
    return _train_v4_postprocess.load_champion_walk_forward_report(
        options=options,
        load_json_fn=load_json,
    )


def _sha256_file(path: Path) -> str:
    return _train_v4_postprocess.sha256_file(path)


def _detect_duplicate_candidate_artifacts(
    *,
    options: TrainV4CryptoCsOptions,
    run_id: str,
    run_dir: Path,
) -> dict[str, Any]:
    return _train_v4_postprocess.detect_duplicate_candidate_artifacts(
        options=options,
        run_id=run_id,
        run_dir=run_dir,
        load_json_fn=load_json,
    )


def _finalize_walk_forward_report(
    *,
    walk_forward: dict[str, Any],
    selection_recommendations: dict[str, Any],
    options: TrainV4CryptoCsOptions,
) -> dict[str, Any]:
    return _train_v4_postprocess.finalize_walk_forward_report(
        walk_forward=walk_forward,
        selection_recommendations=selection_recommendations,
        options=options,
        summarize_walk_forward_trial_panel_fn=_summarize_walk_forward_trial_panel,
        build_selection_search_trial_panel_fn=_build_selection_search_trial_panel,
        summarize_walk_forward_windows_fn=summarize_walk_forward_windows,
        load_champion_walk_forward_report_fn=_load_champion_walk_forward_report,
        resolve_walk_forward_report_threshold_key_fn=_resolve_walk_forward_report_threshold_key,
        compare_balanced_pareto_fn=compare_balanced_pareto,
        compare_spa_like_window_test_fn=compare_spa_like_window_test,
        build_trial_window_differential_diagnostics_fn=build_trial_window_differential_diagnostics,
        build_trial_window_differential_matrix_fn=build_trial_window_differential_matrix,
        run_white_reality_check_fn=run_white_reality_check,
        run_hansen_spa_fn=run_hansen_spa,
    )


def _resolve_selection_recommendation_threshold_key(
    *,
    selection_recommendations: dict[str, Any],
) -> tuple[str, str]:
    return _train_v4_postprocess.resolve_selection_recommendation_threshold_key(
        selection_recommendations=selection_recommendations,
    )


def _build_duplicate_candidate_execution_acceptance(
    *,
    run_id: str,
    duplicate_artifacts: dict[str, Any],
) -> dict[str, Any]:
    return _train_v4_execution.build_duplicate_candidate_execution_acceptance(
        run_id=run_id,
        duplicate_artifacts=duplicate_artifacts,
    )


def _build_duplicate_candidate_runtime_recommendations(
    *,
    run_id: str,
    duplicate_artifacts: dict[str, Any],
) -> dict[str, Any]:
    return _train_v4_execution.build_duplicate_candidate_runtime_recommendations(
        run_id=run_id,
        duplicate_artifacts=duplicate_artifacts,
        utc_now_fn=_utc_now,
    )


def _build_trade_action_policy_v4(
    *,
    options: TrainV4CryptoCsOptions,
    runtime_recommendations: dict[str, Any],
    selection_calibration: dict[str, Any],
    oos_rows: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    return _train_v4_execution.build_trade_action_policy_v4(
        options=options,
        runtime_recommendations=runtime_recommendations,
        selection_calibration=selection_calibration,
        oos_rows=oos_rows,
        build_trade_action_policy_from_oos_rows_fn=build_trade_action_policy_from_oos_rows,
    )


def _build_execution_risk_control_v4(
    *,
    options: TrainV4CryptoCsOptions,
    runtime_recommendations: dict[str, Any],
    selection_calibration: dict[str, Any],
    oos_rows: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    return _train_v4_execution.build_execution_risk_control_v4(
        options=options,
        runtime_recommendations=runtime_recommendations,
        selection_calibration=selection_calibration,
        oos_rows=oos_rows,
    )


def _purge_execution_artifact_run_dirs(
    *,
    output_root: Path,
    execution_acceptance: dict[str, Any] | None,
    runtime_recommendations: dict[str, Any] | None,
) -> dict[str, Any]:
    return _train_v4_execution.purge_execution_artifact_run_dirs(
        output_root=output_root,
        execution_acceptance=execution_acceptance,
        runtime_recommendations=runtime_recommendations,
    )


def _resolve_walk_forward_report_threshold_key(
    walk_forward_report: dict[str, Any] | None,
    *,
    fallback_threshold_key: str = "top_5pct",
) -> str:
    return _train_v4_postprocess.resolve_walk_forward_report_threshold_key(
        walk_forward_report,
        fallback_threshold_key=fallback_threshold_key,
    )


def _build_selection_search_trial_panel(
    *,
    windows: list[dict[str, Any]],
    start_trial_id: int,
) -> list[dict[str, Any]]:
    return _train_v4_walkforward_support.build_selection_search_trial_panel(
        windows=windows,
        start_trial_id=start_trial_id,
        summarize_walk_forward_windows_fn=summarize_walk_forward_windows,
    )


def _compact_trading_metrics(metrics: dict[str, Any]) -> dict[str, Any]:
    return _train_v4_walkforward_trials.compact_trading_metrics(metrics)


def _build_trial_selection_key(metrics: dict[str, Any]) -> dict[str, float]:
    return _train_v4_walkforward_trials.build_trial_selection_key(metrics)


def _fit_booster_sweep_classifier_v4(
    *,
    x_train: np.ndarray,
    y_train: np.ndarray,
    w_train: np.ndarray,
    x_valid: np.ndarray,
    y_valid: np.ndarray,
    w_valid: np.ndarray,
    y_reg_valid: np.ndarray,
    fee_bps_est: float,
    safety_bps: float,
    seed: int,
    nthread: int,
    trials: int,
    eval_sample_weight: np.ndarray | None = None,
) -> dict[str, Any]:
    return _train_v4_models.fit_booster_sweep_classifier_v4(
        x_train=x_train,
        y_train=y_train,
        w_train=w_train,
        x_valid=x_valid,
        y_valid=y_valid,
        w_valid=w_valid,
        y_reg_valid=y_reg_valid,
        fee_bps_est=fee_bps_est,
        safety_bps=safety_bps,
        seed=seed,
        nthread=nthread,
        trials=trials,
        eval_sample_weight=eval_sample_weight,
        try_import_xgboost_fn=_try_import_xgboost,
        sample_xgb_params_fn=_sample_xgb_params,
        evaluate_split_fn=_evaluate_split,
        build_v4_trainer_sweep_sort_key_fn=build_v4_trainer_sweep_sort_key,
        build_v4_shared_economic_objective_profile_fn=build_v4_shared_economic_objective_profile,
    )


_fit_booster_sweep_weighted = _fit_booster_sweep_classifier_v4


def _fit_booster_sweep_regression(
    *,
    x_train: np.ndarray,
    y_train: np.ndarray,
    w_train: np.ndarray,
    x_valid: np.ndarray,
    y_valid_cls: np.ndarray,
    y_valid_reg: np.ndarray,
    w_valid: np.ndarray,
    fee_bps_est: float,
    safety_bps: float,
    seed: int,
    nthread: int,
    trials: int,
    eval_sample_weight: np.ndarray | None = None,
) -> dict[str, Any]:
    return _train_v4_models.fit_booster_sweep_regression(
        x_train=x_train,
        y_train=y_train,
        w_train=w_train,
        x_valid=x_valid,
        y_valid_cls=y_valid_cls,
        y_valid_reg=y_valid_reg,
        w_valid=w_valid,
        fee_bps_est=fee_bps_est,
        safety_bps=safety_bps,
        seed=seed,
        nthread=nthread,
        trials=trials,
        eval_sample_weight=eval_sample_weight,
        try_import_xgboost_fn=_try_import_xgboost,
        sample_xgb_params_fn=_sample_xgb_params,
        evaluate_split_fn=_evaluate_split,
        build_v4_trainer_sweep_sort_key_fn=build_v4_trainer_sweep_sort_key,
        build_v4_shared_economic_objective_profile_fn=build_v4_shared_economic_objective_profile,
    )


def _fit_booster_sweep_ranker(
    *,
    x_train: np.ndarray,
    y_train_rank: np.ndarray,
    ts_train_ms: np.ndarray,
    w_train: np.ndarray,
    x_valid: np.ndarray,
    y_valid_cls: np.ndarray,
    y_valid_reg: np.ndarray,
    y_valid_rank: np.ndarray,
    ts_valid_ms: np.ndarray,
    w_valid: np.ndarray,
    fee_bps_est: float,
    safety_bps: float,
    seed: int,
    nthread: int,
    trials: int,
    eval_sample_weight: np.ndarray | None = None,
) -> dict[str, Any]:
    return _train_v4_models.fit_booster_sweep_ranker(
        x_train=x_train,
        y_train_rank=y_train_rank,
        ts_train_ms=ts_train_ms,
        w_train=w_train,
        x_valid=x_valid,
        y_valid_cls=y_valid_cls,
        y_valid_reg=y_valid_reg,
        y_valid_rank=y_valid_rank,
        ts_valid_ms=ts_valid_ms,
        w_valid=w_valid,
        fee_bps_est=fee_bps_est,
        safety_bps=safety_bps,
        seed=seed,
        nthread=nthread,
        trials=trials,
        eval_sample_weight=eval_sample_weight,
        try_import_xgboost_fn=_try_import_xgboost,
        sample_xgb_params_fn=_sample_xgb_params,
        evaluate_split_fn=_evaluate_split,
        attach_ranking_metrics_fn=_attach_ranking_metrics,
        build_v4_trainer_sweep_sort_key_fn=build_v4_trainer_sweep_sort_key,
        build_v4_shared_economic_objective_profile_fn=build_v4_shared_economic_objective_profile,
        group_counts_by_ts_fn=_group_counts_by_ts,
    )


def _resolve_ranker_budget_profile(
    *,
    options: TrainV4CryptoCsOptions,
    task: str,
    effective_booster_sweep_trials: int | None = None,
) -> dict[str, Any]:
    base_trials = max(
        int(effective_booster_sweep_trials)
        if effective_booster_sweep_trials is not None
        else int(options.booster_sweep_trials),
        1,
    )
    walk_forward_trials = max(int(options.walk_forward_sweep_trials), 1)
    if str(task).strip().lower() != "rank":
        return {
            "applied": False,
            "profile": "default_non_rank",
            "main_trials": base_trials,
            "walk_forward_trials": walk_forward_trials,
        }
    return {
        "applied": True,
        "profile": "oracle_a1_ranker_lane_v1",
        "objective": "rank:pairwise",
        "group_key": "ts_ms",
        "main_trials": min(base_trials, 3),
        "walk_forward_trials": min(walk_forward_trials, 2),
    }


def _attach_ranking_metrics(
    *,
    metrics: dict[str, Any],
    y_rank: np.ndarray,
    ts_ms: np.ndarray,
    scores: np.ndarray,
) -> dict[str, Any]:
    return _train_v4_postprocess.attach_ranking_metrics(
        metrics=metrics,
        y_rank=y_rank,
        ts_ms=ts_ms,
        scores=scores,
    )


def _evaluate_ranking_metrics(
    *,
    y_rank: np.ndarray,
    ts_ms: np.ndarray,
    scores: np.ndarray,
) -> dict[str, Any]:
    return _train_v4_postprocess.evaluate_ranking_metrics(
        y_rank=y_rank,
        ts_ms=ts_ms,
        scores=scores,
    )


def _group_indices_by_ts(ts_ms: np.ndarray) -> list[tuple[int, np.ndarray]]:
    return _train_v4_postprocess.group_indices_by_ts(ts_ms)


def _ndcg_at_k(relevance: np.ndarray, scores: np.ndarray, *, k: int) -> float:
    return _train_v4_postprocess.ndcg_at_k(relevance, scores, k=k)


def _dcg(values: np.ndarray) -> float:
    return _train_v4_postprocess._dcg(values)


def _build_v4_metrics_doc(
    *,
    run_id: str,
    options: TrainV4CryptoCsOptions,
    task: str,
    split_info: Any,
    interval_ms: int,
    rows: dict[str, int],
    valid_metrics: dict[str, Any],
    test_metrics: dict[str, Any],
    walk_forward_summary: dict[str, Any],
    cpcv_lite_summary: dict[str, Any],
    factor_block_selection_summary: dict[str, Any],
    best_params: dict[str, Any],
    sweep_records: list[dict[str, Any]],
    ranker_budget_profile: dict[str, Any],
    cpcv_lite_runtime: dict[str, Any],
    search_budget_decision: dict[str, Any],
    economic_objective_profile: dict[str, Any],
    lane_governance: dict[str, Any],
) -> dict[str, Any]:
    return _train_v4_artifacts.build_v4_metrics_doc(
        run_id=run_id,
        options=options,
        task=task,
        split_info=split_info,
        interval_ms=interval_ms,
        rows=rows,
        valid_metrics=valid_metrics,
        test_metrics=test_metrics,
        walk_forward_summary=walk_forward_summary,
        cpcv_lite_summary=cpcv_lite_summary,
        factor_block_selection_summary=factor_block_selection_summary,
        best_params=best_params,
        sweep_records=sweep_records,
        ranker_budget_profile=ranker_budget_profile,
        cpcv_lite_runtime=cpcv_lite_runtime,
        search_budget_decision=search_budget_decision,
        economic_objective_profile=economic_objective_profile,
        lane_governance=lane_governance,
    )


def _make_v4_leaderboard_row(
    *,
    run_id: str,
    options: TrainV4CryptoCsOptions,
    task: str,
    rows: dict[str, int],
    test_metrics: dict[str, Any],
) -> dict[str, Any]:
    return _train_v4_postprocess.build_leaderboard_row_v4(
        run_id=run_id,
        options=options,
        task=task,
        rows=rows,
        test_metrics=test_metrics,
    )


def _train_config_snapshot_v4(
    *,
    options: TrainV4CryptoCsOptions,
    task: str,
    feature_cols: tuple[str, ...],
    markets: tuple[str, ...],
    selection_recommendations: dict[str, Any],
    selection_policy: dict[str, Any],
    selection_calibration: dict[str, Any],
    research_support_lane: dict[str, Any],
    ranker_budget_profile: dict[str, Any],
    cpcv_lite_summary: dict[str, Any],
    factor_block_selection: dict[str, Any],
    factor_block_selection_context: dict[str, Any],
    cpcv_lite_runtime: dict[str, Any],
    search_budget_decision: dict[str, Any],
    lane_governance: dict[str, Any],
) -> dict[str, Any]:
    return _train_v4_artifacts.train_config_snapshot_v4(
        asdict_fn=asdict,
        options=options,
        task=task,
        feature_cols=feature_cols,
        markets=markets,
        selection_recommendations=selection_recommendations,
        selection_policy=selection_policy,
        selection_calibration=selection_calibration,
        research_support_lane=research_support_lane,
        ranker_budget_profile=ranker_budget_profile,
        cpcv_lite_summary=cpcv_lite_summary,
        factor_block_selection=factor_block_selection,
        factor_block_selection_context=factor_block_selection_context,
        cpcv_lite_runtime=cpcv_lite_runtime,
        search_budget_decision=search_budget_decision,
        lane_governance=lane_governance,
    )


def _build_decision_surface_v4(
    *,
    options: TrainV4CryptoCsOptions,
    task: str,
    selection_policy: dict[str, Any],
    selection_calibration: dict[str, Any],
    factor_block_selection: dict[str, Any],
    research_support_lane: dict[str, Any],
    factor_block_selection_context: dict[str, Any],
    cpcv_lite_runtime: dict[str, Any],
    search_budget_decision: dict[str, Any],
    execution_acceptance: dict[str, Any],
    runtime_recommendations: dict[str, Any],
    promotion: dict[str, Any],
    economic_objective_profile: dict[str, Any],
    lane_governance: dict[str, Any],
) -> dict[str, Any]:
    return _train_v4_artifacts.build_decision_surface_v4(
        options=options,
        task=task,
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


def _build_research_support_lane_v4(
    *,
    walk_forward: dict[str, Any],
    cpcv_lite: dict[str, Any],
) -> dict[str, Any]:
    return _train_v4_governance.build_research_support_lane_v4(
        walk_forward=walk_forward,
        cpcv_lite=cpcv_lite,
    )


def _build_trainer_research_evidence_from_promotion_v4(
    *,
    promotion: dict[str, Any],
    support_lane: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return _train_v4_governance.build_trainer_research_evidence_from_promotion_v4(
        promotion=promotion,
        support_lane=support_lane,
    )


def _write_train_report_v4(logs_root: Path, run_scope: str, payload: dict[str, Any]) -> Path:
    return _train_v4_artifacts.write_train_report_v4(logs_root, run_scope, payload)


def _run_execution_acceptance_v4(
    *,
    options: TrainV4CryptoCsOptions,
    run_id: str,
) -> dict[str, Any]:
    return _train_v4_execution.run_execution_acceptance_v4(
        options=options,
        run_id=run_id,
        resolve_v4_execution_compare_contract_fn=resolve_v4_execution_compare_contract,
        run_execution_acceptance_fn=run_execution_acceptance,
    )


def _build_runtime_recommendations_v4(
    *,
    options: TrainV4CryptoCsOptions,
    run_id: str,
    search_budget_decision: dict[str, Any],
) -> dict[str, Any]:
    return _train_v4_execution.build_runtime_recommendations_v4(
        options=options,
        run_id=run_id,
        search_budget_decision=search_budget_decision,
        optimize_runtime_recommendations_fn=optimize_runtime_recommendations,
        runtime_recommendation_grid_for_profile_fn=runtime_recommendation_grid_for_profile,
    )


def _manual_promotion_decision_v4(
    *,
    options: TrainV4CryptoCsOptions,
    run_id: str,
    walk_forward: dict[str, Any],
    execution_acceptance: dict[str, Any],
    runtime_recommendations: dict[str, Any] | None,
) -> dict[str, Any]:
    return _train_v4_governance.manual_promotion_decision_v4(
        options=options,
        run_id=run_id,
        walk_forward=walk_forward,
        execution_acceptance=execution_acceptance,
        runtime_recommendations=runtime_recommendations,
    )


def _build_duplicate_candidate_promotion_decision_v4(
    *,
    options: TrainV4CryptoCsOptions,
    run_id: str,
    walk_forward: dict[str, Any],
    execution_acceptance: dict[str, Any],
    duplicate_artifacts: dict[str, Any],
    runtime_recommendations: dict[str, Any] | None,
) -> dict[str, Any]:
    return _train_v4_governance.build_duplicate_candidate_promotion_decision_v4(
        options=options,
        run_id=run_id,
        walk_forward=walk_forward,
        execution_acceptance=execution_acceptance,
        duplicate_artifacts=duplicate_artifacts,
        runtime_recommendations=runtime_recommendations,
    )


def _safe_float(value: Any) -> float:
    return _train_v4_postprocess.safe_float(value)


def _utc_now() -> str:
    return _train_v4_postprocess.utc_now()
