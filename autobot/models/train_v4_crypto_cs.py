"""Training orchestration for trainer=v4_crypto_cs."""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import time
from typing import Any, Sequence

import numpy as np

from autobot import __version__ as autobot_version
from autobot.data import expected_interval_ms
from autobot.features.feature_spec import parse_date_to_ts_ms
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
    load_feature_dataset,
    load_feature_spec,
    load_label_spec,
)
from .economic_objective import (
    build_v4_shared_economic_objective_profile,
    build_v4_trainer_sweep_sort_key,
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
    append_factor_block_selection_history,
    build_factor_block_refit_support_summary,
    build_guarded_factor_block_policy,
    build_factor_block_selection_signature,
    build_factor_block_selection_report,
    build_factor_block_window_baseline,
    build_factor_block_window_row,
    evaluate_factor_block_window_rows,
    load_factor_block_selection_history,
    normalize_factor_block_selection_mode,
    normalize_run_scope as normalize_factor_block_run_scope,
    resolve_selected_feature_columns_from_latest,
    v4_factor_block_registry,
    write_latest_guarded_factor_block_policy,
    write_latest_factor_block_selection_pointer,
)
from .multiple_testing import (
    build_trial_window_differential_diagnostics,
    build_trial_window_differential_matrix,
    run_hansen_spa,
    run_white_reality_check,
)
from .execution_acceptance import ExecutionAcceptanceOptions, run_execution_acceptance
from .runtime_recommendations import optimize_runtime_recommendations, runtime_recommendation_grid_for_profile
from .model_card import render_model_card
from .search_budget import resolve_v4_search_budget, write_search_budget_decision
from .research_acceptance import (
    compare_balanced_pareto,
    compare_spa_like_window_test,
    summarize_walk_forward_windows,
)
from .selection_policy import build_selection_policy_from_recommendations
from .selection_calibration import build_selection_calibration_from_oos_rows
from .selection_optimizer import (
    SelectionGridConfig,
    build_selection_recommendations_from_walk_forward,
    build_window_selection_objectives,
)
from .registry import (
    RegistrySavePayload,
    load_json,
    make_run_id,
    save_run,
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
    build_selection_recommendations,
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


def _resolve_cpcv_lite_runtime_config(
    *,
    options: TrainV4CryptoCsOptions,
    factor_block_selection_context: dict[str, Any],
) -> dict[str, Any]:
    requested = bool(options.cpcv_lite_enabled)
    mode = normalize_factor_block_selection_mode(options.factor_block_selection_mode)
    guarded_policy_applied = (
        mode == "guarded_auto"
        and bool((factor_block_selection_context or {}).get("applied", False))
        and str((factor_block_selection_context or {}).get("resolution_source", "")).strip() == "guarded_policy"
    )
    if requested:
        return {
            "enabled": True,
            "trigger": "manual",
            "requested": True,
        }
    if guarded_policy_applied:
        return {
            "enabled": True,
            "trigger": "guarded_policy",
            "requested": False,
        }
    return {
        "enabled": False,
        "trigger": "disabled",
        "requested": False,
    }


def _build_lane_governance_v4(
    *,
    task: str,
    run_scope: str,
    economic_objective_profile: dict[str, Any],
) -> dict[str, Any]:
    task_name = str(task).strip().lower() or "cls"
    normalized_run_scope = normalize_factor_block_run_scope(run_scope)
    if task_name == "rank":
        if "rank_governed" in normalized_run_scope or "rank_promotable" in normalized_run_scope:
            lane_id = "rank_governed_primary"
            lane_role = "production_candidate"
            shadow_only = False
            promotion_allowed = True
            live_replacement_allowed = False
            governance_reasons = ["AUTO_GOVERNED_FROM_RANK_SHADOW_PASS"]
        else:
            lane_id = "rank_shadow"
            lane_role = "shadow"
            shadow_only = True
            promotion_allowed = False
            live_replacement_allowed = False
            governance_reasons = ["RANK_LANE_SHADOW_EVALUATION_ONLY", "EXPLICIT_GOVERNANCE_DECISION_REQUIRED"]
    elif task_name == "cls":
        lane_id = "cls_primary"
        lane_role = "primary"
        shadow_only = False
        promotion_allowed = True
        live_replacement_allowed = True
        governance_reasons = ["PRIMARY_LANE_ELIGIBLE"]
    else:
        lane_id = f"{task_name}_research"
        lane_role = "research"
        shadow_only = False
        promotion_allowed = False
        live_replacement_allowed = False
        governance_reasons = ["NON_PRIMARY_LANE_REQUIRES_EXPLICIT_GOVERNANCE"]
    return {
        "version": 1,
        "policy": "v4_lane_governance_v1",
        "lane_id": lane_id,
        "task": task_name,
        "run_scope": normalized_run_scope,
        "lane_role": lane_role,
        "shadow_only": bool(shadow_only),
        "production_lane_id": "cls_primary",
        "production_task": "cls",
        "comparison_lane_id": "cls_primary" if task_name == "rank" else "",
        "promotion_allowed": bool(promotion_allowed),
        "live_replacement_allowed": bool(live_replacement_allowed),
        "certification_contract_frozen": True,
        "frozen_contract_family": "t21_11_to_t21_16",
        "economic_objective_profile_id": str((economic_objective_profile or {}).get("profile_id", "")).strip()
        or "v4_shared_economic_objective_v1",
        "governance_reasons": list(governance_reasons),
    }


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
    request = build_dataset_request(
        dataset_root=options.dataset_root,
        tf=options.tf,
        quote=options.quote,
        top_n=options.top_n,
        start=options.start,
        end=options.end,
        batch_rows=options.batch_rows,
    )
    feature_spec = load_feature_spec(options.dataset_root)
    label_spec = load_label_spec(options.dataset_root)
    high_tfs = tuple(str(item).strip() for item in (feature_spec.get("high_tfs") or ("15m", "60m", "240m")) if str(item).strip())
    all_feature_cols = feature_columns_from_spec(options.dataset_root)
    feature_cols, factor_block_selection_context = resolve_selected_feature_columns_from_latest(
        registry_root=options.registry_root,
        model_family=options.model_family,
        mode=options.factor_block_selection_mode,
        run_scope=options.run_scope,
        all_feature_columns=all_feature_cols,
        high_tfs=high_tfs or ("15m", "60m", "240m"),
    )
    project_root = options.logs_root.parent.resolve()
    search_budget_decision = resolve_v4_search_budget(
        project_root=project_root,
        logs_root=options.logs_root,
        registry_root=options.registry_root,
        model_family=options.model_family,
        run_scope=options.run_scope,
        requested_booster_sweep_trials=options.booster_sweep_trials,
        factor_block_selection_context=factor_block_selection_context,
        cpcv_requested=bool(options.cpcv_lite_enabled),
    )
    effective_booster_sweep_trials = max(
        int((search_budget_decision.get("applied") or {}).get("booster_sweep_trials", options.booster_sweep_trials)),
        1,
    )
    dataset = load_feature_dataset(
        request,
        feature_columns=feature_cols,
        y_cls_column="y_cls_topq_12",
        y_reg_column="y_reg_net_12",
        y_rank_column="y_rank_cs_12",
    )
    if dataset.rows < int(options.min_rows_for_train):
        raise ValueError(
            f"NEED_MORE_MICRO_DAYS_OR_LOOSEN_UNIVERSE: rows={dataset.rows} < min_rows_for_train={int(options.min_rows_for_train)}"
        )

    interval_ms = expected_interval_ms(options.tf)
    labels, split_info = compute_time_splits(
        dataset.ts_ms,
        train_ratio=options.train_ratio,
        valid_ratio=options.valid_ratio,
        test_ratio=options.test_ratio,
        embargo_bars=options.embargo_bars,
        interval_ms=interval_ms,
    )
    masks = split_masks(labels)
    _validate_split_counts(masks)

    train_mask = masks[SPLIT_TRAIN]
    valid_mask = masks[SPLIT_VALID]
    test_mask = masks[SPLIT_TEST]
    rows = {
        "total": dataset.rows,
        "train": int(np.sum(train_mask)),
        "valid": int(np.sum(valid_mask)),
        "test": int(np.sum(test_mask)),
        "drop": int(np.sum(masks["drop"])),
    }

    x_train = dataset.X[train_mask]
    y_cls_train = dataset.y_cls[train_mask]
    y_reg_train = dataset.y_reg[train_mask]
    y_rank_all = np.asarray(getattr(dataset, "y_rank", dataset.y_reg), dtype=np.float32)
    y_rank_train = y_rank_all[train_mask]
    w_train = dataset.sample_weight[train_mask]
    ts_train = dataset.ts_ms[train_mask]
    x_valid = dataset.X[valid_mask]
    y_valid = dataset.y_cls[valid_mask]
    y_reg_valid = dataset.y_reg[valid_mask]
    y_rank_valid = y_rank_all[valid_mask]
    w_valid = dataset.sample_weight[valid_mask]
    ts_valid = dataset.ts_ms[valid_mask]
    x_test = dataset.X[test_mask]
    y_test = dataset.y_cls[test_mask]
    y_reg_test = dataset.y_reg[test_mask]
    y_rank_test = y_rank_all[test_mask]
    market_valid = dataset.markets[valid_mask]
    market_test = dataset.markets[test_mask]
    ranker_budget_profile = _resolve_ranker_budget_profile(
        options=options,
        task=task,
        effective_booster_sweep_trials=effective_booster_sweep_trials,
    )
    factor_block_registry = v4_factor_block_registry(
        feature_columns=dataset.feature_names,
        high_tfs=high_tfs or ("15m", "60m", "240m"),
    )
    cpcv_lite_runtime = _resolve_cpcv_lite_runtime_config(
        options=options,
        factor_block_selection_context=factor_block_selection_context,
    )

    if task == "cls":
        booster = _fit_booster_sweep_weighted(
            x_train=x_train,
            y_train=y_cls_train,
            w_train=w_train,
            x_valid=x_valid,
            y_valid=y_valid,
            w_valid=w_valid,
            y_reg_valid=y_reg_valid,
            fee_bps_est=options.fee_bps_est,
            safety_bps=options.safety_bps,
            seed=options.seed,
            nthread=options.nthread,
            trials=effective_booster_sweep_trials,
        )
    elif task == "reg":
        booster = _fit_booster_sweep_regression(
            x_train=x_train,
            y_train=y_reg_train,
            w_train=w_train,
            x_valid=x_valid,
            y_valid_cls=y_valid,
            y_valid_reg=y_reg_valid,
            w_valid=w_valid,
            fee_bps_est=options.fee_bps_est,
            safety_bps=options.safety_bps,
            seed=options.seed,
            nthread=options.nthread,
            trials=effective_booster_sweep_trials,
        )
    else:
        booster = _fit_booster_sweep_ranker(
            x_train=x_train,
            y_train_rank=y_rank_train,
            ts_train_ms=ts_train,
            w_train=w_train,
            x_valid=x_valid,
            y_valid_cls=y_valid,
            y_valid_reg=y_reg_valid,
            y_valid_rank=y_rank_valid,
            ts_valid_ms=ts_valid,
            w_valid=w_valid,
            fee_bps_est=options.fee_bps_est,
            safety_bps=options.safety_bps,
            seed=options.seed,
            nthread=options.nthread,
            trials=int(ranker_budget_profile["main_trials"]),
        )

    valid_scores = _predict_scores(booster["bundle"], x_valid)
    valid_metrics = _evaluate_split(
        y_cls=y_valid,
        y_reg=y_reg_valid,
        scores=valid_scores,
        markets=market_valid,
        fee_bps_est=options.fee_bps_est,
        safety_bps=options.safety_bps,
    )
    valid_metrics = _attach_ranking_metrics(
        metrics=valid_metrics,
        y_rank=y_rank_valid,
        ts_ms=ts_valid,
        scores=valid_scores,
    )
    test_scores = _predict_scores(booster["bundle"], x_test)
    test_metrics = _evaluate_split(
        y_cls=y_test,
        y_reg=y_reg_test,
        scores=test_scores,
        markets=market_test,
        fee_bps_est=options.fee_bps_est,
        safety_bps=options.safety_bps,
    )
    test_metrics = _attach_ranking_metrics(
        metrics=test_metrics,
        y_rank=y_rank_test,
        ts_ms=dataset.ts_ms[test_mask],
        scores=test_scores,
    )
    thresholds = _build_thresholds(
        valid_scores=valid_scores,
        y_reg_valid=y_reg_valid,
        fee_bps_est=options.fee_bps_est,
        safety_bps=options.safety_bps,
        ev_scan_steps=options.ev_scan_steps,
        ev_min_selected=options.ev_min_selected,
    )
    fallback_selection_recommendations = build_selection_recommendations(
        valid_scores=valid_scores,
        valid_ts_ms=dataset.ts_ms[valid_mask],
        thresholds=thresholds,
    )
    walk_forward = _run_walk_forward_v4(
        options=options,
        task=task,
        dataset=dataset,
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
    )
    selection_calibration = build_selection_calibration_from_oos_rows(
        oos_rows=walk_forward.pop("_selection_calibration_rows", []),
    )
    walk_forward = _finalize_walk_forward_report(
        walk_forward=walk_forward,
        selection_recommendations=selection_recommendations,
        options=options,
    )
    walk_forward["factor_block_refit_support"] = build_factor_block_refit_support_summary(
        block_registry=factor_block_registry,
        window_support=walk_forward.get("factor_block_refit_windows", []),
    )
    cpcv_lite = _run_cpcv_lite_v4(
        options=options,
        task=task,
        dataset=dataset,
        interval_ms=interval_ms,
        thresholds=thresholds,
        best_params=dict(booster.get("best_params", {})),
        enabled=bool(cpcv_lite_runtime["enabled"]),
        trigger=str(cpcv_lite_runtime["trigger"]),
    )
    research_support_lane = _build_research_support_lane_v4(
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
    metrics = _build_v4_metrics_doc(
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
    leaderboard_row = _make_v4_leaderboard_row(
        run_id=run_id,
        options=options,
        task=task,
        rows=rows,
        test_metrics=test_metrics,
    )

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

    model_card = render_model_card(
        run_id=run_id,
        model_family=options.model_family,
        champion="booster",
        metrics=metrics,
        thresholds=thresholds,
        data_fingerprint=data_fingerprint,
    )
    train_config = _train_config_snapshot_v4(
        options=options,
        task=task,
        feature_cols=dataset.feature_names,
        markets=dataset.selected_markets,
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
        )
    )
    if normalize_factor_block_run_scope(options.run_scope) == "scheduled_daily":
        update_latest_candidate_pointer(options.registry_root, options.model_family, run_id)
        update_latest_candidate_pointer(
            options.registry_root,
            "_global",
            run_id,
            family=options.model_family,
        )
    walk_forward_report_path = run_dir / "walk_forward_report.json"
    walk_forward_report_path.write_text(
        json.dumps(walk_forward, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    cpcv_lite_report_path = None
    if bool(cpcv_lite):
        cpcv_lite_report_path = run_dir / "cpcv_lite_report.json"
        cpcv_lite_report_path.write_text(
            json.dumps(cpcv_lite, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
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
    factor_block_selection_path = run_dir / "factor_block_selection.json"
    factor_block_selection_path.write_text(
        json.dumps(factor_block_selection, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
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
    factor_block_selection_path.write_text(
        json.dumps(factor_block_selection, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    search_budget_decision_path = write_search_budget_decision(
        run_dir=run_dir,
        decision=search_budget_decision,
    )
    duplicate_artifacts = _detect_duplicate_candidate_artifacts(
        options=options,
        run_id=run_id,
        run_dir=run_dir,
    )
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
    execution_artifact_cleanup = _purge_execution_artifact_run_dirs(
        output_root=options.execution_acceptance_output_root,
        execution_acceptance=execution_acceptance,
        runtime_recommendations=runtime_recommendations,
    )
    if execution_artifact_cleanup.get("evaluated"):
        execution_acceptance["artifacts_cleanup"] = execution_artifact_cleanup
        runtime_recommendations["artifacts_cleanup"] = execution_artifact_cleanup
    execution_acceptance_report_path = run_dir / "execution_acceptance_report.json"
    execution_acceptance_report_path.write_text(
        json.dumps(execution_acceptance, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    runtime_recommendations_path = run_dir / "runtime_recommendations.json"
    runtime_recommendations_path.write_text(
        json.dumps(runtime_recommendations, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    if duplicate_candidate:
        promotion = _build_duplicate_candidate_promotion_decision_v4(
            options=options,
            run_id=run_id,
            walk_forward=walk_forward,
            execution_acceptance=execution_acceptance,
            duplicate_artifacts=duplicate_artifacts,
        )
    else:
        promotion = _manual_promotion_decision_v4(
            options=options,
            run_id=run_id,
            walk_forward=walk_forward,
            execution_acceptance=execution_acceptance,
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
    trainer_research_evidence_path = run_dir / "trainer_research_evidence.json"
    trainer_research_evidence_path.write_text(
        json.dumps(trainer_research_evidence, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    economic_objective_profile_path = run_dir / "economic_objective_profile.json"
    economic_objective_profile_path.write_text(
        json.dumps(economic_objective_profile, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    lane_governance_path = run_dir / "lane_governance.json"
    lane_governance_path.write_text(
        json.dumps(lane_governance, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
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
    decision_surface_path = run_dir / "decision_surface.json"
    decision_surface_path.write_text(
        json.dumps(decision_surface, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
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
    )


def _run_walk_forward_v4(
    *,
    options: TrainV4CryptoCsOptions,
    task: str,
    dataset: Any,
    interval_ms: int,
    thresholds: dict[str, Any],
    feature_names: Sequence[str],
    factor_block_registry: Sequence[Any],
    effective_booster_sweep_trials: int,
) -> dict[str, Any]:
    report: dict[str, Any] = {
        "policy": "balanced_pareto_offline",
        "enabled": bool(options.walk_forward_enabled),
        "windows_requested": max(int(options.walk_forward_windows), 0),
        "windows": [],
        "skipped_windows": [],
        "summary": summarize_walk_forward_windows([]),
        "compare_to_champion": compare_balanced_pareto({}, {}),
        "selected_threshold_key": "top_5pct",
        "selected_threshold_key_source": "manual_fallback",
        "_factor_block_window_rows": [],
        "_selection_calibration_rows": [],
        "factor_block_refit_windows": [],
    }
    if not bool(options.walk_forward_enabled):
        report["skip_reason"] = "DISABLED"
        return report

    try:
        window_specs = compute_anchored_walk_forward_splits(
            dataset.ts_ms,
            valid_ratio=options.valid_ratio,
            test_ratio=options.test_ratio,
            window_count=max(int(options.walk_forward_windows), 1),
            embargo_bars=options.embargo_bars,
            interval_ms=interval_ms,
        )
    except ValueError as exc:
        report["skip_reason"] = str(exc)
        return report

    min_train_rows = max(int(options.walk_forward_min_train_rows), 1)
    min_test_rows = max(int(options.walk_forward_min_test_rows), 1)
    ranker_budget_profile = _resolve_ranker_budget_profile(
        options=options,
        task=task,
        effective_booster_sweep_trials=effective_booster_sweep_trials,
    )
    sweep_trials = max(min(int(options.walk_forward_sweep_trials), int(effective_booster_sweep_trials)), 1)
    if task == "rank":
        sweep_trials = min(sweep_trials, int(ranker_budget_profile["walk_forward_trials"]))
    windows: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    for labels, info in window_specs:
        masks = split_masks(labels)
        train_mask = masks[SPLIT_TRAIN]
        valid_mask = masks[SPLIT_VALID]
        test_mask = masks[SPLIT_TEST]
        row_counts = {
            "train": int(np.sum(train_mask)),
            "valid": int(np.sum(valid_mask)),
            "test": int(np.sum(test_mask)),
            "drop": int(np.sum(masks["drop"])),
        }
        if row_counts["train"] < min_train_rows or row_counts["test"] < min_test_rows or row_counts["valid"] <= 0:
            skipped.append(
                {
                    "window_index": info.window_index,
                    "counts": row_counts,
                    "reason": "INSUFFICIENT_ROWS",
                }
            )
            continue

        fitted = _fit_walk_forward_window_model(
            task=task,
            options=options,
            sweep_trials=sweep_trials,
            x_train=dataset.X[train_mask],
            y_cls_train=dataset.y_cls[train_mask],
            y_reg_train=dataset.y_reg[train_mask],
            y_rank_train=np.asarray(getattr(dataset, "y_rank", dataset.y_reg), dtype=np.float32)[train_mask],
            w_train=dataset.sample_weight[train_mask],
            ts_train_ms=dataset.ts_ms[train_mask],
            x_valid=dataset.X[valid_mask],
            y_valid_cls=dataset.y_cls[valid_mask],
            y_valid_reg=dataset.y_reg[valid_mask],
            y_valid_rank=np.asarray(getattr(dataset, "y_rank", dataset.y_reg), dtype=np.float32)[valid_mask],
            w_valid=dataset.sample_weight[valid_mask],
            ts_valid_ms=dataset.ts_ms[valid_mask],
            x_test=dataset.X[test_mask],
            y_test_cls=dataset.y_cls[test_mask],
            y_test_reg=dataset.y_reg[test_mask],
            y_test_rank=np.asarray(getattr(dataset, "y_rank", dataset.y_reg), dtype=np.float32)[test_mask],
            market_test=dataset.markets[test_mask],
            ts_test_ms=dataset.ts_ms[test_mask],
        )
        scores = _predict_scores(fitted["bundle"], dataset.X[test_mask])
        metrics = _evaluate_split(
            y_cls=dataset.y_cls[test_mask],
            y_reg=dataset.y_reg[test_mask],
            scores=scores,
            markets=dataset.markets[test_mask],
            fee_bps_est=options.fee_bps_est,
            safety_bps=options.safety_bps,
        )
        metrics = _attach_ranking_metrics(
            metrics=metrics,
            y_rank=np.asarray(getattr(dataset, "y_rank", dataset.y_reg), dtype=np.float32)[test_mask],
            ts_ms=dataset.ts_ms[test_mask],
            scores=scores,
        )
        oos_periods = _build_oos_period_metrics(
            ts_ms=dataset.ts_ms[test_mask],
            y_cls=dataset.y_cls[test_mask],
            y_reg=dataset.y_reg[test_mask],
            scores=scores,
            markets=dataset.markets[test_mask],
            fee_bps_est=options.fee_bps_est,
            safety_bps=options.safety_bps,
        )
        oos_slices = _build_oos_slice_metrics(
            ts_ms=dataset.ts_ms[test_mask],
            y_cls=dataset.y_cls[test_mask],
            y_reg=dataset.y_reg[test_mask],
            scores=scores,
            markets=dataset.markets[test_mask],
            fee_bps_est=options.fee_bps_est,
            safety_bps=options.safety_bps,
        )
        windows.append(
            {
                "window_index": info.window_index,
                "time_window": {
                    "valid_start_ts": int(info.valid_start_ts),
                    "test_start_ts": int(info.test_start_ts),
                    "test_end_ts": int(info.test_end_ts),
                },
                "counts": row_counts,
                "metrics": _compact_eval_metrics(metrics),
                "oos_periods": oos_periods,
                "oos_slices": oos_slices,
                "selection_optimization": build_window_selection_objectives(
                    scores=scores,
                    y_reg=dataset.y_reg[test_mask],
                    ts_ms=dataset.ts_ms[test_mask],
                    thresholds=thresholds,
                    fee_bps_est=options.fee_bps_est,
                    safety_bps=options.safety_bps,
                    config=SelectionGridConfig(),
                ),
                "trial_records": list(fitted.get("trial_records", [])),
            }
        )
        report["_selection_calibration_rows"].append(
            {
                "window_index": int(info.window_index),
                "scores": np.asarray(scores, dtype=np.float64).tolist(),
                "y_cls": np.asarray(dataset.y_cls[test_mask], dtype=np.int64).tolist(),
            }
        )
        report["_factor_block_window_rows"].extend(
            evaluate_factor_block_window_rows(
                window_index=int(info.window_index),
                model_bundle=fitted["bundle"],
                feature_names=feature_names,
                x_window=dataset.X[test_mask],
                y_cls=dataset.y_cls[test_mask],
                y_reg=dataset.y_reg[test_mask],
                ts_ms=dataset.ts_ms[test_mask],
                thresholds=thresholds,
                fee_bps_est=options.fee_bps_est,
                safety_bps=options.safety_bps,
                block_registry=factor_block_registry,
            )
        )
        refit_evidence = _evaluate_factor_block_refit_window_evidence(
            window_index=int(info.window_index),
            task=task,
            options=options,
            best_params=dict(fitted.get("best_params", {}) or {}),
            full_bundle=fitted["bundle"],
            feature_names=feature_names,
            x_train=dataset.X[train_mask],
            y_cls_train=dataset.y_cls[train_mask],
            y_reg_train=dataset.y_reg[train_mask],
            y_rank_train=np.asarray(getattr(dataset, "y_rank", dataset.y_reg), dtype=np.float32)[train_mask],
            w_train=dataset.sample_weight[train_mask],
            ts_train_ms=dataset.ts_ms[train_mask],
            x_valid=dataset.X[valid_mask],
            y_valid_cls=dataset.y_cls[valid_mask],
            y_valid_reg=dataset.y_reg[valid_mask],
            y_valid_rank=np.asarray(getattr(dataset, "y_rank", dataset.y_reg), dtype=np.float32)[valid_mask],
            w_valid=dataset.sample_weight[valid_mask],
            ts_valid_ms=dataset.ts_ms[valid_mask],
            x_test=dataset.X[test_mask],
            y_test_cls=dataset.y_cls[test_mask],
            y_test_reg=dataset.y_reg[test_mask],
            y_test_rank=np.asarray(getattr(dataset, "y_rank", dataset.y_reg), dtype=np.float32)[test_mask],
            ts_test_ms=dataset.ts_ms[test_mask],
            thresholds=thresholds,
            block_registry=factor_block_registry,
        )
        report["_factor_block_window_rows"].extend(refit_evidence["rows"])
        report["factor_block_refit_windows"].append(refit_evidence["support"])

    report["windows"] = windows
    report["skipped_windows"] = skipped
    report["windows_generated"] = len(window_specs)
    report["trial_panel"] = _summarize_walk_forward_trial_panel(windows, threshold_key="top_5pct")
    return report


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
    report: dict[str, Any] = {
        "policy": "cpcv_lite_research_v1",
        "enabled": bool(enabled),
        "trigger": str(trigger).strip() or "disabled",
        "requested": bool(options.cpcv_lite_enabled),
        "estimate_label": "lite",
        "summary": {
            "status": "disabled",
            "folds_requested": 0,
            "folds_run": 0,
            "skipped_folds": 0,
            "comparable_fold_count": 0,
            "budget_cut": False,
            "budget_reason": "DISABLED",
            "reasons": ["DISABLED"],
        },
        "folds": [],
        "skipped_folds": [],
        "pbo": {
            "comparable": False,
            "reason": "DISABLED",
            "folds_considered": 0,
            "pbo_estimate": 0.0,
            "overfit_fold_count": 0,
            "median_selected_test_percentile": 0.0,
            "mean_selected_test_logit": 0.0,
        },
        "dsr": {
            "comparable": False,
            "reason": "DISABLED",
            "observations": 0,
            "mean_selected_test_objective": 0.0,
            "std_selected_test_objective": 0.0,
            "score_series_sharpe": 0.0,
            "deflated_sharpe_ratio_est": 0.0,
            "benchmark_sharpe": 0.0,
            "effective_trials": 0,
            "effective_trials_source": "selection_config_count_max",
        },
        "insufficiency_reasons": [],
    }
    if not bool(enabled):
        report["skip_reason"] = "DISABLED"
        return report

    fold_specs, plan_report = build_cpcv_lite_plan(
        dataset.ts_ms,
        group_count=options.cpcv_lite_group_count,
        test_group_count=options.cpcv_lite_test_group_count,
        max_combinations=options.cpcv_lite_max_combinations,
        embargo_bars=options.embargo_bars,
        interval_ms=interval_ms,
    )
    report.update(plan_report)
    report["fold_refit_policy"] = {
        "mode": "reuse_main_best_params",
        "task": str(task),
        "params_present": bool(best_params),
    }
    if not fold_specs:
        report["summary"] = {
            "status": "insufficient",
            "folds_requested": 0,
            "folds_run": 0,
            "skipped_folds": 0,
            "comparable_fold_count": 0,
            "budget_cut": bool(report.get("budget_cut", False)),
            "budget_reason": str(report.get("budget_reason", "")).strip(),
            "reasons": list(report.get("insufficiency_reasons", [])),
        }
        return report
    if not best_params:
        report["insufficiency_reasons"] = list(report.get("insufficiency_reasons", [])) + ["MISSING_MAIN_BEST_PARAMS"]
        report["summary"] = {
            "status": "insufficient",
            "folds_requested": int(len(fold_specs)),
            "folds_run": 0,
            "skipped_folds": int(len(fold_specs)),
            "comparable_fold_count": 0,
            "budget_cut": bool(report.get("budget_cut", False)),
            "budget_reason": str(report.get("budget_reason", "")).strip(),
            "reasons": list(report.get("insufficiency_reasons", [])),
        }
        return report

    min_train_rows = max(int(options.cpcv_lite_min_train_rows), 1)
    min_test_rows = max(int(options.cpcv_lite_min_test_rows), 1)
    valid_ratio = min(max(float(options.valid_ratio), 0.10), 0.40)
    folds: list[dict[str, Any]] = []
    skipped_folds: list[dict[str, Any]] = []
    all_y_rank = np.asarray(getattr(dataset, "y_rank", dataset.y_reg), dtype=np.float32)

    for spec in fold_specs:
        train_all_mask = spec.labels == SPLIT_TRAIN
        test_mask = spec.labels == SPLIT_TEST
        row_counts = {
            "train": int(np.sum(train_all_mask)),
            "test": int(np.sum(test_mask)),
            "drop": int(np.sum(spec.labels == SPLIT_DROP)),
        }
        if row_counts["train"] < min_train_rows or row_counts["test"] < min_test_rows:
            skipped_folds.append(
                {
                    "fold_index": int(spec.fold_index),
                    "test_groups": list(spec.test_groups),
                    "purged_groups": list(spec.purged_groups),
                    "counts": row_counts,
                    "reason": "INSUFFICIENT_ROWS",
                }
            )
            continue

        inner_masks = _build_cpcv_inner_train_valid_masks(
            ts_ms=np.asarray(dataset.ts_ms[train_all_mask], dtype=np.int64),
            valid_ratio=valid_ratio,
            embargo_bars=options.embargo_bars,
            interval_ms=interval_ms,
        )
        if inner_masks is None:
            skipped_folds.append(
                {
                    "fold_index": int(spec.fold_index),
                    "test_groups": list(spec.test_groups),
                    "purged_groups": list(spec.purged_groups),
                    "counts": row_counts,
                    "reason": "INSUFFICIENT_INNER_VALIDATION_WINDOW",
                }
            )
            continue

        train_all_x = dataset.X[train_all_mask]
        train_all_y_cls = dataset.y_cls[train_all_mask]
        train_all_y_reg = dataset.y_reg[train_all_mask]
        train_all_y_rank = all_y_rank[train_all_mask]
        train_all_w = dataset.sample_weight[train_all_mask]
        train_all_ts = dataset.ts_ms[train_all_mask]
        fit_bundle = _fit_cpcv_lite_fold_model(
            task=task,
            options=options,
            best_params=best_params,
            x_train=train_all_x[inner_masks["train"]],
            y_cls_train=train_all_y_cls[inner_masks["train"]],
            y_reg_train=train_all_y_reg[inner_masks["train"]],
            y_rank_train=train_all_y_rank[inner_masks["train"]],
            w_train=train_all_w[inner_masks["train"]],
            ts_train_ms=train_all_ts[inner_masks["train"]],
            x_valid=train_all_x[inner_masks["valid"]],
            y_valid_cls=train_all_y_cls[inner_masks["valid"]],
            y_valid_reg=train_all_y_reg[inner_masks["valid"]],
            y_valid_rank=train_all_y_rank[inner_masks["valid"]],
            w_valid=train_all_w[inner_masks["valid"]],
            ts_valid_ms=train_all_ts[inner_masks["valid"]],
            fold_index=int(spec.fold_index),
        )

        train_scores = _predict_scores(fit_bundle, train_all_x)
        test_scores = _predict_scores(fit_bundle, dataset.X[test_mask])
        fold_metrics = _attach_ranking_metrics(
            metrics=_evaluate_split(
                y_cls=dataset.y_cls[test_mask],
                y_reg=dataset.y_reg[test_mask],
                scores=test_scores,
                markets=dataset.markets[test_mask],
                fee_bps_est=options.fee_bps_est,
                safety_bps=options.safety_bps,
            ),
            y_rank=all_y_rank[test_mask],
            ts_ms=dataset.ts_ms[test_mask],
            scores=test_scores,
        )
        train_selection = build_window_selection_objectives(
            scores=train_scores,
            y_reg=train_all_y_reg,
            ts_ms=train_all_ts,
            thresholds=thresholds,
            fee_bps_est=options.fee_bps_est,
            safety_bps=options.safety_bps,
            config=SelectionGridConfig(),
        )
        test_selection = build_window_selection_objectives(
            scores=test_scores,
            y_reg=dataset.y_reg[test_mask],
            ts_ms=dataset.ts_ms[test_mask],
            thresholds=thresholds,
            fee_bps_est=options.fee_bps_est,
            safety_bps=options.safety_bps,
            config=SelectionGridConfig(),
        )
        selection_summary = summarize_cpcv_lite_fold_selection(
            train_selection=train_selection,
            test_selection=test_selection,
        )
        folds.append(
            {
                "fold_index": int(spec.fold_index),
                "test_groups": list(spec.test_groups),
                "purged_groups": list(spec.purged_groups),
                "counts": row_counts,
                "inner_validation": {
                    "train_rows": int(np.sum(inner_masks["train"])),
                    "valid_rows": int(np.sum(inner_masks["valid"])),
                    "drop_rows": int(np.sum(inner_masks["drop"])),
                    "valid_start_ts": int(inner_masks["valid_start_ts"]),
                },
                "metrics": _compact_eval_metrics(fold_metrics),
                "selection_summary": selection_summary,
            }
        )

    report["folds"] = folds
    report["skipped_folds"] = skipped_folds
    report["pbo"] = summarize_cpcv_lite_pbo(folds)
    report["dsr"] = summarize_cpcv_lite_dsr(folds)
    insufficiency_reasons = list(report.get("insufficiency_reasons", []))
    if not folds:
        insufficiency_reasons.append("NO_EVALUATED_FOLDS")
    if not bool(report["pbo"].get("comparable", False)):
        insufficiency_reasons.append(str(report["pbo"].get("reason", "PBO_NOT_COMPARABLE")))
    if not bool(report["dsr"].get("comparable", False)):
        insufficiency_reasons.append(str(report["dsr"].get("reason", "DSR_NOT_COMPARABLE")))
    comparable_fold_count = int(
        sum(
            1
            for fold in folds
            if isinstance(fold, dict) and bool((fold.get("selection_summary") or {}).get("comparable", False))
        )
    )
    status = "trusted"
    if not folds:
        status = "insufficient"
    elif bool(report.get("budget_cut", False)) or skipped_folds or comparable_fold_count < 2:
        status = "partial"
    if bool(report.get("budget_cut", False)):
        insufficiency_reasons.append("BUDGET_CUT")
        if str(report.get("budget_reason", "")).strip():
            insufficiency_reasons.append(f"BUDGET_REASON_{str(report.get('budget_reason', '')).strip()}")
    if skipped_folds:
        insufficiency_reasons.append("SKIPPED_FOLDS_PRESENT")
    if comparable_fold_count < 2:
        insufficiency_reasons.append("INSUFFICIENT_COMPARABLE_FOLDS")
    report["insufficiency_reasons"] = sorted({str(item) for item in insufficiency_reasons if str(item).strip()})
    report["summary"] = {
        "status": status,
        "folds_requested": int(len(fold_specs)),
        "folds_run": int(len(folds)),
        "skipped_folds": int(len(skipped_folds)),
        "comparable_fold_count": comparable_fold_count,
        "budget_cut": bool(report.get("budget_cut", False)),
        "budget_reason": str(report.get("budget_reason", "")).strip(),
        "reasons": list(report["insufficiency_reasons"]),
    }
    return report


def _build_cpcv_inner_train_valid_masks(
    *,
    ts_ms: np.ndarray,
    valid_ratio: float,
    embargo_bars: int,
    interval_ms: int,
) -> dict[str, Any] | None:
    ts_values = np.asarray(ts_ms, dtype=np.int64)
    if ts_values.size <= 0:
        return None
    unique_ts = np.unique(ts_values)
    if unique_ts.size < 4:
        return None
    valid_len = max(int(np.floor(unique_ts.size * float(valid_ratio))), 1)
    if valid_len >= unique_ts.size:
        return None
    valid_start_ts = int(unique_ts[-valid_len])
    embargo_ms = max(int(embargo_bars), 0) * max(int(interval_ms), 1)
    valid_mask = ts_values >= valid_start_ts
    drop_mask = np.zeros(ts_values.shape[0], dtype=bool)
    if embargo_ms > 0:
        drop_mask = np.abs(ts_values - valid_start_ts) <= embargo_ms
        drop_mask &= ~valid_mask
    train_mask = ~(valid_mask | drop_mask)
    if int(np.sum(train_mask)) <= 0 or int(np.sum(valid_mask)) <= 0:
        return None
    return {
        "train": train_mask,
        "valid": valid_mask,
        "drop": drop_mask,
        "valid_start_ts": int(valid_start_ts),
    }


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
    task_name = str(task).strip().lower()
    if task_name == "cls":
        return _fit_fixed_classifier_model(
            options=options,
            best_params=best_params,
            x_train=x_train,
            y_train=y_cls_train,
            w_train=w_train,
            x_valid=x_valid,
            y_valid=y_valid_cls,
            w_valid=w_valid,
            fold_index=fold_index,
        )
    if task_name == "reg":
        return _fit_fixed_regression_model(
            options=options,
            best_params=best_params,
            x_train=x_train,
            y_train=y_reg_train,
            w_train=w_train,
            x_valid=x_valid,
            y_valid=y_valid_reg,
            w_valid=w_valid,
            fold_index=fold_index,
        )
    return _fit_fixed_ranker_model(
        options=options,
        best_params=best_params,
        x_train=x_train,
        y_train=y_rank_train,
        ts_train_ms=ts_train_ms,
        w_train=w_train,
        x_valid=x_valid,
        y_valid=y_valid_rank,
        ts_valid_ms=ts_valid_ms,
        fold_index=fold_index,
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
    xgb = _try_import_xgboost()
    if xgb is None:
        raise RuntimeError("xgboost is required for trainer=v4_crypto_cs")
    params = _normalize_xgb_best_params(best_params)
    train_w = np.clip(np.asarray(w_train, dtype=np.float64), 1e-6, None)
    valid_w = np.clip(np.asarray(w_valid, dtype=np.float64), 1e-6, None)
    estimator = xgb.XGBClassifier(
        objective="binary:logistic",
        tree_method="hist",
        n_estimators=1200,
        learning_rate=params["learning_rate"],
        max_depth=params["max_depth"],
        subsample=params["subsample"],
        colsample_bytree=params["colsample_bytree"],
        min_child_weight=params["min_child_weight"],
        reg_lambda=params["reg_lambda"],
        reg_alpha=params["reg_alpha"],
        max_bin=params["max_bin"],
        random_state=int(options.seed + fold_index),
        nthread=max(int(options.nthread), 1),
        eval_metric="logloss",
    )
    fit_kwargs = {
        "sample_weight": train_w,
        "eval_set": [(x_valid, y_valid)],
        "sample_weight_eval_set": [valid_w],
        "verbose": False,
    }
    try:
        estimator.fit(x_train, y_train, early_stopping_rounds=50, **fit_kwargs)
    except TypeError:
        estimator.fit(x_train, y_train, sample_weight=train_w, eval_set=[(x_valid, y_valid)], verbose=False)
    return {"model_type": "xgboost", "scaler": None, "estimator": estimator}


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
    xgb = _try_import_xgboost()
    if xgb is None:
        raise RuntimeError("xgboost is required for trainer=v4_crypto_cs")
    params = _normalize_xgb_best_params(best_params)
    train_w = np.clip(np.asarray(w_train, dtype=np.float64), 1e-6, None)
    valid_w = np.clip(np.asarray(w_valid, dtype=np.float64), 1e-6, None)
    estimator = xgb.XGBRegressor(
        objective="reg:squarederror",
        tree_method="hist",
        n_estimators=1200,
        learning_rate=params["learning_rate"],
        max_depth=params["max_depth"],
        subsample=params["subsample"],
        colsample_bytree=params["colsample_bytree"],
        min_child_weight=params["min_child_weight"],
        reg_lambda=params["reg_lambda"],
        reg_alpha=params["reg_alpha"],
        max_bin=params["max_bin"],
        random_state=int(options.seed + fold_index),
        nthread=max(int(options.nthread), 1),
        eval_metric="rmse",
    )
    fit_kwargs = {
        "sample_weight": train_w,
        "eval_set": [(x_valid, y_valid)],
        "sample_weight_eval_set": [valid_w],
        "verbose": False,
    }
    try:
        estimator.fit(x_train, y_train, early_stopping_rounds=50, **fit_kwargs)
    except TypeError:
        estimator.fit(x_train, y_train, sample_weight=train_w, eval_set=[(x_valid, y_valid)], verbose=False)
    return {"model_type": "xgboost_regressor", "scaler": None, "estimator": estimator}


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
    xgb = _try_import_xgboost()
    if xgb is None:
        raise RuntimeError("xgboost is required for trainer=v4_crypto_cs")
    params = _normalize_xgb_best_params(best_params)
    train_group = _group_counts_by_ts(ts_train_ms)
    valid_group = _group_counts_by_ts(ts_valid_ms)
    if train_group.size <= 0 or valid_group.size <= 0:
        raise RuntimeError("ranker CPCV-lite fold requires grouped train and valid rows")
    train_w = np.clip(np.asarray(w_train, dtype=np.float64), 1e-6, None)
    estimator = xgb.XGBRanker(
        objective="rank:pairwise",
        tree_method="hist",
        n_estimators=900,
        learning_rate=params["learning_rate"],
        max_depth=params["max_depth"],
        subsample=params["subsample"],
        colsample_bytree=params["colsample_bytree"],
        min_child_weight=params["min_child_weight"],
        reg_lambda=params["reg_lambda"],
        reg_alpha=params["reg_alpha"],
        max_bin=params["max_bin"],
        random_state=int(options.seed + fold_index),
        nthread=max(int(options.nthread), 1),
        eval_metric="ndcg@5",
    )
    fit_kwargs = {
        "group": train_group.tolist(),
        "sample_weight": train_w,
        "eval_set": [(x_valid, np.nan_to_num(np.asarray(y_valid, dtype=np.float32), nan=0.0))],
        "eval_group": [valid_group.tolist()],
        "verbose": False,
    }
    try:
        estimator.fit(
            x_train,
            np.nan_to_num(np.asarray(y_train, dtype=np.float32), nan=0.0),
            early_stopping_rounds=50,
            **fit_kwargs,
        )
    except TypeError:
        estimator.fit(x_train, np.nan_to_num(np.asarray(y_train, dtype=np.float32), nan=0.0), **fit_kwargs)
    return {"model_type": "xgboost_ranker", "scaler": None, "estimator": estimator}


def _normalize_xgb_best_params(best_params: dict[str, Any] | None) -> dict[str, Any]:
    source = dict(best_params or {})
    return {
        "learning_rate": float(source.get("learning_rate", 0.05) or 0.05),
        "max_depth": int(source.get("max_depth", 6) or 6),
        "subsample": float(source.get("subsample", 0.8) or 0.8),
        "colsample_bytree": float(source.get("colsample_bytree", 0.8) or 0.8),
        "min_child_weight": float(source.get("min_child_weight", 1.0) or 1.0),
        "reg_lambda": float(source.get("reg_lambda", 1.0) or 1.0),
        "reg_alpha": float(source.get("reg_alpha", 0.0) or 0.0),
        "max_bin": int(source.get("max_bin", 256) or 256),
    }


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
    if task == "cls":
        return _fit_walk_forward_weighted_trials(
            options=options,
            sweep_trials=sweep_trials,
            x_train=x_train,
            y_train=y_cls_train,
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
        )
    if task == "reg":
        return _fit_walk_forward_regression_trials(
            options=options,
            sweep_trials=sweep_trials,
            x_train=x_train,
            y_train=y_reg_train,
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
        )
    return _fit_walk_forward_ranker_trials(
        options=options,
        sweep_trials=sweep_trials,
        x_train=x_train,
        y_train_rank=y_rank_train,
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
    optional_blocks = [block for block in block_registry if not bool(getattr(block, "protected", False))]
    support: dict[str, Any] = {
        "window_index": int(window_index),
        "policy": "bounded_drop_block_refit_v1",
        "bound_mode": "reuse_window_best_params",
        "task": str(task),
        "status": "supported",
        "best_params_present": bool(best_params),
        "optional_blocks_total": int(len(optional_blocks)),
        "optional_blocks_attempted": 0,
        "optional_blocks_with_rows": 0,
        "optional_blocks_without_rows": int(len(optional_blocks)),
        "reason_codes": [],
        "by_block": {},
    }
    if not optional_blocks:
        support["status"] = "not_applicable"
        support["reason_codes"] = ["NO_OPTIONAL_BLOCKS"]
        return {"rows": [], "support": support}
    if not best_params:
        support["status"] = "insufficient"
        support["reason_codes"] = ["MISSING_WINDOW_BEST_PARAMS"]
        for block in optional_blocks:
            support["by_block"][block.block_id] = {
                "block_id": block.block_id,
                "status": "insufficient",
                "rows_emitted": 0,
                "reason_codes": ["MISSING_WINDOW_BEST_PARAMS"],
            }
        return {"rows": [], "support": support}
    if x_test.size <= 0:
        support["status"] = "insufficient"
        support["reason_codes"] = ["NO_TEST_ROWS"]
        for block in optional_blocks:
            support["by_block"][block.block_id] = {
                "block_id": block.block_id,
                "status": "insufficient",
                "rows_emitted": 0,
                "reason_codes": ["NO_TEST_ROWS"],
            }
        return {"rows": [], "support": support}
    if len(feature_names) <= 0:
        support["status"] = "insufficient"
        support["reason_codes"] = ["NO_FEATURE_NAMES"]
        for block in optional_blocks:
            support["by_block"][block.block_id] = {
                "block_id": block.block_id,
                "status": "insufficient",
                "rows_emitted": 0,
                "reason_codes": ["NO_FEATURE_NAMES"],
            }
        return {"rows": [], "support": support}
    full_scores = _predict_scores(full_bundle, x_test)
    baseline = build_factor_block_window_baseline(
        scores=full_scores,
        y_reg=y_test_reg,
        ts_ms=ts_test_ms,
        thresholds=thresholds,
        fee_bps_est=options.fee_bps_est,
        safety_bps=options.safety_bps,
    )
    if baseline is None:
        support["status"] = "insufficient"
        support["reason_codes"] = ["SELECTION_BASELINE_UNAVAILABLE"]
        for block in optional_blocks:
            support["by_block"][block.block_id] = {
                "block_id": block.block_id,
                "status": "insufficient",
                "rows_emitted": 0,
                "reason_codes": ["SELECTION_BASELINE_UNAVAILABLE"],
            }
        return {"rows": [], "support": support}
    full_metrics = _evaluate_split(
        y_cls=y_test_cls,
        y_reg=y_test_reg,
        scores=full_scores,
        markets=np.array(["_ALL_"] * int(y_test_cls.size), dtype=object),
        fee_bps_est=options.fee_bps_est,
        safety_bps=options.safety_bps,
    )
    full_metrics = _attach_ranking_metrics(
        metrics=full_metrics,
        y_rank=y_test_rank,
        ts_ms=ts_test_ms,
        scores=full_scores,
    )
    full_top5 = ((full_metrics.get("trading", {}) or {}).get("top_5pct", {})) if isinstance(full_metrics, dict) else {}
    full_signature = dict(baseline.get("selection_signature") or {})
    feature_index = {str(name): idx for idx, name in enumerate(feature_names)}
    all_indices = list(range(len(feature_names)))
    rows: list[dict[str, Any]] = []

    for block in optional_blocks:
        drop_indices = [feature_index[name] for name in block.feature_columns if name in feature_index]
        if not drop_indices:
            support["by_block"][block.block_id] = {
                "block_id": block.block_id,
                "status": "insufficient",
                "rows_emitted": 0,
                "reason_codes": ["BLOCK_FEATURES_NOT_PRESENT"],
            }
            continue
        support["optional_blocks_attempted"] = int(support["optional_blocks_attempted"]) + 1
        keep_indices = [idx for idx in all_indices if idx not in set(drop_indices)]
        if not keep_indices:
            support["by_block"][block.block_id] = {
                "block_id": block.block_id,
                "status": "insufficient",
                "rows_emitted": 0,
                "reason_codes": ["DROP_ELIMINATES_ALL_FEATURES"],
            }
            continue
        x_train_drop = np.asarray(x_train[:, keep_indices], dtype=np.float32)
        x_valid_drop = np.asarray(x_valid[:, keep_indices], dtype=np.float32)
        x_test_drop = np.asarray(x_test[:, keep_indices], dtype=np.float32)
        try:
            if task == "cls":
                dropped_bundle = _fit_fixed_classifier_model(
                    options=options,
                    best_params=best_params,
                    x_train=x_train_drop,
                    y_train=y_cls_train,
                    w_train=w_train,
                    x_valid=x_valid_drop,
                    y_valid=y_valid_cls,
                    w_valid=w_valid,
                    fold_index=int(window_index),
                )
            elif task == "reg":
                dropped_bundle = _fit_fixed_regression_model(
                    options=options,
                    best_params=best_params,
                    x_train=x_train_drop,
                    y_train=y_reg_train,
                    w_train=w_train,
                    x_valid=x_valid_drop,
                    y_valid=y_valid_reg,
                    w_valid=w_valid,
                    fold_index=int(window_index),
                )
            else:
                dropped_bundle = _fit_fixed_ranker_model(
                    options=options,
                    best_params=best_params,
                    x_train=x_train_drop,
                    y_train=y_rank_train,
                    ts_train_ms=ts_train_ms,
                    w_train=w_train,
                    x_valid=x_valid_drop,
                    y_valid=y_valid_rank,
                    ts_valid_ms=ts_valid_ms,
                    fold_index=int(window_index),
                )
        except Exception as exc:
            support["by_block"][block.block_id] = {
                "block_id": block.block_id,
                "status": "insufficient",
                "rows_emitted": 0,
                "reason_codes": [f"REFIT_MODEL_FAILED_{type(exc).__name__.upper()}"],
                "error_type": type(exc).__name__,
                "error_message": str(exc),
            }
            continue

        dropped_scores = _predict_scores(dropped_bundle, x_test_drop)
        dropped_metrics = _evaluate_split(
            y_cls=y_test_cls,
            y_reg=y_test_reg,
            scores=dropped_scores,
            markets=np.array(["_ALL_"] * int(y_test_cls.size), dtype=object),
            fee_bps_est=options.fee_bps_est,
            safety_bps=options.safety_bps,
        )
        dropped_metrics = _attach_ranking_metrics(
            metrics=dropped_metrics,
            y_rank=y_test_rank,
            ts_ms=ts_test_ms,
            scores=dropped_scores,
        )
        dropped_top5 = ((dropped_metrics.get("trading", {}) or {}).get("top_5pct", {})) if isinstance(dropped_metrics, dict) else {}
        dropped_signature = build_factor_block_selection_signature(
            scores=dropped_scores,
            y_reg=y_test_reg,
            ts_ms=ts_test_ms,
            threshold=float(baseline["threshold_value"]),
            top_pct=float(baseline["top_pct"]),
            min_candidates=int(baseline["min_candidates_per_ts"]),
            fee_bps_est=options.fee_bps_est,
            safety_bps=options.safety_bps,
        )
        rows.append(
            build_factor_block_window_row(
                window_index=int(window_index),
                block=block,
                feature_count=int(len(drop_indices)),
                full_top5=full_top5,
                candidate_top5=dropped_top5,
                full_signature=full_signature,
                candidate_signature=dropped_signature,
                selection_profile=dict(baseline.get("selection_profile") or {}),
                evidence_mode="refit_drop_block",
                diagnostic_only=False,
            )
        )
        support["by_block"][block.block_id] = {
            "block_id": block.block_id,
            "status": "supported",
            "rows_emitted": 1,
            "reason_codes": ["REFIT_ROW_EMITTED"],
        }

    support["optional_blocks_with_rows"] = int(sum(int(item["rows_emitted"]) > 0 for item in support["by_block"].values()))
    support["optional_blocks_without_rows"] = int(
        max(int(support["optional_blocks_total"]) - int(support["optional_blocks_with_rows"]), 0)
    )
    aggregated_reasons = [
        str(item).strip()
        for block_doc in support["by_block"].values()
        for item in (block_doc.get("reason_codes") or [])
        if str(item).strip()
    ]
    if rows and int(support["optional_blocks_with_rows"]) == int(support["optional_blocks_total"]):
        support["status"] = "supported"
    elif rows:
        support["status"] = "partial"
    else:
        support["status"] = "insufficient"
    support["reason_codes"] = list(dict.fromkeys(aggregated_reasons))
    return {"rows": rows, "support": support}


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
    evidence = _evaluate_factor_block_refit_window_evidence(
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
    return list(evidence["rows"])


def _summarize_walk_forward_trial_panel(
    windows: list[dict[str, Any]],
    *,
    threshold_key: str = "top_5pct",
) -> list[dict[str, Any]]:
    by_trial: dict[int, dict[str, Any]] = {}
    for window in windows:
        window_index = int(window.get("window_index", -1))
        time_window = dict(window.get("time_window", {}))
        for record in window.get("trial_records", []) or []:
            trial_id = int(record.get("trial", -1))
            if trial_id < 0:
                continue
            node = by_trial.setdefault(
                trial_id,
                {
                    "trial": trial_id,
                    "params": dict(record.get("params", {})),
                    "windows": [],
                },
            )
            node["windows"].append(
                {
                    "window_index": window_index,
                    "time_window": time_window,
                    "metrics": dict(record.get("test_metrics", {})),
                    "oos_periods": list(record.get("test_oos_periods", []) or []),
                    "oos_slices": list(record.get("test_oos_slices", []) or []),
                }
            )
            period_rows = node.setdefault("oos_periods", [])
            for period_doc in record.get("test_oos_periods", []) or []:
                if not isinstance(period_doc, dict):
                    continue
                period_rows.append(
                    {
                        "window_index": window_index,
                        "period_index": int(period_doc.get("period_index", -1)),
                        "ts_ms": int(period_doc.get("ts_ms", -1)),
                        "metrics": dict(period_doc.get("metrics", {})),
                    }
                )
            slice_rows = node.setdefault("oos_slices", [])
            for slice_doc in record.get("test_oos_slices", []) or []:
                if not isinstance(slice_doc, dict):
                    continue
                slice_rows.append(
                    {
                        "window_index": window_index,
                        "slice_index": int(slice_doc.get("slice_index", -1)),
                        "slice_key": str(slice_doc.get("slice_key", "")),
                        "metrics": dict(slice_doc.get("metrics", {})),
                    }
                )
    trial_panel: list[dict[str, Any]] = []
    for trial_id in sorted(by_trial):
        node = by_trial[trial_id]
        windows_for_trial = list(node.get("windows", []))
        summary = summarize_walk_forward_windows(windows_for_trial, threshold_key=threshold_key)
        node["summary"] = summary
        node["selected_threshold_key"] = str(threshold_key).strip() or "top_5pct"
        node["windows_run"] = int(summary.get("windows_run", 0) or 0)
        node["oos_period_count"] = len(node.get("oos_periods", []) or [])
        node["oos_slice_count"] = len(node.get("oos_slices", []) or [])
        trial_panel.append(node)
    return trial_panel


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
    xgb = _try_import_xgboost()
    if xgb is None:
        raise RuntimeError("xgboost is required for trainer=v4_crypto_cs")

    rng = np.random.default_rng(int(options.seed))
    w_train_safe = np.asarray(w_train, dtype=np.float64)
    w_valid_safe = np.asarray(w_valid, dtype=np.float64)
    if w_train_safe.size != y_train.size:
        w_train_safe = np.ones(y_train.size, dtype=np.float64)
    if w_valid_safe.size != y_valid_cls.size:
        w_valid_safe = np.ones(y_valid_cls.size, dtype=np.float64)
    w_train_safe = np.clip(w_train_safe, 1e-6, None)
    w_valid_safe = np.clip(w_valid_safe, 1e-6, None)

    best_key: tuple[float, ...] | None = None
    best_bundle: dict[str, Any] | None = None
    best_params: dict[str, Any] | None = None
    trial_records: list[dict[str, Any]] = []
    economic_objective_profile_id = str(build_v4_shared_economic_objective_profile().get("profile_id", "")).strip()
    for trial in range(max(int(sweep_trials), 1)):
        params = _sample_xgb_params(rng)
        estimator = xgb.XGBClassifier(
            objective="binary:logistic",
            tree_method="hist",
            n_estimators=1200,
            learning_rate=params["learning_rate"],
            max_depth=params["max_depth"],
            subsample=params["subsample"],
            colsample_bytree=params["colsample_bytree"],
            min_child_weight=params["min_child_weight"],
            reg_lambda=params["reg_lambda"],
            reg_alpha=params["reg_alpha"],
            max_bin=params["max_bin"],
            random_state=int(options.seed + trial),
            nthread=max(int(options.nthread), 1),
            eval_metric="logloss",
        )
        fit_kwargs = {
            "sample_weight": w_train_safe,
            "eval_set": [(x_valid, y_valid_cls)],
            "sample_weight_eval_set": [w_valid_safe],
            "verbose": False,
        }
        try:
            estimator.fit(x_train, y_train, early_stopping_rounds=50, **fit_kwargs)
        except TypeError:
            estimator.fit(x_train, y_train, sample_weight=w_train_safe, eval_set=[(x_valid, y_valid_cls)], verbose=False)

        valid_scores = estimator.predict_proba(x_valid)[:, 1]
        valid_metrics = _evaluate_split(
            y_cls=y_valid_cls,
            y_reg=y_valid_reg,
            scores=valid_scores,
            markets=np.array(["_ALL_"] * int(y_valid_cls.size), dtype=object),
            fee_bps_est=options.fee_bps_est,
            safety_bps=options.safety_bps,
        )
        key = build_v4_trainer_sweep_sort_key(valid_metrics, task="cls")
        if best_key is None or key > best_key:
            best_key = key
            best_bundle = {"model_type": "xgboost", "scaler": None, "estimator": estimator}
            best_params = dict(params)

        test_scores = estimator.predict_proba(x_test)[:, 1]
        test_metrics = _evaluate_split(
            y_cls=y_test_cls,
            y_reg=y_test_reg,
            scores=test_scores,
            markets=market_test,
            fee_bps_est=options.fee_bps_est,
            safety_bps=options.safety_bps,
        )
        test_oos_periods = _build_oos_period_metrics(
            ts_ms=ts_test_ms,
            y_cls=y_test_cls,
            y_reg=y_test_reg,
            scores=test_scores,
            markets=market_test,
            fee_bps_est=options.fee_bps_est,
            safety_bps=options.safety_bps,
        )
        test_oos_slices = _build_oos_slice_metrics(
            ts_ms=ts_test_ms,
            y_cls=y_test_cls,
            y_reg=y_test_reg,
            scores=test_scores,
            markets=market_test,
            fee_bps_est=options.fee_bps_est,
            safety_bps=options.safety_bps,
        )
        trial_records.append(
            {
                "trial": int(trial),
                "params": params,
                "economic_objective_profile_id": economic_objective_profile_id,
                "valid_selection_key": _build_trial_selection_key(valid_metrics),
                "test_metrics": _compact_eval_metrics(test_metrics),
                "test_oos_periods": test_oos_periods,
                "test_oos_slices": test_oos_slices,
            }
        )
    if best_bundle is None:
        raise RuntimeError("walk-forward weighted sweep failed to produce a model")
    return {"bundle": best_bundle, "trial_records": trial_records, "best_params": dict(best_params or {})}


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
    xgb = _try_import_xgboost()
    if xgb is None:
        raise RuntimeError("xgboost is required for trainer=v4_crypto_cs")

    rng = np.random.default_rng(int(options.seed))
    w_train_safe = np.asarray(w_train, dtype=np.float64)
    w_valid_safe = np.asarray(w_valid, dtype=np.float64)
    if w_train_safe.size != y_train.size:
        w_train_safe = np.ones(y_train.size, dtype=np.float64)
    if w_valid_safe.size != y_valid_reg.size:
        w_valid_safe = np.ones(y_valid_reg.size, dtype=np.float64)
    w_train_safe = np.clip(w_train_safe, 1e-6, None)
    w_valid_safe = np.clip(w_valid_safe, 1e-6, None)

    best_key: tuple[float, ...] | None = None
    best_bundle: dict[str, Any] | None = None
    best_params: dict[str, Any] | None = None
    trial_records: list[dict[str, Any]] = []
    economic_objective_profile_id = str(build_v4_shared_economic_objective_profile().get("profile_id", "")).strip()
    for trial in range(max(int(sweep_trials), 1)):
        params = _sample_xgb_params(rng)
        estimator = xgb.XGBRegressor(
            objective="reg:squarederror",
            tree_method="hist",
            n_estimators=1200,
            learning_rate=params["learning_rate"],
            max_depth=params["max_depth"],
            subsample=params["subsample"],
            colsample_bytree=params["colsample_bytree"],
            min_child_weight=params["min_child_weight"],
            reg_lambda=params["reg_lambda"],
            reg_alpha=params["reg_alpha"],
            max_bin=params["max_bin"],
            random_state=int(options.seed + trial),
            nthread=max(int(options.nthread), 1),
            eval_metric="rmse",
        )
        fit_kwargs = {
            "sample_weight": w_train_safe,
            "eval_set": [(x_valid, y_valid_reg)],
            "sample_weight_eval_set": [w_valid_safe],
            "verbose": False,
        }
        try:
            estimator.fit(x_train, y_train, early_stopping_rounds=50, **fit_kwargs)
        except TypeError:
            estimator.fit(x_train, y_train, sample_weight=w_train_safe, eval_set=[(x_valid, y_valid_reg)], verbose=False)

        valid_scores = 1.0 / (1.0 + np.exp(-np.asarray(estimator.predict(x_valid), dtype=np.float64)))
        valid_metrics = _evaluate_split(
            y_cls=y_valid_cls,
            y_reg=y_valid_reg,
            scores=valid_scores,
            markets=np.array(["_ALL_"] * int(y_valid_cls.size), dtype=object),
            fee_bps_est=options.fee_bps_est,
            safety_bps=options.safety_bps,
        )
        key = build_v4_trainer_sweep_sort_key(valid_metrics, task="reg")
        if best_key is None or key > best_key:
            best_key = key
            best_bundle = {"model_type": "xgboost_regressor", "scaler": None, "estimator": estimator}
            best_params = dict(params)

        test_scores = 1.0 / (1.0 + np.exp(-np.asarray(estimator.predict(x_test), dtype=np.float64)))
        test_metrics = _evaluate_split(
            y_cls=y_test_cls,
            y_reg=y_test_reg,
            scores=test_scores,
            markets=market_test,
            fee_bps_est=options.fee_bps_est,
            safety_bps=options.safety_bps,
        )
        test_oos_periods = _build_oos_period_metrics(
            ts_ms=ts_test_ms,
            y_cls=y_test_cls,
            y_reg=y_test_reg,
            scores=test_scores,
            markets=market_test,
            fee_bps_est=options.fee_bps_est,
            safety_bps=options.safety_bps,
        )
        test_oos_slices = _build_oos_slice_metrics(
            ts_ms=ts_test_ms,
            y_cls=y_test_cls,
            y_reg=y_test_reg,
            scores=test_scores,
            markets=market_test,
            fee_bps_est=options.fee_bps_est,
            safety_bps=options.safety_bps,
        )
        trial_records.append(
            {
                "trial": int(trial),
                "params": params,
                "economic_objective_profile_id": economic_objective_profile_id,
                "valid_selection_key": _build_trial_selection_key(valid_metrics),
                "test_metrics": _compact_eval_metrics(test_metrics),
                "test_oos_periods": test_oos_periods,
                "test_oos_slices": test_oos_slices,
            }
        )
    if best_bundle is None:
        raise RuntimeError("walk-forward regression sweep failed to produce a model")
    return {"bundle": best_bundle, "trial_records": trial_records, "best_params": dict(best_params or {})}


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
    xgb = _try_import_xgboost()
    if xgb is None:
        raise RuntimeError("xgboost is required for trainer=v4_crypto_cs")

    rng = np.random.default_rng(int(options.seed))
    train_group = _group_counts_by_ts(ts_train_ms)
    valid_group = _group_counts_by_ts(ts_valid_ms)
    if train_group.size <= 0 or valid_group.size <= 0:
        raise RuntimeError("walk-forward ranker requires timestamp-grouped train and valid rows")

    w_train_safe = np.asarray(w_train, dtype=np.float64)
    if w_train_safe.size != y_train_rank.size:
        w_train_safe = np.ones(y_train_rank.size, dtype=np.float64)
    w_train_safe = np.clip(w_train_safe, 1e-6, None)

    best_key: tuple[float, ...] | None = None
    best_bundle: dict[str, Any] | None = None
    best_params: dict[str, Any] | None = None
    trial_records: list[dict[str, Any]] = []
    economic_objective_profile_id = str(build_v4_shared_economic_objective_profile().get("profile_id", "")).strip()
    for trial in range(max(int(sweep_trials), 1)):
        params = _sample_xgb_params(rng)
        estimator = xgb.XGBRanker(
            objective="rank:pairwise",
            tree_method="hist",
            n_estimators=900,
            learning_rate=params["learning_rate"],
            max_depth=params["max_depth"],
            subsample=params["subsample"],
            colsample_bytree=params["colsample_bytree"],
            min_child_weight=params["min_child_weight"],
            reg_lambda=params["reg_lambda"],
            reg_alpha=params["reg_alpha"],
            max_bin=params["max_bin"],
            random_state=int(options.seed + trial),
            nthread=max(int(options.nthread), 1),
            eval_metric="ndcg@5",
        )
        fit_kwargs = {
            "group": train_group.tolist(),
            "sample_weight": w_train_safe,
            "eval_set": [(x_valid, np.nan_to_num(np.asarray(y_valid_rank, dtype=np.float32), nan=0.0))],
            "eval_group": [valid_group.tolist()],
            "verbose": False,
        }
        try:
            estimator.fit(
                x_train,
                np.nan_to_num(np.asarray(y_train_rank, dtype=np.float32), nan=0.0),
                early_stopping_rounds=50,
                **fit_kwargs,
            )
        except TypeError:
            estimator.fit(x_train, np.nan_to_num(np.asarray(y_train_rank, dtype=np.float32), nan=0.0), **fit_kwargs)

        valid_scores = 1.0 / (1.0 + np.exp(-np.asarray(estimator.predict(x_valid), dtype=np.float64)))
        valid_metrics = _attach_ranking_metrics(
            metrics=_evaluate_split(
                y_cls=y_valid_cls,
                y_reg=y_valid_reg,
                scores=valid_scores,
                markets=np.array(["_ALL_"] * int(y_valid_cls.size), dtype=object),
                fee_bps_est=options.fee_bps_est,
                safety_bps=options.safety_bps,
            ),
            y_rank=y_valid_rank,
            ts_ms=ts_valid_ms,
            scores=valid_scores,
        )
        key = build_v4_trainer_sweep_sort_key(valid_metrics, task="rank")
        if best_key is None or key > best_key:
            best_key = key
            best_bundle = {"model_type": "xgboost_ranker", "scaler": None, "estimator": estimator}
            best_params = dict(params)

        test_scores = 1.0 / (1.0 + np.exp(-np.asarray(estimator.predict(x_test), dtype=np.float64)))
        test_metrics = _attach_ranking_metrics(
            metrics=_evaluate_split(
                y_cls=y_test_cls,
                y_reg=y_test_reg,
                scores=test_scores,
                markets=market_test,
                fee_bps_est=options.fee_bps_est,
                safety_bps=options.safety_bps,
            ),
            y_rank=y_test_rank,
            ts_ms=ts_test_ms,
            scores=test_scores,
        )
        test_oos_periods = _build_oos_period_metrics(
            ts_ms=ts_test_ms,
            y_cls=y_test_cls,
            y_reg=y_test_reg,
            scores=test_scores,
            markets=market_test,
            fee_bps_est=options.fee_bps_est,
            safety_bps=options.safety_bps,
        )
        test_oos_slices = _build_oos_slice_metrics(
            ts_ms=ts_test_ms,
            y_cls=y_test_cls,
            y_reg=y_test_reg,
            scores=test_scores,
            markets=market_test,
            fee_bps_est=options.fee_bps_est,
            safety_bps=options.safety_bps,
        )
        trial_records.append(
            {
                "trial": int(trial),
                "params": params,
                "economic_objective_profile_id": economic_objective_profile_id,
                "objective": "rank:pairwise",
                "grouping": {"query_key": "ts_ms"},
                "valid_selection_key": _build_trial_selection_key(valid_metrics),
                "test_metrics": _compact_eval_metrics(test_metrics),
                "test_oos_periods": test_oos_periods,
                "test_oos_slices": test_oos_slices,
            }
        )
    if best_bundle is None:
        raise RuntimeError("walk-forward ranker sweep failed to produce a model")
    return {"bundle": best_bundle, "trial_records": trial_records, "best_params": dict(best_params or {})}


def _compact_eval_metrics(metrics: dict[str, Any]) -> dict[str, Any]:
    cls = metrics.get("classification", {}) if isinstance(metrics, dict) else {}
    ranking = metrics.get("ranking", {}) if isinstance(metrics, dict) else {}
    summary = metrics.get("per_market_summary", {}) if isinstance(metrics, dict) else {}
    return {
        "rows": int(metrics.get("rows", 0)) if isinstance(metrics, dict) else 0,
        "classification": {
            "roc_auc": _safe_float(cls.get("roc_auc")),
            "pr_auc": _safe_float(cls.get("pr_auc")),
            "log_loss": _safe_float(cls.get("log_loss")),
            "brier_score": _safe_float(cls.get("brier_score")),
        },
        "ranking": {
            "ts_group_count": int(ranking.get("ts_group_count", 0) or 0),
            "eligible_group_count": int(ranking.get("eligible_group_count", 0) or 0),
            "ndcg_at_5_mean": _safe_float(ranking.get("ndcg_at_5_mean")),
            "ndcg_full_mean": _safe_float(ranking.get("ndcg_full_mean")),
            "top1_match_rate": _safe_float(ranking.get("top1_match_rate")),
        },
        "trading": _compact_trading_metrics(metrics),
        "per_market_summary": {
            "market_count": int(summary.get("market_count", 0) or 0),
            "positive_markets": int(summary.get("positive_markets", 0) or 0),
        },
    }


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
    ts_values = np.asarray(ts_ms, dtype=np.int64)
    if ts_values.size <= 0:
        return []
    unique_ts = np.unique(ts_values)
    if unique_ts.size <= 0:
        return []
    slice_count = max(1, min(int(max_slices), int(unique_ts.size)))
    ts_groups = np.array_split(unique_ts, slice_count)
    slices: list[dict[str, Any]] = []
    for slice_index, ts_group in enumerate(ts_groups):
        if ts_group.size <= 0:
            continue
        mask = np.isin(ts_values, ts_group)
        if not np.any(mask):
            continue
        slice_metrics = _evaluate_split(
            y_cls=y_cls[mask],
            y_reg=y_reg[mask],
            scores=scores[mask],
            markets=markets[mask],
            fee_bps_est=fee_bps_est,
            safety_bps=safety_bps,
        )
        start_ts = int(ts_group[0])
        end_ts = int(ts_group[-1])
        slices.append(
            {
                "slice_index": int(slice_index),
                "slice_key": f"{slice_index}:{start_ts}:{end_ts}",
                "start_ts": start_ts,
                "end_ts": end_ts,
                "rows": int(np.sum(mask)),
                "ts_count": int(ts_group.size),
                "metrics": _compact_eval_metrics(slice_metrics),
            }
        )
    return slices


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
    ts_values = np.asarray(ts_ms, dtype=np.int64)
    if ts_values.size <= 0:
        return []
    unique_ts = np.unique(ts_values)
    periods: list[dict[str, Any]] = []
    for period_index, ts_value in enumerate(unique_ts):
        mask = ts_values == int(ts_value)
        if not np.any(mask):
            continue
        period_metrics = _evaluate_split(
            y_cls=y_cls[mask],
            y_reg=y_reg[mask],
            scores=scores[mask],
            markets=markets[mask],
            fee_bps_est=fee_bps_est,
            safety_bps=safety_bps,
        )
        periods.append(
            {
                "period_index": int(period_index),
                "ts_ms": int(ts_value),
                "rows": int(np.sum(mask)),
                "metrics": _compact_eval_metrics(period_metrics),
            }
        )
    return periods


def _load_champion_walk_forward_report(*, options: TrainV4CryptoCsOptions) -> dict[str, Any] | None:
    champion_doc = load_json(options.registry_root / options.model_family / "champion.json")
    champion_run_id = str(champion_doc.get("run_id", "")).strip()
    if not champion_run_id:
        return None
    run_dir = options.registry_root / options.model_family / champion_run_id
    walk_forward = load_json(run_dir / "walk_forward_report.json")
    if isinstance(walk_forward.get("summary"), dict) and walk_forward.get("summary"):
        return dict(walk_forward)
    metrics = load_json(run_dir / "metrics.json")
    summary = metrics.get("walk_forward") if isinstance(metrics, dict) else None
    if isinstance(summary, dict) and summary:
        return {
            "summary": dict(summary),
            "windows": [],
            "compare_to_champion": {},
            "spa_like_window_test": {},
        }
    return None


def _sha256_file(path: Path) -> str:
    if not path.is_file():
        return ""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _detect_duplicate_candidate_artifacts(
    *,
    options: TrainV4CryptoCsOptions,
    run_id: str,
    run_dir: Path,
) -> dict[str, Any]:
    champion_doc = load_json(options.registry_root / options.model_family / "champion.json")
    champion_run_id = str(champion_doc.get("run_id", "")).strip()
    candidate_model_path = run_dir / "model.bin"
    candidate_thresholds_path = run_dir / "thresholds.json"
    champion_run_dir = options.registry_root / options.model_family / champion_run_id if champion_run_id else Path("")
    champion_model_path = champion_run_dir / "model.bin" if champion_run_id else Path("")
    champion_thresholds_path = champion_run_dir / "thresholds.json" if champion_run_id else Path("")

    duplicate = False
    if champion_run_id and candidate_model_path.is_file() and candidate_thresholds_path.is_file():
        duplicate = (
            champion_model_path.is_file()
            and champion_thresholds_path.is_file()
            and _sha256_file(candidate_model_path) == _sha256_file(champion_model_path)
            and _sha256_file(candidate_thresholds_path) == _sha256_file(champion_thresholds_path)
        )

    reasons: list[str] = []
    if not champion_run_id:
        reasons.append("NO_EXISTING_CHAMPION")
    elif duplicate:
        reasons.append("ARTIFACT_HASH_MATCH")
    return {
        "evaluated": bool(champion_run_id),
        "duplicate": duplicate,
        "basis": "model_bin_and_thresholds_sha256",
        "candidate_ref": run_id,
        "champion_ref": champion_run_id,
        "candidate": {
            "run_dir": str(run_dir),
            "model_bin_path": str(candidate_model_path),
            "model_bin_sha256": _sha256_file(candidate_model_path),
            "thresholds_path": str(candidate_thresholds_path),
            "thresholds_sha256": _sha256_file(candidate_thresholds_path),
        },
        "champion": {
            "run_dir": str(champion_run_dir) if champion_run_id else "",
            "model_bin_path": str(champion_model_path) if champion_run_id else "",
            "model_bin_sha256": _sha256_file(champion_model_path) if champion_run_id else "",
            "thresholds_path": str(champion_thresholds_path) if champion_run_id else "",
            "thresholds_sha256": _sha256_file(champion_thresholds_path) if champion_run_id else "",
        },
        "reasons": reasons,
    }


def _finalize_walk_forward_report(
    *,
    walk_forward: dict[str, Any],
    selection_recommendations: dict[str, Any],
    options: TrainV4CryptoCsOptions,
) -> dict[str, Any]:
    report = dict(walk_forward)
    selected_threshold_key, threshold_key_source = _resolve_selection_recommendation_threshold_key(
        selection_recommendations=selection_recommendations,
    )
    report["selected_threshold_key"] = selected_threshold_key
    report["selected_threshold_key_source"] = threshold_key_source
    windows = report.get("windows", []) if isinstance(report.get("windows"), list) else []
    base_trial_panel = _summarize_walk_forward_trial_panel(windows, threshold_key=selected_threshold_key)
    selection_trial_panel = _build_selection_search_trial_panel(
        windows=windows,
        start_trial_id=max([int(row.get("trial", -1)) for row in base_trial_panel] + [-1]) + 1,
    )
    report["trial_panel"] = base_trial_panel + selection_trial_panel
    report["selection_search_trial_count"] = len(selection_trial_panel)
    report["summary"] = summarize_walk_forward_windows(windows, threshold_key=selected_threshold_key)

    champion_report = _load_champion_walk_forward_report(options=options)
    champion_summary = champion_report.get("summary", {}) if isinstance(champion_report, dict) else {}
    champion_threshold_key = _resolve_walk_forward_report_threshold_key(
        champion_report,
        fallback_threshold_key=selected_threshold_key,
    )
    report["compare_to_champion"] = compare_balanced_pareto(report["summary"], champion_summary or {})
    report["spa_like_window_test"] = compare_spa_like_window_test(
        report.get("windows", []),
        champion_report.get("windows", []) if isinstance(champion_report, dict) else [],
        candidate_threshold_key=selected_threshold_key,
        champion_threshold_key=champion_threshold_key,
    )
    multiple_testing_panel_diagnostics = build_trial_window_differential_diagnostics(
        report.get("trial_panel", []),
        champion_report.get("windows", []) if isinstance(champion_report, dict) else [],
        champion_threshold_key=champion_threshold_key,
    )
    report["multiple_testing_panel_diagnostics"] = multiple_testing_panel_diagnostics
    rc_matrix = build_trial_window_differential_matrix(
        report.get("trial_panel", []),
        champion_report.get("windows", []) if isinstance(champion_report, dict) else [],
        champion_threshold_key=champion_threshold_key,
    )
    report["white_reality_check"] = run_white_reality_check(
        rc_matrix,
        alpha=float(options.multiple_testing_alpha),
        bootstrap_iters=max(int(options.multiple_testing_bootstrap_iters), 100),
        seed=int(options.seed),
        average_block_length=(int(options.multiple_testing_block_length) if int(options.multiple_testing_block_length) > 0 else None),
        diagnostics=multiple_testing_panel_diagnostics,
    )
    report["hansen_spa"] = run_hansen_spa(
        rc_matrix,
        alpha=float(options.multiple_testing_alpha),
        bootstrap_iters=max(int(options.multiple_testing_bootstrap_iters), 100),
        seed=int(options.seed),
        average_block_length=(int(options.multiple_testing_block_length) if int(options.multiple_testing_block_length) > 0 else None),
        diagnostics=multiple_testing_panel_diagnostics,
    )
    if champion_summary:
        report["champion_summary"] = champion_summary
        report["champion_selected_threshold_key"] = champion_threshold_key
    return report


def _resolve_selection_recommendation_threshold_key(
    *,
    selection_recommendations: dict[str, Any],
) -> tuple[str, str]:
    threshold_key = str(selection_recommendations.get("recommended_threshold_key", "")).strip()
    if threshold_key:
        return threshold_key, str(selection_recommendations.get("recommended_threshold_key_source", "walk_forward_objective_optimizer"))
    return "top_5pct", "manual_fallback"


def _build_duplicate_candidate_execution_acceptance(
    *,
    run_id: str,
    duplicate_artifacts: dict[str, Any],
) -> dict[str, Any]:
    return {
        "candidate_ref": run_id,
        "champion_ref": str(duplicate_artifacts.get("champion_ref", "")),
        "status": "skipped",
        "policy": "duplicate_candidate_short_circuit",
        "reason": "DUPLICATE_CANDIDATE",
        "duplicate_candidate": True,
        "duplicate_artifacts": duplicate_artifacts,
    }


def _build_duplicate_candidate_runtime_recommendations(
    *,
    run_id: str,
    duplicate_artifacts: dict[str, Any],
) -> dict[str, Any]:
    return {
        "candidate_ref": run_id,
        "created_at_utc": _utc_now(),
        "status": "skipped",
        "reason": "DUPLICATE_CANDIDATE",
        "duplicate_candidate": True,
        "duplicate_artifacts": duplicate_artifacts,
    }


def _purge_execution_artifact_run_dirs(
    *,
    output_root: Path,
    execution_acceptance: dict[str, Any] | None,
    runtime_recommendations: dict[str, Any] | None,
) -> dict[str, Any]:
    allowed_root = (Path(output_root) / "runs").resolve()
    discovered = sorted(
        {
            run_dir
            for run_dir in _collect_reported_run_dirs(execution_acceptance)
            + _collect_reported_run_dirs(runtime_recommendations)
            if run_dir
        }
    )
    payload: dict[str, Any] = {
        "evaluated": True,
        "allowed_root": str(allowed_root),
        "discovered_run_dirs": discovered,
        "removed_paths": [],
        "missing_paths": [],
        "skipped_outside_root": [],
    }
    for raw_path in discovered:
        candidate = Path(raw_path)
        try:
            resolved = candidate.resolve()
        except OSError:
            payload["missing_paths"].append(str(candidate))
            continue
        try:
            resolved.relative_to(allowed_root)
        except ValueError:
            payload["skipped_outside_root"].append(str(resolved))
            continue
        if not resolved.exists():
            payload["missing_paths"].append(str(resolved))
            continue
        if not resolved.is_dir():
            payload["missing_paths"].append(str(resolved))
            continue
        shutil.rmtree(resolved, ignore_errors=False)
        payload["removed_paths"].append(str(resolved))
    payload["removed_count"] = len(payload["removed_paths"])
    payload["missing_count"] = len(payload["missing_paths"])
    payload["skipped_count"] = len(payload["skipped_outside_root"])
    return payload


def _collect_reported_run_dirs(payload: dict[str, Any] | None) -> list[str]:
    found: list[str] = []

    def _visit(value: Any) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                if key == "run_dir":
                    text = str(child).strip()
                    if text:
                        found.append(text)
                _visit(child)
            return
        if isinstance(value, list):
            for child in value:
                _visit(child)

    _visit(payload if isinstance(payload, dict) else {})
    return found


def _resolve_walk_forward_report_threshold_key(
    walk_forward_report: dict[str, Any] | None,
    *,
    fallback_threshold_key: str = "top_5pct",
) -> str:
    report = dict(walk_forward_report or {})
    threshold_key = str(report.get("selected_threshold_key", "")).strip()
    if threshold_key:
        return threshold_key
    summary = report.get("summary")
    if isinstance(summary, dict):
        threshold_key = str(summary.get("selected_threshold_key", "")).strip()
        if threshold_key:
            return threshold_key
    return str(fallback_threshold_key).strip() or "top_5pct"


def _build_selection_search_trial_panel(
    *,
    windows: list[dict[str, Any]],
    start_trial_id: int,
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, float, int], list[dict[str, Any]]] = {}
    for window in windows:
        if not isinstance(window, dict):
            continue
        window_index = int(window.get("window_index", -1))
        if window_index < 0:
            continue
        selection_optimization = window.get("selection_optimization")
        by_key = selection_optimization.get("by_threshold_key") if isinstance(selection_optimization, dict) else None
        if not isinstance(by_key, dict):
            continue
        for threshold_key, threshold_doc in by_key.items():
            grid_results = threshold_doc.get("grid_results") if isinstance(threshold_doc, dict) else None
            if not isinstance(grid_results, list):
                continue
            for row in grid_results:
                if not isinstance(row, dict):
                    continue
                try:
                    grid_key = (
                        str(threshold_key),
                        float(row.get("top_pct", 0.0)),
                        int(row.get("min_candidates_per_ts", 0)),
                    )
                except Exception:
                    continue
                grouped.setdefault(grid_key, []).append(
                    {
                        "window": dict(window),
                        "window_index": window_index,
                        "ev_net": _safe_float(row.get("ev_net")),
                        "selected_rows": int(row.get("selected_rows", 0) or 0),
                        "period_results": [
                            dict(period_doc)
                            for period_doc in (row.get("period_results") or [])
                            if isinstance(period_doc, dict)
                        ],
                    }
                )

    panel: list[dict[str, Any]] = []
    trial_id = max(int(start_trial_id), 0)
    for (threshold_key, top_pct, min_candidates), rows in sorted(grouped.items()):
        windows_for_trial = [
            {
                "window_index": int(row["window_index"]),
                "metrics": {
                    "trading": {
                        str(threshold_key): {
                            "ev_net": float(row["ev_net"]),
                            "selected_rows": int(row["selected_rows"]),
                        }
                    }
                },
                "panel_source": "oos_periods"
                if any(int(period_doc.get("period_index", -1)) >= 0 for period_doc in (row.get("period_results") or []))
                else "window_summary_only",
                "oos_periods": [
                    {
                        "period_index": int(period_doc.get("period_index", -1)),
                        "ts_ms": int(period_doc.get("ts_ms", -1)),
                        "metrics": {
                            "trading": {
                                str(threshold_key): {
                                    "ev_net": _safe_float(period_doc.get("ev_net")),
                                    "selected_rows": int(period_doc.get("selected_rows", 0) or 0),
                                }
                            }
                        },
                    }
                    for period_doc in (row.get("period_results") or [])
                    if isinstance(period_doc, dict)
                    and int(period_doc.get("period_index", -1)) >= 0
                    and int(period_doc.get("ts_ms", -1)) >= 0
                ],
                "oos_slices": [],
            }
            for row in rows
        ]
        panel.append(
            {
                "trial": int(trial_id),
                "trial_type": "selection_grid",
                "selected_threshold_key": str(threshold_key),
                "params": {
                    "threshold_key": str(threshold_key),
                    "top_pct": float(top_pct),
                    "min_candidates_per_ts": int(min_candidates),
                },
                "windows": windows_for_trial,
                "summary": summarize_walk_forward_windows(windows_for_trial, threshold_key=str(threshold_key)),
                "windows_run": len(windows_for_trial),
                "oos_slice_count": 0,
            }
        )
        trial_id += 1
    return panel


def _compact_trading_metrics(metrics: dict[str, Any]) -> dict[str, Any]:
    trading = metrics.get("trading", {}) if isinstance(metrics, dict) else {}
    if not isinstance(trading, dict):
        return {}
    compact: dict[str, Any] = {}
    for threshold_key, threshold_doc in trading.items():
        if not isinstance(threshold_doc, dict):
            continue
        compact[str(threshold_key)] = {
            "precision": _safe_float(threshold_doc.get("precision")),
            "ev_net": _safe_float(threshold_doc.get("ev_net")),
            "selected_rows": int(threshold_doc.get("selected_rows", 0) or 0),
        }
    return compact


def _build_trial_selection_key(metrics: dict[str, Any]) -> dict[str, float]:
    trading = metrics.get("trading", {}) if isinstance(metrics, dict) else {}
    cls = metrics.get("classification", {}) if isinstance(metrics, dict) else {}
    ranking = metrics.get("ranking", {}) if isinstance(metrics, dict) else {}
    selection_key: dict[str, float] = {}
    if isinstance(trading, dict):
        for threshold_key, threshold_doc in trading.items():
            if not isinstance(threshold_doc, dict):
                continue
            key_root = str(threshold_key).strip().lower()
            selection_key[f"precision_{key_root}"] = _safe_float(threshold_doc.get("precision"))
            selection_key[f"ev_net_{key_root}"] = _safe_float(threshold_doc.get("ev_net"))
            selection_key[f"selected_rows_{key_root}"] = float(int(threshold_doc.get("selected_rows", 0) or 0))
    selection_key["pr_auc"] = _safe_float(cls.get("pr_auc"))
    selection_key["roc_auc"] = _safe_float(cls.get("roc_auc"))
    selection_key["log_loss"] = _safe_float(cls.get("log_loss"))
    selection_key["brier_score"] = _safe_float(cls.get("brier_score"))
    selection_key["ndcg_at_5_mean"] = _safe_float(ranking.get("ndcg_at_5_mean"))
    selection_key["ndcg_full_mean"] = _safe_float(ranking.get("ndcg_full_mean"))
    selection_key["top1_match_rate"] = _safe_float(ranking.get("top1_match_rate"))
    return selection_key


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
) -> dict[str, Any]:
    xgb = _try_import_xgboost()
    if xgb is None:
        raise RuntimeError("xgboost is required for trainer=v4_crypto_cs")

    rng = np.random.default_rng(int(seed))
    trial_rows: list[dict[str, Any]] = []
    best_key: tuple[float, ...] | None = None
    best_bundle: dict[str, Any] | None = None
    best_params: dict[str, Any] = {}
    economic_objective_profile_id = str(build_v4_shared_economic_objective_profile().get("profile_id", "")).strip()

    w_train_safe = np.asarray(w_train, dtype=np.float64)
    w_valid_safe = np.asarray(w_valid, dtype=np.float64)
    if w_train_safe.size != y_train.size:
        w_train_safe = np.ones(y_train.size, dtype=np.float64)
    if w_valid_safe.size != y_valid.size:
        w_valid_safe = np.ones(y_valid.size, dtype=np.float64)
    w_train_safe = np.clip(w_train_safe, 1e-6, None)
    w_valid_safe = np.clip(w_valid_safe, 1e-6, None)

    for trial in range(max(int(trials), 1)):
        params = _sample_xgb_params(rng)
        estimator = xgb.XGBClassifier(
            objective="binary:logistic",
            tree_method="hist",
            n_estimators=1200,
            learning_rate=params["learning_rate"],
            max_depth=params["max_depth"],
            subsample=params["subsample"],
            colsample_bytree=params["colsample_bytree"],
            min_child_weight=params["min_child_weight"],
            reg_lambda=params["reg_lambda"],
            reg_alpha=params["reg_alpha"],
            max_bin=params["max_bin"],
            random_state=int(seed + trial),
            nthread=max(int(nthread), 1),
            eval_metric="logloss",
        )
        fit_kwargs = {
            "sample_weight": w_train_safe,
            "eval_set": [(x_valid, y_valid)],
            "sample_weight_eval_set": [w_valid_safe],
            "verbose": False,
        }
        try:
            estimator.fit(x_train, y_train, early_stopping_rounds=50, **fit_kwargs)
        except TypeError:
            estimator.fit(x_train, y_train, sample_weight=w_train_safe, eval_set=[(x_valid, y_valid)], verbose=False)

        valid_scores = estimator.predict_proba(x_valid)[:, 1]
        valid_metrics = _evaluate_split(
            y_cls=y_valid,
            y_reg=y_reg_valid,
            scores=valid_scores,
            markets=np.array(["_ALL_"] * int(y_valid.size), dtype=object),
            fee_bps_est=fee_bps_est,
            safety_bps=safety_bps,
        )
        key = build_v4_trainer_sweep_sort_key(valid_metrics, task="cls")
        trial_rows.append(
            {
                "trial": trial,
                "backend": "xgboost",
                "params": params,
                "economic_objective_profile_id": economic_objective_profile_id,
                "selection_key": {
                    "ev_net_top5": float(key[0]) if len(key) > 0 else 0.0,
                    "precision_top5": float(key[1]) if len(key) > 1 else 0.0,
                    "pr_auc": float(key[2]) if len(key) > 2 else 0.0,
                    "roc_auc": float(key[3]) if len(key) > 3 else 0.0,
                },
            }
        )
        if best_key is None or key > best_key:
            best_key = key
            best_params = params
            best_bundle = {"model_type": "xgboost", "scaler": None, "estimator": estimator}

    if best_bundle is None:
        raise RuntimeError("booster sweep failed to produce a model")
    return {
        "bundle": best_bundle,
        "best_params": best_params,
        "backend": "xgboost",
        "trials": trial_rows,
    }


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
) -> dict[str, Any]:
    xgb = _try_import_xgboost()
    if xgb is None:
        raise RuntimeError("xgboost is required for trainer=v4_crypto_cs")

    rng = np.random.default_rng(int(seed))
    trial_rows: list[dict[str, Any]] = []
    best_key: tuple[float, ...] | None = None
    best_bundle: dict[str, Any] | None = None
    best_params: dict[str, Any] = {}
    economic_objective_profile_id = str(build_v4_shared_economic_objective_profile().get("profile_id", "")).strip()

    w_train_safe = np.asarray(w_train, dtype=np.float64)
    w_valid_safe = np.asarray(w_valid, dtype=np.float64)
    if w_train_safe.size != y_train.size:
        w_train_safe = np.ones(y_train.size, dtype=np.float64)
    if w_valid_safe.size != y_valid_reg.size:
        w_valid_safe = np.ones(y_valid_reg.size, dtype=np.float64)
    w_train_safe = np.clip(w_train_safe, 1e-6, None)
    w_valid_safe = np.clip(w_valid_safe, 1e-6, None)

    for trial in range(max(int(trials), 1)):
        params = _sample_xgb_params(rng)
        estimator = xgb.XGBRegressor(
            objective="reg:squarederror",
            tree_method="hist",
            n_estimators=1200,
            learning_rate=params["learning_rate"],
            max_depth=params["max_depth"],
            subsample=params["subsample"],
            colsample_bytree=params["colsample_bytree"],
            min_child_weight=params["min_child_weight"],
            reg_lambda=params["reg_lambda"],
            reg_alpha=params["reg_alpha"],
            max_bin=params["max_bin"],
            random_state=int(seed + trial),
            nthread=max(int(nthread), 1),
            eval_metric="rmse",
        )
        fit_kwargs = {
            "sample_weight": w_train_safe,
            "eval_set": [(x_valid, y_valid_reg)],
            "sample_weight_eval_set": [w_valid_safe],
            "verbose": False,
        }
        try:
            estimator.fit(x_train, y_train, early_stopping_rounds=50, **fit_kwargs)
        except TypeError:
            estimator.fit(x_train, y_train, sample_weight=w_train_safe, eval_set=[(x_valid, y_valid_reg)], verbose=False)

        valid_scores = 1.0 / (1.0 + np.exp(-np.asarray(estimator.predict(x_valid), dtype=np.float64)))
        valid_metrics = _evaluate_split(
            y_cls=y_valid_cls,
            y_reg=y_valid_reg,
            scores=valid_scores,
            markets=np.array(["_ALL_"] * int(y_valid_cls.size), dtype=object),
            fee_bps_est=fee_bps_est,
            safety_bps=safety_bps,
        )
        key = build_v4_trainer_sweep_sort_key(valid_metrics, task="reg")
        trial_rows.append(
            {
                "trial": trial,
                "backend": "xgboost_regression",
                "params": params,
                "economic_objective_profile_id": economic_objective_profile_id,
                "selection_key": {
                    "ev_net_top5": float(key[0]) if len(key) > 0 else 0.0,
                    "precision_top5": float(key[1]) if len(key) > 1 else 0.0,
                    "pr_auc": float(key[2]) if len(key) > 2 else 0.0,
                    "roc_auc": float(key[3]) if len(key) > 3 else 0.0,
                },
            }
        )
        if best_key is None or key > best_key:
            best_key = key
            best_params = params
            best_bundle = {"model_type": "xgboost_regressor", "scaler": None, "estimator": estimator}

    if best_bundle is None:
        raise RuntimeError("regression booster sweep failed to produce a model")
    return {
        "bundle": best_bundle,
        "best_params": best_params,
        "backend": "xgboost_regression",
        "trials": trial_rows,
    }


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
) -> dict[str, Any]:
    xgb = _try_import_xgboost()
    if xgb is None:
        raise RuntimeError("xgboost is required for trainer=v4_crypto_cs")

    rng = np.random.default_rng(int(seed))
    trial_rows: list[dict[str, Any]] = []
    best_key: tuple[float, ...] | None = None
    best_bundle: dict[str, Any] | None = None
    best_params: dict[str, Any] = {}
    economic_objective_profile_id = str(build_v4_shared_economic_objective_profile().get("profile_id", "")).strip()

    train_group = _group_counts_by_ts(ts_train_ms)
    valid_group = _group_counts_by_ts(ts_valid_ms)
    if train_group.size <= 0 or valid_group.size <= 0:
        raise RuntimeError("ranker lane requires at least one timestamp-group in train and valid splits")

    y_train = np.nan_to_num(np.asarray(y_train_rank, dtype=np.float32), nan=0.0, posinf=0.0, neginf=0.0)
    w_train_safe = np.asarray(w_train, dtype=np.float64)
    if w_train_safe.size != y_train.size:
        w_train_safe = np.ones(y_train.size, dtype=np.float64)
    w_train_safe = np.clip(w_train_safe, 1e-6, None)

    for trial in range(max(int(trials), 1)):
        params = _sample_xgb_params(rng)
        estimator = xgb.XGBRanker(
            objective="rank:pairwise",
            tree_method="hist",
            n_estimators=900,
            learning_rate=params["learning_rate"],
            max_depth=params["max_depth"],
            subsample=params["subsample"],
            colsample_bytree=params["colsample_bytree"],
            min_child_weight=params["min_child_weight"],
            reg_lambda=params["reg_lambda"],
            reg_alpha=params["reg_alpha"],
            max_bin=params["max_bin"],
            random_state=int(seed + trial),
            nthread=max(int(nthread), 1),
            eval_metric="ndcg@5",
        )
        fit_kwargs = {
            "group": train_group.tolist(),
            "sample_weight": w_train_safe,
            "eval_set": [(x_valid, np.nan_to_num(np.asarray(y_valid_rank, dtype=np.float32), nan=0.0))],
            "eval_group": [valid_group.tolist()],
            "verbose": False,
        }
        try:
            estimator.fit(x_train, y_train, early_stopping_rounds=50, **fit_kwargs)
        except TypeError:
            estimator.fit(x_train, y_train, **fit_kwargs)

        valid_scores = 1.0 / (1.0 + np.exp(-np.asarray(estimator.predict(x_valid), dtype=np.float64)))
        valid_metrics = _attach_ranking_metrics(
            metrics=_evaluate_split(
                y_cls=y_valid_cls,
                y_reg=y_valid_reg,
                scores=valid_scores,
                markets=np.array(["_ALL_"] * int(y_valid_cls.size), dtype=object),
                fee_bps_est=fee_bps_est,
                safety_bps=safety_bps,
            ),
            y_rank=y_valid_rank,
            ts_ms=ts_valid_ms,
            scores=valid_scores,
        )
        key = build_v4_trainer_sweep_sort_key(valid_metrics, task="rank")
        trial_rows.append(
            {
                "trial": trial,
                "backend": "xgboost_ranker",
                "objective": "rank:pairwise",
                "grouping": {"query_key": "ts_ms"},
                "params": params,
                "economic_objective_profile_id": economic_objective_profile_id,
                "selection_key": {
                    "ev_net_top5": float(key[0]) if len(key) > 0 else 0.0,
                    "ndcg_at_5_mean": float(key[1]) if len(key) > 1 else 0.0,
                    "top1_match_rate": float(key[2]) if len(key) > 2 else 0.0,
                },
            }
        )
        if best_key is None or key > best_key:
            best_key = key
            best_params = params
            best_bundle = {"model_type": "xgboost_ranker", "scaler": None, "estimator": estimator}

    if best_bundle is None:
        raise RuntimeError("ranker booster sweep failed to produce a model")
    return {
        "bundle": best_bundle,
        "best_params": best_params,
        "backend": "xgboost_ranker",
        "objective": "rank:pairwise",
        "grouping": {"query_key": "ts_ms"},
        "trials": trial_rows,
    }


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
    payload = dict(metrics) if isinstance(metrics, dict) else {}
    payload["ranking"] = _evaluate_ranking_metrics(
        y_rank=np.asarray(y_rank, dtype=np.float64),
        ts_ms=np.asarray(ts_ms, dtype=np.int64),
        scores=np.asarray(scores, dtype=np.float64),
    )
    return payload


def _evaluate_ranking_metrics(
    *,
    y_rank: np.ndarray,
    ts_ms: np.ndarray,
    scores: np.ndarray,
) -> dict[str, Any]:
    rank_values = np.nan_to_num(np.asarray(y_rank, dtype=np.float64), nan=0.0, posinf=0.0, neginf=0.0)
    ts_values = np.asarray(ts_ms, dtype=np.int64)
    score_values = np.asarray(scores, dtype=np.float64)
    if rank_values.size <= 0 or ts_values.size <= 0 or score_values.size <= 0:
        return {
            "ts_group_count": 0,
            "eligible_group_count": 0,
            "mean_group_size": 0.0,
            "min_group_size": 0,
            "max_group_size": 0,
            "ndcg_at_5_mean": 0.0,
            "ndcg_full_mean": 0.0,
            "top1_match_rate": 0.0,
        }

    ndcg_at5_values: list[float] = []
    ndcg_full_values: list[float] = []
    top1_matches = 0
    group_sizes: list[int] = []
    eligible_groups = 0
    for _, indices in _group_indices_by_ts(ts_values):
        if indices.size <= 0:
            continue
        group_sizes.append(int(indices.size))
        true_values = np.maximum(rank_values[indices], 0.0)
        pred_values = score_values[indices]
        if indices.size == 1:
            eligible_groups += 1
            ndcg_at5_values.append(1.0)
            ndcg_full_values.append(1.0)
            top1_matches += 1
            continue
        eligible_groups += 1
        ndcg_at5_values.append(_ndcg_at_k(true_values, pred_values, k=min(5, int(indices.size))))
        ndcg_full_values.append(_ndcg_at_k(true_values, pred_values, k=int(indices.size)))
        top1_matches += int(int(np.argmax(pred_values)) == int(np.argmax(true_values)))
    if not group_sizes:
        return {
            "ts_group_count": 0,
            "eligible_group_count": 0,
            "mean_group_size": 0.0,
            "min_group_size": 0,
            "max_group_size": 0,
            "ndcg_at_5_mean": 0.0,
            "ndcg_full_mean": 0.0,
            "top1_match_rate": 0.0,
        }
    return {
        "ts_group_count": len(group_sizes),
        "eligible_group_count": int(eligible_groups),
        "mean_group_size": float(np.mean(np.asarray(group_sizes, dtype=np.float64))),
        "min_group_size": int(min(group_sizes)),
        "max_group_size": int(max(group_sizes)),
        "ndcg_at_5_mean": float(np.mean(np.asarray(ndcg_at5_values, dtype=np.float64))) if ndcg_at5_values else 0.0,
        "ndcg_full_mean": float(np.mean(np.asarray(ndcg_full_values, dtype=np.float64))) if ndcg_full_values else 0.0,
        "top1_match_rate": float(top1_matches) / float(max(eligible_groups, 1)),
    }


def _group_indices_by_ts(ts_ms: np.ndarray) -> list[tuple[int, np.ndarray]]:
    values = np.asarray(ts_ms, dtype=np.int64)
    if values.size <= 0:
        return []
    unique, inverse = np.unique(values, return_inverse=True)
    grouped: list[tuple[int, np.ndarray]] = []
    for group_idx, ts_value in enumerate(unique):
        grouped.append((int(ts_value), np.flatnonzero(inverse == group_idx).astype(np.int64, copy=False)))
    return grouped


def _ndcg_at_k(relevance: np.ndarray, scores: np.ndarray, *, k: int) -> float:
    rel = np.asarray(relevance, dtype=np.float64)
    pred = np.asarray(scores, dtype=np.float64)
    if rel.size <= 0 or pred.size <= 0:
        return 0.0
    top_k = max(min(int(k), int(rel.size)), 1)
    order = np.argsort(-pred, kind="mergesort")[:top_k]
    ideal = np.argsort(-rel, kind="mergesort")[:top_k]
    dcg = _dcg(rel[order])
    idcg = _dcg(rel[ideal])
    if idcg <= 1e-12:
        return 0.0
    return float(dcg / idcg)


def _dcg(values: np.ndarray) -> float:
    arr = np.asarray(values, dtype=np.float64)
    if arr.size <= 0:
        return 0.0
    positions = np.arange(2, arr.size + 2, dtype=np.float64)
    gains = np.power(2.0, arr) - 1.0
    discounts = np.log2(positions)
    return float(np.sum(gains / discounts))


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
    backend = "xgboost_ranker" if task == "rank" else "xgboost_regressor" if task == "reg" else "xgboost"
    objective = "rank:pairwise" if task == "rank" else "reg:squarederror" if task == "reg" else "binary:logistic"
    return {
        "run_id": run_id,
        "created_at_utc": _utc_now(),
        "trainer": "v4_crypto_cs",
        "task": task,
        "model_family": options.model_family,
        "tf": options.tf,
        "quote": options.quote,
        "top_n": options.top_n,
        "time_range": {
            "start": options.start,
            "end": options.end,
            "start_ts_ms": parse_date_to_ts_ms(options.start),
            "end_ts_ms": parse_date_to_ts_ms(options.end, end_of_day=True),
        },
        "split_policy": {
            "train_ratio": options.train_ratio,
            "valid_ratio": options.valid_ratio,
            "test_ratio": options.test_ratio,
            "embargo_bars": options.embargo_bars,
            "interval_ms": interval_ms,
            "valid_start_ts": split_info.valid_start_ts,
            "test_start_ts": split_info.test_start_ts,
            "counts": split_info.counts,
        },
        "rows": rows,
        "fee_policy_bps": {
            "fee_bps_est": options.fee_bps_est,
            "safety_bps": options.safety_bps,
        },
        "booster": {
            "backend": backend,
            "params": best_params,
            "objective": objective,
            "grouping_policy": {"query_key": "ts_ms"} if task == "rank" else {},
            "valid": valid_metrics,
            "test": test_metrics,
        },
        "booster_sweep": {"trials": len(sweep_records), "records": sweep_records},
        "ranker_budget_profile": ranker_budget_profile,
        "champion": {
            "name": "booster",
            "backend": backend,
            "params": best_params,
        },
        "champion_metrics": test_metrics,
        "walk_forward": walk_forward_summary,
        "research_only": {
            "cpcv_lite": {
                "summary": cpcv_lite_summary,
                "runtime": dict(cpcv_lite_runtime or {}),
            },
        },
        "search_budget": dict(search_budget_decision or {}),
        "lane_governance": dict(lane_governance or {}),
        "economic_objective": {
            "profile_id": str((economic_objective_profile or {}).get("profile_id", "")).strip()
            or "v4_shared_economic_objective_v1",
            "trainer_sweep": dict(((economic_objective_profile or {}).get("trainer_sweep") or {}).get("task_profiles", {}).get(task) or {}),
            "walk_forward_selection": dict((economic_objective_profile or {}).get("walk_forward_selection") or {}),
            "offline_compare": dict((economic_objective_profile or {}).get("offline_compare") or {}),
            "execution_compare": dict((economic_objective_profile or {}).get("execution_compare") or {}),
            "promotion_compare": dict((economic_objective_profile or {}).get("promotion_compare") or {}),
        },
        "factor_block_selection": factor_block_selection_summary,
    }


def _make_v4_leaderboard_row(
    *,
    run_id: str,
    options: TrainV4CryptoCsOptions,
    task: str,
    rows: dict[str, int],
    test_metrics: dict[str, Any],
) -> dict[str, Any]:
    cls = test_metrics.get("classification", {}) if isinstance(test_metrics, dict) else {}
    ranking = test_metrics.get("ranking", {}) if isinstance(test_metrics, dict) else {}
    top5 = (test_metrics.get("trading", {}) or {}).get("top_5pct", {})
    backend = "xgboost_ranker" if task == "rank" else "xgboost_regressor" if task == "reg" else "xgboost"
    return {
        "run_id": run_id,
        "created_at_utc": _utc_now(),
        "model_family": options.model_family,
        "trainer": "v4_crypto_cs",
        "task": task,
        "champion": "booster",
        "champion_backend": backend,
        "test_roc_auc": _safe_float(cls.get("roc_auc")),
        "test_pr_auc": _safe_float(cls.get("pr_auc")),
        "test_log_loss": _safe_float(cls.get("log_loss")),
        "test_brier_score": _safe_float(cls.get("brier_score")),
        "test_ndcg_at5": _safe_float(ranking.get("ndcg_at_5_mean")),
        "test_top1_match_rate": _safe_float(ranking.get("top1_match_rate")),
        "test_precision_top5": _safe_float(top5.get("precision")),
        "test_ev_net_top5": _safe_float(top5.get("ev_net")),
        "rows_train": int(rows.get("train", 0)),
        "rows_valid": int(rows.get("valid", 0)),
        "rows_test": int(rows.get("test", 0)),
    }


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
    payload = asdict(options)
    payload["dataset_root"] = str(options.dataset_root)
    payload["registry_root"] = str(options.registry_root)
    payload["logs_root"] = str(options.logs_root)
    payload["execution_acceptance_parquet_root"] = str(options.execution_acceptance_parquet_root)
    payload["execution_acceptance_output_root"] = str(options.execution_acceptance_output_root)
    payload["execution_acceptance_top_n"] = max(
        int(options.execution_acceptance_top_n) if int(options.execution_acceptance_top_n) > 0 else int(options.top_n),
        1,
    )
    payload["feature_columns"] = list(feature_cols)
    payload["markets"] = list(markets)
    payload["start_ts_ms"] = parse_date_to_ts_ms(options.start)
    payload["end_ts_ms"] = parse_date_to_ts_ms(options.end, end_of_day=True)
    payload["created_at_utc"] = _utc_now()
    payload["autobot_version"] = autobot_version
    payload["trainer"] = "v4_crypto_cs"
    payload["run_scope"] = normalize_factor_block_run_scope(options.run_scope)
    payload["task"] = task
    payload["y_cls_column"] = "y_cls_topq_12"
    payload["y_reg_column"] = "y_reg_net_12"
    payload["y_rank_column"] = "y_rank_cs_12"
    payload["label_columns"] = ["y_cls_topq_12", "y_reg_net_12", "y_rank_cs_12"]
    payload["ranker_budget_profile"] = ranker_budget_profile
    payload["cpcv_lite"] = {
        "enabled": bool((cpcv_lite_runtime or {}).get("enabled", False)),
        "requested": bool(options.cpcv_lite_enabled),
        "trigger": str((cpcv_lite_runtime or {}).get("trigger", "disabled")).strip() or "disabled",
        "group_count": int(options.cpcv_lite_group_count),
        "test_group_count": int(options.cpcv_lite_test_group_count),
        "max_combinations": int(options.cpcv_lite_max_combinations),
        "summary": dict(cpcv_lite_summary or {}),
    }
    payload["factor_block_selection"] = {
        "mode": normalize_factor_block_selection_mode(options.factor_block_selection_mode),
        "summary": dict((factor_block_selection or {}).get("summary") or {}),
        "refit_support": dict((factor_block_selection or {}).get("refit_support") or {}),
        "resolution_context": dict(factor_block_selection_context or {}),
    }
    payload["search_budget"] = dict(search_budget_decision or {})
    payload["lane_governance"] = dict(lane_governance or {})
    payload["research_support_lane"] = dict(research_support_lane or {})
    payload["selection_recommendations"] = selection_recommendations
    payload["selection_policy"] = dict(selection_policy or {})
    payload["selection_calibration"] = dict(selection_calibration or {})
    return payload


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
    normalized_run_scope = normalize_factor_block_run_scope(options.run_scope)
    search_applied = dict((search_budget_decision or {}).get("applied") or {})
    execution_compare = dict((execution_acceptance or {}).get("compare_to_champion") or {})
    runtime_exit = dict((runtime_recommendations or {}).get("exit") or {})
    factor_block_refit_support = dict((factor_block_selection or {}).get("refit_support") or {})
    factor_block_refit_summary = dict(factor_block_refit_support.get("summary") or {})
    research_support_summary = dict((research_support_lane or {}).get("summary") or {})
    promotion_reasons = [
        str(item).strip()
        for item in ((promotion or {}).get("reasons") or [])
        if str(item).strip()
    ]
    warnings: list[str] = [
        "TRAINER_RESEARCH_PRIOR_IS_TRAIN_PRODUCED",
        "PROMOTION_DECISION_CONTAINS_TRAIN_PRODUCED_RESEARCH_ARTIFACTS",
    ]
    if bool(options.execution_acceptance_enabled):
        warnings.append("EXECUTION_ACCEPTANCE_REUSES_TRAIN_WINDOW")
        warnings.append("RUNTIME_RECOMMENDATIONS_REUSE_TRAIN_WINDOW")
    if str((search_budget_decision or {}).get("status", "")).strip().lower() in {"adjusted", "throttled"}:
        warnings.append("SEARCH_BUDGET_ALTERS_RESEARCH_BREADTH")
    if (
        normalize_factor_block_selection_mode(options.factor_block_selection_mode) == "guarded_auto"
        and not bool((factor_block_selection_context or {}).get("applied", False))
    ):
        warnings.append("GUARDED_FACTOR_POLICY_NOT_ACTIVE")
    if str(factor_block_refit_summary.get("status", "")).strip().lower() in {"insufficient", "partial"}:
        warnings.append("FACTOR_BLOCK_REFIT_SUPPORT_NOT_FULLY_AVAILABLE")
    if str(research_support_summary.get("status", "")).strip().lower() in {"insufficient", "partial", "disabled"}:
        warnings.append("RESEARCH_SUPPORT_LANE_NOT_FULLY_SUPPORTED")
    if bool((lane_governance or {}).get("shadow_only", False)):
        warnings.append("LANE_GOVERNANCE_SHADOW_ONLY")

    return {
        "version": 1,
        "policy": "v4_decision_surface_v1",
        "trainer_entrypoint": {
            "trainer": "v4_crypto_cs",
            "task": str(task).strip().lower() or "cls",
            "model_family": str(options.model_family).strip(),
            "feature_set": str(options.feature_set).strip().lower() or "v4",
            "label_set": str(options.label_set).strip().lower() or "v2",
            "run_scope": normalized_run_scope,
            "dataset_window": {
                "start": str(options.start).strip(),
                "end": str(options.end).strip(),
                "start_ts_ms": parse_date_to_ts_ms(options.start),
                "end_ts_ms": parse_date_to_ts_ms(options.end, end_of_day=True),
            },
            "top_n": int(options.top_n),
            "booster_sweep_trials_requested": int(options.booster_sweep_trials),
            "booster_sweep_trials_applied": int(
                search_applied.get("booster_sweep_trials", options.booster_sweep_trials) or options.booster_sweep_trials
            ),
        },
        "data_split_contract": {
            "train_ratio": float(options.train_ratio),
            "valid_ratio": float(options.valid_ratio),
            "test_ratio": float(options.test_ratio),
            "embargo_bars": int(options.embargo_bars),
            "walk_forward_enabled": bool(options.walk_forward_enabled),
            "walk_forward_windows": int(options.walk_forward_windows),
            "walk_forward_min_train_rows": int(options.walk_forward_min_train_rows),
            "walk_forward_min_test_rows": int(options.walk_forward_min_test_rows),
            "cpcv_lite_requested": bool(options.cpcv_lite_enabled),
            "cpcv_lite_trigger": str((cpcv_lite_runtime or {}).get("trigger", "disabled")).strip() or "disabled",
            "cpcv_lite_enabled": bool((cpcv_lite_runtime or {}).get("enabled", False)),
        },
        "selection_runtime_contract": {
            "selection_policy_mode": str((selection_policy or {}).get("mode", "")).strip() or "unknown",
            "selection_policy_threshold_key": str((selection_policy or {}).get("threshold_key", "")).strip(),
            "selection_policy_source": str(
                (selection_policy or {}).get("selection_recommendation_source", "")
            ).strip(),
            "selection_calibration_method": str((selection_calibration or {}).get("method", "")).strip(),
            "selection_calibration_sample_count": int((selection_calibration or {}).get("sample_count", 0) or 0),
        },
        "research_support_contract": {
            "policy": str((research_support_lane or {}).get("policy", "")).strip()
            or "v4_certification_support_lane_v1",
            "status": str(research_support_summary.get("status", "")).strip() or "unknown",
            "reasons": [
                str(item).strip()
                for item in (research_support_summary.get("reasons") or [])
                if str(item).strip()
            ],
            "multiple_testing_supported": bool(research_support_summary.get("multiple_testing_supported", False)),
            "cpcv_lite_status": str(research_support_summary.get("cpcv_lite_status", "")).strip() or "unknown",
            "support_only": True,
        },
        "lane_governance": dict(lane_governance or {}),
        "economic_objective_contract": {
            "profile_id": str((economic_objective_profile or {}).get("profile_id", "")).strip()
            or "v4_shared_economic_objective_v1",
            "objective_family": str((economic_objective_profile or {}).get("objective_family", "")).strip()
            or "economic_return_first",
            "trainer_sweep": dict(
                (((economic_objective_profile or {}).get("trainer_sweep") or {}).get("task_profiles") or {}).get(task)
                or {}
            ),
            "walk_forward_selection": dict((economic_objective_profile or {}).get("walk_forward_selection") or {}),
            "offline_compare": dict((economic_objective_profile or {}).get("offline_compare") or {}),
            "execution_compare": dict((economic_objective_profile or {}).get("execution_compare") or {}),
            "promotion_compare": dict((economic_objective_profile or {}).get("promotion_compare") or {}),
        },
        "factor_block_contract": {
            "selection_mode": normalize_factor_block_selection_mode(options.factor_block_selection_mode),
            "resolution_source": str((factor_block_selection_context or {}).get("resolution_source", "")).strip()
            or "full_set",
            "applied": bool((factor_block_selection_context or {}).get("applied", False)),
            "resolved_run_id": str((factor_block_selection_context or {}).get("resolved_run_id", "")).strip(),
            "refit_support_status": str(factor_block_refit_summary.get("status", "")).strip() or "unknown",
            "refit_support": factor_block_refit_support,
        },
        "search_budget_contract": {
            "status": str((search_budget_decision or {}).get("status", "")).strip() or "default",
            "lane_class_requested": str((search_budget_decision or {}).get("lane_class_requested", "")).strip()
            or "promotion_eligible",
            "lane_class_effective": str((search_budget_decision or {}).get("lane_class_effective", "")).strip()
            or "promotion_eligible",
            "budget_contract_id": str((search_budget_decision or {}).get("budget_contract_id", "")).strip()
            or "v4_promotion_eligible_budget_v1",
            "promotion_eligible_satisfied": bool(
                ((search_budget_decision or {}).get("promotion_eligible_contract") or {}).get("satisfied", False)
            ),
            "markers": [
                str(item).strip()
                for item in ((search_budget_decision or {}).get("markers") or [])
                if str(item).strip()
            ],
            "runtime_recommendation_profile": str(
                search_applied.get("runtime_recommendation_profile", "full")
            ).strip()
            or "full",
            "cpcv_lite_auto_enabled": bool(search_applied.get("cpcv_lite_auto_enabled", False)),
        },
        "execution_acceptance_contract": {
            "enabled": bool(options.execution_acceptance_enabled),
            "window_source": "train_options_start_end",
            "window_start": str(options.start).strip(),
            "window_end": str(options.end).strip(),
            "selection_use_learned_recommendations": False,
            "exit_use_learned_exit_mode": False,
            "exit_use_learned_hold_bars": False,
            "execution_use_learned_recommendations": False,
            "status": str((execution_acceptance or {}).get("status", "")).strip() or "disabled",
            "compare_decision": str(execution_compare.get("decision", "")).strip(),
            "compare_comparable": bool(execution_compare.get("comparable", False)),
        },
        "runtime_recommendation_contract": {
            "enabled": bool(options.execution_acceptance_enabled),
            "window_source": "train_options_start_end",
            "window_start": str(options.start).strip(),
            "window_end": str(options.end).strip(),
            "selection_use_learned_recommendations": True,
            "exit_use_learned_exit_mode": False,
            "exit_use_learned_hold_bars": False,
            "execution_use_learned_recommendations": False,
            "status": str((runtime_recommendations or {}).get("status", "")).strip() or "unknown",
            "recommended_exit_mode": str(runtime_exit.get("recommended_exit_mode", "")).strip(),
            "recommended_hold_bars": int(runtime_exit.get("recommended_hold_bars", 0) or 0),
        },
        "promotion_contract": {
            "promotion_mode": str((promotion or {}).get("promotion_mode", "")).strip() or "candidate",
            "trainer_evidence_source": "certification_artifact.research_evidence",
            "trainer_research_prior_path": "trainer_research_evidence.json",
            "trainer_research_prior_role": "audit_only_prior",
            "trainer_evidence_expected_consumer": "scripts/candidate_acceptance.ps1",
            "trainer_evidence_includes_execution_acceptance": bool(options.execution_acceptance_enabled),
            "promotion_reasons": promotion_reasons,
        },
        "known_methodology_warnings": sorted(set(warnings)),
    }


def _build_research_support_lane_v4(
    *,
    walk_forward: dict[str, Any],
    cpcv_lite: dict[str, Any],
) -> dict[str, Any]:
    walk_summary = dict((walk_forward or {}).get("summary") or {})
    panel_diagnostics = dict((walk_forward or {}).get("multiple_testing_panel_diagnostics") or {})
    spa_like_doc = dict((walk_forward or {}).get("spa_like_window_test") or {})
    white_rc_doc = dict((walk_forward or {}).get("white_reality_check") or {})
    hansen_spa_doc = dict((walk_forward or {}).get("hansen_spa") or {})
    cpcv_summary = dict((cpcv_lite or {}).get("summary") or {})

    def _reason_list(*sources: Any) -> list[str]:
        reasons: list[str] = []
        for source in sources:
            for item in source or []:
                text = str(item).strip()
                if text and text not in reasons:
                    reasons.append(text)
        return reasons

    def _support_status(doc: dict[str, Any]) -> str:
        if not doc:
            return "missing"
        return "supported" if bool(doc.get("comparable", False)) else "insufficient"

    spa_like_reasons = _reason_list(spa_like_doc.get("reasons"))
    white_rc_reasons = _reason_list(white_rc_doc.get("reasons"), (white_rc_doc.get("panel_diagnostics") or {}).get("reasons"))
    hansen_spa_reasons = _reason_list(
        hansen_spa_doc.get("reasons"),
        (hansen_spa_doc.get("panel_diagnostics") or {}).get("reasons"),
    )
    cpcv_reasons = _reason_list(
        cpcv_summary.get("reasons"),
        (cpcv_lite or {}).get("insufficiency_reasons"),
        [str(cpcv_summary.get("budget_reason", "")).strip()] if str(cpcv_summary.get("budget_reason", "")).strip() else [],
    )

    spa_like_status = _support_status(spa_like_doc)
    white_rc_status = _support_status(white_rc_doc)
    hansen_spa_status = _support_status(hansen_spa_doc)
    cpcv_status_raw = str(cpcv_summary.get("status", "")).strip().lower()
    if not cpcv_status_raw:
        cpcv_status_raw = "disabled" if not bool((cpcv_lite or {}).get("enabled", False)) else "unknown"
    if cpcv_status_raw == "trusted":
        cpcv_support_status = "supported"
    elif cpcv_status_raw in {"partial", "default"}:
        cpcv_support_status = "partial"
    elif cpcv_status_raw == "disabled":
        cpcv_support_status = "disabled"
    else:
        cpcv_support_status = "insufficient"

    windows_run = int(walk_summary.get("windows_run", 0) or 0)
    comparable_components = (
        spa_like_status == "supported"
        and white_rc_status == "supported"
        and hansen_spa_status == "supported"
    )
    cpcv_usable = cpcv_support_status in {"supported", "partial"}
    any_support_evidence = bool(
        windows_run > 0
        or panel_diagnostics
        or spa_like_doc
        or white_rc_doc
        or hansen_spa_doc
        or cpcv_lite
    )
    if comparable_components and cpcv_usable:
        status = "supported"
    elif any_support_evidence and (spa_like_status == "supported" or white_rc_status == "supported" or hansen_spa_status == "supported" or cpcv_usable):
        status = "partial"
    elif cpcv_support_status == "disabled" and windows_run > 0:
        status = "partial"
    else:
        status = "insufficient"

    summary_reasons = _reason_list(
        ["NO_WALK_FORWARD_EVIDENCE"] if windows_run <= 0 else [],
        panel_diagnostics.get("reasons"),
        spa_like_reasons,
        white_rc_reasons,
        hansen_spa_reasons,
        cpcv_reasons,
        ["CPCV_LITE_DISABLED"] if cpcv_support_status == "disabled" else [],
    )
    if status == "supported" and not summary_reasons:
        summary_reasons = ["SUPPORT_LANE_AVAILABLE"]

    return {
        "version": 1,
        "policy": "v4_certification_support_lane_v1",
        "source": "train_v4_crypto_cs",
        "support_only": True,
        "summary": {
            "status": status,
            "windows_run": windows_run,
            "multiple_testing_supported": bool(comparable_components),
            "cpcv_lite_status": cpcv_status_raw,
            "reasons": summary_reasons,
        },
        "multiple_testing_panel_diagnostics": panel_diagnostics,
        "spa_like": {
            "policy": str(spa_like_doc.get("policy", "")).strip(),
            "decision": str(spa_like_doc.get("decision", "")).strip(),
            "comparable": bool(spa_like_doc.get("comparable", False)),
            "status": spa_like_status,
            "reasons": spa_like_reasons,
            "window_count": int(spa_like_doc.get("window_count", 0) or 0),
        },
        "white_rc": {
            "policy": str(white_rc_doc.get("policy", "")).strip(),
            "decision": str(white_rc_doc.get("decision", "")).strip(),
            "comparable": bool(white_rc_doc.get("comparable", False)),
            "status": white_rc_status,
            "reasons": white_rc_reasons,
            "panel_diagnostics": dict(white_rc_doc.get("panel_diagnostics") or {}),
        },
        "hansen_spa": {
            "policy": str(hansen_spa_doc.get("policy", "")).strip(),
            "decision": str(hansen_spa_doc.get("decision", "")).strip(),
            "comparable": bool(hansen_spa_doc.get("comparable", False)),
            "status": hansen_spa_status,
            "reasons": hansen_spa_reasons,
            "panel_diagnostics": dict(hansen_spa_doc.get("panel_diagnostics") or {}),
        },
        "cpcv_lite": {
            "enabled": bool((cpcv_lite or {}).get("enabled", False)),
            "trigger": str((cpcv_lite or {}).get("trigger", "")).strip() or "disabled",
            "status": cpcv_status_raw,
            "support_status": cpcv_support_status,
            "summary": cpcv_summary,
            "insufficiency_reasons": cpcv_reasons,
            "pbo": dict((cpcv_lite or {}).get("pbo") or {}),
            "dsr": dict((cpcv_lite or {}).get("dsr") or {}),
        },
    }


def _build_trainer_research_evidence_from_promotion_v4(
    *,
    promotion: dict[str, Any],
    support_lane: dict[str, Any] | None = None,
) -> dict[str, Any]:
    checks = dict((promotion or {}).get("checks") or {})
    research = dict((promotion or {}).get("research_acceptance") or {})
    offline_compare = dict(research.get("compare_to_champion") or {})
    spa_like_doc = dict(research.get("spa_like_window_test") or {})
    white_rc_doc = dict(research.get("white_reality_check") or {})
    hansen_spa_doc = dict(research.get("hansen_spa") or {})
    walk_summary = dict(research.get("walk_forward_summary") or {})
    execution_doc = dict((promotion or {}).get("execution_acceptance") or {})
    execution_compare = dict(execution_doc.get("compare_to_champion") or {})

    existing_champion_present = bool(checks.get("existing_champion_present", False))
    walk_forward_present = bool(checks.get("walk_forward_present", False))
    walk_forward_windows_run = int(checks.get("walk_forward_windows_run", 0) or 0)
    offline_comparable = bool(checks.get("balanced_pareto_comparable", False))
    offline_candidate_edge = bool(checks.get("balanced_pareto_candidate_edge", False))
    spa_like_present = bool(checks.get("spa_like_present", False))
    spa_like_comparable = bool(checks.get("spa_like_comparable", False))
    spa_like_candidate_edge = bool(checks.get("spa_like_candidate_edge", False))
    white_rc_present = bool(checks.get("white_rc_present", False))
    white_rc_comparable = bool(checks.get("white_rc_comparable", False))
    white_rc_candidate_edge = bool(checks.get("white_rc_candidate_edge", False))
    hansen_spa_present = bool(checks.get("hansen_spa_present", False))
    hansen_spa_comparable = bool(checks.get("hansen_spa_comparable", False))
    hansen_spa_candidate_edge = bool(checks.get("hansen_spa_candidate_edge", False))
    execution_enabled = bool(checks.get("execution_acceptance_enabled", False))
    execution_present = bool(checks.get("execution_acceptance_present", False))
    execution_comparable = bool(checks.get("execution_balanced_pareto_comparable", False))
    execution_candidate_edge = bool(checks.get("execution_balanced_pareto_candidate_edge", False))

    offline_decision = str(offline_compare.get("decision", "")).strip()
    spa_like_decision = str(spa_like_doc.get("decision", "")).strip()
    white_rc_decision = str(white_rc_doc.get("decision", "")).strip()
    hansen_spa_decision = str(hansen_spa_doc.get("decision", "")).strip()
    execution_status = str(execution_doc.get("status", "")).strip()
    execution_decision = str(execution_compare.get("decision", "")).strip()

    reasons: list[str] = []
    if not walk_forward_present:
        reasons.append("NO_WALK_FORWARD_EVIDENCE")
    elif existing_champion_present:
        if not offline_comparable:
            reasons.append("OFFLINE_NOT_COMPARABLE")
        elif not offline_candidate_edge:
            reasons.append("OFFLINE_NOT_CANDIDATE_EDGE")
        if spa_like_present:
            if not spa_like_comparable:
                reasons.append("SPA_LIKE_NOT_COMPARABLE")
            elif not spa_like_candidate_edge:
                reasons.append("SPA_LIKE_NOT_CANDIDATE_EDGE")
        if white_rc_present:
            if not white_rc_comparable:
                reasons.append("WHITE_RC_NOT_COMPARABLE")
            elif not white_rc_candidate_edge:
                reasons.append("WHITE_RC_NOT_CANDIDATE_EDGE")
        if hansen_spa_present:
            if not hansen_spa_comparable:
                reasons.append("HANSEN_SPA_NOT_COMPARABLE")
            elif not hansen_spa_candidate_edge:
                reasons.append("HANSEN_SPA_NOT_CANDIDATE_EDGE")

    offline_pass = walk_forward_present and (
        (not existing_champion_present)
        or (
            offline_comparable
            and offline_candidate_edge
            and ((not spa_like_present) or (spa_like_comparable and spa_like_candidate_edge))
            and ((not white_rc_present) or (white_rc_comparable and white_rc_candidate_edge))
            and ((not hansen_spa_present) or (hansen_spa_comparable and hansen_spa_candidate_edge))
        )
    )
    execution_pass = True
    if execution_enabled:
        if not execution_present:
            execution_pass = False
            reasons.append("NO_EXECUTION_EVIDENCE")
        elif existing_champion_present:
            if not execution_comparable:
                execution_pass = False
                reasons.append("EXECUTION_NOT_COMPARABLE")
            elif not execution_candidate_edge:
                execution_pass = False
                reasons.append("EXECUTION_NOT_CANDIDATE_EDGE")

    available = walk_forward_present or execution_present or bool(offline_decision) or bool(execution_decision)
    passed = offline_pass and execution_pass
    if available and not reasons:
        reasons = ["TRAINER_EVIDENCE_PASS"]

    return {
        "version": 1,
        "policy": "v4_trainer_research_evidence_v1",
        "source": "train_v4_crypto_cs",
        "available": available,
        "pass": passed,
        "offline_pass": offline_pass,
        "execution_pass": execution_pass,
        "reasons": reasons,
        "checks": {
            "existing_champion_present": existing_champion_present,
            "walk_forward_present": walk_forward_present,
            "walk_forward_windows_run": walk_forward_windows_run,
            "offline_comparable": offline_comparable,
            "offline_candidate_edge": offline_candidate_edge,
            "spa_like_present": spa_like_present,
            "spa_like_comparable": spa_like_comparable,
            "spa_like_candidate_edge": spa_like_candidate_edge,
            "white_rc_present": white_rc_present,
            "white_rc_comparable": white_rc_comparable,
            "white_rc_candidate_edge": white_rc_candidate_edge,
            "hansen_spa_present": hansen_spa_present,
            "hansen_spa_comparable": hansen_spa_comparable,
            "hansen_spa_candidate_edge": hansen_spa_candidate_edge,
            "execution_acceptance_enabled": execution_enabled,
            "execution_acceptance_present": execution_present,
            "execution_comparable": execution_comparable,
            "execution_candidate_edge": execution_candidate_edge,
        },
        "offline": {
            "policy": str(research.get("policy", "")).strip(),
            "decision": offline_decision,
            "comparable": offline_comparable,
        },
        "spa_like": {
            "policy": str(spa_like_doc.get("policy", "")).strip(),
            "decision": spa_like_decision,
            "comparable": spa_like_comparable,
        },
        "white_rc": {
            "policy": str(white_rc_doc.get("policy", "")).strip(),
            "decision": white_rc_decision,
            "comparable": white_rc_comparable,
        },
        "hansen_spa": {
            "policy": str(hansen_spa_doc.get("policy", "")).strip(),
            "decision": hansen_spa_decision,
            "comparable": hansen_spa_comparable,
        },
        "execution": {
            "status": execution_status,
            "policy": str(execution_compare.get("policy", "")).strip(),
            "decision": execution_decision,
            "comparable": execution_comparable,
        },
        "support_lane": dict(support_lane or {}),
    }


def _write_train_report_v4(logs_root: Path, run_scope: str, payload: dict[str, Any]) -> Path:
    logs_root.mkdir(parents=True, exist_ok=True)
    path = logs_root / _scoped_train_report_filename(run_scope)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _scoped_train_report_filename(run_scope: str) -> str:
    normalized_scope = normalize_factor_block_run_scope(run_scope)
    if normalized_scope == "scheduled_daily":
        return "train_v4_report.json"
    return f"train_v4_report.{normalized_scope}.json"


def _run_execution_acceptance_v4(
    *,
    options: TrainV4CryptoCsOptions,
    run_id: str,
) -> dict[str, Any]:
    if not bool(options.execution_acceptance_enabled):
        return {
            "policy": "balanced_pareto_execution",
            "enabled": False,
            "status": "skipped",
            "skip_reason": "DISABLED",
            "compare_to_champion": {},
        }
    try:
        selection = replace(
            options.execution_acceptance_model_alpha.selection,
            use_learned_recommendations=False,
        )
        execution_model_alpha = replace(
            options.execution_acceptance_model_alpha,
            selection=selection,
        )
        return run_execution_acceptance(
            ExecutionAcceptanceOptions(
                registry_root=options.registry_root,
                model_family=options.model_family,
                candidate_ref=run_id,
                parquet_root=options.execution_acceptance_parquet_root,
                dataset_name=str(options.execution_acceptance_dataset_name).strip() or "candles_v1",
                output_root_dir=options.execution_acceptance_output_root,
                tf=str(options.tf).strip().lower(),
                quote=str(options.quote).strip().upper(),
                top_n=max(
                    int(options.execution_acceptance_top_n)
                    if int(options.execution_acceptance_top_n) > 0
                    else int(options.top_n),
                    1,
                ),
                start_ts_ms=parse_date_to_ts_ms(options.start),
                end_ts_ms=parse_date_to_ts_ms(options.end, end_of_day=True),
                feature_set=str(options.feature_set).strip().lower() or "v4",
                dense_grid=bool(options.execution_acceptance_dense_grid),
                starting_krw=max(float(options.execution_acceptance_starting_krw), 0.0),
                per_trade_krw=max(float(options.execution_acceptance_per_trade_krw), 1.0),
                max_positions=max(int(options.execution_acceptance_max_positions), 1),
                min_order_krw=max(float(options.execution_acceptance_min_order_krw), 0.0),
                order_timeout_bars=max(int(options.execution_acceptance_order_timeout_bars), 1),
                reprice_max_attempts=max(int(options.execution_acceptance_reprice_max_attempts), 0),
                reprice_tick_steps=max(int(options.execution_acceptance_reprice_tick_steps), 1),
                rules_ttl_sec=max(int(options.execution_acceptance_rules_ttl_sec), 1),
                model_alpha_settings=execution_model_alpha,
            )
        )
    except Exception as exc:
        return {
            "policy": "balanced_pareto_execution",
            "enabled": True,
            "status": "skipped",
            "skip_reason": f"{type(exc).__name__}: {exc}",
            "compare_to_champion": {},
        }


def _build_runtime_recommendations_v4(
    *,
    options: TrainV4CryptoCsOptions,
    run_id: str,
    search_budget_decision: dict[str, Any],
) -> dict[str, Any]:
    if not bool(options.execution_acceptance_enabled):
        return {
            "version": 1,
            "status": "skipped",
            "reason": "EXECUTION_ACCEPTANCE_DISABLED",
        }
    try:
        selection = replace(
            options.execution_acceptance_model_alpha.selection,
            use_learned_recommendations=True,
        )
        exit_settings = replace(
            options.execution_acceptance_model_alpha.exit,
            use_learned_exit_mode=False,
            use_learned_hold_bars=False,
        )
        execution_settings = replace(
            options.execution_acceptance_model_alpha.execution,
            use_learned_recommendations=False,
        )
        runtime_model_alpha = replace(
            options.execution_acceptance_model_alpha,
            selection=selection,
            exit=exit_settings,
            execution=execution_settings,
        )
        return optimize_runtime_recommendations(
            options=ExecutionAcceptanceOptions(
                registry_root=options.registry_root,
                model_family=options.model_family,
                candidate_ref=run_id,
                parquet_root=options.execution_acceptance_parquet_root,
                dataset_name=str(options.execution_acceptance_dataset_name).strip() or "candles_v1",
                output_root_dir=options.execution_acceptance_output_root,
                tf=str(options.tf).strip().lower(),
                quote=str(options.quote).strip().upper(),
                top_n=max(
                    int(options.execution_acceptance_top_n)
                    if int(options.execution_acceptance_top_n) > 0
                    else int(options.top_n),
                    1,
                ),
                start_ts_ms=parse_date_to_ts_ms(options.start),
                end_ts_ms=parse_date_to_ts_ms(options.end, end_of_day=True),
                feature_set=str(options.feature_set).strip().lower() or "v4",
                dense_grid=bool(options.execution_acceptance_dense_grid),
                starting_krw=max(float(options.execution_acceptance_starting_krw), 0.0),
                per_trade_krw=max(float(options.execution_acceptance_per_trade_krw), 1.0),
                max_positions=max(int(options.execution_acceptance_max_positions), 1),
                min_order_krw=max(float(options.execution_acceptance_min_order_krw), 0.0),
                order_timeout_bars=max(int(options.execution_acceptance_order_timeout_bars), 1),
                reprice_max_attempts=max(int(options.execution_acceptance_reprice_max_attempts), 0),
                reprice_tick_steps=max(int(options.execution_acceptance_reprice_tick_steps), 1),
                rules_ttl_sec=max(int(options.execution_acceptance_rules_ttl_sec), 1),
                model_alpha_settings=runtime_model_alpha,
            ),
            candidate_ref=run_id,
            grid=runtime_recommendation_grid_for_profile(
                str((search_budget_decision.get("applied") or {}).get("runtime_recommendation_profile", "full"))
            ),
        )
    except Exception as exc:
        return {
            "version": 1,
            "status": "skipped",
            "reason": f"{type(exc).__name__}: {exc}",
        }


def _manual_promotion_decision_v4(
    *,
    options: TrainV4CryptoCsOptions,
    run_id: str,
    walk_forward: dict[str, Any],
    execution_acceptance: dict[str, Any],
) -> dict[str, Any]:
    champion_doc = load_json(options.registry_root / options.model_family / "champion.json")
    champion_run_id = str(champion_doc.get("run_id", "")).strip()
    reasons = ["MANUAL_PROMOTION_REQUIRED"]
    compare_doc = walk_forward.get("compare_to_champion", {}) if isinstance(walk_forward, dict) else {}
    spa_like_doc = walk_forward.get("spa_like_window_test", {}) if isinstance(walk_forward, dict) else {}
    white_rc_doc = walk_forward.get("white_reality_check", {}) if isinstance(walk_forward, dict) else {}
    hansen_spa_doc = walk_forward.get("hansen_spa", {}) if isinstance(walk_forward, dict) else {}
    walk_summary = walk_forward.get("summary", {}) if isinstance(walk_forward, dict) else {}
    windows_run = int(walk_summary.get("windows_run", 0) or 0)
    execution_status = str(execution_acceptance.get("status", "")).strip().lower()
    execution_compare = (
        execution_acceptance.get("compare_to_champion", {})
        if isinstance(execution_acceptance, dict)
        else {}
    )
    if not champion_run_id:
        reasons.append("NO_EXISTING_CHAMPION")
    if windows_run <= 0:
        reasons.append("NO_WALK_FORWARD_EVIDENCE")
    else:
        decision = str(compare_doc.get("decision", "")).strip().lower()
        if decision == "candidate_edge":
            reasons.append("OFFLINE_BALANCED_PARETO_PASS")
        elif decision == "champion_edge":
            reasons.append("OFFLINE_BALANCED_PARETO_FAIL")
        elif decision:
            reasons.append("OFFLINE_BALANCED_PARETO_HOLD")
        spa_decision = str(spa_like_doc.get("decision", "")).strip().lower()
        if spa_decision == "candidate_edge":
            reasons.append("SPA_LIKE_WINDOW_PASS")
        elif spa_decision == "champion_edge":
            reasons.append("SPA_LIKE_WINDOW_FAIL")
        elif spa_decision:
            reasons.append("SPA_LIKE_WINDOW_HOLD")
        white_rc_decision = str(white_rc_doc.get("decision", "")).strip().lower()
        if white_rc_decision == "candidate_edge":
            reasons.append("WHITE_RC_PASS")
        elif white_rc_decision:
            reasons.append("WHITE_RC_HOLD")
        hansen_spa_decision = str(hansen_spa_doc.get("decision", "")).strip().lower()
        if hansen_spa_decision == "candidate_edge":
            reasons.append("HANSEN_SPA_PASS")
        elif hansen_spa_decision:
            reasons.append("HANSEN_SPA_HOLD")
    if bool(options.execution_acceptance_enabled):
        execution_decision = str(execution_compare.get("decision", "")).strip().lower()
        if execution_status == "skipped":
            reasons.append("NO_EXECUTION_AWARE_EVIDENCE")
        elif execution_decision == "candidate_edge":
            reasons.append("EXECUTION_BALANCED_PARETO_PASS")
        elif execution_decision == "champion_edge":
            reasons.append("EXECUTION_BALANCED_PARETO_FAIL")
        elif execution_status:
            reasons.append("EXECUTION_BALANCED_PARETO_HOLD")
    return {
        "run_id": run_id,
        "promote": False,
        "status": "candidate",
        "promotion_mode": "manual_gate",
        "reasons": reasons,
        "checks": {
            "manual_review_required": True,
            "existing_champion_present": bool(champion_run_id),
            "walk_forward_present": windows_run > 0,
            "walk_forward_windows_run": windows_run,
            "balanced_pareto_comparable": bool(compare_doc.get("comparable", False)),
            "balanced_pareto_candidate_edge": str(compare_doc.get("decision", "")) == "candidate_edge",
            "spa_like_present": bool(spa_like_doc),
            "spa_like_comparable": bool(spa_like_doc.get("comparable", False)),
            "spa_like_candidate_edge": str(spa_like_doc.get("decision", "")) == "candidate_edge",
            "white_rc_present": bool(white_rc_doc),
            "white_rc_comparable": bool(white_rc_doc.get("comparable", False)),
            "white_rc_candidate_edge": bool(white_rc_doc.get("candidate_edge", False)),
            "hansen_spa_present": bool(hansen_spa_doc),
            "hansen_spa_comparable": bool(hansen_spa_doc.get("comparable", False)),
            "hansen_spa_candidate_edge": bool(hansen_spa_doc.get("candidate_edge", False)),
            "execution_acceptance_enabled": bool(options.execution_acceptance_enabled),
            "execution_acceptance_present": execution_status in {"candidate_only", "compared"},
            "execution_balanced_pareto_comparable": bool(execution_compare.get("comparable", False)),
            "execution_balanced_pareto_candidate_edge": str(execution_compare.get("decision", "")) == "candidate_edge",
        },
        "research_acceptance": {
            "policy": str(compare_doc.get("policy", "balanced_pareto_offline")),
            "walk_forward_summary": walk_summary,
            "compare_to_champion": compare_doc,
            "spa_like_window_test": spa_like_doc,
            "white_reality_check": white_rc_doc,
            "hansen_spa": hansen_spa_doc,
        },
        "execution_acceptance": execution_acceptance,
        "candidate_ref": {
            "model_ref": "latest_candidate",
            "model_family": options.model_family,
        },
    }


def _build_duplicate_candidate_promotion_decision_v4(
    *,
    options: TrainV4CryptoCsOptions,
    run_id: str,
    walk_forward: dict[str, Any],
    execution_acceptance: dict[str, Any],
    duplicate_artifacts: dict[str, Any],
) -> dict[str, Any]:
    walk_summary = walk_forward.get("summary", {}) if isinstance(walk_forward, dict) else {}
    research_compare = walk_forward.get("compare_to_champion", {}) if isinstance(walk_forward, dict) else {}
    spa_like_doc = walk_forward.get("spa_like_window_test", {}) if isinstance(walk_forward, dict) else {}
    white_rc_doc = walk_forward.get("white_reality_check", {}) if isinstance(walk_forward, dict) else {}
    hansen_spa_doc = walk_forward.get("hansen_spa", {}) if isinstance(walk_forward, dict) else {}
    champion_ref = str(duplicate_artifacts.get("champion_ref", "")).strip()
    return {
        "run_id": run_id,
        "promote": False,
        "status": "candidate",
        "promotion_mode": "duplicate_candidate_short_circuit",
        "reasons": ["DUPLICATE_CANDIDATE"],
        "checks": {
            "manual_review_required": False,
            "existing_champion_present": bool(champion_ref),
            "walk_forward_present": bool(int(walk_summary.get("windows_run", 0) or 0) > 0),
            "walk_forward_windows_run": int(walk_summary.get("windows_run", 0) or 0),
            "balanced_pareto_comparable": bool(research_compare.get("comparable", False)),
            "balanced_pareto_candidate_edge": False,
            "spa_like_present": bool(spa_like_doc),
            "spa_like_comparable": bool(spa_like_doc.get("comparable", False)),
            "spa_like_candidate_edge": False,
            "white_rc_present": bool(white_rc_doc),
            "white_rc_comparable": bool(white_rc_doc.get("comparable", False)),
            "white_rc_candidate_edge": False,
            "hansen_spa_present": bool(hansen_spa_doc),
            "hansen_spa_comparable": bool(hansen_spa_doc.get("comparable", False)),
            "hansen_spa_candidate_edge": False,
            "execution_acceptance_enabled": bool(options.execution_acceptance_enabled),
            "execution_acceptance_present": False,
            "execution_balanced_pareto_comparable": False,
            "execution_balanced_pareto_candidate_edge": False,
            "duplicate_candidate": True,
        },
        "research_acceptance": {
            "policy": str(research_compare.get("policy", "balanced_pareto_offline")),
            "walk_forward_summary": walk_summary,
            "compare_to_champion": research_compare,
            "spa_like_window_test": spa_like_doc,
            "white_reality_check": white_rc_doc,
            "hansen_spa": hansen_spa_doc,
        },
        "execution_acceptance": execution_acceptance,
        "candidate_ref": {
            "model_ref": "latest_candidate",
            "model_family": options.model_family,
        },
        "duplicate_artifacts": duplicate_artifacts,
    }


def _safe_float(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except Exception:
        return 0.0


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
