"""Core preparation and fit helpers for trainer=v4_crypto_cs."""

from __future__ import annotations

from dataclasses import is_dataclass, replace
from typing import Any, Callable

import numpy as np

from . import live_domain_reweighting as _live_domain_reweighting


def prepare_v4_training_inputs(
    *,
    options: Any,
    task: str,
    build_dataset_request_fn: Callable[..., Any],
    load_feature_spec_fn: Callable[[Any], dict[str, Any]],
    load_label_spec_fn: Callable[[Any], dict[str, Any]],
    feature_columns_from_spec_fn: Callable[[Any], tuple[str, ...]],
    resolve_selected_feature_columns_from_latest_fn: Callable[..., tuple[tuple[str, ...], dict[str, Any]]],
    resolve_v4_search_budget_fn: Callable[..., dict[str, Any]],
    load_feature_dataset_fn: Callable[..., Any],
    load_feature_aux_frame_fn: Callable[..., Any],
    expected_interval_ms_fn: Callable[[str], int],
    compute_time_splits_fn: Callable[..., tuple[Any, Any]],
    split_masks_fn: Callable[[Any], dict[str, Any]],
    validate_split_counts_fn: Callable[[dict[str, Any]], None],
    resolve_ranker_budget_profile_fn: Callable[..., dict[str, Any]],
    factor_block_registry_fn: Callable[..., Any],
    resolve_cpcv_lite_runtime_config_fn: Callable[..., dict[str, Any]],
) -> dict[str, Any]:
    request = build_dataset_request_fn(
        dataset_root=options.dataset_root,
        tf=options.tf,
        quote=options.quote,
        top_n=options.top_n,
        start=options.start,
        end=options.end,
        batch_rows=options.batch_rows,
    )
    feature_spec = load_feature_spec_fn(options.dataset_root)
    label_spec = load_label_spec_fn(options.dataset_root)
    high_tfs = tuple(
        str(item).strip()
        for item in (feature_spec.get("high_tfs") or ("15m", "60m", "240m"))
        if str(item).strip()
    )
    all_feature_cols = feature_columns_from_spec_fn(options.dataset_root)
    feature_cols, factor_block_selection_context = resolve_selected_feature_columns_from_latest_fn(
        registry_root=options.registry_root,
        model_family=options.model_family,
        mode=options.factor_block_selection_mode,
        run_scope=options.run_scope,
        all_feature_columns=all_feature_cols,
        high_tfs=high_tfs or ("15m", "60m", "240m"),
    )
    project_root = options.logs_root.parent.resolve()
    search_budget_decision = resolve_v4_search_budget_fn(
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
    dataset = load_feature_dataset_fn(
        request,
        feature_columns=feature_cols,
        y_cls_column="y_cls_topq_12",
        y_reg_column="y_reg_net_12",
        y_rank_column="y_rank_cs_12",
    )
    try:
        action_aux_frame = load_feature_aux_frame_fn(
            request,
            columns=("close", "rv_12", "rv_36", "atr_14", "atr_pct_14"),
            y_cls_column="y_cls_topq_12",
            y_reg_column="y_reg_net_12",
            y_rank_column="y_rank_cs_12",
        )
        if action_aux_frame.height != dataset.rows:
            raise ValueError(
                "ACTION_AUX_FRAME_ROW_MISMATCH: "
                f"aux_rows={action_aux_frame.height} dataset_rows={dataset.rows}"
            )
        aux_ts_ms = action_aux_frame.get_column("ts_ms").to_numpy().astype(np.int64, copy=False)
        aux_markets = action_aux_frame.get_column("market").to_numpy()
        if not np.array_equal(aux_ts_ms, dataset.ts_ms) or not np.array_equal(aux_markets, dataset.markets):
            raise ValueError("ACTION_AUX_FRAME_ALIGNMENT_FAILED")
        action_aux_arrays = {
            "close": action_aux_frame.get_column("close").to_numpy().astype(np.float64, copy=False),
            "rv_12": action_aux_frame.get_column("rv_12").to_numpy().astype(np.float64, copy=False),
            "rv_36": action_aux_frame.get_column("rv_36").to_numpy().astype(np.float64, copy=False),
            "atr_14": action_aux_frame.get_column("atr_14").to_numpy().astype(np.float64, copy=False),
            "atr_pct_14": action_aux_frame.get_column("atr_pct_14").to_numpy().astype(np.float64, copy=False),
        }
    except Exception:
        action_aux_arrays = {
            "close": np.full(dataset.rows, np.nan, dtype=np.float64),
            "rv_12": np.full(dataset.rows, np.nan, dtype=np.float64),
            "rv_36": np.full(dataset.rows, np.nan, dtype=np.float64),
            "atr_14": np.full(dataset.rows, np.nan, dtype=np.float64),
            "atr_pct_14": np.full(dataset.rows, np.nan, dtype=np.float64),
        }
    if dataset.rows < int(options.min_rows_for_train):
        raise ValueError(
            f"NEED_MORE_MICRO_DAYS_OR_LOOSEN_UNIVERSE: rows={dataset.rows} < min_rows_for_train={int(options.min_rows_for_train)}"
        )
    live_domain_reweighting_enabled = bool(getattr(options, "live_domain_reweighting_enabled", False))
    live_domain_reweighting_path = getattr(options, "live_domain_reweighting_db_path", None)
    adjusted_weight, live_domain_reweighting = _live_domain_reweighting.build_live_candidate_domain_reweighting(
        enabled=live_domain_reweighting_enabled,
        project_root=project_root,
        candidate_db_path=live_domain_reweighting_path,
        source_matrix=dataset.X,
        source_feature_names=dataset.feature_names,
        source_aux_features=action_aux_arrays,
        base_sample_weight=dataset.sample_weight,
        seed=int(getattr(options, "seed", 42)),
        clip_min=float(
            getattr(
                options,
                "live_domain_reweighting_clip_min",
                _live_domain_reweighting.DEFAULT_LIVE_DOMAIN_REWEIGHTING_CLIP_MIN,
            )
        ),
        clip_max=float(
            getattr(
                options,
                "live_domain_reweighting_clip_max",
                _live_domain_reweighting.DEFAULT_LIVE_DOMAIN_REWEIGHTING_CLIP_MAX,
            )
        ),
        min_target_rows=int(
            getattr(
                options,
                "live_domain_reweighting_min_target_rows",
                _live_domain_reweighting.DEFAULT_LIVE_DOMAIN_REWEIGHTING_MIN_TARGET_ROWS,
            )
        ),
        max_target_rows=int(
            getattr(
                options,
                "live_domain_reweighting_max_target_rows",
                _live_domain_reweighting.DEFAULT_LIVE_DOMAIN_REWEIGHTING_MAX_TARGET_ROWS,
            )
        ),
    )
    adjusted_weight_array = np.asarray(adjusted_weight, dtype=np.float32)
    if is_dataclass(dataset):
        dataset = replace(
            dataset,
            sample_weight=adjusted_weight_array,
        )
    else:
        dataset.sample_weight = adjusted_weight_array

    interval_ms = expected_interval_ms_fn(options.tf)
    labels, split_info = compute_time_splits_fn(
        dataset.ts_ms,
        train_ratio=options.train_ratio,
        valid_ratio=options.valid_ratio,
        test_ratio=options.test_ratio,
        embargo_bars=options.embargo_bars,
        interval_ms=interval_ms,
    )
    masks = split_masks_fn(labels)
    validate_split_counts_fn(masks)

    train_mask = masks["train"]
    valid_mask = masks["valid"]
    test_mask = masks["test"]
    rows = {
        "total": dataset.rows,
        "train": int(np.sum(train_mask)),
        "valid": int(np.sum(valid_mask)),
        "test": int(np.sum(test_mask)),
        "drop": int(np.sum(masks["drop"])),
    }
    y_rank_all = np.asarray(getattr(dataset, "y_rank", dataset.y_reg), dtype=np.float32)
    ranker_budget_profile = resolve_ranker_budget_profile_fn(
        options=options,
        task=task,
        effective_booster_sweep_trials=effective_booster_sweep_trials,
    )
    factor_block_registry = factor_block_registry_fn(
        feature_columns=dataset.feature_names,
        high_tfs=high_tfs or ("15m", "60m", "240m"),
    )
    cpcv_lite_runtime = resolve_cpcv_lite_runtime_config_fn(
        options=options,
        factor_block_selection_context=factor_block_selection_context,
    )
    return {
        "request": request,
        "feature_spec": feature_spec,
        "label_spec": label_spec,
        "feature_cols": feature_cols,
        "factor_block_selection_context": factor_block_selection_context,
        "search_budget_decision": search_budget_decision,
        "effective_booster_sweep_trials": effective_booster_sweep_trials,
        "dataset": dataset,
        "action_aux_arrays": action_aux_arrays,
        "interval_ms": interval_ms,
        "split_info": split_info,
        "rows": rows,
        "train_mask": train_mask,
        "valid_mask": valid_mask,
        "test_mask": test_mask,
        "y_rank_all": y_rank_all,
        "ranker_budget_profile": ranker_budget_profile,
        "factor_block_registry": factor_block_registry,
        "cpcv_lite_runtime": cpcv_lite_runtime,
        "high_tfs": high_tfs,
        "live_domain_reweighting": live_domain_reweighting,
    }


def fit_v4_primary_model_bundle(
    *,
    options: Any,
    task: str,
    prepared: dict[str, Any],
    fit_weighted_fn: Callable[..., dict[str, Any]],
    fit_regression_fn: Callable[..., dict[str, Any]],
    fit_ranker_fn: Callable[..., dict[str, Any]],
    predict_scores_fn: Callable[..., Any],
    evaluate_split_fn: Callable[..., dict[str, Any]],
    attach_ranking_metrics_fn: Callable[..., dict[str, Any]],
    build_thresholds_fn: Callable[..., dict[str, Any]],
) -> dict[str, Any]:
    dataset = prepared["dataset"]
    train_mask = prepared["train_mask"]
    valid_mask = prepared["valid_mask"]
    test_mask = prepared["test_mask"]
    y_rank_all = prepared["y_rank_all"]
    effective_booster_sweep_trials = int(prepared["effective_booster_sweep_trials"])
    ranker_budget_profile = dict(prepared["ranker_budget_profile"])

    x_train = dataset.X[train_mask]
    y_cls_train = dataset.y_cls[train_mask]
    y_reg_train = dataset.y_reg[train_mask]
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
    w_test = dataset.sample_weight[test_mask]
    market_valid = dataset.markets[valid_mask]
    market_test = dataset.markets[test_mask]

    if task == "cls":
        booster = fit_weighted_fn(
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
            eval_sample_weight=w_valid,
        )
    elif task == "reg":
        booster = fit_regression_fn(
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
            eval_sample_weight=w_valid,
        )
    else:
        booster = fit_ranker_fn(
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
            eval_sample_weight=w_valid,
        )

    valid_scores = predict_scores_fn(booster["bundle"], x_valid)
    valid_metrics = evaluate_split_fn(
        y_cls=y_valid,
        y_reg=y_reg_valid,
        scores=valid_scores,
        markets=market_valid,
        fee_bps_est=options.fee_bps_est,
        safety_bps=options.safety_bps,
        sample_weight=w_valid,
    )
    valid_metrics = attach_ranking_metrics_fn(
        metrics=valid_metrics,
        y_rank=y_rank_valid,
        ts_ms=ts_valid,
        scores=valid_scores,
    )
    test_scores = predict_scores_fn(booster["bundle"], x_test)
    test_metrics = evaluate_split_fn(
        y_cls=y_test,
        y_reg=y_reg_test,
        scores=test_scores,
        markets=market_test,
        fee_bps_est=options.fee_bps_est,
        safety_bps=options.safety_bps,
        sample_weight=w_test,
    )
    test_metrics = attach_ranking_metrics_fn(
        metrics=test_metrics,
        y_rank=y_rank_test,
        ts_ms=dataset.ts_ms[test_mask],
        scores=test_scores,
    )
    thresholds = build_thresholds_fn(
        valid_scores=valid_scores,
        y_reg_valid=y_reg_valid,
        fee_bps_est=options.fee_bps_est,
        safety_bps=options.safety_bps,
        ev_scan_steps=options.ev_scan_steps,
        ev_min_selected=options.ev_min_selected,
        sample_weight=w_valid,
    )
    return {
        "booster": booster,
        "valid_scores": valid_scores,
        "valid_metrics": valid_metrics,
        "test_metrics": test_metrics,
        "thresholds": thresholds,
        "valid_ts_ms": dataset.ts_ms[valid_mask],
    }
