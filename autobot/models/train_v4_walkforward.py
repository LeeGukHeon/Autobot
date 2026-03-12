"""Walk-forward orchestration helpers for trainer=v4_crypto_cs."""

from __future__ import annotations

from typing import Any, Callable, Sequence

import numpy as np


def fit_walk_forward_window_model(
    *,
    task: str,
    options: Any,
    sweep_trials: int,
    x_train: np.ndarray,
    y_train: np.ndarray,
    y_train_rank: np.ndarray,
    w_train: np.ndarray,
    x_valid: np.ndarray,
    y_valid_cls: np.ndarray,
    y_valid_reg: np.ndarray,
    y_valid_rank: np.ndarray,
    w_valid: np.ndarray,
    x_test: np.ndarray,
    y_test_cls: np.ndarray,
    y_test_reg: np.ndarray,
    y_test_rank: np.ndarray,
    market_test: np.ndarray,
    ts_test_ms: np.ndarray,
    ts_train_ms: np.ndarray,
    ts_valid_ms: np.ndarray,
    fit_walk_forward_weighted_trials_fn: Callable[..., dict[str, Any]],
    fit_walk_forward_regression_trials_fn: Callable[..., dict[str, Any]],
    fit_walk_forward_ranker_trials_fn: Callable[..., dict[str, Any]],
) -> dict[str, Any]:
    if task == "cls":
        return fit_walk_forward_weighted_trials_fn(
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
        )
    if task == "reg":
        return fit_walk_forward_regression_trials_fn(
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
        )
    return fit_walk_forward_ranker_trials_fn(
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
    )


def run_walk_forward_v4(
    *,
    options: Any,
    task: str,
    dataset: Any,
    action_aux_arrays: dict[str, np.ndarray],
    interval_ms: int,
    thresholds: dict[str, Any],
    feature_names: Sequence[str],
    factor_block_registry: Sequence[Any],
    effective_booster_sweep_trials: int,
    summarize_walk_forward_windows_fn: Callable[..., dict[str, Any]],
    compare_balanced_pareto_fn: Callable[..., dict[str, Any]],
    compute_anchored_walk_forward_splits_fn: Callable[..., Any],
    split_masks_fn: Callable[[Any], dict[str, Any]],
    resolve_ranker_budget_profile_fn: Callable[..., dict[str, Any]],
    fit_walk_forward_window_model_fn: Callable[..., dict[str, Any]],
    predict_scores_fn: Callable[..., Any],
    evaluate_split_fn: Callable[..., dict[str, Any]],
    attach_ranking_metrics_fn: Callable[..., dict[str, Any]],
    build_oos_period_metrics_fn: Callable[..., list[dict[str, Any]]],
    build_oos_slice_metrics_fn: Callable[..., list[dict[str, Any]]],
    compact_eval_metrics_fn: Callable[[dict[str, Any]], dict[str, Any]],
    build_window_selection_objectives_fn: Callable[..., dict[str, Any]],
    selection_grid_config_factory: Callable[[], Any],
    evaluate_factor_block_window_rows_fn: Callable[..., list[dict[str, Any]]],
    evaluate_factor_block_refit_window_evidence_fn: Callable[..., dict[str, Any]],
    summarize_walk_forward_trial_panel_fn: Callable[..., list[dict[str, Any]]],
    split_train_label: str,
    split_valid_label: str,
    split_test_label: str,
) -> dict[str, Any]:
    report: dict[str, Any] = {
        "policy": "balanced_pareto_offline",
        "enabled": bool(options.walk_forward_enabled),
        "windows_requested": max(int(options.walk_forward_windows), 0),
        "windows": [],
        "skipped_windows": [],
        "summary": summarize_walk_forward_windows_fn([]),
        "compare_to_champion": compare_balanced_pareto_fn({}, {}),
        "selected_threshold_key": "top_5pct",
        "selected_threshold_key_source": "manual_fallback",
        "_factor_block_window_rows": [],
        "_selection_calibration_rows": [],
        "_trade_action_oos_rows": [],
        "factor_block_refit_windows": [],
    }
    if not bool(options.walk_forward_enabled):
        report["skip_reason"] = "DISABLED"
        return report

    try:
        window_specs = compute_anchored_walk_forward_splits_fn(
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
    ranker_budget_profile = resolve_ranker_budget_profile_fn(
        options=options,
        task=task,
        effective_booster_sweep_trials=effective_booster_sweep_trials,
    )
    sweep_trials = max(min(int(options.walk_forward_sweep_trials), int(effective_booster_sweep_trials)), 1)
    if task == "rank":
        sweep_trials = min(sweep_trials, int(ranker_budget_profile["walk_forward_trials"]))
    windows: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    y_rank_all = np.asarray(getattr(dataset, "y_rank", dataset.y_reg), dtype=np.float32)
    for labels, info in window_specs:
        masks = split_masks_fn(labels)
        train_mask = masks[split_train_label]
        valid_mask = masks[split_valid_label]
        test_mask = masks[split_test_label]
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

        fitted = fit_walk_forward_window_model_fn(
            task=task,
            options=options,
            sweep_trials=sweep_trials,
            x_train=dataset.X[train_mask],
            y_cls_train=dataset.y_cls[train_mask],
            y_reg_train=dataset.y_reg[train_mask],
            y_rank_train=y_rank_all[train_mask],
            w_train=dataset.sample_weight[train_mask],
            ts_train_ms=dataset.ts_ms[train_mask],
            x_valid=dataset.X[valid_mask],
            y_valid_cls=dataset.y_cls[valid_mask],
            y_valid_reg=dataset.y_reg[valid_mask],
            y_valid_rank=y_rank_all[valid_mask],
            w_valid=dataset.sample_weight[valid_mask],
            ts_valid_ms=dataset.ts_ms[valid_mask],
            x_test=dataset.X[test_mask],
            y_test_cls=dataset.y_cls[test_mask],
            y_test_reg=dataset.y_reg[test_mask],
            y_test_rank=y_rank_all[test_mask],
            market_test=dataset.markets[test_mask],
            ts_test_ms=dataset.ts_ms[test_mask],
        )
        scores = predict_scores_fn(fitted["bundle"], dataset.X[test_mask])
        metrics = evaluate_split_fn(
            y_cls=dataset.y_cls[test_mask],
            y_reg=dataset.y_reg[test_mask],
            scores=scores,
            markets=dataset.markets[test_mask],
            fee_bps_est=options.fee_bps_est,
            safety_bps=options.safety_bps,
        )
        metrics = attach_ranking_metrics_fn(
            metrics=metrics,
            y_rank=y_rank_all[test_mask],
            ts_ms=dataset.ts_ms[test_mask],
            scores=scores,
        )
        oos_periods = build_oos_period_metrics_fn(
            ts_ms=dataset.ts_ms[test_mask],
            y_cls=dataset.y_cls[test_mask],
            y_reg=dataset.y_reg[test_mask],
            scores=scores,
            markets=dataset.markets[test_mask],
            fee_bps_est=options.fee_bps_est,
            safety_bps=options.safety_bps,
        )
        oos_slices = build_oos_slice_metrics_fn(
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
                "metrics": compact_eval_metrics_fn(metrics),
                "oos_periods": oos_periods,
                "oos_slices": oos_slices,
                "selection_optimization": build_window_selection_objectives_fn(
                    scores=scores,
                    y_reg=dataset.y_reg[test_mask],
                    ts_ms=dataset.ts_ms[test_mask],
                    thresholds=thresholds,
                    fee_bps_est=options.fee_bps_est,
                    safety_bps=options.safety_bps,
                    config=selection_grid_config_factory(),
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
        report["_trade_action_oos_rows"].append(
            {
                "window_index": int(info.window_index),
                "raw_scores": np.asarray(scores, dtype=np.float64).tolist(),
                "markets": np.asarray(dataset.markets[test_mask], dtype=object).tolist(),
                "ts_ms": np.asarray(dataset.ts_ms[test_mask], dtype=np.int64).tolist(),
                "close": np.asarray(action_aux_arrays.get("close", np.array([]))[test_mask], dtype=np.float64).tolist(),
                "rv_12": np.asarray(action_aux_arrays.get("rv_12", np.array([]))[test_mask], dtype=np.float64).tolist(),
                "rv_36": np.asarray(action_aux_arrays.get("rv_36", np.array([]))[test_mask], dtype=np.float64).tolist(),
                "atr_14": np.asarray(action_aux_arrays.get("atr_14", np.array([]))[test_mask], dtype=np.float64).tolist(),
                "atr_pct_14": np.asarray(action_aux_arrays.get("atr_pct_14", np.array([]))[test_mask], dtype=np.float64).tolist(),
            }
        )
        report["_factor_block_window_rows"].extend(
            evaluate_factor_block_window_rows_fn(
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
        refit_evidence = evaluate_factor_block_refit_window_evidence_fn(
            window_index=int(info.window_index),
            task=task,
            options=options,
            best_params=dict(fitted.get("best_params", {}) or {}),
            full_bundle=fitted["bundle"],
            feature_names=feature_names,
            x_train=dataset.X[train_mask],
            y_cls_train=dataset.y_cls[train_mask],
            y_reg_train=dataset.y_reg[train_mask],
            y_rank_train=y_rank_all[train_mask],
            w_train=dataset.sample_weight[train_mask],
            ts_train_ms=dataset.ts_ms[train_mask],
            x_valid=dataset.X[valid_mask],
            y_valid_cls=dataset.y_cls[valid_mask],
            y_valid_reg=dataset.y_reg[valid_mask],
            y_valid_rank=y_rank_all[valid_mask],
            w_valid=dataset.sample_weight[valid_mask],
            ts_valid_ms=dataset.ts_ms[valid_mask],
            x_test=dataset.X[test_mask],
            y_test_cls=dataset.y_cls[test_mask],
            y_test_reg=dataset.y_reg[test_mask],
            y_test_rank=y_rank_all[test_mask],
            ts_test_ms=dataset.ts_ms[test_mask],
            thresholds=thresholds,
            block_registry=factor_block_registry,
        )
        report["_factor_block_window_rows"].extend(refit_evidence["rows"])
        report["factor_block_refit_windows"].append(refit_evidence["support"])

    report["windows"] = windows
    report["skipped_windows"] = skipped
    report["windows_generated"] = len(window_specs)
    report["trial_panel"] = summarize_walk_forward_trial_panel_fn(windows, threshold_key="top_5pct")
    return report
