"""CPCV-lite helpers for trainer=v4_crypto_cs."""

from __future__ import annotations

from typing import Any, Callable

import numpy as np

from .factor_block_selector import normalize_factor_block_selection_mode


def resolve_cpcv_lite_runtime_config(
    *,
    options: Any,
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


def run_cpcv_lite_v4(
    *,
    options: Any,
    task: str,
    dataset: Any,
    interval_ms: int,
    thresholds: dict[str, Any],
    best_params: dict[str, Any],
    enabled: bool,
    trigger: str,
    build_cpcv_lite_plan_fn: Callable[..., tuple[Any, dict[str, Any]]],
    build_inner_train_valid_masks_fn: Callable[..., dict[str, Any] | None],
    fit_cpcv_lite_fold_model_fn: Callable[..., dict[str, Any]],
    predict_scores_fn: Callable[..., Any],
    evaluate_split_fn: Callable[..., dict[str, Any]],
    attach_ranking_metrics_fn: Callable[..., dict[str, Any]],
    build_window_selection_objectives_fn: Callable[..., Any],
    selection_grid_config_factory: Callable[[], Any],
    summarize_cpcv_lite_fold_selection_fn: Callable[..., dict[str, Any]],
    summarize_cpcv_lite_pbo_fn: Callable[[list[dict[str, Any]]], dict[str, Any]],
    summarize_cpcv_lite_dsr_fn: Callable[[list[dict[str, Any]]], dict[str, Any]],
    split_train_label: str,
    split_test_label: str,
    split_drop_label: str,
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

    fold_specs, plan_report = build_cpcv_lite_plan_fn(
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
        train_all_mask = spec.labels == split_train_label
        test_mask = spec.labels == split_test_label
        row_counts = {
            "train": int(np.sum(train_all_mask)),
            "test": int(np.sum(test_mask)),
            "drop": int(np.sum(spec.labels == split_drop_label)),
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

        inner_masks = build_inner_train_valid_masks_fn(
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
        fit_bundle = fit_cpcv_lite_fold_model_fn(
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

        train_scores = predict_scores_fn(fit_bundle, train_all_x)
        test_scores = predict_scores_fn(fit_bundle, dataset.X[test_mask])
        fold_metrics = attach_ranking_metrics_fn(
            metrics=evaluate_split_fn(
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
        train_selection = build_window_selection_objectives_fn(
            scores=train_scores,
            y_reg=train_all_y_reg,
            ts_ms=train_all_ts,
            thresholds=thresholds,
            fee_bps_est=options.fee_bps_est,
            safety_bps=options.safety_bps,
            config=selection_grid_config_factory(),
        )
        test_selection = build_window_selection_objectives_fn(
            scores=test_scores,
            y_reg=dataset.y_reg[test_mask],
            ts_ms=dataset.ts_ms[test_mask],
            thresholds=thresholds,
            fee_bps_est=options.fee_bps_est,
            safety_bps=options.safety_bps,
            config=selection_grid_config_factory(),
        )
        selection_summary = summarize_cpcv_lite_fold_selection_fn(
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
    report["pbo"] = summarize_cpcv_lite_pbo_fn(folds)
    report["dsr"] = summarize_cpcv_lite_dsr_fn(folds)
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


def build_cpcv_inner_train_valid_masks(
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


def fit_cpcv_lite_fold_model(
    *,
    task: str,
    options: Any,
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
    fit_fixed_classifier_fn: Callable[..., dict[str, Any]],
    fit_fixed_regression_fn: Callable[..., dict[str, Any]],
    fit_fixed_ranker_fn: Callable[..., dict[str, Any]],
) -> dict[str, Any]:
    task_name = str(task).strip().lower()
    if task_name == "cls":
        return fit_fixed_classifier_fn(
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
        return fit_fixed_regression_fn(
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
    return fit_fixed_ranker_fn(
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


def _compact_eval_metrics(metrics: dict[str, Any]) -> dict[str, Any]:
    payload = dict(metrics or {})
    cls = payload.get("classification", {}) if isinstance(payload, dict) else {}
    ranking = payload.get("ranking", {}) if isinstance(payload, dict) else {}
    trading = payload.get("trading", {}) if isinstance(payload, dict) else {}
    return {
        "classification": {
            "roc_auc": _safe_float(cls.get("roc_auc")),
            "pr_auc": _safe_float(cls.get("pr_auc")),
            "log_loss": _safe_float(cls.get("log_loss")),
            "brier_score": _safe_float(cls.get("brier_score")),
        },
        "ranking": {
            "ndcg_at_5_mean": _safe_float(ranking.get("ndcg_at_5_mean")),
            "ndcg_full_mean": _safe_float(ranking.get("ndcg_full_mean")),
            "top1_match_rate": _safe_float(ranking.get("top1_match_rate")),
        },
        "trading": trading,
    }


def _safe_float(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except Exception:
        return 0.0
