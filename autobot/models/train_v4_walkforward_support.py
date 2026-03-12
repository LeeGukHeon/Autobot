"""Walk-forward support helpers for trainer=v4_crypto_cs."""

from __future__ import annotations

from typing import Any, Callable, Sequence

import numpy as np


def evaluate_factor_block_refit_window_evidence(
    *,
    window_index: int,
    task: str,
    options: Any,
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
    predict_scores_fn: Callable[..., Any],
    evaluate_split_fn: Callable[..., dict[str, Any]],
    attach_ranking_metrics_fn: Callable[..., dict[str, Any]],
    fit_fixed_classifier_fn: Callable[..., dict[str, Any]],
    fit_fixed_regression_fn: Callable[..., dict[str, Any]],
    fit_fixed_ranker_fn: Callable[..., dict[str, Any]],
    build_factor_block_window_baseline_fn: Callable[..., Any],
    build_factor_block_selection_signature_fn: Callable[..., Any],
    build_factor_block_window_row_fn: Callable[..., dict[str, Any]],
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

    full_scores = predict_scores_fn(full_bundle, x_test)
    baseline = build_factor_block_window_baseline_fn(
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

    full_metrics = evaluate_split_fn(
        y_cls=y_test_cls,
        y_reg=y_test_reg,
        scores=full_scores,
        markets=np.array(["_ALL_"] * int(y_test_cls.size), dtype=object),
        fee_bps_est=options.fee_bps_est,
        safety_bps=options.safety_bps,
    )
    full_metrics = attach_ranking_metrics_fn(
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
                dropped_bundle = fit_fixed_classifier_fn(
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
                dropped_bundle = fit_fixed_regression_fn(
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
                dropped_bundle = fit_fixed_ranker_fn(
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

        dropped_scores = predict_scores_fn(dropped_bundle, x_test_drop)
        dropped_metrics = evaluate_split_fn(
            y_cls=y_test_cls,
            y_reg=y_test_reg,
            scores=dropped_scores,
            markets=np.array(["_ALL_"] * int(y_test_cls.size), dtype=object),
            fee_bps_est=options.fee_bps_est,
            safety_bps=options.safety_bps,
        )
        dropped_metrics = attach_ranking_metrics_fn(
            metrics=dropped_metrics,
            y_rank=y_test_rank,
            ts_ms=ts_test_ms,
            scores=dropped_scores,
        )
        dropped_top5 = ((dropped_metrics.get("trading", {}) or {}).get("top_5pct", {})) if isinstance(dropped_metrics, dict) else {}
        dropped_signature = build_factor_block_selection_signature_fn(
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
            build_factor_block_window_row_fn(
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


def evaluate_factor_block_refit_window_rows(
    *,
    evaluate_factor_block_refit_window_evidence_fn: Callable[..., dict[str, Any]],
    **kwargs: Any,
) -> list[dict[str, Any]]:
    evidence = evaluate_factor_block_refit_window_evidence_fn(**kwargs)
    return list(evidence["rows"])


def summarize_walk_forward_trial_panel(
    windows: list[dict[str, Any]],
    *,
    threshold_key: str,
    summarize_walk_forward_windows_fn: Callable[..., dict[str, Any]],
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
        summary = summarize_walk_forward_windows_fn(windows_for_trial, threshold_key=threshold_key)
        node["summary"] = summary
        node["selected_threshold_key"] = str(threshold_key).strip() or "top_5pct"
        node["windows_run"] = int(summary.get("windows_run", 0) or 0)
        node["oos_period_count"] = len(node.get("oos_periods", []) or [])
        node["oos_slice_count"] = len(node.get("oos_slices", []) or [])
        trial_panel.append(node)
    return trial_panel


def build_selection_search_trial_panel(
    *,
    windows: list[dict[str, Any]],
    start_trial_id: int,
    summarize_walk_forward_windows_fn: Callable[..., dict[str, Any]],
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
                "summary": summarize_walk_forward_windows_fn(windows_for_trial, threshold_key=str(threshold_key)),
                "windows_run": len(windows_for_trial),
                "oos_slice_count": 0,
            }
        )
        trial_id += 1
    return panel


def _safe_float(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except Exception:
        return 0.0
