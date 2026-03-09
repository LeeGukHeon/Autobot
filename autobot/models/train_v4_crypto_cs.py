"""Training orchestration for trainer=v4_crypto_cs."""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
import json
from pathlib import Path
import time
from typing import Any

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
from .multiple_testing import (
    build_trial_window_differential_matrix,
    run_hansen_spa,
    run_white_reality_check,
)
from .execution_acceptance import ExecutionAcceptanceOptions, run_execution_acceptance
from .runtime_recommendations import optimize_runtime_recommendations
from .model_card import render_model_card
from .research_acceptance import (
    compare_balanced_pareto,
    compare_spa_like_window_test,
    summarize_walk_forward_windows,
)
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
    SPLIT_TEST,
    SPLIT_TRAIN,
    SPLIT_VALID,
    compute_anchored_walk_forward_splits,
    compute_time_splits,
    split_masks,
)
from .train_v1 import (
    build_selection_recommendations,
    _build_thresholds,
    _estimate_dataset_memory_mb,
    _evaluate_split,
    _predict_scores,
    _sample_xgb_params,
    _try_import_xgboost,
    _validate_split_counts,
)
from .train_v3_mtf_micro import _fit_booster_sweep_weighted


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
    execution_acceptance_model_alpha: ModelAlphaSettings = ModelAlphaSettings(
        selection=ModelAlphaSelectionSettings(use_learned_recommendations=False),
        exit=ModelAlphaExitSettings(use_learned_hold_bars=False),
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
    execution_acceptance_report_path: Path | None = None
    runtime_recommendations_path: Path | None = None


def train_and_register_v4_crypto_cs(options: TrainV4CryptoCsOptions) -> TrainV4CryptoCsResult:
    task = str(options.task).strip().lower() or "cls"
    if task not in {"cls", "reg"}:
        raise ValueError("task currently supports only 'cls' or 'reg'")
    if options.feature_set != "v4":
        raise ValueError("trainer v4_crypto_cs requires --feature-set v4")
    if options.label_set != "v2":
        raise ValueError("trainer v4_crypto_cs requires --label-set v2")
    if _try_import_xgboost() is None:
        raise RuntimeError("xgboost is required for trainer=v4_crypto_cs")

    started_at = time.time()
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
    feature_cols = feature_columns_from_spec(options.dataset_root)
    dataset = load_feature_dataset(
        request,
        feature_columns=feature_cols,
        y_cls_column="y_cls_topq_12",
        y_reg_column="y_reg_net_12",
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
    w_train = dataset.sample_weight[train_mask]
    x_valid = dataset.X[valid_mask]
    y_valid = dataset.y_cls[valid_mask]
    y_reg_valid = dataset.y_reg[valid_mask]
    w_valid = dataset.sample_weight[valid_mask]
    x_test = dataset.X[test_mask]
    y_test = dataset.y_cls[test_mask]
    y_reg_test = dataset.y_reg[test_mask]
    market_valid = dataset.markets[valid_mask]
    market_test = dataset.markets[test_mask]

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
            trials=max(int(options.booster_sweep_trials), 1),
        )
    else:
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
            trials=max(int(options.booster_sweep_trials), 1),
        )

    valid_scores = _predict_scores(booster["bundle"], x_valid)
    test_scores = _predict_scores(booster["bundle"], x_test)
    valid_metrics = _evaluate_split(
        y_cls=y_valid,
        y_reg=y_reg_valid,
        scores=valid_scores,
        markets=market_valid,
        fee_bps_est=options.fee_bps_est,
        safety_bps=options.safety_bps,
    )
    test_metrics = _evaluate_split(
        y_cls=y_test,
        y_reg=y_reg_test,
        scores=test_scores,
        markets=market_test,
        fee_bps_est=options.fee_bps_est,
        safety_bps=options.safety_bps,
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
    )
    selection_recommendations = build_selection_recommendations_from_walk_forward(
        windows=walk_forward.get("windows", []),
        fallback_recommendations=fallback_selection_recommendations,
    )
    walk_forward = _finalize_walk_forward_report(
        walk_forward=walk_forward,
        selection_recommendations=selection_recommendations,
        options=options,
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
        best_params=dict(booster.get("best_params", {})),
        sweep_records=list(booster.get("trials", [])),
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
        )
    )
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
    execution_acceptance = _run_execution_acceptance_v4(
        options=options,
        run_id=run_id,
    )
    execution_acceptance_report_path = run_dir / "execution_acceptance_report.json"
    execution_acceptance_report_path.write_text(
        json.dumps(execution_acceptance, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    runtime_recommendations = _build_runtime_recommendations_v4(
        options=options,
        run_id=run_id,
    )
    runtime_recommendations_path = run_dir / "runtime_recommendations.json"
    runtime_recommendations_path.write_text(
        json.dumps(runtime_recommendations, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
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
    status = str(promotion.get("status", "candidate")).strip() or "candidate"

    finished_at = time.time()
    report_path = _write_train_report_v4(
        options.logs_root,
        {
            "run_id": run_id,
            "status": status,
            "task": task,
            "started_at_utc": datetime.fromtimestamp(started_at, tz=timezone.utc).isoformat(),
            "finished_at_utc": datetime.fromtimestamp(finished_at, tz=timezone.utc).isoformat(),
            "duration_sec": round(finished_at - started_at, 3),
            "rows": rows,
            "memory_estimate_mb": _estimate_dataset_memory_mb(dataset),
            "sweep_trials": booster.get("trials", []),
            "candidate": leaderboard_row,
            "walk_forward": walk_forward,
            "execution_acceptance": execution_acceptance,
            "runtime_recommendations": runtime_recommendations,
            "selection_recommendations": selection_recommendations,
            "promotion": promotion,
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
        execution_acceptance_report_path=execution_acceptance_report_path,
        runtime_recommendations_path=runtime_recommendations_path,
    )


def _run_walk_forward_v4(
    *,
    options: TrainV4CryptoCsOptions,
    task: str,
    dataset: Any,
    interval_ms: int,
    thresholds: dict[str, Any],
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
    sweep_trials = max(min(int(options.walk_forward_sweep_trials), int(options.booster_sweep_trials)), 1)
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
            w_train=dataset.sample_weight[train_mask],
            x_valid=dataset.X[valid_mask],
            y_valid_cls=dataset.y_cls[valid_mask],
            y_valid_reg=dataset.y_reg[valid_mask],
            w_valid=dataset.sample_weight[valid_mask],
            x_test=dataset.X[test_mask],
            y_test_cls=dataset.y_cls[test_mask],
            y_test_reg=dataset.y_reg[test_mask],
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

    report["windows"] = windows
    report["skipped_windows"] = skipped
    report["windows_generated"] = len(window_specs)
    report["trial_panel"] = _summarize_walk_forward_trial_panel(windows, threshold_key="top_5pct")
    return report


def _fit_walk_forward_window_model(
    *,
    task: str,
    options: TrainV4CryptoCsOptions,
    sweep_trials: int,
    x_train: np.ndarray,
    y_cls_train: np.ndarray,
    y_reg_train: np.ndarray,
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

    best_key: tuple[float, float, float] | None = None
    best_bundle: dict[str, Any] | None = None
    trial_records: list[dict[str, Any]] = []
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
        valid_cls = valid_metrics.get("classification", {}) if isinstance(valid_metrics, dict) else {}
        valid_top5 = (valid_metrics.get("trading", {}) or {}).get("top_5pct", {})
        key = (
            float(valid_top5.get("precision", 0.0)),
            float(valid_cls.get("pr_auc") or 0.0),
            float(valid_cls.get("roc_auc") or 0.0),
        )
        if best_key is None or key > best_key:
            best_key = key
            best_bundle = {"model_type": "xgboost", "scaler": None, "estimator": estimator}

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
                "valid_selection_key": _build_trial_selection_key(valid_metrics),
                "test_metrics": _compact_eval_metrics(test_metrics),
                "test_oos_periods": test_oos_periods,
                "test_oos_slices": test_oos_slices,
            }
        )
    if best_bundle is None:
        raise RuntimeError("walk-forward weighted sweep failed to produce a model")
    return {"bundle": best_bundle, "trial_records": trial_records}


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

    best_key: tuple[float, float, float] | None = None
    best_bundle: dict[str, Any] | None = None
    trial_records: list[dict[str, Any]] = []
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
        valid_cls = valid_metrics.get("classification", {}) if isinstance(valid_metrics, dict) else {}
        valid_top5 = (valid_metrics.get("trading", {}) or {}).get("top_5pct", {})
        key = (
            float(valid_top5.get("precision", 0.0)),
            float(valid_top5.get("ev_net", 0.0)),
            float(valid_cls.get("pr_auc") or 0.0),
        )
        if best_key is None or key > best_key:
            best_key = key
            best_bundle = {"model_type": "xgboost_regressor", "scaler": None, "estimator": estimator}

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
                "valid_selection_key": _build_trial_selection_key(valid_metrics),
                "test_metrics": _compact_eval_metrics(test_metrics),
                "test_oos_periods": test_oos_periods,
                "test_oos_slices": test_oos_slices,
            }
        )
    if best_bundle is None:
        raise RuntimeError("walk-forward regression sweep failed to produce a model")
    return {"bundle": best_bundle, "trial_records": trial_records}


def _compact_eval_metrics(metrics: dict[str, Any]) -> dict[str, Any]:
    cls = metrics.get("classification", {}) if isinstance(metrics, dict) else {}
    summary = metrics.get("per_market_summary", {}) if isinstance(metrics, dict) else {}
    return {
        "rows": int(metrics.get("rows", 0)) if isinstance(metrics, dict) else 0,
        "classification": {
            "roc_auc": _safe_float(cls.get("roc_auc")),
            "pr_auc": _safe_float(cls.get("pr_auc")),
            "log_loss": _safe_float(cls.get("log_loss")),
            "brier_score": _safe_float(cls.get("brier_score")),
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
    )
    report["hansen_spa"] = run_hansen_spa(
        rc_matrix,
        alpha=float(options.multiple_testing_alpha),
        bootstrap_iters=max(int(options.multiple_testing_bootstrap_iters), 100),
        seed=int(options.seed),
        average_block_length=(int(options.multiple_testing_block_length) if int(options.multiple_testing_block_length) > 0 else None),
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
                            "precision": 0.0,
                        }
                    }
                },
                "oos_slices": [
                    {
                        "slice_index": int(slice_doc.get("slice_index", -1)),
                        "metrics": {
                            "trading": {
                                str(threshold_key): {
                                    "ev_net": float(row["ev_net"]),
                                    "selected_rows": int(row["selected_rows"]),
                                    "precision": 0.0,
                                }
                            }
                        },
                    }
                    for slice_doc in ((row.get("window", {}) or {}).get("oos_slices") or [])
                    if isinstance(slice_doc, dict) and int(slice_doc.get("slice_index", -1)) >= 0
                ],
                "oos_periods": [
                    {
                        "period_index": int(period_doc.get("period_index", -1)),
                        "ts_ms": int(period_doc.get("ts_ms", -1)),
                        "metrics": {
                            "trading": {
                                str(threshold_key): {
                                    "ev_net": _safe_float(period_doc.get("ev_net")),
                                    "selected_rows": int(period_doc.get("selected_rows", 0) or 0),
                                    "precision": 0.0,
                                }
                            }
                        },
                    }
                    for period_doc in (row.get("period_results") or [])
                    if isinstance(period_doc, dict) and int(period_doc.get("period_index", -1)) >= 0
                ],
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
    return selection_key


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
    best_key: tuple[float, float, float] | None = None
    best_bundle: dict[str, Any] | None = None
    best_params: dict[str, Any] = {}

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
        cls = valid_metrics.get("classification", {}) if isinstance(valid_metrics, dict) else {}
        top5 = (valid_metrics.get("trading", {}) or {}).get("top_5pct", {})
        key = (
            float(top5.get("precision", 0.0)),
            float(top5.get("ev_net", 0.0)),
            float(cls.get("pr_auc") or 0.0),
        )
        trial_rows.append(
            {
                "trial": trial,
                "backend": "xgboost_regression",
                "params": params,
                "selection_key": {
                    "precision_top5": key[0],
                    "ev_net_top5": key[1],
                    "pr_auc": key[2],
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
    best_params: dict[str, Any],
    sweep_records: list[dict[str, Any]],
) -> dict[str, Any]:
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
            "backend": "xgboost",
            "params": best_params,
            "valid": valid_metrics,
            "test": test_metrics,
        },
        "booster_sweep": {"trials": len(sweep_records), "records": sweep_records},
        "champion": {
            "name": "booster",
            "backend": "xgboost",
            "params": best_params,
        },
        "champion_metrics": test_metrics,
        "walk_forward": walk_forward_summary,
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
    top5 = (test_metrics.get("trading", {}) or {}).get("top_5pct", {})
    return {
        "run_id": run_id,
        "created_at_utc": _utc_now(),
        "model_family": options.model_family,
        "trainer": "v4_crypto_cs",
        "task": task,
        "champion": "booster",
        "champion_backend": "xgboost",
        "test_roc_auc": _safe_float(cls.get("roc_auc")),
        "test_pr_auc": _safe_float(cls.get("pr_auc")),
        "test_log_loss": _safe_float(cls.get("log_loss")),
        "test_brier_score": _safe_float(cls.get("brier_score")),
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
    payload["task"] = task
    payload["y_cls_column"] = "y_cls_topq_12"
    payload["y_reg_column"] = "y_reg_net_12"
    payload["label_columns"] = ["y_cls_topq_12", "y_reg_net_12"]
    payload["selection_recommendations"] = selection_recommendations
    return payload


def _write_train_report_v4(logs_root: Path, payload: dict[str, Any]) -> Path:
    logs_root.mkdir(parents=True, exist_ok=True)
    path = logs_root / "train_v4_report.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


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


def _safe_float(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except Exception:
        return 0.0


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
