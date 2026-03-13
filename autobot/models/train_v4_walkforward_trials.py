"""Walk-forward trial-fit and OOS metric helpers for trainer=v4_crypto_cs."""

from __future__ import annotations

from typing import Any, Callable

import numpy as np

from .train_v4_models import build_group_level_sample_weight, build_rank_relevance_labels


def fit_walk_forward_weighted_trials(
    *,
    options: Any,
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
    try_import_xgboost_fn: Callable[[], Any],
    sample_xgb_params_fn: Callable[[np.random.Generator], dict[str, Any]],
    evaluate_split_fn: Callable[..., dict[str, Any]],
    build_v4_trainer_sweep_sort_key_fn: Callable[..., tuple[float, ...]],
    compact_eval_metrics_fn: Callable[[dict[str, Any]], dict[str, Any]],
    build_oos_period_metrics_fn: Callable[..., list[dict[str, Any]]],
    build_oos_slice_metrics_fn: Callable[..., list[dict[str, Any]]],
    build_trial_selection_key_fn: Callable[[dict[str, Any]], dict[str, float]],
    build_v4_shared_economic_objective_profile_fn: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    xgb = try_import_xgboost_fn()
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
    economic_objective_profile_id = str(build_v4_shared_economic_objective_profile_fn().get("profile_id", "")).strip()
    for trial in range(max(int(sweep_trials), 1)):
        params = sample_xgb_params_fn(rng)
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
        valid_metrics = evaluate_split_fn(
            y_cls=y_valid_cls,
            y_reg=y_valid_reg,
            scores=valid_scores,
            markets=np.array(["_ALL_"] * int(y_valid_cls.size), dtype=object),
            fee_bps_est=options.fee_bps_est,
            safety_bps=options.safety_bps,
        )
        key = build_v4_trainer_sweep_sort_key_fn(valid_metrics, task="cls")
        if best_key is None or key > best_key:
            best_key = key
            best_bundle = {"model_type": "xgboost", "scaler": None, "estimator": estimator}
            best_params = dict(params)

        test_scores = estimator.predict_proba(x_test)[:, 1]
        test_metrics = evaluate_split_fn(
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
                "valid_selection_key": build_trial_selection_key_fn(valid_metrics),
                "test_metrics": compact_eval_metrics_fn(test_metrics),
                "test_oos_periods": build_oos_period_metrics_fn(
                    ts_ms=ts_test_ms,
                    y_cls=y_test_cls,
                    y_reg=y_test_reg,
                    scores=test_scores,
                    markets=market_test,
                    fee_bps_est=options.fee_bps_est,
                    safety_bps=options.safety_bps,
                ),
                "test_oos_slices": build_oos_slice_metrics_fn(
                    ts_ms=ts_test_ms,
                    y_cls=y_test_cls,
                    y_reg=y_test_reg,
                    scores=test_scores,
                    markets=market_test,
                    fee_bps_est=options.fee_bps_est,
                    safety_bps=options.safety_bps,
                ),
            }
        )
    if best_bundle is None:
        raise RuntimeError("walk-forward weighted sweep failed to produce a model")
    return {"bundle": best_bundle, "trial_records": trial_records, "best_params": dict(best_params or {})}


def fit_walk_forward_regression_trials(
    *,
    options: Any,
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
    try_import_xgboost_fn: Callable[[], Any],
    sample_xgb_params_fn: Callable[[np.random.Generator], dict[str, Any]],
    evaluate_split_fn: Callable[..., dict[str, Any]],
    build_v4_trainer_sweep_sort_key_fn: Callable[..., tuple[float, ...]],
    compact_eval_metrics_fn: Callable[[dict[str, Any]], dict[str, Any]],
    build_oos_period_metrics_fn: Callable[..., list[dict[str, Any]]],
    build_oos_slice_metrics_fn: Callable[..., list[dict[str, Any]]],
    build_trial_selection_key_fn: Callable[[dict[str, Any]], dict[str, float]],
    build_v4_shared_economic_objective_profile_fn: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    xgb = try_import_xgboost_fn()
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
    economic_objective_profile_id = str(build_v4_shared_economic_objective_profile_fn().get("profile_id", "")).strip()
    for trial in range(max(int(sweep_trials), 1)):
        params = sample_xgb_params_fn(rng)
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
        valid_metrics = evaluate_split_fn(
            y_cls=y_valid_cls,
            y_reg=y_valid_reg,
            scores=valid_scores,
            markets=np.array(["_ALL_"] * int(y_valid_cls.size), dtype=object),
            fee_bps_est=options.fee_bps_est,
            safety_bps=options.safety_bps,
        )
        key = build_v4_trainer_sweep_sort_key_fn(valid_metrics, task="reg")
        if best_key is None or key > best_key:
            best_key = key
            best_bundle = {"model_type": "xgboost_regressor", "scaler": None, "estimator": estimator}
            best_params = dict(params)

        test_scores = 1.0 / (1.0 + np.exp(-np.asarray(estimator.predict(x_test), dtype=np.float64)))
        test_metrics = evaluate_split_fn(
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
                "valid_selection_key": build_trial_selection_key_fn(valid_metrics),
                "test_metrics": compact_eval_metrics_fn(test_metrics),
                "test_oos_periods": build_oos_period_metrics_fn(
                    ts_ms=ts_test_ms,
                    y_cls=y_test_cls,
                    y_reg=y_test_reg,
                    scores=test_scores,
                    markets=market_test,
                    fee_bps_est=options.fee_bps_est,
                    safety_bps=options.safety_bps,
                ),
                "test_oos_slices": build_oos_slice_metrics_fn(
                    ts_ms=ts_test_ms,
                    y_cls=y_test_cls,
                    y_reg=y_test_reg,
                    scores=test_scores,
                    markets=market_test,
                    fee_bps_est=options.fee_bps_est,
                    safety_bps=options.safety_bps,
                ),
            }
        )
    if best_bundle is None:
        raise RuntimeError("walk-forward regression sweep failed to produce a model")
    return {"bundle": best_bundle, "trial_records": trial_records, "best_params": dict(best_params or {})}


def fit_walk_forward_ranker_trials(
    *,
    options: Any,
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
    try_import_xgboost_fn: Callable[[], Any],
    sample_xgb_params_fn: Callable[[np.random.Generator], dict[str, Any]],
    evaluate_split_fn: Callable[..., dict[str, Any]],
    attach_ranking_metrics_fn: Callable[..., dict[str, Any]],
    build_v4_trainer_sweep_sort_key_fn: Callable[..., tuple[float, ...]],
    compact_eval_metrics_fn: Callable[[dict[str, Any]], dict[str, Any]],
    build_oos_period_metrics_fn: Callable[..., list[dict[str, Any]]],
    build_oos_slice_metrics_fn: Callable[..., list[dict[str, Any]]],
    build_trial_selection_key_fn: Callable[[dict[str, Any]], dict[str, float]],
    group_counts_by_ts_fn: Callable[[np.ndarray], np.ndarray],
    build_v4_shared_economic_objective_profile_fn: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    xgb = try_import_xgboost_fn()
    if xgb is None:
        raise RuntimeError("xgboost is required for trainer=v4_crypto_cs")

    rng = np.random.default_rng(int(options.seed))
    train_group = group_counts_by_ts_fn(ts_train_ms)
    valid_group = group_counts_by_ts_fn(ts_valid_ms)
    if train_group.size <= 0 or valid_group.size <= 0:
        raise RuntimeError("walk-forward ranker requires timestamp-grouped train and valid rows")

    y_train_rank_label = build_rank_relevance_labels(y_train_rank)
    y_valid_rank_label = build_rank_relevance_labels(y_valid_rank)
    row_train_w = np.asarray(w_train, dtype=np.float64)
    if row_train_w.size != y_train_rank_label.size:
        row_train_w = np.ones(y_train_rank_label.size, dtype=np.float64)
    w_train_safe = build_group_level_sample_weight(row_train_w, train_group)

    best_key: tuple[float, ...] | None = None
    best_bundle: dict[str, Any] | None = None
    best_params: dict[str, Any] | None = None
    trial_records: list[dict[str, Any]] = []
    economic_objective_profile_id = str(build_v4_shared_economic_objective_profile_fn().get("profile_id", "")).strip()
    for trial in range(max(int(sweep_trials), 1)):
        params = sample_xgb_params_fn(rng)
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
            "eval_set": [(x_valid, y_valid_rank_label)],
            "eval_group": [valid_group.tolist()],
            "verbose": False,
        }
        try:
            estimator.fit(
                x_train,
                y_train_rank_label,
                early_stopping_rounds=50,
                **fit_kwargs,
            )
        except TypeError:
            estimator.fit(x_train, y_train_rank_label, **fit_kwargs)

        valid_scores = 1.0 / (1.0 + np.exp(-np.asarray(estimator.predict(x_valid), dtype=np.float64)))
        valid_metrics = attach_ranking_metrics_fn(
            metrics=evaluate_split_fn(
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
        key = build_v4_trainer_sweep_sort_key_fn(valid_metrics, task="rank")
        if best_key is None or key > best_key:
            best_key = key
            best_bundle = {"model_type": "xgboost_ranker", "scaler": None, "estimator": estimator}
            best_params = dict(params)

        test_scores = 1.0 / (1.0 + np.exp(-np.asarray(estimator.predict(x_test), dtype=np.float64)))
        test_metrics = attach_ranking_metrics_fn(
            metrics=evaluate_split_fn(
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
        trial_records.append(
            {
                "trial": int(trial),
                "params": params,
                "economic_objective_profile_id": economic_objective_profile_id,
                "objective": "rank:pairwise",
                "grouping": {"query_key": "ts_ms"},
                "valid_selection_key": build_trial_selection_key_fn(valid_metrics),
                "test_metrics": compact_eval_metrics_fn(test_metrics),
                "test_oos_periods": build_oos_period_metrics_fn(
                    ts_ms=ts_test_ms,
                    y_cls=y_test_cls,
                    y_reg=y_test_reg,
                    scores=test_scores,
                    markets=market_test,
                    fee_bps_est=options.fee_bps_est,
                    safety_bps=options.safety_bps,
                ),
                "test_oos_slices": build_oos_slice_metrics_fn(
                    ts_ms=ts_test_ms,
                    y_cls=y_test_cls,
                    y_reg=y_test_reg,
                    scores=test_scores,
                    markets=market_test,
                    fee_bps_est=options.fee_bps_est,
                    safety_bps=options.safety_bps,
                ),
            }
        )
    if best_bundle is None:
        raise RuntimeError("walk-forward ranker sweep failed to produce a model")
    return {"bundle": best_bundle, "trial_records": trial_records, "best_params": dict(best_params or {})}


def compact_eval_metrics(metrics: dict[str, Any]) -> dict[str, Any]:
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
        "trading": compact_trading_metrics(metrics),
        "per_market_summary": {
            "market_count": int(summary.get("market_count", 0) or 0),
            "positive_markets": int(summary.get("positive_markets", 0) or 0),
        },
    }


def build_oos_slice_metrics(
    *,
    ts_ms: np.ndarray,
    y_cls: np.ndarray,
    y_reg: np.ndarray,
    scores: np.ndarray,
    markets: np.ndarray,
    fee_bps_est: float,
    safety_bps: float,
    evaluate_split_fn: Callable[..., dict[str, Any]],
    compact_eval_metrics_fn: Callable[[dict[str, Any]], dict[str, Any]],
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
        slice_metrics = evaluate_split_fn(
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
                "metrics": compact_eval_metrics_fn(slice_metrics),
            }
        )
    return slices


def build_oos_period_metrics(
    *,
    ts_ms: np.ndarray,
    y_cls: np.ndarray,
    y_reg: np.ndarray,
    scores: np.ndarray,
    markets: np.ndarray,
    fee_bps_est: float,
    safety_bps: float,
    evaluate_split_fn: Callable[..., dict[str, Any]],
    compact_eval_metrics_fn: Callable[[dict[str, Any]], dict[str, Any]],
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
        period_metrics = evaluate_split_fn(
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
                "metrics": compact_eval_metrics_fn(period_metrics),
            }
        )
    return periods


def compact_trading_metrics(metrics: dict[str, Any]) -> dict[str, Any]:
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


def build_trial_selection_key(metrics: dict[str, Any]) -> dict[str, float]:
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


def _safe_float(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except Exception:
        return 0.0
