"""Model fit helpers for trainer=v4_crypto_cs."""

from __future__ import annotations

from typing import Any, Callable

import numpy as np


def build_group_level_sample_weight(row_weights: np.ndarray, group_counts: np.ndarray) -> np.ndarray:
    weights = np.asarray(row_weights, dtype=np.float64)
    groups = np.asarray(group_counts, dtype=np.int64)
    if groups.size <= 0:
        return np.empty(0, dtype=np.float64)
    if weights.size == groups.size:
        return np.clip(weights, 1e-6, None)
    if int(groups.sum()) != int(weights.size):
        raise RuntimeError(
            "ranker sample_weight rows must match the total query-group rows "
            f"(weights={int(weights.size)}, grouped_rows={int(groups.sum())})"
        )
    group_weights = np.empty(groups.size, dtype=np.float64)
    offset = 0
    for index, group_size in enumerate(groups.tolist()):
        size = int(group_size)
        if size <= 0:
            group_weights[index] = 1.0
            continue
        group_slice = weights[offset : offset + size]
        group_weights[index] = float(np.mean(group_slice))
        offset += size
    return np.clip(group_weights, 1e-6, None)


def build_rank_relevance_labels(rank_values: np.ndarray) -> np.ndarray:
    values = np.asarray(rank_values, dtype=np.float64)
    finite = np.nan_to_num(values, nan=0.0, posinf=0.0, neginf=0.0)
    clipped = np.clip(finite, 0.0, 1.0)
    return np.rint(clipped * 1000.0).astype(np.int32, copy=False)


def fit_fixed_classifier_model(
    *,
    options: Any,
    best_params: dict[str, Any],
    x_train: np.ndarray,
    y_train: np.ndarray,
    w_train: np.ndarray,
    x_valid: np.ndarray,
    y_valid: np.ndarray,
    w_valid: np.ndarray,
    fold_index: int,
    try_import_xgboost_fn: Callable[[], Any],
    normalize_xgb_best_params_fn: Callable[[dict[str, Any] | None], dict[str, Any]],
) -> dict[str, Any]:
    xgb = try_import_xgboost_fn()
    if xgb is None:
        raise RuntimeError("xgboost is required for trainer=v4_crypto_cs")
    params = normalize_xgb_best_params_fn(best_params)
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


def fit_fixed_regression_model(
    *,
    options: Any,
    best_params: dict[str, Any],
    x_train: np.ndarray,
    y_train: np.ndarray,
    w_train: np.ndarray,
    x_valid: np.ndarray,
    y_valid: np.ndarray,
    w_valid: np.ndarray,
    fold_index: int,
    try_import_xgboost_fn: Callable[[], Any],
    normalize_xgb_best_params_fn: Callable[[dict[str, Any] | None], dict[str, Any]],
) -> dict[str, Any]:
    xgb = try_import_xgboost_fn()
    if xgb is None:
        raise RuntimeError("xgboost is required for trainer=v4_crypto_cs")
    params = normalize_xgb_best_params_fn(best_params)
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


def fit_fixed_ranker_model(
    *,
    options: Any,
    best_params: dict[str, Any],
    x_train: np.ndarray,
    y_train: np.ndarray,
    ts_train_ms: np.ndarray,
    w_train: np.ndarray,
    x_valid: np.ndarray,
    y_valid: np.ndarray,
    ts_valid_ms: np.ndarray,
    fold_index: int,
    try_import_xgboost_fn: Callable[[], Any],
    normalize_xgb_best_params_fn: Callable[[dict[str, Any] | None], dict[str, Any]],
    group_counts_by_ts_fn: Callable[[np.ndarray], np.ndarray],
) -> dict[str, Any]:
    xgb = try_import_xgboost_fn()
    if xgb is None:
        raise RuntimeError("xgboost is required for trainer=v4_crypto_cs")
    params = normalize_xgb_best_params_fn(best_params)
    train_group = group_counts_by_ts_fn(ts_train_ms)
    valid_group = group_counts_by_ts_fn(ts_valid_ms)
    if train_group.size <= 0 or valid_group.size <= 0:
        raise RuntimeError("ranker CPCV-lite fold requires grouped train and valid rows")
    row_train_w = np.asarray(w_train, dtype=np.float64)
    if row_train_w.size != y_train.size:
        row_train_w = np.ones(y_train.size, dtype=np.float64)
    train_w = build_group_level_sample_weight(row_train_w, train_group)
    y_train_rank = build_rank_relevance_labels(y_train)
    y_valid_rank = build_rank_relevance_labels(y_valid)
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
        "eval_set": [(x_valid, y_valid_rank)],
        "eval_group": [valid_group.tolist()],
        "verbose": False,
    }
    try:
        estimator.fit(
            x_train,
            y_train_rank,
            early_stopping_rounds=50,
            **fit_kwargs,
        )
    except TypeError:
        estimator.fit(x_train, y_train_rank, **fit_kwargs)
    return {"model_type": "xgboost_ranker", "scaler": None, "estimator": estimator}


def normalize_xgb_best_params(best_params: dict[str, Any] | None) -> dict[str, Any]:
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


def fit_booster_sweep_classifier_v4(
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
    try_import_xgboost_fn: Callable[[], Any],
    sample_xgb_params_fn: Callable[[np.random.Generator], dict[str, Any]],
    evaluate_split_fn: Callable[..., dict[str, Any]],
    build_v4_trainer_sweep_sort_key_fn: Callable[..., tuple[float, ...]],
    build_v4_shared_economic_objective_profile_fn: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    xgb = try_import_xgboost_fn()
    if xgb is None:
        raise RuntimeError("xgboost is required for trainer=v4_crypto_cs")

    rng = np.random.default_rng(int(seed))
    trial_rows: list[dict[str, Any]] = []
    best_key: tuple[float, ...] | None = None
    best_bundle: dict[str, Any] | None = None
    best_params: dict[str, Any] = {}
    economic_objective_profile_id = str(build_v4_shared_economic_objective_profile_fn().get("profile_id", "")).strip()

    w_train_safe = np.asarray(w_train, dtype=np.float64)
    w_valid_safe = np.asarray(w_valid, dtype=np.float64)
    if w_train_safe.size != y_train.size:
        w_train_safe = np.ones(y_train.size, dtype=np.float64)
    if w_valid_safe.size != y_valid.size:
        w_valid_safe = np.ones(y_valid.size, dtype=np.float64)
    w_train_safe = np.clip(w_train_safe, 1e-6, None)
    w_valid_safe = np.clip(w_valid_safe, 1e-6, None)

    for trial in range(max(int(trials), 1)):
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
        valid_metrics = evaluate_split_fn(
            y_cls=y_valid,
            y_reg=y_reg_valid,
            scores=valid_scores,
            markets=np.array(["_ALL_"] * int(y_valid.size), dtype=object),
            fee_bps_est=fee_bps_est,
            safety_bps=safety_bps,
        )
        key = build_v4_trainer_sweep_sort_key_fn(valid_metrics, task="cls")
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


def fit_booster_sweep_regression(
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
    try_import_xgboost_fn: Callable[[], Any],
    sample_xgb_params_fn: Callable[[np.random.Generator], dict[str, Any]],
    evaluate_split_fn: Callable[..., dict[str, Any]],
    build_v4_trainer_sweep_sort_key_fn: Callable[..., tuple[float, ...]],
    build_v4_shared_economic_objective_profile_fn: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    xgb = try_import_xgboost_fn()
    if xgb is None:
        raise RuntimeError("xgboost is required for trainer=v4_crypto_cs")

    rng = np.random.default_rng(int(seed))
    trial_rows: list[dict[str, Any]] = []
    best_key: tuple[float, ...] | None = None
    best_bundle: dict[str, Any] | None = None
    best_params: dict[str, Any] = {}
    economic_objective_profile_id = str(build_v4_shared_economic_objective_profile_fn().get("profile_id", "")).strip()

    w_train_safe = np.asarray(w_train, dtype=np.float64)
    w_valid_safe = np.asarray(w_valid, dtype=np.float64)
    if w_train_safe.size != y_train.size:
        w_train_safe = np.ones(y_train.size, dtype=np.float64)
    if w_valid_safe.size != y_valid_reg.size:
        w_valid_safe = np.ones(y_valid_reg.size, dtype=np.float64)
    w_train_safe = np.clip(w_train_safe, 1e-6, None)
    w_valid_safe = np.clip(w_valid_safe, 1e-6, None)

    for trial in range(max(int(trials), 1)):
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
        valid_metrics = evaluate_split_fn(
            y_cls=y_valid_cls,
            y_reg=y_valid_reg,
            scores=valid_scores,
            markets=np.array(["_ALL_"] * int(y_valid_cls.size), dtype=object),
            fee_bps_est=fee_bps_est,
            safety_bps=safety_bps,
        )
        key = build_v4_trainer_sweep_sort_key_fn(valid_metrics, task="reg")
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


def fit_booster_sweep_ranker(
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
    try_import_xgboost_fn: Callable[[], Any],
    sample_xgb_params_fn: Callable[[np.random.Generator], dict[str, Any]],
    evaluate_split_fn: Callable[..., dict[str, Any]],
    attach_ranking_metrics_fn: Callable[..., dict[str, Any]],
    build_v4_trainer_sweep_sort_key_fn: Callable[..., tuple[float, ...]],
    build_v4_shared_economic_objective_profile_fn: Callable[[], dict[str, Any]],
    group_counts_by_ts_fn: Callable[[np.ndarray], np.ndarray],
) -> dict[str, Any]:
    xgb = try_import_xgboost_fn()
    if xgb is None:
        raise RuntimeError("xgboost is required for trainer=v4_crypto_cs")

    rng = np.random.default_rng(int(seed))
    trial_rows: list[dict[str, Any]] = []
    best_key: tuple[float, ...] | None = None
    best_bundle: dict[str, Any] | None = None
    best_params: dict[str, Any] = {}
    economic_objective_profile_id = str(build_v4_shared_economic_objective_profile_fn().get("profile_id", "")).strip()

    train_group = group_counts_by_ts_fn(ts_train_ms)
    valid_group = group_counts_by_ts_fn(ts_valid_ms)
    if train_group.size <= 0 or valid_group.size <= 0:
        raise RuntimeError("ranker lane requires at least one timestamp-group in train and valid splits")

    y_train = build_rank_relevance_labels(y_train_rank)
    y_valid = build_rank_relevance_labels(y_valid_rank)
    row_train_w = np.asarray(w_train, dtype=np.float64)
    if row_train_w.size != y_train.size:
        row_train_w = np.ones(y_train.size, dtype=np.float64)
    w_train_safe = build_group_level_sample_weight(row_train_w, train_group)

    for trial in range(max(int(trials), 1)):
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
            random_state=int(seed + trial),
            nthread=max(int(nthread), 1),
            eval_metric="ndcg@5",
        )
        fit_kwargs = {
            "group": train_group.tolist(),
            "sample_weight": w_train_safe,
            "eval_set": [(x_valid, y_valid)],
            "eval_group": [valid_group.tolist()],
            "verbose": False,
        }
        try:
            estimator.fit(x_train, y_train, early_stopping_rounds=50, **fit_kwargs)
        except TypeError:
            estimator.fit(x_train, y_train, **fit_kwargs)

        valid_scores = 1.0 / (1.0 + np.exp(-np.asarray(estimator.predict(x_valid), dtype=np.float64)))
        valid_metrics = attach_ranking_metrics_fn(
            metrics=evaluate_split_fn(
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
        key = build_v4_trainer_sweep_sort_key_fn(valid_metrics, task="rank")
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
