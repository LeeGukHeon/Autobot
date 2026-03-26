"""Stacked v5 panel ensemble trainer built on the current v4 artifact backbone."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import time
from typing import Any

import numpy as np

from autobot import __version__ as autobot_version

from . import train_v4_crypto_cs as v4
from . import train_v4_persistence as v4_persistence
from . import train_v4_postprocess as v4_postprocess
from .dataset_loader import (
    build_data_fingerprint,
    feature_columns_from_spec,
    load_feature_aux_frame,
    load_feature_spec,
    load_label_spec,
)
from .factor_block_selector import (
    build_factor_block_selection_report,
    resolve_selected_feature_columns_from_latest,
    v4_factor_block_registry,
)
from .model_card import render_model_card
from .registry import RegistrySavePayload, save_run, update_artifact_status, update_latest_pointer
from .research_acceptance import compare_balanced_pareto, summarize_walk_forward_windows
from .search_budget import resolve_v4_search_budget
from .selection_calibration import build_selection_calibration_by_score_source, build_selection_calibration_from_oos_rows
from .selection_optimizer import SelectionGridConfig, build_selection_recommendations_from_walk_forward, build_window_selection_objectives
from .selection_policy import build_selection_policy_from_recommendations
from .split import SPLIT_DROP, SPLIT_TEST, SPLIT_TRAIN, SPLIT_VALID, compute_anchored_walk_forward_splits, compute_time_splits, split_masks
from .train_v1 import _build_thresholds, _evaluate_split, _predict_scores, build_selection_recommendations
from .train_v4_artifacts import build_decision_surface_v4, build_v4_metrics_doc, train_config_snapshot_v4
from .train_v4_core import prepare_v4_training_inputs


TrainV5PanelEnsembleOptions = v4.TrainV4CryptoCsOptions
TrainV5PanelEnsembleResult = v4.TrainV4CryptoCsResult

_STACK_COMPONENT_ORDER = ("cls_score", "rank_score", "mu_h3", "mu_h6", "mu_h12", "mu_h24")


@dataclass(frozen=True)
class _StackMetaModel:
    intercept: float
    coefficients: tuple[float, ...]
    feature_shift: tuple[float, ...]
    feature_scale: tuple[float, ...]

    def predict_proba(self, x: np.ndarray) -> np.ndarray:
        matrix = np.asarray(x, dtype=np.float64)
        shift = np.asarray(self.feature_shift, dtype=np.float64)
        scale = np.asarray(self.feature_scale, dtype=np.float64)
        coeff = np.asarray(self.coefficients, dtype=np.float64)
        safe_scale = np.where(np.abs(scale) < 1e-12, 1.0, scale)
        normalized = (matrix - shift) / safe_scale
        logits = normalized @ coeff + float(self.intercept)
        probs = 1.0 / (1.0 + np.exp(-np.clip(logits, -40.0, 40.0)))
        return np.column_stack([1.0 - probs, probs])


@dataclass(frozen=True)
class V5PanelEnsembleEstimator:
    classifier_bundle: dict[str, Any]
    ranker_bundle: dict[str, Any]
    regressor_bundles: dict[str, dict[str, Any]]
    regression_member_bundles: dict[str, tuple[dict[str, Any], ...]]
    meta_model: _StackMetaModel
    meta_ensemble: tuple[_StackMetaModel, ...]
    regression_horizons: tuple[int, ...]
    uncertainty_temperature: float = 1.0

    def _predict_regression_distribution(self, x: np.ndarray) -> dict[str, dict[str, np.ndarray]]:
        distributions: dict[str, dict[str, np.ndarray]] = {}
        for horizon in self.regression_horizons:
            horizon_key = f"h{int(horizon)}"
            base_bundle = self.regressor_bundles[horizon_key]
            member_bundles = self.regression_member_bundles.get(horizon_key) or ()
            member_predictions: list[np.ndarray] = []
            for member_bundle in member_bundles:
                estimator = member_bundle.get("estimator") if isinstance(member_bundle, dict) else None
                if estimator is None:
                    continue
                member_predictions.append(np.asarray(estimator.predict(x), dtype=np.float64))
            if not member_predictions:
                member_predictions.append(np.asarray(base_bundle["estimator"].predict(x), dtype=np.float64))
            member_matrix = np.column_stack(member_predictions)
            q10 = np.quantile(member_matrix, 0.10, axis=1)
            q50 = np.quantile(member_matrix, 0.50, axis=1)
            q90 = np.quantile(member_matrix, 0.90, axis=1)
            sigma = np.std(member_matrix, axis=1, ddof=0)
            es10 = np.array(
                [
                    float(np.mean(row[row <= quantile])) if np.any(row <= quantile) else float(quantile)
                    for row, quantile in zip(member_matrix, q10, strict=False)
                ],
                dtype=np.float64,
            )
            distributions[horizon_key] = {
                "member_matrix": member_matrix,
                "mu": np.asarray(base_bundle["estimator"].predict(x), dtype=np.float64),
                "q10": q10,
                "q50": q50,
                "q90": q90,
                "sigma": sigma,
                "expected_shortfall_proxy": es10,
            }
        return distributions

    def _component_payload(self, x: np.ndarray) -> dict[str, Any]:
        cls_score = _predict_scores(self.classifier_bundle, x)
        rank_score = _predict_scores(self.ranker_bundle, x)
        mu_by_horizon: dict[int, np.ndarray] = {}
        reg_prob_parts: list[np.ndarray] = []
        regression_distribution = self._predict_regression_distribution(x)
        for horizon in self.regression_horizons:
            horizon_key = f"h{int(horizon)}"
            raw = np.asarray(regression_distribution[horizon_key]["mu"], dtype=np.float64)
            mu_by_horizon[int(horizon)] = raw
            reg_prob_parts.append(_sigmoid(raw))
        component_matrix = np.column_stack([cls_score, rank_score, *reg_prob_parts])
        component_std = np.std(component_matrix, axis=1, ddof=0)
        return {
            "cls_score": cls_score,
            "rank_score": rank_score,
            "mu_by_horizon": mu_by_horizon,
            "regression_distribution": regression_distribution,
            "component_matrix": component_matrix,
            "component_std": component_std,
        }

    def predict_proba(self, x: np.ndarray) -> np.ndarray:
        payload = self._component_payload(x)
        return self.meta_model.predict_proba(payload["component_matrix"])

    def predict(self, x: np.ndarray) -> np.ndarray:
        return self.predict_proba(x)[:, 1]

    def predict_uncertainty(self, x: np.ndarray) -> np.ndarray:
        payload = self._component_payload(x)
        if self.meta_ensemble:
            member_scores = np.column_stack(
                [member.predict_proba(payload["component_matrix"])[:, 1] for member in self.meta_ensemble]
            )
            return np.std(member_scores, axis=1, ddof=0) * max(float(self.uncertainty_temperature), 1e-6)
        return np.asarray(payload["component_std"], dtype=np.float64) * max(float(self.uncertainty_temperature), 1e-6)

    def predict_mu_horizons(self, x: np.ndarray) -> dict[str, np.ndarray]:
        payload = self._component_payload(x)
        return {f"h{horizon}": values for horizon, values in payload["mu_by_horizon"].items()}

    def predict_distributional_contract(self, x: np.ndarray) -> dict[str, dict[str, np.ndarray]]:
        payload = self._component_payload(x)
        quantiles_by_horizon: dict[str, np.ndarray] = {}
        sigma_by_horizon: dict[str, np.ndarray] = {}
        es_proxy_by_horizon: dict[str, np.ndarray] = {}
        mu_by_horizon: dict[str, np.ndarray] = {}
        for horizon in self.regression_horizons:
            horizon_key = f"h{int(horizon)}"
            distribution = payload["regression_distribution"][horizon_key]
            quantiles_by_horizon[horizon_key] = np.column_stack(
                [
                    np.asarray(distribution["q10"], dtype=np.float64),
                    np.asarray(distribution["q50"], dtype=np.float64),
                    np.asarray(distribution["q90"], dtype=np.float64),
                ]
            )
            sigma_by_horizon[horizon_key] = np.asarray(distribution["sigma"], dtype=np.float64)
            es_proxy_by_horizon[horizon_key] = np.asarray(distribution["expected_shortfall_proxy"], dtype=np.float64)
            mu_by_horizon[horizon_key] = np.asarray(distribution["mu"], dtype=np.float64)
        return {
            "mu_by_horizon": mu_by_horizon,
            "return_quantiles_by_horizon": quantiles_by_horizon,
            "sigma_by_horizon": sigma_by_horizon,
            "expected_shortfall_proxy_by_horizon": es_proxy_by_horizon,
        }


def _sigmoid(values: np.ndarray) -> np.ndarray:
    raw = np.asarray(values, dtype=np.float64)
    return 1.0 / (1.0 + np.exp(-np.clip(raw, -40.0, 40.0)))


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _load_v5_regression_targets(
    *,
    request: Any,
    label_spec: dict[str, Any],
    dataset: Any,
    primary_y_cls_column: str,
    primary_y_reg_column: str,
    primary_y_rank_column: str,
) -> dict[str, np.ndarray]:
    column_families = dict((label_spec.get("canonical_multi_horizon_columns") or {}))
    residual_columns = column_families.get("y_reg_resid_leader")
    if not isinstance(residual_columns, list) or not residual_columns:
        raise ValueError("label_spec missing canonical_multi_horizon_columns.y_reg_resid_leader for v5_panel_ensemble")
    aux = load_feature_aux_frame(
        request,
        columns=tuple(str(item).strip() for item in residual_columns if str(item).strip()),
        y_cls_column=primary_y_cls_column,
        y_reg_column=primary_y_reg_column,
        y_rank_column=primary_y_rank_column,
    )
    aux_ts_ms = aux.get_column("ts_ms").to_numpy().astype(np.int64, copy=False)
    aux_markets = aux.get_column("market").to_numpy()
    if not np.array_equal(aux_ts_ms, dataset.ts_ms) or not np.array_equal(aux_markets, dataset.markets):
        raise ValueError("V5_PANEL_TARGET_ALIGNMENT_FAILED")
    targets: dict[str, np.ndarray] = {}
    for name in residual_columns:
        column = str(name).strip()
        horizon_token = column.split("_h")[-1]
        targets[f"h{horizon_token}"] = aux.get_column(column).to_numpy().astype(np.float64, copy=False)
    return targets


def _fit_v5_regression_heads(
    *,
    options: TrainV5PanelEnsembleOptions,
    dataset: Any,
    regression_targets: dict[str, np.ndarray],
    train_mask: np.ndarray,
    valid_mask: np.ndarray,
    sweep_trials: int,
) -> dict[str, dict[str, Any]]:
    results: dict[str, dict[str, Any]] = {}
    for horizon_key, target in regression_targets.items():
        results[horizon_key] = v4._fit_booster_sweep_regression(
            x_train=dataset.X[train_mask],
            y_train=np.asarray(target[train_mask], dtype=np.float64),
            w_train=dataset.sample_weight[train_mask],
            x_valid=dataset.X[valid_mask],
            y_valid_cls=dataset.y_cls[valid_mask],
            y_valid_reg=np.asarray(target[valid_mask], dtype=np.float64),
            w_valid=dataset.sample_weight[valid_mask],
            fee_bps_est=options.fee_bps_est,
            safety_bps=options.safety_bps,
            seed=options.seed + int(horizon_key.replace("h", "")),
            nthread=options.nthread,
            trials=sweep_trials,
            eval_sample_weight=dataset.sample_weight[valid_mask],
        )
    return results


def _build_component_matrix(
    *,
    x: np.ndarray,
    classifier_bundle: dict[str, Any],
    ranker_bundle: dict[str, Any],
    regressor_bundles: dict[str, dict[str, Any]],
) -> tuple[np.ndarray, dict[str, np.ndarray]]:
    cls_score = _predict_scores(classifier_bundle["bundle"] if "bundle" in classifier_bundle else classifier_bundle, x)
    rank_score = _predict_scores(ranker_bundle["bundle"] if "bundle" in ranker_bundle else ranker_bundle, x)
    reg_keys = sorted(regressor_bundles.keys(), key=lambda item: int(item.replace("h", "")))
    raw_mus = {
        key: np.asarray(
            (regressor_bundles[key]["bundle"]["estimator"] if "bundle" in regressor_bundles[key] else regressor_bundles[key]["estimator"]).predict(x),
            dtype=np.float64,
        )
        for key in reg_keys
    }
    reg_probs = [_sigmoid(values) for values in raw_mus.values()]
    matrix = np.column_stack([cls_score, rank_score, *reg_probs])
    payload = {
        "cls_score": cls_score,
        "rank_score": rank_score,
        **{f"mu_{key}": values for key, values in raw_mus.items()},
    }
    return matrix, payload


def _fit_meta_logistic(meta_x: np.ndarray, meta_y: np.ndarray, *, sample_weight: np.ndarray | None = None) -> dict[str, Any]:
    x_values = np.asarray(meta_x, dtype=np.float64)
    y_values = np.asarray(meta_y, dtype=np.int64)
    shift = np.mean(x_values, axis=0) if x_values.size > 0 else np.zeros(x_values.shape[1], dtype=np.float64)
    scale = np.std(x_values, axis=0, ddof=0) if x_values.size > 0 else np.ones(x_values.shape[1], dtype=np.float64)
    scale = np.where(np.abs(scale) < 1e-12, 1.0, scale)
    normalized = (x_values - shift) / scale
    if np.unique(y_values).size < 2:
        probability = float(np.mean(y_values)) if y_values.size > 0 else 0.5
        intercept = float(np.log(np.clip(probability, 1e-6, 1.0 - 1e-6) / np.clip(1.0 - probability, 1e-6, 1.0)))
        model = _StackMetaModel(
            intercept=intercept,
            coefficients=tuple([0.0] * normalized.shape[1]),
            feature_shift=tuple(shift.tolist()),
            feature_scale=tuple(scale.tolist()),
        )
        return {"meta_model": model, "uncertainty_temperature": 1.0}

    try:
        from sklearn.linear_model import LogisticRegression
    except Exception as exc:
        raise RuntimeError(f"sklearn logistic regression is required for v5 panel stacking: {exc}") from exc

    estimator = LogisticRegression(max_iter=1000, solver="lbfgs")
    fit_kwargs: dict[str, Any] = {}
    if sample_weight is not None and np.asarray(sample_weight).size == y_values.size:
        fit_kwargs["sample_weight"] = np.clip(np.asarray(sample_weight, dtype=np.float64), 1e-6, None)
    estimator.fit(normalized, y_values, **fit_kwargs)
    model = _StackMetaModel(
        intercept=float(np.asarray(estimator.intercept_, dtype=np.float64)[0]),
        coefficients=tuple(np.asarray(estimator.coef_, dtype=np.float64)[0].tolist()),
        feature_shift=tuple(shift.tolist()),
        feature_scale=tuple(scale.tolist()),
    )
    return {"meta_model": model, "uncertainty_temperature": 1.0}


def _build_v5_oof_windows(
    *,
    options: TrainV5PanelEnsembleOptions,
    dataset: Any,
    primary_y_reg: np.ndarray,
    regression_targets: dict[str, np.ndarray],
    classifier_best_params: dict[str, Any],
    ranker_best_params: dict[str, Any],
    regressor_best_params: dict[str, dict[str, Any]],
    action_aux_arrays: dict[str, np.ndarray],
) -> dict[str, Any]:
    windows: list[dict[str, Any]] = []
    skipped_windows: list[dict[str, Any]] = []
    selection_calibration_rows: list[dict[str, Any]] = []
    trade_action_rows: list[dict[str, Any]] = []
    meta_rows: list[dict[str, Any]] = []
    meta_weight_parts: list[np.ndarray] = []
    meta_models: list[_StackMetaModel] = []
    regression_member_bundles: dict[str, list[dict[str, Any]]] = {
        str(key): [] for key in regression_targets.keys()
    }
    try:
        window_specs = compute_anchored_walk_forward_splits(
            dataset.ts_ms,
            valid_ratio=options.valid_ratio,
            test_ratio=options.test_ratio,
            window_count=max(int(options.walk_forward_windows), 1),
            embargo_bars=options.embargo_bars,
            interval_ms=v4.expected_interval_ms(options.tf),
        )
    except ValueError as exc:
        return {
            "windows": [],
            "skipped_windows": [{"window_index": -1, "reason": str(exc)}],
            "_selection_calibration_rows": [],
            "_trade_action_oos_rows": [],
            "meta_rows": [],
            "sample_weight": np.empty(0, dtype=np.float64),
        }

    raw_window_rows: list[dict[str, Any]] = []
    for labels, info in window_specs:
        masks = split_masks(labels)
        train_mask = masks[SPLIT_TRAIN]
        valid_mask = masks[SPLIT_VALID]
        test_mask = masks[SPLIT_TEST]
        row_counts = {
            "train": int(np.sum(train_mask)),
            "valid": int(np.sum(valid_mask)),
            "test": int(np.sum(test_mask)),
            "drop": int(np.sum(masks[SPLIT_DROP])),
        }
        if row_counts["train"] <= 0 or row_counts["valid"] <= 0 or row_counts["test"] <= 0:
            skipped_windows.append({"window_index": int(info.window_index), "counts": row_counts, "reason": "INSUFFICIENT_ROWS"})
            continue

        cls_window = v4._fit_fixed_classifier_model(
            options=options,
            best_params=classifier_best_params,
            x_train=dataset.X[train_mask],
            y_train=dataset.y_cls[train_mask],
            w_train=dataset.sample_weight[train_mask],
            x_valid=dataset.X[valid_mask],
            y_valid=dataset.y_cls[valid_mask],
            w_valid=dataset.sample_weight[valid_mask],
            fold_index=int(info.window_index),
        )
        rank_window = v4._fit_fixed_ranker_model(
            options=options,
            best_params=ranker_best_params,
            x_train=dataset.X[train_mask],
            y_train=dataset.y_rank[train_mask],
            ts_train_ms=dataset.ts_ms[train_mask],
            w_train=dataset.sample_weight[train_mask],
            x_valid=dataset.X[valid_mask],
            y_valid=dataset.y_rank[valid_mask],
            ts_valid_ms=dataset.ts_ms[valid_mask],
            fold_index=int(info.window_index),
        )
        reg_windows: dict[str, dict[str, Any]] = {}
        for horizon_key, target in regression_targets.items():
            reg_windows[horizon_key] = v4._fit_fixed_regression_model(
                options=options,
                best_params=regressor_best_params[horizon_key],
                x_train=dataset.X[train_mask],
                y_train=np.asarray(target[train_mask], dtype=np.float64),
                w_train=dataset.sample_weight[train_mask],
                x_valid=dataset.X[valid_mask],
                y_valid=np.asarray(target[valid_mask], dtype=np.float64),
                w_valid=dataset.sample_weight[valid_mask],
                fold_index=int(info.window_index),
            )
            member_bundle = reg_windows[horizon_key].get("bundle")
            if isinstance(member_bundle, dict):
                regression_member_bundles[horizon_key].append(member_bundle)

        meta_x, payload = _build_component_matrix(
            x=dataset.X[test_mask],
            classifier_bundle=cls_window,
            ranker_bundle=rank_window,
            regressor_bundles=reg_windows,
        )
        raw_window_rows.append(
            {
                "window_index": int(info.window_index),
                "counts": row_counts,
                "time_window": {
                    "valid_start_ts": int(info.valid_start_ts),
                    "test_start_ts": int(info.test_start_ts),
                    "test_end_ts": int(info.test_end_ts),
                },
                "meta_x": meta_x,
                "y_cls": np.asarray(dataset.y_cls[test_mask], dtype=np.int64),
                "y_reg": np.asarray(primary_y_reg[test_mask], dtype=np.float64),
                "y_rank": np.asarray(dataset.y_rank[test_mask], dtype=np.float64),
                "sample_weight": np.asarray(dataset.sample_weight[test_mask], dtype=np.float64),
                "markets": np.asarray(dataset.markets[test_mask], dtype=object),
                "ts_ms": np.asarray(dataset.ts_ms[test_mask], dtype=np.int64),
                "payload": payload,
                "mask": np.asarray(test_mask, dtype=bool),
            }
        )

    for window in raw_window_rows:
        other_windows = [item for item in raw_window_rows if item["window_index"] != window["window_index"]]
        if other_windows:
            meta_x_train = np.concatenate([item["meta_x"] for item in other_windows], axis=0)
            meta_y_train = np.concatenate([item["y_cls"] for item in other_windows], axis=0)
            meta_w_train = np.concatenate([item["sample_weight"] for item in other_windows], axis=0)
        else:
            meta_x_train = np.asarray(window["meta_x"], dtype=np.float64)
            meta_y_train = np.asarray(window["y_cls"], dtype=np.int64)
            meta_w_train = np.asarray(window["sample_weight"], dtype=np.float64)

        meta_fit = _fit_meta_logistic(meta_x_train, meta_y_train, sample_weight=meta_w_train)
        meta_models.append(meta_fit["meta_model"])
        final_scores = meta_fit["meta_model"].predict_proba(window["meta_x"])[:, 1]
        lcb_scores = np.clip(final_scores - np.std(window["meta_x"], axis=1, ddof=0), 0.0, 1.0)
        metrics = v4._attach_ranking_metrics(
            metrics=_evaluate_split(
                y_cls=window["y_cls"],
                y_reg=window["y_reg"],
                scores=final_scores,
                markets=window["markets"],
                fee_bps_est=options.fee_bps_est,
                safety_bps=options.safety_bps,
                sample_weight=window["sample_weight"],
            ),
            y_rank=window["y_rank"],
            ts_ms=window["ts_ms"],
            scores=final_scores,
        )
        windows.append(
            {
                "window_index": int(window["window_index"]),
                "time_window": dict(window["time_window"]),
                "counts": dict(window["counts"]),
                "metrics": v4._compact_eval_metrics(metrics),
                "oos_periods": v4._build_oos_period_metrics(
                    ts_ms=window["ts_ms"],
                    y_cls=window["y_cls"],
                    y_reg=window["y_reg"],
                    scores=final_scores,
                    markets=window["markets"],
                    fee_bps_est=options.fee_bps_est,
                    safety_bps=options.safety_bps,
                ),
                "oos_slices": v4._build_oos_slice_metrics(
                    ts_ms=window["ts_ms"],
                    y_cls=window["y_cls"],
                    y_reg=window["y_reg"],
                    scores=final_scores,
                    markets=window["markets"],
                    fee_bps_est=options.fee_bps_est,
                    safety_bps=options.safety_bps,
                ),
                "selection_optimization": build_window_selection_objectives(
                    scores=final_scores,
                    y_reg=window["y_reg"],
                    ts_ms=window["ts_ms"],
                    thresholds={},
                    fee_bps_est=options.fee_bps_est,
                    safety_bps=options.safety_bps,
                    config=SelectionGridConfig(),
                ),
                "selection_optimization_by_score_source": {
                    "score_mean": build_window_selection_objectives(
                        scores=final_scores,
                        y_reg=window["y_reg"],
                        ts_ms=window["ts_ms"],
                        thresholds={},
                        fee_bps_est=options.fee_bps_est,
                        safety_bps=options.safety_bps,
                        config=SelectionGridConfig(),
                    ),
                    "score_lcb": build_window_selection_objectives(
                        scores=lcb_scores,
                        y_reg=window["y_reg"],
                        ts_ms=window["ts_ms"],
                        thresholds={},
                        fee_bps_est=options.fee_bps_est,
                        safety_bps=options.safety_bps,
                        config=SelectionGridConfig(),
                    ),
                },
                "trial_records": [],
            }
        )
        selection_calibration_rows.append(
            {
                "window_index": int(window["window_index"]),
                "scores": np.asarray(final_scores, dtype=np.float64).tolist(),
                "score_lcb": np.asarray(lcb_scores, dtype=np.float64).tolist(),
                "y_cls": np.asarray(window["y_cls"], dtype=np.int64).tolist(),
            }
        )
        mask = np.asarray(window["mask"], dtype=bool)
        trade_action_rows.append(
            {
                "window_index": int(window["window_index"]),
                "raw_scores": np.asarray(final_scores, dtype=np.float64).tolist(),
                "markets": np.asarray(window["markets"], dtype=object).tolist(),
                "ts_ms": np.asarray(window["ts_ms"], dtype=np.int64).tolist(),
                "close": np.asarray(action_aux_arrays.get("close", np.array([]))[mask], dtype=np.float64).tolist(),
                "rv_12": np.asarray(action_aux_arrays.get("rv_12", np.array([]))[mask], dtype=np.float64).tolist(),
                "rv_36": np.asarray(action_aux_arrays.get("rv_36", np.array([]))[mask], dtype=np.float64).tolist(),
                "atr_14": np.asarray(action_aux_arrays.get("atr_14", np.array([]))[mask], dtype=np.float64).tolist(),
                "atr_pct_14": np.asarray(action_aux_arrays.get("atr_pct_14", np.array([]))[mask], dtype=np.float64).tolist(),
            }
        )
        meta_rows.append({"x": np.asarray(window["meta_x"], dtype=np.float64), "y": np.asarray(window["y_cls"], dtype=np.int64)})
        meta_weight_parts.append(np.asarray(window["sample_weight"], dtype=np.float64))

    return {
        "windows": windows,
        "skipped_windows": skipped_windows,
        "_selection_calibration_rows": selection_calibration_rows,
        "_trade_action_oos_rows": trade_action_rows,
        "meta_rows": meta_rows,
        "sample_weight": np.concatenate(meta_weight_parts, axis=0) if meta_weight_parts else np.empty(0, dtype=np.float64),
        "meta_models": meta_models,
        "regression_member_bundles": {key: tuple(value) for key, value in regression_member_bundles.items()},
    }


def _build_walk_forward_report_v5(
    *,
    windows: list[dict[str, Any]],
    skipped: list[dict[str, Any]],
    selection_calibration_rows: list[dict[str, Any]],
    trade_action_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "policy": "stacked_panel_oof_v1",
        "enabled": True,
        "windows_requested": len(windows) + len(skipped),
        "windows": windows,
        "skipped_windows": skipped,
        "summary": summarize_walk_forward_windows(windows),
        "compare_to_champion": compare_balanced_pareto({}, {}),
        "selected_threshold_key": "top_5pct",
        "selected_threshold_key_source": "walk_forward_objective_optimizer",
        "_factor_block_window_rows": [],
        "_selection_calibration_rows": selection_calibration_rows,
        "_trade_action_oos_rows": trade_action_rows,
        "factor_block_refit_windows": [],
        "trial_panel": [],
    }


def _build_disabled_cpcv_lite(*, trigger: str) -> dict[str, Any]:
    return {
        "policy": "cpcv_lite_research_v1",
        "enabled": False,
        "trigger": trigger,
        "estimate_label": "disabled",
        "summary": {"status": "disabled", "reasons": ["V5_PANEL_ENSEMBLE_CPCV_NOT_IMPLEMENTED"]},
        "folds": [],
        "skipped_folds": [],
    }


def _build_v5_metrics_doc(
    *,
    run_id: str,
    options: TrainV5PanelEnsembleOptions,
    split_info: Any,
    interval_ms: int,
    rows: dict[str, int],
    valid_metrics: dict[str, Any],
    test_metrics: dict[str, Any],
    walk_forward: dict[str, Any],
    cpcv_lite: dict[str, Any],
    factor_block_selection: dict[str, Any],
    search_budget_decision: dict[str, Any],
    economic_objective_profile: dict[str, Any],
    lane_governance: dict[str, Any],
    cls_bundle: dict[str, Any],
    rank_bundle: dict[str, Any],
    regressor_results: dict[str, dict[str, Any]],
    meta_fit: dict[str, Any],
    meta_ensemble_count: int,
) -> dict[str, Any]:
    metrics = build_v4_metrics_doc(
        run_id=run_id,
        options=options,
        task="cls",
        split_info=split_info,
        interval_ms=interval_ms,
        rows=rows,
        valid_metrics=valid_metrics,
        test_metrics=test_metrics,
        walk_forward_summary=walk_forward.get("summary", {}),
        cpcv_lite_summary=(cpcv_lite.get("summary") or {}),
        factor_block_selection_summary=(factor_block_selection.get("summary") or {}),
        best_params={"meta_model": "logistic_stack"},
        sweep_records=[],
        ranker_budget_profile={"profile": "v5_panel_ensemble"},
        cpcv_lite_runtime={"enabled": False, "trigger": "disabled"},
        search_budget_decision=search_budget_decision,
        economic_objective_profile=economic_objective_profile,
        lane_governance=lane_governance,
    )
    metrics["trainer"] = "v5_panel_ensemble"
    metrics["panel_ensemble"] = {
        "version": 1,
        "policy": "v5_panel_ensemble_v1",
        "component_order": list(_STACK_COMPONENT_ORDER),
        "regression_horizons": [int(key.replace("h", "")) for key in sorted(regressor_results.keys(), key=lambda item: int(item.replace("h", "")))],
        "classifier_backend": "xgboost",
        "ranker_backend": "xgboost_ranker",
        "regressor_backend": "xgboost_regressor",
        "stack_meta_model": "logistic_regression",
        "classifier_best_params": dict(cls_bundle.get("best_params", {})),
        "ranker_best_params": dict(rank_bundle.get("best_params", {})),
        "regressor_best_params": {key: dict(value.get("best_params", {})) for key, value in regressor_results.items()},
        "uncertainty_mode": "walk_forward_meta_ensemble_std_v1",
        "uncertainty_member_count": int(meta_ensemble_count),
        "uncertainty_temperature": float(meta_fit.get("uncertainty_temperature", 1.0)),
        "distributional_contract": {
            "version": 1,
            "quantile_levels": [0.10, 0.50, 0.90],
            "return_quantiles_field_prefix": "return_quantiles",
            "sigma_field_prefix": "return_sigma",
            "expected_shortfall_proxy_field_prefix": "return_expected_shortfall_proxy",
            "horizon_keys": [f"h{int(key.replace('h', ''))}" for key in sorted(regressor_results.keys(), key=lambda item: int(item.replace("h", "")))],
            "member_ensemble_source": "walk_forward_regression_members",
        },
        "final_output_contract": {
            "score_field": "final_rank_score",
            "score_mean_field": "score_mean",
            "score_std_field": "score_std",
            "score_lcb_field": "score_lcb",
            "uncertainty_field": "score_std",
            "score_aliases": {"final_rank_score": "score_mean", "final_uncertainty": "score_std"},
        },
    }
    return metrics


def _build_v5_leaderboard_row(
    *,
    run_id: str,
    options: TrainV5PanelEnsembleOptions,
    rows: dict[str, int],
    test_metrics: dict[str, Any],
) -> dict[str, Any]:
    row = v4_postprocess.build_leaderboard_row_v4(
        run_id=run_id,
        options=options,
        task="cls",
        rows=rows,
        test_metrics=test_metrics,
    )
    row["trainer"] = "v5_panel_ensemble"
    row["champion_backend"] = "v5_panel_ensemble"
    return row


def _build_v5_train_config(
    *,
    options: TrainV5PanelEnsembleOptions,
    feature_cols: tuple[str, ...],
    markets: tuple[str, ...],
    label_contract: dict[str, Any],
    selection_recommendations: dict[str, Any],
    selection_policy: dict[str, Any],
    selection_calibration: dict[str, Any],
    research_support_lane: dict[str, Any],
    cpcv_lite_summary: dict[str, Any],
    factor_block_selection: dict[str, Any],
    factor_block_selection_context: dict[str, Any],
    cpcv_lite_runtime: dict[str, Any],
    search_budget_decision: dict[str, Any],
    lane_governance: dict[str, Any],
    ensemble_contract: dict[str, Any],
) -> dict[str, Any]:
    payload = train_config_snapshot_v4(
        asdict_fn=v4.asdict,
        options=options,
        task="cls",
        feature_cols=feature_cols,
        markets=markets,
        label_contract=label_contract,
        selection_recommendations=selection_recommendations,
        selection_policy=selection_policy,
        selection_calibration=selection_calibration,
        research_support_lane=research_support_lane,
        ranker_budget_profile={"profile": "v5_panel_ensemble"},
        cpcv_lite_summary=cpcv_lite_summary,
        factor_block_selection=factor_block_selection,
        factor_block_selection_context=factor_block_selection_context,
        cpcv_lite_runtime=cpcv_lite_runtime,
        search_budget_decision=search_budget_decision,
        lane_governance=lane_governance,
    )
    payload["trainer"] = "v5_panel_ensemble"
    payload["task"] = "cls"
    payload["panel_ensemble"] = dict(ensemble_contract or {})
    payload["predictor_contract"] = {
        "version": 1,
        "score_mean_field": "score_mean",
        "score_std_field": "score_std",
        "score_lcb_field": "score_lcb",
        "final_rank_score_field": "final_rank_score",
        "final_uncertainty_field": "score_std",
        "score_aliases": {"final_rank_score": "score_mean", "final_uncertainty": "score_std"},
        "score_lcb_formula": "score_lcb = clip(score_mean - score_std, 0, 1)",
        "distributional_contract": dict((ensemble_contract or {}).get("distributional_contract") or {}),
    }
    return payload


def _score_source_objective(selection_recommendations: dict[str, Any]) -> float:
    if not isinstance(selection_recommendations, dict):
        return float("-inf")
    threshold_key = str(selection_recommendations.get("recommended_threshold_key", "")).strip()
    by_key = selection_recommendations.get("by_threshold_key") if isinstance(selection_recommendations.get("by_threshold_key"), dict) else {}
    if threshold_key and isinstance(by_key.get(threshold_key), dict):
        value = by_key[threshold_key].get("objective_score")
        try:
            return float(value)
        except Exception:
            return float("-inf")
    return float("-inf")


def _apply_score_source_to_windows(windows: list[dict[str, Any]], *, score_source: str) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for window in windows:
        payload = dict(window or {})
        by_source = payload.get("selection_optimization_by_score_source") if isinstance(payload.get("selection_optimization_by_score_source"), dict) else {}
        selected_opt = by_source.get(score_source) or payload.get("selection_optimization")
        payload["selection_optimization"] = dict(selected_opt or {})
        selected.append(payload)
    return selected


def train_and_register_v5_panel_ensemble(options: TrainV5PanelEnsembleOptions) -> TrainV5PanelEnsembleResult:
    if options.feature_set != "v4":
        raise ValueError("trainer v5_panel_ensemble requires --feature-set v4")
    if options.label_set != "v3":
        raise ValueError("trainer v5_panel_ensemble requires --label-set v3")

    started_at = time.time()
    run_id = v4.make_run_id(seed=options.seed)
    economic_objective_profile = v4.build_v4_shared_economic_objective_profile()
    lane_governance = v4._build_lane_governance_v4(
        task="cls",
        run_scope=options.run_scope,
        economic_objective_profile=economic_objective_profile,
    )
    prepared = prepare_v4_training_inputs(
        options=options,
        task="cls",
        build_dataset_request_fn=v4.build_dataset_request,
        load_feature_spec_fn=load_feature_spec,
        load_label_spec_fn=load_label_spec,
        feature_columns_from_spec_fn=feature_columns_from_spec,
        resolve_selected_feature_columns_from_latest_fn=resolve_selected_feature_columns_from_latest,
        resolve_v4_search_budget_fn=resolve_v4_search_budget,
        load_feature_dataset_fn=v4.load_feature_dataset,
        load_feature_aux_frame_fn=load_feature_aux_frame,
        expected_interval_ms_fn=v4.expected_interval_ms,
        compute_time_splits_fn=compute_time_splits,
        split_masks_fn=split_masks,
        validate_split_counts_fn=v4._validate_split_counts,
        resolve_ranker_budget_profile_fn=v4._resolve_ranker_budget_profile,
        factor_block_registry_fn=v4_factor_block_registry,
        resolve_cpcv_lite_runtime_config_fn=v4._resolve_cpcv_lite_runtime_config,
    )

    dataset = prepared["dataset"]
    label_spec = prepared["label_spec"]
    label_contract = prepared["label_contract"]
    request = prepared["request"]
    train_mask = prepared["train_mask"]
    valid_mask = prepared["valid_mask"]
    test_mask = prepared["test_mask"]
    rows = prepared["rows"]
    split_info = prepared["split_info"]
    interval_ms = int(prepared["interval_ms"])
    search_budget_decision = prepared["search_budget_decision"]
    factor_block_selection_context = prepared["factor_block_selection_context"]
    factor_block_registry = prepared["factor_block_registry"]
    cpcv_lite_runtime = prepared["cpcv_lite_runtime"]
    action_aux_arrays = prepared["action_aux_arrays"]
    effective_booster_sweep_trials = int(prepared["effective_booster_sweep_trials"])
    live_domain_reweighting = dict(prepared.get("live_domain_reweighting") or {})

    regression_targets = _load_v5_regression_targets(
        request=request,
        label_spec=label_spec,
        dataset=dataset,
        primary_y_cls_column=str(label_contract["y_cls_column"]),
        primary_y_reg_column=str(label_contract["y_reg_column"]),
        primary_y_rank_column=str(label_contract["y_rank_column"]),
    )
    cls_bundle = v4._fit_booster_sweep_weighted(
        x_train=dataset.X[train_mask],
        y_train=dataset.y_cls[train_mask],
        w_train=dataset.sample_weight[train_mask],
        x_valid=dataset.X[valid_mask],
        y_valid=dataset.y_cls[valid_mask],
        w_valid=dataset.sample_weight[valid_mask],
        y_reg_valid=dataset.y_reg[valid_mask],
        fee_bps_est=options.fee_bps_est,
        safety_bps=options.safety_bps,
        seed=options.seed,
        nthread=options.nthread,
        trials=effective_booster_sweep_trials,
        eval_sample_weight=dataset.sample_weight[valid_mask],
    )
    rank_bundle = v4._fit_booster_sweep_ranker(
        x_train=dataset.X[train_mask],
        y_train_rank=dataset.y_rank[train_mask],
        ts_train_ms=dataset.ts_ms[train_mask],
        w_train=dataset.sample_weight[train_mask],
        x_valid=dataset.X[valid_mask],
        y_valid_cls=dataset.y_cls[valid_mask],
        y_valid_reg=dataset.y_reg[valid_mask],
        y_valid_rank=dataset.y_rank[valid_mask],
        ts_valid_ms=dataset.ts_ms[valid_mask],
        w_valid=dataset.sample_weight[valid_mask],
        fee_bps_est=options.fee_bps_est,
        safety_bps=options.safety_bps,
        seed=options.seed,
        nthread=options.nthread,
        trials=int(v4._resolve_ranker_budget_profile(options=options, task="rank", effective_booster_sweep_trials=effective_booster_sweep_trials)["main_trials"]),
        eval_sample_weight=dataset.sample_weight[valid_mask],
    )
    regressor_results = _fit_v5_regression_heads(
        options=options,
        dataset=dataset,
        regression_targets=regression_targets,
        train_mask=train_mask,
        valid_mask=valid_mask,
        sweep_trials=effective_booster_sweep_trials,
    )

    primary_horizon_key = f"h{int(label_contract.get('primary_horizon_bars', 12))}"
    primary_y_reg = np.asarray(regression_targets.get(primary_horizon_key, dataset.y_reg), dtype=np.float64)
    oof = _build_v5_oof_windows(
        options=options,
        dataset=dataset,
        primary_y_reg=primary_y_reg,
        regression_targets=regression_targets,
        classifier_best_params=dict(cls_bundle.get("best_params", {})),
        ranker_best_params=dict(rank_bundle.get("best_params", {})),
        regressor_best_params={key: dict(value.get("best_params", {})) for key, value in regressor_results.items()},
        action_aux_arrays=action_aux_arrays,
    )
    selection_calibration_rows = list(oof.get("_selection_calibration_rows", []))
    selection_calibration = build_selection_calibration_by_score_source(
        by_score_source={
            "score_mean": build_selection_calibration_from_oos_rows(oos_rows=selection_calibration_rows),
            "score_lcb": build_selection_calibration_from_oos_rows(
                oos_rows=[
                    {
                        "window_index": row.get("window_index"),
                        "scores": row.get("score_lcb") or [],
                        "y_cls": row.get("y_cls") or [],
                    }
                    for row in selection_calibration_rows
                ]
            ),
        },
        default_score_source="score_mean",
    )
    meta_x_all = np.concatenate([row["x"] for row in oof["meta_rows"]], axis=0) if oof["meta_rows"] else np.empty((0, len(_STACK_COMPONENT_ORDER)), dtype=np.float64)
    meta_y_all = np.concatenate([row["y"] for row in oof["meta_rows"]], axis=0) if oof["meta_rows"] else np.empty(0, dtype=np.int64)
    meta_fit = _fit_meta_logistic(meta_x_all, meta_y_all, sample_weight=oof.get("sample_weight"))

    estimator = V5PanelEnsembleEstimator(
        classifier_bundle=cls_bundle["bundle"],
        ranker_bundle=rank_bundle["bundle"],
        regressor_bundles={key: value["bundle"] for key, value in regressor_results.items()},
        regression_member_bundles={
            key: tuple(oof.get("regression_member_bundles", {}).get(key) or ())
            for key in regressor_results.keys()
        },
        meta_model=meta_fit["meta_model"],
        meta_ensemble=tuple(oof.get("meta_models", [])),
        regression_horizons=tuple(int(key.replace("h", "")) for key in sorted(regressor_results.keys(), key=lambda item: int(item.replace("h", "")))),
        uncertainty_temperature=float(meta_fit.get("uncertainty_temperature", 1.0)),
    )
    final_bundle = {"model_type": "v5_panel_ensemble", "scaler": None, "estimator": estimator}

    valid_scores = _predict_scores(final_bundle, dataset.X[valid_mask])
    test_scores = _predict_scores(final_bundle, dataset.X[test_mask])
    valid_metrics = v4._attach_ranking_metrics(
        metrics=_evaluate_split(
            y_cls=dataset.y_cls[valid_mask],
            y_reg=primary_y_reg[valid_mask],
            scores=valid_scores,
            markets=dataset.markets[valid_mask],
            fee_bps_est=options.fee_bps_est,
            safety_bps=options.safety_bps,
            sample_weight=dataset.sample_weight[valid_mask],
        ),
        y_rank=dataset.y_rank[valid_mask],
        ts_ms=dataset.ts_ms[valid_mask],
        scores=valid_scores,
    )
    test_metrics = v4._attach_ranking_metrics(
        metrics=_evaluate_split(
            y_cls=dataset.y_cls[test_mask],
            y_reg=primary_y_reg[test_mask],
            scores=test_scores,
            markets=dataset.markets[test_mask],
            fee_bps_est=options.fee_bps_est,
            safety_bps=options.safety_bps,
            sample_weight=dataset.sample_weight[test_mask],
        ),
        y_rank=dataset.y_rank[test_mask],
        ts_ms=dataset.ts_ms[test_mask],
        scores=test_scores,
    )
    thresholds = _build_thresholds(
        valid_scores=valid_scores,
        y_reg_valid=primary_y_reg[valid_mask],
        fee_bps_est=options.fee_bps_est,
        safety_bps=options.safety_bps,
        ev_scan_steps=options.ev_scan_steps,
        ev_min_selected=options.ev_min_selected,
        sample_weight=dataset.sample_weight[valid_mask],
    )

    walk_forward = _build_walk_forward_report_v5(
        windows=oof.get("windows", []),
        skipped=oof.get("skipped_windows", []),
        selection_calibration_rows=selection_calibration_rows,
        trade_action_rows=oof.get("_trade_action_oos_rows", []),
    )
    fallback_selection_recommendations = build_selection_recommendations(
        valid_scores=valid_scores,
        valid_ts_ms=dataset.ts_ms[valid_mask],
        thresholds=thresholds,
    )
    walk_forward_windows = list(walk_forward.get("windows", []))
    selection_recommendations_mean = build_selection_recommendations_from_walk_forward(
        windows=_apply_score_source_to_windows(walk_forward_windows, score_source="score_mean"),
        fallback_recommendations=fallback_selection_recommendations,
    )
    selection_recommendations_lcb = build_selection_recommendations_from_walk_forward(
        windows=_apply_score_source_to_windows(walk_forward_windows, score_source="score_lcb"),
        fallback_recommendations=fallback_recommendations_mean if (fallback_recommendations_mean := selection_recommendations_mean) else fallback_selection_recommendations,
    )
    chosen_score_source = (
        "score_lcb"
        if _score_source_objective(selection_recommendations_lcb) >= _score_source_objective(selection_recommendations_mean)
        else "score_mean"
    )
    selection_recommendations = selection_recommendations_lcb if chosen_score_source == "score_lcb" else selection_recommendations_mean
    walk_forward["windows"] = _apply_score_source_to_windows(walk_forward_windows, score_source=chosen_score_source)
    walk_forward["selection_policy_compare"] = {
        "version": 1,
        "policy": "score_source_compare_v1",
        "score_mean_objective_score": _score_source_objective(selection_recommendations_mean),
        "score_lcb_objective_score": _score_source_objective(selection_recommendations_lcb),
        "chosen_score_source": chosen_score_source,
    }
    selection_policy = build_selection_policy_from_recommendations(
        selection_recommendations=selection_recommendations,
        fallback_threshold_key="top_5pct",
        forced_threshold_key=getattr(options, "selection_threshold_key_override", None),
        score_source=chosen_score_source,
    )
    cpcv_lite = _build_disabled_cpcv_lite(trigger=str((cpcv_lite_runtime or {}).get("trigger", "disabled")).strip() or "disabled")
    research_support_lane = v4._build_research_support_lane_v4(walk_forward=walk_forward, cpcv_lite=cpcv_lite)
    factor_block_selection = build_factor_block_selection_report(
        block_registry=factor_block_registry,
        window_rows=[],
        selection_mode=options.factor_block_selection_mode,
        feature_columns=dataset.feature_names,
        run_id=run_id,
        refit_support=walk_forward.get("factor_block_refit_support"),
    )
    metrics = _build_v5_metrics_doc(
        run_id=run_id,
        options=options,
        split_info=split_info,
        interval_ms=interval_ms,
        rows=rows,
        valid_metrics=valid_metrics,
        test_metrics=test_metrics,
        walk_forward=walk_forward,
        cpcv_lite=cpcv_lite,
        factor_block_selection=factor_block_selection,
        search_budget_decision=search_budget_decision,
        economic_objective_profile=economic_objective_profile,
        lane_governance=lane_governance,
        cls_bundle=cls_bundle,
        rank_bundle=rank_bundle,
        regressor_results=regressor_results,
        meta_fit=meta_fit,
        meta_ensemble_count=len(oof.get("meta_models", [])),
    )
    leaderboard_row = _build_v5_leaderboard_row(run_id=run_id, options=options, rows=rows, test_metrics=test_metrics)
    train_config = _build_v5_train_config(
        options=options,
        feature_cols=dataset.feature_names,
        markets=dataset.selected_markets,
        label_contract=label_contract,
        selection_recommendations=selection_recommendations,
        selection_policy=selection_policy,
        selection_calibration=selection_calibration,
        research_support_lane=research_support_lane,
        cpcv_lite_summary=cpcv_lite.get("summary", {}),
        factor_block_selection=factor_block_selection,
        factor_block_selection_context=factor_block_selection_context,
        cpcv_lite_runtime=cpcv_lite_runtime,
        search_budget_decision=search_budget_decision,
        lane_governance=lane_governance,
        ensemble_contract=metrics.get("panel_ensemble", {}),
    )
    data_fingerprint = build_data_fingerprint(request=request, selected_markets=dataset.selected_markets, total_rows=dataset.rows)
    data_fingerprint["code_version"] = autobot_version
    if live_domain_reweighting:
        data_fingerprint["live_domain_reweighting"] = live_domain_reweighting
    model_card = render_model_card(
        run_id=run_id,
        model_family=options.model_family,
        champion="panel_ensemble",
        metrics=metrics,
        thresholds=thresholds,
        data_fingerprint=data_fingerprint,
    )

    run_dir = save_run(
        RegistrySavePayload(
            registry_root=options.registry_root,
            model_family=options.model_family,
            run_id=run_id,
            model_bundle=final_bundle,
            metrics=metrics,
            thresholds=thresholds,
            feature_spec=prepared["feature_spec"],
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
    update_artifact_status(run_dir, status="core_saved", core_saved=True)
    _write_json(run_dir / "panel_ensemble_contract.json", metrics.get("panel_ensemble", {}))
    _write_json(
        run_dir / "predictor_contract.json",
        {
            "version": 1,
            "score_mean_field": "score_mean",
            "score_std_field": "score_std",
            "score_lcb_field": "score_lcb",
            "final_rank_score_field": "final_rank_score",
            "final_uncertainty_field": "score_std",
            "score_aliases": {"final_rank_score": "score_mean", "final_uncertainty": "score_std"},
            "score_lcb_formula": "score_lcb = clip(score_mean - score_std, 0, 1)",
            "distributional_contract": dict((metrics.get("panel_ensemble", {}) or {}).get("distributional_contract") or {}),
        },
    )
    support_artifacts = v4_persistence.persist_v4_support_artifacts(
        run_dir=run_dir,
        options=options,
        run_id=run_id,
        factor_block_registry=factor_block_registry,
        walk_forward=walk_forward,
        cpcv_lite=cpcv_lite,
        factor_block_selection=factor_block_selection,
        search_budget_decision=search_budget_decision,
    )
    update_artifact_status(run_dir, status="support_artifacts_written", support_artifacts_written=True)
    duplicate_artifacts = v4._detect_duplicate_candidate_artifacts(
        options=options,
        run_id=run_id,
        run_dir=run_dir,
    )
    trade_action_oos_rows = walk_forward.pop("_trade_action_oos_rows", [])
    duplicate_candidate = bool(duplicate_artifacts.get("duplicate", False))
    if duplicate_candidate:
        execution_acceptance = v4._build_duplicate_candidate_execution_acceptance(
            run_id=run_id,
            duplicate_artifacts=duplicate_artifacts,
        )
    else:
        execution_acceptance = v4._run_execution_acceptance_v4(
            options=options,
            run_id=run_id,
        )
    if duplicate_candidate:
        runtime_recommendations = v4._build_duplicate_candidate_runtime_recommendations(
            run_id=run_id,
            duplicate_artifacts=duplicate_artifacts,
        )
    else:
        runtime_recommendations = v4._build_runtime_recommendations_v4(
            options=options,
            run_id=run_id,
            search_budget_decision=search_budget_decision,
        )
        runtime_recommendations["exit_path_risk"] = v4._build_exit_path_risk_summary_v4(
            runtime_recommendations=runtime_recommendations,
            selection_calibration=selection_calibration,
            oos_rows=trade_action_oos_rows,
        )
        if isinstance(runtime_recommendations.get("exit"), dict):
            runtime_recommendations["exit"]["path_risk"] = dict(runtime_recommendations["exit_path_risk"])
        runtime_recommendations["trade_action"] = v4._build_trade_action_policy_v4(
            options=options,
            runtime_recommendations=runtime_recommendations,
            selection_calibration=selection_calibration,
            oos_rows=trade_action_oos_rows,
        )
        runtime_recommendations["risk_control"] = v4._build_execution_risk_control_v4(
            options=options,
            runtime_recommendations=runtime_recommendations,
            selection_calibration=selection_calibration,
            oos_rows=trade_action_oos_rows,
        )
    execution_artifact_cleanup = v4._purge_execution_artifact_run_dirs(
        output_root=options.execution_acceptance_output_root,
        execution_acceptance=execution_acceptance,
        runtime_recommendations=runtime_recommendations,
    )
    if execution_artifact_cleanup.get("evaluated"):
        execution_acceptance["artifacts_cleanup"] = execution_artifact_cleanup
        runtime_recommendations["artifacts_cleanup"] = execution_artifact_cleanup
    if duplicate_candidate:
        promotion = v4._build_duplicate_candidate_promotion_decision_v4(
            options=options,
            run_id=run_id,
            walk_forward=walk_forward,
            execution_acceptance=execution_acceptance,
            duplicate_artifacts=duplicate_artifacts,
            runtime_recommendations=runtime_recommendations,
        )
    else:
        promotion = v4._manual_promotion_decision_v4(
            options=options,
            run_id=run_id,
            walk_forward=walk_forward,
            execution_acceptance=execution_acceptance,
            runtime_recommendations=runtime_recommendations,
        )
    trainer_research_evidence = v4._build_trainer_research_evidence_from_promotion_v4(
        promotion=promotion,
        support_lane=research_support_lane,
    )
    decision_surface = build_decision_surface_v4(
        options=options,
        task="cls",
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
    decision_surface["panel_ensemble"] = dict(metrics.get("panel_ensemble", {}))
    runtime_artifacts = v4_persistence.persist_v4_runtime_and_governance_artifacts(
        run_dir=run_dir,
        execution_acceptance=execution_acceptance,
        runtime_recommendations=runtime_recommendations,
        promotion=promotion,
        trainer_research_evidence=trainer_research_evidence,
        economic_objective_profile=economic_objective_profile,
        lane_governance=lane_governance,
        decision_surface=decision_surface,
    )
    update_artifact_status(
        run_dir,
        status="trainer_artifacts_complete",
        execution_acceptance_complete=True,
        runtime_recommendations_complete=True,
        governance_artifacts_complete=True,
    )
    if v4.normalize_factor_block_run_scope(options.run_scope) == "scheduled_daily":
        update_latest_pointer(options.registry_root, options.model_family, run_id)
        update_latest_pointer(options.registry_root, "_global", run_id, family=options.model_family)
    status = str(promotion.get("status", "candidate")).strip() or "candidate"
    update_artifact_status(run_dir, status=status)
    experiment_ledger_record = v4.build_experiment_ledger_record(
        run_id=run_id,
        task="cls",
        status=status,
        duration_sec=round(time.time() - started_at, 3),
        run_dir=run_dir,
        search_budget_decision=search_budget_decision,
        walk_forward=walk_forward,
        cpcv_lite=cpcv_lite,
        factor_block_selection=factor_block_selection,
        factor_block_policy={},
        factor_block_selection_context=factor_block_selection_context,
        execution_acceptance=execution_acceptance,
        runtime_recommendations=runtime_recommendations,
        promotion=promotion,
        duplicate_candidate=duplicate_candidate,
        economic_objective_profile=economic_objective_profile,
        lane_governance=lane_governance,
        run_scope=options.run_scope,
    )
    experiment_ledger_path = v4.append_experiment_ledger_record(
        registry_root=options.registry_root,
        model_family=options.model_family,
        record=experiment_ledger_record,
        run_scope=options.run_scope,
    )
    experiment_ledger_history = v4.load_experiment_ledger(
        registry_root=options.registry_root,
        model_family=options.model_family,
        run_scope=options.run_scope,
    )
    experiment_ledger_summary = v4.build_recent_experiment_ledger_summary(
        history_records=experiment_ledger_history,
    )
    experiment_ledger_summary_path = v4.write_latest_experiment_ledger_summary(
        registry_root=options.registry_root,
        model_family=options.model_family,
        run_id=run_id,
        summary=experiment_ledger_summary,
        run_scope=options.run_scope,
    )
    duration_sec = round(time.time() - started_at, 3)
    train_report_path = _write_json(
        options.logs_root / "train_v5_panel_ensemble_report.json",
        {
            "run_id": run_id,
            "trainer": "v5_panel_ensemble",
            "status": status,
            "duration_sec": duration_sec,
            "walk_forward_summary": walk_forward.get("summary", {}),
            "panel_ensemble": metrics.get("panel_ensemble", {}),
        },
    )
    return TrainV5PanelEnsembleResult(
        run_id=run_id,
        run_dir=run_dir,
        status=status,
        leaderboard_row=leaderboard_row,
        metrics=metrics,
        thresholds=thresholds,
        train_report_path=train_report_path,
        promotion_path=runtime_artifacts["promotion_path"],
        walk_forward_report_path=support_artifacts.get("walk_forward_report_path"),
        cpcv_lite_report_path=support_artifacts.get("cpcv_lite_report_path"),
        factor_block_selection_path=support_artifacts.get("factor_block_selection_path"),
        factor_block_history_path=support_artifacts.get("factor_block_history_path"),
        factor_block_policy_path=support_artifacts.get("factor_block_policy_path"),
        search_budget_decision_path=support_artifacts.get("search_budget_decision_path"),
        execution_acceptance_report_path=runtime_artifacts.get("execution_acceptance_report_path"),
        runtime_recommendations_path=runtime_artifacts.get("runtime_recommendations_path"),
        trainer_research_evidence_path=runtime_artifacts.get("trainer_research_evidence_path"),
        economic_objective_profile_path=runtime_artifacts.get("economic_objective_profile_path"),
        lane_governance_path=runtime_artifacts.get("lane_governance_path"),
        decision_surface_path=runtime_artifacts.get("decision_surface_path"),
        experiment_ledger_path=experiment_ledger_path,
        experiment_ledger_summary_path=experiment_ledger_summary_path,
        live_domain_reweighting_path=None,
    )
