from __future__ import annotations

import math
from typing import Any

import numpy as np

from .selection_calibration import apply_selection_calibration


TRADE_ACTION_POLICY_ID = "trade_level_hold_risk_oos_bins_v1"
CONDITIONAL_ACTION_MODEL_ID = "conditional_action_linear_quantile_tail_v2"
DEFAULT_EDGE_BIN_COUNT = 6
DEFAULT_RISK_BIN_COUNT = 6
DEFAULT_MIN_BIN_SAMPLES = 20
DEFAULT_STATE_FEATURE_NAMES = ("selection_score", "rv_12", "rv_36", "atr_pct_14")
DEFAULT_TAIL_CONFIDENCE_LEVEL = 0.90
DEFAULT_CTM_ORDER = 2


def build_trade_action_policy_from_oos_rows(
    *,
    oos_rows: list[dict[str, Any]] | None,
    selection_calibration: dict[str, Any] | None,
    hold_policy_template: dict[str, Any] | None,
    risk_policy_template: dict[str, Any] | None,
    size_multiplier_min: float,
    size_multiplier_max: float,
    edge_bin_count: int = DEFAULT_EDGE_BIN_COUNT,
    risk_bin_count: int = DEFAULT_RISK_BIN_COUNT,
    min_bin_samples: int = DEFAULT_MIN_BIN_SAMPLES,
    lpm_order: int = 2,
    tail_confidence_level: float = DEFAULT_TAIL_CONFIDENCE_LEVEL,
    ctm_order: int = DEFAULT_CTM_ORDER,
    numerical_floor: float = 1e-6,
) -> dict[str, Any]:
    hold_template = dict(hold_policy_template or {})
    risk_template = dict(risk_policy_template or {})
    risk_feature_name = str(risk_template.get("risk_vol_feature", "")).strip()
    if not hold_template or not risk_template or not risk_feature_name:
        return {
            "version": 1,
            "policy": TRADE_ACTION_POLICY_ID,
            "status": "skipped",
            "reason": "MISSING_POLICY_TEMPLATES",
        }

    trade_rows: list[dict[str, Any]] = []
    trade_rows, window_count = build_trade_action_oos_trade_rows(
        oos_rows=list(oos_rows or []),
        selection_calibration=selection_calibration,
        hold_policy_template=hold_template,
        risk_policy_template=risk_template,
        lpm_order=max(int(lpm_order), 1),
        numerical_floor=float(numerical_floor),
    )

    if len(trade_rows) < max(int(min_bin_samples), 2):
        return {
            "version": 1,
            "policy": TRADE_ACTION_POLICY_ID,
            "status": "skipped",
            "reason": "INSUFFICIENT_OOS_ROWS",
            "rows_total": int(len(trade_rows)),
        }

    conditional_action_model = _build_conditional_action_model(
        trade_rows=trade_rows,
        tail_confidence_level=float(tail_confidence_level),
        ctm_order=max(int(ctm_order), 1),
        numerical_floor=float(numerical_floor),
    )
    tail_risk_contract = dict(conditional_action_model.get("tail_risk_contract") or {})
    training_predictions = [
        _predict_contextual_action_metrics(
            model_payload=conditional_action_model,
            selection_score=float(row["selection_score"]),
            row=row,
            lpm_order=max(int(lpm_order), 1),
            numerical_floor=float(numerical_floor),
        )
        if conditional_action_model.get("status") == "ready"
        else None
        for row in trade_rows
    ]
    selection_scores = np.asarray([float(row["selection_score"]) for row in trade_rows], dtype=np.float64)
    risk_values = np.asarray(
        [
            float(
                _resolve_trade_row_risk_state_value(
                    trade_row=row,
                    predicted=predicted,
                    conditional_action_model=conditional_action_model,
                    lpm_order=max(int(lpm_order), 1),
                    numerical_floor=float(numerical_floor),
                )
            )
            for row, predicted in zip(trade_rows, training_predictions, strict=False)
        ],
        dtype=np.float64,
    )
    edge_bounds = _quantile_bounds(selection_scores, bin_count=max(int(edge_bin_count), 1))
    risk_bounds = _quantile_bounds(risk_values, bin_count=max(int(risk_bin_count), 1))
    grouped: dict[tuple[int, int], list[tuple[dict[str, Any], dict[str, float] | None]]] = {}
    for row, predicted, risk_value in zip(trade_rows, training_predictions, risk_values.tolist(), strict=False):
        edge_bin = _resolve_bin_index(float(row["selection_score"]), edge_bounds)
        risk_bin = _resolve_bin_index(float(risk_value), risk_bounds)
        grouped.setdefault((edge_bin, risk_bin), []).append((row, predicted))

    by_bin: list[dict[str, Any]] = []
    comparable_indices: list[int] = []
    for (edge_bin, risk_bin), grouped_rows in sorted(grouped.items()):
        sample_count = len(grouped_rows)
        comparable = sample_count >= max(int(min_bin_samples), 1)
        rows = [row for row, _ in grouped_rows]
        predictions = [predicted for _, predicted in grouped_rows]
        hold_returns = np.asarray(
            [
                float(predicted["hold_mean_return"]) if isinstance(predicted, dict) else float(row["hold_return"])
                for row, predicted in grouped_rows
            ],
            dtype=np.float64,
        )
        risk_returns = np.asarray(
            [
                float(predicted["risk_mean_return"]) if isinstance(predicted, dict) else float(row["risk_return"])
                for row, predicted in grouped_rows
            ],
            dtype=np.float64,
        )
        hold_downside_lpm = np.asarray(
            [
                float(predicted["hold_mean_lpm"]) if isinstance(predicted, dict) else float(row["hold_downside_lpm"])
                for row, predicted in grouped_rows
            ],
            dtype=np.float64,
        )
        risk_downside_lpm = np.asarray(
            [
                float(predicted["risk_mean_lpm"]) if isinstance(predicted, dict) else float(row["risk_downside_lpm"])
                for row, predicted in grouped_rows
            ],
            dtype=np.float64,
        )
        hold_objective = np.asarray(
            [
                float(predicted["hold_mean_objective"]) if isinstance(predicted, dict) else float(row["hold_objective_score"])
                for row, predicted in grouped_rows
            ],
            dtype=np.float64,
        )
        risk_objective = np.asarray(
            [
                float(predicted["risk_mean_objective"]) if isinstance(predicted, dict) else float(row["risk_objective_score"])
                for row, predicted in grouped_rows
            ],
            dtype=np.float64,
        )
        hold_mean_return = float(np.mean(hold_returns))
        risk_mean_return = float(np.mean(risk_returns))
        hold_mean_lpm = float(np.mean(hold_downside_lpm))
        risk_mean_lpm = float(np.mean(risk_downside_lpm))
        hold_mean_dev = float(hold_mean_lpm ** (1.0 / float(max(int(lpm_order), 1)))) if hold_mean_lpm > 0.0 else 0.0
        risk_mean_dev = float(risk_mean_lpm ** (1.0 / float(max(int(lpm_order), 1)))) if risk_mean_lpm > 0.0 else 0.0
        hold_mean_objective = float(np.mean(hold_objective))
        risk_mean_objective = float(np.mean(risk_objective))
        hold_conditional_var = float(
            np.mean(
                [
                    float(predicted["hold_expected_var"])
                    if isinstance(predicted, dict)
                    else float((tail_risk_contract.get("actions") or {}).get("hold", {}).get("sample_expected_var", 0.0) or 0.0)
                    for predicted in predictions
                ]
            )
        )
        risk_conditional_var = float(
            np.mean(
                [
                    float(predicted["risk_expected_var"])
                    if isinstance(predicted, dict)
                    else float((tail_risk_contract.get("actions") or {}).get("risk", {}).get("sample_expected_var", 0.0) or 0.0)
                    for predicted in predictions
                ]
            )
        )
        hold_tail_metrics = _summarize_predicted_tail_moments(
            predictions=predictions,
            action="hold",
            action_contract=dict((tail_risk_contract.get("actions") or {}).get("hold") or {}),
        )
        risk_tail_metrics = _summarize_predicted_tail_moments(
            predictions=predictions,
            action="risk",
            action_contract=dict((tail_risk_contract.get("actions") or {}).get("risk") or {}),
        )
        recommended_action = _select_recommended_action(
            hold_mean_return=hold_mean_return,
            risk_mean_return=risk_mean_return,
            hold_mean_lpm=hold_mean_lpm,
            risk_mean_lpm=risk_mean_lpm,
            hold_mean_objective=hold_mean_objective,
            risk_mean_objective=risk_mean_objective,
        )
        selected_edge = risk_mean_return if recommended_action == "risk" else hold_mean_return
        selected_dev = risk_mean_dev if recommended_action == "risk" else hold_mean_dev
        selected_objective = risk_mean_objective if recommended_action == "risk" else hold_mean_objective
        selected_es = (
            float(risk_tail_metrics["expected_es"])
            if recommended_action == "risk"
            else float(hold_tail_metrics["expected_es"])
        )
        selected_ctm = (
            float(risk_tail_metrics["expected_ctm"])
            if recommended_action == "risk"
            else float(hold_tail_metrics["expected_ctm"])
        )
        selected_tail_probability = (
            float(risk_tail_metrics["tail_probability"])
            if recommended_action == "risk"
            else float(hold_tail_metrics["tail_probability"])
        )
        row_doc = {
            "edge_bin": int(edge_bin),
            "risk_bin": int(risk_bin),
            "sample_count": int(sample_count),
            "comparable": bool(comparable),
            "recommended_action": recommended_action,
            "recommended_notional_multiplier": 1.0,
            "expected_edge": float(selected_edge),
            "expected_downside_deviation": float(selected_dev),
            "expected_objective_score": float(selected_objective),
            "expected_action_value": float(selected_objective),
            "expected_es": float(selected_es),
            "expected_ctm": float(selected_ctm),
            "expected_ctm2": float(selected_ctm),
            "expected_ctm_order": max(int(ctm_order), 1),
            "expected_tail_probability": float(selected_tail_probability),
            "hold": {
                "mean_return": hold_mean_return,
                "mean_conditional_var": float(hold_conditional_var),
                "mean_downside_lpm": hold_mean_lpm,
                "mean_downside_deviation": hold_mean_dev,
                "mean_objective_score": hold_mean_objective,
                "mean_expected_es": float(hold_tail_metrics["expected_es"]),
                "mean_expected_ctm": float(hold_tail_metrics["expected_ctm"]),
                "tail_probability": float(hold_tail_metrics["tail_probability"]),
                "tail_threshold_loss": float(hold_tail_metrics["tail_threshold_loss"]),
            },
            "risk": {
                "mean_return": risk_mean_return,
                "mean_conditional_var": float(risk_conditional_var),
                "mean_downside_lpm": risk_mean_lpm,
                "mean_downside_deviation": risk_mean_dev,
                "mean_objective_score": risk_mean_objective,
                "mean_expected_es": float(risk_tail_metrics["expected_es"]),
                "mean_expected_ctm": float(risk_tail_metrics["expected_ctm"]),
                "tail_probability": float(risk_tail_metrics["tail_probability"]),
                "tail_threshold_loss": float(risk_tail_metrics["tail_threshold_loss"]),
            },
        }
        if comparable:
            comparable_indices.append(len(by_bin))
        by_bin.append(row_doc)

    notional_model = _build_notional_multiplier_model(
        by_bin=by_bin,
        comparable_indices=comparable_indices,
        size_multiplier_min=float(size_multiplier_min),
        size_multiplier_max=float(size_multiplier_max),
        numerical_floor=float(numerical_floor),
    )
    _apply_notional_multipliers(
        by_bin=by_bin,
        comparable_indices=comparable_indices,
        notional_model=notional_model,
        numerical_floor=float(numerical_floor),
    )
    risk_bin_count_effective = max((int(item["risk_bin"]) for item in by_bin), default=-1) + 1
    edge_bin_count_effective = max((int(item["edge_bin"]) for item in by_bin), default=-1) + 1
    return {
        "version": 1,
        "policy": TRADE_ACTION_POLICY_ID,
        "status": "ready",
        "source": "walk_forward_oos_trade_replay",
        "selection_score_source": "selection_calibrated_score",
        "rows_total": int(len(trade_rows)),
        "windows_covered": int(window_count),
        "risk_feature_name": risk_feature_name,
        "risk_state_source": CONDITIONAL_ACTION_MODEL_ID if conditional_action_model.get("status") == "ready" else "raw_risk_feature",
        "state_feature_names": list(DEFAULT_STATE_FEATURE_NAMES),
        "runtime_decision_source": (
            "continuous_conditional_action_value"
            if conditional_action_model.get("status") == "ready"
            else "bin_audit_fallback"
        ),
        "edge_bounds": [float(value) for value in edge_bounds],
        "risk_bounds": [float(value) for value in risk_bounds],
        "edge_bin_count": int(edge_bin_count_effective),
        "risk_bin_count": int(risk_bin_count_effective),
        "min_bin_samples": max(int(min_bin_samples), 1),
        "lpm_order": max(int(lpm_order), 1),
        "tail_confidence_level": float(tail_risk_contract.get("confidence_level", tail_confidence_level)),
        "ctm_order": int(tail_risk_contract.get("ctm_order", ctm_order)),
        "hold_policy_template": hold_template,
        "risk_policy_template": risk_template,
        "tail_risk_contract": tail_risk_contract,
        "notional_model": notional_model,
        "conditional_action_model": conditional_action_model,
        "by_bin": by_bin,
        "summary": {
            "comparable_bins": int(len(comparable_indices)),
            "risk_bins_recommended": int(sum(1 for item in by_bin if item["recommended_action"] == "risk")),
            "hold_bins_recommended": int(sum(1 for item in by_bin if item["recommended_action"] == "hold")),
            "tail_risk_method": str(tail_risk_contract.get("method", "")),
        },
    }


def build_trade_action_oos_trade_rows(
    *,
    oos_rows: list[dict[str, Any]] | None,
    selection_calibration: dict[str, Any] | None,
    hold_policy_template: dict[str, Any] | None,
    risk_policy_template: dict[str, Any] | None,
    lpm_order: int = 2,
    numerical_floor: float = 1e-6,
) -> tuple[list[dict[str, Any]], int]:
    hold_template = dict(hold_policy_template or {})
    risk_template = dict(risk_policy_template or {})
    if not hold_template or not risk_template:
        return [], 0
    trade_rows: list[dict[str, Any]] = []
    window_count = 0
    for window in oos_rows or ():
        if not isinstance(window, dict):
            continue
        window_rows = _simulate_window_rows(
            window=window,
            selection_calibration=selection_calibration,
            hold_policy_template=hold_template,
            risk_policy_template=risk_template,
            lpm_order=max(int(lpm_order), 1),
            numerical_floor=float(numerical_floor),
        )
        if not window_rows:
            continue
        trade_rows.extend(window_rows)
        window_count += 1
    return trade_rows, window_count


def normalize_trade_action_policy(policy: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(policy or {})
    if str(payload.get("policy", "")).strip() != TRADE_ACTION_POLICY_ID:
        return {
            "version": 1,
            "policy": TRADE_ACTION_POLICY_ID,
            "status": "missing",
            "by_bin": [],
        }
    payload["status"] = str(payload.get("status", "")).strip().lower() or "missing"
    payload["risk_feature_name"] = str(payload.get("risk_feature_name", "")).strip()
    payload["edge_bounds"] = [float(value) for value in (payload.get("edge_bounds") or [])]
    payload["risk_bounds"] = [float(value) for value in (payload.get("risk_bounds") or [])]
    payload["min_bin_samples"] = max(int(payload.get("min_bin_samples", DEFAULT_MIN_BIN_SAMPLES) or DEFAULT_MIN_BIN_SAMPLES), 1)
    payload["hold_policy_template"] = dict(payload.get("hold_policy_template") or {})
    payload["risk_policy_template"] = dict(payload.get("risk_policy_template") or {})
    payload["tail_risk_contract"] = dict(payload.get("tail_risk_contract") or {})
    payload["notional_model"] = dict(payload.get("notional_model") or {})
    payload["state_feature_names"] = [
        str(value).strip()
        for value in (payload.get("state_feature_names") or [])
        if str(value).strip()
    ]
    payload["runtime_decision_source"] = str(payload.get("runtime_decision_source", "")).strip()
    payload["tail_confidence_level"] = float(
        payload.get("tail_confidence_level", DEFAULT_TAIL_CONFIDENCE_LEVEL) or DEFAULT_TAIL_CONFIDENCE_LEVEL
    )
    payload["ctm_order"] = max(int(payload.get("ctm_order", DEFAULT_CTM_ORDER) or DEFAULT_CTM_ORDER), 1)
    payload["conditional_action_model"] = dict(payload.get("conditional_action_model") or {})
    payload["by_bin"] = [dict(item) for item in (payload.get("by_bin") or []) if isinstance(item, dict)]
    return payload


def _build_conditional_action_model(
    *,
    trade_rows: list[dict[str, Any]],
    tail_confidence_level: float,
    ctm_order: int,
    numerical_floor: float,
) -> dict[str, Any]:
    if not trade_rows:
        return {"status": "skipped", "reason": "NO_TRADE_ROWS"}
    features = [_extract_state_feature_vector(item) for item in trade_rows]
    if any(vector is None for vector in features):
        return {"status": "skipped", "reason": "MISSING_STATE_FEATURES"}
    x = np.asarray(features, dtype=np.float64)
    if x.ndim != 2 or x.shape[0] <= x.shape[1]:
        return {"status": "skipped", "reason": "INSUFFICIENT_MODEL_ROWS"}
    hold_losses = np.asarray([max(-float(item["hold_return"]), 0.0) for item in trade_rows], dtype=np.float64)
    risk_losses = np.asarray([max(-float(item["risk_return"]), 0.0) for item in trade_rows], dtype=np.float64)
    confidence = min(max(float(tail_confidence_level), 0.50), 0.995)
    target_tail_probability = max(1.0 - float(confidence), float(numerical_floor))
    hold_var_model = _fit_linear_quantile_model(x=x, y=hold_losses, quantile=confidence)
    risk_var_model = _fit_linear_quantile_model(x=x, y=risk_losses, quantile=confidence)
    if hold_var_model is None:
        return {"status": "skipped", "reason": "MODEL_FIT_FAILED:hold_var"}
    if risk_var_model is None:
        return {"status": "skipped", "reason": "MODEL_FIT_FAILED:risk_var"}
    hold_var_hat = _predict_linear_model_batch(hold_var_model, x=x)
    risk_var_hat = _predict_linear_model_batch(risk_var_model, x=x)
    if hold_var_hat is None:
        return {"status": "skipped", "reason": "MODEL_PREDICT_FAILED:hold_var"}
    if risk_var_hat is None:
        return {"status": "skipped", "reason": "MODEL_PREDICT_FAILED:risk_var"}
    hold_var_hat = np.maximum(hold_var_hat, 0.0)
    risk_var_hat = np.maximum(risk_var_hat, 0.0)
    tail_risk_contract = _build_tail_risk_contract(
        hold_losses=hold_losses,
        risk_losses=risk_losses,
        hold_var_hat=hold_var_hat,
        risk_var_hat=risk_var_hat,
        target_tail_probability=target_tail_probability,
        ctm_order=max(int(ctm_order), 1),
        numerical_floor=float(numerical_floor),
    )
    targets = {
        "hold_return": np.asarray([float(item["hold_return"]) for item in trade_rows], dtype=np.float64),
        "risk_return": np.asarray([float(item["risk_return"]) for item in trade_rows], dtype=np.float64),
        "hold_objective": np.asarray([float(item["hold_objective_score"]) for item in trade_rows], dtype=np.float64),
        "risk_objective": np.asarray([float(item["risk_objective_score"]) for item in trade_rows], dtype=np.float64),
        "hold_log_lpm": np.asarray(
            [math.log1p(max(float(item["hold_downside_lpm"]), 0.0)) for item in trade_rows],
            dtype=np.float64,
        ),
        "risk_log_lpm": np.asarray(
            [math.log1p(max(float(item["risk_downside_lpm"]), 0.0)) for item in trade_rows],
            dtype=np.float64,
        ),
    }
    targets["hold_var"] = hold_var_hat
    targets["risk_var"] = risk_var_hat
    targets.update(
        _build_tail_risk_targets(
            hold_losses=hold_losses,
            risk_losses=risk_losses,
            hold_var_hat=hold_var_hat,
            risk_var_hat=risk_var_hat,
            target_tail_probability=float(target_tail_probability),
            ctm_order=max(int(ctm_order), 1),
            numerical_floor=float(numerical_floor),
        )
    )
    fitted_targets: dict[str, Any] = {}
    fitted_targets["hold_var"] = hold_var_model
    fitted_targets["risk_var"] = risk_var_model
    for name, y in targets.items():
        if name in {"hold_var", "risk_var"}:
            continue
        fitted = _fit_linear_ols_model(x=x, y=y)
        if fitted is None:
            return {"status": "skipped", "reason": f"MODEL_FIT_FAILED:{name}"}
        fitted_targets[name] = fitted
    return {
        "status": "ready",
        "model": CONDITIONAL_ACTION_MODEL_ID,
        "feature_names": list(DEFAULT_STATE_FEATURE_NAMES),
        "numerical_floor": float(numerical_floor),
        "targets": fitted_targets,
        "tail_risk_contract": tail_risk_contract,
        "diagnostics": {
            "rows_total": int(x.shape[0]),
            "feature_count": int(x.shape[1]),
            "targets": {
                name: dict((fitted_targets.get(name) or {}).get("fit_metrics") or {})
                for name in sorted(fitted_targets)
            },
        },
        "rows_total": int(x.shape[0]),
    }


def _fit_linear_ols_model(*, x: np.ndarray, y: np.ndarray) -> dict[str, Any] | None:
    if x.ndim != 2 or y.ndim != 1 or x.shape[0] != y.shape[0] or x.shape[0] <= 1:
        return None
    x_mean = np.mean(x, axis=0)
    x_scale = np.std(x, axis=0)
    x_scale = np.where(x_scale > 1e-12, x_scale, 1.0)
    xz = (x - x_mean) / x_scale
    design = np.column_stack([np.ones(xz.shape[0], dtype=np.float64), xz])
    try:
        coef, *_ = np.linalg.lstsq(design, y, rcond=None)
    except np.linalg.LinAlgError:
        return None
    fitted = design @ coef
    residual = y - fitted
    ss_res = float(np.sum(residual ** 2))
    ss_tot = float(np.sum((y - np.mean(y)) ** 2))
    r2 = None if ss_tot <= 1e-12 else float(1.0 - (ss_res / ss_tot))
    return {
        "intercept": float(coef[0]),
        "coef": [float(value) for value in coef[1:].tolist()],
        "x_mean": [float(value) for value in x_mean.tolist()],
        "x_scale": [float(value) for value in x_scale.tolist()],
        "fit_metrics": {
            "sample_count": int(y.shape[0]),
            "rmse": float(np.sqrt(np.mean(residual ** 2))),
            "mae": float(np.mean(np.abs(residual))),
            "r2": r2,
        },
    }


def _fit_linear_quantile_model(
    *,
    x: np.ndarray,
    y: np.ndarray,
    quantile: float,
    max_iter: int = 2_000,
    learning_rate: float = 0.05,
    ridge_lambda: float = 1e-4,
) -> dict[str, Any] | None:
    if x.ndim != 2 or y.ndim != 1 or x.shape[0] != y.shape[0] or x.shape[0] <= x.shape[1]:
        return None
    q = min(max(float(quantile), 1e-3), 1.0 - 1e-3)
    x_mean = np.mean(x, axis=0)
    x_scale = np.std(x, axis=0)
    x_scale = np.where(x_scale > 1e-12, x_scale, 1.0)
    xz = (x - x_mean) / x_scale
    design = np.column_stack([np.ones(xz.shape[0], dtype=np.float64), xz])
    beta = np.zeros(design.shape[1], dtype=np.float64)
    beta[0] = float(np.quantile(y, q))
    m = np.zeros_like(beta)
    v = np.zeros_like(beta)
    beta1 = 0.9
    beta2 = 0.999
    eps = 1e-8
    for iteration in range(1, max(int(max_iter), 1) + 1):
        pred = design @ beta
        residual = y - pred
        grad = design.T @ (np.where(residual < 0.0, 1.0 - q, -q)) / max(float(y.shape[0]), 1.0)
        grad[1:] += float(ridge_lambda) * beta[1:]
        m = (beta1 * m) + ((1.0 - beta1) * grad)
        v = (beta2 * v) + ((1.0 - beta2) * (grad ** 2))
        m_hat = m / (1.0 - (beta1 ** iteration))
        v_hat = v / (1.0 - (beta2 ** iteration))
        beta -= float(learning_rate) * (m_hat / (np.sqrt(v_hat) + eps))
    fitted = design @ beta
    residual = y - fitted
    pinball = np.where(residual >= 0.0, q * residual, (q - 1.0) * residual)
    exceedance_rate = float(np.mean((y >= fitted).astype(np.float64)))
    return {
        "intercept": float(beta[0]),
        "coef": [float(value) for value in beta[1:].tolist()],
        "x_mean": [float(value) for value in x_mean.tolist()],
        "x_scale": [float(value) for value in x_scale.tolist()],
        "fit_metrics": {
            "sample_count": int(y.shape[0]),
            "pinball_loss": float(np.mean(pinball)),
            "mae": float(np.mean(np.abs(residual))),
            "quantile": float(q),
            "exceedance_rate": exceedance_rate,
        },
    }


def _predict_linear_model(model: dict[str, Any], *, vector: np.ndarray) -> float | None:
    coef = np.asarray(model.get("coef") or [], dtype=np.float64)
    x_mean = np.asarray(model.get("x_mean") or [], dtype=np.float64)
    x_scale = np.asarray(model.get("x_scale") or [], dtype=np.float64)
    if coef.size <= 0 or coef.size != vector.size or x_mean.size != vector.size or x_scale.size != vector.size:
        return None
    scaled = (vector - x_mean) / np.where(x_scale > 1e-12, x_scale, 1.0)
    return float(float(model.get("intercept", 0.0) or 0.0) + float(np.dot(coef, scaled)))


def _predict_linear_model_batch(model: dict[str, Any], *, x: np.ndarray) -> np.ndarray | None:
    coef = np.asarray(model.get("coef") or [], dtype=np.float64)
    x_mean = np.asarray(model.get("x_mean") or [], dtype=np.float64)
    x_scale = np.asarray(model.get("x_scale") or [], dtype=np.float64)
    if x.ndim != 2 or coef.size <= 0 or coef.size != x.shape[1] or x_mean.size != x.shape[1] or x_scale.size != x.shape[1]:
        return None
    scaled = (x - x_mean) / np.where(x_scale > 1e-12, x_scale, 1.0)
    return np.asarray(float(model.get("intercept", 0.0) or 0.0) + (scaled @ coef), dtype=np.float64)


def _extract_state_feature_vector(trade_row: dict[str, Any]) -> list[float] | None:
    values = []
    for feature_name in DEFAULT_STATE_FEATURE_NAMES:
        value = _safe_optional_float(trade_row.get(feature_name))
        if value is None or not math.isfinite(float(value)):
            return None
        values.append(float(value))
    return values


def _predict_contextual_action_metrics(
    *,
    model_payload: dict[str, Any],
    selection_score: float,
    row: dict[str, Any] | None,
    lpm_order: int,
    numerical_floor: float,
) -> dict[str, float] | None:
    if str(model_payload.get("status", "")).strip().lower() != "ready":
        return None
    vector = _build_runtime_state_vector(
        selection_score=selection_score,
        row=row,
        model_payload=model_payload,
    )
    if vector is None:
        return None
    targets = dict(model_payload.get("targets") or {})
    hold_return = _predict_linear_model(dict(targets.get("hold_return") or {}), vector=vector)
    risk_return = _predict_linear_model(dict(targets.get("risk_return") or {}), vector=vector)
    hold_objective = _predict_linear_model(dict(targets.get("hold_objective") or {}), vector=vector)
    risk_objective = _predict_linear_model(dict(targets.get("risk_objective") or {}), vector=vector)
    hold_var = _predict_linear_model(dict(targets.get("hold_var") or {}), vector=vector)
    risk_var = _predict_linear_model(dict(targets.get("risk_var") or {}), vector=vector)
    hold_es_direct = _predict_linear_model(dict(targets.get("hold_es_direct") or {}), vector=vector)
    risk_es_direct = _predict_linear_model(dict(targets.get("risk_es_direct") or {}), vector=vector)
    hold_ctm_direct = _predict_linear_model(dict(targets.get("hold_ctm_direct") or {}), vector=vector)
    risk_ctm_direct = _predict_linear_model(dict(targets.get("risk_ctm_direct") or {}), vector=vector)
    hold_log_lpm = _predict_linear_model(dict(targets.get("hold_log_lpm") or {}), vector=vector)
    risk_log_lpm = _predict_linear_model(dict(targets.get("risk_log_lpm") or {}), vector=vector)
    values = (
        hold_return,
        risk_return,
        hold_objective,
        risk_objective,
        hold_var,
        risk_var,
        hold_es_direct,
        risk_es_direct,
        hold_ctm_direct,
        risk_ctm_direct,
        hold_log_lpm,
        risk_log_lpm,
    )
    if any(value is None or not math.isfinite(float(value)) for value in values):
        return None
    tail_risk_contract = dict(model_payload.get("tail_risk_contract") or {})
    ctm_order = max(int(tail_risk_contract.get("ctm_order", DEFAULT_CTM_ORDER) or DEFAULT_CTM_ORDER), 1)
    tail_probability_target = min(
        max(float(tail_risk_contract.get("target_tail_probability", 1.0 - DEFAULT_TAIL_CONFIDENCE_LEVEL) or (1.0 - DEFAULT_TAIL_CONFIDENCE_LEVEL)), float(numerical_floor)),
        1.0,
    )
    hold_var_value = max(float(hold_var), 0.0)
    risk_var_value = max(float(risk_var), 0.0)
    hold_es = max(float(hold_es_direct), hold_var_value, 0.0)
    risk_es = max(float(risk_es_direct), risk_var_value, 0.0)
    hold_ctm = max(float(hold_ctm_direct), 0.0)
    risk_ctm = max(float(risk_ctm_direct), 0.0)
    hold_mean_lpm = max(math.expm1(float(hold_log_lpm)), 0.0)
    risk_mean_lpm = max(math.expm1(float(risk_log_lpm)), 0.0)
    hold_mean_dev = hold_mean_lpm ** (1.0 / float(max(int(lpm_order), 1))) if hold_mean_lpm > 0.0 else 0.0
    risk_mean_dev = risk_mean_lpm ** (1.0 / float(max(int(lpm_order), 1))) if risk_mean_lpm > 0.0 else 0.0
    action = _select_continuous_action_value(
        hold_action_value=float(hold_objective),
        risk_action_value=float(risk_objective),
        hold_expected_return=float(hold_return),
        risk_expected_return=float(risk_return),
        hold_expected_es=float(hold_es),
        risk_expected_es=float(risk_es),
    )
    risk_state_value = float(risk_es if action == "risk" else hold_es)
    return {
        "hold_mean_return": float(hold_return),
        "risk_mean_return": float(risk_return),
        "hold_mean_lpm": float(hold_mean_lpm),
        "risk_mean_lpm": float(risk_mean_lpm),
        "hold_mean_downside_deviation": float(hold_mean_dev),
        "risk_mean_downside_deviation": float(risk_mean_dev),
        "hold_mean_objective": float(hold_objective),
        "risk_mean_objective": float(risk_objective),
        "hold_expected_var": float(hold_var_value),
        "risk_expected_var": float(risk_var_value),
        "hold_tail_probability": float(tail_probability_target),
        "risk_tail_probability": float(tail_probability_target),
        "hold_expected_es": float(hold_es),
        "risk_expected_es": float(risk_es),
        "hold_expected_ctm": float(hold_ctm),
        "risk_expected_ctm": float(risk_ctm),
        "hold_expected_ctm2": float(hold_ctm),
        "risk_expected_ctm2": float(risk_ctm),
        "ctm_order": int(ctm_order),
        "hold_tail_threshold_loss": float(hold_var_value),
        "risk_tail_threshold_loss": float(risk_var_value),
        "recommended_action": action,
        "risk_state_value": max(float(risk_state_value), float(numerical_floor)),
        "sample_count": int(model_payload.get("rows_total", 0) or 0),
    }


def _select_continuous_action_value(
    *,
    hold_action_value: float,
    risk_action_value: float,
    hold_expected_return: float,
    risk_expected_return: float,
    hold_expected_es: float,
    risk_expected_es: float,
) -> str:
    if risk_action_value > hold_action_value:
        return "risk"
    if hold_action_value > risk_action_value:
        return "hold"
    return _select_recommended_action(
        hold_mean_return=float(hold_expected_return),
        risk_mean_return=float(risk_expected_return),
        hold_mean_lpm=float(hold_expected_es),
        risk_mean_lpm=float(risk_expected_es),
        hold_mean_objective=float(hold_action_value),
        risk_mean_objective=float(risk_action_value),
    )


def _build_runtime_state_vector(
    *,
    selection_score: float,
    row: dict[str, Any] | None,
    model_payload: dict[str, Any],
) -> np.ndarray | None:
    resolved_row = dict(row or {})
    resolved_values: list[float] = []
    for index, feature_name in enumerate(DEFAULT_STATE_FEATURE_NAMES):
        if feature_name == "selection_score":
            resolved_values.append(float(selection_score))
            continue
        value = _resolve_row_risk_feature_value(row=resolved_row, feature_name=feature_name)
        if value is None and feature_name == "rv_12":
            value = _resolve_row_risk_feature_value(row=resolved_row, feature_name="rv_36")
        if value is None and feature_name == "rv_36":
            value = _resolve_row_risk_feature_value(row=resolved_row, feature_name="rv_12")
        if value is None:
            return None
        resolved_values.append(float(value))
    return np.asarray(resolved_values, dtype=np.float64)


def _resolve_trade_row_risk_state_value(
    *,
    trade_row: dict[str, Any],
    predicted: dict[str, float] | None,
    conditional_action_model: dict[str, Any],
    lpm_order: int,
    numerical_floor: float,
) -> float:
    if predicted is not None:
        return max(float(predicted["risk_state_value"]), float(numerical_floor))
    predicted = _predict_contextual_action_metrics(
        model_payload=conditional_action_model,
        selection_score=float(trade_row["selection_score"]),
        row=trade_row,
        lpm_order=lpm_order,
        numerical_floor=numerical_floor,
    )
    if predicted is not None:
        return max(float(predicted["risk_state_value"]), float(numerical_floor))
    return max(float(trade_row.get("risk_value", 0.0) or 0.0), float(numerical_floor))


def resolve_trade_action(
    policy: dict[str, Any] | None,
    *,
    selection_score: float,
    row: dict[str, Any] | None,
) -> dict[str, Any] | None:
    normalized = normalize_trade_action_policy(policy)
    if normalized.get("status") != "ready":
        return None
    conditional_action_model = dict(normalized.get("conditional_action_model") or {})
    fallback_reason_code = ""
    if conditional_action_model.get("status") == "ready":
        predicted = _predict_contextual_action_metrics(
            model_payload=conditional_action_model,
            selection_score=float(selection_score),
            row=row,
            lpm_order=max(int(normalized.get("lpm_order", 2) or 2), 1),
            numerical_floor=1e-6,
        )
        if predicted is not None:
            predicted_action = str(predicted.get("recommended_action") or "").strip().lower()
            edge_bin = int(_resolve_bin_index(float(selection_score), [float(v) for v in (normalized.get("edge_bounds") or [])]))
            risk_bin = int(
                _resolve_bin_index(
                    float(predicted["risk_state_value"]),
                    [float(v) for v in (normalized.get("risk_bounds") or [])],
                )
            )
            action = predicted_action if predicted_action in {"hold", "risk"} else "hold"
            template = (
                dict(normalized.get("risk_policy_template") or {})
                if action == "risk"
                else dict(normalized.get("hold_policy_template") or {})
            )
            expected_edge = float(predicted["risk_mean_return"] if action == "risk" else predicted["hold_mean_return"])
            expected_downside_deviation = float(
                predicted["risk_mean_downside_deviation"] if action == "risk" else predicted["hold_mean_downside_deviation"]
            )
            expected_objective_score = float(
                predicted["risk_mean_objective"] if action == "risk" else predicted["hold_mean_objective"]
            )
            expected_es = float(predicted["risk_expected_es"] if action == "risk" else predicted["hold_expected_es"])
            expected_ctm = float(predicted["risk_expected_ctm"] if action == "risk" else predicted["hold_expected_ctm"])
            expected_tail_probability = float(
                predicted["risk_tail_probability"] if action == "risk" else predicted["hold_tail_probability"]
            )
            continuous_score = expected_edge / max(expected_es, 1e-6) if expected_edge > 0.0 else 0.0
            raw_risk_feature_value = _resolve_row_risk_feature_value(
                row=row,
                feature_name=str(normalized.get("risk_feature_name", "")).strip(),
            )
            return {
                "policy": TRADE_ACTION_POLICY_ID,
                "recommended_action": action,
                "recommended_notional_multiplier": _resolve_continuous_notional_multiplier(
                    policy=normalized,
                    score=float(continuous_score),
                    numerical_floor=1e-6,
                ),
                "expected_edge": expected_edge,
                "expected_downside_deviation": expected_downside_deviation,
                "expected_objective_score": expected_objective_score,
                "expected_action_value": expected_objective_score,
                "expected_es": expected_es,
                "expected_ctm": expected_ctm,
                "expected_ctm2": expected_ctm,
                "expected_ctm_order": int(predicted.get("ctm_order", DEFAULT_CTM_ORDER) or DEFAULT_CTM_ORDER),
                "expected_tail_probability": expected_tail_probability,
                "sample_count": int(predicted.get("sample_count", 0) or 0),
                "edge_bin": edge_bin,
                "risk_bin": risk_bin,
                "risk_feature_name": str(normalized.get("risk_feature_name", "")).strip(),
                "risk_feature_value": (
                    float(raw_risk_feature_value)
                    if raw_risk_feature_value is not None and math.isfinite(float(raw_risk_feature_value))
                    else float(predicted["risk_state_value"])
                ),
                "support_level": "full",
                "risk_state_source": CONDITIONAL_ACTION_MODEL_ID,
                "decision_source": "continuous_conditional_action_value",
                "chosen_action_source": "continuous_conditional_action_value",
                "exit_policy_template": template,
                "diagnostics": {
                    "hold_mean_return": float(predicted["hold_mean_return"]),
                    "risk_mean_return": float(predicted["risk_mean_return"]),
                    "hold_mean_objective": float(predicted["hold_mean_objective"]),
                    "risk_mean_objective": float(predicted["risk_mean_objective"]),
                    "hold_expected_var": float(predicted["hold_expected_var"]),
                    "risk_expected_var": float(predicted["risk_expected_var"]),
                    "hold_tail_probability": float(predicted["hold_tail_probability"]),
                    "risk_tail_probability": float(predicted["risk_tail_probability"]),
                    "hold_tail_threshold_loss": float(predicted["hold_tail_threshold_loss"]),
                    "risk_tail_threshold_loss": float(predicted["risk_tail_threshold_loss"]),
                    "hold_expected_es": float(predicted["hold_expected_es"]),
                    "risk_expected_es": float(predicted["risk_expected_es"]),
                    "hold_expected_ctm": float(predicted["hold_expected_ctm"]),
                    "risk_expected_ctm": float(predicted["risk_expected_ctm"]),
                    "hold_expected_ctm2": float(predicted["hold_expected_ctm2"]),
                    "risk_expected_ctm2": float(predicted["risk_expected_ctm2"]),
                },
            }
        fallback_reason_code = "TRADE_ACTION_INSUFFICIENT_STATE_SUPPORT"
    if str(normalized.get("runtime_decision_source", "")).strip().lower() == "continuous_conditional_action_value":
        if conditional_action_model.get("status") != "ready":
            return _insufficient_trade_action_decision(
                normalized,
                reason_code="TRADE_ACTION_CONDITIONAL_MODEL_MISSING",
            )
    risk_feature_name = str(normalized.get("risk_feature_name", "")).strip()
    if not risk_feature_name:
        if fallback_reason_code:
            return _insufficient_trade_action_decision(normalized, reason_code=fallback_reason_code)
        return None
    risk_value = _resolve_row_risk_feature_value(row=row, feature_name=risk_feature_name)
    if risk_value is None or not math.isfinite(float(risk_value)):
        if fallback_reason_code:
            return _insufficient_trade_action_decision(normalized, reason_code=fallback_reason_code)
        return None
    edge_bounds = [float(value) for value in (normalized.get("edge_bounds") or [])]
    risk_bounds = [float(value) for value in (normalized.get("risk_bounds") or [])]
    if len(edge_bounds) < 2 or len(risk_bounds) < 2:
        if fallback_reason_code:
            return _insufficient_trade_action_decision(normalized, reason_code=fallback_reason_code)
        return None
    edge_bin = _resolve_bin_index(float(selection_score), edge_bounds)
    risk_bin = _resolve_bin_index(float(risk_value), risk_bounds)
    min_bin_samples = max(int(normalized.get("min_bin_samples", DEFAULT_MIN_BIN_SAMPLES) or DEFAULT_MIN_BIN_SAMPLES), 1)
    for item in normalized.get("by_bin") or []:
        if int(item.get("edge_bin", -1)) != edge_bin or int(item.get("risk_bin", -1)) != risk_bin:
            continue
        if not bool(item.get("comparable", False)):
            if fallback_reason_code:
                return _insufficient_trade_action_decision(normalized, reason_code=fallback_reason_code)
            return None
        if int(item.get("sample_count", 0) or 0) < min_bin_samples:
            if fallback_reason_code:
                return _insufficient_trade_action_decision(normalized, reason_code=fallback_reason_code)
            return None
        action = str(item.get("recommended_action", "")).strip().lower()
        if action not in {"hold", "risk"}:
            if fallback_reason_code:
                return _insufficient_trade_action_decision(normalized, reason_code=fallback_reason_code)
            return None
        template = (
            dict(normalized.get("risk_policy_template") or {})
            if action == "risk"
            else dict(normalized.get("hold_policy_template") or {})
        )
        return {
            "policy": TRADE_ACTION_POLICY_ID,
            "recommended_action": action,
            "recommended_notional_multiplier": float(item.get("recommended_notional_multiplier", 1.0) or 1.0),
            "expected_edge": float(item.get("expected_edge", 0.0) or 0.0),
            "expected_downside_deviation": float(item.get("expected_downside_deviation", 0.0) or 0.0),
            "expected_objective_score": float(item.get("expected_objective_score", 0.0) or 0.0),
            "expected_action_value": float(
                item.get("expected_action_value", item.get("expected_objective_score", 0.0)) or 0.0
            ),
            "expected_es": float(item.get("expected_es", 0.0) or 0.0),
            "expected_ctm": float(item.get("expected_ctm", item.get("expected_ctm2", 0.0)) or 0.0),
            "expected_ctm2": float(item.get("expected_ctm2", item.get("expected_ctm", 0.0)) or 0.0),
            "expected_ctm_order": max(int(item.get("expected_ctm_order", DEFAULT_CTM_ORDER) or DEFAULT_CTM_ORDER), 1),
            "expected_tail_probability": float(item.get("expected_tail_probability", 0.0) or 0.0),
            "sample_count": int(item.get("sample_count", 0) or 0),
            "edge_bin": int(edge_bin),
            "risk_bin": int(risk_bin),
            "risk_feature_name": risk_feature_name,
            "risk_feature_value": float(risk_value),
            "support_level": "fallback_bin" if fallback_reason_code else "full",
            "status": "fallback_support" if fallback_reason_code else "legacy_fallback",
            "support_reason_code": fallback_reason_code,
            "decision_source": "bin_audit_fallback",
            "chosen_action_source": "bin_audit_fallback",
            "exit_policy_template": template,
        }
    if fallback_reason_code:
        return _insufficient_trade_action_decision(normalized, reason_code=fallback_reason_code)
    return None


def _simulate_window_rows(
    *,
    window: dict[str, Any],
    selection_calibration: dict[str, Any] | None,
    hold_policy_template: dict[str, Any],
    risk_policy_template: dict[str, Any],
    lpm_order: int,
    numerical_floor: float,
) -> list[dict[str, Any]]:
    raw_scores = np.asarray(window.get("raw_scores") or [], dtype=np.float64)
    markets = np.asarray(window.get("markets") or [], dtype=object)
    ts_ms = np.asarray(window.get("ts_ms") or [], dtype=np.int64)
    close = np.asarray(window.get("close") or [], dtype=np.float64)
    rv_12 = np.asarray(window.get("rv_12") or [], dtype=np.float64)
    rv_36 = np.asarray(window.get("rv_36") or [], dtype=np.float64)
    atr_14 = np.asarray(window.get("atr_14") or [], dtype=np.float64)
    atr_pct_14 = np.asarray(window.get("atr_pct_14") or [], dtype=np.float64)
    size = raw_scores.size
    if size <= 0 or any(arr.size != size for arr in (markets, ts_ms, close, rv_12, rv_36, atr_14, atr_pct_14)):
        return []
    selection_scores = apply_selection_calibration(raw_scores, selection_calibration)
    rows: list[dict[str, Any]] = []
    by_market: dict[str, list[int]] = {}
    for index, market in enumerate(markets.tolist()):
        market_value = str(market).strip().upper()
        if not market_value:
            continue
        by_market.setdefault(market_value, []).append(index)
    for market_indices in by_market.values():
        ordered = sorted(market_indices, key=lambda idx: int(ts_ms[idx]))
        market_close = close[ordered]
        market_rv_12 = rv_12[ordered]
        market_rv_36 = rv_36[ordered]
        market_atr_14 = atr_14[ordered]
        market_atr_pct_14 = atr_pct_14[ordered]
        market_scores = selection_scores[ordered]
        for offset, original_index in enumerate(ordered):
            row_payload = {
                "close": _safe_float(market_close[offset]),
                "rv_12": _safe_float(market_rv_12[offset]),
                "rv_36": _safe_float(market_rv_36[offset]),
                "atr_14": _safe_float(market_atr_14[offset]),
                "atr_pct_14": _safe_float(market_atr_pct_14[offset]),
            }
            risk_value = _resolve_row_risk_feature_value(
                row=row_payload,
                feature_name=str(risk_policy_template.get("risk_vol_feature", "")).strip(),
            )
            if risk_value is None or not math.isfinite(float(risk_value)):
                continue
            hold_result = _simulate_template_return(
                close_values=market_close,
                start_index=offset,
                template=hold_policy_template,
                row=row_payload,
                lpm_order=lpm_order,
                numerical_floor=numerical_floor,
            )
            risk_result = _simulate_template_return(
                close_values=market_close,
                start_index=offset,
                template=risk_policy_template,
                row=row_payload,
                lpm_order=lpm_order,
                numerical_floor=numerical_floor,
            )
            if not hold_result or not risk_result:
                continue
            rows.append(
                {
                    "window_index": int(window.get("window_index", -1)),
                    "selection_score": float(market_scores[offset]),
                    "risk_value": float(risk_value),
                    "rv_12": float(row_payload["rv_12"]),
                    "rv_36": float(row_payload["rv_36"]),
                    "atr_pct_14": float(row_payload["atr_pct_14"]),
                    "hold_return": float(hold_result["return"]),
                    "hold_downside_lpm": float(hold_result["downside_lpm"]),
                    "hold_objective_score": float(hold_result["objective_score"]),
                    "risk_return": float(risk_result["return"]),
                    "risk_downside_lpm": float(risk_result["downside_lpm"]),
                    "risk_objective_score": float(risk_result["objective_score"]),
                }
            )
    return rows


def _simulate_template_return(
    *,
    close_values: np.ndarray,
    start_index: int,
    template: dict[str, Any],
    row: dict[str, Any],
    lpm_order: int,
    numerical_floor: float,
) -> dict[str, float] | None:
    hold_bars = max(int(template.get("hold_bars", 0) or 0), 0)
    if hold_bars <= 0:
        return None
    end_index = int(start_index) + hold_bars
    if int(start_index) < 0 or end_index >= int(close_values.size):
        return None
    entry_price = _safe_float(close_values[start_index])
    if entry_price <= 0.0:
        return None
    mode = str(template.get("mode", "hold")).strip().lower() or "hold"
    tp_pct, sl_pct, trailing_pct = _resolve_template_thresholds(template=template, row=row)
    exit_fee_rate = max(float(template.get("expected_exit_fee_rate", 0.0) or 0.0), 0.0)
    exit_slippage_bps = max(float(template.get("expected_exit_slippage_bps", 0.0) or 0.0), 0.0)
    peak_price = entry_price
    for index in range(int(start_index) + 1, end_index + 1):
        ref_price = _safe_float(close_values[index])
        if ref_price <= 0.0:
            return None
        if mode == "risk":
            peak_price = max(peak_price, ref_price)
            net_return = _net_return_after_costs(
                entry_price=entry_price,
                exit_price=ref_price,
                exit_fee_rate=exit_fee_rate,
                exit_slippage_bps=exit_slippage_bps,
            )
            trailing_drawdown = _net_drawdown_from_peak_after_costs(
                peak_price=peak_price,
                current_price=ref_price,
                exit_fee_rate=exit_fee_rate,
                exit_slippage_bps=exit_slippage_bps,
            )
            if tp_pct > 0.0 and net_return >= tp_pct:
                return _result_doc(net_return, lpm_order=lpm_order, numerical_floor=numerical_floor)
            if sl_pct > 0.0 and net_return <= -sl_pct:
                return _result_doc(net_return, lpm_order=lpm_order, numerical_floor=numerical_floor)
            if trailing_pct > 0.0 and trailing_drawdown >= trailing_pct:
                return _result_doc(net_return, lpm_order=lpm_order, numerical_floor=numerical_floor)
            if index == end_index:
                return _result_doc(net_return, lpm_order=lpm_order, numerical_floor=numerical_floor)
            continue

        net_return = _net_return_after_costs(
            entry_price=entry_price,
            exit_price=ref_price,
            exit_fee_rate=exit_fee_rate,
            exit_slippage_bps=exit_slippage_bps,
        )
        if sl_pct > 0.0 and net_return <= -sl_pct:
            return _result_doc(net_return, lpm_order=lpm_order, numerical_floor=numerical_floor)
        if index == end_index:
            return _result_doc(net_return, lpm_order=lpm_order, numerical_floor=numerical_floor)
    return None


def _resolve_template_thresholds(
    *,
    template: dict[str, Any],
    row: dict[str, Any],
) -> tuple[float, float, float]:
    base_tp = max(float(template.get("tp_pct", 0.0) or 0.0), 0.0)
    base_sl = max(float(template.get("sl_pct", 0.0) or 0.0), 0.0)
    base_trailing = max(float(template.get("trailing_pct", 0.0) or 0.0), 0.0)
    if str(template.get("risk_scaling_mode", "fixed")).strip().lower() != "volatility_scaled":
        return base_tp, base_sl, base_trailing
    sigma_bar = _resolve_row_risk_feature_value(
        row=row,
        feature_name=str(template.get("risk_vol_feature", "")).strip(),
    )
    if sigma_bar is None or sigma_bar <= 0.0:
        return base_tp, base_sl, base_trailing
    horizon_bars = max(int(template.get("hold_bars", 0) or 0), 1)
    sigma_horizon = float(sigma_bar) * math.sqrt(float(horizon_bars))
    if not math.isfinite(sigma_horizon) or sigma_horizon <= 0.0:
        return base_tp, base_sl, base_trailing
    tp_mult = _safe_optional_float(template.get("tp_vol_multiplier"))
    sl_mult = _safe_optional_float(template.get("sl_vol_multiplier"))
    trailing_mult = _safe_optional_float(template.get("trailing_vol_multiplier"))
    tp_pct = max(float(tp_mult) * sigma_horizon, 0.0) if tp_mult is not None else base_tp
    sl_pct = max(float(sl_mult) * sigma_horizon, 0.0) if sl_mult is not None else base_sl
    trailing_pct = max(float(trailing_mult) * sigma_horizon, 0.0) if trailing_mult is not None else base_trailing
    return tp_pct, sl_pct, trailing_pct


def _resolve_row_risk_feature_value(*, row: dict[str, Any] | None, feature_name: str) -> float | None:
    if not isinstance(row, dict):
        return None
    feature = str(feature_name).strip()
    if not feature:
        return None
    if feature == "rv_12":
        feature = "vol_12" if "vol_12" in row else "rv_12"
    elif feature == "rv_36":
        feature = "vol_36" if "vol_36" in row else "rv_36"
    if feature == "atr_pct_14":
        value = _safe_optional_float(row.get("atr_pct_14"))
        if value is not None:
            return max(float(value), 0.0)
        atr = _safe_optional_float(row.get("atr_14"))
        close = _safe_optional_float(row.get("close"))
        if atr is None or close is None or close <= 0.0:
            return None
        return max(float(atr) / float(close), 0.0)
    value = _safe_optional_float(row.get(feature))
    if value is None:
        return None
    return max(float(value), 0.0)


def _select_recommended_action(
    *,
    hold_mean_return: float,
    risk_mean_return: float,
    hold_mean_lpm: float,
    risk_mean_lpm: float,
    hold_mean_objective: float,
    risk_mean_objective: float,
) -> str:
    risk_dominates = risk_mean_return >= hold_mean_return and risk_mean_lpm <= hold_mean_lpm
    hold_dominates = hold_mean_return >= risk_mean_return and hold_mean_lpm <= risk_mean_lpm
    if risk_dominates and not hold_dominates:
        return "risk"
    if hold_dominates and not risk_dominates:
        return "hold"
    return "risk" if risk_mean_objective > hold_mean_objective else "hold"


def _apply_notional_multipliers(
    *,
    by_bin: list[dict[str, Any]],
    comparable_indices: list[int],
    notional_model: dict[str, Any],
    numerical_floor: float,
) -> None:
    if not comparable_indices:
        return
    for index in comparable_indices:
        row = by_bin[index]
        edge = max(float(row.get("expected_edge", 0.0) or 0.0), 0.0)
        downside = _resolve_risk_budget_denominator(row=row, numerical_floor=numerical_floor)
        score = edge / downside if edge > 0.0 else 0.0
        by_bin[index]["recommended_notional_multiplier"] = _resolve_notional_multiplier_from_model(
            model=notional_model,
            score=float(score),
        )


def _build_notional_multiplier_model(
    *,
    by_bin: list[dict[str, Any]],
    comparable_indices: list[int],
    size_multiplier_min: float,
    size_multiplier_max: float,
    numerical_floor: float,
) -> dict[str, Any]:
    scores: list[float] = []
    for index in comparable_indices:
        row = by_bin[index]
        edge = max(float(row.get("expected_edge", 0.0) or 0.0), 0.0)
        downside = _resolve_risk_budget_denominator(row=row, numerical_floor=numerical_floor)
        scores.append(edge / downside if edge > 0.0 else 0.0)
    positive_scores = sorted(float(score) for score in scores if float(score) > 0.0)
    if not positive_scores:
        return {
            "status": "degraded",
            "method": "empirical_es_score_ratio_unclipped_v2",
            "score_anchor": 1.0,
            "comparable_score_count": int(len(scores)),
            "deprecated_requested_size_multiplier_min": float(max(float(size_multiplier_min), 0.0)),
            "deprecated_requested_size_multiplier_max": float(
                max(float(size_multiplier_max), max(float(size_multiplier_min), 0.0))
            ),
        }
    anchor = float(np.median(np.asarray(positive_scores, dtype=np.float64)))
    return {
        "status": "ready",
        "method": "empirical_es_score_ratio_unclipped_v2",
        "score_anchor": max(float(anchor), float(numerical_floor)),
        "score_upper": float(np.quantile(np.asarray(positive_scores, dtype=np.float64), 0.90)),
        "comparable_score_count": int(len(scores)),
        "deprecated_requested_size_multiplier_min": float(max(float(size_multiplier_min), 0.0)),
        "deprecated_requested_size_multiplier_max": float(
            max(float(size_multiplier_max), max(float(size_multiplier_min), 0.0))
        ),
    }


def _resolve_continuous_notional_multiplier(
    *,
    policy: dict[str, Any],
    score: float,
    numerical_floor: float,
) -> float:
    model = dict(policy.get("notional_model") or {})
    if not model:
        return 1.0
    return _resolve_notional_multiplier_from_model(model=model, score=float(score))


def _resolve_notional_multiplier_from_model(*, model: dict[str, Any], score: float) -> float:
    anchor = max(float(model.get("score_anchor", 1.0) or 1.0), 1e-6)
    raw_multiplier = max(float(score), 0.0) / anchor
    return float(max(raw_multiplier, 0.0))


def _quantile_bounds(values: np.ndarray, *, bin_count: int) -> list[float]:
    if values.size <= 0:
        return [0.0, 1.0]
    quantiles = np.linspace(0.0, 1.0, max(int(bin_count), 1) + 1)
    bounds = np.quantile(values, quantiles).astype(np.float64, copy=False).tolist()
    deduped: list[float] = []
    for value in bounds:
        numeric = float(value)
        if not deduped or numeric > deduped[-1]:
            deduped.append(numeric)
    if len(deduped) < 2:
        base = float(values[0])
        return [base, base + 1e-9]
    return deduped


def _resolve_bin_index(value: float, bounds: list[float]) -> int:
    if len(bounds) < 2:
        return 0
    clipped = float(value)
    if clipped <= bounds[0]:
        return 0
    if clipped >= bounds[-1]:
        return len(bounds) - 2
    return max(min(int(np.searchsorted(bounds, clipped, side="right") - 1), len(bounds) - 2), 0)


def _build_tail_risk_contract(
    *,
    hold_losses: np.ndarray,
    risk_losses: np.ndarray,
    hold_var_hat: np.ndarray,
    risk_var_hat: np.ndarray,
    target_tail_probability: float,
    ctm_order: int,
    numerical_floor: float,
) -> dict[str, Any]:
    order = max(int(ctm_order), 1)
    return {
        "status": "ready",
        "method": "conditional_linear_quantile_tail_v2",
        "confidence_level": float(1.0 - max(float(target_tail_probability), float(numerical_floor))),
        "target_tail_probability": float(target_tail_probability),
        "ctm_order": int(order),
        "rows_total": int(hold_losses.size),
        "actions": {
            "hold": _build_action_tail_contract(
                losses=hold_losses,
                conditional_var_hat=hold_var_hat,
                target_tail_probability=float(target_tail_probability),
                ctm_order=int(order),
                numerical_floor=float(numerical_floor),
            ),
            "risk": _build_action_tail_contract(
                losses=risk_losses,
                conditional_var_hat=risk_var_hat,
                target_tail_probability=float(target_tail_probability),
                ctm_order=int(order),
                numerical_floor=float(numerical_floor),
            ),
        },
    }


def _build_action_tail_contract(
    *,
    losses: np.ndarray,
    conditional_var_hat: np.ndarray,
    target_tail_probability: float,
    ctm_order: int,
    numerical_floor: float,
) -> dict[str, Any]:
    clean_losses = np.asarray(losses, dtype=np.float64)
    clean_losses = np.where(np.isfinite(clean_losses), np.clip(clean_losses, 0.0, None), 0.0)
    conditional_var = np.asarray(conditional_var_hat, dtype=np.float64)
    conditional_var = np.where(np.isfinite(conditional_var), np.clip(conditional_var, 0.0, None), 0.0)
    if clean_losses.size <= 0:
        return {
            "sample_expected_var": 0.0,
            "tail_count": 0,
            "target_tail_probability": float(target_tail_probability),
            "realized_tail_event_rate": 0.0,
            "sample_expected_es": 0.0,
            "sample_expected_ctm": 0.0,
        }
    tail_mask = clean_losses >= np.maximum(conditional_var - float(numerical_floor), 0.0)
    probability = float(np.mean(tail_mask.astype(np.float64)))
    denominator = max(float(target_tail_probability), float(numerical_floor))
    numerator = float(np.mean(clean_losses * tail_mask.astype(np.float64)))
    ctm_numerator = float(np.mean((clean_losses ** max(int(ctm_order), 1)) * tail_mask.astype(np.float64)))
    return {
        "sample_expected_var": float(np.mean(conditional_var)),
        "tail_count": int(np.sum(tail_mask)),
        "target_tail_probability": float(target_tail_probability),
        "realized_tail_event_rate": float(probability),
        "sample_expected_es": float(numerator / denominator),
        "sample_expected_ctm": float(ctm_numerator / denominator),
    }


def _build_tail_risk_targets(
    *,
    hold_losses: np.ndarray,
    risk_losses: np.ndarray,
    hold_var_hat: np.ndarray,
    risk_var_hat: np.ndarray,
    target_tail_probability: float,
    ctm_order: int,
    numerical_floor: float,
) -> dict[str, np.ndarray]:
    denominator = max(float(target_tail_probability), max(float(numerical_floor), 1e-12))
    hold_tail_hit = (hold_losses >= np.maximum(hold_var_hat - float(numerical_floor), 0.0)).astype(np.float64)
    risk_tail_hit = (risk_losses >= np.maximum(risk_var_hat - float(numerical_floor), 0.0)).astype(np.float64)
    return {
        "hold_es_direct": (hold_losses * hold_tail_hit) / denominator,
        "risk_es_direct": (risk_losses * risk_tail_hit) / denominator,
        "hold_ctm_direct": ((hold_losses ** ctm_order) * hold_tail_hit) / denominator,
        "risk_ctm_direct": ((risk_losses ** ctm_order) * risk_tail_hit) / denominator,
    }


def _summarize_predicted_tail_moments(
    *,
    predictions: list[dict[str, float] | None],
    action: str,
    action_contract: dict[str, Any],
) -> dict[str, float]:
    prefix = str(action).strip().lower()
    expected_es_key = f"{prefix}_expected_es"
    expected_ctm_key = f"{prefix}_expected_ctm"
    tail_probability_key = f"{prefix}_tail_probability"
    expected_var_key = f"{prefix}_expected_var"
    predicted_es = [float(item[expected_es_key]) for item in predictions if isinstance(item, dict) and expected_es_key in item]
    predicted_ctm = [float(item[expected_ctm_key]) for item in predictions if isinstance(item, dict) and expected_ctm_key in item]
    predicted_probability = [
        float(item[tail_probability_key]) for item in predictions if isinstance(item, dict) and tail_probability_key in item
    ]
    predicted_var = [float(item[expected_var_key]) for item in predictions if isinstance(item, dict) and expected_var_key in item]
    return {
        "tail_probability": float(np.mean(predicted_probability)) if predicted_probability else float(
            action_contract.get("target_tail_probability", 0.0) or 0.0
        ),
        "tail_threshold_loss": float(np.mean(predicted_var)) if predicted_var else float(
            action_contract.get("sample_expected_var", 0.0) or 0.0
        ),
        "expected_es": float(np.mean(predicted_es)) if predicted_es else float(
            action_contract.get("sample_expected_es", 0.0) or 0.0
        ),
        "expected_ctm": float(np.mean(predicted_ctm)) if predicted_ctm else float(
            action_contract.get("sample_expected_ctm", 0.0) or 0.0
        ),
    }


def _resolve_risk_budget_denominator(*, row: dict[str, Any], numerical_floor: float) -> float:
    expected_es = _safe_optional_float(row.get("expected_es"))
    if expected_es is not None and expected_es > 0.0:
        return max(float(expected_es), float(numerical_floor))
    return max(float(row.get("expected_downside_deviation", 0.0) or 0.0), float(numerical_floor))


def _insufficient_trade_action_decision(
    policy: dict[str, Any],
    *,
    reason_code: str,
) -> dict[str, Any]:
    return {
        "policy": TRADE_ACTION_POLICY_ID,
        "status": "insufficient_evidence",
        "reason_code": str(reason_code).strip() or "TRADE_ACTION_INSUFFICIENT_EVIDENCE",
        "decision_source": "INSUFFICIENT_EVIDENCE",
        "chosen_action_source": "INSUFFICIENT_EVIDENCE",
        "risk_feature_name": str(policy.get("risk_feature_name", "")).strip(),
    }


def _result_doc(net_return: float, *, lpm_order: int, numerical_floor: float) -> dict[str, float]:
    downside = max(-float(net_return), 0.0)
    downside_lpm = downside ** max(int(lpm_order), 1)
    denominator = max(downside, float(numerical_floor))
    raw = float(net_return) / denominator
    objective = math.copysign(math.log1p(abs(raw)), raw)
    return {
        "return": float(net_return),
        "downside_lpm": float(downside_lpm),
        "objective_score": float(objective),
    }


def _net_return_after_costs(
    *,
    entry_price: float,
    exit_price: float,
    exit_fee_rate: float,
    exit_slippage_bps: float,
) -> float:
    entry_value = max(float(entry_price), 1e-12)
    effective_exit = max(float(exit_price) * (1.0 - (max(float(exit_slippage_bps), 0.0) / 10_000.0)), 0.0)
    gross = (effective_exit / entry_value) - 1.0
    return float(gross - max(float(exit_fee_rate), 0.0))


def _net_drawdown_from_peak_after_costs(
    *,
    peak_price: float,
    current_price: float,
    exit_fee_rate: float,
    exit_slippage_bps: float,
) -> float:
    peak_value = max(float(peak_price), 1e-12)
    effective_current = max(float(current_price) * (1.0 - (max(float(exit_slippage_bps), 0.0) / 10_000.0)), 0.0)
    gross = 1.0 - (effective_current / peak_value)
    return float(max(gross + max(float(exit_fee_rate), 0.0), 0.0))


def _safe_optional_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_float(value: Any) -> float:
    parsed = _safe_optional_float(value)
    if parsed is None or not math.isfinite(float(parsed)):
        return 0.0
    return float(parsed)
