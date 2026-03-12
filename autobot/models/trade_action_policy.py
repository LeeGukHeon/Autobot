from __future__ import annotations

import math
from typing import Any

import numpy as np

from .selection_calibration import apply_selection_calibration


TRADE_ACTION_POLICY_ID = "trade_level_hold_risk_oos_bins_v1"
CONDITIONAL_ACTION_MODEL_ID = "conditional_action_linear_ols_v1"
DEFAULT_EDGE_BIN_COUNT = 6
DEFAULT_RISK_BIN_COUNT = 6
DEFAULT_MIN_BIN_SAMPLES = 20
DEFAULT_STATE_FEATURE_NAMES = ("selection_score", "rv_12", "rv_36", "atr_pct_14")


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
        numerical_floor=float(numerical_floor),
    )
    selection_scores = np.asarray([float(row["selection_score"]) for row in trade_rows], dtype=np.float64)
    risk_values = np.asarray(
        [
            float(
                _resolve_trade_row_risk_state_value(
                    trade_row=row,
                    conditional_action_model=conditional_action_model,
                    lpm_order=max(int(lpm_order), 1),
                    numerical_floor=float(numerical_floor),
                )
            )
            for row in trade_rows
        ],
        dtype=np.float64,
    )
    edge_bounds = _quantile_bounds(selection_scores, bin_count=max(int(edge_bin_count), 1))
    risk_bounds = _quantile_bounds(risk_values, bin_count=max(int(risk_bin_count), 1))
    grouped: dict[tuple[int, int], list[dict[str, Any]]] = {}
    for row, risk_value in zip(trade_rows, risk_values.tolist(), strict=False):
        edge_bin = _resolve_bin_index(float(row["selection_score"]), edge_bounds)
        risk_bin = _resolve_bin_index(float(risk_value), risk_bounds)
        grouped.setdefault((edge_bin, risk_bin), []).append(row)

    by_bin: list[dict[str, Any]] = []
    comparable_indices: list[int] = []
    for (edge_bin, risk_bin), rows in sorted(grouped.items()):
        sample_count = len(rows)
        comparable = sample_count >= max(int(min_bin_samples), 1)
        hold_returns = np.asarray([float(item["hold_return"]) for item in rows], dtype=np.float64)
        risk_returns = np.asarray([float(item["risk_return"]) for item in rows], dtype=np.float64)
        hold_downside_lpm = np.asarray([float(item["hold_downside_lpm"]) for item in rows], dtype=np.float64)
        risk_downside_lpm = np.asarray([float(item["risk_downside_lpm"]) for item in rows], dtype=np.float64)
        hold_objective = np.asarray([float(item["hold_objective_score"]) for item in rows], dtype=np.float64)
        risk_objective = np.asarray([float(item["risk_objective_score"]) for item in rows], dtype=np.float64)
        hold_mean_return = float(np.mean(hold_returns))
        risk_mean_return = float(np.mean(risk_returns))
        hold_mean_lpm = float(np.mean(hold_downside_lpm))
        risk_mean_lpm = float(np.mean(risk_downside_lpm))
        hold_mean_dev = float(hold_mean_lpm ** (1.0 / float(max(int(lpm_order), 1)))) if hold_mean_lpm > 0.0 else 0.0
        risk_mean_dev = float(risk_mean_lpm ** (1.0 / float(max(int(lpm_order), 1)))) if risk_mean_lpm > 0.0 else 0.0
        hold_mean_objective = float(np.mean(hold_objective))
        risk_mean_objective = float(np.mean(risk_objective))
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
            "hold": {
                "mean_return": hold_mean_return,
                "mean_downside_lpm": hold_mean_lpm,
                "mean_downside_deviation": hold_mean_dev,
                "mean_objective_score": hold_mean_objective,
            },
            "risk": {
                "mean_return": risk_mean_return,
                "mean_downside_lpm": risk_mean_lpm,
                "mean_downside_deviation": risk_mean_dev,
                "mean_objective_score": risk_mean_objective,
            },
        }
        if comparable:
            comparable_indices.append(len(by_bin))
        by_bin.append(row_doc)

    _apply_notional_multipliers(
        by_bin=by_bin,
        comparable_indices=comparable_indices,
        size_multiplier_min=float(size_multiplier_min),
        size_multiplier_max=float(size_multiplier_max),
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
        "edge_bounds": [float(value) for value in edge_bounds],
        "risk_bounds": [float(value) for value in risk_bounds],
        "edge_bin_count": int(edge_bin_count_effective),
        "risk_bin_count": int(risk_bin_count_effective),
        "min_bin_samples": max(int(min_bin_samples), 1),
        "lpm_order": max(int(lpm_order), 1),
        "hold_policy_template": hold_template,
        "risk_policy_template": risk_template,
        "conditional_action_model": conditional_action_model,
        "by_bin": by_bin,
        "summary": {
            "comparable_bins": int(len(comparable_indices)),
            "risk_bins_recommended": int(sum(1 for item in by_bin if item["recommended_action"] == "risk")),
            "hold_bins_recommended": int(sum(1 for item in by_bin if item["recommended_action"] == "hold")),
        },
    }


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
    payload["conditional_action_model"] = dict(payload.get("conditional_action_model") or {})
    payload["by_bin"] = [dict(item) for item in (payload.get("by_bin") or []) if isinstance(item, dict)]
    return payload


def _build_conditional_action_model(
    *,
    trade_rows: list[dict[str, Any]],
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
    targets = {
        "hold_return": np.asarray([float(item["hold_return"]) for item in trade_rows], dtype=np.float64),
        "risk_return": np.asarray([float(item["risk_return"]) for item in trade_rows], dtype=np.float64),
        "hold_objective": np.asarray([float(item["hold_objective_score"]) for item in trade_rows], dtype=np.float64),
        "risk_objective": np.asarray([float(item["risk_objective_score"]) for item in trade_rows], dtype=np.float64),
        "hold_es_proxy": np.asarray(
            [max(-float(item["hold_return"]), 0.0) for item in trade_rows],
            dtype=np.float64,
        ),
        "risk_es_proxy": np.asarray(
            [max(-float(item["risk_return"]), 0.0) for item in trade_rows],
            dtype=np.float64,
        ),
        "hold_ctm2_proxy": np.asarray(
            [max(-float(item["hold_return"]), 0.0) ** 2 for item in trade_rows],
            dtype=np.float64,
        ),
        "risk_ctm2_proxy": np.asarray(
            [max(-float(item["risk_return"]), 0.0) ** 2 for item in trade_rows],
            dtype=np.float64,
        ),
        "hold_log_lpm": np.asarray(
            [math.log1p(max(float(item["hold_downside_lpm"]), 0.0)) for item in trade_rows],
            dtype=np.float64,
        ),
        "risk_log_lpm": np.asarray(
            [math.log1p(max(float(item["risk_downside_lpm"]), 0.0)) for item in trade_rows],
            dtype=np.float64,
        ),
    }
    fitted_targets: dict[str, Any] = {}
    for name, y in targets.items():
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
    return {
        "intercept": float(coef[0]),
        "coef": [float(value) for value in coef[1:].tolist()],
        "x_mean": [float(value) for value in x_mean.tolist()],
        "x_scale": [float(value) for value in x_scale.tolist()],
    }


def _predict_linear_model(model: dict[str, Any], *, vector: np.ndarray) -> float | None:
    coef = np.asarray(model.get("coef") or [], dtype=np.float64)
    x_mean = np.asarray(model.get("x_mean") or [], dtype=np.float64)
    x_scale = np.asarray(model.get("x_scale") or [], dtype=np.float64)
    if coef.size <= 0 or coef.size != vector.size or x_mean.size != vector.size or x_scale.size != vector.size:
        return None
    scaled = (vector - x_mean) / np.where(x_scale > 1e-12, x_scale, 1.0)
    return float(float(model.get("intercept", 0.0) or 0.0) + float(np.dot(coef, scaled)))


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
    hold_es_proxy = _predict_linear_model(dict(targets.get("hold_es_proxy") or {}), vector=vector)
    risk_es_proxy = _predict_linear_model(dict(targets.get("risk_es_proxy") or {}), vector=vector)
    hold_ctm2_proxy = _predict_linear_model(dict(targets.get("hold_ctm2_proxy") or {}), vector=vector)
    risk_ctm2_proxy = _predict_linear_model(dict(targets.get("risk_ctm2_proxy") or {}), vector=vector)
    hold_log_lpm = _predict_linear_model(dict(targets.get("hold_log_lpm") or {}), vector=vector)
    risk_log_lpm = _predict_linear_model(dict(targets.get("risk_log_lpm") or {}), vector=vector)
    values = (
        hold_return,
        risk_return,
        hold_objective,
        risk_objective,
        hold_es_proxy,
        risk_es_proxy,
        hold_ctm2_proxy,
        risk_ctm2_proxy,
        hold_log_lpm,
        risk_log_lpm,
    )
    if any(value is None or not math.isfinite(float(value)) for value in values):
        return None
    hold_es = max(float(hold_es_proxy), 0.0)
    risk_es = max(float(risk_es_proxy), 0.0)
    hold_ctm2 = max(float(hold_ctm2_proxy), 0.0)
    risk_ctm2 = max(float(risk_ctm2_proxy), 0.0)
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
        "hold_expected_es": float(hold_es),
        "risk_expected_es": float(risk_es),
        "hold_expected_ctm2": float(hold_ctm2),
        "risk_expected_ctm2": float(risk_ctm2),
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
    defaults = [float(value) for value in (dict(next(iter((model_payload.get("targets") or {}).values()), {}) ).get("x_mean") or [])]
    if len(defaults) != len(DEFAULT_STATE_FEATURE_NAMES):
        defaults = [0.0] * len(DEFAULT_STATE_FEATURE_NAMES)
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
            value = float(defaults[index])
        resolved_values.append(float(value))
    return np.asarray(resolved_values, dtype=np.float64)


def _resolve_trade_row_risk_state_value(
    *,
    trade_row: dict[str, Any],
    conditional_action_model: dict[str, Any],
    lpm_order: int,
    numerical_floor: float,
) -> float:
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
            expected_ctm2 = float(predicted["risk_expected_ctm2"] if action == "risk" else predicted["hold_expected_ctm2"])
            continuous_score = expected_edge / max(expected_downside_deviation, 1e-6) if expected_edge > 0.0 else 0.0
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
                "expected_es": expected_es,
                "expected_ctm2": expected_ctm2,
                "sample_count": int(predicted.get("sample_count", 0) or 0),
                "edge_bin": edge_bin,
                "risk_bin": risk_bin,
                "risk_feature_name": str(normalized.get("risk_feature_name", "")).strip(),
                "risk_feature_value": (
                    float(raw_risk_feature_value)
                    if raw_risk_feature_value is not None and math.isfinite(float(raw_risk_feature_value))
                    else float(predicted["risk_state_value"])
                ),
                "risk_state_source": CONDITIONAL_ACTION_MODEL_ID,
                "decision_source": "continuous_conditional_action_value",
                "exit_policy_template": template,
                "diagnostics": {
                    "hold_mean_return": float(predicted["hold_mean_return"]),
                    "risk_mean_return": float(predicted["risk_mean_return"]),
                    "hold_mean_objective": float(predicted["hold_mean_objective"]),
                    "risk_mean_objective": float(predicted["risk_mean_objective"]),
                    "hold_expected_es": float(predicted["hold_expected_es"]),
                    "risk_expected_es": float(predicted["risk_expected_es"]),
                    "hold_expected_ctm2": float(predicted["hold_expected_ctm2"]),
                    "risk_expected_ctm2": float(predicted["risk_expected_ctm2"]),
                },
            }
    risk_feature_name = str(normalized.get("risk_feature_name", "")).strip()
    if not risk_feature_name:
        return None
    risk_value = _resolve_row_risk_feature_value(row=row, feature_name=risk_feature_name)
    if risk_value is None or not math.isfinite(float(risk_value)):
        return None
    edge_bounds = [float(value) for value in (normalized.get("edge_bounds") or [])]
    risk_bounds = [float(value) for value in (normalized.get("risk_bounds") or [])]
    if len(edge_bounds) < 2 or len(risk_bounds) < 2:
        return None
    edge_bin = _resolve_bin_index(float(selection_score), edge_bounds)
    risk_bin = _resolve_bin_index(float(risk_value), risk_bounds)
    min_bin_samples = max(int(normalized.get("min_bin_samples", DEFAULT_MIN_BIN_SAMPLES) or DEFAULT_MIN_BIN_SAMPLES), 1)
    for item in normalized.get("by_bin") or []:
        if int(item.get("edge_bin", -1)) != edge_bin or int(item.get("risk_bin", -1)) != risk_bin:
            continue
        if not bool(item.get("comparable", False)):
            return None
        if int(item.get("sample_count", 0) or 0) < min_bin_samples:
            return None
        action = str(item.get("recommended_action", "")).strip().lower()
        if action not in {"hold", "risk"}:
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
            "sample_count": int(item.get("sample_count", 0) or 0),
            "edge_bin": int(edge_bin),
            "risk_bin": int(risk_bin),
            "risk_feature_name": risk_feature_name,
            "risk_feature_value": float(risk_value),
            "exit_policy_template": template,
        }
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
    size_multiplier_min: float,
    size_multiplier_max: float,
    numerical_floor: float,
) -> None:
    min_multiplier = max(float(size_multiplier_min), 0.0)
    max_multiplier = max(float(size_multiplier_max), min_multiplier)
    if not comparable_indices:
        return
    scores: list[tuple[float, int]] = []
    for index in comparable_indices:
        row = by_bin[index]
        edge = max(float(row.get("expected_edge", 0.0) or 0.0), 0.0)
        downside = max(float(row.get("expected_downside_deviation", 0.0) or 0.0), float(numerical_floor))
        score = edge / downside if edge > 0.0 else 0.0
        scores.append((float(score), int(index)))
    scores.sort(key=lambda item: item[0])
    if len(scores) == 1:
        by_bin[scores[0][1]]["recommended_notional_multiplier"] = float(max_multiplier if scores[0][0] > 0.0 else min_multiplier)
        return
    for rank, (score, index) in enumerate(scores):
        if score <= 0.0:
            multiplier = min_multiplier
        else:
            ratio = float(rank) / float(max(len(scores) - 1, 1))
            multiplier = min_multiplier + (ratio * (max_multiplier - min_multiplier))
        by_bin[index]["recommended_notional_multiplier"] = float(multiplier)


def _resolve_continuous_notional_multiplier(
    *,
    policy: dict[str, Any],
    score: float,
    numerical_floor: float,
) -> float:
    comparable_rows = [dict(item) for item in (policy.get("by_bin") or []) if bool(item.get("comparable", False))]
    if not comparable_rows:
        return 1.0
    min_multiplier = min(float(item.get("recommended_notional_multiplier", 1.0) or 1.0) for item in comparable_rows)
    max_multiplier = max(float(item.get("recommended_notional_multiplier", 1.0) or 1.0) for item in comparable_rows)
    scored_rows: list[float] = []
    for item in comparable_rows:
        edge = max(float(item.get("expected_edge", 0.0) or 0.0), 0.0)
        downside = max(float(item.get("expected_downside_deviation", 0.0) or 0.0), float(numerical_floor))
        scored_rows.append(edge / downside if edge > 0.0 else 0.0)
    min_score = min(scored_rows, default=0.0)
    max_score = max(scored_rows, default=0.0)
    if max_score <= min_score + float(numerical_floor):
        return float(max_multiplier if score > 0.0 else min_multiplier)
    clipped = min(max(float(score), min_score), max_score)
    ratio = (clipped - min_score) / max(max_score - min_score, float(numerical_floor))
    return float(min_multiplier + (ratio * (max_multiplier - min_multiplier)))


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
