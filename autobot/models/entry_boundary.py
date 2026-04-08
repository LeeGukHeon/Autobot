"""Risk-calibrated entry boundary builders and evaluators."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np


@dataclass(frozen=True)
class _LinearRiskModel:
    intercept: float
    coefficients: tuple[float, ...]
    feature_shift: tuple[float, ...]
    feature_scale: tuple[float, ...]
    constant_probability: float | None = None

    def predict_proba(self, x: np.ndarray) -> np.ndarray:
        matrix = np.asarray(x, dtype=np.float64)
        if self.constant_probability is not None:
            probs = np.full(matrix.shape[0], float(self.constant_probability), dtype=np.float64)
            return np.column_stack([1.0 - probs, probs])
        shift = np.asarray(self.feature_shift, dtype=np.float64)
        scale = np.asarray(self.feature_scale, dtype=np.float64)
        safe_scale = np.where(np.abs(scale) < 1e-12, 1.0, scale)
        normalized = (matrix - shift) / safe_scale
        coeff = np.asarray(self.coefficients, dtype=np.float64)
        logits = normalized @ coeff + float(self.intercept)
        probs = 1.0 / (1.0 + np.exp(-np.clip(logits, -40.0, 40.0)))
        return np.column_stack([1.0 - probs, probs])


def build_risk_calibrated_entry_boundary(
    *,
    final_rank_score: np.ndarray,
    final_expected_return: np.ndarray,
    final_expected_es: np.ndarray,
    final_tradability: np.ndarray,
    final_uncertainty: np.ndarray,
    final_alpha_lcb: np.ndarray,
    realized_return: np.ndarray,
    sequence_support_score: np.ndarray | None = None,
    lob_support_score: np.ndarray | None = None,
    input_quality_summary: dict[str, Any] | None = None,
    tradability_provenance: dict[str, Any] | None = None,
    severe_loss_ratio: float = 0.02,
    target_max_severe_loss_rate: float = 0.10,
) -> dict[str, Any]:
    support_score = _resolve_support_score_array(
        sequence_support_score=sequence_support_score,
        lob_support_score=lob_support_score,
        rows=int(np.asarray(final_rank_score, dtype=np.float64).shape[0]),
    )
    quality_penalty_policy = _build_quality_penalty_policy(
        input_quality_summary=input_quality_summary,
        tradability_provenance=tradability_provenance,
    )
    x = np.column_stack(
        [
            np.asarray(final_expected_return, dtype=np.float64),
            np.asarray(final_expected_es, dtype=np.float64),
            np.asarray(final_uncertainty, dtype=np.float64),
            np.asarray(final_tradability, dtype=np.float64),
            np.asarray(final_alpha_lcb, dtype=np.float64),
            np.asarray(final_rank_score, dtype=np.float64),
            np.asarray(support_score, dtype=np.float64),
        ]
    )
    severe_loss_ratio_value = abs(float(severe_loss_ratio))
    y = (np.asarray(realized_return, dtype=np.float64) <= -severe_loss_ratio_value).astype(np.int64)
    risk_model = _fit_logistic_risk_model(x, y)
    risk_prob = risk_model.predict_proba(x)[:, 1]

    alpha_lcb_values = np.asarray(final_alpha_lcb, dtype=np.float64)
    alpha_floor_candidates = sorted(
        {
            0.0,
            *np.quantile(alpha_lcb_values, [0.1, 0.25, 0.5]).tolist(),
        }
    )
    tradability_candidates = sorted({0.0, *np.quantile(np.asarray(final_tradability, dtype=np.float64), [0.1, 0.25, 0.5]).tolist()})
    risk_candidates = sorted({1.0, *np.quantile(risk_prob, [0.25, 0.5, 0.75, 0.9]).tolist()})
    support_threshold_candidates = sorted(
        {
            0.0,
            float(quality_penalty_policy.get("default_support_score_threshold", 0.0) or 0.0),
            *np.unique(np.asarray(support_score, dtype=np.float64)).tolist(),
        }
    )

    best: dict[str, Any] | None = None
    realized = np.asarray(realized_return, dtype=np.float64)
    for alpha_lcb_floor in alpha_floor_candidates:
        for tradability_threshold in tradability_candidates:
            for support_score_threshold in support_threshold_candidates:
                adjusted_tradability = np.maximum(
                    np.asarray(final_tradability, dtype=np.float64)
                    - float(quality_penalty_policy.get("tradability_penalty", 0.0) or 0.0),
                    0.0,
                )
                adjusted_risk_prob = risk_prob * _support_quality_multiplier(
                    support_score=support_score,
                    reduced_context_multiplier=float(
                        quality_penalty_policy.get("reduced_context_severe_loss_risk_multiplier", 1.0) or 1.0
                    ),
                    unknown_support_multiplier=float(
                        quality_penalty_policy.get("unknown_support_severe_loss_risk_multiplier", 1.0) or 1.0
                    ),
                ) * float(quality_penalty_policy.get("tradability_evidence_risk_multiplier", 1.0) or 1.0)
                for risk_threshold in risk_candidates:
                    allowed_mask = (
                        (alpha_lcb_values > float(alpha_lcb_floor))
                        & (adjusted_tradability >= float(tradability_threshold))
                        & (adjusted_risk_prob <= float(risk_threshold))
                        & (support_score >= float(support_score_threshold))
                    )
                    accepted = realized[allowed_mask]
                    if accepted.size <= 0:
                        continue
                    severe_rate = float(np.mean(accepted <= -severe_loss_ratio_value))
                    nonpositive_rate = float(np.mean(accepted <= 0.0))
                    ev_mean = float(np.mean(accepted))
                    candidate = {
                        "alpha_lcb_floor": float(alpha_lcb_floor),
                        "tradability_threshold": float(tradability_threshold),
                        "severe_loss_risk_threshold": float(risk_threshold),
                        "support_score_threshold": float(support_score_threshold),
                        "accepted_rows": int(accepted.size),
                        "severe_loss_rate": severe_rate,
                        "nonpositive_rate": nonpositive_rate,
                        "ev_mean": ev_mean,
                    }
                    if severe_rate > float(target_max_severe_loss_rate):
                        continue
                    if best is None or (candidate["ev_mean"], -candidate["severe_loss_rate"], candidate["accepted_rows"]) > (
                        best["ev_mean"],
                        -best["severe_loss_rate"],
                        best["accepted_rows"],
                    ):
                        best = candidate

    if best is None:
        best = {
            "alpha_lcb_floor": 0.0,
            "tradability_threshold": 0.0,
            "severe_loss_risk_threshold": float(np.quantile(risk_prob, 0.5)) if risk_prob.size > 0 else 1.0,
            "support_score_threshold": float(quality_penalty_policy.get("default_support_score_threshold", 0.0) or 0.0),
            "accepted_rows": 0,
            "severe_loss_rate": 1.0,
            "nonpositive_rate": 1.0,
            "ev_mean": 0.0,
        }
    support_quality_breakdown = _build_support_quality_breakdown(
        support_score=support_score,
        realized_return=realized,
        severe_loss_ratio=severe_loss_ratio_value,
    )
    strict_full_severe_rate = _safe_optional_float(
        ((support_quality_breakdown.get("strict_full_like") or {}).get("severe_loss_rate"))
    )
    reduced_context_severe_rate = _safe_optional_float(
        ((support_quality_breakdown.get("reduced_context_like") or {}).get("severe_loss_rate"))
    )
    if (
        float(quality_penalty_policy.get("default_support_score_threshold", 0.0) or 0.0) > 0.0
        and strict_full_severe_rate is not None
        and reduced_context_severe_rate is not None
        and strict_full_severe_rate <= float(target_max_severe_loss_rate)
        and reduced_context_severe_rate > float(target_max_severe_loss_rate)
    ):
        best["support_score_threshold"] = max(
            float(best.get("support_score_threshold", 0.0) or 0.0),
            2.0,
        )

    return {
        "version": 1,
        "policy": "risk_calibrated_entry_boundary_v1",
        "alpha_lcb_floor": float(best["alpha_lcb_floor"]),
        "tradability_threshold": float(best["tradability_threshold"]),
        "severe_loss_risk_threshold": float(best["severe_loss_risk_threshold"]),
        "support_quality_policy": {
            "enabled": True,
            "support_score_threshold": float(best["support_score_threshold"]),
            "tradability_penalty": float(quality_penalty_policy.get("tradability_penalty", 0.0) or 0.0),
            "reduced_context_severe_loss_risk_multiplier": float(
                quality_penalty_policy.get("reduced_context_severe_loss_risk_multiplier", 1.0) or 1.0
            ),
            "unknown_support_severe_loss_risk_multiplier": float(
                quality_penalty_policy.get("unknown_support_severe_loss_risk_multiplier", 1.0) or 1.0
            ),
            "tradability_evidence_risk_multiplier": float(
                quality_penalty_policy.get("tradability_evidence_risk_multiplier", 1.0) or 1.0
            ),
            "overall_input_quality_status": str(quality_penalty_policy.get("overall_input_quality_status") or ""),
            "tradability_evidence_strength": str(quality_penalty_policy.get("tradability_evidence_strength") or ""),
            "tradability_quality_status": str(quality_penalty_policy.get("tradability_quality_status") or ""),
        },
        "target_max_severe_loss_rate": float(target_max_severe_loss_rate),
        "severe_loss_ratio": float(severe_loss_ratio_value),
        "severe_loss_bps": float(severe_loss_ratio_value),
        "risk_model": {
            "feature_names": [
                "final_expected_return",
                "final_expected_es",
                "final_uncertainty",
                "final_tradability",
                "final_alpha_lcb",
                "final_rank_score",
                "support_quality_score",
            ],
            "intercept": float(risk_model.intercept),
            "coefficients": list(risk_model.coefficients),
            "feature_shift": list(risk_model.feature_shift),
            "feature_scale": list(risk_model.feature_scale),
            "constant_probability": risk_model.constant_probability,
        },
        "calibration_metrics": {
            "accepted_rows": int(best["accepted_rows"]),
            "severe_loss_rate": float(best["severe_loss_rate"]),
            "nonpositive_rate": float(best["nonpositive_rate"]),
            "ev_mean": float(best["ev_mean"]),
            "support_quality_breakdown": support_quality_breakdown,
        },
        "input_quality_summary": dict(input_quality_summary or {}),
        "tradability_provenance": dict(tradability_provenance or {}),
        "formula": {
            "entry_allowed": "alpha_lcb > floor AND adjusted_tradability >= threshold AND support_quality >= threshold AND adjusted_severe_loss_risk <= threshold",
            "reason_codes": [
                "ENTRY_BOUNDARY_ALPHA_LCB_NOT_POSITIVE",
                "ENTRY_BOUNDARY_TRADABILITY_BELOW_THRESHOLD",
                "ENTRY_BOUNDARY_SEVERE_LOSS_RISK_HIGH",
                "ENTRY_BOUNDARY_SUPPORT_QUALITY_BELOW_THRESHOLD",
            ],
        },
    }


def evaluate_entry_boundary(
    *,
    row: dict[str, Any],
    contract: dict[str, Any] | None,
) -> dict[str, Any]:
    payload = dict(contract or {})
    if not payload:
        return {"enabled": False, "allowed": True, "reason_codes": [], "estimated_severe_loss_risk": None}

    features = np.asarray(
        [
            _safe_optional_float(row.get("final_expected_return")) or 0.0,
            _safe_optional_float(row.get("final_expected_es")) or 0.0,
            _safe_optional_float(row.get("final_uncertainty")) or 0.0,
            _safe_optional_float(row.get("final_tradability")) or 0.0,
            _safe_optional_float(row.get("final_alpha_lcb")) or 0.0,
            _safe_optional_float(row.get("final_rank_score")) or 0.0,
            _resolve_row_support_quality_score(row=row),
        ],
        dtype=np.float64,
    ).reshape(1, -1)
    risk_doc = dict(payload.get("risk_model") or {})
    risk_model = _LinearRiskModel(
        intercept=float(risk_doc.get("intercept") or 0.0),
        coefficients=tuple(float(item) for item in (risk_doc.get("coefficients") or [])),
        feature_shift=tuple(float(item) for item in (risk_doc.get("feature_shift") or [0.0] * features.shape[1])),
        feature_scale=tuple(float(item) for item in (risk_doc.get("feature_scale") or [1.0] * features.shape[1])),
        constant_probability=(
            float(risk_doc.get("constant_probability")) if risk_doc.get("constant_probability") is not None else None
        ),
    )
    severe_risk = float(risk_model.predict_proba(features)[0, 1])
    support_policy = dict(payload.get("support_quality_policy") or {})
    support_quality_score = _resolve_row_support_quality_score(row=row)
    adjusted_tradability = max(
        (_safe_optional_float(row.get("final_tradability")) or 0.0)
        - float(support_policy.get("tradability_penalty", 0.0) or 0.0),
        0.0,
    )
    adjusted_severe_risk = severe_risk * float(
        _support_quality_multiplier(
            support_score=np.asarray([support_quality_score], dtype=np.float64),
            reduced_context_multiplier=float(
                support_policy.get("reduced_context_severe_loss_risk_multiplier", 1.0) or 1.0
            ),
            unknown_support_multiplier=float(
                support_policy.get("unknown_support_severe_loss_risk_multiplier", 1.0) or 1.0
            ),
        )[0]
    ) * float(support_policy.get("tradability_evidence_risk_multiplier", 1.0) or 1.0)
    reason_codes: list[str] = []
    if (_safe_optional_float(row.get("final_alpha_lcb")) or 0.0) <= float(payload.get("alpha_lcb_floor") or 0.0):
        reason_codes.append("ENTRY_BOUNDARY_ALPHA_LCB_NOT_POSITIVE")
    if adjusted_tradability < float(payload.get("tradability_threshold") or 0.0):
        reason_codes.append("ENTRY_BOUNDARY_TRADABILITY_BELOW_THRESHOLD")
    if support_quality_score < float(support_policy.get("support_score_threshold", 0.0) or 0.0):
        reason_codes.append("ENTRY_BOUNDARY_SUPPORT_QUALITY_BELOW_THRESHOLD")
    if adjusted_severe_risk > float(payload.get("severe_loss_risk_threshold") or 1.0):
        reason_codes.append("ENTRY_BOUNDARY_SEVERE_LOSS_RISK_HIGH")
    return {
        "enabled": True,
        "allowed": len(reason_codes) == 0,
        "reason_codes": reason_codes,
        "estimated_severe_loss_risk": severe_risk,
        "quality_adjusted_severe_loss_risk": float(adjusted_severe_risk),
        "support_quality_score": float(support_quality_score),
        "adjusted_tradability": float(adjusted_tradability),
        "alpha_lcb_floor": float(payload.get("alpha_lcb_floor") or 0.0),
        "tradability_threshold": float(payload.get("tradability_threshold") or 0.0),
        "severe_loss_risk_threshold": float(payload.get("severe_loss_risk_threshold") or 1.0),
        "support_score_threshold": float(support_policy.get("support_score_threshold", 0.0) or 0.0),
    }


def _fit_logistic_risk_model(x: np.ndarray, y: np.ndarray) -> _LinearRiskModel:
    x_values = np.asarray(x, dtype=np.float64)
    y_values = np.asarray(y, dtype=np.int64)
    if np.unique(y_values).size < 2:
        probability = float(np.mean(y_values)) if y_values.size > 0 else 0.5
        return _LinearRiskModel(
            intercept=0.0,
            coefficients=tuple([0.0] * x_values.shape[1]),
            feature_shift=tuple([0.0] * x_values.shape[1]),
            feature_scale=tuple([1.0] * x_values.shape[1]),
            constant_probability=probability,
        )
    from sklearn.linear_model import LogisticRegression

    shift = np.mean(x_values, axis=0)
    scale = np.std(x_values, axis=0, ddof=0)
    scale = np.where(np.abs(scale) < 1e-12, 1.0, scale)
    normalized = (x_values - shift) / scale
    estimator = LogisticRegression(max_iter=1000, solver="lbfgs")
    estimator.fit(normalized, y_values)
    return _LinearRiskModel(
        intercept=float(np.asarray(estimator.intercept_, dtype=np.float64)[0]),
        coefficients=tuple(np.asarray(estimator.coef_, dtype=np.float64)[0].tolist()),
        feature_shift=tuple(shift.tolist()),
        feature_scale=tuple(scale.tolist()),
    )


def _safe_optional_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _resolve_support_score_array(
    *,
    sequence_support_score: np.ndarray | None,
    lob_support_score: np.ndarray | None,
    rows: int,
) -> np.ndarray:
    default = np.full(int(rows), 2.0, dtype=np.float64)
    sequence = np.asarray(sequence_support_score, dtype=np.float64) if sequence_support_score is not None else default
    lob = np.asarray(lob_support_score, dtype=np.float64) if lob_support_score is not None else default
    if sequence.shape[0] != int(rows):
        sequence = default
    if lob.shape[0] != int(rows):
        lob = default
    return np.minimum(sequence, lob)


def _build_quality_penalty_policy(
    *,
    input_quality_summary: dict[str, Any] | None,
    tradability_provenance: dict[str, Any] | None,
) -> dict[str, Any]:
    summary = dict(input_quality_summary or {})
    tradability = dict(tradability_provenance or {})
    overall_status = str(summary.get("overall_quality_status") or "").strip().lower()
    tradability_strength = str(tradability.get("evidence_strength") or "").strip().lower()
    tradability_quality_status = str(tradability.get("quality_status") or "").strip().lower()
    degraded = overall_status == "degraded" or tradability_strength == "thin" or tradability_quality_status in {
        "thin_training_evidence",
        "proxy_backed",
        "missing",
    }
    caution = overall_status == "caution" or tradability_strength == "moderate"
    return {
        "overall_input_quality_status": overall_status,
        "tradability_evidence_strength": tradability_strength,
        "tradability_quality_status": tradability_quality_status,
        "default_support_score_threshold": 1.0 if degraded else 0.0,
        "tradability_penalty": 0.05 if degraded else (0.02 if caution else 0.0),
        "reduced_context_severe_loss_risk_multiplier": 1.15 if degraded else (1.08 if caution else 1.0),
        "unknown_support_severe_loss_risk_multiplier": 1.25 if degraded else (1.10 if caution else 1.0),
        "tradability_evidence_risk_multiplier": 1.15 if degraded else (1.05 if caution else 1.0),
    }


def _support_quality_multiplier(
    *,
    support_score: np.ndarray,
    reduced_context_multiplier: float,
    unknown_support_multiplier: float,
) -> np.ndarray:
    values = np.asarray(support_score, dtype=np.float64)
    return np.where(
        values < 1.0,
        float(unknown_support_multiplier),
        np.where(values < 2.0, float(reduced_context_multiplier), 1.0),
    ).astype(np.float64, copy=False)


def _build_support_quality_breakdown(
    *,
    support_score: np.ndarray,
    realized_return: np.ndarray,
    severe_loss_ratio: float,
) -> dict[str, Any]:
    support = np.asarray(support_score, dtype=np.float64)
    realized = np.asarray(realized_return, dtype=np.float64)
    groups = {
        "strict_full_like": support >= 2.0,
        "reduced_context_like": (support >= 1.0) & (support < 2.0),
        "unknown_or_missing": support < 1.0,
    }
    payload: dict[str, Any] = {}
    for name, mask in groups.items():
        count = int(np.sum(mask))
        accepted = realized[mask]
        payload[name] = {
            "rows": count,
            "mean_realized_return": float(np.mean(accepted)) if accepted.size > 0 else None,
            "severe_loss_rate": float(np.mean(accepted <= -float(severe_loss_ratio))) if accepted.size > 0 else None,
        }
    return payload


def _resolve_row_support_quality_score(*, row: dict[str, Any]) -> float:
    sequence_support = _safe_optional_float(row.get("sequence_support_score"))
    lob_support = _safe_optional_float(row.get("lob_support_score"))
    values = [value for value in (sequence_support, lob_support) if value is not None]
    if not values:
        return 2.0
    return float(min(values))
