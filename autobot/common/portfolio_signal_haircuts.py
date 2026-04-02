from __future__ import annotations

from typing import Any


def resolve_portfolio_signal_haircuts(
    *,
    uncertainty: float | None = None,
    expected_return_bps: float | None = None,
    expected_es_bps: float | None = None,
    tradability_prob: float | None = None,
    alpha_lcb_bps: float | None = None,
) -> dict[str, Any]:
    confidence_haircut = _confidence_haircut(uncertainty)
    alpha_strength_haircut, alpha_reason_codes = _alpha_strength_haircut(
        expected_return_bps=expected_return_bps,
        alpha_lcb_bps=alpha_lcb_bps,
    )
    expected_es_haircut, expected_es_reason_codes = _expected_es_haircut(
        expected_return_bps=expected_return_bps,
        expected_es_bps=expected_es_bps,
    )
    tradability_haircut, tradability_reason_codes = _tradability_haircut(tradability_prob)
    combined_haircut = min(
        float(confidence_haircut),
        float(alpha_strength_haircut),
        float(expected_es_haircut),
        float(tradability_haircut),
    )
    reason_codes: list[str] = []
    for code in alpha_reason_codes + expected_es_reason_codes + tradability_reason_codes:
        if code not in reason_codes:
            reason_codes.append(code)
    if float(confidence_haircut) < 0.999999 and "PORTFOLIO_CONFIDENCE_HAIRCUT" not in reason_codes:
        reason_codes.append("PORTFOLIO_CONFIDENCE_HAIRCUT")
    return {
        "confidence_haircut": float(confidence_haircut),
        "alpha_strength_haircut": float(alpha_strength_haircut),
        "expected_es_haircut": float(expected_es_haircut),
        "tradability_haircut": float(tradability_haircut),
        "combined_haircut": float(combined_haircut),
        "reason_codes": reason_codes,
    }


def _confidence_haircut(uncertainty: float | None) -> float:
    resolved = _safe_optional_float(uncertainty)
    if resolved is None:
        return 1.0
    return max(min(1.0 / (1.0 + abs(float(resolved))), 1.0), 0.25)


def _alpha_strength_haircut(
    *,
    expected_return_bps: float | None,
    alpha_lcb_bps: float | None,
) -> tuple[float, list[str]]:
    reasons: list[str] = []
    lcb = _safe_optional_float(alpha_lcb_bps)
    edge = _safe_optional_float(expected_return_bps)
    if lcb is not None:
        if float(lcb) <= 0.0:
            reasons.append("PORTFOLIO_ALPHA_LCB_HAIRCUT")
            return 0.50, reasons
        if float(lcb) < 10.0:
            reasons.append("PORTFOLIO_ALPHA_LCB_HAIRCUT")
            return 0.75, reasons
        return 1.0, reasons
    if edge is not None and float(edge) < 10.0:
        reasons.append("PORTFOLIO_LOW_EXPECTED_RETURN_HAIRCUT")
        return 0.85, reasons
    return 1.0, reasons


def _expected_es_haircut(
    *,
    expected_return_bps: float | None,
    expected_es_bps: float | None,
) -> tuple[float, list[str]]:
    reasons: list[str] = []
    expected_es_value = _safe_optional_float(expected_es_bps)
    expected_return_value = _safe_optional_float(expected_return_bps)
    if expected_es_value is None or expected_es_value <= 0.0:
        return 1.0, reasons
    if expected_return_value is None or expected_return_value <= 0.0:
        reasons.append("PORTFOLIO_EXPECTED_ES_HAIRCUT")
        return 0.50, reasons
    loss_ratio = float(expected_es_value) / max(float(expected_return_value), 1e-12)
    if loss_ratio >= 2.0:
        reasons.append("PORTFOLIO_EXPECTED_ES_HAIRCUT")
        return 0.50, reasons
    if loss_ratio >= 1.0:
        reasons.append("PORTFOLIO_EXPECTED_ES_HAIRCUT")
        return 0.75, reasons
    return 1.0, reasons


def _tradability_haircut(tradability_prob: float | None) -> tuple[float, list[str]]:
    reasons: list[str] = []
    resolved = _safe_optional_float(tradability_prob)
    if resolved is None:
        return 1.0, reasons
    value = max(min(float(resolved), 1.0), 0.0)
    if value < 0.25:
        reasons.append("PORTFOLIO_TRADABILITY_HAIRCUT")
        return 0.25, reasons
    if value < 0.50:
        reasons.append("PORTFOLIO_TRADABILITY_HAIRCUT")
        return 0.50, reasons
    if value < 0.75:
        reasons.append("PORTFOLIO_TRADABILITY_HAIRCUT")
        return 0.75, reasons
    return 1.0, reasons


def _safe_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
