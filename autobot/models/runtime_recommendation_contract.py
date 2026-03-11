from __future__ import annotations

from typing import Any

from .research_acceptance import compare_execution_balanced_pareto

_EXIT_MODE_VALUES = {"hold", "risk"}


def resolve_exit_mode_recommendation(
    best_hold_row: dict[str, Any] | None,
    best_risk_row: dict[str, Any] | None,
) -> dict[str, Any]:
    if isinstance(best_hold_row, dict) and isinstance(best_risk_row, dict):
        compare_doc = compare_execution_balanced_pareto(
            best_risk_row.get("summary", {}),
            best_hold_row.get("summary", {}),
        )
        decision = str(compare_doc.get("decision", "")).strip().lower()
        comparable = bool(compare_doc.get("comparable"))
        if comparable and decision == "candidate_edge":
            return {
                "recommended_exit_mode": "risk",
                "recommended_exit_mode_source": "execution_backtest_grid_search_compare",
                "recommended_exit_mode_reason_code": "RISK_EXECUTION_COMPARE_EDGE",
                "exit_mode_compare": dict(compare_doc),
            }
        if comparable and decision == "champion_edge":
            reason_code = "HOLD_EXECUTION_COMPARE_EDGE"
        elif comparable:
            reason_code = "HOLD_EXECUTION_COMPARE_INDETERMINATE"
        else:
            reason_code = "HOLD_EXECUTION_COMPARE_INSUFFICIENT_EVIDENCE"
        return {
            "recommended_exit_mode": "hold",
            "recommended_exit_mode_source": "execution_backtest_grid_search_compare",
            "recommended_exit_mode_reason_code": reason_code,
            "exit_mode_compare": dict(compare_doc),
        }
    if isinstance(best_risk_row, dict):
        return {
            "recommended_exit_mode": "risk",
            "recommended_exit_mode_source": "execution_backtest_grid_search_risk_only",
            "recommended_exit_mode_reason_code": "ONLY_RISK_AVAILABLE",
        }
    if isinstance(best_hold_row, dict):
        return {
            "recommended_exit_mode": "hold",
            "recommended_exit_mode_source": "execution_backtest_grid_search_hold_only",
            "recommended_exit_mode_reason_code": "ONLY_HOLD_AVAILABLE",
        }
    return {
        "recommended_exit_mode": "hold",
        "recommended_exit_mode_source": "manual_fallback",
        "recommended_exit_mode_reason_code": "NO_EXECUTION_EXIT_EVIDENCE",
    }


def normalize_runtime_exit_payload(exit_payload: dict[str, Any] | None) -> dict[str, Any]:
    normalized = dict(exit_payload or {})
    hold_row = _build_exit_row(
        summary=normalized.get("summary"),
        grid_point=normalized.get("grid_point"),
        objective_score=normalized.get("objective_score"),
    )
    risk_row = _build_exit_row(
        summary=normalized.get("risk_summary"),
        grid_point=normalized.get("risk_grid_point"),
        objective_score=normalized.get("risk_objective_score"),
    )
    derived = resolve_exit_mode_recommendation(hold_row, risk_row)
    backfilled_fields: list[str] = []
    for key, value in derived.items():
        if _is_missing_contract_value(normalized.get(key)):
            normalized[key] = value
            backfilled_fields.append(key)
    issues = runtime_exit_contract_issues(normalized)
    normalized["contract_issues"] = list(issues)
    normalized["contract_backfilled_fields"] = list(backfilled_fields)
    if issues:
        normalized["contract_status"] = "invalid"
    elif backfilled_fields:
        normalized["contract_status"] = "backfilled"
    else:
        normalized["contract_status"] = "ok"
    return normalized


def normalize_runtime_recommendations_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    normalized = dict(payload or {})
    exit_payload = normalized.get("exit")
    if isinstance(exit_payload, dict):
        normalized["exit"] = normalize_runtime_exit_payload(exit_payload)
    return normalized


def runtime_exit_contract_issues(exit_payload: dict[str, Any] | None) -> list[str]:
    payload = dict(exit_payload or {})
    issues: list[str] = []
    mode = str(payload.get("recommended_exit_mode") or "").strip().lower()
    source = str(payload.get("recommended_exit_mode_source") or "").strip()
    reason_code = str(payload.get("recommended_exit_mode_reason_code") or "").strip()
    hold_summary = payload.get("summary")
    risk_summary = payload.get("risk_summary")
    has_hold_summary = isinstance(hold_summary, dict) and bool(hold_summary)
    has_risk_summary = isinstance(risk_summary, dict) and bool(risk_summary)
    has_exit_candidates = any(
        [
            payload.get("recommended_hold_bars") not in (None, ""),
            isinstance(payload.get("grid_point"), dict) and bool(payload.get("grid_point")),
            isinstance(payload.get("risk_grid_point"), dict) and bool(payload.get("risk_grid_point")),
            payload.get("objective_score") not in (None, ""),
            payload.get("risk_objective_score") not in (None, ""),
            payload.get("recommended_risk_scaling_mode") not in (None, ""),
            payload.get("recommended_risk_vol_feature") not in (None, ""),
            payload.get("recommended_tp_vol_multiplier") not in (None, ""),
            payload.get("recommended_sl_vol_multiplier") not in (None, ""),
            payload.get("recommended_trailing_vol_multiplier") not in (None, ""),
        ]
    )
    compare_doc = payload.get("exit_mode_compare")
    if mode not in _EXIT_MODE_VALUES:
        issues.append("RECOMMENDED_EXIT_MODE_MISSING")
    if not source:
        issues.append("RECOMMENDED_EXIT_MODE_SOURCE_MISSING")
    if not reason_code:
        issues.append("RECOMMENDED_EXIT_MODE_REASON_CODE_MISSING")
    if has_exit_candidates and not (has_hold_summary or has_risk_summary):
        issues.append("EXIT_RECOMMENDATION_EVIDENCE_MISSING")
    if has_hold_summary and has_risk_summary:
        if not isinstance(compare_doc, dict) or not str(compare_doc.get("decision") or "").strip():
            issues.append("EXIT_MODE_COMPARE_MISSING")
    return issues


def _build_exit_row(
    *,
    summary: Any,
    grid_point: Any,
    objective_score: Any,
) -> dict[str, Any] | None:
    if not isinstance(summary, dict) or not summary:
        return None
    row: dict[str, Any] = {"summary": dict(summary)}
    if isinstance(grid_point, dict) and grid_point:
        row["grid_point"] = dict(grid_point)
    utility_total = _safe_optional_float(objective_score)
    if utility_total is not None:
        row["utility_total"] = float(utility_total)
    return row


def _is_missing_contract_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, dict):
        return not value
    if isinstance(value, list):
        return not value
    return False


def _safe_optional_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
