from __future__ import annotations

from typing import Any

from .execution_risk_control import normalize_execution_risk_control_payload
from .research_acceptance import compare_execution_balanced_pareto

_RUNTIME_RECOMMENDATIONS_VERSION = 1
_RUNTIME_EXIT_CONTRACT_VERSION = 1
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
    backfilled_fields: list[str] = []
    normalized["version"], version_backfilled = _normalize_contract_version(
        normalized.get("version"),
        fallback_version=_RUNTIME_EXIT_CONTRACT_VERSION,
    )
    if version_backfilled:
        backfilled_fields.append("version")
    hold_family = normalized.get("hold_family")
    if not isinstance(hold_family, dict) or not hold_family:
        hold_family = _build_legacy_exit_family_doc(
            family="hold",
            summary=normalized.get("summary"),
            grid_point=normalized.get("grid_point"),
            objective_score=normalized.get("objective_score"),
        )
        if hold_family:
            normalized["hold_family"] = hold_family
    risk_family = normalized.get("risk_family")
    if not isinstance(risk_family, dict) or not risk_family:
        risk_family = _build_legacy_exit_family_doc(
            family="risk",
            summary=normalized.get("risk_summary"),
            grid_point=normalized.get("risk_grid_point"),
            objective_score=normalized.get("risk_objective_score"),
        )
        if risk_family:
            normalized["risk_family"] = risk_family
    family_compare = normalized.get("family_compare")
    family_compare_status = (
        str((family_compare or {}).get("status", "")).strip().lower() if isinstance(family_compare, dict) else ""
    )
    family_compare_supported = (
        bool((family_compare or {}).get("comparable", False)) if isinstance(family_compare, dict) else False
    )
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
    if family_compare_status and (family_compare_status != "supported" or not family_compare_supported):
        derived = {
            "recommended_exit_mode": "",
            "recommended_exit_mode_source": "execution_backtest_family_compare",
            "recommended_exit_mode_reason_code": "EXIT_FAMILY_INSUFFICIENT_EVIDENCE",
            "exit_mode_compare": {
                "decision": str((family_compare or {}).get("decision", "")).strip() or "not_comparable",
                "comparable": False,
                "reasons": list((family_compare or {}).get("reason_codes") or []),
            },
        }
    else:
        derived = resolve_exit_mode_recommendation(hold_row, risk_row)
    for key, value in derived.items():
        if _is_missing_contract_value(normalized.get(key)):
            normalized[key] = value
            backfilled_fields.append(key)
    family_compare = normalized.get("family_compare")
    if not isinstance(family_compare, dict) or not family_compare:
        family_compare = _build_legacy_family_compare_doc(
            hold_family=normalized.get("hold_family"),
            risk_family=normalized.get("risk_family"),
            derived=derived,
        )
        if family_compare:
            normalized["family_compare"] = family_compare
            backfilled_fields.append("family_compare")
    if _is_missing_contract_value(normalized.get("hold_family_status")) and isinstance(normalized.get("hold_family"), dict):
        normalized["hold_family_status"] = str((normalized.get("hold_family") or {}).get("status", "")).strip()
        backfilled_fields.append("hold_family_status")
    if _is_missing_contract_value(normalized.get("risk_family_status")) and isinstance(normalized.get("risk_family"), dict):
        normalized["risk_family_status"] = str((normalized.get("risk_family") or {}).get("status", "")).strip()
        backfilled_fields.append("risk_family_status")
    if _is_missing_contract_value(normalized.get("family_compare_status")) and isinstance(normalized.get("family_compare"), dict):
        normalized["family_compare_status"] = str((normalized.get("family_compare") or {}).get("status", "")).strip()
        backfilled_fields.append("family_compare_status")
    if _is_missing_contract_value(normalized.get("chosen_family")) and isinstance(normalized.get("family_compare"), dict):
        chosen_mode = str(derived.get("recommended_exit_mode", "")).strip().lower()
        normalized["chosen_family"] = chosen_mode if chosen_mode in _EXIT_MODE_VALUES else ""
        backfilled_fields.append("chosen_family")
    if _is_missing_contract_value(normalized.get("chosen_rule_id")):
        chosen_mode = str(normalized.get("chosen_family") or derived.get("recommended_exit_mode") or "").strip().lower()
        if chosen_mode == "risk":
            normalized["chosen_rule_id"] = str(((normalized.get("risk_family") or {}).get("best_comparable_rule_id")) or "").strip()
        elif chosen_mode == "hold":
            normalized["chosen_rule_id"] = str(((normalized.get("hold_family") or {}).get("best_comparable_rule_id")) or "").strip()
        if normalized.get("chosen_rule_id"):
            backfilled_fields.append("chosen_rule_id")
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
    backfilled_fields: list[str] = []
    issues: list[str] = []
    source_family = str(normalized.get("source_family") or "").strip().lower()
    decision_contract_version = str(normalized.get("decision_contract_version") or "").strip()
    normalized["version"], version_backfilled = _normalize_contract_version(
        normalized.get("version"),
        fallback_version=_RUNTIME_RECOMMENDATIONS_VERSION,
    )
    if version_backfilled:
        backfilled_fields.append("version")
    if int(normalized["version"]) != _RUNTIME_RECOMMENDATIONS_VERSION:
        issues.append("RUNTIME_RECOMMENDATIONS_VERSION_UNSUPPORTED")
    exit_payload = normalized.get("exit")
    if isinstance(exit_payload, dict):
        normalized["exit"] = normalize_runtime_exit_payload(exit_payload)
        exit_contract_status = str((normalized["exit"] or {}).get("contract_status", "")).strip().lower()
        if exit_contract_status == "backfilled":
            backfilled_fields.append("exit")
        elif exit_contract_status == "invalid":
            issues.append("EXIT_CONTRACT_INVALID")
    risk_control_payload = normalized.get("risk_control")
    if isinstance(risk_control_payload, dict):
        normalized["risk_control"] = normalize_execution_risk_control_payload(risk_control_payload)
        risk_control_status = str((normalized["risk_control"] or {}).get("contract_status", "")).strip().lower()
        if risk_control_status == "backfilled":
            backfilled_fields.append("risk_control")
        elif risk_control_status == "invalid":
            issues.append("RISK_CONTROL_CONTRACT_INVALID")
    if source_family == "train_v5_fusion" and decision_contract_version == "v5_post_model_contract_v1":
        top_level_doc_issues = {
            "exit": "RUNTIME_RECOMMENDATIONS_EXIT_DOC_MISSING",
            "execution": "RUNTIME_RECOMMENDATIONS_EXECUTION_DOC_MISSING",
            "risk_control": "RUNTIME_RECOMMENDATIONS_RISK_CONTROL_DOC_MISSING",
            "trade_action": "RUNTIME_RECOMMENDATIONS_TRADE_ACTION_DOC_MISSING",
        }
        for field_name, issue_code in top_level_doc_issues.items():
            field_payload = normalized.get(field_name)
            if not isinstance(field_payload, dict) or not field_payload:
                issues.append(issue_code)
        provenance_fields = {
            "sequence_variant_name": "RUNTIME_RECOMMENDATIONS_SEQUENCE_VARIANT_NAME_MISSING",
            "lob_variant_name": "RUNTIME_RECOMMENDATIONS_LOB_VARIANT_NAME_MISSING",
            "sequence_backbone_name": "RUNTIME_RECOMMENDATIONS_SEQUENCE_BACKBONE_NAME_MISSING",
            "sequence_pretrain_method": "RUNTIME_RECOMMENDATIONS_SEQUENCE_PRETRAIN_METHOD_MISSING",
            "sequence_pretrain_status": "RUNTIME_RECOMMENDATIONS_SEQUENCE_PRETRAIN_STATUS_MISSING",
            "sequence_pretrain_objective": "RUNTIME_RECOMMENDATIONS_SEQUENCE_PRETRAIN_OBJECTIVE_MISSING",
            "lob_backbone_name": "RUNTIME_RECOMMENDATIONS_LOB_BACKBONE_NAME_MISSING",
            "tradability_source_run_id": "RUNTIME_RECOMMENDATIONS_TRADABILITY_SOURCE_RUN_ID_MISSING",
            "domain_weighting_policy": "RUNTIME_RECOMMENDATIONS_DOMAIN_WEIGHTING_POLICY_MISSING",
            "domain_weighting_source_kind": "RUNTIME_RECOMMENDATIONS_DOMAIN_WEIGHTING_SOURCE_KIND_MISSING",
        }
        for field_name, issue_code in provenance_fields.items():
            if not str(normalized.get(field_name) or "").strip():
                issues.append(issue_code)
        if not bool(normalized.get("runtime_deploy_contract_ready", False)):
            issues.append("RUNTIME_RECOMMENDATIONS_DEPLOY_CONTRACT_NOT_READY")
    normalized["contract_backfilled_fields"] = list(dict.fromkeys(backfilled_fields))
    normalized["contract_issues"] = list(dict.fromkeys(issues))
    if normalized["contract_issues"]:
        normalized["contract_status"] = "invalid"
    elif normalized["contract_backfilled_fields"]:
        normalized["contract_status"] = "backfilled"
    else:
        normalized["contract_status"] = "ok"
    return normalized


def runtime_exit_contract_issues(exit_payload: dict[str, Any] | None) -> list[str]:
    payload = dict(exit_payload or {})
    issues: list[str] = []
    version = _safe_optional_int(payload.get("version"))
    if version is None:
        issues.append("EXIT_CONTRACT_VERSION_MISSING")
    elif version != _RUNTIME_EXIT_CONTRACT_VERSION:
        issues.append("EXIT_CONTRACT_VERSION_UNSUPPORTED")
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
    hold_family = payload.get("hold_family")
    risk_family = payload.get("risk_family")
    family_compare = payload.get("family_compare")
    family_compare_status = str((family_compare or {}).get("status", "")).strip().lower() if isinstance(family_compare, dict) else ""
    if mode not in _EXIT_MODE_VALUES and family_compare_status == "supported":
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
    if (has_hold_summary or has_risk_summary) and not isinstance(hold_family, dict):
        issues.append("EXIT_HOLD_FAMILY_MISSING")
    if (has_hold_summary or has_risk_summary) and not isinstance(risk_family, dict):
        issues.append("EXIT_RISK_FAMILY_MISSING")
    if isinstance(hold_family, dict) and isinstance(risk_family, dict):
        if not isinstance(family_compare, dict) or not str(family_compare.get("status") or "").strip():
            issues.append("EXIT_FAMILY_COMPARE_MISSING")
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


def _build_legacy_exit_family_doc(
    *,
    family: str,
    summary: Any,
    grid_point: Any,
    objective_score: Any,
) -> dict[str, Any]:
    row = _build_exit_row(summary=summary, grid_point=grid_point, objective_score=objective_score)
    if not isinstance(row, dict):
        return {}
    family_name = str(family).strip().lower() or "hold"
    rule_id = _legacy_rule_id(family_name, row)
    compact_row = {
        "rule_id": rule_id,
        "kind": "risk_exit" if family_name == "risk" else "hold",
        "grid_point": dict(row.get("grid_point", {})),
        "utility_total": float(row.get("utility_total", 0.0) or 0.0),
        "objective_score": float(row.get("utility_total", 0.0) or 0.0),
        "summary": dict(row.get("summary", {})),
    }
    return {
        "family": family_name,
        "status": "legacy_backfilled",
        "rows_total": 1,
        "comparable_rows": 1,
        "reason_codes": ["LEGACY_SINGLE_RULE_BACKFILL"],
        "best_rule_id": rule_id,
        "best_comparable_rule_id": rule_id,
        "best_rule": compact_row,
        "best_comparable_rule": compact_row,
        "top_rules": [compact_row],
    }


def _build_legacy_family_compare_doc(
    *,
    hold_family: Any,
    risk_family: Any,
    derived: dict[str, Any],
) -> dict[str, Any]:
    hold_payload = dict(hold_family or {}) if isinstance(hold_family, dict) else {}
    risk_payload = dict(risk_family or {}) if isinstance(risk_family, dict) else {}
    compare_doc = dict(derived.get("exit_mode_compare") or {})
    status = "supported" if bool(compare_doc.get("comparable", False)) else "legacy_backfilled"
    return {
        "status": status,
        "decision": str(compare_doc.get("decision", "")).strip() or "legacy_backfilled",
        "comparable": bool(compare_doc.get("comparable", False)),
        "reason_codes": [str(item).strip() for item in (compare_doc.get("reasons") or []) if str(item).strip()]
        or ["LEGACY_SINGLE_RULE_COMPARE_BACKFILL"],
        "recommended_exit_mode": str(derived.get("recommended_exit_mode", "")).strip(),
        "hold_rule_id": str(hold_payload.get("best_comparable_rule_id", "")).strip(),
        "risk_rule_id": str(risk_payload.get("best_comparable_rule_id", "")).strip(),
    }


def _legacy_rule_id(family: str, row: dict[str, Any]) -> str:
    grid_point = dict(row.get("grid_point", {}))
    if family == "risk":
        return (
            f"risk_h{int(grid_point.get('hold_bars', 0) or 0)}"
            f"_{str(grid_point.get('risk_vol_feature', '')).strip().lower() or 'legacy'}"
        )
    return f"hold_h{int(grid_point.get('hold_bars', 0) or 0)}"


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


def _safe_optional_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalize_contract_version(value: Any, *, fallback_version: int) -> tuple[int, bool]:
    resolved = _safe_optional_int(value)
    if resolved is None:
        return int(fallback_version), True
    return int(resolved), False
