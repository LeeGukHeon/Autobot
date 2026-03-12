"""Governance and promotion helpers for trainer=v4_crypto_cs."""

from __future__ import annotations

from typing import Any

from .registry import load_json


def build_research_support_lane_v4(
    *,
    walk_forward: dict[str, Any],
    cpcv_lite: dict[str, Any],
) -> dict[str, Any]:
    walk_summary = dict((walk_forward or {}).get("summary") or {})
    panel_diagnostics = dict((walk_forward or {}).get("multiple_testing_panel_diagnostics") or {})
    spa_like_doc = dict((walk_forward or {}).get("spa_like_window_test") or {})
    white_rc_doc = dict((walk_forward or {}).get("white_reality_check") or {})
    hansen_spa_doc = dict((walk_forward or {}).get("hansen_spa") or {})
    cpcv_summary = dict((cpcv_lite or {}).get("summary") or {})

    def _reason_list(*sources: Any) -> list[str]:
        reasons: list[str] = []
        for source in sources:
            for item in source or []:
                text = str(item).strip()
                if text and text not in reasons:
                    reasons.append(text)
        return reasons

    def _support_status(doc: dict[str, Any]) -> str:
        if not doc:
            return "missing"
        return "supported" if bool(doc.get("comparable", False)) else "insufficient"

    spa_like_reasons = _reason_list(spa_like_doc.get("reasons"))
    white_rc_reasons = _reason_list(white_rc_doc.get("reasons"), (white_rc_doc.get("panel_diagnostics") or {}).get("reasons"))
    hansen_spa_reasons = _reason_list(
        hansen_spa_doc.get("reasons"),
        (hansen_spa_doc.get("panel_diagnostics") or {}).get("reasons"),
    )
    cpcv_reasons = _reason_list(
        cpcv_summary.get("reasons"),
        (cpcv_lite or {}).get("insufficiency_reasons"),
        [str(cpcv_summary.get("budget_reason", "")).strip()] if str(cpcv_summary.get("budget_reason", "")).strip() else [],
    )

    spa_like_status = _support_status(spa_like_doc)
    white_rc_status = _support_status(white_rc_doc)
    hansen_spa_status = _support_status(hansen_spa_doc)
    cpcv_status_raw = str(cpcv_summary.get("status", "")).strip().lower()
    if not cpcv_status_raw:
        cpcv_status_raw = "disabled" if not bool((cpcv_lite or {}).get("enabled", False)) else "unknown"
    if cpcv_status_raw == "trusted":
        cpcv_support_status = "supported"
    elif cpcv_status_raw in {"partial", "default"}:
        cpcv_support_status = "partial"
    elif cpcv_status_raw == "disabled":
        cpcv_support_status = "disabled"
    else:
        cpcv_support_status = "insufficient"

    windows_run = int(walk_summary.get("windows_run", 0) or 0)
    comparable_components = (
        spa_like_status == "supported"
        and white_rc_status == "supported"
        and hansen_spa_status == "supported"
    )
    cpcv_usable = cpcv_support_status in {"supported", "partial"}
    any_support_evidence = bool(
        windows_run > 0
        or panel_diagnostics
        or spa_like_doc
        or white_rc_doc
        or hansen_spa_doc
        or cpcv_lite
    )
    if comparable_components and cpcv_usable:
        status = "supported"
    elif any_support_evidence and (
        spa_like_status == "supported" or white_rc_status == "supported" or hansen_spa_status == "supported" or cpcv_usable
    ):
        status = "partial"
    elif cpcv_support_status == "disabled" and windows_run > 0:
        status = "partial"
    else:
        status = "insufficient"

    summary_reasons = _reason_list(
        ["NO_WALK_FORWARD_EVIDENCE"] if windows_run <= 0 else [],
        panel_diagnostics.get("reasons"),
        spa_like_reasons,
        white_rc_reasons,
        hansen_spa_reasons,
        cpcv_reasons,
        ["CPCV_LITE_DISABLED"] if cpcv_support_status == "disabled" else [],
    )
    if status == "supported" and not summary_reasons:
        summary_reasons = ["SUPPORT_LANE_AVAILABLE"]

    return {
        "version": 1,
        "policy": "v4_certification_support_lane_v1",
        "source": "train_v4_crypto_cs",
        "support_only": True,
        "summary": {
            "status": status,
            "windows_run": windows_run,
            "multiple_testing_supported": bool(comparable_components),
            "cpcv_lite_status": cpcv_status_raw,
            "reasons": summary_reasons,
        },
        "multiple_testing_panel_diagnostics": panel_diagnostics,
        "spa_like": {
            "policy": str(spa_like_doc.get("policy", "")).strip(),
            "decision": str(spa_like_doc.get("decision", "")).strip(),
            "comparable": bool(spa_like_doc.get("comparable", False)),
            "status": spa_like_status,
            "reasons": spa_like_reasons,
            "window_count": int(spa_like_doc.get("window_count", 0) or 0),
        },
        "white_rc": {
            "policy": str(white_rc_doc.get("policy", "")).strip(),
            "decision": str(white_rc_doc.get("decision", "")).strip(),
            "comparable": bool(white_rc_doc.get("comparable", False)),
            "status": white_rc_status,
            "reasons": white_rc_reasons,
            "panel_diagnostics": dict(white_rc_doc.get("panel_diagnostics") or {}),
        },
        "hansen_spa": {
            "policy": str(hansen_spa_doc.get("policy", "")).strip(),
            "decision": str(hansen_spa_doc.get("decision", "")).strip(),
            "comparable": bool(hansen_spa_doc.get("comparable", False)),
            "status": hansen_spa_status,
            "reasons": hansen_spa_reasons,
            "panel_diagnostics": dict(hansen_spa_doc.get("panel_diagnostics") or {}),
        },
        "cpcv_lite": {
            "enabled": bool((cpcv_lite or {}).get("enabled", False)),
            "trigger": str((cpcv_lite or {}).get("trigger", "")).strip() or "disabled",
            "status": cpcv_status_raw,
            "support_status": cpcv_support_status,
            "summary": cpcv_summary,
            "insufficiency_reasons": cpcv_reasons,
            "pbo": dict((cpcv_lite or {}).get("pbo") or {}),
            "dsr": dict((cpcv_lite or {}).get("dsr") or {}),
        },
    }


def build_trainer_research_evidence_from_promotion_v4(
    *,
    promotion: dict[str, Any],
    support_lane: dict[str, Any] | None = None,
) -> dict[str, Any]:
    checks = dict((promotion or {}).get("checks") or {})
    research = dict((promotion or {}).get("research_acceptance") or {})
    offline_compare = dict(research.get("compare_to_champion") or {})
    spa_like_doc = dict(research.get("spa_like_window_test") or {})
    white_rc_doc = dict(research.get("white_reality_check") or {})
    hansen_spa_doc = dict(research.get("hansen_spa") or {})
    walk_summary = dict(research.get("walk_forward_summary") or {})
    execution_doc = dict((promotion or {}).get("execution_acceptance") or {})
    execution_compare = dict(execution_doc.get("compare_to_champion") or {})

    existing_champion_present = bool(checks.get("existing_champion_present", False))
    walk_forward_present = bool(checks.get("walk_forward_present", False))
    walk_forward_windows_run = int(checks.get("walk_forward_windows_run", 0) or 0)
    offline_comparable = bool(checks.get("balanced_pareto_comparable", False))
    offline_candidate_edge = bool(checks.get("balanced_pareto_candidate_edge", False))
    spa_like_present = bool(checks.get("spa_like_present", False))
    spa_like_comparable = bool(checks.get("spa_like_comparable", False))
    spa_like_candidate_edge = bool(checks.get("spa_like_candidate_edge", False))
    white_rc_present = bool(checks.get("white_rc_present", False))
    white_rc_comparable = bool(checks.get("white_rc_comparable", False))
    white_rc_candidate_edge = bool(checks.get("white_rc_candidate_edge", False))
    hansen_spa_present = bool(checks.get("hansen_spa_present", False))
    hansen_spa_comparable = bool(checks.get("hansen_spa_comparable", False))
    hansen_spa_candidate_edge = bool(checks.get("hansen_spa_candidate_edge", False))
    execution_enabled = bool(checks.get("execution_acceptance_enabled", False))
    execution_present = bool(checks.get("execution_acceptance_present", False))
    execution_comparable = bool(checks.get("execution_balanced_pareto_comparable", False))
    execution_candidate_edge = bool(checks.get("execution_balanced_pareto_candidate_edge", False))

    offline_decision = str(offline_compare.get("decision", "")).strip()
    spa_like_decision = str(spa_like_doc.get("decision", "")).strip()
    white_rc_decision = str(white_rc_doc.get("decision", "")).strip()
    hansen_spa_decision = str(hansen_spa_doc.get("decision", "")).strip()
    execution_status = str(execution_doc.get("status", "")).strip()
    execution_decision = str(execution_compare.get("decision", "")).strip()

    reasons: list[str] = []
    if not walk_forward_present:
        reasons.append("NO_WALK_FORWARD_EVIDENCE")
    elif existing_champion_present:
        if not offline_comparable:
            reasons.append("OFFLINE_NOT_COMPARABLE")
        elif not offline_candidate_edge:
            reasons.append("OFFLINE_NOT_CANDIDATE_EDGE")
        if spa_like_present:
            if not spa_like_comparable:
                reasons.append("SPA_LIKE_NOT_COMPARABLE")
            elif not spa_like_candidate_edge:
                reasons.append("SPA_LIKE_NOT_CANDIDATE_EDGE")
        if white_rc_present:
            if not white_rc_comparable:
                reasons.append("WHITE_RC_NOT_COMPARABLE")
            elif not white_rc_candidate_edge:
                reasons.append("WHITE_RC_NOT_CANDIDATE_EDGE")
        if hansen_spa_present:
            if not hansen_spa_comparable:
                reasons.append("HANSEN_SPA_NOT_COMPARABLE")
            elif not hansen_spa_candidate_edge:
                reasons.append("HANSEN_SPA_NOT_CANDIDATE_EDGE")

    offline_pass = walk_forward_present and (
        (not existing_champion_present)
        or (
            offline_comparable
            and offline_candidate_edge
            and ((not spa_like_present) or (spa_like_comparable and spa_like_candidate_edge))
            and ((not white_rc_present) or (white_rc_comparable and white_rc_candidate_edge))
            and ((not hansen_spa_present) or (hansen_spa_comparable and hansen_spa_candidate_edge))
        )
    )
    execution_pass = True
    if execution_enabled:
        if not execution_present:
            execution_pass = False
            reasons.append("NO_EXECUTION_EVIDENCE")
        elif existing_champion_present:
            if not execution_comparable:
                execution_pass = False
                reasons.append("EXECUTION_NOT_COMPARABLE")
            elif not execution_candidate_edge:
                execution_pass = False
                reasons.append("EXECUTION_NOT_CANDIDATE_EDGE")

    available = walk_forward_present or execution_present or bool(offline_decision) or bool(execution_decision)
    passed = offline_pass and execution_pass
    if available and not reasons:
        reasons = ["TRAINER_EVIDENCE_PASS"]

    return {
        "version": 1,
        "policy": "v4_trainer_research_evidence_v1",
        "source": "train_v4_crypto_cs",
        "available": available,
        "pass": passed,
        "offline_pass": offline_pass,
        "execution_pass": execution_pass,
        "reasons": reasons,
        "checks": {
            "existing_champion_present": existing_champion_present,
            "walk_forward_present": walk_forward_present,
            "walk_forward_windows_run": walk_forward_windows_run,
            "offline_comparable": offline_comparable,
            "offline_candidate_edge": offline_candidate_edge,
            "spa_like_present": spa_like_present,
            "spa_like_comparable": spa_like_comparable,
            "spa_like_candidate_edge": spa_like_candidate_edge,
            "white_rc_present": white_rc_present,
            "white_rc_comparable": white_rc_comparable,
            "white_rc_candidate_edge": white_rc_candidate_edge,
            "hansen_spa_present": hansen_spa_present,
            "hansen_spa_comparable": hansen_spa_comparable,
            "hansen_spa_candidate_edge": hansen_spa_candidate_edge,
            "execution_acceptance_enabled": execution_enabled,
            "execution_acceptance_present": execution_present,
            "execution_comparable": execution_comparable,
            "execution_candidate_edge": execution_candidate_edge,
        },
        "offline": {
            "policy": str(research.get("policy", "")).strip(),
            "decision": offline_decision,
            "comparable": offline_comparable,
        },
        "spa_like": {
            "policy": str(spa_like_doc.get("policy", "")).strip(),
            "decision": spa_like_decision,
            "comparable": spa_like_comparable,
        },
        "white_rc": {
            "policy": str(white_rc_doc.get("policy", "")).strip(),
            "decision": white_rc_decision,
            "comparable": white_rc_comparable,
        },
        "hansen_spa": {
            "policy": str(hansen_spa_doc.get("policy", "")).strip(),
            "decision": hansen_spa_decision,
            "comparable": hansen_spa_comparable,
        },
        "execution": {
            "status": execution_status,
            "policy": str(execution_compare.get("policy", "")).strip(),
            "decision": execution_decision,
            "comparable": execution_comparable,
        },
        "support_lane": dict(support_lane or {}),
    }


def manual_promotion_decision_v4(
    *,
    options: Any,
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


def build_duplicate_candidate_promotion_decision_v4(
    *,
    options: Any,
    run_id: str,
    walk_forward: dict[str, Any],
    execution_acceptance: dict[str, Any],
    duplicate_artifacts: dict[str, Any],
) -> dict[str, Any]:
    walk_summary = walk_forward.get("summary", {}) if isinstance(walk_forward, dict) else {}
    research_compare = walk_forward.get("compare_to_champion", {}) if isinstance(walk_forward, dict) else {}
    spa_like_doc = walk_forward.get("spa_like_window_test", {}) if isinstance(walk_forward, dict) else {}
    white_rc_doc = walk_forward.get("white_reality_check", {}) if isinstance(walk_forward, dict) else {}
    hansen_spa_doc = walk_forward.get("hansen_spa", {}) if isinstance(walk_forward, dict) else {}
    champion_ref = str(duplicate_artifacts.get("champion_ref", "")).strip()
    return {
        "run_id": run_id,
        "promote": False,
        "status": "candidate",
        "promotion_mode": "duplicate_candidate_short_circuit",
        "reasons": ["DUPLICATE_CANDIDATE"],
        "checks": {
            "manual_review_required": False,
            "existing_champion_present": bool(champion_ref),
            "walk_forward_present": bool(int(walk_summary.get("windows_run", 0) or 0) > 0),
            "walk_forward_windows_run": int(walk_summary.get("windows_run", 0) or 0),
            "balanced_pareto_comparable": bool(research_compare.get("comparable", False)),
            "balanced_pareto_candidate_edge": False,
            "spa_like_present": bool(spa_like_doc),
            "spa_like_comparable": bool(spa_like_doc.get("comparable", False)),
            "spa_like_candidate_edge": False,
            "white_rc_present": bool(white_rc_doc),
            "white_rc_comparable": bool(white_rc_doc.get("comparable", False)),
            "white_rc_candidate_edge": False,
            "hansen_spa_present": bool(hansen_spa_doc),
            "hansen_spa_comparable": bool(hansen_spa_doc.get("comparable", False)),
            "hansen_spa_candidate_edge": False,
            "execution_acceptance_enabled": bool(options.execution_acceptance_enabled),
            "execution_acceptance_present": False,
            "execution_balanced_pareto_comparable": False,
            "execution_balanced_pareto_candidate_edge": False,
            "duplicate_candidate": True,
        },
        "research_acceptance": {
            "policy": str(research_compare.get("policy", "balanced_pareto_offline")),
            "walk_forward_summary": walk_summary,
            "compare_to_champion": research_compare,
            "spa_like_window_test": spa_like_doc,
            "white_reality_check": white_rc_doc,
            "hansen_spa": hansen_spa_doc,
        },
        "execution_acceptance": execution_acceptance,
        "candidate_ref": {
            "model_ref": "latest_candidate",
            "model_family": options.model_family,
        },
        "duplicate_artifacts": duplicate_artifacts,
    }
