"""Live risk budget ledger helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from autobot.risk.portfolio_budget import summarize_portfolio_exposure


RISK_BUDGET_LEDGER_VERSION = 1


def initialize_live_risk_budget_ledger(
    *,
    ledger_path: Path,
    latest_path: Path,
    lane: str,
    unit_name: str,
    rollout_mode: str,
) -> None:
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    if not ledger_path.exists():
        ledger_path.write_text("", encoding="utf-8")
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_payload = _load_latest_summary(latest_path=latest_path)
    if latest_payload is None:
        latest_payload = _rebuild_latest_summary_from_ledger(
            ledger_path=ledger_path,
            lane=lane,
            unit_name=unit_name,
            rollout_mode=rollout_mode,
        )
    latest_payload["artifact_version"] = RISK_BUDGET_LEDGER_VERSION
    latest_payload["lane"] = str(lane).strip()
    latest_payload["unit_name"] = str(unit_name).strip()
    latest_payload["rollout_mode"] = str(rollout_mode).strip().lower()
    latest_payload["latest_jsonl_path"] = str(ledger_path)
    latest_path.write_text(json.dumps(latest_payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def append_live_risk_budget_entry(
    *,
    ledger_path: Path,
    latest_path: Path,
    store: Any,
    lane: str,
    unit_name: str,
    rollout_mode: str,
    market: str,
    side: str,
    status: str,
    reason_code: str,
    meta_payload: dict[str, Any],
    ts_ms: int,
    intent_id: str | None,
    base_budget_quote: float | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    entry = build_live_risk_budget_entry(
        store=store,
        lane=lane,
        unit_name=unit_name,
        rollout_mode=rollout_mode,
        market=market,
        side=side,
        status=status,
        reason_code=reason_code,
        meta_payload=meta_payload,
        ts_ms=ts_ms,
        intent_id=intent_id,
        base_budget_quote=base_budget_quote,
    )
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    with ledger_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, ensure_ascii=False, separators=(",", ":")) + "\n")
    latest_payload = _update_latest_summary(
        latest_path=latest_path,
        lane=lane,
        unit_name=unit_name,
        rollout_mode=rollout_mode,
        ledger_path=ledger_path,
        entry=entry,
    )
    return entry, latest_payload


def build_live_risk_budget_entry(
    *,
    store: Any,
    lane: str,
    unit_name: str,
    rollout_mode: str,
    market: str,
    side: str,
    status: str,
    reason_code: str,
    meta_payload: dict[str, Any],
    ts_ms: int,
    intent_id: str | None,
    base_budget_quote: float | None,
) -> dict[str, Any]:
    meta = dict(meta_payload or {})
    strategy = _as_dict(meta.get("strategy"))
    strategy_meta = _as_dict(strategy.get("meta"))
    portfolio_budget = _as_dict(meta.get("portfolio_budget"))
    sizing = _resolve_sizing_payload(meta)
    admissibility = _as_dict(meta.get("admissibility"))
    admissibility_decision = _as_dict(admissibility.get("decision"))
    size_ladder = _as_dict(meta.get("size_ladder"))
    risk_control = _as_dict(meta.get("risk_control"))
    risk_control_online = _as_dict(meta.get("risk_control_online"))
    if portfolio_budget:
        exposure = {
            "current_total_cash_at_risk_quote": float(portfolio_budget.get("current_total_cash_at_risk_quote", 0.0) or 0.0),
            "projected_total_cash_at_risk_quote": float(portfolio_budget.get("projected_total_cash_at_risk_quote", 0.0) or 0.0),
            "cluster_utilization": dict(portfolio_budget.get("cluster_utilization") or {}),
        }
    else:
        exposure = summarize_portfolio_exposure(
            store=store,
            decision_market=market,
            decision_target_notional_quote=_safe_optional_float(sizing.get("target_notional_quote")),
        )
    budget_reason_codes = _extract_budget_reason_codes(
        status=status,
        meta_payload=meta,
        admissibility_decision=admissibility_decision,
    )
    requested_multiplier = _safe_optional_float(size_ladder.get("requested_multiplier"))
    resolved_multiplier = _safe_optional_float(size_ladder.get("resolved_multiplier"))
    if requested_multiplier is None:
        requested_multiplier = _safe_optional_float(strategy_meta.get("notional_multiplier"))
    if resolved_multiplier is None:
        resolved_multiplier = requested_multiplier
    target_notional_quote = _safe_optional_float(sizing.get("target_notional_quote"))
    if target_notional_quote is None:
        target_notional_quote = _safe_optional_float(portfolio_budget.get("target_notional_quote"))
    admissible_notional_quote = _safe_optional_float(sizing.get("admissible_notional_quote"))
    if admissible_notional_quote is None:
        admissible_notional_quote = _safe_optional_float(portfolio_budget.get("resolved_notional_quote"))
    adjusted_notional_quote = _safe_optional_float(admissibility_decision.get("adjusted_notional"))
    uncertainty = _resolve_uncertainty(strategy_meta)
    uncertainty_weighted_notional_quote = None
    uncertainty_formula = None
    if target_notional_quote is not None and uncertainty is not None:
        uncertainty_weighted_notional_quote = float(target_notional_quote) / (1.0 + abs(float(uncertainty)))
        uncertainty_formula = "weighted_notional_quote = target_notional_quote / (1 + abs(uncertainty))"
    position_budget_fraction = None
    base_budget_value = _safe_optional_float(base_budget_quote)
    if portfolio_budget and _safe_optional_float(portfolio_budget.get("position_budget_fraction")) is not None:
        position_budget_fraction = float(_safe_optional_float(portfolio_budget.get("position_budget_fraction")) or 0.0)
    elif base_budget_value is not None and base_budget_value > 0.0 and target_notional_quote is not None:
        position_budget_fraction = float(target_notional_quote) / float(base_budget_value)
    return {
        "artifact_version": RISK_BUDGET_LEDGER_VERSION,
        "ts_ms": int(ts_ms),
        "lane": str(lane).strip(),
        "unit_name": str(unit_name).strip(),
        "rollout_mode": str(rollout_mode).strip().lower(),
        "intent_id": str(intent_id).strip() if intent_id is not None else None,
        "market": str(market).strip().upper(),
        "side": str(side).strip().lower(),
        "status": str(status).strip().upper(),
        "strategy_reason_code": str(reason_code).strip(),
        "skip_reason": _optional_text(meta.get("skip_reason")),
        "runtime_model_run_id": _optional_text(_dig(meta, "runtime", "live_runtime_model_run_id")),
        "model_family": _optional_text(_dig(meta, "runtime", "model_family")),
        "current_total_cash_at_risk_quote": float(exposure["current_total_cash_at_risk_quote"]),
        "projected_total_cash_at_risk_quote": float(exposure["projected_total_cash_at_risk_quote"]),
        "cluster_utilization": exposure["cluster_utilization"],
        "uncertainty_weighted_exposure": {
            "uncertainty": uncertainty,
            "weighted_notional_quote": uncertainty_weighted_notional_quote,
            "formula": uncertainty_formula,
        },
        "recent_severe_loss_evidence": {
            "enabled": bool(risk_control_online.get("enabled", False)),
            "recent_trade_count": _safe_optional_int(risk_control_online.get("recent_trade_count")),
            "recent_nonpositive_rate": _safe_optional_float(risk_control_online.get("recent_nonpositive_rate")),
            "recent_nonpositive_rate_ucb": _safe_optional_float(risk_control_online.get("recent_nonpositive_rate_ucb")),
            "recent_severe_loss_rate": _safe_optional_float(risk_control_online.get("recent_severe_loss_rate")),
            "recent_severe_loss_rate_ucb": _safe_optional_float(risk_control_online.get("recent_severe_loss_rate_ucb")),
            "halt_triggered": bool(risk_control_online.get("halt_triggered", False)),
            "halt_reason_code": _optional_text(risk_control_online.get("halt_reason_code")),
            "martingale_halt_reason_code": _optional_text(risk_control_online.get("martingale_halt_reason_code")),
        },
        "current_risk_regime": {
            "entry_state": _resolve_entry_state(
                status=status,
                skip_reason=_optional_text(meta.get("skip_reason")),
                requested_multiplier=requested_multiplier,
                resolved_multiplier=resolved_multiplier,
                risk_control_online=risk_control_online,
                risk_control=risk_control,
                portfolio_budget=portfolio_budget,
            ),
            "size_ladder_enabled": bool(size_ladder.get("enabled", False)),
            "size_ladder_clamped": bool(
                requested_multiplier is not None
                and resolved_multiplier is not None
                and resolved_multiplier + 1e-12 < requested_multiplier
            ),
            "risk_control_blocked": bool(risk_control.get("enabled", False) and not bool(risk_control.get("allowed", True))),
            "risk_control_online_halt_triggered": bool(risk_control_online.get("halt_triggered", False)),
        },
        "budget_reason_codes": budget_reason_codes,
        "portfolio_budget_control": {
            "enabled": bool(portfolio_budget.get("enabled", False)),
            "allowed": bool(portfolio_budget.get("allowed", True)),
            "enforcement_mode": _optional_text(portfolio_budget.get("enforcement_mode")),
            "warning_only": bool(portfolio_budget.get("warning_only", False)),
            "warning_reason_codes": [
                _optional_text(item)
                for item in (portfolio_budget.get("warning_reason_codes") or [])
                if _optional_text(item) is not None
            ],
            "resolved_notional_quote": _safe_optional_float(portfolio_budget.get("resolved_notional_quote")),
            "diagnostic_resolved_notional_quote": _safe_optional_float(portfolio_budget.get("diagnostic_resolved_notional_quote")),
            "structural_resolved_notional_quote": _safe_optional_float(portfolio_budget.get("structural_resolved_notional_quote")),
        },
        "sizing": {
            "base_budget_quote": base_budget_value,
            "position_budget_fraction": position_budget_fraction,
            "target_notional_quote": target_notional_quote,
            "admissible_notional_quote": admissible_notional_quote,
            "adjusted_notional_quote": adjusted_notional_quote,
            "requested_multiplier": requested_multiplier,
            "resolved_multiplier": resolved_multiplier,
            "max_notional_quote": _safe_optional_float(portfolio_budget.get("max_notional_quote")),
            "fee_reserve_quote": _safe_optional_float(admissibility_decision.get("fee_reserve_quote")),
            "expected_edge_bps": _safe_optional_float(admissibility_decision.get("expected_edge_bps")),
            "expected_net_edge_bps": _safe_optional_float(admissibility_decision.get("expected_net_edge_bps")),
            "estimated_total_cost_bps": _safe_optional_float(admissibility_decision.get("estimated_total_cost_bps")),
            "replace_risk_budget_bps": _safe_optional_float(admissibility_decision.get("replace_risk_budget_bps")),
        },
    }


def _update_latest_summary(
    *,
    latest_path: Path,
    lane: str,
    unit_name: str,
    rollout_mode: str,
    ledger_path: Path,
    entry: dict[str, Any],
) -> dict[str, Any]:
    payload = {}
    if latest_path.exists():
        try:
            loaded = json.loads(latest_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                payload = dict(loaded)
        except Exception:
            payload = {}
    payload.setdefault("artifact_version", RISK_BUDGET_LEDGER_VERSION)
    payload["lane"] = str(lane).strip()
    payload["unit_name"] = str(unit_name).strip()
    payload["rollout_mode"] = str(rollout_mode).strip().lower()
    payload["latest_jsonl_path"] = str(ledger_path)
    payload["updated_ts_ms"] = int(entry.get("ts_ms") or 0)
    payload["total_entries"] = int(payload.get("total_entries", 0) or 0) + 1
    status_counts = _as_dict(payload.get("status_counts"))
    skip_reason_counts = _as_dict(payload.get("skip_reason_counts"))
    budget_reason_code_counts = _as_dict(payload.get("budget_reason_code_counts"))
    _inc(status_counts, str(entry.get("status") or "UNKNOWN").strip().upper())
    skip_reason = _optional_text(entry.get("skip_reason"))
    if skip_reason is not None:
        _inc(skip_reason_counts, skip_reason)
    for item in entry.get("budget_reason_codes") or []:
        reason_code = _optional_text(item)
        if reason_code is not None:
            _inc(budget_reason_code_counts, reason_code)
    payload["status_counts"] = status_counts
    payload["skip_reason_counts"] = skip_reason_counts
    payload["budget_reason_code_counts"] = budget_reason_code_counts
    payload["last_entry"] = entry
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


def _load_latest_summary(*, latest_path: Path) -> dict[str, Any] | None:
    if not latest_path.exists():
        return None
    try:
        loaded = json.loads(latest_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(loaded, dict):
        return None
    return dict(loaded)


def _rebuild_latest_summary_from_ledger(
    *,
    ledger_path: Path,
    lane: str,
    unit_name: str,
    rollout_mode: str,
) -> dict[str, Any]:
    payload = {
        "artifact_version": RISK_BUDGET_LEDGER_VERSION,
        "lane": str(lane).strip(),
        "unit_name": str(unit_name).strip(),
        "rollout_mode": str(rollout_mode).strip().lower(),
        "total_entries": 0,
        "status_counts": {},
        "skip_reason_counts": {},
        "budget_reason_code_counts": {},
        "latest_jsonl_path": str(ledger_path),
        "last_entry": None,
    }
    if not ledger_path.exists():
        return payload
    for line in ledger_path.read_text(encoding="utf-8").splitlines():
        text = str(line).strip()
        if not text:
            continue
        try:
            entry = json.loads(text)
        except Exception:
            continue
        if not isinstance(entry, dict):
            continue
        payload["total_entries"] = int(payload.get("total_entries", 0) or 0) + 1
        _inc(payload["status_counts"], str(entry.get("status") or "UNKNOWN").strip().upper())
        skip_reason = _optional_text(entry.get("skip_reason"))
        if skip_reason is not None:
            _inc(payload["skip_reason_counts"], skip_reason)
        for item in entry.get("budget_reason_codes") or []:
            reason_code = _optional_text(item)
            if reason_code is not None:
                _inc(payload["budget_reason_code_counts"], reason_code)
        payload["last_entry"] = entry
        payload["updated_ts_ms"] = int(entry.get("ts_ms") or 0)
    return payload


def _resolve_entry_state(
    *,
    status: str,
    skip_reason: str | None,
    requested_multiplier: float | None,
    resolved_multiplier: float | None,
    risk_control_online: dict[str, Any],
    risk_control: dict[str, Any],
    portfolio_budget: dict[str, Any],
) -> str:
    status_value = str(status).strip().upper()
    if bool(risk_control_online.get("halt_triggered", False)):
        return "online_halt"
    if bool(risk_control.get("enabled", False) and not bool(risk_control.get("allowed", True))):
        return "risk_blocked"
    if portfolio_budget and not bool(portfolio_budget.get("allowed", True)):
        return "portfolio_blocked"
    if portfolio_budget and bool(portfolio_budget.get("warning_only", False)):
        return "canary_warning"
    if portfolio_budget and bool(portfolio_budget.get("budget_clamped", False)):
        return "sized_down"
    if status_value in {"SKIPPED", "REJECTED_ADMISSIBILITY"}:
        return "blocked" if skip_reason else "rejected"
    if status_value == "SHADOW":
        return "shadow"
    if (
        requested_multiplier is not None
        and resolved_multiplier is not None
        and resolved_multiplier + 1e-12 < requested_multiplier
    ):
        return "sized_down"
    if status_value == "SUBMITTED":
        return "submitted"
    if status_value == "SUBMITTING":
        return "submitting"
    return "normal"


def _extract_budget_reason_codes(
    *,
    status: str,
    meta_payload: dict[str, Any],
    admissibility_decision: dict[str, Any],
) -> list[str]:
    reasons: list[str] = []
    for value in (
        meta_payload.get("skip_reason"),
        _dig(meta_payload, "size_ladder", "reason_code"),
        _dig(meta_payload, "trade_gate", "reason_code"),
        _dig(meta_payload, "micro_order_policy", "reason_code"),
        _dig(meta_payload, "execution_policy", "skip_reason_code"),
        _dig(meta_payload, "risk_control", "reason_code"),
        _dig(meta_payload, "risk_control_online", "halt_reason_code"),
        _dig(meta_payload, "risk_control_online", "martingale_halt_reason_code"),
        _dig(meta_payload, "execution_trace", "operational_overlay", "abort_reason"),
        admissibility_decision.get("reject_code"),
    ):
        text = _optional_text(value)
        if text is None or text in {"POLICY_DISABLED", "POLICY_OK", "ALLOW", "OK", "PASSED"}:
            continue
        if text not in reasons:
            reasons.append(text)
    for item in _as_dict(meta_payload.get("risk_control_online")).get("halt_reason_codes") or []:
        text = _optional_text(item)
        if text is not None and text not in reasons:
            reasons.append(text)
    status_value = str(status).strip().upper()
    if status_value == "REJECTED_ADMISSIBILITY":
        reject_code = _optional_text(admissibility_decision.get("reject_code"))
        if reject_code is not None and reject_code not in reasons:
            reasons.append(reject_code)
    for item in _as_dict(meta_payload.get("portfolio_budget")).get("risk_reason_codes") or []:
        text = _optional_text(item)
        if text is not None and text not in reasons:
            reasons.append(text)
    return reasons


def _resolve_sizing_payload(meta_payload: dict[str, Any]) -> dict[str, Any]:
    admissibility = _as_dict(meta_payload.get("admissibility"))
    sizing = _as_dict(admissibility.get("sizing"))
    if sizing:
        return sizing
    return _as_dict(meta_payload.get("sizing"))


def _resolve_uncertainty(strategy_meta: dict[str, Any]) -> float | None:
    for key in ("score_std", "uncertainty_sigma", "prediction_std", "uncertainty"):
        resolved = _safe_optional_float(strategy_meta.get(key))
        if resolved is not None:
            return float(resolved)
    return None


def _inc(mapping: dict[str, Any], key: str) -> None:
    key_value = str(key).strip()
    if not key_value:
        return
    mapping[key_value] = int(mapping.get(key_value, 0) or 0) + 1


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _dig(payload: dict[str, Any], *path: str) -> Any:
    current: Any = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _safe_optional_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_optional_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None
