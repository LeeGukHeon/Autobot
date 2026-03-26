"""Opportunity log helpers for backtest, paper, and live decision paths."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
import json
from pathlib import Path
from typing import Any

from autobot.backtest.strategy_adapter import StrategyOpportunityRecord, StrategyOrderIntent, StrategyStepResult

OPPORTUNITY_LOG_VERSION = 1


def reset_opportunity_log(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("", encoding="utf-8")


def reset_counterfactual_action_log(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("", encoding="utf-8")


def append_strategy_opportunities(
    *,
    path: Path,
    result: StrategyStepResult,
    ts_ms: int,
    run_id: str,
    lane: str,
    source: str,
) -> int:
    records = _build_records(result=result, ts_ms=ts_ms, run_id=run_id, lane=lane, source=source)
    if not records:
        return 0
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
    return len(records)


def append_counterfactual_actions(
    *,
    path: Path,
    result: StrategyStepResult,
    ts_ms: int,
    run_id: str,
    lane: str,
    source: str,
) -> int:
    records = _build_counterfactual_records(result=result, ts_ms=ts_ms, run_id=run_id, lane=lane, source=source)
    if not records:
        return 0
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
    return len(records)


def backfill_realized_outcomes(
    *,
    opportunity_log_path: Path,
    outcome_by_intent: dict[str, dict[str, Any]] | None,
    counterfactual_log_path: Path | None = None,
) -> dict[str, int]:
    outcomes = {
        str(key).strip(): dict(value)
        for key, value in (outcome_by_intent or {}).items()
        if str(key).strip() and isinstance(value, dict)
    }
    opportunity_rows = _load_jsonl_rows(opportunity_log_path)
    updated_opportunities = _apply_realized_outcomes(rows=opportunity_rows, outcome_by_intent=outcomes)
    if updated_opportunities > 0:
        _write_jsonl_rows(opportunity_log_path, opportunity_rows)

    updated_counterfactual = 0
    if counterfactual_log_path is not None:
        counterfactual_rows = _load_jsonl_rows(counterfactual_log_path)
        updated_counterfactual = _apply_realized_outcomes(rows=counterfactual_rows, outcome_by_intent=outcomes)
        if updated_counterfactual > 0:
            _write_jsonl_rows(counterfactual_log_path, counterfactual_rows)

    return {
        "updated_opportunity_rows": int(updated_opportunities),
        "updated_counterfactual_rows": int(updated_counterfactual),
    }


def _build_records(
    *,
    result: StrategyStepResult,
    ts_ms: int,
    run_id: str,
    lane: str,
    source: str,
) -> list[dict[str, Any]]:
    if result.opportunities:
        return [
            _normalize_explicit_record(record=record, run_id=run_id, lane=lane, source=source)
            for record in result.opportunities
        ]
    return [
        _fallback_record_from_intent(intent=intent, ts_ms=ts_ms, run_id=run_id, lane=lane, source=source)
        for intent in result.intents
    ]


def _build_counterfactual_records(
    *,
    result: StrategyStepResult,
    ts_ms: int,
    run_id: str,
    lane: str,
    source: str,
) -> list[dict[str, Any]]:
    records = _build_records(result=result, ts_ms=ts_ms, run_id=run_id, lane=lane, source=source)
    counterfactual_rows: list[dict[str, Any]] = []
    for record in records:
        actions = record.get("candidate_actions_json")
        if isinstance(actions, list) and actions:
            for index, action in enumerate(actions):
                payload = dict(action) if isinstance(action, dict) else {"action": action}
                counterfactual_rows.append(
                    {
                        "artifact_version": OPPORTUNITY_LOG_VERSION,
                        "opportunity_id": record.get("opportunity_id"),
                        "ts_ms": record.get("ts_ms"),
                        "market": record.get("market"),
                        "side": record.get("side"),
                        "run_id": record.get("run_id"),
                        "lane": record.get("lane"),
                        "source": record.get("source"),
                        "action_index": index,
                        "decision_outcome": record.get("decision_outcome"),
                        "chosen_action": record.get("chosen_action"),
                        "chosen_action_propensity": record.get("chosen_action_propensity"),
                        "no_trade_action_propensity": record.get("no_trade_action_propensity"),
                        "behavior_policy_name": record.get("behavior_policy_name"),
                        "behavior_policy_mode": record.get("behavior_policy_mode"),
                        "behavior_policy_support": record.get("behavior_policy_support"),
                        "skip_reason_code": record.get("skip_reason_code"),
                        "intent_id": record.get("intent_id"),
                        "realized_outcome_json": record.get("realized_outcome_json"),
                        "action_propensity": payload.get("propensity"),
                        "action_payload": payload,
                    }
                )
            continue
        chosen_action = str(record.get("chosen_action") or "").strip()
        if not chosen_action:
            continue
        counterfactual_rows.append(
            {
                "artifact_version": OPPORTUNITY_LOG_VERSION,
                "opportunity_id": record.get("opportunity_id"),
                "ts_ms": record.get("ts_ms"),
                "market": record.get("market"),
                "side": record.get("side"),
                "run_id": record.get("run_id"),
                "lane": record.get("lane"),
                "source": record.get("source"),
                "action_index": 0,
                "decision_outcome": record.get("decision_outcome"),
                "chosen_action": chosen_action,
                "chosen_action_propensity": record.get("chosen_action_propensity"),
                "no_trade_action_propensity": record.get("no_trade_action_propensity"),
                "behavior_policy_name": record.get("behavior_policy_name"),
                "behavior_policy_mode": record.get("behavior_policy_mode"),
                "behavior_policy_support": record.get("behavior_policy_support"),
                "skip_reason_code": record.get("skip_reason_code"),
                "intent_id": record.get("intent_id"),
                "realized_outcome_json": record.get("realized_outcome_json"),
                "action_propensity": record.get("chosen_action_propensity"),
                "action_payload": {
                    "action_code": chosen_action,
                    "selected": True,
                },
            }
        )
    return counterfactual_rows


def _normalize_explicit_record(
    *,
    record: StrategyOpportunityRecord,
    run_id: str,
    lane: str,
    source: str,
) -> dict[str, Any]:
    payload = _to_jsonable(record)
    if not isinstance(payload, dict):
        payload = {}
    payload.setdefault("opportunity_id", "")
    payload.setdefault("ts_ms", 0)
    payload.setdefault("market", "")
    payload.setdefault("side", "")
    payload["run_id"] = str(payload.get("run_id") or run_id or "").strip()
    payload["lane"] = str(lane).strip()
    payload["source"] = str(source).strip()
    payload["artifact_version"] = OPPORTUNITY_LOG_VERSION
    payload.setdefault("decision_outcome", "")
    payload.setdefault("feature_hash", "")
    payload.setdefault("selection_score", None)
    payload.setdefault("selection_score_raw", None)
    payload.setdefault("uncertainty", None)
    payload.setdefault("expected_edge_bps", None)
    payload.setdefault("candidate_actions_json", None)
    payload.setdefault("chosen_action_propensity", None)
    payload.setdefault("no_trade_action_propensity", None)
    payload.setdefault("behavior_policy_name", "")
    payload.setdefault("behavior_policy_mode", "")
    payload.setdefault("behavior_policy_support", "")
    payload.setdefault("skip_reason_code", None)
    payload.setdefault("realized_outcome_json", None)
    payload.setdefault("meta", {})
    return payload


def _fallback_record_from_intent(
    *,
    intent: StrategyOrderIntent,
    ts_ms: int,
    run_id: str,
    lane: str,
    source: str,
) -> dict[str, Any]:
    meta = dict(intent.meta or {})
    selection_score = _safe_optional_float(intent.score)
    if selection_score is None:
        selection_score = _safe_optional_float(intent.prob)
    return {
        "artifact_version": OPPORTUNITY_LOG_VERSION,
        "opportunity_id": f"fallback:{int(ts_ms)}:{str(intent.market).strip().upper()}:{str(intent.side).strip().lower()}",
        "ts_ms": int(ts_ms),
        "market": str(intent.market).strip().upper(),
        "side": str(intent.side).strip().lower(),
        "run_id": str(run_id).strip(),
        "lane": str(lane).strip(),
        "source": str(source).strip(),
        "decision_outcome": "intent_created",
        "feature_hash": "",
        "selection_score": selection_score,
        "selection_score_raw": selection_score,
        "uncertainty": None,
        "expected_edge_bps": _safe_optional_float(meta.get("expected_edge_bps")),
        "candidate_actions_json": None,
        "chosen_action": "intent_created",
        "chosen_action_propensity": None,
        "no_trade_action_propensity": None,
        "behavior_policy_name": "",
        "behavior_policy_mode": "",
        "behavior_policy_support": "",
        "skip_reason_code": None,
        "reason_code": str(intent.reason_code).strip(),
        "intent_id": None,
        "meta": meta,
        "realized_outcome_json": None,
    }


def _to_jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return _to_jsonable(asdict(value))
    if isinstance(value, dict):
        return {str(key): _to_jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_to_jsonable(item) for item in value]
    if isinstance(value, tuple):
        return [_to_jsonable(item) for item in value]
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return str(value)
    return value


def _safe_optional_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _load_jsonl_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def _write_jsonl_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")


def _apply_realized_outcomes(
    *,
    rows: list[dict[str, Any]],
    outcome_by_intent: dict[str, dict[str, Any]],
) -> int:
    updated = 0
    for row in rows:
        intent_id = str(row.get("intent_id") or "").strip()
        if not intent_id:
            continue
        outcome = outcome_by_intent.get(intent_id)
        if not isinstance(outcome, dict):
            continue
        if row.get("realized_outcome_json") == outcome:
            continue
        row["realized_outcome_json"] = dict(outcome)
        updated += 1
    return updated


def build_trade_journal_outcomes_by_intent(rows: list[dict[str, Any]] | None) -> dict[str, dict[str, Any]]:
    outcomes: dict[str, dict[str, Any]] = {}
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        intent_id = str(row.get("entry_intent_id") or "").strip()
        if not intent_id:
            continue
        outcomes[intent_id] = {
            "intent_id": intent_id,
            "market": str(row.get("market") or "").strip().upper(),
            "status": str(row.get("status") or "").strip().upper() or None,
            "close_mode": _optional_text(row.get("close_mode")),
            "close_reason_code": _optional_text(row.get("close_reason_code")),
            "realized_pnl_quote": _safe_optional_float(row.get("realized_pnl_quote")),
            "realized_pnl_pct": _safe_optional_float(row.get("realized_pnl_pct")),
            "expected_net_edge_bps": _safe_optional_float(row.get("expected_net_edge_bps")),
            "entry_notional_quote": _safe_optional_float(row.get("entry_notional_quote")),
            "exit_notional_quote": _safe_optional_float(row.get("exit_notional_quote")),
            "entry_filled_ts_ms": _safe_optional_int(row.get("entry_filled_ts_ms")),
            "exit_ts_ms": _safe_optional_int(row.get("exit_ts_ms")),
            "closed": str(row.get("status") or "").strip().upper() == "CLOSED",
            "source": "trade_journal",
        }
    return outcomes


def _safe_optional_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None
