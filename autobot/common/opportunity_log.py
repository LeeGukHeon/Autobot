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
    payload.setdefault("feature_hash", "")
    payload.setdefault("selection_score", None)
    payload.setdefault("selection_score_raw", None)
    payload.setdefault("uncertainty", None)
    payload.setdefault("expected_edge_bps", None)
    payload.setdefault("candidate_actions_json", None)
    payload.setdefault("chosen_action_propensity", None)
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
        "feature_hash": "",
        "selection_score": selection_score,
        "selection_score_raw": selection_score,
        "uncertainty": None,
        "expected_edge_bps": _safe_optional_float(meta.get("expected_edge_bps")),
        "candidate_actions_json": None,
        "chosen_action": "intent_created",
        "chosen_action_propensity": None,
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
