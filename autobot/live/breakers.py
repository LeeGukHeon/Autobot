"""Persistent live breaker and kill-switch contracts."""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any

from autobot.upbit.exceptions import AuthError, RateLimitError, UpbitError

from .state_store import BreakerEventRecord, BreakerStateRecord, LiveStateStore

ACTION_WARN = "WARN"
ACTION_HALT_NEW_INTENTS = "HALT_NEW_INTENTS"
ACTION_HALT_AND_CANCEL_BOT_ORDERS = "HALT_AND_CANCEL_BOT_ORDERS"
ACTION_FULL_KILL_SWITCH = "FULL_KILL_SWITCH"

BREAKER_KEY_LIVE = "live"
EVENT_KIND_WARN = "WARN"
EVENT_KIND_ARM = "ARM"
EVENT_KIND_CLEAR = "CLEAR"

HALTING_ACTIONS = frozenset(
    {
        ACTION_HALT_NEW_INTENTS,
        ACTION_HALT_AND_CANCEL_BOT_ORDERS,
        ACTION_FULL_KILL_SWITCH,
    }
)
BOT_CANCEL_ACTIONS = frozenset({ACTION_HALT_AND_CANCEL_BOT_ORDERS, ACTION_FULL_KILL_SWITCH})

ACTION_SEVERITY = {
    ACTION_WARN: 0,
    ACTION_HALT_NEW_INTENTS: 1,
    ACTION_HALT_AND_CANCEL_BOT_ORDERS: 2,
    ACTION_FULL_KILL_SWITCH: 3,
}

REASON_ACTION_MAP = {
    "UNKNOWN_OPEN_ORDERS_DETECTED": ACTION_HALT_AND_CANCEL_BOT_ORDERS,
    "UNKNOWN_POSITIONS_DETECTED": ACTION_FULL_KILL_SWITCH,
    "LOCAL_POSITION_MISSING_ON_EXCHANGE": ACTION_HALT_AND_CANCEL_BOT_ORDERS,
    "LOCAL_OPEN_ORDER_NOT_FOUND_ON_EXCHANGE": ACTION_WARN,
    "STALE_PRIVATE_WS_STREAM": ACTION_HALT_NEW_INTENTS,
    "STALE_EXECUTOR_STREAM": ACTION_HALT_NEW_INTENTS,
    "WS_PUBLIC_STALE": ACTION_HALT_NEW_INTENTS,
    "MODEL_POINTER_DIVERGENCE": ACTION_HALT_NEW_INTENTS,
    "MODEL_POINTER_UNRESOLVED": ACTION_HALT_NEW_INTENTS,
    "LIVE_ROLLOUT_NOT_ARMED": ACTION_HALT_NEW_INTENTS,
    "LIVE_ROLLOUT_UNIT_MISMATCH": ACTION_HALT_NEW_INTENTS,
    "LIVE_ROLLOUT_MODE_MISMATCH": ACTION_HALT_NEW_INTENTS,
    "LIVE_TEST_ORDER_REQUIRED": ACTION_HALT_NEW_INTENTS,
    "LIVE_TEST_ORDER_STALE": ACTION_HALT_NEW_INTENTS,
    "LIVE_BREAKER_ACTIVE": ACTION_HALT_NEW_INTENTS,
    "LIVE_CANARY_REQUIRES_SINGLE_SLOT": ACTION_HALT_NEW_INTENTS,
    "REPEATED_CANCEL_REJECTS": ACTION_HALT_NEW_INTENTS,
    "REPEATED_REPLACE_REJECTS": ACTION_HALT_NEW_INTENTS,
    "REPEATED_RATE_LIMIT_ERRORS": ACTION_HALT_NEW_INTENTS,
    "REPEATED_AUTH_ERRORS": ACTION_FULL_KILL_SWITCH,
    "REPEATED_NONCE_ERRORS": ACTION_FULL_KILL_SWITCH,
    "IDENTIFIER_COLLISION": ACTION_FULL_KILL_SWITCH,
    "MANUAL_KILL_SWITCH": ACTION_FULL_KILL_SWITCH,
}

COUNTER_CONFIG = {
    "cancel_reject": {
        "checkpoint": "breaker_counter:cancel_reject",
        "reason_code": "REPEATED_CANCEL_REJECTS",
    },
    "replace_reject": {
        "checkpoint": "breaker_counter:replace_reject",
        "reason_code": "REPEATED_REPLACE_REJECTS",
    },
    "rate_limit_error": {
        "checkpoint": "breaker_counter:rate_limit_error",
        "reason_code": "REPEATED_RATE_LIMIT_ERRORS",
    },
    "auth_error": {
        "checkpoint": "breaker_counter:auth_error",
        "reason_code": "REPEATED_AUTH_ERRORS",
    },
    "nonce_error": {
        "checkpoint": "breaker_counter:nonce_error",
        "reason_code": "REPEATED_NONCE_ERRORS",
    },
}


@dataclass(frozen=True)
class BreakerDecision:
    active: bool
    action: str | None
    reason_codes: tuple[str, ...]
    source: str
    details: dict[str, Any]
    updated_ts: int


def action_for_reason(reason_code: str) -> str:
    return REASON_ACTION_MAP.get(str(reason_code).strip().upper(), ACTION_WARN)


def choose_action(reason_codes: list[str] | tuple[str, ...]) -> str:
    normalized = [str(item).strip().upper() for item in reason_codes if str(item).strip()]
    if not normalized:
        return ACTION_WARN
    return max((action_for_reason(item) for item in normalized), key=lambda item: ACTION_SEVERITY[item])


def breaker_status(store: LiveStateStore) -> dict[str, Any]:
    current = store.breaker_state(breaker_key=BREAKER_KEY_LIVE)
    counters = {name: _counter_payload(store=store, counter_name=name) for name in COUNTER_CONFIG}
    events = store.list_breaker_events(breaker_key=BREAKER_KEY_LIVE, limit=20)
    report = {
        "breaker_key": BREAKER_KEY_LIVE,
        "active": bool(current.get("active")) if current is not None else False,
        "action": current.get("action") if current is not None else None,
        "reason_codes": list(current.get("reason_codes", [])) if current is not None else [],
        "source": current.get("source") if current is not None else None,
        "details": dict(current.get("details", {})) if current is not None else {},
        "updated_ts": current.get("updated_ts") if current is not None else None,
        "armed_ts": current.get("armed_ts") if current is not None else None,
        "new_intents_allowed": not (current and bool(current.get("active"))),
        "counters": counters,
        "recent_events": events,
    }
    _write_breaker_report(store=store, payload=report)
    return report


def new_intents_allowed(store: LiveStateStore) -> bool:
    current = store.breaker_state(breaker_key=BREAKER_KEY_LIVE)
    return not (current and bool(current.get("active")))


def arm_breaker(
    store: LiveStateStore,
    *,
    reason_codes: list[str] | tuple[str, ...],
    source: str,
    ts_ms: int,
    action: str | None = None,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized = _normalize_reason_codes(reason_codes)
    if not normalized:
        return breaker_status(store)
    current = store.breaker_state(breaker_key=BREAKER_KEY_LIVE)
    existing_reasons = []
    if current is not None:
        existing_reasons = [str(item).strip().upper() for item in current.get("reason_codes", []) if str(item).strip()]
    merged_reasons = _normalize_reason_codes([*existing_reasons, *normalized])
    requested_action = str(action or choose_action(merged_reasons)).strip().upper() or ACTION_WARN
    if current is not None and current.get("action"):
        current_action = str(current.get("action")).strip().upper()
        effective_action = max((requested_action, current_action), key=lambda item: ACTION_SEVERITY.get(item, 0))
    else:
        effective_action = requested_action
    detail_payload = dict(current.get("details", {})) if current is not None else {}
    detail_payload.update(dict(details or {}))
    if effective_action == ACTION_WARN:
        return record_warning(store, reason_codes=merged_reasons, source=source, ts_ms=ts_ms, details=detail_payload)
    store.upsert_breaker_state(
        BreakerStateRecord(
            breaker_key=BREAKER_KEY_LIVE,
            active=True,
            action=effective_action,
            source=source,
            reason_codes_json=json.dumps(merged_reasons, ensure_ascii=False, sort_keys=True),
            details_json=json.dumps(detail_payload, ensure_ascii=False, sort_keys=True),
            updated_ts=int(ts_ms),
            armed_ts=int(current.get("armed_ts") or ts_ms) if current is not None and bool(current.get("active")) else int(ts_ms),
        )
    )
    store.append_breaker_event(
        BreakerEventRecord(
            ts_ms=int(ts_ms),
            breaker_key=BREAKER_KEY_LIVE,
            event_kind=EVENT_KIND_ARM,
            action=effective_action,
            source=source,
            reason_codes_json=json.dumps(merged_reasons, ensure_ascii=False, sort_keys=True),
            details_json=json.dumps(detail_payload, ensure_ascii=False, sort_keys=True),
        )
    )
    return breaker_status(store)


def record_warning(
    store: LiveStateStore,
    *,
    reason_codes: list[str] | tuple[str, ...],
    source: str,
    ts_ms: int,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized = _normalize_reason_codes(reason_codes)
    if not normalized:
        return breaker_status(store)
    store.append_breaker_event(
        BreakerEventRecord(
            ts_ms=int(ts_ms),
            breaker_key=BREAKER_KEY_LIVE,
            event_kind=EVENT_KIND_WARN,
            action=ACTION_WARN,
            source=source,
            reason_codes_json=json.dumps(normalized, ensure_ascii=False, sort_keys=True),
            details_json=json.dumps(dict(details or {}), ensure_ascii=False, sort_keys=True),
        )
    )
    return breaker_status(store)


def clear_breaker(
    store: LiveStateStore,
    *,
    source: str,
    ts_ms: int,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    _reset_all_counters(store=store, ts_ms=ts_ms)
    store.upsert_breaker_state(
        BreakerStateRecord(
            breaker_key=BREAKER_KEY_LIVE,
            active=False,
            action=None,
            source=source,
            reason_codes_json="[]",
            details_json=json.dumps(dict(details or {}), ensure_ascii=False, sort_keys=True),
            updated_ts=int(ts_ms),
            armed_ts=0,
        )
    )
    store.append_breaker_event(
        BreakerEventRecord(
            ts_ms=int(ts_ms),
            breaker_key=BREAKER_KEY_LIVE,
            event_kind=EVENT_KIND_CLEAR,
            action=None,
            source=source,
            reason_codes_json="[]",
            details_json=json.dumps(dict(details or {}), ensure_ascii=False, sort_keys=True),
        )
    )
    return breaker_status(store)


def clear_breaker_reasons(
    store: LiveStateStore,
    *,
    reason_codes: list[str] | tuple[str, ...],
    source: str,
    ts_ms: int,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized = _normalize_reason_codes(reason_codes)
    if not normalized:
        return breaker_status(store)
    current = store.breaker_state(breaker_key=BREAKER_KEY_LIVE)
    if current is None:
        return breaker_status(store)
    existing_reasons = [str(item).strip().upper() for item in current.get("reason_codes", []) if str(item).strip()]
    remaining_reasons = [item for item in existing_reasons if item not in normalized]
    if len(remaining_reasons) == len(existing_reasons):
        return breaker_status(store)
    detail_payload = dict(current.get("details", {}))
    detail_payload.update(dict(details or {}))
    detail_payload["cleared_reason_codes"] = list(normalized)
    detail_payload["remaining_reason_codes"] = list(remaining_reasons)
    next_action = choose_action(remaining_reasons) if remaining_reasons else None
    next_active = bool(remaining_reasons)
    armed_ts = int(current.get("armed_ts") or ts_ms) if next_active else 0
    store.upsert_breaker_state(
        BreakerStateRecord(
            breaker_key=BREAKER_KEY_LIVE,
            active=next_active,
            action=next_action,
            source=source,
            reason_codes_json=json.dumps(remaining_reasons, ensure_ascii=False, sort_keys=True),
            details_json=json.dumps(detail_payload, ensure_ascii=False, sort_keys=True),
            updated_ts=int(ts_ms),
            armed_ts=armed_ts,
        )
    )
    store.append_breaker_event(
        BreakerEventRecord(
            ts_ms=int(ts_ms),
            breaker_key=BREAKER_KEY_LIVE,
            event_kind=EVENT_KIND_CLEAR,
            action=next_action,
            source=source,
            reason_codes_json=json.dumps(normalized, ensure_ascii=False, sort_keys=True),
            details_json=json.dumps(detail_payload, ensure_ascii=False, sort_keys=True),
        )
    )
    return breaker_status(store)


def record_counter_failure(
    store: LiveStateStore,
    *,
    counter_name: str,
    limit: int,
    source: str,
    ts_ms: int,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    counter_cfg = COUNTER_CONFIG[counter_name]
    payload = _counter_payload(store=store, counter_name=counter_name)
    next_count = int(payload.get("count", 0)) + 1
    next_payload = {
        "name": counter_name,
        "count": next_count,
        "last_source": source,
        "last_details": dict(details or {}),
        "updated_ts": int(ts_ms),
    }
    store.set_checkpoint(name=counter_cfg["checkpoint"], payload=next_payload, ts_ms=ts_ms)
    if next_count >= max(int(limit), 1):
        return arm_breaker(
            store,
            reason_codes=[counter_cfg["reason_code"]],
            source=source,
            ts_ms=ts_ms,
            details={"counter_name": counter_name, "counter": next_payload, **dict(details or {})},
        )
    return breaker_status(store)


def reset_counter(
    store: LiveStateStore,
    *,
    counter_name: str,
    source: str,
    ts_ms: int,
) -> dict[str, Any]:
    counter_cfg = COUNTER_CONFIG[counter_name]
    store.set_checkpoint(
        name=counter_cfg["checkpoint"],
        payload={
            "name": counter_name,
            "count": 0,
            "last_source": source,
            "updated_ts": int(ts_ms),
        },
        ts_ms=ts_ms,
    )
    return breaker_status(store)


def classify_upbit_exception(exc: Exception) -> str | None:
    if isinstance(exc, UpbitError):
        error_name = str(exc.error_name or "").strip().lower()
        message = str(exc.message or "").strip().lower()
        status_code = int(exc.status_code) if exc.status_code is not None else None
        if "nonce" in error_name or "nonce" in message:
            return "REPEATED_NONCE_ERRORS"
        if isinstance(exc, RateLimitError) or status_code in {418, 429}:
            return "REPEATED_RATE_LIMIT_ERRORS"
        if isinstance(exc, AuthError) or status_code == 401:
            return "REPEATED_AUTH_ERRORS"
    return None


def classify_identifier_collision(exc: Exception) -> bool:
    return "IDENTIFIER_COLLISION" in str(exc)


def classify_executor_reject_reason(reason: str | None) -> str | None:
    text = str(reason or "").strip().lower()
    if not text:
        return None
    if "identifier" in text and ("collision" in text or "duplicate" in text or "exists" in text):
        return "IDENTIFIER_COLLISION"
    if "nonce" in text:
        return "REPEATED_NONCE_ERRORS"
    if "429" in text or "418" in text or "rate limit" in text or "too many" in text:
        return "REPEATED_RATE_LIMIT_ERRORS"
    if "401" in text or "auth" in text or "jwt" in text or "unauthorized" in text:
        return "REPEATED_AUTH_ERRORS"
    return None


def should_cancel_bot_orders(action: str | None) -> bool:
    return str(action or "").strip().upper() in BOT_CANCEL_ACTIONS


def evaluate_cycle_contracts(
    store: LiveStateStore,
    *,
    report: dict[str, Any] | None,
    source: str,
    ts_ms: int,
) -> dict[str, Any]:
    report = report if isinstance(report, dict) else {}
    counts = report.get("counts") if isinstance(report.get("counts"), dict) else {}
    halted_reasons = {str(item).strip().upper() for item in report.get("halted_reasons", []) if str(item).strip()}
    warnings: list[str] = []
    halts: list[str] = []

    if int(counts.get("local_only_open_orders") or 0) > 0:
        warnings.append("LOCAL_OPEN_ORDER_NOT_FOUND_ON_EXCHANGE")
    if int(counts.get("external_open_orders") or 0) > 0 or "UNKNOWN_OPEN_ORDERS_DETECTED" in halted_reasons:
        halts.append("UNKNOWN_OPEN_ORDERS_DETECTED")
    if "UNKNOWN_POSITIONS_DETECTED" in halted_reasons:
        halts.append("UNKNOWN_POSITIONS_DETECTED")
    if int(counts.get("local_positions_missing_on_exchange") or 0) > 0:
        halts.append("LOCAL_POSITION_MISSING_ON_EXCHANGE")

    if warnings:
        record_warning(
            store,
            reason_codes=warnings,
            source=source,
            ts_ms=ts_ms,
            details={"counts": counts},
        )
    if halts:
        return arm_breaker(
            store,
            reason_codes=halts,
            source=source,
            ts_ms=ts_ms,
            details={"counts": counts, "halted_reasons": sorted(halted_reasons)},
        )
    return breaker_status(store)


def active_breaker_decision(store: LiveStateStore) -> BreakerDecision:
    current = store.breaker_state(breaker_key=BREAKER_KEY_LIVE)
    if current is None:
        return BreakerDecision(
            active=False,
            action=None,
            reason_codes=(),
            source="none",
            details={},
            updated_ts=0,
        )
    return BreakerDecision(
        active=bool(current.get("active")),
        action=_as_optional_upper(current.get("action")),
        reason_codes=tuple(str(item).strip().upper() for item in current.get("reason_codes", [])),
        source=str(current.get("source") or "unknown"),
        details=dict(current.get("details", {})),
        updated_ts=int(current.get("updated_ts") or 0),
    )


def _counter_payload(*, store: LiveStateStore, counter_name: str) -> dict[str, Any]:
    counter_cfg = COUNTER_CONFIG[counter_name]
    payload = store.get_checkpoint(name=counter_cfg["checkpoint"])
    if payload is None:
        return {"name": counter_name, "count": 0}
    inner = payload.get("payload") if isinstance(payload, dict) else None
    if isinstance(inner, dict):
        return dict(inner)
    return {"name": counter_name, "count": 0}


def _normalize_reason_codes(reason_codes: list[str] | tuple[str, ...]) -> list[str]:
    unique: list[str] = []
    for item in reason_codes:
        code = str(item).strip().upper()
        if not code or code in unique:
            continue
        unique.append(code)
    return unique


def _reset_all_counters(*, store: LiveStateStore, ts_ms: int) -> None:
    for counter_name in COUNTER_CONFIG:
        reset_counter(store, counter_name=counter_name, source="breaker_clear", ts_ms=ts_ms)


def _write_breaker_report(*, store: LiveStateStore, payload: dict[str, Any]) -> None:
    path = store.db_path.parent / "live_breaker_report.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")


def _as_optional_upper(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    return text or None
