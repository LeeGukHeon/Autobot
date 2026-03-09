"""Normalized live order-state machine and transition helpers."""

from __future__ import annotations

from dataclasses import dataclass


LOCAL_ORDER_STATE_INTENT_NEW = "INTENT_NEW"
LOCAL_ORDER_STATE_SUBMITTING = "SUBMITTING"
LOCAL_ORDER_STATE_OPEN = "OPEN"
LOCAL_ORDER_STATE_PARTIAL = "PARTIAL"
LOCAL_ORDER_STATE_REPLACING = "REPLACING"
LOCAL_ORDER_STATE_CANCELING = "CANCELING"
LOCAL_ORDER_STATE_DONE = "DONE"
LOCAL_ORDER_STATE_CANCELLED = "CANCELLED"
LOCAL_ORDER_STATE_REJECTED = "REJECTED"
LOCAL_ORDER_STATE_UNKNOWN = "UNKNOWN_EXCHANGE_STATE"

OPEN_LOCAL_ORDER_STATES = frozenset(
    {
        LOCAL_ORDER_STATE_INTENT_NEW,
        LOCAL_ORDER_STATE_SUBMITTING,
        LOCAL_ORDER_STATE_OPEN,
        LOCAL_ORDER_STATE_PARTIAL,
        LOCAL_ORDER_STATE_REPLACING,
        LOCAL_ORDER_STATE_CANCELING,
    }
)

TERMINAL_LOCAL_ORDER_STATES = frozenset(
    {
        LOCAL_ORDER_STATE_DONE,
        LOCAL_ORDER_STATE_CANCELLED,
        LOCAL_ORDER_STATE_REJECTED,
    }
)

_LEGAL_TRANSITIONS = {
    LOCAL_ORDER_STATE_INTENT_NEW: {
        LOCAL_ORDER_STATE_SUBMITTING,
        LOCAL_ORDER_STATE_OPEN,
        LOCAL_ORDER_STATE_PARTIAL,
        LOCAL_ORDER_STATE_REPLACING,
        LOCAL_ORDER_STATE_CANCELLED,
        LOCAL_ORDER_STATE_DONE,
        LOCAL_ORDER_STATE_REJECTED,
        LOCAL_ORDER_STATE_UNKNOWN,
    },
    LOCAL_ORDER_STATE_SUBMITTING: {
        LOCAL_ORDER_STATE_SUBMITTING,
        LOCAL_ORDER_STATE_OPEN,
        LOCAL_ORDER_STATE_PARTIAL,
        LOCAL_ORDER_STATE_DONE,
        LOCAL_ORDER_STATE_CANCELLED,
        LOCAL_ORDER_STATE_REJECTED,
        LOCAL_ORDER_STATE_UNKNOWN,
    },
    LOCAL_ORDER_STATE_OPEN: {
        LOCAL_ORDER_STATE_OPEN,
        LOCAL_ORDER_STATE_PARTIAL,
        LOCAL_ORDER_STATE_REPLACING,
        LOCAL_ORDER_STATE_CANCELING,
        LOCAL_ORDER_STATE_DONE,
        LOCAL_ORDER_STATE_CANCELLED,
        LOCAL_ORDER_STATE_UNKNOWN,
    },
    LOCAL_ORDER_STATE_PARTIAL: {
        LOCAL_ORDER_STATE_PARTIAL,
        LOCAL_ORDER_STATE_REPLACING,
        LOCAL_ORDER_STATE_CANCELING,
        LOCAL_ORDER_STATE_DONE,
        LOCAL_ORDER_STATE_CANCELLED,
        LOCAL_ORDER_STATE_UNKNOWN,
    },
    LOCAL_ORDER_STATE_REPLACING: {
        LOCAL_ORDER_STATE_REPLACING,
        LOCAL_ORDER_STATE_OPEN,
        LOCAL_ORDER_STATE_PARTIAL,
        LOCAL_ORDER_STATE_DONE,
        LOCAL_ORDER_STATE_CANCELLED,
        LOCAL_ORDER_STATE_REJECTED,
        LOCAL_ORDER_STATE_UNKNOWN,
    },
    LOCAL_ORDER_STATE_CANCELING: {
        LOCAL_ORDER_STATE_CANCELING,
        LOCAL_ORDER_STATE_OPEN,
        LOCAL_ORDER_STATE_PARTIAL,
        LOCAL_ORDER_STATE_DONE,
        LOCAL_ORDER_STATE_CANCELLED,
        LOCAL_ORDER_STATE_UNKNOWN,
    },
    LOCAL_ORDER_STATE_DONE: {LOCAL_ORDER_STATE_DONE, LOCAL_ORDER_STATE_UNKNOWN},
    LOCAL_ORDER_STATE_CANCELLED: {LOCAL_ORDER_STATE_CANCELLED, LOCAL_ORDER_STATE_UNKNOWN},
    LOCAL_ORDER_STATE_REJECTED: {LOCAL_ORDER_STATE_REJECTED, LOCAL_ORDER_STATE_UNKNOWN},
    LOCAL_ORDER_STATE_UNKNOWN: {
        LOCAL_ORDER_STATE_OPEN,
        LOCAL_ORDER_STATE_PARTIAL,
        LOCAL_ORDER_STATE_REPLACING,
        LOCAL_ORDER_STATE_CANCELING,
        LOCAL_ORDER_STATE_DONE,
        LOCAL_ORDER_STATE_CANCELLED,
        LOCAL_ORDER_STATE_REJECTED,
        LOCAL_ORDER_STATE_UNKNOWN,
    },
}


@dataclass(frozen=True)
class NormalizedOrderState:
    local_state: str
    exchange_state: str | None
    event_name: str | None


def normalize_order_state(
    *,
    exchange_state: str | None,
    event_name: str | None = None,
    executed_volume: float | None = None,
) -> NormalizedOrderState:
    raw_state = _clean_lower(exchange_state)
    event = _clean_upper(event_name)
    filled = float(executed_volume or 0.0)

    if event == "ORDER_ACCEPTED":
        return NormalizedOrderState(
            local_state=LOCAL_ORDER_STATE_SUBMITTING,
            exchange_state=raw_state,
            event_name=event,
        )
    if event == "ORDER_REPLACED":
        return NormalizedOrderState(
            local_state=LOCAL_ORDER_STATE_REPLACING,
            exchange_state=raw_state,
            event_name=event,
        )
    if event == "ORDER_CANCEL_REQUESTED":
        return NormalizedOrderState(
            local_state=LOCAL_ORDER_STATE_CANCELING,
            exchange_state=raw_state,
            event_name=event,
        )

    if raw_state in {"wait", "watch", "open"}:
        return NormalizedOrderState(
            local_state=LOCAL_ORDER_STATE_PARTIAL if filled > 0.0 else LOCAL_ORDER_STATE_OPEN,
            exchange_state=raw_state,
            event_name=event,
        )
    if raw_state == "partial":
        return NormalizedOrderState(
            local_state=LOCAL_ORDER_STATE_PARTIAL,
            exchange_state=raw_state,
            event_name=event,
        )
    if raw_state == "done":
        return NormalizedOrderState(
            local_state=LOCAL_ORDER_STATE_DONE,
            exchange_state=raw_state,
            event_name=event,
        )
    if raw_state in {"cancel", "cancelled"}:
        return NormalizedOrderState(
            local_state=LOCAL_ORDER_STATE_CANCELLED,
            exchange_state=raw_state,
            event_name=event,
        )
    if raw_state in {"reject", "rejected"}:
        return NormalizedOrderState(
            local_state=LOCAL_ORDER_STATE_REJECTED,
            exchange_state=raw_state,
            event_name=event,
        )
    if raw_state in {"cancel_reject", "replace_reject"}:
        return NormalizedOrderState(
            local_state=LOCAL_ORDER_STATE_OPEN,
            exchange_state=raw_state,
            event_name=event,
        )
    return NormalizedOrderState(
        local_state=LOCAL_ORDER_STATE_UNKNOWN,
        exchange_state=raw_state,
        event_name=event,
    )


def is_legal_transition(previous_state: str | None, next_state: str | None) -> bool:
    previous = _clean_upper(previous_state) or LOCAL_ORDER_STATE_INTENT_NEW
    candidate = _clean_upper(next_state)
    if candidate is None:
        return False
    allowed = _LEGAL_TRANSITIONS.get(previous)
    if allowed is None:
        return candidate == LOCAL_ORDER_STATE_UNKNOWN
    return candidate in allowed


def resolve_transition(previous_state: str | None, next_state: str | None) -> tuple[str, bool]:
    previous = _clean_upper(previous_state) or LOCAL_ORDER_STATE_INTENT_NEW
    candidate = _clean_upper(next_state) or LOCAL_ORDER_STATE_UNKNOWN
    if previous == candidate:
        return candidate, True
    if is_legal_transition(previous, candidate):
        return candidate, True
    return LOCAL_ORDER_STATE_UNKNOWN, False


def is_open_local_state(value: str | None) -> bool:
    return (_clean_upper(value) or "") in OPEN_LOCAL_ORDER_STATES


def _clean_lower(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    return text or None


def _clean_upper(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    return text or None
