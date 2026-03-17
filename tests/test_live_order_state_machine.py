from __future__ import annotations

from autobot.live.order_state import (
    LOCAL_ORDER_STATE_CANCELLED,
    LOCAL_ORDER_STATE_OPEN,
    LOCAL_ORDER_STATE_PARTIAL,
    LOCAL_ORDER_STATE_REPLACING,
    LOCAL_ORDER_STATE_SUBMITTING,
    LOCAL_ORDER_STATE_UNKNOWN,
    is_legal_transition,
    normalize_order_state,
    resolve_transition,
)


def test_normalize_order_accepted_maps_to_submitting() -> None:
    normalized = normalize_order_state(exchange_state="wait", event_name="ORDER_ACCEPTED", executed_volume=0.0)
    assert normalized.local_state == LOCAL_ORDER_STATE_SUBMITTING


def test_normalize_partial_open_state_maps_to_partial() -> None:
    normalized = normalize_order_state(exchange_state="wait", event_name="ORDER_STATE", executed_volume=0.001)
    assert normalized.local_state == LOCAL_ORDER_STATE_PARTIAL


def test_normalize_trade_state_maps_to_partial() -> None:
    normalized = normalize_order_state(exchange_state="trade", event_name="ORDER_STATE", executed_volume=0.001)
    assert normalized.local_state == LOCAL_ORDER_STATE_PARTIAL


def test_cancel_reject_preserves_open_local_state() -> None:
    normalized = normalize_order_state(exchange_state="cancel_reject", event_name="CANCEL_RESULT", executed_volume=0.0)
    assert normalized.local_state == LOCAL_ORDER_STATE_OPEN


def test_replace_event_maps_to_replacing() -> None:
    normalized = normalize_order_state(exchange_state="wait", event_name="ORDER_REPLACED", executed_volume=0.0)
    assert normalized.local_state == LOCAL_ORDER_STATE_REPLACING


def test_illegal_terminal_to_open_transition_resolves_to_unknown() -> None:
    assert not is_legal_transition(LOCAL_ORDER_STATE_CANCELLED, LOCAL_ORDER_STATE_OPEN)
    resolved, ok = resolve_transition(LOCAL_ORDER_STATE_CANCELLED, LOCAL_ORDER_STATE_OPEN)
    assert ok is False
    assert resolved == LOCAL_ORDER_STATE_UNKNOWN
