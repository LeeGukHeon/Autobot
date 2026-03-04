from __future__ import annotations

from autobot.execution.order_supervisor import (
    PRICE_MODE_CROSS_1T,
    PRICE_MODE_PASSIVE_MAKER,
    REASON_CHASE_LIMIT_EXCEEDED,
    REASON_MIN_NOTIONAL_DUST_ABORT,
    SUPERVISOR_ACTION_ABORT,
    SUPERVISOR_ACTION_REPLACE,
    OrderExecProfile,
    build_limit_price_from_mode,
    evaluate_supervisor_action,
)


def test_price_mode_mapping_and_chase_limit_abort() -> None:
    passive_buy = build_limit_price_from_mode(
        side="bid",
        ref_price=100.0,
        tick_size=1.0,
        price_mode=PRICE_MODE_PASSIVE_MAKER,
    )
    cross_buy = build_limit_price_from_mode(
        side="bid",
        ref_price=100.0,
        tick_size=1.0,
        price_mode=PRICE_MODE_CROSS_1T,
    )
    assert passive_buy == 99.0
    assert cross_buy == 101.0

    profile = OrderExecProfile(
        timeout_ms=1,
        replace_interval_ms=1,
        max_replaces=3,
        price_mode=PRICE_MODE_CROSS_1T,
        max_chase_bps=10,
        min_replace_interval_ms_global=1,
    )
    action = evaluate_supervisor_action(
        profile=profile,
        side="bid",
        now_ts_ms=2,
        created_ts_ms=0,
        last_action_ts_ms=0,
        last_replace_ts_ms=0,
        replace_count=0,
        remaining_volume=1.0,
        ref_price=100.0,
        tick_size=1.0,
        initial_ref_price=100.0,
        min_total=1.0,
        replaces_last_minute=0,
        max_replaces_per_min_per_market=10,
    )
    assert action.action == SUPERVISOR_ACTION_ABORT
    assert action.reason_code == REASON_CHASE_LIMIT_EXCEEDED


def test_dust_abort_rule() -> None:
    profile = OrderExecProfile(
        timeout_ms=1,
        replace_interval_ms=1,
        max_replaces=3,
        price_mode=PRICE_MODE_PASSIVE_MAKER,
        max_chase_bps=10_000,
        min_replace_interval_ms_global=1,
    )
    action = evaluate_supervisor_action(
        profile=profile,
        side="bid",
        now_ts_ms=2,
        created_ts_ms=0,
        last_action_ts_ms=0,
        last_replace_ts_ms=0,
        replace_count=0,
        remaining_volume=0.001,
        ref_price=10_000.0,
        tick_size=1.0,
        initial_ref_price=10_000.0,
        min_total=5_000.0,
        replaces_last_minute=0,
        max_replaces_per_min_per_market=10,
    )
    assert action.action == SUPERVISOR_ACTION_ABORT
    assert action.reason_code == REASON_MIN_NOTIONAL_DUST_ABORT


def test_timeout_replace_action() -> None:
    profile = OrderExecProfile(
        timeout_ms=1,
        replace_interval_ms=1,
        max_replaces=3,
        price_mode=PRICE_MODE_PASSIVE_MAKER,
        max_chase_bps=10_000,
        min_replace_interval_ms_global=1,
    )
    action = evaluate_supervisor_action(
        profile=profile,
        side="bid",
        now_ts_ms=2,
        created_ts_ms=0,
        last_action_ts_ms=0,
        last_replace_ts_ms=0,
        replace_count=0,
        remaining_volume=1.0,
        ref_price=10_000.0,
        tick_size=1.0,
        initial_ref_price=10_000.0,
        min_total=5_000.0,
        replaces_last_minute=0,
        max_replaces_per_min_per_market=10,
    )
    assert action.action == SUPERVISOR_ACTION_REPLACE
    assert action.target_price is not None
