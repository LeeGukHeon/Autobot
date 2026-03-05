from __future__ import annotations

from autobot.strategy.micro_order_policy import (
    CROSS_BLOCK_TICK_BPS_TOO_HIGH,
    REASON_MICRO_MISSING_ABORT,
    REASON_MICRO_MISSING_FALLBACK,
    REASON_TICK_BPS_ABORT,
    TIER_HIGH,
    TIER_LOW,
    MicroOrderPolicySafetySettings,
    MicroOrderPolicySettings,
    MicroOrderPolicyTierSettings,
    MicroOrderPolicyTiersSettings,
    MicroOrderPolicyV1,
)
from autobot.strategy.micro_snapshot import MicroSnapshot


def _snapshot(*, trade_events: int, trade_notional_krw: float) -> MicroSnapshot:
    return MicroSnapshot(
        market="KRW-BTC",
        snapshot_ts_ms=1_000,
        last_event_ts_ms=1_000,
        trade_events=trade_events,
        trade_coverage_ms=60_000,
        trade_notional_krw=trade_notional_krw,
        trade_imbalance=0.0,
        trade_source="ws",
        spread_bps_mean=5.0,
        depth_top5_notional_krw=1_000_000.0,
        book_events=10,
        book_coverage_ms=60_000,
        book_available=True,
    )


def test_micro_order_policy_assigns_tiers_from_liquidity_score() -> None:
    policy = MicroOrderPolicyV1(MicroOrderPolicySettings(enabled=True))
    low = policy.evaluate(
        micro_snapshot=_snapshot(trade_events=1, trade_notional_krw=100.0),
        market="KRW-BTC",
        ref_price=100_000_000.0,
        tick_size=1_000.0,
        replace_attempt=0,
    )
    high = policy.evaluate(
        micro_snapshot=_snapshot(trade_events=500, trade_notional_krw=5_000_000_000.0),
        market="KRW-BTC",
        ref_price=100_000_000.0,
        tick_size=1_000.0,
        replace_attempt=0,
    )

    assert low.allow is True
    assert low.tier == TIER_LOW
    assert low.profile is not None
    assert high.allow is True
    assert high.tier == TIER_HIGH
    assert high.profile is not None


def test_micro_order_policy_missing_modes() -> None:
    fallback = MicroOrderPolicyV1(
        MicroOrderPolicySettings(enabled=True, on_missing="static_fallback")
    ).evaluate(micro_snapshot=None)
    conservative = MicroOrderPolicyV1(
        MicroOrderPolicySettings(enabled=True, on_missing="conservative")
    ).evaluate(micro_snapshot=None)
    abort = MicroOrderPolicyV1(
        MicroOrderPolicySettings(enabled=True, on_missing="abort")
    ).evaluate(micro_snapshot=None)

    assert fallback.allow is True
    assert fallback.reason_code == REASON_MICRO_MISSING_FALLBACK
    assert fallback.tier == "MID"
    assert conservative.allow is True
    assert conservative.reason_code == REASON_MICRO_MISSING_FALLBACK
    assert conservative.tier == TIER_LOW
    assert abort.allow is False
    assert abort.reason_code == REASON_MICRO_MISSING_ABORT


def test_micro_order_policy_forbids_post_only_with_cross() -> None:
    tiers = MicroOrderPolicyTiersSettings(
        high=MicroOrderPolicyTierSettings(
            timeout_ms=10_000,
            replace_interval_ms=2_000,
            max_replaces=3,
            price_mode="CROSS_1T",
            max_chase_bps=20,
            post_only=True,
        )
    )
    settings = MicroOrderPolicySettings(
        enabled=True,
        safety=MicroOrderPolicySafetySettings(forbid_post_only_with_cross=True),
        tiers=tiers,
    )
    decision = MicroOrderPolicyV1(settings).evaluate(
        micro_snapshot=_snapshot(trade_events=999, trade_notional_krw=9_999_999_999.0),
        market="KRW-BTC",
        ref_price=100_000_000.0,
        tick_size=1_000.0,
        replace_attempt=2,
    )

    assert decision.allow is True
    assert decision.profile is not None
    assert decision.profile.price_mode == "CROSS_1T"
    assert decision.profile.post_only is False


def test_micro_order_policy_tick_bps_guard_and_monotonic() -> None:
    policy = MicroOrderPolicyV1(
        MicroOrderPolicySettings(
            enabled=True,
            cross_tick_bps_max=10.0,
            cross_escalate_after_timeouts=2,
        )
    )
    doge_like = policy.evaluate(
        micro_snapshot=_snapshot(trade_events=999, trade_notional_krw=9_999_999_999.0),
        market="KRW-DOGE",
        ref_price=131.0,
        tick_size=1.0,
        replace_attempt=2,
    )
    tighter_tick = policy.evaluate(
        micro_snapshot=_snapshot(trade_events=999, trade_notional_krw=9_999_999_999.0),
        market="KRW-BTC",
        ref_price=100_000_000.0,
        tick_size=1_000.0,
        replace_attempt=2,
    )

    assert doge_like.profile is not None
    assert tighter_tick.profile is not None
    assert float(doge_like.diagnostics["tick_bps"]) > float(tighter_tick.diagnostics["tick_bps"])
    assert doge_like.profile.price_mode == "JOIN"
    assert doge_like.diagnostics["cross_block_reason"] == CROSS_BLOCK_TICK_BPS_TOO_HIGH
    assert tighter_tick.profile.price_mode == "CROSS_1T"


def test_micro_order_policy_cross_escalates_after_second_timeout() -> None:
    policy = MicroOrderPolicyV1(
        MicroOrderPolicySettings(
            enabled=True,
            cross_tick_bps_max=10.0,
            cross_escalate_after_timeouts=2,
        )
    )
    first = policy.evaluate(
        micro_snapshot=_snapshot(trade_events=999, trade_notional_krw=9_999_999_999.0),
        market="KRW-BTC",
        ref_price=100_000_000.0,
        tick_size=1_000.0,
        replace_attempt=0,
    )
    second = policy.evaluate(
        micro_snapshot=_snapshot(trade_events=999, trade_notional_krw=9_999_999_999.0),
        market="KRW-BTC",
        ref_price=100_000_000.0,
        tick_size=1_000.0,
        replace_attempt=1,
    )
    third = policy.evaluate(
        micro_snapshot=_snapshot(trade_events=999, trade_notional_krw=9_999_999_999.0),
        market="KRW-BTC",
        ref_price=100_000_000.0,
        tick_size=1_000.0,
        replace_attempt=2,
    )

    assert first.profile is not None
    assert second.profile is not None
    assert third.profile is not None
    assert first.profile.price_mode == "JOIN"
    assert second.profile.price_mode == "JOIN"
    assert third.profile.price_mode == "CROSS_1T"


def test_micro_order_policy_abort_if_tick_bps_too_high() -> None:
    policy = MicroOrderPolicyV1(
        MicroOrderPolicySettings(
            enabled=True,
            abort_if_tick_bps_gt=25.0,
        )
    )
    decision = policy.evaluate(
        micro_snapshot=_snapshot(trade_events=10, trade_notional_krw=10_000.0),
        market="KRW-DOGE",
        ref_price=131.0,
        tick_size=1.0,
        replace_attempt=0,
    )

    assert decision.allow is False
    assert decision.reason_code == REASON_TICK_BPS_ABORT
