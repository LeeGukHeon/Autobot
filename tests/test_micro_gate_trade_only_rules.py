from __future__ import annotations

from autobot.strategy.micro_gate_v1 import MicroGateSettings, MicroGateTradeSettings, MicroGateV1
from autobot.strategy.micro_snapshot import MicroSnapshot


def _snapshot(
    *,
    now_ts_ms: int,
    trade_events: int = 1,
    trade_coverage_ms: int = 0,
    trade_notional_krw: float = 0.0,
    last_event_delta_ms: int = 0,
) -> MicroSnapshot:
    return MicroSnapshot(
        market="KRW-BTC",
        snapshot_ts_ms=now_ts_ms,
        last_event_ts_ms=now_ts_ms - max(int(last_event_delta_ms), 0),
        trade_events=trade_events,
        trade_coverage_ms=trade_coverage_ms,
        trade_notional_krw=trade_notional_krw,
        trade_imbalance=0.0,
        trade_source="ws",
        spread_bps_mean=None,
        depth_top5_notional_krw=None,
        book_events=0,
        book_coverage_ms=0,
        book_available=False,
    )


def test_trade_only_blocks_low_liquidity_trade() -> None:
    gate = MicroGateV1(
        MicroGateSettings(
            enabled=True,
            mode="trade_only",
            trade=MicroGateTradeSettings(
                min_trade_events=2,
                min_trade_coverage_ms=0,
                min_trade_notional_krw=0.0,
            ),
        )
    )
    decision = gate.evaluate(
        micro_snapshot=_snapshot(now_ts_ms=1_000, trade_events=1),
        now_ts_ms=1_000,
    )
    assert not decision.allow
    assert decision.severity == "BLOCK"
    assert "LOW_LIQUIDITY_TRADE" in decision.reasons


def test_trade_only_blocks_stale_micro() -> None:
    gate = MicroGateV1(
        MicroGateSettings(
            enabled=True,
            mode="trade_only",
            stale_ms=1_000,
        )
    )
    decision = gate.evaluate(
        micro_snapshot=_snapshot(now_ts_ms=10_000, trade_events=1, last_event_delta_ms=2_000),
        now_ts_ms=10_000,
    )
    assert not decision.allow
    assert decision.severity == "BLOCK"
    assert "STALE_MICRO" in decision.reasons


def test_trade_only_warn_allow_on_missing_snapshot() -> None:
    gate = MicroGateV1(
        MicroGateSettings(
            enabled=True,
            mode="trade_only",
            on_missing="warn_allow",
        )
    )
    decision = gate.evaluate(micro_snapshot=None, now_ts_ms=1_000)
    assert decision.allow
    assert decision.severity == "WARN"
    assert decision.reasons == ("MICRO_MISSING",)
