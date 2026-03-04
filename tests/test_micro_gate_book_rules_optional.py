from __future__ import annotations

from autobot.strategy.micro_gate_v1 import MicroGateBookSettings, MicroGateSettings, MicroGateV1
from autobot.strategy.micro_snapshot import MicroSnapshot


def _snapshot(
    *,
    spread_bps_mean: float | None,
    depth_top5_notional_krw: float | None,
    book_available: bool,
    book_events: int = 1,
    book_coverage_ms: int = 1,
) -> MicroSnapshot:
    return MicroSnapshot(
        market="KRW-BTC",
        snapshot_ts_ms=1_000,
        last_event_ts_ms=1_000,
        trade_events=5,
        trade_coverage_ms=1_000,
        trade_notional_krw=100_000.0,
        trade_imbalance=0.0,
        trade_source="ws",
        spread_bps_mean=spread_bps_mean,
        depth_top5_notional_krw=depth_top5_notional_krw,
        book_events=book_events,
        book_coverage_ms=book_coverage_ms,
        book_available=book_available,
    )


def test_trade_and_book_blocks_wide_spread() -> None:
    gate = MicroGateV1(
        MicroGateSettings(
            enabled=True,
            mode="trade_and_book",
            book=MicroGateBookSettings(max_spread_bps=5.0),
        )
    )
    decision = gate.evaluate(
        micro_snapshot=_snapshot(
            spread_bps_mean=10.0,
            depth_top5_notional_krw=1_000_000.0,
            book_available=True,
        ),
        now_ts_ms=1_000,
    )
    assert not decision.allow
    assert "WIDE_SPREAD" in decision.reasons


def test_trade_only_ignores_book_rules() -> None:
    gate = MicroGateV1(
        MicroGateSettings(
            enabled=True,
            mode="trade_only",
            book=MicroGateBookSettings(max_spread_bps=1.0),
        )
    )
    decision = gate.evaluate(
        micro_snapshot=_snapshot(
            spread_bps_mean=100.0,
            depth_top5_notional_krw=10.0,
            book_available=True,
        ),
        now_ts_ms=1_000,
    )
    assert decision.allow
    assert decision.severity == "OK"


def test_trade_and_book_warns_when_book_missing() -> None:
    gate = MicroGateV1(
        MicroGateSettings(
            enabled=True,
            mode="trade_and_book",
            on_missing="warn_allow",
            book=MicroGateBookSettings(max_spread_bps=1.0),
        )
    )
    decision = gate.evaluate(
        micro_snapshot=_snapshot(
            spread_bps_mean=None,
            depth_top5_notional_krw=None,
            book_available=False,
            book_events=0,
            book_coverage_ms=0,
        ),
        now_ts_ms=1_000,
    )
    assert decision.allow
    assert decision.severity == "WARN"
    assert decision.reasons == ("BOOK_MISSING",)
