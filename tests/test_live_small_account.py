from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from autobot.live.small_account import (
    CANARY_MAX_OPEN_ORDERS_PER_MARKET_EXCEEDED,
    CANARY_MAX_POSITIONS_EXCEEDED,
    CANARY_MULTIPLE_ACTIVE_MARKETS,
    build_small_account_runtime_report,
    compute_small_account_cost_breakdown,
    derive_volume_from_target_notional,
    record_small_account_decision,
)
from autobot.live.state_store import LiveStateStore, PositionRecord


@dataclass(frozen=True)
class _Decision:
    admissible: bool
    reject_code: str | None
    expected_edge_bps: float | None = None
    expected_net_edge_bps: float | None = None
    estimated_total_cost_bps: float | None = None


def test_cost_breakdown_is_explicit_and_deterministic() -> None:
    breakdown = compute_small_account_cost_breakdown(
        price=100_000_000.0,
        tick_size=1_000.0,
        fee_rate=0.0005,
        expected_edge_bps=8.0,
        replace_risk_steps=2,
    )

    assert breakdown.fee_cost_bps == 5.0
    assert breakdown.tick_proxy_bps == 0.1
    assert breakdown.replace_risk_budget_bps == 0.2
    assert breakdown.estimated_total_cost_bps == 5.3
    assert breakdown.expected_net_edge_bps == 2.7
    assert "expected_net_edge_bps" in breakdown.formula


def test_derive_volume_from_target_notional_reserves_fee_for_bids() -> None:
    envelope = derive_volume_from_target_notional(
        side="bid",
        price=5_000.0,
        target_notional_quote=5_000.0,
        fee_rate=0.0005,
    )

    assert envelope.admissible_notional_quote < envelope.target_notional_quote
    assert envelope.fee_reserve_quote > 0.0
    assert envelope.admissible_volume == envelope.admissible_notional_quote / envelope.adjusted_price


def test_record_small_account_decision_updates_counters_and_report(tmp_path: Path) -> None:
    with LiveStateStore(tmp_path / "live_state.db") as store:
        record_small_account_decision(
            store=store,
            decision=_Decision(admissible=False, reject_code="REJECT_BELOW_MIN_TOTAL"),
            source="unit",
            ts_ms=1000,
            market="KRW-BTC",
        )
        record_small_account_decision(
            store=store,
            decision=_Decision(admissible=False, reject_code="REJECT_EXPECTED_EDGE_NOT_POSITIVE_AFTER_COST"),
            source="unit",
            ts_ms=1001,
            market="KRW-BTC",
        )
        record_small_account_decision(
            store=store,
            decision=_Decision(admissible=False, reject_code="REJECT_DUST_REMAINDER"),
            source="unit",
            ts_ms=1002,
            market="KRW-BTC",
        )
        report = build_small_account_runtime_report(
            store=store,
            canary_enabled=False,
            max_positions=1,
            max_open_orders_per_market=1,
            local_positions=[],
            exchange_bot_open_orders=[],
            ts_ms=1003,
        )

    assert report["counters"]["rejected_for_cost_count"] == 1
    assert report["counters"]["rejected_for_min_total_count"] == 1
    assert report["counters"]["dust_abort_count"] == 1
    assert (tmp_path / "live_small_account_report.json").exists()


def test_canary_report_detects_multi_slot_violations(tmp_path: Path) -> None:
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.upsert_position(
            PositionRecord(
                market="KRW-BTC",
                base_currency="BTC",
                base_amount=0.01,
                avg_entry_price=100_000_000.0,
                updated_ts=1000,
            )
        )
        store.upsert_position(
            PositionRecord(
                market="KRW-ETH",
                base_currency="ETH",
                base_amount=0.02,
                avg_entry_price=4_000_000.0,
                updated_ts=1001,
            )
        )
        report = build_small_account_runtime_report(
            store=store,
            canary_enabled=True,
            max_positions=1,
            max_open_orders_per_market=1,
            local_positions=store.list_positions(),
            exchange_bot_open_orders=[
                {"market": "KRW-BTC", "uuid": "bot-1", "identifier": "AUTOBOT-1"},
                {"market": "KRW-BTC", "uuid": "bot-2", "identifier": "AUTOBOT-2"},
            ],
            ts_ms=1002,
        )

    violations = set(report["canary"]["violations"])
    assert CANARY_MAX_POSITIONS_EXCEEDED in violations
    assert CANARY_MULTIPLE_ACTIVE_MARKETS in violations
    assert CANARY_MAX_OPEN_ORDERS_PER_MARKET_EXCEEDED in violations
