from __future__ import annotations

import pytest

from autobot.live.admissibility import (
    REJECT_BELOW_MIN_TOTAL,
    REJECT_DUST_REMAINDER,
    REJECT_EXPECTED_EDGE_NOT_POSITIVE_AFTER_COST,
    REJECT_FEE_RESERVE_INSUFFICIENT,
    REJECT_INSUFFICIENT_FREE_BALANCE,
    build_live_order_admissibility_snapshot,
    evaluate_live_limit_order,
)


def _chance_payload() -> dict[str, object]:
    return {
        "bid_fee": "0.0005",
        "ask_fee": "0.0005",
        "market": {
            "bid": {"min_total": "5000"},
            "ask": {"min_total": "5000"},
        },
    }


def _accounts_payload(*, krw_free: float = 14700.0, btc_free: float = 0.02) -> list[dict[str, object]]:
    return [
        {"currency": "KRW", "balance": str(krw_free), "locked": "0", "avg_buy_price": "1"},
        {"currency": "BTC", "balance": str(btc_free), "locked": "0", "avg_buy_price": "100000000"},
    ]


def _instruments_payload() -> list[dict[str, object]]:
    return [{"market": "KRW-BTC", "tick_size": "1000"}]


def test_build_snapshot_extracts_exact_contract_fields() -> None:
    snapshot = build_live_order_admissibility_snapshot(
        market="krw-btc",
        side="bid",
        chance_payload=_chance_payload(),
        instruments_payload=_instruments_payload(),
        accounts_payload=_accounts_payload(),
        ts_ms=123,
    )

    assert snapshot.market == "KRW-BTC"
    assert snapshot.side == "bid"
    assert snapshot.tick_size == 1000.0
    assert snapshot.min_total == 5000.0
    assert snapshot.bid_fee == 0.0005
    assert snapshot.quote_balance.free == 14700.0
    assert snapshot.base_balance.free == 0.02
    assert snapshot.ts_ms == 123
    assert snapshot.chance_hash
    assert snapshot.instruments_hash
    assert snapshot.accounts_hash


def test_bid_order_is_price_aligned_and_admissible_for_small_account() -> None:
    snapshot = build_live_order_admissibility_snapshot(
        market="KRW-BTC",
        side="bid",
        chance_payload=_chance_payload(),
        instruments_payload=_instruments_payload(),
        accounts_payload=_accounts_payload(),
    )

    decision = evaluate_live_limit_order(snapshot=snapshot, price=5_550.0, volume=1.0)

    assert decision.admissible
    assert decision.adjusted_price == 5000.0
    assert decision.price_adjusted
    assert decision.adjusted_notional == 5000.0
    assert decision.fee_reserve_quote == pytest.approx(2.5)
    assert decision.remaining_quote_after == pytest.approx(9697.5)


def test_bid_order_rejects_when_fee_reserve_breaks_small_account() -> None:
    snapshot = build_live_order_admissibility_snapshot(
        market="KRW-BTC",
        side="bid",
        chance_payload=_chance_payload(),
        instruments_payload=_instruments_payload(),
        accounts_payload=_accounts_payload(krw_free=5000.0),
    )

    decision = evaluate_live_limit_order(snapshot=snapshot, price=5000.0, volume=1.0)

    assert not decision.admissible
    assert decision.reject_code == REJECT_FEE_RESERVE_INSUFFICIENT


def test_ask_order_rejects_dust_remainder() -> None:
    snapshot = build_live_order_admissibility_snapshot(
        market="KRW-BTC",
        side="ask",
        chance_payload=_chance_payload(),
        instruments_payload=_instruments_payload(),
        accounts_payload=_accounts_payload(btc_free=2.0),
    )

    decision = evaluate_live_limit_order(snapshot=snapshot, price=5000.0, volume=1.5)

    assert not decision.admissible
    assert decision.reject_code == REJECT_DUST_REMAINDER


def test_ask_order_rejects_insufficient_base_balance() -> None:
    snapshot = build_live_order_admissibility_snapshot(
        market="KRW-BTC",
        side="ask",
        chance_payload=_chance_payload(),
        instruments_payload=_instruments_payload(),
        accounts_payload=_accounts_payload(btc_free=0.01),
    )

    decision = evaluate_live_limit_order(snapshot=snapshot, price=500_000.0, volume=0.02)

    assert not decision.admissible
    assert decision.reject_code == REJECT_INSUFFICIENT_FREE_BALANCE


def test_order_rejects_below_min_total() -> None:
    snapshot = build_live_order_admissibility_snapshot(
        market="KRW-BTC",
        side="bid",
        chance_payload=_chance_payload(),
        instruments_payload=_instruments_payload(),
        accounts_payload=_accounts_payload(),
    )

    decision = evaluate_live_limit_order(snapshot=snapshot, price=5000.0, volume=0.5)

    assert not decision.admissible
    assert decision.reject_code == REJECT_BELOW_MIN_TOTAL


def test_expected_edge_gate_is_exact_and_explicit() -> None:
    snapshot = build_live_order_admissibility_snapshot(
        market="KRW-BTC",
        side="bid",
        chance_payload=_chance_payload(),
        instruments_payload=_instruments_payload(),
        accounts_payload=_accounts_payload(),
    )

    decision = evaluate_live_limit_order(
        snapshot=snapshot,
        price=5000.0,
        volume=1.0,
        expected_edge_bps=4.0,
    )

    assert not decision.admissible
    assert decision.reject_code == REJECT_EXPECTED_EDGE_NOT_POSITIVE_AFTER_COST
