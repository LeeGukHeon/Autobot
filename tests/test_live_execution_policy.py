from __future__ import annotations

from pathlib import Path

from autobot.live.execution_attempts import (
    mark_execution_attempt_cancelled,
    record_execution_attempt_submission,
    update_execution_attempt_fill_from_position,
    update_execution_attempt_from_order,
)
from autobot.live.state_store import LiveStateStore, OrderRecord
from autobot.models.live_execution_policy import (
    build_live_execution_contract,
    build_live_execution_survival_model,
    candidate_action_codes_for_price_mode,
    select_live_execution_action,
)


def test_live_execution_policy_prefers_higher_fill_lower_shortfall_action() -> None:
    attempts = [
        {
            "action_code": "LIMIT_GTC_JOIN",
            "spread_bps": 4.0,
            "depth_top5_notional_krw": 3_000_000.0,
            "snapshot_age_ms": 100.0,
            "expected_edge_bps": 20.0,
            "submitted_ts_ms": 0,
            "first_fill_ts_ms": 2_500,
            "shortfall_bps": 1.5,
        },
        {
            "action_code": "LIMIT_GTC_JOIN",
            "spread_bps": 4.0,
            "depth_top5_notional_krw": 3_000_000.0,
            "snapshot_age_ms": 100.0,
            "expected_edge_bps": 20.0,
            "submitted_ts_ms": 0,
            "first_fill_ts_ms": 2_800,
            "shortfall_bps": 1.0,
        },
        {
            "action_code": "BEST_IOC",
            "spread_bps": 4.0,
            "depth_top5_notional_krw": 3_000_000.0,
            "snapshot_age_ms": 100.0,
            "expected_edge_bps": 20.0,
            "submitted_ts_ms": 0,
            "first_fill_ts_ms": 300,
            "shortfall_bps": 8.0,
        },
    ] * 10
    model = build_live_execution_survival_model(attempts=attempts)

    decision = select_live_execution_action(
        model_payload=model,
        current_state={
            "spread_bps": 4.0,
            "depth_top5_notional_krw": 3_000_000.0,
            "snapshot_age_ms": 100.0,
            "expected_edge_bps": 20.0,
        },
        expected_edge_bps=20.0,
        candidate_actions=["LIMIT_GTC_JOIN", "BEST_IOC"],
    )

    assert decision["status"] == "selected"
    assert decision["selected_action_code"] == "LIMIT_GTC_JOIN"


def test_live_execution_attempts_store_submission_ws_fill_and_shortfall(tmp_path: Path) -> None:
    with LiveStateStore(tmp_path / "live_state.db") as store:
        attempt_id = record_execution_attempt_submission(
            store=store,
            journal_id="journal-1",
            intent_id="intent-1",
            order_uuid="order-1",
            order_identifier="identifier-1",
            market="KRW-BTC",
            side="bid",
            ord_type="limit",
            time_in_force="gtc",
            meta_payload={
                "strategy": {
                    "meta": {
                        "model_prob": 0.91,
                        "trade_action": {"expected_edge": 0.0025, "expected_es": 0.0010},
                    }
                },
                "execution": {
                    "effective_ref_price": 100.0,
                    "requested_price": 100.0,
                    "requested_volume": 2.0,
                    "exec_profile": {"price_mode": "JOIN"},
                },
                "admissibility": {
                    "sizing": {"admissible_notional_quote": 200.0},
                    "snapshot": {"tick_size": 1.0},
                    "decision": {"expected_edge_bps": 25.0, "expected_net_edge_bps": 20.0},
                },
                "micro_state": {
                    "spread_bps": 4.0,
                    "depth_top5_notional_krw": 2_000_000.0,
                    "trade_coverage_ms": 60_000,
                    "book_coverage_ms": 60_000,
                    "snapshot_age_ms": 100,
                    "micro_quality_score": 0.8,
                },
                "execution_policy": {
                    "selected_action_code": "LIMIT_GTC_JOIN",
                },
            },
            ts_ms=1_000,
        )
        assert attempt_id
        store.upsert_order(
            OrderRecord(
                uuid="order-1",
                identifier="identifier-1",
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                time_in_force="gtc",
                price=100.0,
                volume_req=2.0,
                volume_filled=1.0,
                state="wait",
                created_ts=1_000,
                updated_ts=2_000,
                intent_id="intent-1",
                local_state="PARTIAL",
                raw_exchange_state="wait",
                last_event_name="ORDER_STATE",
                event_source="private_ws",
            )
        )
        update_execution_attempt_from_order(
            store=store,
            order=store.order_by_uuid(uuid="order-1") or {},
            intent_id="intent-1",
            ts_ms=2_000,
        )
        update_execution_attempt_fill_from_position(
            store=store,
            intent_id="intent-1",
            journal_id="journal-1",
            fill_price=101.0,
            filled_volume=2.0,
            ts_ms=3_000,
        )

        attempt = store.latest_execution_attempt_by_intent(intent_id="intent-1")

    assert attempt is not None
    assert attempt["first_fill_ts_ms"] == 2_000
    assert attempt["full_fill_ts_ms"] == 3_000
    assert attempt["final_state"] == "FILLED"
    assert attempt["shortfall_bps"] is not None
    assert attempt["full_fill"] is True


def test_live_execution_attempts_mark_cancelled(tmp_path: Path) -> None:
    with LiveStateStore(tmp_path / "live_state.db") as store:
        record_execution_attempt_submission(
            store=store,
            journal_id="journal-1",
            intent_id="intent-1",
            order_uuid="order-1",
            order_identifier="identifier-1",
            market="KRW-BTC",
            side="bid",
            ord_type="limit",
            time_in_force="gtc",
            meta_payload={},
            ts_ms=1_000,
        )
        mark_execution_attempt_cancelled(
            store=store,
            intent_id="intent-1",
            order_uuid="order-1",
            ts_ms=2_000,
            final_state="MISSED",
            outcome_payload={"reason_code": "TIMEOUT"},
        )
        attempt = store.execution_attempt_by_order_uuid(order_uuid="order-1")

    assert attempt is not None
    assert attempt["final_state"] == "MISSED"
    assert attempt["cancelled_ts_ms"] == 2_000
    assert attempt["outcome"]["reason_code"] == "TIMEOUT"


def test_candidate_action_codes_for_price_mode_cross_prefers_best() -> None:
    assert candidate_action_codes_for_price_mode(price_mode="CROSS_1T")[0] == "BEST_IOC"


def test_live_execution_contract_contains_fill_and_miss_models() -> None:
    attempts = [
        {
            "action_code": "LIMIT_GTC_PASSIVE_MAKER",
            "spread_bps": 4.0,
            "depth_top5_notional_krw": 3_000_000.0,
            "snapshot_age_ms": 100.0,
            "expected_edge_bps": 20.0,
            "expected_net_edge_bps": 18.0,
            "submitted_ts_ms": 0,
            "final_state": "MISSED",
        }
    ] * 5

    payload = build_live_execution_contract(attempts=attempts)

    assert payload["policy"] == "live_execution_contract_v1"
    assert payload["fill_model"]["policy"] == "live_fill_hazard_survival_v1"
    assert payload["miss_cost_model"]["policy"] == "execution_miss_cost_summary_v1"
