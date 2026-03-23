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


def test_live_execution_policy_uses_matching_long_deadline_horizon() -> None:
    attempts = [
        {
            "action_code": "LIMIT_GTC_JOIN",
            "spread_bps": 4.0,
            "depth_top5_notional_krw": 3_000_000.0,
            "snapshot_age_ms": 100.0,
            "expected_edge_bps": 20.0,
            "submitted_ts_ms": 0,
            "first_fill_ts_ms": 250_000,
            "shortfall_bps": 1.0,
        }
    ] * 24
    model_payload = build_live_execution_contract(attempts=attempts)

    decision = select_live_execution_action(
        model_payload=model_payload,
        current_state={
            "spread_bps": 4.0,
            "depth_top5_notional_krw": 3_000_000.0,
            "snapshot_age_ms": 100.0,
            "expected_edge_bps": 20.0,
        },
        expected_edge_bps=20.0,
        candidate_actions=["LIMIT_GTC_JOIN"],
        deadline_ms=300_000,
    )

    assert decision["status"] == "selected"
    assert decision["selected_p_fill_source_horizon_ms"] == 300_000
    assert decision["selected_p_fill_deadline"] > 0.8


def test_live_execution_policy_unseen_action_uses_price_mode_prior() -> None:
    attempts = (
        [
            {
                "action_code": "LIMIT_GTC_JOIN",
                "spread_bps": 4.0,
                "depth_top5_notional_krw": 3_000_000.0,
                "snapshot_age_ms": 100.0,
                "expected_edge_bps": 20.0,
                "submitted_ts_ms": 0,
                "first_fill_ts_ms": 100_000,
                "shortfall_bps": 1.0,
            }
        ]
        * 10
        + [
            {
                "action_code": "LIMIT_GTC_JOIN",
                "spread_bps": 4.0,
                "depth_top5_notional_krw": 3_000_000.0,
                "snapshot_age_ms": 100.0,
                "expected_edge_bps": 10.0,
                "expected_net_edge_bps": 10.0,
                "submitted_ts_ms": 0,
                "final_state": "MISSED",
                "shortfall_bps": 0.0,
            }
        ]
        * 10
    )
    model_payload = build_live_execution_contract(attempts=attempts)

    decision = select_live_execution_action(
        model_payload=model_payload,
        current_state={
            "spread_bps": 4.0,
            "depth_top5_notional_krw": 3_000_000.0,
            "snapshot_age_ms": 100.0,
            "expected_edge_bps": 20.0,
        },
        expected_edge_bps=20.0,
        candidate_actions=["LIMIT_IOC_JOIN"],
        deadline_ms=300_000,
    )

    assert decision["status"] == "selected"
    assert decision["selected_action_code"] == "LIMIT_IOC_JOIN"
    assert decision["selected_stats_source"] == "price_mode_prior"
    assert decision["selected_p_fill_deadline"] > 0.0


def test_live_execution_policy_canary_strong_edge_escalates_from_passive_to_join() -> None:
    model_payload = {
        "policy": "live_execution_contract_v1",
        "rows_total": 100,
        "fill_model": {
            "action_stats": {
                "LIMIT_GTC_PASSIVE_MAKER": {
                    "sample_count": 50,
                    "p_fill_within_3000ms": 0.71,
                    "p_fill_within_default": 0.71,
                    "mean_shortfall_bps": 0.0,
                    "mean_time_to_first_fill_ms": 20_000.0,
                },
                "LIMIT_GTC_JOIN": {
                    "sample_count": 50,
                    "p_fill_within_3000ms": 0.61,
                    "p_fill_within_default": 0.61,
                    "mean_shortfall_bps": 0.0,
                    "mean_time_to_first_fill_ms": 5_000.0,
                },
            },
            "state_action_stats": {},
        },
        "miss_cost_model": {
            "action_stats": {
                "LIMIT_GTC_PASSIVE_MAKER": {"sample_count": 50, "mean_miss_cost_bps": 80.0},
                "LIMIT_GTC_JOIN": {"sample_count": 50, "mean_miss_cost_bps": 80.0},
            },
            "state_action_stats": {},
        },
    }

    decision = select_live_execution_action(
        model_payload=model_payload,
        current_state={
            "spread_bps": 4.0,
            "depth_top5_notional_krw": 3_000_000.0,
            "snapshot_age_ms": 100.0,
            "expected_edge_bps": 80.0,
            "rollout_mode": "canary",
        },
        expected_edge_bps=80.0,
        candidate_actions=["LIMIT_GTC_PASSIVE_MAKER", "LIMIT_GTC_JOIN"],
    )

    assert decision["status"] == "selected"
    assert decision["selected_action_code"] == "LIMIT_GTC_JOIN"
    assert decision["selection_reason_code"] == "CANARY_STRONG_EDGE_STAGE_ESCALATION"
    assert decision["urgency_override"]["enabled"] is True


def test_live_execution_policy_non_canary_keeps_passive_when_utility_is_higher() -> None:
    model_payload = {
        "policy": "live_execution_contract_v1",
        "rows_total": 100,
        "fill_model": {
            "action_stats": {
                "LIMIT_GTC_PASSIVE_MAKER": {
                    "sample_count": 50,
                    "p_fill_within_3000ms": 0.71,
                    "p_fill_within_default": 0.71,
                    "mean_shortfall_bps": 0.0,
                    "mean_time_to_first_fill_ms": 20_000.0,
                },
                "LIMIT_GTC_JOIN": {
                    "sample_count": 50,
                    "p_fill_within_3000ms": 0.61,
                    "p_fill_within_default": 0.61,
                    "mean_shortfall_bps": 0.0,
                    "mean_time_to_first_fill_ms": 5_000.0,
                },
            },
            "state_action_stats": {},
        },
        "miss_cost_model": {
            "action_stats": {
                "LIMIT_GTC_PASSIVE_MAKER": {"sample_count": 50, "mean_miss_cost_bps": 80.0},
                "LIMIT_GTC_JOIN": {"sample_count": 50, "mean_miss_cost_bps": 80.0},
            },
            "state_action_stats": {},
        },
    }

    decision = select_live_execution_action(
        model_payload=model_payload,
        current_state={
            "spread_bps": 4.0,
            "depth_top5_notional_krw": 3_000_000.0,
            "snapshot_age_ms": 100.0,
            "expected_edge_bps": 80.0,
            "rollout_mode": "live",
        },
        expected_edge_bps=80.0,
        candidate_actions=["LIMIT_GTC_PASSIVE_MAKER", "LIMIT_GTC_JOIN"],
    )

    assert decision["status"] == "selected"
    assert decision["selected_action_code"] == "LIMIT_GTC_PASSIVE_MAKER"
    assert decision["selection_reason_code"] == "UTILITY_MAX"


def test_live_execution_policy_miss_cost_model_uses_cleanup_proxy_not_raw_edge_only() -> None:
    attempts = [
        {
            "action_code": "LIMIT_GTC_PASSIVE_MAKER",
            "spread_bps": 4.0,
            "depth_top5_notional_krw": 3_000_000.0,
            "snapshot_age_ms": 100.0,
            "expected_edge_bps": 80.0,
            "expected_net_edge_bps": 80.0,
            "submitted_ts_ms": 0,
            "final_state": "MISSED",
            "shortfall_bps": 0.0,
            "fill_fraction": 0.0,
        }
    ] * 10

    payload = build_live_execution_contract(attempts=attempts)
    stats = payload["miss_cost_model"]["action_stats"]["LIMIT_GTC_PASSIVE_MAKER"]

    assert stats["sample_count"] == 10
    assert stats["mean_opportunity_cost_bps"] == 80.0
    assert stats["mean_cleanup_cost_bps"] > 0.0
    assert 0.0 < stats["mean_miss_cost_bps"] < 80.0


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

    assert payload["policy"] == "live_execution_contract_v2"
    assert payload["fill_model"]["policy"] == "live_fill_hazard_survival_v2"
    assert payload["miss_cost_model"]["policy"] == "execution_miss_cost_summary_v2"
    assert "price_mode_stats" in payload["fill_model"]
    assert "global_stats" in payload["fill_model"]
    assert "price_mode_stats" in payload["miss_cost_model"]
    assert "global_stats" in payload["miss_cost_model"]
