from __future__ import annotations

import json

import pytest

from autobot.live.state_store import IntentRecord, LiveStateStore, OrderRecord, RiskPlanRecord, TradeJournalRecord
from autobot.live.trade_journal import (
    activate_trade_journal_for_position,
    backfill_order_execution_details,
    cancel_pending_entry_journal,
    close_trade_journal_for_market,
    record_entry_submission,
    rebind_pending_entry_journal_order,
)


def test_trade_journal_tracks_submitted_open_and_closed_trade(tmp_path) -> None:
    db_path = tmp_path / "live_state.db"
    meta_payload = {
        "strategy": {
            "meta": {
                "model_prob": 0.91,
                "selection_policy_mode": "rank_effective_quantile",
                "notional_multiplier": 1.2,
                "model_exit_plan": {
                    "expected_exit_fee_rate": 0.0005,
                    "expected_exit_slippage_bps": 2.5,
                },
                "exit_recommendation": {
                    "recommended_exit_mode": "risk",
                    "recommended_exit_mode_source": "execution_backtest_grid_search_compare",
                    "recommended_exit_mode_reason_code": "RISK_EXECUTION_COMPARE_EDGE",
                    "chosen_family": "risk",
                    "chosen_rule_id": "risk_h6_rv_36_tp2p5_sl1p5_tr0p75",
                    "hold_family_status": "supported",
                    "risk_family_status": "supported",
                    "family_compare_status": "supported",
                },
                "trade_action": {
                    "recommended_action": "risk",
                    "expected_edge": 0.0123,
                    "expected_downside_deviation": 0.0045,
                    "expected_es": 0.0061,
                    "expected_ctm": 0.000041,
                    "expected_ctm_order": 2,
                    "expected_action_value": 1.7,
                    "decision_source": "continuous_conditional_action_value",
                    "recommended_notional_multiplier": 1.2,
                },
            }
        },
        "admissibility": {
            "snapshot": {
                "bid_fee": 0.0005,
                "ask_fee": 0.0005,
            },
            "decision": {
                "expected_net_edge_bps": 98.7,
            }
        },
        "execution": {
            "initial_ref_price": 99.8,
            "requested_price": 100.0,
        },
        "submit_result": {"accepted": True, "order_uuid": "entry-order-1"},
    }

    with LiveStateStore(db_path) as store:
        store.upsert_intent(
            IntentRecord(
                intent_id="intent-1",
                ts_ms=1_000,
                market="KRW-BTC",
                side="bid",
                price=100.0,
                volume=1.0,
                reason_code="MODEL_ALPHA_ENTRY_V1",
                meta_json=json.dumps(meta_payload, ensure_ascii=False, sort_keys=True),
                status="SUBMITTED",
            )
        )
        record_entry_submission(
            store=store,
            market="KRW-BTC",
            intent_id="intent-1",
            requested_price=100.0,
            requested_volume=1.0,
            reason_code="MODEL_ALPHA_ENTRY_V1",
            meta_payload=meta_payload,
            ts_ms=1_000,
            order_uuid="entry-order-1",
        )
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="plan-1",
                market="KRW-BTC",
                side="long",
                entry_price_str="100",
                qty_str="1",
                state="ACTIVE",
                created_ts=1_000,
                updated_ts=1_200,
                plan_source="model_alpha_v1",
                source_intent_id="intent-1",
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="entry-order-1",
                identifier="entry-order-1",
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price=100.0,
                volume_req=1.0,
                volume_filled=1.0,
                state="done",
                created_ts=1_000,
                updated_ts=1_100,
                intent_id="intent-1",
                local_state="DONE",
                raw_exchange_state="done",
                last_event_name="ORDER_STATE",
                event_source="test",
                root_order_uuid="entry-order-1",
                executed_funds=100.0,
                paid_fee=0.05,
            )
        )
        activate_trade_journal_for_position(
            store=store,
            market="KRW-BTC",
            position={"market": "KRW-BTC", "base_amount": 1.0, "avg_entry_price": 100.0, "updated_ts": 1_200},
            ts_ms=1_200,
            entry_intent={"intent_id": "intent-1", "created_ts": 1_000, "order_uuid": "entry-order-1"},
            plan_id="plan-1",
        )
        store.upsert_order(
            OrderRecord(
                uuid="exit-order-1",
                identifier="exit-order-1",
                market="KRW-BTC",
                side="ask",
                ord_type="limit",
                price=103.0,
                volume_req=1.0,
                volume_filled=1.0,
                state="done",
                created_ts=1_800,
                updated_ts=1_900,
                intent_id="exit-intent-1",
                tp_sl_link="plan-1",
                local_state="DONE",
                raw_exchange_state="done",
                last_event_name="ORDER_STATE",
                event_source="test",
                root_order_uuid="exit-order-1",
                executed_funds=103.0,
                paid_fee=0.0515,
            )
        )
        close_trade_journal_for_market(
            store=store,
            market="KRW-BTC",
            position={"market": "KRW-BTC", "base_amount": 1.0, "avg_entry_price": 100.0, "updated_ts": 1_200},
            ts_ms=2_000,
        )
        rows = store.list_trade_journal()

    assert len(rows) == 1
    row = rows[0]
    assert row["status"] == "CLOSED"
    assert row["entry_intent_id"] == "intent-1"
    assert row["entry_order_uuid"] == "entry-order-1"
    assert row["exit_order_uuid"] == "exit-order-1"
    assert row["plan_id"] == "plan-1"
    assert row["trade_action"] == "risk"
    assert row["expected_edge_bps"] == 123.0
    assert row["expected_downside_bps"] == 45.0
    assert row["expected_net_edge_bps"] == 98.7
    assert row["realized_pnl_quote"] == pytest.approx(2.8985)
    assert row["realized_pnl_pct"] == pytest.approx(2.8970514742628906)
    assert row["close_mode"] == "managed_exit_order"
    assert row["entry_meta"]["execution"]["initial_ref_price"] == 99.8
    assert row["entry_meta"]["strategy"]["meta"]["trade_action"]["expected_es"] == 0.0061
    assert row["entry_meta"]["strategy"]["meta"]["trade_action"]["expected_ctm"] == 0.000041
    assert row["entry_meta"]["strategy"]["meta"]["trade_action"]["decision_source"] == "continuous_conditional_action_value"
    assert row["entry_meta"]["strategy"]["meta"]["exit_recommendation"]["chosen_family"] == "risk"
    assert row["entry_meta"]["strategy"]["meta"]["exit_recommendation"]["chosen_rule_id"] == "risk_h6_rv_36_tp2p5_sl1p5_tr0p75"
    assert row["exit_meta"]["gross_pnl_quote"] == pytest.approx(3.0)
    assert row["exit_meta"]["total_fee_quote"] == pytest.approx(0.1015)
    assert row["exit_meta"]["entry_realized_slippage_bps"] == pytest.approx(20.04008016032014)


def test_backfill_order_execution_details_updates_done_order_settlement_fields(tmp_path) -> None:
    class _Client:
        def order(self, *, uuid=None, identifier=None):  # noqa: ANN001, ANN201
            assert uuid == "order-done-1"
            return {
                "uuid": uuid,
                "identifier": identifier,
                "market": "KRW-BTC",
                "state": "done",
                "price": "100",
                "volume": "1",
                "executed_volume": "1",
                "paid_fee": "0.05",
                "reserved_fee": "0.05",
                "remaining_fee": "0",
                "trades": [{"funds": "100"}],
            }

    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.upsert_order(
            OrderRecord(
                uuid="order-done-1",
                identifier="identifier-1",
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price=100.0,
                volume_req=1.0,
                volume_filled=1.0,
                state="done",
                created_ts=1000,
                updated_ts=1100,
                local_state="DONE",
                raw_exchange_state="done",
                last_event_name="ORDER_STATE",
                event_source="test",
                root_order_uuid="order-done-1",
            )
        )
        report = backfill_order_execution_details(store=store, client=_Client())
        order = store.order_by_uuid(uuid="order-done-1")

    assert report["orders_updated"] == 1
    assert order is not None
    assert order["executed_funds"] == 100.0
    assert order["paid_fee"] == 0.05
    assert order["reserved_fee"] == 0.05


def test_activate_trade_journal_does_not_reuse_other_open_journal_on_same_market(tmp_path) -> None:
    meta_payload = {
        "strategy": {
            "meta": {
                "model_prob": 0.77,
                "selection_policy_mode": "rank_effective_quantile",
                "trade_action": {"recommended_action": "hold"},
            }
        }
    }

    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.upsert_trade_journal(
            TradeJournalRecord(
                journal_id="journal-old",
                market="KRW-BSV",
                status="OPEN",
                entry_intent_id="intent-old",
                entry_order_uuid="order-old",
                plan_id="plan-old",
                entry_submitted_ts_ms=1_000,
                entry_filled_ts_ms=1_100,
                entry_price=22000.0,
                qty=0.2,
                entry_notional_quote=4400.0,
                entry_reason_code="MODEL_ALPHA_ENTRY_V1",
                updated_ts=1_100,
            )
        )
        store.upsert_intent(
            IntentRecord(
                intent_id="intent-new",
                ts_ms=2_000,
                market="KRW-BSV",
                side="bid",
                price=22100.0,
                volume=0.25,
                reason_code="MODEL_ALPHA_ENTRY_V1",
                meta_json=json.dumps(meta_payload, ensure_ascii=False, sort_keys=True),
                status="SUBMITTED",
            )
        )
        activate_trade_journal_for_position(
            store=store,
            market="KRW-BSV",
            position={"market": "KRW-BSV", "base_amount": 0.25, "avg_entry_price": 22100.0, "updated_ts": 2_100},
            ts_ms=2_100,
            entry_intent={"intent_id": "intent-new", "created_ts": 2_000, "order_uuid": "order-new"},
            plan_id="plan-new",
        )
        rows = sorted(store.list_trade_journal(), key=lambda item: str(item["journal_id"]))

    assert len(rows) == 2
    assert rows[0]["journal_id"] == "intent-new"
    assert rows[0]["plan_id"] == "plan-new"
    assert rows[0]["entry_intent_id"] == "intent-new"
    assert rows[1]["journal_id"] == "journal-old"
    assert rows[1]["plan_id"] == "plan-old"
    assert rows[1]["entry_intent_id"] == "intent-old"


def test_cancel_pending_entry_journal_marks_cancelled_entry(tmp_path) -> None:
    with LiveStateStore(tmp_path / "live_state.db") as store:
        record_entry_submission(
            store=store,
            market="KRW-DOGE",
            intent_id="intent-doge-1",
            requested_price=134.0,
            requested_volume=41.9,
            reason_code="MODEL_ALPHA_ENTRY_V1",
            meta_payload={"strategy": {"meta": {"model_prob": 0.79}}},
            ts_ms=1_000,
            order_uuid="doge-order-1",
        )

        journal_id = cancel_pending_entry_journal(
            store=store,
            market="KRW-DOGE",
            ts_ms=2_000,
            entry_intent_id="intent-doge-1",
            entry_order_uuid="doge-order-1",
            close_reason_code="MAX_REPLACES_REACHED",
            close_mode="entry_order_timeout",
        )
        row = store.trade_journal_by_entry_intent(entry_intent_id="intent-doge-1")

    assert journal_id == "intent-doge-1"
    assert row is not None
    assert row["status"] == "CANCELLED_ENTRY"
    assert row["entry_order_uuid"] == "doge-order-1"
    assert row["close_reason_code"] == "MAX_REPLACES_REACHED"
    assert row["close_mode"] == "entry_order_timeout"
    assert row["exit_ts_ms"] == 2_000


def test_rebind_pending_entry_journal_order_moves_pending_entry_to_replaced_order(tmp_path) -> None:
    with LiveStateStore(tmp_path / "live_state.db") as store:
        record_entry_submission(
            store=store,
            market="KRW-DOGE",
            intent_id="intent-doge-2",
            requested_price=134.0,
            requested_volume=41.9,
            reason_code="MODEL_ALPHA_ENTRY_V1",
            meta_payload={"strategy": {"meta": {"model_prob": 0.79}}},
            ts_ms=1_000,
            order_uuid="doge-order-old",
        )

        journal_id = rebind_pending_entry_journal_order(
            store=store,
            entry_intent_id="intent-doge-2",
            previous_entry_order_uuid="doge-order-old",
            new_entry_order_uuid="doge-order-new",
            ts_ms=2_000,
        )
        row = store.trade_journal_by_entry_intent(entry_intent_id="intent-doge-2")

    assert journal_id == "intent-doge-2"
    assert row is not None
    assert row["status"] == "PENDING_ENTRY"
    assert row["entry_order_uuid"] == "doge-order-new"
    assert row["updated_ts"] == 2_000
