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
    recompute_trade_journal_records,
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


def test_close_trade_journal_reuses_existing_row_by_exit_order_uuid(tmp_path) -> None:
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.upsert_trade_journal(
            TradeJournalRecord(
                journal_id="journal-existing",
                market="KRW-DOGE",
                status="CLOSED",
                entry_intent_id="intent-existing",
                entry_order_uuid="entry-order-existing",
                exit_order_uuid="exit-order-existing",
                plan_id="plan-existing",
                entry_submitted_ts_ms=1_000,
                entry_filled_ts_ms=1_100,
                exit_ts_ms=1_900,
                entry_price=138.0,
                exit_price=138.0,
                qty=40.2113436,
                entry_notional_quote=5550.0,
                exit_notional_quote=5544.4508345832,
                realized_pnl_quote=-5.5491654168,
                realized_pnl_pct=-0.1,
                entry_reason_code="MODEL_ALPHA_ENTRY_V1",
                close_reason_code="ORDER_STATE",
                close_mode="managed_exit_order",
                updated_ts=1_900,
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="exit-order-existing",
                identifier="exit-order-existing",
                market="KRW-DOGE",
                side="ask",
                ord_type="limit",
                price=138.0,
                volume_req=40.2113436,
                volume_filled=40.2113436,
                state="done",
                created_ts=1_800,
                updated_ts=1_900,
                local_state="DONE",
                raw_exchange_state="done",
                last_event_name="ORDER_STATE",
                event_source="test",
                root_order_uuid="exit-order-existing",
                executed_funds=5549.1654168,
                paid_fee=0.0,
            )
        )

        journal_id = close_trade_journal_for_market(
            store=store,
            market="KRW-DOGE",
            position={"market": "KRW-DOGE", "base_amount": 40.2113436, "avg_entry_price": 138.0, "updated_ts": 1_900},
            ts_ms=1_950,
            exit_order_uuid="exit-order-existing",
            plan_id=None,
        )
        rows = store.list_trade_journal(statuses=("CLOSED",))

    assert journal_id == "journal-existing"
    assert len(rows) == 1
    assert rows[0]["journal_id"] == "journal-existing"


def test_close_trade_journal_reuses_matching_open_row_before_importing_position_sync(tmp_path) -> None:
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.upsert_trade_journal(
            TradeJournalRecord(
                journal_id="journal-open",
                market="KRW-NOM",
                status="OPEN",
                entry_intent_id="intent-nom",
                entry_order_uuid="entry-order-nom",
                exit_order_uuid=None,
                plan_id=None,
                entry_submitted_ts_ms=1_000,
                entry_filled_ts_ms=1_100,
                entry_price=7.49,
                qty=751.77764922,
                entry_notional_quote=5633.63,
                updated_ts=1_100,
            )
        )

        journal_id = close_trade_journal_for_market(
            store=store,
            market="KRW-NOM",
            position={"market": "KRW-NOM", "base_amount": 751.77764922, "avg_entry_price": 7.49, "updated_ts": 2_000},
            ts_ms=2_000,
            exit_price=7.73,
            plan_id=None,
        )
        rows = store.list_trade_journal(statuses=("CLOSED",), market="KRW-NOM")

    assert journal_id == "journal-open"
    assert len(rows) == 1
    assert rows[0]["journal_id"] == "journal-open"
    assert rows[0]["realized_pnl_quote"] is None
    assert rows[0]["exit_price"] is None
    assert rows[0]["exit_meta"]["close_verified"] is False
    assert rows[0]["exit_meta"]["close_verification_status"] == "unverified_position_sync"
    assert rows[0]["exit_meta"]["observed_exit_price"] == 7.73


def test_recompute_trade_journal_records_clears_unverified_close_pnl(tmp_path) -> None:
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.upsert_trade_journal(
            TradeJournalRecord(
                journal_id="journal-unverified",
                market="KRW-NOM",
                status="CLOSED",
                entry_intent_id="intent-nom",
                entry_order_uuid="entry-order-nom",
                exit_order_uuid="exit-order-missing",
                plan_id="plan-nom",
                entry_submitted_ts_ms=1_000,
                entry_filled_ts_ms=1_100,
                exit_ts_ms=2_000,
                entry_price=7.49,
                exit_price=7.73,
                qty=751.77764922,
                entry_notional_quote=5633.63,
                exit_notional_quote=5811.24,
                realized_pnl_quote=177.61,
                realized_pnl_pct=3.15,
                close_reason_code="POSITION_CLOSED",
                close_mode="missing_on_exchange_after_exit_plan",
                updated_ts=2_000,
            )
        )
        compact_report = recompute_trade_journal_records(store=store)
        row = store.trade_journal_by_id(journal_id="journal-unverified")

    assert compact_report["rows_updated"] == 1
    assert row is not None
    assert row["realized_pnl_quote"] is None
    assert row["realized_pnl_pct"] is None
    assert row["exit_notional_quote"] is None
    assert row["exit_price"] is None
    assert row["exit_meta"]["close_verified"] is False
    assert row["exit_meta"]["close_verification_status"] == "unverified_missing_exit_order"
    assert row["exit_meta"]["observed_exit_price"] == 7.73


def test_recompute_trade_journal_records_preserves_verified_exit_timestamp(tmp_path) -> None:
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.upsert_order(
            OrderRecord(
                uuid="entry-order-verified",
                identifier="AUTOBOT-entry-verified",
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price=100.0,
                volume_req=1.0,
                volume_filled=1.0,
                state="done",
                created_ts=1000,
                updated_ts=1100,
                intent_id="intent-verified",
                local_state="DONE",
                raw_exchange_state="done",
                last_event_name="ORDER_STATE",
                event_source="test",
                root_order_uuid="entry-order-verified",
                executed_funds=100.0,
                paid_fee=0.05,
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="exit-order-verified",
                identifier="AUTOBOT-exit-verified",
                market="KRW-BTC",
                side="ask",
                ord_type="limit",
                price=103.0,
                volume_req=1.0,
                volume_filled=1.0,
                state="done",
                created_ts=1900,
                updated_ts=999999,
                intent_id="intent-exit-verified",
                local_state="DONE",
                raw_exchange_state="done",
                last_event_name="ORDER_STATE",
                event_source="test",
                root_order_uuid="exit-order-verified",
                executed_funds=103.0,
                paid_fee=0.0515,
            )
        )
        store.upsert_trade_journal(
            TradeJournalRecord(
                journal_id="journal-verified",
                market="KRW-BTC",
                status="CLOSED",
                entry_intent_id="intent-verified",
                entry_order_uuid="entry-order-verified",
                exit_order_uuid="exit-order-verified",
                plan_id="plan-verified",
                entry_submitted_ts_ms=1000,
                entry_filled_ts_ms=1100,
                exit_ts_ms=1900,
                entry_price=100.0,
                exit_price=103.0,
                qty=1.0,
                entry_notional_quote=100.05,
                exit_notional_quote=102.9485,
                realized_pnl_quote=2.8985,
                realized_pnl_pct=2.8970514742628906,
                entry_reason_code="MODEL_ALPHA_ENTRY_V1",
                close_reason_code="ORDER_STATE",
                close_mode="managed_exit_order",
                entry_meta_json=json.dumps(
                    {
                        "admissibility": {"sizing": {"fee_rate": 0.0005}, "snapshot": {"bid_fee": 0.0005, "ask_fee": 0.0005}},
                        "execution": {"requested_price": 100.0},
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                exit_meta_json=json.dumps({"close_verified": True, "close_verification_status": "verified_exit_order"}, ensure_ascii=False, sort_keys=True),
                updated_ts=1900,
            )
        )

        recompute_trade_journal_records(store=store)
        row = store.trade_journal_by_id(journal_id="journal-verified")

    assert row is not None
    assert row["exit_ts_ms"] == 1900


def test_recompute_trade_journal_records_corrects_late_stale_verified_exit_timestamp(tmp_path) -> None:
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.upsert_order(
            OrderRecord(
                uuid="entry-order-stale",
                identifier="AUTOBOT-entry-stale",
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price=100.0,
                volume_req=1.0,
                volume_filled=1.0,
                state="done",
                created_ts=1000,
                updated_ts=1100,
                intent_id="intent-stale",
                local_state="DONE",
                raw_exchange_state="done",
                last_event_name="ORDER_STATE",
                event_source="test",
                root_order_uuid="entry-order-stale",
                executed_funds=100.0,
                paid_fee=0.05,
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="exit-order-stale",
                identifier="AUTOBOT-exit-stale",
                market="KRW-BTC",
                side="ask",
                ord_type="limit",
                price=103.0,
                volume_req=1.0,
                volume_filled=1.0,
                state="done",
                created_ts=1800,
                updated_ts=1900,
                intent_id="intent-exit-stale",
                local_state="DONE",
                raw_exchange_state="done",
                last_event_name="ORDER_STATE",
                event_source="test",
                root_order_uuid="exit-order-stale",
                executed_funds=103.0,
                paid_fee=0.0515,
            )
        )
        store.upsert_trade_journal(
            TradeJournalRecord(
                journal_id="journal-stale",
                market="KRW-BTC",
                status="CLOSED",
                entry_intent_id="intent-stale",
                entry_order_uuid="entry-order-stale",
                exit_order_uuid="exit-order-stale",
                plan_id="plan-stale",
                entry_submitted_ts_ms=1000,
                entry_filled_ts_ms=1100,
                exit_ts_ms=5000,
                entry_price=100.0,
                exit_price=103.0,
                qty=1.0,
                entry_notional_quote=100.05,
                exit_notional_quote=102.9485,
                realized_pnl_quote=2.8985,
                realized_pnl_pct=2.8970514742628906,
                entry_reason_code="MODEL_ALPHA_ENTRY_V1",
                close_reason_code="ORDER_STATE",
                close_mode="managed_exit_order",
                entry_meta_json=json.dumps(
                    {
                        "admissibility": {"sizing": {"fee_rate": 0.0005}, "snapshot": {"bid_fee": 0.0005, "ask_fee": 0.0005}},
                        "execution": {"requested_price": 100.0},
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                exit_meta_json=json.dumps({"close_verified": True, "close_verification_status": "verified_exit_order"}, ensure_ascii=False, sort_keys=True),
                updated_ts=5000,
            )
        )

        recompute_trade_journal_records(store=store)
        row = store.trade_journal_by_id(journal_id="journal-stale")

    assert row is not None
    assert row["exit_ts_ms"] == 1900


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


def test_recompute_trade_journal_records_compacts_cancelled_pending_entry(tmp_path) -> None:
    with LiveStateStore(tmp_path / "live_state.db") as store:
        record_entry_submission(
            store=store,
            market="KRW-DOGE",
            intent_id="intent-doge-3",
            requested_price=134.0,
            requested_volume=41.9,
            reason_code="MODEL_ALPHA_ENTRY_V1",
            meta_payload={"strategy": {"meta": {"model_prob": 0.79}}},
            ts_ms=1_000,
            order_uuid="doge-order-3",
        )
        store.upsert_order(
            OrderRecord(
                uuid="doge-order-3",
                identifier="doge-order-3",
                market="KRW-DOGE",
                side="bid",
                ord_type="limit",
                price=134.0,
                volume_req=41.9,
                volume_filled=0.0,
                state="cancel",
                created_ts=1_000,
                updated_ts=2_000,
                intent_id="intent-doge-3",
                local_state="CANCELLED",
                raw_exchange_state="cancel",
                last_event_name="ORDER_TIMEOUT",
                event_source="test",
                root_order_uuid="doge-order-3",
            )
        )

        assert store.list_trade_journal()[0]["status"] == "PENDING_ENTRY"
        compact_report = recompute_trade_journal_records(store=store)
        row = store.trade_journal_by_entry_intent(entry_intent_id="intent-doge-3")

    assert compact_report["rows_compacted"] == 1
    assert row is not None
    assert row["status"] == "CANCELLED_ENTRY"
    assert row["close_reason_code"] == "ORDER_TIMEOUT"
    assert row["close_mode"] == "entry_order_timeout"
    assert row["exit_ts_ms"] == 2_000
