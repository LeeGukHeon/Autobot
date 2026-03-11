from __future__ import annotations

import json

import pytest

from autobot.live.state_store import IntentRecord, LiveStateStore, OrderRecord, RiskPlanRecord
from autobot.live.trade_journal import (
    activate_trade_journal_for_position,
    close_trade_journal_for_market,
    record_entry_submission,
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
                "trade_action": {
                    "recommended_action": "risk",
                    "expected_edge": 0.0123,
                    "expected_downside_deviation": 0.0045,
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
    assert row["exit_meta"]["gross_pnl_quote"] == pytest.approx(3.0)
    assert row["exit_meta"]["total_fee_quote"] == pytest.approx(0.1015)
    assert row["exit_meta"]["entry_realized_slippage_bps"] == pytest.approx(20.04008016032014)
