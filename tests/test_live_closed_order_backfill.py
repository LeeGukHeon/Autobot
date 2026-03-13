from __future__ import annotations

import json
import pytest

from autobot.live.closed_order_backfill import backfill_recent_bot_closed_orders
from autobot.live.reconcile import reconcile_exchange_snapshot
from autobot.live.state_store import IntentRecord, LiveStateStore, OrderRecord, RiskPlanRecord, TradeJournalRecord


class _StubClosedOrdersClient:
    def __init__(self, payload):  # noqa: ANN001
        self.payload = payload
        self.calls = []

    def closed_orders(self, **kwargs):  # noqa: ANN003, ANN201
        self.calls.append(dict(kwargs))
        return list(self.payload)


def test_backfill_recent_bot_closed_orders_upserts_closed_order_and_recomputes(tmp_path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_order(
            OrderRecord(
                uuid="entry-order-1",
                identifier="AUTOBOT-autobot-001-intent-entry-1700000000000-a",
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price=100.0,
                volume_req=1.0,
                volume_filled=1.0,
                state="done",
                created_ts=1_000,
                updated_ts=1_100,
                intent_id="intent-entry",
                local_state="DONE",
                raw_exchange_state="done",
                last_event_name="ORDER_STATE",
                event_source="test",
                replace_seq=0,
                root_order_uuid="entry-order-1",
                executed_funds=100.0,
                paid_fee=0.05,
            )
        )
        store.upsert_trade_journal(
            TradeJournalRecord(
                journal_id="journal-1",
                market="KRW-BTC",
                status="CLOSED",
                entry_intent_id="intent-entry",
                entry_order_uuid="entry-order-1",
                exit_order_uuid="exit-order-1",
                plan_id="plan-1",
                entry_submitted_ts_ms=1_000,
                entry_filled_ts_ms=1_100,
                exit_ts_ms=2_000,
                entry_price=100.0,
                exit_price=None,
                qty=1.0,
                entry_notional_quote=100.0,
                exit_notional_quote=None,
                realized_pnl_quote=None,
                realized_pnl_pct=None,
                entry_reason_code="MODEL_ALPHA_ENTRY_V1",
                close_reason_code="POSITION_CLOSED",
                close_mode="missing_on_exchange_after_exit_plan",
                entry_meta_json=json.dumps(
                    {
                        "admissibility": {"sizing": {"fee_rate": 0.0005}, "snapshot": {"bid_fee": 0.0005, "ask_fee": 0.0005}},
                        "execution": {"requested_price": 100.0},
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                updated_ts=2_000,
            )
        )

        client = _StubClosedOrdersClient(
            [
                {
                    "uuid": "exit-order-1",
                    "identifier": "AUTOBOT-autobot-001-intent-exit-1700000000001-a-rid_run-123",
                    "market": "KRW-BTC",
                    "side": "ask",
                    "ord_type": "limit",
                    "state": "done",
                    "price": "103",
                    "volume": "1",
                    "executed_volume": "1",
                    "executed_funds": "103",
                    "paid_fee": "0.0515",
                    "created_at": "2026-03-13T00:10:00Z",
                    "done_at": "2026-03-13T00:11:00Z",
                }
            ]
        )

        report = backfill_recent_bot_closed_orders(
            store=store,
            client=client,
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            now_ts_ms=1_700_000_200_000,
        )
        order = store.order_by_uuid(uuid="exit-order-1")
        journal = store.trade_journal_by_id(journal_id="journal-1")

    assert report["supported"] is True
    assert report["orders_upserted"] == 1
    assert client.calls
    assert order is not None
    assert order["local_state"] == "DONE"
    assert journal is not None
    assert journal["realized_pnl_quote"] == pytest.approx(2.8985)
    assert journal["exit_price"] == 103.0


def test_backfill_recent_bot_closed_orders_accepts_tracked_risk_exit_without_bot_identifier(tmp_path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_order(
            OrderRecord(
                uuid="entry-order-1",
                identifier="AUTOBOT-autobot-candidate-001-intent-enso-1700000000000-a",
                market="KRW-ENSO",
                side="bid",
                ord_type="limit",
                price=1872.0,
                volume_req=3.0,
                volume_filled=3.0,
                state="done",
                created_ts=1_000,
                updated_ts=1_100,
                intent_id="intent-enso",
                local_state="DONE",
                raw_exchange_state="done",
                last_event_name="ORDER_STATE",
                event_source="test",
                replace_seq=0,
                root_order_uuid="entry-order-1",
                executed_funds=5616.0,
                paid_fee=2.808,
            )
        )
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="plan-enso",
                market="KRW-ENSO",
                side="long",
                entry_price_str="1872.0",
                qty_str="3.0",
                tp_enabled=True,
                tp_pct=3.5,
                sl_enabled=False,
                trailing_enabled=False,
                timeout_ts_ms=2_000,
                state="EXITING",
                last_eval_ts_ms=1_900,
                last_action_ts_ms=1_950,
                current_exit_order_uuid="exit-order-enso",
                current_exit_order_identifier="AUTOBOT-RISK-model-risk-1773370244893",
                replace_attempt=0,
                created_ts=1_000,
                updated_ts=1_950,
                plan_source="model_alpha_v1",
                source_intent_id="intent-enso",
            )
        )
        store.upsert_trade_journal(
            TradeJournalRecord(
                journal_id="journal-enso",
                market="KRW-ENSO",
                status="CLOSED",
                entry_intent_id="intent-enso",
                entry_order_uuid="entry-order-1",
                exit_order_uuid="exit-order-enso",
                plan_id="plan-enso",
                entry_submitted_ts_ms=1_000,
                entry_filled_ts_ms=1_100,
                exit_ts_ms=2_000,
                entry_price=1872.0,
                exit_price=None,
                qty=3.0,
                entry_notional_quote=5616.0,
                exit_notional_quote=None,
                realized_pnl_quote=None,
                realized_pnl_pct=None,
                entry_reason_code="MODEL_ALPHA_ENTRY_V1",
                close_reason_code="POSITION_CLOSED",
                close_mode="missing_on_exchange_after_exit_plan",
                entry_meta_json=json.dumps(
                    {
                        "admissibility": {"sizing": {"fee_rate": 0.0005}, "snapshot": {"bid_fee": 0.0005, "ask_fee": 0.0005}},
                        "execution": {"requested_price": 1872.0},
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                updated_ts=2_000,
            )
        )

        client = _StubClosedOrdersClient(
            [
                {
                    "uuid": "exit-order-enso",
                    "identifier": "AUTOBOT-RISK-model-risk-1773370244893",
                    "market": "KRW-ENSO",
                    "side": "ask",
                    "ord_type": "limit",
                    "state": "done",
                    "price": "1929",
                    "volume": "3",
                    "executed_volume": "3",
                    "executed_funds": "5790",
                    "paid_fee": "2.895",
                    "created_at": "2026-03-13T02:50:44Z",
                    "done_at": "2026-03-13T02:50:44Z",
                }
            ]
        )

        report = backfill_recent_bot_closed_orders(
            store=store,
            client=client,
            bot_id="autobot-candidate-001",
            identifier_prefix="AUTOBOT",
            now_ts_ms=1_700_000_200_000,
        )
        order = store.order_by_uuid(uuid="exit-order-enso")
        journal = store.trade_journal_by_id(journal_id="journal-enso")

    assert report["orders_upserted"] == 1
    assert order is not None
    assert order["local_state"] == "DONE"
    assert journal is not None
    assert journal["realized_pnl_quote"] is not None


def test_backfill_recent_bot_closed_orders_accepts_tracked_exit_uuid_from_journal(tmp_path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_order(
            OrderRecord(
                uuid="entry-order-j",
                identifier="AUTOBOT-autobot-candidate-001-intent-j-1700000000000-a",
                market="KRW-ENSO",
                side="bid",
                ord_type="limit",
                price=1872.0,
                volume_req=3.0,
                volume_filled=3.0,
                state="done",
                created_ts=1_000,
                updated_ts=1_100,
                intent_id="intent-j",
                local_state="DONE",
                raw_exchange_state="done",
                last_event_name="ORDER_STATE",
                event_source="test",
                replace_seq=0,
                root_order_uuid="entry-order-j",
                executed_funds=5616.0,
                paid_fee=2.808,
            )
        )
        store.upsert_trade_journal(
            TradeJournalRecord(
                journal_id="journal-j",
                market="KRW-ENSO",
                status="CLOSED",
                entry_intent_id="intent-j",
                entry_order_uuid="entry-order-j",
                exit_order_uuid="exit-order-j",
                plan_id="plan-j",
                entry_submitted_ts_ms=1_000,
                entry_filled_ts_ms=1_100,
                exit_ts_ms=2_000,
                entry_price=1872.0,
                exit_price=None,
                qty=3.0,
                entry_notional_quote=5616.0,
                exit_notional_quote=None,
                realized_pnl_quote=None,
                realized_pnl_pct=None,
                entry_reason_code="MODEL_ALPHA_ENTRY_V1",
                close_reason_code="POSITION_CLOSED",
                close_mode="missing_on_exchange_after_exit_plan",
                entry_meta_json=json.dumps(
                    {
                        "admissibility": {"sizing": {"fee_rate": 0.0005}, "snapshot": {"bid_fee": 0.0005, "ask_fee": 0.0005}},
                        "execution": {"requested_price": 1872.0},
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                updated_ts=2_000,
            )
        )
        client = _StubClosedOrdersClient(
            [
                {
                    "uuid": "exit-order-j",
                    "identifier": "AUTOBOT-RISK-journal-only",
                    "market": "KRW-ENSO",
                    "side": "ask",
                    "ord_type": "limit",
                    "state": "done",
                    "price": "1930",
                    "volume": "3",
                    "executed_volume": "3",
                    "executed_funds": "5790",
                    "paid_fee": "2.895",
                    "created_at": "2026-03-13T02:50:44Z",
                    "done_at": "2026-03-13T02:50:44Z",
                }
            ]
        )
        report = backfill_recent_bot_closed_orders(
            store=store,
            client=client,
            bot_id="autobot-candidate-001",
            identifier_prefix="AUTOBOT",
            now_ts_ms=1_700_000_200_000,
        )
        order = store.order_by_uuid(uuid="exit-order-j")
        journal = store.trade_journal_by_id(journal_id="journal-j")

    assert report["orders_upserted"] == 1
    assert order is not None
    assert journal is not None
    assert journal["realized_pnl_quote"] is not None


def test_backfill_recent_bot_closed_orders_preserves_existing_intent_contract_and_status(tmp_path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_intent(
            IntentRecord(
                intent_id="intent-awe",
                ts_ms=1_000,
                market="KRW-AWE",
                side="bid",
                price=77.0,
                volume=77.88313635,
                reason_code="MODEL_ALPHA_ENTRY_V1",
                status="SUBMITTED",
                meta_json=json.dumps(
                    {
                        "submit_result": {"accepted": True, "order_uuid": "entry-order-awe"},
                        "strategy": {
                            "meta": {
                                "model_exit_plan": {
                                    "source": "model_alpha_v1",
                                    "mode": "risk",
                                    "hold_bars": 6,
                                    "interval_ms": 300000,
                                    "timeout_delta_ms": 1800000,
                                    "tp_pct": 0.03,
                                    "sl_pct": 0.02,
                                    "trailing_pct": 0.0,
                                }
                            }
                        },
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="entry-order-awe",
                identifier="AUTOBOT-autobot-001-intent-awe-1000-a",
                market="KRW-AWE",
                side="bid",
                ord_type="limit",
                price=77.0,
                volume_req=77.88313635,
                volume_filled=77.88313635,
                state="done",
                created_ts=1_000,
                updated_ts=1_100,
                intent_id="intent-awe",
                local_state="DONE",
                raw_exchange_state="done",
                last_event_name="SUBMIT_ACCEPTED",
                event_source="test",
                root_order_uuid="entry-order-awe",
            )
        )
        client = _StubClosedOrdersClient(
            [
                {
                    "uuid": "entry-order-awe",
                    "identifier": "AUTOBOT-autobot-001-intent-awe-1000-a-rid_run-1",
                    "market": "KRW-AWE",
                    "side": "bid",
                    "ord_type": "limit",
                    "state": "done",
                    "price": "77",
                    "volume": "77.88313635",
                    "executed_volume": "77.88313635",
                    "executed_funds": "5997.0015",
                    "paid_fee": "2.9985",
                    "created_at": "2026-03-13T10:21:01Z",
                    "done_at": "2026-03-13T10:21:01Z",
                }
            ]
        )

        report = backfill_recent_bot_closed_orders(
            store=store,
            client=client,
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            now_ts_ms=1_700_000_200_000,
        )
        intent = store.intent_by_id(intent_id="intent-awe")

    assert report["orders_upserted"] == 1
    assert intent is not None
    assert intent["status"] == "SUBMITTED"
    assert intent["reason_code"] == "MODEL_ALPHA_ENTRY_V1"
    assert intent["meta"]["submit_result"]["accepted"] is True
    assert intent["meta"]["strategy"]["meta"]["model_exit_plan"]["mode"] == "risk"
    assert intent["meta"]["closed_orders_backfill"]["order_uuid"] == "entry-order-awe"


def test_backfill_then_reconcile_recovers_model_plan_for_filled_bid(tmp_path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_intent(
            IntentRecord(
                intent_id="intent-awe",
                ts_ms=1_000,
                market="KRW-AWE",
                side="bid",
                price=77.0,
                volume=77.88313635,
                reason_code="MODEL_ALPHA_ENTRY_V1",
                status="SUBMITTED",
                meta_json=json.dumps(
                    {
                        "submit_result": {"accepted": True, "order_uuid": "entry-order-awe"},
                        "strategy": {
                            "meta": {
                                "model_exit_plan": {
                                    "source": "model_alpha_v1",
                                    "mode": "risk",
                                    "hold_bars": 6,
                                    "interval_ms": 300000,
                                    "timeout_delta_ms": 1800000,
                                    "tp_pct": 0.03,
                                    "sl_pct": 0.02,
                                    "trailing_pct": 0.0,
                                }
                            }
                        },
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            )
        )
        client = _StubClosedOrdersClient(
            [
                {
                    "uuid": "entry-order-awe",
                    "identifier": "AUTOBOT-autobot-001-intent-awe-1000-a-rid_run-1",
                    "market": "KRW-AWE",
                    "side": "bid",
                    "ord_type": "limit",
                    "state": "done",
                    "price": "77",
                    "volume": "77.88313635",
                    "executed_volume": "77.88313635",
                    "executed_funds": "5997.0015",
                    "paid_fee": "2.9985",
                    "created_at": "2026-03-13T10:21:01Z",
                    "done_at": "2026-03-13T10:21:01Z",
                }
            ]
        )

        backfill_recent_bot_closed_orders(
            store=store,
            client=client,
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            now_ts_ms=1_700_000_200_000,
        )
        report = reconcile_exchange_snapshot(
            store=store,
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            accounts_payload=[
                {
                    "currency": "AWE",
                    "balance": "77.88313635",
                    "locked": "0",
                    "avg_buy_price": "76.9",
                }
            ],
            open_orders_payload=[],
            unknown_open_orders_policy="ignore",
            unknown_positions_policy="attach_strategy_risk",
            dry_run=False,
            ts_ms=5_000,
        )
        plans = store.list_risk_plans(market="KRW-AWE")

    assert report["halted"] is False
    assert any(item["type"] == "import_managed_position_from_bot_intent" for item in report["actions"])
    assert len(plans) == 1
    assert plans[0]["source_intent_id"] == "intent-awe"
    assert plans[0]["tp"]["tp_pct"] == pytest.approx(3.0)
    assert plans[0]["sl"]["sl_pct"] == pytest.approx(2.0)
