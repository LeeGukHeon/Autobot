from __future__ import annotations

from pathlib import Path
import json
from types import SimpleNamespace

from autobot.live.model_alpha_projection import find_latest_model_entry_intent
from autobot.live.state_store import (
    IntentRecord,
    LiveStateStore,
    OrderLineageRecord,
    OrderRecord,
    PositionRecord,
    RiskPlanRecord,
    TradeJournalRecord,
)
from autobot.live.trade_journal import record_entry_submission
from autobot.live.ws_handlers import apply_private_ws_event
from autobot.risk.live_risk_manager import LiveRiskManager
from autobot.risk.models import RiskManagerConfig
from autobot.upbit.ws import MyAssetEvent, MyOrderEvent


class _RiskExitGateway:
    def submit_intent(self, *, intent, identifier: str, meta_json: str):  # noqa: ANN001, ANN201
        _ = meta_json
        return SimpleNamespace(
            accepted=True,
            reason="accepted",
            upbit_uuid="exit-order-from-risk",
            identifier=identifier,
        )


def test_ws_order_event_upserts_order_and_intent(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        action = apply_private_ws_event(
            store=store,
            event=MyOrderEvent(
                ts_ms=1700000000000,
                uuid="ws-order-1",
                identifier="AUTOBOT-autobot-001-intent-1-1-a",
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                state="wait",
                price=100000000.0,
                volume=0.01,
                executed_volume=0.0,
            ),
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            quote_currency="KRW",
        )
        order = store.order_by_uuid(uuid="ws-order-1")
        intents = store.list_intents()

    assert action["type"] == "ws_order_upsert"
    assert order is not None
    assert order["intent_id"] is not None
    assert order["local_state"] == "OPEN"
    assert len(intents) == 1
    assert intents[0]["status"] in {"INFERRED_FROM_EXCHANGE", "UPDATED_FROM_WS"}
    assert intents[0]["intent_id"] == "intent-1"


def test_ws_order_event_skips_unknown_external_order(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        action = apply_private_ws_event(
            store=store,
            event=MyOrderEvent(
                ts_ms=1700000000000,
                uuid="manual-order-1",
                identifier="MANUAL-order-1",
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                state="wait",
                price=100000000.0,
                volume=0.01,
                executed_volume=0.0,
            ),
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            quote_currency="KRW",
        )

    assert action["type"] == "ws_order_skip"
    assert action["reason"] == "external_order"


def test_ws_order_event_preserves_existing_intent_execution_meta(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_intent(
            IntentRecord(
                intent_id="intent-1",
                ts_ms=1699999999000,
                market="KRW-BTC",
                side="bid",
                price=100000000.0,
                volume=0.01,
                reason_code="MODEL_ALPHA_ENTRY_V1",
                meta_json=json.dumps(
                    {
                        "execution": {
                            "requested_price": 100000000.0,
                            "initial_ref_price": 100100000.0,
                            "exec_profile": {
                                "timeout_ms": 45000,
                                "replace_interval_ms": 15000,
                                "max_replaces": 3,
                                "price_mode": "JOIN",
                                "max_chase_bps": 15,
                                "min_replace_interval_ms_global": 1500,
                            },
                        },
                        "strategy": {"meta": {"model_prob": 0.81}},
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                status="SUBMITTED",
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="ws-order-1",
                identifier="AUTOBOT-autobot-001-intent-1-1-a",
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price=100000000.0,
                volume_req=0.01,
                volume_filled=0.0,
                state="wait",
                created_ts=1699999999000,
                updated_ts=1699999999000,
                intent_id="intent-1",
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="SUBMIT_ACCEPTED",
                event_source="test",
                replace_seq=0,
                root_order_uuid="ws-order-1",
            )
        )

        action = apply_private_ws_event(
            store=store,
            event=MyOrderEvent(
                ts_ms=1700000000000,
                uuid="ws-order-1",
                identifier="AUTOBOT-autobot-001-intent-1-1-a",
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                state="wait",
                price=100000000.0,
                volume=0.01,
                executed_volume=0.0,
            ),
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            quote_currency="KRW",
        )
        intent = store.intent_by_id(intent_id="intent-1")

    assert action["type"] == "ws_order_upsert"
    assert intent is not None
    assert intent["status"] == "UPDATED_FROM_WS"
    assert intent["reason_code"] == "MODEL_ALPHA_ENTRY_V1"
    assert intent["meta"]["execution"]["exec_profile"]["replace_interval_ms"] == 15000
    assert intent["meta"]["strategy"]["meta"]["model_prob"] == 0.81
    assert intent["meta"]["source"] == "private_ws"
    assert intent["meta"]["order_uuid"] == "ws-order-1"
    assert intent["ts_ms"] == 1699999999000


def test_ws_order_event_maps_upbit_trade_state_to_partial(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_intent(
            IntentRecord(
                intent_id="intent-trade-state",
                ts_ms=1_000,
                market="KRW-BTC",
                side="bid",
                price=100000000.0,
                volume=0.01,
                reason_code="MODEL_ALPHA_ENTRY_V1",
                meta_json=json.dumps({}, ensure_ascii=False, sort_keys=True),
                status="SUBMITTED",
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="ws-order-trade-state",
                identifier="AUTOBOT-autobot-001-intent-trade-state-1-a",
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price=100000000.0,
                volume_req=0.01,
                volume_filled=0.0,
                state="wait",
                created_ts=1_000,
                updated_ts=1_000,
                intent_id="intent-trade-state",
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="ORDER_ACCEPTED",
                event_source="test",
                replace_seq=0,
                root_order_uuid="ws-order-trade-state",
            )
        )

        action = apply_private_ws_event(
            store=store,
            event=MyOrderEvent(
                ts_ms=2_000,
                uuid="ws-order-trade-state",
                identifier="AUTOBOT-autobot-001-intent-trade-state-1-a",
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                state="trade",
                price=100000000.0,
                volume=0.01,
                executed_volume=0.004,
            ),
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            quote_currency="KRW",
        )
        order = store.order_by_uuid(uuid="ws-order-trade-state")

    assert action["type"] == "ws_order_upsert"
    assert order is not None
    assert order["state"] == "trade"
    assert order["local_state"] == "PARTIAL"
    assert order["volume_filled"] == 0.004


def test_ws_order_event_treats_two_partial_fills_with_same_uuid_as_same_order(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_intent(
            IntentRecord(
                intent_id="intent-double-partial",
                ts_ms=1_000,
                market="KRW-BTC",
                side="bid",
                price=100000000.0,
                volume=0.01,
                reason_code="MODEL_ALPHA_ENTRY_V1",
                meta_json=json.dumps({}, ensure_ascii=False, sort_keys=True),
                status="SUBMITTED",
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="ws-order-double-partial",
                identifier="AUTOBOT-autobot-001-intent-double-partial-1-a",
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price=100000000.0,
                volume_req=0.01,
                volume_filled=0.0,
                state="wait",
                created_ts=1_000,
                updated_ts=1_000,
                intent_id="intent-double-partial",
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="ORDER_ACCEPTED",
                event_source="test",
                replace_seq=0,
                root_order_uuid="ws-order-double-partial",
            )
        )

        first_action = apply_private_ws_event(
            store=store,
            event=MyOrderEvent(
                ts_ms=2_000,
                uuid="ws-order-double-partial",
                identifier="AUTOBOT-autobot-001-intent-double-partial-1-a",
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                state="trade",
                price=100000000.0,
                volume=0.01,
                executed_volume=0.004,
            ),
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            quote_currency="KRW",
        )
        second_action = apply_private_ws_event(
            store=store,
            event=MyOrderEvent(
                ts_ms=3_000,
                uuid="ws-order-double-partial",
                identifier="AUTOBOT-autobot-001-intent-double-partial-1-a",
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                state="trade",
                price=100000000.0,
                volume=0.01,
                executed_volume=0.007,
            ),
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            quote_currency="KRW",
        )
        orders = store.list_orders(open_only=False)
        order = store.order_by_uuid(uuid="ws-order-double-partial")
        intent = store.intent_by_id(intent_id="intent-double-partial")

    assert first_action["type"] == "ws_order_upsert"
    assert second_action["type"] == "ws_order_upsert"
    assert len(orders) == 1
    assert order is not None
    assert order["uuid"] == "ws-order-double-partial"
    assert order["local_state"] == "PARTIAL"
    assert order["volume_filled"] == 0.007
    assert order["updated_ts"] == 3_000
    assert intent is not None
    assert intent["intent_id"] == "intent-double-partial"
    assert intent["status"] == "UPDATED_FROM_WS"


def test_ws_order_event_preserves_existing_intent_price_and_volume_when_event_fields_are_missing(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_intent(
            IntentRecord(
                intent_id="intent-keep-fields",
                ts_ms=1699999999000,
                market="KRW-BTC",
                side="bid",
                price=100000000.0,
                volume=0.01,
                reason_code="MODEL_ALPHA_ENTRY_V1",
                meta_json=json.dumps(
                    {
                        "execution": {"requested_price": 100000000.0},
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                status="SUBMITTED",
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="ws-order-keep-fields",
                identifier="AUTOBOT-autobot-001-intent-keep-fields-1-a",
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price=100000000.0,
                volume_req=0.01,
                volume_filled=0.0,
                state="wait",
                created_ts=1699999999000,
                updated_ts=1699999999000,
                intent_id="intent-keep-fields",
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="SUBMIT_ACCEPTED",
                event_source="test",
                replace_seq=0,
                root_order_uuid="ws-order-keep-fields",
            )
        )

        apply_private_ws_event(
            store=store,
            event=MyOrderEvent(
                ts_ms=1700000000000,
                uuid="ws-order-keep-fields",
                identifier="AUTOBOT-autobot-001-intent-keep-fields-1-a",
                market="KRW-BTC",
                side=None,
                ord_type="limit",
                state="wait",
                price=None,
                volume=None,
                executed_volume=0.0,
            ),
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            quote_currency="KRW",
        )
        intent = store.intent_by_id(intent_id="intent-keep-fields")

    assert intent is not None
    assert intent["market"] == "KRW-BTC"
    assert intent["side"] == "bid"
    assert intent["price"] == 100000000.0
    assert intent["volume"] == 0.01


def test_ws_order_event_recovers_replaced_entry_intent_from_lineage(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    replacement_identifier = "AUTOBOT-autobot-001-SUPREP-intent-entry-1-1700000001000"
    with LiveStateStore(db_path) as store:
        store.upsert_intent(
            IntentRecord(
                intent_id="intent-entry",
                ts_ms=1_000,
                market="KRW-BTC",
                side="bid",
                price=100000000.0,
                volume=0.01,
                reason_code="MODEL_ALPHA_ENTRY_V1",
                meta_json=json.dumps(
                    {
                        "strategy": {
                            "meta": {
                                "model_prob": 0.84,
                                "model_exit_plan": {
                                    "source": "model_alpha_v1",
                                    "mode": "hold",
                                    "hold_bars": 6,
                                    "interval_ms": 300000,
                                    "timeout_delta_ms": 1800000,
                                    "tp_pct": 0.02,
                                    "sl_pct": 0.01,
                                    "trailing_pct": 0.015,
                                },
                            }
                        },
                        "submit_result": {"accepted": True, "order_uuid": "entry-order-1"},
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                status="SUBMITTED",
            )
        )
        record_entry_submission(
            store=store,
            market="KRW-BTC",
            intent_id="intent-entry",
            requested_price=100000000.0,
            requested_volume=0.01,
            reason_code="MODEL_ALPHA_ENTRY_V1",
            meta_payload={
                "strategy": {
                    "meta": {
                        "model_prob": 0.84,
                        "model_exit_plan": {
                            "source": "model_alpha_v1",
                            "mode": "hold",
                            "hold_bars": 6,
                            "interval_ms": 300000,
                            "timeout_delta_ms": 1800000,
                            "tp_pct": 0.02,
                            "sl_pct": 0.01,
                            "trailing_pct": 0.015,
                        },
                    }
                },
                "submit_result": {"accepted": True, "order_uuid": "entry-order-1"},
            },
            ts_ms=1_000,
            order_uuid="entry-order-1",
        )
        store.upsert_order(
            OrderRecord(
                uuid="entry-order-1",
                identifier="AUTOBOT-autobot-001-intent-entry-1000-abc123",
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price=100000000.0,
                volume_req=0.01,
                volume_filled=0.0,
                state="cancel",
                created_ts=1_000,
                updated_ts=1_100,
                intent_id="intent-entry",
                local_state="CANCELLED",
                raw_exchange_state="cancel",
                last_event_name="ORDER_REPLACED",
                event_source="test",
                replace_seq=0,
                root_order_uuid="entry-order-1",
            )
        )
        store.append_order_lineage(
            OrderLineageRecord(
                ts_ms=1_100,
                event_source="live_order_supervisor",
                intent_id="intent-entry",
                prev_uuid="entry-order-1",
                prev_identifier="AUTOBOT-autobot-001-intent-entry-1000-abc123",
                new_uuid=None,
                new_identifier=replacement_identifier,
                replace_seq=1,
            )
        )

        action = apply_private_ws_event(
            store=store,
            event=MyOrderEvent(
                ts_ms=1700000002000,
                uuid="entry-order-2",
                identifier=replacement_identifier,
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                state="done",
                price=100000000.0,
                volume=0.01,
                executed_volume=0.01,
            ),
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            quote_currency="KRW",
        )
        order = store.order_by_uuid(uuid="entry-order-2")
        journal = store.trade_journal_by_entry_intent(entry_intent_id="intent-entry")
        entry_intent = find_latest_model_entry_intent(
            store=store,
            market="KRW-BTC",
            position={"market": "KRW-BTC", "base_amount": 0.01, "avg_entry_price": 100000000.0},
        )
        intents = store.list_intents()

    assert action["type"] == "ws_order_upsert"
    assert action["intent_id"] == "intent-entry"
    assert order is not None
    assert order["intent_id"] == "intent-entry"
    assert order["replace_seq"] == 1
    assert order["prev_order_uuid"] == "entry-order-1"
    assert journal is not None
    assert journal["entry_order_uuid"] == "entry-order-2"
    assert entry_intent is not None
    assert entry_intent["intent_id"] == "intent-entry"
    assert entry_intent["order_uuid"] == "entry-order-2"
    assert not any(str(item["intent_id"]).startswith("inferred-entry-order-2") for item in intents)


def test_ws_asset_event_upserts_and_deletes_position(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        upsert_action = apply_private_ws_event(
            store=store,
            event=MyAssetEvent(
                ts_ms=1700000000001,
                currency="ETH",
                balance=0.1,
                locked=0.05,
                avg_buy_price=3000000.0,
            ),
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            quote_currency="KRW",
        )
        delete_action = apply_private_ws_event(
            store=store,
            event=MyAssetEvent(
                ts_ms=1700000000002,
                currency="ETH",
                balance=0.0,
                locked=0.0,
                avg_buy_price=0.0,
            ),
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            quote_currency="KRW",
        )
        positions = store.list_positions()

    assert upsert_action["type"] == "ws_asset_position_upsert"
    assert delete_action["type"] == "ws_asset_position_delete"
    assert positions == []


def test_ws_asset_event_delete_closes_managed_plan_and_journal_immediately(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_position(
            PositionRecord(
                market="KRW-ETH",
                base_currency="ETH",
                base_amount=0.15,
                avg_entry_price=3_000_000.0,
                updated_ts=1_000,
                managed=True,
            )
        )
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="plan-eth-1",
                market="KRW-ETH",
                side="long",
                entry_price_str="3000000",
                qty_str="0.15",
                tp_enabled=True,
                tp_price_str="3090000",
                tp_pct=3.0,
                sl_enabled=True,
                sl_price_str="2940000",
                sl_pct=2.0,
                trailing_enabled=False,
                trail_pct=None,
                state="ACTIVE",
                last_eval_ts_ms=1_000,
                last_action_ts_ms=1_000,
                created_ts=1_000,
                updated_ts=1_000,
                plan_source="model_alpha_v1",
                source_intent_id="intent-eth-1",
            )
        )
        store.upsert_trade_journal(
            TradeJournalRecord(
                journal_id="journal-eth-1",
                market="KRW-ETH",
                status="OPEN",
                entry_intent_id="intent-eth-1",
                plan_id="plan-eth-1",
                entry_submitted_ts_ms=900,
                entry_filled_ts_ms=1_000,
                entry_price=3_000_000.0,
                qty=0.15,
                entry_notional_quote=450_000.0,
                updated_ts=1_000,
            )
        )

        action = apply_private_ws_event(
            store=store,
            event=MyAssetEvent(
                ts_ms=2_000,
                currency="ETH",
                balance=0.0,
                locked=0.0,
                avg_buy_price=0.0,
            ),
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            quote_currency="KRW",
        )
        position = store.position_by_market(market="KRW-ETH")
        plan = store.risk_plan_by_id(plan_id="plan-eth-1")
        journal = store.trade_journal_by_id(journal_id="journal-eth-1")

    assert action["type"] == "ws_asset_position_delete"
    assert action["closed_plan_ids"] == ["plan-eth-1"]
    assert action["closed_journal_ids"] == ["journal-eth-1"]
    assert position is None
    assert plan is not None
    assert plan["state"] == "CLOSED"
    assert journal is not None
    assert journal["status"] == "CLOSED"
    assert journal["close_reason_code"] == "POSITION_CLOSED"
    assert journal["close_mode"] == "position_sync"
    assert journal["exit_meta"]["source"] == "private_ws_asset_event"
    assert journal["exit_meta"]["asset_balance_zero"] is True


def test_ws_asset_event_bootstraps_managed_position_and_risk_plan_from_model_entry_intent(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_intent(
            IntentRecord(
                intent_id="intent-entry-1",
                ts_ms=1_000,
                market="KRW-BTC",
                side="bid",
                price=100_000_000.0,
                volume=0.01,
                reason_code="MODEL_ALPHA_ENTRY_V1",
                meta_json=json.dumps(
                    {
                        "model_exit_plan": {
                            "source": "model_alpha_v1",
                            "mode": "hold",
                            "hold_bars": 6,
                            "timeout_delta_ms": 1_800_000,
                            "tp_pct": 0.02,
                            "sl_pct": 0.01,
                            "trailing_pct": 0.015,
                        },
                        "submit_result": {"accepted": True, "order_uuid": "entry-order-1"},
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                status="SUBMITTED",
            )
        )

        action = apply_private_ws_event(
            store=store,
            event=MyAssetEvent(
                ts_ms=2_000,
                currency="BTC",
                balance=0.01,
                locked=0.0,
                avg_buy_price=100_000_000.0,
            ),
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            quote_currency="KRW",
        )
        position = store.position_by_market(market="KRW-BTC")
        plans = store.list_risk_plans(market="KRW-BTC")

    assert action["type"] == "ws_asset_position_upsert"
    assert action["managed"] is True
    assert position is not None
    assert position["managed"] is True
    assert position["tp"]["tp_pct"] == 2.0
    assert position["sl"]["sl_pct"] == 1.0
    assert position["trailing"]["trail_pct"] == 0.015
    assert len(plans) == 1
    assert plans[0]["plan_source"] == "model_alpha_v1"
    assert plans[0]["source_intent_id"] == "intent-entry-1"


def test_ws_asset_event_bootstrap_also_activates_trade_journal_from_model_entry_intent(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_intent(
            IntentRecord(
                intent_id="intent-entry-journal",
                ts_ms=1_000,
                market="KRW-BTC",
                side="bid",
                price=100_000_000.0,
                volume=0.01,
                reason_code="MODEL_ALPHA_ENTRY_V1",
                meta_json=json.dumps(
                    {
                        "strategy": {
                            "meta": {
                                "model_exit_plan": {
                                    "source": "model_alpha_v1",
                                    "mode": "hold",
                                    "hold_bars": 6,
                                    "interval_ms": 300000,
                                    "timeout_delta_ms": 1_800_000,
                                    "tp_pct": 0.02,
                                    "sl_pct": 0.01,
                                    "trailing_pct": 0.015,
                                }
                            }
                        },
                        "submit_result": {"accepted": True, "order_uuid": "entry-order-journal"},
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                status="SUBMITTED",
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="entry-order-journal",
                identifier="AUTOBOT-autobot-001-intent-entry-journal-1-a",
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price=100_000_000.0,
                volume_req=0.01,
                volume_filled=0.01,
                state="done",
                created_ts=1_000,
                updated_ts=1_100,
                intent_id="intent-entry-journal",
                local_state="DONE",
                raw_exchange_state="done",
                last_event_name="ORDER_STATE",
                event_source="test",
                replace_seq=0,
                root_order_uuid="entry-order-journal",
                executed_funds=1_000_000.0,
                paid_fee=500.0,
            )
        )

        action = apply_private_ws_event(
            store=store,
            event=MyAssetEvent(
                ts_ms=2_000,
                currency="BTC",
                balance=0.01,
                locked=0.0,
                avg_buy_price=100_000_000.0,
            ),
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            quote_currency="KRW",
        )
        journal = store.trade_journal_by_entry_intent(entry_intent_id="intent-entry-journal")
        plans = store.list_risk_plans(market="KRW-BTC")

    assert action["type"] == "ws_asset_position_upsert"
    assert journal is not None
    assert journal["status"] == "OPEN"
    assert journal["entry_intent_id"] == "intent-entry-journal"
    assert journal["plan_id"] == plans[0]["plan_id"]


def test_ws_asset_event_bootstrapped_risk_plan_links_exit_order_when_triggered(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_intent(
            IntentRecord(
                intent_id="intent-entry-exit-link",
                ts_ms=1_000,
                market="KRW-BTC",
                side="bid",
                price=100_000_000.0,
                volume=0.004,
                reason_code="MODEL_ALPHA_ENTRY_V1",
                meta_json=json.dumps(
                    {
                        "strategy": {
                            "meta": {
                                "model_exit_plan": {
                                    "source": "model_alpha_v1",
                                    "mode": "risk",
                                    "hold_bars": 6,
                                    "interval_ms": 300000,
                                    "timeout_delta_ms": 1_800_000,
                                    "tp_pct": 0.02,
                                    "sl_pct": 0.01,
                                    "trailing_pct": 0.015,
                                }
                            }
                        },
                        "submit_result": {"accepted": True},
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                status="SUBMITTED",
            )
        )

        apply_private_ws_event(
            store=store,
            event=MyAssetEvent(
                ts_ms=2_000,
                currency="BTC",
                balance=0.004,
                locked=0.0,
                avg_buy_price=100_000_000.0,
            ),
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            quote_currency="KRW",
        )
        plan = store.list_risk_plans(market="KRW-BTC")[0]
        journal = store.trade_journal_by_entry_intent(entry_intent_id="intent-entry-exit-link")

        manager = LiveRiskManager(
            store=store,
            executor_gateway=_RiskExitGateway(),
            config=RiskManagerConfig(exit_aggress_bps=10.0),
        )
        actions = manager.evaluate_price(market="KRW-BTC", last_price=103_000_000.0, ts_ms=3_000)
        updated_plan = store.risk_plan_by_id(plan_id=str(plan["plan_id"]))
        exit_order = store.order_by_uuid(uuid="exit-order-from-risk")

    assert any(item["type"] == "risk_exit_submitted" for item in actions)
    assert journal is not None
    assert journal["status"] == "OPEN"
    assert updated_plan is not None
    assert updated_plan["state"] == "EXITING"
    assert updated_plan["source_intent_id"] == "intent-entry-exit-link"
    assert updated_plan["current_exit_order_uuid"] == "exit-order-from-risk"
    assert exit_order is not None
    assert exit_order["tp_sl_link"] == updated_plan["plan_id"]
    assert exit_order["volume_req"] == 0.004


def test_ws_asset_event_bootstraps_managed_risk_from_single_entry_intent_before_order_event(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_intent(
            IntentRecord(
                intent_id="intent-entry-no-order",
                ts_ms=1_000,
                market="KRW-BTC",
                side="bid",
                price=100_000_000.0,
                volume=0.01,
                reason_code="MODEL_ALPHA_ENTRY_V1",
                meta_json=json.dumps(
                    {
                        "strategy": {
                            "meta": {
                                "model_exit_plan": {
                                    "source": "model_alpha_v1",
                                    "mode": "hold",
                                    "hold_bars": 6,
                                    "interval_ms": 300000,
                                    "timeout_delta_ms": 1_800_000,
                                    "tp_pct": 0.02,
                                    "sl_pct": 0.01,
                                    "trailing_pct": 0.015,
                                }
                            }
                        },
                        "submit_result": {"accepted": True},
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                status="SUBMITTED",
            )
        )

        action = apply_private_ws_event(
            store=store,
            event=MyAssetEvent(
                ts_ms=2_000,
                currency="BTC",
                balance=0.01,
                locked=0.0,
                avg_buy_price=100_000_000.0,
            ),
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            quote_currency="KRW",
        )
        position = store.position_by_market(market="KRW-BTC")
        plans = store.list_risk_plans(market="KRW-BTC")

    assert action["type"] == "ws_asset_position_upsert"
    assert action["managed"] is True
    assert position is not None
    assert position["managed"] is True
    assert len(plans) == 1
    assert plans[0]["source_intent_id"] == "intent-entry-no-order"
    assert plans[0]["qty_str"] == "0.01"
    assert plans[0]["plan_source"] == "model_alpha_v1"


def test_ws_asset_event_bootstraps_partial_position_risk_from_partial_bid_fill_first(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_intent(
            IntentRecord(
                intent_id="intent-entry-partial-first",
                ts_ms=1_000,
                market="KRW-BTC",
                side="bid",
                price=100_000_000.0,
                volume=0.01,
                reason_code="MODEL_ALPHA_ENTRY_V1",
                meta_json=json.dumps(
                    {
                        "strategy": {
                            "meta": {
                                "model_exit_plan": {
                                    "source": "model_alpha_v1",
                                    "mode": "hold",
                                    "hold_bars": 6,
                                    "interval_ms": 300000,
                                    "timeout_delta_ms": 1_800_000,
                                    "tp_pct": 0.02,
                                    "sl_pct": 0.01,
                                    "trailing_pct": 0.015,
                                }
                            }
                        },
                        "submit_result": {"accepted": True, "order_uuid": "entry-order-partial-first"},
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                status="SUBMITTED",
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="entry-order-partial-first",
                identifier="AUTOBOT-autobot-001-intent-entry-partial-first-1-a",
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price=100_000_000.0,
                volume_req=0.01,
                volume_filled=0.004,
                state="trade",
                created_ts=1_000,
                updated_ts=1_500,
                intent_id="intent-entry-partial-first",
                local_state="PARTIAL",
                raw_exchange_state="trade",
                last_event_name="ORDER_STATE",
                event_source="private_ws",
                replace_seq=0,
                root_order_uuid="entry-order-partial-first",
            )
        )

        action = apply_private_ws_event(
            store=store,
            event=MyAssetEvent(
                ts_ms=2_000,
                currency="BTC",
                balance=0.004,
                locked=0.0,
                avg_buy_price=100_000_000.0,
            ),
            bot_id="autobot-001",
            identifier_prefix="AUTOBOT",
            quote_currency="KRW",
        )
        position = store.position_by_market(market="KRW-BTC")
        plans = store.list_risk_plans(market="KRW-BTC")

    assert action["type"] == "ws_asset_position_upsert"
    assert action["managed"] is True
    assert position is not None
    assert position["base_amount"] == 0.004
    assert len(plans) == 1
    assert plans[0]["source_intent_id"] == "intent-entry-partial-first"
    assert plans[0]["qty_str"] == "0.004"
    assert plans[0]["tp"]["tp_pct"] == 2.0
