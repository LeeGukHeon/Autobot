from __future__ import annotations

from pathlib import Path
import json

from autobot.live.state_store import IntentRecord, LiveStateStore, OrderRecord
from autobot.live.ws_handlers import apply_private_ws_event
from autobot.upbit.ws import MyAssetEvent, MyOrderEvent


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
