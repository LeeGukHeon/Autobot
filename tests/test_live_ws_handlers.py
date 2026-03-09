from __future__ import annotations

from pathlib import Path

from autobot.live.state_store import LiveStateStore
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
