from __future__ import annotations

from pathlib import Path

import pytest

from autobot.live.breakers import arm_breaker
from autobot.live.state_store import LiveStateStore, OrderLineageRecord, OrderRecord, PositionRecord


def test_state_store_upserts_and_exports(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.bootstrap_bot_meta(bot_id="autobot-001", version="0.1.0", ts_ms=1000)
        assert store.acquire_run_lock(bot_id="autobot-001", ts_ms=1001)

        store.upsert_position(
            PositionRecord(
                market="KRW-BTC",
                base_currency="BTC",
                base_amount=0.01,
                avg_entry_price=100000000.0,
                updated_ts=1002,
                managed=False,
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="uuid-1",
                identifier="AUTOBOT-autobot-001-intent-1-1000-a",
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price=100000000.0,
                volume_req=0.01,
                volume_filled=0.0,
                state="wait",
                created_ts=1000,
                updated_ts=1003,
                intent_id="intent-1",
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="ORDER_STATE",
                event_source="test",
                root_order_uuid="uuid-1",
            )
        )
        store.append_order_lineage(
            OrderLineageRecord(
                ts_ms=1004,
                event_source="test",
                intent_id="intent-1",
                prev_uuid="uuid-1",
                prev_identifier="AUTOBOT-autobot-001-intent-1-1000-a",
                new_uuid="uuid-2",
                new_identifier="AUTOBOT-autobot-001-intent-1-1004-b",
                replace_seq=1,
            )
        )

        assert len(store.list_positions()) == 1
        assert len(store.list_orders(open_only=True)) == 1
        assert len(store.list_order_lineage(intent_id="intent-1")) == 1
        store.set_runtime_contract(payload={"live_runtime_model_run_id": "run-1"}, ts_ms=1004)
        store.set_ws_public_contract(payload={"ws_public_last_checkpoint_ts_ms": 1003}, ts_ms=1004)
        store.set_live_runtime_health(payload={"model_pointer_divergence": False}, ts_ms=1004)

        exported = store.export_state()
        assert exported["db_path"] == str(db_path)
        assert exported["positions"][0]["managed"] is False
        assert exported["orders"][0]["uuid"] == "uuid-1"
        assert exported["orders"][0]["local_state"] == "OPEN"
        assert exported["order_lineage"][0]["new_uuid"] == "uuid-2"
        assert store.runtime_contract()["live_runtime_model_run_id"] == "run-1"
        assert store.ws_public_contract()["ws_public_last_checkpoint_ts_ms"] == 1003
        assert store.live_runtime_health()["model_pointer_divergence"] is False
        assert exported["breaker_state"] == []
        arm_breaker(store, reason_codes=["MANUAL_KILL_SWITCH"], source="test", ts_ms=1005)
        exported_after_breaker = store.export_state()
        assert len(exported_after_breaker["breaker_state"]) == 1
        assert exported_after_breaker["breaker_state"][0]["active"] is True

        store.release_run_lock(bot_id="autobot-001")


def test_run_lock_is_unique(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as first:
        with LiveStateStore(db_path) as second:
            assert first.acquire_run_lock(bot_id="autobot-001", ts_ms=1)
            assert not second.acquire_run_lock(bot_id="autobot-001", ts_ms=2)


def test_state_store_rejects_identifier_collision(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_order(
            OrderRecord(
                uuid="uuid-1",
                identifier="AUTOBOT-collision-1",
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price=1000.0,
                volume_req=1.0,
                volume_filled=0.0,
                state="wait",
                created_ts=1000,
                updated_ts=1000,
            )
        )
        with pytest.raises(ValueError, match="IDENTIFIER_COLLISION"):
            store.upsert_order(
                OrderRecord(
                    uuid="uuid-2",
                    identifier="AUTOBOT-collision-1",
                    market="KRW-BTC",
                    side="bid",
                    ord_type="limit",
                    price=1000.0,
                    volume_req=1.0,
                    volume_filled=0.0,
                    state="wait",
                    created_ts=1001,
                    updated_ts=1001,
                )
            )
