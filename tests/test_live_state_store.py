from __future__ import annotations

from pathlib import Path
import sqlite3

import pytest

import autobot.live.state_store as state_store_module
from autobot.live.breakers import arm_breaker
from autobot.live.state_store import (
    LiveStateStore,
    OrderLineageRecord,
    OrderRecord,
    PositionRecord,
    RiskPlanRecord,
    TradeJournalRecord,
)


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
        assert exported_after_breaker["breaker_state"][0]["primary_reason_type"] == "OPERATIONAL_POLICY"
        assert exported_after_breaker["breaker_state"][0]["typed_reason_codes"][0]["clear_policy"] == "MANUAL"

        store.release_run_lock(bot_id="autobot-001")


def test_run_lock_is_unique(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as first:
        with LiveStateStore(db_path) as second:
            assert first.acquire_run_lock(bot_id="autobot-001", ts_ms=1)
            assert not second.acquire_run_lock(bot_id="autobot-001", ts_ms=2)


def test_run_lock_reclaims_dead_owner(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        with store._conn:
            store._conn.execute(
                "INSERT INTO run_locks (bot_id, acquired_ts, owner_pid) VALUES (?, ?, ?)",
                ("autobot-001", 1000, 424242),
            )
        monkeypatch.setattr(state_store_module, "_pid_is_alive", lambda pid: False)
        assert store.acquire_run_lock(bot_id="autobot-001", ts_ms=2000)
        lock = store.run_lock(bot_id="autobot-001")
        assert lock is not None
        assert lock["owner_pid"] == state_store_module.os.getpid()
        assert lock["acquired_ts"] == 2000


def test_run_lock_does_not_reclaim_live_owner(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        with store._conn:
            store._conn.execute(
                "INSERT INTO run_locks (bot_id, acquired_ts, owner_pid) VALUES (?, ?, ?)",
                ("autobot-001", 1000, 31337),
            )
        monkeypatch.setattr(state_store_module, "_pid_is_alive", lambda pid: True)
        assert not store.acquire_run_lock(bot_id="autobot-001", ts_ms=2000)
        lock = store.run_lock(bot_id="autobot-001")
        assert lock is not None
        assert lock["owner_pid"] == 31337
        assert lock["acquired_ts"] == 1000


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


def test_state_store_rebinds_pending_order_identifier_without_collision(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_order(
            OrderRecord(
                uuid="pending-intent-1",
                identifier="AUTOBOT-autobot-001-intent-1-1000-a",
                market="KRW-BTC",
                side="ask",
                ord_type="limit",
                price=1000.0,
                volume_req=1.0,
                volume_filled=0.0,
                state="wait",
                created_ts=1000,
                updated_ts=1000,
                intent_id="intent-1",
                tp_sl_link="plan-1",
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="SUBMIT_ACCEPTED",
                event_source="test",
                root_order_uuid="pending-intent-1",
            )
        )
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="plan-1",
                market="KRW-BTC",
                side="long",
                entry_price_str="900",
                qty_str="1",
                tp_enabled=False,
                sl_enabled=False,
                trailing_enabled=False,
                state="EXITING",
                current_exit_order_uuid="pending-intent-1",
                current_exit_order_identifier="AUTOBOT-autobot-001-intent-1-1000-a",
                created_ts=900,
                updated_ts=1000,
            )
        )
        store.upsert_trade_journal(
            TradeJournalRecord(
                journal_id="journal-1",
                market="KRW-BTC",
                status="OPEN",
                entry_intent_id="intent-1",
                entry_order_uuid="entry-uuid-1",
                exit_order_uuid="pending-intent-1",
                plan_id="plan-1",
                entry_submitted_ts_ms=900,
                entry_filled_ts_ms=950,
                entry_price=900.0,
                qty=1.0,
                updated_ts=1000,
            )
        )

        store.upsert_order(
            OrderRecord(
                uuid="real-order-1",
                identifier="AUTOBOT-autobot-001-intent-1-1000-a",
                market="KRW-BTC",
                side="ask",
                ord_type="limit",
                price=1000.0,
                volume_req=1.0,
                volume_filled=0.0,
                state="wait",
                created_ts=1200,
                updated_ts=1200,
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="ORDER_STATE",
                event_source="private_ws",
            )
        )
        migrated = store.order_by_uuid(uuid="real-order-1")
        pending = store.order_by_uuid(uuid="pending-intent-1")
        plan = store.risk_plan_by_id(plan_id="plan-1")
        journal = store.trade_journal_by_id(journal_id="journal-1")

    assert pending is None
    assert migrated is not None
    assert migrated["intent_id"] == "intent-1"
    assert migrated["tp_sl_link"] == "plan-1"
    assert migrated["created_ts"] == 1000
    assert migrated["root_order_uuid"] == "real-order-1"
    assert plan is not None
    assert plan["current_exit_order_uuid"] == "real-order-1"
    assert journal is not None
    assert journal["exit_order_uuid"] == "real-order-1"


def test_state_store_preserves_intent_id_when_exchange_refresh_omits_it(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_order(
            OrderRecord(
                uuid="uuid-1",
                identifier="AUTOBOT-autobot-001-intent-1-1000-a",
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price=1000.0,
                volume_req=1.0,
                volume_filled=0.0,
                state="wait",
                created_ts=1000,
                updated_ts=1000,
                intent_id="intent-1",
                tp_sl_link="tp-sl-1",
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="SUBMIT_ACCEPTED",
                event_source="test",
                root_order_uuid="uuid-1",
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="uuid-1",
                identifier="AUTOBOT-autobot-001-intent-1-1000-a",
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price=1000.0,
                volume_req=1.0,
                volume_filled=1.0,
                state="done",
                created_ts=1000,
                updated_ts=2000,
                intent_id=None,
                tp_sl_link=None,
                local_state="DONE",
                raw_exchange_state="done",
                last_event_name="EXCHANGE_SNAPSHOT",
                event_source="reconcile_snapshot",
                root_order_uuid="uuid-1",
            )
        )
        row = store.order_by_uuid(uuid="uuid-1")

    assert row is not None
    assert row["intent_id"] == "intent-1"
    assert row["tp_sl_link"] == "tp-sl-1"


@pytest.mark.parametrize("side", ["bid", "ask"])
def test_state_store_preserves_open_order_updated_ts_for_unchanged_reconcile_snapshot(
    tmp_path: Path,
    side: str,
) -> None:
    db_path = tmp_path / f"live_state_{side}.db"
    with LiveStateStore(db_path) as store:
        store.upsert_order(
            OrderRecord(
                uuid=f"uuid-{side}",
                identifier=f"AUTOBOT-{side}-1",
                market="KRW-DOGE",
                side=side,
                ord_type="limit",
                price=145.0,
                volume_req=39.0,
                volume_filled=0.0,
                state="wait",
                created_ts=1000,
                updated_ts=1100,
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="ORDER_STATE",
                event_source="private_ws",
                root_order_uuid=f"uuid-{side}",
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid=f"uuid-{side}",
                identifier=f"AUTOBOT-{side}-1",
                market="KRW-DOGE",
                side=side,
                ord_type="limit",
                price=145.0,
                volume_req=39.0,
                volume_filled=0.0,
                state="wait",
                created_ts=1000,
                updated_ts=2000,
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="EXCHANGE_SNAPSHOT",
                event_source="reconcile_snapshot",
                root_order_uuid=f"uuid-{side}",
            )
        )
        row = store.order_by_uuid(uuid=f"uuid-{side}")

    assert row is not None
    assert row["updated_ts"] == 1100
    assert row["event_source"] == "reconcile_snapshot"
    assert row["last_event_name"] == "EXCHANGE_SNAPSHOT"


@pytest.mark.parametrize("side", ["bid", "ask"])
def test_state_store_advances_updated_ts_when_reconcile_snapshot_has_material_order_change(
    tmp_path: Path,
    side: str,
) -> None:
    db_path = tmp_path / f"live_state_change_{side}.db"
    with LiveStateStore(db_path) as store:
        store.upsert_order(
            OrderRecord(
                uuid=f"uuid-{side}",
                identifier=f"AUTOBOT-{side}-1",
                market="KRW-DOGE",
                side=side,
                ord_type="limit",
                price=145.0,
                volume_req=39.0,
                volume_filled=0.0,
                state="wait",
                created_ts=1000,
                updated_ts=1100,
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="ORDER_STATE",
                event_source="private_ws",
                root_order_uuid=f"uuid-{side}",
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid=f"uuid-{side}",
                identifier=f"AUTOBOT-{side}-1",
                market="KRW-DOGE",
                side=side,
                ord_type="limit",
                price=145.0,
                volume_req=39.0,
                volume_filled=5.0,
                state="trade",
                created_ts=1000,
                updated_ts=2000,
                local_state="PARTIAL",
                raw_exchange_state="trade",
                last_event_name="EXCHANGE_SNAPSHOT",
                event_source="reconcile_snapshot",
                root_order_uuid=f"uuid-{side}",
            )
        )
        row = store.order_by_uuid(uuid=f"uuid-{side}")

    assert row is not None
    assert row["updated_ts"] == 2000
    assert row["local_state"] == "PARTIAL"
    assert row["volume_filled"] == 5.0


def test_state_store_migrates_legacy_orders_schema_before_index_creation(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy_live_state.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.executescript(
            """
            CREATE TABLE orders (
                uuid TEXT PRIMARY KEY,
                identifier TEXT UNIQUE,
                market TEXT NOT NULL,
                side TEXT,
                ord_type TEXT,
                price REAL,
                volume_req REAL,
                volume_filled REAL NOT NULL DEFAULT 0,
                state TEXT NOT NULL,
                created_ts INTEGER NOT NULL,
                updated_ts INTEGER NOT NULL,
                intent_id TEXT,
                tp_sl_link TEXT
            );
            """
        )
        conn.commit()
    finally:
        conn.close()

    with LiveStateStore(db_path) as store:
        store.upsert_order(
            OrderRecord(
                uuid="legacy-uuid-1",
                identifier="AUTOBOT-legacy-1",
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price=1000.0,
                volume_req=1.0,
                volume_filled=0.0,
                state="wait",
                created_ts=1000,
                updated_ts=1000,
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="ORDER_STATE",
                event_source="test",
                root_order_uuid="legacy-uuid-1",
            )
        )
        row = store.order_by_uuid(uuid="legacy-uuid-1")
        assert row is not None
        assert row["root_order_uuid"] == "legacy-uuid-1"
        assert row["local_state"] == "OPEN"
