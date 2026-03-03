from __future__ import annotations

from pathlib import Path

from autobot.live.state_store import LiveStateStore, OrderRecord, PositionRecord


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
            )
        )

        assert len(store.list_positions()) == 1
        assert len(store.list_orders(open_only=True)) == 1

        exported = store.export_state()
        assert exported["db_path"] == str(db_path)
        assert exported["positions"][0]["managed"] is False
        assert exported["orders"][0]["uuid"] == "uuid-1"

        store.release_run_lock(bot_id="autobot-001")


def test_run_lock_is_unique(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as first:
        with LiveStateStore(db_path) as second:
            assert first.acquire_run_lock(bot_id="autobot-001", ts_ms=1)
            assert not second.acquire_run_lock(bot_id="autobot-001", ts_ms=2)
