from __future__ import annotations

import json
from pathlib import Path

from autobot.live.execution_attempts_backfill import backfill_execution_attempts_for_db
from autobot.live.state_store import LiveStateStore, OrderRecord, TradeJournalRecord


def test_execution_attempts_backfill_builds_filled_and_cancelled_entry_attempts(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        entry_meta = {
            "execution": {
                "initial_ref_price": 101.0,
                "effective_ref_price": 101.0,
                "requested_price": 100.0,
                "exec_profile": {"price_mode": "PASSIVE_MAKER"},
            },
            "strategy": {
                "meta": {
                    "model_prob": 0.91,
                    "trade_action": {
                        "expected_edge": 0.0030,
                        "expected_es": 0.0010,
                    },
                }
            },
        }
        store.upsert_trade_journal(
            TradeJournalRecord(
                journal_id="journal-filled",
                market="KRW-BTC",
                status="CLOSED",
                entry_intent_id="intent-filled",
                entry_order_uuid="order-filled",
                entry_submitted_ts_ms=2_000_000_000_000,
                entry_filled_ts_ms=2_000_000_001_000,
                entry_price=100.0,
                qty=2.0,
                expected_edge_bps=30.0,
                expected_net_edge_bps=24.0,
                entry_meta_json=json.dumps(entry_meta, ensure_ascii=False, sort_keys=True),
                updated_ts=2_000_000_002_000,
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="order-filled",
                identifier="identifier-filled",
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                time_in_force=None,
                price=100.0,
                volume_req=2.0,
                volume_filled=2.0,
                state="done",
                created_ts=2_000_000_000_100,
                updated_ts=2_000_000_001_000,
                intent_id="intent-filled",
                local_state="DONE",
                raw_exchange_state="done",
                last_event_name="ORDER_STATE",
                event_source="private_ws",
            )
        )
        store.upsert_trade_journal(
            TradeJournalRecord(
                journal_id="journal-missed",
                market="KRW-ETH",
                status="CANCELLED_ENTRY",
                entry_intent_id="intent-missed",
                entry_order_uuid="order-missed",
                entry_submitted_ts_ms=2_000_000_010_000,
                exit_ts_ms=2_000_000_020_000,
                entry_price=200.0,
                qty=1.5,
                expected_edge_bps=20.0,
                expected_net_edge_bps=15.0,
                entry_meta_json=json.dumps(entry_meta, ensure_ascii=False, sort_keys=True),
                updated_ts=2_000_000_020_000,
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="order-missed",
                identifier="identifier-missed",
                market="KRW-ETH",
                side="bid",
                ord_type="limit",
                time_in_force=None,
                price=200.0,
                volume_req=1.5,
                volume_filled=0.0,
                state="cancel",
                created_ts=2_000_000_010_100,
                updated_ts=2_000_000_020_000,
                intent_id="intent-missed",
                local_state="CANCELLED",
                raw_exchange_state="cancel",
                last_event_name="ORDER_TIMEOUT",
                event_source="live_order_supervisor",
            )
        )

    report = backfill_execution_attempts_for_db(
        db_path=db_path,
        lookback_days=3650,
        limit=100,
    )

    assert report["attempts_upserted"] == 2
    with LiveStateStore(db_path) as store:
        attempts = store.list_execution_attempts(limit=10)
    by_journal = {item["journal_id"]: item for item in attempts}
    assert by_journal["journal-filled"]["final_state"] == "FILLED"
    assert by_journal["journal-filled"]["action_code"] == "LIMIT_GTC_PASSIVE_MAKER"
    assert by_journal["journal-filled"]["time_in_force"] == "gtc"
    assert by_journal["journal-filled"]["shortfall_bps"] is not None
    assert by_journal["journal-missed"]["final_state"] == "MISSED"
    assert by_journal["journal-missed"]["time_in_force"] == "gtc"
