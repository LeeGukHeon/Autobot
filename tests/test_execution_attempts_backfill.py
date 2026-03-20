from __future__ import annotations

import json
from pathlib import Path

import polars as pl

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


def test_execution_attempts_backfill_reconstructs_micro_state_from_micro_parquet(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    micro_root = tmp_path / "data" / "parquet" / "micro_v1"
    ts_ms = 2_000_000_000_000
    market = "KRW-BTC"
    date_value = "2033-05-18"
    part_dir = micro_root / "tf=5m" / f"market={market}" / f"date={date_value}"
    part_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "market": [market],
            "tf": ["5m"],
            "ts_ms": [ts_ms],
            "trade_source": ["ws"],
            "trade_events": [5],
            "book_events": [9],
            "trade_min_ts_ms": [ts_ms - 35_000],
            "trade_max_ts_ms": [ts_ms - 2_000],
            "book_min_ts_ms": [ts_ms - 45_000],
            "book_max_ts_ms": [ts_ms - 1_000],
            "trade_coverage_ms": [33_000],
            "book_coverage_ms": [44_000],
            "micro_trade_available": [True],
            "micro_book_available": [True],
            "micro_available": [True],
            "trade_count": [5],
            "buy_count": [3],
            "sell_count": [2],
            "trade_volume_total": [100.0],
            "buy_volume": [60.0],
            "sell_volume": [40.0],
            "trade_imbalance": [0.2],
            "vwap": [100.0],
            "avg_trade_size": [20.0],
            "max_trade_size": [30.0],
            "last_trade_price": [101.0],
            "mid_mean": [100.5],
            "spread_bps_mean": [7.5],
            "depth_bid_top5_mean": [12_345.0],
            "depth_ask_top5_mean": [23_456.0],
            "imbalance_top5_mean": [0.1],
            "microprice_bias_bps_mean": [0.5],
            "book_update_count": [9],
        }
    ).write_parquet(part_dir / "part-000.parquet")

    with LiveStateStore(db_path) as store:
        entry_meta = {
            "micro_state": {
                "spread_bps": None,
                "depth_top5_notional_krw": None,
                "trade_coverage_ms": None,
                "book_coverage_ms": None,
                "snapshot_age_ms": None,
                "micro_quality_score": None,
            },
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
                market=market,
                status="CLOSED",
                entry_intent_id="intent-filled",
                entry_order_uuid="order-filled",
                entry_submitted_ts_ms=ts_ms,
                entry_filled_ts_ms=ts_ms + 1_000,
                entry_price=100.0,
                qty=2.0,
                expected_edge_bps=30.0,
                expected_net_edge_bps=24.0,
                entry_meta_json=json.dumps(entry_meta, ensure_ascii=False, sort_keys=True),
                updated_ts=ts_ms + 2_000,
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="order-filled",
                identifier="identifier-filled",
                market=market,
                side="bid",
                ord_type="limit",
                time_in_force=None,
                price=100.0,
                volume_req=2.0,
                volume_filled=2.0,
                state="done",
                created_ts=ts_ms + 100,
                updated_ts=ts_ms + 1_000,
                intent_id="intent-filled",
                local_state="DONE",
                raw_exchange_state="done",
                last_event_name="ORDER_STATE",
                event_source="private_ws",
            )
        )

    report = backfill_execution_attempts_for_db(
        db_path=db_path,
        lookback_days=3650,
        limit=100,
        micro_root=micro_root,
        micro_tf="5m",
    )

    assert report["micro_replay_enabled"] is True
    assert report["micro_journals_updated"] == 1
    assert report["micro_attempts_enriched"] == 1
    with LiveStateStore(db_path) as store:
        attempts = store.list_execution_attempts(limit=10)
        journals = store.list_trade_journal(limit=10)
    attempt = next(item for item in attempts if item["journal_id"] == "journal-filled")
    journal = next(item for item in journals if item["journal_id"] == "journal-filled")
    assert attempt["spread_bps"] == 7.5
    assert attempt["depth_top5_notional_krw"] == 35_801.0
    assert attempt["trade_coverage_ms"] == 33_000
    assert attempt["book_coverage_ms"] == 44_000
    assert attempt["snapshot_age_ms"] == 1_000
    assert attempt["micro_quality_score"] is not None
    journal_micro_state = (journal.get("entry_meta") or {}).get("micro_state") or {}
    assert journal_micro_state["spread_bps"] == 7.5
    assert journal_micro_state["depth_top5_notional_krw"] == 35_801.0
    assert journal_micro_state["trade_coverage_ms"] == 33_000
    assert journal_micro_state["book_coverage_ms"] == 44_000
    assert journal_micro_state["snapshot_age_ms"] == 1_000
    assert journal_micro_state["micro_quality_score"] is not None
