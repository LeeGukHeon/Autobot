from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from autobot.live.state_store import ExecutionAttemptRecord, LiveStateStore, OrderRecord, TradeJournalRecord
from autobot.ops.private_execution_label_store import build_private_execution_label_store


def test_build_private_execution_label_store_writes_dataset_and_reports(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    db_path = project_root / "data" / "state" / "live" / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_trade_journal(
            TradeJournalRecord(
                journal_id="journal-1",
                market="KRW-BTC",
                status="CLOSED",
                entry_intent_id="intent-1",
                entry_submitted_ts_ms=1_774_569_689_000,
                entry_filled_ts_ms=1_774_569_709_000,
                exit_ts_ms=1_774_569_900_000,
                entry_price=100.0,
                exit_price=101.0,
                qty=1.0,
                realized_pnl_quote=10.0,
                model_prob=0.9,
                expected_edge_bps=30.0,
                expected_downside_bps=5.0,
                expected_net_edge_bps=20.0,
                entry_meta_json=json.dumps(
                    {
                        "runtime": {"model_family": "train_v5_fusion", "live_runtime_model_run_id": "run-live"},
                        "strategy": {
                            "meta": {
                                "model_exit_plan": {
                                    "bar_interval_ms": 60_000,
                                    "hold_bars": 30,
                                }
                            }
                        },
                        "execution_policy": {"deadline_ms": 60_000, "selected_action_code": "LIMIT_GTC_JOIN"},
                        "execution": {"requested_price": 100.0, "requested_volume": 1.0},
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                exit_meta_json=json.dumps({"entry_realized_slippage_bps": 1.5}, ensure_ascii=False, sort_keys=True),
                updated_ts=1_774_569_900_000,
            )
        )
        store.upsert_execution_attempt(
            ExecutionAttemptRecord(
                attempt_id="attempt-1",
                journal_id="journal-1",
                intent_id="intent-1",
                order_uuid="order-1",
                order_identifier="AUTOBOT-1",
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                time_in_force="gtc",
                action_code="LIMIT_GTC_JOIN",
                requested_price=100.0,
                requested_volume=1.0,
                spread_bps=2.0,
                depth_top5_notional_krw=1000000.0,
                snapshot_age_ms=500,
                model_prob=0.9,
                expected_edge_bps=30.0,
                expected_net_edge_bps=20.0,
                expected_es_bps=5.0,
                submitted_ts_ms=1_774_569_689_000,
                first_fill_ts_ms=1_774_569_709_000,
                full_fill_ts_ms=1_774_569_709_000,
                final_ts_ms=1_774_569_709_000,
                final_state="FILLED",
                filled_price=100.1,
                shortfall_bps=1.0,
                filled_volume=1.0,
                fill_fraction=1.0,
                partial_fill=False,
                full_fill=True,
                outcome_json="{}",
                updated_ts=1_774_569_709_000,
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="order-1",
                identifier="AUTOBOT-1",
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price=100.0,
                volume_req=1.0,
                volume_filled=1.0,
                state="done",
                created_ts=1_774_569_689_000,
                updated_ts=1_774_569_709_000,
                intent_id="intent-1",
                local_state="FILLED",
                replace_seq=2,
                root_order_uuid="root-order-1",
                executed_funds=100.1,
                paid_fee=0.05,
                remaining_fee=0.0,
                time_in_force="gtc",
            )
        )
        store.set_runtime_contract(payload={"version": 1, "decision_contract_version": "v5_post_model_contract_v1"}, ts_ms=1_774_569_600_000)
        store.set_live_rollout_contract(payload={"mode": "candidate", "lane_id": "canary"}, ts_ms=1_774_569_600_000)
        store.set_ws_public_contract(payload={"ws_public_stale": False}, ts_ms=1_774_569_600_000)
        store.set_live_runtime_health(payload={"status": "ready"}, ts_ms=1_774_569_600_000)

    payload = build_private_execution_label_store(project_root=project_root)

    build_report = json.loads(Path(payload["build_report_path"]).read_text(encoding="utf-8"))
    validate_report = json.loads(Path(payload["validate_report_path"]).read_text(encoding="utf-8"))
    label_contract = json.loads(Path(payload["label_contract_path"]).read_text(encoding="utf-8"))
    manifest = pl.read_parquet(project_root / "data" / "parquet" / "private_execution_v1" / "_meta" / "manifest.parquet")
    frame = pl.read_parquet(Path(manifest.item(0, "part_file")))

    assert build_report["status"] == "PASS"
    assert validate_report["status"] == "PASS"
    assert label_contract["policy"] == "private_execution_label_contract_v1"
    assert frame.height == 1
    assert frame.item(0, "runtime_model_family") == "train_v5_fusion"
    assert frame.item(0, "runtime_decision_contract_version") == "v5_post_model_contract_v1"
    assert str(frame.item(0, "order_local_state") or "").strip() != ""
    assert frame.item(0, "order_replace_seq") == 2
    assert frame.item(0, "rollout_mode") == "candidate"
    assert frame.item(0, "live_runtime_health_status") == "ready"
    assert frame.item(0, "decision_bar_interval_ms") == 60_000
    assert frame.item(0, "decision_bucket_ts_ms") == 1_774_569_660_000
    assert frame.item(0, "y_tradeable") == 1
    assert frame.item(0, "filled_within_deadline") is True


def test_build_private_execution_label_store_preserves_mixed_one_minute_and_legacy_five_minute_intervals(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    db_path = project_root / "data" / "state" / "live" / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_trade_journal(
            TradeJournalRecord(
                journal_id="journal-1m",
                market="KRW-BTC",
                status="CLOSED",
                entry_intent_id="intent-1m",
                entry_submitted_ts_ms=1_774_569_689_000,
                entry_filled_ts_ms=1_774_569_709_000,
                exit_ts_ms=1_774_569_900_000,
                entry_price=100.0,
                exit_price=101.0,
                qty=1.0,
                realized_pnl_quote=10.0,
                model_prob=0.9,
                expected_edge_bps=30.0,
                expected_downside_bps=5.0,
                expected_net_edge_bps=20.0,
                entry_meta_json=json.dumps(
                    {
                        "runtime": {
                            "model_family": "train_v5_fusion",
                            "live_runtime_model_run_id": "run-1m",
                            "tf": "1m",
                        },
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                updated_ts=1_774_569_900_000,
            )
        )
        store.upsert_trade_journal(
            TradeJournalRecord(
                journal_id="journal-legacy",
                market="KRW-ETH",
                status="CLOSED",
                entry_intent_id="intent-legacy",
                entry_submitted_ts_ms=1_774_569_689_000,
                entry_filled_ts_ms=1_774_569_709_000,
                exit_ts_ms=1_774_569_900_000,
                entry_price=200.0,
                exit_price=201.0,
                qty=1.0,
                realized_pnl_quote=5.0,
                model_prob=0.8,
                expected_edge_bps=15.0,
                expected_downside_bps=4.0,
                expected_net_edge_bps=10.0,
                entry_meta_json=json.dumps(
                    {
                        "runtime": {"model_family": "train_v4_crypto_cs", "live_runtime_model_run_id": "run-legacy"},
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                updated_ts=1_774_569_900_000,
            )
        )

    payload = build_private_execution_label_store(project_root=project_root)

    manifest = pl.read_parquet(project_root / "data" / "parquet" / "private_execution_v1" / "_meta" / "manifest.parquet")
    parts = [pl.read_parquet(Path(path)) for path in manifest.get_column("part_file").to_list()]
    frame = pl.concat(parts, how="vertical_relaxed").sort("market")

    intervals = {
        str(row["market"]): int(row["decision_bar_interval_ms"])
        for row in frame.select(["market", "decision_bar_interval_ms"]).to_dicts()
    }
    buckets = {
        str(row["market"]): int(row["decision_bucket_ts_ms"])
        for row in frame.select(["market", "decision_bucket_ts_ms"]).to_dicts()
    }

    assert payload["status"] == "PASS"
    assert intervals["KRW-BTC"] == 60_000
    assert intervals["KRW-ETH"] == 300_000
    assert buckets["KRW-BTC"] == 1_774_569_660_000
    assert buckets["KRW-ETH"] == 1_774_569_600_000
