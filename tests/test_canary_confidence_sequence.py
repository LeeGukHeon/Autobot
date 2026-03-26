from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from autobot.live.canary_confidence_sequence import (
    build_canary_confidence_sequence_report,
    canary_confidence_sequence_latest_path,
)
from autobot.live.model_alpha_runtime_execute import write_live_canary_confidence_sequence_artifact
from autobot.live.state_store import LiveStateStore, TradeJournalRecord


def test_canary_confidence_sequence_promote_eligible_with_positive_lcb(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    run_id = "run-promote"
    with LiveStateStore(db_path) as store:
        for idx in range(20):
            store.upsert_trade_journal(
                TradeJournalRecord(
                    journal_id=f"journal-{idx}",
                    market="KRW-BTC",
                    status="CLOSED",
                    entry_intent_id=f"intent-{idx}",
                    entry_order_uuid=f"entry-{idx}",
                    exit_order_uuid=f"exit-{idx}",
                    entry_submitted_ts_ms=1_000 + (idx * 1_000),
                    entry_filled_ts_ms=1_100 + (idx * 1_000),
                    exit_ts_ms=1_900 + (idx * 1_000),
                    entry_price=100.0,
                    exit_price=115.0,
                    qty=1.0,
                    entry_notional_quote=100.0,
                    exit_notional_quote=115.0,
                    realized_pnl_quote=15.0,
                    realized_pnl_pct=15.0,
                    expected_net_edge_bps=500.0,
                    entry_meta_json=json.dumps({"runtime": {"live_runtime_model_run_id": run_id}}, ensure_ascii=False),
                    exit_meta_json=json.dumps({"close_verified": True, "close_verification_status": "verified_exit_order"}, ensure_ascii=False),
                    updated_ts=2_000 + (idx * 1_000),
                )
            )

        report = build_canary_confidence_sequence_report(
            store=store,
            run_id=run_id,
            confidence_monitor_config={
                "min_closed_trade_count": 4,
                "severe_loss_rate_threshold": 1.0,
                "edge_gap_breach_rate_threshold": 1.0,
                "confidence_delta": 0.2,
            },
            runtime_health={},
            lane="live_candidate",
            unit_name="autobot-live-alpha-candidate.service",
            rollout_mode="canary",
            ts_ms=99_000,
        )

    assert report["policy"] == "canary_confidence_sequence_v1"
    assert report["decision"]["promote_eligible"] is True
    assert report["decision"]["status"] == "promote_eligible"
    assert report["reward_stream"]["risk_adjusted_return_lcb"] > 0.0


def test_canary_confidence_sequence_aborts_on_negative_reward_stream(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    run_id = "run-abort"
    with LiveStateStore(db_path) as store:
        for idx in range(20):
            store.upsert_trade_journal(
                TradeJournalRecord(
                    journal_id=f"loss-{idx}",
                    market="KRW-BTC",
                    status="CLOSED",
                    entry_intent_id=f"intent-loss-{idx}",
                    entry_order_uuid=f"entry-loss-{idx}",
                    exit_order_uuid=f"exit-loss-{idx}",
                    entry_submitted_ts_ms=1_000 + (idx * 1_000),
                    entry_filled_ts_ms=1_100 + (idx * 1_000),
                    exit_ts_ms=1_900 + (idx * 1_000),
                    entry_price=100.0,
                    exit_price=85.0,
                    qty=1.0,
                    entry_notional_quote=100.0,
                    exit_notional_quote=85.0,
                    realized_pnl_quote=-15.0,
                    realized_pnl_pct=-15.0,
                    expected_net_edge_bps=-500.0,
                    entry_meta_json=json.dumps({"runtime": {"live_runtime_model_run_id": run_id}}, ensure_ascii=False),
                    exit_meta_json=json.dumps({"close_verified": True, "close_verification_status": "verified_exit_order"}, ensure_ascii=False),
                    updated_ts=2_000 + (idx * 1_000),
                )
            )

        report = build_canary_confidence_sequence_report(
            store=store,
            run_id=run_id,
            confidence_monitor_config={
                "min_closed_trade_count": 4,
                "severe_loss_rate_threshold": 0.4,
                "confidence_delta": 0.2,
            },
            runtime_health={},
            lane="live_candidate",
            unit_name="autobot-live-alpha-candidate.service",
            rollout_mode="canary",
            ts_ms=99_000,
        )

    assert report["decision"]["abort"] is True
    assert report["decision"]["status"] == "abort"


def test_runtime_writer_persists_canary_confidence_sequence_latest(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    run_id = "run-write"
    with LiveStateStore(db_path) as store:
        for idx in range(8):
            store.upsert_trade_journal(
                TradeJournalRecord(
                    journal_id=f"journal-write-{idx}",
                    market="KRW-BTC",
                    status="CLOSED",
                    entry_intent_id=f"intent-write-{idx}",
                    entry_order_uuid=f"entry-write-{idx}",
                    exit_order_uuid=f"exit-write-{idx}",
                    entry_submitted_ts_ms=1_000 + (idx * 1_000),
                    entry_filled_ts_ms=1_100 + (idx * 1_000),
                    exit_ts_ms=1_900 + (idx * 1_000),
                    entry_price=100.0,
                    exit_price=110.0,
                    qty=1.0,
                    entry_notional_quote=100.0,
                    exit_notional_quote=110.0,
                    realized_pnl_quote=10.0,
                    realized_pnl_pct=10.0,
                    expected_net_edge_bps=400.0,
                    entry_meta_json=json.dumps({"runtime": {"live_runtime_model_run_id": run_id}}, ensure_ascii=False),
                    exit_meta_json=json.dumps({"close_verified": True, "close_verification_status": "verified_exit_order"}, ensure_ascii=False),
                    updated_ts=2_000 + (idx * 1_000),
                )
            )

        settings = SimpleNamespace(
            daemon=SimpleNamespace(
                rollout_mode="canary",
                rollout_target_unit="autobot-live-alpha-candidate.service",
                registry_root=str(tmp_path / "models" / "registry"),
            )
        )
        report = write_live_canary_confidence_sequence_artifact(
            store=store,
            settings=settings,
            run_id=run_id,
            risk_control_payload={"confidence_sequence_monitors": {"min_closed_trade_count": 4, "confidence_delta": 0.2}},
            ts_ms=123_456,
        )

    latest_path = canary_confidence_sequence_latest_path(
        project_root=tmp_path,
        unit_name="autobot-live-alpha-candidate.service",
    )
    assert report is not None
    assert latest_path.exists()
    payload = json.loads(latest_path.read_text(encoding="utf-8"))
    assert payload["policy"] == "canary_confidence_sequence_v1"
    assert payload["run_id"] == run_id
