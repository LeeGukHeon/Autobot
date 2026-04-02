from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from autobot.live.canary_confidence_sequence import (
    build_canary_confidence_sequence_report,
    canary_confidence_sequence_latest_path,
)
from autobot.live.paper_live_divergence import paper_live_divergence_latest_path, write_paper_live_divergence_report
from autobot.live.model_alpha_runtime_execute import write_live_canary_confidence_sequence_artifact
from autobot.live.state_store import LiveStateStore, TradeJournalRecord


def test_canary_confidence_sequence_promote_eligible_with_positive_lcb(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    run_id = "run-promote"
    latest_divergence_path = paper_live_divergence_latest_path(
        project_root=tmp_path,
        unit_name="autobot-live-alpha-candidate.service",
    )
    write_paper_live_divergence_report(
        latest_path=latest_divergence_path,
        payload={
            "artifact_version": 1,
            "status": "ready",
            "matching": {"matched_opportunities": 20},
            "feature_divergence": {"feature_hash_match_ratio": 1.0, "feature_divergence_rate": 0.0},
            "decision_divergence": {"decision_divergence_rate": 0.0},
            "matched_records": [{"feature_hash_match": True, "decision_match": True} for _ in range(20)],
            "artifact_path": str(latest_divergence_path),
        },
    )
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
                "feature_divergence_rate_threshold": 1.0,
                "decision_divergence_rate_threshold": 1.0,
                "confidence_delta": 0.2,
            },
            runtime_health={},
            lane="live_candidate",
            unit_name="autobot-live-alpha-candidate.service",
            rollout_mode="canary",
            ts_ms=99_000,
            project_root=tmp_path,
        )

    assert report["policy"] == "canary_confidence_sequence_v1"
    assert report["decision"]["promote_eligible"] is True
    assert report["decision"]["status"] == "promote_eligible"
    assert report["reward_stream"]["risk_adjusted_return_lcb"] > 0.0


def test_canary_confidence_sequence_requires_divergence_evidence_for_promotion(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    run_id = "run-promote-no-divergence"
    with LiveStateStore(db_path) as store:
        for idx in range(12):
            store.upsert_trade_journal(
                TradeJournalRecord(
                    journal_id=f"journal-no-divergence-{idx}",
                    market="KRW-BTC",
                    status="CLOSED",
                    entry_intent_id=f"intent-no-divergence-{idx}",
                    entry_order_uuid=f"entry-no-divergence-{idx}",
                    exit_order_uuid=f"exit-no-divergence-{idx}",
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
            project_root=tmp_path,
        )

    assert report["decision"]["promote_eligible"] is False
    assert report["decision"]["status"] == "continue"
    assert "CANARY_DIVERGENCE_INSUFFICIENT_EVIDENCE" in report["decision"]["blocking_reason_codes"]


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


def test_canary_confidence_sequence_reads_divergence_artifact(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    run_id = "run-divergence"
    latest_divergence_path = paper_live_divergence_latest_path(
        project_root=tmp_path,
        unit_name="autobot-live-alpha-candidate.service",
    )
    write_paper_live_divergence_report(
        latest_path=latest_divergence_path,
        payload={
            "artifact_version": 1,
            "status": "ready",
            "matching": {"matched_opportunities": 6},
            "feature_divergence": {"feature_hash_match_ratio": 0.0, "feature_divergence_rate": 1.0},
            "decision_divergence": {"decision_divergence_rate": 0.5},
            "matched_records": [{"feature_hash_match": False, "decision_match": False} for _ in range(6)],
            "artifact_path": str(latest_divergence_path),
        },
    )
    with LiveStateStore(db_path) as store:
        for idx in range(6):
            store.upsert_trade_journal(
                TradeJournalRecord(
                    journal_id=f"journal-divergence-{idx}",
                    market="KRW-BTC",
                    status="CLOSED",
                    entry_intent_id=f"intent-divergence-{idx}",
                    entry_order_uuid=f"entry-divergence-{idx}",
                    exit_order_uuid=f"exit-divergence-{idx}",
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

        report = build_canary_confidence_sequence_report(
            store=store,
            run_id=run_id,
            confidence_monitor_config={
                "min_closed_trade_count": 4,
                "confidence_delta": 0.2,
                "feature_divergence_rate_threshold": 0.5,
                "feature_divergence_rate_reason_code": "FEATURE_DIVERGENCE_CS_BREACH",
            },
            runtime_health={},
            lane="live_candidate",
            unit_name="autobot-live-alpha-candidate.service",
            rollout_mode="canary",
            ts_ms=99_000,
            project_root=tmp_path,
        )

    monitor = report["monitors"]["paper_live_feature_divergence_rate"]
    assert monitor["available"] is True
    assert monitor["halt_triggered"] is True
    assert report["decision"]["execution_liquidation_summary"]["exit_decision_reasons_top"] == []
    assert report["decision"]["abort"] is True
    assert "FEATURE_DIVERGENCE_CS_BREACH" in report["decision"]["abort_reason_codes"]
