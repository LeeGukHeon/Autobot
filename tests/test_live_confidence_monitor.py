from __future__ import annotations

import json
from pathlib import Path

from autobot.live import model_alpha_runtime_execute as runtime_execute
from autobot.live.paper_live_divergence import paper_live_divergence_latest_path, write_paper_live_divergence_report
from autobot.live.state_store import ExecutionAttemptRecord, IntentRecord, LiveStateStore, TradeJournalRecord
from autobot.risk.confidence_monitor import (
    build_live_risk_confidence_sequence_report,
    live_risk_confidence_sequence_latest_path,
    write_live_risk_confidence_sequence_report,
)


def _intent_meta(run_id: str) -> str:
    return json.dumps({"runtime": {"live_runtime_model_run_id": run_id}}, ensure_ascii=False, sort_keys=True)


def _journal_entry_meta(run_id: str) -> str:
    return json.dumps(
        {"runtime": {"live_runtime_model_run_id": run_id}},
        ensure_ascii=False,
        sort_keys=True,
    )


def _journal_exit_meta() -> str:
    return json.dumps({"close_verified": True}, ensure_ascii=False, sort_keys=True)


def test_build_live_risk_confidence_sequence_report_triggers_trade_and_execution_monitors(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    run_id = "run-live"
    with LiveStateStore(db_path) as store:
        for index in range(12):
            intent_id = f"intent-{index}"
            journal_id = f"journal-{index}"
            store.upsert_intent(
                IntentRecord(
                    intent_id=intent_id,
                    ts_ms=1_000 + index,
                    market="KRW-BTC",
                    side="bid",
                    price=100.0,
                    volume=1.0,
                    reason_code="MODEL_ALPHA_ENTRY_V1",
                    meta_json=_intent_meta(run_id),
                    status="SUBMITTED",
                )
            )
            if index < 8:
                store.upsert_trade_journal(
                    TradeJournalRecord(
                        journal_id=journal_id,
                        market="KRW-BTC",
                        status="CLOSED",
                        entry_intent_id=intent_id,
                        entry_order_uuid=f"entry-{index}",
                        exit_order_uuid=f"exit-{index}",
                        plan_id=f"plan-{index}",
                        entry_submitted_ts_ms=1_000 + index,
                        entry_filled_ts_ms=1_100 + index,
                        exit_ts_ms=2_000 + index,
                        entry_price=100.0,
                        exit_price=98.0,
                        qty=1.0,
                        entry_notional_quote=100.0,
                        exit_notional_quote=98.0,
                        realized_pnl_quote=-2.0,
                        realized_pnl_pct=-2.0,
                        expected_net_edge_bps=12.0,
                        entry_meta_json=_journal_entry_meta(run_id),
                        exit_meta_json=_journal_exit_meta(),
                        updated_ts=2_000 + index,
                    )
                )
            store.upsert_execution_attempt(
                ExecutionAttemptRecord(
                    attempt_id=f"attempt-{index}",
                    journal_id=journal_id,
                    intent_id=intent_id,
                    order_uuid=f"order-{index}",
                    order_identifier=f"AUTOBOT-{index}",
                    market="KRW-BTC",
                    side="bid",
                    ord_type="limit",
                    time_in_force="gtc",
                    action_code="JOIN",
                    price_mode="JOIN",
                    requested_price=100.0,
                    requested_volume=1.0,
                    requested_notional_quote=100.0,
                    reference_price=100.0,
                    submitted_ts_ms=1_000 + index,
                    final_ts_ms=2_000 + index,
                    final_state="MISSED" if index < 10 else "FILLED",
                    filled_volume=0.0 if index < 10 else 1.0,
                    fill_fraction=0.0 if index < 10 else 1.0,
                    partial_fill=False,
                    full_fill=bool(index >= 10),
                    outcome_json="{}",
                    updated_ts=2_000 + index,
                )
            )

        report = build_live_risk_confidence_sequence_report(
            store=store,
            run_id=run_id,
            confidence_monitor_config={
                "enabled": True,
                "mode": "time_uniform_chernoff_rate_v1",
                "confidence_delta": 0.10,
                "min_closed_trade_count": 8,
                "min_execution_attempt_count": 12,
                "nonpositive_rate_threshold": 0.45,
                "severe_loss_rate_threshold": 0.20,
                "execution_miss_rate_threshold": 0.55,
                "edge_gap_breach_rate_threshold": 0.60,
                "edge_gap_tolerance_bps": 5.0,
                "severe_loss_return_threshold": 0.01,
                "nonpositive_rate_reason_code": "RISK_CONTROL_NONPOSITIVE_RATE_CS_BREACH",
                "severe_loss_rate_reason_code": "RISK_CONTROL_SEVERE_LOSS_RATE_CS_BREACH",
                "execution_miss_rate_reason_code": "EXECUTION_MISS_RATE_CS_BREACH",
                "edge_gap_rate_reason_code": "RISK_CONTROL_EDGE_GAP_CS_BREACH",
                "feature_divergence_rate_threshold": 0.10,
                "feature_divergence_rate_reason_code": "FEATURE_DIVERGENCE_CS_BREACH",
            },
            runtime_health={"model_pointer_divergence": False, "ws_public_stale": False},
            lane="live_candidate",
            unit_name="autobot-live-alpha-candidate.service",
            rollout_mode="canary",
            ts_ms=10_000,
        )

    assert report["halt_triggered"] is True
    assert "RISK_CONTROL_NONPOSITIVE_RATE_CS_BREACH" in report["triggered_reason_codes"]
    assert "RISK_CONTROL_SEVERE_LOSS_RATE_CS_BREACH" in report["triggered_reason_codes"]
    assert "EXECUTION_MISS_RATE_CS_BREACH" in report["triggered_reason_codes"]
    assert "RISK_CONTROL_EDGE_GAP_CS_BREACH" in report["triggered_reason_codes"]
    assert report["monitors"]["paper_live_feature_divergence_rate"]["available"] is False
    assert report["monitors"]["paper_live_feature_divergence_rate"]["status"] == "insufficient_evidence"


def test_resolve_execution_risk_control_online_threshold_merges_confidence_sequence_halts(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    run_id = "run-live"
    with LiveStateStore(db_path) as store:
        for index in range(12):
            intent_id = f"intent-{index}"
            journal_id = f"journal-{index}"
            store.upsert_intent(
                IntentRecord(
                    intent_id=intent_id,
                    ts_ms=1_000 + index,
                    market="KRW-BTC",
                    side="bid",
                    price=100.0,
                    volume=1.0,
                    reason_code="MODEL_ALPHA_ENTRY_V1",
                    meta_json=_intent_meta(run_id),
                    status="SUBMITTED",
                )
            )
            store.upsert_trade_journal(
                TradeJournalRecord(
                    journal_id=journal_id,
                    market="KRW-BTC",
                    status="CANCELLED_ENTRY",
                    entry_intent_id=intent_id,
                    expected_net_edge_bps=10.0,
                    entry_meta_json=_journal_entry_meta(run_id),
                    exit_meta_json=json.dumps({"entry_cancelled": True}, ensure_ascii=False, sort_keys=True),
                    updated_ts=2_000 + index,
                )
            )
            store.upsert_execution_attempt(
                ExecutionAttemptRecord(
                    attempt_id=f"attempt-{index}",
                    journal_id=journal_id,
                    intent_id=intent_id,
                    order_uuid=f"order-{index}",
                    order_identifier=f"AUTOBOT-{index}",
                    market="KRW-BTC",
                    side="bid",
                    ord_type="limit",
                    time_in_force="gtc",
                    action_code="JOIN",
                    price_mode="JOIN",
                    requested_price=100.0,
                    requested_volume=1.0,
                    requested_notional_quote=100.0,
                    reference_price=100.0,
                    submitted_ts_ms=1_000 + index,
                    final_ts_ms=2_000 + index,
                    final_state="MISSED",
                    filled_volume=0.0,
                    fill_fraction=0.0,
                    partial_fill=False,
                    full_fill=False,
                    outcome_json="{}",
                    updated_ts=2_000 + index,
                )
            )

        state = runtime_execute.resolve_execution_risk_control_online_threshold(
            store=store,
            run_id=run_id,
            risk_control_payload={
                "version": 1,
                "policy": "execution_risk_control_hoeffding_v1",
                "status": "ready",
                "selected_threshold": 1.0,
                "threshold_results": [{"threshold": 1.0}],
                "nonpositive_alpha": 0.95,
                "severe_loss_alpha": 0.95,
                "online_adaptation": {
                    "enabled": True,
                    "mode": "recent_closed_trade_hoeffding_stepup_v1",
                    "lookback_trades": 12,
                    "max_step_up": 0,
                    "recovery_streak_required": 1,
                    "min_halt_trade_count": 12,
                    "halt_breach_streak": 99,
                    "halt_reason_code": "RISK_CONTROL_ONLINE_BREACH_STREAK",
                    "confidence_delta": 0.10,
                    "checkpoint_name": "execution_risk_control_online_buffer",
                },
                "confidence_sequence_monitors": {
                    "enabled": True,
                    "mode": "time_uniform_chernoff_rate_v1",
                    "confidence_delta": 0.10,
                    "min_closed_trade_count": 12,
                    "min_execution_attempt_count": 12,
                    "nonpositive_rate_threshold": 0.95,
                    "severe_loss_rate_threshold": 0.95,
                    "execution_miss_rate_threshold": 0.20,
                    "edge_gap_breach_rate_threshold": 0.95,
                    "edge_gap_tolerance_bps": 5.0,
                    "severe_loss_return_threshold": 0.01,
                    "nonpositive_rate_reason_code": "RISK_CONTROL_NONPOSITIVE_RATE_CS_BREACH",
                    "severe_loss_rate_reason_code": "RISK_CONTROL_SEVERE_LOSS_RATE_CS_BREACH",
                    "execution_miss_rate_reason_code": "EXECUTION_MISS_RATE_CS_BREACH",
                    "edge_gap_rate_reason_code": "RISK_CONTROL_EDGE_GAP_CS_BREACH",
                    "feature_divergence_rate_threshold": 0.10,
                    "feature_divergence_rate_reason_code": "FEATURE_DIVERGENCE_CS_BREACH",
                },
            },
        )

    assert state["halt_triggered"] is True
    assert "EXECUTION_MISS_RATE_CS_BREACH" in state["halt_reason_codes"]
    assert state["halt_reason_code"] == "EXECUTION_MISS_RATE_CS_BREACH"
    assert state["confidence_sequence"]["monitors"]["execution_miss_rate"]["halt_triggered"] is True


def test_write_live_risk_confidence_sequence_report_writes_latest_artifact(tmp_path: Path) -> None:
    latest_path = live_risk_confidence_sequence_latest_path(
        project_root=tmp_path,
        unit_name="autobot-live-alpha-candidate.service",
    )
    write_live_risk_confidence_sequence_report(
        latest_path=latest_path,
        payload={"artifact_version": 1, "halt_triggered": False},
    )

    assert latest_path.exists()
    assert json.loads(latest_path.read_text(encoding="utf-8"))["artifact_version"] == 1


def test_build_live_risk_confidence_sequence_report_reads_divergence_artifact(tmp_path: Path) -> None:
    latest_divergence_path = paper_live_divergence_latest_path(
        project_root=tmp_path,
        unit_name="autobot-live-alpha-candidate.service",
    )
    write_paper_live_divergence_report(
        latest_path=latest_divergence_path,
        payload={
            "artifact_version": 1,
            "status": "ready",
            "matching": {"matched_opportunities": 8},
            "feature_divergence": {"feature_hash_match_ratio": 0.25, "feature_divergence_rate": 0.75},
            "decision_divergence": {"decision_divergence_rate": 0.25},
            "matched_records": [
                {"feature_hash_match": False, "decision_match": True},
                {"feature_hash_match": False, "decision_match": True},
                {"feature_hash_match": False, "decision_match": True},
                {"feature_hash_match": False, "decision_match": True},
                {"feature_hash_match": False, "decision_match": False},
                {"feature_hash_match": False, "decision_match": False},
                {"feature_hash_match": True, "decision_match": True},
                {"feature_hash_match": True, "decision_match": True},
            ],
            "artifact_path": str(latest_divergence_path),
        },
    )
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        report = build_live_risk_confidence_sequence_report(
            store=store,
            run_id="run-live",
            confidence_monitor_config={
                "enabled": True,
                "mode": "time_uniform_chernoff_rate_v1",
                "confidence_delta": 0.10,
                "min_closed_trade_count": 4,
                "min_execution_attempt_count": 4,
                "nonpositive_rate_threshold": 1.0,
                "severe_loss_rate_threshold": 1.0,
                "execution_miss_rate_threshold": 1.0,
                "edge_gap_breach_rate_threshold": 1.0,
                "feature_divergence_rate_threshold": 0.50,
                "feature_divergence_rate_reason_code": "FEATURE_DIVERGENCE_CS_BREACH",
            },
            runtime_health={},
            lane="live_candidate",
            unit_name="autobot-live-alpha-candidate.service",
            rollout_mode="canary",
            ts_ms=10_000,
            project_root=tmp_path,
        )

    monitor = report["monitors"]["paper_live_feature_divergence_rate"]
    assert monitor["available"] is True
    assert monitor["status"] == "ready"
    assert monitor["halt_triggered"] is True
    assert monitor["reason_code"] == "FEATURE_DIVERGENCE_CS_BREACH"
    decision_monitor = report["monitors"]["paper_live_decision_divergence_rate"]
    assert decision_monitor["available"] is True
    assert decision_monitor["status"] == "ready"
    assert decision_monitor["decision_divergence_rate"] == 0.25
