from __future__ import annotations

import json
from pathlib import Path

from autobot.live.risk_budget_ledger import (
    append_live_risk_budget_entry,
    initialize_live_risk_budget_ledger,
)
from autobot.live.state_store import LiveStateStore, OrderRecord, PositionRecord


def test_risk_budget_ledger_tracks_sizing_and_skip_reasons(tmp_path: Path) -> None:
    ledger_path = tmp_path / "logs" / "risk_budget_ledger" / "candidate" / "latest.jsonl"
    latest_path = ledger_path.with_name("latest.json")
    initialize_live_risk_budget_ledger(
        ledger_path=ledger_path,
        latest_path=latest_path,
        lane="live_candidate",
        unit_name="autobot-live-alpha-candidate.service",
        rollout_mode="canary",
    )

    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.upsert_position(
            PositionRecord(
                market="KRW-BTC",
                base_currency="BTC",
                base_amount=0.01,
                avg_entry_price=100_000_000.0,
                updated_ts=1000,
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="order-1",
                identifier="AUTOBOT-1",
                market="KRW-ETH",
                side="bid",
                ord_type="limit",
                price=5000.0,
                volume_req=2.0,
                volume_filled=0.5,
                state="wait",
                created_ts=1500,
                updated_ts=1500,
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="SUBMIT_ACCEPTED",
                event_source="live_model_alpha_runtime",
                remaining_fee=5.0,
            )
        )
        entry, summary = append_live_risk_budget_entry(
            ledger_path=ledger_path,
            latest_path=latest_path,
            store=store,
            lane="live_candidate",
            unit_name="autobot-live-alpha-candidate.service",
            rollout_mode="canary",
            market="KRW-ETH",
            side="bid",
            status="SKIPPED",
            reason_code="MODEL_ALPHA_ENTRY_V1",
            meta_payload={
                "skip_reason": "RISK_CONTROL_BELOW_THRESHOLD",
                "strategy": {
                    "meta": {
                        "notional_multiplier": 1.2,
                        "score_std": 0.25,
                    }
                },
                "size_ladder": {
                    "enabled": True,
                    "requested_multiplier": 1.2,
                    "resolved_multiplier": 0.8,
                },
                "sizing": {
                    "target_notional_quote": 12_000.0,
                    "admissible_notional_quote": 11_994.0,
                },
                "risk_control": {
                    "enabled": True,
                    "allowed": False,
                    "reason_code": "RISK_CONTROL_BELOW_THRESHOLD",
                },
                "risk_control_online": {
                    "enabled": True,
                    "recent_trade_count": 4,
                    "recent_severe_loss_rate": 0.25,
                    "recent_severe_loss_rate_ucb": 0.4,
                },
            },
            ts_ms=2000,
            intent_id="intent-1",
            base_budget_quote=10_000.0,
        )

    rows = [json.loads(line) for line in ledger_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    latest = json.loads(latest_path.read_text(encoding="utf-8"))
    eth_cluster = next(
        item for item in entry["cluster_utilization"]["clusters"] if item["cluster_id"] == "ETH_LED"
    )

    assert len(rows) == 1
    assert entry["current_total_cash_at_risk_quote"] == 1_007_505.0
    assert entry["projected_total_cash_at_risk_quote"] == 1_019_505.0
    assert entry["cluster_utilization"]["decision_cluster_id"] == "ETH_LED"
    assert eth_cluster["open_bid_order_notional_quote"] == 7_505.0
    assert entry["cluster_utilization"]["decision_cluster_current_notional_quote"] == 7_505.0
    assert entry["cluster_utilization"]["decision_cluster_projected_notional_quote"] == 19_505.0
    assert entry["sizing"]["position_budget_fraction"] == 1.2
    assert entry["budget_reason_codes"] == ["RISK_CONTROL_BELOW_THRESHOLD"]
    assert entry["current_risk_regime"]["entry_state"] == "risk_blocked"
    assert entry["uncertainty_weighted_exposure"]["uncertainty"] == 0.25
    assert entry["uncertainty_weighted_exposure"]["weighted_notional_quote"] == 9_600.0
    assert summary["total_entries"] == 1
    assert latest["skip_reason_counts"]["RISK_CONTROL_BELOW_THRESHOLD"] == 1


def test_initialize_risk_budget_ledger_preserves_existing_history_on_restart(tmp_path: Path) -> None:
    ledger_path = tmp_path / "logs" / "risk_budget_ledger" / "candidate" / "latest.jsonl"
    latest_path = ledger_path.with_name("latest.json")
    initialize_live_risk_budget_ledger(
        ledger_path=ledger_path,
        latest_path=latest_path,
        lane="live_candidate",
        unit_name="autobot-live-alpha-candidate.service",
        rollout_mode="canary",
    )

    with LiveStateStore(tmp_path / "live_state.db") as store:
        append_live_risk_budget_entry(
            ledger_path=ledger_path,
            latest_path=latest_path,
            store=store,
            lane="live_candidate",
            unit_name="autobot-live-alpha-candidate.service",
            rollout_mode="canary",
            market="KRW-BTC",
            side="bid",
            status="SHADOW",
            reason_code="MODEL_ALPHA_ENTRY_V1",
            meta_payload={"sizing": {"target_notional_quote": 5000.0}},
            ts_ms=1000,
            intent_id="intent-1",
            base_budget_quote=10_000.0,
        )

    before_lines = ledger_path.read_text(encoding="utf-8").splitlines()
    before_latest = json.loads(latest_path.read_text(encoding="utf-8"))

    initialize_live_risk_budget_ledger(
        ledger_path=ledger_path,
        latest_path=latest_path,
        lane="live_candidate",
        unit_name="autobot-live-alpha-candidate.service",
        rollout_mode="canary",
    )

    after_lines = ledger_path.read_text(encoding="utf-8").splitlines()
    after_latest = json.loads(latest_path.read_text(encoding="utf-8"))

    assert after_lines == before_lines
    assert after_latest["total_entries"] == before_latest["total_entries"] == 1
    assert after_latest["last_entry"]["intent_id"] == "intent-1"


def test_initialize_risk_budget_ledger_rebuilds_latest_summary_when_missing(tmp_path: Path) -> None:
    ledger_path = tmp_path / "logs" / "risk_budget_ledger" / "candidate" / "latest.jsonl"
    latest_path = ledger_path.with_name("latest.json")
    initialize_live_risk_budget_ledger(
        ledger_path=ledger_path,
        latest_path=latest_path,
        lane="live_candidate",
        unit_name="autobot-live-alpha-candidate.service",
        rollout_mode="canary",
    )

    with LiveStateStore(tmp_path / "live_state.db") as store:
        append_live_risk_budget_entry(
            ledger_path=ledger_path,
            latest_path=latest_path,
            store=store,
            lane="live_candidate",
            unit_name="autobot-live-alpha-candidate.service",
            rollout_mode="canary",
            market="KRW-ETH",
            side="bid",
            status="SKIPPED",
            reason_code="MODEL_ALPHA_ENTRY_V1",
            meta_payload={"skip_reason": "RISK_CONTROL_ONLINE_BREACH_STREAK", "sizing": {"target_notional_quote": 7000.0}},
            ts_ms=2000,
            intent_id="intent-2",
            base_budget_quote=10_000.0,
        )

    latest_path.unlink()
    initialize_live_risk_budget_ledger(
        ledger_path=ledger_path,
        latest_path=latest_path,
        lane="live_candidate",
        unit_name="autobot-live-alpha-candidate.service",
        rollout_mode="canary",
    )
    rebuilt = json.loads(latest_path.read_text(encoding="utf-8"))

    assert rebuilt["total_entries"] == 1
    assert rebuilt["skip_reason_counts"]["RISK_CONTROL_ONLINE_BREACH_STREAK"] == 1
    assert rebuilt["last_entry"]["intent_id"] == "intent-2"
