from __future__ import annotations

import json
from pathlib import Path

from autobot.live.reconcile import resume_risk_plans_after_reconcile
from autobot.live.state_store import LiveStateStore, OrderRecord, PositionRecord, RiskPlanRecord


def test_resume_relinks_open_exit_order(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_position(
            PositionRecord(
                market="KRW-BTC",
                base_currency="BTC",
                base_amount=0.01,
                avg_entry_price=100000000.0,
                updated_ts=1000,
                trailing_json=json.dumps({"enabled": True, "high_watermark_price_str": "110000000"}, ensure_ascii=False),
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="exit-1",
                identifier="AUTOBOT-RISK-EXIT-1",
                market="KRW-BTC",
                side="ask",
                ord_type="limit",
                price=101000000.0,
                volume_req=0.01,
                volume_filled=0.0,
                state="wait",
                created_ts=1000,
                updated_ts=1001,
                intent_id="intent-exit-1",
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="ORDER_STATE",
                event_source="test",
                root_order_uuid="exit-1",
            )
        )
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="plan-1",
                market="KRW-BTC",
                side="long",
                entry_price_str="100000000",
                qty_str="0.01",
                tp_enabled=True,
                tp_pct=3.0,
                sl_enabled=True,
                sl_pct=2.0,
                trailing_enabled=True,
                trail_pct=0.01,
                high_watermark_price_str="110000000",
                armed_ts_ms=900,
                state="EXITING",
                last_eval_ts_ms=950,
                last_action_ts_ms=960,
                current_exit_order_uuid="exit-1",
                current_exit_order_identifier="AUTOBOT-RISK-EXIT-1",
                replace_attempt=0,
                created_ts=900,
                updated_ts=960,
            )
        )

        report = resume_risk_plans_after_reconcile(store=store, ts_ms=2000)
        plan = store.risk_plan_by_id(plan_id="plan-1")

    assert report["halted"] is False
    assert report["counts"]["plans_resumed_exiting"] == 1
    assert plan is not None
    assert plan["state"] == "EXITING"
    assert plan["current_exit_order_uuid"] == "exit-1"
    assert plan["trailing"]["high_watermark_price_str"] == "110000000"


def test_resume_closes_plan_when_position_missing(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="plan-2",
                market="KRW-ETH",
                side="long",
                entry_price_str="3000000",
                qty_str="0.1",
                tp_enabled=True,
                tp_pct=3.0,
                sl_enabled=True,
                sl_pct=2.0,
                trailing_enabled=False,
                state="EXITING",
                last_eval_ts_ms=950,
                last_action_ts_ms=960,
                current_exit_order_uuid="missing-exit",
                current_exit_order_identifier="AUTOBOT-RISK-MISSING",
                replace_attempt=1,
                created_ts=900,
                updated_ts=960,
            )
        )

        report = resume_risk_plans_after_reconcile(store=store, ts_ms=2000)
        plan = store.risk_plan_by_id(plan_id="plan-2")

    assert report["halted"] is False
    assert report["counts"]["plans_closed"] == 1
    assert plan is not None
    assert plan["state"] == "CLOSED"
    assert plan["current_exit_order_uuid"] is None


def test_resume_retriggers_when_position_open_but_exit_missing(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_position(
            PositionRecord(
                market="KRW-XRP",
                base_currency="XRP",
                base_amount=100.0,
                avg_entry_price=500.0,
                updated_ts=1000,
            )
        )
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="plan-3",
                market="KRW-XRP",
                side="long",
                entry_price_str="500",
                qty_str="100",
                tp_enabled=True,
                tp_pct=3.0,
                sl_enabled=True,
                sl_pct=2.0,
                trailing_enabled=False,
                state="EXITING",
                last_eval_ts_ms=950,
                last_action_ts_ms=960,
                current_exit_order_uuid="gone-exit",
                current_exit_order_identifier="AUTOBOT-RISK-GONE",
                replace_attempt=1,
                created_ts=900,
                updated_ts=960,
            )
        )

        report = resume_risk_plans_after_reconcile(store=store, ts_ms=2000)
        plan = store.risk_plan_by_id(plan_id="plan-3")

    assert report["halted"] is False
    assert report["counts"]["plans_retriggered"] == 1
    assert plan is not None
    assert plan["state"] == "TRIGGERED"
    assert plan["current_exit_order_uuid"] is None


def test_resume_halts_on_ambiguous_market_exit(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_position(
            PositionRecord(
                market="KRW-SOL",
                base_currency="SOL",
                base_amount=1.0,
                avg_entry_price=100000.0,
                updated_ts=1000,
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="other-exit",
                identifier="AUTOBOT-RISK-OTHER",
                market="KRW-SOL",
                side="ask",
                ord_type="limit",
                price=101000.0,
                volume_req=1.0,
                volume_filled=0.0,
                state="wait",
                created_ts=1000,
                updated_ts=1001,
                intent_id="intent-other-exit",
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="ORDER_STATE",
                event_source="test",
                root_order_uuid="other-exit",
            )
        )
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="plan-4",
                market="KRW-SOL",
                side="long",
                entry_price_str="100000",
                qty_str="1",
                tp_enabled=True,
                tp_pct=3.0,
                sl_enabled=True,
                sl_pct=2.0,
                trailing_enabled=False,
                state="EXITING",
                last_eval_ts_ms=950,
                last_action_ts_ms=960,
                current_exit_order_uuid="missing-exit",
                current_exit_order_identifier="AUTOBOT-RISK-MISSING",
                replace_attempt=1,
                created_ts=900,
                updated_ts=960,
            )
        )

        report = resume_risk_plans_after_reconcile(store=store, ts_ms=2000)

    assert report["halted"] is True
    assert report["counts"]["plans_halted_for_review"] == 1
    assert report["halted_plan_ids"] == ["plan-4"]
