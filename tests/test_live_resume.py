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


def test_resume_preserves_model_derived_risk_plan_metadata(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_position(
            PositionRecord(
                market="KRW-KITE",
                base_currency="KITE",
                base_amount=13.56787669,
                avg_entry_price=442.0,
                updated_ts=1000,
            )
        )
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="model-risk-intent-1",
                market="KRW-KITE",
                side="long",
                entry_price_str="442",
                qty_str="13.56787669",
                tp_enabled=False,
                tp_pct=0.0,
                sl_enabled=False,
                sl_pct=0.0,
                trailing_enabled=False,
                trail_pct=0.0,
                state="ACTIVE",
                last_eval_ts_ms=950,
                last_action_ts_ms=960,
                replace_attempt=0,
                created_ts=1000,
                updated_ts=1001,
                timeout_ts_ms=1801000,
                plan_source="model_alpha_v1",
                source_intent_id="intent-1",
            )
        )

        report = resume_risk_plans_after_reconcile(store=store, ts_ms=2000)
        plan = store.risk_plan_by_id(plan_id="model-risk-intent-1")

    assert report["halted"] is False
    assert report["counts"]["plans_kept_active"] == 1
    assert plan is not None
    assert plan["state"] == "ACTIVE"
    assert plan["timeout_ts_ms"] == 1801000
    assert plan["plan_source"] == "model_alpha_v1"
    assert plan["source_intent_id"] == "intent-1"


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


def test_resume_adopts_single_market_exit_order_for_triggered_plan(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_position(
            PositionRecord(
                market="KRW-KITE",
                base_currency="KITE",
                base_amount=12.77,
                avg_entry_price=441.0,
                updated_ts=1000,
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="exit-kite-1",
                identifier="AUTOBOT-autobot-candidate-001-kite-exit",
                market="KRW-KITE",
                side="ask",
                ord_type="limit",
                price=443.0,
                volume_req=12.77,
                volume_filled=0.0,
                state="wait",
                created_ts=1000,
                updated_ts=1001,
                intent_id="intent-kite-exit",
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="ORDER_STATE",
                event_source="test",
                root_order_uuid="exit-kite-1",
            )
        )
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="plan-kite-1",
                market="KRW-KITE",
                side="long",
                entry_price_str="441",
                qty_str="12.77",
                tp_enabled=False,
                sl_enabled=False,
                trailing_enabled=False,
                state="TRIGGERED",
                last_eval_ts_ms=950,
                last_action_ts_ms=0,
                replace_attempt=0,
                created_ts=900,
                updated_ts=960,
                timeout_ts_ms=2700000,
                plan_source="model_alpha_v1",
                source_intent_id="intent-kite-entry",
            )
        )

        report = resume_risk_plans_after_reconcile(store=store, ts_ms=2000)
        plan = store.risk_plan_by_id(plan_id="plan-kite-1")
        order = store.order_by_uuid(uuid="exit-kite-1")

    assert report["halted"] is False
    assert report["counts"]["plans_resumed_exiting"] == 1
    assert plan is not None
    assert plan["state"] == "EXITING"
    assert plan["current_exit_order_uuid"] == "exit-kite-1"
    assert plan["last_action_ts_ms"] == 2000
    assert order is not None
    assert order["tp_sl_link"] == "plan-kite-1"


def test_resume_ignores_closed_history_when_primary_active_plan_exists(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_position(
            PositionRecord(
                market="KRW-KITE",
                base_currency="KITE",
                base_amount=12.77,
                avg_entry_price=441.0,
                updated_ts=1000,
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="exit-kite-active",
                identifier="AUTOBOT-autobot-candidate-001-kite-active-exit",
                market="KRW-KITE",
                side="ask",
                ord_type="limit",
                price=443.0,
                volume_req=12.77,
                volume_filled=0.0,
                state="wait",
                created_ts=1000,
                updated_ts=1001,
                intent_id="intent-kite-exit",
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="ORDER_STATE",
                event_source="test",
                root_order_uuid="exit-kite-active",
            )
        )
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="plan-kite-active",
                market="KRW-KITE",
                side="long",
                entry_price_str="441",
                qty_str="12.77",
                state="EXITING",
                current_exit_order_uuid="exit-kite-active",
                current_exit_order_identifier="AUTOBOT-autobot-candidate-001-kite-active-exit",
                created_ts=1000,
                updated_ts=1001,
                plan_source="model_alpha_v1",
                source_intent_id="intent-kite-entry",
            )
        )
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="plan-kite-closed-1",
                market="KRW-KITE",
                side="long",
                entry_price_str="441",
                qty_str="12.70",
                state="CLOSED",
                created_ts=100,
                updated_ts=200,
                plan_source="model_alpha_v1",
                source_intent_id="intent-kite-old-1",
            )
        )
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="plan-kite-closed-2",
                market="KRW-KITE",
                side="long",
                entry_price_str="441",
                qty_str="12.72",
                state="CLOSED",
                created_ts=300,
                updated_ts=400,
                plan_source="model_alpha_v1",
                source_intent_id="intent-kite-old-2",
            )
        )

        report = resume_risk_plans_after_reconcile(store=store, ts_ms=2000)
        active_plan = store.risk_plan_by_id(plan_id="plan-kite-active")
        closed_plan = store.risk_plan_by_id(plan_id="plan-kite-closed-1")

    assert report["halted"] is False
    assert report["counts"]["plans_resumed_exiting"] == 1
    assert report["counts"]["plans_halted_for_review"] == 0
    assert any(item["action"] == "KEEP_CLOSED_HISTORY" for item in report["plans"])
    assert active_plan is not None
    assert active_plan["state"] == "EXITING"
    assert closed_plan is not None
    assert closed_plan["state"] == "CLOSED"


def test_resume_halts_when_multiple_active_plans_exist_for_same_market(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_position(
            PositionRecord(
                market="KRW-KITE",
                base_currency="KITE",
                base_amount=12.77,
                avg_entry_price=441.0,
                updated_ts=1000,
            )
        )
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="plan-kite-primary",
                market="KRW-KITE",
                side="long",
                entry_price_str="441",
                qty_str="12.77",
                state="ACTIVE",
                created_ts=1000,
                updated_ts=1001,
                plan_source="model_alpha_v1",
                source_intent_id="intent-kite-primary",
            )
        )
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="plan-kite-duplicate",
                market="KRW-KITE",
                side="long",
                entry_price_str="441",
                qty_str="12.77",
                state="TRIGGERED",
                created_ts=900,
                updated_ts=950,
                plan_source="model_alpha_v1",
                source_intent_id="intent-kite-duplicate",
            )
        )

        report = resume_risk_plans_after_reconcile(store=store, ts_ms=2000)

    assert report["halted"] is True
    assert report["counts"]["plans_halted_for_review"] == 1
    assert report["halted_plan_ids"] == ["plan-kite-duplicate"]
    assert any(item["action"] == "HALT_DUPLICATE_ACTIVE_PLAN" for item in report["plans"])
