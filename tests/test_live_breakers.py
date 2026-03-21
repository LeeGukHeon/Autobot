from __future__ import annotations

from pathlib import Path

from autobot.live.breakers import (
    ACTION_FULL_KILL_SWITCH,
    ACTION_HALT_NEW_INTENTS,
    arm_breaker,
    breaker_status,
    clear_recovered_risk_exit_stuck_breaker,
    clear_breaker_reasons,
    clear_breaker,
    classify_upbit_exception,
    evaluate_cycle_contracts,
    record_counter_failure,
    reset_counter,
)
from autobot.live.state_store import LiveStateStore, OrderRecord, RiskPlanRecord
from autobot.upbit.exceptions import AuthError, RateLimitError


def test_breaker_arm_and_clear_persists_state(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        armed = arm_breaker(
            store,
            reason_codes=["MANUAL_KILL_SWITCH"],
            source="test",
            ts_ms=1000,
            action=ACTION_FULL_KILL_SWITCH,
            details={"note": "manual"},
        )
        current = breaker_status(store)
        cleared = clear_breaker(store, source="test", ts_ms=2000, details={"note": "clear"})

    assert armed["active"] is True
    assert armed["action"] == ACTION_FULL_KILL_SWITCH
    assert "MANUAL_KILL_SWITCH" in armed["reason_codes"]
    assert current["active"] is True
    assert cleared["active"] is False
    assert cleared["new_intents_allowed"] is True
    assert (tmp_path / "live_breaker_report.json").exists()


def test_breaker_counter_arms_after_threshold(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        record_counter_failure(
            store,
            counter_name="replace_reject",
            limit=2,
            source="test",
            ts_ms=1000,
            details={"attempt": 1},
        )
        status_after_second = record_counter_failure(
            store,
            counter_name="replace_reject",
            limit=2,
            source="test",
            ts_ms=2000,
            details={"attempt": 2},
        )

    assert status_after_second["active"] is True
    assert status_after_second["action"] == ACTION_HALT_NEW_INTENTS
    assert "REPEATED_REPLACE_REJECTS" in status_after_second["reason_codes"]
    assert status_after_second["counters"]["replace_reject"]["count"] == 2


def test_reset_counter_clears_counter_reason_from_active_breaker(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        record_counter_failure(
            store,
            counter_name="replace_reject",
            limit=1,
            source="test",
            ts_ms=1000,
            details={"attempt": 1},
        )
        cleared = reset_counter(
            store,
            counter_name="replace_reject",
            source="test",
            ts_ms=2000,
        )

    assert cleared["active"] is False
    assert "REPEATED_REPLACE_REJECTS" not in cleared["reason_codes"]
    assert cleared["counters"]["replace_reject"]["count"] == 0


def test_clear_breaker_reasons_preserves_structural_halts(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        arm_breaker(
            store,
            reason_codes=["MANUAL_KILL_SWITCH", "WS_PUBLIC_STALE"],
            source="test",
            ts_ms=1000,
            action=ACTION_FULL_KILL_SWITCH,
        )
        cleared = clear_breaker_reasons(
            store,
            reason_codes=["WS_PUBLIC_STALE"],
            source="test",
            ts_ms=2000,
            details={"recovered": True},
        )

    assert cleared["active"] is True
    assert cleared["action"] == ACTION_FULL_KILL_SWITCH
    assert cleared["reason_codes"] == ["MANUAL_KILL_SWITCH"]


def test_classify_upbit_exception_is_exact() -> None:
    assert classify_upbit_exception(RateLimitError("slow down", status_code=429)) == "REPEATED_RATE_LIMIT_ERRORS"
    assert classify_upbit_exception(AuthError("nonce used", status_code=401)) == "REPEATED_NONCE_ERRORS"


def test_evaluate_cycle_contracts_clears_recovered_position_mismatch_reason(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        arm_breaker(
            store,
            reason_codes=["LOCAL_POSITION_MISSING_ON_EXCHANGE"],
            source="sync_cycle",
            ts_ms=1000,
        )
        status = evaluate_cycle_contracts(
            store,
            report={
                "counts": {
                    "external_open_orders": 0,
                    "local_positions_missing_on_exchange": 0,
                },
                "halted_reasons": [],
            },
            source="sync_cycle",
            ts_ms=2000,
        )

    assert status["active"] is False
    assert "LOCAL_POSITION_MISSING_ON_EXCHANGE" not in status["reason_codes"]


def test_arm_breaker_coalesces_duplicate_identical_arm_events(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        first = arm_breaker(
            store,
            reason_codes=["MODEL_POINTER_UNRESOLVED"],
            source="live_model_handoff",
            ts_ms=1000,
            action=ACTION_HALT_NEW_INTENTS,
            details={"note": "first"},
        )
        second = arm_breaker(
            store,
            reason_codes=["MODEL_POINTER_UNRESOLVED"],
            source="live_model_handoff",
            ts_ms=2000,
            action=ACTION_HALT_NEW_INTENTS,
            details={"note": "second"},
        )

    assert first["active"] is True
    assert second["active"] is True
    assert second["updated_ts"] == 2000
    assert second["reason_codes"] == ["MODEL_POINTER_UNRESOLVED"]
    assert len(second["recent_events"]) == 1
    assert second["recent_events"][0]["event_kind"] == "ARM"
    assert second["details"]["note"] == "second"


def test_arm_breaker_keeps_new_arm_event_when_source_changes(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        arm_breaker(
            store,
            reason_codes=["MODEL_POINTER_UNRESOLVED"],
            source="live_model_handoff",
            ts_ms=1000,
            action=ACTION_HALT_NEW_INTENTS,
        )
        status = arm_breaker(
            store,
            reason_codes=["MODEL_POINTER_UNRESOLVED"],
            source="sync_cycle",
            ts_ms=2000,
            action=ACTION_HALT_NEW_INTENTS,
        )

    assert len(status["recent_events"]) == 2
    assert status["recent_events"][0]["source"] == "sync_cycle"
    assert status["recent_events"][1]["source"] == "live_model_handoff"


def test_clear_recovered_risk_exit_stuck_breaker_when_plan_closed(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="risk-closed-1",
                market="KRW-AKT",
                side="long",
                entry_price_str="800",
                qty_str="7.5",
                tp_enabled=True,
                tp_pct=2.0,
                sl_enabled=True,
                sl_pct=1.5,
                trailing_enabled=False,
                state="CLOSED",
                last_eval_ts_ms=1000,
                last_action_ts_ms=3000,
                current_exit_order_uuid="exit-done-1",
                current_exit_order_identifier="AUTOBOT-RISKREP-1",
                replace_attempt=1,
                created_ts=1000,
                updated_ts=3000,
            )
        )
        arm_breaker(
            store,
            reason_codes=["RISK_EXIT_STUCK_MAX_REPLACES"],
            source="risk_manager",
            ts_ms=2000,
            action=ACTION_HALT_NEW_INTENTS,
            details={
                "plan_id": "risk-closed-1",
                "market": "KRW-AKT",
                "current_exit_order_uuid": "exit-done-1",
                "current_exit_order_identifier": "AUTOBOT-RISKREP-1",
            },
        )
        cleared = clear_recovered_risk_exit_stuck_breaker(
            store,
            source="test_recovery",
            ts_ms=4000,
            details={"note": "plan closed"},
        )

    assert cleared["active"] is False
    assert "RISK_EXIT_STUCK_MAX_REPLACES" not in cleared["reason_codes"]


def test_clear_recovered_risk_exit_stuck_breaker_keeps_active_when_exit_still_open(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="risk-open-1",
                market="KRW-AKT",
                side="long",
                entry_price_str="800",
                qty_str="7.5",
                tp_enabled=True,
                tp_pct=2.0,
                sl_enabled=True,
                sl_pct=1.5,
                trailing_enabled=False,
                state="EXITING",
                last_eval_ts_ms=1000,
                last_action_ts_ms=3000,
                current_exit_order_uuid="exit-open-1",
                current_exit_order_identifier="AUTOBOT-RISKREP-OPEN",
                replace_attempt=1,
                created_ts=1000,
                updated_ts=3000,
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="exit-open-1",
                identifier="AUTOBOT-RISKREP-OPEN",
                market="KRW-AKT",
                side="ask",
                ord_type="limit",
                price=812.0,
                volume_req=7.5,
                volume_filled=0.0,
                state="wait",
                created_ts=3000,
                updated_ts=3000,
                intent_id="intent-risk-open-1",
                tp_sl_link="risk-open-1",
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="SUBMIT_ACCEPTED",
                event_source="test",
                replace_seq=1,
                root_order_uuid="exit-open-1",
                prev_order_uuid=None,
                prev_order_identifier=None,
            )
        )
        armed = arm_breaker(
            store,
            reason_codes=["RISK_EXIT_STUCK_MAX_REPLACES"],
            source="risk_manager",
            ts_ms=2000,
            action=ACTION_HALT_NEW_INTENTS,
            details={
                "plan_id": "risk-open-1",
                "market": "KRW-AKT",
                "current_exit_order_uuid": "exit-open-1",
                "current_exit_order_identifier": "AUTOBOT-RISKREP-OPEN",
            },
        )
        status = clear_recovered_risk_exit_stuck_breaker(
            store,
            source="test_recovery",
            ts_ms=4000,
            details={"note": "still open"},
        )

    assert armed["active"] is True
    assert status["active"] is True
    assert "RISK_EXIT_STUCK_MAX_REPLACES" in status["reason_codes"]
