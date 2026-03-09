from __future__ import annotations

from pathlib import Path

from autobot.live.breakers import (
    ACTION_FULL_KILL_SWITCH,
    ACTION_HALT_NEW_INTENTS,
    arm_breaker,
    breaker_status,
    clear_breaker_reasons,
    clear_breaker,
    classify_upbit_exception,
    record_counter_failure,
)
from autobot.live.state_store import LiveStateStore
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
