from __future__ import annotations

import json
from pathlib import Path
import time

from autobot.live.execution_policy_refresh import (
    build_combined_execution_policy_refresh_payload,
    build_execution_policy_refresh_payload,
    refresh_combined_execution_policy,
    refresh_execution_policy,
)
from autobot.live.state_store import LiveStateStore


def _insert_attempt(store: LiveStateStore, *, attempt_id: str, action_code: str, first_fill_delay_ms: int, shortfall_bps: float) -> None:
    now_ts_ms = int(time.time() * 1000)
    store._conn.execute(  # noqa: SLF001
        """
        INSERT INTO execution_attempts (
            attempt_id, market, side, ord_type, time_in_force, action_code,
            submitted_ts_ms, first_fill_ts_ms, final_ts_ms, final_state,
            shortfall_bps, updated_ts
        ) VALUES (?, 'KRW-BTC', 'bid', 'limit', 'gtc', ?, ?, ?, ?, 'FILLED', ?, ?)
        """,
        (
            attempt_id,
            action_code,
            now_ts_ms,
            now_ts_ms + first_fill_delay_ms,
            now_ts_ms + first_fill_delay_ms,
            shortfall_bps,
            now_ts_ms + first_fill_delay_ms,
        ),
    )
    store._conn.commit()  # noqa: SLF001


def test_refresh_execution_policy_writes_checkpoint_and_output(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    output_path = tmp_path / "artifact.json"
    with LiveStateStore(db_path) as store:
        _insert_attempt(store, attempt_id="a-1", action_code="LIMIT_GTC_JOIN", first_fill_delay_ms=2000, shortfall_bps=1.2)
        _insert_attempt(store, attempt_id="a-2", action_code="BEST_IOC", first_fill_delay_ms=300, shortfall_bps=6.0)

    payload = refresh_execution_policy(
        db_path=db_path,
        output_path=output_path,
        lookback_days=30,
        limit=100,
    )

    assert payload["rows_total"] == 2
    assert output_path.exists()
    written = json.loads(output_path.read_text(encoding="utf-8"))
    assert written["model"]["rows_total"] == 2
    with LiveStateStore(db_path) as store:
        checkpoint = store.get_checkpoint(name="live_execution_policy_model")
    assert checkpoint is not None
    assert checkpoint["payload"]["model"]["rows_total"] == 2


def test_build_execution_policy_refresh_payload_uses_recent_attempts(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    with LiveStateStore(db_path) as store:
        _insert_attempt(store, attempt_id="a-1", action_code="LIMIT_GTC_JOIN", first_fill_delay_ms=2000, shortfall_bps=1.2)
        payload = build_execution_policy_refresh_payload(store=store, lookback_days=30, limit=100)

    assert payload["policy"] == "live_execution_policy_refresh_v1"
    assert payload["model"]["status"] == "ready"
    assert payload["model"]["rows_total"] == 1


def test_refresh_combined_execution_policy_pools_multiple_live_dbs(tmp_path: Path) -> None:
    live_db = tmp_path / "live_state.db"
    candidate_db = tmp_path / "live_candidate.db"
    output_path = tmp_path / "combined.json"
    with LiveStateStore(live_db) as store:
        _insert_attempt(store, attempt_id="live-1", action_code="LIMIT_GTC_JOIN", first_fill_delay_ms=2000, shortfall_bps=1.2)
    with LiveStateStore(candidate_db) as store:
        _insert_attempt(store, attempt_id="candidate-1", action_code="BEST_IOC", first_fill_delay_ms=300, shortfall_bps=6.0)

    payload = refresh_combined_execution_policy(
        db_paths=[live_db, candidate_db],
        output_path=output_path,
        lookback_days=30,
        limit_per_db=100,
    )

    assert payload["rows_total"] == 2
    assert len(payload["db_row_counts"]) == 2
    assert output_path.exists()
    with LiveStateStore(live_db) as live_store:
        live_checkpoint = live_store.get_checkpoint(name="live_execution_policy_model")
    with LiveStateStore(candidate_db) as candidate_store:
        candidate_checkpoint = candidate_store.get_checkpoint(name="live_execution_policy_model")
    assert live_checkpoint is not None
    assert candidate_checkpoint is not None
    assert live_checkpoint["payload"]["rows_total"] == 2
    assert candidate_checkpoint["payload"]["rows_total"] == 2


def test_build_combined_execution_policy_refresh_payload_records_db_counts(tmp_path: Path) -> None:
    live_db = tmp_path / "live_state.db"
    candidate_db = tmp_path / "live_candidate.db"
    with LiveStateStore(live_db) as store:
        _insert_attempt(store, attempt_id="live-1", action_code="LIMIT_GTC_JOIN", first_fill_delay_ms=2000, shortfall_bps=1.2)
    with LiveStateStore(candidate_db) as store:
        _insert_attempt(store, attempt_id="candidate-1", action_code="BEST_IOC", first_fill_delay_ms=300, shortfall_bps=6.0)

    payload = build_combined_execution_policy_refresh_payload(
        db_paths=[live_db, candidate_db],
        lookback_days=30,
        limit_per_db=100,
    )

    assert payload["rows_total"] == 2
    assert payload["model"]["rows_total"] == 2
    assert {item["rows_total"] for item in payload["db_row_counts"]} == {1}
