import json
from pathlib import Path
import sqlite3

from autobot.dashboard_server import build_dashboard_snapshot


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _init_live_db(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    with conn:
        conn.execute("CREATE TABLE positions (market TEXT PRIMARY KEY, base_currency TEXT, base_amount REAL, avg_entry_price REAL, updated_ts INTEGER, tp_json TEXT, sl_json TEXT, trailing_json TEXT, managed INTEGER)")
        conn.execute("CREATE TABLE orders (uuid TEXT PRIMARY KEY, identifier TEXT, market TEXT, side TEXT, ord_type TEXT, price REAL, volume_req REAL, volume_filled REAL, state TEXT, created_ts INTEGER, updated_ts INTEGER, intent_id TEXT, tp_sl_link TEXT, local_state TEXT, raw_exchange_state TEXT, last_event_name TEXT, event_source TEXT, replace_seq INTEGER, root_order_uuid TEXT, prev_order_uuid TEXT, prev_order_identifier TEXT)")
        conn.execute("CREATE TABLE intents (intent_id TEXT PRIMARY KEY, ts_ms INTEGER, market TEXT, side TEXT, price REAL, volume REAL, reason_code TEXT, meta_json TEXT, status TEXT)")
        conn.execute("CREATE TABLE risk_plans (plan_id TEXT PRIMARY KEY, market TEXT, side TEXT, entry_price_str TEXT, qty_str TEXT, tp_enabled INTEGER, tp_price_str TEXT, tp_pct REAL, sl_enabled INTEGER, sl_price_str TEXT, sl_pct REAL, trailing_enabled INTEGER, trail_pct REAL, high_watermark_price_str TEXT, armed_ts_ms INTEGER, timeout_ts_ms INTEGER, state TEXT, last_eval_ts_ms INTEGER, last_action_ts_ms INTEGER, current_exit_order_uuid TEXT, current_exit_order_identifier TEXT, replace_attempt INTEGER, created_ts INTEGER, updated_ts INTEGER, plan_source TEXT, source_intent_id TEXT)")
        conn.execute("CREATE TABLE checkpoints (name TEXT PRIMARY KEY, ts_ms INTEGER, payload_json TEXT)")
        conn.execute("CREATE TABLE breaker_states (breaker_key TEXT PRIMARY KEY, active INTEGER, action TEXT, source TEXT, reason_codes_json TEXT, details_json TEXT, updated_ts INTEGER, armed_ts INTEGER)")
        conn.execute("INSERT INTO intents VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", ("intent-1", 1, "KRW-BTC", "bid", 100.0, 1.0, "MODEL_ALPHA_ENTRY_V1", "{}", "SUBMITTED"))
        conn.execute("INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("order-1", "id-1", "KRW-BTC", "bid", "limit", 100.0, 1.0, 0.0, "wait", 1, 2, "intent-1", None, "OPEN", "wait", None, "runtime", 0, None, None, None))
        conn.execute("INSERT INTO risk_plans VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("plan-1", "KRW-BTC", "ask", "100", "1", 0, None, None, 0, None, None, 0, None, None, None, 10, "ACTIVE", 0, 0, None, None, 0, 1, 2, "model_alpha_v1", "intent-1"))
        conn.execute("INSERT INTO checkpoints VALUES (?, ?, ?)", ("live_runtime_health", 1, json.dumps({"live_runtime_model_run_id": "run-123", "ws_public_stale": False})))
        conn.execute("INSERT INTO checkpoints VALUES (?, ?, ?)", ("live_rollout_status", 1, json.dumps({"mode": "canary", "order_emission_allowed": True})))
    conn.close()


def test_build_dashboard_snapshot_collects_core_sections(tmp_path: Path) -> None:
    project_root = tmp_path
    _write_json(project_root / "logs" / "model_v4_acceptance" / "latest.json", {"generated_at": "2026-03-10T00:10:00Z", "candidate_run_id": "run-abc", "overall_pass": False, "backtest_pass": False, "reasons": ["TRAINER_EVIDENCE_REQUIRED_FAILED"], "notes": ["PAPER_SOAK_SKIPPED"], "gates": {"backtest": {"decision_basis": "TRAINER_EVIDENCE_REQUIRED_FAIL"}}})
    _write_json(project_root / "logs" / "model_v4_challenger" / "latest.json", {"steps": {"start_challenger": {"candidate_run_id": "run-abc", "started": False, "reason": "TRAINER_EVIDENCE_REQUIRED_FAILED", "acceptance_notes": ["PAPER_SOAK_SKIPPED"]}}})
    _write_json(project_root / "logs" / "model_v4_challenger" / "current_state.json", {"candidate_run_id": "run-abc"})
    _write_json(project_root / "logs" / "live_rollout" / "latest.json", {"contract": {"mode": "canary"}, "status": {"order_emission_allowed": True}})
    _write_json(project_root / "data" / "paper" / "runs" / "paper-20260310-001000" / "summary.json", {"run_id": "paper-20260310-001000", "orders_submitted": 1, "orders_filled": 1, "realized_pnl_quote": 1234.0})
    meta_dir = project_root / "data" / "raw_ws" / "upbit" / "_meta"
    _write_json(meta_dir / "ws_public_health.json", {"run_id": "ws-run-1", "connected": True, "subscribed_markets_count": 50})
    _write_json(meta_dir / "ws_collect_report.json", {"run_id": "collect-1", "generated_at": "2026-03-10T00:00:00Z"})
    _write_json(meta_dir / "ws_runs_summary.json", {"run_id": "ws-run-1"})
    _init_live_db(project_root / "data" / "state" / "live" / "live_state.db")

    snapshot = build_dashboard_snapshot(project_root)

    assert snapshot["training"]["acceptance"]["candidate_run_id"] == "run-abc"
    assert snapshot["challenger"]["reason"] == "TRAINER_EVIDENCE_REQUIRED_FAILED"
    assert snapshot["paper"]["recent_runs"][0]["run_id"] == "paper-20260310-001000"
    assert snapshot["live"]["states"][0]["positions_count"] == 0
    assert snapshot["live"]["states"][0]["open_orders_count"] == 1
    assert snapshot["live"]["states"][0]["active_risk_plans_count"] == 1
