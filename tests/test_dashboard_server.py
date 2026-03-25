import io
import json
from pathlib import Path
import sqlite3
import time

import pytest

from autobot.dashboard_server import (
    _execute_dashboard_operation,
    _json_response,
    _load_dashboard_asset,
    _run_clear_live_breaker,
    _unit_snapshot,
    build_dashboard_snapshot,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _init_live_db(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    now_ms = int(time.time() * 1000)
    with conn:
        conn.execute("CREATE TABLE positions (market TEXT PRIMARY KEY, base_currency TEXT, base_amount REAL, avg_entry_price REAL, updated_ts INTEGER, tp_json TEXT, sl_json TEXT, trailing_json TEXT, managed INTEGER)")
        conn.execute("CREATE TABLE orders (uuid TEXT PRIMARY KEY, identifier TEXT, market TEXT, side TEXT, ord_type TEXT, price REAL, volume_req REAL, volume_filled REAL, state TEXT, created_ts INTEGER, updated_ts INTEGER, intent_id TEXT, tp_sl_link TEXT, local_state TEXT, raw_exchange_state TEXT, last_event_name TEXT, event_source TEXT, replace_seq INTEGER, root_order_uuid TEXT, prev_order_uuid TEXT, prev_order_identifier TEXT)")
        conn.execute("CREATE TABLE intents (intent_id TEXT PRIMARY KEY, ts_ms INTEGER, market TEXT, side TEXT, price REAL, volume REAL, reason_code TEXT, meta_json TEXT, status TEXT)")
        conn.execute("CREATE TABLE risk_plans (plan_id TEXT PRIMARY KEY, market TEXT, side TEXT, entry_price_str TEXT, qty_str TEXT, tp_enabled INTEGER, tp_price_str TEXT, tp_pct REAL, sl_enabled INTEGER, sl_price_str TEXT, sl_pct REAL, trailing_enabled INTEGER, trail_pct REAL, high_watermark_price_str TEXT, armed_ts_ms INTEGER, timeout_ts_ms INTEGER, state TEXT, last_eval_ts_ms INTEGER, last_action_ts_ms INTEGER, current_exit_order_uuid TEXT, current_exit_order_identifier TEXT, replace_attempt INTEGER, created_ts INTEGER, updated_ts INTEGER, plan_source TEXT, source_intent_id TEXT)")
        conn.execute("CREATE TABLE trade_journal (journal_id TEXT PRIMARY KEY, market TEXT, status TEXT, entry_intent_id TEXT, entry_order_uuid TEXT, exit_order_uuid TEXT, plan_id TEXT, entry_submitted_ts_ms INTEGER, entry_filled_ts_ms INTEGER, exit_ts_ms INTEGER, entry_price REAL, exit_price REAL, qty REAL, entry_notional_quote REAL, exit_notional_quote REAL, realized_pnl_quote REAL, realized_pnl_pct REAL, entry_reason_code TEXT, close_reason_code TEXT, close_mode TEXT, model_prob REAL, selection_policy_mode TEXT, trade_action TEXT, expected_edge_bps REAL, expected_downside_bps REAL, expected_net_edge_bps REAL, notional_multiplier REAL, entry_meta_json TEXT, exit_meta_json TEXT, updated_ts INTEGER)")
        conn.execute("CREATE TABLE checkpoints (name TEXT PRIMARY KEY, ts_ms INTEGER, payload_json TEXT)")
        conn.execute("CREATE TABLE breaker_state (breaker_key TEXT PRIMARY KEY, active INTEGER, action TEXT, source TEXT, reason_codes_json TEXT, details_json TEXT, updated_ts INTEGER, armed_ts INTEGER)")
        conn.execute("CREATE TABLE breaker_states (breaker_key TEXT PRIMARY KEY, active INTEGER, action TEXT, source TEXT, reason_codes_json TEXT, details_json TEXT, updated_ts INTEGER, armed_ts INTEGER)")
        conn.execute(
            "INSERT INTO intents VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "intent-1",
                1,
                "KRW-BTC",
                "bid",
                100.0,
                1.0,
                "MODEL_ALPHA_ENTRY_V1",
                json.dumps(
                    {
                        "strategy": {
                            "meta": {
                                "exit_recommendation": {
                                    "recommended_exit_mode": "hold",
                                    "recommended_exit_mode_source": "execution_backtest_grid_search_compare",
                                    "recommended_exit_mode_reason_code": "HOLD_EXECUTION_COMPARE_EDGE",
                                    "chosen_family": "hold",
                                    "chosen_rule_id": "hold_h6",
                                    "hold_family_status": "supported",
                                    "risk_family_status": "supported",
                                    "family_compare_status": "supported",
                                },
                                "trade_action": {
                                    "recommended_action": "risk",
                                    "expected_edge": 0.0123,
                                    "expected_downside_deviation": 0.0045,
                                    "expected_es": 0.0061,
                                    "expected_ctm": 0.000041,
                                    "expected_ctm_order": 2,
                                    "expected_action_value": 1.7,
                                    "decision_source": "continuous_conditional_action_value",
                                    "recommended_notional_multiplier": 1.2,
                                }
                            }
                        },
                        "admissibility": {
                            "decision": {
                                "expected_net_edge_bps": 98.7,
                                "reject_code": "EXPECTED_EDGE_NOT_POSITIVE_AFTER_COST",
                            }
                        },
                        "trade_gate": {
                            "reason_code": "ALLOW",
                        },
                    },
                    ensure_ascii=False,
                ),
                "SUBMITTED",
            ),
        )
        conn.execute("INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("order-1", "id-1", "KRW-BTC", "bid", "limit", 100.0, 1.0, 0.0, "wait", 1, 2, "intent-1", None, "OPEN", "wait", None, "runtime", 0, None, None, None))
        conn.execute("INSERT INTO risk_plans VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("plan-1", "KRW-BTC", "ask", "100", "1", 0, None, None, 0, None, None, 0, None, None, None, 10, "ACTIVE", 0, 0, None, None, 0, 1, 2, "model_alpha_v1", "intent-1"))
        conn.execute(
            "INSERT INTO trade_journal VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "journal-1",
                "KRW-BTC",
                "CLOSED",
                "intent-1",
                "order-1",
                "order-2",
                "plan-1",
                now_ms - 60_000,
                now_ms - 55_000,
                now_ms - 5_000,
                100.0,
                103.0,
                1.0,
                100.0,
                103.0,
                3.0,
                3.0,
                "MODEL_ALPHA_ENTRY_V1",
                "ORDER_STATE",
                "managed_exit_order",
                0.91,
                "rank_effective_quantile",
                "risk",
                123.0,
                45.0,
                98.7,
                1.2,
                json.dumps(
                    {
                        "strategy": {
                            "meta": {
                                "exit_recommendation": {
                                    "recommended_exit_mode": "hold",
                                    "recommended_exit_mode_source": "execution_backtest_grid_search_compare",
                                    "recommended_exit_mode_reason_code": "HOLD_EXECUTION_COMPARE_EDGE",
                                    "chosen_family": "hold",
                                    "chosen_rule_id": "hold_h6",
                                    "hold_family_status": "supported",
                                    "risk_family_status": "supported",
                                    "family_compare_status": "supported",
                                },
                                "trade_action": {
                                    "recommended_action": "risk",
                                    "expected_edge": 0.0123,
                                    "expected_downside_deviation": 0.0045,
                                    "expected_es": 0.0061,
                                    "expected_ctm": 0.000041,
                                    "expected_ctm_order": 2,
                                    "expected_action_value": 1.7,
                                    "decision_source": "continuous_conditional_action_value",
                                    "recommended_notional_multiplier": 1.2,
                                }
                            }
                        }
                    },
                    ensure_ascii=False,
                ),
                json.dumps({"close_mode": "managed_exit_order"}, ensure_ascii=False),
                now_ms - 5_000,
            ),
        )
        conn.execute("INSERT INTO checkpoints VALUES (?, ?, ?)", ("live_runtime_health", 1, json.dumps({"live_runtime_model_run_id": "run-123", "ws_public_stale": False})))
        conn.execute("INSERT INTO checkpoints VALUES (?, ?, ?)", ("live_rollout_status", 1, json.dumps({"mode": "canary", "order_emission_allowed": True})))
    conn.close()


def test_build_dashboard_snapshot_collects_core_sections(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AUTOBOT_DASHBOARD_OPS_ENABLED", raising=False)
    monkeypatch.delenv("AUTOBOT_DASHBOARD_OPS_TOKEN", raising=False)
    project_root = tmp_path
    _write_json(project_root / "logs" / "model_v4_acceptance" / "latest.json", {"generated_at": "2026-03-10T00:10:00Z", "candidate_run_id": "run-abc", "overall_pass": False, "backtest_pass": False, "reasons": ["TRAINER_EVIDENCE_REQUIRED_FAILED"], "notes": ["PAPER_SOAK_SKIPPED"], "gates": {"backtest": {"decision_basis": "TRAINER_EVIDENCE_REQUIRED_FAIL"}}})
    _write_json(project_root / "logs" / "model_v4_challenger" / "latest.json", {"steps": {"start_challenger": {"candidate_run_id": "run-abc", "started": False, "reason": "TRAINER_EVIDENCE_REQUIRED_FAILED", "acceptance_notes": ["PAPER_SOAK_SKIPPED"]}}})
    _write_json(project_root / "logs" / "model_v4_challenger" / "current_state.json", {"candidate_run_id": "run-abc"})
    _write_json(project_root / "logs" / "model_v4_rank_shadow_cycle" / "latest.json", {"status": "shadow_pass", "next_action": "use_rank_governed_lane", "candidate_run_id": "rank-run-001", "lane_id": "rank_shadow"})
    _write_json(project_root / "logs" / "model_v4_rank_shadow_cycle" / "latest_governance_action.json", {"selected_lane_id": "rank_governed_primary", "selected_acceptance_script": "v4_rank_governed_candidate_acceptance.ps1"})
    _write_json(project_root / "logs" / "live_rollout" / "latest.json", {"contract": {"mode": "canary"}, "status": {"order_emission_allowed": True}})
    _write_json(project_root / "data" / "paper" / "runs" / "paper-20260310-001000" / "summary.json", {"run_id": "paper-20260310-001000", "orders_submitted": 1, "orders_filled": 1, "realized_pnl_quote": 1234.0, "paper_runtime_role": "champion", "paper_runtime_model_run_id": "champion-run-001"})
    meta_dir = project_root / "data" / "raw_ws" / "upbit" / "_meta"
    _write_json(meta_dir / "ws_public_health.json", {"run_id": "ws-run-1", "connected": True, "subscribed_markets_count": 50})
    _write_json(meta_dir / "ws_collect_report.json", {"run_id": "collect-1", "generated_at": "2026-03-10T00:00:00Z"})
    _write_json(meta_dir / "ws_runs_summary.json", {"run_id": "ws-run-1"})
    _init_live_db(project_root / "data" / "state" / "live" / "live_state.db")
    _write_json(
        project_root / "data" / "state" / "live" / "live_breaker_report.json",
        {"active": False},
    )
    db_path = project_root / "data" / "state" / "live" / "live_state.db"
    conn = sqlite3.connect(db_path)
    with conn:
        conn.execute(
            "INSERT OR REPLACE INTO checkpoints VALUES (?, ?, ?)",
            (
                "live_model_alpha_last_run",
                2,
                json.dumps(
                    {
                        "private_ws_events_total": 7,
                        "private_ws_last_event_ts_ms": 1700000000000,
                        "private_ws_stats": {"received_events": 7},
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            ),
        )
        conn.execute(
            "INSERT OR REPLACE INTO checkpoints VALUES (?, ?, ?)",
            (
                "last_ws_event",
                3,
                json.dumps(
                    {
                        "event_type": "myOrder",
                        "event_ts_ms": 1700000000001,
                        "latency_ms": 15,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            ),
        )
    conn.close()
    _write_json(
        project_root / "models" / "registry" / "model_alpha_v1" / "run-123" / "runtime_recommendations.json",
        {
            "exit": {
                "recommended_exit_mode": "hold",
                "recommended_exit_mode_source": "execution_backtest_grid_search_compare",
                "recommended_exit_mode_reason_code": "HOLD_EXECUTION_COMPARE_EDGE",
                "chosen_family": "hold",
                "chosen_rule_id": "hold_h6",
                "hold_family_status": "supported",
                "risk_family_status": "supported",
                "family_compare_status": "supported",
                "recommended_hold_bars": 6,
                "objective_score": 0.15,
                "grid_point": {"hold_bars": 6},
                "hold_family": {
                    "status": "supported",
                    "rows_total": 4,
                    "comparable_rows": 3,
                    "best_rule_id": "hold_h6",
                    "best_comparable_rule_id": "hold_h6",
                },
                "recommended_risk_scaling_mode": "volatility_scaled",
                "recommended_risk_vol_feature": "atr_14",
                "recommended_tp_vol_multiplier": 1.8,
                "recommended_sl_vol_multiplier": 0.9,
                "recommended_trailing_vol_multiplier": 1.1,
                "risk_objective_score": 0.11,
                "risk_grid_point": {
                    "risk_scaling_mode": "volatility_scaled",
                    "risk_vol_feature": "atr_14",
                    "tp_vol_multiplier": 1.8,
                    "sl_vol_multiplier": 0.9,
                    "trailing_vol_multiplier": 1.1,
                },
                "summary": {
                    "realized_pnl_quote": 12000.0,
                    "fill_rate": 0.82,
                    "max_drawdown_pct": 4.5,
                    "slippage_bps_mean": 11.0,
                    "orders_filled": 8,
                },
                "risk_summary": {
                    "realized_pnl_quote": 11800.0,
                    "fill_rate": 0.78,
                    "max_drawdown_pct": 5.0,
                    "slippage_bps_mean": 13.5,
                    "orders_filled": 7,
                },
                "risk_family": {
                    "status": "supported",
                    "rows_total": 8,
                    "comparable_rows": 5,
                    "best_rule_id": "risk_h6_atr_14_tp1p8_sl0p9_tr1p1",
                    "best_comparable_rule_id": "risk_h6_atr_14_tp1p8_sl0p9_tr1p1",
                },
                "family_compare": {
                    "status": "supported",
                    "decision": "champion_edge",
                    "comparable": True,
                    "reason_codes": ["UTILITY_TIE_BREAK_FAIL"],
                    "hold_rule_id": "hold_h6",
                    "risk_rule_id": "risk_h6_atr_14_tp1p8_sl0p9_tr1p1",
                },
                "exit_mode_compare": {
                    "decision": "champion_edge",
                    "reasons": ["UTILITY_TIE_BREAK_FAIL"],
                    "utility_score": -0.05,
                    "comparable": True,
                },
            },
            "trade_action": {
                "status": "ready",
                "source": "walk_forward_oos_trade_replay",
                "risk_feature_name": "rv_12",
                "runtime_decision_source": "continuous_conditional_action_value",
                "state_feature_names": ["selection_score", "rv_12", "rv_36", "atr_pct_14"],
                "tail_confidence_level": 0.9,
                "ctm_order": 2,
                "tail_risk_contract": {"method": "conditional_linear_quantile_tail_v2"},
                "conditional_action_model": {
                    "status": "ready",
                    "model": "conditional_action_linear_quantile_tail_v2",
                },
                "summary": {
                    "hold_bins_recommended": 5,
                    "risk_bins_recommended": 1,
                },
                "by_bin": [
                    {
                        "edge_bin": 3,
                        "risk_bin": 0,
                        "recommended_action": "risk",
                        "expected_edge": 0.0123,
                        "expected_downside_deviation": 0.0045,
                        "expected_es": 0.0061,
                        "expected_ctm": 0.000041,
                        "expected_ctm_order": 2,
                        "expected_action_value": 1.7,
                        "expected_tail_probability": 0.18,
                        "recommended_notional_multiplier": 1.2,
                        "sample_count": 42,
                    }
                ],
            },
        },
    )

    import autobot.dashboard_server as dashboard_server_module

    original_ticker_loader = dashboard_server_module._load_live_market_tickers
    original_account_loader = dashboard_server_module._load_live_account_summary
    dashboard_server_module._load_live_market_tickers = lambda project_root, markets: {  # type: ignore[assignment]
        "KRW-BTC": {"trade_price": 104.0, "trade_timestamp": int(time.time() * 1000)}
    }
    dashboard_server_module._load_live_account_summary = lambda project_root: {}  # type: ignore[assignment]
    try:
        snapshot = build_dashboard_snapshot(project_root)
    finally:
        dashboard_server_module._load_live_market_tickers = original_ticker_loader  # type: ignore[assignment]
        dashboard_server_module._load_live_account_summary = original_account_loader  # type: ignore[assignment]
    runtime_artifacts = snapshot["live"]["states"][0]["runtime_artifacts"]
    runtime_recommendations = runtime_artifacts["runtime_recommendations"]
    exit_compare = runtime_recommendations["exit_mode_compare"]
    recent_intent = snapshot["live"]["states"][0]["recent_intents"][0]
    today_summary = snapshot["live"]["states"][0]["today_trade_summary"]

    assert snapshot["training"]["acceptance"]["candidate_run_id"] == "run-abc"
    assert snapshot["training"]["rank_shadow"]["status"] == "shadow_pass"
    assert snapshot["training"]["rank_shadow"]["governance_action"]["selected_lane_id"] == "rank_governed_primary"
    assert snapshot["operations"]["enabled"] is False
    assert snapshot["operations"]["token_required"] is True
    assert snapshot["challenger"]["reason"] == "TRAINER_EVIDENCE_REQUIRED_FAILED"
    assert snapshot["paper"]["recent_runs"][0]["run_id"] == "paper-20260310-001000"
    assert snapshot["paper"]["recent_runs"][0]["paper_runtime_role"] == "champion"
    assert snapshot["paper"]["recent_runs"][0]["paper_runtime_model_run_id"] == "champion-run-001"
    assert snapshot["live"]["states"][0]["positions_count"] == 0
    assert snapshot["live"]["states"][0]["open_orders_count"] == 1
    assert snapshot["live"]["states"][0]["active_risk_plans_count"] == 1
    assert snapshot["live"]["states"][0]["recent_trades"][0]["realized_pnl_quote"] == 3.0
    assert snapshot["live"]["states"][0]["positions"] == []
    assert snapshot["live"]["states"][0]["capital_summary"]["position_cost_quote_total"] == 0.0
    assert snapshot["live"]["states"][0]["capital_summary"]["position_market_value_quote_total"] == 0.0
    assert snapshot["live"]["states"][0]["capital_summary"]["position_unrealized_pnl_quote_total"] == 0.0
    assert today_summary["closed_count"] == 1
    assert today_summary["wins"] == 1
    assert today_summary["net_pnl_quote_total"] == 3.0
    assert today_summary["current_positions_count"] == 0
    assert today_summary["current_position_market_value_quote_total"] == 0.0
    assert today_summary["current_pending_orders_count"] == 1
    assert snapshot["live"]["states"][0]["daemon_last_run"]["private_ws_events_total"] == 7
    assert snapshot["live"]["states"][0]["last_ws_event"]["event_type"] == "myOrder"
    assert runtime_artifacts["exists"] is True
    assert runtime_recommendations["recommended_exit_mode"] == "hold"
    assert runtime_recommendations["hold_grid_point"]["hold_bars"] == 6
    assert runtime_recommendations["chosen_family"] == "hold"
    assert runtime_recommendations["hold_family"]["status"] == "supported"
    assert runtime_recommendations["risk_family"]["comparable_rows"] == 5
    assert runtime_recommendations["family_compare"]["decision"] == "champion_edge"
    assert runtime_recommendations["recommended_risk_vol_feature"] == "atr_14"
    assert runtime_recommendations["trade_action"]["status"] == "ready"
    assert runtime_recommendations["trade_action"]["sample_bins"][0]["expected_edge_bps"] == 123.0
    assert runtime_recommendations["trade_action"]["sample_bins"][0]["expected_es_bps"] == pytest.approx(61.0)
    assert runtime_recommendations["trade_action"]["conditional_action_model"] == "conditional_action_linear_quantile_tail_v2"
    assert exit_compare["hold"]["orders_filled"] == 8
    assert exit_compare["risk"]["slippage_bps_mean"] == 13.5
    assert exit_compare["summary_ko"]
    assert recent_intent["trade_action_recommended_action"] == "risk"
    assert recent_intent["trade_action_expected_edge_bps"] == 123.0
    assert recent_intent["trade_action_expected_es_bps"] == pytest.approx(61.0)
    assert recent_intent["trade_action_decision_source"] == "continuous_conditional_action_value"
    assert recent_intent["exit_recommendation_chosen_family"] == "hold"
    assert recent_intent["exit_recommendation_chosen_rule_id"] == "hold_h6"
    assert snapshot["live"]["states"][0]["recent_trades"][0]["exit_recommendation_chosen_family"] == "hold"
    assert recent_intent["expected_net_edge_bps"] == 98.7
    assert recent_intent["skip_reason"] == "EXPECTED_EDGE_NOT_POSITIVE_AFTER_COST"


def test_build_dashboard_snapshot_includes_paired_paper_latest_and_history(tmp_path: Path) -> None:
    project_root = tmp_path
    paired_run_root = project_root / "logs" / "paired_paper" / "runs" / "paired-20260325-001000-demo"
    latest_payload = {
        "mode": "paired_paper_live_service_v1",
        "generated_at_utc": "2026-03-25T00:10:00Z",
        "run_root": str(paired_run_root),
        "report_path": str(paired_run_root / "paired_paper_report.json"),
        "capture": {
            "duration_sec_requested": 0,
            "markets_subscribed": 17,
            "ticker_events_captured": 120,
            "trade_events_captured": 90,
            "orderbook_events_captured": 45,
            "source_mode": "live_ws_fanout_service",
        },
        "gate": {
            "evaluated": True,
            "pair_ready": True,
            "matched_opportunities": 12,
            "min_matched_opportunities": 1,
            "pass": True,
            "reason": "PAIRED_PAPER_READY",
        },
        "paired_report": {
            "champion": {
                "run_dir": str(paired_run_root / "champion" / "runs" / "paper-20260325-000001"),
                "run_id": "paper-20260325-000001",
                "paper_runtime_role": "champion",
                "paper_runtime_model_run_id": "champion-run-001",
                "orders_filled": 3,
                "realized_pnl_quote": 150.0,
            },
            "challenger": {
                "run_dir": str(paired_run_root / "challenger" / "runs" / "paper-20260325-000001"),
                "run_id": "paper-20260325-000001",
                "paper_runtime_role": "challenger",
                "paper_runtime_model_run_id": "candidate-run-001",
                "orders_filled": 4,
                "realized_pnl_quote": 175.0,
            },
            "clock_alignment": {
                "matched_opportunities": 12,
                "matched_ratio_vs_champion": 1.0,
                "matched_ratio_vs_challenger": 0.92,
                "feature_hash_match_ratio": 1.0,
                "pair_ready": True,
            },
            "paired_deltas": {
                "matched_pnl_delta_quote": 25.0,
                "matched_fill_delta": 1,
                "matched_slippage_delta_bps": -4.5,
                "matched_no_trade_delta": 2,
            },
        },
        "promotion_decision": {
            "decision": {
                "promote": True,
                "decision": "promote_challenger",
                "hard_failures": [],
            }
        },
    }
    history_payload = {
        **latest_payload,
        "generated_at_utc": "2026-03-24T00:10:00Z",
        "promotion_decision": {
            "decision": {
                "promote": False,
                "decision": "keep_champion",
                "hard_failures": ["PAIRED_PAPER_NOT_READY"],
            }
        },
    }
    _write_json(project_root / "logs" / "paired_paper" / "latest.json", latest_payload)
    _write_json(project_root / "logs" / "paired_paper" / "archive" / "paired-20260324-001000-demo.json", history_payload)

    snapshot = build_dashboard_snapshot(project_root)

    paired_latest = snapshot["paper"]["paired_latest"]
    paired_history = snapshot["paper"]["paired_history"]

    assert "paper_paired" in snapshot["services"]
    assert paired_latest["mode"] == "paired_paper_live_service_v1"
    assert paired_latest["source_mode"] == "live_ws_fanout_service"
    assert paired_latest["matched_opportunities"] == 12
    assert paired_latest["gate_pass"] is True
    assert paired_latest["decision"] == "promote_challenger"
    assert paired_latest["champion_run_id"] == "champion-run-001"
    assert paired_latest["challenger_run_id"] == "candidate-run-001"
    assert paired_latest["matched_pnl_delta_quote"] == pytest.approx(25.0)
    assert paired_latest["matched_slippage_delta_bps"] == pytest.approx(-4.5)
    assert len(paired_history) == 1
    assert paired_history[0]["decision"] == "keep_champion"
    assert paired_history[0]["hard_failures"] == ["PAIRED_PAPER_NOT_READY"]


def test_build_dashboard_snapshot_reads_breaker_state_table_name_used_by_live_db(tmp_path: Path) -> None:
    project_root = tmp_path
    _write_json(project_root / "logs" / "live_rollout" / "latest.json", {"contract": {"mode": "canary"}, "status": {"order_emission_allowed": False}})
    db_path = project_root / "data" / "state" / "live_candidate" / "live_state.db"
    _init_live_db(db_path)
    conn = sqlite3.connect(db_path)
    with conn:
        conn.execute(
            "INSERT INTO breaker_state VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "live",
                1,
                "HALT_NEW_INTENTS",
                "risk_replace",
                json.dumps(["REPEATED_REPLACE_REJECTS"], ensure_ascii=False),
                json.dumps({"counter_name": "replace_reject"}, ensure_ascii=False),
                1234,
                1234,
            ),
        )
    conn.close()

    snapshot = build_dashboard_snapshot(project_root)
    candidate_state = next(
        item
        for item in snapshot["live"]["states"]
        if "live_candidate" in str(item.get("db_path", ""))
    )

    assert candidate_state["breaker_active"] is True
    assert candidate_state["active_breakers"]
    assert candidate_state["active_breakers"][0]["reason_codes"] == ["REPEATED_REPLACE_REJECTS"]


def test_build_dashboard_snapshot_summarizes_current_position_capital(tmp_path: Path) -> None:
    project_root = tmp_path
    _write_json(project_root / "logs" / "live_rollout" / "latest.json", {"contract": {"mode": "canary"}, "status": {"order_emission_allowed": True}})
    db_path = project_root / "data" / "state" / "live_candidate" / "live_state.db"
    _init_live_db(db_path)
    conn = sqlite3.connect(db_path)
    with conn:
        conn.execute(
            "INSERT OR REPLACE INTO positions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("KRW-BTC", "BTC", 2.0, 100.0, 1234, "{}", "{}", "{}", 1),
        )
    conn.close()

    import autobot.dashboard_server as dashboard_server_module

    original_ticker_loader = dashboard_server_module._load_live_market_tickers
    original_account_loader = dashboard_server_module._load_live_account_summary
    dashboard_server_module._load_live_market_tickers = lambda project_root, markets: {  # type: ignore[assignment]
        "KRW-BTC": {"trade_price": 104.0, "trade_timestamp": int(time.time() * 1000)}
    }
    dashboard_server_module._load_live_account_summary = lambda project_root: {  # type: ignore[assignment]
        "cash_total_quote": 1000.0,
        "asset_market_value_quote_total": 208.0,
        "total_equity_quote": 1208.0,
    }
    try:
        snapshot = build_dashboard_snapshot(project_root)
    finally:
        dashboard_server_module._load_live_market_tickers = original_ticker_loader  # type: ignore[assignment]
        dashboard_server_module._load_live_account_summary = original_account_loader  # type: ignore[assignment]

    candidate_state = next(item for item in snapshot["live"]["states"] if item.get("service_key") == "live_candidate")
    capital = candidate_state["capital_summary"]
    account = candidate_state["account_summary"]

    assert capital["positions_count"] == 1
    assert capital["priced_positions_count"] == 1
    assert capital["position_cost_quote_total"] == pytest.approx(200.0)
    assert capital["position_market_value_quote_total"] == pytest.approx(208.0)
    assert capital["position_unrealized_pnl_quote_total"] == pytest.approx(8.0)
    assert account["cash_total_quote"] == pytest.approx(1000.0)
    assert account["asset_market_value_quote_total"] == pytest.approx(208.0)
    assert account["total_equity_quote"] == pytest.approx(1208.0)


def test_build_dashboard_snapshot_uses_service_configured_live_db_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project_root = tmp_path
    legacy_main_db = project_root / "data" / "state" / "live_state.db"
    canonical_main_db = project_root / "data" / "state" / "live" / "live_state.db"
    _init_live_db(legacy_main_db)
    _init_live_db(canonical_main_db)

    def _fake_systemctl_show(unit_name: str, *properties: str) -> dict[str, str]:
        if unit_name == "autobot-live-alpha.service":
            return {"Environment": "AUTOBOT_LIVE_STATE_DB_PATH=data/state/live_state.db"}
        if unit_name == "autobot-live-alpha-candidate.service":
            return {"Environment": "AUTOBOT_LIVE_STATE_DB_PATH=data/state/live_candidate/live_state.db"}
        return {}

    monkeypatch.setattr("autobot.dashboard_server._systemctl_show", _fake_systemctl_show)

    snapshot = build_dashboard_snapshot(project_root)
    live_states = snapshot["live"]["states"]
    main_state = next(item for item in live_states if item.get("service_key") == "live_main")

    assert main_state["label"] == "메인 라이브"
    assert Path(str(main_state["db_path"])) == legacy_main_db
    assert len(live_states) == 2
    assert not any(item.get("label") == "레거시 라이브 DB" for item in live_states)
    assert not any(item.get("label") == "보조 라이브 DB" for item in live_states)


def test_build_dashboard_snapshot_falls_back_to_partial_paper_run_when_summary_is_missing(tmp_path: Path) -> None:
    project_root = tmp_path
    run_dir = project_root / "data" / "paper" / "runs" / "paper-20260313-010000-demo"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "orders.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"order_id": "order-1", "state": "FILLED"}, ensure_ascii=False),
                json.dumps({"order_id": "order-1", "state": "FILLED"}, ensure_ascii=False),
                json.dumps({"order_id": "order-2", "state": "OPEN"}, ensure_ascii=False),
            ]
        ),
        encoding="utf-8",
    )
    (run_dir / "fills.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"order_id": "order-1", "market": "KRW-BTC"}, ensure_ascii=False),
                json.dumps({"order_id": "order-1", "market": "KRW-BTC"}, ensure_ascii=False),
            ]
        ),
        encoding="utf-8",
    )
    (run_dir / "events.jsonl").write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "event_type": "RUN_STARTED",
                        "payload": {
                            "run_id": "paper-20260313-010000-demo",
                            "feature_provider": "LIVE_V4",
                            "micro_provider": "LIVE_WS",
                            "warmup_satisfied": True,
                            "paper_runtime_role": "challenger",
                            "paper_runtime_model_run_id": "candidate-run-123",
                        },
                    },
                    ensure_ascii=False,
                ),
                json.dumps({"event_type": "PORTFOLIO_SNAPSHOT", "payload": {}}, ensure_ascii=False),
            ]
        ),
        encoding="utf-8",
    )
    (run_dir / "equity.csv").write_text(
        "\n".join(
            [
                "1773392672029,50304.74709382385,2579.093644963885,0.0,782.7007792700134,-208.803267400843,14",
                "1773392677070,50381.45665072069,2579.093644963885,0.0,782.7007792700134,-132.09371050400316,14",
            ]
        ),
        encoding="utf-8",
    )

    snapshot = build_dashboard_snapshot(project_root)
    recent_runs = snapshot["paper"]["recent_runs"]

    assert recent_runs
    assert recent_runs[0]["run_id"] == "paper-20260313-010000-demo"
    assert recent_runs[0]["feature_provider"] == "LIVE_V4"
    assert recent_runs[0]["micro_provider"] == "LIVE_WS"
    assert recent_runs[0]["paper_runtime_role"] == "challenger"
    assert recent_runs[0]["paper_runtime_role_label"] == "챌린저"
    assert recent_runs[0]["paper_runtime_model_run_id"] == "candidate-run-123"
    assert recent_runs[0]["orders_submitted"] == 2
    assert recent_runs[0]["orders_filled"] == 1
    assert recent_runs[0]["fill_rate"] == pytest.approx(0.5)
    assert recent_runs[0]["realized_pnl_quote"] == pytest.approx(782.7007792700134)
    assert recent_runs[0]["unrealized_pnl_quote"] == pytest.approx(-132.09371050400316)


def test_build_dashboard_snapshot_includes_candidate_trade_analysis(tmp_path: Path) -> None:
    project_root = tmp_path
    _write_json(project_root / "logs" / "live_rollout" / "latest.json", {"contract": {"mode": "canary"}, "status": {"order_emission_allowed": True}})
    db_path = project_root / "data" / "state" / "live_candidate" / "live_state.db"
    _init_live_db(db_path)
    conn = sqlite3.connect(db_path)
    with conn:
        conn.execute(
            "UPDATE trade_journal SET exit_meta_json = ? WHERE journal_id = ?",
            (json.dumps({"close_mode": "managed_exit_order", "close_verified": True, "close_verification_status": "verified_exit_order"}, ensure_ascii=False, sort_keys=True), "journal-1"),
        )
    conn.close()

    snapshot = build_dashboard_snapshot(project_root)
    candidate_state = next(item for item in snapshot["live"]["states"] if "후보" in str(item.get("label")))

    assert candidate_state["trade_analysis"]["closed_total"] == 1
    assert candidate_state["trade_analysis"]["verified_closed_total"] == 1
    assert candidate_state["trade_analysis"]["realized_pnl_quote_total_verified"] == 3.0


def test_build_dashboard_snapshot_dedupes_duplicate_closed_trade_rows(tmp_path: Path) -> None:
    project_root = tmp_path
    _write_json(project_root / "logs" / "live_rollout" / "latest.json", {"contract": {"mode": "canary"}, "status": {"order_emission_allowed": True}})
    _init_live_db(project_root / "data" / "state" / "live" / "live_state.db")
    db_path = project_root / "data" / "state" / "live" / "live_state.db"
    conn = sqlite3.connect(db_path)
    with conn:
        row = conn.execute(
            "SELECT * FROM trade_journal WHERE journal_id = ?",
            ("journal-1",),
        ).fetchone()
        assert row is not None
        duplicate = list(row)
        duplicate[0] = "imported-KRW-BTC-dup"
        duplicate[3] = None
        duplicate[4] = None
        duplicate[6] = None
        conn.execute(
            "INSERT INTO trade_journal VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            tuple(duplicate),
        )
    conn.close()

    snapshot = build_dashboard_snapshot(project_root)
    today_summary = snapshot["live"]["states"][0]["today_trade_summary"]
    recent_trades = snapshot["live"]["states"][0]["recent_trades"]

    assert today_summary["closed_count"] == 1
    assert today_summary["net_pnl_quote_total"] == 3.0
    assert len(recent_trades) == 1


def test_build_dashboard_snapshot_keeps_distinct_canonical_rows_with_shared_exit_order_uuid(tmp_path: Path) -> None:
    project_root = tmp_path
    _write_json(project_root / "logs" / "live_rollout" / "latest.json", {"contract": {"mode": "canary"}, "status": {"order_emission_allowed": True}})
    _init_live_db(project_root / "data" / "state" / "live" / "live_state.db")
    db_path = project_root / "data" / "state" / "live" / "live_state.db"
    conn = sqlite3.connect(db_path)
    with conn:
        row = conn.execute(
            "SELECT * FROM trade_journal WHERE journal_id = ?",
            ("journal-1",),
        ).fetchone()
        assert row is not None
        duplicate = list(row)
        duplicate[0] = "journal-2"
        duplicate[3] = "intent-2"
        duplicate[4] = "order-2"
        duplicate[6] = "plan-2"
        duplicate[15] = 4.0
        duplicate[16] = 4.0
        conn.execute(
            "INSERT INTO trade_journal VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            tuple(duplicate),
        )
    conn.close()

    snapshot = build_dashboard_snapshot(project_root)
    today_summary = snapshot["live"]["states"][0]["today_trade_summary"]
    recent_trades = snapshot["live"]["states"][0]["recent_trades"]

    assert today_summary["closed_count"] == 2
    assert today_summary["net_pnl_quote_total"] == 7.0
    assert len(recent_trades) == 2


def test_build_dashboard_snapshot_hides_synthetic_trade_rows_when_canonical_trade_exists(tmp_path: Path) -> None:
    project_root = tmp_path
    _write_json(project_root / "logs" / "live_rollout" / "latest.json", {"contract": {"mode": "canary"}, "status": {"order_emission_allowed": True}})
    _init_live_db(project_root / "data" / "state" / "live" / "live_state.db")
    db_path = project_root / "data" / "state" / "live" / "live_state.db"
    conn = sqlite3.connect(db_path)
    with conn:
        row = conn.execute(
            "SELECT * FROM trade_journal WHERE journal_id = ?",
            ("journal-1",),
        ).fetchone()
        assert row is not None
        duplicate = list(row)
        duplicate[0] = "trade-KRW-BTC-dup"
        duplicate[3] = None
        duplicate[4] = None
        conn.execute(
            "INSERT INTO trade_journal VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            tuple(duplicate),
        )
    conn.close()

    snapshot = build_dashboard_snapshot(project_root)
    recent_trades = snapshot["live"]["states"][0]["recent_trades"]

    assert len(recent_trades) == 1
    assert recent_trades[0]["journal_id"] == "journal-1"


def test_build_dashboard_snapshot_keeps_distinct_cancelled_entry_rows(tmp_path: Path) -> None:
    project_root = tmp_path
    _write_json(project_root / "logs" / "live_rollout" / "latest.json", {"contract": {"mode": "canary"}, "status": {"order_emission_allowed": True}})
    _init_live_db(project_root / "data" / "state" / "live" / "live_state.db")
    db_path = project_root / "data" / "state" / "live" / "live_state.db"
    conn = sqlite3.connect(db_path)
    with conn:
        row = conn.execute(
            "SELECT * FROM trade_journal WHERE journal_id = ?",
            ("journal-1",),
        ).fetchone()
        assert row is not None
        first = list(row)
        first[0] = "cancelled-entry-1"
        first[2] = "CANCELLED_ENTRY"
        first[3] = "intent-cancel-1"
        first[4] = "entry-order-cancel-1"
        first[5] = None
        first[6] = None
        first[15] = None
        first[16] = None
        first[18] = "MANUAL_CANCELLED_ENTRY"
        first[19] = "external_manual_cancel"
        first[28] = json.dumps({}, ensure_ascii=False)
        first[29] = int(first[29]) + 1
        second = list(first)
        second[0] = "cancelled-entry-2"
        second[3] = "intent-cancel-2"
        second[4] = "entry-order-cancel-2"
        second[29] = int(first[29]) + 1
        conn.execute(
            "INSERT INTO trade_journal VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            tuple(first),
        )
        conn.execute(
            "INSERT INTO trade_journal VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            tuple(second),
        )
    conn.close()

    snapshot = build_dashboard_snapshot(project_root)
    today_summary = snapshot["live"]["states"][0]["today_trade_summary"]
    recent_trades = snapshot["live"]["states"][0]["recent_trades"]
    cancelled_entries = [item for item in recent_trades if item["status"] == "CANCELLED_ENTRY"]

    assert today_summary["cancelled_count"] == 2
    assert len(cancelled_entries) == 2


def test_build_dashboard_snapshot_excludes_unverified_close_from_pnl_summary(tmp_path: Path) -> None:
    project_root = tmp_path
    _write_json(project_root / "logs" / "live_rollout" / "latest.json", {"contract": {"mode": "canary"}, "status": {"order_emission_allowed": True}})
    _init_live_db(project_root / "data" / "state" / "live" / "live_state.db")
    db_path = project_root / "data" / "state" / "live" / "live_state.db"
    conn = sqlite3.connect(db_path)
    with conn:
        row = conn.execute(
            "SELECT exit_meta_json FROM trade_journal WHERE journal_id = ?",
            ("journal-1",),
        ).fetchone()
        assert row is not None
        exit_meta = json.loads(row[0])
        exit_meta["close_verified"] = False
        exit_meta["close_verification_status"] = "unverified_position_sync"
        conn.execute(
            "UPDATE trade_journal SET exit_meta_json = ?, realized_pnl_quote = NULL, exit_notional_quote = NULL, exit_price = NULL WHERE journal_id = ?",
            (json.dumps(exit_meta, ensure_ascii=False, sort_keys=True), "journal-1"),
        )
    conn.close()

    snapshot = build_dashboard_snapshot(project_root)
    today_summary = snapshot["live"]["states"][0]["today_trade_summary"]
    recent_trade = snapshot["live"]["states"][0]["recent_trades"][0]

    assert today_summary["closed_count"] == 1
    assert today_summary["verified_closed_count"] == 0
    assert today_summary["unverified_closed_count"] == 1
    assert today_summary["net_pnl_quote_total"] == 0.0
    assert recent_trade["close_verified"] is False
    assert recent_trade["close_verification_status"] == "unverified_position_sync"


def test_dashboard_asset_keeps_live_risk_plan_percent_points_unscaled() -> None:
    js = str(_load_dashboard_asset("dashboard.js"))

    assert 'fmtPct(Number(plan.tp_pct))' in js
    assert 'fmtPct(Number(plan.sl_pct))' in js
    assert 'fmtPct(Number(plan.trail_pct))' in js
    assert 'fmtPct(Number(plan.tp_pct) * 100)' not in js
    assert 'fmtPct(Number(plan.sl_pct) * 100)' not in js
    assert 'fmtPct(Number(plan.trail_pct) * 100)' not in js
    assert 'CANCELLED_ENTRY: "진입 취소"' in js
    assert 'UPDATED_FROM_CLOSED_ORDERS: "체결 이력 보정 반영"' in js


def test_dashboard_asset_blank_strings_no_longer_render_as_epoch() -> None:
    js = str(_load_dashboard_asset("dashboard.js"))
    html = str(_load_dashboard_asset("index.html"))
    css = str(_load_dashboard_asset("dashboard.css"))

    assert 'if (value == null) return null;' in js
    assert 'if (typeof value === "string" && value.trim() === "") return null;' in js
    assert 'function tryParseStructuredText(value)' in js
    assert 'function humanizeStructuredValue(value, depth = 0)' in js
    assert 'function fmtDateLabel(value)' in js
    assert 'function responseErrorText(response)' in js
    assert 'function shouldDisplayLiveState(snapshot, item)' in js
    assert 'const explicit = String((item || {}).service_key || "").trim();' in js
    assert "페이퍼 챔피언" in js
    assert "페이퍼 챌린저" in js
    assert "paper_runtime_model_run_id" in js
    assert 'pill("상태", "실행 중", "good")' in js
    assert "현재 run ·" in js
    assert "current-paper-run" in js
    assert "paper-role-head" in css
    assert "paper-role-start" in css
    assert "paper-role-current" in css
    assert "총자본" in js
    assert "총 자본" in js
    assert "현금" in js
    assert "close_display_confirmed" in js
    assert "수동 정리" in js
    assert "손익 미집계" in js
    assert "EventSource" in js
    assert "/api/stream" in js
    assert 'stream.addEventListener("snapshot", applySnapshotEvent);' in js
    assert "return bTs - aTs;" in js
    assert "current_pending_orders_count" in js
    assert "setTab(state.activeTab, false, { scroll: false });" in js
    assert "live-selector-card" in css
    assert "live-command-shell" in css
    assert "animation: rise-in" not in css
    assert "position-focus" in css
    assert "mobile-menu-label" in html


def test_unit_snapshot_normalizes_blank_timer_timestamps(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fake_systemctl_show(unit_name: str, *properties: str) -> dict[str, str]:
        assert unit_name == "autobot-v4-challenger-spawn.timer"
        assert "NextElapseUSecRealtime" in properties
        assert "LastTriggerUSec" in properties
        return {
            "ActiveState": "active",
            "SubState": "waiting",
            "UnitFileState": "enabled",
            "MainPID": "0",
            "ExecMainStartTimestamp": "",
            "ExecMainExitTimestamp": "",
            "Description": "Spawn timer",
            "NextElapseUSecRealtime": "",
            "LastTriggerUSec": "",
        }

    monkeypatch.setattr("autobot.dashboard_server._systemctl_show", _fake_systemctl_show)

    snapshot = _unit_snapshot("autobot-v4-challenger-spawn.timer", timer=True)

    assert snapshot["next_run_at"] is None
    assert snapshot["last_trigger_at"] is None
    assert snapshot["started_at"] is None
    assert snapshot["exited_at"] is None


def test_dashboard_server_no_longer_embeds_legacy_html_js_fallback() -> None:
    source = Path("autobot/dashboard_server.py").read_text(encoding="utf-8")

    assert "INDEX_HTML =" not in source
    assert "window.renderLive = function renderLive(live)" not in source


def test_json_response_swallows_connection_reset_during_end_headers() -> None:
    class _FailingHandler:
        def __init__(self) -> None:
            self.wfile = io.BytesIO()

        def send_response(self, status: int) -> None:
            self.status = status

        def send_header(self, name: str, value: str) -> None:
            _ = name, value

        def end_headers(self) -> None:
            raise ConnectionResetError("peer reset")

    _json_response(_FailingHandler(), {"ok": True})


def test_build_dashboard_snapshot_backfills_legacy_runtime_exit_compare(tmp_path: Path) -> None:
    project_root = tmp_path
    _write_json(project_root / "logs" / "live_rollout" / "latest.json", {"contract": {"mode": "canary"}, "status": {"order_emission_allowed": True}})
    _init_live_db(project_root / "data" / "state" / "live" / "live_state.db")
    _write_json(
        project_root / "models" / "registry" / "model_alpha_v1" / "run-123" / "runtime_recommendations.json",
        {
            "exit": {
                "mode": "hold",
                "recommended_hold_bars": 6,
                "recommendation_source": "execution_backtest_grid_search",
                "objective_score": 0.71,
                "risk_objective_score": 0.0,
                "grid_point": {"hold_bars": 6},
                "risk_grid_point": {
                    "risk_scaling_mode": "volatility_scaled",
                    "risk_vol_feature": "rv_12",
                    "tp_vol_multiplier": 1.5,
                    "sl_vol_multiplier": 1.0,
                    "trailing_vol_multiplier": 0.0,
                },
                "summary": {
                    "realized_pnl_quote": 12608.64,
                    "fill_rate": 0.9660,
                    "max_drawdown_pct": 1.685,
                    "slippage_bps_mean": 2.118,
                    "orders_filled": 398,
                },
                "risk_summary": {
                    "realized_pnl_quote": 11252.59,
                    "fill_rate": 0.9638,
                    "max_drawdown_pct": 1.886,
                    "slippage_bps_mean": 2.501,
                    "orders_filled": 586,
                },
            }
        },
    )

    snapshot = build_dashboard_snapshot(project_root)
    runtime_recommendations = snapshot["live"]["states"][0]["runtime_artifacts"]["runtime_recommendations"]

    assert runtime_recommendations["recommended_exit_mode"] == "hold"
    assert runtime_recommendations["recommended_exit_mode_reason_code"] == "HOLD_EXECUTION_COMPARE_EDGE"
    assert runtime_recommendations["contract_status"] == "backfilled"
    assert runtime_recommendations["contract_issues"] == []
    assert runtime_recommendations["exit_mode_compare"]["decision"] == "champion_edge"


def test_build_dashboard_snapshot_includes_pointer_provenance(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project_root = tmp_path
    run_dir = project_root / "models" / "registry" / "train_v4_crypto_cs" / "run-001"
    _write_json(
        run_dir / "train_config.yaml",
        {
            "created_at_utc": "2026-03-20T00:00:00Z",
            "run_scope": "scheduled_daily",
            "task": "cls",
            "trainer": "v4_crypto_cs",
            "start": "2026-03-04",
            "end": "2026-03-18",
        },
    )
    _write_json(
        run_dir / "search_budget_decision.json",
        {
            "status": "default",
            "lane_class_effective": "promotion_eligible",
            "applied": {
                "booster_sweep_trials": 10,
                "runtime_recommendation_profile": "compact",
            },
        },
    )
    _write_json(
        run_dir / "runtime_recommendations.json",
        {
            "trade_action": {"status": "ready"},
            "exit": {"recommended_exit_mode": "hold"},
            "risk_control": {
                "status": "ready",
                "operating_mode": "safety_executor_only_v1",
                "live_gate": {"enabled": False},
            },
        },
    )
    _write_json(run_dir / "promotion_decision.json", {"status": "candidate"})
    _write_json(
        project_root / "models" / "registry" / "train_v4_crypto_cs" / "latest_candidate.json",
        {"run_id": "run-001", "updated_at_utc": "2026-03-20T01:00:00Z"},
    )

    monkeypatch.delenv("AUTOBOT_DASHBOARD_OPS_ENABLED", raising=False)
    monkeypatch.delenv("AUTOBOT_DASHBOARD_OPS_TOKEN", raising=False)
    snapshot = build_dashboard_snapshot(project_root)

    pointer = snapshot["training"]["pointers"]["latest_candidate"]
    provenance = pointer["provenance"]
    assert pointer["run_id"] == "run-001"
    assert provenance["run_scope"] == "scheduled_daily"
    assert provenance["task"] == "cls"
    assert provenance["budget_lane_class_effective"] == "promotion_eligible"
    assert provenance["risk_control_operating_mode"] == "safety_executor_only_v1"


def test_build_dashboard_snapshot_enables_ops_when_token_present(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AUTOBOT_DASHBOARD_OPS_ENABLED", "true")
    monkeypatch.setenv("AUTOBOT_DASHBOARD_OPS_TOKEN", "secret-token")
    snapshot = build_dashboard_snapshot(tmp_path)
    assert snapshot["operations"]["enabled"] is True
    assert snapshot["operations"]["token_required"] is True
    assert snapshot["operations"]["actions"]


def test_build_dashboard_snapshot_includes_active_training_progress(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project_root = tmp_path
    _write_json(project_root / "logs" / "model_v4_acceptance" / "latest.json", {"generated_at": "2026-03-21T01:00:00Z"})
    _write_json(project_root / "logs" / "model_v4_challenger" / "latest.json", {"steps": {}})
    _write_json(project_root / "logs" / "model_v4_rank_shadow_cycle" / "latest.json", {})
    _write_json(project_root / "logs" / "model_v4_rank_shadow_cycle" / "latest_governance_action.json", {})
    _write_json(project_root / "logs" / "live_rollout" / "latest.json", {})

    def _fake_systemctl_show(unit_name: str, *properties: str) -> dict[str, str]:
        if unit_name == "autobot-v4-challenger-spawn.service":
            return {
                "ActiveState": "active",
                "SubState": "running",
                "UnitFileState": "enabled",
                "MainPID": "4321",
                "ExecMainStartTimestamp": "Sat 2026-03-21 10:00:00 UTC",
                "ExecMainExitTimestamp": "",
                "Description": "Spawn service",
            }
        return {}

    monkeypatch.setattr("autobot.dashboard_server._systemctl_show", _fake_systemctl_show)
    monkeypatch.setattr(
        "autobot.dashboard_server._list_process_rows",
        lambda: [
            {
                "pid": 4322,
                "ppid": 4321,
                "args": "/home/ubuntu/MyApps/Autobot/.venv/bin/python -m autobot.cli model train --trainer v4_crypto_cs --model-family train_v4_crypto_cs --feature-set v4 --label-set v2 --task cls --run-scope scheduled_daily --tf 5m --quote KRW --top-n 50 --start 2026-03-04 --end 2026-03-19",
            }
        ],
    )

    snapshot = build_dashboard_snapshot(project_root)

    activity = snapshot["training"]["current_activity"]
    assert activity["active"] is True
    assert activity["stage_key"] == "scheduled_daily_train"
    assert activity["stage_label_ko"] == "본 학습"
    assert activity["progress_pct"] == 68
    assert "2026-03-04" in activity["detail_ko"]


def test_build_dashboard_snapshot_exposes_recovery_ops_actions(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AUTOBOT_DASHBOARD_OPS_ENABLED", "true")
    monkeypatch.setenv("AUTOBOT_DASHBOARD_OPS_TOKEN", "secret-token")

    snapshot = build_dashboard_snapshot(tmp_path)
    actions = {item["id"]: item for item in snapshot["operations"]["actions"]}

    assert "restart_paired_paper" in actions
    assert actions["restart_paired_paper"]["description"] == "autobot-paper-v4-paired.service 재시작"
    assert "restart_paper_champion" not in actions
    assert "restart_paper_challenger" not in actions
    assert "clear_canary_breaker" in actions
    assert actions["clear_canary_breaker"]["category"] == "recovery"
    assert "clear_live_main_breaker" in actions
    assert actions["clear_live_main_breaker"]["category"] == "recovery"
    assert actions["start_spawn_only"]["description"].startswith("00:20 challenger spawn")
    assert actions["start_promote_only"]["description"].startswith("00:10 challenger promote")
    assert "paired paper lane" in actions["adopt_latest_candidate"]["description"]


def test_dashboard_server_source_keeps_single_dashboard_ops_catalog_and_execute_definitions() -> None:
    source = (Path(__file__).resolve().parents[1] / "autobot" / "dashboard_server.py").read_text(encoding="utf-8")

    assert source.count("def _dashboard_ops_catalog(") == 1
    assert source.count("def _execute_dashboard_operation(") == 1
    assert "def _dashboard_ops_catalog_legacy_unused(" not in source
    assert "def _execute_dashboard_operation_legacy_unused(" not in source


def test_execute_dashboard_operation_adopt_latest_candidate_uses_shared_adoption_script(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_root = tmp_path
    _write_json(
        project_root / "models" / "registry" / "train_v4_crypto_cs" / "latest_candidate.json",
        {"run_id": "run-001"},
    )
    captured: dict[str, object] = {}

    def _fake_run_dashboard_command(command: list[str], *, timeout_sec: int = 20) -> dict[str, object]:
        captured["command"] = list(command)
        captured["timeout_sec"] = timeout_sec
        return {
            "started_at": "2026-03-24T00:00:00Z",
            "completed_at": "2026-03-24T00:00:01Z",
            "exit_code": 0,
            "stdout_preview": "ok",
            "stderr_preview": "",
            "success": True,
        }

    monkeypatch.setattr("autobot.dashboard_server._run_dashboard_command", _fake_run_dashboard_command)
    monkeypatch.setattr("autobot.dashboard_server._resolve_pwsh_exe", lambda: "pwsh")
    monkeypatch.setattr("autobot.dashboard_server._project_python_exe", lambda root: "/venv/bin/python")

    result = _execute_dashboard_operation(project_root, "adopt_latest_candidate")

    assert result["success"] is True
    assert result["run_id"] == "run-001"
    command = list(captured["command"] or [])
    assert command[0] == "pwsh"
    assert str(project_root / "scripts" / "adopt_v4_candidate_for_server.ps1") in command
    assert command[command.index("-CandidateRunId") + 1] == "run-001"
    assert command[command.index("-CandidateTargetUnits") + 1] == "autobot-live-alpha-candidate.service"
    assert int(captured["timeout_sec"]) == 120
    history_path = project_root / "logs" / "dashboard_ops" / "ops_history.jsonl"
    assert history_path.exists()
    assert '"action_id": "adopt_latest_candidate"' in history_path.read_text(encoding="utf-8")


def test_run_clear_live_breaker_resolves_breaker_state_and_cleans_online_buffer(tmp_path: Path) -> None:
    project_root = tmp_path
    db_path = project_root / "data" / "state" / "live_candidate" / "live_state.db"
    _init_live_db(db_path)

    conn = sqlite3.connect(db_path)
    with conn:
        conn.execute(
            "INSERT OR REPLACE INTO breaker_state VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "live",
                1,
                "HALT_NEW_INTENTS",
                "test_source",
                json.dumps(["RISK_CONTROL_ONLINE_BREACH_STREAK"]),
                json.dumps({"active": True}),
                int(time.time() * 1000),
                int(time.time() * 1000),
            ),
        )
        conn.execute(
            "INSERT OR REPLACE INTO checkpoints VALUES (?, ?, ?)",
            (
                "execution_risk_control_online_buffer:test-run",
                int(time.time() * 1000),
                json.dumps({"breach_count": 3}),
            ),
        )
    conn.close()

    result = _run_clear_live_breaker(
        project_root,
        db_rel_path="data/state/live_candidate/live_state.db",
        source="dashboard_ops_clear_canary_breaker",
        note="unit-test",
    )

    assert result["success"] is True
    assert result["exit_code"] == 0
    assert "deleted_checkpoints" in result["stdout_preview"]

    conn = sqlite3.connect(db_path)
    with conn:
        row = conn.execute(
            "SELECT active, reason_codes_json, source FROM breaker_state WHERE breaker_key = ?",
            ("live",),
        ).fetchone()
        checkpoint = conn.execute(
            "SELECT payload_json FROM checkpoints WHERE name = ?",
            ("execution_risk_control_online_buffer:test-run",),
        ).fetchone()
    conn.close()

    assert row is not None
    assert int(row[0]) == 0
    assert json.loads(row[1]) == []
    assert row[2] == "dashboard_ops_clear_canary_breaker"
    assert checkpoint is None


def test_build_dashboard_snapshot_detects_manual_training_process_when_spawn_service_is_inactive(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project_root = tmp_path
    _write_json(project_root / "logs" / "model_v4_acceptance" / "latest.json", {"generated_at": "2026-03-21T01:00:00Z"})
    _write_json(project_root / "logs" / "model_v4_challenger" / "latest.json", {"steps": {}})
    _write_json(project_root / "logs" / "model_v4_rank_shadow_cycle" / "latest.json", {})
    _write_json(project_root / "logs" / "model_v4_rank_shadow_cycle" / "latest_governance_action.json", {})
    _write_json(project_root / "logs" / "live_rollout" / "latest.json", {})

    def _fake_systemctl_show(unit_name: str, *properties: str) -> dict[str, str]:
        if unit_name == "autobot-v4-challenger-spawn.service":
            return {
                "ActiveState": "inactive",
                "SubState": "dead",
                "UnitFileState": "disabled",
                "MainPID": "0",
                "ExecMainStartTimestamp": "",
                "ExecMainExitTimestamp": "",
                "Description": "Spawn service",
            }
        return {}

    monkeypatch.setattr("autobot.dashboard_server._systemctl_show", _fake_systemctl_show)
    monkeypatch.setattr(
        "autobot.dashboard_server._list_process_rows",
        lambda: [
            {
                "pid": 9991,
                "ppid": 1,
                "args": "/snap/powershell/332/opt/powershell/pwsh -NoProfile -File /home/ubuntu/MyApps/Autobot/scripts/daily_champion_challenger_v4_for_server.ps1 -Mode spawn_only",
            },
            {
                "pid": 9992,
                "ppid": 9991,
                "args": "/home/ubuntu/MyApps/Autobot/.venv/bin/python -m autobot.cli model train --trainer v4_crypto_cs --model-family train_v4_crypto_cs --feature-set v4 --label-set v2 --task cls --run-scope scheduled_daily --tf 5m --quote KRW --top-n 50 --start 2026-03-04 --end 2026-03-20",
            },
        ],
    )

    snapshot = build_dashboard_snapshot(project_root)
    activity = snapshot["training"]["current_activity"]
    assert activity["active"] is True
    assert activity["stage_key"] == "scheduled_daily_train"
    assert activity["process_pid"] == 9992
