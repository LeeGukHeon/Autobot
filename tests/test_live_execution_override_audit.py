from __future__ import annotations

from autobot.ops.live_execution_override_audit import (
    _build_findings,
    _summarize_breakers,
    _summarize_dimension,
    _summarize_execution_overrides,
)
from autobot.live.execution_attempts import _merge_outcome_json


def test_summarize_dimension_reports_mode_level_fill_and_pnl() -> None:
    rows = [
        {
            "price_mode": "JOIN",
            "final_state": "FILLED",
            "fill_fraction": 1.0,
            "shortfall_bps": 1.5,
            "expected_edge_bps": 20.0,
            "expected_net_edge_bps": 10.0,
            "micro_quality_score": 0.8,
            "journal_status": "CLOSED",
            "journal_realized_pnl_quote": 5.0,
        },
        {
            "price_mode": "JOIN",
            "final_state": "MISSED",
            "fill_fraction": 0.0,
            "shortfall_bps": None,
            "expected_edge_bps": 18.0,
            "expected_net_edge_bps": 9.0,
            "micro_quality_score": 0.7,
            "journal_status": "CANCELLED_ENTRY",
            "journal_realized_pnl_quote": None,
        },
        {
            "price_mode": "PASSIVE_MAKER",
            "final_state": "FILLED",
            "fill_fraction": 1.0,
            "shortfall_bps": -2.0,
            "expected_edge_bps": 15.0,
            "expected_net_edge_bps": 8.0,
            "micro_quality_score": 0.75,
            "journal_status": "CLOSED",
            "journal_realized_pnl_quote": -3.0,
        },
    ]

    summary = _summarize_dimension(attempts=rows, key_name="price_mode")

    assert summary[0]["price_mode"] == "JOIN"
    assert summary[0]["attempts"] == 2
    assert summary[0]["fills"] == 1
    assert summary[0]["misses"] == 1
    assert summary[0]["fill_rate"] == 0.5
    assert summary[0]["closed_trade_realized_pnl_quote_total"] == 5.0
    assert summary[1]["price_mode"] == "PASSIVE_MAKER"
    assert summary[1]["closed_trade_realized_pnl_quote_total"] == -3.0


def test_build_findings_flags_manual_promotion_mismatch_and_live_miss_loop() -> None:
    payload = {
        "registry_run": {
            "trainer_research_evidence": {
                "pass": False,
            },
            "promotion_decision": {
                "promote": True,
                "promotion_mode": "manual",
            },
            "runtime_recommendations": {
                "execution": {
                    "recommended_price_mode": "JOIN",
                },
                "risk_control": {
                    "live_gate": {
                        "enabled": False,
                    }
                },
            },
        },
        "execution_attempt_summary": {
            "recommended_price_mode_match_rate": 0.25,
            "missed_count": 25,
            "attempts_total": 100,
            "positive_expected_net_edge_closed_losses": {
                "count": 5,
            },
            "positive_expected_net_edge_missed_attempts": {
                "count": 7,
            },
        },
        "breaker_summary": {
            "live_breaker_active": True,
            "rollout_order_emission_allowed": False,
        },
    }

    findings = _build_findings(payload)
    codes = {item["code"] for item in findings}

    assert "TRAINER_EVIDENCE_NOT_PASSING" in codes
    assert "MANUAL_PROMOTION_OVERRIDES_RESEARCH_EVIDENCE" in codes
    assert "LIVE_PRICE_MODE_DIVERGES_FROM_RUN_RECOMMENDATION" in codes
    assert "LIVE_MISS_RATE_HIGH" in codes
    assert "EXPECTED_EDGE_NOT_REALIZED" in codes
    assert "POSITIVE_EDGE_ENTRIES_ARE_BEING_MISSED" in codes
    assert "BREAKER_OR_ROLLOUT_SUPPRESSES_NEW_INTENTS" in codes
    assert "RISK_CONTROL_LIVE_GATE_DISABLED_BY_DESIGN" in codes


def test_summarize_execution_overrides_counts_demotions_to_passive_maker() -> None:
    attempts = [
        {
            "outcome": {
                "execution_trace": {
                    "initial_exec_profile": {"price_mode": "JOIN"},
                    "after_operational_overlay": {"price_mode": "JOIN"},
                    "after_micro_order_policy": {"price_mode": "PASSIVE_MAKER"},
                    "execution_policy": {"selected_price_mode": "PASSIVE_MAKER"},
                    "final_submit": {"submit_price_mode": "PASSIVE_MAKER"},
                }
            }
        },
        {
            "outcome": {
                "execution_trace": {
                    "initial_exec_profile": {"price_mode": "JOIN"},
                    "after_operational_overlay": {"price_mode": "PASSIVE_MAKER"},
                    "after_micro_order_policy": {"price_mode": "PASSIVE_MAKER"},
                    "execution_policy": {"selected_price_mode": "PASSIVE_MAKER"},
                    "final_submit": {"submit_price_mode": "PASSIVE_MAKER"},
                }
            }
        },
    ]

    summary = _summarize_execution_overrides(
        attempts=attempts,
        recommended_price_mode="JOIN",
    )

    assert summary["trace_rows_available"] == 2
    assert summary["operational_demote_to_passive_maker_count"] == 1
    assert summary["micro_policy_demote_to_passive_maker_count"] == 1
    assert summary["execution_policy_demote_to_passive_maker_count"] == 0
    assert summary["final_submit_passive_maker_count"] == 2


def test_summarize_execution_overrides_falls_back_to_journal_entry_meta() -> None:
    attempts = [
        {
            "journal_entry_meta": {
                "execution": {
                    "exec_profile": {
                        "price_mode": "PASSIVE_MAKER",
                    }
                },
                "execution_policy": {
                    "selected_price_mode": "PASSIVE_MAKER",
                },
            }
        }
    ]

    summary = _summarize_execution_overrides(
        attempts=attempts,
        recommended_price_mode="JOIN",
    )

    assert summary["trace_rows_available"] == 1
    assert summary["execution_policy_demote_to_passive_maker_count"] == 1
    assert summary["final_submit_passive_maker_count"] == 1


def test_summarize_breakers_reports_reason_types() -> None:
    summary = _summarize_breakers(
        breaker_events=[
            {
                "source": "live_model_alpha_runtime",
                "reason_codes": ["LIVE_PUBLIC_WS_STREAM_FAILED"],
                "reason_types": ["INFRA"],
            },
            {
                "source": "execution_risk_control_online_halt",
                "reason_codes": ["RISK_CONTROL_MARTINGALE_EVIDENCE"],
                "reason_types": ["STATISTICAL_RISK"],
            },
        ],
        breaker_state=[
            {
                "breaker_key": "live",
                "active": True,
                "reason_codes": ["RISK_CONTROL_MARTINGALE_EVIDENCE"],
                "reason_types": ["STATISTICAL_RISK"],
                "primary_reason_type": "STATISTICAL_RISK",
                "typed_reason_codes": [
                    {
                        "reason_code": "RISK_CONTROL_MARTINGALE_EVIDENCE",
                        "breaker_type": "STATISTICAL_RISK",
                    }
                ],
            }
        ],
        rollout_status_checkpoint={"payload": {"status": {"mode": "canary", "start_allowed": False}}},
    )

    assert summary["live_breaker_primary_reason_type"] == "STATISTICAL_RISK"
    assert summary["reason_type_counts"] == [
        {"reason_type": "INFRA", "count": 1},
        {"reason_type": "STATISTICAL_RISK", "count": 1},
    ]


def test_merge_outcome_json_preserves_trace_and_appends_status_history() -> None:
    merged = _merge_outcome_json(
        existing={
            "status": "submitted",
            "execution_trace": {
                "initial_exec_profile": {"price_mode": "JOIN"},
            },
        },
        patch={
            "status": "ws_update",
            "local_state": "CANCELLED",
        },
    )

    assert "\"execution_trace\"" in merged
    assert "\"submitted\"" in merged
    assert "\"ws_update\"" in merged
