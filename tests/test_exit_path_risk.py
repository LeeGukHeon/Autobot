from __future__ import annotations

import pytest

from autobot.common.path_risk_guidance import build_path_risk_runtime_inputs, resolve_path_risk_guidance_from_plan
from autobot.models.exit_path_risk import build_exit_path_risk_summary
from autobot.models.train_v4_execution import build_exit_path_risk_summary_v4


def test_build_exit_path_risk_summary_emits_pathwise_quantiles() -> None:
    summary = build_exit_path_risk_summary(
        oos_rows=[
            {
                "window_index": 0,
                "raw_scores": [0.9, 0.9, 0.9, 0.2, 0.2, 0.2],
                "markets": ["KRW-BTC", "KRW-BTC", "KRW-BTC", "KRW-ETH", "KRW-ETH", "KRW-ETH"],
                "ts_ms": [1, 2, 3, 1, 2, 3],
                "close": [100.0, 103.0, 102.0, 100.0, 98.0, 101.0],
                "rv_12": [0.01, 0.01, 0.01, 0.02, 0.02, 0.02],
                "rv_36": [0.01, 0.01, 0.01, 0.02, 0.02, 0.02],
                "atr_14": [1.0, 1.0, 1.0, 2.0, 2.0, 2.0],
                "atr_pct_14": [0.01, 0.01, 0.01, 0.02, 0.02, 0.02],
            }
        ],
        selection_calibration={"mode": "identity_v1"},
        risk_feature_name="rv_12",
        horizons=(1, 2),
        expected_exit_fee_rate=0.0,
        expected_exit_slippage_bps=0.0,
        recommended_hold_bars=2,
    )

    assert summary["status"] == "ready"
    assert summary["risk_feature_name"] == "rv_12"
    assert summary["horizons"] == [1, 2]
    assert summary["sample_count"] == 6
    assert summary["recommended_hold_bars"] == 2
    assert summary["recommended_summary"]["hold_bars"] == 2
    by_horizon = {int(item["hold_bars"]): item for item in summary["overall_by_horizon"]}
    assert by_horizon[1]["sample_count"] == 4
    assert by_horizon[2]["sample_count"] == 2
    assert by_horizon[2]["mfe_q50"] >= by_horizon[1]["mfe_q50"]
    assert "mfe_q25" in by_horizon[1]
    assert "mfe_above_25bps_rate" in by_horizon[1]
    assert "terminal_return_q25" in by_horizon[1]
    assert "terminal_positive_rate" in by_horizon[1]
    assert "drawdown_from_now_q80" in by_horizon[1]
    assert "profit_positive_q" in by_horizon[1]
    assert "profit_above_floor_q" in by_horizon[1]
    assert "continue_edge_q50" in by_horizon[1]


def test_build_exit_path_risk_summary_v4_uses_runtime_recommendation_context() -> None:
    summary = build_exit_path_risk_summary_v4(
        runtime_recommendations={
            "exit": {
                "recommended_hold_bars": 6,
                "recommended_risk_vol_feature": "atr_pct_14",
                "expected_exit_fee_rate": 0.0005,
                "expected_exit_slippage_bps": 2.5,
            }
        },
        selection_calibration={"mode": "identity_v1"},
        oos_rows=[
            {
                "window_index": 0,
                "raw_scores": [0.8, 0.8, 0.8, 0.8, 0.8, 0.8, 0.8],
                "markets": ["KRW-BTC"] * 7,
                "ts_ms": [1, 2, 3, 4, 5, 6, 7],
                "close": [100.0, 101.0, 102.0, 103.0, 102.5, 104.0, 105.0],
                "rv_12": [0.01] * 7,
                "rv_36": [0.01] * 7,
                "atr_14": [1.0] * 7,
                "atr_pct_14": [0.01] * 7,
            }
        ],
    )

    assert summary["status"] == "ready"
    assert summary["risk_feature_name"] == "atr_pct_14"
    assert summary["recommended_hold_bars"] == 6
    assert summary["expected_exit_fee_rate"] == 0.0005
    assert summary["expected_exit_slippage_bps"] == 2.5
    assert 6 in summary["horizons"]


def test_resolve_path_risk_guidance_prefers_bucket_specific_summary_and_computes_value_compare() -> None:
    guidance = resolve_path_risk_guidance_from_plan(
        plan_payload={
            "hold_bars": 6,
            "bar_interval_ms": 300_000,
            "min_tp_floor_pct": 0.0005,
            "entry_selection_score": 0.91,
            "entry_risk_feature_value": 0.03,
            "path_risk": {
                "status": "ready",
                "selection_bucket_bounds": [0.4, 0.8],
                "risk_bucket_bounds": [0.01, 0.02],
                "overall_by_horizon": [
                    {
                        "hold_bars": 3,
                        "reachable_tp_q60": 0.04,
                        "bounded_sl_q80": 0.03,
                        "terminal_return_q50": 0.02,
                        "terminal_return_q75": 0.03,
                    }
                ],
                "by_bucket": [
                    {
                        "hold_bars": 3,
                        "selection_bucket": 2,
                        "risk_bucket": 2,
                        "reachable_tp_q60": 0.015,
                        "bounded_sl_q80": 0.008,
                        "terminal_return_q50": 0.004,
                        "terminal_return_q75": 0.006,
                    }
                ],
            },
        },
        elapsed_bars=3,
        current_return_ratio=0.012,
    )

    assert guidance["applied"] is True
    assert guidance["selected_hold_bars"] == 3
    assert guidance["selected_selection_bucket"] == 2
    assert guidance["selected_risk_bucket"] == 2
    assert guidance["reachable_tp_ratio"] == 0.015
    assert guidance["bounded_sl_ratio"] == 0.008
    assert guidance["continuation_value_ratio"] == 0.004
    assert guidance["continue_edge_q50"] == 0.004
    assert guidance["continue_edge_q75"] == 0.006
    assert guidance["immediate_exit_value_ratio"] == 0.012
    assert guidance["exit_now_value_net"] is not None
    assert guidance["continue_value_net"] is not None
    assert guidance["continuation_should_exit"] is True
    assert guidance["continuation_gap"] == guidance["continuation_gap_ratio"]


def test_resolve_path_risk_guidance_holds_when_continue_value_still_dominates() -> None:
    guidance = resolve_path_risk_guidance_from_plan(
        plan_payload={
            "hold_bars": 6,
            "bar_interval_ms": 300_000,
            "expected_exit_fee_rate": 0.0005,
            "expected_exit_slippage_bps": 2.5,
            "entry_selection_score": 0.91,
            "entry_risk_feature_value": 0.03,
            "path_risk": {
                "status": "ready",
                "overall_by_horizon": [
                    {
                        "hold_bars": 3,
                        "reachable_tp_q60": 0.025,
                        "bounded_sl_q80": 0.010,
                        "terminal_return_q25": 0.004,
                        "terminal_return_q50": 0.010,
                        "terminal_return_q75": 0.018,
                        "terminal_return_mean": 0.011,
                        "terminal_positive_rate": 0.70,
                        "terminal_nonnegative_rate": 0.74,
                        "terminal_above_10bps_rate": 0.68,
                        "terminal_above_25bps_rate": 0.60,
                        "terminal_above_50bps_rate": 0.52,
                        "drawdown_from_now_q80": 0.012,
                        "drawdown_from_now_q90": 0.018,
                    }
                ],
                "recommended_summary": {
                    "hold_bars": 3,
                    "reachable_tp_q60": 0.025,
                    "bounded_sl_q80": 0.010,
                    "terminal_return_q25": 0.004,
                    "terminal_return_q50": 0.010,
                    "terminal_return_q75": 0.018,
                    "terminal_return_mean": 0.011,
                    "terminal_positive_rate": 0.70,
                    "terminal_nonnegative_rate": 0.74,
                    "terminal_above_10bps_rate": 0.68,
                    "terminal_above_25bps_rate": 0.60,
                    "terminal_above_50bps_rate": 0.52,
                    "drawdown_from_now_q80": 0.012,
                    "drawdown_from_now_q90": 0.018,
                },
            },
        },
        elapsed_bars=3,
        current_return_ratio=0.006,
    )

    assert guidance["applied"] is True
    assert guidance["continuation_should_exit"] is False
    assert guidance["continue_value_net"] is not None
    assert guidance["exit_now_value_net"] is not None
    assert guidance["continue_value_net"] > guidance["exit_now_value_net"]
    assert guidance["profit_preservation_prob"] == guidance["profit_preservation_rate"]


def test_resolve_path_risk_guidance_uses_immediate_execution_cost_proxy() -> None:
    guidance = resolve_path_risk_guidance_from_plan(
        plan_payload={
            "hold_bars": 6,
            "bar_interval_ms": 300_000,
            "expected_exit_fee_rate": 0.0005,
            "expected_exit_slippage_bps": 2.5,
            "expected_immediate_exit_cost_ratio": 0.0040,
            "expected_immediate_exit_fill_probability": 0.55,
            "expected_immediate_exit_slippage_bps": 3.0,
            "expected_immediate_exit_price_mode": "JOIN",
            "path_risk": {
                "status": "ready",
                "overall_by_horizon": [
                    {
                        "hold_bars": 3,
                        "reachable_tp_q60": 0.02,
                        "bounded_sl_q80": 0.01,
                        "terminal_return_q25": 0.001,
                        "terminal_return_q50": 0.003,
                        "terminal_return_q75": 0.006,
                        "terminal_return_mean": 0.0035,
                        "terminal_positive_rate": 0.55,
                        "terminal_nonnegative_rate": 0.60,
                        "terminal_above_10bps_rate": 0.50,
                        "terminal_above_25bps_rate": 0.40,
                        "terminal_above_50bps_rate": 0.30,
                        "drawdown_from_now_q80": 0.012,
                        "drawdown_from_now_q90": 0.018,
                    }
                ],
            },
        },
        elapsed_bars=3,
        current_return_ratio=0.006,
    )

    assert guidance["immediate_exit_cost_ratio"] == 0.004
    assert guidance["immediate_exit_fill_probability"] == 0.55
    assert guidance["immediate_exit_price_mode"] == "JOIN"
    assert guidance["exit_now_value_net"] == 0.002
    assert guidance["expected_immediate_exit_cleanup_cost_bps"] == 0.0
    assert guidance["expected_immediate_exit_miss_cost_bps"] == 0.0


def test_resolve_path_risk_guidance_exposes_tp_hit_prob_at_current_tp() -> None:
    guidance = resolve_path_risk_guidance_from_plan(
        plan_payload={
            "hold_bars": 6,
            "bar_interval_ms": 300_000,
            "tp_pct": 0.012,
            "path_risk": {
                "status": "ready",
                "overall_by_horizon": [
                    {
                        "hold_bars": 3,
                        "reachable_tp_q60": 0.006,
                        "bounded_sl_q80": 0.008,
                        "mfe_q25": 0.003,
                        "mfe_q50": 0.006,
                        "mfe_q75": 0.009,
                        "mfe_q90": 0.011,
                        "mfe_above_10bps_rate": 0.70,
                        "mfe_above_25bps_rate": 0.52,
                        "mfe_above_50bps_rate": 0.32,
                        "terminal_return_q25": 0.001,
                        "terminal_return_q50": 0.003,
                        "terminal_return_q75": 0.004,
                        "terminal_return_mean": 0.003,
                        "terminal_positive_rate": 0.62,
                        "terminal_nonnegative_rate": 0.66,
                        "terminal_above_10bps_rate": 0.58,
                        "terminal_above_25bps_rate": 0.44,
                        "terminal_above_50bps_rate": 0.28,
                        "drawdown_from_now_q80": 0.010,
                        "drawdown_from_now_q90": 0.013,
                    }
                ],
            },
        },
        elapsed_bars=3,
        current_return_ratio=0.007,
    )

    assert guidance["tp_hit_prob_at_current_tp"] is not None


def test_resolve_path_risk_guidance_can_trigger_execution_liquidation_capture() -> None:
    guidance = resolve_path_risk_guidance_from_plan(
        plan_payload={
            "hold_bars": 6,
            "bar_interval_ms": 300_000,
            "tp_pct": 0.01,
            "expected_exit_fee_rate": 0.0005,
            "expected_exit_slippage_bps": 2.5,
            "expected_immediate_exit_cost_ratio": 0.0015,
            "expected_immediate_exit_fill_probability": 0.30,
            "expected_immediate_exit_cleanup_cost_bps": 20.0,
            "expected_immediate_exit_miss_cost_bps": 40.0,
            "path_risk": {
                "status": "ready",
                "overall_by_horizon": [
                    {
                        "hold_bars": 3,
                        "reachable_tp_q60": 0.012,
                        "bounded_sl_q80": 0.008,
                        "terminal_return_q25": 0.004,
                        "terminal_return_q50": 0.006,
                        "terminal_return_q75": 0.008,
                        "terminal_return_mean": 0.006,
                        "terminal_positive_rate": 0.60,
                        "terminal_nonnegative_rate": 0.65,
                        "terminal_above_10bps_rate": 0.55,
                        "terminal_above_25bps_rate": 0.45,
                        "terminal_above_50bps_rate": 0.35,
                        "drawdown_from_now_q80": 0.010,
                        "drawdown_from_now_q90": 0.015,
                    }
                ],
            },
        },
        elapsed_bars=3,
        current_return_ratio=0.007,
    )

    assert guidance["continuation_should_exit"] is True
    assert guidance["continuation_reason_code"] in {
        "PATH_RISK_EXECUTION_LIQUIDATION_CAPTURE",
        "PATH_RISK_CONTINUATION_CAPTURE",
    }
    assert guidance["execution_liquidation_penalty_ratio"] > 0.0
    assert 0.0 <= guidance["tp_hit_prob_at_current_tp"] <= 1.0
    assert guidance["tp_hit_prob_at_current_tp"] < 0.30


def test_build_path_risk_runtime_inputs_aggregates_cluster_and_pressure_state() -> None:
    payload = build_path_risk_runtime_inputs(
        market="KRW-BTC",
        entry_price=100.0,
        current_price=101.0,
        selection_score=0.85,
        risk_feature_value=0.02,
        positions=[
            {"market": "KRW-BTC", "entry_price": 100.0, "qty": 1.0},
            {"market": "KRW-ETH", "avg_entry_price": 200.0, "base_amount": 0.5},
            {"market": "KRW-XRP", "avg_entry_price": 50.0, "base_amount": 2.0},
        ],
        base_budget_quote=100.0,
        max_positions_total=2,
    )

    assert payload["current_return_ratio"] == pytest.approx(0.01)
    assert payload["selection_score"] == 0.85
    assert payload["risk_feature_value"] == 0.02
    assert payload["portfolio_open_positions"] == 3
    assert payload["same_cluster_open_positions"] == 1
    assert payload["portfolio_pressure_ratio"] == 1.5
