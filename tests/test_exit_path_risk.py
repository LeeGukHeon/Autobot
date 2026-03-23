from __future__ import annotations

from autobot.common.path_risk_guidance import resolve_path_risk_guidance_from_plan
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
    assert "terminal_return_q25" in by_horizon[1]
    assert "terminal_positive_rate" in by_horizon[1]
    assert "drawdown_from_now_q80" in by_horizon[1]


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
    assert guidance["immediate_exit_value_ratio"] == 0.012
    assert guidance["exit_now_value_net"] is not None
    assert guidance["continue_value_net"] is not None
    assert guidance["continuation_should_exit"] is True


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
