from __future__ import annotations

from autobot.models.trade_action_policy import build_trade_action_policy_from_oos_rows, resolve_trade_action


def test_build_trade_action_policy_learns_bin_level_hold_vs_risk_preference() -> None:
    oos_rows = [
        {
            "window_index": 0,
            "raw_scores": [0.95, 0.95, 0.95, 0.95, 0.95, 0.20, 0.20, 0.20, 0.20, 0.20],
            "markets": [
                "KRW-BTC",
                "KRW-BTC",
                "KRW-BTC",
                "KRW-BTC",
                "KRW-BTC",
                "KRW-ETH",
                "KRW-ETH",
                "KRW-ETH",
                "KRW-ETH",
                "KRW-ETH",
            ],
            "ts_ms": [1, 2, 3, 4, 5, 1, 2, 3, 4, 5],
            "close": [100.0, 103.0, 95.0, 100.0, 103.0, 100.0, 101.0, 103.0, 106.0, 109.0],
            "rv_12": [0.10, 0.10, 0.10, 0.10, 0.10, 0.50, 0.50, 0.50, 0.50, 0.50],
            "rv_36": [0.10, 0.10, 0.10, 0.10, 0.10, 0.50, 0.50, 0.50, 0.50, 0.50],
            "atr_14": [1.0, 1.0, 1.0, 1.0, 1.0, 5.0, 5.0, 5.0, 5.0, 5.0],
            "atr_pct_14": [0.01, 0.01, 0.01, 0.01, 0.01, 0.05, 0.05, 0.05, 0.05, 0.05],
        },
        {
            "window_index": 1,
            "raw_scores": [0.92, 0.92, 0.92, 0.18, 0.18, 0.18],
            "markets": ["KRW-XRP", "KRW-XRP", "KRW-XRP", "KRW-DOGE", "KRW-DOGE", "KRW-DOGE"],
            "ts_ms": [1, 2, 3, 1, 2, 3],
            "close": [100.0, 103.0, 95.0, 100.0, 101.0, 104.0],
            "rv_12": [0.10, 0.10, 0.10, 0.50, 0.50, 0.50],
            "rv_36": [0.10, 0.10, 0.10, 0.50, 0.50, 0.50],
            "atr_14": [1.0, 1.0, 1.0, 5.0, 5.0, 5.0],
            "atr_pct_14": [0.01, 0.01, 0.01, 0.05, 0.05, 0.05],
        },
    ]
    policy = build_trade_action_policy_from_oos_rows(
        oos_rows=oos_rows,
        selection_calibration={"mode": "identity_v1"},
        hold_policy_template={
            "mode": "hold",
            "hold_bars": 2,
            "risk_scaling_mode": "fixed",
            "risk_vol_feature": "rv_36",
            "tp_pct": 0.0,
            "sl_pct": 0.05,
            "trailing_pct": 0.0,
            "expected_exit_fee_rate": 0.0,
            "expected_exit_slippage_bps": 0.0,
        },
        risk_policy_template={
            "mode": "risk",
            "hold_bars": 2,
            "risk_scaling_mode": "fixed",
            "risk_vol_feature": "rv_36",
            "tp_pct": 0.01,
            "sl_pct": 0.02,
            "trailing_pct": 0.0,
            "expected_exit_fee_rate": 0.0,
            "expected_exit_slippage_bps": 0.0,
        },
        size_multiplier_min=0.5,
        size_multiplier_max=1.5,
        edge_bin_count=2,
        risk_bin_count=2,
        min_bin_samples=1,
    )

    assert policy["status"] == "ready"
    high_score_low_risk = resolve_trade_action(policy, selection_score=0.95, row={"rv_36": 0.10})
    low_score_high_risk = resolve_trade_action(policy, selection_score=0.18, row={"rv_36": 0.50})

    assert high_score_low_risk is not None
    assert 0.5 <= float(high_score_low_risk["recommended_notional_multiplier"]) <= 1.5
    assert low_score_high_risk is not None
    assert 0.5 <= float(low_score_high_risk["recommended_notional_multiplier"]) <= 1.5
    assert high_score_low_risk["recommended_action"] in {"hold", "risk"}
    assert low_score_high_risk["recommended_action"] in {"hold", "risk"}
    assert high_score_low_risk["recommended_action"] != low_score_high_risk["recommended_action"]
    assert high_score_low_risk["decision_source"] == "continuous_conditional_action_value"
    assert low_score_high_risk["decision_source"] == "continuous_conditional_action_value"
