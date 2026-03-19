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
    assert policy["tail_risk_contract"]["method"] == "conditional_linear_quantile_tail_v2"
    assert policy["conditional_action_model"]["status"] == "ready"
    high_score_low_risk = resolve_trade_action(policy, selection_score=0.95, row={"rv_36": 0.10, "atr_pct_14": 0.01})
    low_score_high_risk = resolve_trade_action(policy, selection_score=0.18, row={"rv_36": 0.50, "atr_pct_14": 0.05})

    assert high_score_low_risk is not None
    assert float(high_score_low_risk["recommended_notional_multiplier"]) > 0.0
    assert low_score_high_risk is not None
    assert float(low_score_high_risk["recommended_notional_multiplier"]) > 0.0
    assert high_score_low_risk["recommended_action"] in {"hold", "risk"}
    assert low_score_high_risk["recommended_action"] in {"hold", "risk"}
    assert (
        int(high_score_low_risk["edge_bin"]) != int(low_score_high_risk["edge_bin"])
        or int(high_score_low_risk["risk_bin"]) != int(low_score_high_risk["risk_bin"])
        or float(high_score_low_risk["expected_edge"]) != float(low_score_high_risk["expected_edge"])
        or float(high_score_low_risk["recommended_notional_multiplier"])
        != float(low_score_high_risk["recommended_notional_multiplier"])
    )
    assert high_score_low_risk["decision_source"] == "continuous_conditional_action_value"
    assert low_score_high_risk["decision_source"] == "continuous_conditional_action_value"
    assert float(high_score_low_risk["expected_es"]) >= 0.0
    assert float(high_score_low_risk["expected_ctm"]) >= 0.0
    assert float(high_score_low_risk["expected_action_value"]) == float(high_score_low_risk["expected_objective_score"])


def test_resolve_trade_action_returns_insufficient_evidence_when_latest_policy_lacks_conditional_model() -> None:
    decision = resolve_trade_action(
        {
            "version": 1,
            "policy": "trade_level_hold_risk_oos_bins_v1",
            "status": "ready",
            "risk_feature_name": "rv_36",
            "runtime_decision_source": "continuous_conditional_action_value",
            "conditional_action_model": {"status": "missing"},
            "by_bin": [],
        },
        selection_score=0.6,
        row={"rv_36": 0.2},
    )

    assert isinstance(decision, dict)
    assert decision["status"] == "insufficient_evidence"
    assert decision["decision_source"] == "INSUFFICIENT_EVIDENCE"


def test_resolve_trade_action_does_not_mean_impute_missing_runtime_state_for_latest_policy() -> None:
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
        }
    ]
    policy = build_trade_action_policy_from_oos_rows(
        oos_rows=oos_rows,
        selection_calibration={"mode": "identity_v1"},
        hold_policy_template={
            "mode": "hold",
            "hold_bars": 2,
            "risk_scaling_mode": "fixed",
            "risk_vol_feature": "rv_36",
            "sl_pct": 0.02,
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
            "expected_exit_fee_rate": 0.0,
            "expected_exit_slippage_bps": 0.0,
        },
        size_multiplier_min=0.5,
        size_multiplier_max=1.5,
        min_bin_samples=1,
    )
    assert policy["status"] == "ready"

    decision = resolve_trade_action(policy, selection_score=0.7, row={})

    assert isinstance(decision, dict)
    assert decision["status"] == "insufficient_evidence"
    assert decision["reason_code"] == "TRADE_ACTION_INSUFFICIENT_STATE_SUPPORT"


def test_resolve_trade_action_falls_back_to_bin_policy_when_conditional_state_is_partial() -> None:
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
        }
    ]
    policy = build_trade_action_policy_from_oos_rows(
        oos_rows=oos_rows,
        selection_calibration={"mode": "identity_v1"},
        hold_policy_template={
            "mode": "hold",
            "hold_bars": 2,
            "risk_scaling_mode": "fixed",
            "risk_vol_feature": "rv_36",
            "sl_pct": 0.02,
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
            "expected_exit_fee_rate": 0.0,
            "expected_exit_slippage_bps": 0.0,
        },
        size_multiplier_min=0.5,
        size_multiplier_max=1.5,
        min_bin_samples=1,
    )

    decision = resolve_trade_action(
        policy,
        selection_score=0.95,
        row={"rv_36": 0.50},
    )

    assert isinstance(decision, dict)
    assert decision["decision_source"] == "bin_audit_fallback"
    assert decision["support_level"] == "fallback_bin"
    assert decision.get("support_reason_code", "") in {"", "TRADE_ACTION_INSUFFICIENT_STATE_SUPPORT"}
