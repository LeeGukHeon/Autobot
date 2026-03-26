from __future__ import annotations

from autobot.models.execution_risk_control import (
    build_execution_risk_control_from_oos_rows,
    resolve_execution_risk_control_decision,
    resolve_execution_risk_control_martingale_state,
    resolve_execution_risk_control_online_state,
    resolve_execution_risk_control_size_decision,
)
from autobot.models.runtime_recommendation_contract import normalize_runtime_recommendations_payload


def _build_window() -> dict[str, object]:
    close = [100.0]
    # High-score bucket: mostly positive one-bar returns with one loss.
    for value in ([0.02] * 24 + [-0.01] + [0.02] * 6 + [-0.03] * 19):
        close.append(close[-1] * (1.0 + value))
    raw_scores = ([0.85] * 31) + ([0.20] * 19) + [0.20]
    size = len(close)
    assert len(raw_scores) == size
    return {
        "window_index": 0,
        "raw_scores": raw_scores,
        "markets": ["KRW-BTC"] * size,
        "ts_ms": [1_000 + (idx * 300_000) for idx in range(size)],
        "close": close,
        "rv_12": [0.10] * size,
        "rv_36": [0.12] * size,
        "atr_14": [1.0] * size,
        "atr_pct_14": [0.01] * size,
    }


def _trade_action_policy() -> dict[str, object]:
    return {
        "version": 1,
        "policy": "trade_level_hold_risk_oos_bins_v1",
        "status": "ready",
        "risk_feature_name": "rv_12",
        "edge_bounds": [0.0, 0.5, 1.0],
        "risk_bounds": [0.0, 1.0],
        "min_bin_samples": 1,
        "runtime_decision_source": "bin_audit_fallback",
        "hold_policy_template": {"mode": "hold", "hold_bars": 1},
        "risk_policy_template": {"mode": "risk", "hold_bars": 1, "risk_vol_feature": "rv_12"},
        "by_bin": [
            {
                "edge_bin": 0,
                "risk_bin": 0,
                "comparable": True,
                "sample_count": 20,
                "recommended_action": "risk",
                "recommended_notional_multiplier": 0.5,
                "expected_edge": -0.002,
                "expected_downside_deviation": 0.015,
                "expected_action_value": 0.2,
                "expected_objective_score": 0.2,
                "expected_es": 0.02,
                "expected_ctm": 0.0,
                "expected_tail_probability": 0.10,
            },
            {
                "edge_bin": 1,
                "risk_bin": 0,
                "comparable": True,
                "sample_count": 31,
                "recommended_action": "risk",
                "recommended_notional_multiplier": 1.1,
                "expected_edge": 0.012,
                "expected_downside_deviation": 0.006,
                "expected_action_value": 2.0,
                "expected_objective_score": 2.0,
                "expected_es": 0.01,
                "expected_ctm": 0.0,
                "expected_tail_probability": 0.10,
            },
        ],
    }


def _build_subgroup_window() -> dict[str, object]:
    btc_close = [100.0]
    for _ in range(25):
        btc_close.append(btc_close[-1] * 1.02)
    eth_close = [100.0]
    for _ in range(25):
        eth_close.append(eth_close[-1] * 0.98)
    close = btc_close + eth_close
    size = len(close)
    return {
        "window_index": 0,
        "raw_scores": [0.85] * size,
        "markets": (["KRW-BTC"] * len(btc_close)) + (["KRW-ETH"] * len(eth_close)),
        "ts_ms": [1_000 + (idx * 300_000) for idx in range(len(btc_close))]
        + [9_000_000 + (idx * 300_000) for idx in range(len(eth_close))],
        "close": close,
        "rv_12": ([0.10] * len(btc_close)) + ([0.60] * len(eth_close)),
        "rv_36": ([0.12] * len(btc_close)) + ([0.62] * len(eth_close)),
        "atr_14": [1.0] * size,
        "atr_pct_14": ([0.01] * len(btc_close)) + ([0.04] * len(eth_close)),
    }


def _build_weighted_windows() -> list[dict[str, object]]:
    old_close = [100.0]
    for _ in range(25):
        old_close.append(old_close[-1] * 1.02)
    new_close = [100.0]
    for _ in range(25):
        new_close.append(new_close[-1] * 0.98)
    return [
        {
            "window_index": 0,
            "raw_scores": [0.85] * len(old_close),
            "markets": ["KRW-BTC"] * len(old_close),
            "ts_ms": [1_000 + (idx * 300_000) for idx in range(len(old_close))],
            "close": old_close,
            "rv_12": [0.10] * len(old_close),
            "rv_36": [0.12] * len(old_close),
            "atr_14": [1.0] * len(old_close),
            "atr_pct_14": [0.01] * len(old_close),
        },
        {
            "window_index": 1,
            "raw_scores": [0.85] * len(new_close),
            "markets": ["KRW-BTC"] * len(new_close),
            "ts_ms": [9_000_000 + (idx * 300_000) for idx in range(len(new_close))],
            "close": new_close,
            "rv_12": [0.60] * len(new_close),
            "rv_36": [0.62] * len(new_close),
            "atr_14": [1.0] * len(new_close),
            "atr_pct_14": [0.04] * len(new_close),
        },
    ]


def _build_density_ratio_windows() -> list[dict[str, object]]:
    old_close = [100.0]
    for _ in range(25):
        old_close.append(old_close[-1] * 0.98)
    new_close = [100.0]
    for _ in range(25):
        new_close.append(new_close[-1] * 1.02)
    return [
        {
            "window_index": 0,
            "raw_scores": [0.85] * len(old_close),
            "markets": ["KRW-BTC"] * len(old_close),
            "ts_ms": [1_000 + (idx * 300_000) for idx in range(len(old_close))],
            "close": old_close,
            "rv_12": [0.10] * len(old_close),
            "rv_36": [0.12] * len(old_close),
            "atr_14": [1.0] * len(old_close),
            "atr_pct_14": [0.01] * len(old_close),
        },
        {
            "window_index": 1,
            "raw_scores": [0.85] * len(new_close),
            "markets": ["KRW-BTC"] * len(new_close),
            "ts_ms": [9_000_000 + (idx * 300_000) for idx in range(len(new_close))],
            "close": new_close,
            "rv_12": [0.60] * len(new_close),
            "rv_36": [0.62] * len(new_close),
            "atr_14": [1.0] * len(new_close),
            "atr_pct_14": [0.04] * len(new_close),
        },
    ]


def _subgroup_trade_action_policy() -> dict[str, object]:
    return {
        "version": 1,
        "policy": "trade_level_hold_risk_oos_bins_v1",
        "status": "ready",
        "risk_feature_name": "rv_12",
        "edge_bounds": [0.0, 0.5, 1.0],
        "risk_bounds": [0.0, 0.30, 1.0],
        "min_bin_samples": 1,
        "runtime_decision_source": "bin_audit_fallback",
        "hold_policy_template": {"mode": "hold", "hold_bars": 1},
        "risk_policy_template": {"mode": "risk", "hold_bars": 1, "risk_vol_feature": "rv_12"},
        "by_bin": [
            {
                "edge_bin": 1,
                "risk_bin": 0,
                "comparable": True,
                "sample_count": 25,
                "recommended_action": "risk",
                "recommended_notional_multiplier": 1.2,
                "expected_edge": 0.02,
                "expected_downside_deviation": 0.002,
                "expected_action_value": 4.0,
                "expected_objective_score": 4.0,
                "expected_es": 0.003,
                "expected_ctm": 0.0,
                "expected_tail_probability": 0.10,
            },
            {
                "edge_bin": 1,
                "risk_bin": 1,
                "comparable": True,
                "sample_count": 25,
                "recommended_action": "risk",
                "recommended_notional_multiplier": 0.7,
                "expected_edge": 0.005,
                "expected_downside_deviation": 0.012,
                "expected_action_value": 2.0,
                "expected_objective_score": 2.0,
                "expected_es": 0.02,
                "expected_ctm": 0.0,
                "expected_tail_probability": 0.10,
            },
        ],
    }


def test_build_execution_risk_control_selects_feasible_action_value_threshold() -> None:
    payload = build_execution_risk_control_from_oos_rows(
        oos_rows=[_build_window()],
        selection_calibration=None,
        trade_action_policy=_trade_action_policy(),
        hold_policy_template={"mode": "hold", "hold_bars": 1},
        risk_policy_template={"mode": "risk", "hold_bars": 1, "risk_vol_feature": "rv_12"},
        nonpositive_alpha=0.30,
        severe_loss_alpha=0.20,
        severe_loss_return_threshold=0.02,
        confidence_delta=0.20,
        min_coverage=10,
    )

    assert payload["status"] == "ready"
    assert payload["operating_mode"] == "safety_executor_only_v1"
    assert payload["decision_metric_name"] == "edge_to_es_ratio"
    assert float(payload["selected_threshold"]) > 0.0
    assert int(payload["selected_coverage"]) >= 10
    assert float(payload["selected_nonpositive_rate_ucb"]) <= 0.30
    assert float(payload["selected_severe_loss_rate_ucb"]) <= 0.20
    assert payload["live_gate"]["enabled"] is False
    assert payload["live_gate"]["mode"] == "safety_executor_only_v1"
    assert payload["live_gate"]["metric_name"] == "edge_to_es_ratio"
    assert payload["live_gate"]["positive_edge_required"] is True
    assert payload["confidence_sequence_monitors"]["enabled"] is True
    assert payload["confidence_sequence_monitors"]["mode"] == "time_uniform_chernoff_rate_v1"
    assert payload["size_ladder"]["status"] == "ready"
    assert float(payload["size_ladder"]["global_max_multiplier"]) > 0.0


def test_runtime_recommendations_normalize_keeps_valid_risk_control_contract() -> None:
    normalized = normalize_runtime_recommendations_payload(
        {
            "version": 1,
            "risk_control": {
                "version": 1,
                "policy": "execution_risk_control_hoeffding_v1",
                "status": "ready",
                "decision_metric_name": "edge_to_es_ratio",
                "selected_threshold": 2.0,
                "selected_coverage": 31,
                "selected_nonpositive_rate_ucb": 0.18,
                "selected_severe_loss_rate_ucb": 0.11,
                "live_gate": {
                    "enabled": True,
                    "metric_name": "edge_to_es_ratio",
                    "threshold": 2.0,
                    "skip_reason_code": "RISK_CONTROL_BELOW_THRESHOLD",
                    "edge_skip_reason_code": "RISK_CONTROL_EDGE_NONPOSITIVE",
                    "positive_edge_required": True,
                },
                "subgroup_family": {
                    "enabled": True,
                    "feature_name": "rv_12",
                    "bucket_count_requested": 2,
                    "bucket_count_effective": 2,
                    "bounds": [0.1, 0.3, 0.6],
                    "min_coverage": 10,
                },
                "weighting": {
                    "enabled": True,
                    "mode": "window_recency_exponential_v1",
                    "half_life_windows": 2.0,
                    "max_window_index": 1,
                },
                "online_adaptation": {
                    "enabled": True,
                    "mode": "recent_closed_trade_hoeffding_stepup_v1",
                    "lookback_trades": 12,
                    "max_step_up": 2,
                    "confidence_delta": 0.10,
                    "checkpoint_name": "execution_risk_control_online_buffer",
                },
                "confidence_sequence_monitors": {
                    "enabled": True,
                    "mode": "time_uniform_chernoff_rate_v1",
                    "confidence_delta": 0.10,
                    "min_closed_trade_count": 8,
                    "min_execution_attempt_count": 12,
                    "nonpositive_rate_threshold": 0.45,
                    "severe_loss_rate_threshold": 0.20,
                    "execution_miss_rate_threshold": 0.55,
                    "edge_gap_breach_rate_threshold": 0.60,
                    "edge_gap_tolerance_bps": 5.0,
                    "severe_loss_return_threshold": 0.01,
                    "nonpositive_rate_reason_code": "RISK_CONTROL_NONPOSITIVE_RATE_CS_BREACH",
                    "severe_loss_rate_reason_code": "RISK_CONTROL_SEVERE_LOSS_RATE_CS_BREACH",
                    "execution_miss_rate_reason_code": "EXECUTION_MISS_RATE_CS_BREACH",
                    "edge_gap_rate_reason_code": "RISK_CONTROL_EDGE_GAP_CS_BREACH",
                    "feature_divergence_rate_threshold": 0.10,
                    "feature_divergence_rate_reason_code": "FEATURE_DIVERGENCE_CS_BREACH",
                },
            },
        }
    )

    assert normalized["contract_status"] == "ok"
    assert normalized["risk_control"]["contract_status"] == "ok"
    assert normalized["risk_control"]["contract_issues"] == []


def test_resolve_execution_risk_control_decision_blocks_when_edge_to_es_ratio_is_below_threshold() -> None:
    decision = resolve_execution_risk_control_decision(
        risk_control_payload={
            "version": 1,
            "policy": "execution_risk_control_hoeffding_v1",
            "status": "ready",
            "decision_metric_name": "edge_to_es_ratio",
            "selected_threshold": 2.0,
            "selected_coverage": 31,
            "selected_nonpositive_rate_ucb": 0.18,
            "selected_severe_loss_rate_ucb": 0.11,
            "live_gate": {
                "enabled": True,
                "metric_name": "edge_to_es_ratio",
                "threshold": 2.0,
                "skip_reason_code": "RISK_CONTROL_BELOW_THRESHOLD",
                "edge_skip_reason_code": "RISK_CONTROL_EDGE_NONPOSITIVE",
                "positive_edge_required": True,
            },
            "subgroup_family": {
                "enabled": True,
                "feature_name": "rv_12",
                "bucket_count_requested": 2,
                "bucket_count_effective": 2,
                "bounds": [0.1, 0.3, 0.6],
                "min_coverage": 10,
            },
        },
        selection_score=0.9,
        trade_action={
            "recommended_action": "risk",
            "expected_action_value": 0.4,
            "expected_edge": 0.004,
            "expected_es": 0.01,
        },
    )

    assert decision["enabled"] is True
    assert decision["allowed"] is False
    assert decision["reason_code"] == "RISK_CONTROL_BELOW_THRESHOLD"
    assert float(decision["metric_value"]) == 0.4


def test_resolve_execution_risk_control_size_decision_clamps_requested_multiplier() -> None:
    decision = resolve_execution_risk_control_size_decision(
        risk_control_payload={
            "version": 1,
            "policy": "execution_risk_control_hoeffding_v1",
            "status": "ready",
            "decision_metric_name": "edge_to_es_ratio",
            "selected_threshold": 2.0,
            "selected_coverage": 31,
            "selected_nonpositive_rate_ucb": 0.18,
            "selected_severe_loss_rate_ucb": 0.11,
            "live_gate": {
                "enabled": True,
                "metric_name": "edge_to_es_ratio",
                "threshold": 2.0,
                "skip_reason_code": "RISK_CONTROL_BELOW_THRESHOLD",
                "edge_skip_reason_code": "RISK_CONTROL_EDGE_NONPOSITIVE",
                "positive_edge_required": True,
            },
            "subgroup_family": {
                "enabled": True,
                "feature_name": "rv_12",
                "bucket_count_requested": 2,
                "bucket_count_effective": 2,
                "bounds": [0.1, 0.3, 0.6],
                "min_coverage": 10,
            },
            "size_ladder": {
                "enabled": True,
                "status": "ready",
                "feature_name": "rv_12",
                "global_max_multiplier": 1.5,
                "group_limits": [
                    {"bucket_index": 0, "label": "rv_12:q0", "coverage": 12, "max_multiplier": 1.5, "status": "ok"},
                    {"bucket_index": 1, "label": "rv_12:q1", "coverage": 12, "max_multiplier": 0.5, "status": "ok"},
                ],
            },
        },
        trade_action={"risk_feature_value": 0.55},
        requested_multiplier=3.0,
    )

    assert decision["enabled"] is True
    assert decision["allowed"] is True
    assert float(decision["resolved_multiplier"]) == 0.5
    assert float(decision["requested_multiplier"]) == 3.0
    assert decision["subgroup_bucket"] == 1


def test_build_execution_risk_control_uses_subgroup_constraints_to_raise_threshold() -> None:
    payload = build_execution_risk_control_from_oos_rows(
        oos_rows=[_build_subgroup_window()],
        selection_calibration=None,
        trade_action_policy=_subgroup_trade_action_policy(),
        hold_policy_template={"mode": "hold", "hold_bars": 1},
        risk_policy_template={"mode": "risk", "hold_bars": 1, "risk_vol_feature": "rv_12"},
        nonpositive_alpha=0.35,
        severe_loss_alpha=0.25,
        severe_loss_return_threshold=0.015,
        confidence_delta=0.20,
        min_coverage=10,
        subgroup_bucket_count=2,
        subgroup_min_coverage=10,
    )

    assert payload["status"] == "ready"
    assert float(payload["selected_threshold"]) > 0.0
    assert payload["subgroup_family"]["feature_name"] == "rv_12"
    assert int(payload["subgroup_family"]["bucket_count_effective"]) == 2
    subgroup_results = payload["selected_subgroup_results"]
    assert len(subgroup_results) == 1
    assert subgroup_results[0]["status"] == "ok"

    assert payload["live_gate"]["enabled"] is False


def test_resolve_execution_risk_control_decision_blocks_when_expected_edge_is_nonpositive() -> None:
    decision = resolve_execution_risk_control_decision(
        risk_control_payload={
            "version": 1,
            "policy": "execution_risk_control_hoeffding_v1",
            "status": "ready",
            "decision_metric_name": "edge_to_es_ratio",
            "selected_threshold": 0.5,
            "selected_coverage": 31,
            "selected_nonpositive_rate_ucb": 0.18,
            "selected_severe_loss_rate_ucb": 0.11,
                "live_gate": {
                    "enabled": True,
                    "metric_name": "edge_to_es_ratio",
                    "threshold": 0.5,
                    "skip_reason_code": "RISK_CONTROL_BELOW_THRESHOLD",
                    "edge_skip_reason_code": "RISK_CONTROL_EDGE_NONPOSITIVE",
                    "positive_edge_required": True,
                },
                "subgroup_family": {
                    "enabled": False,
                    "feature_name": "",
                    "bucket_count_requested": 0,
                    "bucket_count_effective": 0,
                    "bounds": [],
                    "min_coverage": 0,
                },
            },
            selection_score=0.9,
            trade_action={
                "recommended_action": "risk",
                "expected_edge": 0.0,
            "expected_es": 0.01,
        },
    )

    assert decision["enabled"] is True
    assert decision["allowed"] is False
    assert decision["reason_code"] == "RISK_CONTROL_EDGE_NONPOSITIVE"


def test_build_execution_risk_control_applies_recency_weighting() -> None:
    payload = build_execution_risk_control_from_oos_rows(
        oos_rows=_build_weighted_windows(),
        selection_calibration=None,
        trade_action_policy=_trade_action_policy(),
        hold_policy_template={"mode": "hold", "hold_bars": 1},
        risk_policy_template={"mode": "risk", "hold_bars": 1, "risk_vol_feature": "rv_12"},
        nonpositive_alpha=0.95,
        severe_loss_alpha=0.95,
        severe_loss_return_threshold=0.015,
        confidence_delta=0.20,
        min_coverage=10,
        subgroup_bucket_count=1,
        weighting_half_life_windows=0.5,
    )

    assert payload["status"] == "ready"
    assert payload["weighting"]["mode"] == "window_recency_exponential_v1"
    assert float(payload["weighting"]["half_life_windows"]) == 0.5
    assert float(payload["selected_effective_sample_size"]) > 0.0


def test_build_execution_risk_control_applies_covariate_similarity_weighting() -> None:
    disabled = build_execution_risk_control_from_oos_rows(
        oos_rows=_build_weighted_windows(),
        selection_calibration=None,
        trade_action_policy=_trade_action_policy(),
        hold_policy_template={"mode": "hold", "hold_bars": 1},
        risk_policy_template={"mode": "risk", "hold_bars": 1, "risk_vol_feature": "rv_12"},
        nonpositive_alpha=0.95,
        severe_loss_alpha=0.95,
        severe_loss_return_threshold=0.015,
        confidence_delta=0.20,
        min_coverage=10,
        subgroup_bucket_count=1,
        weighting_half_life_windows=1000.0,
        weighting_covariate_similarity_enabled=False,
    )
    enabled = build_execution_risk_control_from_oos_rows(
        oos_rows=_build_weighted_windows(),
        selection_calibration=None,
        trade_action_policy=_trade_action_policy(),
        hold_policy_template={"mode": "hold", "hold_bars": 1},
        risk_policy_template={"mode": "risk", "hold_bars": 1, "risk_vol_feature": "rv_12"},
        nonpositive_alpha=0.95,
        severe_loss_alpha=0.95,
        severe_loss_return_threshold=0.015,
        confidence_delta=0.20,
        min_coverage=10,
        subgroup_bucket_count=1,
        weighting_half_life_windows=1000.0,
        weighting_covariate_similarity_enabled=True,
    )

    assert disabled["status"] == "ready"
    assert enabled["weighting"]["covariate_similarity"]["enabled"] is True
    assert enabled["weighting"]["covariate_similarity"]["mode"] == "latest_window_gaussian_state_similarity_v1"
    if enabled["status"] == "ready":
        assert float(enabled["selected_mean_return"]) < float(disabled["selected_mean_return"])
    else:
        assert enabled["reason"] == "NO_FEASIBLE_THRESHOLD"


def test_build_execution_risk_control_applies_density_ratio_weighting() -> None:
    disabled = build_execution_risk_control_from_oos_rows(
        oos_rows=_build_density_ratio_windows(),
        selection_calibration=None,
        trade_action_policy=_trade_action_policy(),
        hold_policy_template={"mode": "hold", "hold_bars": 1},
        risk_policy_template={"mode": "risk", "hold_bars": 1, "risk_vol_feature": "rv_12"},
        nonpositive_alpha=0.95,
        severe_loss_alpha=0.95,
        severe_loss_return_threshold=0.015,
        confidence_delta=0.20,
        min_coverage=10,
        subgroup_bucket_count=1,
        weighting_half_life_windows=1000.0,
        weighting_covariate_similarity_enabled=False,
        weighting_density_ratio_enabled=False,
    )
    enabled = build_execution_risk_control_from_oos_rows(
        oos_rows=_build_density_ratio_windows(),
        selection_calibration=None,
        trade_action_policy=_trade_action_policy(),
        hold_policy_template={"mode": "hold", "hold_bars": 1},
        risk_policy_template={"mode": "risk", "hold_bars": 1, "risk_vol_feature": "rv_12"},
        nonpositive_alpha=0.95,
        severe_loss_alpha=0.95,
        severe_loss_return_threshold=0.015,
        confidence_delta=0.20,
        min_coverage=10,
        subgroup_bucket_count=1,
        weighting_half_life_windows=1000.0,
        weighting_covariate_similarity_enabled=False,
        weighting_density_ratio_enabled=True,
    )

    assert disabled["status"] == "ready"
    assert enabled["weighting"]["density_ratio"]["enabled"] is True
    assert enabled["weighting"]["density_ratio"]["mode"] == "latest_window_logistic_density_ratio_crossfit_v1"
    assert enabled["weighting"]["density_ratio"]["classifier_status"] == "ready_crossfit"
    assert int(enabled["weighting"]["density_ratio"]["crossfit_fold_count"]) >= 2
    assert enabled["weighting"]["density_ratio"]["crossfit_probability_source"] == "out_of_fold"
    if enabled["status"] == "ready":
        assert float(enabled["selected_mean_return"]) > float(disabled["selected_mean_return"])
    else:
        assert enabled["reason"] == "NO_FEASIBLE_THRESHOLD"


def test_resolve_execution_risk_control_online_state_requires_recovery_streak_to_step_down() -> None:
    payload = {
        "version": 1,
        "policy": "execution_risk_control_hoeffding_v1",
        "status": "ready",
        "selected_threshold": 1.0,
        "threshold_results": [{"threshold": 2.0}, {"threshold": 1.0}],
        "nonpositive_alpha": 0.30,
        "severe_loss_alpha": 0.20,
        "online_adaptation": {
            "enabled": True,
            "mode": "recent_closed_trade_hoeffding_stepup_v1",
            "lookback_trades": 2,
            "max_step_up": 2,
            "recovery_streak_required": 2,
            "min_halt_trade_count": 1,
            "halt_breach_streak": 3,
            "halt_reason_code": "RISK_CONTROL_ONLINE_BREACH_STREAK",
            "confidence_delta": 0.20,
            "checkpoint_name": "execution_risk_control_online_buffer",
        },
    }
    first = resolve_execution_risk_control_online_state(
        risk_control_payload=payload,
        previous_state={"step_up": 2, "breach_streak": 0, "recovery_streak": 0, "halt_triggered": False},
        recent_trade_count=2,
        recent_nonpositive_rate_ucb=0.0,
        recent_severe_loss_rate_ucb=0.0,
        recent_max_exit_ts_ms=1,
    )
    second = resolve_execution_risk_control_online_state(
        risk_control_payload=payload,
        previous_state=first,
        recent_trade_count=2,
        recent_nonpositive_rate_ucb=0.0,
        recent_severe_loss_rate_ucb=0.0,
        recent_max_exit_ts_ms=2,
    )

    assert first["effective_min_halt_trade_count"] == 1
    assert first["step_up"] == 2
    assert first["recovery_streak"] == 1
    assert second["step_up"] == 1
    assert second["recovery_streak"] == 0
    assert second["clear_halt"] is False


def test_resolve_execution_risk_control_online_state_preserves_base_threshold_when_grid_omits_exact_value() -> None:
    payload = {
        "version": 1,
        "policy": "execution_risk_control_hoeffding_v1",
        "status": "ready",
        "selected_threshold": 7.602947545899657,
        "threshold_results": [
            {"threshold": 7.954755951921612},
            {"threshold": 8.05242376963567},
        ],
        "nonpositive_alpha": 0.45,
        "severe_loss_alpha": 0.20,
        "online_adaptation": {
            "enabled": True,
            "mode": "recent_closed_trade_hoeffding_stepup_v1",
            "lookback_trades": 12,
            "max_step_up": 2,
            "recovery_streak_required": 2,
            "min_halt_trade_count": 1,
            "halt_breach_streak": 3,
            "halt_reason_code": "RISK_CONTROL_ONLINE_BREACH_STREAK",
            "confidence_delta": 0.10,
            "checkpoint_name": "execution_risk_control_online_buffer",
        },
    }

    state = resolve_execution_risk_control_online_state(
        risk_control_payload=payload,
        previous_state={"step_up": 0, "breach_streak": 0, "recovery_streak": 0, "halt_triggered": False},
        recent_trade_count=0,
        recent_nonpositive_rate_ucb=0.0,
        recent_severe_loss_rate_ucb=0.0,
    )

    assert state["step_up"] == 0
    assert float(state["base_threshold"]) == float(payload["selected_threshold"])
    assert float(state["adaptive_threshold"]) == float(payload["selected_threshold"])


def test_resolve_execution_risk_control_online_state_clears_halt_after_recovery() -> None:
    payload = {
        "version": 1,
        "policy": "execution_risk_control_hoeffding_v1",
        "status": "ready",
        "selected_threshold": 1.0,
        "threshold_results": [{"threshold": 2.0}, {"threshold": 1.0}],
        "nonpositive_alpha": 0.30,
        "severe_loss_alpha": 0.20,
        "online_adaptation": {
            "enabled": True,
            "mode": "recent_closed_trade_hoeffding_stepup_v1",
            "lookback_trades": 2,
            "max_step_up": 2,
            "recovery_streak_required": 1,
            "min_halt_trade_count": 1,
            "halt_breach_streak": 2,
            "halt_reason_code": "RISK_CONTROL_ONLINE_BREACH_STREAK",
            "confidence_delta": 0.20,
            "checkpoint_name": "execution_risk_control_online_buffer",
        },
    }
    next_state = resolve_execution_risk_control_online_state(
        risk_control_payload=payload,
        previous_state={"step_up": 1, "breach_streak": 0, "recovery_streak": 0, "halt_triggered": True},
        recent_trade_count=2,
        recent_nonpositive_rate_ucb=0.0,
        recent_severe_loss_rate_ucb=0.0,
        recent_max_exit_ts_ms=1,
    )

    assert next_state["step_up"] == 0
    assert next_state["clear_halt"] is True
    assert next_state["halt_triggered"] is False


def test_resolve_execution_risk_control_online_state_triggers_halt_on_breach_streak() -> None:
    payload = {
        "version": 1,
        "policy": "execution_risk_control_hoeffding_v1",
        "status": "ready",
        "selected_threshold": 1.0,
        "threshold_results": [{"threshold": 2.0}, {"threshold": 1.0}],
        "nonpositive_alpha": 0.30,
        "severe_loss_alpha": 0.20,
        "online_adaptation": {
            "enabled": True,
            "mode": "recent_closed_trade_hoeffding_stepup_v1",
            "lookback_trades": 2,
            "max_step_up": 2,
            "recovery_streak_required": 2,
            "min_halt_trade_count": 2,
            "halt_breach_streak": 2,
            "halt_reason_code": "RISK_CONTROL_ONLINE_BREACH_STREAK",
            "confidence_delta": 0.20,
            "checkpoint_name": "execution_risk_control_online_buffer",
        },
    }
    first = resolve_execution_risk_control_online_state(
        risk_control_payload=payload,
        previous_state={},
        recent_trade_count=2,
        recent_nonpositive_rate_ucb=1.0,
        recent_severe_loss_rate_ucb=1.0,
        recent_max_exit_ts_ms=1,
    )
    second = resolve_execution_risk_control_online_state(
        risk_control_payload=payload,
        previous_state=first,
        recent_trade_count=2,
        recent_nonpositive_rate_ucb=1.0,
        recent_severe_loss_rate_ucb=1.0,
        recent_max_exit_ts_ms=2,
    )

    assert first["halt_triggered"] is False
    assert second["halt_triggered"] is True
    assert second["halt_reason_code"] == "RISK_CONTROL_ONLINE_BREACH_STREAK"


def test_resolve_execution_risk_control_online_state_does_not_increment_streak_without_new_evidence() -> None:
    payload = {
        "version": 1,
        "policy": "execution_risk_control_hoeffding_v1",
        "status": "ready",
        "selected_threshold": 1.0,
        "threshold_results": [{"threshold": 2.0}, {"threshold": 1.0}],
        "nonpositive_alpha": 0.30,
        "severe_loss_alpha": 0.20,
        "online_adaptation": {
            "enabled": True,
            "mode": "recent_closed_trade_hoeffding_stepup_v1",
            "lookback_trades": 2,
            "max_step_up": 2,
            "recovery_streak_required": 2,
            "min_halt_trade_count": 2,
            "halt_breach_streak": 2,
            "halt_reason_code": "RISK_CONTROL_ONLINE_BREACH_STREAK",
            "confidence_delta": 0.20,
            "checkpoint_name": "execution_risk_control_online_buffer",
        },
    }
    previous = {
        "step_up": 2,
        "breach_streak": 7,
        "recovery_streak": 0,
        "halt_triggered": True,
        "last_processed_exit_ts_ms": 100,
    }
    next_state = resolve_execution_risk_control_online_state(
        risk_control_payload=payload,
        previous_state=previous,
        recent_trade_count=2,
        recent_nonpositive_rate_ucb=1.0,
        recent_severe_loss_rate_ucb=1.0,
        recent_max_exit_ts_ms=100,
    )

    assert next_state["new_evidence"] is False
    assert next_state["step_up"] == 2
    assert next_state["breach_streak"] == 7
    assert next_state["halt_triggered"] is True


def test_resolve_execution_risk_control_online_state_requires_min_trade_count_for_halt() -> None:
    payload = {
        "version": 1,
        "policy": "execution_risk_control_hoeffding_v1",
        "status": "ready",
        "selected_threshold": 1.0,
        "threshold_results": [{"threshold": 2.0}, {"threshold": 1.0}],
        "nonpositive_alpha": 0.30,
        "severe_loss_alpha": 0.20,
        "online_adaptation": {
            "enabled": True,
            "mode": "recent_closed_trade_hoeffding_stepup_v1",
            "lookback_trades": 12,
            "max_step_up": 2,
            "recovery_streak_required": 2,
            "min_halt_trade_count": 5,
            "halt_breach_streak": 1,
            "halt_reason_code": "RISK_CONTROL_ONLINE_BREACH_STREAK",
            "confidence_delta": 0.20,
            "checkpoint_name": "execution_risk_control_online_buffer",
        },
    }
    state = resolve_execution_risk_control_online_state(
        risk_control_payload=payload,
        previous_state={},
        recent_trade_count=2,
        recent_nonpositive_rate_ucb=1.0,
        recent_severe_loss_rate_ucb=1.0,
        recent_max_exit_ts_ms=1,
    )

    assert state["effective_min_halt_trade_count"] == 6
    assert state["breach_streak"] == 0
    assert state["halt_sample_ready"] is False
    assert state["halt_triggered"] is False


def test_resolve_execution_risk_control_online_state_does_not_build_streak_below_effective_floor() -> None:
    payload = {
        "version": 1,
        "policy": "execution_risk_control_hoeffding_v1",
        "status": "ready",
        "selected_threshold": 1.0,
        "threshold_results": [{"threshold": 2.0}, {"threshold": 1.0}],
        "nonpositive_alpha": 0.30,
        "severe_loss_alpha": 0.20,
        "online_adaptation": {
            "enabled": True,
            "mode": "recent_closed_trade_hoeffding_stepup_v1",
            "lookback_trades": 12,
            "max_step_up": 2,
            "recovery_streak_required": 2,
            "min_halt_trade_count": 5,
            "halt_breach_streak": 3,
            "halt_reason_code": "RISK_CONTROL_ONLINE_BREACH_STREAK",
            "confidence_delta": 0.20,
            "checkpoint_name": "execution_risk_control_online_buffer",
        },
    }

    state = resolve_execution_risk_control_online_state(
        risk_control_payload=payload,
        previous_state={"step_up": 2, "breach_streak": 2, "recovery_streak": 0, "halt_triggered": False},
        recent_trade_count=5,
        recent_nonpositive_rate_ucb=1.0,
        recent_severe_loss_rate_ucb=1.0,
        recent_max_exit_ts_ms=1,
    )

    assert state["effective_min_halt_trade_count"] == 6
    assert state["step_up"] == 0
    assert state["breach_streak"] == 0
    assert state["halt_triggered"] is False


def test_resolve_execution_risk_control_martingale_state_triggers_halt() -> None:
    payload = {
        "version": 1,
        "policy": "execution_risk_control_hoeffding_v1",
        "status": "ready",
        "nonpositive_alpha": 0.30,
        "severe_loss_alpha": 0.20,
        "severe_loss_return_threshold": 0.01,
        "online_adaptation": {
            "enabled": True,
            "martingale_enabled": True,
            "martingale_mode": "bernoulli_betting_eprocess_v1",
            "martingale_bet_fraction": 1.0,
            "martingale_halt_threshold": 2.0,
            "martingale_clear_threshold": 1.1,
            "martingale_halt_reason_code": "RISK_CONTROL_MARTINGALE_EVIDENCE",
        },
    }
    state = resolve_execution_risk_control_martingale_state(
        risk_control_payload=payload,
        previous_state={},
        observations=[
            {"exit_ts_ms": 1, "pnl_pct": -0.02},
            {"exit_ts_ms": 2, "pnl_pct": -0.02},
        ],
    )

    assert state["enabled"] is True
    assert state["martingale_halt_triggered"] is True
    assert state["martingale_halt_reason_code"] == "RISK_CONTROL_MARTINGALE_EVIDENCE"
    assert float(state["martingale_max_e_value"]) >= 2.0


def test_resolve_execution_risk_control_martingale_state_escalates_to_critical_halt() -> None:
    payload = {
        "version": 1,
        "policy": "execution_risk_control_hoeffding_v1",
        "status": "ready",
        "nonpositive_alpha": 0.30,
        "severe_loss_alpha": 0.20,
        "severe_loss_return_threshold": 0.01,
        "online_adaptation": {
            "enabled": True,
            "martingale_enabled": True,
            "martingale_mode": "bernoulli_betting_eprocess_v1",
            "martingale_bet_fraction": 1.0,
            "martingale_halt_threshold": 2.0,
            "martingale_clear_threshold": 1.1,
            "martingale_escalation_threshold": 4.0,
            "martingale_halt_reason_code": "RISK_CONTROL_MARTINGALE_EVIDENCE",
            "martingale_critical_reason_code": "RISK_CONTROL_MARTINGALE_CRITICAL_EVIDENCE",
        },
    }
    state = resolve_execution_risk_control_martingale_state(
        risk_control_payload=payload,
        previous_state={},
        observations=[
            {"exit_ts_ms": 1, "pnl_pct": -0.02},
            {"exit_ts_ms": 2, "pnl_pct": -0.02},
            {"exit_ts_ms": 3, "pnl_pct": -0.02},
        ],
    )

    assert state["enabled"] is True
    assert state["martingale_halt_triggered"] is True
    assert state["martingale_critical_triggered"] is True
    assert state["martingale_halt_reason_code"] == "RISK_CONTROL_MARTINGALE_CRITICAL_EVIDENCE"
    assert state["martingale_halt_action"] == "HALT_AND_CANCEL_BOT_ORDERS"
    assert float(state["martingale_max_e_value"]) >= 4.0


def test_resolve_execution_risk_control_martingale_state_clears_halt_after_recovery() -> None:
    payload = {
        "version": 1,
        "policy": "execution_risk_control_hoeffding_v1",
        "status": "ready",
        "nonpositive_alpha": 0.30,
        "severe_loss_alpha": 0.20,
        "severe_loss_return_threshold": 0.01,
        "online_adaptation": {
            "enabled": True,
            "martingale_enabled": True,
            "martingale_mode": "bernoulli_betting_eprocess_v1",
            "martingale_bet_fraction": 1.0,
            "martingale_halt_threshold": 2.0,
            "martingale_clear_threshold": 1.1,
            "martingale_escalation_threshold": 4.0,
            "martingale_halt_reason_code": "RISK_CONTROL_MARTINGALE_EVIDENCE",
            "martingale_critical_reason_code": "RISK_CONTROL_MARTINGALE_CRITICAL_EVIDENCE",
        },
    }
    previous = {
        "martingale_nonpositive_e_value": 2.5,
        "martingale_severe_e_value": 2.5,
        "martingale_halt_triggered": True,
        "martingale_halt_reason_code": "RISK_CONTROL_MARTINGALE_CRITICAL_EVIDENCE",
        "martingale_last_processed_exit_ts_ms": 2,
    }
    state = resolve_execution_risk_control_martingale_state(
        risk_control_payload=payload,
        previous_state=previous,
        observations=[
            {"exit_ts_ms": 3, "pnl_pct": 0.02},
            {"exit_ts_ms": 4, "pnl_pct": 0.02},
            {"exit_ts_ms": 5, "pnl_pct": 0.02},
            {"exit_ts_ms": 6, "pnl_pct": 0.02},
            {"exit_ts_ms": 7, "pnl_pct": 0.02},
        ],
    )

    assert state["martingale_halt_triggered"] is False
    assert state["martingale_clear_halt"] is True
    assert state["martingale_clear_reason_codes"] == ["RISK_CONTROL_MARTINGALE_CRITICAL_EVIDENCE"]
