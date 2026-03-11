from __future__ import annotations

from autobot.models.economic_objective import (
    build_v4_shared_economic_objective_profile,
    build_v4_trainer_sweep_sort_key,
    compare_v4_profiled_pareto,
    resolve_v4_promotion_compare_contract,
)


def test_build_v4_trainer_sweep_sort_key_prefers_ev_net_first_for_cls() -> None:
    lower_ev_higher_precision = {
        "classification": {"pr_auc": 0.80, "roc_auc": 0.82},
        "trading": {"top_5pct": {"precision": 0.70, "ev_net": 0.0010}},
    }
    higher_ev_lower_precision = {
        "classification": {"pr_auc": 0.72, "roc_auc": 0.74},
        "trading": {"top_5pct": {"precision": 0.62, "ev_net": 0.0015}},
    }

    left_key = build_v4_trainer_sweep_sort_key(lower_ev_higher_precision, task="cls")
    right_key = build_v4_trainer_sweep_sort_key(higher_ev_lower_precision, task="cls")

    assert right_key > left_key


def test_compare_v4_profiled_pareto_emits_profile_metadata() -> None:
    compare = compare_v4_profiled_pareto(
        {
            "orders_filled": 12,
            "realized_pnl_quote": 1050.0,
            "fill_rate": 0.95,
            "max_drawdown_pct": 0.70,
            "slippage_bps_mean": 3.0,
        },
        {
            "orders_filled": 11,
            "realized_pnl_quote": 1000.0,
            "fill_rate": 0.94,
            "max_drawdown_pct": 0.72,
            "slippage_bps_mean": 3.2,
        },
        context="execution_compare",
    )

    profile = build_v4_shared_economic_objective_profile()
    assert compare["comparable"] is True
    assert compare["economic_objective_profile_id"] == profile["profile_id"]
    assert compare["economic_objective_context"] == "execution_compare"
    assert compare["metric_order"]["higher_is_better"] == ["realized_pnl_quote", "fill_rate"]
    assert compare["primary_metric_order"]["higher_is_better"] == ["realized_pnl_quote"]
    assert compare["primary_metric_order"]["lower_is_better"] == ["max_drawdown_pct"]
    assert compare["implementation_metric_order"]["higher_is_better"] == ["fill_rate"]
    assert compare["implementation_metric_order"]["lower_is_better"] == ["slippage_bps_mean"]


def test_compare_v4_profiled_pareto_prefers_downside_edge_before_execution_friction_tie_break() -> None:
    compare = compare_v4_profiled_pareto(
        {
            "orders_filled": 12,
            "realized_pnl_quote": 980.0,
            "fill_rate": 0.90,
            "max_drawdown_pct": 0.50,
            "slippage_bps_mean": 4.2,
        },
        {
            "orders_filled": 11,
            "realized_pnl_quote": 1000.0,
            "fill_rate": 0.91,
            "max_drawdown_pct": 0.81,
            "slippage_bps_mean": 3.8,
        },
        context="execution_compare",
    )

    assert compare["comparable"] is True
    assert compare["candidate_dominates"] is False
    assert compare["champion_dominates"] is False
    assert compare["decision"] == "candidate_edge"
    assert compare["primary_utility_score"] > 0.0
    assert compare["implementation_utility_score"] < 0.0
    assert compare["reasons"] == ["PRIMARY_RETURN_DOWNSIDE_UTILITY_PASS"]


def test_resolve_v4_promotion_compare_contract_exposes_thresholds_and_policy_variants() -> None:
    resolved = resolve_v4_promotion_compare_contract(
        "conservative_pareto",
        overrides={"candidate_min_orders_filled": 45},
    )

    assert resolved["profile_id"] == "v4_shared_economic_objective_v3"
    assert resolved["policy_name_requested"] == "conservative_pareto"
    assert resolved["policy_name_effective"] == "conservative_pareto"
    assert resolved["candidate_min_orders_filled"] == 45
    assert resolved["candidate_min_realized_pnl_quote"] == 0.0
    assert resolved["candidate_min_deflated_sharpe_ratio"] == 0.20
    assert resolved["champion_pnl_tolerance_pct"] == 0.02
    assert resolved["champion_min_utility_edge_pct"] == 0.05
    assert resolved["allow_stability_override"] is True
    assert resolved["cli_override_keys"] == ["candidate_min_orders_filled"]
