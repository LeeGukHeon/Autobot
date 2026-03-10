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


def test_resolve_v4_promotion_compare_contract_exposes_thresholds_and_policy_variants() -> None:
    resolved = resolve_v4_promotion_compare_contract(
        "conservative_pareto",
        overrides={"candidate_min_orders_filled": 45},
    )

    assert resolved["profile_id"] == "v4_shared_economic_objective_v1"
    assert resolved["policy_name_requested"] == "conservative_pareto"
    assert resolved["policy_name_effective"] == "conservative_pareto"
    assert resolved["candidate_min_orders_filled"] == 45
    assert resolved["candidate_min_realized_pnl_quote"] == 0.0
    assert resolved["candidate_min_deflated_sharpe_ratio"] == 0.20
    assert resolved["champion_pnl_tolerance_pct"] == 0.02
    assert resolved["champion_min_utility_edge_pct"] == 0.05
    assert resolved["allow_stability_override"] is True
    assert resolved["cli_override_keys"] == ["candidate_min_orders_filled"]
