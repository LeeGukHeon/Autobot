from __future__ import annotations

from autobot.strategy.v5_post_model_contract import (
    V5_CONTINUATION_EXIT_REASON,
    V5_POST_MODEL_CONTRACT_VERSION,
    annotate_v5_runtime_recommendations,
    rank_v5_entry_candidates,
    resolve_v5_entry_gate,
    resolve_v5_exit_decision,
    resolve_v5_target_notional,
)


def test_annotate_v5_runtime_recommendations_populates_contract_identity() -> None:
    payload = annotate_v5_runtime_recommendations({"status": "ready"})

    assert payload["decision_contract_version"] == V5_POST_MODEL_CONTRACT_VERSION
    assert payload["entry_ownership"] == "predictor_boundary"
    assert payload["sizing_ownership"] == "portfolio_budget_first"
    assert payload["trade_action_role"] == "advisory_only_v1"
    assert payload["exit_ownership"] == "continuation_value_controller"


def test_rank_v5_entry_candidates_orders_by_alpha_lcb_expected_return_and_uncertainty() -> None:
    ranked = rank_v5_entry_candidates(
        [
            {
                "market": "KRW-BTC",
                "final_alpha_lcb": 0.004,
                "final_expected_return": 0.010,
                "final_uncertainty": 0.002,
            },
            {
                "market": "KRW-ETH",
                "final_alpha_lcb": 0.012,
                "final_expected_return": 0.008,
                "final_uncertainty": 0.010,
            },
            {
                "market": "KRW-XRP",
                "final_alpha_lcb": 0.012,
                "final_expected_return": 0.008,
                "final_uncertainty": 0.001,
            },
        ]
    )

    assert [item["market"] for item in ranked] == ["KRW-XRP", "KRW-ETH", "KRW-BTC"]
    assert [item["selected_rank"] for item in ranked] == [1, 2, 3]


def test_resolve_v5_target_notional_reuses_portfolio_signal_haircuts() -> None:
    payload = resolve_v5_target_notional(
        base_budget_quote=10_000.0,
        final_expected_return=0.0008,
        final_expected_es=0.0016,
        final_tradability=0.40,
        final_uncertainty=0.25,
        final_alpha_lcb=0.0004,
    )

    assert payload["target_notional_quote"] == 5_000.0
    assert payload["requested_notional_multiplier"] == 0.5
    assert "PORTFOLIO_ALPHA_LCB_HAIRCUT" in payload["reason_codes"]
    assert "PORTFOLIO_EXPECTED_ES_HAIRCUT" in payload["reason_codes"]
    assert "PORTFOLIO_TRADABILITY_HAIRCUT" in payload["reason_codes"]
    assert "PORTFOLIO_CONFIDENCE_HAIRCUT" in payload["reason_codes"]


def test_resolve_v5_entry_gate_blocks_nonpositive_edge() -> None:
    payload = resolve_v5_entry_gate(
        market="KRW-BTC",
        final_expected_return=0.01,
        final_expected_es=0.002,
        final_tradability=0.8,
        final_uncertainty=0.01,
        final_alpha_lcb=0.007,
        entry_boundary_decision={"enabled": True, "allowed": True, "reason_codes": []},
        expected_net_edge_bps=0.0,
    )

    assert payload["allowed"] is False
    assert "ENTRY_GATE_EXPECTED_EDGE_NOT_POSITIVE_AFTER_COST" in payload["reason_codes"]


def test_resolve_v5_exit_decision_prefers_continuation_controller() -> None:
    payload = resolve_v5_exit_decision(
        continuation_guidance={
            "continuation_should_exit": True,
            "exit_now_value_net": 0.006,
            "continue_value_net": 0.003,
            "immediate_exit_cost_ratio": 0.001,
            "alpha_decay_penalty_ratio": 0.0005,
        },
        net_return_ratio=0.007,
        trailing_drawdown_ratio=0.0,
        stop_loss_ratio=0.02,
        trailing_ratio=0.01,
        timeout_elapsed=False,
        mode="risk",
    )

    assert payload["should_exit"] is True
    assert payload["decision_reason_code"] == V5_CONTINUATION_EXIT_REASON
    assert payload["continue_value_lcb"] == 0.003
