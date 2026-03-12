from __future__ import annotations

from pathlib import Path

from autobot.models.predictor import ModelPredictor
from autobot.models.runtime_recommendation_contract import normalize_runtime_recommendations_payload
from autobot.models.runtime_recommendations import _build_exit_doc, _rank_execution_rows
from autobot.strategy.model_alpha_v1 import (
    ModelAlphaExecutionSettings,
    ModelAlphaExitSettings,
    ModelAlphaSettings,
    resolve_runtime_model_alpha_settings,
)


def _dummy_predictor(*, runtime_recommendations: dict[str, object]) -> ModelPredictor:
    return ModelPredictor(
        run_dir=Path("models/registry/train_v4_crypto_cs/run"),
        model_bundle={"model_type": "dummy"},
        model_ref="candidate_v4",
        model_family="train_v4_crypto_cs",
        feature_columns=(),
        train_config={},
        thresholds={},
        selection_recommendations={},
        runtime_recommendations=runtime_recommendations,
    )


def test_resolve_runtime_model_alpha_settings_applies_learned_exit_hold_and_execution() -> None:
    predictor = _dummy_predictor(
        runtime_recommendations={
            "exit": {
                "recommended_exit_mode": "risk",
                "recommended_exit_mode_source": "execution_backtest_grid_search_compare",
                "recommended_exit_mode_reason_code": "RISK_EXECUTION_COMPARE_EDGE",
                "recommended_hold_bars": 12,
                "recommendation_source": "execution_backtest_grid_search",
                "summary": {
                    "orders_filled": 12,
                    "realized_pnl_quote": 100.0,
                    "fill_rate": 0.9,
                    "max_drawdown_pct": 2.0,
                    "slippage_bps_mean": 5.0,
                },
                "risk_summary": {
                    "orders_filled": 12,
                    "realized_pnl_quote": 130.0,
                    "fill_rate": 0.94,
                    "max_drawdown_pct": 1.2,
                    "slippage_bps_mean": 3.2,
                },
                "exit_mode_compare": {"decision": "candidate_edge", "comparable": True},
                "recommended_risk_scaling_mode": "volatility_scaled",
                "recommended_risk_vol_feature": "rv_36",
                "recommended_tp_vol_multiplier": 2.5,
                "recommended_sl_vol_multiplier": 1.5,
                "recommended_trailing_vol_multiplier": 0.75,
            },
            "execution": {
                "recommended_price_mode": "CROSS_1T",
                "recommended_timeout_bars": 4,
                "recommended_replace_max": 1,
                "recommendation_source": "execution_backtest_grid_search",
            },
        }
    )
    settings = ModelAlphaSettings(
        exit=ModelAlphaExitSettings(mode="hold", hold_bars=6, use_learned_hold_bars=True),
        execution=ModelAlphaExecutionSettings(
            price_mode="JOIN",
            timeout_bars=2,
            replace_max=2,
            use_learned_recommendations=True,
        ),
    )

    resolved, state = resolve_runtime_model_alpha_settings(predictor=predictor, settings=settings)

    assert resolved.exit.mode == "risk"
    assert resolved.exit.hold_bars == 12
    assert resolved.exit.risk_scaling_mode == "volatility_scaled"
    assert resolved.exit.risk_vol_feature == "rv_36"
    assert resolved.exit.tp_vol_multiplier == 2.5
    assert resolved.exit.sl_vol_multiplier == 1.5
    assert resolved.exit.trailing_vol_multiplier == 0.75
    assert resolved.execution.price_mode == "CROSS_1T"
    assert resolved.execution.timeout_bars == 4
    assert resolved.execution.replace_max == 1
    assert state["exit_mode_source"] == "execution_backtest_grid_search_compare"
    assert state["exit_hold_bars_source"] == "execution_backtest_grid_search"
    assert state["execution_source"] == "execution_backtest_grid_search"


def test_normalize_runtime_recommendations_backfills_legacy_exit_mode_compare() -> None:
    payload = normalize_runtime_recommendations_payload(
        {
            "exit": {
                "mode": "hold",
                "recommended_hold_bars": 6,
                "recommendation_source": "execution_backtest_grid_search",
                "objective_score": 0.72,
                "risk_objective_score": 0.0,
                "summary": {
                    "orders_filled": 10,
                    "realized_pnl_quote": 120.0,
                    "fill_rate": 0.95,
                    "max_drawdown_pct": 1.1,
                    "slippage_bps_mean": 2.0,
                },
                "risk_summary": {
                    "orders_filled": 10,
                    "realized_pnl_quote": 90.0,
                    "fill_rate": 0.90,
                    "max_drawdown_pct": 1.8,
                    "slippage_bps_mean": 3.5,
                },
                "grid_point": {"hold_bars": 6},
                "risk_grid_point": {
                    "risk_scaling_mode": "volatility_scaled",
                    "risk_vol_feature": "rv_12",
                    "tp_vol_multiplier": 1.5,
                    "sl_vol_multiplier": 1.0,
                    "trailing_vol_multiplier": 0.0,
                },
            }
        }
    )

    exit_doc = payload["exit"]
    assert exit_doc["recommended_exit_mode"] == "hold"
    assert exit_doc["recommended_exit_mode_source"] == "execution_backtest_grid_search_compare"
    assert exit_doc["recommended_exit_mode_reason_code"] == "HOLD_EXECUTION_COMPARE_EDGE"
    assert exit_doc["exit_mode_compare"]["decision"] == "champion_edge"
    assert exit_doc["contract_status"] == "backfilled"
    assert "recommended_exit_mode" in exit_doc["contract_backfilled_fields"]
    assert exit_doc["contract_issues"] == []
    assert exit_doc["hold_family"]["status"] == "legacy_backfilled"
    assert exit_doc["family_compare"]["status"] == "supported"


def test_resolve_runtime_model_alpha_settings_rejects_partial_invalid_exit_contract() -> None:
    predictor = _dummy_predictor(
        runtime_recommendations={
            "exit": {
                "recommended_hold_bars": 12,
                "recommendation_source": "execution_backtest_grid_search",
                "recommended_risk_scaling_mode": "volatility_scaled",
                "recommended_risk_vol_feature": "rv_36",
                "recommended_tp_vol_multiplier": 2.5,
                "recommended_sl_vol_multiplier": 1.5,
                "recommended_trailing_vol_multiplier": 0.75,
            }
        }
    )
    settings = ModelAlphaSettings(
        exit=ModelAlphaExitSettings(
            mode="hold",
            hold_bars=6,
            risk_scaling_mode="fixed",
            risk_vol_feature="rv_12",
            tp_vol_multiplier=1.0,
            sl_vol_multiplier=0.5,
            trailing_vol_multiplier=0.25,
            use_learned_exit_mode=True,
            use_learned_hold_bars=True,
            use_learned_risk_recommendations=True,
        ),
        execution=ModelAlphaExecutionSettings(use_learned_recommendations=False),
    )

    resolved, state = resolve_runtime_model_alpha_settings(predictor=predictor, settings=settings)

    assert resolved.exit.mode == "hold"
    assert resolved.exit.hold_bars == 6
    assert resolved.exit.risk_scaling_mode == "fixed"
    assert resolved.exit.risk_vol_feature == "rv_12"
    assert resolved.exit.tp_vol_multiplier == 1.0
    assert resolved.exit.sl_vol_multiplier == 0.5
    assert resolved.exit.trailing_vol_multiplier == 0.25
    assert state["exit_contract_status"] == "invalid"
    assert "EXIT_RECOMMENDATION_EVIDENCE_MISSING" in state["exit_contract_issues"]


def test_resolve_runtime_model_alpha_settings_keeps_manual_exit_when_family_compare_is_insufficient() -> None:
    predictor = _dummy_predictor(
        runtime_recommendations={
            "exit": {
                "recommended_exit_mode_source": "execution_backtest_family_compare",
                "recommended_exit_mode_reason_code": "EXIT_FAMILY_INSUFFICIENT_EVIDENCE",
                "recommended_hold_bars": 12,
                "recommended_risk_scaling_mode": "volatility_scaled",
                "recommended_risk_vol_feature": "rv_36",
                "recommended_tp_vol_multiplier": 2.5,
                "recommended_sl_vol_multiplier": 1.5,
                "recommended_trailing_vol_multiplier": 0.75,
                "summary": {"orders_filled": 12, "realized_pnl_quote": 100.0, "fill_rate": 0.9, "max_drawdown_pct": 2.0, "slippage_bps_mean": 5.0},
                "risk_summary": {"orders_filled": 8, "realized_pnl_quote": 90.0, "fill_rate": 0.8, "max_drawdown_pct": 2.5, "slippage_bps_mean": 6.0},
                "grid_point": {"hold_bars": 12},
                "risk_grid_point": {
                    "hold_bars": 12,
                    "risk_scaling_mode": "volatility_scaled",
                    "risk_vol_feature": "rv_36",
                    "tp_vol_multiplier": 2.5,
                    "sl_vol_multiplier": 1.5,
                    "trailing_vol_multiplier": 0.75,
                },
                "hold_family": {"status": "insufficient_support", "best_rule_id": "hold_h12", "best_comparable_rule_id": ""},
                "risk_family": {"status": "supported", "best_rule_id": "risk_h12_rv_36_tp2p5_sl1p5_tr0p75", "best_comparable_rule_id": "risk_h12_rv_36_tp2p5_sl1p5_tr0p75"},
                "family_compare": {
                    "status": "insufficient_support",
                    "decision": "not_comparable",
                    "comparable": False,
                    "reason_codes": ["HOLD_FAMILY_NO_COMPARABLE_RULE"],
                },
                "family_compare_status": "insufficient_support",
            }
        }
    )
    settings = ModelAlphaSettings(
        exit=ModelAlphaExitSettings(
            mode="hold",
            hold_bars=6,
            risk_scaling_mode="fixed",
            risk_vol_feature="rv_12",
            tp_vol_multiplier=1.0,
            sl_vol_multiplier=0.5,
            trailing_vol_multiplier=0.25,
            use_learned_exit_mode=True,
            use_learned_hold_bars=True,
            use_learned_risk_recommendations=True,
        ),
        execution=ModelAlphaExecutionSettings(use_learned_recommendations=False),
    )

    resolved, state = resolve_runtime_model_alpha_settings(predictor=predictor, settings=settings)

    assert resolved.exit.mode == "hold"
    assert resolved.exit.hold_bars == 6
    assert resolved.exit.risk_scaling_mode == "fixed"
    assert state["exit_family_compare_status"] == "insufficient_support"
    assert "HOLD_FAMILY_NO_COMPARABLE_RULE" in state["exit_family_compare_reason_codes"]


def test_rank_execution_rows_prefers_pairwise_winner() -> None:
    rows = [
        {
            "grid_point": {"hold_bars": 3},
            "summary": {
                "orders_filled": 12,
                "realized_pnl_quote": 100.0,
                "fill_rate": 0.90,
                "max_drawdown_pct": 1.5,
                "slippage_bps_mean": 4.0,
            },
        },
        {
            "grid_point": {"hold_bars": 6},
            "summary": {
                "orders_filled": 12,
                "realized_pnl_quote": 140.0,
                "fill_rate": 0.94,
                "max_drawdown_pct": 1.1,
                "slippage_bps_mean": 3.2,
            },
        },
        {
            "grid_point": {"hold_bars": 9},
            "summary": {
                "orders_filled": 12,
                "realized_pnl_quote": 120.0,
                "fill_rate": 0.92,
                "max_drawdown_pct": 1.8,
                "slippage_bps_mean": 4.8,
            },
        },
    ]

    ranked = _rank_execution_rows(rows)

    assert ranked
    assert ranked[0]["grid_point"]["hold_bars"] == 6
    assert ranked[0]["wins"] >= ranked[-1]["wins"]


def test_build_exit_doc_prefers_risk_mode_when_risk_policy_wins() -> None:
    hold_family = {
        "status": "supported",
        "best_rule": {
            "kind": "hold",
            "grid_point": {"hold_bars": 6},
            "summary": {
                "orders_filled": 12,
                "realized_pnl_quote": 100.0,
                "fill_rate": 0.90,
                "max_drawdown_pct": 2.0,
                "slippage_bps_mean": 5.0,
            },
        },
        "best_comparable_rule": {
            "kind": "hold",
            "grid_point": {"hold_bars": 6},
            "summary": {
                "orders_filled": 12,
                "realized_pnl_quote": 100.0,
                "fill_rate": 0.90,
                "max_drawdown_pct": 2.0,
                "slippage_bps_mean": 5.0,
            },
        },
    }
    risk_family = {
        "status": "supported",
        "best_rule": {
            "kind": "risk_exit",
            "grid_point": {
                "hold_bars": 9,
                "risk_scaling_mode": "volatility_scaled",
                "risk_vol_feature": "rv_12",
                "tp_vol_multiplier": 2.0,
                "sl_vol_multiplier": 1.0,
                "trailing_vol_multiplier": 0.75,
            },
            "summary": {
                "orders_filled": 12,
                "realized_pnl_quote": 98.0,
                "fill_rate": 0.89,
                "max_drawdown_pct": 0.8,
                "slippage_bps_mean": 4.2,
            },
        },
        "best_comparable_rule": {
            "kind": "risk_exit",
            "grid_point": {
                "hold_bars": 9,
                "risk_scaling_mode": "volatility_scaled",
                "risk_vol_feature": "rv_12",
                "tp_vol_multiplier": 2.0,
                "sl_vol_multiplier": 1.0,
                "trailing_vol_multiplier": 0.75,
            },
            "summary": {
                "orders_filled": 12,
                "realized_pnl_quote": 98.0,
                "fill_rate": 0.89,
                "max_drawdown_pct": 0.8,
                "slippage_bps_mean": 4.2,
            },
        },
    }
    exit_doc = _build_exit_doc(
        hold_family=hold_family,
        risk_family=risk_family,
        family_compare={"status": "supported", "decision": "candidate_edge", "comparable": True, "reason_codes": []},
        fallback_hold_bars=6,
        fallback_exit=ModelAlphaExitSettings(mode="hold", hold_bars=6),
    )

    assert exit_doc["recommended_exit_mode"] == "risk"
    assert exit_doc["recommended_exit_mode_reason_code"] == "RISK_EXECUTION_COMPARE_EDGE"
    assert exit_doc["recommended_hold_bars"] == 9
    assert exit_doc["selected_policy_kind"] == "risk_exit"
    assert exit_doc["selected_grid_point"]["hold_bars"] == 9
