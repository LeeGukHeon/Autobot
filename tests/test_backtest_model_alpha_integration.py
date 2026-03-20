from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import polars as pl

from autobot.backtest.engine import BacktestRunEngine, BacktestRunSettings
from autobot.models.predictor import ModelPredictor
from autobot.models.registry import RegistrySavePayload, save_run
from autobot.models.trade_action_policy import TRADE_ACTION_POLICY_ID, build_trade_action_policy_from_oos_rows
from autobot.paper.sim_exchange import MarketRules
from autobot.strategy.model_alpha_v1 import (
    ModelAlphaExecutionSettings,
    ModelAlphaExitSettings,
    ModelAlphaPositionSettings,
    ModelAlphaSelectionSettings,
    ModelAlphaSettings,
    ModelAlphaStrategyV1,
    build_model_alpha_exit_plan_payload,
)


class _StaticRulesProvider:
    def get_rules(self, *, market: str, reference_price: float, ts_ms: int) -> MarketRules:
        _ = (market, reference_price, ts_ms)
        return MarketRules(
            bid_fee=0.0005,
            ask_fee=0.0005,
            maker_bid_fee=0.0002,
            maker_ask_fee=0.0002,
            min_total=5_000.0,
            tick_size=1.0,
        )


class _DummyEstimator:
    def predict_proba(self, x: np.ndarray) -> np.ndarray:
        logits = x[:, 0].astype(np.float64)
        probs = 1.0 / (1.0 + np.exp(-logits))
        probs = np.clip(probs, 1e-6, 1.0 - 1e-6)
        return np.column_stack([1.0 - probs, probs])


def _build_strategy(
    *,
    groups: list[tuple[int, pl.DataFrame]],
    settings: ModelAlphaSettings,
    thresholds: dict[str, float] | None = None,
    selection_recommendations: dict[str, object] | None = None,
    selection_policy: dict[str, object] | None = None,
    selection_calibration: dict[str, object] | None = None,
    runtime_recommendations: dict[str, object] | None = None,
) -> ModelAlphaStrategyV1:
    predictor = ModelPredictor(
        run_dir=Path("."),
        model_bundle={"model_type": "xgboost", "scaler": None, "estimator": _DummyEstimator()},
        model_ref="run",
        model_family="train_v3_mtf_micro",
        feature_columns=("f1",),
        train_config={"dataset_root": "unused"},
        thresholds=thresholds or {},
        selection_recommendations=selection_recommendations or {},
        runtime_recommendations=runtime_recommendations or {},
        selection_policy=selection_policy or {},
        selection_calibration=selection_calibration or {},
    )
    from autobot.models.dataset_loader import FeatureTsGroup

    feature_groups = [FeatureTsGroup(ts_ms=ts, frame=frame) for ts, frame in groups]
    return ModelAlphaStrategyV1(
        predictor=predictor,
        feature_groups=feature_groups,
        settings=settings,
        interval_ms=300_000,
    )


def test_model_alpha_selection_allows_zero_without_forced_pick() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [1_000, 1_000],
            "market": ["KRW-BTC", "KRW-ETH"],
            "f1": [-5.0, -4.0],
            "close": [100.0, 200.0],
        }
    )
    strategy = _build_strategy(
        groups=[(1_000, frame)],
        settings=ModelAlphaSettings(
            selection=ModelAlphaSelectionSettings(top_pct=0.05, min_prob=0.99, min_candidates_per_ts=1),
        ),
    )
    result = strategy.on_ts(
        ts_ms=1_000,
        active_markets=["KRW-BTC", "KRW-ETH"],
        latest_prices={"KRW-BTC": 100.0, "KRW-ETH": 200.0},
        open_markets=set(),
    )
    assert result.selected_rows == 0
    assert not any(intent.side == "bid" for intent in result.intents)


def test_model_alpha_uses_registry_threshold_when_min_prob_is_null() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [1_000, 1_000, 1_000],
            "market": ["KRW-BTC", "KRW-ETH", "KRW-XRP"],
            "f1": [2.0, 0.4, -0.3],
            "close": [100.0, 200.0, 300.0],
        }
    )
    strategy = _build_strategy(
        groups=[(1_000, frame)],
        settings=ModelAlphaSettings(
            selection=ModelAlphaSelectionSettings(top_pct=1.0, min_prob=None, min_candidates_per_ts=1),
        ),
        thresholds={"top_5pct": 0.7},
    )
    result = strategy.on_ts(
        ts_ms=1_000,
        active_markets=["KRW-BTC", "KRW-ETH", "KRW-XRP"],
        latest_prices={"KRW-BTC": 100.0, "KRW-ETH": 200.0, "KRW-XRP": 300.0},
        open_markets=set(),
    )
    assert result.min_prob_source == "registry:top_5pct"
    assert result.min_prob_used == 0.7
    assert result.eligible_rows == 1
    assert result.selected_rows == 1
    assert [intent.market for intent in result.intents if intent.side == "bid"] == ["KRW-BTC"]


def test_model_alpha_manual_min_prob_overrides_registry_threshold() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [1_000, 1_000],
            "market": ["KRW-BTC", "KRW-ETH"],
            "f1": [2.0, 0.3],
            "close": [100.0, 200.0],
        }
    )
    strategy = _build_strategy(
        groups=[(1_000, frame)],
        settings=ModelAlphaSettings(
            selection=ModelAlphaSelectionSettings(top_pct=1.0, min_prob=0.5, min_candidates_per_ts=1),
        ),
        thresholds={"top_5pct": 0.95},
    )
    result = strategy.on_ts(
        ts_ms=1_000,
        active_markets=["KRW-BTC", "KRW-ETH"],
        latest_prices={"KRW-BTC": 100.0, "KRW-ETH": 200.0},
        open_markets=set(),
    )
    assert result.min_prob_source == "manual"
    assert result.min_prob_used == 0.5
    assert result.eligible_rows == 2
    assert result.selected_rows == 2


def test_model_alpha_uses_registry_selection_recommendations_when_enabled() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [1_000, 1_000, 1_000, 1_000],
            "market": ["KRW-BTC", "KRW-ETH", "KRW-XRP", "KRW-DOGE"],
            "f1": [2.0, 1.0, 0.4, -0.1],
            "close": [100.0, 200.0, 300.0, 400.0],
        }
    )
    strategy = _build_strategy(
        groups=[(1_000, frame)],
        settings=ModelAlphaSettings(
            selection=ModelAlphaSelectionSettings(
                top_pct=0.10,
                min_prob=None,
                min_candidates_per_ts=10,
                use_learned_recommendations=True,
            ),
        ),
        thresholds={"top_5pct": 0.5, "ev_opt": 0.6, "ev_opt_selected_rows": 2},
        selection_recommendations={
            "by_threshold_key": {
                "top_5pct": {
                    "recommended_top_pct": 0.5,
                    "recommended_min_candidates_per_ts": 1,
                }
            }
        },
    )
    result = strategy.on_ts(
        ts_ms=1_000,
        active_markets=["KRW-BTC", "KRW-ETH", "KRW-XRP", "KRW-DOGE"],
        latest_prices={"KRW-BTC": 100.0, "KRW-ETH": 200.0, "KRW-XRP": 300.0, "KRW-DOGE": 400.0},
        open_markets=set(),
    )
    assert result.min_prob_source == "registry:top_5pct"
    assert result.top_pct_source == "registry_recommendation:top_5pct"
    assert result.min_candidates_source == "registry_recommendation:top_5pct"
    assert result.top_pct_used == 0.5
    assert result.min_candidates_used == 1
    assert result.eligible_rows == 3
    assert result.selected_rows == 1


def test_model_alpha_manual_selection_can_disable_registry_recommendations() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [1_000, 1_000, 1_000],
            "market": ["KRW-BTC", "KRW-ETH", "KRW-XRP"],
            "f1": [2.0, 1.0, 0.4],
            "close": [100.0, 200.0, 300.0],
        }
    )
    strategy = _build_strategy(
        groups=[(1_000, frame)],
        settings=ModelAlphaSettings(
            selection=ModelAlphaSelectionSettings(
                top_pct=1.0,
                min_prob=None,
                min_candidates_per_ts=2,
                use_learned_recommendations=False,
            ),
        ),
        thresholds={"top_5pct": 0.5, "ev_opt": 0.6, "ev_opt_selected_rows": 2},
        selection_recommendations={
            "by_threshold_key": {
                "top_5pct": {
                    "recommended_top_pct": 0.5,
                    "recommended_min_candidates_per_ts": 1,
                }
            }
        },
    )
    result = strategy.on_ts(
        ts_ms=1_000,
        active_markets=["KRW-BTC", "KRW-ETH", "KRW-XRP"],
        latest_prices={"KRW-BTC": 100.0, "KRW-ETH": 200.0, "KRW-XRP": 300.0},
        open_markets=set(),
    )
    assert result.top_pct_source == "manual"
    assert result.min_candidates_source == "manual"
    assert result.top_pct_used == 1.0
    assert result.min_candidates_used == 2
    assert result.eligible_rows == 3
    assert result.selected_rows == 3


def test_model_alpha_uses_learned_threshold_key_when_recommended() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [1_000, 1_000, 1_000],
            "market": ["KRW-BTC", "KRW-ETH", "KRW-XRP"],
            "f1": [3.0, 2.5, 0.2],
            "close": [100.0, 200.0, 300.0],
        }
    )
    strategy = _build_strategy(
        groups=[(1_000, frame)],
        settings=ModelAlphaSettings(
            selection=ModelAlphaSelectionSettings(
                top_pct=1.0,
                min_prob=None,
                min_candidates_per_ts=1,
                registry_threshold_key="top_5pct",
                use_learned_recommendations=True,
            ),
        ),
        thresholds={"top_5pct": 0.5, "ev_opt": 0.9},
        selection_recommendations={
            "recommended_threshold_key": "ev_opt",
            "by_threshold_key": {
                "ev_opt": {
                    "recommended_top_pct": 1.0,
                    "recommended_min_candidates_per_ts": 1,
                },
                "top_5pct": {
                    "recommended_top_pct": 1.0,
                    "recommended_min_candidates_per_ts": 1,
                },
            },
        },
    )
    result = strategy.on_ts(
        ts_ms=1_000,
        active_markets=["KRW-BTC", "KRW-ETH", "KRW-XRP"],
        latest_prices={"KRW-BTC": 100.0, "KRW-ETH": 200.0, "KRW-XRP": 300.0},
        open_markets=set(),
    )

    assert result.min_prob_source == "registry:ev_opt"
    assert result.min_prob_used == 0.9
    assert result.top_pct_source == "registry_recommendation:ev_opt:learned_threshold_key"
    assert result.min_candidates_source == "registry_recommendation:ev_opt:learned_threshold_key"
    assert result.eligible_rows == 2
    assert result.selected_rows == 2


def test_model_alpha_min_candidates_blocks_on_eligible_rows() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [1_000, 1_000, 1_000],
            "market": ["KRW-BTC", "KRW-ETH", "KRW-XRP"],
            "f1": [2.0, -0.3, -0.7],
            "close": [100.0, 200.0, 300.0],
        }
    )
    strategy = _build_strategy(
        groups=[(1_000, frame)],
        settings=ModelAlphaSettings(
            selection=ModelAlphaSelectionSettings(top_pct=1.0, min_prob=0.7, min_candidates_per_ts=2),
        ),
    )
    result = strategy.on_ts(
        ts_ms=1_000,
        active_markets=["KRW-BTC", "KRW-ETH", "KRW-XRP"],
        latest_prices={"KRW-BTC": 100.0, "KRW-ETH": 200.0, "KRW-XRP": 300.0},
        open_markets=set(),
    )
    assert result.scored_rows == 3
    assert result.eligible_rows == 1
    assert result.blocked_min_candidates_ts == 1
    assert result.selected_rows == 0
    assert not any(intent.side == "bid" for intent in result.intents)


def test_model_alpha_auto_selection_policy_uses_rank_quantile_when_registry_policy_exists() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [1_000, 1_000, 1_000],
            "market": ["KRW-BTC", "KRW-ETH", "KRW-XRP"],
            "f1": [2.0, 1.0, 0.1],
            "close": [100.0, 200.0, 300.0],
        }
    )
    strategy = _build_strategy(
        groups=[(1_000, frame)],
        settings=ModelAlphaSettings(
            selection=ModelAlphaSelectionSettings(
                top_pct=1.0,
                min_prob=None,
                min_candidates_per_ts=1,
                registry_threshold_key="top_1pct",
                use_learned_recommendations=True,
                selection_policy_mode="auto",
            ),
        ),
        thresholds={"top_1pct": 0.9999},
        selection_recommendations={
            "recommended_threshold_key": "top_1pct",
            "by_threshold_key": {
                "top_1pct": {
                    "recommended_top_pct": 1.0,
                    "recommended_min_candidates_per_ts": 1,
                    "eligible_ratio": 0.01,
                }
            },
        },
        selection_policy={
            "mode": "rank_effective_quantile",
            "selection_fraction": 0.34,
            "min_candidates_per_ts": 1,
            "threshold_key": "top_1pct",
            "eligible_ratio": 0.01,
            "recommended_top_pct": 1.0,
        },
    )
    result = strategy.on_ts(
        ts_ms=1_000,
        active_markets=["KRW-BTC", "KRW-ETH", "KRW-XRP"],
        latest_prices={"KRW-BTC": 100.0, "KRW-ETH": 200.0, "KRW-XRP": 300.0},
        open_markets=set(),
    )
    bid_intents = [intent.market for intent in result.intents if intent.side == "bid"]
    assert result.selection_policy_mode == "rank_effective_quantile"
    assert result.selection_policy_source == "registry_selection_policy"
    assert result.min_prob_used == 0.0
    assert result.eligible_rows == 3
    assert result.selected_rows == 1
    assert bid_intents == ["KRW-BTC"]


def test_model_alpha_manual_min_prob_keeps_raw_threshold_even_with_registry_policy() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [1_000, 1_000, 1_000],
            "market": ["KRW-BTC", "KRW-ETH", "KRW-XRP"],
            "f1": [2.0, 1.0, 0.1],
            "close": [100.0, 200.0, 300.0],
        }
    )
    strategy = _build_strategy(
        groups=[(1_000, frame)],
        settings=ModelAlphaSettings(
            selection=ModelAlphaSelectionSettings(
                top_pct=1.0,
                min_prob=0.7,
                min_candidates_per_ts=1,
                selection_policy_mode="auto",
            ),
        ),
        selection_policy={
            "mode": "rank_effective_quantile",
            "selection_fraction": 0.34,
            "min_candidates_per_ts": 1,
            "threshold_key": "top_1pct",
            "eligible_ratio": 0.01,
            "recommended_top_pct": 1.0,
        },
    )
    result = strategy.on_ts(
        ts_ms=1_000,
        active_markets=["KRW-BTC", "KRW-ETH", "KRW-XRP"],
        latest_prices={"KRW-BTC": 100.0, "KRW-ETH": 200.0, "KRW-XRP": 300.0},
        open_markets=set(),
    )
    assert result.selection_policy_mode == "raw_threshold"
    assert result.selection_policy_source == "manual_min_prob"
    assert result.min_prob_used == 0.7
    assert result.eligible_rows == 2
    assert result.selected_rows == 2


def test_model_alpha_rank_policy_uses_calibrated_probability_for_intent_metadata() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [1_000, 1_000, 1_000],
            "market": ["KRW-BTC", "KRW-ETH", "KRW-XRP"],
            "f1": [2.0, 1.0, 0.1],
            "close": [100.0, 200.0, 300.0],
        }
    )
    strategy = _build_strategy(
        groups=[(1_000, frame)],
        settings=ModelAlphaSettings(
            selection=ModelAlphaSelectionSettings(
                top_pct=1.0,
                min_prob=None,
                min_candidates_per_ts=1,
                selection_policy_mode="auto",
            ),
        ),
        selection_policy={
            "mode": "rank_effective_quantile",
            "selection_fraction": 0.34,
            "min_candidates_per_ts": 1,
            "threshold_key": "top_1pct",
            "eligible_ratio": 0.01,
            "recommended_top_pct": 1.0,
        },
        selection_calibration={
            "mode": "isotonic_oos_v1",
            "x_knots": [0.0, 1.0],
            "y_knots": [0.0, 0.5],
            "comparable": True,
        },
    )
    result = strategy.on_ts(
        ts_ms=1_000,
        active_markets=["KRW-BTC", "KRW-ETH", "KRW-XRP"],
        latest_prices={"KRW-BTC": 100.0, "KRW-ETH": 200.0, "KRW-XRP": 300.0},
        open_markets=set(),
    )
    bid_intent = next(intent for intent in result.intents if intent.side == "bid")
    assert bid_intent.market == "KRW-BTC"
    assert float(bid_intent.prob or 0.0) < 0.5
    assert float((bid_intent.meta or {}).get("model_prob_raw", 0.0)) > float(bid_intent.prob or 0.0)

def test_model_alpha_min_candidates_can_block_all_entries() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [1_000, 1_000, 1_000],
            "market": ["KRW-BTC", "KRW-ETH", "KRW-XRP"],
            "f1": [1.0, 0.5, 0.2],
            "close": [100.0, 200.0, 300.0],
        }
    )
    strategy = _build_strategy(
        groups=[(1_000, frame)],
        settings=ModelAlphaSettings(
            selection=ModelAlphaSelectionSettings(top_pct=0.5, min_prob=0.0, min_candidates_per_ts=10),
        ),
    )
    result = strategy.on_ts(
        ts_ms=1_000,
        active_markets=["KRW-BTC", "KRW-ETH", "KRW-XRP"],
        latest_prices={"KRW-BTC": 100.0, "KRW-ETH": 200.0, "KRW-XRP": 300.0},
        open_markets=set(),
    )
    assert result.blocked_min_candidates_ts == 1
    assert result.selected_rows == 0
    assert not any(intent.side == "bid" for intent in result.intents)


def test_model_alpha_prob_ramp_sets_notional_multiplier_from_conviction() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [1_000, 1_000],
            "market": ["KRW-BTC", "KRW-ETH"],
            "f1": [2.0, 0.3],
            "close": [100.0, 200.0],
        }
    )
    strategy = _build_strategy(
        groups=[(1_000, frame)],
        settings=ModelAlphaSettings(
            selection=ModelAlphaSelectionSettings(top_pct=1.0, min_prob=None, min_candidates_per_ts=1),
            position=ModelAlphaPositionSettings(
                max_positions_total=2,
                cooldown_bars=0,
                sizing_mode="prob_ramp",
                size_multiplier_min=0.5,
                size_multiplier_max=1.5,
            ),
        ),
        thresholds={"top_5pct": 0.5},
    )
    result = strategy.on_ts(
        ts_ms=1_000,
        active_markets=["KRW-BTC", "KRW-ETH"],
        latest_prices={"KRW-BTC": 100.0, "KRW-ETH": 200.0},
        open_markets=set(),
    )
    bid_intents = {intent.market: intent for intent in result.intents if intent.side == "bid"}
    assert set(bid_intents) == {"KRW-BTC", "KRW-ETH"}
    btc_multiplier = float((bid_intents["KRW-BTC"].meta or {}).get("notional_multiplier", 0.0))
    eth_multiplier = float((bid_intents["KRW-ETH"].meta or {}).get("notional_multiplier", 0.0))
    assert btc_multiplier > eth_multiplier
    assert 0.5 < eth_multiplier < 1.5
    assert eth_multiplier >= 0.5
    assert btc_multiplier <= 1.5


def test_model_alpha_trade_action_policy_applies_trade_level_risk_plan_and_sizing() -> None:
    frame0 = pl.DataFrame(
        {
            "ts_ms": [1_000],
            "market": ["KRW-BTC"],
            "f1": [4.0],
            "close": [100.0],
            "rv_12": [0.1],
            "rv_36": [0.1],
            "atr_pct_14": [0.01],
        }
    )
    frame1 = pl.DataFrame(
        {
            "ts_ms": [301_000],
            "market": ["KRW-BTC"],
            "f1": [4.0],
            "close": [101.5],
            "rv_12": [0.1],
            "rv_36": [0.1],
            "atr_pct_14": [0.01],
        }
    )
    runtime_recommendations = {
        "exit": {
            "recommended_exit_mode": "risk",
            "recommended_exit_mode_source": "execution_backtest_grid_search_compare",
            "recommended_exit_mode_reason_code": "RISK_EXECUTION_COMPARE_EDGE",
            "recommended_hold_bars": 3,
            "chosen_family": "risk",
            "chosen_rule_id": "risk_h3_rv_36_tp1p5_sl1p0_tr0p0",
            "hold_family_status": "supported",
            "risk_family_status": "supported",
            "family_compare_status": "supported",
            "family_compare": {"reason_codes": []},
            "summary": {"orders_filled": 5, "realized_pnl_quote": 10.0, "fill_rate": 0.9, "max_drawdown_pct": 1.0, "slippage_bps_mean": 2.0},
            "risk_summary": {"orders_filled": 5, "realized_pnl_quote": 12.0, "fill_rate": 0.92, "max_drawdown_pct": 0.8, "slippage_bps_mean": 2.1},
            "grid_point": {"hold_bars": 6},
            "risk_grid_point": {
                "hold_bars": 3,
                "risk_scaling_mode": "fixed",
                "risk_vol_feature": "rv_36",
                "tp_vol_multiplier": 1.5,
                "sl_vol_multiplier": 1.0,
                "trailing_vol_multiplier": 0.0,
            },
        },
        "trade_action": {
            "version": 1,
            "policy": TRADE_ACTION_POLICY_ID,
            "status": "ready",
            "risk_feature_name": "rv_36",
            "edge_bounds": [0.0, 0.5, 1.0],
            "risk_bounds": [0.0, 0.2, 1.0],
            "min_bin_samples": 1,
            "hold_policy_template": {
                "mode": "hold",
                "hold_bars": 6,
                "risk_scaling_mode": "fixed",
                "risk_vol_feature": "rv_36",
                "tp_pct": 0.0,
                "sl_pct": 0.02,
                "trailing_pct": 0.0,
            },
            "risk_policy_template": {
                "mode": "risk",
                "hold_bars": 3,
                "risk_scaling_mode": "fixed",
                "risk_vol_feature": "rv_36",
                "tp_pct": 0.01,
                "sl_pct": 0.02,
                "trailing_pct": 0.0,
            },
            "by_bin": [
                {
                    "edge_bin": 1,
                    "risk_bin": 0,
                    "sample_count": 5,
                    "comparable": True,
                    "recommended_action": "risk",
                    "recommended_notional_multiplier": 1.25,
                    "expected_edge": 0.01,
                    "expected_downside_deviation": 0.005,
                    "expected_es": 0.006,
                    "expected_ctm": 0.000036,
                    "expected_ctm_order": 2,
                    "expected_action_value": 1.0,
                    "expected_objective_score": 1.0,
                }
            ],
        }
    }
    strategy = _build_strategy(
        groups=[(1_000, frame0), (301_000, frame1)],
        settings=ModelAlphaSettings(
            selection=ModelAlphaSelectionSettings(top_pct=1.0, min_prob=0.0, min_candidates_per_ts=1),
            exit=ModelAlphaExitSettings(mode="hold", hold_bars=6, tp_pct=0.05, sl_pct=0.03),
        ),
        runtime_recommendations=runtime_recommendations,
    )

    first = strategy.on_ts(
        ts_ms=1_000,
        active_markets=["KRW-BTC"],
        latest_prices={"KRW-BTC": 100.0},
        open_markets=set(),
    )
    bid_intent = next(intent for intent in first.intents if intent.side == "bid")
    assert float((bid_intent.meta or {}).get("notional_multiplier", 0.0)) == 1.25
    assert str((bid_intent.meta or {}).get("notional_multiplier_source", "")) == "trade_action_policy"
    assert dict((bid_intent.meta or {}).get("trade_action") or {})["expected_es"] == 0.006
    assert dict((bid_intent.meta or {}).get("exit_recommendation") or {})["chosen_family"] == "risk"
    assert dict((bid_intent.meta or {}).get("exit_recommendation") or {})["chosen_rule_id"] == "risk_h3_rv_36_tp1p5_sl1p0_tr0p0"
    exit_plan = dict((bid_intent.meta or {}).get("model_exit_plan") or {})
    assert exit_plan["mode"] == "risk"
    assert exit_plan["hold_bars"] == 3
    assert exit_plan["tp_pct"] == 0.01

    from autobot.backtest.strategy_adapter import StrategyFillEvent

    strategy.on_fill(
        StrategyFillEvent(
            ts_ms=1_000,
            market="KRW-BTC",
            side="bid",
            price=100.0,
            volume=1.0,
            meta=dict(bid_intent.meta or {}),
        )
    )
    second = strategy.on_ts(
        ts_ms=301_000,
        active_markets=["KRW-BTC"],
        latest_prices={"KRW-BTC": 101.5},
        open_markets={"KRW-BTC"},
    )
    assert any(intent.reason_code == "MODEL_ALPHA_EXIT_TP" for intent in second.intents)


def test_model_alpha_risk_control_blocks_entry_before_intent_creation() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [1_000],
            "market": ["KRW-BTC"],
            "f1": [4.0],
            "close": [100.0],
            "rv_12": [0.1],
            "rv_36": [0.1],
            "atr_pct_14": [0.01],
        }
    )
    runtime_recommendations = {
        "trade_action": {
            "version": 1,
            "policy": TRADE_ACTION_POLICY_ID,
            "status": "ready",
            "risk_feature_name": "rv_36",
            "edge_bounds": [0.0, 0.5, 1.0],
            "risk_bounds": [0.0, 0.2, 1.0],
            "min_bin_samples": 1,
            "hold_policy_template": {"mode": "hold", "hold_bars": 6, "risk_vol_feature": "rv_36", "sl_pct": 0.02},
            "risk_policy_template": {"mode": "risk", "hold_bars": 3, "risk_vol_feature": "rv_36", "tp_pct": 0.01},
            "by_bin": [
                {
                    "edge_bin": 1,
                    "risk_bin": 0,
                    "sample_count": 5,
                    "comparable": True,
                    "recommended_action": "risk",
                    "recommended_notional_multiplier": 1.25,
                    "expected_edge": 0.01,
                    "expected_downside_deviation": 0.005,
                    "expected_action_value": 1.0,
                    "expected_objective_score": 1.0,
                }
            ],
        },
        "risk_control": {
            "version": 1,
            "policy": "execution_risk_control_hoeffding_v1",
            "status": "ready",
            "decision_metric_name": "expected_action_value",
            "selected_threshold": 2.0,
            "selected_coverage": 31,
            "selected_nonpositive_rate_ucb": 0.18,
            "selected_severe_loss_rate_ucb": 0.11,
            "live_gate": {
                "enabled": True,
                "metric_name": "expected_action_value",
                "threshold": 2.0,
                "skip_reason_code": "RISK_CONTROL_BELOW_THRESHOLD",
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
    }
    strategy = _build_strategy(
        groups=[(1_000, frame)],
        settings=ModelAlphaSettings(
            selection=ModelAlphaSelectionSettings(top_pct=1.0, min_prob=0.0, min_candidates_per_ts=1),
        ),
        runtime_recommendations=runtime_recommendations,
    )

    result = strategy.on_ts(
        ts_ms=1_000,
        active_markets=["KRW-BTC"],
        latest_prices={"KRW-BTC": 100.0},
        open_markets=set(),
    )

    assert result.intents == ()
    assert result.skipped_reasons["RISK_CONTROL_BELOW_THRESHOLD"] == 1


def test_model_alpha_exit_timeout_still_fires_for_position_outside_active_universe() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [1_000],
            "market": ["KRW-BTC"],
            "f1": [3.0],
            "close": [100.0],
        }
    )
    strategy = _build_strategy(
        groups=[(1_000, frame)],
        settings=ModelAlphaSettings(
            selection=ModelAlphaSelectionSettings(top_pct=1.0, min_prob=0.0, min_candidates_per_ts=1),
            exit=ModelAlphaExitSettings(mode="hold", hold_bars=1),
        ),
    )

    first = strategy.on_ts(
        ts_ms=1_000,
        active_markets=["KRW-BTC"],
        latest_prices={"KRW-BTC": 100.0},
        open_markets=set(),
    )
    bid_intent = next(intent for intent in first.intents if intent.side == "bid")

    from autobot.backtest.strategy_adapter import StrategyFillEvent

    strategy.on_fill(
        StrategyFillEvent(
            ts_ms=1_000,
            market="KRW-BTC",
            side="bid",
            price=100.0,
            volume=1.0,
            meta=dict(bid_intent.meta or {}),
        )
    )
    second = strategy.on_ts(
        ts_ms=301_000,
        active_markets=[],
        latest_prices={"KRW-BTC": 100.0},
        open_markets=set(),
    )

    assert any(intent.reason_code == "MODEL_ALPHA_EXIT_HOLD_TIMEOUT" for intent in second.intents)


def test_model_alpha_counts_tracked_positions_outside_active_universe_for_position_limit() -> None:
    frame_entry = pl.DataFrame(
        {
            "ts_ms": [1_000],
            "market": ["KRW-BTC"],
            "f1": [3.0],
            "close": [100.0],
        }
    )
    frame_next = pl.DataFrame(
        {
            "ts_ms": [301_000],
            "market": ["KRW-ETH"],
            "f1": [3.0],
            "close": [200.0],
        }
    )
    strategy = _build_strategy(
        groups=[(1_000, frame_entry), (301_000, frame_next)],
        settings=ModelAlphaSettings(
            selection=ModelAlphaSelectionSettings(top_pct=1.0, min_prob=0.0, min_candidates_per_ts=1),
            position=ModelAlphaPositionSettings(max_positions_total=1, cooldown_bars=0),
            exit=ModelAlphaExitSettings(mode="hold", hold_bars=6),
        ),
    )

    first = strategy.on_ts(
        ts_ms=1_000,
        active_markets=["KRW-BTC"],
        latest_prices={"KRW-BTC": 100.0},
        open_markets=set(),
    )
    bid_intent = next(intent for intent in first.intents if intent.side == "bid")

    from autobot.backtest.strategy_adapter import StrategyFillEvent

    strategy.on_fill(
        StrategyFillEvent(
            ts_ms=1_000,
            market="KRW-BTC",
            side="bid",
            price=100.0,
            volume=1.0,
            meta=dict(bid_intent.meta or {}),
        )
    )
    second = strategy.on_ts(
        ts_ms=301_000,
        active_markets=["KRW-ETH"],
        latest_prices={"KRW-BTC": 100.0, "KRW-ETH": 200.0},
        open_markets=set(),
    )

    assert not any(intent.side == "bid" and intent.market == "KRW-ETH" for intent in second.intents)
    assert second.skipped_reasons["MAX_POSITIONS_TOTAL"] == 1


def test_model_alpha_risk_control_size_ladder_clamps_multiplier_in_shared_strategy() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [1_000],
            "market": ["KRW-BTC"],
            "f1": [4.0],
            "close": [100.0],
            "rv_36": [0.1],
        }
    )
    runtime_recommendations = {
        "trade_action": {
            "version": 1,
            "policy": TRADE_ACTION_POLICY_ID,
            "status": "ready",
            "risk_feature_name": "rv_36",
            "edge_bounds": [0.0, 0.5, 1.0],
            "risk_bounds": [0.0, 0.2, 1.0],
            "min_bin_samples": 1,
            "hold_policy_template": {"mode": "hold", "hold_bars": 6, "risk_vol_feature": "rv_36", "sl_pct": 0.02},
            "risk_policy_template": {"mode": "risk", "hold_bars": 3, "risk_vol_feature": "rv_36", "tp_pct": 0.01},
            "by_bin": [
                {
                    "edge_bin": 1,
                    "risk_bin": 0,
                    "sample_count": 5,
                    "comparable": True,
                    "recommended_action": "risk",
                    "recommended_notional_multiplier": 1.25,
                    "expected_edge": 0.01,
                    "expected_downside_deviation": 0.005,
                    "expected_action_value": 1.0,
                    "expected_objective_score": 1.0,
                }
            ],
        },
        "risk_control": {
            "version": 1,
            "policy": "execution_risk_control_hoeffding_v1",
            "status": "ready",
            "decision_metric_name": "expected_action_value",
            "selected_threshold": 0.5,
            "selected_coverage": 31,
            "selected_nonpositive_rate_ucb": 0.18,
            "selected_severe_loss_rate_ucb": 0.11,
            "live_gate": {
                "enabled": True,
                "metric_name": "expected_action_value",
                "threshold": 0.5,
                "skip_reason_code": "RISK_CONTROL_BELOW_THRESHOLD",
            },
            "subgroup_family": {
                "enabled": False,
                "feature_name": "",
                "bucket_count_requested": 0,
                "bucket_count_effective": 0,
                "bounds": [],
                "min_coverage": 0,
            },
            "size_ladder": {
                "enabled": True,
                "status": "ready",
                "feature_name": "",
                "global_max_multiplier": 0.75,
                "group_limits": [],
            },
        },
    }
    strategy = _build_strategy(
        groups=[(1_000, frame)],
        settings=ModelAlphaSettings(
            selection=ModelAlphaSelectionSettings(top_pct=1.0, min_prob=0.0, min_candidates_per_ts=1),
        ),
        runtime_recommendations=runtime_recommendations,
    )

    result = strategy.on_ts(
        ts_ms=1_000,
        active_markets=["KRW-BTC"],
        latest_prices={"KRW-BTC": 100.0},
        open_markets=set(),
    )

    bid_intent = next(intent for intent in result.intents if intent.side == "bid")
    assert float((bid_intent.meta or {}).get("notional_multiplier", 0.0)) == 0.75
    assert str((bid_intent.meta or {}).get("notional_multiplier_source", "")) == "risk_control_size_ladder"
    assert dict((bid_intent.meta or {}).get("risk_control_static") or {})["allowed"] is True
    assert float(dict((bid_intent.meta or {}).get("size_ladder_static") or {})["resolved_multiplier"]) == 0.75


def test_model_alpha_trade_action_fallback_bin_applies_size_haircut() -> None:
    policy = build_trade_action_policy_from_oos_rows(
        oos_rows=[
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
        ],
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
    frame = pl.DataFrame(
        {
            "ts_ms": [1_000],
            "market": ["KRW-BTC"],
            "f1": [4.0],
            "close": [100.0],
            "rv_36": [0.50],
        }
    )
    strategy = _build_strategy(
        groups=[(1_000, frame)],
        settings=ModelAlphaSettings(
            selection=ModelAlphaSelectionSettings(top_pct=1.0, min_prob=0.0, min_candidates_per_ts=1),
        ),
        runtime_recommendations={"trade_action": policy},
    )

    result = strategy.on_ts(
        ts_ms=1_000,
        active_markets=["KRW-BTC"],
        latest_prices={"KRW-BTC": 100.0},
        open_markets=set(),
    )

    bid_intent = next(intent for intent in result.intents if intent.side == "bid")
    meta = dict(bid_intent.meta or {})
    assert meta["support_level"] == "fallback_bin"
    assert float(meta["support_size_multiplier"]) == 0.5
    assert bool(meta["support_size_haircut_applied"]) is True


def test_model_alpha_trade_action_policy_uses_volatility_alias_columns() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [1_000],
            "market": ["KRW-BTC"],
            "f1": [4.0],
            "close": [100.0],
            "vol_36": [0.1],
        }
    )
    runtime_recommendations = {
        "trade_action": {
            "version": 1,
            "policy": TRADE_ACTION_POLICY_ID,
            "status": "ready",
            "risk_feature_name": "rv_36",
            "edge_bounds": [0.0, 0.5, 1.0],
            "risk_bounds": [0.0, 0.2, 1.0],
            "min_bin_samples": 1,
            "hold_policy_template": {"mode": "hold", "hold_bars": 6, "risk_vol_feature": "rv_36", "sl_pct": 0.02},
            "risk_policy_template": {"mode": "risk", "hold_bars": 3, "risk_vol_feature": "rv_36", "tp_pct": 0.01},
            "by_bin": [
                {
                    "edge_bin": 1,
                    "risk_bin": 0,
                    "sample_count": 5,
                    "comparable": True,
                    "recommended_action": "risk",
                    "recommended_notional_multiplier": 1.25,
                    "expected_edge": 0.01,
                    "expected_downside_deviation": 0.005,
                    "expected_objective_score": 1.0,
                }
            ],
        }
    }
    strategy = _build_strategy(
        groups=[(1_000, frame)],
        settings=ModelAlphaSettings(
            selection=ModelAlphaSelectionSettings(top_pct=1.0, min_prob=0.0, min_candidates_per_ts=1),
            exit=ModelAlphaExitSettings(mode="hold", hold_bars=6, tp_pct=0.05, sl_pct=0.03),
        ),
        runtime_recommendations=runtime_recommendations,
    )
    result = strategy.on_ts(
        ts_ms=1_000,
        active_markets=["KRW-BTC"],
        latest_prices={"KRW-BTC": 100.0},
        open_markets=set(),
    )
    bid_intent = next(intent for intent in result.intents if intent.side == "bid")
    assert dict((bid_intent.meta or {}).get("trade_action") or {})["recommended_action"] == "risk"
    assert str((bid_intent.meta or {}).get("notional_multiplier_source", "")) == "trade_action_policy"


def test_model_alpha_execution_frontier_selects_join_stage_when_passive_net_edge_is_negative() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [1_000],
            "market": ["KRW-BTC"],
            "f1": [5.0],
            "close": [100.0],
            "rv_12": [0.10],
            "rv_36": [0.12],
            "atr_pct_14": [0.01],
        }
    )
    strategy = _build_strategy(
        groups=[(1_000, frame)],
        settings=ModelAlphaSettings(
            selection=ModelAlphaSelectionSettings(top_pct=1.0, min_prob=0.0, min_candidates_per_ts=1),
            execution=ModelAlphaExecutionSettings(price_mode="JOIN", timeout_bars=3, replace_max=2),
        ),
        runtime_recommendations={
            "trade_action": {
                "version": 1,
                "policy": TRADE_ACTION_POLICY_ID,
                "status": "ready",
                "risk_feature_name": "rv_12",
                "edge_bounds": [0.0, 1.0],
                "risk_bounds": [0.0, 1.0],
                "min_bin_samples": 1,
                "runtime_decision_source": "bin_audit_fallback",
                "hold_policy_template": {"mode": "hold", "hold_bars": 6},
                "risk_policy_template": {"mode": "risk", "hold_bars": 6, "risk_vol_feature": "rv_12"},
                "by_bin": [
                    {
                        "edge_bin": 0,
                        "risk_bin": 0,
                        "comparable": True,
                        "sample_count": 10,
                        "recommended_action": "risk",
                        "recommended_notional_multiplier": 1.0,
                        "expected_edge": 0.0003,
                        "expected_downside_deviation": 0.0001,
                        "expected_action_value": 1.0,
                        "expected_objective_score": 1.0,
                        "expected_es": 0.0001,
                        "expected_ctm": 0.0,
                        "expected_tail_probability": 0.05,
                    }
                ],
            },
            "execution": {
                "policy": "empirical_fill_frontier_v1",
                "stage_decision_mode": "sequential_positive_net_edge_v1",
                "stage_order": ["PASSIVE_MAKER", "JOIN", "CROSS_1T"],
                "no_trade_region": {"positive_net_edge_required": True},
                "stages": [
                    {
                        "stage": "PASSIVE_MAKER",
                        "supported": True,
                        "recommended_price_mode": "PASSIVE_MAKER",
                        "recommended_timeout_bars": 4,
                        "recommended_replace_max": 1,
                        "expected_fill_probability": 0.20,
                        "expected_slippage_bps": 1.0,
                    },
                    {
                        "stage": "JOIN",
                        "supported": True,
                        "recommended_price_mode": "JOIN",
                        "recommended_timeout_bars": 2,
                        "recommended_replace_max": 1,
                        "expected_fill_probability": 0.95,
                        "expected_slippage_bps": 2.0,
                    },
                    {
                        "stage": "CROSS_1T",
                        "supported": True,
                        "recommended_price_mode": "CROSS_1T",
                        "recommended_timeout_bars": 1,
                        "recommended_replace_max": 0,
                        "expected_fill_probability": 0.99,
                        "expected_slippage_bps": 5.0,
                    },
                ],
            },
        },
    )

    result = strategy.on_ts(
        ts_ms=1_000,
        active_markets=["KRW-BTC"],
        latest_prices={"KRW-BTC": 100.0},
        open_markets=set(),
    )

    bid_intent = next(intent for intent in result.intents if intent.side == "bid")
    exec_profile = dict((bid_intent.meta or {}).get("exec_profile") or {})
    execution_decision = dict((bid_intent.meta or {}).get("execution_decision") or {})
    assert exec_profile["price_mode"] == "JOIN"
    assert int(exec_profile["timeout_ms"]) == 600_000
    assert execution_decision["selected_stage"] == "JOIN"
    assert execution_decision["reason_code"] == "EXECUTION_STAGE_JOIN"


def test_model_alpha_execution_frontier_blocks_no_trade_region_when_all_stages_are_negative() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [1_000],
            "market": ["KRW-BTC"],
            "f1": [5.0],
            "close": [100.0],
            "rv_12": [0.10],
            "rv_36": [0.12],
            "atr_pct_14": [0.01],
        }
    )
    strategy = _build_strategy(
        groups=[(1_000, frame)],
        settings=ModelAlphaSettings(
            selection=ModelAlphaSelectionSettings(top_pct=1.0, min_prob=0.0, min_candidates_per_ts=1),
        ),
        runtime_recommendations={
            "trade_action": {
                "version": 1,
                "policy": TRADE_ACTION_POLICY_ID,
                "status": "ready",
                "risk_feature_name": "rv_12",
                "edge_bounds": [0.0, 1.0],
                "risk_bounds": [0.0, 1.0],
                "min_bin_samples": 1,
                "runtime_decision_source": "bin_audit_fallback",
                "hold_policy_template": {"mode": "hold", "hold_bars": 6},
                "risk_policy_template": {"mode": "risk", "hold_bars": 6, "risk_vol_feature": "rv_12"},
                "by_bin": [
                    {
                        "edge_bin": 0,
                        "risk_bin": 0,
                        "comparable": True,
                        "sample_count": 10,
                        "recommended_action": "risk",
                        "recommended_notional_multiplier": 1.0,
                        "expected_edge": 0.00005,
                        "expected_downside_deviation": 0.0001,
                        "expected_action_value": 1.0,
                        "expected_objective_score": 1.0,
                        "expected_es": 0.0001,
                        "expected_ctm": 0.0,
                        "expected_tail_probability": 0.05,
                    }
                ],
            },
            "execution": {
                "policy": "empirical_fill_frontier_v1",
                "stage_decision_mode": "sequential_positive_net_edge_v1",
                "stage_order": ["PASSIVE_MAKER", "JOIN", "CROSS_1T"],
                "no_trade_region": {"positive_net_edge_required": True},
                "stages": [
                    {
                        "stage": "PASSIVE_MAKER",
                        "supported": True,
                        "recommended_price_mode": "PASSIVE_MAKER",
                        "recommended_timeout_bars": 4,
                        "recommended_replace_max": 1,
                        "expected_fill_probability": 0.20,
                        "expected_slippage_bps": 1.0,
                    },
                    {
                        "stage": "JOIN",
                        "supported": True,
                        "recommended_price_mode": "JOIN",
                        "recommended_timeout_bars": 2,
                        "recommended_replace_max": 1,
                        "expected_fill_probability": 0.95,
                        "expected_slippage_bps": 2.0,
                    },
                ],
            },
        },
    )

    result = strategy.on_ts(
        ts_ms=1_000,
        active_markets=["KRW-BTC"],
        latest_prices={"KRW-BTC": 100.0},
        open_markets=set(),
    )

    assert not any(intent.side == "bid" for intent in result.intents)
    assert result.skipped_reasons["EXECUTION_NO_TRADE_REGION"] == 1


def test_model_alpha_cooldown_and_hold_exit() -> None:
    frame0 = pl.DataFrame({"ts_ms": [1_000], "market": ["KRW-BTC"], "f1": [5.0], "close": [100.0]})
    frame1 = pl.DataFrame({"ts_ms": [301_000], "market": ["KRW-BTC"], "f1": [5.0], "close": [102.0]})
    frame2 = pl.DataFrame({"ts_ms": [601_000], "market": ["KRW-BTC"], "f1": [5.0], "close": [103.0]})
    strategy = _build_strategy(
        groups=[(1_000, frame0), (301_000, frame1), (601_000, frame2)],
        settings=ModelAlphaSettings(
            selection=ModelAlphaSelectionSettings(top_pct=1.0, min_prob=0.0, min_candidates_per_ts=1),
            position=ModelAlphaPositionSettings(max_positions_total=1, cooldown_bars=2),
            exit=ModelAlphaExitSettings(mode="hold", hold_bars=1),
        ),
    )

    first = strategy.on_ts(
        ts_ms=1_000,
        active_markets=["KRW-BTC"],
        latest_prices={"KRW-BTC": 100.0},
        open_markets=set(),
    )
    assert any(intent.side == "bid" for intent in first.intents)

    from autobot.backtest.strategy_adapter import StrategyFillEvent

    strategy.on_fill(StrategyFillEvent(ts_ms=301_000, market="KRW-BTC", side="bid", price=101.0, volume=1.0))
    second = strategy.on_ts(
        ts_ms=301_000,
        active_markets=["KRW-BTC"],
        latest_prices={"KRW-BTC": 102.0},
        open_markets={"KRW-BTC"},
    )
    assert not any(intent.side == "ask" for intent in second.intents)

    third = strategy.on_ts(
        ts_ms=601_000,
        active_markets=["KRW-BTC"],
        latest_prices={"KRW-BTC": 103.0},
        open_markets={"KRW-BTC"},
    )
    assert any(intent.reason_code == "MODEL_ALPHA_EXIT_HOLD_TIMEOUT" for intent in third.intents)

    strategy.on_fill(StrategyFillEvent(ts_ms=601_000, market="KRW-BTC", side="ask", price=103.0, volume=1.0))
    fourth = strategy.on_ts(
        ts_ms=601_000,
        active_markets=["KRW-BTC"],
        latest_prices={"KRW-BTC": 103.0},
        open_markets=set(),
    )
    assert not any(intent.side == "bid" for intent in fourth.intents)


def test_model_alpha_hold_exit_uses_sl_guard_before_timeout() -> None:
    strategy = _build_strategy(
        groups=[],
        settings=ModelAlphaSettings(
            selection=ModelAlphaSelectionSettings(top_pct=1.0, min_prob=0.0, min_candidates_per_ts=1),
            exit=ModelAlphaExitSettings(mode="hold", hold_bars=6, sl_pct=0.01),
        ),
    )
    from autobot.backtest.strategy_adapter import StrategyFillEvent

    strategy.on_fill(StrategyFillEvent(ts_ms=1_000, market="KRW-BTC", side="bid", price=100.0, volume=1.0))
    result = strategy.on_ts(
        ts_ms=301_000,
        active_markets=["KRW-BTC"],
        latest_prices={"KRW-BTC": 98.7},
        open_markets={"KRW-BTC"},
    )
    assert any(intent.reason_code == "MODEL_ALPHA_EXIT_SL" for intent in result.intents)
    assert not any(intent.reason_code == "MODEL_ALPHA_EXIT_HOLD_TIMEOUT" for intent in result.intents)


def test_build_model_alpha_exit_plan_payload_keeps_sl_guard_for_hold_mode() -> None:
    payload = build_model_alpha_exit_plan_payload(
        settings=ModelAlphaSettings(
            exit=ModelAlphaExitSettings(
                mode="hold",
                hold_bars=6,
                tp_pct=0.02,
                sl_pct=0.01,
                trailing_pct=0.015,
            )
        ),
        row=None,
        interval_ms=300_000,
    )

    assert payload["mode"] == "hold"
    assert payload["tp_pct"] == 0.0
    assert payload["sl_pct"] == 0.01
    assert payload["trailing_pct"] == 0.0


def test_model_alpha_risk_exit_tp() -> None:
    strategy = _build_strategy(
        groups=[],
        settings=ModelAlphaSettings(
            selection=ModelAlphaSelectionSettings(top_pct=1.0, min_prob=0.0, min_candidates_per_ts=1),
            exit=ModelAlphaExitSettings(mode="risk", hold_bars=0, tp_pct=0.01, sl_pct=0.2, trailing_pct=0.0),
        ),
    )
    from autobot.backtest.strategy_adapter import StrategyFillEvent

    strategy.on_fill(StrategyFillEvent(ts_ms=1_000, market="KRW-BTC", side="bid", price=100.0, volume=1.0))
    result = strategy.on_ts(
        ts_ms=301_000,
        active_markets=["KRW-BTC"],
        latest_prices={"KRW-BTC": 102.0},
        open_markets={"KRW-BTC"},
    )
    assert any(intent.reason_code == "MODEL_ALPHA_EXIT_TP" for intent in result.intents)


def test_model_alpha_risk_exit_tp_uses_net_return_after_fees() -> None:
    strategy = _build_strategy(
        groups=[],
        settings=ModelAlphaSettings(
            selection=ModelAlphaSelectionSettings(top_pct=1.0, min_prob=0.0, min_candidates_per_ts=1),
            exit=ModelAlphaExitSettings(mode="risk", hold_bars=0, tp_pct=0.001, sl_pct=0.2, trailing_pct=0.0),
        ),
    )
    from autobot.backtest.strategy_adapter import StrategyFillEvent

    strategy.on_fill(
        StrategyFillEvent(
            ts_ms=1_000,
            market="KRW-BTC",
            side="bid",
            price=100.0,
            volume=1.0,
            fee_quote=0.1,
        )
    )
    too_small = strategy.on_ts(
        ts_ms=301_000,
        active_markets=["KRW-BTC"],
        latest_prices={"KRW-BTC": 100.15},
        open_markets={"KRW-BTC"},
    )
    assert not any(intent.reason_code == "MODEL_ALPHA_EXIT_TP" for intent in too_small.intents)

    enough = strategy.on_ts(
        ts_ms=302_000,
        active_markets=["KRW-BTC"],
        latest_prices={"KRW-BTC": 100.35},
        open_markets={"KRW-BTC"},
    )
    assert any(intent.reason_code == "MODEL_ALPHA_EXIT_TP" for intent in enough.intents)


def test_model_alpha_risk_exit_respects_expected_exit_cost_overrides() -> None:
    strategy = _build_strategy(
        groups=[],
        settings=ModelAlphaSettings(
            selection=ModelAlphaSelectionSettings(top_pct=1.0, min_prob=0.0, min_candidates_per_ts=1),
            exit=ModelAlphaExitSettings(
                mode="risk",
                hold_bars=0,
                tp_pct=0.001,
                sl_pct=0.2,
                trailing_pct=0.0,
                expected_exit_slippage_bps=20.0,
                expected_exit_fee_bps=10.0,
            ),
        ),
    )
    from autobot.backtest.strategy_adapter import StrategyFillEvent

    strategy.on_fill(
        StrategyFillEvent(
            ts_ms=1_000,
            market="KRW-BTC",
            side="bid",
            price=100.0,
            volume=1.0,
            fee_quote=0.0,
        )
    )
    blocked = strategy.on_ts(
        ts_ms=301_000,
        active_markets=["KRW-BTC"],
        latest_prices={"KRW-BTC": 100.35},
        open_markets={"KRW-BTC"},
    )
    assert not any(intent.reason_code == "MODEL_ALPHA_EXIT_TP" for intent in blocked.intents)

    enough = strategy.on_ts(
        ts_ms=302_000,
        active_markets=["KRW-BTC"],
        latest_prices={"KRW-BTC": 100.60},
        open_markets={"KRW-BTC"},
    )
    assert any(intent.reason_code == "MODEL_ALPHA_EXIT_TP" for intent in enough.intents)


def test_model_alpha_risk_exit_uses_volatility_scaled_thresholds() -> None:
    frame1 = pl.DataFrame(
        {
            "ts_ms": [301_000],
            "market": ["KRW-BTC"],
            "f1": [0.0],
            "close": [103.0],
            "rv_12": [0.01],
        }
    )
    frame2 = pl.DataFrame(
        {
            "ts_ms": [601_000],
            "market": ["KRW-BTC"],
            "f1": [0.0],
            "close": [105.0],
            "rv_12": [0.01],
        }
    )
    strategy = _build_strategy(
        groups=[(301_000, frame1), (601_000, frame2)],
        settings=ModelAlphaSettings(
            selection=ModelAlphaSelectionSettings(top_pct=1.0, min_prob=0.0, min_candidates_per_ts=1),
            exit=ModelAlphaExitSettings(
                mode="risk",
                hold_bars=4,
                risk_scaling_mode="volatility_scaled",
                risk_vol_feature="rv_12",
                tp_vol_multiplier=2.0,
                sl_vol_multiplier=1.0,
                trailing_vol_multiplier=0.0,
                tp_pct=0.01,
                sl_pct=0.01,
                trailing_pct=0.0,
            ),
        ),
    )
    from autobot.backtest.strategy_adapter import StrategyFillEvent

    strategy.on_fill(StrategyFillEvent(ts_ms=1_000, market="KRW-BTC", side="bid", price=100.0, volume=1.0))

    blocked = strategy.on_ts(
        ts_ms=301_000,
        active_markets=["KRW-BTC"],
        latest_prices={"KRW-BTC": 103.0},
        open_markets={"KRW-BTC"},
    )
    assert not any(intent.reason_code == "MODEL_ALPHA_EXIT_TP" for intent in blocked.intents)

    enough = strategy.on_ts(
        ts_ms=601_000,
        active_markets=["KRW-BTC"],
        latest_prices={"KRW-BTC": 105.0},
        open_markets={"KRW-BTC"},
    )
    assert any(intent.reason_code == "MODEL_ALPHA_EXIT_TP" for intent in enough.intents)


def test_model_alpha_restart_fill_restores_peak_price_from_exit_plan() -> None:
    strategy = _build_strategy(
        groups=[],
        settings=ModelAlphaSettings(
            selection=ModelAlphaSelectionSettings(top_pct=1.0, min_prob=0.0, min_candidates_per_ts=1),
            exit=ModelAlphaExitSettings(mode="risk", hold_bars=6, tp_pct=0.2, sl_pct=0.2, trailing_pct=0.05),
        ),
    )
    from autobot.backtest.strategy_adapter import StrategyFillEvent

    strategy.on_fill(
        StrategyFillEvent(
            ts_ms=1_000,
            market="KRW-BTC",
            side="bid",
            price=100.0,
            volume=1.0,
            meta={
                "model_exit_plan": {
                    "source": "model_alpha_v1",
                    "mode": "risk",
                    "hold_bars": 6,
                    "interval_ms": 300_000,
                    "timeout_delta_ms": 1_800_000,
                    "tp_pct": 0.20,
                    "sl_pct": 0.20,
                    "trailing_pct": 0.05,
                    "high_watermark_price": 110.0,
                    "high_watermark_price_str": "110",
                }
            },
        )
    )
    result = strategy.on_ts(
        ts_ms=301_000,
        active_markets=["KRW-BTC"],
        latest_prices={"KRW-BTC": 104.0},
        open_markets={"KRW-BTC"},
    )

    assert any(intent.reason_code == "MODEL_ALPHA_EXIT_TRAILING" for intent in result.intents)


def test_model_alpha_risk_exit_applies_common_micro_overlay_from_row_features() -> None:
    frame1 = pl.DataFrame(
        {
            "ts_ms": [301_000],
            "market": ["KRW-BTC"],
            "f1": [0.0],
            "close": [104.0],
            "m_trade_events": [32.0],
            "m_book_events": [24.0],
            "m_trade_coverage_ms": [5_000.0],
            "m_book_coverage_ms": [5_000.0],
            "m_trade_max_ts_ms": [301_000.0],
            "m_book_max_ts_ms": [301_000.0],
            "m_trade_imbalance": [-0.9],
            "m_spread_proxy": [45.0],
            "m_depth_bid_top5_mean": [20_000.0],
            "m_depth_ask_top5_mean": [20_000.0],
            "m_micro_available": [1.0],
            "m_micro_book_available": [1.0],
        }
    )
    frame2 = pl.DataFrame(
        {
            "ts_ms": [601_000],
            "market": ["KRW-BTC"],
            "f1": [0.0],
            "close": [102.8],
            "m_trade_events": [32.0],
            "m_book_events": [24.0],
            "m_trade_coverage_ms": [5_000.0],
            "m_book_coverage_ms": [5_000.0],
            "m_trade_max_ts_ms": [601_000.0],
            "m_book_max_ts_ms": [601_000.0],
            "m_trade_imbalance": [-0.9],
            "m_spread_proxy": [45.0],
            "m_depth_bid_top5_mean": [20_000.0],
            "m_depth_ask_top5_mean": [20_000.0],
            "m_micro_available": [1.0],
            "m_micro_book_available": [1.0],
        }
    )
    strategy = _build_strategy(
        groups=[(301_000, frame1), (601_000, frame2)],
        settings=ModelAlphaSettings(
            selection=ModelAlphaSelectionSettings(top_pct=1.0, min_prob=0.0, min_candidates_per_ts=1),
            exit=ModelAlphaExitSettings(
                mode="risk",
                hold_bars=6,
                risk_scaling_mode="fixed",
                tp_pct=0.20,
                sl_pct=0.20,
                trailing_pct=0.0,
            ),
        ),
    )
    from autobot.backtest.strategy_adapter import StrategyFillEvent

    strategy.on_fill(
        StrategyFillEvent(
            ts_ms=1_000,
            market="KRW-BTC",
            side="bid",
            price=100.0,
            volume=1.0,
        )
    )
    first = strategy.on_ts(
        ts_ms=301_000,
        active_markets=["KRW-BTC"],
        latest_prices={"KRW-BTC": 104.0},
        open_markets={"KRW-BTC"},
    )
    second = strategy.on_ts(
        ts_ms=601_000,
        active_markets=["KRW-BTC"],
        latest_prices={"KRW-BTC": 102.8},
        open_markets={"KRW-BTC"},
    )

    assert not any(intent.reason_code == "MODEL_ALPHA_EXIT_TRAILING" for intent in first.intents)
    assert any(intent.reason_code == "MODEL_ALPHA_EXIT_TRAILING" for intent in second.intents)


def test_model_alpha_risk_exit_reprices_with_current_volatility_and_remaining_horizon() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [301_000],
            "market": ["KRW-BTC"],
            "f1": [0.0],
            "close": [104.0],
            "rv_12": [0.01],
        }
    )
    settings = ModelAlphaSettings(
        selection=ModelAlphaSelectionSettings(top_pct=1.0, min_prob=0.0, min_candidates_per_ts=1),
        exit=ModelAlphaExitSettings(
            mode="risk",
            hold_bars=4,
            risk_scaling_mode="volatility_scaled",
            risk_vol_feature="rv_12",
            tp_vol_multiplier=2.0,
            sl_vol_multiplier=1.0,
            trailing_vol_multiplier=0.0,
            tp_pct=0.01,
            sl_pct=0.01,
            trailing_pct=0.0,
        ),
    )
    strategy = _build_strategy(groups=[(301_000, frame)], settings=settings)
    entry_plan = build_model_alpha_exit_plan_payload(
        settings=settings,
        row={"rv_12": 0.02, "close": 100.0},
        interval_ms=300_000,
    )

    from autobot.backtest.strategy_adapter import StrategyFillEvent

    strategy.on_fill(
        StrategyFillEvent(
            ts_ms=1_000,
            market="KRW-BTC",
            side="bid",
            price=100.0,
            volume=1.0,
            meta={"model_exit_plan": entry_plan},
        )
    )

    result = strategy.on_ts(
        ts_ms=301_000,
        active_markets=["KRW-BTC"],
        latest_prices={"KRW-BTC": 104.0},
        open_markets={"KRW-BTC"},
    )

    assert any(intent.reason_code == "MODEL_ALPHA_EXIT_TP" for intent in result.intents)


def test_model_alpha_risk_exit_repricing_never_widens_entry_thresholds() -> None:
    frame = pl.DataFrame(
        {
            "ts_ms": [301_000],
            "market": ["KRW-BTC"],
            "f1": [0.0],
            "close": [104.5],
            "rv_12": [0.03],
        }
    )
    settings = ModelAlphaSettings(
        selection=ModelAlphaSelectionSettings(top_pct=1.0, min_prob=0.0, min_candidates_per_ts=1),
        exit=ModelAlphaExitSettings(
            mode="risk",
            hold_bars=4,
            risk_scaling_mode="volatility_scaled",
            risk_vol_feature="rv_12",
            tp_vol_multiplier=2.0,
            sl_vol_multiplier=1.0,
            trailing_vol_multiplier=0.0,
            tp_pct=0.01,
            sl_pct=0.01,
            trailing_pct=0.0,
        ),
    )
    strategy = _build_strategy(groups=[(301_000, frame)], settings=settings)
    entry_plan = build_model_alpha_exit_plan_payload(
        settings=settings,
        row={"rv_12": 0.01, "close": 100.0},
        interval_ms=300_000,
    )

    from autobot.backtest.strategy_adapter import StrategyFillEvent

    strategy.on_fill(
        StrategyFillEvent(
            ts_ms=1_000,
            market="KRW-BTC",
            side="bid",
            price=100.0,
            volume=1.0,
            meta={"model_exit_plan": entry_plan},
        )
    )

    result = strategy.on_ts(
        ts_ms=301_000,
        active_markets=["KRW-BTC"],
        latest_prices={"KRW-BTC": 104.5},
        open_markets={"KRW-BTC"},
    )

    assert any(intent.reason_code == "MODEL_ALPHA_EXIT_TP" for intent in result.intents)


def test_model_alpha_risk_exit_supports_atr_pct_volatility_feature() -> None:
    frame1 = pl.DataFrame(
        {
            "ts_ms": [301_000],
            "market": ["KRW-BTC"],
            "f1": [0.0],
            "close": [97.0],
            "atr_14": [4.0],
        }
    )
    frame2 = pl.DataFrame(
        {
            "ts_ms": [601_000],
            "market": ["KRW-BTC"],
            "f1": [0.0],
            "close": [95.0],
            "atr_14": [4.0],
        }
    )
    strategy = _build_strategy(
        groups=[(301_000, frame1), (601_000, frame2)],
        settings=ModelAlphaSettings(
            selection=ModelAlphaSelectionSettings(top_pct=1.0, min_prob=0.0, min_candidates_per_ts=1),
            exit=ModelAlphaExitSettings(
                mode="risk",
                hold_bars=1,
                risk_scaling_mode="volatility_scaled",
                risk_vol_feature="atr_pct_14",
                tp_vol_multiplier=3.0,
                sl_vol_multiplier=1.0,
                trailing_vol_multiplier=0.0,
                tp_pct=0.01,
                sl_pct=0.01,
                trailing_pct=0.0,
            ),
        ),
    )
    from autobot.backtest.strategy_adapter import StrategyFillEvent

    strategy.on_fill(StrategyFillEvent(ts_ms=1_000, market="KRW-BTC", side="bid", price=100.0, volume=1.0))

    blocked = strategy.on_ts(
        ts_ms=301_000,
        active_markets=["KRW-BTC"],
        latest_prices={"KRW-BTC": 97.0},
        open_markets={"KRW-BTC"},
    )
    assert not any(intent.reason_code == "MODEL_ALPHA_EXIT_SL" for intent in blocked.intents)

    enough = strategy.on_ts(
        ts_ms=601_000,
        active_markets=["KRW-BTC"],
        latest_prices={"KRW-BTC": 95.0},
        open_markets={"KRW-BTC"},
    )
    assert any(intent.reason_code == "MODEL_ALPHA_EXIT_SL" for intent in enough.intents)


def test_backtest_model_alpha_run_generates_artifacts(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    dataset_root = tmp_path / "features_v3"
    registry_root = tmp_path / "registry"
    out_root = tmp_path / "backtest"
    _write_candles(parquet_root / "candles_v1")
    _write_features(dataset_root)
    _save_model_run(registry_root=registry_root, dataset_root=dataset_root, thresholds={"top_5pct": 0.5})

    settings = BacktestRunSettings(
        dataset_name="candles_v1",
        parquet_root=str(parquet_root),
        tf="5m",
        quote="KRW",
        top_n=2,
        markets=("KRW-BTC", "KRW-ETH"),
        universe_mode="fixed_list",
        from_ts_ms=0,
        to_ts_ms=3_600_000,
        starting_krw=200_000.0,
        per_trade_krw=10_000.0,
        max_positions=1,
        output_root_dir=str(out_root),
        strategy="model_alpha_v1",
        model_ref="run_v3",
        model_family="train_v3_mtf_micro",
        feature_set="v3",
        model_registry_root=str(registry_root),
        model_feature_dataset_root=str(dataset_root),
        model_alpha=ModelAlphaSettings(
            model_ref="run_v3",
            model_family="train_v3_mtf_micro",
            selection=ModelAlphaSelectionSettings(top_pct=1.0, min_prob=None, min_candidates_per_ts=1),
            position=ModelAlphaPositionSettings(max_positions_total=1, cooldown_bars=0),
            exit=ModelAlphaExitSettings(mode="hold", hold_bars=1),
            execution=ModelAlphaExecutionSettings(price_mode="PASSIVE_MAKER", timeout_bars=3, replace_max=4),
        ),
    )
    engine = BacktestRunEngine(
        run_settings=settings,
        upbit_settings=None,
        rules_provider=_StaticRulesProvider(),  # type: ignore[arg-type]
    )

    summary = engine.run()
    run_dir = Path(summary.run_dir)
    assert summary.strategy == "model_alpha_v1"
    assert (run_dir / "summary.json").exists()
    assert (run_dir / "trades.csv").exists()
    assert (run_dir / "per_market.csv").exists()
    assert (run_dir / "selection_stats.json").exists()
    assert (run_dir / "debug_mismatch.json").exists()

    selection_stats = json.loads((run_dir / "selection_stats.json").read_text(encoding="utf-8"))
    assert int(selection_stats.get("scored_rows", 0)) > 0
    assert int(selection_stats.get("eligible_rows", 0)) > 0
    assert int(selection_stats.get("selected_rows", 0)) >= 0
    assert float(selection_stats.get("min_prob_used", 0.0)) == 0.5
    assert str(selection_stats.get("min_prob_source")) == "registry:top_5pct"
    assert "top_pct_used" in selection_stats
    assert "min_candidates_used" in selection_stats

    events_payloads = [
        json.loads(line)
        for line in (run_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    selection_events = [item for item in events_payloads if item.get("event_type") == "MODEL_ALPHA_SELECTION"]
    assert selection_events
    selection_payload = selection_events[0].get("payload", {})
    assert "top_pct_used" in selection_payload
    assert "min_candidates_used" in selection_payload
    intent_events = [item for item in events_payloads if item.get("event_type") == "INTENT_CREATED"]
    assert intent_events
    intent_meta = ((intent_events[0].get("payload") or {}).get("meta") or {})
    exec_profile = intent_meta.get("exec_profile") or {}
    assert exec_profile.get("price_mode") == "PASSIVE_MAKER"
    assert int(exec_profile.get("timeout_ms", 0)) == 900_000
    assert int(exec_profile.get("replace_interval_ms", 0)) == 900_000
    assert int(exec_profile.get("max_replaces", -1)) == 4


def test_backtest_model_alpha_v4_run_uses_v4_feature_dataset(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    dataset_root = tmp_path / "features_v4"
    registry_root = tmp_path / "registry"
    out_root = tmp_path / "backtest"
    _write_candles(parquet_root / "candles_v1")
    _write_features(dataset_root)
    _save_model_run(
        registry_root=registry_root,
        dataset_root=dataset_root,
        thresholds={"top_5pct": 0.5},
        model_family="train_v4_crypto_cs",
        run_id="run_v4",
    )

    settings = BacktestRunSettings(
        dataset_name="candles_v1",
        parquet_root=str(parquet_root),
        tf="5m",
        quote="KRW",
        top_n=2,
        markets=("KRW-BTC", "KRW-ETH"),
        universe_mode="fixed_list",
        from_ts_ms=0,
        to_ts_ms=3_600_000,
        starting_krw=200_000.0,
        per_trade_krw=10_000.0,
        max_positions=1,
        output_root_dir=str(out_root),
        strategy="model_alpha_v1",
        model_ref="run_v4",
        model_family="train_v4_crypto_cs",
        feature_set="v4",
        model_registry_root=str(registry_root),
        model_feature_dataset_root=str(dataset_root),
        model_alpha=ModelAlphaSettings(
            model_ref="run_v4",
            model_family="train_v4_crypto_cs",
            feature_set="v4",
            selection=ModelAlphaSelectionSettings(top_pct=1.0, min_prob=None, min_candidates_per_ts=1),
            position=ModelAlphaPositionSettings(max_positions_total=1, cooldown_bars=0),
            exit=ModelAlphaExitSettings(mode="hold", hold_bars=1),
        ),
    )

    summary = BacktestRunEngine(
        run_settings=settings,
        upbit_settings=None,
        rules_provider=_StaticRulesProvider(),  # type: ignore[arg-type]
    ).run()

    assert summary.strategy == "model_alpha_v1"
    assert summary.orders_submitted > 0


def test_backtest_model_alpha_entry_sizing_respects_min_total_buffer(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    dataset_root = tmp_path / "features_v3"
    registry_root = tmp_path / "registry"
    out_root = tmp_path / "backtest"
    _write_candles(parquet_root / "candles_v1")
    _write_features(dataset_root)
    _save_model_run(registry_root=registry_root, dataset_root=dataset_root, thresholds={"top_5pct": 0.5})

    settings = BacktestRunSettings(
        dataset_name="candles_v1",
        parquet_root=str(parquet_root),
        tf="5m",
        quote="KRW",
        top_n=2,
        markets=("KRW-BTC", "KRW-ETH"),
        universe_mode="fixed_list",
        from_ts_ms=0,
        to_ts_ms=3_600_000,
        starting_krw=200_000.0,
        per_trade_krw=4_900.0,
        max_positions=1,
        output_root_dir=str(out_root),
        strategy="model_alpha_v1",
        model_ref="run_v3",
        model_family="train_v3_mtf_micro",
        feature_set="v3",
        model_registry_root=str(registry_root),
        model_feature_dataset_root=str(dataset_root),
        model_alpha=ModelAlphaSettings(
            model_ref="run_v3",
            model_family="train_v3_mtf_micro",
            selection=ModelAlphaSelectionSettings(top_pct=1.0, min_prob=None, min_candidates_per_ts=1),
            position=ModelAlphaPositionSettings(
                max_positions_total=1,
                cooldown_bars=0,
                entry_min_notional_buffer_bps=100.0,
            ),
            exit=ModelAlphaExitSettings(mode="hold", hold_bars=1),
        ),
    )
    summary = BacktestRunEngine(
        run_settings=settings,
        upbit_settings=None,
        rules_provider=_StaticRulesProvider(),  # type: ignore[arg-type]
    ).run()
    run_dir = Path(summary.run_dir)
    payloads = [
        json.loads(line)
        for line in (run_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    intent_events = [item for item in payloads if item.get("event_type") == "INTENT_CREATED"]
    assert intent_events
    intent_payload = intent_events[0].get("payload") or {}
    assert float(intent_payload.get("price", 0.0)) * float(intent_payload.get("volume", 0.0)) >= 5_050.0


def test_backtest_model_alpha_entry_sizing_uses_prob_ramp_multiplier(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    dataset_root = tmp_path / "features_v3"
    registry_root = tmp_path / "registry"
    out_root = tmp_path / "backtest"
    _write_candles(parquet_root / "candles_v1")
    _write_features(dataset_root)
    _save_model_run(registry_root=registry_root, dataset_root=dataset_root, thresholds={"top_5pct": 0.0})

    settings = BacktestRunSettings(
        dataset_name="candles_v1",
        parquet_root=str(parquet_root),
        tf="5m",
        quote="KRW",
        top_n=2,
        markets=("KRW-BTC", "KRW-ETH"),
        universe_mode="fixed_list",
        from_ts_ms=0,
        to_ts_ms=3_600_000,
        starting_krw=200_000.0,
        per_trade_krw=10_000.0,
        max_positions=1,
        output_root_dir=str(out_root),
        strategy="model_alpha_v1",
        model_ref="run_v3",
        model_family="train_v3_mtf_micro",
        feature_set="v3",
        model_registry_root=str(registry_root),
        model_feature_dataset_root=str(dataset_root),
        model_alpha=ModelAlphaSettings(
            model_ref="run_v3",
            model_family="train_v3_mtf_micro",
            selection=ModelAlphaSelectionSettings(top_pct=1.0, min_prob=None, min_candidates_per_ts=1),
            position=ModelAlphaPositionSettings(
                max_positions_total=1,
                cooldown_bars=0,
                sizing_mode="prob_ramp",
                size_multiplier_min=0.5,
                size_multiplier_max=1.5,
            ),
            exit=ModelAlphaExitSettings(mode="hold", hold_bars=1),
        ),
    )
    summary = BacktestRunEngine(
        run_settings=settings,
        upbit_settings=None,
        rules_provider=_StaticRulesProvider(),  # type: ignore[arg-type]
    ).run()
    run_dir = Path(summary.run_dir)
    payloads = [
        json.loads(line)
        for line in (run_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    intent_events = [item for item in payloads if item.get("event_type") == "INTENT_CREATED"]
    assert intent_events
    intent_payload = intent_events[0].get("payload") or {}
    meta = intent_payload.get("meta") or {}
    assert float(meta.get("notional_multiplier", 0.0)) > 1.0
    assert float(meta.get("target_notional_quote", 0.0)) > 10_000.0
    assert float(intent_payload.get("price", 0.0)) * float(intent_payload.get("volume", 0.0)) >= float(
        meta.get("target_notional_quote", 0.0)
    )


def _write_candles(dataset_root: Path) -> None:
    for market, start_price in [("KRW-BTC", 100.0), ("KRW-ETH", 200.0)]:
        part_dir = dataset_root / "tf=5m" / f"market={market}"
        part_dir.mkdir(parents=True, exist_ok=True)
        ts = [i * 300_000 for i in range(16)]
        close = [start_price + i * 1.0 for i in range(16)]
        pl.DataFrame(
            {
                "ts_ms": ts,
                "open": [value - 0.5 for value in close],
                "high": [value + 2.0 for value in close],
                "low": [value - 2.0 for value in close],
                "close": close,
                "volume_base": [10.0 + i for i in range(16)],
                "volume_quote": [close[i] * (10.0 + i) for i in range(16)],
                "volume_quote_est": [False for _ in close],
            }
        ).write_parquet(part_dir / "part-000.parquet")


def _write_features(dataset_root: Path) -> None:
    meta_dir = dataset_root / "_meta"
    meta_dir.mkdir(parents=True, exist_ok=True)
    (meta_dir / "feature_spec.json").write_text(json.dumps({"feature_columns": ["f1"]}), encoding="utf-8")
    (meta_dir / "label_spec.json").write_text(json.dumps({"label_columns": ["y_reg", "y_cls"]}), encoding="utf-8")
    for market, sign in [("KRW-BTC", 1.0), ("KRW-ETH", -1.0)]:
        part_dir = dataset_root / "tf=5m" / f"market={market}" / "date=2026-01-01"
        part_dir.mkdir(parents=True, exist_ok=True)
        ts = [i * 300_000 for i in range(12)]
        close = [100.0 + i if market == "KRW-BTC" else 200.0 + i for i in range(12)]
        f1 = [sign * (0.5 + 0.1 * i) for i in range(12)]
        pl.DataFrame(
            {
                "ts_ms": ts,
                "close": close,
                "f1": f1,
                "sample_weight": [1.0 for _ in ts],
                "y_reg": [0.0 for _ in ts],
                "y_cls": [1 if sign > 0 else 0 for _ in ts],
            }
        ).write_parquet(part_dir / "part-000.parquet")


def _save_model_run(
    *,
    registry_root: Path,
    dataset_root: Path,
    thresholds: dict[str, float] | None = None,
    model_family: str = "train_v3_mtf_micro",
    run_id: str = "run_v3",
) -> None:
    save_run(
        RegistrySavePayload(
            registry_root=registry_root,
            model_family=model_family,
            run_id=run_id,
            model_bundle={"model_type": "xgboost", "scaler": None, "estimator": _DummyEstimator()},
            metrics={},
            thresholds=thresholds or {},
            feature_spec={"feature_columns": ["f1"]},
            label_spec={"label_columns": ["y_reg", "y_cls"]},
            train_config={"dataset_root": str(dataset_root), "feature_columns": ["f1"], "batch_rows": 1000},
            data_fingerprint={},
            leaderboard_row={"run_id": run_id, "test_precision_top5": 0.1},
            model_card_text="# model alpha",
        )
    )
