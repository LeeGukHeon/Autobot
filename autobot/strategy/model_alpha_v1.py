"""Model-driven alpha strategy for backtest integration."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
import hashlib
import json
import math
from typing import Any, Callable, Iterable, Sequence

import numpy as np
import polars as pl

from autobot.backtest.strategy_adapter import (
    BacktestStrategyAdapter,
    StrategyFillEvent,
    StrategyOpportunityRecord,
    StrategyOrderIntent,
    StrategyStepResult,
)
from autobot.common.dynamic_exit_overlay import resolve_dynamic_exit_overlay
from autobot.common.model_exit_contract import normalize_model_exit_plan_payload
from autobot.common.path_risk_guidance import build_path_risk_runtime_inputs, resolve_path_risk_guidance_from_plan
from autobot.execution.order_supervisor import make_legacy_exec_profile, order_exec_profile_to_dict
from autobot.models.dataset_loader import FeatureTsGroup
from autobot.models.entry_boundary import evaluate_entry_boundary
from autobot.models.execution_risk_control import (
    normalize_execution_risk_control_payload,
    resolve_execution_risk_control_decision,
    resolve_execution_risk_control_size_decision,
)
from autobot.models.predictor import ModelPredictor
from autobot.models.selection_policy import DEFAULT_SELECTION_POLICY_MODE
from autobot.models.trade_action_policy import normalize_trade_action_policy, resolve_trade_action
from autobot.strategy.operational_overlay_v1 import (
    ModelAlphaOperationalSettings,
    build_regime_snapshot_from_scored_frame,
    load_calibrated_operational_settings,
    resolve_operational_max_positions,
    resolve_operational_risk_multiplier,
)
from autobot.strategy.micro_snapshot import MicroSnapshot
from autobot.strategy.v5_post_model_contract import (
    V5_CONTINUATION_EXIT_REASON,
    V5_ENTRY_OWNER,
    V5_POST_MODEL_CONTRACT_VERSION,
    V5_SIZING_OWNER,
    V5_STALE_TIMEOUT_EXIT_REASON,
    V5_TRADE_ACTION_ROLE,
    annotate_v5_runtime_recommendations,
    build_v5_liquidation_policy,
    build_v5_entry_decision_payload,
    finalize_v5_entry_decision,
    is_v5_post_model_contract,
    rank_v5_entry_candidates,
    resolve_v5_entry_gate,
    resolve_v5_exit_decision,
    resolve_v5_target_notional,
)

from . import model_alpha_runtime_contract as _runtime_contract

_EXECUTION_STAGE_ORDER: tuple[str, ...] = ("PASSIVE_MAKER", "JOIN", "CROSS_1T")


@dataclass(frozen=True)
class ModelAlphaSelectionSettings:
    top_pct: float = 0.50
    min_prob: float | None = None
    min_candidates_per_ts: int = 1
    registry_threshold_key: str = "top_5pct"
    use_learned_recommendations: bool = True
    selection_policy_mode: str = "auto"  # auto | raw_threshold | rank_effective_quantile


@dataclass(frozen=True)
class ModelAlphaPositionSettings:
    max_positions_total: int = 3
    cooldown_bars: int = 6
    entry_min_notional_buffer_bps: float = 25.0
    base_budget_quote: float | None = None
    sizing_mode: str = "prob_ramp"  # fixed | prob_ramp
    size_multiplier_min: float = 0.5
    size_multiplier_max: float = 1.5


@dataclass(frozen=True)
class ModelAlphaExitSettings:
    mode: str = "hold"  # hold | risk
    hold_bars: int = 6
    use_learned_exit_mode: bool = True
    use_learned_hold_bars: bool = True
    use_learned_risk_recommendations: bool = True
    use_trade_level_action_policy: bool = True
    risk_scaling_mode: str = "fixed"  # fixed | volatility_scaled
    risk_vol_feature: str = "rv_12"
    tp_vol_multiplier: float | None = None
    sl_vol_multiplier: float | None = None
    trailing_vol_multiplier: float | None = None
    tp_pct: float = 0.02
    sl_pct: float = 0.01
    trailing_pct: float = 0.0
    expected_exit_slippage_bps: float | None = None
    expected_exit_fee_bps: float | None = None


@dataclass(frozen=True)
class ModelAlphaExecutionSettings:
    price_mode: str = "JOIN"
    timeout_bars: int = 2
    replace_max: int = 2
    use_learned_recommendations: bool = True


@dataclass(frozen=True)
class ModelAlphaSettings:
    model_ref: str = "champion_v4"
    model_family: str | None = "train_v4_crypto_cs"
    feature_set: str = "v4"
    selection: ModelAlphaSelectionSettings = field(default_factory=ModelAlphaSelectionSettings)
    position: ModelAlphaPositionSettings = field(default_factory=ModelAlphaPositionSettings)
    exit: ModelAlphaExitSettings = field(default_factory=ModelAlphaExitSettings)
    execution: ModelAlphaExecutionSettings = field(default_factory=ModelAlphaExecutionSettings)
    operational: ModelAlphaOperationalSettings = field(default_factory=ModelAlphaOperationalSettings)


@dataclass
class _PositionState:
    entry_ts_ms: int
    entry_price: float
    peak_price: float
    entry_fee_rate: float
    qty: float
    exit_plan: dict[str, Any] = field(default_factory=dict)


class ModelAlphaStrategyV1(BacktestStrategyAdapter):
    def __init__(
        self,
        *,
        predictor: ModelPredictor,
        feature_groups: Iterable[FeatureTsGroup] | None,
        settings: ModelAlphaSettings,
        interval_ms: int,
        live_frame_provider: Callable[[int, Sequence[str]], pl.DataFrame | None] | None = None,
        enable_operational_overlay: bool = False,
    ) -> None:
        self._predictor = predictor
        self._feature_iter = iter(feature_groups or ())
        self._settings, self._runtime_recommendation_state = resolve_runtime_model_alpha_settings(
            predictor=predictor,
            settings=settings,
        )
        self._runtime_recommendations = dict(getattr(predictor, "runtime_recommendations", {}) or {})
        if is_v5_post_model_contract(self._runtime_recommendations):
            self._runtime_recommendations = annotate_v5_runtime_recommendations(self._runtime_recommendations)
        self._trade_action_policy = normalize_trade_action_policy(
            self._runtime_recommendations.get("trade_action")
        )
        self._execution_recommendation = dict(
            self._runtime_recommendations.get("execution") or {}
        )
        self._risk_control_policy = normalize_execution_risk_control_payload(
            self._runtime_recommendations.get("risk_control")
        )
        self._is_v5_post_model_contract = is_v5_post_model_contract(self._runtime_recommendations)
        self._runtime_recommendation_state["trade_action_policy_status"] = str(
            self._trade_action_policy.get("status", "missing")
        )
        self._runtime_recommendation_state["trade_action_risk_feature_name"] = str(
            self._trade_action_policy.get("risk_feature_name", "")
        )
        self._runtime_recommendation_state["decision_contract_version"] = str(
            self._runtime_recommendations.get("decision_contract_version", "")
        ).strip()
        self._runtime_recommendation_state["entry_ownership"] = str(
            self._runtime_recommendations.get("entry_ownership", "")
        ).strip()
        self._runtime_recommendation_state["sizing_ownership"] = str(
            self._runtime_recommendations.get("sizing_ownership", "")
        ).strip()
        self._runtime_recommendation_state["trade_action_role"] = str(
            self._runtime_recommendations.get("trade_action_role", "")
        ).strip()
        self._runtime_recommendation_state["exit_ownership"] = str(
            self._runtime_recommendations.get("exit_ownership", "")
        ).strip()
        self._interval_ms = max(int(interval_ms), 1)
        self._pending_group: FeatureTsGroup | None = None
        self._positions: dict[str, _PositionState] = {}
        self._cooldown_until_ts_ms: dict[str, int] = {}
        self._live_frame_provider = live_frame_provider
        self._enable_operational_overlay = bool(enable_operational_overlay)
        self._operational_settings = (
            load_calibrated_operational_settings(base_settings=settings.operational)
            if self._enable_operational_overlay
            else settings.operational
        )

    @property
    def predictor_run_id(self) -> str:
        return str(self._predictor.run_dir.name).strip()
    def on_ts(
        self,
        *,
        ts_ms: int,
        active_markets: Sequence[str],
        latest_prices: dict[str, float],
        open_markets: set[str],
    ) -> StrategyStepResult:
        ts_value = int(ts_ms)
        active_set = {str(item).strip().upper() for item in active_markets if str(item).strip()}
        tracked_open_markets = {str(item).strip().upper() for item in open_markets if str(item).strip()}
        tracked_open_markets.update(str(market).strip().upper() for market in self._positions.keys() if str(market).strip())
        min_prob_used, min_prob_source = _resolve_selection_min_prob(
            predictor=self._predictor,
            settings=self._settings.selection,
        )
        selection_policy, selection_policy_source = _resolve_selection_policy(
            predictor=self._predictor,
            settings=self._settings.selection,
        )
        top_pct_used, top_pct_source = _resolve_selection_top_pct(
            predictor=self._predictor,
            settings=self._settings.selection,
            selection_policy=selection_policy,
            selection_policy_source=selection_policy_source,
        )
        entry_boundary_contract = getattr(self._predictor, "entry_boundary_contract", {}) or {}
        min_candidates_used, min_candidates_source = _resolve_selection_min_candidates(
            predictor=self._predictor,
            settings=self._settings.selection,
            selection_policy=selection_policy,
            selection_policy_source=selection_policy_source,
        )
        if str(selection_policy.get("mode", "")).strip().lower() == DEFAULT_SELECTION_POLICY_MODE:
            min_prob_used = 0.0
            min_prob_source = f"{selection_policy_source}:{DEFAULT_SELECTION_POLICY_MODE}"
        frame: pl.DataFrame | None
        if self._live_frame_provider is not None:
            frame = self._live_frame_provider(ts_value, tuple(sorted(active_set)))
        else:
            self._advance_to_ts(ts_value)
            frame = self._take_group_if_equal(ts_value)
        frame_by_market: dict[str, dict[str, Any]] = {}
        if frame is not None and frame.height > 0:
            for row in frame.iter_rows(named=True):
                market = str(row.get("market", "")).strip().upper()
                if market:
                    frame_by_market[market] = row

        intents: list[StrategyOrderIntent] = []
        opportunities: list[StrategyOpportunityRecord] = []
        skipped_reasons: dict[str, int] = {}
        scored_rows = 0
        selected_rows = 0
        dropped_min_prob_rows = 0
        dropped_top_pct_rows = 0
        blocked_min_candidates_ts = 0

        def append_entry_opportunity(
            *,
            row: dict[str, Any],
            chosen_action: str,
            reason_code: str,
            skip_reason_code: str | None = None,
            trade_action_decision: dict[str, Any] | None = None,
            execution_decision: dict[str, Any] | None = None,
            execution_profile: dict[str, Any] | None = None,
            notional_multiplier: float | None = None,
            support_level: str = "",
            support_size_multiplier: float | None = None,
            entry_decision_payload: dict[str, Any] | None = None,
            sizing_decision_payload: dict[str, Any] | None = None,
            safety_vetoes_payload: dict[str, Any] | None = None,
            exit_decision_payload: dict[str, Any] | None = None,
            liquidation_policy_payload: dict[str, Any] | None = None,
            legacy_heuristics_payload: dict[str, Any] | None = None,
        ) -> None:
            market_name = str(row.get("market", "")).strip().upper()
            if not market_name:
                return
            resolved_entry_decision = dict(entry_decision_payload or {})
            resolved_reason_codes = [
                str(code).strip()
                for code in (resolved_entry_decision.get("reason_codes") or [])
                if str(code).strip()
            ]
            if not resolved_reason_codes and str(chosen_action).strip().lower() == "intent_created":
                resolved_reason_codes = ["ENTRY_ALLOWED"]
            if not resolved_reason_codes and skip_reason_code:
                resolved_reason_codes = [str(skip_reason_code).strip()]
            behavior_policy = _build_behavior_policy_logging(
                execution_decision=execution_decision,
                execution_profile=execution_profile,
                decision_outcome=chosen_action,
                skip_reason_code=skip_reason_code,
            )
            opportunities.append(
                StrategyOpportunityRecord(
                    opportunity_id=f"entry:{ts_value}:{market_name}",
                    ts_ms=ts_value,
                    market=market_name,
                    side="bid",
                    decision_outcome=str(chosen_action).strip(),
                    selection_score=_safe_optional_float(row.get("model_prob")),
                    selection_score_raw=_safe_optional_float(row.get("model_prob_raw")),
                    feature_hash=_build_opportunity_feature_hash(row=row),
                    chosen_action=str(behavior_policy.get("chosen_action") or "").strip(),
                    reason_code=str(reason_code).strip(),
                    skip_reason_code=(str(skip_reason_code).strip() if skip_reason_code else None),
                    expected_edge_bps=_resolve_v5_strategy_expected_edge_bps(
                        row=row,
                        trade_action=trade_action_decision,
                    ),
                    uncertainty=_resolve_opportunity_uncertainty(row),
                    expected_net_edge_bps=_resolve_v5_strategy_expected_net_edge_bps(
                        row=row,
                        execution_decision=execution_decision,
                        trade_action=trade_action_decision,
                    ),
                    final_alpha_lcb=_safe_optional_float(row.get("final_alpha_lcb")),
                    alpha_lcb_floor=_safe_optional_float(resolved_entry_decision.get("alpha_lcb_floor")),
                    reason_codes=tuple(resolved_reason_codes),
                    run_id=self.predictor_run_id,
                    candidate_actions_json=tuple(behavior_policy.get("candidate_actions_json") or ()),
                    chosen_action_propensity=_safe_optional_float(behavior_policy.get("chosen_action_propensity")),
                    no_trade_action_propensity=_safe_optional_float(behavior_policy.get("no_trade_action_propensity")),
                    behavior_policy_name=str(behavior_policy.get("behavior_policy_name") or "").strip(),
                    behavior_policy_mode=str(behavior_policy.get("behavior_policy_mode") or "").strip(),
                    behavior_policy_support=str(behavior_policy.get("behavior_policy_support") or "").strip(),
                    meta={
                        "selection_policy_mode": str(selection_mode),
                        "selection_policy_source": str(selection_policy_source),
                        "support_level": support_level or "full",
                        "support_size_multiplier": support_size_multiplier,
                        "notional_multiplier": notional_multiplier,
                        "state_features": _build_live_state_feature_snapshot(row=row),
                        "behavior_policy": dict(behavior_policy),
                        "entry_decision": dict(resolved_entry_decision),
                        "sizing_decision": dict(sizing_decision_payload or {}),
                        "safety_vetoes": dict(safety_vetoes_payload or {}),
                        "exit_decision": dict(exit_decision_payload or {}),
                        "liquidation_policy": dict(liquidation_policy_payload or {}),
                        "legacy_heuristics_applied": dict(legacy_heuristics_payload or {}),
                    },
                )
            )

        for market in sorted(tracked_open_markets):
            if market not in self._positions:
                continue
            row = frame_by_market.get(market)
            ref_price = _resolve_ref_price(row=row, latest_prices=latest_prices, market=market)
            if ref_price is None:
                _inc_reason(skipped_reasons, "EXIT_REF_PRICE_MISSING")
                continue
            state = self._positions[market]
            reason_code = self._resolve_exit_reason(
                ts_ms=ts_value,
                market=market,
                state=state,
                ref_price=ref_price,
                row=row,
            )
            if reason_code is None:
                continue
            exit_intent_meta = (
                _build_v5_exit_intent_meta(
                    state=state,
                    reason_code=reason_code,
                    trigger_ts_ms=ts_value,
                )
                if self._is_v5_post_model_contract
                else {"strategy": "model_alpha_v1", "exit_mode": str(self._settings.exit.mode)}
            )
            intents.append(
                StrategyOrderIntent(
                    market=market,
                    side="ask",
                    ref_price=ref_price,
                    reason_code=reason_code,
                    meta=exit_intent_meta,
                )
            )

        if frame is None or frame.height <= 0:
            missing_rows = len(active_set)
            return StrategyStepResult(
                intents=tuple(intents),
                skipped_missing_features_rows=max(int(missing_rows), 0),
                skipped_reasons={"NO_FEATURE_ROWS_AT_TS": int(missing_rows)} if missing_rows > 0 else {},
                min_prob_used=min_prob_used,
                min_prob_source=min_prob_source,
                top_pct_used=top_pct_used,
                top_pct_source=top_pct_source,
                min_candidates_used=min_candidates_used,
                min_candidates_source=min_candidates_source,
                selection_policy_mode=str(selection_policy.get("mode", "raw_threshold")),
                selection_policy_source=selection_policy_source,
            )

        frame_active = frame.filter(pl.col("market").is_in(list(active_set))) if active_set else frame
        scored_rows = int(frame_active.height)
        missing_rows = max(len(active_set) - scored_rows, 0)
        if scored_rows <= 0:
            return StrategyStepResult(
                intents=tuple(intents),
                scored_rows=0,
                skipped_missing_features_rows=int(missing_rows),
                skipped_reasons={"NO_ACTIVE_MARKET_FEATURE_ROWS": int(missing_rows)} if missing_rows > 0 else {},
                min_prob_used=min_prob_used,
                min_prob_source=min_prob_source,
                top_pct_used=top_pct_used,
                top_pct_source=top_pct_source,
                min_candidates_used=min_candidates_used,
                min_candidates_source=min_candidates_source,
                selection_policy_mode=str(selection_policy.get("mode", "raw_threshold")),
                selection_policy_source=selection_policy_source,
            )

        selection_mode = str(selection_policy.get("mode", "raw_threshold")).strip().lower()
        matrix = frame_active.select(list(self._predictor.feature_columns)).to_numpy().astype(np.float32, copy=False)
        score_contract = self._predictor.predict_score_contract(matrix)
        raw_probs = np.asarray(score_contract["score_mean"], dtype=np.float64)
        if selection_mode == DEFAULT_SELECTION_POLICY_MODE:
            probs = self._predictor.predict_selection_scores(matrix).astype(np.float64, copy=False)
        else:
            probs = raw_probs
        scored = frame_active.with_columns(
            pl.Series(name="model_prob", values=probs),
            pl.Series(name="model_prob_raw", values=raw_probs),
            pl.Series(name="final_rank_score", values=np.asarray(score_contract["final_rank_score"], dtype=np.float64)),
            pl.Series(name="final_uncertainty", values=_optional_float_list(score_contract["final_uncertainty"])),
            pl.Series(name="score_mean", values=np.asarray(score_contract["score_mean"], dtype=np.float64)),
            pl.Series(name="score_std", values=_optional_float_list(score_contract["score_std"])),
            pl.Series(name="score_lcb", values=np.asarray(score_contract["score_lcb"], dtype=np.float64)),
            pl.Series(name="final_expected_return", values=_optional_float_list(score_contract["final_expected_return"])),
            pl.Series(name="final_expected_es", values=_optional_float_list(score_contract["final_expected_es"])),
            pl.Series(name="final_tradability", values=_optional_float_list(score_contract["final_tradability"])),
            pl.Series(name="final_alpha_lcb", values=_optional_float_list(score_contract["final_alpha_lcb"])),
        )
        v5_post_model = bool(self._is_v5_post_model_contract)
        ranked_selected_rows: list[dict[str, Any]] = []
        if v5_post_model:
            eligible = scored
            eligible_rows = int(scored.height)
            dropped_min_prob_rows = 0
        elif selection_mode == DEFAULT_SELECTION_POLICY_MODE:
            eligible = scored
            eligible_rows = int(eligible.height)
            dropped_min_prob_rows = 0
        else:
            eligible = scored.filter(pl.col("model_prob") >= float(min_prob_used))
            eligible_rows = int(eligible.height)
            dropped_min_prob_rows = max(scored_rows - eligible_rows, 0)
            if dropped_min_prob_rows > 0:
                for row in scored.filter(pl.col("model_prob") < float(min_prob_used)).iter_rows(named=True):
                    _inc_reason(skipped_reasons, "MIN_PROB_NOT_MET")
                    append_entry_opportunity(
                        row=row,
                        chosen_action="skip",
                        reason_code="MODEL_ALPHA_ENTRY_V1",
                        skip_reason_code="MIN_PROB_NOT_MET",
                    )
        operational_state: dict[str, Any] = {}
        operational_regime_score = 0.0
        operational_risk_multiplier = 1.0
        base_max_positions = max(int(self._settings.position.max_positions_total), 1)
        operational_max_positions = base_max_positions

        if self._enable_operational_overlay and bool(self._operational_settings.enabled):
            regime = build_regime_snapshot_from_scored_frame(
                scored=scored,
                eligible_rows=eligible_rows,
                scored_rows=scored_rows,
                ts_ms=ts_value,
            )
            operational_regime_score = float(regime.regime_score)
            operational_risk_multiplier = resolve_operational_risk_multiplier(
                settings=self._operational_settings,
                regime_score=regime.regime_score,
                breadth_ratio=regime.breadth_ratio,
                micro_quality_score=regime.micro_feature_quality_score,
            )
            operational_max_positions = resolve_operational_max_positions(
                base_max_positions=base_max_positions,
                settings=self._operational_settings,
                regime_score=regime.regime_score,
                breadth_ratio=regime.breadth_ratio,
                micro_quality_score=regime.micro_feature_quality_score,
            )
            operational_state = {
                "runtime_recommendation_state": dict(self._runtime_recommendation_state),
                "enabled": True,
                "regime_score": float(regime.regime_score),
                "breadth_score": float(regime.breadth_score),
                "breadth_ratio": float(regime.breadth_ratio),
                "dispersion_score": float(regime.dispersion_score),
                "dispersion_value": float(regime.dispersion_value),
                "micro_feature_quality_score": float(regime.micro_feature_quality_score),
                "trade_coverage_ms": float(regime.trade_coverage_ms),
                "book_coverage_ms": float(regime.book_coverage_ms),
                "spread_bps": float(regime.spread_bps),
                "depth_krw": float(regime.depth_krw),
                "session_score": float(regime.session_score),
                "session_bucket": str(regime.session_bucket),
                "risk_multiplier": float(operational_risk_multiplier),
                "max_positions_used": int(operational_max_positions),
                "calibration_artifact_enabled": bool(self._operational_settings.use_calibration_artifact),
                "calibration_artifact_path": str(self._operational_settings.calibration_artifact_path),
            }

        min_candidates = max(int(min_candidates_used), 0)
        if not v5_post_model and eligible_rows < min_candidates:
            blocked_min_candidates_ts = 1
            _inc_reason(skipped_reasons, "MIN_CANDIDATES_NOT_MET")
            for row in eligible.iter_rows(named=True):
                append_entry_opportunity(
                    row=row,
                    chosen_action="skip",
                    reason_code="MODEL_ALPHA_ENTRY_V1",
                    skip_reason_code="MIN_CANDIDATES_NOT_MET",
                )
            return StrategyStepResult(
                intents=tuple(intents),
                opportunities=tuple(opportunities),
                scored_rows=scored_rows,
                eligible_rows=eligible_rows,
                selected_rows=0,
                skipped_missing_features_rows=int(missing_rows),
                dropped_min_prob_rows=dropped_min_prob_rows,
                blocked_min_candidates_ts=blocked_min_candidates_ts,
                min_prob_used=min_prob_used,
                min_prob_source=min_prob_source,
                top_pct_used=top_pct_used,
                top_pct_source=top_pct_source,
                min_candidates_used=min_candidates_used,
                min_candidates_source=min_candidates_source,
                selection_policy_mode=selection_mode,
                selection_policy_source=selection_policy_source,
                operational_regime_score=operational_regime_score,
                operational_risk_multiplier=operational_risk_multiplier,
                operational_max_positions=operational_max_positions,
                operational_state=operational_state,
                skipped_reasons=skipped_reasons,
            )

        if v5_post_model:
            ranked_selected_rows = rank_v5_entry_candidates(
                [
                    {
                        "market": str(row.get("market", "")).strip().upper(),
                        "final_alpha_lcb": _safe_optional_float(row.get("final_alpha_lcb")),
                        "final_expected_return": _safe_optional_float(row.get("final_expected_return")),
                        "final_uncertainty": _safe_optional_float(row.get("final_uncertainty")),
                    }
                    for row in scored.iter_rows(named=True)
                ]
            )
            selected_rows = int(len(ranked_selected_rows))
            dropped_top_pct_rows = 0
        elif selection_mode == DEFAULT_SELECTION_POLICY_MODE:
            select_count = max(int(math.floor(eligible_rows * max(float(top_pct_used), 0.0))), min_candidates)
            select_count = min(select_count, eligible_rows)
        else:
            select_count = int(math.floor(eligible_rows * max(float(top_pct_used), 0.0)))
        if not v5_post_model:
            if select_count > 0:
                selected = eligible.sort("model_prob", descending=True).head(select_count)
            else:
                selected = eligible.head(0)
            ranked_selected_rows = [dict(row) for row in selected.iter_rows(named=True)]
            selected_rows = int(selected.height)
            dropped_top_pct_rows = max(eligible_rows - selected_rows, 0)
        if not v5_post_model and dropped_top_pct_rows > 0:
            selected_markets = {
                str(row.get("market", "")).strip().upper()
                for row in ranked_selected_rows
                if str(row.get("market", "")).strip()
            }
            for row in eligible.iter_rows(named=True):
                market_name = str(row.get("market", "")).strip().upper()
                if market_name in selected_markets:
                    continue
                _inc_reason(skipped_reasons, "TOP_PCT_NOT_SELECTED")
                append_entry_opportunity(
                    row=row,
                    chosen_action="skip",
                    reason_code="MODEL_ALPHA_ENTRY_V1",
                    skip_reason_code="TOP_PCT_NOT_SELECTED",
                )

        active_positions = len(tracked_open_markets)
        max_positions = max(int(operational_max_positions), 1)
        can_open = max(max_positions - active_positions, 0)
        if can_open <= 0:
            _inc_reason(skipped_reasons, "MAX_POSITIONS_TOTAL")
        selected_rank_by_market = {
            str(item.get("market", "")).strip().upper(): int(item.get("selected_rank") or 0)
            for item in ranked_selected_rows
            if str(item.get("market", "")).strip()
        }
        selected_market_set = {
            str(item.get("market", "")).strip().upper()
            for item in ranked_selected_rows
            if str(item.get("market", "")).strip()
        }
        selected_row_docs = (
            [
                dict(row)
                for row in scored.iter_rows(named=True)
                if str(row.get("market", "")).strip().upper() in selected_market_set
            ]
            if v5_post_model
            else ranked_selected_rows
        )
        selected_row_docs.sort(
            key=lambda item: (
                int(selected_rank_by_market.get(str(item.get("market", "")).strip().upper(), 0) or 0),
                str(item.get("market", "")).strip().upper(),
            )
        )
        for row in selected_row_docs:
            if can_open <= 0:
                append_entry_opportunity(
                    row=row,
                    chosen_action="skip",
                    reason_code="MODEL_ALPHA_ENTRY_V1",
                    skip_reason_code="MAX_POSITIONS_TOTAL",
                )
                continue
            market = str(row.get("market", "")).strip().upper()
            if not market:
                _inc_reason(skipped_reasons, "EMPTY_MARKET")
                continue
            if market in tracked_open_markets:
                _inc_reason(skipped_reasons, "ALREADY_OPEN")
                append_entry_opportunity(
                    row=row,
                    chosen_action="skip",
                    reason_code="MODEL_ALPHA_ENTRY_V1",
                    skip_reason_code="ALREADY_OPEN",
                )
                continue
            cooldown_until = int(self._cooldown_until_ts_ms.get(market, 0))
            if ts_value < cooldown_until:
                _inc_reason(skipped_reasons, "COOLDOWN_ACTIVE")
                append_entry_opportunity(
                    row=row,
                    chosen_action="skip",
                    reason_code="MODEL_ALPHA_ENTRY_V1",
                    skip_reason_code="COOLDOWN_ACTIVE",
                )
                continue
            ref_price = _resolve_ref_price(row=row, latest_prices=latest_prices, market=market)
            if ref_price is None:
                _inc_reason(skipped_reasons, "ENTRY_REF_PRICE_MISSING")
                append_entry_opportunity(
                    row=row,
                    chosen_action="skip",
                    reason_code="MODEL_ALPHA_ENTRY_V1",
                    skip_reason_code="ENTRY_REF_PRICE_MISSING",
                )
                continue
            entry_boundary_decision = evaluate_entry_boundary(
                row=row,
                contract=entry_boundary_contract if isinstance(entry_boundary_contract, dict) else {},
            )
            if (
                not v5_post_model
                and bool(entry_boundary_decision.get("enabled"))
                and not bool(entry_boundary_decision.get("allowed"))
            ):
                skip_reason_code = (
                    str((entry_boundary_decision.get("reason_codes") or ["ENTRY_BOUNDARY_BLOCKED"])[0]).strip()
                    or "ENTRY_BOUNDARY_BLOCKED"
                )
                _inc_reason(skipped_reasons, skip_reason_code)
                append_entry_opportunity(
                    row=row,
                    chosen_action="skip",
                    reason_code="MODEL_ALPHA_ENTRY_V1",
                    skip_reason_code=skip_reason_code,
                )
                continue
            base_notional_multiplier = _resolve_entry_notional_multiplier(
                prob=float(row.get("model_prob", 0.0)),
                threshold=float(min_prob_used),
                settings=self._settings.position,
            )
            trade_action_decision = _resolve_trade_action_decision(
                policy=self._trade_action_policy,
                row=row,
                selection_score=float(row.get("model_prob", 0.0)),
                enabled=bool(self._settings.exit.use_trade_level_action_policy),
            )
            trade_action_insufficient = (
                isinstance(trade_action_decision, dict)
                and str(trade_action_decision.get("status", "")).strip().lower() == "insufficient_evidence"
            )
            if (
                not v5_post_model
                and trade_action_insufficient
            ):
                _inc_reason(
                    skipped_reasons,
                    str(trade_action_decision.get("reason_code", "TRADE_ACTION_INSUFFICIENT_EVIDENCE")).strip()
                    or "TRADE_ACTION_INSUFFICIENT_EVIDENCE",
                )
                append_entry_opportunity(
                    row=row,
                    chosen_action="skip",
                    reason_code="MODEL_ALPHA_ENTRY_V1",
                    skip_reason_code=(
                        str(trade_action_decision.get("reason_code", "TRADE_ACTION_INSUFFICIENT_EVIDENCE")).strip()
                        or "TRADE_ACTION_INSUFFICIENT_EVIDENCE"
                    ),
                    trade_action_decision=trade_action_decision,
                )
                continue
            trade_action_support_level = (
                str(trade_action_decision.get("support_level", "")).strip().lower()
                if isinstance(trade_action_decision, dict)
                else ""
            )
            trade_action_support_multiplier = (
                1.0
                if v5_post_model
                else _resolve_trade_action_support_multiplier(
                    support_level=trade_action_support_level,
                )
            )
            effective_exit_settings = (
                self._settings
                if v5_post_model
                else _resolve_trade_action_exit_settings(
                    base_settings=self._settings,
                    decision=trade_action_decision,
                )
            )
            v5_sizing_decision = (
                resolve_v5_target_notional(
                    base_budget_quote=self._settings.position.base_budget_quote,
                    final_expected_return=_safe_optional_float(row.get("final_expected_return")),
                    final_expected_es=_safe_optional_float(row.get("final_expected_es")),
                    final_tradability=_safe_optional_float(row.get("final_tradability")),
                    final_uncertainty=_safe_optional_float(row.get("final_uncertainty")),
                    final_alpha_lcb=_safe_optional_float(row.get("final_alpha_lcb")),
                )
                if v5_post_model
                else None
            )
            requested_notional_multiplier = (
                float((v5_sizing_decision or {}).get("requested_notional_multiplier", 0.0) or 0.0)
                if v5_post_model
                else (
                    float(trade_action_decision.get("recommended_notional_multiplier", base_notional_multiplier))
                    if isinstance(trade_action_decision, dict) and "recommended_notional_multiplier" in trade_action_decision
                    else float(base_notional_multiplier)
                )
            )
            if not v5_post_model:
                requested_notional_multiplier = float(requested_notional_multiplier) * float(trade_action_support_multiplier)
            risk_control_decision = _resolve_runtime_risk_control_decision(
                payload=self._risk_control_policy,
                selection_score=float(row.get("model_prob", 0.0)),
                trade_action=trade_action_decision,
                enabled=bool(self._settings.exit.use_trade_level_action_policy),
            )
            if isinstance(risk_control_decision, dict) and bool(risk_control_decision.get("enabled")) and not bool(
                risk_control_decision.get("allowed")
            ) and not v5_post_model:
                _inc_reason(
                    skipped_reasons,
                    str(risk_control_decision.get("reason_code", "RISK_CONTROL_BELOW_THRESHOLD")).strip()
                    or "RISK_CONTROL_BELOW_THRESHOLD",
                )
                append_entry_opportunity(
                    row=row,
                    chosen_action="skip",
                    reason_code="MODEL_ALPHA_ENTRY_V1",
                    skip_reason_code=(
                        str(risk_control_decision.get("reason_code", "RISK_CONTROL_BELOW_THRESHOLD")).strip()
                        or "RISK_CONTROL_BELOW_THRESHOLD"
                    ),
                    trade_action_decision=trade_action_decision,
                    notional_multiplier=float(requested_notional_multiplier),
                    support_level=trade_action_support_level,
                    support_size_multiplier=float(trade_action_support_multiplier),
                )
                continue
            size_ladder_decision = _resolve_runtime_risk_control_size_decision(
                payload=self._risk_control_policy,
                trade_action=trade_action_decision,
                requested_multiplier=requested_notional_multiplier,
                enabled=bool(self._settings.exit.use_trade_level_action_policy),
            )
            notional_multiplier = (
                float(size_ladder_decision.get("resolved_multiplier"))
                if isinstance(size_ladder_decision, dict) and size_ladder_decision.get("resolved_multiplier") is not None
                else float(requested_notional_multiplier)
            )
            if isinstance(v5_sizing_decision, dict):
                v5_sizing_decision["resolved_notional_multiplier"] = float(notional_multiplier)
            if (
                isinstance(size_ladder_decision, dict)
                and bool(size_ladder_decision.get("enabled"))
                and not bool(size_ladder_decision.get("allowed"))
            ):
                _inc_reason(
                    skipped_reasons,
                    str(size_ladder_decision.get("reason_code", "SIZE_LADDER_NO_ADMISSIBLE_MULTIPLIER")).strip()
                    or "SIZE_LADDER_NO_ADMISSIBLE_MULTIPLIER",
                )
                append_entry_opportunity(
                    row=row,
                    chosen_action="skip",
                    reason_code="MODEL_ALPHA_ENTRY_V1",
                    skip_reason_code=(
                        str(size_ladder_decision.get("reason_code", "SIZE_LADDER_NO_ADMISSIBLE_MULTIPLIER")).strip()
                        or "SIZE_LADDER_NO_ADMISSIBLE_MULTIPLIER"
                    ),
                    trade_action_decision=trade_action_decision,
                    notional_multiplier=float(notional_multiplier),
                    support_level=trade_action_support_level,
                    support_size_multiplier=float(trade_action_support_multiplier),
                )
                continue
            if float(notional_multiplier) <= 0.0:
                nonpositive_reason_code = (
                    "V5_TARGET_NOTIONAL_NONPOSITIVE"
                    if v5_post_model
                    else "TRADE_ACTION_TARGET_NOTIONAL_NONPOSITIVE"
                )
                _inc_reason(skipped_reasons, nonpositive_reason_code)
                append_entry_opportunity(
                    row=row,
                    chosen_action="skip",
                    reason_code="MODEL_ALPHA_ENTRY_V1",
                    skip_reason_code=nonpositive_reason_code,
                    trade_action_decision=trade_action_decision,
                    notional_multiplier=float(notional_multiplier),
                    support_level=trade_action_support_level,
                    support_size_multiplier=float(trade_action_support_multiplier),
                )
                continue
            if bool(self._settings.execution.use_learned_recommendations):
                dynamic_exec_profile, execution_decision = _resolve_runtime_execution_profile(
                    execution_doc=self._execution_recommendation,
                    trade_action=trade_action_decision,
                    interval_ms=self._interval_ms,
                    fallback_settings=self._settings.execution,
                    fallback_expected_edge_bps=(
                        _resolve_v5_strategy_expected_edge_bps(row=row, trade_action=trade_action_decision)
                        if v5_post_model
                        else None
                    ),
                )
            else:
                manual_timeout_ms = max(int(self._settings.execution.timeout_bars), 1) * max(int(self._interval_ms), 1)
                dynamic_exec_profile = order_exec_profile_to_dict(
                    make_legacy_exec_profile(
                        timeout_ms=manual_timeout_ms,
                        replace_interval_ms=manual_timeout_ms,
                        max_replaces=max(int(self._settings.execution.replace_max), 0),
                        price_mode=str(self._settings.execution.price_mode),
                        max_chase_bps=10_000,
                        min_replace_interval_ms_global=1_500,
                    )
                )
                execution_decision = {
                    "status": "disabled",
                    "reason_code": "EXECUTION_LEARNED_RECOMMENDATIONS_DISABLED",
                }
            safety_vetoes_payload = (
                _build_v5_strategy_safety_vetoes(
                    entry_boundary_decision=entry_boundary_decision,
                    risk_control_decision=risk_control_decision,
                    size_ladder_decision=size_ladder_decision,
                    execution_decision=execution_decision,
                )
                if v5_post_model
                else {}
            )
            if isinstance(execution_decision, dict) and str(execution_decision.get("status", "")).strip().lower() == "blocked":
                _inc_reason(
                    skipped_reasons,
                    str(execution_decision.get("reason_code", "EXECUTION_NO_TRADE_REGION")).strip()
                    or "EXECUTION_NO_TRADE_REGION",
                )
                append_entry_opportunity(
                    row=row,
                    chosen_action="skip",
                    reason_code="MODEL_ALPHA_ENTRY_V1",
                    skip_reason_code=(
                        str(execution_decision.get("reason_code", "EXECUTION_NO_TRADE_REGION")).strip()
                        or "EXECUTION_NO_TRADE_REGION"
                    ),
                    trade_action_decision=trade_action_decision,
                    execution_profile=dynamic_exec_profile,
                    execution_decision=execution_decision,
                    notional_multiplier=float(notional_multiplier),
                    support_level=trade_action_support_level,
                    support_size_multiplier=float(trade_action_support_multiplier),
                    safety_vetoes_payload=safety_vetoes_payload,
                )
                continue
            v5_expected_net_edge_bps = _resolve_v5_strategy_expected_net_edge_bps(
                row=row,
                execution_decision=execution_decision,
                trade_action=trade_action_decision,
            )
            legacy_selection_shadow = {
                "selection_min_prob_used": float(min_prob_used),
                "selection_min_prob_source": str(min_prob_source),
                "selection_top_pct_used": float(top_pct_used),
                "selection_top_pct_source": str(top_pct_source),
                "selection_min_candidates_used": int(min_candidates_used),
                "selection_min_candidates_source": str(min_candidates_source),
                "selection_policy_mode": str(selection_mode),
                "selection_policy_source": str(selection_policy_source),
                "legacy_min_prob_gate_active": bool(not v5_post_model and selection_mode != DEFAULT_SELECTION_POLICY_MODE),
                "legacy_top_pct_gate_active": bool(not v5_post_model),
            }
            entry_decision_payload = (
                build_v5_entry_decision_payload(
                    gate_payload=resolve_v5_entry_gate(
                        market=market,
                        final_expected_return=_safe_optional_float(row.get("final_expected_return")),
                        final_expected_es=_safe_optional_float(row.get("final_expected_es")),
                        final_tradability=_safe_optional_float(row.get("final_tradability")),
                        final_uncertainty=_safe_optional_float(row.get("final_uncertainty")),
                        final_alpha_lcb=_safe_optional_float(row.get("final_alpha_lcb")),
                        entry_boundary_decision=entry_boundary_decision,
                        expected_net_edge_bps=v5_expected_net_edge_bps,
                    ),
                    selected_rank=selected_rank_by_market.get(market),
                    legacy_selection_shadow=legacy_selection_shadow,
                )
                if v5_post_model
                else {}
            )
            if v5_post_model and not bool(entry_decision_payload.get("allowed", True)):
                skip_reason_code = (
                    str((entry_decision_payload.get("reason_codes") or ["ENTRY_GATE_BLOCKED"])[0]).strip()
                    or "ENTRY_GATE_BLOCKED"
                )
                _inc_reason(skipped_reasons, skip_reason_code)
                append_entry_opportunity(
                    row=row,
                    chosen_action="skip",
                    reason_code="MODEL_ALPHA_ENTRY_V1",
                    skip_reason_code=skip_reason_code,
                    trade_action_decision=trade_action_decision,
                    execution_profile=dynamic_exec_profile,
                    execution_decision=execution_decision,
                    notional_multiplier=float(notional_multiplier),
                    support_level=trade_action_support_level,
                    support_size_multiplier=float(trade_action_support_multiplier),
                    entry_decision_payload=entry_decision_payload,
                    sizing_decision_payload=v5_sizing_decision,
                    safety_vetoes_payload=safety_vetoes_payload,
                )
                continue
            exit_recommendation_meta = _build_runtime_exit_recommendation_meta(
                self._runtime_recommendation_state
            )
            liquidation_policy_payload = (
                _build_v5_liquidation_policy_payload(
                    execution_decision=execution_decision,
                    execution_profile=dynamic_exec_profile,
                )
                if v5_post_model
                else {}
            )
            exit_decision_payload = (
                _build_v5_pending_exit_decision_payload(
                    liquidation_policy_payload=liquidation_policy_payload,
                )
                if v5_post_model
                else {}
            )
            legacy_heuristics_applied = (
                _build_v5_legacy_heuristics_applied_payload(
                    trade_action_insufficient=trade_action_insufficient,
                    trade_action_support_level=trade_action_support_level,
                )
                if v5_post_model
                else {}
            )
            intent = StrategyOrderIntent(
                market=market,
                side="bid",
                ref_price=ref_price,
                reason_code="MODEL_ALPHA_ENTRY_V1",
                prob=float(row.get("model_prob", 0.0)),
                score=float(row.get("model_prob", 0.0)),
                meta={
                    "strategy": "model_alpha_v1",
                    "model_prob": float(row.get("model_prob", 0.0)),
                    "score_mean": _safe_optional_float(row.get("score_mean", row.get("model_prob"))),
                    "score_std": _safe_optional_float(row.get("score_std")),
                    "score_lcb": _safe_optional_float(row.get("score_lcb")),
                    "final_expected_return": _safe_optional_float(row.get("final_expected_return")),
                    "final_expected_es": _safe_optional_float(row.get("final_expected_es")),
                    "final_tradability": _safe_optional_float(row.get("final_tradability")),
                    "final_alpha_lcb": _safe_optional_float(row.get("final_alpha_lcb")),
                    "entry_boundary": dict(entry_boundary_decision or {}),
                    "model_prob_raw": float(row.get("model_prob_raw", row.get("model_prob", 0.0))),
                    "uncertainty": _resolve_opportunity_uncertainty(row),
                    "selection_min_prob_used": float(min_prob_used),
                    "selection_min_prob_source": str(min_prob_source),
                    "selection_top_pct_used": float(top_pct_used),
                    "selection_top_pct_source": str(top_pct_source),
                    "selection_min_candidates_used": int(min_candidates_used),
                    "selection_min_candidates_source": str(min_candidates_source),
                    "selection_policy_mode": str(selection_mode),
                    "selection_policy_source": str(selection_policy_source),
                    "support_level": trade_action_support_level or "full",
                    "support_size_multiplier": float(trade_action_support_multiplier),
                    "support_size_haircut_applied": bool(float(trade_action_support_multiplier) < 1.0),
                    "decision_contract_version": (
                        V5_POST_MODEL_CONTRACT_VERSION
                        if v5_post_model
                        else str(self._runtime_recommendations.get("decision_contract_version", "")).strip()
                    ),
                    "entry_ownership": str(self._runtime_recommendations.get("entry_ownership", "")).strip(),
                    "sizing_ownership": str(self._runtime_recommendations.get("sizing_ownership", "")).strip(),
                    "trade_action_role": str(self._runtime_recommendations.get("trade_action_role", "")).strip(),
                    "exit_ownership": str(self._runtime_recommendations.get("exit_ownership", "")).strip(),
                    "sizing_mode": (
                        "v5_portfolio_budget_first"
                        if v5_post_model
                        else (
                            "risk_control_size_ladder"
                            if isinstance(size_ladder_decision, dict) and bool(size_ladder_decision.get("enabled"))
                            else (
                                "trade_action_policy"
                                if isinstance(trade_action_decision, dict) and "recommended_notional_multiplier" in trade_action_decision
                                else str(self._settings.position.sizing_mode)
                            )
                        )
                    ),
                    "base_notional_multiplier": float(base_notional_multiplier),
                    "notional_multiplier": float(notional_multiplier) * float(operational_risk_multiplier),
                    "notional_multiplier_source": (
                        "v5_signal_haircuts"
                        if v5_post_model
                        else (
                            "risk_control_size_ladder"
                            if isinstance(size_ladder_decision, dict) and bool(size_ladder_decision.get("enabled"))
                            else (
                                "trade_action_policy"
                                if isinstance(trade_action_decision, dict) and "recommended_notional_multiplier" in trade_action_decision
                                else str(self._settings.position.sizing_mode)
                            )
                        )
                    ),
                    "expected_edge_bps": (
                        float(v5_expected_net_edge_bps)
                        if v5_post_model and v5_expected_net_edge_bps is not None
                        else _resolve_trade_action_expected_edge_bps(trade_action_decision)
                    ),
                    "expected_net_edge_bps": (
                        float(v5_expected_net_edge_bps)
                        if v5_post_model and v5_expected_net_edge_bps is not None
                        else _resolve_trade_action_expected_edge_bps(trade_action_decision)
                    ),
                    "state_features": _build_live_state_feature_snapshot(row=row),
                    "model_exit_plan": build_model_alpha_exit_plan_payload(
                        settings=effective_exit_settings,
                        row=row,
                        interval_ms=self._interval_ms,
                        observed_entry_fee_rate=0.0,
                        exit_path_risk=dict(exit_recommendation_meta.get("path_risk") or {}),
                        entry_selection_score=float(row.get("model_prob", 0.0)),
                        execution_decision=dict(execution_decision or {}),
                    ),
                    "exit_recommendation": exit_recommendation_meta,
                    "trade_action": dict(trade_action_decision or {}),
                    "risk_control_static": dict(risk_control_decision or {}),
                    "size_ladder_static": dict(size_ladder_decision or {}),
                    "execution_decision": dict(execution_decision or {}),
                    "exec_profile": dict(dynamic_exec_profile or {}),
                    "operational_overlay": dict(operational_state),
                    "entry_decision": dict(entry_decision_payload or {}),
                    "sizing_decision": dict(v5_sizing_decision or {}),
                    "safety_vetoes": dict(safety_vetoes_payload or {}),
                    "exit_decision": dict(exit_decision_payload or {}),
                    "liquidation_policy": dict(liquidation_policy_payload or {}),
                    "legacy_heuristics_applied": dict(legacy_heuristics_applied or {}),
                },
            )
            intents.append(intent)
            append_entry_opportunity(
                row=row,
                chosen_action="intent_created",
                reason_code="MODEL_ALPHA_ENTRY_V1",
                trade_action_decision=trade_action_decision,
                execution_profile=dynamic_exec_profile,
                execution_decision=execution_decision,
                notional_multiplier=float(notional_multiplier) * float(operational_risk_multiplier),
                support_level=trade_action_support_level,
                support_size_multiplier=float(trade_action_support_multiplier),
                entry_decision_payload=entry_decision_payload,
                sizing_decision_payload=v5_sizing_decision,
                safety_vetoes_payload=safety_vetoes_payload,
                exit_decision_payload=exit_decision_payload,
                liquidation_policy_payload=liquidation_policy_payload,
                legacy_heuristics_payload=legacy_heuristics_applied,
            )
            can_open -= 1

        return StrategyStepResult(
            intents=tuple(intents),
            opportunities=tuple(opportunities),
            scored_rows=scored_rows,
            eligible_rows=eligible_rows,
            selected_rows=selected_rows,
            skipped_missing_features_rows=int(missing_rows),
            dropped_min_prob_rows=dropped_min_prob_rows,
            dropped_top_pct_rows=dropped_top_pct_rows,
            blocked_min_candidates_ts=blocked_min_candidates_ts,
            min_prob_used=min_prob_used,
            min_prob_source=min_prob_source,
            top_pct_used=top_pct_used,
            top_pct_source=top_pct_source,
            min_candidates_used=min_candidates_used,
            min_candidates_source=min_candidates_source,
            selection_policy_mode=selection_mode,
            selection_policy_source=selection_policy_source,
            operational_regime_score=operational_regime_score,
            operational_risk_multiplier=operational_risk_multiplier,
            operational_max_positions=operational_max_positions,
            operational_state=operational_state,
            skipped_reasons=skipped_reasons,
        )

    def on_fill(self, event: StrategyFillEvent) -> None:
        market = str(event.market).strip().upper()
        if not market:
            return
        side = str(event.side).strip().lower()
        if side == "bid":
            price = max(float(event.price), 1e-12)
            notional = max(float(event.price) * float(event.volume), 1e-12)
            fee_rate = max(float(event.fee_quote), 0.0) / notional
            exit_plan = _extract_model_exit_plan_from_fill_meta(event.meta)
            peak_price = max(price, _safe_optional_float(exit_plan.get("high_watermark_price")) or 0.0)
            peak_price = max(peak_price, _safe_optional_float(exit_plan.get("high_watermark_price_str")) or 0.0)
            self._positions[market] = _PositionState(
                entry_ts_ms=int(event.ts_ms),
                entry_price=price,
                peak_price=peak_price,
                entry_fee_rate=fee_rate,
                qty=max(float(event.volume), 0.0),
                exit_plan=exit_plan,
            )
            return

        if side == "ask":
            self._positions.pop(market, None)
            cooldown_bars = max(int(self._settings.position.cooldown_bars), 0)
            if cooldown_bars > 0:
                self._cooldown_until_ts_ms[market] = int(event.ts_ms) + cooldown_bars * self._interval_ms

    def _resolve_exit_reason(
        self,
        *,
        ts_ms: int,
        market: str,
        state: _PositionState,
        ref_price: float,
        row: dict[str, Any] | None,
    ) -> str | None:
        plan = _resolve_position_exit_plan(
            market=market,
            state=state,
            positions=self._positions,
            settings=self._settings,
            ts_ms=ts_ms,
            current_price=ref_price,
            row=row,
            interval_ms=self._interval_ms,
        )
        state.exit_plan = dict(plan)
        state.exit_plan["last_price"] = float(ref_price)
        mode = str(plan.get("mode", "hold")).strip().lower() or "hold"
        hold_ms = max(int(plan.get("timeout_delta_ms", 0) or 0), 0)
        tp_pct = max(float(plan.get("tp_pct", 0.0) or 0.0), 0.0)
        sl_pct = max(float(plan.get("sl_pct", 0.0) or 0.0), 0.0)
        trailing_pct = max(float(plan.get("trailing_pct", 0.0) or 0.0), 0.0)
        exit_fee_rate = max(float(plan.get("expected_exit_fee_rate", 0.0) or 0.0), 0.0)
        exit_slippage_bps = max(float(plan.get("expected_exit_slippage_bps", 0.0) or 0.0), 0.0)
        if mode == "hold":
            entry_price = max(float(state.entry_price), 1e-12)
            if sl_pct > 0:
                net_return = _net_return_after_costs(
                    entry_price=entry_price,
                    exit_price=float(ref_price),
                    entry_fee_rate=max(float(state.entry_fee_rate), 0.0),
                    exit_fee_rate=exit_fee_rate,
                    exit_slippage_bps=exit_slippage_bps,
                )
                if self._is_v5_post_model_contract:
                    exit_decision = resolve_v5_exit_decision(
                        continuation_guidance={},
                        net_return_ratio=float(net_return),
                        trailing_drawdown_ratio=None,
                        stop_loss_ratio=float(sl_pct),
                        trailing_ratio=0.0,
                        timeout_elapsed=bool(hold_ms > 0 and int(ts_ms) - int(state.entry_ts_ms) >= hold_ms),
                        mode=mode,
                    )
                    state.exit_plan["exit_decision"] = dict(exit_decision)
                    if bool(exit_decision.get("should_exit")):
                        return str(exit_decision.get("decision_reason_code", "")).strip() or None
                elif net_return <= -sl_pct:
                    return "MODEL_ALPHA_EXIT_SL"
            if hold_ms > 0 and int(ts_ms) - int(state.entry_ts_ms) >= hold_ms:
                return V5_STALE_TIMEOUT_EXIT_REASON if self._is_v5_post_model_contract else "MODEL_ALPHA_EXIT_HOLD_TIMEOUT"
            return None

        # risk mode
        state.peak_price = max(float(state.peak_price), float(ref_price))
        state.exit_plan["high_watermark_price"] = float(state.peak_price)
        state.exit_plan["high_watermark_price_str"] = str(state.peak_price)
        entry_price = max(float(state.entry_price), 1e-12)
        net_return = _net_return_after_costs(
            entry_price=entry_price,
            exit_price=float(ref_price),
            entry_fee_rate=max(float(state.entry_fee_rate), 0.0),
            exit_fee_rate=exit_fee_rate,
            exit_slippage_bps=exit_slippage_bps,
        )
        path_risk_inputs = build_path_risk_runtime_inputs(
            market=market,
            entry_price=entry_price,
            current_price=float(ref_price),
            selection_score=_safe_optional_float(row.get("model_prob")) if isinstance(row, dict) else None,
            risk_feature_value=_resolve_row_risk_volatility(
                row=row,
                feature_name=str(plan.get("risk_vol_feature", "")).strip(),
            ),
            positions=list(self._positions.values()),
            base_budget_quote=self._settings.position.base_budget_quote,
            max_positions_total=max(int(self._settings.position.max_positions_total), 1),
        )
        continuation_guidance = resolve_path_risk_guidance_from_plan(
            plan_payload=plan,
            elapsed_bars=max(
                int((int(ts_ms) - int(state.entry_ts_ms)) // max(int(plan.get("bar_interval_ms", self._interval_ms) or self._interval_ms), 1)),
                0,
            ),
            **path_risk_inputs,
        )
        trailing_drawdown = _net_drawdown_from_peak_after_costs(
            peak_price=max(float(state.peak_price), entry_price),
            current_price=float(ref_price),
            exit_fee_rate=exit_fee_rate,
            exit_slippage_bps=exit_slippage_bps,
        )
        if self._is_v5_post_model_contract:
            exit_decision = resolve_v5_exit_decision(
                continuation_guidance=continuation_guidance,
                net_return_ratio=float(net_return),
                trailing_drawdown_ratio=float(trailing_drawdown),
                stop_loss_ratio=float(sl_pct),
                trailing_ratio=float(trailing_pct),
                timeout_elapsed=bool(hold_ms > 0 and int(ts_ms) - int(state.entry_ts_ms) >= hold_ms),
                mode=mode,
            )
            state.exit_plan["exit_decision"] = dict(exit_decision)
            if bool(exit_decision.get("should_exit")):
                return str(exit_decision.get("decision_reason_code", "")).strip() or None
            return None
        if tp_pct > 0 and net_return >= tp_pct:
            return "MODEL_ALPHA_EXIT_TP"
        if sl_pct > 0 and net_return <= -sl_pct:
            return "MODEL_ALPHA_EXIT_SL"
        if bool(continuation_guidance.get("continuation_should_exit")):
            return "MODEL_ALPHA_EXIT_CONTINUATION"
        if trailing_pct > 0 and trailing_drawdown >= trailing_pct:
            return "MODEL_ALPHA_EXIT_TRAILING"
        if hold_ms > 0 and int(ts_ms) - int(state.entry_ts_ms) >= hold_ms:
            return "MODEL_ALPHA_EXIT_TIMEOUT"
        return None

    def _advance_to_ts(self, target_ts_ms: int) -> None:
        while True:
            if self._pending_group is not None and int(self._pending_group.ts_ms) >= int(target_ts_ms):
                return
            try:
                self._pending_group = next(self._feature_iter)
            except StopIteration:
                self._pending_group = None
                return

    def _take_group_if_equal(self, ts_ms: int) -> pl.DataFrame | None:
        if self._pending_group is None:
            return None
        if int(self._pending_group.ts_ms) != int(ts_ms):
            return None
        frame = self._pending_group.frame
        self._pending_group = None
        return frame


def resolve_model_alpha_runtime_row_columns(*, predictor: ModelPredictor) -> tuple[str, ...]:
    return _runtime_contract.resolve_model_alpha_runtime_row_columns(predictor=predictor)


def _resolve_ref_price(*, row: dict[str, Any] | None, latest_prices: dict[str, float], market: str) -> float | None:
    close = None if row is None else row.get("close")
    if close is not None:
        try:
            close_value = float(close)
        except (TypeError, ValueError):
            close_value = 0.0
        if close_value > 0:
            return close_value
    latest = latest_prices.get(str(market).strip().upper())
    if latest is None:
        return None
    try:
        latest_value = float(latest)
    except (TypeError, ValueError):
        return None
    return latest_value if latest_value > 0 else None


def _inc_reason(reason_counts: dict[str, int], reason: str, delta: int = 1) -> None:
    key = str(reason).strip().upper()
    if not key:
        return
    reason_counts[key] = int(reason_counts.get(key, 0)) + int(delta)


def _resolve_selection_min_prob(
    *,
    predictor: ModelPredictor,
    settings: ModelAlphaSelectionSettings,
) -> tuple[float, str]:
    return _runtime_contract.resolve_selection_min_prob(predictor=predictor, settings=settings)


def _resolve_selection_top_pct(
    *,
    predictor: ModelPredictor,
    settings: ModelAlphaSelectionSettings,
    selection_policy: dict[str, Any] | None = None,
    selection_policy_source: str = "manual",
) -> tuple[float, str]:
    return _runtime_contract.resolve_selection_top_pct(
        predictor=predictor,
        settings=settings,
        selection_policy=selection_policy,
        selection_policy_source=selection_policy_source,
    )


def _resolve_selection_min_candidates(
    *,
    predictor: ModelPredictor,
    settings: ModelAlphaSelectionSettings,
    selection_policy: dict[str, Any] | None = None,
    selection_policy_source: str = "manual",
) -> tuple[int, str]:
    return _runtime_contract.resolve_selection_min_candidates(
        predictor=predictor,
        settings=settings,
        selection_policy=selection_policy,
        selection_policy_source=selection_policy_source,
    )


def _resolve_selection_policy(
    *,
    predictor: ModelPredictor,
    settings: ModelAlphaSelectionSettings,
) -> tuple[dict[str, Any], str]:
    return _runtime_contract.resolve_selection_policy(predictor=predictor, settings=settings)


def resolve_runtime_model_alpha_settings(
    *,
    predictor: ModelPredictor,
    settings: ModelAlphaSettings,
) -> tuple[ModelAlphaSettings, dict[str, Any]]:
    return _runtime_contract.resolve_runtime_model_alpha_settings(predictor=predictor, settings=settings)


def _resolve_trade_action_decision(
    *,
    policy: dict[str, Any] | None,
    row: dict[str, Any] | None,
    selection_score: float,
    enabled: bool,
) -> dict[str, Any] | None:
    if not enabled:
        return None
    decision = resolve_trade_action(
        policy,
        selection_score=float(selection_score),
        row=row,
    )
    if not isinstance(decision, dict) or not decision:
        return None
    return decision


def _resolve_runtime_risk_control_decision(
    *,
    payload: dict[str, Any] | None,
    selection_score: float,
    trade_action: dict[str, Any] | None,
    enabled: bool,
) -> dict[str, Any] | None:
    if not enabled:
        return None
    decision = resolve_execution_risk_control_decision(
        risk_control_payload=payload,
        selection_score=float(selection_score),
        trade_action=trade_action,
    )
    if not isinstance(decision, dict) or not decision:
        return None
    return decision


def _resolve_runtime_risk_control_size_decision(
    *,
    payload: dict[str, Any] | None,
    trade_action: dict[str, Any] | None,
    requested_multiplier: float,
    enabled: bool,
) -> dict[str, Any] | None:
    if not enabled:
        return None
    decision = resolve_execution_risk_control_size_decision(
        risk_control_payload=payload,
        trade_action=trade_action,
        requested_multiplier=float(requested_multiplier),
    )
    if not isinstance(decision, dict) or not decision:
        return None
    return decision


def _resolve_trade_action_exit_settings(
    *,
    base_settings: ModelAlphaSettings,
    decision: dict[str, Any] | None,
) -> ModelAlphaSettings:
    if not isinstance(decision, dict):
        return base_settings
    template = dict(decision.get("exit_policy_template") or {})
    action = str(decision.get("recommended_action", "")).strip().lower()
    if action not in {"hold", "risk"} or not template:
        return base_settings
    resolved_exit = replace(
        base_settings.exit,
        mode=action,
        hold_bars=max(int(template.get("hold_bars", base_settings.exit.hold_bars) or base_settings.exit.hold_bars), 1),
        risk_scaling_mode=str(template.get("risk_scaling_mode", base_settings.exit.risk_scaling_mode)).strip().lower()
        or str(base_settings.exit.risk_scaling_mode),
        risk_vol_feature=str(template.get("risk_vol_feature", base_settings.exit.risk_vol_feature)).strip()
        or str(base_settings.exit.risk_vol_feature),
        tp_vol_multiplier=_safe_optional_float(template.get("tp_vol_multiplier", base_settings.exit.tp_vol_multiplier)),
        sl_vol_multiplier=_safe_optional_float(template.get("sl_vol_multiplier", base_settings.exit.sl_vol_multiplier)),
        trailing_vol_multiplier=_safe_optional_float(
            template.get("trailing_vol_multiplier", base_settings.exit.trailing_vol_multiplier)
        ),
        tp_pct=(
            float(template.get("tp_pct"))
            if template.get("tp_pct") is not None
            else float(base_settings.exit.tp_pct)
        ),
        sl_pct=(
            float(template.get("sl_pct"))
            if template.get("sl_pct") is not None
            else float(base_settings.exit.sl_pct)
        ),
        trailing_pct=(
            float(template.get("trailing_pct"))
            if template.get("trailing_pct") is not None
            else float(base_settings.exit.trailing_pct)
        ),
        expected_exit_fee_bps=(
            float(template.get("expected_exit_fee_rate", 0.0) or 0.0) * 10_000.0
            if template.get("expected_exit_fee_rate") is not None
            else base_settings.exit.expected_exit_fee_bps
        ),
        expected_exit_slippage_bps=(
            float(template.get("expected_exit_slippage_bps", 0.0) or 0.0)
            if template.get("expected_exit_slippage_bps") is not None
            else base_settings.exit.expected_exit_slippage_bps
        ),
    )
    return replace(base_settings, exit=resolved_exit)


def _build_runtime_exit_recommendation_meta(runtime_state: dict[str, Any] | None) -> dict[str, Any]:
    return _runtime_contract.build_runtime_exit_recommendation_meta(runtime_state)


def _extract_model_exit_plan_from_fill_meta(meta: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(meta, dict):
        return {}
    payload = meta.get("model_exit_plan")
    if isinstance(payload, dict):
        return normalize_model_exit_plan_payload(payload)
    strategy_meta = meta.get("strategy")
    if isinstance(strategy_meta, dict):
        nested = strategy_meta.get("meta")
        if isinstance(nested, dict) and isinstance(nested.get("model_exit_plan"), dict):
            return normalize_model_exit_plan_payload(nested.get("model_exit_plan"))
    return {}


def _resolve_position_exit_plan(
    *,
    market: str,
    state: _PositionState,
    positions: dict[str, _PositionState] | None,
    settings: ModelAlphaSettings,
    ts_ms: int,
    current_price: float | None,
    row: dict[str, Any] | None,
    interval_ms: int,
) -> dict[str, Any]:
    base_plan = (
        dict(state.exit_plan)
        if isinstance(state.exit_plan, dict) and state.exit_plan
        else build_model_alpha_exit_plan_payload(
            settings=settings,
            row=row,
            interval_ms=interval_ms,
            observed_entry_fee_rate=max(float(state.entry_fee_rate), 0.0),
        )
    )
    return _reprice_position_exit_plan(
        market=market,
        state=state,
        plan=base_plan,
        row=row,
        ts_ms=ts_ms,
        current_price=current_price,
        positions=positions,
        settings=settings,
        operational_settings=settings.operational,
        interval_ms=interval_ms,
    )


def _resolve_entry_notional_multiplier(
    *,
    prob: float,
    threshold: float,
    settings: ModelAlphaPositionSettings,
) -> float:
    mode = str(settings.sizing_mode).strip().lower() or "prob_ramp"
    if mode == "fixed":
        return 1.0

    min_multiplier = max(float(settings.size_multiplier_min), 0.0)
    max_multiplier = max(float(settings.size_multiplier_max), min_multiplier)
    if mode != "prob_ramp":
        return 1.0

    prob_value = _clamp_prob(prob)
    threshold_value = _clamp_prob(threshold)
    conviction_span = max(1.0 - threshold_value, 1e-12)
    conviction = max(prob_value - threshold_value, 0.0) / conviction_span
    conviction = max(min(conviction, 1.0), 0.0)
    return min_multiplier + (conviction * (max_multiplier - min_multiplier))


def _clamp_prob(value: float) -> float:
    return max(min(float(value), 1.0), 0.0)


def _resolve_trade_action_support_multiplier(*, support_level: str) -> float:
    normalized = str(support_level).strip().lower()
    if normalized == "fallback_bin":
        return 0.5
    return 1.0


def _resolve_runtime_execution_profile(
    *,
    execution_doc: dict[str, Any] | None,
    trade_action: dict[str, Any] | None,
    interval_ms: int,
    fallback_settings: ModelAlphaExecutionSettings,
    fallback_expected_edge_bps: float | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    execution_payload = dict(execution_doc or {})
    stage_order = [
        str(value).strip().upper()
        for value in (execution_payload.get("stage_order") or _EXECUTION_STAGE_ORDER)
        if str(value).strip()
    ]
    if not stage_order:
        stage_order = list(_EXECUTION_STAGE_ORDER)
    stage_docs = [
        dict(item)
        for item in (execution_payload.get("stages") or [])
        if isinstance(item, dict) and str(item.get("stage", "")).strip()
    ]
    stage_by_name = {
        str(item.get("stage", "")).strip().upper(): item
        for item in stage_docs
        if str(item.get("stage", "")).strip()
    }
    expected_edge_bps = _resolve_trade_action_expected_edge_bps(trade_action=trade_action)
    if expected_edge_bps is None:
        expected_edge_bps = _safe_optional_float(fallback_expected_edge_bps)
    decision: dict[str, Any] = {
        "policy": str(execution_payload.get("policy", "")).strip(),
        "stage_decision_mode": str(execution_payload.get("stage_decision_mode", "")).strip(),
        "stage_order": list(stage_order),
        "expected_edge_bps": expected_edge_bps,
        "evaluated_stages": [],
        "status": "fallback",
        "reason_code": "EXECUTION_DYNAMIC_FRONTIER_UNAVAILABLE",
    }
    if not stage_docs:
        decision["status"] = "frontier_missing"
        decision["reason_code"] = "EXECUTION_STAGE_FRONTIER_MISSING"
        return {}, decision
    if expected_edge_bps is None:
        decision["status"] = "edge_missing"
        decision["reason_code"] = "EXECUTION_EDGE_MISSING"
        return {}, decision
    positive_edge_required = bool(
        ((execution_payload.get("no_trade_region") or {}).get("positive_net_edge_required", True))
    )
    selected_stage: dict[str, Any] | None = None
    for stage_name in stage_order:
        stage = stage_by_name.get(stage_name)
        if not isinstance(stage, dict) or not bool(stage.get("supported", False)):
            continue
        fill_probability = _clamp01(_safe_optional_float(stage.get("expected_fill_probability")) or 0.0)
        slippage_bps = max(_safe_optional_float(stage.get("expected_slippage_bps")) or 0.0, 0.0)
        cleanup_cost_bps = max(_safe_optional_float(stage.get("expected_cleanup_cost_bps")) or 0.0, 0.0)
        miss_cost_bps = max(_safe_optional_float(stage.get("expected_miss_cost_bps")) or cleanup_cost_bps, 0.0)
        net_edge_bps = (float(expected_edge_bps) * float(fill_probability)) - float(slippage_bps)
        evaluation = {
            "stage": stage_name,
            "fill_probability": float(fill_probability),
            "expected_slippage_bps": float(slippage_bps),
            "expected_cleanup_cost_bps": float(cleanup_cost_bps),
            "expected_miss_cost_bps": float(miss_cost_bps),
            "expected_time_to_fill_ms": _safe_optional_float(stage.get("expected_time_to_fill_ms")),
            "net_edge_bps": float(net_edge_bps),
            "validation_comparable": bool(stage.get("validation_comparable", False)),
        }
        decision["evaluated_stages"].append(evaluation)
        if float(net_edge_bps) > 0.0 and selected_stage is None:
            selected_stage = stage
            decision["selected_stage"] = stage_name
            decision["selected_net_edge_bps"] = float(net_edge_bps)
            decision["selected_fill_probability"] = float(fill_probability)
            decision["selected_expected_slippage_bps"] = float(slippage_bps)
            decision["selected_expected_cleanup_cost_bps"] = float(cleanup_cost_bps)
            decision["selected_expected_miss_cost_bps"] = float(miss_cost_bps)
            decision["selected_expected_time_to_fill_ms"] = _safe_optional_float(stage.get("expected_time_to_fill_ms"))
            decision["selected_price_mode"] = str(stage.get("recommended_price_mode", "")).strip().upper()
            decision["status"] = "selected"
            decision["reason_code"] = f"EXECUTION_STAGE_{stage_name}"
    if selected_stage is None:
        if positive_edge_required:
            decision["status"] = "blocked"
            decision["reason_code"] = "EXECUTION_NO_TRADE_REGION"
            return {}, decision
        selected_stage = next(
            (
                stage_by_name.get(stage_name)
                for stage_name in stage_order
                if isinstance(stage_by_name.get(stage_name), dict)
                and bool((stage_by_name.get(stage_name) or {}).get("supported", False))
            ),
            None,
        )
    if not isinstance(selected_stage, dict):
        return {}, decision
    timeout_bars = max(
        _safe_optional_int(selected_stage.get("recommended_timeout_bars")) or int(fallback_settings.timeout_bars),
        1,
    )
    replace_max = max(
        _safe_optional_int(selected_stage.get("recommended_replace_max")) or int(fallback_settings.replace_max),
        0,
    )
    price_mode = (
        str(selected_stage.get("recommended_price_mode", fallback_settings.price_mode)).strip().upper()
        or str(fallback_settings.price_mode).strip().upper()
    )
    profile = make_legacy_exec_profile(
        timeout_ms=max(int(interval_ms), 1) * int(timeout_bars),
        replace_interval_ms=max(int(interval_ms), 1) * int(timeout_bars),
        max_replaces=int(replace_max),
        price_mode=price_mode,
        max_chase_bps=10_000,
        min_replace_interval_ms_global=1_500,
    )
    decision["selected_timeout_bars"] = int(timeout_bars)
    decision["selected_replace_max"] = int(replace_max)
    decision["selected_price_mode"] = str(price_mode).strip().upper()
    return order_exec_profile_to_dict(profile), decision


def _clamp01(value: float) -> float:
    return max(min(float(value), 1.0), 0.0)


def _safe_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def build_model_alpha_exit_plan_payload(
    *,
    settings: ModelAlphaSettings,
    row: dict[str, Any] | None,
    interval_ms: int,
    observed_entry_fee_rate: float = 0.0,
    exit_path_risk: dict[str, Any] | None = None,
    entry_selection_score: float | None = None,
    execution_decision: dict[str, Any] | None = None,
) -> dict[str, Any]:
    mode = str(settings.exit.mode).strip().lower() or "hold"
    hold_bars = max(int(settings.exit.hold_bars), 0)
    timeout_delta_ms = hold_bars * max(int(interval_ms), 1) if hold_bars > 0 else 0
    tp_pct, sl_pct, trailing_pct = _resolve_runtime_risk_exit_thresholds(
        settings=settings,
        row=row,
    )
    exit_fee_rate, exit_slippage_bps = _resolve_expected_exit_costs(
        settings=settings,
        observed_entry_fee_rate=max(float(observed_entry_fee_rate), 0.0),
    )
    min_tp_floor_pct = _resolve_min_tp_floor_pct(
        entry_fee_rate=max(float(observed_entry_fee_rate), 0.0),
        exit_fee_rate=float(exit_fee_rate),
        exit_slippage_bps=float(exit_slippage_bps),
    )
    immediate_execution_costs = _resolve_immediate_exit_execution_costs(
        execution_decision=execution_decision,
        fallback_exit_fee_rate=float(exit_fee_rate),
        fallback_exit_slippage_bps=float(exit_slippage_bps),
    )
    return normalize_model_exit_plan_payload(
        {
            "source": "model_alpha_v1",
            "version": 1,
            "mode": mode,
            "hold_bars": hold_bars,
            "interval_ms": max(int(interval_ms), 1),
            "timeout_delta_ms": int(timeout_delta_ms),
            "tp_pct": float(tp_pct) if mode == "risk" and float(tp_pct) > 0.0 else 0.0,
            "sl_pct": float(sl_pct) if float(sl_pct) > 0.0 else 0.0,
            "trailing_pct": float(trailing_pct) if mode == "risk" and float(trailing_pct) > 0.0 else 0.0,
            "base_tp_pct": float(tp_pct) if mode == "risk" and float(tp_pct) > 0.0 else 0.0,
            "base_sl_pct": float(sl_pct) if float(sl_pct) > 0.0 else 0.0,
            "base_trailing_pct": float(trailing_pct) if mode == "risk" and float(trailing_pct) > 0.0 else 0.0,
            "base_timeout_delta_ms": int(timeout_delta_ms),
            "min_tp_floor_pct": float(min_tp_floor_pct),
            "expected_exit_fee_rate": float(exit_fee_rate),
            "expected_exit_slippage_bps": float(exit_slippage_bps),
            "expected_immediate_exit_fee_rate": float(immediate_execution_costs["fee_rate"]),
            "expected_immediate_exit_slippage_bps": float(immediate_execution_costs["slippage_bps"]),
            "expected_immediate_exit_fill_probability": float(immediate_execution_costs["fill_probability"]),
            "expected_immediate_exit_time_to_fill_ms": immediate_execution_costs["time_to_fill_ms"],
            "expected_immediate_exit_price_mode": str(immediate_execution_costs["price_mode"]),
            "expected_immediate_exit_cleanup_cost_bps": float(immediate_execution_costs["cleanup_cost_bps"]),
            "expected_immediate_exit_miss_cost_bps": float(immediate_execution_costs["miss_cost_bps"]),
            "expected_immediate_exit_cost_ratio": float(immediate_execution_costs["cost_ratio"]),
            "expected_liquidation_cost": float(immediate_execution_costs["cost_ratio"]),
            "risk_scaling_mode": str(settings.exit.risk_scaling_mode),
            "risk_vol_feature": str(settings.exit.risk_vol_feature),
            "tp_vol_multiplier": _safe_optional_float(settings.exit.tp_vol_multiplier),
            "sl_vol_multiplier": _safe_optional_float(settings.exit.sl_vol_multiplier),
            "trailing_vol_multiplier": _safe_optional_float(settings.exit.trailing_vol_multiplier),
            "entry_selection_score": _safe_optional_float(entry_selection_score),
            "entry_risk_feature_value": _resolve_row_risk_volatility(row=row, feature_name=settings.exit.risk_vol_feature),
            "use_learned_exit_mode": bool(settings.exit.use_learned_exit_mode),
            "use_learned_hold_bars": bool(settings.exit.use_learned_hold_bars),
            "use_learned_risk_recommendations": bool(settings.exit.use_learned_risk_recommendations),
            "use_trade_level_action_policy": bool(settings.exit.use_trade_level_action_policy),
            "path_risk": dict(exit_path_risk or {}) if isinstance(exit_path_risk, dict) else {},
            "continue_value_lcb": None,
            "exit_now_value_net": None,
            "alpha_decay_penalty": None,
        }
    )


def _reprice_position_exit_plan(
    *,
    market: str,
    state: _PositionState,
    plan: dict[str, Any],
    row: dict[str, Any] | None,
    ts_ms: int,
    current_price: float | None,
    positions: dict[str, _PositionState] | None,
    settings: ModelAlphaSettings,
    operational_settings: ModelAlphaOperationalSettings,
    interval_ms: int,
) -> dict[str, Any]:
    normalized = normalize_model_exit_plan_payload(plan)
    mode = str(normalized.get("mode", "hold")).strip().lower() or "hold"
    scaling_mode = str(normalized.get("risk_scaling_mode", "")).strip().lower() or "fixed"
    if mode != "risk":
        return normalized
    base_tp_pct = _safe_optional_float(normalized.get("base_tp_pct"))
    base_sl_pct = _safe_optional_float(normalized.get("base_sl_pct"))
    base_trailing_pct = _safe_optional_float(normalized.get("base_trailing_pct"))
    current_trailing_pct = _safe_optional_float(normalized.get("trailing_pct"))
    base_timeout_delta_ms = _safe_optional_int(normalized.get("base_timeout_delta_ms"))
    overlay_tp_basis = base_tp_pct if base_tp_pct is not None else _safe_optional_float(normalized.get("tp_pct"))
    overlay_sl_basis = base_sl_pct if base_sl_pct is not None else _safe_optional_float(normalized.get("sl_pct"))
    overlay_trailing_basis = (
        current_trailing_pct
        if current_trailing_pct is not None and current_trailing_pct > 0.0
        else (base_trailing_pct if base_trailing_pct is not None else current_trailing_pct)
    )
    overlay_timeout_basis = (
        base_timeout_delta_ms
        if base_timeout_delta_ms is not None
        else _safe_optional_int(normalized.get("timeout_delta_ms"))
    )
    if scaling_mode == "volatility_scaled":
        risk_vol_feature = str(normalized.get("risk_vol_feature", "")).strip()
        tp_mult = _safe_optional_float(normalized.get("tp_vol_multiplier"))
        sl_mult = _safe_optional_float(normalized.get("sl_vol_multiplier"))
        trailing_mult = _safe_optional_float(normalized.get("trailing_vol_multiplier"))
        if risk_vol_feature and any(value is not None for value in (tp_mult, sl_mult, trailing_mult)):
            sigma_bar = _resolve_row_risk_volatility(row=row, feature_name=risk_vol_feature)
            if sigma_bar is not None and sigma_bar > 0:
                total_hold_bars = max(int(normalized.get("hold_bars", 0) or 0), 1)
                plan_interval_ms = max(int(normalized.get("bar_interval_ms", interval_ms) or interval_ms), 1)
                elapsed_bars = max(int((int(ts_ms) - int(state.entry_ts_ms)) // plan_interval_ms), 0)
                remaining_bars = max(total_hold_bars - elapsed_bars, 1)
                sigma_horizon = float(sigma_bar) * math.sqrt(float(remaining_bars))
                if math.isfinite(sigma_horizon) and sigma_horizon > 0:
                    repriced_tp = max(float(tp_mult) * sigma_horizon, 0.0) if tp_mult is not None else None
                    repriced_sl = max(float(sl_mult) * sigma_horizon, 0.0) if sl_mult is not None else None
                    repriced_trailing = max(float(trailing_mult) * sigma_horizon, 0.0) if trailing_mult is not None else None

                    tp_pct = _tighten_only_exit_threshold(normalized.get("tp_pct"), repriced_tp)
                    sl_pct = _tighten_only_exit_threshold(normalized.get("sl_pct"), repriced_sl)
                    trailing_pct = _tighten_only_exit_threshold(normalized.get("trailing_pct"), repriced_trailing)
                    normalized.update(
                        {
                            "tp_ratio": float(tp_pct),
                            "sl_ratio": float(sl_pct),
                            "trailing_ratio": float(trailing_pct),
                            "tp_pct": float(tp_pct),
                            "sl_pct": float(sl_pct),
                            "trailing_pct": float(trailing_pct),
                            "intratrade_reprice_applied": True,
                            "intratrade_remaining_bars": int(remaining_bars),
                            "intratrade_sigma_bar": float(sigma_bar),
                            "intratrade_sigma_horizon": float(sigma_horizon),
                        }
                    )
                    overlay_tp_basis = float(tp_pct)
                    overlay_sl_basis = float(sl_pct)
                    overlay_trailing_basis = float(trailing_pct)
    plan_interval_ms = max(int(normalized.get("bar_interval_ms", interval_ms) or interval_ms), 1)
    elapsed_bars = max(int((int(ts_ms) - int(state.entry_ts_ms)) // plan_interval_ms), 0)
    path_risk_inputs = build_path_risk_runtime_inputs(
        market=market,
        entry_price=float(state.entry_price),
        current_price=float(current_price if current_price is not None else state.entry_price),
        selection_score=_safe_optional_float(row.get("model_prob")) if isinstance(row, dict) else None,
        risk_feature_value=_resolve_row_risk_volatility(
            row=row,
            feature_name=str(normalized.get("risk_vol_feature", "")).strip(),
        ),
        positions=list((positions or {}).values()),
        base_budget_quote=settings.position.base_budget_quote,
        max_positions_total=max(int(settings.position.max_positions_total), 1),
    )
    path_risk_guidance = resolve_path_risk_guidance_from_plan(
        plan_payload=normalized,
        elapsed_bars=elapsed_bars,
        **path_risk_inputs,
    )
    if bool(path_risk_guidance.get("applied")):
        guided_tp = _safe_optional_float(path_risk_guidance.get("reachable_tp_ratio"))
        guided_sl = _safe_optional_float(path_risk_guidance.get("bounded_sl_ratio"))
        if guided_tp is not None and guided_tp > 0.0:
            tightened_tp = _tighten_only_exit_threshold(normalized.get("tp_pct"), guided_tp)
            normalized["tp_ratio"] = float(tightened_tp)
            normalized["tp_pct"] = float(tightened_tp)
            overlay_tp_basis = float(tightened_tp)
        if guided_sl is not None and guided_sl > 0.0:
            tightened_sl = _tighten_only_exit_threshold(normalized.get("sl_pct"), guided_sl)
            normalized["sl_ratio"] = float(tightened_sl)
            normalized["sl_pct"] = float(tightened_sl)
            overlay_sl_basis = float(tightened_sl)
        normalized["path_risk_applied"] = True
        normalized["path_risk_selected_hold_bars"] = _safe_optional_int(path_risk_guidance.get("selected_hold_bars"))
        normalized["path_risk_reachable_tp"] = guided_tp
        normalized["path_risk_bounded_sl"] = guided_sl
        normalized["path_risk_terminal_return_q50"] = _safe_optional_float(path_risk_guidance.get("terminal_return_q50"))
        normalized["path_risk_terminal_return_q75"] = _safe_optional_float(path_risk_guidance.get("terminal_return_q75"))
        normalized["continue_value_lcb"] = _safe_optional_float(path_risk_guidance.get("continue_value_lcb"))
        normalized["exit_now_value_net"] = _safe_optional_float(path_risk_guidance.get("exit_now_value_net"))
        normalized["expected_liquidation_cost"] = _safe_optional_float(path_risk_guidance.get("immediate_exit_cost_ratio"))
        normalized["alpha_decay_penalty"] = _safe_optional_float(path_risk_guidance.get("alpha_decay_penalty_ratio"))
    micro_snapshot = _micro_snapshot_from_row(row=row, ts_ms=ts_ms)
    current_return_ratio = (
        (float(current_price) / max(float(state.entry_price), 1e-12)) - 1.0
        if current_price is not None and float(current_price) > 0.0
        else 0.0
    )
    overlay = resolve_dynamic_exit_overlay(
        settings=operational_settings,
        micro_snapshot=micro_snapshot,
        now_ts_ms=int(ts_ms),
        current_return_ratio=float(current_return_ratio),
        elapsed_ms=max(int(ts_ms) - int(state.entry_ts_ms), 0),
        tp_pct=overlay_tp_basis,
        sl_pct=overlay_sl_basis,
        trailing_enabled=bool((overlay_trailing_basis or 0.0) > 0.0),
        trailing_pct=overlay_trailing_basis,
        timeout_delta_ms=overlay_timeout_basis,
    )
    if overlay.applied:
        normalized.update(
            {
                "tp_ratio": float(overlay.tp_pct or 0.0),
                "sl_ratio": float(overlay.sl_pct or 0.0),
                "trailing_ratio": float(overlay.trailing_pct or 0.0),
                "tp_pct": float(overlay.tp_pct or 0.0),
                "sl_pct": float(overlay.sl_pct or 0.0),
                "trailing_pct": float(overlay.trailing_pct or 0.0),
                "timeout_delta_ms": int(overlay.timeout_delta_ms or 0),
                "micro_overlay_applied": True,
                "micro_overlay_quality_score": overlay.quality_score,
                "micro_overlay_trade_imbalance": overlay.trade_imbalance,
                "micro_overlay_activation_strength": overlay.activation_strength,
                "micro_overlay_risk_multiplier": overlay.risk_multiplier,
            }
        )
    return normalized


def _tighten_only_exit_threshold(entry_value: Any, repriced_value: float | None) -> float:
    entry_threshold = max(_safe_optional_float(entry_value) or 0.0, 0.0)
    next_threshold = max(_safe_optional_float(repriced_value) or 0.0, 0.0)
    if entry_threshold <= 0.0 or next_threshold <= 0.0:
        return float(entry_threshold)
    return float(min(entry_threshold, next_threshold))


def _micro_snapshot_from_row(*, row: dict[str, Any] | None, ts_ms: int) -> MicroSnapshot | None:
    if not isinstance(row, dict):
        return None
    trade_events = max(_safe_optional_int(row.get("m_trade_events")) or 0, 0)
    book_events = max(_safe_optional_int(row.get("m_book_events")) or 0, 0)
    trade_coverage_ms = max(_safe_optional_int(row.get("m_trade_coverage_ms")) or 0, 0)
    book_coverage_ms = max(_safe_optional_int(row.get("m_book_coverage_ms")) or 0, 0)
    trade_imbalance = _safe_optional_float(row.get("m_trade_imbalance"))
    spread_bps = _safe_optional_float(row.get("m_spread_proxy"))
    bid_depth = _safe_optional_float(row.get("m_depth_bid_top5_mean"))
    ask_depth = _safe_optional_float(row.get("m_depth_ask_top5_mean"))
    depth_top5_notional_krw = _safe_optional_float(row.get("m_depth_top5_notional_krw"))
    if depth_top5_notional_krw is None:
        depth_top5_notional_krw = max(float(bid_depth or 0.0) + float(ask_depth or 0.0), 0.0)
    last_event_ts_ms = max(
        [
            int(value)
            for value in (
                _safe_optional_int(row.get("m_trade_max_ts_ms")),
                _safe_optional_int(row.get("m_book_max_ts_ms")),
                int(ts_ms),
            )
            if value is not None
        ],
        default=int(ts_ms),
    )
    book_available = bool((_safe_optional_float(row.get("m_micro_book_available")) or 0.0) > 0.0 or book_events > 0)
    micro_available = bool((_safe_optional_float(row.get("m_micro_available")) or 0.0) > 0.0)
    if not micro_available and trade_events <= 0 and book_events <= 0 and spread_bps is None and depth_top5_notional_krw is None:
        return None
    close_value = _safe_optional_float(row.get("close")) or 0.0
    trade_volume_base = _safe_optional_float(row.get("m_trade_volume_base")) or 0.0
    trade_notional_krw = max(float(close_value) * float(trade_volume_base), 0.0)
    best_bid_price = _safe_optional_float(row.get("m_best_bid_price"))
    best_ask_price = _safe_optional_float(row.get("m_best_ask_price"))
    best_bid_notional = _safe_optional_float(row.get("m_best_bid_notional_krw"))
    best_ask_notional = _safe_optional_float(row.get("m_best_ask_notional_krw"))
    return MicroSnapshot(
        market=str(row.get("market", "")).strip().upper(),
        snapshot_ts_ms=int(ts_ms),
        last_event_ts_ms=int(last_event_ts_ms),
        trade_events=int(trade_events),
        trade_coverage_ms=int(trade_coverage_ms),
        trade_notional_krw=float(trade_notional_krw),
        trade_imbalance=trade_imbalance,
        trade_source="dataset",
        spread_bps_mean=spread_bps,
        depth_top5_notional_krw=depth_top5_notional_krw,
        depth_bid_top5_notional_krw=bid_depth,
        depth_ask_top5_notional_krw=ask_depth,
        best_bid_price=best_bid_price,
        best_ask_price=best_ask_price,
        best_bid_notional_krw=best_bid_notional,
        best_ask_notional_krw=best_ask_notional,
        book_events=int(book_events),
        book_coverage_ms=int(book_coverage_ms),
        book_available=bool(book_available),
    )


def _build_live_state_feature_snapshot(*, row: dict[str, Any] | None) -> dict[str, float]:
    if not isinstance(row, dict):
        return {}
    snapshot: dict[str, float] = {}
    for feature_name in ("rv_12", "rv_36", "atr_pct_14"):
        resolved = _resolve_row_risk_volatility(row=row, feature_name=feature_name)
        if resolved is not None:
            snapshot[feature_name] = float(resolved)
    trade_coverage_ms = _safe_optional_float(row.get("m_trade_coverage_ms"))
    if trade_coverage_ms is not None:
        snapshot["m_trade_coverage_ms"] = float(trade_coverage_ms)
    book_coverage_ms = _safe_optional_float(row.get("m_book_coverage_ms"))
    if book_coverage_ms is not None:
        snapshot["m_book_coverage_ms"] = float(book_coverage_ms)
    spread_bps = _safe_optional_float(row.get("m_spread_proxy"))
    if spread_bps is not None:
        snapshot["m_spread_proxy"] = float(spread_bps)
    depth_top5_notional_krw = _safe_optional_float(row.get("m_depth_top5_notional_krw"))
    if depth_top5_notional_krw is None:
        bid_depth = _safe_optional_float(row.get("m_depth_bid_top5_mean")) or 0.0
        ask_depth = _safe_optional_float(row.get("m_depth_ask_top5_mean")) or 0.0
        combined_depth = max(float(bid_depth) + float(ask_depth), 0.0)
        if combined_depth > 0.0:
            depth_top5_notional_krw = combined_depth
    if depth_top5_notional_krw is not None:
        snapshot["m_depth_top5_notional_krw"] = float(depth_top5_notional_krw)
    return snapshot


def _resolve_trade_action_expected_edge_bps(trade_action: dict[str, Any] | None = None) -> float | None:
    if not isinstance(trade_action, dict):
        return None
    expected_edge = _safe_optional_float(trade_action.get("expected_edge"))
    if expected_edge is None:
        return None
    return float(expected_edge) * 10_000.0


def _resolve_v5_strategy_expected_edge_bps(
    *,
    row: dict[str, Any] | None,
    trade_action: dict[str, Any] | None,
) -> float | None:
    trade_action_edge_bps = _resolve_trade_action_expected_edge_bps(trade_action)
    if trade_action_edge_bps is not None:
        return float(trade_action_edge_bps)
    if not isinstance(row, dict):
        return None
    final_expected_return = _safe_optional_float(row.get("final_expected_return"))
    if final_expected_return is not None:
        return float(final_expected_return) * 10_000.0
    final_alpha_lcb = _safe_optional_float(row.get("final_alpha_lcb"))
    if final_alpha_lcb is None:
        return None
    return float(final_alpha_lcb) * 10_000.0


def _resolve_v5_strategy_expected_net_edge_bps(
    *,
    row: dict[str, Any] | None,
    execution_decision: dict[str, Any] | None,
    trade_action: dict[str, Any] | None,
) -> float | None:
    if isinstance(execution_decision, dict):
        selected_net_edge_bps = _safe_optional_float(execution_decision.get("selected_net_edge_bps"))
        if selected_net_edge_bps is not None:
            return float(selected_net_edge_bps)
    return _resolve_v5_strategy_expected_edge_bps(row=row, trade_action=trade_action)


def _build_v5_strategy_safety_vetoes(
    *,
    entry_boundary_decision: dict[str, Any] | None,
    risk_control_decision: dict[str, Any] | None,
    size_ladder_decision: dict[str, Any] | None,
    execution_decision: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        "entry_boundary": dict(entry_boundary_decision or {}),
        "risk_control_static": dict(risk_control_decision or {}),
        "size_ladder_static": dict(size_ladder_decision or {}),
        "execution_decision": dict(execution_decision or {}),
    }


def _build_v5_liquidation_policy_payload(
    *,
    execution_decision: dict[str, Any] | None,
    execution_profile: dict[str, Any] | None,
) -> dict[str, Any]:
    decision = dict(execution_decision or {})
    profile = dict(execution_profile or {})
    return {
        "owner": "liquidation_execution_policy",
        "price_mode": str(profile.get("price_mode", "")).strip().upper(),
        "timeout_ms": _safe_optional_int(profile.get("timeout_ms")),
        "replace_interval_ms": _safe_optional_int(profile.get("replace_interval_ms")),
        "selected_stage": str(decision.get("selected_stage", "")).strip().upper(),
        "expected_fill_probability": _safe_optional_float(decision.get("selected_fill_probability")),
        "expected_net_edge_bps": _safe_optional_float(decision.get("selected_net_edge_bps")),
        "expected_slippage_bps": _safe_optional_float(decision.get("selected_expected_slippage_bps")),
    }


def _build_v5_exit_intent_meta(
    *,
    state: _PositionState,
    reason_code: str,
    trigger_ts_ms: int | None = None,
) -> dict[str, Any]:
    plan = dict(state.exit_plan or {})
    exit_decision = dict(plan.get("exit_decision") or {})
    liquidation_policy = build_v5_liquidation_policy(
        exit_decision=exit_decision,
        model_exit_plan=plan,
        entry_price=float(state.entry_price),
        qty=float(state.qty),
        last_price=_safe_optional_float(plan.get("last_price")),
        tick_size=_safe_optional_float(plan.get("tick_size")),
        ts_ms=trigger_ts_ms,
        created_ts_ms=int(state.entry_ts_ms),
        trigger_ts_ms=trigger_ts_ms,
    )
    return {
        "strategy": "model_alpha_v1",
        "exit_mode": str(plan.get("mode", "")),
        "decision_contract_version": V5_POST_MODEL_CONTRACT_VERSION,
        "entry_ownership": V5_ENTRY_OWNER,
        "sizing_ownership": V5_SIZING_OWNER,
        "trade_action_role": V5_TRADE_ACTION_ROLE,
        "exit_ownership": "continuation_value_controller",
        "model_exit_plan": plan,
        "exit_decision": exit_decision,
        "liquidation_policy": liquidation_policy,
        "entry_price": float(state.entry_price),
        "entry_ts_ms": int(state.entry_ts_ms),
        "qty": float(state.qty),
        "reason_code": str(reason_code).strip(),
    }


def _build_v5_pending_exit_decision_payload(
    *,
    liquidation_policy_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        "owner": "continuation_value_controller",
        "status": "pending_position_update",
        "decision_reason_code": "",
        "exit_now_value_net": None,
        "continue_value_net": None,
        "continue_value_lcb": None,
        "expected_liquidation_cost": _safe_optional_float(
            (liquidation_policy_payload or {}).get("expected_slippage_bps")
        ),
        "alpha_decay_penalty": None,
    }


def _build_v5_legacy_heuristics_applied_payload(
    *,
    trade_action_insufficient: bool,
    trade_action_support_level: str,
) -> dict[str, Any]:
    return {
        "selection_min_prob_authority_removed": True,
        "selection_top_pct_authority_removed": True,
        "trade_action_exit_authority_removed": True,
        "trade_action_primary_sizing_authority_removed": True,
        "trade_action_insufficient_evidence": bool(trade_action_insufficient),
        "trade_action_support_level": str(trade_action_support_level or "").strip().lower(),
    }


def _resolve_opportunity_uncertainty(row: dict[str, Any] | None) -> float | None:
    if not isinstance(row, dict):
        return None
    for key in ("score_std", "uncertainty_sigma", "prediction_std"):
        resolved = _safe_optional_float(row.get(key))
        if resolved is not None:
            return float(resolved)
    return None


def _optional_float_list(values: np.ndarray) -> list[float | None]:
    result: list[float | None] = []
    for value in np.asarray(values, dtype=np.float64):
        result.append(float(value) if np.isfinite(value) else None)
    return result


def _build_opportunity_feature_hash(*, row: dict[str, Any] | None) -> str:
    if not isinstance(row, dict):
        return ""
    normalized: dict[str, Any] = {}
    for key in sorted(str(item) for item in row.keys()):
        value = row.get(key)
        if hasattr(value, "item"):
            try:
                value = value.item()
            except Exception:
                value = str(value)
        elif isinstance(value, (list, tuple, dict)):
            value = json.loads(json.dumps(value, ensure_ascii=False, sort_keys=True))
        elif value is not None and not isinstance(value, (str, int, float, bool)):
            value = str(value)
        normalized[key] = value
    payload = json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _build_behavior_policy_logging(
    *,
    execution_decision: dict[str, Any] | None,
    execution_profile: dict[str, Any] | None,
    decision_outcome: str,
    skip_reason_code: str | None,
) -> dict[str, Any]:
    chosen_action = _resolve_behavior_policy_chosen_action(
        execution_decision=execution_decision,
        execution_profile=execution_profile,
        decision_outcome=decision_outcome,
    )
    candidate_actions = _build_counterfactual_candidate_actions(
        execution_decision=execution_decision,
        execution_profile=execution_profile,
        chosen_action=chosen_action,
        skip_reason_code=skip_reason_code,
    )
    chosen_action_propensity = None
    no_trade_action_propensity = None
    for item in candidate_actions:
        action_code = str(item.get("action_code") or "").strip().upper()
        propensity = _safe_optional_float(item.get("propensity"))
        if action_code == str(chosen_action).strip().upper():
            chosen_action_propensity = propensity
        if action_code == "NO_TRADE":
            no_trade_action_propensity = propensity
    mode = "pre_execution_no_trade" if str(chosen_action).strip().upper() == "NO_TRADE" else "deterministic_execution_stage"
    if not candidate_actions:
        mode = "behavior_policy_unavailable"
    return {
        "behavior_policy_name": "model_alpha_execution_behavior_policy_v1",
        "behavior_policy_mode": mode,
        "behavior_policy_support": "deterministic_no_exploration",
        "chosen_action": str(chosen_action).strip().upper(),
        "chosen_action_propensity": chosen_action_propensity,
        "no_trade_action_propensity": no_trade_action_propensity,
        "candidate_actions_json": candidate_actions,
    }


def _resolve_behavior_policy_chosen_action(
    *,
    execution_decision: dict[str, Any] | None,
    execution_profile: dict[str, Any] | None,
    decision_outcome: str,
) -> str:
    if str(decision_outcome).strip().lower() == "skip":
        return "NO_TRADE"
    if isinstance(execution_decision, dict):
        selected_stage = str(execution_decision.get("selected_stage", "")).strip().upper()
        if selected_stage:
            return selected_stage
    profile = dict(execution_profile or {})
    price_mode = str(profile.get("price_mode", "")).strip().upper()
    if price_mode:
        return price_mode
    return "INTENT_CREATED"


def _build_counterfactual_candidate_actions(
    execution_decision: dict[str, Any] | None,
    *,
    execution_profile: dict[str, Any] | None = None,
    chosen_action: str | None = None,
    skip_reason_code: str | None = None,
) -> list[dict[str, Any]]:
    chosen_action_code = str(chosen_action or "").strip().upper()
    rows: list[dict[str, Any]] = []
    if isinstance(execution_decision, dict):
        for item in execution_decision.get("evaluated_stages") or []:
            if not isinstance(item, dict):
                continue
            stage_name = str(item.get("stage", "")).strip().upper()
            if not stage_name:
                continue
            rows.append(
                {
                    "action_code": stage_name,
                    "selected": bool(chosen_action_code and stage_name == chosen_action_code),
                    "propensity": 1.0 if chosen_action_code and stage_name == chosen_action_code else 0.0,
                    "action_family": "execution",
                    "predicted_utility_bps": _safe_optional_float(item.get("net_edge_bps")),
                    "fill_probability": _safe_optional_float(item.get("fill_probability")),
                    "expected_slippage_bps": _safe_optional_float(item.get("expected_slippage_bps")),
                    "expected_cleanup_cost_bps": _safe_optional_float(item.get("expected_cleanup_cost_bps")),
                    "expected_miss_cost_bps": _safe_optional_float(item.get("expected_miss_cost_bps")),
                    "expected_time_to_fill_ms": _safe_optional_float(item.get("expected_time_to_fill_ms")),
                    "validation_comparable": bool(item.get("validation_comparable", False)),
                }
            )
    if chosen_action_code and chosen_action_code not in {"NO_TRADE", "INTENT_CREATED"} and not any(
        str(item.get("action_code") or "").strip().upper() == chosen_action_code for item in rows
    ):
        profile = dict(execution_profile or {})
        rows.append(
            {
                "action_code": chosen_action_code,
                "selected": True,
                "propensity": 1.0,
                "action_family": "execution",
                "predicted_utility_bps": _safe_optional_float((execution_decision or {}).get("selected_net_edge_bps")),
                "fill_probability": None,
                "expected_slippage_bps": None,
                "expected_cleanup_cost_bps": None,
                "expected_miss_cost_bps": None,
                "expected_time_to_fill_ms": _safe_optional_float(profile.get("timeout_ms")),
                "validation_comparable": False,
            }
        )
    no_trade_selected = chosen_action_code == "NO_TRADE"
    rows.append(
        {
            "action_code": "NO_TRADE",
            "selected": bool(no_trade_selected),
            "propensity": 1.0 if no_trade_selected else 0.0,
            "action_family": "no_trade",
            "predicted_utility_bps": 0.0,
            "fill_probability": None,
            "expected_slippage_bps": None,
            "expected_cleanup_cost_bps": None,
            "expected_miss_cost_bps": None,
            "expected_time_to_fill_ms": 0.0,
            "validation_comparable": False,
            "skip_reason_code": (str(skip_reason_code).strip() if skip_reason_code is not None else None) or None,
        }
    )
    return rows


def _resolve_runtime_risk_exit_thresholds(
    *,
    settings: ModelAlphaSettings,
    row: dict[str, Any] | None,
) -> tuple[float, float, float]:
    base_tp = max(float(settings.exit.tp_pct), 0.0)
    base_sl = max(float(settings.exit.sl_pct), 0.0)
    base_trailing = max(float(settings.exit.trailing_pct), 0.0)
    scaling_mode = str(settings.exit.risk_scaling_mode).strip().lower() or "fixed"
    if scaling_mode != "volatility_scaled":
        return base_tp, base_sl, base_trailing

    sigma_bar = _resolve_row_risk_volatility(row=row, feature_name=settings.exit.risk_vol_feature)
    if sigma_bar is None or sigma_bar <= 0:
        return base_tp, base_sl, base_trailing

    horizon_bars = max(int(settings.exit.hold_bars), 1)
    sigma_horizon = float(sigma_bar) * math.sqrt(float(horizon_bars))
    if not math.isfinite(sigma_horizon) or sigma_horizon <= 0:
        return base_tp, base_sl, base_trailing

    tp_mult = _safe_optional_float(settings.exit.tp_vol_multiplier)
    sl_mult = _safe_optional_float(settings.exit.sl_vol_multiplier)
    trailing_mult = _safe_optional_float(settings.exit.trailing_vol_multiplier)

    tp_pct = max(float(tp_mult) * sigma_horizon, 0.0) if tp_mult is not None else base_tp
    sl_pct = max(float(sl_mult) * sigma_horizon, 0.0) if sl_mult is not None else base_sl
    trailing_pct = max(float(trailing_mult) * sigma_horizon, 0.0) if trailing_mult is not None else base_trailing
    return tp_pct, sl_pct, trailing_pct


def _resolve_row_risk_volatility(*, row: dict[str, Any] | None, feature_name: str) -> float | None:
    if not isinstance(row, dict):
        return None
    feature = str(feature_name).strip()
    if not feature:
        return None
    if feature == "rv_12":
        feature = "vol_12" if "vol_12" in row else "rv_12"
    elif feature == "rv_36":
        feature = "vol_36" if "vol_36" in row else "rv_36"
    if feature == "atr_pct_14":
        atr = _safe_optional_float(row.get("atr_14"))
        close = _safe_optional_float(row.get("close"))
        if atr is None or close is None or close <= 0:
            return None
        return max(float(atr) / float(close), 0.0)
    value = _safe_optional_float(row.get(feature))
    if value is None:
        return None
    return max(float(value), 0.0)


def _resolve_expected_exit_costs(
    *,
    settings: ModelAlphaSettings,
    observed_entry_fee_rate: float,
) -> tuple[float, float]:
    exit_cfg = settings.exit
    fee_bps_override = _safe_optional_float(exit_cfg.expected_exit_fee_bps)
    if fee_bps_override is None:
        exit_fee_rate = max(float(observed_entry_fee_rate), 0.0)
    else:
        exit_fee_rate = max(float(fee_bps_override), 0.0) / 10_000.0

    slippage_bps_override = _safe_optional_float(exit_cfg.expected_exit_slippage_bps)
    if slippage_bps_override is None:
        exit_slippage_bps = _default_expected_exit_slippage_bps(settings.execution.price_mode)
    else:
        exit_slippage_bps = max(float(slippage_bps_override), 0.0)
    return exit_fee_rate, exit_slippage_bps


def _default_expected_exit_slippage_bps(price_mode: str) -> float:
    mode = str(price_mode).strip().upper()
    if mode == "PASSIVE_MAKER":
        return 0.0
    if mode == "CROSS_1T":
        return 6.0
    return 2.5


def _resolve_immediate_exit_execution_costs(
    *,
    execution_decision: dict[str, Any] | None,
    fallback_exit_fee_rate: float,
    fallback_exit_slippage_bps: float,
) -> dict[str, Any]:
    payload = dict(execution_decision or {})
    fill_probability = _safe_optional_float(payload.get("selected_fill_probability"))
    if fill_probability is None:
        fill_probability = 1.0
    fill_probability = max(min(float(fill_probability), 1.0), 0.0)
    slippage_bps = _safe_optional_float(payload.get("selected_expected_slippage_bps"))
    if slippage_bps is None:
        slippage_bps = _safe_optional_float(payload.get("expected_slippage_bps"))
    slippage_bps = max(float(slippage_bps if slippage_bps is not None else fallback_exit_slippage_bps), 0.0)
    cleanup_cost_bps = _safe_optional_float(payload.get("selected_expected_cleanup_cost_bps"))
    if cleanup_cost_bps is None:
        cleanup_cost_bps = _safe_optional_float(payload.get("expected_cleanup_cost_bps"))
    cleanup_cost_bps = max(float(cleanup_cost_bps or 0.0), 0.0)
    miss_cost_bps = _safe_optional_float(payload.get("selected_expected_miss_cost_bps"))
    if miss_cost_bps is None:
        miss_cost_bps = _safe_optional_float(payload.get("expected_miss_cost_bps"))
    miss_cost_bps = max(float(miss_cost_bps if miss_cost_bps is not None else cleanup_cost_bps), 0.0)
    fee_rate = max(float(fallback_exit_fee_rate), 0.0)
    uncertainty_bps = (1.0 - fill_probability) * max(float(slippage_bps), 5.0)
    residual_cleanup_bps = (1.0 - fill_probability) * max(float(cleanup_cost_bps), 0.0)
    residual_miss_bps = (1.0 - fill_probability) * max(float(miss_cost_bps - cleanup_cost_bps), 0.0) * 0.25
    cost_ratio = fee_rate + (float(slippage_bps + uncertainty_bps + residual_cleanup_bps + residual_miss_bps) / 10_000.0)
    return {
        "fee_rate": fee_rate,
        "slippage_bps": float(slippage_bps),
        "cleanup_cost_bps": float(cleanup_cost_bps),
        "miss_cost_bps": float(miss_cost_bps),
        "fill_probability": float(fill_probability),
        "time_to_fill_ms": _safe_optional_int(payload.get("selected_expected_time_to_fill_ms")),
        "price_mode": str(payload.get("selected_stage") or payload.get("selected_price_mode") or "").strip().upper() or "UNKNOWN",
        "cost_ratio": float(cost_ratio),
    }


def _resolve_min_tp_floor_pct(
    *,
    entry_fee_rate: float,
    exit_fee_rate: float,
    exit_slippage_bps: float,
) -> float:
    entry_multiplier = 1.0 + max(float(entry_fee_rate), 0.0)
    exit_multiplier = max(1.0 - (max(float(exit_slippage_bps), 0.0) / 10_000.0), 0.0) * (
        1.0 - max(float(exit_fee_rate), 0.0)
    )
    if exit_multiplier <= 0.0:
        return 0.0
    return max((entry_multiplier / exit_multiplier) - 1.0, 0.0)


def _net_return_after_costs(
    *,
    entry_price: float,
    exit_price: float,
    entry_fee_rate: float,
    exit_fee_rate: float,
    exit_slippage_bps: float,
) -> float:
    entry_value = max(float(entry_price), 1e-12)
    exit_value = max(float(exit_price), 1e-12)
    entry_cost = entry_value * (1.0 + max(float(entry_fee_rate), 0.0))
    exit_proceeds = _expected_exit_proceeds(
        exit_price=exit_value,
        exit_fee_rate=exit_fee_rate,
        exit_slippage_bps=exit_slippage_bps,
    )
    return (exit_proceeds / max(entry_cost, 1e-12)) - 1.0


def _net_drawdown_from_peak_after_costs(
    *,
    peak_price: float,
    current_price: float,
    exit_fee_rate: float,
    exit_slippage_bps: float,
) -> float:
    peak_value = max(float(peak_price), 1e-12)
    current_value = max(float(current_price), 1e-12)
    peak_proceeds = _expected_exit_proceeds(
        exit_price=peak_value,
        exit_fee_rate=exit_fee_rate,
        exit_slippage_bps=exit_slippage_bps,
    )
    current_proceeds = _expected_exit_proceeds(
        exit_price=current_value,
        exit_fee_rate=exit_fee_rate,
        exit_slippage_bps=exit_slippage_bps,
    )
    return 1.0 - (current_proceeds / max(peak_proceeds, 1e-12))


def _expected_exit_proceeds(*, exit_price: float, exit_fee_rate: float, exit_slippage_bps: float) -> float:
    slip_multiplier = max(1.0 - (max(float(exit_slippage_bps), 0.0) / 10_000.0), 0.0)
    effective_exit_price = max(float(exit_price), 1e-12) * slip_multiplier
    return effective_exit_price * (1.0 - max(float(exit_fee_rate), 0.0))
