"""Model-driven alpha strategy for backtest integration."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
import math
from typing import Any, Callable, Iterable, Sequence

import numpy as np
import polars as pl

from autobot.backtest.strategy_adapter import BacktestStrategyAdapter, StrategyFillEvent, StrategyOrderIntent, StrategyStepResult
from autobot.common.dynamic_exit_overlay import resolve_dynamic_exit_overlay
from autobot.common.model_exit_contract import normalize_model_exit_plan_payload
from autobot.models.dataset_loader import FeatureTsGroup
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

from . import model_alpha_runtime_contract as _runtime_contract


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
        self._trade_action_policy = normalize_trade_action_policy(
            (getattr(predictor, "runtime_recommendations", {}) or {}).get("trade_action")
        )
        self._risk_control_policy = normalize_execution_risk_control_payload(
            (getattr(predictor, "runtime_recommendations", {}) or {}).get("risk_control")
        )
        self._runtime_recommendation_state["trade_action_policy_status"] = str(
            self._trade_action_policy.get("status", "missing")
        )
        self._runtime_recommendation_state["trade_action_risk_feature_name"] = str(
            self._trade_action_policy.get("risk_feature_name", "")
        )
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
        skipped_reasons: dict[str, int] = {}
        scored_rows = 0
        selected_rows = 0
        dropped_min_prob_rows = 0
        dropped_top_pct_rows = 0
        blocked_min_candidates_ts = 0

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
            intents.append(
                StrategyOrderIntent(
                    market=market,
                    side="ask",
                    ref_price=ref_price,
                    reason_code=reason_code,
                    meta={"strategy": "model_alpha_v1", "exit_mode": str(self._settings.exit.mode)},
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
        raw_probs = self._predictor.predict_scores(matrix).astype(np.float64, copy=False)
        if selection_mode == DEFAULT_SELECTION_POLICY_MODE:
            probs = self._predictor.predict_selection_scores(matrix).astype(np.float64, copy=False)
        else:
            probs = raw_probs
        scored = frame_active.with_columns(
            pl.Series(name="model_prob", values=probs),
            pl.Series(name="model_prob_raw", values=raw_probs),
        )
        if selection_mode == DEFAULT_SELECTION_POLICY_MODE:
            eligible = scored
            eligible_rows = int(eligible.height)
            dropped_min_prob_rows = 0
        else:
            eligible = scored.filter(pl.col("model_prob") >= float(min_prob_used))
            eligible_rows = int(eligible.height)
            dropped_min_prob_rows = max(scored_rows - eligible_rows, 0)
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
        if eligible_rows < min_candidates:
            blocked_min_candidates_ts = 1
            _inc_reason(skipped_reasons, "MIN_CANDIDATES_NOT_MET")
            return StrategyStepResult(
                intents=tuple(intents),
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

        if selection_mode == DEFAULT_SELECTION_POLICY_MODE:
            select_count = max(int(math.floor(eligible_rows * max(float(top_pct_used), 0.0))), min_candidates)
            select_count = min(select_count, eligible_rows)
        else:
            select_count = int(math.floor(eligible_rows * max(float(top_pct_used), 0.0)))
        if select_count > 0:
            selected = eligible.sort("model_prob", descending=True).head(select_count)
        else:
            selected = eligible.head(0)
        selected_rows = int(selected.height)
        dropped_top_pct_rows = max(eligible_rows - selected_rows, 0)

        active_positions = len(tracked_open_markets)
        max_positions = max(int(operational_max_positions), 1)
        can_open = max(max_positions - active_positions, 0)
        if can_open <= 0:
            _inc_reason(skipped_reasons, "MAX_POSITIONS_TOTAL")
        for row in selected.iter_rows(named=True):
            if can_open <= 0:
                break
            market = str(row.get("market", "")).strip().upper()
            if not market:
                _inc_reason(skipped_reasons, "EMPTY_MARKET")
                continue
            if market in tracked_open_markets:
                _inc_reason(skipped_reasons, "ALREADY_OPEN")
                continue
            cooldown_until = int(self._cooldown_until_ts_ms.get(market, 0))
            if ts_value < cooldown_until:
                _inc_reason(skipped_reasons, "COOLDOWN_ACTIVE")
                continue
            ref_price = _resolve_ref_price(row=row, latest_prices=latest_prices, market=market)
            if ref_price is None:
                _inc_reason(skipped_reasons, "ENTRY_REF_PRICE_MISSING")
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
            if (
                isinstance(trade_action_decision, dict)
                and str(trade_action_decision.get("status", "")).strip().lower() == "insufficient_evidence"
            ):
                _inc_reason(
                    skipped_reasons,
                    str(trade_action_decision.get("reason_code", "TRADE_ACTION_INSUFFICIENT_EVIDENCE")).strip()
                    or "TRADE_ACTION_INSUFFICIENT_EVIDENCE",
                )
                continue
            effective_exit_settings = _resolve_trade_action_exit_settings(
                base_settings=self._settings,
                decision=trade_action_decision,
            )
            requested_notional_multiplier = (
                float(trade_action_decision.get("recommended_notional_multiplier", base_notional_multiplier))
                if isinstance(trade_action_decision, dict) and "recommended_notional_multiplier" in trade_action_decision
                else float(base_notional_multiplier)
            )
            risk_control_decision = _resolve_runtime_risk_control_decision(
                payload=self._risk_control_policy,
                selection_score=float(row.get("model_prob", 0.0)),
                trade_action=trade_action_decision,
                enabled=bool(self._settings.exit.use_trade_level_action_policy),
            )
            if isinstance(risk_control_decision, dict) and bool(risk_control_decision.get("enabled")) and not bool(
                risk_control_decision.get("allowed")
            ):
                _inc_reason(
                    skipped_reasons,
                    str(risk_control_decision.get("reason_code", "RISK_CONTROL_BELOW_THRESHOLD")).strip()
                    or "RISK_CONTROL_BELOW_THRESHOLD",
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
                continue
            if float(notional_multiplier) <= 0.0:
                _inc_reason(skipped_reasons, "TRADE_ACTION_TARGET_NOTIONAL_NONPOSITIVE")
                continue
            intents.append(
                StrategyOrderIntent(
                    market=market,
                    side="bid",
                    ref_price=ref_price,
                    reason_code="MODEL_ALPHA_ENTRY_V1",
                    prob=float(row.get("model_prob", 0.0)),
                    score=float(row.get("model_prob", 0.0)),
                    meta={
                        "strategy": "model_alpha_v1",
                        "model_prob": float(row.get("model_prob", 0.0)),
                        "model_prob_raw": float(row.get("model_prob_raw", row.get("model_prob", 0.0))),
                        "selection_min_prob_used": float(min_prob_used),
                        "selection_min_prob_source": str(min_prob_source),
                        "selection_top_pct_used": float(top_pct_used),
                        "selection_top_pct_source": str(top_pct_source),
                        "selection_min_candidates_used": int(min_candidates_used),
                        "selection_min_candidates_source": str(min_candidates_source),
                        "selection_policy_mode": str(selection_mode),
                        "selection_policy_source": str(selection_policy_source),
                        "sizing_mode": (
                            "risk_control_size_ladder"
                            if isinstance(size_ladder_decision, dict) and bool(size_ladder_decision.get("enabled"))
                            else (
                                "trade_action_policy"
                                if isinstance(trade_action_decision, dict) and "recommended_notional_multiplier" in trade_action_decision
                                else str(self._settings.position.sizing_mode)
                            )
                        ),
                        "base_notional_multiplier": float(base_notional_multiplier),
                        "notional_multiplier": float(notional_multiplier) * float(operational_risk_multiplier),
                        "notional_multiplier_source": (
                            "risk_control_size_ladder"
                            if isinstance(size_ladder_decision, dict) and bool(size_ladder_decision.get("enabled"))
                            else (
                                "trade_action_policy"
                                if isinstance(trade_action_decision, dict) and "recommended_notional_multiplier" in trade_action_decision
                                else str(self._settings.position.sizing_mode)
                            )
                        ),
                        "model_exit_plan": build_model_alpha_exit_plan_payload(
                            settings=effective_exit_settings,
                            row=row,
                            interval_ms=self._interval_ms,
                            observed_entry_fee_rate=0.0,
                        ),
                        "exit_recommendation": _build_runtime_exit_recommendation_meta(
                            self._runtime_recommendation_state
                        ),
                        "trade_action": dict(trade_action_decision or {}),
                        "risk_control_static": dict(risk_control_decision or {}),
                        "size_ladder_static": dict(size_ladder_decision or {}),
                        "operational_overlay": dict(operational_state),
                    },
                )
            )
            can_open -= 1

        return StrategyStepResult(
            intents=tuple(intents),
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
            state=state,
            settings=self._settings,
            ts_ms=ts_ms,
            current_price=ref_price,
            row=row,
            interval_ms=self._interval_ms,
        )
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
                if net_return <= -sl_pct:
                    return "MODEL_ALPHA_EXIT_SL"
            if hold_ms > 0 and int(ts_ms) - int(state.entry_ts_ms) >= hold_ms:
                return "MODEL_ALPHA_EXIT_HOLD_TIMEOUT"
            return None

        # risk mode
        state.peak_price = max(float(state.peak_price), float(ref_price))
        entry_price = max(float(state.entry_price), 1e-12)
        net_return = _net_return_after_costs(
            entry_price=entry_price,
            exit_price=float(ref_price),
            entry_fee_rate=max(float(state.entry_fee_rate), 0.0),
            exit_fee_rate=exit_fee_rate,
            exit_slippage_bps=exit_slippage_bps,
        )
        trailing_drawdown = _net_drawdown_from_peak_after_costs(
            peak_price=max(float(state.peak_price), entry_price),
            current_price=float(ref_price),
            exit_fee_rate=exit_fee_rate,
            exit_slippage_bps=exit_slippage_bps,
        )
        if tp_pct > 0 and net_return >= tp_pct:
            return "MODEL_ALPHA_EXIT_TP"
        if sl_pct > 0 and net_return <= -sl_pct:
            return "MODEL_ALPHA_EXIT_SL"
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
    state: _PositionState,
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
        state=state,
        plan=base_plan,
        row=row,
        ts_ms=ts_ms,
        current_price=current_price,
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
            "risk_scaling_mode": str(settings.exit.risk_scaling_mode),
            "risk_vol_feature": str(settings.exit.risk_vol_feature),
            "tp_vol_multiplier": _safe_optional_float(settings.exit.tp_vol_multiplier),
            "sl_vol_multiplier": _safe_optional_float(settings.exit.sl_vol_multiplier),
            "trailing_vol_multiplier": _safe_optional_float(settings.exit.trailing_vol_multiplier),
            "use_learned_exit_mode": bool(settings.exit.use_learned_exit_mode),
            "use_learned_hold_bars": bool(settings.exit.use_learned_hold_bars),
            "use_learned_risk_recommendations": bool(settings.exit.use_learned_risk_recommendations),
            "use_trade_level_action_policy": bool(settings.exit.use_trade_level_action_policy),
        }
    )


def _reprice_position_exit_plan(
    *,
    state: _PositionState,
    plan: dict[str, Any],
    row: dict[str, Any] | None,
    ts_ms: int,
    current_price: float | None,
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
    base_timeout_delta_ms = _safe_optional_int(normalized.get("base_timeout_delta_ms"))
    overlay_tp_basis = base_tp_pct if base_tp_pct is not None else _safe_optional_float(normalized.get("tp_pct"))
    overlay_sl_basis = base_sl_pct if base_sl_pct is not None else _safe_optional_float(normalized.get("sl_pct"))
    overlay_trailing_basis = (
        base_trailing_pct if base_trailing_pct is not None else _safe_optional_float(normalized.get("trailing_pct"))
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
    depth_top5_notional_krw = _safe_optional_float(row.get("m_depth_top5_notional_krw"))
    if depth_top5_notional_krw is None:
        bid_depth = _safe_optional_float(row.get("m_depth_bid_top5_mean")) or 0.0
        ask_depth = _safe_optional_float(row.get("m_depth_ask_top5_mean")) or 0.0
        depth_top5_notional_krw = max(float(bid_depth) + float(ask_depth), 0.0)
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
        book_events=int(book_events),
        book_coverage_ms=int(book_coverage_ms),
        book_available=bool(book_available),
    )


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
