"""Model-driven alpha strategy for backtest integration."""

from __future__ import annotations

from dataclasses import dataclass, field
import math
from typing import Any, Callable, Iterable, Sequence

import numpy as np
import polars as pl

from autobot.backtest.strategy_adapter import BacktestStrategyAdapter, StrategyFillEvent, StrategyOrderIntent, StrategyStepResult
from autobot.models.dataset_loader import FeatureTsGroup
from autobot.models.predictor import ModelPredictor
from autobot.strategy.operational_overlay_v1 import (
    ModelAlphaOperationalSettings,
    build_regime_snapshot_from_scored_frame,
    load_calibrated_operational_settings,
    resolve_operational_max_positions,
    resolve_operational_risk_multiplier,
)


@dataclass(frozen=True)
class ModelAlphaSelectionSettings:
    top_pct: float = 0.05
    min_prob: float | None = None
    min_candidates_per_ts: int = 10
    registry_threshold_key: str = "top_5pct"
    use_learned_recommendations: bool = True


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


@dataclass(frozen=True)
class ModelAlphaSettings:
    model_ref: str = "champion_v3"
    model_family: str | None = None
    feature_set: str = "v3"
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
        self._settings = settings
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
        min_prob_used, min_prob_source = _resolve_selection_min_prob(
            predictor=self._predictor,
            settings=self._settings.selection,
        )
        top_pct_used, top_pct_source = _resolve_selection_top_pct(
            predictor=self._predictor,
            settings=self._settings.selection,
        )
        min_candidates_used, min_candidates_source = _resolve_selection_min_candidates(
            predictor=self._predictor,
            settings=self._settings.selection,
        )
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

        for market in sorted(open_markets):
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
            )

        matrix = frame_active.select(list(self._predictor.feature_columns)).to_numpy().astype(np.float32, copy=False)
        probs = self._predictor.predict_scores(matrix).astype(np.float64, copy=False)
        scored = frame_active.with_columns(pl.Series(name="model_prob", values=probs))
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
                micro_quality_score=regime.micro_feature_quality_score,
            )
            operational_max_positions = resolve_operational_max_positions(
                base_max_positions=base_max_positions,
                settings=self._operational_settings,
                regime_score=regime.regime_score,
                breadth_ratio=regime.breadth_ratio,
            )
            operational_state = {
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
                operational_regime_score=operational_regime_score,
                operational_risk_multiplier=operational_risk_multiplier,
                operational_max_positions=operational_max_positions,
                operational_state=operational_state,
                skipped_reasons=skipped_reasons,
            )

        select_count = int(math.floor(eligible_rows * max(float(top_pct_used), 0.0)))
        if select_count > 0:
            selected = eligible.sort("model_prob", descending=True).head(select_count)
        else:
            selected = eligible.head(0)
        selected_rows = int(selected.height)
        dropped_top_pct_rows = max(eligible_rows - selected_rows, 0)

        active_positions = len(open_markets)
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
            if market in open_markets:
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
                        "selection_min_prob_used": float(min_prob_used),
                        "selection_min_prob_source": str(min_prob_source),
                        "selection_top_pct_used": float(top_pct_used),
                        "selection_top_pct_source": str(top_pct_source),
                        "selection_min_candidates_used": int(min_candidates_used),
                        "selection_min_candidates_source": str(min_candidates_source),
                        "sizing_mode": str(self._settings.position.sizing_mode),
                        "notional_multiplier": _resolve_entry_notional_multiplier(
                            prob=float(row.get("model_prob", 0.0)),
                            threshold=float(min_prob_used),
                            settings=self._settings.position,
                        ) * float(operational_risk_multiplier),
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
            self._positions[market] = _PositionState(
                entry_ts_ms=int(event.ts_ms),
                entry_price=price,
                peak_price=price,
                entry_fee_rate=fee_rate,
            )
            return

        if side == "ask":
            self._positions.pop(market, None)
            cooldown_bars = max(int(self._settings.position.cooldown_bars), 0)
            if cooldown_bars > 0:
                self._cooldown_until_ts_ms[market] = int(event.ts_ms) + cooldown_bars * self._interval_ms

    def _resolve_exit_reason(self, *, ts_ms: int, market: str, state: _PositionState, ref_price: float) -> str | None:
        mode = str(self._settings.exit.mode).strip().lower() or "hold"
        hold_ms = max(int(self._settings.exit.hold_bars), 0) * self._interval_ms
        if mode == "hold":
            if hold_ms > 0 and int(ts_ms) - int(state.entry_ts_ms) >= hold_ms:
                return "MODEL_ALPHA_EXIT_HOLD_TIMEOUT"
            return None

        # risk mode
        state.peak_price = max(float(state.peak_price), float(ref_price))
        entry_price = max(float(state.entry_price), 1e-12)
        tp_pct = max(float(self._settings.exit.tp_pct), 0.0)
        sl_pct = max(float(self._settings.exit.sl_pct), 0.0)
        trailing_pct = max(float(self._settings.exit.trailing_pct), 0.0)
        exit_fee_rate, exit_slippage_bps = _resolve_expected_exit_costs(
            settings=self._settings,
            observed_entry_fee_rate=max(float(state.entry_fee_rate), 0.0),
        )
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
    manual_min_prob = _safe_optional_float(settings.min_prob)
    if manual_min_prob is not None:
        return _clamp_prob(manual_min_prob), "manual"

    threshold_key, _ = _resolve_runtime_threshold_key(
        predictor=predictor,
        settings=settings,
    )
    thresholds = predictor.thresholds if isinstance(predictor.thresholds, dict) else {}
    registry_value = _safe_optional_float(thresholds.get(threshold_key))
    if registry_value is not None:
        return _clamp_prob(registry_value), f"registry:{threshold_key}"

    if threshold_key != "top_5pct":
        fallback_value = _safe_optional_float(thresholds.get("top_5pct"))
        if fallback_value is not None:
            return _clamp_prob(fallback_value), "registry:top_5pct_fallback"

    return 0.0, "fallback_zero"


def _resolve_selection_top_pct(
    *,
    predictor: ModelPredictor,
    settings: ModelAlphaSelectionSettings,
) -> tuple[float, str]:
    manual_value = max(min(float(settings.top_pct), 1.0), 0.0)
    if not bool(settings.use_learned_recommendations):
        return manual_value, "manual"

    recommendation, source = _resolve_selection_recommendation_entry(
        predictor=predictor,
        settings=settings,
    )
    recommended_value = _safe_optional_float(recommendation.get("recommended_top_pct")) if recommendation else None
    if recommended_value is not None:
        return _clamp_prob(recommended_value), source
    return manual_value, "manual_fallback"


def _resolve_selection_min_candidates(
    *,
    predictor: ModelPredictor,
    settings: ModelAlphaSelectionSettings,
) -> tuple[int, str]:
    manual_value = max(int(settings.min_candidates_per_ts), 0)
    if not bool(settings.use_learned_recommendations):
        return manual_value, "manual"

    recommendation, source = _resolve_selection_recommendation_entry(
        predictor=predictor,
        settings=settings,
    )
    try:
        recommended_value = recommendation.get("recommended_min_candidates_per_ts") if recommendation else None
        if recommended_value is not None:
            return max(int(recommended_value), 0), source
    except (TypeError, ValueError):
        pass
    return manual_value, "manual_fallback"


def _resolve_selection_recommendation_entry(
    *,
    predictor: ModelPredictor,
    settings: ModelAlphaSelectionSettings,
) -> tuple[dict[str, Any], str]:
    recommendations = predictor.selection_recommendations if isinstance(predictor.selection_recommendations, dict) else {}
    by_key = recommendations.get("by_threshold_key")
    if not isinstance(by_key, dict):
        return {}, "manual_fallback"

    threshold_key, threshold_key_source = _resolve_runtime_threshold_key(
        predictor=predictor,
        settings=settings,
    )
    entry = by_key.get(threshold_key)
    if isinstance(entry, dict):
        source = f"registry_recommendation:{threshold_key}"
        if threshold_key_source == "registry_recommendation":
            source += ":learned_threshold_key"
        return entry, source

    if threshold_key != "top_5pct":
        fallback_entry = by_key.get("top_5pct")
        if isinstance(fallback_entry, dict):
            return fallback_entry, "registry_recommendation:top_5pct_fallback"
    return {}, "manual_fallback"


def _resolve_runtime_threshold_key(
    *,
    predictor: ModelPredictor,
    settings: ModelAlphaSelectionSettings,
) -> tuple[str, str]:
    default_key = str(settings.registry_threshold_key).strip() or "top_5pct"
    if not bool(settings.use_learned_recommendations):
        return default_key, "settings"

    recommendations = predictor.selection_recommendations if isinstance(predictor.selection_recommendations, dict) else {}
    recommended_key = str(recommendations.get("recommended_threshold_key", "")).strip()
    by_key = recommendations.get("by_threshold_key") if isinstance(recommendations.get("by_threshold_key"), dict) else {}
    if recommended_key and isinstance(by_key, dict) and isinstance(by_key.get(recommended_key), dict):
        return recommended_key, "registry_recommendation"
    return default_key, "settings"


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
