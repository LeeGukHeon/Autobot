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


@dataclass(frozen=True)
class ModelAlphaSelectionSettings:
    top_pct: float = 0.05
    min_prob: float = 0.58
    min_candidates_per_ts: int = 10


@dataclass(frozen=True)
class ModelAlphaPositionSettings:
    max_positions_total: int = 3
    cooldown_bars: int = 6


@dataclass(frozen=True)
class ModelAlphaExitSettings:
    mode: str = "hold"  # hold | risk
    hold_bars: int = 6
    tp_pct: float = 0.02
    sl_pct: float = 0.01
    trailing_pct: float = 0.0


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


@dataclass
class _PositionState:
    entry_ts_ms: int
    entry_price: float
    peak_price: float


class ModelAlphaStrategyV1(BacktestStrategyAdapter):
    def __init__(
        self,
        *,
        predictor: ModelPredictor,
        feature_groups: Iterable[FeatureTsGroup] | None,
        settings: ModelAlphaSettings,
        interval_ms: int,
        live_frame_provider: Callable[[int, Sequence[str]], pl.DataFrame | None] | None = None,
    ) -> None:
        self._predictor = predictor
        self._feature_iter = iter(feature_groups or ())
        self._settings = settings
        self._interval_ms = max(int(interval_ms), 1)
        self._pending_group: FeatureTsGroup | None = None
        self._positions: dict[str, _PositionState] = {}
        self._cooldown_until_ts_ms: dict[str, int] = {}
        self._live_frame_provider = live_frame_provider

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
            )

        min_candidates = max(int(self._settings.selection.min_candidates_per_ts), 0)
        if scored_rows < min_candidates:
            blocked_min_candidates_ts = 1
            _inc_reason(skipped_reasons, "MIN_CANDIDATES_NOT_MET")
            return StrategyStepResult(
                intents=tuple(intents),
                scored_rows=scored_rows,
                selected_rows=0,
                skipped_missing_features_rows=int(missing_rows),
                blocked_min_candidates_ts=blocked_min_candidates_ts,
                skipped_reasons=skipped_reasons,
            )

        matrix = frame_active.select(list(self._predictor.feature_columns)).to_numpy().astype(np.float32, copy=False)
        probs = self._predictor.predict_scores(matrix).astype(np.float64, copy=False)
        scored = frame_active.with_columns(pl.Series(name="model_prob", values=probs))
        min_prob = float(self._settings.selection.min_prob)
        eligible = scored.filter(pl.col("model_prob") >= min_prob)
        dropped_min_prob_rows = max(scored_rows - int(eligible.height), 0)

        select_count = int(math.floor(int(eligible.height) * max(float(self._settings.selection.top_pct), 0.0)))
        if select_count > 0:
            selected = eligible.sort("model_prob", descending=True).head(select_count)
        else:
            selected = eligible.head(0)
        selected_rows = int(selected.height)
        dropped_top_pct_rows = max(int(eligible.height) - selected_rows, 0)

        active_positions = len(open_markets)
        max_positions = max(int(self._settings.position.max_positions_total), 1)
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
                    meta={"strategy": "model_alpha_v1", "model_prob": float(row.get("model_prob", 0.0))},
                )
            )
            can_open -= 1

        return StrategyStepResult(
            intents=tuple(intents),
            scored_rows=scored_rows,
            selected_rows=selected_rows,
            skipped_missing_features_rows=int(missing_rows),
            dropped_min_prob_rows=dropped_min_prob_rows,
            dropped_top_pct_rows=dropped_top_pct_rows,
            blocked_min_candidates_ts=blocked_min_candidates_ts,
            skipped_reasons=skipped_reasons,
        )

    def on_fill(self, event: StrategyFillEvent) -> None:
        market = str(event.market).strip().upper()
        if not market:
            return
        side = str(event.side).strip().lower()
        if side == "bid":
            price = max(float(event.price), 1e-12)
            self._positions[market] = _PositionState(
                entry_ts_ms=int(event.ts_ms),
                entry_price=price,
                peak_price=price,
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
        if tp_pct > 0 and ref_price >= entry_price * (1.0 + tp_pct):
            return "MODEL_ALPHA_EXIT_TP"
        if sl_pct > 0 and ref_price <= entry_price * (1.0 - sl_pct):
            return "MODEL_ALPHA_EXIT_SL"
        if trailing_pct > 0 and ref_price <= state.peak_price * (1.0 - trailing_pct):
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
