from __future__ import annotations

from dataclasses import dataclass

from autobot.strategy.operational_overlay_v1 import (
    ModelAlphaOperationalSettings,
    compute_micro_quality_composite,
    resolve_operational_risk_multiplier,
)
from autobot.strategy.micro_snapshot import MicroSnapshot


@dataclass(frozen=True)
class DynamicExitOverlayResult:
    applied: bool
    quality_score: float | None
    trade_imbalance: float | None
    activation_strength: float
    risk_multiplier: float | None
    tp_pct: float | None
    sl_pct: float | None
    trailing_enabled: bool
    trailing_pct: float | None
    timeout_delta_ms: int | None


def resolve_dynamic_exit_overlay(
    *,
    settings: ModelAlphaOperationalSettings | None,
    micro_snapshot: MicroSnapshot | None,
    now_ts_ms: int,
    current_return_ratio: float,
    elapsed_ms: int,
    tp_pct: float | None,
    sl_pct: float | None,
    trailing_enabled: bool,
    trailing_pct: float | None,
    timeout_delta_ms: int | None,
) -> DynamicExitOverlayResult:
    if timeout_delta_ms is not None and int(timeout_delta_ms) <= 0:
        timeout_delta_ms = None
    if settings is None or not bool(settings.enabled) or micro_snapshot is None:
        return DynamicExitOverlayResult(
            applied=False,
            quality_score=None,
            trade_imbalance=None,
            activation_strength=0.0,
            risk_multiplier=None,
            tp_pct=tp_pct,
            sl_pct=sl_pct,
            trailing_enabled=bool(trailing_enabled),
            trailing_pct=trailing_pct,
            timeout_delta_ms=timeout_delta_ms,
        )

    micro_quality = compute_micro_quality_composite(
        micro_snapshot=micro_snapshot,
        now_ts_ms=int(now_ts_ms),
        settings=settings,
    )
    if micro_quality is None:
        return DynamicExitOverlayResult(
            applied=False,
            quality_score=None,
            trade_imbalance=None,
            activation_strength=0.0,
            risk_multiplier=None,
            tp_pct=tp_pct,
            sl_pct=sl_pct,
            trailing_enabled=bool(trailing_enabled),
            trailing_pct=trailing_pct,
            timeout_delta_ms=timeout_delta_ms,
        )

    quality_score = _clamp01(float(micro_quality.score))
    conservative_threshold = max(float(settings.micro_quality_conservative_threshold), 1e-6)
    quality_penalty = _clamp01((conservative_threshold - quality_score) / conservative_threshold)
    trade_imbalance = _as_optional_float(micro_snapshot.trade_imbalance) or 0.0
    adverse_flow = _clamp01(max(-float(trade_imbalance), 0.0))
    activation_strength = max(float(quality_penalty), float(adverse_flow))
    if activation_strength <= 0.0:
        return DynamicExitOverlayResult(
            applied=False,
            quality_score=quality_score,
            trade_imbalance=float(trade_imbalance),
            activation_strength=0.0,
            risk_multiplier=1.0,
            tp_pct=tp_pct,
            sl_pct=sl_pct,
            trailing_enabled=bool(trailing_enabled),
            trailing_pct=trailing_pct,
            timeout_delta_ms=timeout_delta_ms,
        )

    risk_multiplier = resolve_operational_risk_multiplier(
        settings=settings,
        regime_score=float(quality_score),
        breadth_ratio=None,
        micro_quality_score=float(quality_score),
    )
    tighten_scale = min(max(float(risk_multiplier), 0.25), 1.0)

    next_tp_pct = _tighten_optional_pct(tp_pct, tighten_scale)
    next_sl_pct = _tighten_optional_pct(sl_pct, tighten_scale)
    next_trailing_enabled = bool(trailing_enabled)
    next_trailing_pct = trailing_pct

    if float(current_return_ratio) > 0.0:
        allowed_drawdown_share = max(0.20, 0.60 - (0.40 * float(activation_strength)))
        profit_lock_trail = max(float(current_return_ratio) * float(allowed_drawdown_share), 0.0015)
        next_trailing_enabled = True
        next_trailing_pct = (
            min(float(next_trailing_pct), float(profit_lock_trail))
            if next_trailing_pct is not None and float(next_trailing_pct) > 0.0
            else float(profit_lock_trail)
        )
    elif next_trailing_enabled and next_trailing_pct is not None and float(next_trailing_pct) > 0.0:
        next_trailing_pct = min(float(next_trailing_pct), float(next_trailing_pct) * float(tighten_scale))

    next_timeout_delta_ms = timeout_delta_ms
    if next_timeout_delta_ms is not None and int(next_timeout_delta_ms) > int(elapsed_ms):
        remaining_ms = max(int(next_timeout_delta_ms) - int(elapsed_ms), 0)
        compressed_remaining_ms = max(int(float(remaining_ms) * float(tighten_scale)), 60_000)
        next_timeout_delta_ms = min(int(next_timeout_delta_ms), int(elapsed_ms) + int(compressed_remaining_ms))

    applied = any(
        [
            not _same_optional_float(tp_pct, next_tp_pct),
            not _same_optional_float(sl_pct, next_sl_pct),
            bool(trailing_enabled) != bool(next_trailing_enabled),
            not _same_optional_float(trailing_pct, next_trailing_pct),
            _as_optional_int(timeout_delta_ms) != _as_optional_int(next_timeout_delta_ms),
        ]
    )
    return DynamicExitOverlayResult(
        applied=bool(applied),
        quality_score=float(quality_score),
        trade_imbalance=float(trade_imbalance),
        activation_strength=float(activation_strength),
        risk_multiplier=float(risk_multiplier),
        tp_pct=next_tp_pct,
        sl_pct=next_sl_pct,
        trailing_enabled=bool(next_trailing_enabled),
        trailing_pct=next_trailing_pct if bool(next_trailing_enabled) else None,
        timeout_delta_ms=_as_optional_int(next_timeout_delta_ms),
    )


def _tighten_optional_pct(value: float | None, tighten_scale: float) -> float | None:
    if value is None or float(value) <= 0.0:
        return value
    return min(float(value), float(value) * float(tighten_scale))


def _same_optional_float(left: float | None, right: float | None, *, tol: float = 1e-12) -> bool:
    if left is None and right is None:
        return True
    if left is None or right is None:
        return False
    return abs(float(left) - float(right)) <= float(tol)


def _as_optional_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_optional_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _clamp01(value: float) -> float:
    return max(min(float(value), 1.0), 0.0)
