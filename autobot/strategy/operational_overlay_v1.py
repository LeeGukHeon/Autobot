"""Runtime operational overlay helpers for paper/live-style execution.

This module keeps alpha selection fixed and adapts only runtime risk,
slot usage, and execution posture from observable market regime inputs.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
import json
import math
from pathlib import Path
from typing import Any, TYPE_CHECKING

import polars as pl

from autobot.execution.order_supervisor import (
    OrderExecProfile,
    PRICE_MODE_CROSS_1T,
    PRICE_MODE_JOIN,
    PRICE_MODE_PASSIVE_MAKER,
    normalize_order_exec_profile,
)
if TYPE_CHECKING:
    from autobot.strategy.micro_snapshot import MicroSnapshot


@dataclass(frozen=True)
class ModelAlphaOperationalSettings:
    enabled: bool = True
    use_calibration_artifact: bool = True
    calibration_artifact_path: str = "logs/operational_overlay/latest.json"
    risk_multiplier_min: float = 0.80
    risk_multiplier_max: float = 1.20
    max_positions_scale_min: float = 0.50
    max_positions_scale_max: float = 1.50
    session_overlap_boost: float = 0.10
    session_offpeak_penalty: float = 0.05
    micro_quality_block_threshold: float = 0.15
    micro_quality_conservative_threshold: float = 0.35
    micro_quality_aggressive_threshold: float = 0.75
    max_execution_spread_bps_for_join: float = 20.0
    max_execution_spread_bps_for_cross: float = 6.0
    min_execution_depth_krw_for_cross: float = 1_500_000.0
    snapshot_stale_ms: int = 15_000
    conservative_timeout_scale: float = 1.25
    aggressive_timeout_scale: float = 0.75
    conservative_replace_interval_scale: float = 1.50
    aggressive_replace_interval_scale: float = 0.50
    conservative_max_replaces_scale: float = 0.50
    aggressive_max_replaces_bonus: int = 1
    conservative_max_chase_bps_scale: float = 0.75
    aggressive_max_chase_bps_bonus: int = 5
    profit_lock_arm_min_return_pct: float = 0.75
    profit_lock_arm_tp_fraction: float = 0.20
    runtime_timeout_ms_floor: int = 5_000
    runtime_replace_interval_ms_floor: int = 1_500
    empirical_state_score_model_enabled: bool = False
    empirical_state_score_intercept: float = 0.0
    empirical_state_score_regime_coef: float = 0.0
    empirical_state_score_breadth_coef: float = 0.0
    empirical_state_score_micro_coef: float = 0.0
    empirical_state_score_output_scale: float = 1.0


@dataclass(frozen=True)
class OperationalRegimeSnapshot:
    regime_score: float
    breadth_score: float
    breadth_ratio: float
    dispersion_score: float
    dispersion_value: float
    micro_feature_quality_score: float
    trade_coverage_ms: float
    book_coverage_ms: float
    spread_bps: float
    depth_krw: float
    session_score: float
    session_bucket: str


@dataclass(frozen=True)
class MicroQualityComposite:
    score: float
    spread_score: float
    depth_score: float
    trade_coverage_score: float
    book_coverage_score: float
    age_score: float
    spread_bps: float | None
    depth_krw: float | None
    trade_coverage_ms: int | None
    book_coverage_ms: int | None
    snapshot_age_ms: int | None


@dataclass(frozen=True)
class OperationalExecutionOverlayDecision:
    risk_multiplier: float
    exec_profile: OrderExecProfile
    micro_quality: MicroQualityComposite | None
    abort_reason: str | None
    diagnostics: dict[str, Any]


def load_calibrated_operational_settings(
    *,
    base_settings: ModelAlphaOperationalSettings,
) -> ModelAlphaOperationalSettings:
    if not bool(base_settings.enabled) or not bool(base_settings.use_calibration_artifact):
        return base_settings
    raw_path = str(base_settings.calibration_artifact_path).strip()
    if not raw_path:
        return base_settings
    path = Path(raw_path)
    if not path.exists():
        return base_settings
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return base_settings
    if not isinstance(payload, dict):
        return base_settings
    calibrated = payload.get("calibrated_settings")
    if not isinstance(calibrated, dict):
        return base_settings
    merged = dict(asdict(base_settings))
    for field_name, current_value in merged.items():
        if field_name in {"enabled", "use_calibration_artifact", "calibration_artifact_path"}:
            continue
        if field_name not in calibrated:
            continue
        merged[field_name] = _coerce_like(current_value=current_value, new_value=calibrated.get(field_name))
    try:
        return ModelAlphaOperationalSettings(**merged)
    except Exception:
        return base_settings


def build_regime_snapshot_from_scored_frame(
    *,
    scored: pl.DataFrame,
    eligible_rows: int,
    scored_rows: int,
    ts_ms: int,
) -> OperationalRegimeSnapshot:
    breadth_ratio = (float(eligible_rows) / float(scored_rows)) if scored_rows > 0 else 0.0
    breadth_score = _clamp01(breadth_ratio)

    dispersion_value = _median_or_zero(scored, "market_dispersion_12")
    if dispersion_value <= 0:
        dispersion_value = _std_or_zero(scored, "model_prob")
    dispersion_score = _score_lower_better(dispersion_value, 0.05)

    trade_coverage_ms = _median_or_zero(scored, "m_trade_coverage_ms")
    book_coverage_ms = _median_or_zero(scored, "m_book_coverage_ms")
    spread_bps = _median_or_zero(scored, "m_spread_proxy")
    depth_krw = _sum_depth_krw(scored)
    micro_feature_quality_score = _mean_scores(
        [
            _score_higher_better(trade_coverage_ms, 60_000.0),
            _score_higher_better(book_coverage_ms, 60_000.0),
            _score_lower_better(spread_bps, 25.0),
            _score_log_depth(depth_krw, 5_000_000.0),
        ]
    )

    session_bucket, session_score = _session_bucket_and_score(ts_ms)
    regime_score = _mean_scores(
        [
            breadth_score * 0.35,
            dispersion_score * 0.20,
            micro_feature_quality_score * 0.35,
            session_score * 0.10,
        ],
        weighted=True,
    )
    return OperationalRegimeSnapshot(
        regime_score=_clamp01(regime_score),
        breadth_score=_clamp01(breadth_score),
        breadth_ratio=float(breadth_ratio),
        dispersion_score=_clamp01(dispersion_score),
        dispersion_value=float(max(dispersion_value, 0.0)),
        micro_feature_quality_score=_clamp01(micro_feature_quality_score),
        trade_coverage_ms=float(max(trade_coverage_ms, 0.0)),
        book_coverage_ms=float(max(book_coverage_ms, 0.0)),
        spread_bps=float(max(spread_bps, 0.0)),
        depth_krw=float(max(depth_krw, 0.0)),
        session_score=_clamp01(session_score),
        session_bucket=session_bucket,
    )


def resolve_operational_risk_multiplier(
    *,
    settings: ModelAlphaOperationalSettings,
    regime_score: float,
    breadth_ratio: float | None = None,
    micro_quality_score: float | None = None,
) -> float:
    if bool(settings.empirical_state_score_model_enabled):
        base_score = _resolve_empirical_state_score(
            settings=settings,
            regime_score=regime_score,
            breadth_ratio=breadth_ratio,
            micro_quality_score=micro_quality_score,
        )
    else:
        base_score = _clamp01(regime_score)
        if micro_quality_score is not None:
            base_score = _clamp01((base_score * 0.70) + (_clamp01(micro_quality_score) * 0.30))
    lower = max(float(settings.risk_multiplier_min), 0.0)
    upper = max(float(settings.risk_multiplier_max), lower)
    return lower + (base_score * (upper - lower))


def resolve_operational_max_positions(
    *,
    base_max_positions: int,
    settings: ModelAlphaOperationalSettings,
    regime_score: float,
    breadth_ratio: float,
    micro_quality_score: float | None = None,
) -> int:
    base_value = max(int(base_max_positions), 1)
    if breadth_ratio <= 0.10:
        return 1
    scale_min = max(float(settings.max_positions_scale_min), 0.10)
    scale_max = max(float(settings.max_positions_scale_max), scale_min)
    if bool(settings.empirical_state_score_model_enabled):
        state_score = _resolve_empirical_state_score(
            settings=settings,
            regime_score=regime_score,
            breadth_ratio=breadth_ratio,
            micro_quality_score=micro_quality_score,
        )
    else:
        state_score = _clamp01(regime_score)
    scaled = float(base_value) * (scale_min + (state_score * (scale_max - scale_min)))
    ceiling = max(int(math.ceil(float(base_value) * scale_max)), 1)
    return max(1, min(int(round(scaled)), ceiling))


def compute_micro_quality_composite(
    *,
    micro_snapshot: "MicroSnapshot | None",
    now_ts_ms: int | None,
    settings: ModelAlphaOperationalSettings,
) -> MicroQualityComposite | None:
    if micro_snapshot is None:
        return None
    age_ms = None
    if now_ts_ms is not None and int(micro_snapshot.last_event_ts_ms) > 0:
        age_ms = max(int(now_ts_ms) - int(micro_snapshot.last_event_ts_ms), 0)
    spread_bps = (
        float(micro_snapshot.spread_bps_mean)
        if micro_snapshot.spread_bps_mean is not None
        else None
    )
    depth_krw = (
        float(micro_snapshot.depth_top5_notional_krw)
        if micro_snapshot.depth_top5_notional_krw is not None
        else None
    )
    trade_cov = int(max(int(micro_snapshot.trade_coverage_ms), 0))
    book_cov = int(max(int(micro_snapshot.book_coverage_ms), 0))
    spread_score = _score_lower_better(spread_bps, float(settings.max_execution_spread_bps_for_join) or 20.0)
    depth_score = _score_log_depth(depth_krw, 5_000_000.0)
    trade_score = _score_higher_better(float(trade_cov), 60_000.0)
    book_score = _score_higher_better(float(book_cov), 60_000.0)
    age_score = _score_lower_better(float(age_ms) if age_ms is not None else None, float(settings.snapshot_stale_ms))
    score = _mean_scores([spread_score, depth_score, trade_score, book_score, age_score])
    return MicroQualityComposite(
        score=_clamp01(score),
        spread_score=_clamp01(spread_score),
        depth_score=_clamp01(depth_score),
        trade_coverage_score=_clamp01(trade_score),
        book_coverage_score=_clamp01(book_score),
        age_score=_clamp01(age_score),
        spread_bps=spread_bps,
        depth_krw=depth_krw,
        trade_coverage_ms=trade_cov,
        book_coverage_ms=book_cov,
        snapshot_age_ms=age_ms,
    )


def resolve_operational_execution_overlay(
    *,
    base_profile: OrderExecProfile,
    settings: ModelAlphaOperationalSettings,
    micro_quality: MicroQualityComposite | None,
    ts_ms: int,
) -> OperationalExecutionOverlayDecision:
    normalized = normalize_order_exec_profile(base_profile)
    session_bucket, _ = _session_bucket_and_score(ts_ms)
    if micro_quality is None:
        diagnostics = {
            "mode": "no_snapshot",
            "session_bucket": session_bucket,
            "micro_quality": None,
        }
        return OperationalExecutionOverlayDecision(
            risk_multiplier=1.0,
            exec_profile=normalized,
            micro_quality=None,
            abort_reason=None,
            diagnostics=diagnostics,
        )

    quality_score = _clamp01(micro_quality.score)
    if quality_score < float(settings.micro_quality_block_threshold):
        return OperationalExecutionOverlayDecision(
            risk_multiplier=max(float(settings.risk_multiplier_min), 0.0),
            exec_profile=normalized,
            micro_quality=micro_quality,
            abort_reason="MICRO_QUALITY_TOO_LOW",
            diagnostics={
                "mode": "abort",
                "session_bucket": session_bucket,
                "micro_quality": asdict(micro_quality),
            },
        )

    mode = "neutral"
    adjusted_mode = str(normalized.price_mode).strip().upper()
    spread_bps = float(micro_quality.spread_bps or 0.0)
    depth_krw = float(micro_quality.depth_krw or 0.0)
    conservative = (
        quality_score < float(settings.micro_quality_conservative_threshold)
        or spread_bps > float(settings.max_execution_spread_bps_for_join)
        or session_bucket == "offpeak"
    )
    aggressive = (
        quality_score >= float(settings.micro_quality_aggressive_threshold)
        and spread_bps <= float(settings.max_execution_spread_bps_for_cross)
        and depth_krw >= float(settings.min_execution_depth_krw_for_cross)
        and session_bucket == "asia_us_overlap"
    )

    if conservative:
        mode = "conservative"
        adjusted_mode = _demote_price_mode(adjusted_mode)
    elif aggressive:
        mode = "aggressive"
        adjusted_mode = _promote_price_mode(adjusted_mode)

    timeout_ms = int(normalized.timeout_ms)
    replace_interval_ms = int(normalized.replace_interval_ms)
    max_replaces = int(normalized.max_replaces)
    max_chase_bps = int(normalized.max_chase_bps)
    timeout_floor = max(int(settings.runtime_timeout_ms_floor), 1_000)
    replace_floor = max(
        int(settings.runtime_replace_interval_ms_floor),
        int(normalized.min_replace_interval_ms_global),
        1,
    )
    if conservative:
        timeout_ms = max(int(math.ceil(timeout_ms * float(settings.conservative_timeout_scale))), timeout_floor)
        replace_interval_ms = max(
            int(math.ceil(replace_interval_ms * float(settings.conservative_replace_interval_scale))),
            replace_floor,
        )
        max_replaces = max(int(math.floor(max_replaces * float(settings.conservative_max_replaces_scale))), 0)
        max_chase_bps = max(int(math.floor(max_chase_bps * float(settings.conservative_max_chase_bps_scale))), 0)
    elif aggressive:
        timeout_ms = max(int(math.floor(timeout_ms * float(settings.aggressive_timeout_scale))), timeout_floor)
        replace_interval_ms = max(
            int(math.floor(replace_interval_ms * float(settings.aggressive_replace_interval_scale))),
            replace_floor,
        )
        max_replaces = max(max_replaces + max(int(settings.aggressive_max_replaces_bonus), 0), max_replaces)
        max_chase_bps = max(max_chase_bps + max(int(settings.aggressive_max_chase_bps_bonus), 0), 0)

    profile = normalize_order_exec_profile(
        OrderExecProfile(
            timeout_ms=timeout_ms,
            replace_interval_ms=replace_interval_ms,
            max_replaces=max_replaces,
            price_mode=adjusted_mode,
            max_chase_bps=max_chase_bps,
            min_replace_interval_ms_global=int(normalized.min_replace_interval_ms_global),
            post_only=bool(normalized.post_only),
        )
    )
    runtime_risk = 0.85 + (quality_score * 0.25)
    return OperationalExecutionOverlayDecision(
        risk_multiplier=float(runtime_risk),
        exec_profile=profile,
        micro_quality=micro_quality,
        abort_reason=None,
        diagnostics={
            "mode": mode,
            "session_bucket": session_bucket,
            "runtime_risk_multiplier": float(runtime_risk),
            "base_price_mode": str(normalized.price_mode),
            "selected_price_mode": str(profile.price_mode),
            "base_timeout_ms": int(normalized.timeout_ms),
            "selected_timeout_ms": int(profile.timeout_ms),
            "base_replace_interval_ms": int(normalized.replace_interval_ms),
            "selected_replace_interval_ms": int(profile.replace_interval_ms),
            "base_max_replaces": int(normalized.max_replaces),
            "selected_max_replaces": int(profile.max_replaces),
            "base_max_chase_bps": int(normalized.max_chase_bps),
            "selected_max_chase_bps": int(profile.max_chase_bps),
            "micro_quality": asdict(micro_quality),
        },
    )


def _session_bucket_and_score(ts_ms: int) -> tuple[str, float]:
    dt = datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc)
    hour = int(dt.hour)
    if 12 <= hour < 16:
        return "asia_us_overlap", 1.0
    if 0 <= hour < 5:
        return "offpeak", 0.45
    return "normal", 0.70


def _promote_price_mode(mode: str) -> str:
    value = str(mode).strip().upper()
    if value == PRICE_MODE_PASSIVE_MAKER:
        return PRICE_MODE_JOIN
    if value == PRICE_MODE_JOIN:
        return PRICE_MODE_CROSS_1T
    return PRICE_MODE_CROSS_1T


def _demote_price_mode(mode: str) -> str:
    value = str(mode).strip().upper()
    if value == PRICE_MODE_CROSS_1T:
        return PRICE_MODE_JOIN
    if value == PRICE_MODE_JOIN:
        return PRICE_MODE_PASSIVE_MAKER
    return PRICE_MODE_PASSIVE_MAKER


def _median_or_zero(frame: pl.DataFrame, column: str) -> float:
    if column not in frame.columns or frame.height <= 0:
        return 0.0
    values = [float(item) for item in frame.get_column(column).drop_nulls().to_list() if item is not None]
    if not values:
        return 0.0
    values.sort()
    middle = len(values) // 2
    if len(values) % 2 == 1:
        return float(values[middle])
    return float((values[middle - 1] + values[middle]) / 2.0)


def _std_or_zero(frame: pl.DataFrame, column: str) -> float:
    if column not in frame.columns or frame.height <= 1:
        return 0.0
    values = [float(item) for item in frame.get_column(column).drop_nulls().to_list() if item is not None]
    if len(values) <= 1:
        return 0.0
    mean = sum(values) / float(len(values))
    variance = sum((value - mean) ** 2 for value in values) / float(len(values))
    return float(math.sqrt(max(variance, 0.0)))


def _sum_depth_krw(frame: pl.DataFrame) -> float:
    bid = _median_or_zero(frame, "m_depth_bid_top5_mean")
    ask = _median_or_zero(frame, "m_depth_ask_top5_mean")
    return max(float(bid) + float(ask), 0.0)


def _score_higher_better(value: float | None, reference: float) -> float:
    if value is None or reference <= 0:
        return 0.0
    return _clamp01(float(value) / float(reference))


def _score_lower_better(value: float | None, reference: float) -> float:
    if value is None or reference <= 0:
        return 0.0
    return _clamp01(1.0 - (float(value) / float(reference)))


def _score_log_depth(value: float | None, reference: float) -> float:
    if value is None or value <= 0 or reference <= 0:
        return 0.0
    numerator = math.log1p(float(value))
    denominator = math.log1p(float(reference))
    if denominator <= 0:
        return 0.0
    return _clamp01(numerator / denominator)


def _mean_scores(values: list[float], *, weighted: bool = False) -> float:
    valid = [float(value) for value in values]
    if not valid:
        return 0.0
    if weighted:
        return sum(valid)
    return sum(valid) / float(len(valid))


def _clamp01(value: float) -> float:
    return max(min(float(value), 1.0), 0.0)


def _resolve_empirical_state_score(
    *,
    settings: ModelAlphaOperationalSettings,
    regime_score: float,
    breadth_ratio: float | None,
    micro_quality_score: float | None,
) -> float:
    linear = float(settings.empirical_state_score_intercept)
    linear += float(settings.empirical_state_score_regime_coef) * _clamp01(regime_score)
    linear += float(settings.empirical_state_score_breadth_coef) * _clamp01(breadth_ratio or 0.0)
    linear += float(settings.empirical_state_score_micro_coef) * _clamp01(micro_quality_score or 0.0)
    scale = max(float(settings.empirical_state_score_output_scale), 1e-6)
    return _sigmoid(linear / scale)


def _sigmoid(value: float) -> float:
    clipped = max(min(float(value), 30.0), -30.0)
    return 1.0 / (1.0 + math.exp(-clipped))


def _coerce_like(*, current_value: Any, new_value: Any) -> Any:
    if isinstance(current_value, bool):
        return bool(new_value)
    if isinstance(current_value, int) and not isinstance(current_value, bool):
        try:
            return int(new_value)
        except (TypeError, ValueError):
            return current_value
    if isinstance(current_value, float):
        try:
            return float(new_value)
        except (TypeError, ValueError):
            return current_value
    if isinstance(current_value, str):
        return str(new_value)
    return new_value
