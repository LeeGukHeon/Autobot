"""Shared data-quality budget helpers for train/eval/live layers."""

from __future__ import annotations

from typing import Any

import polars as pl


def clamp_quality_weight_from_synth_ratio(
    synth_ratio: float | None,
    *,
    floor: float,
    power: float,
) -> float:
    if synth_ratio is None:
        return 1.0
    bounded_floor = min(max(float(floor), 0.0), 1.0)
    bounded_power = max(float(power), 0.0)
    quality_base = min(max(1.0 - float(synth_ratio), bounded_floor), 1.0)
    return float(quality_base**bounded_power)


def quality_weight_expr_from_synth_ratio(
    *,
    synth_ratio_expr: pl.Expr,
    floor: float,
    power: float,
) -> pl.Expr:
    bounded_floor = min(max(float(floor), 0.0), 1.0)
    bounded_power = max(float(power), 0.0)
    return (
        (pl.lit(1.0, dtype=pl.Float64) - synth_ratio_expr.cast(pl.Float64))
        .clip(bounded_floor, 1.0)
        .pow(bounded_power)
        .fill_null(1.0)
    )


def resolve_universe_quality_score(
    *,
    value_est: float,
    synth_ratio_lookback: float | None,
    q_floor: float,
    beta: float,
) -> dict[str, float]:
    bounded_q_floor = min(max(float(q_floor), 0.0), 1.0)
    bounded_beta = max(float(beta), 0.0)
    if synth_ratio_lookback is None:
        q_value = 1.0
    else:
        q_value = min(max(1.0 - float(synth_ratio_lookback), bounded_q_floor), 1.0)
    quality_weight = float(q_value**bounded_beta)
    score = float(value_est) * float(quality_weight)
    return {
        "q": float(q_value),
        "quality_weight": float(quality_weight),
        "score": float(score),
    }


def resolve_market_quality_budget(
    *,
    market: str,
    one_m_synth_ratio_mean: float | None,
    rows_dropped_no_micro: int | float | None = None,
    validate_status: str = "",
    leakage_fail_rows: int | float | None = None,
    stale_rows: int | float | None = None,
    universe_quality_weight: float | None = None,
    universe_quality_score: float | None = None,
    selected_for_universe: bool = False,
    synth_weight_floor: float = 0.2,
    synth_weight_power: float = 2.0,
) -> dict[str, Any]:
    synth_quality_weight = clamp_quality_weight_from_synth_ratio(
        one_m_synth_ratio_mean,
        floor=synth_weight_floor,
        power=synth_weight_power,
    )
    dropped_no_micro_value = max(int(rows_dropped_no_micro or 0), 0)
    leakage_fail_rows_value = max(int(leakage_fail_rows or 0), 0)
    stale_rows_value = max(int(stale_rows or 0), 0)
    validation_pass = (
        str(validate_status or "").strip().upper() not in {"FAIL"}
        and leakage_fail_rows_value <= 0
        and stale_rows_value <= 0
    )
    micro_drop_multiplier = 0.75 if dropped_no_micro_value > 0 else 1.0
    training_weight_multiplier = min(float(synth_quality_weight), float(micro_drop_multiplier))
    universe_weight_value = max(min(float(universe_quality_weight or 1.0), 1.0), 0.0)
    paired_inclusion_weight = min(float(training_weight_multiplier), float(universe_weight_value))
    selected_for_universe_value = bool(selected_for_universe)
    paired_inclusion_allowed = bool(
        validation_pass
        and (selected_for_universe_value or float(universe_quality_score or 0.0) > 0.0)
        and float(paired_inclusion_weight) >= 0.25
    )
    return {
        "market": str(market or "").strip().upper(),
        "one_m_synth_ratio_mean": _safe_optional_float(one_m_synth_ratio_mean),
        "rows_dropped_no_micro": int(dropped_no_micro_value),
        "validate_status": str(validate_status or "").strip().upper() or "UNKNOWN",
        "leakage_fail_rows": int(leakage_fail_rows_value),
        "stale_rows": int(stale_rows_value),
        "validation_pass": bool(validation_pass),
        "training_weight_multiplier": float(training_weight_multiplier),
        "universe_quality_weight": float(universe_weight_value),
        "universe_quality_score": _safe_optional_float(universe_quality_score),
        "selected_for_universe": bool(selected_for_universe_value),
        "paired_inclusion_weight": float(paired_inclusion_weight),
        "paired_inclusion_allowed": bool(paired_inclusion_allowed),
    }


def summarize_market_quality_budget(entries: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> dict[str, Any]:
    rows = [dict(item) for item in entries if isinstance(item, dict)]
    selected_rows = [item for item in rows if bool(item.get("selected_for_universe", False))]
    if not selected_rows:
        selected_rows = list(rows)
    eligible_rows = [item for item in selected_rows if bool(item.get("paired_inclusion_allowed", False))]
    training_weights = [
        float(item.get("training_weight_multiplier") or 0.0)
        for item in selected_rows
    ]
    universe_scores = [
        float(item.get("universe_quality_score") or 0.0)
        for item in selected_rows
        if item.get("universe_quality_score") is not None
    ]
    paired_weights = [
        float(item.get("paired_inclusion_weight") or 0.0)
        for item in selected_rows
    ]
    selected_count = len(selected_rows)
    eligible_count = len(eligible_rows)
    eligible_ratio = (float(eligible_count) / float(selected_count)) if selected_count > 0 else 0.0
    training_weight_mean = (sum(training_weights) / float(len(training_weights))) if training_weights else 0.0
    paired_weight_mean = (sum(paired_weights) / float(len(paired_weights))) if paired_weights else 0.0
    universe_quality_score_mean = (sum(universe_scores) / float(len(universe_scores))) if universe_scores else 0.0
    paired_inclusion_pass = bool(
        selected_count > 0
        and eligible_count > 0
        and training_weight_mean >= 0.25
        and paired_weight_mean >= 0.25
    )
    return {
        "selected_market_count": int(selected_count),
        "eligible_market_count": int(eligible_count),
        "eligible_market_ratio": float(eligible_ratio),
        "training_weight_mean": float(training_weight_mean),
        "paired_inclusion_weight_mean": float(paired_weight_mean),
        "universe_quality_score_mean": float(universe_quality_score_mean),
        "paired_inclusion_pass": bool(paired_inclusion_pass),
    }


def resolve_market_quality_weight_map(certification_payload: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    payload = dict(certification_payload or {})
    rows = payload.get("market_quality_budget")
    if not isinstance(rows, list):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for item in rows:
        if not isinstance(item, dict):
            continue
        market = str(item.get("market") or "").strip().upper()
        if not market:
            continue
        out[market] = dict(item)
    return out


def _safe_optional_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
