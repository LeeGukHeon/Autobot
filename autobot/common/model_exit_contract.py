from __future__ import annotations

from typing import Any


MODEL_EXIT_PLAN_SOURCE = "model_alpha_v1"
MODEL_EXIT_PLAN_VERSION = 1


def normalize_model_exit_plan_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    raw = dict(payload or {}) if isinstance(payload, dict) else {}
    source = str(raw.get("source", MODEL_EXIT_PLAN_SOURCE)).strip().lower() or MODEL_EXIT_PLAN_SOURCE
    mode = str(raw.get("mode", "hold")).strip().lower() or "hold"
    hold_bars = max(_as_int(raw.get("hold_bars")) or 0, 0)
    bar_interval_ms = max(_first_int(raw.get("bar_interval_ms"), raw.get("interval_ms")) or 0, 0)
    timeout_delta_ms = max(_as_int(raw.get("timeout_delta_ms")) or 0, 0)
    if timeout_delta_ms <= 0 and hold_bars > 0 and bar_interval_ms > 0:
        timeout_delta_ms = hold_bars * bar_interval_ms
    if bar_interval_ms <= 0 and hold_bars > 0 and timeout_delta_ms > 0:
        bar_interval_ms = int(timeout_delta_ms / hold_bars)

    tp_ratio = max(_first_float(raw.get("tp_ratio"), raw.get("tp_pct")) or 0.0, 0.0)
    sl_ratio = max(_first_float(raw.get("sl_ratio"), raw.get("sl_pct")) or 0.0, 0.0)
    trailing_ratio = max(_first_float(raw.get("trailing_ratio"), raw.get("trailing_pct")) or 0.0, 0.0)

    expected_exit_fee_ratio = max(
        _first_float(raw.get("expected_exit_fee_ratio"), raw.get("expected_exit_fee_rate")) or 0.0,
        0.0,
    )
    expected_exit_slippage_bps = max(_as_float(raw.get("expected_exit_slippage_bps")) or 0.0, 0.0)

    normalized = dict(raw)
    normalized.update(
        {
            "source": source,
            "version": max(_as_int(raw.get("version")) or MODEL_EXIT_PLAN_VERSION, 1),
            "mode": mode,
            "hold_bars": hold_bars,
            "bar_interval_ms": bar_interval_ms,
            "interval_ms": bar_interval_ms,
            "timeout_delta_ms": timeout_delta_ms,
            "tp_ratio": tp_ratio,
            "sl_ratio": sl_ratio,
            "trailing_ratio": trailing_ratio,
            "tp_pct": tp_ratio,
            "sl_pct": sl_ratio,
            "trailing_pct": trailing_ratio,
            "expected_exit_fee_ratio": expected_exit_fee_ratio,
            "expected_exit_fee_rate": expected_exit_fee_ratio,
            "expected_exit_slippage_bps": expected_exit_slippage_bps,
        }
    )
    return normalized


def is_model_exit_plan_payload(payload: dict[str, Any] | None) -> bool:
    if not isinstance(payload, dict):
        return False
    normalized = normalize_model_exit_plan_payload(payload)
    return str(normalized.get("source", "")).strip().lower() == MODEL_EXIT_PLAN_SOURCE


def _first_float(*values: Any) -> float | None:
    for value in values:
        parsed = _as_float(value)
        if parsed is not None:
            return parsed
    return None


def _first_int(*values: Any) -> int | None:
    for value in values:
        parsed = _as_int(value)
        if parsed is not None:
            return parsed
    return None


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
