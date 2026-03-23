from __future__ import annotations

from typing import Any


def resolve_path_risk_guidance_from_plan(
    *,
    plan_payload: dict[str, Any] | None,
    elapsed_bars: int | None = None,
    created_ts: int | None = None,
    ts_ms: int | None = None,
    current_return_ratio: float | None = None,
    selection_score: float | None = None,
    risk_feature_value: float | None = None,
) -> dict[str, Any]:
    payload = dict(plan_payload or {}) if isinstance(plan_payload, dict) else {}
    path_risk = dict(payload.get("path_risk") or {}) if isinstance(payload.get("path_risk"), dict) else {}
    if str(path_risk.get("status", "")).strip().lower() != "ready":
        return {"applied": False}

    overall_summaries = [
        dict(item)
        for item in (path_risk.get("overall_by_horizon") or [])
        if isinstance(item, dict) and int(item.get("hold_bars", 0) or 0) > 0
    ]
    bucket_summaries = [
        dict(item)
        for item in (path_risk.get("by_bucket") or [])
        if isinstance(item, dict) and int(item.get("hold_bars", 0) or 0) > 0
    ]
    if not overall_summaries:
        recommended_summary = dict(path_risk.get("recommended_summary") or {})
        if recommended_summary:
            overall_summaries = [recommended_summary]
    if not overall_summaries and not bucket_summaries:
        return {"applied": False}

    hold_bars = max(_as_int(payload.get("hold_bars")) or 0, 1)
    if elapsed_bars is None:
        if created_ts is not None and ts_ms is not None:
            bar_interval_ms = max(_as_int(payload.get("bar_interval_ms")) or _as_int(payload.get("interval_ms")) or 0, 1)
            elapsed_bars = max(int((int(ts_ms) - int(created_ts)) // int(bar_interval_ms)), 0)
        else:
            elapsed_bars = 0
    remaining_bars = max(int(hold_bars) - int(elapsed_bars), 1)
    resolved_selection_score = _as_float(selection_score)
    if resolved_selection_score is None:
        resolved_selection_score = _as_float(payload.get("entry_selection_score"))
    resolved_risk_feature_value = _as_float(risk_feature_value)
    if resolved_risk_feature_value is None:
        resolved_risk_feature_value = _as_float(payload.get("entry_risk_feature_value"))
    selected = _select_path_risk_summary(
        overall_summaries=overall_summaries,
        bucket_summaries=bucket_summaries,
        selection_score=resolved_selection_score,
        risk_feature_value=resolved_risk_feature_value,
        selection_bucket_bounds=path_risk.get("selection_bucket_bounds"),
        risk_bucket_bounds=path_risk.get("risk_bucket_bounds"),
        remaining_bars=remaining_bars,
    )
    if not isinstance(selected, dict):
        return {"applied": False}
    reachable_tp_ratio = _as_float(selected.get("reachable_tp_q60"))
    bounded_sl_ratio = _as_float(selected.get("bounded_sl_q80"))
    terminal_return_q50 = _as_float(selected.get("terminal_return_q50"))
    terminal_return_q75 = _as_float(selected.get("terminal_return_q75"))
    min_tp_floor_ratio = max(_as_float(payload.get("min_tp_floor_pct")) or 0.0, 0.0)
    continuation_margin_ratio = max(min_tp_floor_ratio, 0.0005)
    continuation_profit_floor_ratio = max(min_tp_floor_ratio, 0.001)

    continuation_should_exit = False
    continuation_reason_code = ""
    continuation_threshold_ratio = None
    immediate_exit_value_ratio = _as_float(current_return_ratio)
    continuation_value_ratio = terminal_return_q50
    continuation_advantage_ratio = (
        (float(continuation_value_ratio) - float(immediate_exit_value_ratio))
        if continuation_value_ratio is not None and immediate_exit_value_ratio is not None
        else None
    )
    upside_left_ratio = (
        max(float(reachable_tp_ratio) - float(immediate_exit_value_ratio), 0.0)
        if reachable_tp_ratio is not None and immediate_exit_value_ratio is not None
        else None
    )
    if immediate_exit_value_ratio is not None and float(immediate_exit_value_ratio) > 0.0:
        continuation_anchor = (
            float(terminal_return_q75)
            if terminal_return_q75 is not None
            else (float(terminal_return_q50) if terminal_return_q50 is not None else None)
        )
        if continuation_anchor is not None:
            continuation_threshold_ratio = max(
                float(continuation_anchor) + float(continuation_margin_ratio),
                float(continuation_profit_floor_ratio),
            )
            if (
                continuation_advantage_ratio is not None
                and float(continuation_advantage_ratio) <= -float(continuation_margin_ratio)
            ):
                continuation_should_exit = True
                continuation_reason_code = "PATH_RISK_CONTINUATION_CAPTURE"
            elif upside_left_ratio is not None and float(upside_left_ratio) <= float(continuation_margin_ratio):
                continuation_should_exit = True
                continuation_reason_code = "PATH_RISK_CONTINUATION_CAPTURE"
            elif float(immediate_exit_value_ratio) >= float(continuation_threshold_ratio):
                continuation_should_exit = True
                continuation_reason_code = "PATH_RISK_CONTINUATION_CAPTURE"

    return {
        "applied": bool(reachable_tp_ratio is not None or bounded_sl_ratio is not None or continuation_should_exit),
        "selected_hold_bars": int(selected.get("hold_bars", remaining_bars) or remaining_bars),
        "selected_selection_bucket": _as_int(selected.get("selection_bucket")),
        "selected_risk_bucket": _as_int(selected.get("risk_bucket")),
        "remaining_bars": int(remaining_bars),
        "reachable_tp_ratio": reachable_tp_ratio,
        "bounded_sl_ratio": bounded_sl_ratio,
        "terminal_return_q50": terminal_return_q50,
        "terminal_return_q75": terminal_return_q75,
        "selection_score": resolved_selection_score,
        "risk_feature_value": resolved_risk_feature_value,
        "immediate_exit_value_ratio": immediate_exit_value_ratio,
        "continuation_value_ratio": continuation_value_ratio,
        "continuation_advantage_ratio": continuation_advantage_ratio,
        "upside_left_ratio": upside_left_ratio,
        "continuation_margin_ratio": float(continuation_margin_ratio),
        "continuation_profit_floor_ratio": float(continuation_profit_floor_ratio),
        "continuation_threshold_ratio": continuation_threshold_ratio,
        "continuation_should_exit": bool(continuation_should_exit),
        "continuation_reason_code": continuation_reason_code,
    }


def _select_path_risk_summary(
    *,
    overall_summaries: list[dict[str, Any]],
    bucket_summaries: list[dict[str, Any]],
    selection_score: float | None,
    risk_feature_value: float | None,
    selection_bucket_bounds: Any,
    risk_bucket_bounds: Any,
    remaining_bars: int,
) -> dict[str, Any] | None:
    if selection_score is not None and risk_feature_value is not None and bucket_summaries:
        selection_bounds = _normalize_bounds(selection_bucket_bounds)
        risk_bounds = _normalize_bounds(risk_bucket_bounds)
        selection_bucket = _resolve_bucket_index(float(selection_score), selection_bounds)
        risk_bucket = _resolve_bucket_index(float(risk_feature_value), risk_bounds)
        candidates = [
            item
            for item in bucket_summaries
            if _as_int(item.get("selection_bucket")) == int(selection_bucket)
            and _as_int(item.get("risk_bucket")) == int(risk_bucket)
        ]
        if candidates:
            return min(
                candidates,
                key=lambda item: (
                    abs(int(item.get("hold_bars", remaining_bars) or remaining_bars) - int(remaining_bars)),
                    int(item.get("hold_bars", remaining_bars) or remaining_bars),
                ),
            )
    if overall_summaries:
        return min(
            overall_summaries,
            key=lambda item: (
                abs(int(item.get("hold_bars", remaining_bars) or remaining_bars) - int(remaining_bars)),
                int(item.get("hold_bars", remaining_bars) or remaining_bars),
            ),
        )
    return None


def _normalize_bounds(value: Any) -> list[float]:
    if not isinstance(value, list):
        return []
    bounds: list[float] = []
    for item in value:
        parsed = _as_float(item)
        if parsed is not None:
            bounds.append(float(parsed))
    return bounds


def _resolve_bucket_index(value: float, bounds: list[float]) -> int:
    bucket = 0
    for bound in bounds:
        if float(value) > float(bound):
            bucket += 1
            continue
        break
    return int(bucket)


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
