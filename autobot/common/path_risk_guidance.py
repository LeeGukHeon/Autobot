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
    drawdown_from_now_q80 = _as_float(selected.get("drawdown_from_now_q80"))
    drawdown_from_now_q90 = _as_float(selected.get("drawdown_from_now_q90"))
    terminal_return_q50 = _as_float(selected.get("terminal_return_q50"))
    terminal_return_q25 = _as_float(selected.get("terminal_return_q25"))
    terminal_return_q75 = _as_float(selected.get("terminal_return_q75"))
    terminal_return_mean = _as_float(selected.get("terminal_return_mean"))
    terminal_positive_rate = _as_float(selected.get("terminal_positive_rate"))
    terminal_nonnegative_rate = _as_float(selected.get("terminal_nonnegative_rate"))
    terminal_above_10bps_rate = _as_float(selected.get("terminal_above_10bps_rate"))
    terminal_above_25bps_rate = _as_float(selected.get("terminal_above_25bps_rate"))
    terminal_above_50bps_rate = _as_float(selected.get("terminal_above_50bps_rate"))
    min_tp_floor_ratio = max(_as_float(payload.get("min_tp_floor_pct")) or 0.0, 0.0)
    continuation_margin_ratio = max(min_tp_floor_ratio, 0.0005)
    continuation_profit_floor_ratio = max(min_tp_floor_ratio, 0.001)
    expected_exit_fee_rate = max(_as_float(payload.get("expected_exit_fee_rate")) or 0.0, 0.0)
    expected_exit_slippage_bps = max(_as_float(payload.get("expected_exit_slippage_bps")) or 0.0, 0.0)
    immediate_exit_fee_rate = max(_as_float(payload.get("expected_immediate_exit_fee_rate")) or expected_exit_fee_rate, 0.0)
    immediate_exit_slippage_bps = max(
        _as_float(payload.get("expected_immediate_exit_slippage_bps")) or expected_exit_slippage_bps,
        0.0,
    )
    immediate_exit_fill_probability = _as_float(payload.get("expected_immediate_exit_fill_probability"))
    if immediate_exit_fill_probability is None:
        immediate_exit_fill_probability = 1.0
    immediate_exit_fill_probability = max(min(float(immediate_exit_fill_probability), 1.0), 0.0)
    immediate_exit_time_to_fill_ms = _as_int(payload.get("expected_immediate_exit_time_to_fill_ms"))
    immediate_exit_price_mode = str(payload.get("expected_immediate_exit_price_mode") or "").strip().upper()
    immediate_exit_cost_ratio = _as_float(payload.get("expected_immediate_exit_cost_ratio"))
    if immediate_exit_cost_ratio is None:
        immediate_exit_cost_ratio = float(immediate_exit_fee_rate) + (float(immediate_exit_slippage_bps) / 10_000.0)
    deferred_exit_cost_ratio = immediate_exit_cost_ratio

    continuation_should_exit = False
    continuation_reason_code = ""
    continuation_threshold_ratio = None
    immediate_exit_value_ratio = _as_float(current_return_ratio)
    alpha_decay_penalty_ratio = None
    if immediate_exit_value_ratio is not None:
        continuation_anchor_for_decay = (
            terminal_return_q75
            if terminal_return_q75 is not None
            else (terminal_return_q50 if terminal_return_q50 is not None else terminal_return_mean)
        )
        if continuation_anchor_for_decay is not None:
            alpha_decay_penalty_ratio = max(
                float(immediate_exit_value_ratio) - float(continuation_anchor_for_decay),
                0.0,
            ) * min(float(remaining_bars) / float(max(hold_bars, 1)), 1.0) * 0.5
    exit_now_value_net = (
        (float(immediate_exit_value_ratio) - float(immediate_exit_cost_ratio))
        if immediate_exit_value_ratio is not None
        else None
    )
    continue_value_net = (
        float(terminal_return_q50) - float(deferred_exit_cost_ratio) - float(alpha_decay_penalty_ratio or 0.0)
        if terminal_return_q50 is not None
        else None
    )
    optimistic_continue_value_net = (
        float(terminal_return_q75) - float(deferred_exit_cost_ratio)
        if terminal_return_q75 is not None
        else None
    )
    continuation_gap_ratio = (
        float(continue_value_net) - float(exit_now_value_net)
        if continue_value_net is not None and exit_now_value_net is not None
        else None
    )
    continuation_value_ratio = terminal_return_q50
    continuation_advantage_ratio = continuation_gap_ratio
    upside_left_ratio = (
        max(float(reachable_tp_ratio) - float(immediate_exit_value_ratio), 0.0)
        if reachable_tp_ratio is not None and immediate_exit_value_ratio is not None
        else None
    )
    profit_preservation_rate = _resolve_profit_preservation_rate(
        current_return_ratio=immediate_exit_value_ratio,
        terminal_positive_rate=terminal_positive_rate,
        terminal_nonnegative_rate=terminal_nonnegative_rate,
        terminal_above_10bps_rate=terminal_above_10bps_rate,
        terminal_above_25bps_rate=terminal_above_25bps_rate,
        terminal_above_50bps_rate=terminal_above_50bps_rate,
    )
    if immediate_exit_value_ratio is not None and float(immediate_exit_value_ratio) > 0.0:
        continuation_anchor = (
            float(terminal_return_q75)
            if terminal_return_q75 is not None
            else (
                float(terminal_return_q50)
                if terminal_return_q50 is not None
                else (float(terminal_return_mean) if terminal_return_mean is not None else None)
            )
        )
        if continuation_anchor is not None:
            continuation_threshold_ratio = max(
                float(continuation_anchor) + float(continuation_margin_ratio),
                float(continuation_profit_floor_ratio),
            )
            if (
                continuation_gap_ratio is not None
                and float(continuation_gap_ratio) <= -float(continuation_margin_ratio)
            ):
                continuation_should_exit = True
                continuation_reason_code = "PATH_RISK_CONTINUATION_CAPTURE"
            elif (
                upside_left_ratio is not None
                and float(upside_left_ratio) <= float(continuation_margin_ratio)
                and optimistic_continue_value_net is not None
                and exit_now_value_net is not None
                and float(optimistic_continue_value_net) <= float(exit_now_value_net) + float(continuation_margin_ratio)
            ):
                continuation_should_exit = True
                continuation_reason_code = "PATH_RISK_CONTINUATION_CAPTURE"
            elif (
                profit_preservation_rate is not None
                and float(profit_preservation_rate) < 0.45
                and exit_now_value_net is not None
                and continue_value_net is not None
                and float(exit_now_value_net) >= float(continue_value_net)
            ):
                continuation_should_exit = True
                continuation_reason_code = "PATH_RISK_CONTINUATION_CAPTURE"
            elif (
                exit_now_value_net is not None
                and float(immediate_exit_value_ratio) >= float(continuation_threshold_ratio)
                and continue_value_net is not None
                and float(exit_now_value_net) >= float(continue_value_net)
            ):
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
        "drawdown_from_now_q80": drawdown_from_now_q80,
        "drawdown_from_now_q90": drawdown_from_now_q90,
        "terminal_return_q25": terminal_return_q25,
        "terminal_return_q50": terminal_return_q50,
        "terminal_return_q75": terminal_return_q75,
        "terminal_return_mean": terminal_return_mean,
        "terminal_positive_rate": terminal_positive_rate,
        "terminal_nonnegative_rate": terminal_nonnegative_rate,
        "terminal_above_10bps_rate": terminal_above_10bps_rate,
        "terminal_above_25bps_rate": terminal_above_25bps_rate,
        "terminal_above_50bps_rate": terminal_above_50bps_rate,
        "selection_score": resolved_selection_score,
        "risk_feature_value": resolved_risk_feature_value,
        "immediate_exit_value_ratio": immediate_exit_value_ratio,
        "immediate_exit_fee_rate": float(immediate_exit_fee_rate),
        "immediate_exit_slippage_bps": float(immediate_exit_slippage_bps),
        "immediate_exit_fill_probability": float(immediate_exit_fill_probability),
        "immediate_exit_time_to_fill_ms": immediate_exit_time_to_fill_ms,
        "immediate_exit_price_mode": immediate_exit_price_mode,
        "immediate_exit_cost_ratio": float(immediate_exit_cost_ratio),
        "deferred_exit_cost_ratio": float(deferred_exit_cost_ratio),
        "continuation_value_ratio": continuation_value_ratio,
        "exit_now_value_net": exit_now_value_net,
        "continue_value_net": continue_value_net,
        "optimistic_continue_value_net": optimistic_continue_value_net,
        "alpha_decay_penalty_ratio": alpha_decay_penalty_ratio,
        "profit_preservation_rate": profit_preservation_rate,
        "continuation_gap_ratio": continuation_gap_ratio,
        "continuation_advantage_ratio": continuation_advantage_ratio,
        "upside_left_ratio": upside_left_ratio,
        "continuation_margin_ratio": float(continuation_margin_ratio),
        "continuation_profit_floor_ratio": float(continuation_profit_floor_ratio),
        "continuation_threshold_ratio": continuation_threshold_ratio,
        "continuation_should_exit": bool(continuation_should_exit),
        "continuation_reason_code": continuation_reason_code,
    }


def _resolve_profit_preservation_rate(
    *,
    current_return_ratio: float | None,
    terminal_positive_rate: float | None,
    terminal_nonnegative_rate: float | None,
    terminal_above_10bps_rate: float | None,
    terminal_above_25bps_rate: float | None,
    terminal_above_50bps_rate: float | None,
) -> float | None:
    if current_return_ratio is None:
        return terminal_positive_rate
    current_return = float(current_return_ratio)
    if current_return >= 0.0050 and terminal_above_50bps_rate is not None:
        return float(terminal_above_50bps_rate)
    if current_return >= 0.0025 and terminal_above_25bps_rate is not None:
        return float(terminal_above_25bps_rate)
    if current_return >= 0.0010 and terminal_above_10bps_rate is not None:
        return float(terminal_above_10bps_rate)
    if current_return >= 0.0 and terminal_nonnegative_rate is not None:
        return float(terminal_nonnegative_rate)
    return terminal_positive_rate


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
