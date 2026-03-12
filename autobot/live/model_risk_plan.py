from __future__ import annotations

import json
from typing import Any

from autobot.common.model_exit_contract import is_model_exit_plan_payload, normalize_model_exit_plan_payload

from .state_store import PositionRecord, RiskPlanRecord


def extract_model_exit_plan(meta: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(meta, dict):
        return None
    payload = meta.get("model_exit_plan")
    if not isinstance(payload, dict):
        strategy_payload = meta.get("strategy")
        if isinstance(strategy_payload, dict):
            strategy_meta = strategy_payload.get("meta")
            if isinstance(strategy_meta, dict):
                payload = strategy_meta.get("model_exit_plan")
    if not is_model_exit_plan_payload(payload):
        return None
    return normalize_model_exit_plan_payload(payload)


def build_position_record_from_model_exit_plan(
    *,
    market: str,
    base_currency: str,
    base_amount: float,
    avg_entry_price: float,
    plan_payload: dict[str, Any],
    updated_ts: int,
    managed: bool = True,
) -> PositionRecord:
    normalized_plan = normalize_model_exit_plan_payload(plan_payload)
    tp_json, sl_json, trailing_json = _build_position_policy_jsons(normalized_plan)
    return PositionRecord(
        market=str(market).strip().upper(),
        base_currency=str(base_currency).strip().upper(),
        base_amount=float(base_amount),
        avg_entry_price=float(avg_entry_price),
        updated_ts=int(updated_ts),
        tp_json=tp_json,
        sl_json=sl_json,
        trailing_json=trailing_json,
        managed=bool(managed),
    )


def build_risk_plan_record_from_model_exit_plan(
    *,
    market: str,
    qty: float,
    entry_price: float,
    plan_payload: dict[str, Any],
    created_ts: int,
    updated_ts: int,
    plan_id: str,
    source_intent_id: str | None,
) -> RiskPlanRecord:
    normalized_plan = normalize_model_exit_plan_payload(plan_payload)
    timeout_delta_ms = max(_as_int(normalized_plan.get("timeout_delta_ms")) or 0, 0)
    timeout_ts_ms = int(created_ts) + timeout_delta_ms if timeout_delta_ms > 0 else None
    tp_pct = _to_percent_points(_as_float(normalized_plan.get("tp_ratio")))
    sl_pct = _to_percent_points(_as_float(normalized_plan.get("sl_ratio")))
    trailing_pct = max(_as_float(normalized_plan.get("trailing_ratio")) or 0.0, 0.0)
    return RiskPlanRecord(
        plan_id=str(plan_id).strip(),
        market=str(market).strip().upper(),
        side="long",
        entry_price_str=_format_decimal(entry_price),
        qty_str=_format_decimal(qty),
        tp_enabled=(tp_pct or 0.0) > 0.0,
        tp_price_str=None,
        tp_pct=tp_pct if (tp_pct or 0.0) > 0.0 else None,
        sl_enabled=(sl_pct or 0.0) > 0.0,
        sl_price_str=None,
        sl_pct=sl_pct if (sl_pct or 0.0) > 0.0 else None,
        trailing_enabled=trailing_pct > 0.0,
        trail_pct=trailing_pct if trailing_pct > 0.0 else None,
        high_watermark_price_str=None,
        armed_ts_ms=None,
        timeout_ts_ms=timeout_ts_ms,
        state="ACTIVE",
        last_eval_ts_ms=int(updated_ts),
        last_action_ts_ms=0,
        current_exit_order_uuid=None,
        current_exit_order_identifier=None,
        replace_attempt=0,
        created_ts=int(created_ts),
        updated_ts=int(updated_ts),
        plan_source="model_alpha_v1",
        source_intent_id=_as_optional_str(source_intent_id),
    )


def build_model_risk_plan_id(*, market: str, intent_id: str | None) -> str:
    market_value = str(market).strip().upper()
    intent_value = _as_optional_str(intent_id)
    if intent_value:
        return f"model-risk-{intent_value[:24]}"
    return f"model-risk-{market_value}"


def build_model_derived_risk_records(
    *,
    market: str,
    base_currency: str,
    base_amount: float,
    avg_entry_price: float,
    plan_payload: dict[str, Any],
    created_ts: int,
    updated_ts: int,
    intent_id: str | None,
) -> tuple[PositionRecord, RiskPlanRecord]:
    plan_id = build_model_risk_plan_id(market=market, intent_id=intent_id)
    return (
        build_position_record_from_model_exit_plan(
            market=market,
            base_currency=base_currency,
            base_amount=base_amount,
            avg_entry_price=avg_entry_price,
            plan_payload=plan_payload,
            updated_ts=updated_ts,
            managed=True,
        ),
        build_risk_plan_record_from_model_exit_plan(
            market=market,
            qty=base_amount,
            entry_price=avg_entry_price,
            plan_payload=plan_payload,
            created_ts=created_ts,
            updated_ts=updated_ts,
            plan_id=plan_id,
            source_intent_id=intent_id,
        ),
    )


def build_model_exit_plan_from_position(position: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(position, dict):
        return None
    tp = dict(position.get("tp") or {})
    sl = dict(position.get("sl") or {})
    trailing = dict(position.get("trailing") or {})
    shared = next(
        (
            source
            for source in (tp, sl, trailing)
            if str(source.get("source", "")).strip().lower() == "model_alpha_v1"
        ),
        None,
    )
    if not isinstance(shared, dict):
        return None
    hold_bars = max(_as_int(shared.get("hold_bars")) or 0, 0)
    timeout_delta_ms = max(_as_int(shared.get("timeout_delta_ms")) or 0, 0)
    interval_ms = int(timeout_delta_ms / hold_bars) if hold_bars > 0 and timeout_delta_ms > 0 else 0
    tp_pct = _from_percent_points(_as_float(tp.get("tp_pct")))
    sl_pct = _from_percent_points(_as_float(sl.get("sl_pct")))
    trailing_pct = max(_as_float(trailing.get("trail_pct")) or 0.0, 0.0)
    return normalize_model_exit_plan_payload(
        {
            "source": "model_alpha_v1",
            "version": 1,
            "mode": str(shared.get("mode", "hold")).strip().lower() or "hold",
            "hold_bars": hold_bars,
            "interval_ms": interval_ms,
            "timeout_delta_ms": timeout_delta_ms,
            "tp_pct": float(tp_pct or 0.0),
            "sl_pct": float(sl_pct or 0.0),
            "trailing_pct": float(trailing_pct),
            "expected_exit_fee_rate": 0.0,
            "expected_exit_slippage_bps": 0.0,
        }
    )


def _build_position_policy_jsons(plan_payload: dict[str, Any]) -> tuple[str, str, str]:
    normalized_plan = normalize_model_exit_plan_payload(plan_payload)
    mode = str(normalized_plan.get("mode", "hold")).strip().lower() or "hold"
    hold_bars = max(_as_int(normalized_plan.get("hold_bars")) or 0, 0)
    timeout_delta_ms = max(_as_int(normalized_plan.get("timeout_delta_ms")) or 0, 0)
    tp_pct = _to_percent_points(_as_float(normalized_plan.get("tp_ratio")))
    sl_pct = _to_percent_points(_as_float(normalized_plan.get("sl_ratio")))
    trailing_pct = max(_as_float(normalized_plan.get("trailing_ratio")) or 0.0, 0.0)
    trailing_enabled = trailing_pct > 0.0
    shared = {
        "source": "model_alpha_v1",
        "mode": mode,
        "hold_bars": hold_bars,
        "timeout_delta_ms": timeout_delta_ms,
    }
    tp_json = json.dumps(
        {
            **shared,
            "enabled": (tp_pct or 0.0) > 0.0,
            "tp_pct": tp_pct if (tp_pct or 0.0) > 0.0 else None,
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    sl_json = json.dumps(
        {
            **shared,
            "enabled": (sl_pct or 0.0) > 0.0,
            "sl_pct": sl_pct if (sl_pct or 0.0) > 0.0 else None,
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    trailing_json = json.dumps(
        {
            **shared,
            "enabled": trailing_enabled,
            "trail_pct": trailing_pct if trailing_enabled else None,
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    return tp_json, sl_json, trailing_json


def _format_decimal(value: float) -> str:
    text = f"{float(value):.12f}"
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _to_percent_points(value: float | None) -> float | None:
    if value is None:
        return None
    return max(float(value) * 100.0, 0.0)


def _from_percent_points(value: float | None) -> float | None:
    if value is None:
        return None
    return max(float(value) / 100.0, 0.0)


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


def _as_optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
