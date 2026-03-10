from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
import json
import time
from typing import Any

from autobot.execution.intent import new_order_intent
from autobot.live.admissibility import round_price_to_tick
from autobot.live.breakers import (
    arm_breaker,
    classify_executor_reject_reason,
    new_intents_allowed,
    record_counter_failure,
    reset_counter,
)
from autobot.live.state_store import LiveStateStore, RiskPlanRecord

from .models import RiskManagerConfig, RiskPlan


class LiveRiskManager:
    def __init__(
        self,
        *,
        store: LiveStateStore,
        executor_gateway: Any | None,
        config: RiskManagerConfig | None = None,
        identifier_prefix: str = "AUTOBOT",
        tick_size_resolver: Callable[[str], float | None] | None = None,
    ) -> None:
        self._store = store
        self._executor_gateway = executor_gateway
        self._config = config or RiskManagerConfig()
        self._identifier_prefix = str(identifier_prefix).strip().upper() or "AUTOBOT"
        self._tick_size_resolver = tick_size_resolver

    def attach_default_risk(
        self,
        *,
        market: str,
        entry_price: float,
        qty: float,
        ts_ms: int | None = None,
        plan_id: str | None = None,
    ) -> RiskPlan:
        now_ts = int(ts_ms if ts_ms is not None else time.time() * 1000)
        market_value = str(market).strip().upper()
        if not market_value:
            raise ValueError("market is required")
        if entry_price <= 0:
            raise ValueError("entry_price must be positive")
        if qty <= 0:
            raise ValueError("qty must be positive")

        plan_key = _as_optional_str(plan_id) or f"default-risk-{market_value}"
        plan = RiskPlan(
            plan_id=plan_key,
            market=market_value,
            side="long",
            entry_price=float(entry_price),
            qty=float(qty),
            tp_enabled=self._config.default_tp_pct > 0,
            tp_pct=self._config.default_tp_pct if self._config.default_tp_pct > 0 else None,
            sl_enabled=self._config.default_sl_pct > 0,
            sl_pct=self._config.default_sl_pct if self._config.default_sl_pct > 0 else None,
            trailing_enabled=bool(self._config.default_trailing_enabled),
            trail_pct=self._config.default_trail_pct if self._config.default_trailing_enabled else None,
            state="ACTIVE",
            created_ts=now_ts,
            updated_ts=now_ts,
        )
        self._upsert_plan(plan)
        return plan

    def attach_model_risk(
        self,
        *,
        market: str,
        entry_price: float,
        qty: float,
        tp_pct: float | None,
        sl_pct: float | None,
        trailing_pct: float | None,
        timeout_ts_ms: int | None,
        ts_ms: int | None = None,
        plan_id: str | None = None,
        plan_source: str | None = "model_alpha_v1",
        source_intent_id: str | None = None,
    ) -> RiskPlan:
        now_ts = int(ts_ms if ts_ms is not None else time.time() * 1000)
        market_value = str(market).strip().upper()
        if not market_value:
            raise ValueError("market is required")
        if entry_price <= 0:
            raise ValueError("entry_price must be positive")
        if qty <= 0:
            raise ValueError("qty must be positive")

        plan_key = _as_optional_str(plan_id) or f"model-risk-{market_value}"
        tp_value = max(float(tp_pct), 0.0) if tp_pct is not None else None
        sl_value = max(float(sl_pct), 0.0) if sl_pct is not None else None
        trailing_value = max(float(trailing_pct), 0.0) if trailing_pct is not None else None
        plan = RiskPlan(
            plan_id=plan_key,
            market=market_value,
            side="long",
            entry_price=float(entry_price),
            qty=float(qty),
            tp_enabled=(tp_value or 0.0) > 0.0,
            tp_pct=tp_value if (tp_value or 0.0) > 0.0 else None,
            sl_enabled=(sl_value or 0.0) > 0.0,
            sl_pct=sl_value if (sl_value or 0.0) > 0.0 else None,
            trailing_enabled=(trailing_value or 0.0) > 0.0,
            trail_pct=trailing_value if (trailing_value or 0.0) > 0.0 else None,
            timeout_ts_ms=_as_int(timeout_ts_ms),
            state="ACTIVE",
            created_ts=now_ts,
            updated_ts=now_ts,
            plan_source=_as_optional_str(plan_source),
            source_intent_id=_as_optional_str(source_intent_id),
        )
        self._upsert_plan(plan)
        return plan

    def evaluate_price(
        self,
        *,
        market: str,
        last_price: float,
        ts_ms: int | None = None,
    ) -> list[dict[str, Any]]:
        now_ts = int(ts_ms if ts_ms is not None else time.time() * 1000)
        market_value = str(market).strip().upper()
        if not market_value or last_price <= 0:
            return []

        actions: list[dict[str, Any]] = []
        for plan in self._load_plans(market=market_value, states=("ACTIVE", "TRIGGERED", "EXITING")):
            updated = replace(plan, last_eval_ts_ms=now_ts, updated_ts=now_ts)
            if updated.state in {"ACTIVE", "TRIGGERED"}:
                updated, trailing_action = self._update_trailing(updated, last_price=last_price, ts_ms=now_ts)
                if trailing_action is not None:
                    actions.append(trailing_action)

                trigger = self._detect_trigger(updated, last_price=last_price, ts_ms=now_ts)
                if trigger is not None:
                    updated, action = self._submit_exit_order(
                        updated,
                        trigger_reason=trigger,
                        last_price=last_price,
                        ts_ms=now_ts,
                    )
                    actions.append(action)
            elif updated.state == "EXITING":
                timeout_ms = max(int(self._config.order_timeout_sec), 1) * 1000
                if (
                    updated.last_action_ts_ms > 0
                    and now_ts - updated.last_action_ts_ms >= timeout_ms
                    and updated.replace_attempt < max(int(self._config.replace_max), 0)
                ):
                    updated, action = self._replace_exit_order(updated, last_price=last_price, ts_ms=now_ts)
                    actions.append(action)

            self._upsert_plan(updated)
        return actions

    def handle_executor_event(self, event: dict[str, Any]) -> dict[str, Any] | None:
        event_type = str(event.get("event_type", "")).strip().upper()
        payload = event.get("payload")
        if not isinstance(payload, dict):
            return None
        event_name = str(payload.get("event_name", "")).strip().upper()
        ts_ms = _as_int(event.get("ts_ms")) or int(time.time() * 1000)
        uuid = _as_optional_str(payload.get("uuid")) or _as_optional_str(payload.get("upbit_uuid"))
        identifier = _as_optional_str(payload.get("identifier"))

        if event_name == "ORDER_REPLACED":
            prev_uuid = _as_optional_str(payload.get("prev_uuid"))
            prev_identifier = _as_optional_str(payload.get("prev_identifier"))
            new_uuid = _as_optional_str(payload.get("new_uuid"))
            new_identifier = _as_optional_str(payload.get("new_identifier"))
            for plan in self._load_plans(states=("EXITING",)):
                if not _plan_matches(plan, uuid=prev_uuid, identifier=prev_identifier):
                    continue
                updated = replace(
                    plan,
                    current_exit_order_uuid=new_uuid or plan.current_exit_order_uuid,
                    current_exit_order_identifier=new_identifier or plan.current_exit_order_identifier,
                    replace_attempt=plan.replace_attempt + 1,
                    last_action_ts_ms=ts_ms,
                    updated_ts=ts_ms,
                )
                self._upsert_plan(updated)
                return {"type": "risk_replace_ack", "plan_id": plan.plan_id}
            return None

        if event_type not in {"ORDER_UPDATE", "FILL"} and event_name not in {"ORDER_STATE", "CANCEL_RESULT"}:
            return None

        state = str(payload.get("state", "")).strip().lower()
        for plan in self._load_plans(states=("EXITING", "TRIGGERED")):
            if not _plan_matches(plan, uuid=uuid, identifier=identifier):
                continue
            if state == "done":
                updated = replace(
                    plan,
                    state="CLOSED",
                    last_action_ts_ms=ts_ms,
                    updated_ts=ts_ms,
                )
                self._upsert_plan(updated)
                return {"type": "risk_closed", "plan_id": plan.plan_id}
            if state in {"cancel", "cancelled", "cancel_reject"}:
                updated = replace(plan, state="EXITING", updated_ts=ts_ms)
                self._upsert_plan(updated)
                return {"type": "risk_exit_still_open", "plan_id": plan.plan_id, "state": state}
        return None

    def _submit_exit_order(
        self,
        plan: RiskPlan,
        *,
        trigger_reason: str,
        last_price: float,
        ts_ms: int,
    ) -> tuple[RiskPlan, dict[str, Any]]:
        if not new_intents_allowed(self._store):
            updated = replace(plan, state="TRIGGERED", updated_ts=ts_ms)
            return updated, {"type": "risk_blocked_by_breaker", "plan_id": plan.plan_id, "reason": trigger_reason}
        exit_price = self._resolve_exit_price(market=plan.market, last_price=last_price, step=1)
        volume = _format_decimal(plan.qty, self._config.volume_digits)
        identifier = f"{self._identifier_prefix}-RISK-{plan.plan_id[:10]}-{ts_ms}"
        if self._executor_gateway is None or not hasattr(self._executor_gateway, "submit_intent"):
            updated = replace(plan, state="TRIGGERED", updated_ts=ts_ms)
            return updated, {"type": "risk_triggered_no_executor", "plan_id": plan.plan_id, "reason": trigger_reason}

        meta = {
            "risk": {
                "plan_id": plan.plan_id,
                "trigger_reason": trigger_reason,
                "entry_price": plan.entry_price,
                "last_price": float(last_price),
                "exit_price": float(exit_price),
            }
        }
        intent = new_order_intent(
            market=plan.market,
            side="ask",
            price=exit_price,
            volume=plan.qty,
            reason_code=f"RISK_{trigger_reason}",
            ord_type="limit",
            time_in_force="gtc",
            meta=meta,
            ts_ms=ts_ms,
        )
        result = self._executor_gateway.submit_intent(
            intent=intent,
            identifier=identifier,
            meta_json=json.dumps(meta, ensure_ascii=False, sort_keys=True),
        )
        if bool(getattr(result, "accepted", False)):
            reset_counter(self._store, counter_name="rate_limit_error", source="risk_submit_ok", ts_ms=ts_ms)
            reset_counter(self._store, counter_name="auth_error", source="risk_submit_ok", ts_ms=ts_ms)
            reset_counter(self._store, counter_name="nonce_error", source="risk_submit_ok", ts_ms=ts_ms)
            updated = replace(
                plan,
                state="EXITING",
                last_action_ts_ms=ts_ms,
                current_exit_order_uuid=_as_optional_str(getattr(result, "upbit_uuid", None)),
                current_exit_order_identifier=_as_optional_str(getattr(result, "identifier", None)) or identifier,
                updated_ts=ts_ms,
            )
            return updated, {
                "type": "risk_exit_submitted",
                "plan_id": plan.plan_id,
                "trigger_reason": trigger_reason,
                "identifier": updated.current_exit_order_identifier,
                "price_str": _format_decimal(exit_price, self._config.price_digits),
                "volume_str": volume,
            }

        reject_reason = str(getattr(result, "reason", "") or "")
        classified_reject = classify_executor_reject_reason(reject_reason)
        if classified_reject == "REPEATED_RATE_LIMIT_ERRORS":
            record_counter_failure(
                self._store,
                counter_name="rate_limit_error",
                limit=3,
                source="risk_submit",
                ts_ms=ts_ms,
                details={"plan_id": plan.plan_id, "reason": reject_reason},
            )
        elif classified_reject == "REPEATED_AUTH_ERRORS":
            record_counter_failure(
                self._store,
                counter_name="auth_error",
                limit=2,
                source="risk_submit",
                ts_ms=ts_ms,
                details={"plan_id": plan.plan_id, "reason": reject_reason},
            )
        elif classified_reject == "REPEATED_NONCE_ERRORS":
            record_counter_failure(
                self._store,
                counter_name="nonce_error",
                limit=2,
                source="risk_submit",
                ts_ms=ts_ms,
                details={"plan_id": plan.plan_id, "reason": reject_reason},
            )
        elif classified_reject == "IDENTIFIER_COLLISION":
            arm_breaker(
                self._store,
                reason_codes=["IDENTIFIER_COLLISION"],
                source="risk_submit",
                ts_ms=ts_ms,
                details={"plan_id": plan.plan_id, "reason": reject_reason},
            )
        updated = replace(plan, state="TRIGGERED", updated_ts=ts_ms)
        return updated, {
            "type": "risk_exit_rejected",
            "plan_id": plan.plan_id,
            "trigger_reason": trigger_reason,
            "reason": str(getattr(result, "reason", "")),
        }

    def _replace_exit_order(
        self,
        plan: RiskPlan,
        *,
        last_price: float,
        ts_ms: int,
    ) -> tuple[RiskPlan, dict[str, Any]]:
        if not new_intents_allowed(self._store):
            updated = replace(plan, updated_ts=ts_ms)
            return updated, {"type": "risk_replace_blocked_by_breaker", "plan_id": plan.plan_id}
        if self._executor_gateway is None or not hasattr(self._executor_gateway, "replace_order"):
            updated = replace(plan, state="TRIGGERED", updated_ts=ts_ms)
            return updated, {"type": "risk_replace_no_executor", "plan_id": plan.plan_id}

        replace_step = plan.replace_attempt + 1
        new_price = self._resolve_exit_price(
            market=plan.market,
            last_price=last_price,
            step=max(replace_step, 1),
        )
        new_identifier = f"{self._identifier_prefix}-RISKREP-{plan.plan_id[:8]}-{replace_step}-{ts_ms}"
        result = self._executor_gateway.replace_order(
            intent_id=f"risk-replace-{plan.plan_id}-{replace_step}",
            prev_order_uuid=plan.current_exit_order_uuid,
            prev_order_identifier=plan.current_exit_order_identifier,
            new_identifier=new_identifier,
            new_price_str=_format_decimal(new_price, self._config.price_digits),
            new_volume_str="remain_only",
            new_time_in_force="gtc",
        )
        if bool(getattr(result, "accepted", False)):
            reset_counter(self._store, counter_name="replace_reject", source="risk_replace_ok", ts_ms=ts_ms)
            reset_counter(self._store, counter_name="rate_limit_error", source="risk_replace_ok", ts_ms=ts_ms)
            reset_counter(self._store, counter_name="auth_error", source="risk_replace_ok", ts_ms=ts_ms)
            reset_counter(self._store, counter_name="nonce_error", source="risk_replace_ok", ts_ms=ts_ms)
            updated = replace(
                plan,
                state="EXITING",
                current_exit_order_uuid=_as_optional_str(getattr(result, "new_order_uuid", None))
                or plan.current_exit_order_uuid,
                current_exit_order_identifier=_as_optional_str(getattr(result, "new_identifier", None))
                or new_identifier,
                replace_attempt=replace_step,
                last_action_ts_ms=ts_ms,
                updated_ts=ts_ms,
            )
            return updated, {
                "type": "risk_exit_replaced",
                "plan_id": plan.plan_id,
                "replace_attempt": replace_step,
                "identifier": updated.current_exit_order_identifier,
            }

        reject_reason = str(getattr(result, "reason", "") or "")
        record_counter_failure(
            self._store,
            counter_name="replace_reject",
            limit=max(int(self._config.replace_max), 1),
            source="risk_replace",
            ts_ms=ts_ms,
            details={"plan_id": plan.plan_id, "reason": reject_reason},
        )
        classified_reject = classify_executor_reject_reason(reject_reason)
        if classified_reject == "REPEATED_RATE_LIMIT_ERRORS":
            record_counter_failure(
                self._store,
                counter_name="rate_limit_error",
                limit=3,
                source="risk_replace",
                ts_ms=ts_ms,
                details={"plan_id": plan.plan_id, "reason": reject_reason},
            )
        elif classified_reject == "REPEATED_AUTH_ERRORS":
            record_counter_failure(
                self._store,
                counter_name="auth_error",
                limit=2,
                source="risk_replace",
                ts_ms=ts_ms,
                details={"plan_id": plan.plan_id, "reason": reject_reason},
            )
        elif classified_reject == "REPEATED_NONCE_ERRORS":
            record_counter_failure(
                self._store,
                counter_name="nonce_error",
                limit=2,
                source="risk_replace",
                ts_ms=ts_ms,
                details={"plan_id": plan.plan_id, "reason": reject_reason},
            )
        if replace_step >= max(int(self._config.replace_max), 0):
            updated = replace(plan, state="TRIGGERED", updated_ts=ts_ms)
            return updated, {
                "type": "risk_replace_max_reached",
                "plan_id": plan.plan_id,
                "replace_attempt": replace_step,
            }

        updated = replace(plan, updated_ts=ts_ms)
        return updated, {
            "type": "risk_replace_failed",
            "plan_id": plan.plan_id,
            "replace_attempt": replace_step,
            "reason": str(getattr(result, "reason", "")),
        }

    def _resolve_exit_price(self, *, market: str, last_price: float, step: int) -> float:
        raw = _aggressive_exit_price(
            last_price=last_price,
            base_bps=self._config.exit_aggress_bps,
            step=step,
            digits=self._config.price_digits,
        )
        tick_size = self._resolve_tick_size(market)
        if tick_size is None or tick_size <= 0:
            return raw
        return round_price_to_tick(
            price=raw,
            tick_size=float(tick_size),
            side="ask",
        )

    def _resolve_tick_size(self, market: str) -> float | None:
        if self._tick_size_resolver is None:
            return None
        try:
            value = self._tick_size_resolver(str(market).strip().upper())
        except Exception:
            return None
        try:
            tick_size = float(value) if value is not None else 0.0
        except (TypeError, ValueError):
            return None
        return tick_size if tick_size > 0 else None

    def _update_trailing(
        self,
        plan: RiskPlan,
        *,
        last_price: float,
        ts_ms: int,
    ) -> tuple[RiskPlan, dict[str, Any] | None]:
        if not plan.trailing_enabled:
            return plan, None
        watermark = plan.high_watermark_price or 0.0
        if last_price <= watermark:
            return plan, None
        updated = replace(
            plan,
            high_watermark_price=last_price,
            armed_ts_ms=plan.armed_ts_ms or ts_ms,
            updated_ts=ts_ms,
        )
        return updated, {"type": "risk_trailing_watermark", "plan_id": plan.plan_id, "watermark": last_price}

    def _detect_trigger(self, plan: RiskPlan, *, last_price: float, ts_ms: int) -> str | None:
        tp_price = plan.resolve_tp_price()
        if tp_price is not None and last_price >= tp_price:
            return "TP"

        sl_price = plan.resolve_sl_price()
        if sl_price is not None and last_price <= sl_price:
            return "SL"

        if plan.trailing_enabled and plan.trail_pct is not None and plan.trail_pct > 0:
            watermark = plan.high_watermark_price or 0.0
            if watermark > 0:
                floor = watermark * (1.0 - plan.trail_pct)
                if last_price <= floor:
                    return "TRAILING"
        if plan.timeout_ts_ms is not None and int(ts_ms) >= int(plan.timeout_ts_ms):
            return "TIMEOUT"
        return None

    def _load_plans(
        self,
        *,
        market: str | None = None,
        states: tuple[str, ...] | None = None,
    ) -> list[RiskPlan]:
        rows = self._store.list_risk_plans(states=states, market=market)
        return [_risk_plan_from_row(item) for item in rows]

    def _upsert_plan(self, plan: RiskPlan) -> None:
        record = RiskPlanRecord(
            plan_id=plan.plan_id,
            market=plan.market,
            side=plan.side,
            entry_price_str=_format_decimal(plan.entry_price, self._config.price_digits),
            qty_str=_format_decimal(plan.qty, self._config.volume_digits),
            tp_enabled=plan.tp_enabled,
            tp_price_str=_optional_decimal(plan.tp_price, self._config.price_digits),
            tp_pct=plan.tp_pct,
            sl_enabled=plan.sl_enabled,
            sl_price_str=_optional_decimal(plan.sl_price, self._config.price_digits),
            sl_pct=plan.sl_pct,
            trailing_enabled=plan.trailing_enabled,
            trail_pct=plan.trail_pct,
            high_watermark_price_str=_optional_decimal(plan.high_watermark_price, self._config.price_digits),
            armed_ts_ms=plan.armed_ts_ms,
            timeout_ts_ms=plan.timeout_ts_ms,
            state=plan.state,
            last_eval_ts_ms=int(plan.last_eval_ts_ms),
            last_action_ts_ms=int(plan.last_action_ts_ms),
            current_exit_order_uuid=plan.current_exit_order_uuid,
            current_exit_order_identifier=plan.current_exit_order_identifier,
            replace_attempt=int(plan.replace_attempt),
            created_ts=int(plan.created_ts),
            updated_ts=int(plan.updated_ts),
            plan_source=plan.plan_source,
            source_intent_id=plan.source_intent_id,
        )
        self._store.upsert_risk_plan(record)


def _risk_plan_from_row(row: dict[str, Any]) -> RiskPlan:
    tp = row.get("tp") if isinstance(row.get("tp"), dict) else {}
    sl = row.get("sl") if isinstance(row.get("sl"), dict) else {}
    trailing = row.get("trailing") if isinstance(row.get("trailing"), dict) else {}
    return RiskPlan(
        plan_id=str(row.get("plan_id", "")).strip(),
        market=str(row.get("market", "")).strip().upper(),
        side=str(row.get("side", "long")).strip().lower(),
        entry_price=float(row.get("entry_price_str") or 0.0),
        qty=float(row.get("qty_str") or 0.0),
        tp_enabled=bool(tp.get("enabled")),
        tp_price=_as_float(tp.get("tp_price_str")),
        tp_pct=_as_float(tp.get("tp_pct")),
        sl_enabled=bool(sl.get("enabled")),
        sl_price=_as_float(sl.get("sl_price_str")),
        sl_pct=_as_float(sl.get("sl_pct")),
        trailing_enabled=bool(trailing.get("enabled")),
        trail_pct=_as_float(trailing.get("trail_pct")),
        high_watermark_price=_as_float(trailing.get("high_watermark_price_str")),
        armed_ts_ms=_as_int(trailing.get("armed_ts_ms")),
        timeout_ts_ms=_as_int(row.get("timeout_ts_ms")),
        state=str(row.get("state", "ACTIVE")).strip().upper(),
        last_eval_ts_ms=int(row.get("last_eval_ts_ms") or 0),
        last_action_ts_ms=int(row.get("last_action_ts_ms") or 0),
        current_exit_order_uuid=_as_optional_str(row.get("current_exit_order_uuid")),
        current_exit_order_identifier=_as_optional_str(row.get("current_exit_order_identifier")),
        replace_attempt=int(row.get("replace_attempt") or 0),
        created_ts=int(row.get("created_ts") or 0),
        updated_ts=int(row.get("updated_ts") or 0),
        plan_source=_as_optional_str(row.get("plan_source")),
        source_intent_id=_as_optional_str(row.get("source_intent_id")),
    )


def _plan_matches(plan: RiskPlan, *, uuid: str | None, identifier: str | None) -> bool:
    if uuid and plan.current_exit_order_uuid and uuid == plan.current_exit_order_uuid:
        return True
    if identifier and plan.current_exit_order_identifier and identifier == plan.current_exit_order_identifier:
        return True
    return False


def _aggressive_exit_price(*, last_price: float, base_bps: float, step: int, digits: int) -> float:
    bps = max(float(base_bps), 0.0) * max(int(step), 1)
    raw = max(float(last_price) * (1.0 - bps / 10000.0), 1e-8)
    return round(raw, max(int(digits), 0))


def _format_decimal(value: float, digits: int) -> str:
    precision = max(int(digits), 0)
    text = f"{float(value):.{precision}f}"
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _optional_decimal(value: float | None, digits: int) -> str | None:
    if value is None:
        return None
    return _format_decimal(float(value), digits)


def _as_optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _as_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
