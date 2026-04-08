"""Execution-calibrated protective liquidation policy helpers."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path
from typing import Any

from autobot.execution.order_supervisor import (
    PRICE_MODE_CROSS_1T,
    PRICE_MODE_JOIN,
    PRICE_MODE_PASSIVE_MAKER,
    build_limit_price_from_mode,
)
from autobot.strategy.micro_snapshot import MicroSnapshot


PROTECTIVE_LIQUIDATION_POLICY_VERSION = 1

TIER_SOFT_EXIT = "soft_exit"
TIER_NORMAL_PROTECTIVE = "normal_protective"
TIER_URGENT_DEFENSIVE = "urgent_defensive"
TIER_EMERGENCY_FLATTEN = "emergency_flatten"

QUEUE_STATE_UNKNOWN = "unknown"
QUEUE_STATE_STABLE = "stable"
QUEUE_STATE_THIN_TOP = "thin_top"
QUEUE_STATE_ADVERSE_FLOW = "adverse_flow"


@dataclass(frozen=True)
class ProtectiveLiquidationPolicy:
    version: int
    tier_name: str
    urgency_score: float
    trigger_reason: str
    ord_type: str
    time_in_force: str
    price_mode: str
    target_price: float | None
    target_volume: float | None
    exit_aggress_bps: float
    timeout_ms: int
    replace_interval_ms: int
    max_replaces: int
    replace_allowed: bool
    queue_state: str
    micro_state: str
    reason_codes: tuple[str, ...]
    inputs: dict[str, Any]


def resolve_protective_liquidation_policy(
    *,
    trigger_reason: str,
    entry_price: float,
    qty: float,
    last_price: float,
    tick_size: float | None,
    base_exit_aggress_bps: float,
    base_timeout_sec: int,
    base_replace_max: int,
    ts_ms: int,
    created_ts_ms: int,
    trigger_ts_ms: int | None,
    breaker_action: str | None,
    micro_snapshot: MicroSnapshot | None,
    active_order_present: bool,
    stop_breach_ratio: float | None,
    expected_liquidation_cost_ratio: float | None = None,
    continue_value_lcb: float | None = None,
    exit_now_value_net: float | None = None,
) -> ProtectiveLiquidationPolicy:
    trigger = str(trigger_reason).strip().upper()
    breaker_action_value = str(breaker_action or "").strip().upper()
    base_timeout_ms = max(int(base_timeout_sec), 1) * 1000
    base_replace_value = max(int(base_replace_max), 0)
    tick_value = float(tick_size) if tick_size is not None and float(tick_size) > 0.0 else 1.0
    elapsed_trigger_ms = max(int(ts_ms) - int(trigger_ts_ms if trigger_ts_ms is not None else created_ts_ms), 0)
    stop_breach_value = max(float(stop_breach_ratio or 0.0), 0.0)
    spread_bps = _safe_optional_float(getattr(micro_snapshot, "spread_bps_mean", None))
    depth_top5 = _safe_optional_float(getattr(micro_snapshot, "depth_top5_notional_krw", None))
    trade_imbalance = _safe_optional_float(getattr(micro_snapshot, "trade_imbalance", None))
    best_bid_price = _safe_optional_float(getattr(micro_snapshot, "best_bid_price", None))
    best_ask_price = _safe_optional_float(getattr(micro_snapshot, "best_ask_price", None))
    expected_liquidation_cost_value = max(float(expected_liquidation_cost_ratio or 0.0), 0.0)
    continue_value_lcb_value = _safe_optional_float(continue_value_lcb)
    exit_now_value_net_value = _safe_optional_float(exit_now_value_net)
    queue_state = _resolve_queue_state(
        depth_top5_notional_krw=depth_top5,
        trade_imbalance=trade_imbalance,
        micro_snapshot=micro_snapshot,
    )

    reason_codes: list[str] = []
    tier_name = TIER_NORMAL_PROTECTIVE
    price_mode = PRICE_MODE_JOIN
    ord_type = "limit"
    time_in_force = "gtc"
    exit_aggress_bps = max(float(base_exit_aggress_bps), 1.0)
    timeout_ms = base_timeout_ms
    replace_interval_ms = base_timeout_ms
    max_replaces = base_replace_value
    replace_allowed = True

    if trigger in {"PATH_RISK_CONTINUATION", "TP"}:
        tier_name = TIER_SOFT_EXIT
        price_mode = PRICE_MODE_JOIN
        time_in_force = "gtc"
        exit_aggress_bps = max(float(base_exit_aggress_bps) * 0.5, 1.0)
        timeout_ms = max(base_timeout_ms, 60_000)
        replace_interval_ms = timeout_ms
        max_replaces = max(base_replace_value, 1)
        reason_codes.append("PROTECTIVE_SOFT_EXIT")
        if expected_liquidation_cost_value >= 0.003:
            reason_codes.append("PROTECTIVE_HIGH_LIQUIDATION_COST")

    spread_wide = spread_bps is not None and float(spread_bps) >= 25.0
    depth_thin = depth_top5 is not None and float(depth_top5) < 800_000.0
    adverse_flow = trade_imbalance is not None and float(trade_imbalance) <= -0.50
    liquidation_cost_heavy = expected_liquidation_cost_value >= 0.005
    continuation_edge_thin = (
        continue_value_lcb_value is not None
        and exit_now_value_net_value is not None
        and float(exit_now_value_net_value) >= float(continue_value_lcb_value)
    )

    if trigger in {"SL", "TRAILING", "TIMEOUT"} or spread_wide or depth_thin or adverse_flow or liquidation_cost_heavy:
        tier_name = TIER_NORMAL_PROTECTIVE
        price_mode = PRICE_MODE_JOIN
        time_in_force = "gtc"
        exit_aggress_bps = max(float(base_exit_aggress_bps), 2.0)
        timeout_ms = max(min(base_timeout_ms, 30_000), 8_000)
        replace_interval_ms = max(min(timeout_ms, 15_000), 3_000)
        max_replaces = min(max(base_replace_value, 1), 2)
        reason_codes.append("PROTECTIVE_NORMAL_EXIT")
        if liquidation_cost_heavy:
            reason_codes.append("PROTECTIVE_HIGH_LIQUIDATION_COST")

    if (
        stop_breach_value >= 0.005
        or trigger in {"SL", "TRAILING"}
        or depth_thin
        or adverse_flow
        or (liquidation_cost_heavy and continuation_edge_thin)
        or (elapsed_trigger_ms >= max(base_timeout_ms, 1) and (depth_thin or spread_wide))
    ):
        tier_name = TIER_URGENT_DEFENSIVE
        price_mode = PRICE_MODE_CROSS_1T
        ord_type = "limit"
        time_in_force = "ioc"
        exit_aggress_bps = max(float(base_exit_aggress_bps) * 2.0, 6.0)
        timeout_ms = max(min(base_timeout_ms // 2 if base_timeout_ms > 1 else 5_000, 10_000), 5_000)
        replace_interval_ms = max(min(timeout_ms, 5_000), 1_500)
        max_replaces = min(max(base_replace_value, 0), 1)
        reason_codes.append("PROTECTIVE_URGENT_DEFENSIVE")

    if (
        breaker_action_value in {"HALT_AND_CANCEL_BOT_ORDERS", "FULL_KILL_SWITCH"}
        or stop_breach_value >= 0.010
        or (depth_thin and spread_wide and elapsed_trigger_ms >= max(base_timeout_ms // 2, 1))
    ):
        tier_name = TIER_EMERGENCY_FLATTEN
        price_mode = PRICE_MODE_CROSS_1T
        ord_type = "best" if not active_order_present else "limit"
        time_in_force = "ioc"
        exit_aggress_bps = max(float(base_exit_aggress_bps) * 3.0, 10.0)
        timeout_ms = 5_000
        replace_interval_ms = 5_000
        max_replaces = 0
        replace_allowed = False
        reason_codes.append("PROTECTIVE_EMERGENCY_FLATTEN")
        if active_order_present:
            reason_codes.append("PROTECTIVE_ACTIVE_ORDER_LIMIT_FALLBACK")

    micro_state = "micro_calibrated" if micro_snapshot is not None else "micro_missing"
    target_price = _resolve_target_price(
        ord_type=ord_type,
        price_mode=price_mode,
        last_price=last_price,
        tick_size=tick_value,
        best_bid_price=best_bid_price,
        best_ask_price=best_ask_price,
    )
    target_volume = float(qty) if float(qty) > 0.0 else None

    urgency_score = _resolve_urgency_score(
        tier_name=tier_name,
        stop_breach_ratio=stop_breach_value,
        elapsed_trigger_ms=elapsed_trigger_ms,
        base_timeout_ms=base_timeout_ms,
        spread_bps=spread_bps,
        depth_top5_notional_krw=depth_top5,
    )
    return ProtectiveLiquidationPolicy(
        version=PROTECTIVE_LIQUIDATION_POLICY_VERSION,
        tier_name=tier_name,
        urgency_score=float(urgency_score),
        trigger_reason=trigger,
        ord_type=ord_type,
        time_in_force=time_in_force,
        price_mode=price_mode,
        target_price=target_price,
        target_volume=target_volume,
        exit_aggress_bps=float(exit_aggress_bps),
        timeout_ms=int(timeout_ms),
        replace_interval_ms=int(replace_interval_ms),
        max_replaces=int(max_replaces),
        replace_allowed=bool(replace_allowed),
        queue_state=queue_state,
        micro_state=micro_state,
        reason_codes=tuple(dict.fromkeys(reason_codes)),
        inputs={
            "entry_price": float(entry_price),
            "last_price": float(last_price),
            "stop_breach_ratio": float(stop_breach_value),
            "elapsed_trigger_ms": int(elapsed_trigger_ms),
            "spread_bps_mean": spread_bps,
            "depth_top5_notional_krw": depth_top5,
            "trade_imbalance": trade_imbalance,
            "best_bid_price": best_bid_price,
            "best_ask_price": best_ask_price,
            "expected_liquidation_cost_ratio": float(expected_liquidation_cost_value),
            "continue_value_lcb": continue_value_lcb_value,
            "exit_now_value_net": exit_now_value_net_value,
            "breaker_action": breaker_action_value or None,
            "active_order_present": bool(active_order_present),
        },
    )


def protective_liquidation_policy_to_dict(policy: ProtectiveLiquidationPolicy) -> dict[str, Any]:
    return asdict(policy)


def write_protective_liquidation_report(
    *,
    state_db_path: Path,
    payload: dict[str, Any],
) -> Path:
    path = Path(state_db_path).resolve().parent / "protective_liquidation_report.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _resolve_target_price(
    *,
    ord_type: str,
    price_mode: str,
    last_price: float,
    tick_size: float,
    best_bid_price: float | None,
    best_ask_price: float | None,
) -> float | None:
    if str(ord_type).strip().lower() == "best":
        return None
    mode = str(price_mode).strip().upper()
    if mode == PRICE_MODE_CROSS_1T and best_bid_price is not None:
        ref_price = float(best_bid_price)
    elif best_ask_price is not None:
        ref_price = float(best_ask_price)
    else:
        ref_price = float(last_price)
    return build_limit_price_from_mode(
        side="ask",
        ref_price=max(ref_price, float(tick_size)),
        tick_size=max(float(tick_size), 1e-12),
        price_mode=mode,
    )


def _resolve_queue_state(
    *,
    depth_top5_notional_krw: float | None,
    trade_imbalance: float | None,
    micro_snapshot: MicroSnapshot | None,
) -> str:
    if micro_snapshot is None:
        return QUEUE_STATE_UNKNOWN
    if depth_top5_notional_krw is None or float(depth_top5_notional_krw) < 800_000.0:
        return QUEUE_STATE_THIN_TOP
    if trade_imbalance is not None and float(trade_imbalance) <= -0.50:
        return QUEUE_STATE_ADVERSE_FLOW
    return QUEUE_STATE_STABLE


def _resolve_urgency_score(
    *,
    tier_name: str,
    stop_breach_ratio: float,
    elapsed_trigger_ms: int,
    base_timeout_ms: int,
    spread_bps: float | None,
    depth_top5_notional_krw: float | None,
) -> float:
    tier_base = {
        TIER_SOFT_EXIT: 0.25,
        TIER_NORMAL_PROTECTIVE: 0.50,
        TIER_URGENT_DEFENSIVE: 0.75,
        TIER_EMERGENCY_FLATTEN: 1.0,
    }.get(str(tier_name).strip(), 0.50)
    breach_component = min(max(float(stop_breach_ratio) / 0.01, 0.0), 1.0)
    elapsed_component = min(max(float(elapsed_trigger_ms) / float(max(base_timeout_ms, 1)), 0.0), 1.0)
    spread_component = min(max(float(spread_bps or 0.0) / 50.0, 0.0), 1.0)
    depth_component = 0.0
    if depth_top5_notional_krw is not None and float(depth_top5_notional_krw) > 0.0:
        depth_component = min(max((800_000.0 - float(depth_top5_notional_krw)) / 800_000.0, 0.0), 1.0)
    return min(max((tier_base * 0.50) + (breach_component * 0.20) + (elapsed_component * 0.15) + (spread_component * 0.10) + (depth_component * 0.05), 0.0), 1.0)


def _safe_optional_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
