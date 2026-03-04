"""Shared order supervision helpers for timeout/replace policy execution."""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, Sequence

EPSILON = 1e-12

PRICE_MODE_PASSIVE_MAKER = "PASSIVE_MAKER"
PRICE_MODE_JOIN = "JOIN"
PRICE_MODE_CROSS_1T = "CROSS_1T"

SUPERVISOR_ACTION_WAIT = "WAIT"
SUPERVISOR_ACTION_REPLACE = "REPLACE"
SUPERVISOR_ACTION_ABORT = "ABORT"

REASON_TIMEOUT_REPLACE = "TIMEOUT_REPLACE"
REASON_CHASE_LIMIT_EXCEEDED = "CHASE_LIMIT_EXCEEDED"
REASON_MAX_REPLACES_REACHED = "MAX_REPLACES_REACHED"
REASON_MIN_NOTIONAL_DUST_ABORT = "MIN_NOTIONAL_DUST_ABORT"


@dataclass(frozen=True)
class OrderExecProfile:
    timeout_ms: int
    replace_interval_ms: int
    max_replaces: int
    price_mode: str
    max_chase_bps: int
    min_replace_interval_ms_global: int
    post_only: bool = False


@dataclass(frozen=True)
class SupervisorAction:
    action: str
    reason_code: str | None = None
    target_price: float | None = None


def normalize_order_exec_profile(
    profile: OrderExecProfile,
    *,
    forbid_post_only_with_cross: bool = True,
) -> OrderExecProfile:
    mode = _normalize_price_mode(profile.price_mode)
    post_only = bool(profile.post_only)
    if forbid_post_only_with_cross and mode == PRICE_MODE_CROSS_1T:
        post_only = False
    return OrderExecProfile(
        timeout_ms=max(int(profile.timeout_ms), 1),
        replace_interval_ms=max(int(profile.replace_interval_ms), 1),
        max_replaces=max(int(profile.max_replaces), 0),
        price_mode=mode,
        max_chase_bps=max(int(profile.max_chase_bps), 0),
        min_replace_interval_ms_global=max(int(profile.min_replace_interval_ms_global), 1),
        post_only=post_only,
    )


def make_legacy_exec_profile(
    *,
    timeout_ms: int,
    replace_interval_ms: int,
    max_replaces: int,
    price_mode: str = PRICE_MODE_JOIN,
    max_chase_bps: int = 10_000,
    min_replace_interval_ms_global: int = 1_500,
) -> OrderExecProfile:
    return normalize_order_exec_profile(
        OrderExecProfile(
            timeout_ms=max(int(timeout_ms), 1),
            replace_interval_ms=max(int(replace_interval_ms), 1),
            max_replaces=max(int(max_replaces), 0),
            price_mode=price_mode,
            max_chase_bps=max(int(max_chase_bps), 0),
            min_replace_interval_ms_global=max(int(min_replace_interval_ms_global), 1),
            post_only=False,
        )
    )


def order_exec_profile_to_dict(profile: OrderExecProfile) -> dict[str, Any]:
    normalized = normalize_order_exec_profile(profile)
    return {
        "timeout_ms": int(normalized.timeout_ms),
        "replace_interval_ms": int(normalized.replace_interval_ms),
        "max_replaces": int(normalized.max_replaces),
        "price_mode": normalized.price_mode,
        "max_chase_bps": int(normalized.max_chase_bps),
        "min_replace_interval_ms_global": int(normalized.min_replace_interval_ms_global),
        "post_only": bool(normalized.post_only),
    }


def order_exec_profile_from_dict(value: Any, *, fallback: OrderExecProfile) -> OrderExecProfile:
    if not isinstance(value, dict):
        return normalize_order_exec_profile(fallback)
    return normalize_order_exec_profile(
        OrderExecProfile(
            timeout_ms=int(value.get("timeout_ms", fallback.timeout_ms)),
            replace_interval_ms=int(value.get("replace_interval_ms", fallback.replace_interval_ms)),
            max_replaces=int(value.get("max_replaces", fallback.max_replaces)),
            price_mode=str(value.get("price_mode", fallback.price_mode)),
            max_chase_bps=int(value.get("max_chase_bps", fallback.max_chase_bps)),
            min_replace_interval_ms_global=int(
                value.get("min_replace_interval_ms_global", fallback.min_replace_interval_ms_global)
            ),
            post_only=bool(value.get("post_only", fallback.post_only)),
        )
    )


def build_limit_price_from_mode(
    *,
    side: str,
    ref_price: float,
    tick_size: float,
    price_mode: str,
) -> float:
    side_value = str(side).strip().lower()
    if side_value not in {"bid", "ask"}:
        raise ValueError(f"unsupported side={side}")
    ref_value = max(float(ref_price), EPSILON)
    tick_value = max(float(tick_size), EPSILON)
    mode = _normalize_price_mode(price_mode)

    if mode == PRICE_MODE_PASSIVE_MAKER:
        if side_value == "bid":
            target = max(ref_value - tick_value, tick_value)
        else:
            target = ref_value + tick_value
    elif mode == PRICE_MODE_CROSS_1T:
        if side_value == "bid":
            target = ref_value + tick_value
        else:
            target = max(ref_value - tick_value, tick_value)
    else:
        target = ref_value

    return round_price_to_tick(price=target, tick_size=tick_value, side=side_value)


def evaluate_supervisor_action(
    *,
    profile: OrderExecProfile,
    side: str,
    now_ts_ms: int,
    created_ts_ms: int,
    last_action_ts_ms: int,
    last_replace_ts_ms: int,
    replace_count: int,
    remaining_volume: float,
    ref_price: float,
    tick_size: float,
    initial_ref_price: float,
    min_total: float,
    replaces_last_minute: int,
    max_replaces_per_min_per_market: int,
) -> SupervisorAction:
    normalized = normalize_order_exec_profile(profile)
    now_value = int(now_ts_ms)
    due_by_interval = (now_value - int(last_action_ts_ms)) >= int(normalized.replace_interval_ms)
    due_by_timeout = (now_value - int(created_ts_ms)) >= int(normalized.timeout_ms)
    if not due_by_interval and not due_by_timeout:
        return SupervisorAction(action=SUPERVISOR_ACTION_WAIT)

    if int(replace_count) >= int(normalized.max_replaces):
        return SupervisorAction(
            action=SUPERVISOR_ACTION_ABORT,
            reason_code=REASON_MAX_REPLACES_REACHED,
        )
    if (now_value - int(last_replace_ts_ms)) < int(normalized.min_replace_interval_ms_global):
        return SupervisorAction(action=SUPERVISOR_ACTION_WAIT)

    max_replaces_per_min = max(int(max_replaces_per_min_per_market), 1)
    if int(replaces_last_minute) >= max_replaces_per_min:
        return SupervisorAction(
            action=SUPERVISOR_ACTION_ABORT,
            reason_code=REASON_MAX_REPLACES_REACHED,
        )

    try:
        target_price = build_limit_price_from_mode(
            side=side,
            ref_price=ref_price,
            tick_size=tick_size,
            price_mode=normalized.price_mode,
        )
    except ValueError:
        # Side should be normalized by call sites, but keep supervisor fail-safe.
        target_price = build_limit_price_from_mode(
            side=side,
            ref_price=ref_price,
            tick_size=tick_size,
            price_mode=PRICE_MODE_JOIN,
        )

    if chase_limit_exceeded(
        side=side,
        target_price=target_price,
        initial_ref_price=initial_ref_price,
        max_chase_bps=normalized.max_chase_bps,
    ):
        return SupervisorAction(
            action=SUPERVISOR_ACTION_ABORT,
            reason_code=REASON_CHASE_LIMIT_EXCEEDED,
        )

    remain_notional = float(remaining_volume) * float(target_price)
    if remain_notional + EPSILON < max(float(min_total), 0.0):
        return SupervisorAction(
            action=SUPERVISOR_ACTION_ABORT,
            reason_code=REASON_MIN_NOTIONAL_DUST_ABORT,
        )

    return SupervisorAction(
        action=SUPERVISOR_ACTION_REPLACE,
        reason_code=REASON_TIMEOUT_REPLACE,
        target_price=float(target_price),
    )


def chase_limit_exceeded(*, side: str, target_price: float, initial_ref_price: float, max_chase_bps: int) -> bool:
    side_value = str(side).strip().lower()
    base = max(float(initial_ref_price), EPSILON)
    chase = max(int(max_chase_bps), 0) / 10_000.0
    if side_value == "bid":
        upper = base * (1.0 + chase)
        return float(target_price) > upper + EPSILON
    if side_value == "ask":
        lower = base * (1.0 - chase)
        return float(target_price) + EPSILON < lower
    return True


def round_price_to_tick(*, price: float, tick_size: float, side: str) -> float:
    if float(price) <= 0:
        raise ValueError("price must be positive")
    tick_value = max(float(tick_size), EPSILON)
    scaled = float(price) / tick_value
    side_value = str(side).strip().lower()
    if side_value == "bid":
        rounded_ticks = math.floor(scaled + EPSILON)
    elif side_value == "ask":
        rounded_ticks = math.ceil(scaled - EPSILON)
    else:
        raise ValueError("side must be bid or ask")
    rounded = max(rounded_ticks * tick_value, tick_value)
    digits = decimal_places(tick_value)
    return round(rounded, digits)


def decimal_places(value: float) -> int:
    text = f"{float(value):.16f}".rstrip("0")
    if "." not in text:
        return 0
    return len(text.split(".", 1)[1])


def slippage_bps(*, side: str, fill_price: float, ref_price: float) -> float | None:
    ref = float(ref_price)
    if ref <= 0:
        return None
    fill = float(fill_price)
    side_value = str(side).strip().lower()
    if side_value == "bid":
        return ((fill - ref) / ref) * 10_000.0
    if side_value == "ask":
        return ((ref - fill) / ref) * 10_000.0
    return None


def mean(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    return float(sum(float(item) for item in values)) / float(len(values))


def percentile(values: Sequence[float], q: float) -> float:
    if not values:
        return 0.0
    q_value = min(max(float(q), 0.0), 1.0)
    ordered = sorted(float(item) for item in values)
    if len(ordered) == 1:
        return ordered[0]
    pos = q_value * (len(ordered) - 1)
    low = int(math.floor(pos))
    high = int(math.ceil(pos))
    if low == high:
        return ordered[low]
    weight = pos - low
    return ordered[low] + (ordered[high] - ordered[low]) * weight


def _normalize_price_mode(value: str) -> str:
    normalized = str(value).strip().upper()
    if normalized not in {PRICE_MODE_PASSIVE_MAKER, PRICE_MODE_JOIN, PRICE_MODE_CROSS_1T}:
        return PRICE_MODE_JOIN
    return normalized
