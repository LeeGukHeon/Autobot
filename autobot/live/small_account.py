"""Small-account cost, sizing, and canary reporting helpers."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import math
from typing import Any

from .state_store import LiveStateStore

EPSILON = 1e-12

REJECTED_FOR_COST_CHECKPOINT = "small_account_rejected_for_cost"
REJECTED_FOR_MIN_TOTAL_CHECKPOINT = "small_account_rejected_for_min_total"
DUST_ABORT_CHECKPOINT = "small_account_dust_abort"

CANARY_MAX_POSITIONS_EXCEEDED = "SMALL_ACCOUNT_CANARY_MAX_POSITIONS_EXCEEDED"
CANARY_MULTIPLE_ACTIVE_MARKETS = "SMALL_ACCOUNT_CANARY_MULTIPLE_ACTIVE_MARKETS"
CANARY_MAX_OPEN_ORDERS_PER_MARKET_EXCEEDED = "SMALL_ACCOUNT_CANARY_MAX_OPEN_ORDERS_PER_MARKET_EXCEEDED"

COST_FORMULA = "expected_net_edge_bps = expected_gross_edge_bps - fee_cost_bps - tick_proxy_bps - replace_risk_budget_bps"


@dataclass(frozen=True)
class SmallAccountCostBreakdown:
    fee_cost_bps: float
    tick_proxy_bps: float
    replace_risk_budget_bps: float
    estimated_total_cost_bps: float
    expected_gross_edge_bps: float | None
    expected_net_edge_bps: float | None
    formula: str = COST_FORMULA


@dataclass(frozen=True)
class SmallAccountSizingEnvelope:
    target_notional_quote: float
    adjusted_price: float
    fee_rate: float
    admissible_notional_quote: float
    fee_reserve_quote: float
    admissible_volume: float
    formula: str


def compute_small_account_cost_breakdown(
    *,
    price: float,
    tick_size: float,
    fee_rate: float,
    expected_edge_bps: float | None = None,
    replace_risk_steps: int = 0,
) -> SmallAccountCostBreakdown:
    price_value = max(float(price), EPSILON)
    tick_proxy_bps = max(float(tick_size), 0.0) / price_value * 10_000.0
    fee_cost_bps = max(float(fee_rate), 0.0) * 10_000.0
    replace_risk_budget_bps = tick_proxy_bps * max(int(replace_risk_steps), 0)
    estimated_total_cost_bps = fee_cost_bps + tick_proxy_bps + replace_risk_budget_bps
    expected_gross_edge_bps = float(expected_edge_bps) if expected_edge_bps is not None else None
    expected_net_edge_bps = None
    if expected_gross_edge_bps is not None:
        expected_net_edge_bps = expected_gross_edge_bps - estimated_total_cost_bps
    return SmallAccountCostBreakdown(
        fee_cost_bps=fee_cost_bps,
        tick_proxy_bps=tick_proxy_bps,
        replace_risk_budget_bps=replace_risk_budget_bps,
        estimated_total_cost_bps=estimated_total_cost_bps,
        expected_gross_edge_bps=expected_gross_edge_bps,
        expected_net_edge_bps=expected_net_edge_bps,
    )


def derive_volume_from_target_notional(
    *,
    side: str,
    price: float,
    target_notional_quote: float,
    fee_rate: float,
) -> SmallAccountSizingEnvelope:
    price_value = max(float(price), EPSILON)
    target_quote = max(float(target_notional_quote), 0.0)
    fee_rate_value = max(float(fee_rate), 0.0)
    side_value = str(side).strip().lower()
    if side_value not in {"bid", "ask"}:
        raise ValueError("side must be bid or ask")
    if target_quote <= 0.0:
        raise ValueError("target_notional_quote must be positive")

    if side_value == "bid":
        admissible_notional_quote = target_quote / (1.0 + fee_rate_value)
        fee_reserve_quote = admissible_notional_quote * fee_rate_value
        formula = "admissible_notional_quote = target_notional_quote / (1 + fee_rate)"
    else:
        admissible_notional_quote = target_quote
        fee_reserve_quote = admissible_notional_quote * fee_rate_value
        formula = "admissible_notional_quote = target_notional_quote"
    admissible_volume = admissible_notional_quote / price_value
    return SmallAccountSizingEnvelope(
        target_notional_quote=target_quote,
        adjusted_price=price_value,
        fee_rate=fee_rate_value,
        admissible_notional_quote=admissible_notional_quote,
        fee_reserve_quote=fee_reserve_quote,
        admissible_volume=admissible_volume,
        formula=formula,
    )


def record_small_account_decision(
    *,
    store: LiveStateStore,
    decision: Any,
    source: str,
    ts_ms: int,
    market: str | None = None,
) -> dict[str, Any]:
    decision_payload = {
        "market": market,
        "source": str(source).strip().lower() or "unknown",
        "ts_ms": int(ts_ms),
        "admissible": bool(getattr(decision, "admissible", False)),
        "reject_code": getattr(decision, "reject_code", None),
        "expected_gross_edge_bps": getattr(decision, "expected_edge_bps", None),
        "expected_net_edge_bps": getattr(decision, "expected_net_edge_bps", None),
        "estimated_total_cost_bps": getattr(decision, "estimated_total_cost_bps", None),
    }
    store.set_checkpoint(name="last_small_account_decision", payload=decision_payload, ts_ms=ts_ms)
    reject_code = str(getattr(decision, "reject_code", "") or "").strip().upper()
    if not reject_code:
        return decision_payload
    checkpoint_name = _counter_checkpoint_for_reject(reject_code)
    if checkpoint_name is None:
        return decision_payload
    _increment_counter(store=store, checkpoint_name=checkpoint_name, source=source, ts_ms=ts_ms, market=market)
    return decision_payload


def build_small_account_runtime_report(
    *,
    store: LiveStateStore,
    canary_enabled: bool,
    max_positions: int,
    max_open_orders_per_market: int,
    local_positions: list[dict[str, Any]],
    exchange_bot_open_orders: list[dict[str, Any]],
    ts_ms: int,
    persist: bool = True,
) -> dict[str, Any]:
    positions = list(local_positions)
    bot_orders = list(exchange_bot_open_orders)
    open_orders_by_market: dict[str, int] = {}
    active_markets: set[str] = set()
    for item in positions:
        market = str(item.get("market", "")).strip().upper()
        if market:
            active_markets.add(market)
    for item in bot_orders:
        market = str(item.get("market", "")).strip().upper()
        if not market:
            continue
        active_markets.add(market)
        open_orders_by_market[market] = open_orders_by_market.get(market, 0) + 1

    violations: list[str] = []
    if canary_enabled:
        if len(positions) > max(int(max_positions), 1):
            violations.append(CANARY_MAX_POSITIONS_EXCEEDED)
        if len(active_markets) > max(int(max_positions), 1):
            violations.append(CANARY_MULTIPLE_ACTIVE_MARKETS)
        if any(count > max(int(max_open_orders_per_market), 1) for count in open_orders_by_market.values()):
            violations.append(CANARY_MAX_OPEN_ORDERS_PER_MARKET_EXCEEDED)

    payload = {
        "ts_ms": int(ts_ms),
        "canary": {
            "enabled": bool(canary_enabled),
            "max_positions": max(int(max_positions), 1),
            "max_open_orders_per_market": max(int(max_open_orders_per_market), 1),
            "violations": violations,
        },
        "state": {
            "positions_count": len(positions),
            "bot_open_orders_count": len(bot_orders),
            "bot_open_orders_by_market": open_orders_by_market,
            "active_markets": sorted(active_markets),
        },
        "counters": {
            "rejected_for_cost_count": _counter_count(store=store, checkpoint_name=REJECTED_FOR_COST_CHECKPOINT),
            "rejected_for_min_total_count": _counter_count(
                store=store,
                checkpoint_name=REJECTED_FOR_MIN_TOTAL_CHECKPOINT,
            ),
            "dust_abort_count": _counter_count(store=store, checkpoint_name=DUST_ABORT_CHECKPOINT),
        },
        "formula": {
            "cost_formula": COST_FORMULA,
        },
    }
    if persist:
        store.set_checkpoint(name="last_small_account_report", payload=payload, ts_ms=ts_ms)
        _write_small_account_report(store=store, payload=payload)
    return payload


def _counter_checkpoint_for_reject(reject_code: str) -> str | None:
    if reject_code == "REJECT_EXPECTED_EDGE_NOT_POSITIVE_AFTER_COST":
        return REJECTED_FOR_COST_CHECKPOINT
    if reject_code == "REJECT_BELOW_MIN_TOTAL":
        return REJECTED_FOR_MIN_TOTAL_CHECKPOINT
    if reject_code == "REJECT_DUST_REMAINDER":
        return DUST_ABORT_CHECKPOINT
    return None


def _counter_count(*, store: LiveStateStore, checkpoint_name: str) -> int:
    payload = store.get_checkpoint(name=checkpoint_name)
    inner = payload.get("payload") if isinstance(payload, dict) else None
    if not isinstance(inner, dict):
        return 0
    try:
        return int(inner.get("count", 0))
    except (TypeError, ValueError):
        return 0


def _increment_counter(
    *,
    store: LiveStateStore,
    checkpoint_name: str,
    source: str,
    ts_ms: int,
    market: str | None,
) -> None:
    payload = store.get_checkpoint(name=checkpoint_name)
    inner = payload.get("payload") if isinstance(payload, dict) else None
    current = dict(inner) if isinstance(inner, dict) else {"count": 0, "sources": {}, "markets": {}}
    current["count"] = int(current.get("count", 0)) + 1
    sources = dict(current.get("sources", {})) if isinstance(current.get("sources"), dict) else {}
    source_key = str(source).strip().lower() or "unknown"
    sources[source_key] = int(sources.get(source_key, 0)) + 1
    current["sources"] = sources
    if market:
        markets = dict(current.get("markets", {})) if isinstance(current.get("markets"), dict) else {}
        market_key = str(market).strip().upper()
        if market_key:
            markets[market_key] = int(markets.get(market_key, 0)) + 1
        current["markets"] = markets
    current["last_ts_ms"] = int(ts_ms)
    store.set_checkpoint(name=checkpoint_name, payload=current, ts_ms=ts_ms)


def _write_small_account_report(*, store: LiveStateStore, payload: dict[str, Any]) -> None:
    path = store.db_path.parent / "live_small_account_report.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")


def cost_breakdown_to_payload(value: SmallAccountCostBreakdown) -> dict[str, Any]:
    return asdict(value)


def sizing_envelope_to_payload(value: SmallAccountSizingEnvelope) -> dict[str, Any]:
    return asdict(value)
