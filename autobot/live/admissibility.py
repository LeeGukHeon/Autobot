"""Exact live-order admissibility helpers for Upbit spot trading."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
import math
import time
from typing import Any

EPSILON = 1e-12

REJECT_TICK_SIZE_UNAVAILABLE = "TICK_SIZE_UNAVAILABLE"
REJECT_MIN_TOTAL_UNAVAILABLE = "MIN_TOTAL_UNAVAILABLE"
REJECT_PRICE_NOT_TICK_ALIGNED = "PRICE_NOT_TICK_ALIGNED"
REJECT_BELOW_MIN_TOTAL = "BELOW_MIN_TOTAL"
REJECT_INSUFFICIENT_FREE_BALANCE = "INSUFFICIENT_FREE_BALANCE"
REJECT_FEE_RESERVE_INSUFFICIENT = "FEE_RESERVE_INSUFFICIENT"
REJECT_DUST_REMAINDER = "DUST_REMAINDER"
REJECT_EXPECTED_EDGE_NOT_POSITIVE_AFTER_COST = "EXPECTED_EDGE_NOT_POSITIVE_AFTER_COST"


@dataclass(frozen=True)
class AccountBalanceSnapshot:
    currency: str
    free: float
    locked: float
    avg_buy_price: float | None


@dataclass(frozen=True)
class LiveOrderAdmissibilitySnapshot:
    market: str
    side: str
    base_currency: str
    quote_currency: str
    tick_size: float
    min_total: float
    bid_fee: float
    ask_fee: float
    quote_balance: AccountBalanceSnapshot
    base_balance: AccountBalanceSnapshot
    chance_hash: str
    instruments_hash: str
    accounts_hash: str
    ts_ms: int


@dataclass(frozen=True)
class LiveOrderAdmissibilityDecision:
    admissible: bool
    reject_code: str | None
    requested_price: float
    requested_volume: float
    adjusted_price: float
    adjusted_volume: float
    adjusted_notional: float
    fee_reserve_quote: float
    remaining_quote_after: float | None
    remaining_base_after: float | None
    price_adjusted: bool
    expected_edge_bps: float | None
    estimated_entry_cost_bps: float


def build_live_order_admissibility_snapshot(
    *,
    market: str,
    side: str,
    chance_payload: dict[str, Any],
    instruments_payload: Any,
    accounts_payload: Any,
    ts_ms: int | None = None,
) -> LiveOrderAdmissibilitySnapshot:
    market_value = str(market).strip().upper()
    side_value = str(side).strip().lower()
    if not market_value or "-" not in market_value:
        raise ValueError("market must be like KRW-BTC")
    if side_value not in {"bid", "ask"}:
        raise ValueError("side must be bid or ask")
    quote_currency, base_currency = market_value.split("-", 1)
    tick_size = extract_tick_size(instruments_payload, market=market_value)
    min_total = extract_min_total(chance_payload, side=side_value, market=market_value)
    bid_fee = _extract_positive_or_zero(chance_payload.get("bid_fee"))
    ask_fee = _extract_positive_or_zero(chance_payload.get("ask_fee"))
    quote_balance = extract_account_balance(accounts_payload, currency=quote_currency)
    base_balance = extract_account_balance(accounts_payload, currency=base_currency)
    now_ts = int(ts_ms if ts_ms is not None else time.time() * 1000)
    return LiveOrderAdmissibilitySnapshot(
        market=market_value,
        side=side_value,
        base_currency=base_currency,
        quote_currency=quote_currency,
        tick_size=tick_size,
        min_total=min_total,
        bid_fee=bid_fee,
        ask_fee=ask_fee,
        quote_balance=quote_balance,
        base_balance=base_balance,
        chance_hash=_canonical_payload_hash(chance_payload),
        instruments_hash=_canonical_payload_hash(instruments_payload),
        accounts_hash=_canonical_payload_hash(accounts_payload),
        ts_ms=now_ts,
    )


def evaluate_live_limit_order(
    *,
    snapshot: LiveOrderAdmissibilitySnapshot,
    price: float,
    volume: float,
    expected_edge_bps: float | None = None,
) -> LiveOrderAdmissibilityDecision:
    requested_price = float(price)
    requested_volume = float(volume)
    if requested_price <= 0 or requested_volume <= 0:
        raise ValueError("price and volume must be positive")
    if snapshot.tick_size <= 0:
        return _reject(
            snapshot=snapshot,
            reject_code=REJECT_TICK_SIZE_UNAVAILABLE,
            requested_price=requested_price,
            requested_volume=requested_volume,
            adjusted_price=requested_price,
            adjusted_volume=requested_volume,
        )
    if snapshot.min_total <= 0:
        return _reject(
            snapshot=snapshot,
            reject_code=REJECT_MIN_TOTAL_UNAVAILABLE,
            requested_price=requested_price,
            requested_volume=requested_volume,
            adjusted_price=requested_price,
            adjusted_volume=requested_volume,
        )

    adjusted_price = round_price_to_tick(price=requested_price, tick_size=snapshot.tick_size, side=snapshot.side)
    price_adjusted = not math.isclose(adjusted_price, requested_price, rel_tol=0.0, abs_tol=snapshot.tick_size / 10.0)
    adjusted_volume = requested_volume
    adjusted_notional = adjusted_price * adjusted_volume

    if adjusted_notional + EPSILON < snapshot.min_total:
        return _reject(
            snapshot=snapshot,
            reject_code=REJECT_BELOW_MIN_TOTAL,
            requested_price=requested_price,
            requested_volume=requested_volume,
            adjusted_price=adjusted_price,
            adjusted_volume=adjusted_volume,
            price_adjusted=price_adjusted,
        )

    fee_rate = snapshot.bid_fee if snapshot.side == "bid" else snapshot.ask_fee
    estimated_entry_cost_bps = max(float(fee_rate), 0.0) * 10_000.0
    if expected_edge_bps is not None and float(expected_edge_bps) <= estimated_entry_cost_bps + EPSILON:
        return _reject(
            snapshot=snapshot,
            reject_code=REJECT_EXPECTED_EDGE_NOT_POSITIVE_AFTER_COST,
            requested_price=requested_price,
            requested_volume=requested_volume,
            adjusted_price=adjusted_price,
            adjusted_volume=adjusted_volume,
            price_adjusted=price_adjusted,
            expected_edge_bps=float(expected_edge_bps),
        )

    if snapshot.side == "bid":
        free_quote = max(float(snapshot.quote_balance.free), 0.0)
        if free_quote + EPSILON < adjusted_notional:
            return _reject(
                snapshot=snapshot,
                reject_code=REJECT_INSUFFICIENT_FREE_BALANCE,
                requested_price=requested_price,
                requested_volume=requested_volume,
                adjusted_price=adjusted_price,
                adjusted_volume=adjusted_volume,
                price_adjusted=price_adjusted,
                expected_edge_bps=expected_edge_bps,
            )
        fee_reserve_quote = adjusted_notional * fee_rate
        total_required_quote = adjusted_notional + fee_reserve_quote
        if free_quote + EPSILON < total_required_quote:
            return _reject(
                snapshot=snapshot,
                reject_code=REJECT_FEE_RESERVE_INSUFFICIENT,
                requested_price=requested_price,
                requested_volume=requested_volume,
                adjusted_price=adjusted_price,
                adjusted_volume=adjusted_volume,
                price_adjusted=price_adjusted,
                expected_edge_bps=expected_edge_bps,
            )
        return LiveOrderAdmissibilityDecision(
            admissible=True,
            reject_code=None,
            requested_price=requested_price,
            requested_volume=requested_volume,
            adjusted_price=adjusted_price,
            adjusted_volume=adjusted_volume,
            adjusted_notional=adjusted_notional,
            fee_reserve_quote=fee_reserve_quote,
            remaining_quote_after=max(free_quote - total_required_quote, 0.0),
            remaining_base_after=max(float(snapshot.base_balance.free), 0.0),
            price_adjusted=price_adjusted,
            expected_edge_bps=float(expected_edge_bps) if expected_edge_bps is not None else None,
            estimated_entry_cost_bps=estimated_entry_cost_bps,
        )

    free_base = max(float(snapshot.base_balance.free), 0.0)
    if free_base + EPSILON < adjusted_volume:
        return _reject(
            snapshot=snapshot,
            reject_code=REJECT_INSUFFICIENT_FREE_BALANCE,
            requested_price=requested_price,
            requested_volume=requested_volume,
            adjusted_price=adjusted_price,
            adjusted_volume=adjusted_volume,
            price_adjusted=price_adjusted,
            expected_edge_bps=expected_edge_bps,
        )
    remaining_base_after = max(free_base - adjusted_volume, 0.0)
    if remaining_base_after > EPSILON and (remaining_base_after * adjusted_price) + EPSILON < snapshot.min_total:
        return _reject(
            snapshot=snapshot,
            reject_code=REJECT_DUST_REMAINDER,
            requested_price=requested_price,
            requested_volume=requested_volume,
            adjusted_price=adjusted_price,
            adjusted_volume=adjusted_volume,
            price_adjusted=price_adjusted,
            expected_edge_bps=expected_edge_bps,
        )
    return LiveOrderAdmissibilityDecision(
        admissible=True,
        reject_code=None,
        requested_price=requested_price,
        requested_volume=requested_volume,
        adjusted_price=adjusted_price,
        adjusted_volume=adjusted_volume,
        adjusted_notional=adjusted_notional,
        fee_reserve_quote=adjusted_notional * fee_rate,
        remaining_quote_after=None,
        remaining_base_after=remaining_base_after,
        price_adjusted=price_adjusted,
        expected_edge_bps=float(expected_edge_bps) if expected_edge_bps is not None else None,
        estimated_entry_cost_bps=estimated_entry_cost_bps,
    )


def build_live_admissibility_report(
    *,
    snapshot: LiveOrderAdmissibilitySnapshot,
    decision: LiveOrderAdmissibilityDecision,
    test_order_payload: Any | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "market": snapshot.market,
        "side": snapshot.side,
        "snapshot": {
            "base_currency": snapshot.base_currency,
            "quote_currency": snapshot.quote_currency,
            "tick_size": snapshot.tick_size,
            "min_total": snapshot.min_total,
            "bid_fee": snapshot.bid_fee,
            "ask_fee": snapshot.ask_fee,
            "quote_balance": asdict(snapshot.quote_balance),
            "base_balance": asdict(snapshot.base_balance),
            "chance_hash": snapshot.chance_hash,
            "instruments_hash": snapshot.instruments_hash,
            "accounts_hash": snapshot.accounts_hash,
            "ts_ms": snapshot.ts_ms,
        },
        "decision": asdict(decision),
    }
    if test_order_payload is not None:
        payload["test_order"] = test_order_payload
    return payload


def extract_min_total(chance_payload: dict[str, Any], *, side: str, market: str) -> float:
    market_payload = chance_payload.get("market")
    if not isinstance(market_payload, dict):
        raise ValueError("chance payload missing market object")
    side_value = str(side).strip().lower()
    side_payload = market_payload.get(side_value)
    if isinstance(side_payload, dict):
        side_min_total = _extract_positive_or_none(side_payload.get("min_total"))
        if side_min_total is not None:
            return side_min_total
    candidate_values: list[float] = []
    for side_key in ("bid", "ask"):
        candidate_side = market_payload.get(side_key)
        if not isinstance(candidate_side, dict):
            continue
        candidate_min_total = _extract_positive_or_none(candidate_side.get("min_total"))
        if candidate_min_total is not None:
            candidate_values.append(candidate_min_total)
    if candidate_values:
        return max(candidate_values)
    raise ValueError(f"chance payload missing min_total for market={market}")


def extract_tick_size(instruments_payload: Any, *, market: str) -> float:
    if not isinstance(instruments_payload, list):
        raise ValueError("orderbook instruments payload must be a list")
    market_value = str(market).strip().upper()
    for item in instruments_payload:
        if not isinstance(item, dict):
            continue
        item_market = str(item.get("market", "")).strip().upper()
        if item_market != market_value:
            continue
        tick_size = _extract_positive_or_none(item.get("tick_size"))
        if tick_size is not None:
            return tick_size
    raise ValueError(f"tick_size not found for market={market_value}")


def extract_account_balance(accounts_payload: Any, *, currency: str) -> AccountBalanceSnapshot:
    currency_value = str(currency).strip().upper()
    if not isinstance(accounts_payload, list):
        raise ValueError("accounts payload must be a list")
    for item in accounts_payload:
        if not isinstance(item, dict):
            continue
        item_currency = str(item.get("currency", "")).strip().upper()
        if item_currency != currency_value:
            continue
        free = _extract_nonnegative_float(item.get("balance"))
        locked = _extract_nonnegative_float(item.get("locked"))
        avg_buy_price = _extract_positive_or_none(item.get("avg_buy_price"))
        return AccountBalanceSnapshot(
            currency=currency_value,
            free=free,
            locked=locked,
            avg_buy_price=avg_buy_price,
        )
    return AccountBalanceSnapshot(currency=currency_value, free=0.0, locked=0.0, avg_buy_price=None)


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
    return round(rounded, _decimal_places(tick_value))


def format_number_string(value: float, *, digits: int = 16) -> str:
    return f"{float(value):.{max(int(digits), 0)}f}".rstrip("0").rstrip(".")


def _reject(
    *,
    snapshot: LiveOrderAdmissibilitySnapshot,
    reject_code: str,
    requested_price: float,
    requested_volume: float,
    adjusted_price: float,
    adjusted_volume: float,
    price_adjusted: bool = False,
    expected_edge_bps: float | None = None,
) -> LiveOrderAdmissibilityDecision:
    adjusted_notional = float(adjusted_price) * float(adjusted_volume)
    fee_rate = snapshot.bid_fee if snapshot.side == "bid" else snapshot.ask_fee
    return LiveOrderAdmissibilityDecision(
        admissible=False,
        reject_code=reject_code,
        requested_price=requested_price,
        requested_volume=requested_volume,
        adjusted_price=adjusted_price,
        adjusted_volume=adjusted_volume,
        adjusted_notional=adjusted_notional,
        fee_reserve_quote=adjusted_notional * fee_rate,
        remaining_quote_after=None,
        remaining_base_after=None,
        price_adjusted=price_adjusted,
        expected_edge_bps=float(expected_edge_bps) if expected_edge_bps is not None else None,
        estimated_entry_cost_bps=max(float(fee_rate), 0.0) * 10_000.0,
    )


def _canonical_payload_hash(payload: Any) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _extract_positive_or_none(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number) or number <= 0:
        return None
    return number


def _extract_positive_or_zero(value: Any) -> float:
    number = _extract_positive_or_none(value)
    return 0.0 if number is None else number


def _extract_nonnegative_float(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(number) or number < 0:
        return 0.0
    return number


def _decimal_places(value: float) -> int:
    text = f"{float(value):.16f}".rstrip("0")
    if "." not in text:
        return 0
    return len(text.split(".", 1)[1])
