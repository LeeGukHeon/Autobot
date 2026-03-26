from __future__ import annotations

import csv
from collections import deque
from pathlib import Path
from typing import Any, Iterable


def summarize_fill_records(fill_records: Iterable[dict[str, Any]] | None) -> dict[str, Any]:
    rows = [dict(item) for item in (fill_records or []) if isinstance(item, dict)]
    return summarize_trade_rows(rows)


def summarize_trades_csv(path: Path) -> dict[str, Any]:
    try:
        with Path(path).open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
    except OSError:
        return summarize_trade_rows([])
    return summarize_trade_rows(rows)


def summarize_trade_rows(rows: Iterable[dict[str, Any]] | None) -> dict[str, Any]:
    normalized_rows = [dict(item) for item in (rows or []) if isinstance(item, dict)]
    position_queues: dict[str, deque[dict[str, float]]] = {}
    round_trip_pnls: list[float] = []
    exit_reason_counts: dict[str, int] = {}
    market_loss_abs_totals: dict[str, float] = {}

    for row in normalized_rows:
        market = str(row.get("market", "")).strip().upper()
        side = str(row.get("side", "")).strip().lower()
        volume = _safe_optional_float(row.get("volume")) or 0.0
        price = _safe_optional_float(row.get("fill_price"))
        if price is None:
            price = _safe_optional_float(row.get("price")) or 0.0
        fee_quote = _safe_optional_float(row.get("fee_quote")) or 0.0
        if not market or volume <= 0.0 or price <= 0.0:
            continue

        if side == "bid":
            queue = position_queues.setdefault(market, deque())
            queue.append(
                {
                    "remaining_volume": float(volume),
                    "price": float(price),
                    "fee_per_unit": float(fee_quote) / float(volume) if float(volume) > 0.0 else 0.0,
                }
            )
            continue

        if side != "ask":
            continue

        queue = position_queues.setdefault(market, deque())
        remaining_volume = float(volume)
        gross_pnl_quote = 0.0
        entry_fee_quote = 0.0
        matched_volume = 0.0
        while queue and remaining_volume > 1e-12:
            entry_lot = queue[0]
            lot_remaining = max(float(entry_lot.get("remaining_volume", 0.0)), 0.0)
            if lot_remaining <= 1e-12:
                queue.popleft()
                continue
            matched = min(lot_remaining, remaining_volume)
            gross_pnl_quote += (float(price) - float(entry_lot.get("price", 0.0))) * float(matched)
            entry_fee_quote += float(entry_lot.get("fee_per_unit", 0.0)) * float(matched)
            matched_volume += float(matched)
            remaining_volume -= float(matched)
            next_remaining = max(lot_remaining - float(matched), 0.0)
            if next_remaining <= 1e-12:
                queue.popleft()
            else:
                entry_lot["remaining_volume"] = float(next_remaining)
                queue[0] = entry_lot

        if matched_volume <= 1e-12:
            continue

        exit_fee_quote = float(fee_quote) * (float(matched_volume) / float(volume)) if float(volume) > 0.0 else 0.0
        net_pnl_quote = float(gross_pnl_quote) - float(entry_fee_quote) - float(exit_fee_quote)
        round_trip_pnls.append(float(net_pnl_quote))
        if net_pnl_quote < 0.0:
            market_loss_abs_totals[market] = float(market_loss_abs_totals.get(market, 0.0)) + abs(float(net_pnl_quote))
        reason_code = str(row.get("reason_code", "")).strip().upper() or "UNKNOWN"
        exit_reason_counts[reason_code] = int(exit_reason_counts.get(reason_code, 0)) + 1

    wins = [float(value) for value in round_trip_pnls if float(value) > 0.0]
    losses = [abs(float(value)) for value in round_trip_pnls if float(value) < 0.0]
    closed_trade_count = len(round_trip_pnls)
    win_pnl_quote_total = float(sum(wins))
    loss_pnl_quote_total_abs = float(sum(losses))
    avg_win_quote = (win_pnl_quote_total / len(wins)) if wins else 0.0
    avg_loss_quote = (loss_pnl_quote_total_abs / len(losses)) if losses else 0.0
    payoff_ratio = (
        win_pnl_quote_total / loss_pnl_quote_total_abs
        if loss_pnl_quote_total_abs > 0.0
        else (9_999.0 if win_pnl_quote_total > 0.0 else 0.0)
    )
    tp_exit_count = sum(count for reason, count in exit_reason_counts.items() if reason.endswith("_TP"))
    sl_exit_count = sum(count for reason, count in exit_reason_counts.items() if reason.endswith("_SL"))
    trailing_exit_count = sum(count for reason, count in exit_reason_counts.items() if reason.endswith("_TRAILING"))
    timeout_exit_count = sum(count for reason, count in exit_reason_counts.items() if "TIMEOUT" in reason)
    total_negative_abs = sum(float(value) for value in market_loss_abs_totals.values())
    market_loss_concentration = (
        max((float(value) / total_negative_abs) for value in market_loss_abs_totals.values())
        if total_negative_abs > 0.0 and market_loss_abs_totals
        else 0.0
    )
    denom = max(closed_trade_count, 1)
    return {
        "closed_trade_count": int(closed_trade_count),
        "wins": int(len(wins)),
        "losses": int(len(losses)),
        "win_pnl_quote_total": float(win_pnl_quote_total),
        "loss_pnl_quote_total_abs": float(loss_pnl_quote_total_abs),
        "avg_win_quote": float(avg_win_quote),
        "avg_loss_quote": float(avg_loss_quote),
        "payoff_ratio": float(max(payoff_ratio, 0.0)),
        "tp_exit_count": int(tp_exit_count),
        "sl_exit_count": int(sl_exit_count),
        "trailing_exit_count": int(trailing_exit_count),
        "timeout_exit_count": int(timeout_exit_count),
        "tp_exit_share": float(tp_exit_count) / float(denom),
        "sl_exit_share": float(sl_exit_count) / float(denom),
        "trailing_exit_share": float(trailing_exit_count) / float(denom),
        "timeout_exit_share": float(timeout_exit_count) / float(denom),
        "market_loss_concentration": max(min(float(market_loss_concentration), 1.0), 0.0),
        "exit_reason_counts": {
            str(key).strip().upper(): int(value)
            for key, value in exit_reason_counts.items()
            if str(key).strip()
        },
    }


def build_intent_outcomes_from_trade_csv(path: Path) -> dict[str, dict[str, Any]]:
    try:
        with Path(path).open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
    except OSError:
        return {}
    return build_intent_outcomes_from_trade_rows(rows)


def build_intent_outcomes_from_trade_rows(rows: Iterable[dict[str, Any]] | None) -> dict[str, dict[str, Any]]:
    normalized_rows = [dict(item) for item in (rows or []) if isinstance(item, dict)]
    position_queues: dict[str, deque[dict[str, Any]]] = {}
    outcomes: dict[str, dict[str, Any]] = {}

    for row in normalized_rows:
        market = str(row.get("market", "")).strip().upper()
        side = str(row.get("side", "")).strip().lower()
        volume = _safe_optional_float(row.get("volume")) or 0.0
        price = _safe_optional_float(row.get("fill_price"))
        if price is None:
            price = _safe_optional_float(row.get("price")) or 0.0
        fee_quote = _safe_optional_float(row.get("fee_quote")) or 0.0
        intent_id = str(row.get("intent_id", "")).strip()
        if not market or volume <= 0.0 or price <= 0.0:
            continue

        if side == "bid":
            queue = position_queues.setdefault(market, deque())
            queue.append(
                {
                    "intent_id": intent_id,
                    "remaining_volume": float(volume),
                    "price": float(price),
                    "fee_per_unit": float(fee_quote) / float(volume) if float(volume) > 0.0 else 0.0,
                }
            )
            if intent_id:
                outcome = outcomes.setdefault(intent_id, _empty_intent_outcome(intent_id=intent_id, market=market))
                outcome["entry_fill_count"] = int(outcome.get("entry_fill_count", 0)) + 1
                outcome["entry_notional_quote"] = float(outcome.get("entry_notional_quote", 0.0)) + float(price) * float(volume)
                outcome["entry_fee_quote"] = float(outcome.get("entry_fee_quote", 0.0)) + float(fee_quote)
                outcome["entry_filled_volume"] = float(outcome.get("entry_filled_volume", 0.0)) + float(volume)
                outcome["open_volume"] = float(outcome.get("open_volume", 0.0)) + float(volume)
                _append_optional_float(outcome.setdefault("entry_slippages_bps", []), row.get("slippage_bps"))
                _inc_nested_count(outcome.setdefault("entry_price_mode_counts", {}), row.get("price_mode"))
            continue

        if side != "ask":
            continue

        queue = position_queues.setdefault(market, deque())
        remaining_volume = float(volume)
        ask_reason_code = str(row.get("reason_code", "")).strip().upper() or "UNKNOWN"
        ask_slippage = _safe_optional_float(row.get("slippage_bps"))
        while queue and remaining_volume > 1e-12:
            entry_lot = queue[0]
            lot_remaining = max(float(entry_lot.get("remaining_volume", 0.0) or 0.0), 0.0)
            if lot_remaining <= 1e-12:
                queue.popleft()
                continue
            matched = min(lot_remaining, remaining_volume)
            entry_intent_id = str(entry_lot.get("intent_id") or "").strip()
            if entry_intent_id:
                outcome = outcomes.setdefault(entry_intent_id, _empty_intent_outcome(intent_id=entry_intent_id, market=market))
                entry_fee_quote = float(entry_lot.get("fee_per_unit", 0.0) or 0.0) * float(matched)
                exit_fee_quote = float(fee_quote) * (float(matched) / float(volume)) if float(volume) > 0.0 else 0.0
                gross_pnl_quote = (float(price) - float(entry_lot.get("price", 0.0) or 0.0)) * float(matched)
                net_pnl_quote = float(gross_pnl_quote) - float(entry_fee_quote) - float(exit_fee_quote)
                outcome["exit_fill_count"] = int(outcome.get("exit_fill_count", 0)) + 1
                outcome["exit_notional_quote"] = float(outcome.get("exit_notional_quote", 0.0)) + float(price) * float(matched)
                outcome["exit_fee_quote"] = float(outcome.get("exit_fee_quote", 0.0)) + float(exit_fee_quote)
                outcome["closed_volume"] = float(outcome.get("closed_volume", 0.0)) + float(matched)
                outcome["open_volume"] = max(float(outcome.get("open_volume", 0.0)) - float(matched), 0.0)
                outcome["realized_pnl_quote"] = float(outcome.get("realized_pnl_quote", 0.0)) + float(net_pnl_quote)
                _append_optional_float(outcome.setdefault("exit_slippages_bps", []), ask_slippage)
                _inc_nested_count(outcome.setdefault("exit_reason_counts", {}), ask_reason_code)
            remaining_volume -= float(matched)
            next_remaining = max(lot_remaining - float(matched), 0.0)
            if next_remaining <= 1e-12:
                queue.popleft()
            else:
                entry_lot["remaining_volume"] = float(next_remaining)
                queue[0] = entry_lot

    for intent_id, outcome in outcomes.items():
        entry_volume = float(outcome.get("entry_filled_volume", 0.0) or 0.0)
        closed_volume = float(outcome.get("closed_volume", 0.0) or 0.0)
        exit_fill_count = int(outcome.get("exit_fill_count", 0) or 0)
        outcome["closed"] = bool(exit_fill_count > 0 and entry_volume > 0.0 and abs(entry_volume - closed_volume) <= 1e-9)
        outcome["entry_slippage_bps_mean"] = _mean_optional(outcome.pop("entry_slippages_bps", []))
        outcome["exit_slippage_bps_mean"] = _mean_optional(outcome.pop("exit_slippages_bps", []))
        if exit_fill_count <= 0:
            outcome["realized_pnl_quote"] = None
        outcome["net_flow_quote"] = (
            -float(outcome.get("entry_notional_quote", 0.0) or 0.0)
            - float(outcome.get("entry_fee_quote", 0.0) or 0.0)
            + float(outcome.get("exit_notional_quote", 0.0) or 0.0)
            - float(outcome.get("exit_fee_quote", 0.0) or 0.0)
        )
        outcomes[intent_id] = outcome
    return outcomes


def _empty_intent_outcome(*, intent_id: str, market: str) -> dict[str, Any]:
    return {
        "intent_id": intent_id,
        "market": market,
        "entry_fill_count": 0,
        "entry_notional_quote": 0.0,
        "entry_fee_quote": 0.0,
        "entry_filled_volume": 0.0,
        "entry_price_mode_counts": {},
        "exit_fill_count": 0,
        "exit_notional_quote": 0.0,
        "exit_fee_quote": 0.0,
        "closed_volume": 0.0,
        "open_volume": 0.0,
        "realized_pnl_quote": 0.0,
        "exit_reason_counts": {},
    }


def _inc_nested_count(mapping: dict[str, int], value: Any) -> None:
    key = str(value or "").strip().upper()
    if not key:
        return
    mapping[key] = int(mapping.get(key, 0)) + 1


def _append_optional_float(values: list[float], raw_value: Any) -> None:
    value = _safe_optional_float(raw_value)
    if value is None:
        return
    values.append(float(value))


def _mean_optional(values: list[float]) -> float | None:
    if not values:
        return None
    return float(sum(float(value) for value in values) / float(len(values)))


def _safe_optional_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
