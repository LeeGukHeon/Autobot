"""Canonical raw-trade normalization for WS-primary and REST-repair sources."""

from __future__ import annotations

from typing import Any, Iterable


RAW_TRADE_V1_COLUMNS: tuple[str, ...] = (
    "market",
    "event_ts_ms",
    "price",
    "volume",
    "ask_bid",
    "side",
    "sequential_id",
    "source",
    "source_event_channel",
    "recv_ts_ms",
    "days_ago",
    "collected_at_ms",
)


def normalize_rest_trade_row(row: dict[str, Any]) -> dict[str, Any] | None:
    market = _as_str(row.get("market"), upper=True)
    ts_ms = _to_int(row.get("timestamp_ms"))
    price = _to_float(row.get("trade_price"))
    volume = _to_float(row.get("trade_volume"))
    ask_bid = _as_str(row.get("ask_bid"), upper=True)
    sequential_id = _to_int(row.get("sequential_id"))
    collected_at_ms = _to_int(row.get("collected_at_ms"))
    days_ago = _to_int(row.get("days_ago"))
    if market is None or ts_ms is None or price is None or volume is None:
        return None
    if price <= 0.0 or volume <= 0.0:
        return None
    if ask_bid not in {"ASK", "BID"}:
        return None
    if sequential_id is None:
        return None
    side = _trade_side_from_ask_bid(ask_bid)
    if side is None:
        return None

    return {
        "market": market,
        "event_ts_ms": int(ts_ms),
        "price": float(price),
        "volume": float(volume),
        "ask_bid": ask_bid,
        "side": side,
        "sequential_id": int(sequential_id),
        "source": "rest",
        "source_event_channel": "trade",
        "recv_ts_ms": None,
        "days_ago": days_ago,
        "collected_at_ms": collected_at_ms,
    }


def normalize_ws_trade_row(row: dict[str, Any]) -> dict[str, Any] | None:
    channel = _as_str(row.get("channel"), upper=False)
    if channel != "trade":
        return None

    market = _as_str(row.get("market"), upper=True)
    ts_ms = _to_int(row.get("trade_ts_ms"))
    price = _to_float(row.get("price"))
    volume = _to_float(row.get("volume"))
    ask_bid = _as_str(row.get("ask_bid"), upper=True)
    sequential_id = _to_int(row.get("sequential_id"))
    recv_ts_ms = _to_int(row.get("recv_ts_ms"))
    collected_at_ms = _to_int(row.get("collected_at_ms"))
    if market is None or ts_ms is None or price is None or volume is None:
        return None
    if price <= 0.0 or volume <= 0.0:
        return None
    if ask_bid not in {"ASK", "BID"}:
        return None
    if sequential_id is None:
        return None
    side = _trade_side_from_ask_bid(ask_bid)
    if side is None:
        return None

    return {
        "market": market,
        "event_ts_ms": int(ts_ms),
        "price": float(price),
        "volume": float(volume),
        "ask_bid": ask_bid,
        "side": side,
        "sequential_id": int(sequential_id),
        "source": "ws",
        "source_event_channel": "trade",
        "recv_ts_ms": recv_ts_ms,
        "days_ago": None,
        "collected_at_ms": collected_at_ms,
    }


def canonical_trade_key(row: dict[str, Any]) -> tuple[str, int]:
    market = _as_str(row.get("market"), upper=True)
    sequential_id = _to_int(row.get("sequential_id"))
    if market is None or sequential_id is None:
        raise ValueError("canonical trade key requires market and sequential_id")
    return market, int(sequential_id)


def merge_canonical_trade_rows(
    *row_groups: Iterable[dict[str, Any]],
    prefer_source_order: tuple[str, ...] = ("ws", "rest"),
) -> list[dict[str, Any]]:
    source_rank = {
        str(source).strip().lower(): index
        for index, source in enumerate(prefer_source_order)
        if str(source).strip()
    }
    merged: dict[tuple[str, int], dict[str, Any]] = {}
    for group in row_groups:
        for raw_row in group:
            if not isinstance(raw_row, dict):
                continue
            try:
                key = canonical_trade_key(raw_row)
            except ValueError:
                continue
            current = merged.get(key)
            if current is None:
                merged[key] = dict(raw_row)
                continue
            if _prefer_candidate(raw_row, current, source_rank=source_rank):
                merged[key] = dict(raw_row)
    return sorted(
        merged.values(),
        key=lambda item: (
            int(_to_int(item.get("event_ts_ms")) or 0),
            int(_to_int(item.get("sequential_id")) or 0),
        ),
    )


def _prefer_candidate(
    candidate: dict[str, Any],
    current: dict[str, Any],
    *,
    source_rank: dict[str, int],
) -> bool:
    candidate_source = str(candidate.get("source", "")).strip().lower()
    current_source = str(current.get("source", "")).strip().lower()
    candidate_rank = source_rank.get(candidate_source, len(source_rank) + 1)
    current_rank = source_rank.get(current_source, len(source_rank) + 1)
    if candidate_rank != current_rank:
        return candidate_rank < current_rank
    return _non_null_score(candidate) > _non_null_score(current)


def _non_null_score(row: dict[str, Any]) -> int:
    score = 0
    for column in RAW_TRADE_V1_COLUMNS:
        if row.get(column) is not None:
            score += 1
    return score


def _trade_side_from_ask_bid(ask_bid: str | None) -> str | None:
    if ask_bid == "BID":
        return "buy"
    if ask_bid == "ASK":
        return "sell"
    return None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            return int(float(text))
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_str(value: Any, *, upper: bool) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text.upper() if upper else text.lower()
