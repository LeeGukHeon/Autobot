"""Checkpoint helpers for resumable REST ticks collection."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def checkpoint_key(
    market: str,
    days_ago: int | None = None,
    *,
    target_date: str | None = None,
) -> str:
    market_value = str(market).strip().upper()
    if str(target_date or "").strip():
        return f"{market_value}|{str(target_date).strip()}"
    if days_ago is None:
        raise ValueError("checkpoint_key requires days_ago or target_date")
    return f"{market_value}|{int(days_ago)}"


def load_ticks_checkpoint(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(raw, dict):
        return {}

    normalized: dict[str, dict[str, Any]] = {}
    for key, value in raw.items():
        if not isinstance(key, str) or not isinstance(value, dict):
            continue
        market = str(value.get("market", "")).strip().upper()
        days_ago = _as_int(value.get("days_ago"))
        target_date = _as_str(value.get("target_date"))
        if not market or (days_ago is None and not target_date):
            continue
        normalized_key = checkpoint_key(market, days_ago, target_date=target_date)
        normalized[normalized_key] = {
            "market": market,
            "days_ago": (int(days_ago) if days_ago is not None else None),
            "target_date": target_date,
            "last_cursor": _as_str(value.get("last_cursor")),
            "last_success_ts_ms": _as_int(value.get("last_success_ts_ms")),
            "pages_collected": _as_int(value.get("pages_collected"), default=0) or 0,
            "updated_at_ms": _as_int(value.get("updated_at_ms")),
        }
    return normalized


def save_ticks_checkpoint(path: Path, state: dict[str, dict[str, Any]]) -> None:
    payload: dict[str, dict[str, Any]] = {}
    for key, value in sorted(state.items()):
        if not isinstance(key, str) or not isinstance(value, dict):
            continue
        payload[key] = {
            "market": _as_str(value.get("market")),
            "days_ago": _as_int(value.get("days_ago")),
            "target_date": _as_str(value.get("target_date")),
            "last_cursor": _as_str(value.get("last_cursor")),
            "last_success_ts_ms": _as_int(value.get("last_success_ts_ms")),
            "pages_collected": _as_int(value.get("pages_collected"), default=0),
            "updated_at_ms": _as_int(value.get("updated_at_ms")),
        }

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    tmp_path.replace(path)


def update_ticks_checkpoint(
    state: dict[str, dict[str, Any]],
    *,
    market: str,
    days_ago: int,
    target_date: str | None = None,
    last_cursor: str | None,
    last_success_ts_ms: int,
    pages_collected: int,
    updated_at_ms: int,
) -> dict[str, Any]:
    key = checkpoint_key(market, days_ago, target_date=target_date)
    existing_pages = 0
    existing = state.get(key)
    if isinstance(existing, dict):
        existing_pages = _as_int(existing.get("pages_collected"), default=0) or 0

    entry = {
        "market": str(market).strip().upper(),
        "days_ago": int(days_ago),
        "target_date": _as_str(target_date),
        "last_cursor": _as_str(last_cursor),
        "last_success_ts_ms": int(last_success_ts_ms),
        "pages_collected": int(existing_pages + max(int(pages_collected), 0)),
        "updated_at_ms": int(updated_at_ms),
    }
    state[key] = entry
    return entry


def _as_int(value: Any, *, default: int | None = None) -> int | None:
    if value is None:
        return default
    try:
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return default
            return int(float(text))
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
