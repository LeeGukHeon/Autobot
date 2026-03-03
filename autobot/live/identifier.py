"""Helpers for generating and classifying bot order identifiers."""

from __future__ import annotations

import time
import uuid


def new_order_identifier(
    *,
    prefix: str,
    bot_id: str,
    intent_id: str,
    nonce: str | None = None,
    ts_ms: int | None = None,
) -> str:
    prefix_value = _normalize_token(prefix, upper=True)
    bot_id_value = _normalize_token(bot_id, upper=False)
    intent_id_value = _normalize_token(intent_id, upper=False)
    nonce_value = _normalize_token(nonce or uuid.uuid4().hex[:10], upper=False)
    ts_value = str(int(ts_ms if ts_ms is not None else time.time() * 1000))
    return f"{prefix_value}-{bot_id_value}-{intent_id_value}-{ts_value}-{nonce_value}"


def is_bot_identifier(identifier: str | None, *, prefix: str, bot_id: str) -> bool:
    if identifier is None:
        return False
    identifier_value = str(identifier).strip()
    if not identifier_value:
        return False
    expected_prefix = f"{_normalize_token(prefix, upper=True)}-{_normalize_token(bot_id, upper=False)}-"
    return identifier_value.startswith(expected_prefix)


def _normalize_token(raw: str, *, upper: bool) -> str:
    text = str(raw).strip()
    if not text:
        raise ValueError("identifier token must not be blank")
    normalized = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "-" for ch in text)
    return normalized.upper() if upper else normalized.lower()
