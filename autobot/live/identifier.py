"""Helpers for generating and classifying bot order identifiers."""

from __future__ import annotations

import time
import uuid


def new_order_identifier(
    *,
    prefix: str,
    bot_id: str,
    intent_id: str,
    run_token: str | None = None,
    nonce: str | None = None,
    ts_ms: int | None = None,
) -> str:
    prefix_value = _normalize_token(prefix, upper=True)
    bot_id_value = _normalize_token(bot_id, upper=False)
    intent_id_value = _normalize_token(intent_id, upper=False)
    nonce_value = _normalize_token(nonce or uuid.uuid4().hex[:10], upper=False)
    ts_value = str(int(ts_ms if ts_ms is not None else time.time() * 1000))
    run_token_value = _normalize_token(run_token, upper=False) if run_token is not None else None
    base = f"{prefix_value}-{bot_id_value}-{intent_id_value}-{ts_value}-{nonce_value}"
    return f"{base}-rid_{run_token_value}" if run_token_value is not None else base


def new_protective_order_identifier(
    *,
    prefix: str,
    bot_id: str,
    marker: str,
    scope_token: str,
    ts_ms: int | None = None,
    step: int | None = None,
) -> str:
    prefix_value = _normalize_token(prefix, upper=True)
    bot_id_value = _normalize_token(bot_id, upper=False)
    marker_value = _normalize_token(marker, upper=True)
    scope_value = _normalize_token(scope_token, upper=False)
    ts_value = str(int(ts_ms if ts_ms is not None else time.time() * 1000))
    if step is None:
        return f"{prefix_value}-{bot_id_value}-{marker_value}-{scope_value}-{ts_value}"
    return f"{prefix_value}-{bot_id_value}-{marker_value}-{scope_value}-{int(step)}-{ts_value}"


def is_bot_identifier(identifier: str | None, *, prefix: str, bot_id: str) -> bool:
    if identifier is None:
        return False
    identifier_value = str(identifier).strip()
    if not identifier_value:
        return False
    normalized_prefix = _normalize_token(prefix, upper=True)
    expected_prefix = f"{normalized_prefix}-{_normalize_token(bot_id, upper=False)}-"
    return identifier_value.startswith(expected_prefix)


def extract_intent_id_from_identifier(identifier: str | None, *, prefix: str, bot_id: str) -> str | None:
    if not is_bot_identifier(identifier, prefix=prefix, bot_id=bot_id):
        return None
    identifier_value = str(identifier).strip()
    expected_prefix = f"{_normalize_token(prefix, upper=True)}-{_normalize_token(bot_id, upper=False)}-"
    suffix = identifier_value[len(expected_prefix) :]
    if not suffix:
        return None
    run_token = None
    if "-rid_" in suffix:
        suffix, run_token = suffix.rsplit("-rid_", 1)
    parts = suffix.rsplit("-", 2)
    if len(parts) != 3:
        return None
    intent_id, ts_ms, nonce = parts
    if not intent_id or not ts_ms.isdigit() or not nonce or (run_token is not None and not run_token):
        return None
    return intent_id


def extract_run_token_from_identifier(identifier: str | None, *, prefix: str, bot_id: str) -> str | None:
    if not is_bot_identifier(identifier, prefix=prefix, bot_id=bot_id):
        return None
    identifier_value = str(identifier).strip()
    expected_prefix = f"{_normalize_token(prefix, upper=True)}-{_normalize_token(bot_id, upper=False)}-"
    suffix = identifier_value[len(expected_prefix) :]
    if not suffix:
        return None
    if "-rid_" not in suffix:
        return None
    suffix, run_token = suffix.rsplit("-rid_", 1)
    parts = suffix.rsplit("-", 2)
    if len(parts) != 3:
        return None
    _, ts_ms, nonce = parts
    if not ts_ms.isdigit() or not nonce or not run_token:
        return None
    return run_token


def _normalize_token(raw: str, *, upper: bool) -> str:
    text = str(raw).strip()
    if not text:
        raise ValueError("identifier token must not be blank")
    normalized = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "-" for ch in text)
    return normalized.upper() if upper else normalized.lower()
