"""WebSocket subscription payload builders."""

from __future__ import annotations

from typing import Any, Iterable

from .models import Subscription


VALID_WS_FORMATS = {"DEFAULT", "SIMPLE", "JSON_LIST", "SIMPLE_LIST"}
VALID_PRIVATE_WS_TYPES = {"myOrder", "myAsset"}


def build_subscribe_payload(
    ticket: str,
    subscriptions: Iterable[Subscription | dict[str, Any]],
    fmt: str | None = "SIMPLE_LIST",
) -> list[dict[str, Any]]:
    ticket_value = ticket.strip()
    if not ticket_value:
        raise ValueError("ticket is required")

    normalized_subs: list[Subscription] = []
    for raw in subscriptions:
        normalized_subs.append(_normalize_subscription(raw))
    if not normalized_subs:
        raise ValueError("at least one subscription is required")

    payload: list[dict[str, Any]] = [{"ticket": ticket_value}]
    for sub in normalized_subs:
        item: dict[str, Any] = {"type": sub.type, "codes": list(sub.codes)}
        if sub.is_only_snapshot is not None:
            item["is_only_snapshot"] = bool(sub.is_only_snapshot)
        if sub.is_only_realtime is not None:
            item["is_only_realtime"] = bool(sub.is_only_realtime)
        payload.append(item)

    if fmt:
        normalized_fmt = fmt.strip().upper()
        if normalized_fmt not in VALID_WS_FORMATS:
            allowed = ", ".join(sorted(VALID_WS_FORMATS))
            raise ValueError(f"format must be one of: {allowed}")
        payload.append({"format": normalized_fmt})

    return payload


def build_private_subscribe_payload(
    ticket: str,
    types: Iterable[str],
    fmt: str | None = "SIMPLE_LIST",
) -> list[dict[str, Any]]:
    ticket_value = ticket.strip()
    if not ticket_value:
        raise ValueError("ticket is required")

    normalized_types: list[str] = []
    seen: set[str] = set()
    for raw_type in types:
        type_value = _normalize_private_type(raw_type)
        if type_value in seen:
            continue
        seen.add(type_value)
        normalized_types.append(type_value)
    if not normalized_types:
        raise ValueError("at least one private subscription type is required")

    payload: list[dict[str, Any]] = [{"ticket": ticket_value}]
    for type_value in normalized_types:
        payload.append({"type": type_value})

    if fmt:
        normalized_fmt = fmt.strip().upper()
        if normalized_fmt not in VALID_WS_FORMATS:
            allowed = ", ".join(sorted(VALID_WS_FORMATS))
            raise ValueError(f"format must be one of: {allowed}")
        payload.append({"format": normalized_fmt})
    return payload


def _normalize_subscription(raw: Subscription | dict[str, Any]) -> Subscription:
    if isinstance(raw, Subscription):
        sub_type = raw.type
        codes = raw.codes
        only_snapshot = raw.is_only_snapshot
        only_realtime = raw.is_only_realtime
    elif isinstance(raw, dict):
        sub_type = str(raw.get("type", "")).strip()
        codes = tuple(raw.get("codes", ()))
        only_snapshot = raw.get("is_only_snapshot")
        only_realtime = raw.get("is_only_realtime")
    else:
        raise ValueError("subscription must be Subscription or dict")

    if not sub_type:
        raise ValueError("subscription.type is required")
    normalized_codes = _normalize_codes(codes)
    if not normalized_codes:
        raise ValueError("subscription.codes is required")

    return Subscription(
        type=sub_type,
        codes=normalized_codes,
        is_only_snapshot=bool(only_snapshot) if only_snapshot is not None else None,
        is_only_realtime=bool(only_realtime) if only_realtime is not None else None,
    )


def _normalize_codes(codes: Iterable[Any]) -> tuple[str, ...]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_code in codes:
        code = str(raw_code).strip().upper()
        if not code or code in seen:
            continue
        seen.add(code)
        normalized.append(code)
    return tuple(normalized)


def _normalize_private_type(raw: str) -> str:
    lowered = str(raw).strip().lower()
    if lowered == "myorder":
        return "myOrder"
    if lowered == "myasset":
        return "myAsset"
    allowed = ", ".join(sorted(VALID_PRIVATE_WS_TYPES))
    raise ValueError(f"private subscription type must be one of: {allowed}")
