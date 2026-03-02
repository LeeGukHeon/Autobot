"""Parser for Upbit `Remaining-Req` response header."""

from __future__ import annotations

from .types import RemainingReqInfo


def parse_remaining_req_header(value: str | None) -> RemainingReqInfo | None:
    if value is None:
        return None

    raw = value.strip()
    if not raw:
        return None

    parsed: dict[str, str] = {}
    for token in raw.split(";"):
        if "=" not in token:
            continue
        key, token_value = token.split("=", 1)
        parsed[key.strip().lower()] = token_value.strip()

    group = parsed.get("group", "default").strip() or "default"
    sec_str = parsed.get("sec")
    if sec_str is None:
        return None

    try:
        sec = int(sec_str)
    except ValueError:
        return None

    min_value: int | None = None
    if "min" in parsed:
        try:
            min_value = int(parsed["min"])
        except ValueError:
            min_value = None

    return RemainingReqInfo(group=group, sec=sec, min=min_value, raw=raw)
