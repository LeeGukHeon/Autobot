"""Query-string helpers that preserve order and Upbit hash semantics."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import unquote, urlencode

from .types import ParamScalar, ParamValue, ParamsInput


def build_query_string(params: ParamsInput | None) -> str:
    """Build the canonical query string used for Upbit query hashing."""

    pairs = normalize_param_pairs(params)
    if not pairs:
        return ""
    return unquote(urlencode(pairs, doseq=True))


def normalize_param_pairs(params: ParamsInput | None) -> list[tuple[str, str]]:
    """Flatten mapping or pair-sequence into ordered key/value pairs."""

    if params is None:
        return []

    if isinstance(params, Mapping):
        items: Sequence[tuple[str, ParamValue]] = list(params.items())
    else:
        items = list(params)

    normalized: list[tuple[str, str]] = []
    for key, raw_value in items:
        if raw_value is None:
            continue

        if _is_sequence(raw_value):
            for value in raw_value:
                if value is None:
                    continue
                normalized.append((str(key), _stringify_scalar(value)))
            continue

        normalized.append((str(key), _stringify_scalar(raw_value)))
    return normalized


def _is_sequence(value: Any) -> bool:
    if isinstance(value, (str, bytes, bytearray)):
        return False
    return isinstance(value, Sequence)


def _stringify_scalar(value: ParamScalar) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)
