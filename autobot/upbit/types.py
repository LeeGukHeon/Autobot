"""Shared types for Upbit REST integration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence, TypeAlias


ParamScalar: TypeAlias = str | int | float | bool
ParamValue: TypeAlias = ParamScalar | Sequence[ParamScalar]
ParamsInput: TypeAlias = Mapping[str, ParamValue] | Sequence[tuple[str, ParamValue]]
JSONValue: TypeAlias = dict[str, Any] | list[Any] | str | int | float | bool | None


@dataclass(frozen=True)
class RemainingReqInfo:
    """Normalized value for the Upbit `Remaining-Req` header."""

    group: str
    sec: int
    min: int | None = None
    raw: str | None = None
