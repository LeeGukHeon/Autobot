"""Raw CSV header to canonical contract mapping."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


TIMESTAMP_CANDIDATES: tuple[tuple[str, str], ...] = (
    ("timestamp", "timestamp_ms"),
    ("ts_ms", "timestamp_ms"),
    ("candle_date_time_utc", "utc_string"),
    ("candle_date_time_kst", "kst_string"),
    ("datetime", "datetime_string"),
    ("date", "datetime_string"),
    ("time", "datetime_string"),
)


COLUMN_CANDIDATES: dict[str, tuple[str, ...]] = {
    "open": ("opening_price", "open"),
    "high": ("high_price", "high"),
    "low": ("low_price", "low"),
    "close": ("trade_price", "close"),
    "volume_base": ("candle_acc_trade_volume", "volume", "vol"),
    "volume_quote": ("candle_acc_trade_price", "volume_quote"),
}


@dataclass(frozen=True)
class ColumnMapping:
    ts_source: str
    ts_policy: str
    open_col: str
    high_col: str
    low_col: str
    close_col: str
    volume_base_col: str
    volume_quote_col: str | None

    def as_dict(self) -> dict[str, str | None]:
        return {
            "ts_source": self.ts_source,
            "ts_policy": self.ts_policy,
            "open": self.open_col,
            "high": self.high_col,
            "low": self.low_col,
            "close": self.close_col,
            "volume_base": self.volume_base_col,
            "volume_quote": self.volume_quote_col,
        }


class ColumnMappingError(ValueError):
    """Raised when required columns are missing from a raw CSV header."""

    def __init__(self, message: str, missing_fields: Iterable[str], headers: Iterable[str]) -> None:
        self.missing_fields = tuple(missing_fields)
        self.headers = tuple(headers)
        super().__init__(message)


def detect_column_mapping(headers: Iterable[str]) -> ColumnMapping:
    """Detect canonical mapping from a raw header list."""

    indexed = {header.strip().lower(): header.strip() for header in headers if header.strip()}
    source_headers = list(headers)

    ts_source = None
    ts_policy = None
    for candidate, policy in TIMESTAMP_CANDIDATES:
        if candidate in indexed:
            ts_source = indexed[candidate]
            ts_policy = policy
            break

    missing: list[str] = []
    if ts_source is None or ts_policy is None:
        missing.append("ts_ms")

    selected: dict[str, str | None] = {
        "open": _pick_first(indexed, COLUMN_CANDIDATES["open"]),
        "high": _pick_first(indexed, COLUMN_CANDIDATES["high"]),
        "low": _pick_first(indexed, COLUMN_CANDIDATES["low"]),
        "close": _pick_first(indexed, COLUMN_CANDIDATES["close"]),
        "volume_base": _pick_first(indexed, COLUMN_CANDIDATES["volume_base"]),
        "volume_quote": _pick_first(indexed, COLUMN_CANDIDATES["volume_quote"]),
    }

    for required in ("open", "high", "low", "close", "volume_base"):
        if selected[required] is None:
            missing.append(required)

    if missing:
        raise ColumnMappingError(
            message=f"Required columns missing: {', '.join(missing)}",
            missing_fields=missing,
            headers=source_headers,
        )

    return ColumnMapping(
        ts_source=ts_source,
        ts_policy=ts_policy,
        open_col=str(selected["open"]),
        high_col=str(selected["high"]),
        low_col=str(selected["low"]),
        close_col=str(selected["close"]),
        volume_base_col=str(selected["volume_base"]),
        volume_quote_col=selected["volume_quote"],
    )


def _pick_first(indexed_headers: dict[str, str], candidates: Iterable[str]) -> str | None:
    for candidate in candidates:
        if candidate in indexed_headers:
            return indexed_headers[candidate]
    return None
