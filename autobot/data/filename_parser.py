"""Filename parser for raw Upbit candle CSVs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Iterable


UPBIT_FILENAME_REGEX = re.compile(
    r"^upbit_(?P<quote>[A-Za-z0-9]+)_(?P<symbol>[A-Za-z0-9]+)_(?P<tf>[0-9]+m)_full\.csv$"
)


@dataclass(frozen=True)
class ParsedFilename:
    quote: str
    symbol: str
    tf: str
    market: str


class FilenameParseError(ValueError):
    """Raised when parsing a source CSV filename fails."""

    def __init__(self, code: str, filename: str, detail: str) -> None:
        self.code = code
        self.filename = filename
        self.detail = detail
        super().__init__(f"[{code}] {filename}: {detail}")


def parse_upbit_filename(path: str | Path, supported_tfs: Iterable[str] | None = None) -> dict[str, str]:
    """Parse filename into quote/symbol/tf/market."""

    filename = Path(path).name
    match = UPBIT_FILENAME_REGEX.match(filename)
    if not match:
        raise FilenameParseError(
            code="PATTERN_MISMATCH",
            filename=filename,
            detail="Expected format upbit_{QUOTE}_{SYMBOL}_{TF}_full.csv",
        )

    quote = match.group("quote").upper()
    symbol = match.group("symbol").upper()
    tf = match.group("tf").lower()

    if supported_tfs is not None:
        normalized_tfs = {item.lower().strip() for item in supported_tfs}
        if tf not in normalized_tfs:
            raise FilenameParseError(
                code="UNSUPPORTED_TF",
                filename=filename,
                detail=f"Timeframe '{tf}' is not enabled by config",
            )

    return {
        "quote": quote,
        "symbol": symbol,
        "tf": tf,
        "market": f"{quote}-{symbol}",
    }
