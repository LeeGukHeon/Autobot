"""Streaming readers and normalizers for raw microstructure sources."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import io
import json
from pathlib import Path
from typing import Any, Iterator

import zstandard as zstd


@dataclass
class ParseCounters:
    raw_rows: int = 0
    parsed_rows: int = 0
    parse_drop: int = 0
    filtered_rows: int = 0

    def add(self, other: "ParseCounters") -> None:
        self.raw_rows += int(other.raw_rows)
        self.parsed_rows += int(other.parsed_rows)
        self.parse_drop += int(other.parse_drop)
        self.filtered_rows += int(other.filtered_rows)

    @property
    def parse_ok_ratio(self) -> float:
        if self.raw_rows <= 0:
            return 1.0
        return float(self.parsed_rows) / float(self.raw_rows)

    def to_dict(self) -> dict[str, Any]:
        return {
            "raw_rows": int(self.raw_rows),
            "parsed_rows": int(self.parsed_rows),
            "parse_drop": int(self.parse_drop),
            "filtered_rows": int(self.filtered_rows),
            "parse_ok_ratio": round(self.parse_ok_ratio, 8),
        }


def iter_jsonl_zst_rows(path: Path) -> Iterator[dict[str, Any]]:
    """Yield JSON objects from a compressed jsonl.zst file without loading it in memory."""
    if not path.exists():
        raise FileNotFoundError(f"part file not found: {path}")

    decompressor = zstd.ZstdDecompressor()
    with path.open("rb") as fp:
        with decompressor.stream_reader(fp) as reader:
            text_reader = io.TextIOWrapper(reader, encoding="utf-8")
            for line_no, line in enumerate(text_reader, start=1):
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    item = json.loads(stripped)
                except json.JSONDecodeError as exc:
                    raise ValueError(f"Invalid JSON at {path}:{line_no}") from exc
                if not isinstance(item, dict):
                    raise ValueError(f"JSON row must be object at {path}:{line_no}")
                yield item


def parse_date_range(*, start: str, end: str) -> tuple[str, ...]:
    start_date = date.fromisoformat(str(start).strip())
    end_date = date.fromisoformat(str(end).strip())
    if end_date < start_date:
        raise ValueError("end must be >= start")
    dates: list[str] = []
    cursor = start_date
    while cursor <= end_date:
        dates.append(cursor.isoformat())
        cursor += timedelta(days=1)
    return tuple(dates)


def discover_rest_tick_files(
    *,
    raw_ticks_root: Path,
    date_value: str,
    markets: set[str] | None = None,
) -> list[Path]:
    date_dir = raw_ticks_root / f"date={date_value}"
    if not date_dir.exists():
        return []

    if markets:
        files: list[Path] = []
        for market in sorted(markets):
            market_dir = date_dir / f"market={market}"
            if not market_dir.exists():
                continue
            files.extend(path for path in market_dir.glob("*.jsonl.zst") if path.is_file())
        return sorted(files)

    return sorted(path for path in date_dir.glob("market=*/*.jsonl.zst") if path.is_file())


def discover_ws_files(
    *,
    raw_ws_root: Path,
    channel: str,
    date_value: str,
) -> list[Path]:
    channel_value = str(channel).strip().lower()
    if channel_value not in {"trade", "orderbook"}:
        raise ValueError(f"unsupported ws channel: {channel}")

    pattern = f"{channel_value}/date={date_value}/hour=*/*.jsonl.zst"
    return sorted(path for path in raw_ws_root.glob(pattern) if path.is_file())


def normalize_rest_trade_row(row: dict[str, Any]) -> dict[str, Any] | None:
    market = _as_str(row.get("market"), upper=True)
    ts_ms = _to_int(row.get("timestamp_ms"))
    price = _to_float(row.get("trade_price"))
    volume = _to_float(row.get("trade_volume"))
    ask_bid = _as_str(row.get("ask_bid"), upper=True)
    if market is None or ts_ms is None or price is None or volume is None:
        return None
    if price <= 0.0 or volume <= 0.0:
        return None
    side = _trade_side_from_ask_bid(ask_bid)
    if side is None:
        return None

    return {
        "market": market,
        "event_ts_ms": int(ts_ms),
        "price": float(price),
        "volume": float(volume),
        "side": side,
        "source": "rest",
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
    if market is None or ts_ms is None or price is None or volume is None:
        return None
    if price <= 0.0 or volume <= 0.0:
        return None
    side = _trade_side_from_ask_bid(ask_bid)
    if side is None:
        return None

    return {
        "market": market,
        "event_ts_ms": int(ts_ms),
        "price": float(price),
        "volume": float(volume),
        "side": side,
        "source": "ws",
    }


def normalize_ws_orderbook_row(row: dict[str, Any], *, topk: int) -> dict[str, Any] | None:
    channel = _as_str(row.get("channel"), upper=False)
    if channel != "orderbook":
        return None

    topk_value = max(int(topk), 1)
    market = _as_str(row.get("market"), upper=True)
    ts_ms = _to_int(row.get("ts_ms"))
    ask1_price = _to_float(row.get("ask1_price"))
    bid1_price = _to_float(row.get("bid1_price"))
    if market is None or ts_ms is None or ask1_price is None or bid1_price is None:
        return None
    if ask1_price <= 0.0 or bid1_price <= 0.0:
        return None

    mid = (float(ask1_price) + float(bid1_price)) / 2.0
    if mid <= 0.0:
        return None

    spread_bps = ((float(ask1_price) - float(bid1_price)) / mid) * 10_000.0

    depth_bid = 0.0
    depth_ask = 0.0
    for level in range(1, topk_value + 1):
        bid_size = max(_to_float(row.get(f"bid{level}_size")) or 0.0, 0.0)
        ask_size = max(_to_float(row.get(f"ask{level}_size")) or 0.0, 0.0)
        depth_bid += bid_size
        depth_ask += ask_size

    imbalance = 0.0
    if depth_bid + depth_ask > 0.0:
        imbalance = (depth_bid - depth_ask) / (depth_bid + depth_ask)

    bid1_size = max(_to_float(row.get("bid1_size")) or 0.0, 0.0)
    ask1_size = max(_to_float(row.get("ask1_size")) or 0.0, 0.0)
    microprice_bias_bps: float | None = None
    if bid1_size + ask1_size > 0.0:
        microprice = ((float(ask1_price) * bid1_size) + (float(bid1_price) * ask1_size)) / (bid1_size + ask1_size)
        microprice_bias_bps = ((microprice - mid) / mid) * 10_000.0

    return {
        "market": market,
        "event_ts_ms": int(ts_ms),
        "mid": float(mid),
        "spread_bps": float(spread_bps),
        "depth_bid_topk": float(depth_bid),
        "depth_ask_topk": float(depth_ask),
        "imbalance_topk": float(imbalance),
        "microprice_bias_bps": (float(microprice_bias_bps) if microprice_bias_bps is not None else None),
        "source": "ws",
    }


def bar_ts_floor(ts_ms: int, *, interval_ms: int) -> int:
    interval = max(int(interval_ms), 1)
    value = int(ts_ms)
    return (value // interval) * interval


def date_utc_from_ts_ms(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc)
    return dt.date().isoformat()


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
