"""Micro snapshot providers used by MicroGate risk filtering."""

from __future__ import annotations

from bisect import bisect_right
from collections import OrderedDict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import time
from typing import Any, Protocol, Sequence

import polars as pl

from autobot.upbit.ws.ws_rate_limiter import WebSocketRateLimiter


@dataclass(frozen=True)
class MicroSnapshot:
    market: str
    snapshot_ts_ms: int
    last_event_ts_ms: int
    trade_events: int = 0
    trade_coverage_ms: int = 0
    trade_notional_krw: float = 0.0
    trade_imbalance: float | None = None
    trade_source: str = "none"
    spread_bps_mean: float | None = None
    depth_top5_notional_krw: float | None = None
    depth_bid_top5_notional_krw: float | None = None
    depth_ask_top5_notional_krw: float | None = None
    book_events: int = 0
    book_coverage_ms: int = 0
    book_available: bool = False


class MicroSnapshotProvider(Protocol):
    def get(self, market: str, ts_ms: int) -> MicroSnapshot | None: ...


@dataclass(frozen=True)
class LiveWsProviderSettings:
    enabled: bool = False
    window_sec: int = 60
    orderbook_topk: int = 5
    orderbook_level: int | str | None = 0
    subscribe_format: str = "DEFAULT"
    max_markets: int = 30
    reconnect_max_per_min: int = 3
    backoff_base_sec: float = 1.0
    backoff_max_sec: float = 32.0
    connect_rps: int = 5
    message_rps: int = 5
    message_rpm: int = 100
    max_subscribe_messages_per_min: int = 100


class LiveWsRateLimitGuard:
    """Simple fixed-window guard for reconnect/subscribe events."""

    def __init__(self, *, max_subscribe_messages_per_min: int, max_reconnect_per_min: int) -> None:
        self._max_subscribe_per_min = max(int(max_subscribe_messages_per_min), 1)
        self._max_reconnect_per_min = max(int(max_reconnect_per_min), 1)
        self._subscribe_window: deque[float] = deque()
        self._reconnect_window: deque[float] = deque()

    def allow_subscribe(self, *, now_monotonic: float | None = None) -> bool:
        now = float(now_monotonic) if now_monotonic is not None else time.monotonic()
        self._trim(self._subscribe_window, now)
        if len(self._subscribe_window) >= self._max_subscribe_per_min:
            return False
        self._subscribe_window.append(now)
        return True

    def allow_reconnect(self, *, now_monotonic: float | None = None) -> bool:
        now = float(now_monotonic) if now_monotonic is not None else time.monotonic()
        self._trim(self._reconnect_window, now)
        if len(self._reconnect_window) >= self._max_reconnect_per_min:
            return False
        self._reconnect_window.append(now)
        return True

    @staticmethod
    def _trim(window: deque[float], now: float) -> None:
        cutoff = now - 60.0
        while window and window[0] <= cutoff:
            window.popleft()


@dataclass(frozen=True)
class _OfflineDayCacheEntry:
    ts_values: tuple[int, ...]
    snapshots: dict[int, MicroSnapshot]


class OfflineMicroSnapshotProvider:
    """Parquet-backed micro snapshot provider for backtest/offline runs."""

    def __init__(
        self,
        *,
        micro_root: str | Path,
        tf: str,
        cache_entries: int = 64,
    ) -> None:
        self._micro_root = Path(micro_root)
        self._tf = str(tf).strip().lower()
        self._cache_entries = max(int(cache_entries), 1)
        self._fallback_tolerance_ms = max(_interval_ms_from_tf(self._tf), 1)
        self._cache: OrderedDict[tuple[str, str], _OfflineDayCacheEntry] = OrderedDict()

    def get(self, market: str, ts_ms: int) -> MicroSnapshot | None:
        market_value = str(market).strip().upper()
        if not market_value:
            return None
        ts_value = int(ts_ms)
        date_value = datetime.fromtimestamp(ts_value / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")
        cache_key = (market_value, date_value)

        day_entry = self._cache.get(cache_key)
        if day_entry is None:
            day_entry = self._load_day_entry(market=market_value, date_value=date_value)
            self._cache[cache_key] = day_entry
            while len(self._cache) > self._cache_entries:
                self._cache.popitem(last=False)
        else:
            self._cache.move_to_end(cache_key)

        if not day_entry.ts_values:
            return None
        direct = day_entry.snapshots.get(ts_value)
        if direct is not None:
            return direct

        idx = bisect_right(day_entry.ts_values, ts_value) - 1
        if idx < 0:
            return None
        snapped_ts = int(day_entry.ts_values[idx])
        if (ts_value - snapped_ts) > self._fallback_tolerance_ms:
            return None
        return day_entry.snapshots.get(snapped_ts)

    def _load_day_entry(self, *, market: str, date_value: str) -> _OfflineDayCacheEntry:
        day_dir = self._micro_root / f"tf={self._tf}" / f"market={market}" / f"date={date_value}"
        if not day_dir.exists():
            return _OfflineDayCacheEntry(ts_values=(), snapshots={})

        files = sorted(path for path in day_dir.glob("*.parquet") if path.is_file())
        if not files:
            return _OfflineDayCacheEntry(ts_values=(), snapshots={})

        lazy = pl.scan_parquet([str(path) for path in files])
        schema_names = lazy.collect_schema().names()
        wanted = [name for name in _OFFLINE_WANTED_COLUMNS if name in schema_names]
        if "ts_ms" not in wanted:
            return _OfflineDayCacheEntry(ts_values=(), snapshots={})

        frame = _collect_lazy(lazy.select(wanted).sort("ts_ms")).unique(
            subset=["ts_ms"],
            keep="last",
            maintain_order=True,
        )
        if frame.height <= 0:
            return _OfflineDayCacheEntry(ts_values=(), snapshots={})

        snapshots: dict[int, MicroSnapshot] = {}
        ts_values: list[int] = []
        for row in frame.iter_rows(named=True):
            ts_value = _to_int(row.get("ts_ms"))
            if ts_value is None:
                continue
            snapshot = _snapshot_from_row(market=market, row=row)
            snapshots[int(ts_value)] = snapshot
            ts_values.append(int(ts_value))
        ts_values.sort()
        return _OfflineDayCacheEntry(ts_values=tuple(ts_values), snapshots=snapshots)


class LiveWsMicroSnapshotProvider:
    """In-memory rolling micro snapshot provider for live runtime."""

    def __init__(self, settings: LiveWsProviderSettings | None = None) -> None:
        self.settings = settings or LiveWsProviderSettings()
        self._window_ms = max(int(self.settings.window_sec), 1) * 1000
        self._orderbook_topk = max(int(self.settings.orderbook_topk), 1)
        self._tracked_markets: tuple[str, ...] = ()

        self._trade_events: dict[str, deque[dict[str, Any]]] = {}
        self._book_events: dict[str, deque[dict[str, Any]]] = {}

        self._connect_limiter = WebSocketRateLimiter(per_second=max(int(self.settings.connect_rps), 1))
        self._message_limiter = WebSocketRateLimiter(
            per_second=max(int(self.settings.message_rps), 1),
            per_minute=max(int(self.settings.message_rpm), 1),
        )
        self._guard = LiveWsRateLimitGuard(
            max_subscribe_messages_per_min=int(self.settings.max_subscribe_messages_per_min),
            max_reconnect_per_min=int(self.settings.reconnect_max_per_min),
        )

    async def acquire_connect_slot(self) -> None:
        await self._connect_limiter.acquire()

    async def acquire_message_slot(self) -> None:
        await self._message_limiter.acquire()

    def allow_subscribe_now(self, *, now_monotonic: float | None = None) -> bool:
        return self._guard.allow_subscribe(now_monotonic=now_monotonic)

    def allow_reconnect_now(self, *, now_monotonic: float | None = None) -> bool:
        return self._guard.allow_reconnect(now_monotonic=now_monotonic)

    def track_markets(self, markets: Sequence[str], *, now_monotonic: float | None = None) -> bool:
        normalized = _normalize_markets(markets, max_markets=self.settings.max_markets)
        if normalized == self._tracked_markets:
            return True
        if not self._guard.allow_subscribe(now_monotonic=now_monotonic):
            return False
        self._tracked_markets = normalized
        return True

    def ingest_trade(self, record: dict[str, Any]) -> None:
        market = str(record.get("market", "")).strip().upper()
        if not market:
            return
        ts_ms = _to_int(record.get("trade_ts_ms")) or _to_int(record.get("ts_ms"))
        price = _to_float(record.get("price") or record.get("trade_price"))
        volume = _to_float(record.get("volume") or record.get("trade_volume"))
        if ts_ms is None or price is None or volume is None:
            return
        event = {
            "ts_ms": int(ts_ms),
            "price": float(price),
            "volume": float(volume),
            "ask_bid": str(record.get("ask_bid", "")).strip().upper(),
        }
        queue = self._trade_events.setdefault(market, deque())
        queue.append(event)
        self._trim_queue(queue=queue, now_ts_ms=int(ts_ms))

    def ingest_orderbook(self, record: dict[str, Any]) -> None:
        market = str(record.get("market", "")).strip().upper()
        if not market:
            return
        ts_ms = _to_int(record.get("ts_ms"))
        if ts_ms is None:
            return
        event: dict[str, Any] = {"ts_ms": int(ts_ms)}
        for idx in range(1, self._orderbook_topk + 1):
            event[f"ask{idx}_price"] = _to_float(record.get(f"ask{idx}_price"))
            event[f"ask{idx}_size"] = _to_float(record.get(f"ask{idx}_size"))
            event[f"bid{idx}_price"] = _to_float(record.get(f"bid{idx}_price"))
            event[f"bid{idx}_size"] = _to_float(record.get(f"bid{idx}_size"))
        queue = self._book_events.setdefault(market, deque())
        queue.append(event)
        self._trim_queue(queue=queue, now_ts_ms=int(ts_ms))

    def get(self, market: str, ts_ms: int) -> MicroSnapshot | None:
        market_value = str(market).strip().upper()
        if not market_value:
            return None
        ts_value = int(ts_ms)
        lower_ts = ts_value - self._window_ms

        trades = [
            event
            for event in self._trade_events.get(market_value, deque())
            if lower_ts <= int(event.get("ts_ms", 0)) <= ts_value
        ]
        books = [
            event
            for event in self._book_events.get(market_value, deque())
            if lower_ts <= int(event.get("ts_ms", 0)) <= ts_value
        ]

        if not trades and not books:
            return None

        trade_events = len(trades)
        trade_min_ts = min((int(event["ts_ms"]) for event in trades), default=0)
        trade_max_ts = max((int(event["ts_ms"]) for event in trades), default=0)
        trade_coverage_ms = max(trade_max_ts - trade_min_ts, 0) if trade_events > 0 else 0
        trade_notional_krw = sum(float(event["price"]) * float(event["volume"]) for event in trades)
        bid_volume = sum(float(event["volume"]) for event in trades if str(event.get("ask_bid")) == "BID")
        ask_volume = sum(float(event["volume"]) for event in trades if str(event.get("ask_bid")) == "ASK")
        total_volume = bid_volume + ask_volume
        trade_imbalance = ((bid_volume - ask_volume) / total_volume) if total_volume > 0 else None

        book_events = len(books)
        book_min_ts = min((int(event["ts_ms"]) for event in books), default=0)
        book_max_ts = max((int(event["ts_ms"]) for event in books), default=0)
        book_coverage_ms = max(book_max_ts - book_min_ts, 0) if book_events > 0 else 0

        spreads = [_spread_bps(event) for event in books]
        spread_values = [value for value in spreads if value is not None]
        spread_mean = (sum(spread_values) / float(len(spread_values))) if spread_values else None

        bid_depths = [_depth_topk_notional_for_side(event, topk=self._orderbook_topk, side="bid") for event in books]
        ask_depths = [_depth_topk_notional_for_side(event, topk=self._orderbook_topk, side="ask") for event in books]
        bid_depth_values = [value for value in bid_depths if value is not None]
        ask_depth_values = [value for value in ask_depths if value is not None]
        bid_depth_mean = (sum(bid_depth_values) / float(len(bid_depth_values))) if bid_depth_values else None
        ask_depth_mean = (sum(ask_depth_values) / float(len(ask_depth_values))) if ask_depth_values else None
        depth_values = [value for value in (bid_depth_mean, ask_depth_mean) if value is not None]
        depth_mean = (sum(depth_values) / float(len(depth_values))) if depth_values else None

        last_event_ts_ms = max(trade_max_ts, book_max_ts)
        return MicroSnapshot(
            market=market_value,
            snapshot_ts_ms=ts_value,
            last_event_ts_ms=int(last_event_ts_ms),
            trade_events=int(trade_events),
            trade_coverage_ms=int(trade_coverage_ms),
            trade_notional_krw=float(trade_notional_krw),
            trade_imbalance=trade_imbalance,
            trade_source="ws" if trade_events > 0 else "none",
            spread_bps_mean=spread_mean,
            depth_top5_notional_krw=depth_mean,
            depth_bid_top5_notional_krw=bid_depth_mean,
            depth_ask_top5_notional_krw=ask_depth_mean,
            book_events=int(book_events),
            book_coverage_ms=int(book_coverage_ms),
            book_available=bool(book_events > 0),
        )

    def _trim_queue(self, *, queue: deque[dict[str, Any]], now_ts_ms: int) -> None:
        cutoff = int(now_ts_ms) - self._window_ms
        while queue and int(queue[0].get("ts_ms", 0)) < cutoff:
            queue.popleft()


_OFFLINE_WANTED_COLUMNS: tuple[str, ...] = (
    "ts_ms",
    "trade_source",
    "trade_events",
    "trade_coverage_ms",
    "trade_count",
    "trade_volume_total",
    "trade_imbalance",
    "vwap",
    "last_trade_price",
    "trade_max_ts_ms",
    "book_events",
    "book_update_count",
    "book_coverage_ms",
    "book_max_ts_ms",
    "micro_book_available",
    "spread_bps_mean",
    "depth_bid_top5_mean",
    "depth_ask_top5_mean",
)


def _snapshot_from_row(*, market: str, row: dict[str, Any]) -> MicroSnapshot:
    ts_ms = _to_int(row.get("ts_ms")) or 0
    trade_events = _to_int(row.get("trade_events"))
    if trade_events is None:
        trade_events = _to_int(row.get("trade_count")) or 0
    trade_coverage_ms = _to_int(row.get("trade_coverage_ms")) or 0

    trade_volume_total = _to_float(row.get("trade_volume_total")) or 0.0
    vwap = _to_float(row.get("vwap"))
    last_trade_price = _to_float(row.get("last_trade_price"))
    if vwap is not None and vwap > 0:
        trade_notional_krw = trade_volume_total * vwap
    elif last_trade_price is not None and last_trade_price > 0:
        trade_notional_krw = trade_volume_total * last_trade_price
    else:
        trade_notional_krw = 0.0

    depth_bid = _to_float(row.get("depth_bid_top5_mean"))
    depth_ask = _to_float(row.get("depth_ask_top5_mean"))
    depth_values = [value for value in (depth_bid, depth_ask) if value is not None]
    depth_top5_notional_krw = sum(depth_values) if depth_values else None

    book_events = _to_int(row.get("book_events"))
    if book_events is None:
        book_events = _to_int(row.get("book_update_count")) or 0
    book_coverage_ms = _to_int(row.get("book_coverage_ms")) or 0
    book_available = bool(row.get("micro_book_available")) or int(book_events) > 0

    trade_max_ts = _to_int(row.get("trade_max_ts_ms")) or 0
    book_max_ts = _to_int(row.get("book_max_ts_ms")) or 0
    last_event_ts = max(trade_max_ts, book_max_ts)
    if last_event_ts <= 0:
        last_event_ts = int(ts_ms)
    return MicroSnapshot(
        market=market,
        snapshot_ts_ms=int(ts_ms),
        last_event_ts_ms=int(last_event_ts),
        trade_events=int(trade_events),
        trade_coverage_ms=int(trade_coverage_ms),
        trade_notional_krw=float(trade_notional_krw),
        trade_imbalance=_to_float(row.get("trade_imbalance")),
        trade_source=str(row.get("trade_source") or "none").strip().lower() or "none",
        spread_bps_mean=_to_float(row.get("spread_bps_mean")),
        depth_top5_notional_krw=depth_top5_notional_krw,
        depth_bid_top5_notional_krw=depth_bid,
        depth_ask_top5_notional_krw=depth_ask,
        book_events=int(book_events),
        book_coverage_ms=int(book_coverage_ms),
        book_available=bool(book_available),
    )


def _normalize_markets(markets: Sequence[str], *, max_markets: int) -> tuple[str, ...]:
    seen: set[str] = set()
    normalized: list[str] = []
    for raw in markets:
        market = str(raw).strip().upper()
        if not market or market in seen:
            continue
        seen.add(market)
        normalized.append(market)
        if len(normalized) >= max(int(max_markets), 1):
            break
    return tuple(normalized)


def _spread_bps(event: dict[str, Any]) -> float | None:
    bid = _to_float(event.get("bid1_price"))
    ask = _to_float(event.get("ask1_price"))
    if bid is None or ask is None:
        return None
    mid = (bid + ask) / 2.0
    if mid <= 0:
        return None
    return ((ask - bid) / mid) * 10_000.0


def _depth_topk_notional(event: dict[str, Any], *, topk: int) -> float | None:
    total = 0.0
    used = 0
    for idx in range(1, max(int(topk), 1) + 1):
        ask_p = _to_float(event.get(f"ask{idx}_price"))
        ask_s = _to_float(event.get(f"ask{idx}_size"))
        bid_p = _to_float(event.get(f"bid{idx}_price"))
        bid_s = _to_float(event.get(f"bid{idx}_size"))
        if ask_p is not None and ask_s is not None:
            total += ask_p * ask_s
            used += 1
        if bid_p is not None and bid_s is not None:
            total += bid_p * bid_s
            used += 1
    if used <= 0:
        return None
    return total


def _depth_topk_notional_for_side(event: dict[str, Any], *, topk: int, side: str) -> float | None:
    total = 0.0
    used = 0
    side_value = str(side).strip().lower()
    for idx in range(1, max(int(topk), 1) + 1):
        if side_value == "bid":
            price = _to_float(event.get(f"bid{idx}_price"))
            size = _to_float(event.get(f"bid{idx}_size"))
        else:
            price = _to_float(event.get(f"ask{idx}_price"))
            size = _to_float(event.get(f"ask{idx}_size"))
        if price is not None and size is not None:
            total += price * size
            used += 1
    if used <= 0:
        return None
    return total


def _collect_lazy(lazy_frame: pl.LazyFrame) -> pl.DataFrame:
    try:
        return lazy_frame.collect(engine="streaming")
    except TypeError:
        return lazy_frame.collect(streaming=True)


def _interval_ms_from_tf(tf: str) -> int:
    value = str(tf).strip().lower()
    if value.endswith("m"):
        try:
            return max(int(value[:-1]), 1) * 60_000
        except ValueError:
            return 300_000
    if value.endswith("h"):
        try:
            return max(int(value[:-1]), 1) * 3_600_000
        except ValueError:
            return 300_000
    return 300_000


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
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            return float(text)
        return float(value)
    except (TypeError, ValueError):
        return None
