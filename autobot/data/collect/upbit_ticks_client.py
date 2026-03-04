"""Upbit trades/ticks REST client with pagination and retry guards."""

from __future__ import annotations

from dataclasses import dataclass
import random
import time
from typing import Any, Callable, Sequence

from ...upbit import (
    NetworkError,
    RateLimitError,
    ServerError,
    UpbitHttpClient,
    UpbitPublicClient,
    UpbitSettings,
)


TickPageFetcher = Callable[[str, int, int, str | None], Sequence[dict[str, Any]]]


@dataclass(frozen=True)
class TicksFetchResult:
    market: str
    days_ago: int
    ticks: tuple[dict[str, Any], ...]
    calls_made: int
    throttled_count: int
    backoff_count: int
    pages_collected: int
    loop_guard_triggered: bool
    truncated_by_budget: bool
    start_cursor: str | None
    last_cursor: str | None
    raw_rows: int
    unique_rows: int

    @property
    def min_ts_ms(self) -> int | None:
        if not self.ticks:
            return None
        return int(self.ticks[0]["timestamp_ms"])

    @property
    def max_ts_ms(self) -> int | None:
        if not self.ticks:
            return None
        return int(self.ticks[-1]["timestamp_ms"])


class UpbitTicksClient:
    """Trades/ticks range fetcher for `GET /v1/trades/ticks`."""

    def __init__(
        self,
        settings: UpbitSettings | None = None,
        *,
        page_fetcher: TickPageFetcher | None = None,
        max_retry_attempts: int = 3,
        base_backoff_sec: float = 0.3,
        max_backoff_sec: float = 8.0,
        sleep_fn: Callable[[float], None] | None = None,
        rng: random.Random | None = None,
    ) -> None:
        if settings is None and page_fetcher is None:
            raise ValueError("settings is required when page_fetcher is not provided")
        self._settings = settings
        self._page_fetcher = page_fetcher
        self._max_retry_attempts = max(int(max_retry_attempts), 1)
        self._base_backoff_sec = max(float(base_backoff_sec), 0.01)
        self._max_backoff_sec = max(float(max_backoff_sec), self._base_backoff_sec)
        self._sleep = sleep_fn or time.sleep
        self._rng = rng or random.Random()

    def fetch_trades_ticks(
        self,
        *,
        market: str,
        days_ago: int,
        start_cursor: str | None = None,
        max_pages: int | None = None,
        max_requests: int | None = None,
        count: int = 200,
    ) -> TicksFetchResult:
        if self._page_fetcher is not None:
            return self._fetch_with_page_fetcher(
                page_fetcher=self._page_fetcher,
                market=market,
                days_ago=days_ago,
                start_cursor=start_cursor,
                max_pages=max_pages,
                max_requests=max_requests,
                count=count,
            )

        assert self._settings is not None
        with UpbitHttpClient(self._settings) as http_client:
            rest_client = UpbitPublicClient(http_client)

            def _rest_fetcher(
                market_name: str,
                day: int,
                request_count: int,
                cursor: str | None,
            ) -> Sequence[dict[str, Any]]:
                payload = rest_client.trades_ticks(
                    market=market_name,
                    count=request_count,
                    cursor=cursor,
                    days_ago=day,
                )
                if not isinstance(payload, list):
                    return []
                return [item for item in payload if isinstance(item, dict)]

            return self._fetch_with_page_fetcher(
                page_fetcher=_rest_fetcher,
                market=market,
                days_ago=days_ago,
                start_cursor=start_cursor,
                max_pages=max_pages,
                max_requests=max_requests,
                count=count,
            )

    def _fetch_with_page_fetcher(
        self,
        *,
        page_fetcher: TickPageFetcher,
        market: str,
        days_ago: int,
        start_cursor: str | None,
        max_pages: int | None,
        max_requests: int | None,
        count: int,
    ) -> TicksFetchResult:
        market_value = str(market).strip().upper()
        days_ago_value = int(days_ago)
        if not market_value:
            raise ValueError("market is required")
        if days_ago_value < 1 or days_ago_value > 7:
            raise ValueError("days_ago must be between 1 and 7")

        count_value = max(min(int(count), 200), 1)
        cursor = str(start_cursor).strip() if start_cursor else None

        calls_made = 0
        throttled_count = 0
        backoff_count = 0
        pages_collected = 0
        loop_guard_triggered = False
        truncated_by_budget = False
        raw_rows = 0

        previous_min_seq: int | None = None
        selected_rows: dict[int, dict[str, Any]] = {}

        while True:
            if max_pages is not None and pages_collected >= max(int(max_pages), 0):
                truncated_by_budget = True
                break
            if max_requests is not None and calls_made >= max(int(max_requests), 0):
                truncated_by_budget = True
                break

            payload, page_calls, page_throttled, page_backoff = self._fetch_page_with_retry(
                page_fetcher=page_fetcher,
                market=market_value,
                days_ago=days_ago_value,
                count=count_value,
                cursor=cursor,
            )
            calls_made += page_calls
            throttled_count += page_throttled
            backoff_count += page_backoff

            if not payload:
                break

            collected_at_ms = int(time.time() * 1000)
            parsed = _parse_ticks_payload(
                payload,
                market=market_value,
                days_ago=days_ago_value,
                collected_at_ms=collected_at_ms,
            )
            if not parsed:
                break

            pages_collected += 1
            raw_rows += len(parsed)
            min_seq = min(int(row["sequential_id"]) for row in parsed)
            if previous_min_seq is not None and min_seq >= previous_min_seq:
                loop_guard_triggered = True
                break

            for row in parsed:
                selected_rows[int(row["sequential_id"])] = row

            next_cursor = str(min_seq)
            if cursor is not None and next_cursor == cursor:
                loop_guard_triggered = True
                break

            previous_min_seq = min_seq
            cursor = next_cursor

        ticks = tuple(sorted(selected_rows.values(), key=lambda row: (int(row["timestamp_ms"]), int(row["sequential_id"]))))
        return TicksFetchResult(
            market=market_value,
            days_ago=days_ago_value,
            ticks=ticks,
            calls_made=calls_made,
            throttled_count=throttled_count,
            backoff_count=backoff_count,
            pages_collected=pages_collected,
            loop_guard_triggered=loop_guard_triggered,
            truncated_by_budget=truncated_by_budget,
            start_cursor=start_cursor,
            last_cursor=cursor,
            raw_rows=raw_rows,
            unique_rows=len(selected_rows),
        )

    def _fetch_page_with_retry(
        self,
        *,
        page_fetcher: TickPageFetcher,
        market: str,
        days_ago: int,
        count: int,
        cursor: str | None,
    ) -> tuple[list[dict[str, Any]], int, int, int]:
        page_calls = 0
        throttled_count = 0
        backoff_count = 0

        for attempt in range(1, self._max_retry_attempts + 1):
            page_calls += 1
            try:
                payload = page_fetcher(market, days_ago, count, cursor)
                normalized = [dict(item) for item in payload if isinstance(item, dict)]
                return normalized, page_calls, throttled_count, backoff_count
            except RateLimitError as exc:
                throttled_count += 1
                if exc.banned:
                    raise
                if attempt >= self._max_retry_attempts:
                    raise
                cooldown = float(exc.cooldown_sec or self._retry_delay_sec(attempt))
                jitter = self._rng.uniform(0.0, min(cooldown * 0.25, 0.5))
                self._sleep(max(cooldown + jitter, 0.01))
                backoff_count += 1
            except (NetworkError, ServerError):
                if attempt >= self._max_retry_attempts:
                    raise
                self._sleep(self._retry_delay_sec(attempt))
                backoff_count += 1

        return [], page_calls, throttled_count, backoff_count

    def _retry_delay_sec(self, attempt: int) -> float:
        delay = min(self._base_backoff_sec * (2 ** max(attempt - 1, 0)), self._max_backoff_sec)
        jitter = self._rng.uniform(0.0, delay * 0.2)
        return max(delay + jitter, 0.01)


def _parse_ticks_payload(
    payload: Sequence[dict[str, Any]],
    *,
    market: str,
    days_ago: int,
    collected_at_ms: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in payload:
        market_value = _as_str(item.get("market"), upper=True) or market
        ts_ms = _as_int(item.get("timestamp"))
        if ts_ms is None:
            continue
        if abs(int(ts_ms)) < 10_000_000_000:
            ts_ms = int(ts_ms) * 1000
        trade_price = _as_float(item.get("trade_price"))
        trade_volume = _as_float(item.get("trade_volume"))
        ask_bid = _as_str(item.get("ask_bid"), upper=True)
        sequential_id = _as_int(item.get("sequential_id"))
        if trade_price is None or trade_volume is None or sequential_id is None:
            continue
        if ask_bid not in {"ASK", "BID"}:
            continue

        rows.append(
            {
                "market": market_value,
                "timestamp_ms": int(ts_ms),
                "trade_price": float(trade_price),
                "trade_volume": float(trade_volume),
                "ask_bid": ask_bid,
                "sequential_id": int(sequential_id),
                "days_ago": int(days_ago),
                "collected_at_ms": int(collected_at_ms),
            }
        )
    return rows


def _as_int(value: Any) -> int | None:
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


def _as_float(value: Any) -> float | None:
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
    if upper:
        return text.upper()
    return text
