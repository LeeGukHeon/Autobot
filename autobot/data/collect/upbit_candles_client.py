"""Upbit minute-candle top-up client with paging and retry guards."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
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


PageFetcher = Callable[[str, int, int, str | None], Sequence[dict[str, Any]]]


@dataclass(frozen=True)
class CandleFetchResult:
    market: str
    tf: str
    start_ts_ms: int
    end_ts_ms: int
    candles: tuple[dict[str, Any], ...]
    calls_made: int
    throttled_count: int
    backoff_count: int
    loop_guard_triggered: bool
    truncated_by_budget: bool

    @property
    def min_ts_ms(self) -> int | None:
        if not self.candles:
            return None
        return int(self.candles[0]["ts_ms"])

    @property
    def max_ts_ms(self) -> int | None:
        if not self.candles:
            return None
        return int(self.candles[-1]["ts_ms"])


class UpbitCandlesClient:
    """Minute candle range fetcher using Upbit REST pagination."""

    def __init__(
        self,
        settings: UpbitSettings | None = None,
        *,
        page_fetcher: PageFetcher | None = None,
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

    def fetch_minutes_range(
        self,
        *,
        market: str,
        tf: str,
        start_ts_ms: int,
        end_ts_ms: int,
        max_requests: int | None = None,
    ) -> CandleFetchResult:
        if self._page_fetcher is not None:
            return self._fetch_minutes_range_with_page_fetcher(
                page_fetcher=self._page_fetcher,
                market=market,
                tf=tf,
                start_ts_ms=start_ts_ms,
                end_ts_ms=end_ts_ms,
                max_requests=max_requests,
            )

        assert self._settings is not None
        with UpbitHttpClient(self._settings) as http_client:
            rest_client = UpbitPublicClient(http_client)

            def _rest_fetcher(market_name: str, tf_min: int, count: int, to: str | None) -> Sequence[dict[str, Any]]:
                payload = rest_client.candles_minutes(
                    market=market_name,
                    tf_min=tf_min,
                    count=count,
                    to=to,
                )
                if not isinstance(payload, list):
                    return []
                return [item for item in payload if isinstance(item, dict)]

            return self._fetch_minutes_range_with_page_fetcher(
                page_fetcher=_rest_fetcher,
                market=market,
                tf=tf,
                start_ts_ms=start_ts_ms,
                end_ts_ms=end_ts_ms,
                max_requests=max_requests,
            )

    def _fetch_minutes_range_with_page_fetcher(
        self,
        *,
        page_fetcher: PageFetcher,
        market: str,
        tf: str,
        start_ts_ms: int,
        end_ts_ms: int,
        max_requests: int | None,
    ) -> CandleFetchResult:
        market_value = str(market).strip().upper()
        tf_value = str(tf).strip().lower()
        tf_min = _tf_to_minutes(tf_value)
        if end_ts_ms < start_ts_ms:
            raise ValueError("end_ts_ms must be >= start_ts_ms")

        calls_made = 0
        throttled_count = 0
        backoff_count = 0
        loop_guard_triggered = False
        truncated_by_budget = False

        earliest_seen: int | None = None
        to_ts_ms = int(end_ts_ms)
        selected_rows: dict[int, dict[str, Any]] = {}

        while True:
            if max_requests is not None and calls_made >= max(int(max_requests), 0):
                truncated_by_budget = True
                break

            payload, page_calls, page_throttled, page_backoff = self._fetch_page_with_retry(
                page_fetcher=page_fetcher,
                market=market_value,
                tf_min=tf_min,
                to_ts_ms=to_ts_ms,
                count=200,
            )
            calls_made += page_calls
            throttled_count += page_throttled
            backoff_count += page_backoff

            if not payload:
                break

            parsed = _parse_candle_payload(payload)
            if not parsed:
                break

            parsed.sort(key=lambda row: int(row["ts_ms"]), reverse=True)
            earliest_page_ts = int(parsed[-1]["ts_ms"])
            if earliest_seen is not None and earliest_page_ts >= earliest_seen:
                loop_guard_triggered = True
                break
            earliest_seen = earliest_page_ts

            for row in parsed:
                ts_ms = int(row["ts_ms"])
                if ts_ms < start_ts_ms:
                    continue
                if ts_ms > end_ts_ms:
                    continue
                selected_rows[ts_ms] = row

            if earliest_page_ts <= start_ts_ms:
                break
            next_to_ts_ms = earliest_page_ts - 1
            if next_to_ts_ms >= to_ts_ms:
                loop_guard_triggered = True
                break
            to_ts_ms = next_to_ts_ms

        candles = tuple(sorted(selected_rows.values(), key=lambda row: int(row["ts_ms"])))
        return CandleFetchResult(
            market=market_value,
            tf=tf_value,
            start_ts_ms=int(start_ts_ms),
            end_ts_ms=int(end_ts_ms),
            candles=candles,
            calls_made=calls_made,
            throttled_count=throttled_count,
            backoff_count=backoff_count,
            loop_guard_triggered=loop_guard_triggered,
            truncated_by_budget=truncated_by_budget,
        )

    def _fetch_page_with_retry(
        self,
        *,
        page_fetcher: PageFetcher,
        market: str,
        tf_min: int,
        to_ts_ms: int,
        count: int,
    ) -> tuple[list[dict[str, Any]], int, int, int]:
        page_calls = 0
        throttled_count = 0
        backoff_count = 0

        for attempt in range(1, self._max_retry_attempts + 1):
            page_calls += 1
            to_param = _to_param_from_ts_ms(to_ts_ms)
            try:
                payload = page_fetcher(market, tf_min, count, to_param)
                normalized = [dict(item) for item in payload if isinstance(item, dict)]
                return normalized, page_calls, throttled_count, backoff_count
            except RateLimitError as exc:
                throttled_count += 1
                if attempt >= self._max_retry_attempts:
                    raise
                delay = float(exc.cooldown_sec or 1.0)
                jitter = self._rng.uniform(0.0, 0.25)
                self._sleep(max(delay + jitter, 0.01))
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


def _parse_candle_payload(payload: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in payload:
        ts_ms = _parse_candle_ts_ms(item)
        if ts_ms is None:
            continue
        row = {
            "ts_ms": int(ts_ms),
            "open": _as_float(item.get("opening_price")),
            "high": _as_float(item.get("high_price")),
            "low": _as_float(item.get("low_price")),
            "close": _as_float(item.get("trade_price")),
            "volume_base": _as_float(item.get("candle_acc_trade_volume")),
            "volume_quote": _as_float(item.get("candle_acc_trade_price")),
            "volume_quote_est": False,
        }
        if (
            row["open"] is None
            or row["high"] is None
            or row["low"] is None
            or row["close"] is None
            or row["volume_base"] is None
        ):
            continue
        rows.append(row)
    return rows


def _parse_candle_ts_ms(item: dict[str, Any]) -> int | None:
    utc_text = item.get("candle_date_time_utc")
    if utc_text:
        text = str(utc_text).strip()
        if text:
            normalized = text.replace("Z", "+00:00")
            try:
                parsed = datetime.fromisoformat(normalized)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                else:
                    parsed = parsed.astimezone(timezone.utc)
                return int(parsed.timestamp() * 1000)
            except ValueError:
                pass

    timestamp_value = item.get("timestamp")
    if timestamp_value is not None:
        try:
            raw = int(float(timestamp_value))
        except (TypeError, ValueError):
            return None
        if abs(raw) < 10_000_000_000:
            return raw * 1000
        return raw
    return None


def _to_param_from_ts_ms(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc)
    return dt.isoformat(timespec="seconds")


def _tf_to_minutes(tf: str) -> int:
    text = str(tf).strip().lower()
    if not text.endswith("m"):
        raise ValueError(f"Unsupported timeframe for minute candle endpoint: {tf}")
    try:
        return int(text[:-1])
    except ValueError as exc:
        raise ValueError(f"Unsupported timeframe for minute candle endpoint: {tf}") from exc


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
