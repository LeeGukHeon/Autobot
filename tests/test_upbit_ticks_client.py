from __future__ import annotations

import random

from autobot.data.collect.upbit_ticks_client import UpbitTicksClient
from autobot.upbit.exceptions import RateLimitError


def test_fetch_trades_ticks_breaks_on_repeated_min_seq() -> None:
    calls = {"count": 0}
    page_1 = [
        _row(seq=300, ts_ms=1_700_000_010_000, price=100.0),
        _row(seq=299, ts_ms=1_700_000_009_000, price=101.0),
    ]
    page_2 = [
        _row(seq=350, ts_ms=1_700_000_008_500, price=99.0),
        _row(seq=299, ts_ms=1_700_000_008_000, price=98.0),
    ]

    def fetcher(market: str, days_ago: int, count: int, cursor: str | None) -> list[dict]:
        calls["count"] += 1
        if calls["count"] == 1:
            return page_1
        if calls["count"] == 2:
            return page_2
        return []

    client = UpbitTicksClient(page_fetcher=fetcher)
    result = client.fetch_trades_ticks(
        market="KRW-BTC",
        days_ago=1,
    )

    assert result.calls_made == 2
    assert result.loop_guard_triggered is True
    assert result.unique_rows >= 2


def test_fetch_trades_ticks_retries_on_rate_limit() -> None:
    calls = {"count": 0}
    slept: list[float] = []

    def fetcher(market: str, days_ago: int, count: int, cursor: str | None) -> list[dict]:
        calls["count"] += 1
        if calls["count"] == 1:
            raise RateLimitError("rate limit", cooldown_sec=0.01)
        return [_row(seq=200, ts_ms=1_700_000_020_000, price=102.0)]

    client = UpbitTicksClient(
        page_fetcher=fetcher,
        max_retry_attempts=2,
        sleep_fn=lambda value: slept.append(value),
        rng=random.Random(0),
    )
    result = client.fetch_trades_ticks(
        market="KRW-BTC",
        days_ago=1,
        max_pages=1,
    )

    assert result.calls_made == 2
    assert result.throttled_count == 1
    assert result.backoff_count == 1
    assert len(slept) == 1
    assert result.unique_rows == 1


def _row(*, seq: int, ts_ms: int, price: float) -> dict:
    return {
        "market": "KRW-BTC",
        "timestamp": ts_ms,
        "trade_price": price,
        "trade_volume": 0.1,
        "ask_bid": "BID",
        "sequential_id": seq,
    }
