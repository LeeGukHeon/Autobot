from __future__ import annotations

from autobot.data.collect.upbit_candles_client import UpbitCandlesClient
from autobot.data.inventory import parse_utc_ts_ms


def test_fetch_minutes_range_breaks_on_repeated_earliest_page_ts() -> None:
    calls = {"count": 0}
    page_1 = [
        _row("2026-03-01T00:03:00+00:00", 101.0),
        _row("2026-03-01T00:02:00+00:00", 100.0),
    ]
    page_2 = [
        _row("2026-03-01T00:03:00+00:00", 100.5),
        _row("2026-03-01T00:02:00+00:00", 99.5),
    ]

    def fetcher(market: str, tf: str, count: int, to: str | None) -> list[dict]:
        calls["count"] += 1
        assert tf == "1m"
        if calls["count"] == 1:
            return page_1
        if calls["count"] == 2:
            return page_2
        return []

    start_ts_ms = parse_utc_ts_ms("2026-03-01T00:00:00+00:00")
    end_ts_ms = parse_utc_ts_ms("2026-03-01T00:03:00+00:00")
    assert start_ts_ms is not None
    assert end_ts_ms is not None

    client = UpbitCandlesClient(page_fetcher=fetcher)
    result = client.fetch_minutes_range(
        market="KRW-BTC",
        tf="1m",
        start_ts_ms=start_ts_ms,
        end_ts_ms=end_ts_ms,
    )
    assert result.calls_made == 2
    assert result.loop_guard_triggered is True
    assert len(result.candles) >= 2


def test_fetch_candles_range_supports_second_candles() -> None:
    calls: list[tuple[str, str, int, str | None]] = []

    def fetcher(market: str, tf: str, count: int, to: str | None) -> list[dict]:
        calls.append((market, tf, count, to))
        return [
            _row("2026-03-27T00:00:01+00:00", 101.0),
            _row("2026-03-27T00:00:00+00:00", 100.0),
        ]

    start_ts_ms = parse_utc_ts_ms("2026-03-27T00:00:00+00:00")
    end_ts_ms = parse_utc_ts_ms("2026-03-27T00:00:01+00:00")
    assert start_ts_ms is not None
    assert end_ts_ms is not None

    client = UpbitCandlesClient(page_fetcher=fetcher)
    result = client.fetch_candles_range(
        market="KRW-BTC",
        tf="1s",
        start_ts_ms=start_ts_ms,
        end_ts_ms=end_ts_ms,
    )

    assert len(calls) == 1
    assert calls[0][1] == "1s"
    assert result.tf == "1s"
    assert result.calls_made == 1
    assert [int(row["ts_ms"]) for row in result.candles] == [start_ts_ms, end_ts_ms]


def _row(candle_utc: str, price: float) -> dict:
    return {
        "candle_date_time_utc": candle_utc,
        "opening_price": price,
        "high_price": price + 1.0,
        "low_price": price - 1.0,
        "trade_price": price + 0.2,
        "candle_acc_trade_volume": 10.0,
        "candle_acc_trade_price": 1000.0,
    }
