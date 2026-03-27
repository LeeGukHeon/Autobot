from __future__ import annotations

import pytest

from autobot.strategy.micro_snapshot import LiveWsMicroSnapshotProvider, LiveWsProviderSettings, LiveWsRateLimitGuard


def test_live_ws_rate_limit_guard_windows() -> None:
    guard = LiveWsRateLimitGuard(max_subscribe_messages_per_min=2, max_reconnect_per_min=1)

    assert guard.allow_subscribe(now_monotonic=0.0)
    assert guard.allow_subscribe(now_monotonic=10.0)
    assert not guard.allow_subscribe(now_monotonic=20.0)
    assert guard.allow_subscribe(now_monotonic=61.0)

    assert guard.allow_reconnect(now_monotonic=0.0)
    assert not guard.allow_reconnect(now_monotonic=30.0)
    assert guard.allow_reconnect(now_monotonic=61.0)


def test_live_ws_provider_track_markets_respects_subscribe_guard() -> None:
    provider = LiveWsMicroSnapshotProvider(
        LiveWsProviderSettings(
            enabled=True,
            max_subscribe_messages_per_min=1,
            max_markets=30,
        )
    )
    assert provider.track_markets(["KRW-BTC"], now_monotonic=0.0)
    assert not provider.track_markets(["KRW-BTC", "KRW-ETH"], now_monotonic=1.0)


def test_live_ws_provider_reports_actual_last_event_timestamp() -> None:
    provider = LiveWsMicroSnapshotProvider(LiveWsProviderSettings(enabled=True))
    provider.ingest_trade(
        {
            "market": "KRW-BTC",
            "trade_ts_ms": 1_700_000_000_000,
            "price": 100.0,
            "volume": 0.1,
            "ask_bid": "BID",
        }
    )

    snapshot = provider.get("KRW-BTC", 1_700_000_050_000)
    assert snapshot is not None
    assert snapshot.snapshot_ts_ms == 1_700_000_050_000
    assert snapshot.last_event_ts_ms == 1_700_000_000_000


def test_live_ws_provider_counts_buy_sell_trades_and_total_volume() -> None:
    provider = LiveWsMicroSnapshotProvider(LiveWsProviderSettings(enabled=True))
    provider.ingest_trade(
        {
            "market": "KRW-BTC",
            "trade_ts_ms": 1_700_000_000_000,
            "price": 100.0,
            "volume": 0.1,
            "ask_bid": "BID",
        }
    )
    provider.ingest_trade(
        {
            "market": "KRW-BTC",
            "trade_ts_ms": 1_700_000_010_000,
            "price": 101.0,
            "volume": 0.2,
            "ask_bid": "ASK",
        }
    )

    snapshot = provider.get("KRW-BTC", 1_700_000_050_000)

    assert snapshot is not None
    assert snapshot.trade_count == 2
    assert snapshot.buy_count == 1
    assert snapshot.sell_count == 1
    assert snapshot.trade_volume_total == pytest.approx(0.3, rel=0, abs=1e-12)
    assert snapshot.buy_volume == pytest.approx(0.1, rel=0, abs=1e-12)
    assert snapshot.sell_volume == pytest.approx(0.2, rel=0, abs=1e-12)
