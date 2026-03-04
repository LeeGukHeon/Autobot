from __future__ import annotations

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
