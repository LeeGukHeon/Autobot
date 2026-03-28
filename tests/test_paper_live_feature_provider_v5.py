from __future__ import annotations

from autobot.paper.live_features_v5 import LiveFeatureProviderV5


def test_live_feature_provider_v5_last_build_stats_returns_copy() -> None:
    provider = LiveFeatureProviderV5.__new__(LiveFeatureProviderV5)
    provider._last_build_stats = {  # type: ignore[attr-defined]
        "provider": "LIVE_V5",
        "built_rows": 3,
    }

    stats = provider.last_build_stats()
    assert stats == {"provider": "LIVE_V5", "built_rows": 3}

    stats["built_rows"] = 0
    assert provider._last_build_stats["built_rows"] == 3  # type: ignore[attr-defined]
