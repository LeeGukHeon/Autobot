"""Default LIVE_V4 runtime builder backed by the native v4 implementation."""

from __future__ import annotations

from typing import Any

from .live_features_v4_native import LiveFeatureProviderV4Native


class LiveFeatureProviderV4(LiveFeatureProviderV4Native):
    """Default LIVE_V4 provider that now runs on the native v4 path."""

    def build_frame(self, *, ts_ms: int, markets: tuple[str, ...] | list[str]) -> Any:
        frame = super().build_frame(ts_ms=ts_ms, markets=markets)
        stats = dict(self._last_build_stats)
        if stats:
            stats["provider"] = "LIVE_V4"
            if str(stats.get("base_provider", "")).strip():
                stats["base_provider"] = "LIVE_V4_BASE"
            base_stats = stats.get("base_provider_stats")
            if isinstance(base_stats, dict):
                nested = dict(base_stats)
                nested["provider"] = "LIVE_V4_BASE"
                stats["base_provider_stats"] = nested
            self._last_build_stats = stats
        return frame

    def status(self, *, now_ts_ms: int, requested_ts_ms: int | None = None) -> dict[str, Any]:
        payload = super().status(now_ts_ms=now_ts_ms, requested_ts_ms=requested_ts_ms)
        payload["provider"] = "LIVE_V4"
        if str(payload.get("base_provider", "")).strip():
            payload["base_provider"] = "LIVE_V4_BASE"
        return payload
