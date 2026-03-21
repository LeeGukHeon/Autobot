"""Scaffold for a native LIVE_V4 runtime row builder.

This module is intentionally not wired into the default runtime yet.
It exists so we can migrate away from the current:

    LIVE_V4 -> LiveFeatureProviderV3 -> v4 enrichments

stack in a controlled way.

The next implementation step is to extract the shared online runtime candle/micro
state engine from ``live_features_v3.py`` into a neutral core module and then
complete this provider on top of that core.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Sequence

import polars as pl

from autobot.strategy.micro_snapshot import MicroSnapshotProvider
from autobot.upbit.ws.models import TickerEvent


class LiveFeatureProviderV4Native:
    """Placeholder for a native v4 runtime feature builder.

    The class mirrors the public interface used by paper/live runtime:

    - ``ingest_ticker(event)``
    - ``build_frame(ts_ms=..., markets=...)``
    - ``status(now_ts_ms=..., requested_ts_ms=...)``
    - ``last_build_stats()``

    It is intentionally not yet enabled by runtime selection code.
    """

    def __init__(
        self,
        *,
        feature_columns: Sequence[str],
        extra_columns: Sequence[str] = (),
        tf: str = "5m",
        quote: str = "KRW",
        high_tfs: Sequence[str] = ("15m", "60m", "240m"),
        micro_snapshot_provider: MicroSnapshotProvider | None = None,
        micro_max_age_ms: int = 300_000,
        parquet_root: str | Path = "data/parquet",
        candles_dataset_name: str = "candles_api_v1",
        bootstrap_1m_bars: int = 2000,
    ) -> None:
        self._feature_columns = tuple(str(col).strip() for col in feature_columns if str(col).strip())
        self._extra_columns = tuple(str(col).strip() for col in extra_columns if str(col).strip())
        self._tf = str(tf).strip().lower() or "5m"
        self._quote = str(quote).strip().upper() or "KRW"
        self._high_tfs = tuple(str(item).strip().lower() for item in high_tfs if str(item).strip())
        self._micro_snapshot_provider = micro_snapshot_provider
        self._micro_max_age_ms = max(int(micro_max_age_ms), 0)
        self._parquet_root = Path(parquet_root)
        self._candles_dataset_name = str(candles_dataset_name).strip() or "candles_api_v1"
        self._bootstrap_1m_bars = max(int(bootstrap_1m_bars), 256)
        self._last_requested_ts_ms: int | None = None
        self._last_built_ts_ms: int | None = None
        self._last_build_stats: dict[str, Any] = {
            "provider": "LIVE_V4_NATIVE",
            "status": "scaffold",
            "built_rows": 0,
            "requested_feature_count": int(len(self._feature_columns)),
        }

    def ingest_ticker(self, event: TickerEvent) -> None:
        _ = event

    def build_frame(self, *, ts_ms: int, markets: Sequence[str]) -> pl.DataFrame:
        self._last_requested_ts_ms = int(ts_ms)
        self._last_built_ts_ms = None
        self._last_build_stats = {
            "provider": "LIVE_V4_NATIVE",
            "status": "not_implemented",
            "requested_ts_ms": int(ts_ms),
            "requested_markets": int(len(tuple(markets))),
            "built_rows": 0,
            "requested_feature_count": int(len(self._feature_columns)),
            "skip_reason": "LIVE_V4_NATIVE_NOT_IMPLEMENTED",
        }
        raise NotImplementedError(
            "LiveFeatureProviderV4Native is a scaffold only; extract shared online core from live_features_v3.py before enabling it."
        )

    def status(self, *, now_ts_ms: int, requested_ts_ms: int | None = None) -> dict[str, Any]:
        return {
            "provider": "LIVE_V4_NATIVE",
            "status": str(self._last_build_stats.get("status", "scaffold")),
            "now_ts_ms": int(now_ts_ms),
            "requested_ts_ms": int(requested_ts_ms) if requested_ts_ms is not None else self._last_requested_ts_ms,
            "built_ts_ms": int(self._last_built_ts_ms) if self._last_built_ts_ms is not None else None,
        }

    def last_build_stats(self) -> dict[str, Any]:
        return dict(self._last_build_stats)
