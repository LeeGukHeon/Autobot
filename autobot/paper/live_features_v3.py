"""Live feature row builder for Paper model_alpha_v1 (feature_set=v3)."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Sequence

import polars as pl

from autobot.strategy.micro_snapshot import MicroSnapshotProvider

from .live_features_multitf_base import _LiveMultiTfRuntimeBase


class LiveFeatureProviderV3(_LiveMultiTfRuntimeBase):
    """On-demand v3 feature builder for Paper runtime."""

    def __init__(
        self,
        *,
        feature_columns: Sequence[str],
        extra_columns: Sequence[str] = (),
        tf: str = "5m",
        high_tfs: Sequence[str] = ("15m", "60m", "240m"),
        micro_snapshot_provider: MicroSnapshotProvider | None = None,
        micro_max_age_ms: int = 300_000,
        parquet_root: str | Path = "data/parquet",
        candles_dataset_name: str = "candles_api_v1",
        bootstrap_1m_bars: int = 2000,
        max_1m_history: int = 5000,
        missing_feature_warn_ratio: float = 0.05,
        missing_feature_skip_ratio: float = 0.20,
    ) -> None:
        self._feature_columns = tuple(str(col).strip() for col in feature_columns if str(col).strip())
        self._extra_columns = tuple(str(col).strip() for col in extra_columns if str(col).strip())
        super().__init__(
            tf=tf,
            high_tfs=high_tfs,
            micro_snapshot_provider=micro_snapshot_provider,
            micro_max_age_ms=micro_max_age_ms,
            parquet_root=str(parquet_root),
            candles_dataset_name=str(candles_dataset_name),
            bootstrap_1m_bars=bootstrap_1m_bars,
            max_1m_history=max_1m_history,
            missing_feature_warn_ratio=missing_feature_warn_ratio,
            missing_feature_skip_ratio=missing_feature_skip_ratio,
        )

    def build_frame(self, *, ts_ms: int, markets: Sequence[str]) -> pl.DataFrame:
        frame, stats = self._build_runtime_frame(
            ts_ms=ts_ms,
            markets=markets,
            feature_columns=self._feature_columns,
            extra_columns=self._extra_columns,
            provider_name="LIVE_V3",
        )
        self._last_build_stats = stats
        return frame

    def status(self, *, now_ts_ms: int, requested_ts_ms: int | None = None) -> dict[str, Any]:
        payload = super().status(now_ts_ms=now_ts_ms, requested_ts_ms=requested_ts_ms)
        payload["provider"] = "LIVE_V3"
        return payload
