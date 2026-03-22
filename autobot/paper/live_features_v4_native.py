"""Native LIVE_V4 runtime row builder built directly on the online core."""

from __future__ import annotations

from pathlib import Path
from typing import Sequence

import polars as pl

from autobot.features.feature_blocks_v4_live_base import feature_columns_v4_live_base_contract
from autobot.features.feature_set_v4 import (
    attach_interaction_features_v4,
    attach_order_flow_panel_v1,
    attach_periodicity_features_v4,
    attach_spillover_breadth_features_v4,
    attach_trend_volume_features_v4,
)
from autobot.strategy.micro_snapshot import MicroSnapshotProvider

from .live_features_multitf_base import _LiveMultiTfRuntimeBase
from .live_features_v4_common import (
    project_requested_v4_columns,
)


class LiveFeatureProviderV4Native(_LiveMultiTfRuntimeBase):
    """Build the LIVE_V4 contract directly on the shared online runtime core."""

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
        max_1m_history: int = 5000,
    ) -> None:
        self._feature_columns = tuple(str(col).strip() for col in feature_columns if str(col).strip())
        self._extra_columns = tuple(str(col).strip() for col in extra_columns if str(col).strip())
        self._quote = str(quote).strip().upper() or "KRW"
        self._parquet_root = Path(parquet_root)
        self._candles_dataset_name = str(candles_dataset_name).strip() or "candles_api_v1"
        self._base_feature_columns = feature_columns_v4_live_base_contract(high_tfs=high_tfs)
        super().__init__(
            tf=tf,
            high_tfs=high_tfs,
            micro_snapshot_provider=micro_snapshot_provider,
            micro_max_age_ms=micro_max_age_ms,
            parquet_root=str(parquet_root),
            candles_dataset_name=str(candles_dataset_name),
            bootstrap_1m_bars=bootstrap_1m_bars,
            max_1m_history=max_1m_history,
            missing_feature_warn_ratio=1.0,
            missing_feature_skip_ratio=1.0,
        )

    def build_frame(self, *, ts_ms: int, markets: Sequence[str]) -> pl.DataFrame:
        base_frame, base_stats = self._build_runtime_frame(
            ts_ms=ts_ms,
            markets=markets,
            feature_columns=self._base_feature_columns,
            extra_columns=self._extra_columns,
            provider_name="LIVE_V4_NATIVE_BASE",
            missing_feature_warn_ratio=1.0,
            missing_feature_skip_ratio=1.0,
        )
        if base_frame.height <= 0:
            self._last_build_stats = {
                "provider": "LIVE_V4_NATIVE",
                "base_provider": "LIVE_V4_NATIVE_BASE",
                "requested_ts_ms": int(ts_ms),
                "built_rows": 0,
                "requested_feature_count": int(len(self._feature_columns)),
                "base_provider_stats": dict(base_stats),
            }
            return base_frame

        enriched = attach_spillover_breadth_features_v4(
            base_frame.sort(["ts_ms", "market"]),
            quote=self._quote,
            float_dtype="float32",
        )
        enriched = attach_periodicity_features_v4(enriched, float_dtype="float32")
        enriched = attach_trend_volume_features_v4(enriched, float_dtype="float32")
        enriched = attach_order_flow_panel_v1(enriched, float_dtype="float32")
        enriched = attach_interaction_features_v4(enriched, float_dtype="float32")
        final_frame, missing_columns = project_requested_v4_columns(
            frame=enriched,
            feature_columns=self._feature_columns,
            extra_columns=self._extra_columns,
        )
        hard_gate_triggered = len(missing_columns) > 0
        if hard_gate_triggered:
            final_frame = final_frame.head(0)
        self._last_build_stats = {
            "provider": "LIVE_V4_NATIVE",
            "base_provider": "LIVE_V4_NATIVE_BASE",
            "requested_ts_ms": int(ts_ms),
            "built_rows": int(final_frame.height),
            "requested_feature_count": int(len(self._feature_columns)),
            "missing_feature_columns": list(missing_columns),
            "hard_gate_triggered": hard_gate_triggered,
            "skip_reason": "MISSING_V4_FEATURE_COLUMNS" if hard_gate_triggered else "",
            "base_provider_stats": dict(base_stats),
        }
        return final_frame

    def status(self, *, now_ts_ms: int, requested_ts_ms: int | None = None) -> dict[str, Any]:
        payload = super().status(now_ts_ms=now_ts_ms, requested_ts_ms=requested_ts_ms)
        payload["provider"] = "LIVE_V4_NATIVE"
        payload["base_provider"] = "LIVE_V4_NATIVE_BASE"
        return payload
