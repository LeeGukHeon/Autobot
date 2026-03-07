"""Live feature row builder for Paper model_alpha_v1 (feature_set=v4)."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Sequence

import polars as pl

from autobot.features.feature_blocks_v3 import feature_columns_v3_contract
from autobot.features.feature_set_v4 import (
    attach_interaction_features_v4,
    attach_periodicity_features_v4,
    attach_spillover_breadth_features_v4,
    attach_trend_volume_features_v4,
)
from autobot.strategy.micro_snapshot import MicroSnapshotProvider
from autobot.upbit.ws.models import TickerEvent

from .live_features_v3 import LiveFeatureProviderV3


class LiveFeatureProviderV4:
    """Compose LIVE_V3 rows into the v4 contract without mutating LIVE_V3."""

    def __init__(
        self,
        *,
        feature_columns: Sequence[str],
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
        self._quote = str(quote).strip().upper() or "KRW"
        self._high_tfs = tuple(str(item).strip().lower() for item in high_tfs if str(item).strip())
        self._base_provider = LiveFeatureProviderV3(
            feature_columns=feature_columns_v3_contract(high_tfs=self._high_tfs),
            tf=tf,
            high_tfs=self._high_tfs,
            micro_snapshot_provider=micro_snapshot_provider,
            micro_max_age_ms=micro_max_age_ms,
            parquet_root=parquet_root,
            candles_dataset_name=candles_dataset_name,
            bootstrap_1m_bars=bootstrap_1m_bars,
            missing_feature_warn_ratio=1.0,
            missing_feature_skip_ratio=1.0,
        )
        self._last_build_stats: dict[str, Any] = {}

    def ingest_ticker(self, event: TickerEvent) -> None:
        self._base_provider.ingest_ticker(event)

    def build_frame(self, *, ts_ms: int, markets: Sequence[str]) -> pl.DataFrame:
        base_frame = self._base_provider.build_frame(ts_ms=int(ts_ms), markets=markets)
        base_stats = self._base_provider.last_build_stats()
        if base_frame.height <= 0:
            self._last_build_stats = {
                "provider": "LIVE_V4",
                "base_provider": "LIVE_V3",
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
        enriched = attach_periodicity_features_v4(
            enriched,
            float_dtype="float32",
        )
        enriched = attach_trend_volume_features_v4(
            enriched,
            float_dtype="float32",
        )
        enriched = attach_interaction_features_v4(
            enriched,
            float_dtype="float32",
        )
        final_frame, missing_columns = _project_requested_columns(
            frame=enriched,
            feature_columns=self._feature_columns,
        )
        hard_gate_triggered = len(missing_columns) > 0
        if hard_gate_triggered:
            final_frame = final_frame.head(0)
        self._last_build_stats = {
            "provider": "LIVE_V4",
            "base_provider": "LIVE_V3",
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
        payload = self._base_provider.status(now_ts_ms=now_ts_ms, requested_ts_ms=requested_ts_ms)
        payload["provider"] = "LIVE_V4"
        payload["base_provider"] = "LIVE_V3"
        return payload

    def last_build_stats(self) -> dict[str, Any]:
        return dict(self._last_build_stats)


def _project_requested_columns(
    *,
    frame: pl.DataFrame,
    feature_columns: Sequence[str],
) -> tuple[pl.DataFrame, tuple[str, ...]]:
    required_order = ["ts_ms", "market", "close", *list(feature_columns)]
    working = frame
    missing_columns: list[str] = []
    if "ts_ms" not in working.columns:
        working = working.with_columns(pl.lit(0, dtype=pl.Int64).alias("ts_ms"))
    if "market" not in working.columns:
        working = working.with_columns(pl.lit("", dtype=pl.Utf8).alias("market"))
    if "close" not in working.columns:
        working = working.with_columns(pl.lit(0.0, dtype=pl.Float32).alias("close"))
    for name in feature_columns:
        if name in working.columns:
            continue
        missing_columns.append(str(name))
    if missing_columns:
        schema: dict[str, pl.DataType] = {}
        for name in required_order:
            if name in working.columns:
                schema[name] = working.schema[name]
            elif name == "ts_ms":
                schema[name] = pl.Int64
            elif name == "market":
                schema[name] = pl.Utf8
            else:
                schema[name] = pl.Float32
        return pl.DataFrame(schema=schema), tuple(missing_columns)
    return working.select(required_order), tuple()
