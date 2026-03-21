"""Native LIVE_V4 runtime row builder built directly on the online core."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Any, Sequence

import polars as pl

from autobot.features.ctrend_v1 import (
    build_ctrend_v1_daily_feature_frame,
    ctrend_v1_feature_columns,
    ctrend_v1_history_lookback_days,
)
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
from .live_features_online_core import _resolve_dataset_path
from .live_features_v4_common import (
    load_market_candles_merged,
    project_requested_v4_columns,
    resolve_ctrend_history_roots,
    utc_day_start_ts_ms,
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
        self._primary_candles_root = _resolve_dataset_path(
            parquet_root=self._parquet_root,
            dataset_name=self._candles_dataset_name,
        )
        self._ctrend_history_roots = resolve_ctrend_history_roots(
            parquet_root=self._parquet_root,
            primary_root=self._primary_candles_root,
        )
        self._ctrend_feature_columns = tuple(
            name for name in ctrend_v1_feature_columns() if name in set(self._feature_columns)
        )
        self._ctrend_history_lookback_days = ctrend_v1_history_lookback_days()
        self._ctrend_daily_cache: dict[tuple[str, str], dict[str, Any] | None] = {}
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
        if self._ctrend_feature_columns:
            enriched = self._attach_ctrend_live_features(enriched)
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
            "ctrend_requested_feature_count": int(len(self._ctrend_feature_columns)),
            "ctrend_cache_entries": int(len(self._ctrend_daily_cache)),
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

    def _attach_ctrend_live_features(self, frame: pl.DataFrame) -> pl.DataFrame:
        if frame.height <= 0 or not self._ctrend_feature_columns:
            return frame
        keyed = frame.with_columns(
            pl.from_epoch(pl.col("ts_ms"), time_unit="ms").dt.date().alias("__ctrend_join_date")
        )
        join_rows: list[dict[str, Any]] = []
        for item in keyed.select(["market", "__ctrend_join_date"]).unique().iter_rows(named=True):
            market = str(item.get("market", "")).strip().upper()
            join_date = item.get("__ctrend_join_date")
            if not market or not isinstance(join_date, date):
                continue
            cached = self._load_ctrend_daily_row(market=market, join_date=join_date)
            if cached is None:
                continue
            payload: dict[str, Any] = {
                "market": market,
                "__ctrend_join_date": join_date,
            }
            for name in self._ctrend_feature_columns:
                payload[name] = cached.get(name)
            join_rows.append(payload)
        if not join_rows:
            return keyed.drop("__ctrend_join_date")
        ctrend_frame = pl.DataFrame(join_rows)
        return keyed.join(
            ctrend_frame,
            on=["market", "__ctrend_join_date"],
            how="left",
        ).drop("__ctrend_join_date")

    def _load_ctrend_daily_row(self, *, market: str, join_date: date) -> dict[str, Any] | None:
        cache_key = (str(market).strip().upper(), join_date.isoformat())
        if cache_key in self._ctrend_daily_cache:
            return self._ctrend_daily_cache[cache_key]
        start_date = join_date - timedelta(days=max(self._ctrend_history_lookback_days, 1))
        end_date = join_date
        history = load_market_candles_merged(
            roots=self._ctrend_history_roots,
            market=cache_key[0],
            tf="5m",
            start_ts_ms=utc_day_start_ts_ms(start_date),
            end_ts_ms=utc_day_start_ts_ms(end_date + timedelta(days=1)),
        )
        if history.height <= 0:
            self._ctrend_daily_cache[cache_key] = None
            return None
        daily = build_ctrend_v1_daily_feature_frame(history, float_dtype="float32")
        matched = daily.filter(pl.col("__broadcast_date") == pl.lit(join_date, dtype=pl.Date)).select(
            ["market", *list(self._ctrend_feature_columns)]
        )
        if matched.height <= 0:
            self._ctrend_daily_cache[cache_key] = None
            return None
        row = matched.tail(1).row(0, named=True)
        payload = {name: row.get(name) for name in self._ctrend_feature_columns}
        self._ctrend_daily_cache[cache_key] = payload
        return payload
