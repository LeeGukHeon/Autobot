"""Live feature row builder for Paper model_alpha_v1 (feature_set=v4)."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Sequence

import polars as pl

from autobot.features.ctrend_v1 import (
    build_ctrend_v1_daily_feature_frame,
    ctrend_v1_feature_columns,
    ctrend_v1_history_lookback_days,
)
from autobot.features.feature_blocks_v3 import feature_columns_v3_contract
from autobot.features.feature_set_v4 import (
    attach_interaction_features_v4,
    attach_order_flow_panel_v1,
    attach_periodicity_features_v4,
    attach_spillover_breadth_features_v4,
    attach_trend_volume_features_v4,
)
from autobot.strategy.micro_snapshot import MicroSnapshotProvider
from autobot.upbit.ws.models import TickerEvent

from .live_features_v3 import LiveFeatureProviderV3, _market_files, _resolve_dataset_path


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
        self._parquet_root = Path(parquet_root)
        self._candles_dataset_name = str(candles_dataset_name).strip() or "candles_api_v1"
        self._primary_candles_root = _resolve_dataset_path(
            parquet_root=self._parquet_root,
            dataset_name=self._candles_dataset_name,
        )
        self._ctrend_history_roots = _resolve_ctrend_history_roots(
            parquet_root=self._parquet_root,
            primary_root=self._primary_candles_root,
        )
        self._ctrend_feature_columns = tuple(
            name for name in ctrend_v1_feature_columns() if name in set(self._feature_columns)
        )
        self._ctrend_history_lookback_days = ctrend_v1_history_lookback_days()
        self._ctrend_daily_cache: dict[tuple[str, str], dict[str, Any] | None] = {}
        self._base_provider = LiveFeatureProviderV3(
            feature_columns=feature_columns_v3_contract(high_tfs=self._high_tfs),
            tf=tf,
            high_tfs=self._high_tfs,
            micro_snapshot_provider=micro_snapshot_provider,
            micro_max_age_ms=micro_max_age_ms,
            parquet_root=self._parquet_root,
            candles_dataset_name=self._candles_dataset_name,
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
        enriched = attach_order_flow_panel_v1(
            enriched,
            float_dtype="float32",
        )
        enriched = attach_interaction_features_v4(
            enriched,
            float_dtype="float32",
        )
        if self._ctrend_feature_columns:
            enriched = self._attach_ctrend_live_features(enriched)
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
            "ctrend_requested_feature_count": int(len(self._ctrend_feature_columns)),
            "ctrend_cache_entries": int(len(self._ctrend_daily_cache)),
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
        history = _load_market_candles_merged(
            roots=self._ctrend_history_roots,
            market=cache_key[0],
            tf="5m",
            start_ts_ms=_utc_day_start_ts_ms(start_date),
            end_ts_ms=_utc_day_start_ts_ms(end_date + timedelta(days=1)),
        )
        if history.height <= 0:
            self._ctrend_daily_cache[cache_key] = None
            return None
        daily = build_ctrend_v1_daily_feature_frame(history, float_dtype="float32")
        matched = daily.filter(
            pl.col("__broadcast_date") == pl.lit(join_date, dtype=pl.Date)
        ).select(["market", *list(self._ctrend_feature_columns)])
        if matched.height <= 0:
            self._ctrend_daily_cache[cache_key] = None
            return None
        row = matched.tail(1).row(0, named=True)
        payload = {name: row.get(name) for name in self._ctrend_feature_columns}
        self._ctrend_daily_cache[cache_key] = payload
        return payload


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


def _resolve_ctrend_history_roots(*, parquet_root: Path, primary_root: Path) -> tuple[Path, ...]:
    roots: list[Path] = []
    fallback = parquet_root / "candles_v1"
    if fallback.exists():
        roots.append(fallback)
    if primary_root not in roots:
        roots.append(primary_root)
    return tuple(roots)


def _load_market_candles_merged(
    *,
    roots: Sequence[Path],
    market: str,
    tf: str,
    start_ts_ms: int,
    end_ts_ms: int,
) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    for root in roots:
        if not root.exists():
            continue
        frame = _load_market_candles_window(
            dataset_root=root,
            market=market,
            tf=tf,
            start_ts_ms=start_ts_ms,
            end_ts_ms=end_ts_ms,
        )
        if frame.height > 0:
            frames.append(frame)
    if not frames:
        return pl.DataFrame(
            schema={
                "ts_ms": pl.Int64,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume_base": pl.Float64,
                "market": pl.Utf8,
            }
        )
    merged = pl.concat(frames, how="vertical_relaxed").sort("ts_ms")
    return merged.unique(subset=["ts_ms"], keep="last", maintain_order=True)


def _load_market_candles_window(
    *,
    dataset_root: Path,
    market: str,
    tf: str,
    start_ts_ms: int,
    end_ts_ms: int,
) -> pl.DataFrame:
    files = _market_files(dataset_root=dataset_root, tf=tf, market=market)
    if not files:
        return pl.DataFrame()
    try:
        return (
            pl.scan_parquet([str(path) for path in files])
            .select(
                [
                    pl.col("ts_ms").cast(pl.Int64).alias("ts_ms"),
                    pl.col("open").cast(pl.Float64).alias("open"),
                    pl.col("high").cast(pl.Float64).alias("high"),
                    pl.col("low").cast(pl.Float64).alias("low"),
                    pl.col("close").cast(pl.Float64).alias("close"),
                    pl.col("volume_base").cast(pl.Float64).alias("volume_base"),
                ]
            )
            .filter(
                (pl.col("ts_ms") >= int(start_ts_ms))
                & (pl.col("ts_ms") < int(end_ts_ms))
            )
            .sort("ts_ms")
            .collect()
            .with_columns(pl.lit(str(market).strip().upper(), dtype=pl.Utf8).alias("market"))
        )
    except Exception:
        return pl.DataFrame()


def _utc_day_start_ts_ms(day: date) -> int:
    return int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp() * 1000)
