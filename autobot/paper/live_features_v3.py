"""Live feature row builder for Paper model_alpha_v1 (feature_set=v3)."""

from __future__ import annotations

from collections import Counter, deque
from dataclasses import dataclass
import math
from pathlib import Path
from typing import Any, Sequence

import polars as pl

from autobot.data import expected_interval_ms
from autobot.features.feature_blocks_v3 import compute_base_features_v3
from autobot.features.micro_join import prefixed_micro_columns
from autobot.features.multitf_join_v1 import (
    aggregate_1m_for_base,
    compute_high_tf_features,
    densify_1m_candles,
    join_1m_aggregate,
    join_high_tf_asof,
)
from autobot.strategy.micro_snapshot import MicroSnapshotProvider
from autobot.upbit.ws.models import TickerEvent


@dataclass
class _ActiveMinute:
    minute_start_ts_ms: int
    open: float
    high: float
    low: float
    close: float
    volume_base: float
    trade_events: int


@dataclass(frozen=True)
class _MinuteCandle:
    ts_ms: int
    open: float
    high: float
    low: float
    close: float
    volume_base: float
    is_synth_1m: bool


@dataclass
class _MarketState:
    bootstrap_loaded: bool = False
    bootstrap_status: str = "UNSET"
    bootstrap_rows: int = 0
    candles_1m: deque[_MinuteCandle] | None = None
    active: _ActiveMinute | None = None
    last_price: float | None = None
    last_closed_price: float | None = None
    last_event_ts_ms: int | None = None
    prev_acc_trade_price_24h: float | None = None


class LiveFeatureProviderV3:
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
        self._tf = str(tf).strip().lower() or "5m"
        self._high_tfs = tuple(str(item).strip().lower() for item in high_tfs if str(item).strip())
        self._micro_snapshot_provider = micro_snapshot_provider
        self._micro_max_age_ms = max(int(micro_max_age_ms), 0)
        self._parquet_root = Path(parquet_root)
        self._candles_dataset_name = str(candles_dataset_name).strip() or "candles_api_v1"
        self._candles_root = _resolve_dataset_path(
            parquet_root=self._parquet_root,
            dataset_name=self._candles_dataset_name,
        )
        self._bootstrap_1m_bars = max(int(bootstrap_1m_bars), 256)
        self._max_1m_history = max(int(max_1m_history), self._bootstrap_1m_bars)
        self._lookback_1m_bars = max(min(self._max_1m_history, 5000), 2400)
        self._fallback_synth_1m_bars = 120
        self._missing_feature_warn_ratio = min(max(float(missing_feature_warn_ratio), 0.0), 1.0)
        self._missing_feature_skip_ratio = min(max(float(missing_feature_skip_ratio), 0.0), 1.0)
        self._market_state: dict[str, _MarketState] = {}
        self._latest_feature_ts_ms: int | None = None
        self._last_requested_ts_ms: int | None = None
        self._last_built_ts_ms: int | None = None
        self._last_build_stats: dict[str, Any] = {}

    def ingest_ticker(self, event: TickerEvent) -> None:
        market = str(event.market).strip().upper()
        if not market:
            return
        price = _safe_float(event.trade_price)
        if price is None or price <= 0:
            return
        ts_ms = int(event.ts_ms)
        minute_start = (ts_ms // 60_000) * 60_000

        state = self._market_state.setdefault(market, _MarketState())
        if state.candles_1m is None:
            state.candles_1m = deque(maxlen=self._max_1m_history)

        volume_base = _estimate_volume_base_from_ticker(
            trade_price=price,
            acc_trade_price_24h=_safe_float(event.acc_trade_price_24h) or 0.0,
            prev_acc_trade_price_24h=state.prev_acc_trade_price_24h,
        )
        state.prev_acc_trade_price_24h = _safe_float(event.acc_trade_price_24h) or state.prev_acc_trade_price_24h
        state.last_price = price
        state.last_event_ts_ms = ts_ms

        if state.active is None:
            state.active = _ActiveMinute(
                minute_start_ts_ms=minute_start,
                open=price,
                high=price,
                low=price,
                close=price,
                volume_base=volume_base,
                trade_events=1,
            )
            return

        active = state.active
        if minute_start < int(active.minute_start_ts_ms):
            return
        if minute_start == int(active.minute_start_ts_ms):
            active.high = max(float(active.high), price)
            active.low = min(float(active.low), price)
            active.close = price
            active.volume_base = float(active.volume_base) + volume_base
            active.trade_events = int(active.trade_events) + 1
            return

        self._finalize_active_minute(state=state)
        state.active = _ActiveMinute(
            minute_start_ts_ms=minute_start,
            open=price,
            high=price,
            low=price,
            close=price,
            volume_base=volume_base,
            trade_events=1,
        )

    def build_frame(self, *, ts_ms: int, markets: Sequence[str]) -> pl.DataFrame:
        ts_value = int(ts_ms)
        requested = _normalize_markets(markets)
        self._last_requested_ts_ms = ts_value
        rows: list[dict[str, Any]] = []
        built_markets: list[str] = []
        skipped_markets: list[str] = []
        skip_reasons: dict[str, int] = {}
        built_reasons: dict[str, int] = {}
        missing_feature_counter: Counter[str] = Counter()
        warn_missing_feature_market_count = 0
        missing_feature_cells_total = 0

        for market in requested:
            row, reason, missing_feature_cells, missing_features = self._build_market_row(market=market, ts_ms=ts_value)
            missing_feature_cells_total += int(missing_feature_cells)
            for name in missing_features:
                missing_feature_counter[str(name)] += 1
            feature_count = max(len(self._feature_columns), 1)
            missing_ratio = float(missing_feature_cells) / float(feature_count)
            if missing_ratio > float(self._missing_feature_warn_ratio):
                warn_missing_feature_market_count += 1
            if row is None:
                skipped_markets.append(market)
                skip_reasons[reason] = int(skip_reasons.get(reason, 0)) + 1
                continue
            built_markets.append(market)
            if reason != "OK":
                built_reasons[reason] = int(built_reasons.get(reason, 0)) + 1
            rows.append(row)

        total_feature_cells = max(len(self._feature_columns), 1) * max(len(requested), 1)
        missing_feature_ratio = float(missing_feature_cells_total) / float(total_feature_cells)
        missing_feature_topk = [
            {"feature": name, "count": int(count)}
            for name, count in sorted(
                missing_feature_counter.items(),
                key=lambda item: (-int(item[1]), str(item[0])),
            )[:10]
        ]
        bootstrap_missing = 0
        bootstrap_partial = 0
        for market in requested:
            state = self._market_state.get(market)
            status = str(getattr(state, "bootstrap_status", "UNSET")).strip().upper() if state is not None else "UNSET"
            if status in {"MISSING", "EMPTY"}:
                bootstrap_missing += 1
            elif status in {"PARTIAL"}:
                bootstrap_partial += 1

        if rows:
            self._latest_feature_ts_ms = ts_value
            self._last_built_ts_ms = ts_value
        else:
            self._last_built_ts_ms = None
        frame = _rows_to_frame(rows=rows, feature_columns=self._feature_columns, extra_columns=self._extra_columns)
        self._last_build_stats = {
            "provider": "LIVE_V3",
            "requested_ts_ms": ts_value,
            "built_ts_ms": int(self._last_built_ts_ms) if self._last_built_ts_ms is not None else None,
            "requested_markets": int(len(requested)),
            "built_markets": int(len(built_markets)),
            "skipped_markets": int(len(skipped_markets)),
            "built_rows": int(frame.height),
            "built_market_samples": list(built_markets[:20]),
            "skipped_market_samples": list(skipped_markets[:20]),
            "skip_reasons": dict(sorted(skip_reasons.items(), key=lambda item: str(item[0]))),
            "built_reasons": dict(sorted(built_reasons.items(), key=lambda item: str(item[0]))),
            "missing_feature_cells_total": int(missing_feature_cells_total),
            "missing_feature_ratio": float(missing_feature_ratio),
            "missing_feature_warn_market_count": int(warn_missing_feature_market_count),
            "missing_feature_topk": missing_feature_topk,
            "missing_feature_warn_ratio_threshold": float(self._missing_feature_warn_ratio),
            "missing_feature_skip_ratio_threshold": float(self._missing_feature_skip_ratio),
            "bootstrap_missing_markets_count": int(bootstrap_missing),
            "bootstrap_partial_markets_count": int(bootstrap_partial),
        }
        return frame

    def status(self, *, now_ts_ms: int, requested_ts_ms: int | None = None) -> dict[str, Any]:
        latest = self._latest_feature_ts_ms
        gap_min = None
        if latest is not None:
            gap_min = max(float(int(now_ts_ms) - int(latest)) / 60_000.0, 0.0)
        return {
            "provider": "LIVE_V3",
            "now_ts_ms": int(now_ts_ms),
            "requested_ts_ms": int(requested_ts_ms) if requested_ts_ms is not None else self._last_requested_ts_ms,
            "built_ts_ms": int(self._last_built_ts_ms) if self._last_built_ts_ms is not None else None,
            "latest_feature_ts_ms": int(latest) if latest is not None else None,
            "gap_min": float(gap_min) if gap_min is not None else None,
        }

    def last_build_stats(self) -> dict[str, Any]:
        return dict(self._last_build_stats)

    def _build_market_row(self, *, market: str, ts_ms: int) -> tuple[dict[str, Any] | None, str, int, tuple[str, ...]]:
        state = self._market_state.setdefault(market, _MarketState())
        if state.candles_1m is None:
            state.candles_1m = deque(maxlen=self._max_1m_history)
        if not state.bootstrap_loaded:
            self._bootstrap_market(market=market, state=state)
            state.bootstrap_loaded = True

        self._flush_active_until_ts(state=state, ts_ms=ts_ms)
        one_m = self._build_one_m_frame(state=state, ts_ms=ts_ms)
        if one_m.height <= 0:
            return None, "NO_1M_HISTORY", 0, ()

        base = _rollup_from_1m(one_m=one_m, tf=self._tf).filter(pl.col("ts_ms") <= int(ts_ms))
        if base.height <= 0:
            return None, "NO_BASE_CANDLE", 0, ()

        base_featured = compute_base_features_v3(base, tf=self._tf, float_dtype="float32").sort("ts_ms")
        if base_featured.height <= 0:
            return None, "NO_BASE_FEATURES", 0, ()

        dense_start = int(base_featured.get_column("ts_ms").min())
        dense_end = int(base_featured.get_column("ts_ms").max())
        one_m_dense = densify_1m_candles(
            one_m.select(["ts_ms", "open", "high", "low", "close", "volume_base"]),
            start_ts_ms=dense_start,
            end_ts_ms=dense_end,
        )
        one_m_agg = aggregate_1m_for_base(one_m_dense, base_tf=self._tf, float_dtype="float32")
        working, _ = join_1m_aggregate(
            base_frame=base_featured,
            one_m_agg=one_m_agg,
            required_bars=5,
            max_missing_ratio=0.2,
            drop_if_real_count_zero=True,
        )

        for high_tf in self._high_tfs:
            high_candles = _rollup_from_1m(one_m=one_m, tf=high_tf).filter(pl.col("ts_ms") <= int(ts_ms))
            high_features = compute_high_tf_features(high_candles, tf=high_tf, float_dtype="float32")
            staleness = int(round(expected_interval_ms(high_tf) * 2.0))
            working, _ = join_high_tf_asof(
                base_frame=working,
                high_tf_features=high_features,
                tf=high_tf,
                max_staleness_ms=staleness,
            )

        base_with_aux = _attach_runtime_aux_columns(base)
        target = working.filter(pl.col("ts_ms") == int(ts_ms)).tail(1)
        if target.height <= 0:
            return None, "NO_FEATURE_ROW_AT_TS", 0, ()

        base_row = target.row(0, named=True)
        aux_target = base_with_aux.filter(pl.col("ts_ms") == int(ts_ms)).tail(1)
        aux_row = aux_target.row(0, named=True) if aux_target.height > 0 else {}
        close_value = _safe_float(base_row.get("close")) or _safe_float(state.last_price)
        if close_value is None or close_value <= 0:
            close_value = 0.0

        micro_values, micro_reason = self._micro_feature_values(
            market=market,
            ts_ms=ts_ms,
            close_value=float(close_value),
        )
        missing_features = 0
        missing_columns: list[str] = []
        out: dict[str, Any] = {
            "ts_ms": int(ts_ms),
            "market": market,
            "close": float(close_value),
        }
        for col in self._feature_columns:
            value = base_row.get(col)
            if value is None and col in micro_values:
                value = micro_values.get(col)
            if value is None:
                value = 0.0
                missing_features += 1
                missing_columns.append(str(col))
            normalized = _to_feature_float(value)
            if normalized is None:
                normalized = 0.0
                missing_features += 1
                missing_columns.append(str(col))
            out[col] = float(normalized)
        for col in self._extra_columns:
            if str(col) == "close":
                continue
            out[str(col)] = _resolve_extra_row_value(
                column=str(col),
                base_row=base_row,
                aux_row=aux_row,
                close_value=float(close_value),
            )
        feature_count = max(len(self._feature_columns), 1)
        missing_ratio = float(missing_features) / float(feature_count)
        if missing_ratio > float(self._missing_feature_skip_ratio):
            return None, "MISSING_FEATURE_RATIO_HIGH", missing_features, tuple(sorted(set(missing_columns)))
        return out, micro_reason, missing_features, tuple(sorted(set(missing_columns)))

    def _micro_feature_values(self, *, market: str, ts_ms: int, close_value: float) -> tuple[dict[str, float], str]:
        default_values = _default_micro_feature_values()
        if self._micro_snapshot_provider is None:
            return default_values, "MISSING_MICRO"
        snapshot = self._micro_snapshot_provider.get(market, int(ts_ms))
        if snapshot is None:
            return default_values, "MISSING_MICRO"

        age_ms = max(int(ts_ms) - int(snapshot.last_event_ts_ms), 0)
        if self._micro_max_age_ms > 0 and age_ms > self._micro_max_age_ms:
            return default_values, "STALE_MICRO"

        trade_events = max(int(snapshot.trade_events), 0)
        book_events = max(int(snapshot.book_events), 0)
        trade_volume_total = 0.0
        if close_value > 0 and snapshot.trade_notional_krw > 0:
            trade_volume_total = float(snapshot.trade_notional_krw) / max(float(close_value), 1e-12)
        trade_imbalance = float(snapshot.trade_imbalance) if snapshot.trade_imbalance is not None else 0.0
        buy_ratio = min(max((trade_imbalance + 1.0) / 2.0, 0.0), 1.0)
        buy_volume = trade_volume_total * buy_ratio
        sell_volume = max(trade_volume_total - buy_volume, 0.0)

        source = str(snapshot.trade_source).strip().lower()
        source_value = 2.0 if source == "ws" else 1.0 if source == "rest" else 0.0
        source_ws = 1.0 if source == "ws" else 0.0
        source_rest = 1.0 if source == "rest" else 0.0

        depth_total = float(snapshot.depth_top5_notional_krw) if snapshot.depth_top5_notional_krw is not None else 0.0
        depth_bid = depth_total / 2.0
        depth_ask = depth_total / 2.0
        trade_available = 1.0 if trade_events > 0 else 0.0
        book_available = 1.0 if bool(snapshot.book_available) else 0.0
        micro_available = 1.0 if (trade_available > 0 or book_available > 0) else 0.0

        values = dict(default_values)
        values.update(
            {
                "m_trade_source": source_value,
                "m_trade_events": float(trade_events),
                "m_book_events": float(book_events),
                "m_trade_min_ts_ms": float(max(int(snapshot.last_event_ts_ms) - int(snapshot.trade_coverage_ms), 0)),
                "m_trade_max_ts_ms": float(int(snapshot.last_event_ts_ms)),
                "m_book_min_ts_ms": float(max(int(snapshot.last_event_ts_ms) - int(snapshot.book_coverage_ms), 0)),
                "m_book_max_ts_ms": float(int(snapshot.last_event_ts_ms)),
                "m_trade_coverage_ms": float(max(int(snapshot.trade_coverage_ms), 0)),
                "m_book_coverage_ms": float(max(int(snapshot.book_coverage_ms), 0)),
                "m_micro_trade_available": trade_available,
                "m_micro_book_available": book_available,
                "m_micro_available": micro_available,
                "m_trade_count": float(trade_events),
                "m_buy_count": float(round(trade_events * buy_ratio)),
                "m_sell_count": float(max(trade_events - round(trade_events * buy_ratio), 0)),
                "m_trade_volume_total": float(trade_volume_total),
                "m_buy_volume": float(buy_volume),
                "m_sell_volume": float(sell_volume),
                "m_trade_imbalance": float(trade_imbalance),
                "m_vwap": float(close_value),
                "m_avg_trade_size": float(trade_volume_total / trade_events) if trade_events > 0 else 0.0,
                "m_max_trade_size": float(trade_volume_total / trade_events) if trade_events > 0 else 0.0,
                "m_last_trade_price": float(close_value),
                "m_mid_mean": float(close_value),
                "m_spread_bps_mean": float(snapshot.spread_bps_mean) if snapshot.spread_bps_mean is not None else 0.0,
                "m_depth_bid_top5_mean": float(depth_bid),
                "m_depth_ask_top5_mean": float(depth_ask),
                "m_imbalance_top5_mean": 0.0,
                "m_microprice_bias_bps_mean": 0.0,
                "m_book_update_count": float(book_events),
                "m_spread_proxy": float(snapshot.spread_bps_mean) if snapshot.spread_bps_mean is not None else 0.0,
                "m_trade_volume_base": float(trade_volume_total),
                "m_trade_buy_ratio": float(buy_ratio) if trade_volume_total > 0 else 0.0,
                "m_signed_volume": float(buy_volume - sell_volume),
                "m_source_ws": float(source_ws),
                "m_source_rest": float(source_rest),
            }
        )
        return values, "OK"

    def _bootstrap_market(self, *, market: str, state: _MarketState) -> None:
        if state.candles_1m is None:
            state.candles_1m = deque(maxlen=self._max_1m_history)
        market_files = _market_files(dataset_root=self._candles_root, tf="1m", market=market)
        if not market_files:
            state.bootstrap_status = "MISSING"
            state.bootstrap_rows = 0
            return

        try:
            lazy = (
                pl.scan_parquet([str(path) for path in market_files])
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
                .sort("ts_ms")
                .unique(subset=["ts_ms"], keep="last", maintain_order=True)
                .tail(self._bootstrap_1m_bars)
            )
            frame = _collect_lazy(lazy)
        except Exception:
            frame = pl.DataFrame()

        if frame.height <= 0:
            state.bootstrap_status = "EMPTY"
            state.bootstrap_rows = 0
            return
        loaded_rows = 0
        for row in frame.iter_rows(named=True):
            ts_value = _safe_int(row.get("ts_ms"))
            open_v = _safe_float(row.get("open"))
            high_v = _safe_float(row.get("high"))
            low_v = _safe_float(row.get("low"))
            close_v = _safe_float(row.get("close"))
            vol_v = _safe_float(row.get("volume_base"))
            if ts_value is None or open_v is None or high_v is None or low_v is None or close_v is None or vol_v is None:
                continue
            state.candles_1m.append(
                _MinuteCandle(
                    ts_ms=int(ts_value),
                    open=float(open_v),
                    high=float(high_v),
                    low=float(low_v),
                    close=float(close_v),
                    volume_base=float(max(vol_v, 0.0)),
                    is_synth_1m=False,
                )
            )
            state.last_price = float(close_v)
            state.last_closed_price = float(close_v)
            loaded_rows += 1
        state.bootstrap_rows = int(loaded_rows)
        if loaded_rows <= 0:
            state.bootstrap_status = "EMPTY"
        elif loaded_rows < int(self._bootstrap_1m_bars):
            state.bootstrap_status = "PARTIAL"
        else:
            state.bootstrap_status = "OK"

    def _flush_active_until_ts(self, *, state: _MarketState, ts_ms: int) -> None:
        target_ts = int(ts_ms)
        while True:
            active = state.active
            if active is None:
                break
            minute_end = int(active.minute_start_ts_ms) + 60_000
            if minute_end > target_ts:
                break
            self._finalize_active_minute(state=state)
            state.active = None

        if state.candles_1m is None or not state.candles_1m:
            return
        last_ts = int(state.candles_1m[-1].ts_ms)
        if last_ts >= target_ts:
            return
        close_value = _safe_float(state.last_closed_price)
        if close_value is None or close_value <= 0:
            return
        next_ts = last_ts + 60_000
        min_next_ts = target_ts - max(int(self._lookback_1m_bars) - 1, 0) * 60_000
        if next_ts < min_next_ts:
            next_ts = min_next_ts
        while next_ts <= target_ts:
            self._append_synth_minute(state=state, ts_ms=next_ts, close=close_value)
            next_ts += 60_000

    def _finalize_active_minute(self, *, state: _MarketState) -> None:
        active = state.active
        if active is None:
            return
        if state.candles_1m is None:
            state.candles_1m = deque(maxlen=self._max_1m_history)
        candle = _MinuteCandle(
            ts_ms=int(active.minute_start_ts_ms) + 60_000,
            open=float(active.open),
            high=float(active.high),
            low=float(active.low),
            close=float(active.close),
            volume_base=max(float(active.volume_base), 0.0),
            is_synth_1m=False,
        )
        state.candles_1m.append(candle)
        state.last_price = float(candle.close)
        state.last_closed_price = float(candle.close)

    def _append_synth_minute(self, *, state: _MarketState, ts_ms: int, close: float) -> None:
        if state.candles_1m is None:
            state.candles_1m = deque(maxlen=self._max_1m_history)
        if state.candles_1m and int(state.candles_1m[-1].ts_ms) >= int(ts_ms):
            return
        synth = _MinuteCandle(
            ts_ms=int(ts_ms),
            open=float(close),
            high=float(close),
            low=float(close),
            close=float(close),
            volume_base=0.0,
            is_synth_1m=True,
        )
        state.candles_1m.append(synth)
        state.last_closed_price = float(close)

    def _build_one_m_frame(self, *, state: _MarketState, ts_ms: int) -> pl.DataFrame:
        records: list[dict[str, Any]] = []
        cutoff_ts_ms = int(ts_ms) - int(self._lookback_1m_bars) * 60_000
        if state.candles_1m is not None:
            for candle in state.candles_1m:
                if int(candle.ts_ms) <= int(ts_ms) and int(candle.ts_ms) >= int(cutoff_ts_ms):
                    records.append(
                        {
                            "ts_ms": int(candle.ts_ms),
                            "open": float(candle.open),
                            "high": float(candle.high),
                            "low": float(candle.low),
                            "close": float(candle.close),
                            "volume_base": float(candle.volume_base),
                            "is_synth_1m": bool(candle.is_synth_1m),
                        }
                    )

        if records:
            records.sort(key=lambda item: int(item["ts_ms"]))
            last_ts = int(records[-1]["ts_ms"])
            if last_ts < int(ts_ms):
                last_close = float(records[-1]["close"])
                records.append(
                    {
                        "ts_ms": int(ts_ms),
                        "open": float(last_close),
                        "high": float(last_close),
                        "low": float(last_close),
                        "close": float(last_close),
                        "volume_base": 0.0,
                        "is_synth_1m": True,
                    }
                )
            return pl.DataFrame(records).sort("ts_ms")

        price = _safe_float(state.last_closed_price)
        if price is None or price <= 0:
            active = state.active
            if active is None or (int(active.minute_start_ts_ms) + 60_000) <= int(ts_ms):
                price = _safe_float(state.last_price)
        if price is None or price <= 0:
            return pl.DataFrame(
                schema={
                    "ts_ms": pl.Int64,
                    "open": pl.Float64,
                    "high": pl.Float64,
                    "low": pl.Float64,
                    "close": pl.Float64,
                    "volume_base": pl.Float64,
                    "is_synth_1m": pl.Boolean,
                }
            )
        first_ts = int(ts_ms) - (int(self._fallback_synth_1m_bars) - 1) * 60_000
        synth_rows: list[dict[str, Any]] = []
        for index in range(int(self._fallback_synth_1m_bars)):
            minute_end = int(first_ts) + index * 60_000
            synth_rows.append(
                {
                    "ts_ms": int(minute_end),
                    "open": float(price),
                    "high": float(price),
                    "low": float(price),
                    "close": float(price),
                    "volume_base": 0.0,
                    "is_synth_1m": True,
                }
            )
        return pl.DataFrame(synth_rows).sort("ts_ms")


def _rollup_from_1m(*, one_m: pl.DataFrame, tf: str) -> pl.DataFrame:
    if one_m.height <= 0:
        return pl.DataFrame(
            schema={
                "ts_ms": pl.Int64,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume_base": pl.Float64,
            }
        )
    interval_ms = int(expected_interval_ms(tf))
    grouped = (
        one_m.sort("ts_ms")
        .with_columns(
            (((pl.col("ts_ms") // interval_ms) * interval_ms) + interval_ms).cast(pl.Int64).alias("__ts_tf")
        )
        .group_by("__ts_tf")
        .agg(
            [
                pl.col("open").first().cast(pl.Float64).alias("open"),
                pl.col("high").max().cast(pl.Float64).alias("high"),
                pl.col("low").min().cast(pl.Float64).alias("low"),
                pl.col("close").last().cast(pl.Float64).alias("close"),
                pl.col("volume_base").sum().cast(pl.Float64).alias("volume_base"),
            ]
        )
        .rename({"__ts_tf": "ts_ms"})
        .sort("ts_ms")
    )
    return grouped


def _rows_to_frame(*, rows: list[dict[str, Any]], feature_columns: Sequence[str], extra_columns: Sequence[str] = ()) -> pl.DataFrame:
    schema: dict[str, pl.DataType] = {"ts_ms": pl.Int64, "market": pl.Utf8}
    feature_col_set = {str(col) for col in feature_columns}
    for col in feature_columns:
        schema[str(col)] = pl.Float32
    for col in extra_columns:
        name = str(col)
        if name == "close" or name in feature_col_set:
            continue
        schema[name] = pl.Float64
    schema["close"] = pl.Float64

    if not rows:
        return pl.DataFrame(schema=schema)
    frame = pl.DataFrame(rows)
    ordered = [
        "ts_ms",
        "market",
        *[str(col) for col in feature_columns],
        *[str(col) for col in extra_columns if str(col) != "close" and str(col) not in feature_col_set],
        "close",
    ]
    present = [name for name in ordered if name in frame.columns]
    return frame.select(present).sort("market")


def _resolve_extra_row_value(*, column: str, base_row: dict[str, Any], aux_row: dict[str, Any], close_value: float) -> float | None:
    name = str(column).strip()
    if not name:
        return None
    if name in aux_row:
        return _safe_float(aux_row.get(name))
    if name in base_row:
        return _safe_float(base_row.get(name))
    if name == "rv_12":
        return _safe_float(base_row.get("vol_12"))
    if name == "rv_36":
        return _safe_float(base_row.get("vol_36"))
    if name == "atr_pct_14":
        atr = _safe_float(aux_row.get("atr_14"))
        if atr is None or close_value <= 0.0:
            return None
        return float(atr) / float(close_value)
    return None


def _attach_runtime_aux_columns(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.height <= 0:
        return frame
    return frame.sort("ts_ms").with_columns(
        [
            pl.max_horizontal(
                [
                    (pl.col("high") - pl.col("low")),
                    (pl.col("high") - pl.col("close").shift(1)).abs(),
                    (pl.col("low") - pl.col("close").shift(1)).abs(),
                ]
            ).alias("__true_range"),
        ]
    ).with_columns(
        [
            pl.col("__true_range").rolling_mean(window_size=14, min_samples=14).alias("atr_14"),
            (pl.col("__true_range").rolling_mean(window_size=14, min_samples=14) / pl.col("close")).alias("atr_pct_14"),
        ]
    ).drop("__true_range")


def _default_micro_feature_values() -> dict[str, float]:
    values: dict[str, float] = {}
    for name in prefixed_micro_columns():
        values[name] = 0.0
    values.update(
        {
            "m_spread_proxy": 0.0,
            "m_trade_volume_base": 0.0,
            "m_trade_buy_ratio": 0.0,
            "m_signed_volume": 0.0,
            "m_source_ws": 0.0,
            "m_source_rest": 0.0,
        }
    )
    return values


def _resolve_dataset_path(*, parquet_root: Path, dataset_name: str) -> Path:
    candidate = Path(str(dataset_name).strip())
    if candidate.exists() or candidate.is_absolute():
        return candidate
    return parquet_root / candidate


def _market_files(*, dataset_root: Path, tf: str, market: str) -> list[Path]:
    market_dir = dataset_root / f"tf={str(tf).strip().lower()}" / f"market={str(market).strip().upper()}"
    if not market_dir.exists():
        return []
    direct = sorted(path for path in market_dir.glob("part-*.parquet") if path.is_file())
    if direct:
        return direct
    legacy = market_dir / "part.parquet"
    if legacy.exists():
        return [legacy]
    nested: list[Path] = []
    for date_dir in sorted(market_dir.glob("date=*")):
        if not date_dir.is_dir():
            continue
        nested.extend(sorted(path for path in date_dir.glob("*.parquet") if path.is_file()))
    if nested:
        return nested
    return sorted(path for path in market_dir.rglob("*.parquet") if path.is_file())


def _estimate_volume_base_from_ticker(
    *,
    trade_price: float,
    acc_trade_price_24h: float,
    prev_acc_trade_price_24h: float | None,
) -> float:
    price = max(float(trade_price), 1e-12)
    current_acc = max(float(acc_trade_price_24h), 0.0)
    if prev_acc_trade_price_24h is None:
        notional_delta = max(price * 1e-8, 1e-8)
    elif current_acc >= float(prev_acc_trade_price_24h):
        notional_delta = max(current_acc - float(prev_acc_trade_price_24h), 0.0)
        if notional_delta <= 0:
            notional_delta = max(price * 1e-8, 1e-8)
    else:
        notional_delta = max(price * 1e-8, 1e-8)
    return max(notional_delta / price, 1e-12)


def _to_feature_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    try:
        casted = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(casted):
        return None
    return casted


def _normalize_markets(markets: Sequence[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in markets:
        market = str(raw).strip().upper()
        if not market or market in seen:
            continue
        seen.add(market)
        out.append(market)
    return tuple(out)


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            return float(text)
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            return int(float(text))
        return int(value)
    except (TypeError, ValueError):
        return None


def _collect_lazy(lazy_frame: pl.LazyFrame) -> pl.DataFrame:
    try:
        return lazy_frame.collect(engine="streaming")
    except TypeError:
        return lazy_frame.collect(streaming=True)
