"""Shared multi-timeframe runtime row builder for live V3/V4 providers."""

from __future__ import annotations

from collections import Counter
from typing import Any, Sequence

import polars as pl

from autobot.data import expected_interval_ms
from autobot.features.feature_blocks_v4_live_base import compute_base_features_v4_live_base
from autobot.features.multitf_join_v1 import (
    aggregate_1m_for_base,
    compute_high_tf_features,
    densify_1m_candles,
    join_1m_aggregate,
    join_high_tf_asof,
)

from .live_features_online_core import (
    _MarketState,
    _OnlineMinuteRuntimeCore,
    _attach_runtime_aux_columns,
    _normalize_markets,
    _resolve_extra_row_value,
    _rollup_from_1m,
    _rows_to_frame,
    _safe_float,
    _to_feature_float,
)


class _LiveMultiTfRuntimeBase(_OnlineMinuteRuntimeCore):
    """Shared multi-timeframe base-row builder on top of the online core."""

    def __init__(
        self,
        *,
        tf: str,
        high_tfs: Sequence[str],
        micro_snapshot_provider: Any,
        micro_max_age_ms: int,
        parquet_root: str,
        candles_dataset_name: str,
        bootstrap_1m_bars: int,
        bootstrap_end_ts_ms: int | None = None,
        max_1m_history: int = 5000,
        context_micro_required: bool = False,
        context_history_bars: int = 1,
        missing_feature_warn_ratio: float = 0.05,
        missing_feature_skip_ratio: float = 0.20,
    ) -> None:
        super().__init__(
            tf=tf,
            micro_snapshot_provider=micro_snapshot_provider,
            micro_max_age_ms=micro_max_age_ms,
            parquet_root=parquet_root,
            candles_dataset_name=candles_dataset_name,
            bootstrap_1m_bars=bootstrap_1m_bars,
            bootstrap_end_ts_ms=bootstrap_end_ts_ms,
            max_1m_history=max_1m_history,
        )
        self._high_tfs = tuple(str(item).strip().lower() for item in high_tfs if str(item).strip())
        self._context_micro_required = bool(context_micro_required)
        self._context_history_bars = max(int(context_history_bars), 1)
        self._missing_feature_warn_ratio = min(max(float(missing_feature_warn_ratio), 0.0), 1.0)
        self._missing_feature_skip_ratio = min(max(float(missing_feature_skip_ratio), 0.0), 1.0)

    def _build_runtime_market_row(
        self,
        *,
        market: str,
        ts_ms: int,
        feature_columns: Sequence[str],
        extra_columns: Sequence[str] = (),
        missing_feature_skip_ratio: float | None = None,
    ) -> tuple[dict[str, Any] | None, str, int, tuple[str, ...]]:
        state = self._market_state.setdefault(market, _MarketState())
        if state.candles_1m is None:
            from collections import deque

            state.candles_1m = deque(maxlen=self._max_1m_history)
        if not state.bootstrap_loaded:
            self._bootstrap_market(market=market, state=state)
            state.bootstrap_loaded = True

        self._flush_active_until_ts(state=state, ts_ms=ts_ms)
        one_m = self._build_one_m_frame(state=state, ts_ms=ts_ms)
        if one_m.height <= 0:
            return None, "NO_1M_HISTORY", 0, ()

        base = self._resolve_runtime_tf_frame(
            market=market,
            state=state,
            tf=self._tf,
            ts_ms=ts_ms,
            one_m=one_m,
        )
        if base.height <= 0:
            return None, "NO_BASE_CANDLE", 0, ()

        base_featured = compute_base_features_v4_live_base(base, tf=self._tf, float_dtype="float32").sort("ts_ms")
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
            high_candles = self._resolve_runtime_tf_frame(
                market=market,
                state=state,
                tf=high_tf,
                ts_ms=ts_ms,
                one_m=one_m,
            )
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
        for col in feature_columns:
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
            out[str(col)] = float(normalized)
        for col in extra_columns:
            col_name = str(col)
            if col_name == "close":
                continue
            out[col_name] = _resolve_extra_row_value(
                column=col_name,
                base_row=base_row,
                aux_row=aux_row,
                close_value=float(close_value),
            )
        feature_count = max(len(tuple(feature_columns)), 1)
        missing_ratio = float(missing_features) / float(feature_count)
        limit = (
            self._missing_feature_skip_ratio
            if missing_feature_skip_ratio is None
            else min(max(float(missing_feature_skip_ratio), 0.0), 1.0)
        )
        if missing_ratio > float(limit):
            return None, "MISSING_FEATURE_RATIO_HIGH", missing_features, tuple(sorted(set(missing_columns)))
        return out, micro_reason, missing_features, tuple(sorted(set(missing_columns)))

    def _resolve_runtime_tf_frame(
        self,
        *,
        market: str,
        state: _MarketState,
        tf: str,
        ts_ms: int,
        one_m: pl.DataFrame,
    ) -> pl.DataFrame:
        tf_value = str(tf).strip().lower()
        ts_value = int(ts_ms)
        interval_ms = max(int(expected_interval_ms(tf_value)), 1)
        tail_bars = max(256, int(self._bootstrap_1m_bars // interval_ms) + 8)

        canonical = (
            self._load_canonical_tf_frame(
                market=market,
                state=state,
                tf=tf_value,
                tail_bars=tail_bars,
            )
            .filter(pl.col("ts_ms") <= ts_value)
            .sort("ts_ms")
        )
        rolled = _rollup_from_1m(one_m=one_m, tf=tf_value).filter(pl.col("ts_ms") <= ts_value).sort("ts_ms")

        if canonical.height <= 0:
            return rolled
        if rolled.height <= 0:
            return canonical

        latest_canonical_ts = int(canonical.get_column("ts_ms").max())
        if latest_canonical_ts >= ts_value:
            return canonical

        rolled = rolled.filter(pl.col("ts_ms") > latest_canonical_ts)
        if rolled.height <= 0:
            return canonical
        return (
            pl.concat([canonical, rolled], how="vertical_relaxed")
            .sort("ts_ms")
            .unique(subset=["ts_ms"], keep="last", maintain_order=True)
        )

    def _filter_context_for_micro_contract(self, frame: pl.DataFrame) -> tuple[pl.DataFrame, dict[str, Any]]:
        if not self._context_micro_required or frame.height <= 0 or "m_micro_available" not in frame.columns:
            return frame, {
                "context_micro_required": bool(self._context_micro_required),
                "context_rows_before_micro_filter": int(frame.height),
                "context_rows_after_micro_filter": int(frame.height),
                "context_rows_dropped_no_micro": 0,
            }
        filtered = frame.filter(pl.col("m_micro_available").cast(pl.Float64) > 0.0)
        return filtered, {
            "context_micro_required": True,
            "context_rows_before_micro_filter": int(frame.height),
            "context_rows_after_micro_filter": int(filtered.height),
            "context_rows_dropped_no_micro": int(max(frame.height - filtered.height, 0)),
        }

    def _build_runtime_context_frame(
        self,
        *,
        ts_ms: int,
        markets: Sequence[str],
        feature_columns: Sequence[str],
        extra_columns: Sequence[str] = (),
        provider_name: str,
        missing_feature_warn_ratio: float | None = None,
        missing_feature_skip_ratio: float | None = None,
        history_bars: int | None = None,
    ) -> tuple[pl.DataFrame, dict[str, Any]]:
        ts_value = int(ts_ms)
        requested = _normalize_markets(markets)
        self._last_requested_ts_ms = ts_value
        history_window = max(int(history_bars or self._context_history_bars), 1)
        rows: list[dict[str, Any]] = []
        built_markets: list[str] = []
        skipped_markets: list[str] = []
        skip_reasons: dict[str, int] = {}
        built_reasons: dict[str, int] = {}
        missing_feature_counter: Counter[str] = Counter()
        warn_missing_feature_market_count = 0
        missing_feature_cells_total = 0
        warn_ratio = (
            self._missing_feature_warn_ratio
            if missing_feature_warn_ratio is None
            else min(max(float(missing_feature_warn_ratio), 0.0), 1.0)
        )
        skip_ratio = (
            self._missing_feature_skip_ratio
            if missing_feature_skip_ratio is None
            else min(max(float(missing_feature_skip_ratio), 0.0), 1.0)
        )

        for market in requested:
            state = self._market_state.setdefault(market, _MarketState())
            if state.candles_1m is None:
                from collections import deque

                state.candles_1m = deque(maxlen=self._max_1m_history)
            if not state.bootstrap_loaded:
                self._bootstrap_market(market=market, state=state)
                state.bootstrap_loaded = True
            self._flush_active_until_ts(state=state, ts_ms=ts_value)
            one_m = self._build_one_m_frame(state=state, ts_ms=ts_value)
            if one_m.height <= 0:
                skipped_markets.append(market)
                skip_reasons["NO_1M_HISTORY"] = int(skip_reasons.get("NO_1M_HISTORY", 0)) + 1
                continue
            base = self._resolve_runtime_tf_frame(
                market=market,
                state=state,
                tf=self._tf,
                ts_ms=ts_value,
                one_m=one_m,
            )
            if base.height <= 0:
                skipped_markets.append(market)
                skip_reasons["NO_BASE_CANDLE"] = int(skip_reasons.get("NO_BASE_CANDLE", 0)) + 1
                continue
            ts_candidates = [
                int(value)
                for value in base.get_column("ts_ms").drop_nulls().to_list()
                if int(value) <= ts_value
            ]
            history_ts_values = ts_candidates[-history_window:]
            if not history_ts_values:
                skipped_markets.append(market)
                skip_reasons["NO_FEATURE_ROW_AT_TS"] = int(skip_reasons.get("NO_FEATURE_ROW_AT_TS", 0)) + 1
                continue
            built_current = False
            for history_ts in history_ts_values:
                row, reason, missing_feature_cells, missing_features = self._build_runtime_market_row(
                    market=market,
                    ts_ms=int(history_ts),
                    feature_columns=feature_columns,
                    extra_columns=extra_columns,
                    missing_feature_skip_ratio=skip_ratio,
                )
                missing_feature_cells_total += int(missing_feature_cells)
                for name in missing_features:
                    missing_feature_counter[str(name)] += 1
                feature_count = max(len(tuple(feature_columns)), 1)
                missing_ratio = float(missing_feature_cells) / float(feature_count)
                if missing_ratio > float(warn_ratio):
                    warn_missing_feature_market_count += 1
                if row is None:
                    if int(history_ts) == ts_value:
                        skipped_markets.append(market)
                        skip_reasons[reason] = int(skip_reasons.get(reason, 0)) + 1
                    continue
                if int(history_ts) == ts_value:
                    built_current = True
                if reason != "OK":
                    built_reasons[reason] = int(built_reasons.get(reason, 0)) + 1
                rows.append(row)
            if built_current:
                built_markets.append(market)
            elif market not in skipped_markets:
                skipped_markets.append(market)
                skip_reasons["NO_FEATURE_ROW_AT_TS"] = int(skip_reasons.get("NO_FEATURE_ROW_AT_TS", 0)) + 1

        total_feature_cells = max(len(tuple(feature_columns)), 1) * max(len(requested), 1)
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

        frame = _rows_to_frame(rows=rows, feature_columns=feature_columns, extra_columns=extra_columns)
        if frame.height > 0 and int(frame.get_column("ts_ms").max()) == ts_value:
            self._latest_feature_ts_ms = ts_value
            self._last_built_ts_ms = ts_value
        else:
            self._last_built_ts_ms = None
        stats = {
            "provider": str(provider_name),
            "requested_ts_ms": ts_value,
            "built_ts_ms": int(self._last_built_ts_ms) if self._last_built_ts_ms is not None else None,
            "requested_markets": int(len(requested)),
            "built_markets": int(len(built_markets)),
            "skipped_markets": int(len(skipped_markets)),
            "built_rows": int(frame.filter(pl.col("ts_ms") == ts_value).height if frame.height > 0 and "ts_ms" in frame.columns else 0),
            "context_history_bars": int(history_window),
            "context_panel_rows": int(frame.height),
            "built_market_samples": list(built_markets[:20]),
            "skipped_market_samples": list(skipped_markets[:20]),
            "skip_reasons": dict(sorted(skip_reasons.items(), key=lambda item: str(item[0]))),
            "built_reasons": dict(sorted(built_reasons.items(), key=lambda item: str(item[0]))),
            "missing_feature_cells_total": int(missing_feature_cells_total),
            "missing_feature_ratio": float(missing_feature_ratio),
            "missing_feature_warn_market_count": int(warn_missing_feature_market_count),
            "missing_feature_topk": missing_feature_topk,
            "missing_feature_warn_ratio_threshold": float(warn_ratio),
            "missing_feature_skip_ratio_threshold": float(skip_ratio),
            "bootstrap_missing_markets_count": int(bootstrap_missing),
            "bootstrap_partial_markets_count": int(bootstrap_partial),
        }
        return frame, stats

    def _build_runtime_frame(
        self,
        *,
        ts_ms: int,
        markets: Sequence[str],
        feature_columns: Sequence[str],
        extra_columns: Sequence[str] = (),
        provider_name: str,
        missing_feature_warn_ratio: float | None = None,
        missing_feature_skip_ratio: float | None = None,
    ) -> tuple[pl.DataFrame, dict[str, Any]]:
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
        warn_ratio = (
            self._missing_feature_warn_ratio
            if missing_feature_warn_ratio is None
            else min(max(float(missing_feature_warn_ratio), 0.0), 1.0)
        )
        skip_ratio = (
            self._missing_feature_skip_ratio
            if missing_feature_skip_ratio is None
            else min(max(float(missing_feature_skip_ratio), 0.0), 1.0)
        )

        for market in requested:
            row, reason, missing_feature_cells, missing_features = self._build_runtime_market_row(
                market=market,
                ts_ms=ts_value,
                feature_columns=feature_columns,
                extra_columns=extra_columns,
                missing_feature_skip_ratio=skip_ratio,
            )
            missing_feature_cells_total += int(missing_feature_cells)
            for name in missing_features:
                missing_feature_counter[str(name)] += 1
            feature_count = max(len(tuple(feature_columns)), 1)
            missing_ratio = float(missing_feature_cells) / float(feature_count)
            if missing_ratio > float(warn_ratio):
                warn_missing_feature_market_count += 1
            if row is None:
                skipped_markets.append(market)
                skip_reasons[reason] = int(skip_reasons.get(reason, 0)) + 1
                continue
            built_markets.append(market)
            if reason != "OK":
                built_reasons[reason] = int(built_reasons.get(reason, 0)) + 1
            rows.append(row)

        total_feature_cells = max(len(tuple(feature_columns)), 1) * max(len(requested), 1)
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
        frame = _rows_to_frame(rows=rows, feature_columns=feature_columns, extra_columns=extra_columns)
        stats = {
            "provider": str(provider_name),
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
            "missing_feature_warn_ratio_threshold": float(warn_ratio),
            "missing_feature_skip_ratio_threshold": float(skip_ratio),
            "bootstrap_missing_markets_count": int(bootstrap_missing),
            "bootstrap_partial_markets_count": int(bootstrap_partial),
        }
        return frame, stats
