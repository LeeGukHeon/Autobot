"""LIVE_V5 runtime builder for sequence/LOB/fusion families."""

from __future__ import annotations

from collections import Counter, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import polars as pl

from autobot.data.collect.sequence_tensor_store import (
    _build_lob_tensor,
    _build_micro_tensor,
    _build_minute_tensor,
    _build_second_tensor,
    _resolve_context_end_ts_ms,
)
from autobot.models.predictor import ModelPredictor, load_predictor_from_registry
from autobot.models.train_v5_lob import _build_pooled_lob_features, _pooled_lob_feature_names
from autobot.models.train_v5_sequence import (
    _build_known_covariates,
    _build_pooled_sequence_features,
    _pooled_feature_names,
)
from autobot.strategy.micro_snapshot import MicroSnapshot, MicroSnapshotProvider
from autobot.upbit.ws.models import TickerEvent

from .live_features_online_core import _estimate_volume_base_from_ticker, _market_files, _safe_float
from .live_features_v4_native import LiveFeatureProviderV4Native


@dataclass
class _SecondCandleState:
    second_start_ts_ms: int
    open: float
    high: float
    low: float
    close: float
    volume_base: float
    volume_quote: float


class LiveFeatureProviderV5:
    """Build runtime rows for v5 expert and fusion families on live feeds."""

    def __init__(
        self,
        *,
        predictor: ModelPredictor,
        registry_root: str | Path,
        feature_columns: Sequence[str],
        extra_columns: Sequence[str] = (),
        tf: str = "5m",
        quote: str = "KRW",
        micro_snapshot_provider: MicroSnapshotProvider | None = None,
        micro_max_age_ms: int = 300_000,
        parquet_root: str | Path = "data/parquet",
        candles_dataset_name: str = "candles_api_v1",
        bootstrap_1m_bars: int = 2_000,
        bootstrap_end_ts_ms: int | None = None,
        max_1m_history: int = 5_000,
        second_dataset_name: str = "candles_second_v1",
        ws_candle_dataset_name: str = "ws_candle_v1",
        micro_dataset_name: str = "micro_v1",
        lob_dataset_name: str = "lob30_v1",
    ) -> None:
        self._predictor = predictor
        self._registry_root = Path(registry_root)
        self._feature_columns = tuple(str(col).strip() for col in feature_columns if str(col).strip())
        self._extra_columns = tuple(str(col).strip() for col in extra_columns if str(col).strip())
        self._tf = str(tf).strip().lower() or "5m"
        self._quote = str(quote).strip().upper() or "KRW"
        self._parquet_root = Path(parquet_root)
        self._micro_snapshot_provider = micro_snapshot_provider
        self._micro_max_age_ms = max(int(micro_max_age_ms), 0)
        self._second_root = Path(self._parquet_root) / second_dataset_name
        self._ws_candle_root = Path(self._parquet_root) / ws_candle_dataset_name
        self._micro_root = Path(self._parquet_root) / micro_dataset_name
        self._lob_root = Path(self._parquet_root) / lob_dataset_name
        self._sequence_feature_names = tuple(_pooled_feature_names())
        self._lob_feature_names = tuple(_pooled_lob_feature_names())
        self._last_build_stats: dict[str, Any] = {}
        self._last_requested_ts_ms: int | None = None
        self._last_built_ts_ms: int | None = None

        self._mode = self._resolve_mode()
        self._panel_predictor: ModelPredictor | None = None
        self._sequence_predictor: ModelPredictor | None = None
        self._lob_predictor: ModelPredictor | None = None
        self._configure_child_predictors()

        base_feature_columns: Sequence[str] = ()
        if self._panel_predictor is not None:
            base_feature_columns = tuple(self._panel_predictor.feature_columns)
        self._base_provider = LiveFeatureProviderV4Native(
            feature_columns=base_feature_columns,
            extra_columns=self._extra_columns,
            tf=self._tf,
            quote=self._quote,
            micro_snapshot_provider=micro_snapshot_provider,
            micro_max_age_ms=self._micro_max_age_ms,
            parquet_root=self._parquet_root,
            candles_dataset_name=candles_dataset_name,
            bootstrap_1m_bars=bootstrap_1m_bars,
            bootstrap_end_ts_ms=bootstrap_end_ts_ms,
            max_1m_history=max_1m_history,
        )

        self._second_history: dict[str, deque[dict[str, float]]] = {}
        self._second_active: dict[str, _SecondCandleState] = {}
        self._second_bootstrapped: set[str] = set()
        self._micro_history: dict[str, deque[dict[str, float]]] = {}
        self._micro_bootstrapped: set[str] = set()
        self._lob_history: dict[str, deque[dict[str, float]]] = {}
        self._lob_bootstrapped: set[str] = set()
        self._last_micro_bucket_ts_ms: dict[str, int] = {}
        self._last_lob_event_ts_ms: dict[str, int] = {}
        self._second_history_len = 256
        self._micro_history_len = 256
        self._lob_history_len = 512

    def _resolve_mode(self) -> str:
        family = str(self._predictor.model_family or "").strip().lower()
        if family == "train_v5_fusion" or any(
            str(col).startswith(("panel_", "sequence_", "lob_")) for col in self._feature_columns
        ):
            return "FUSION"
        if family == "train_v5_sequence":
            return "SEQUENCE"
        if family == "train_v5_lob":
            return "LOB"
        return "PANEL"

    def _configure_child_predictors(self) -> None:
        if self._mode == "SEQUENCE":
            self._sequence_predictor = self._predictor
            return
        if self._mode == "LOB":
            self._lob_predictor = self._predictor
            return
        if self._mode != "FUSION":
            return
        fusion_contract = self._load_json(self._predictor.run_dir / "fusion_model_contract.json")
        input_experts = dict(fusion_contract.get("input_experts") or {})
        self._panel_predictor = self._load_predictor_from_input_path(input_experts.get("panel"))
        self._sequence_predictor = self._load_predictor_from_input_path(input_experts.get("sequence"))
        self._lob_predictor = self._load_predictor_from_input_path(input_experts.get("lob"))

    def _load_predictor_from_input_path(self, path_value: Any) -> ModelPredictor | None:
        text = str(path_value or "").strip()
        if not text:
            return None
        input_path = Path(text)
        run_dir = input_path.parent
        if not run_dir.exists():
            return None
        family = run_dir.parent.name if run_dir.parent is not None else ""
        run_id = run_dir.name
        if not family or not run_id:
            return None
        return load_predictor_from_registry(
            registry_root=self._registry_root,
            model_ref=run_id,
            model_family=family,
        )

    def ingest_ticker(self, event: TickerEvent) -> None:
        self._base_provider.ingest_ticker(event)
        market = str(event.market).strip().upper()
        if not market:
            return
        price = _safe_float(event.trade_price)
        if price is None or price <= 0.0:
            return
        ts_ms = int(event.ts_ms)
        volume_base = _estimate_volume_base_from_ticker(
            trade_price=float(price),
            acc_trade_price_24h=float(_safe_float(event.acc_trade_price_24h) or 0.0),
            prev_acc_trade_price_24h=self._resolve_prev_acc_trade_price_24h(market=market),
        )
        second_start_ts_ms = (ts_ms // 1_000) * 1_000
        active = self._second_active.get(market)
        if active is None:
            self._second_active[market] = _SecondCandleState(
                second_start_ts_ms=second_start_ts_ms,
                open=float(price),
                high=float(price),
                low=float(price),
                close=float(price),
                volume_base=float(volume_base),
                volume_quote=float(volume_base * float(price)),
            )
            return
        if second_start_ts_ms < int(active.second_start_ts_ms):
            return
        if second_start_ts_ms == int(active.second_start_ts_ms):
            active.high = max(float(active.high), float(price))
            active.low = min(float(active.low), float(price))
            active.close = float(price)
            active.volume_base = float(active.volume_base) + float(volume_base)
            active.volume_quote = float(active.volume_quote) + float(volume_base * float(price))
            return

        self._finalize_active_second(market=market, fill_gap_until_ts_ms=second_start_ts_ms - 1_000)
        self._second_active[market] = _SecondCandleState(
            second_start_ts_ms=second_start_ts_ms,
            open=float(price),
            high=float(price),
            low=float(price),
            close=float(price),
            volume_base=float(volume_base),
            volume_quote=float(volume_base * float(price)),
        )

    def build_frame(self, *, ts_ms: int, markets: Sequence[str]) -> pl.DataFrame:
        ts_value = int(ts_ms)
        self._last_requested_ts_ms = ts_value
        base_frame = self._base_provider.build_frame(ts_ms=ts_value, markets=markets)
        rows_by_market = {
            str(row.get("market", "")).strip().upper(): row
            for row in base_frame.iter_rows(named=True)
            if str(row.get("market", "")).strip()
        }
        requested = tuple(str(item).strip().upper() for item in markets if str(item).strip())
        rows: list[dict[str, Any]] = []
        skip_reasons: Counter[str] = Counter()

        for market in requested:
            base_row = rows_by_market.get(market)
            if base_row is None:
                skip_reasons["NO_BASE_FEATURE_ROW"] += 1
                continue
            self._ensure_market_bootstrap(market=market, ts_ms=ts_value)
            self._flush_active_second(market=market, target_ts_ms=ts_value)
            self._update_live_histories(market=market, ts_ms=ts_value)

            if self._mode == "SEQUENCE":
                feature_values = self._build_sequence_feature_values(market=market, ts_ms=ts_value)
            elif self._mode == "LOB":
                feature_values = self._build_lob_feature_values(market=market, ts_ms=ts_value)
            elif self._mode == "FUSION":
                feature_values = self._build_fusion_feature_values(market=market, ts_ms=ts_value, base_row=base_row)
            else:
                feature_values = {}

            if not feature_values:
                skip_reasons["V5_FEATURE_VALUES_UNAVAILABLE"] += 1
                continue

            row_payload: dict[str, Any] = {
                "ts_ms": ts_value,
                "market": market,
                "close": base_row.get("close"),
            }
            for name in self._extra_columns:
                if str(name) == "close":
                    continue
                row_payload[str(name)] = base_row.get(str(name))
            missing_columns: list[str] = []
            for name in self._feature_columns:
                column = str(name)
                if column not in feature_values:
                    missing_columns.append(column)
                    row_payload[column] = 0.0
                else:
                    row_payload[column] = feature_values[column]
            if missing_columns:
                skip_reasons["MISSING_V5_FEATURE_COLUMNS"] += 1
                continue
            rows.append(row_payload)

        if rows:
            self._last_built_ts_ms = ts_value
        else:
            self._last_built_ts_ms = None

        if rows:
            frame = pl.DataFrame(rows).sort(["market", "ts_ms"])
        else:
            schema: dict[str, pl.DataType] = {
                "ts_ms": pl.Int64,
                "market": pl.Utf8,
                "close": pl.Float64,
            }
            for name in self._extra_columns:
                if str(name) != "close":
                    schema[str(name)] = pl.Float64
            for name in self._feature_columns:
                schema[str(name)] = pl.Float64
            frame = pl.DataFrame(schema=schema)
        self._last_build_stats = {
            "provider": "LIVE_V5",
            "mode": self._mode,
            "requested_ts_ms": ts_value,
            "built_ts_ms": self._last_built_ts_ms,
            "requested_markets": len(requested),
            "built_rows": int(frame.height),
            "skip_reasons": dict(sorted(skip_reasons.items(), key=lambda item: str(item[0]))),
            "base_provider_stats": dict(getattr(self._base_provider, "_last_build_stats", {}) or {}),
            "primary_model_family": str(self._predictor.model_family or ""),
        }
        return frame

    def status(self, *, now_ts_ms: int, requested_ts_ms: int | None = None) -> dict[str, Any]:
        payload = self._base_provider.status(now_ts_ms=now_ts_ms, requested_ts_ms=requested_ts_ms)
        payload["provider"] = "LIVE_V5"
        payload["mode"] = self._mode
        payload["built_ts_ms"] = self._last_built_ts_ms
        payload["requested_ts_ms"] = int(requested_ts_ms) if requested_ts_ms is not None else self._last_requested_ts_ms
        payload["primary_model_family"] = str(self._predictor.model_family or "")
        return payload

    def _resolve_prev_acc_trade_price_24h(self, *, market: str) -> float | None:
        state = self._base_provider._market_state.get(market)  # noqa: SLF001
        if state is None:
            return None
        return _safe_float(getattr(state, "prev_acc_trade_price_24h", None))

    def _ensure_market_bootstrap(self, *, market: str, ts_ms: int) -> None:
        if market not in self._second_bootstrapped:
            self._second_history[market] = self._load_second_bootstrap(market=market, ts_ms=ts_ms)
            self._second_bootstrapped.add(market)
        if market not in self._micro_bootstrapped:
            self._micro_history[market] = self._load_micro_bootstrap(market=market, ts_ms=ts_ms)
            self._micro_bootstrapped.add(market)
            if self._micro_history[market]:
                self._last_micro_bucket_ts_ms[market] = int(self._micro_history[market][-1]["ts_ms"])
        if market not in self._lob_bootstrapped:
            self._lob_history[market] = self._load_lob_bootstrap(market=market, ts_ms=ts_ms)
            self._lob_bootstrapped.add(market)
            if self._lob_history[market]:
                self._last_lob_event_ts_ms[market] = int(self._lob_history[market][-1]["ts_ms"])

    def _load_second_bootstrap(self, *, market: str, ts_ms: int) -> deque[dict[str, float]]:
        rows: list[dict[str, Any]] = []
        for market_dir in (
            self._second_root / "tf=1s" / f"market={market}",
            self._ws_candle_root / "tf=1s" / f"market={market}",
        ):
            frame = self._load_market_frame(
                market_dir=market_dir,
                end_ts_ms=ts_ms,
                wanted_columns=("ts_ms", "close", "volume_base", "volume_quote"),
            )
            if frame.height > 0:
                rows.extend([dict(item) for item in frame.iter_rows(named=True)])
        history = deque(maxlen=self._second_history_len)
        if not rows:
            return history
        merged = (
            pl.DataFrame(rows)
            .sort("ts_ms")
            .unique(subset=["ts_ms"], keep="last", maintain_order=True)
            .tail(self._second_history_len)
        )
        for row in merged.iter_rows(named=True):
            history.append(
                {
                    "ts_ms": float(int(row.get("ts_ms") or 0)),
                    "close": float(_safe_float(row.get("close")) or 0.0),
                    "volume_base": float(_safe_float(row.get("volume_base")) or 0.0),
                    "volume_quote": float(_safe_float(row.get("volume_quote")) or 0.0),
                }
            )
        return history

    def _load_micro_bootstrap(self, *, market: str, ts_ms: int) -> deque[dict[str, float]]:
        frame = self._load_dataset_tail(
            dataset_root=self._micro_root,
            market=market,
            tf="1m",
            end_ts_ms=ts_ms,
            tail_rows=self._micro_history_len,
            wanted_columns=(
                "ts_ms",
                "trade_events",
                "trade_imbalance",
                "spread_bps_mean",
                "depth_bid_top5_mean",
                "depth_ask_top5_mean",
                "imbalance_top5_mean",
                "microprice_bias_bps_mean",
            ),
        )
        history = deque(maxlen=self._micro_history_len)
        for row in frame.iter_rows(named=True):
            history.append(self._normalize_micro_row(dict(row)))
        return history

    def _load_lob_bootstrap(self, *, market: str, ts_ms: int) -> deque[dict[str, float]]:
        frame = self._load_lob_frame(market=market, end_ts_ms=ts_ms, tail_rows=self._lob_history_len)
        history = deque(maxlen=self._lob_history_len)
        for row in frame.iter_rows(named=True):
            history.append(self._normalize_lob_row(dict(row)))
        return history

    def _finalize_active_second(self, *, market: str, fill_gap_until_ts_ms: int | None = None) -> None:
        active = self._second_active.get(market)
        if active is None:
            return
        history = self._second_history.setdefault(market, deque(maxlen=self._second_history_len))
        self._append_second_row(
            history=history,
            ts_ms=int(active.second_start_ts_ms),
            close=float(active.close),
            volume_base=float(active.volume_base),
            volume_quote=float(active.volume_quote),
        )
        if fill_gap_until_ts_ms is not None:
            next_ts = int(active.second_start_ts_ms) + 1_000
            while next_ts <= int(fill_gap_until_ts_ms):
                self._append_second_row(
                    history=history,
                    ts_ms=next_ts,
                    close=float(active.close),
                    volume_base=0.0,
                    volume_quote=0.0,
                )
                next_ts += 1_000
        self._second_active.pop(market, None)

    def _flush_active_second(self, *, market: str, target_ts_ms: int) -> None:
        active = self._second_active.get(market)
        if active is None:
            return
        if int(active.second_start_ts_ms) > int(target_ts_ms):
            return
        self._finalize_active_second(market=market)

    def _append_second_row(
        self,
        *,
        history: deque[dict[str, float]],
        ts_ms: int,
        close: float,
        volume_base: float,
        volume_quote: float,
    ) -> None:
        row = {
            "ts_ms": float(int(ts_ms)),
            "close": float(close),
            "volume_base": float(volume_base),
            "volume_quote": float(volume_quote),
        }
        if history and int(history[-1]["ts_ms"]) == int(ts_ms):
            history[-1] = row
            return
        history.append(row)

    def _update_live_histories(self, *, market: str, ts_ms: int) -> None:
        if self._micro_snapshot_provider is None:
            return
        snapshot = self._micro_snapshot_provider.get(market, int(ts_ms))
        if snapshot is None:
            return
        self._update_live_micro_history(market=market, ts_ms=ts_ms, snapshot=snapshot)
        self._update_live_lob_history(market=market, snapshot=snapshot)

    def _update_live_micro_history(self, *, market: str, ts_ms: int, snapshot: MicroSnapshot) -> None:
        minute_bucket_ts_ms = (int(ts_ms) // 60_000) * 60_000
        previous_bucket = self._last_micro_bucket_ts_ms.get(market)
        if previous_bucket is not None and minute_bucket_ts_ms <= int(previous_bucket):
            return
        history = self._micro_history.setdefault(market, deque(maxlen=self._micro_history_len))
        history.append(
            self._normalize_micro_row(
                {
                    "ts_ms": minute_bucket_ts_ms,
                    "trade_events": int(snapshot.trade_events),
                    "trade_imbalance": float(snapshot.trade_imbalance or 0.0),
                    "spread_bps_mean": float(snapshot.spread_bps_mean or 0.0),
                    "depth_bid_top5_mean": float(snapshot.depth_bid_top5_notional_krw or 0.0),
                    "depth_ask_top5_mean": float(snapshot.depth_ask_top5_notional_krw or 0.0),
                    "imbalance_top5_mean": float(snapshot.imbalance_top5_mean or 0.0),
                    "microprice_bias_bps_mean": float(snapshot.microprice_bias_bps_mean or 0.0),
                }
            )
        )
        self._last_micro_bucket_ts_ms[market] = minute_bucket_ts_ms

    def _update_live_lob_history(self, *, market: str, snapshot: MicroSnapshot) -> None:
        history = self._lob_history.setdefault(market, deque(maxlen=self._lob_history_len))
        last_ts_ms = int(self._last_lob_event_ts_ms.get(market, 0))
        for event in sorted(snapshot.recent_orderbook_events, key=lambda item: int(item[0])):
            event_ts_ms = int(event[0])
            if event_ts_ms <= last_ts_ms:
                continue
            history.append(
                self._lob_row_from_levels(
                    market=market,
                    ts_ms=event_ts_ms,
                    bid_levels=tuple(event[1] or ()),
                    ask_levels=tuple(event[2] or ()),
                )
            )
            last_ts_ms = event_ts_ms
        if last_ts_ms > 0:
            self._last_lob_event_ts_ms[market] = last_ts_ms

    def _frame_from_second_history(self, *, market: str, ts_ms: int) -> pl.DataFrame:
        history = self._second_history.get(market) or deque()
        rows = [row for row in history if int(row.get("ts_ms", 0)) <= int(ts_ms)]
        if not rows:
            return pl.DataFrame(schema={"ts_ms": pl.Int64, "close": pl.Float64, "volume_base": pl.Float64, "volume_quote": pl.Float64})
        return pl.DataFrame(rows).with_columns(pl.col("ts_ms").cast(pl.Int64)).sort("ts_ms")

    def _frame_from_minute_history(self, *, market: str, ts_ms: int) -> pl.DataFrame:
        state = self._base_provider._market_state.get(market)  # noqa: SLF001
        if state is None:
            return pl.DataFrame(schema={"ts_ms": pl.Int64, "close": pl.Float64, "volume_base": pl.Float64, "volume_quote": pl.Float64})
        one_m = self._base_provider._build_one_m_frame(state=state, ts_ms=int(ts_ms))  # noqa: SLF001
        if one_m.height <= 0:
            return pl.DataFrame(schema={"ts_ms": pl.Int64, "close": pl.Float64, "volume_base": pl.Float64, "volume_quote": pl.Float64})
        if "volume_quote" not in one_m.columns:
            one_m = one_m.with_columns((pl.col("close") * pl.col("volume_base")).alias("volume_quote"))
        return one_m.select(["ts_ms", "close", "volume_base", "volume_quote"]).sort("ts_ms")

    def _frame_from_micro_history(self, *, market: str, ts_ms: int) -> pl.DataFrame:
        history = self._micro_history.get(market) or deque()
        rows = [row for row in history if int(row.get("ts_ms", 0)) <= int(ts_ms)]
        if not rows:
            return pl.DataFrame(
                schema={
                    "ts_ms": pl.Int64,
                    "trade_events": pl.Float64,
                    "trade_imbalance": pl.Float64,
                    "spread_bps_mean": pl.Float64,
                    "depth_bid_top5_mean": pl.Float64,
                    "depth_ask_top5_mean": pl.Float64,
                    "imbalance_top5_mean": pl.Float64,
                    "microprice_bias_bps_mean": pl.Float64,
                }
            )
        return pl.DataFrame(rows).with_columns(pl.col("ts_ms").cast(pl.Int64)).sort("ts_ms")

    def _frame_from_lob_history(self, *, market: str, ts_ms: int) -> pl.DataFrame:
        history = self._lob_history.get(market) or deque()
        rows = [row for row in history if int(row.get("ts_ms", 0)) <= int(ts_ms)]
        if not rows:
            schema: dict[str, pl.DataType] = {"market": pl.Utf8, "ts_ms": pl.Int64, "total_bid_size": pl.Float64, "total_ask_size": pl.Float64}
            for idx in range(1, 31):
                schema[f"bid{idx}_price"] = pl.Float64
                schema[f"bid{idx}_size"] = pl.Float64
                schema[f"ask{idx}_price"] = pl.Float64
                schema[f"ask{idx}_size"] = pl.Float64
            return pl.DataFrame(schema=schema)
        return pl.DataFrame(rows).with_columns(pl.col("ts_ms").cast(pl.Int64)).sort("ts_ms")

    def _build_online_sequence_payload(self, *, market: str, ts_ms: int) -> dict[str, np.ndarray] | None:
        second_frame = self._frame_from_second_history(market=market, ts_ms=ts_ms)
        minute_frame = self._frame_from_minute_history(market=market, ts_ms=ts_ms)
        micro_frame = self._frame_from_micro_history(market=market, ts_ms=ts_ms)
        lob_frame = self._frame_from_lob_history(market=market, ts_ms=ts_ms)
        if second_frame.height <= 0 or minute_frame.height <= 0 or micro_frame.height <= 0 or lob_frame.height <= 0:
            return None

        context_end_ts_ms = _resolve_context_end_ts_ms(
            anchor_ts_ms=int(ts_ms),
            second_frame=second_frame,
            lob_frame=lob_frame,
        )
        second_tensor, _, _ = _build_second_tensor(frame=second_frame, anchor_ts_ms=context_end_ts_ms, lookback_steps=120)
        minute_tensor, _, _ = _build_minute_tensor(frame=minute_frame, anchor_ts_ms=int(ts_ms), lookback_steps=30)
        micro_tensor, _, _ = _build_micro_tensor(frame=micro_frame, anchor_ts_ms=int(ts_ms), lookback_steps=30)
        lob_tensor, lob_global_tensor, _, _ = _build_lob_tensor(
            frame=lob_frame,
            micro_frame=micro_frame,
            anchor_ts_ms=context_end_ts_ms,
            lookback_steps=32,
        )
        known_covariates = _build_known_covariates(
            anchor_ts_ms=int(ts_ms),
            ws_by_market=self._build_ws_close_maps(ts_ms=ts_ms),
        )
        second_batch = np.expand_dims(np.asarray(second_tensor, dtype=np.float32), axis=0)
        minute_batch = np.expand_dims(np.asarray(minute_tensor, dtype=np.float32), axis=0)
        micro_batch = np.expand_dims(np.asarray(micro_tensor, dtype=np.float32), axis=0)
        lob_batch = np.expand_dims(np.asarray(lob_tensor, dtype=np.float32), axis=0)
        lob_global_batch = np.expand_dims(np.asarray(lob_global_tensor, dtype=np.float32), axis=0)
        known_covariates_batch = np.expand_dims(np.asarray(known_covariates, dtype=np.float32), axis=0)
        pooled = _build_pooled_sequence_features(
            second=second_batch,
            minute=minute_batch,
            micro=micro_batch,
            lob=lob_batch,
            lob_global=lob_global_batch,
            known_covariates=known_covariates_batch,
        )
        return {
            "second_tensor": second_batch,
            "minute_tensor": minute_batch,
            "micro_tensor": micro_batch,
            "lob_tensor": lob_batch,
            "lob_global_tensor": lob_global_batch,
            "known_covariates": known_covariates_batch,
            "pooled_features": pooled,
        }

    def _build_sequence_feature_values(self, *, market: str, ts_ms: int) -> dict[str, float]:
        payload = self._build_online_sequence_payload(market=market, ts_ms=ts_ms)
        if payload is None:
            return {}
        pooled = payload["pooled_features"][0]
        return {name: float(pooled[idx]) for idx, name in enumerate(self._sequence_feature_names)}

    def _build_lob_feature_values(self, *, market: str, ts_ms: int) -> dict[str, float]:
        payload = self._build_online_sequence_payload(market=market, ts_ms=ts_ms)
        if payload is None:
            return {}
        pooled = _build_pooled_lob_features(
            lob=payload["lob_tensor"],
            lob_global=payload["lob_global_tensor"],
            micro=payload["micro_tensor"],
        )[0]
        return {name: float(pooled[idx]) for idx, name in enumerate(self._lob_feature_names)}

    def _build_fusion_feature_values(
        self,
        *,
        market: str,
        ts_ms: int,
        base_row: dict[str, Any],
    ) -> dict[str, float]:
        if self._panel_predictor is None or self._sequence_predictor is None or self._lob_predictor is None:
            return {}
        payload = self._build_online_sequence_payload(market=market, ts_ms=ts_ms)
        if payload is None:
            return {}

        panel_matrix = np.asarray(
            [[float(_safe_float(base_row.get(name)) or 0.0) for name in self._panel_predictor.feature_columns]],
            dtype=np.float32,
        )
        fusion_values: dict[str, float] = {}
        panel_contract = self._panel_predictor.predict_score_contract(panel_matrix)
        for key, values in panel_contract.items():
            if key == "uncertainty_available":
                continue
            fusion_values[f"panel_{key}"] = float(np.asarray(values, dtype=np.float64)[0])

        sequence_estimator = self._sequence_predictor.model_bundle.get("estimator")
        if sequence_estimator is None or not hasattr(sequence_estimator, "predict_cache_batch"):
            return {}
        sequence_payload = sequence_estimator.predict_cache_batch(
            {
                "second": payload["second_tensor"],
                "minute": payload["minute_tensor"],
                "micro": payload["micro_tensor"],
                "lob": payload["lob_tensor"],
                "lob_global": payload["lob_global_tensor"],
                "known_covariates": payload["known_covariates"],
            }
        )
        fusion_values["sequence_directional_probability_primary"] = float(
            np.asarray(sequence_payload["directional_probability_primary"], dtype=np.float64)[0]
        )
        fusion_values["sequence_sequence_uncertainty_primary"] = float(
            np.asarray(sequence_payload["sequence_uncertainty_primary"], dtype=np.float64)[0]
        )
        return_quantiles = np.asarray(sequence_payload["return_quantiles_by_horizon"], dtype=np.float64)[0]
        horizons = tuple(int(item) for item in getattr(sequence_estimator, "horizons_minutes", (3, 6, 12, 24)))
        quantiles = tuple(float(item) for item in getattr(sequence_estimator, "quantile_levels", (0.1, 0.5, 0.9)))
        for horizon_idx, horizon in enumerate(horizons):
            for quantile_idx, quantile in enumerate(quantiles):
                fusion_values[f"sequence_return_quantile_h{int(horizon)}_q{int(round(float(quantile) * 100))}"] = float(
                    return_quantiles[horizon_idx, quantile_idx]
                )
        regime_embedding = np.asarray(sequence_payload["regime_embedding"], dtype=np.float64)[0]
        for idx, value in enumerate(regime_embedding):
            fusion_values[f"sequence_regime_embedding_{int(idx)}"] = float(value)

        lob_estimator = self._lob_predictor.model_bundle.get("estimator")
        if lob_estimator is None or not hasattr(lob_estimator, "predict_lob_contract"):
            return {}
        lob_payload = lob_estimator.predict_lob_contract(
            {
                "lob": payload["lob_tensor"],
                "lob_global": payload["lob_global_tensor"],
                "micro": payload["micro_tensor"],
            }
        )
        for key, values in lob_payload.items():
            fusion_values[f"lob_{key}"] = float(np.asarray(values, dtype=np.float64)[0])
        return {name: float(fusion_values[name]) for name in self._feature_columns if name in fusion_values}

    def _build_ws_close_maps(self, *, ts_ms: int) -> dict[str, dict[int, float]]:
        payload: dict[str, dict[int, float]] = {}
        for market, state in self._base_provider._market_state.items():  # noqa: SLF001
            one_m = self._base_provider._build_one_m_frame(state=state, ts_ms=int(ts_ms))  # noqa: SLF001
            if one_m.height <= 0 or "ts_ms" not in one_m.columns or "close" not in one_m.columns:
                continue
            payload[str(market).strip().upper()] = {
                int(row["ts_ms"]): float(row["close"])
                for row in one_m.iter_rows(named=True)
                if row.get("ts_ms") is not None and row.get("close") is not None
            }
        return payload

    def _normalize_micro_row(self, row: dict[str, Any]) -> dict[str, float]:
        return {
            "ts_ms": float(int(row.get("ts_ms") or 0)),
            "trade_events": float(_safe_float(row.get("trade_events")) or 0.0),
            "trade_imbalance": float(_safe_float(row.get("trade_imbalance")) or 0.0),
            "spread_bps_mean": float(_safe_float(row.get("spread_bps_mean")) or 0.0),
            "depth_bid_top5_mean": float(_safe_float(row.get("depth_bid_top5_mean")) or 0.0),
            "depth_ask_top5_mean": float(_safe_float(row.get("depth_ask_top5_mean")) or 0.0),
            "imbalance_top5_mean": float(_safe_float(row.get("imbalance_top5_mean")) or 0.0),
            "microprice_bias_bps_mean": float(_safe_float(row.get("microprice_bias_bps_mean")) or 0.0),
        }

    def _normalize_lob_row(self, row: dict[str, Any]) -> dict[str, float]:
        payload: dict[str, float] = {
            "market": str(row.get("market", "")).strip().upper() or "",
            "ts_ms": float(int(row.get("ts_ms") or 0)),
            "total_bid_size": float(_safe_float(row.get("total_bid_size")) or 0.0),
            "total_ask_size": float(_safe_float(row.get("total_ask_size")) or 0.0),
        }
        for idx in range(1, 31):
            payload[f"bid{idx}_price"] = float(_safe_float(row.get(f"bid{idx}_price")) or 0.0)
            payload[f"bid{idx}_size"] = float(_safe_float(row.get(f"bid{idx}_size")) or 0.0)
            payload[f"ask{idx}_price"] = float(_safe_float(row.get(f"ask{idx}_price")) or 0.0)
            payload[f"ask{idx}_size"] = float(_safe_float(row.get(f"ask{idx}_size")) or 0.0)
        return payload

    def _lob_row_from_levels(
        self,
        *,
        market: str,
        ts_ms: int,
        bid_levels: tuple[tuple[float, float], ...],
        ask_levels: tuple[tuple[float, float], ...],
    ) -> dict[str, float]:
        payload: dict[str, float] = {"market": market, "ts_ms": float(int(ts_ms))}
        total_bid = 0.0
        total_ask = 0.0
        for idx in range(1, 31):
            bid_price, bid_size = bid_levels[idx - 1] if idx <= len(bid_levels) else (0.0, 0.0)
            ask_price, ask_size = ask_levels[idx - 1] if idx <= len(ask_levels) else (0.0, 0.0)
            payload[f"bid{idx}_price"] = float(bid_price)
            payload[f"bid{idx}_size"] = float(bid_size)
            payload[f"ask{idx}_price"] = float(ask_price)
            payload[f"ask{idx}_size"] = float(ask_size)
            total_bid += float(bid_size)
            total_ask += float(ask_size)
        payload["total_bid_size"] = float(total_bid)
        payload["total_ask_size"] = float(total_ask)
        return payload

    def _load_market_frame(
        self,
        *,
        market_dir: Path,
        end_ts_ms: int,
        wanted_columns: Sequence[str],
    ) -> pl.DataFrame:
        if not market_dir.exists():
            return pl.DataFrame()
        files = sorted(path for path in market_dir.rglob("*.parquet") if path.is_file())
        if not files:
            return pl.DataFrame()
        try:
            return (
                pl.scan_parquet([str(path) for path in files])
                .select([pl.col(name) for name in wanted_columns if name])
                .filter(pl.col("ts_ms") <= int(end_ts_ms))
                .sort("ts_ms")
                .collect()
            )
        except Exception:
            return pl.DataFrame()

    def _load_dataset_tail(
        self,
        *,
        dataset_root: Path,
        market: str,
        tf: str,
        end_ts_ms: int,
        tail_rows: int,
        wanted_columns: Sequence[str],
    ) -> pl.DataFrame:
        files = _market_files(dataset_root=dataset_root, tf=tf, market=market)
        if not files:
            return pl.DataFrame()
        try:
            return (
                pl.scan_parquet([str(path) for path in files])
                .select([pl.col(name) for name in wanted_columns if name])
                .filter(pl.col("ts_ms") <= int(end_ts_ms))
                .sort("ts_ms")
                .tail(max(int(tail_rows), 1))
                .collect()
            )
        except Exception:
            return pl.DataFrame()

    def _load_lob_frame(self, *, market: str, end_ts_ms: int, tail_rows: int) -> pl.DataFrame:
        market_dir = self._lob_root / f"market={market}"
        if not market_dir.exists():
            return pl.DataFrame()
        files = sorted(path for path in market_dir.rglob("*.parquet") if path.is_file())
        if not files:
            return pl.DataFrame()
        wanted_columns = ["market", "ts_ms", "total_bid_size", "total_ask_size"]
        for idx in range(1, 31):
            wanted_columns.extend((f"bid{idx}_price", f"bid{idx}_size", f"ask{idx}_price", f"ask{idx}_size"))
        try:
            return (
                pl.scan_parquet([str(path) for path in files])
                .select([pl.col(name) for name in wanted_columns])
                .filter(pl.col("ts_ms") <= int(end_ts_ms))
                .sort("ts_ms")
                .tail(max(int(tail_rows), 1))
                .collect()
            )
        except Exception:
            return pl.DataFrame()

    def _load_json(self, path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        try:
            import json

            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return payload if isinstance(payload, dict) else {}
