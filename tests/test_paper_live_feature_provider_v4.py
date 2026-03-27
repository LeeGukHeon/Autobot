from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from autobot.models.registry import RegistrySavePayload, save_run
from autobot.paper.engine import PaperRunEngine, PaperRunSettings
from autobot.paper.live_features_v4 import LiveFeatureProviderV4
from autobot.paper.live_features_v4_native import LiveFeatureProviderV4Native
from autobot.paper.sim_exchange import MarketRules
from autobot.strategy.micro_snapshot import MicroSnapshot
from autobot.strategy.model_alpha_v1 import (
    ModelAlphaPositionSettings,
    ModelAlphaSelectionSettings,
    ModelAlphaSettings,
)
from autobot.upbit.config import (
    UpbitAuthSettings,
    UpbitRateLimitSettings,
    UpbitRetrySettings,
    UpbitSettings,
    UpbitTimeoutSettings,
    UpbitWebSocketSettings,
)
from autobot.upbit.ws.models import TickerEvent


class _DummyEstimator:
    def predict_proba(self, x: np.ndarray) -> np.ndarray:
        logits = x[:, 0].astype(np.float64)
        probs = 1.0 / (1.0 + np.exp(-logits))
        probs = np.clip(probs, 1e-6, 1.0 - 1e-6)
        return np.column_stack([1.0 - probs, probs])


class _FakeWsClient:
    def __init__(self, events: list[TickerEvent]) -> None:
        self._events = events

    async def stream_ticker(self, markets: list[str], *, duration_sec: float | None = None):
        _ = (markets, duration_sec)
        for event in self._events:
            await asyncio.sleep(0.05)
            yield event


class _StaticRulesProvider:
    def get_rules(self, *, market: str, reference_price: float, ts_ms: int) -> MarketRules:
        _ = (market, reference_price, ts_ms)
        return MarketRules(
            bid_fee=0.0005,
            ask_fee=0.0005,
            maker_bid_fee=0.0002,
            maker_ask_fee=0.0002,
            min_total=5_000.0,
            tick_size=1.0,
        )


class _StaticMicroSnapshotProvider:
    def __init__(self, snapshot: MicroSnapshot) -> None:
        self._snapshot = snapshot

    def get(self, market: str, ts_ms: int) -> MicroSnapshot | None:
        _ = (market, ts_ms)
        return self._snapshot


class _MappingMicroSnapshotProvider:
    def __init__(self, snapshots: dict[str, MicroSnapshot | None]) -> None:
        self._snapshots = {str(key).strip().upper(): value for key, value in snapshots.items()}

    def get(self, market: str, ts_ms: int) -> MicroSnapshot | None:
        _ = ts_ms
        return self._snapshots.get(str(market).strip().upper())


class _TimeKeyedMicroSnapshotProvider:
    def __init__(self, snapshots: dict[tuple[str, int], MicroSnapshot | None]) -> None:
        self._snapshots = {
            (str(market).strip().upper(), int(ts_ms)): value
            for (market, ts_ms), value in snapshots.items()
        }

    def get(self, market: str, ts_ms: int) -> MicroSnapshot | None:
        return self._snapshots.get((str(market).strip().upper(), int(ts_ms)))


def test_live_feature_provider_v4_builds_v4_columns_from_native_base(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    _write_one_m_candles(dataset_root=parquet_root / "candles_api_v1", market="KRW-BTC")

    provider = LiveFeatureProviderV4(
        feature_columns=(
            "logret_1",
            "btc_ret_12",
            "oflow_v1_signed_volume_imbalance_1",
            "oflow_v1_depth_conditioned_flow_1",
            "market_breadth_pos_12",
            "hour_sin",
            "trend_consensus",
            "mom_x_spread",
        ),
        tf="5m",
        quote="KRW",
        parquet_root=parquet_root,
        candles_dataset_name="candles_api_v1",
        bootstrap_1m_bars=2000,
    )
    provider.ingest_ticker(
        TickerEvent(
            market="KRW-BTC",
            ts_ms=301_000,
            trade_price=121.0,
            acc_trade_price_24h=1_000_100_000.0,
        )
    )

    frame = provider.build_frame(ts_ms=300_000, markets=["KRW-BTC"])
    assert frame.height == 1
    row = frame.row(0, named=True)
    assert int(row["ts_ms"]) == 300_000
    assert str(row["market"]) == "KRW-BTC"
    assert "btc_ret_12" in frame.columns
    assert "oflow_v1_signed_volume_imbalance_1" in frame.columns
    assert "oflow_v1_depth_conditioned_flow_1" in frame.columns
    assert "market_breadth_pos_12" in frame.columns
    assert "hour_sin" in frame.columns
    assert "trend_consensus" in frame.columns
    assert "mom_x_spread" in frame.columns

    status = provider.status(now_ts_ms=300_000)
    assert status["provider"] == "LIVE_V4"
    stats = provider.last_build_stats()
    assert stats["provider"] == "LIVE_V4"
    assert stats["base_provider"] == "LIVE_V4_BASE"


def test_live_feature_provider_v4_native_matches_live_v4_output(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    _write_one_m_candles(dataset_root=parquet_root / "candles_api_v1", market="KRW-BTC")
    feature_columns = (
        "logret_1",
        "btc_ret_12",
        "oflow_v1_signed_volume_imbalance_1",
        "oflow_v1_depth_conditioned_flow_1",
        "market_breadth_pos_12",
        "hour_sin",
        "trend_consensus",
        "mom_x_spread",
    )
    provider = LiveFeatureProviderV4(
        feature_columns=feature_columns,
        tf="5m",
        quote="KRW",
        parquet_root=parquet_root,
        candles_dataset_name="candles_api_v1",
        bootstrap_1m_bars=2000,
    )
    native_provider = LiveFeatureProviderV4Native(
        feature_columns=feature_columns,
        tf="5m",
        quote="KRW",
        parquet_root=parquet_root,
        candles_dataset_name="candles_api_v1",
        bootstrap_1m_bars=2000,
    )
    event = TickerEvent(
        market="KRW-BTC",
        ts_ms=301_000,
        trade_price=121.0,
        acc_trade_price_24h=1_000_100_000.0,
    )
    provider.ingest_ticker(event)
    native_provider.ingest_ticker(event)

    frame = provider.build_frame(ts_ms=300_000, markets=["KRW-BTC"])
    native_frame = native_provider.build_frame(ts_ms=300_000, markets=["KRW-BTC"])
    assert frame.columns == native_frame.columns
    assert frame.height == native_frame.height == 1
    row = frame.row(0, named=True)
    native_row = native_frame.row(0, named=True)
    for key in frame.columns:
        left = row[key]
        right = native_row[key]
        if isinstance(left, float):
            assert left == right
        else:
            assert left == right
    native_status = native_provider.status(now_ts_ms=300_000)
    assert native_status["provider"] == "LIVE_V4_NATIVE"
    native_stats = native_provider.last_build_stats()
    assert native_stats["provider"] == "LIVE_V4_NATIVE"
    assert native_stats["base_provider"] == "LIVE_V4_NATIVE_BASE"


def test_live_feature_provider_v4_hard_gates_missing_requested_columns(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    _write_one_m_candles(dataset_root=parquet_root / "candles_api_v1", market="KRW-BTC")

    provider = LiveFeatureProviderV4(
        feature_columns=(
            "logret_1",
            "btc_ret_12",
            "missing_v4_feature_for_test",
        ),
        tf="5m",
        quote="KRW",
        parquet_root=parquet_root,
        candles_dataset_name="candles_api_v1",
        bootstrap_1m_bars=2000,
    )
    provider.ingest_ticker(
        TickerEvent(
            market="KRW-BTC",
            ts_ms=301_000,
            trade_price=121.0,
            acc_trade_price_24h=1_000_100_000.0,
        )
    )

    frame = provider.build_frame(ts_ms=300_000, markets=["KRW-BTC"])
    assert frame.height == 0
    stats = provider.last_build_stats()
    assert stats["hard_gate_triggered"] is True
    assert stats["skip_reason"] == "MISSING_V4_FEATURE_COLUMNS"
    assert "missing_v4_feature_for_test" in stats["missing_feature_columns"]


def test_live_feature_provider_v4_hard_gates_removed_ctrend_columns(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    request_dt = datetime(2026, 1, 2, 0, 5, tzinfo=timezone.utc)
    request_ts_ms = int(request_dt.timestamp() * 1000)
    _write_one_m_candles(
        dataset_root=parquet_root / "candles_api_v1",
        market="KRW-BTC",
        start_ts_ms=request_ts_ms - (720 * 60_000),
        count=900,
    )
    provider = LiveFeatureProviderV4(
        feature_columns=(
            "logret_1",
            "btc_ret_12",
            "oflow_v1_signed_volume_imbalance_1",
            "ctrend_v1_rsi_14",
            "ctrend_v1_cci_20",
        ),
        tf="5m",
        quote="KRW",
        parquet_root=parquet_root,
        candles_dataset_name="candles_api_v1",
        bootstrap_1m_bars=720,
    )
    provider.ingest_ticker(
        TickerEvent(
            market="KRW-BTC",
            ts_ms=request_ts_ms + 1_000,
            trade_price=121.0,
            acc_trade_price_24h=1_000_100_000.0,
        )
    )

    frame = provider.build_frame(ts_ms=request_ts_ms, markets=["KRW-BTC"])
    assert frame.height == 0
    stats = provider.last_build_stats()
    assert stats["hard_gate_triggered"] is True
    assert "ctrend_v1_rsi_14" in stats["missing_feature_columns"]
    assert "ctrend_v1_cci_20" in stats["missing_feature_columns"]


def test_paper_engine_model_alpha_live_v4_scores_without_no_feature_rows(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    registry_root = tmp_path / "registry"
    _write_one_m_candles(dataset_root=parquet_root / "candles_api_v1", market="KRW-BTC")
    _save_model_run(
        registry_root=registry_root,
        model_family="train_v4_crypto_cs",
        run_id="run_live_v4",
        feature_columns=[
            "logret_1",
            "btc_ret_12",
            "market_breadth_pos_12",
            "hour_sin",
            "trend_consensus",
            "mom_x_spread",
        ],
    )

    now_ms = int(time.time() * 1000)
    base_ts = (now_ms // 300_000) * 300_000
    events = [
        TickerEvent(
            market="KRW-BTC",
            ts_ms=base_ts + 1_000,
            trade_price=121.0,
            acc_trade_price_24h=1_000_100_000.0,
        ),
        TickerEvent(
            market="KRW-BTC",
            ts_ms=base_ts + 2_000,
            trade_price=121.5,
            acc_trade_price_24h=1_000_200_000.0,
        ),
    ]

    settings = UpbitSettings(
        base_url="https://api.upbit.com",
        timeout=UpbitTimeoutSettings(),
        auth=UpbitAuthSettings(),
        ratelimit=UpbitRateLimitSettings(),
        retry=UpbitRetrySettings(),
        websocket=UpbitWebSocketSettings(),
    )
    run_settings = PaperRunSettings(
        duration_sec=2,
        quote="KRW",
        top_n=1,
        tf="5m",
        strategy="model_alpha_v1",
        model_ref="run_live_v4",
        model_family="train_v4_crypto_cs",
        feature_set="v4",
        model_registry_root=str(registry_root),
        print_every_sec=60.0,
        decision_interval_sec=0.1,
        universe_refresh_sec=1,
        universe_hold_sec=0,
        momentum_window_sec=60,
        min_momentum_pct=0.2,
        starting_krw=50_000.0,
        per_trade_krw=10_000.0,
        max_positions=1,
        out_root_dir=str(tmp_path),
        paper_feature_provider="live_v4",
        paper_live_parquet_root=str(parquet_root),
        paper_live_candles_dataset="candles_api_v1",
        model_alpha=ModelAlphaSettings(
            model_ref="run_live_v4",
            model_family="train_v4_crypto_cs",
            feature_set="v4",
            selection=ModelAlphaSelectionSettings(top_pct=1.0, min_prob=0.0, min_candidates_per_ts=1),
            position=ModelAlphaPositionSettings(max_positions_total=1, cooldown_bars=0),
        ),
    )
    engine = PaperRunEngine(
        upbit_settings=settings,
        run_settings=run_settings,
        ws_client=_FakeWsClient(events),
        market_loader=lambda quote: ["KRW-BTC"] if quote == "KRW" else [],
        rules_provider=_StaticRulesProvider(),  # type: ignore[arg-type]
    )

    summary = asyncio.run(engine.run())
    run_dir = Path(summary.run_dir)
    payloads = [
        json.loads(line)
        for line in (run_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    selections = [item for item in payloads if item.get("event_type") == "MODEL_ALPHA_SELECTION"]
    assert selections
    assert any(int(item.get("payload", {}).get("scored_rows", 0)) > 0 for item in selections)
    assert all("NO_FEATURE_ROWS_AT_TS" not in item.get("payload", {}).get("reasons", {}) for item in selections)
    status_events = [item for item in payloads if item.get("event_type") == "FEATURE_PROVIDER_STATUS"]
    assert status_events
    assert str((status_events[-1].get("payload") or {}).get("provider")) == "LIVE_V4"
    built_events = [item for item in payloads if item.get("event_type") == "LIVE_FEATURES_BUILT"]
    assert built_events
    built_payload = built_events[-1].get("payload") or {}
    assert str(built_payload.get("provider")) == "LIVE_V4"
    assert str((built_payload.get("base_provider_stats") or {}).get("provider")) == "LIVE_V4_BASE"


def test_paper_engine_model_alpha_live_v4_native_scores_without_no_feature_rows(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    registry_root = tmp_path / "registry"
    _write_one_m_candles(dataset_root=parquet_root / "candles_api_v1", market="KRW-BTC")
    _save_model_run(
        registry_root=registry_root,
        model_family="train_v4_crypto_cs",
        run_id="run_live_v4_native",
        feature_columns=[
            "logret_1",
            "btc_ret_12",
            "market_breadth_pos_12",
            "hour_sin",
            "trend_consensus",
            "mom_x_spread",
        ],
    )

    now_ms = int(time.time() * 1000)
    base_ts = (now_ms // 300_000) * 300_000
    events = [
        TickerEvent(
            market="KRW-BTC",
            ts_ms=base_ts + 1_000,
            trade_price=121.0,
            acc_trade_price_24h=1_000_100_000.0,
        ),
        TickerEvent(
            market="KRW-BTC",
            ts_ms=base_ts + 2_000,
            trade_price=121.5,
            acc_trade_price_24h=1_000_200_000.0,
        ),
    ]

    settings = UpbitSettings(
        base_url="https://api.upbit.com",
        timeout=UpbitTimeoutSettings(),
        auth=UpbitAuthSettings(),
        ratelimit=UpbitRateLimitSettings(),
        retry=UpbitRetrySettings(),
        websocket=UpbitWebSocketSettings(),
    )
    run_settings = PaperRunSettings(
        duration_sec=2,
        quote="KRW",
        top_n=1,
        tf="5m",
        strategy="model_alpha_v1",
        model_ref="run_live_v4_native",
        model_family="train_v4_crypto_cs",
        feature_set="v4",
        model_registry_root=str(registry_root),
        print_every_sec=60.0,
        decision_interval_sec=0.1,
        universe_refresh_sec=1,
        universe_hold_sec=0,
        momentum_window_sec=60,
        min_momentum_pct=0.2,
        starting_krw=50_000.0,
        per_trade_krw=10_000.0,
        max_positions=1,
        out_root_dir=str(tmp_path),
        paper_feature_provider="live_v4_native",
        paper_live_parquet_root=str(parquet_root),
        paper_live_candles_dataset="candles_api_v1",
        model_alpha=ModelAlphaSettings(
            model_ref="run_live_v4_native",
            model_family="train_v4_crypto_cs",
            selection=ModelAlphaSelectionSettings(top_pct=1.0, min_prob=0.0, min_candidates_per_ts=1),
            position=ModelAlphaPositionSettings(max_positions_total=1, cooldown_bars=0),
        ),
    )
    engine = PaperRunEngine(
        upbit_settings=settings,
        run_settings=run_settings,
        ws_client=_FakeWsClient(events),
        market_loader=lambda quote: ["KRW-BTC"] if quote == "KRW" else [],
        rules_provider=_StaticRulesProvider(),  # type: ignore[arg-type]
    )

    summary = asyncio.run(engine.run())
    run_dir = Path(summary.run_dir)
    payloads = [
        json.loads(line)
        for line in (run_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    selections = [item for item in payloads if item.get("event_type") == "MODEL_ALPHA_SELECTION"]
    assert selections
    assert any(int(item.get("payload", {}).get("scored_rows", 0)) > 0 for item in selections)
    assert all("NO_FEATURE_ROWS_AT_TS" not in item.get("payload", {}).get("reasons", {}) for item in selections)
    built_events = [item for item in payloads if item.get("event_type") == "LIVE_FEATURES_BUILT"]
    assert built_events
    built_payload = built_events[-1].get("payload") or {}
    assert str(built_payload.get("provider")) == "LIVE_V4_NATIVE"
    assert str((built_payload.get("base_provider_stats") or {}).get("provider")) == "LIVE_V4_NATIVE_BASE"


def test_paper_engine_model_alpha_live_v4_hard_gates_old_ctrend_models(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    registry_root = tmp_path / "registry"
    now_ms = int(time.time() * 1000)
    base_ts = (now_ms // 300_000) * 300_000
    _write_one_m_candles(
        dataset_root=parquet_root / "candles_api_v1",
        market="KRW-BTC",
        start_ts_ms=base_ts - (1_000 * 60_000),
        count=1_200,
    )
    _save_model_run(
        registry_root=registry_root,
        model_family="train_v4_crypto_cs",
        run_id="run_live_v4_ctrend",
        feature_columns=[
            "logret_1",
            "btc_ret_12",
            "oflow_v1_signed_volume_imbalance_1",
            "oflow_v1_depth_conditioned_flow_1",
            "ctrend_v1_rsi_14",
            "ctrend_v1_cci_20",
            "ctrend_v1_ma_gap_200",
            "ctrend_v1_boll_width_20_2",
        ],
    )

    events = [
        TickerEvent(
            market="KRW-BTC",
            ts_ms=base_ts + 1_000,
            trade_price=121.0,
            acc_trade_price_24h=1_000_100_000.0,
        ),
        TickerEvent(
            market="KRW-BTC",
            ts_ms=base_ts + 2_000,
            trade_price=121.5,
            acc_trade_price_24h=1_000_200_000.0,
        ),
    ]

    settings = UpbitSettings(
        base_url="https://api.upbit.com",
        timeout=UpbitTimeoutSettings(),
        auth=UpbitAuthSettings(),
        ratelimit=UpbitRateLimitSettings(),
        retry=UpbitRetrySettings(),
        websocket=UpbitWebSocketSettings(),
    )
    run_settings = PaperRunSettings(
        duration_sec=2,
        quote="KRW",
        top_n=1,
        tf="5m",
        strategy="model_alpha_v1",
        model_ref="run_live_v4_ctrend",
        model_family="train_v4_crypto_cs",
        feature_set="v4",
        model_registry_root=str(registry_root),
        print_every_sec=60.0,
        decision_interval_sec=0.1,
        universe_refresh_sec=1,
        universe_hold_sec=0,
        momentum_window_sec=60,
        min_momentum_pct=0.2,
        starting_krw=50_000.0,
        per_trade_krw=10_000.0,
        max_positions=1,
        out_root_dir=str(tmp_path),
        paper_feature_provider="live_v4",
        paper_live_parquet_root=str(parquet_root),
        paper_live_candles_dataset="candles_api_v1",
        model_alpha=ModelAlphaSettings(
            model_ref="run_live_v4_ctrend",
            model_family="train_v4_crypto_cs",
            feature_set="v4",
            selection=ModelAlphaSelectionSettings(top_pct=1.0, min_prob=0.0, min_candidates_per_ts=1),
            position=ModelAlphaPositionSettings(max_positions_total=1, cooldown_bars=0),
        ),
    )
    engine = PaperRunEngine(
        upbit_settings=settings,
        run_settings=run_settings,
        ws_client=_FakeWsClient(events),
        market_loader=lambda quote: ["KRW-BTC"] if quote == "KRW" else [],
        rules_provider=_StaticRulesProvider(),  # type: ignore[arg-type]
    )

    summary = asyncio.run(engine.run())
    run_dir = Path(summary.run_dir)
    payloads = [
        json.loads(line)
        for line in (run_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    selections = [item for item in payloads if item.get("event_type") == "MODEL_ALPHA_SELECTION"]
    assert selections
    assert all(int(item.get("payload", {}).get("scored_rows", 0)) == 0 for item in selections)
    assert any("NO_FEATURE_ROWS_AT_TS" in (item.get("payload", {}).get("reasons", {}) or {}) for item in selections)
    built_events = [item for item in payloads if item.get("event_type") == "LIVE_FEATURES_BUILT"]
    assert built_events
    built_payload = built_events[-1].get("payload") or {}
    assert str(built_payload.get("provider")) == "LIVE_V4"
    assert built_payload.get("hard_gate_triggered") is True
    missing_columns = list(built_payload.get("missing_feature_columns") or [])
    assert "ctrend_v1_rsi_14" in missing_columns
    assert "ctrend_v1_cci_20" in missing_columns


def test_live_feature_provider_v4_uses_rich_micro_snapshot_fields_without_approximation(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    _write_one_m_candles(dataset_root=parquet_root / "candles_api_v1", market="KRW-BTC")
    snapshot = MicroSnapshot(
        market="KRW-BTC",
        snapshot_ts_ms=300_000,
        last_event_ts_ms=359_000,
        trade_events=6,
        trade_count=6,
        buy_count=4,
        sell_count=2,
        trade_coverage_ms=247_000,
        trade_min_ts_ms=112_000,
        trade_max_ts_ms=359_000,
        trade_notional_krw=4_191_192.0,
        trade_imbalance=0.14,
        trade_source="ws",
        trade_volume_total=40_079.39,
        buy_volume=22_936.19,
        sell_volume=17_143.20,
        vwap=104.57,
        avg_trade_size=6_679.89,
        max_trade_size=14_471.79,
        last_trade_price=104.0,
        mid_mean=104.5,
        spread_bps_mean=95.69,
        depth_top5_notional_krw=4_974_606.08,
        depth_bid_top5_notional_krw=2_732_748.80,
        depth_ask_top5_notional_krw=2_241_857.28,
        imbalance_top5_mean=0.0986,
        microprice_bias_bps_mean=22.60,
        book_events=71,
        book_coverage_ms=290_800,
        book_min_ts_ms=68_200,
        book_max_ts_ms=359_000,
        book_available=True,
    )

    provider = LiveFeatureProviderV4(
        feature_columns=(
            "m_trade_source",
            "m_trade_count",
            "m_buy_count",
            "m_sell_count",
            "m_trade_volume_total",
            "m_buy_volume",
            "m_sell_volume",
            "m_trade_min_ts_ms",
            "m_trade_max_ts_ms",
            "m_vwap",
            "m_avg_trade_size",
            "m_max_trade_size",
            "m_last_trade_price",
            "m_book_min_ts_ms",
            "m_book_max_ts_ms",
            "m_mid_mean",
            "m_depth_bid_top5_mean",
            "m_depth_ask_top5_mean",
            "m_imbalance_top5_mean",
            "m_microprice_bias_bps_mean",
            "m_trade_volume_base",
            "m_signed_volume",
        ),
        tf="5m",
        quote="KRW",
        parquet_root=parquet_root,
        candles_dataset_name="candles_api_v1",
        bootstrap_1m_bars=2000,
        micro_snapshot_provider=_StaticMicroSnapshotProvider(snapshot),
    )

    frame = provider.build_frame(ts_ms=300_000, markets=["KRW-BTC"])
    row = frame.row(0, named=True)

    assert float(row["m_trade_source"]) == 2.0
    assert float(row["m_trade_count"]) == 6.0
    assert float(row["m_buy_count"]) == 4.0
    assert float(row["m_sell_count"]) == 2.0
    assert float(row["m_trade_volume_total"]) == pytest.approx(40_079.39, rel=0, abs=1e-4)
    assert float(row["m_buy_volume"]) == pytest.approx(22_936.19, rel=0, abs=1e-4)
    assert float(row["m_sell_volume"]) == pytest.approx(17_143.20, rel=0, abs=1e-4)
    assert float(row["m_trade_min_ts_ms"]) == pytest.approx(112_000.0, rel=0, abs=1e-6)
    assert float(row["m_trade_max_ts_ms"]) == pytest.approx(359_000.0, rel=0, abs=1e-6)
    assert float(row["m_vwap"]) == pytest.approx(104.57, rel=0, abs=1e-4)
    assert float(row["m_book_min_ts_ms"]) == pytest.approx(68_200.0, rel=0, abs=1e-6)
    assert float(row["m_book_max_ts_ms"]) == pytest.approx(359_000.0, rel=0, abs=1e-6)
    assert float(row["m_mid_mean"]) == pytest.approx(104.5, rel=0, abs=1e-6)
    assert float(row["m_depth_bid_top5_mean"]) == pytest.approx(2_732_748.80, rel=0, abs=1e-2)
    assert float(row["m_depth_ask_top5_mean"]) == pytest.approx(2_241_857.28, rel=0, abs=1e-2)
    assert float(row["m_imbalance_top5_mean"]) == pytest.approx(0.0986, rel=0, abs=1e-6)
    assert float(row["m_microprice_bias_bps_mean"]) == pytest.approx(22.60, rel=0, abs=1e-6)
    assert float(row["m_trade_volume_base"]) == pytest.approx(40_079.39, rel=0, abs=1e-4)
    assert float(row["m_signed_volume"]) == pytest.approx(5_792.99, rel=0, abs=1e-2)


def test_live_feature_provider_v4_keeps_boundary_minute_in_current_base_bar(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    part_dir = parquet_root / "candles_api_v1" / "tf=1m" / "market=KRW-BTC" / "date=2026-01-01"
    part_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "ts_ms": [60_000, 120_000, 180_000, 240_000, 300_000, 360_000, 420_000, 480_000, 540_000, 600_000],
            "open": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
            "high": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
            "low": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
            "close": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
            "volume_base": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0],
        }
    ).write_parquet(part_dir / "part-000.parquet")

    provider = LiveFeatureProviderV4(
        feature_columns=("one_m_count", "one_m_volume_sum"),
        tf="5m",
        quote="KRW",
        parquet_root=parquet_root,
        candles_dataset_name="candles_api_v1",
        bootstrap_1m_bars=256,
        bootstrap_end_ts_ms=600_000,
    )

    frame = provider.build_frame(ts_ms=600_000, markets=["KRW-BTC"])

    assert frame.height == 1
    row = frame.row(0, named=True)
    assert float(row["close"]) == pytest.approx(10.0, rel=0, abs=1e-9)
    assert float(row["one_m_count"]) == pytest.approx(5.0, rel=0, abs=1e-9)
    assert float(row["one_m_volume_sum"]) == pytest.approx(400.0, rel=0, abs=1e-9)


def test_live_feature_provider_v4_prefers_canonical_base_tf_over_conflicting_1m_rollup(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    one_m_dir = parquet_root / "candles_api_v1" / "tf=1m" / "market=KRW-BTC" / "date=2026-01-01"
    one_m_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "ts_ms": [60_000, 120_000, 180_000, 240_000, 300_000, 360_000, 420_000, 480_000, 540_000, 600_000],
            "open": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
            "high": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
            "low": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
            "close": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
            "volume_base": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0],
        }
    ).write_parquet(one_m_dir / "part-000.parquet")
    base_dir = parquet_root / "candles_api_v1" / "tf=5m" / "market=KRW-BTC" / "date=2026-01-01"
    base_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "ts_ms": [300_000, 600_000],
            "open": [111.0, 222.0],
            "high": [115.0, 230.0],
            "low": [110.0, 221.0],
            "close": [114.0, 229.0],
            "volume_base": [15.0, 25.0],
        }
    ).write_parquet(base_dir / "part-000.parquet")

    provider = LiveFeatureProviderV4(
        feature_columns=("one_m_count", "one_m_volume_sum"),
        tf="5m",
        quote="KRW",
        parquet_root=parquet_root,
        candles_dataset_name="candles_api_v1",
        bootstrap_1m_bars=256,
        bootstrap_end_ts_ms=600_000,
    )

    frame = provider.build_frame(ts_ms=600_000, markets=["KRW-BTC"])

    assert frame.height == 1
    row = frame.row(0, named=True)
    assert float(row["close"]) == pytest.approx(229.0, rel=0, abs=1e-9)
    assert float(row["one_m_count"]) == pytest.approx(5.0, rel=0, abs=1e-9)
    assert float(row["one_m_volume_sum"]) == pytest.approx(400.0, rel=0, abs=1e-9)


def test_live_feature_provider_v4_can_require_micro_for_cross_sectional_context(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    _write_one_m_candles(dataset_root=parquet_root / "candles_api_v1", market="KRW-BTC", start_ts_ms=60_000, count=599)
    part_dir = parquet_root / "candles_api_v1" / "tf=1m" / "market=KRW-ETH" / "date=2026-01-01"
    part_dir.mkdir(parents=True, exist_ok=True)
    ts_values = [60_000 + (i * 60_000) for i in range(599)]
    close_values = [200.0 - (i * 0.05) for i in range(len(ts_values))]
    pl.DataFrame(
        {
            "ts_ms": ts_values,
            "open": [value + 0.02 for value in close_values],
            "high": [value + 0.05 for value in close_values],
            "low": [value - 0.05 for value in close_values],
            "close": close_values,
            "volume_base": [10.0 + (i % 5) for i in range(len(ts_values))],
        }
    ).write_parquet(part_dir / "part-000.parquet")
    request_ts_ms = 35_700_000

    btc_snapshot = MicroSnapshot(
        market="KRW-BTC",
        snapshot_ts_ms=request_ts_ms,
        last_event_ts_ms=request_ts_ms,
        trade_events=1,
        trade_count=1,
        buy_count=1,
        sell_count=0,
        trade_coverage_ms=60_000,
        trade_notional_krw=100.0,
        trade_imbalance=1.0,
        trade_source="ws",
        trade_volume_total=1.0,
        buy_volume=1.0,
        sell_volume=0.0,
        vwap=100.0,
        avg_trade_size=1.0,
        max_trade_size=1.0,
        last_trade_price=100.0,
        mid_mean=100.0,
        spread_bps_mean=1.0,
        depth_top5_notional_krw=1_000.0,
        depth_bid_top5_notional_krw=500.0,
        depth_ask_top5_notional_krw=500.0,
        imbalance_top5_mean=0.0,
        microprice_bias_bps_mean=0.0,
        book_events=1,
        book_coverage_ms=60_000,
        book_available=True,
    )
    provider = LiveFeatureProviderV4(
        feature_columns=("market_breadth_pos_12",),
        tf="5m",
        quote="KRW",
        parquet_root=parquet_root,
        candles_dataset_name="candles_api_v1",
        bootstrap_1m_bars=2000,
        bootstrap_end_ts_ms=request_ts_ms,
        micro_snapshot_provider=_MappingMicroSnapshotProvider(
            {
                "KRW-BTC": btc_snapshot,
                "KRW-ETH": None,
            }
        ),
        context_micro_required=True,
    )

    frame = provider.build_frame(ts_ms=request_ts_ms, markets=["KRW-BTC", "KRW-ETH"])

    assert frame.height == 1
    row = frame.row(0, named=True)
    assert str(row["market"]) == "KRW-BTC"
    assert float(row["market_breadth_pos_12"]) == pytest.approx(1.0, rel=0, abs=1e-6)
    stats = provider.last_build_stats()
    assert stats["context_micro_required"] is True
    assert int(stats["context_rows_before_micro_filter"]) == 2
    assert int(stats["context_rows_after_micro_filter"]) == 1
    assert int(stats["context_rows_dropped_no_micro"]) == 1


def test_live_feature_provider_v4_can_use_history_context_for_rolling_order_flow_features(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    _write_one_m_candles(dataset_root=parquet_root / "candles_api_v1", market="KRW-BTC", start_ts_ms=60_000, count=700)
    snapshot_map: dict[tuple[str, int], MicroSnapshot | None] = {}
    bar_ts_values = [300_000 + (i * 300_000) for i in range(14)]
    for index, ts_value in enumerate(bar_ts_values):
        is_buy = index % 2 == 0
        snapshot_map[("KRW-BTC", ts_value)] = MicroSnapshot(
            market="KRW-BTC",
            snapshot_ts_ms=ts_value,
            last_event_ts_ms=ts_value,
            trade_events=1,
            trade_count=1,
            buy_count=1 if is_buy else 0,
            sell_count=0 if is_buy else 1,
            trade_coverage_ms=60_000,
            trade_notional_krw=100.0,
            trade_imbalance=1.0 if is_buy else -1.0,
            trade_source="ws",
            trade_volume_total=1.0,
            buy_volume=1.0 if is_buy else 0.0,
            sell_volume=0.0 if is_buy else 1.0,
            vwap=100.0,
            avg_trade_size=1.0,
            max_trade_size=1.0,
            last_trade_price=100.0,
            mid_mean=100.0,
            spread_bps_mean=1.0,
            depth_top5_notional_krw=1_000.0,
            depth_bid_top5_notional_krw=500.0,
            depth_ask_top5_notional_krw=500.0,
            imbalance_top5_mean=0.0,
            microprice_bias_bps_mean=0.0,
            book_events=1,
            book_coverage_ms=60_000,
            book_available=True,
        )

    request_ts_ms = bar_ts_values[-1]
    provider = LiveFeatureProviderV4(
        feature_columns=("oflow_v1_flow_sign_persistence_12",),
        tf="5m",
        quote="KRW",
        parquet_root=parquet_root,
        candles_dataset_name="candles_api_v1",
        bootstrap_1m_bars=2000,
        bootstrap_end_ts_ms=request_ts_ms,
        micro_snapshot_provider=_TimeKeyedMicroSnapshotProvider(snapshot_map),
        context_micro_required=True,
        context_history_bars=12,
    )

    frame = provider.build_frame(ts_ms=request_ts_ms, markets=["KRW-BTC"])

    assert frame.height == 1
    row = frame.row(0, named=True)
    assert float(row["oflow_v1_flow_sign_persistence_12"]) == pytest.approx(0.0, rel=0, abs=1e-6)
    stats = provider.last_build_stats()
    assert int(stats["context_history_bars"]) == 12


def test_live_feature_provider_v4_history_context_matches_repeated_row_builds(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    _write_one_m_candles(dataset_root=parquet_root / "candles_api_v1", market="KRW-BTC", start_ts_ms=60_000, count=700)
    snapshot_map: dict[tuple[str, int], MicroSnapshot | None] = {}
    bar_ts_values = [300_000 + (i * 300_000) for i in range(6)]
    for index, ts_value in enumerate(bar_ts_values):
        is_buy = index % 2 == 0
        snapshot_map[("KRW-BTC", ts_value)] = MicroSnapshot(
            market="KRW-BTC",
            snapshot_ts_ms=ts_value,
            last_event_ts_ms=ts_value,
            trade_events=1,
            trade_count=1,
            buy_count=1 if is_buy else 0,
            sell_count=0 if is_buy else 1,
            trade_coverage_ms=60_000,
            trade_notional_krw=100.0,
            trade_imbalance=1.0 if is_buy else -1.0,
            trade_source="ws",
            trade_volume_total=1.0,
            buy_volume=1.0 if is_buy else 0.0,
            sell_volume=0.0 if is_buy else 1.0,
            vwap=100.0,
            avg_trade_size=1.0,
            max_trade_size=1.0,
            last_trade_price=100.0,
            mid_mean=100.0,
            spread_bps_mean=1.0,
            depth_top5_notional_krw=1_000.0,
            depth_bid_top5_notional_krw=500.0,
            depth_ask_top5_notional_krw=500.0,
            imbalance_top5_mean=0.0,
            microprice_bias_bps_mean=0.0,
            book_events=1,
            book_coverage_ms=60_000,
            book_available=True,
        )

    request_ts_ms = bar_ts_values[-1]
    provider_context = LiveFeatureProviderV4(
        feature_columns=("logret_1", "one_m_count", "m_trade_count"),
        tf="5m",
        quote="KRW",
        parquet_root=parquet_root,
        candles_dataset_name="candles_api_v1",
        bootstrap_1m_bars=2000,
        bootstrap_end_ts_ms=request_ts_ms,
        micro_snapshot_provider=_TimeKeyedMicroSnapshotProvider(snapshot_map),
        context_micro_required=True,
        context_history_bars=6,
    )
    frame_context, _ = provider_context._build_runtime_context_frame(
        ts_ms=request_ts_ms,
        markets=["KRW-BTC"],
        feature_columns=("logret_1", "one_m_count", "m_trade_count"),
        provider_name="DBG",
        missing_feature_warn_ratio=1.0,
        missing_feature_skip_ratio=1.0,
        history_bars=6,
    )
    frame_context = frame_context.sort("ts_ms")

    provider_manual = LiveFeatureProviderV4(
        feature_columns=("logret_1", "one_m_count", "m_trade_count"),
        tf="5m",
        quote="KRW",
        parquet_root=parquet_root,
        candles_dataset_name="candles_api_v1",
        bootstrap_1m_bars=2000,
        bootstrap_end_ts_ms=request_ts_ms,
        micro_snapshot_provider=_TimeKeyedMicroSnapshotProvider(snapshot_map),
        context_micro_required=True,
        context_history_bars=6,
    )
    rows = []
    for ts_value in bar_ts_values:
        row, reason, missing_cells, missing_features = provider_manual._build_runtime_market_row(
            market="KRW-BTC",
            ts_ms=ts_value,
            feature_columns=("logret_1", "one_m_count", "m_trade_count"),
        )
        assert reason in {"OK", "MISSING_MICRO"}
        assert row is not None
        rows.append(row)
    frame_manual = pl.DataFrame(rows).sort("ts_ms")

    assert frame_context.select(["ts_ms", "market", "close", "logret_1", "one_m_count", "m_trade_count"]).to_dicts() == frame_manual.select(
        ["ts_ms", "market", "close", "logret_1", "one_m_count", "m_trade_count"]
    ).to_dicts()


def _write_one_m_candles(*, dataset_root: Path, market: str, start_ts_ms: int = 60_000, count: int = 599) -> None:
    part_dir = dataset_root / "tf=1m" / f"market={market}" / "date=2026-01-01"
    part_dir.mkdir(parents=True, exist_ok=True)
    ts_values = [int(start_ts_ms) + (i * 60_000) for i in range(int(count))]
    close_values = [100.0 + (i * 0.05) for i in range(len(ts_values))]
    frame = pl.DataFrame(
        {
            "ts_ms": ts_values,
            "open": [value - 0.02 for value in close_values],
            "high": [value + 0.05 for value in close_values],
            "low": [value - 0.05 for value in close_values],
            "close": close_values,
            "volume_base": [10.0 + (i % 5) for i in range(len(ts_values))],
        }
    )
    frame.write_parquet(part_dir / "part-000.parquet")

def _save_model_run(*, registry_root: Path, model_family: str, run_id: str, feature_columns: list[str]) -> None:
    save_run(
        RegistrySavePayload(
            registry_root=registry_root,
            model_family=model_family,
            run_id=run_id,
            model_bundle={"model_type": "xgboost", "scaler": None, "estimator": _DummyEstimator()},
            metrics={},
            thresholds={},
            feature_spec={"feature_columns": list(feature_columns)},
            label_spec={"label_columns": ["y_reg_net_12", "y_cls_topq_12"]},
            train_config={"dataset_root": "unused", "feature_columns": list(feature_columns)},
            data_fingerprint={},
            leaderboard_row={"run_id": run_id, "test_precision_top5": 0.1},
            model_card_text="# live_v4 test model",
        )
    )
