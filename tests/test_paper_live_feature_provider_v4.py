from __future__ import annotations

import asyncio
import json
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import polars as pl

from autobot.models.registry import RegistrySavePayload, save_run
from autobot.paper.engine import PaperRunEngine, PaperRunSettings
from autobot.paper.live_features_v4 import LiveFeatureProviderV4
from autobot.paper.live_features_v4_native import LiveFeatureProviderV4Native
from autobot.paper.sim_exchange import MarketRules
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


def test_live_feature_provider_v4_builds_ctrend_columns_from_current_history(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    request_dt = datetime(2026, 1, 2, 0, 5, tzinfo=timezone.utc)
    request_ts_ms = int(request_dt.timestamp() * 1000)
    _write_one_m_candles(
        dataset_root=parquet_root / "candles_api_v1",
        market="KRW-BTC",
        start_ts_ms=request_ts_ms - (720 * 60_000),
        count=900,
    )
    _write_ctrend_history(
        dataset_root=parquet_root / "candles_v1",
        market="KRW-BTC",
        end_date=request_dt.date() - timedelta(days=1),
        days=280,
    )

    provider = LiveFeatureProviderV4(
        feature_columns=(
            "logret_1",
            "btc_ret_12",
            "oflow_v1_signed_volume_imbalance_1",
            "ctrend_v1_rsi_14",
            "ctrend_v1_cci_20",
            "ctrend_v1_ma_gap_200",
            "ctrend_v1_vol_ma_gap_200",
            "ctrend_v1_boll_width_20_2",
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
    assert frame.height == 1
    row = frame.row(0, named=True)
    assert row["oflow_v1_signed_volume_imbalance_1"] == 0.0
    assert row["ctrend_v1_rsi_14"] is not None
    assert row["ctrend_v1_cci_20"] is not None
    assert row["ctrend_v1_ma_gap_200"] is not None
    assert row["ctrend_v1_vol_ma_gap_200"] is not None
    assert row["ctrend_v1_boll_width_20_2"] is not None
    stats = provider.last_build_stats()
    assert stats["hard_gate_triggered"] is False
    assert stats["ctrend_requested_feature_count"] == 5


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


def test_paper_engine_model_alpha_live_v4_scores_with_ctrend_features(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    registry_root = tmp_path / "registry"
    now_ms = int(time.time() * 1000)
    base_ts = (now_ms // 300_000) * 300_000
    request_dt = datetime.fromtimestamp(base_ts / 1000.0, tz=timezone.utc)
    _write_one_m_candles(
        dataset_root=parquet_root / "candles_api_v1",
        market="KRW-BTC",
        start_ts_ms=base_ts - (1_000 * 60_000),
        count=1_200,
    )
    _write_ctrend_history(
        dataset_root=parquet_root / "candles_v1",
        market="KRW-BTC",
        end_date=request_dt.date() - timedelta(days=1),
        days=300,
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
    assert any(int(item.get("payload", {}).get("scored_rows", 0)) > 0 for item in selections)
    assert all("NO_FEATURE_ROWS_AT_TS" not in item.get("payload", {}).get("reasons", {}) for item in selections)
    built_events = [item for item in payloads if item.get("event_type") == "LIVE_FEATURES_BUILT"]
    assert built_events
    built_payload = built_events[-1].get("payload") or {}
    assert str(built_payload.get("provider")) == "LIVE_V4"
    assert int(built_payload.get("ctrend_requested_feature_count", 0)) == 4
    assert list(built_payload.get("missing_feature_columns") or []) == []


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


def _write_ctrend_history(*, dataset_root: Path, market: str, end_date: date, days: int) -> None:
    part_dir = dataset_root / "tf=5m" / f"market={market}" / "date=history"
    part_dir.mkdir(parents=True, exist_ok=True)
    first_date = end_date - timedelta(days=max(int(days) - 1, 0))
    ts_values: list[int] = []
    close_values: list[float] = []
    volume_values: list[float] = []
    for index in range(int(days)):
        current = first_date + timedelta(days=index)
        ts_values.append(int(datetime(current.year, current.month, current.day, 0, 5, tzinfo=timezone.utc).timestamp() * 1000))
        close_values.append(100.0 + (index * 0.4))
        volume_values.append(1_000.0 + (index * 3.0))
    pl.DataFrame(
        {
            "ts_ms": ts_values,
            "open": [value - 0.1 for value in close_values],
            "high": [value + 0.2 for value in close_values],
            "low": [value - 0.2 for value in close_values],
            "close": close_values,
            "volume_base": volume_values,
        }
    ).write_parquet(part_dir / "part-000.parquet")


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
