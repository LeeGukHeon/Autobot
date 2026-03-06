from __future__ import annotations

import asyncio
import json
from pathlib import Path
import time

import numpy as np
import polars as pl

from autobot.models.registry import RegistrySavePayload, save_run
from autobot.paper.engine import PaperRunEngine, PaperRunSettings
from autobot.paper.live_features_v3 import LiveFeatureProviderV3
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


def test_live_feature_provider_v3_builds_row_from_bootstrap_and_ticker(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    _write_one_m_candles(dataset_root=parquet_root / "candles_api_v1", market="KRW-BTC")

    provider = LiveFeatureProviderV3(
        feature_columns=("logret_1", "one_m_count", "tf15m_ret_1", "m_trade_events", "m_spread_proxy"),
        tf="5m",
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
    assert "logret_1" in frame.columns
    assert "one_m_count" in frame.columns

    status = provider.status(now_ts_ms=300_000)
    assert status["provider"] == "LIVE_V3"
    assert int(status["latest_feature_ts_ms"]) == 300_000


def test_paper_engine_model_alpha_live_v3_scores_without_no_feature_rows(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    registry_root = tmp_path / "registry"
    _write_one_m_candles(dataset_root=parquet_root / "candles_api_v1", market="KRW-BTC")
    _save_model_run(registry_root=registry_root, feature_columns=["logret_1"])

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
        model_ref="run_live_v3",
        model_family="train_v3_mtf_micro",
        feature_set="v3",
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
        paper_feature_provider="live_v3",
        paper_live_parquet_root=str(parquet_root),
        paper_live_candles_dataset="candles_api_v1",
        model_alpha=ModelAlphaSettings(
            model_ref="run_live_v3",
            model_family="train_v3_mtf_micro",
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
    assert any(item.get("event_type") == "FEATURE_PROVIDER_STATUS" for item in payloads)
    assert any(item.get("event_type") == "LIVE_FEATURES_BUILT" for item in payloads)


def _write_one_m_candles(*, dataset_root: Path, market: str) -> None:
    part_dir = dataset_root / "tf=1m" / f"market={market}" / "date=2026-01-01"
    part_dir.mkdir(parents=True, exist_ok=True)
    ts_values = [i * 60_000 for i in range(1, 600)]
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


def _save_model_run(*, registry_root: Path, feature_columns: list[str]) -> None:
    save_run(
        RegistrySavePayload(
            registry_root=registry_root,
            model_family="train_v3_mtf_micro",
            run_id="run_live_v3",
            model_bundle={"model_type": "xgboost", "scaler": None, "estimator": _DummyEstimator()},
            metrics={},
            thresholds={},
            feature_spec={"feature_columns": list(feature_columns)},
            label_spec={"label_columns": ["y_reg", "y_cls"]},
            train_config={"dataset_root": "unused", "feature_columns": list(feature_columns)},
            data_fingerprint={},
            leaderboard_row={"run_id": "run_live_v3", "test_precision_top5": 0.1},
            model_card_text="# live_v3 test model",
        )
    )
