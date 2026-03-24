from __future__ import annotations

import asyncio
import json
from pathlib import Path

import autobot.paper.engine as paper_engine_mod
from autobot.paper.engine import PaperRunEngine, PaperRunSettings
from autobot.paper.sim_exchange import MarketRules
from autobot.strategy.micro_gate_v1 import MicroGateSettings
from autobot.strategy.micro_snapshot import LiveWsMicroSnapshotProvider, LiveWsProviderSettings
from autobot.strategy.model_alpha_v1 import ModelAlphaExecutionSettings, ModelAlphaSettings
from autobot.strategy.micro_order_policy import MicroOrderPolicySettings
from autobot.upbit.config import (
    UpbitAuthSettings,
    UpbitRateLimitSettings,
    UpbitRetrySettings,
    UpbitSettings,
    UpbitTimeoutSettings,
    UpbitWebSocketSettings,
)
from autobot.upbit.ws.models import OrderbookEvent, OrderbookUnit, TickerEvent, TradeEvent


class _FakeWsClient:
    def __init__(self, events: list[TickerEvent]) -> None:
        self._events = events

    async def stream_ticker(self, markets: list[str], *, duration_sec: float | None = None):
        _ = (markets, duration_sec)
        for event in self._events:
            await asyncio.sleep(0.01)
            yield event


class _FakeWsClientWithMicro(_FakeWsClient):
    async def stream_trade(self, markets: list[str], *, duration_sec: float | None = None):
        _ = (markets, duration_sec)
        yield TradeEvent(
            market="KRW-BTC",
            ts_ms=2_500,
            trade_price=101.0,
            trade_volume=0.25,
            ask_bid="BID",
            sequential_id=10,
        )

    async def stream_orderbook(self, markets: list[str], *, duration_sec: float | None = None, level: int | str | None = 0):
        _ = (markets, duration_sec, level)
        yield OrderbookEvent(
            market="KRW-BTC",
            ts_ms=2_600,
            total_ask_size=10.0,
            total_bid_size=9.0,
            units=(
                OrderbookUnit(ask_price=101.2, ask_size=1.2, bid_price=100.8, bid_size=1.3),
                OrderbookUnit(ask_price=101.3, ask_size=1.1, bid_price=100.7, bid_size=1.2),
            ),
        )


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


def _make_settings() -> UpbitSettings:
    return UpbitSettings(
        base_url="https://api.upbit.com",
        timeout=UpbitTimeoutSettings(),
        auth=UpbitAuthSettings(),
        ratelimit=UpbitRateLimitSettings(),
        retry=UpbitRetrySettings(),
        websocket=UpbitWebSocketSettings(),
    )


def test_paper_live_ws_provider_selection_and_warmup_metrics(tmp_path: Path) -> None:
    events = [
        TickerEvent(market="KRW-BTC", ts_ms=1_000, trade_price=100.0, acc_trade_price_24h=1_000_000_000.0),
        TickerEvent(market="KRW-BTC", ts_ms=2_000, trade_price=101.0, acc_trade_price_24h=1_001_000_000.0),
        TickerEvent(market="KRW-BTC", ts_ms=3_000, trade_price=102.0, acc_trade_price_24h=1_002_000_000.0),
    ]
    settings = _make_settings()
    run_settings = PaperRunSettings(
        duration_sec=2,
        quote="KRW",
        top_n=1,
        print_every_sec=60,
        decision_interval_sec=0.1,
        universe_refresh_sec=1,
        universe_hold_sec=0,
        momentum_window_sec=60,
        min_momentum_pct=0.2,
        starting_krw=50_000.0,
        per_trade_krw=10_000.0,
        max_positions=2,
        out_root_dir=str(tmp_path),
        micro_order_policy=MicroOrderPolicySettings(enabled=True, on_missing="static_fallback"),
        paper_micro_provider="live_ws",
        paper_micro_warmup_sec=10,
        paper_micro_warmup_min_trade_events_per_market=1,
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

    events_payloads = [
        json.loads(line)
        for line in (run_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    run_started = next(item for item in events_payloads if item.get("event_type") == "RUN_STARTED")
    assert run_started["payload"]["micro_provider"] == "LIVE_WS"

    summary_payload = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    assert "warmup_elapsed_sec" in summary_payload
    assert summary_payload["warmup_elapsed_sec"] >= 0.0
    assert summary_payload["warmup_trade_events_total"] >= 1
    assert summary_payload["micro_cache_markets_with_samples"] >= 1


def test_paper_live_ws_provider_selection_allows_micro_gate_only_runtime(tmp_path: Path) -> None:
    engine = PaperRunEngine(
        upbit_settings=_make_settings(),
        run_settings=PaperRunSettings(
            out_root_dir=str(tmp_path),
            paper_micro_provider="live_ws",
            micro_gate=MicroGateSettings(
                enabled=True,
                live_ws=LiveWsProviderSettings(enabled=True),
            ),
            micro_order_policy=MicroOrderPolicySettings(enabled=False),
        ),
        market_loader=lambda quote: ["KRW-BTC"] if quote == "KRW" else [],
        rules_provider=_StaticRulesProvider(),  # type: ignore[arg-type]
    )

    provider = engine._resolve_micro_snapshot_provider(markets=["KRW-BTC"])

    assert isinstance(provider, LiveWsMicroSnapshotProvider)
    assert engine._runtime_state["micro_provider_decision"]["effective_provider"] == "LIVE_WS"
    assert engine._runtime_state["micro_provider_decision"]["provider_decision"] == "LIVE_WS_FORCED"


def test_paper_live_ws_provider_consumes_real_trade_and_orderbook_streams(tmp_path: Path) -> None:
    events = [
        TickerEvent(market="KRW-BTC", ts_ms=1_000, trade_price=100.0, acc_trade_price_24h=1_000_000_000.0),
        TickerEvent(market="KRW-BTC", ts_ms=3_000, trade_price=101.0, acc_trade_price_24h=1_001_000_000.0),
    ]
    settings = _make_settings()
    run_settings = PaperRunSettings(
        duration_sec=1,
        quote="KRW",
        top_n=1,
        print_every_sec=60,
        decision_interval_sec=0.1,
        universe_refresh_sec=1,
        universe_hold_sec=0,
        starting_krw=50_000.0,
        per_trade_krw=10_000.0,
        max_positions=2,
        out_root_dir=str(tmp_path),
        micro_order_policy=MicroOrderPolicySettings(enabled=True, on_missing="static_fallback"),
        paper_micro_provider="live_ws",
        paper_micro_warmup_sec=1,
        paper_micro_warmup_min_trade_events_per_market=1,
    )
    engine = PaperRunEngine(
        upbit_settings=settings,
        run_settings=run_settings,
        ws_client=_FakeWsClientWithMicro(events),
        market_loader=lambda quote: ["KRW-BTC"] if quote == "KRW" else [],
        rules_provider=_StaticRulesProvider(),  # type: ignore[arg-type]
    )

    asyncio.run(engine.run())

    provider = engine._micro_snapshot_provider
    assert isinstance(provider, LiveWsMicroSnapshotProvider)
    snapshot = provider.get("KRW-BTC", 3_000)
    assert snapshot is not None
    assert snapshot.trade_events >= 1
    assert snapshot.book_available is True
    assert snapshot.book_events >= 1
    assert snapshot.best_ask_price == 101.2
    assert snapshot.best_bid_price == 100.8
    assert snapshot.best_ask_notional_krw == 121.44
    assert snapshot.best_bid_notional_krw == 131.04
    assert snapshot.ask_levels == ((101.2, 1.2), (101.3, 1.1))
    assert snapshot.bid_levels == ((100.8, 1.3), (100.7, 1.2))


def test_paper_provider_selection_allows_execution_snapshot_runtime_without_gate_or_policy(
    tmp_path: Path,
    monkeypatch,
) -> None:
    offline_root = tmp_path / "micro_v1"
    offline_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(paper_engine_mod, "_resolve_micro_root", lambda dataset_name: offline_root)

    engine = PaperRunEngine(
        upbit_settings=_make_settings(),
        run_settings=PaperRunSettings(
            out_root_dir=str(tmp_path),
            strategy="model_alpha_v1",
            model_alpha=ModelAlphaSettings(
                execution=ModelAlphaExecutionSettings(use_learned_recommendations=True),
            ),
            micro_gate=MicroGateSettings(enabled=False),
            micro_order_policy=MicroOrderPolicySettings(enabled=False),
        ),
        market_loader=lambda quote: ["KRW-BTC"] if quote == "KRW" else [],
        rules_provider=_StaticRulesProvider(),  # type: ignore[arg-type]
    )

    provider = engine._resolve_micro_snapshot_provider(markets=["KRW-BTC"])

    assert provider is not None
    assert engine._runtime_state["micro_provider_decision"]["effective_provider"] == "OFFLINE_PARQUET"
