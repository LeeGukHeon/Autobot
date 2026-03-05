from __future__ import annotations

import asyncio
import json
from pathlib import Path

from autobot.backtest.strategy_adapter import StrategyFillEvent, StrategyOrderIntent, StrategyStepResult
from autobot.paper.engine import PaperRunEngine, PaperRunSettings
from autobot.paper.sim_exchange import MarketRules
from autobot.strategy.model_alpha_v1 import ModelAlphaSettings
from autobot.upbit.config import (
    UpbitAuthSettings,
    UpbitRateLimitSettings,
    UpbitRetrySettings,
    UpbitSettings,
    UpbitTimeoutSettings,
    UpbitWebSocketSettings,
)
from autobot.upbit.ws.models import TickerEvent


class _FakeWsClient:
    def __init__(self, events: list[TickerEvent]) -> None:
        self._events = events

    async def stream_ticker(self, markets: list[str], *, duration_sec: float | None = None):
        _ = (markets, duration_sec)
        for event in self._events:
            await asyncio.sleep(0.25)
            yield event


class _StaticRulesProvider:
    def get_rules(self, *, market: str, reference_price: float, ts_ms: int) -> MarketRules:
        _ = (market, reference_price, ts_ms)
        return MarketRules(
            bid_fee=0.0005,
            ask_fee=0.0005,
            maker_bid_fee=0.0002,
            maker_ask_fee=0.0002,
            min_total=5000.0,
            tick_size=1.0,
        )


class _DummyModelStrategy:
    def __init__(self) -> None:
        self.on_ts_calls = 0
        self.on_fill_calls = 0

    def on_ts(
        self,
        *,
        ts_ms: int,
        active_markets: list[str],
        latest_prices: dict[str, float],
        open_markets: set[str],
    ) -> StrategyStepResult:
        _ = (ts_ms, active_markets)
        self.on_ts_calls += 1
        price = float(latest_prices.get("KRW-BTC", 100.0))
        if "KRW-BTC" in open_markets:
            return StrategyStepResult(scored_rows=1, selected_rows=0)
        return StrategyStepResult(
            intents=(
                StrategyOrderIntent(
                    market="KRW-BTC",
                    side="bid",
                    ref_price=price,
                    reason_code="MODEL_ALPHA_ENTRY_V1",
                    score=0.9,
                    prob=0.9,
                ),
            ),
            scored_rows=1,
            selected_rows=1,
        )

    def on_fill(self, event: StrategyFillEvent) -> None:
        _ = event
        self.on_fill_calls += 1


class _PaperEngineWithDummyModel(PaperRunEngine):
    def __init__(self, *args, dummy_strategy: _DummyModelStrategy, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._dummy_strategy = dummy_strategy

    def _build_model_alpha_strategy(
        self,
        *,
        active_markets: list[str],
        decision_start_ts_ms: int,
        decision_end_ts_ms: int,
    ):
        _ = (active_markets, decision_start_ts_ms, decision_end_ts_ms)
        return self._dummy_strategy


def test_paper_engine_model_alpha_strategy_cycle(tmp_path: Path) -> None:
    events = [
        TickerEvent(
            market="KRW-BTC",
            ts_ms=1_000,
            trade_price=100.0,
            acc_trade_price_24h=1_000_000_000_000.0,
        ),
        TickerEvent(
            market="KRW-BTC",
            ts_ms=301_000,
            trade_price=101.0,
            acc_trade_price_24h=1_000_500_000_000.0,
        ),
        TickerEvent(
            market="KRW-BTC",
            ts_ms=302_000,
            trade_price=101.0,
            acc_trade_price_24h=1_001_000_000_000.0,
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
        model_ref="latest_v3",
        feature_set="v3",
        model_alpha=ModelAlphaSettings(model_ref="latest_v3"),
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
    )
    dummy_strategy = _DummyModelStrategy()
    engine = _PaperEngineWithDummyModel(
        upbit_settings=settings,
        run_settings=run_settings,
        ws_client=_FakeWsClient(events),
        market_loader=lambda quote: ["KRW-BTC"] if quote == "KRW" else [],
        rules_provider=_StaticRulesProvider(),
        dummy_strategy=dummy_strategy,
    )

    summary = asyncio.run(engine.run())

    assert summary.orders_submitted >= 1
    assert dummy_strategy.on_ts_calls >= 1
    assert dummy_strategy.on_fill_calls >= 1

    run_dir = Path(summary.run_dir)
    event_types = {
        json.loads(line).get("event_type")
        for line in (run_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    }
    assert "MODEL_ALPHA_SELECTION" in event_types
