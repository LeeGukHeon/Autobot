from __future__ import annotations

import asyncio
import json
from pathlib import Path

from autobot.paper.engine import PaperRunEngine, PaperRunSettings
from autobot.paper.sim_exchange import MarketRules
from autobot.strategy.micro_order_policy import MicroOrderPolicySettings
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
            await asyncio.sleep(0.01)
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


def test_paper_live_ws_provider_selection_and_warmup_metrics(tmp_path: Path) -> None:
    events = [
        TickerEvent(market="KRW-BTC", ts_ms=1_000, trade_price=100.0, acc_trade_price_24h=1_000_000_000.0),
        TickerEvent(market="KRW-BTC", ts_ms=2_000, trade_price=101.0, acc_trade_price_24h=1_001_000_000.0),
        TickerEvent(market="KRW-BTC", ts_ms=3_000, trade_price=102.0, acc_trade_price_24h=1_002_000_000.0),
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

