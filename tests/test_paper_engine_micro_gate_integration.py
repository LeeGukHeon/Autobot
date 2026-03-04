from __future__ import annotations

import asyncio
import json
from pathlib import Path

from autobot.paper.engine import PaperRunEngine, PaperRunSettings
from autobot.paper.sim_exchange import MarketRules
from autobot.strategy.micro_gate_v1 import MicroGateSettings, MicroGateTradeSettings
from autobot.strategy.micro_snapshot import MicroSnapshot
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


class _LowLiquidityProvider:
    def get(self, market: str, ts_ms: int) -> MicroSnapshot | None:
        _ = market
        return MicroSnapshot(
            market="KRW-BTC",
            snapshot_ts_ms=ts_ms,
            last_event_ts_ms=ts_ms,
            trade_events=0,
            trade_coverage_ms=0,
            trade_notional_krw=0.0,
            trade_imbalance=None,
            trade_source="ws",
            spread_bps_mean=None,
            depth_top5_notional_krw=None,
            book_events=0,
            book_coverage_ms=0,
            book_available=False,
        )


def test_paper_engine_logs_micro_gate_reasons_when_enabled(tmp_path: Path) -> None:
    events = [
        TickerEvent(
            market="KRW-BTC",
            ts_ms=1_000,
            trade_price=100.0,
            acc_trade_price_24h=1_000_000_000_000.0,
        ),
        TickerEvent(
            market="KRW-BTC",
            ts_ms=61_000,
            trade_price=101.0,
            acc_trade_price_24h=1_000_500_000_000.0,
        ),
        TickerEvent(
            market="KRW-BTC",
            ts_ms=62_000,
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
        micro_gate=MicroGateSettings(
            enabled=True,
            mode="trade_only",
            on_missing="warn_allow",
            trade=MicroGateTradeSettings(min_trade_events=1),
        ),
    )
    engine = PaperRunEngine(
        upbit_settings=settings,
        run_settings=run_settings,
        ws_client=_FakeWsClient(events),
        market_loader=lambda quote: ["KRW-BTC"] if quote == "KRW" else [],
        rules_provider=_StaticRulesProvider(),
        micro_snapshot_provider=_LowLiquidityProvider(),  # type: ignore[arg-type]
    )

    summary = asyncio.run(engine.run())
    assert summary.candidates_blocked_by_micro >= 1
    assert summary.micro_blocked_reasons.get("LOW_LIQUIDITY_TRADE", 0) >= 1

    run_dir = Path(summary.run_dir)
    blocked_events = [
        json.loads(line)
        for line in (run_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip() and json.loads(line).get("event_type") == "TRADE_GATE_BLOCKED"
    ]
    assert blocked_events
    payload = blocked_events[0]["payload"]
    assert "LOW_LIQUIDITY_TRADE" in payload.get("gate_reasons", [])
