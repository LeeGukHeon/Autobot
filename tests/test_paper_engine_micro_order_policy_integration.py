from __future__ import annotations

import asyncio
import json
from pathlib import Path

from autobot.paper.engine import PaperRunEngine, PaperRunSettings
from autobot.paper.sim_exchange import MarketRules
from autobot.strategy.micro_order_policy import (
    MicroOrderPolicySafetySettings,
    MicroOrderPolicySettings,
    MicroOrderPolicyTierSettings,
    MicroOrderPolicyTieringSettings,
    MicroOrderPolicyTiersSettings,
)
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
            min_total=5_000.0,
            tick_size=1.0,
        )


class _StableMicroProvider:
    def get(self, market: str, ts_ms: int) -> MicroSnapshot | None:
        return MicroSnapshot(
            market=market,
            snapshot_ts_ms=ts_ms,
            last_event_ts_ms=ts_ms,
            trade_events=1,
            trade_coverage_ms=60_000,
            trade_notional_krw=100.0,
            trade_imbalance=0.0,
            trade_source="ws",
            spread_bps_mean=5.0,
            depth_top5_notional_krw=100_000.0,
            book_events=1,
            book_coverage_ms=60_000,
            book_available=True,
        )


def test_paper_engine_emits_replace_events_with_micro_order_policy(tmp_path: Path) -> None:
    events = [
        TickerEvent(market="KRW-BTC", ts_ms=1_000, trade_price=100.0, acc_trade_price_24h=1_000_000_000.0),
        TickerEvent(market="KRW-BTC", ts_ms=1_200, trade_price=101.0, acc_trade_price_24h=1_001_000_000.0),
        TickerEvent(market="KRW-BTC", ts_ms=1_400, trade_price=102.0, acc_trade_price_24h=1_002_000_000.0),
        TickerEvent(market="KRW-BTC", ts_ms=1_600, trade_price=99.0, acc_trade_price_24h=1_003_000_000.0),
    ]

    settings = UpbitSettings(
        base_url="https://api.upbit.com",
        timeout=UpbitTimeoutSettings(),
        auth=UpbitAuthSettings(),
        ratelimit=UpbitRateLimitSettings(),
        retry=UpbitRetrySettings(),
        websocket=UpbitWebSocketSettings(),
    )
    policy = MicroOrderPolicySettings(
        enabled=True,
        on_missing="static_fallback",
        tiering=MicroOrderPolicyTieringSettings(w_notional=1.0, w_events=0.5, t1=100.0, t2=200.0),
        safety=MicroOrderPolicySafetySettings(min_replace_interval_ms_global=1),
        tiers=MicroOrderPolicyTiersSettings(
            low=MicroOrderPolicyTierSettings(
                timeout_ms=5_000,
                replace_interval_ms=300,
                max_replaces=1,
                price_mode="PASSIVE_MAKER",
                max_chase_bps=1_000,
            )
        ),
    )
    run_settings = PaperRunSettings(
        duration_sec=3,
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
        micro_order_policy=policy,
    )
    engine = PaperRunEngine(
        upbit_settings=settings,
        run_settings=run_settings,
        ws_client=_FakeWsClient(events),
        market_loader=lambda quote: ["KRW-BTC"] if quote == "KRW" else [],
        rules_provider=_StaticRulesProvider(),  # type: ignore[arg-type]
        micro_snapshot_provider=_StableMicroProvider(),  # type: ignore[arg-type]
    )

    summary = asyncio.run(engine.run())
    assert summary.orders_submitted >= 1
    assert summary.orders_filled >= 1

    run_dir = Path(summary.run_dir)
    events_payloads = [
        json.loads(line)
        for line in (run_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert any(item.get("event_type") == "ORDER_SUBMITTED" for item in events_payloads)

    report = json.loads((run_dir / "micro_order_policy_report.json").read_text(encoding="utf-8"))
    assert report["tiers"].get("LOW", 0) >= 1
    assert isinstance(report.get("replace_reasons", {}), dict)
    assert "tick_bps_stats" in report
    assert "cross_block_reasons" in report
    assert "cross_allowed_count" in report
    assert "cross_used_count" in report
    assert "resolver_failed_fallback_used_count" in report
    assert (run_dir / "slippage_by_market.csv").exists()
    assert (run_dir / "price_mode_by_market.csv").exists()
