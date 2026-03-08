from __future__ import annotations

import asyncio
import json
from pathlib import Path

from autobot.backtest.strategy_adapter import StrategyFillEvent, StrategyOrderIntent, StrategyStepResult
from autobot.paper.engine import PaperRunEngine, PaperRunSettings
from autobot.paper.sim_exchange import MarketRules
from autobot.strategy.micro_order_policy import (
    MicroOrderPolicySettings,
    MicroOrderPolicyTierSettings,
    MicroOrderPolicyTieringSettings,
    MicroOrderPolicyTiersSettings,
)
from autobot.strategy.micro_snapshot import MicroSnapshot
from autobot.strategy.model_alpha_v1 import ModelAlphaExecutionSettings, ModelAlphaPositionSettings, ModelAlphaSettings
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
        self.last_duration_sec: float | None = None

    async def stream_ticker(self, markets: list[str], *, duration_sec: float | None = None):
        _ = markets
        self.last_duration_sec = duration_sec
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


class _DummyModelStrategy:
    def __init__(
        self,
        *,
        score: float = 0.9,
        prob: float = 0.9,
        intent_meta: dict[str, object] | None = None,
    ) -> None:
        self.on_ts_calls = 0
        self.on_fill_calls = 0
        self._score = float(score)
        self._prob = float(prob)
        self._intent_meta = dict(intent_meta or {})

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
                    score=self._score,
                    prob=self._prob,
                    meta=dict(self._intent_meta),
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
            trade_price=99.0,
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
        model_alpha=ModelAlphaSettings(
            model_ref="latest_v3",
            execution=ModelAlphaExecutionSettings(price_mode="PASSIVE_MAKER", timeout_bars=3, replace_max=4),
        ),
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
    events_payloads = [
        json.loads(line)
        for line in (run_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    event_types = {item.get("event_type") for item in events_payloads}
    assert "MODEL_ALPHA_SELECTION" in event_types
    selection_events = [item for item in events_payloads if item.get("event_type") == "MODEL_ALPHA_SELECTION"]
    assert selection_events
    selection_payload = selection_events[0].get("payload", {})
    assert "eligible_rows" in selection_payload
    assert "min_prob_used" in selection_payload
    assert "min_prob_source" in selection_payload
    assert "top_pct_used" in selection_payload
    assert "min_candidates_used" in selection_payload
    intent_events = [item for item in events_payloads if item.get("event_type") == "INTENT_CREATED"]
    assert intent_events
    intent_meta = ((intent_events[0].get("payload") or {}).get("meta") or {})
    exec_profile = intent_meta.get("exec_profile") or {}
    assert exec_profile.get("price_mode") == "PASSIVE_MAKER"
    assert int(exec_profile.get("timeout_ms", 0)) == 900_000
    assert int(exec_profile.get("replace_interval_ms", 0)) == 900_000
    assert int(exec_profile.get("max_replaces", -1)) == 4


def test_paper_engine_model_alpha_micro_policy_guards_strategy_exec_profile(tmp_path: Path) -> None:
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
        tiers=MicroOrderPolicyTiersSettings(
            low=MicroOrderPolicyTierSettings(
                timeout_ms=120_000,
                replace_interval_ms=60_000,
                max_replaces=1,
                price_mode="PASSIVE_MAKER",
                max_chase_bps=10,
            )
        ),
    )
    run_settings = PaperRunSettings(
        duration_sec=2,
        quote="KRW",
        top_n=1,
        tf="5m",
        strategy="model_alpha_v1",
        model_ref="latest_v3",
        feature_set="v3",
        model_alpha=ModelAlphaSettings(
            model_ref="latest_v3",
            execution=ModelAlphaExecutionSettings(price_mode="JOIN", timeout_bars=3, replace_max=4),
        ),
        micro_order_policy=policy,
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
        micro_snapshot_provider=_StableMicroProvider(),  # type: ignore[arg-type]
        dummy_strategy=dummy_strategy,
    )

    summary = asyncio.run(engine.run())
    assert summary.micro_quality_score_mean > 0.0
    assert summary.runtime_risk_multiplier_mean > 0.0
    assert summary.rolling_windows_total >= 1
    assert summary.rolling_active_windows >= 0
    run_dir = Path(summary.run_dir)
    payloads = [
        json.loads(line)
        for line in (run_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    intent_events = [item for item in payloads if item.get("event_type") == "INTENT_CREATED"]
    assert intent_events
    intent_meta = ((intent_events[0].get("payload") or {}).get("meta") or {})
    exec_profile = intent_meta.get("exec_profile") or {}
    assert exec_profile.get("price_mode") == "PASSIVE_MAKER"
    assert int(exec_profile.get("timeout_ms", 0)) == 900_000
    assert int(exec_profile.get("replace_interval_ms", 0)) == 900_000
    assert int(exec_profile.get("max_replaces", -1)) == 1


def test_paper_engine_model_alpha_entry_sizing_respects_min_total_buffer(tmp_path: Path) -> None:
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
        model_alpha=ModelAlphaSettings(
            model_ref="latest_v3",
            position=ModelAlphaPositionSettings(
                max_positions_total=2,
                cooldown_bars=0,
                entry_min_notional_buffer_bps=100.0,
            ),
        ),
        print_every_sec=60,
        decision_interval_sec=0.1,
        universe_refresh_sec=1,
        universe_hold_sec=0,
        momentum_window_sec=60,
        min_momentum_pct=0.2,
        starting_krw=50_000.0,
        per_trade_krw=4_900.0,
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
    run_dir = Path(summary.run_dir)
    payloads = [
        json.loads(line)
        for line in (run_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    intent_events = [item for item in payloads if item.get("event_type") == "INTENT_CREATED"]
    assert intent_events
    intent_payload = intent_events[0].get("payload") or {}
    assert float(intent_payload.get("price", 0.0)) * float(intent_payload.get("volume", 0.0)) >= 5_050.0


def test_paper_engine_model_alpha_entry_sizing_uses_notional_multiplier(tmp_path: Path) -> None:
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
        model_alpha=ModelAlphaSettings(
            model_ref="latest_v3",
            position=ModelAlphaPositionSettings(
                max_positions_total=2,
                cooldown_bars=0,
                sizing_mode="prob_ramp",
                size_multiplier_min=0.5,
                size_multiplier_max=1.5,
            ),
        ),
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
    dummy_strategy = _DummyModelStrategy(intent_meta={"notional_multiplier": 1.4})
    engine = _PaperEngineWithDummyModel(
        upbit_settings=settings,
        run_settings=run_settings,
        ws_client=_FakeWsClient(events),
        market_loader=lambda quote: ["KRW-BTC"] if quote == "KRW" else [],
        rules_provider=_StaticRulesProvider(),
        dummy_strategy=dummy_strategy,
    )

    summary = asyncio.run(engine.run())
    run_dir = Path(summary.run_dir)
    payloads = [
        json.loads(line)
        for line in (run_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    intent_events = [item for item in payloads if item.get("event_type") == "INTENT_CREATED"]
    assert intent_events
    intent_payload = intent_events[0].get("payload") or {}
    meta = intent_payload.get("meta") or {}
    assert float(meta.get("notional_multiplier", 0.0)) == 1.4
    assert float(meta.get("target_notional_quote", 0.0)) >= 14_000.0
    assert float(intent_payload.get("price", 0.0)) * float(intent_payload.get("volume", 0.0)) >= float(
        meta.get("target_notional_quote", 0.0)
    )


def test_paper_engine_duration_zero_runs_until_stream_ends(tmp_path: Path) -> None:
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
        duration_sec=0,
        quote="KRW",
        top_n=1,
        tf="5m",
        strategy="model_alpha_v1",
        model_ref="latest_v3",
        feature_set="v3",
        print_every_sec=60,
        decision_interval_sec=0.1,
        universe_refresh_sec=1,
        universe_hold_sec=0,
        out_root_dir=str(tmp_path),
    )
    dummy_strategy = _DummyModelStrategy()
    ws_client = _FakeWsClient(events)
    engine = _PaperEngineWithDummyModel(
        upbit_settings=settings,
        run_settings=run_settings,
        ws_client=ws_client,
        market_loader=lambda quote: ["KRW-BTC"] if quote == "KRW" else [],
        rules_provider=_StaticRulesProvider(),
        dummy_strategy=dummy_strategy,
    )

    summary = asyncio.run(engine.run())
    assert ws_client.last_duration_sec is None
    assert summary.duration_sec > 0.0
    assert dummy_strategy.on_ts_calls >= 1
