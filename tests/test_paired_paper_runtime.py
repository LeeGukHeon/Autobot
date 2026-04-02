from __future__ import annotations

import asyncio
import json
from pathlib import Path

import autobot.paper.paired_runtime as paired_runtime_module
from autobot.backtest.strategy_adapter import StrategyFillEvent, StrategyOpportunityRecord, StrategyOrderIntent, StrategyStepResult
from autobot.paper.engine import PaperRunEngine, PaperRunSettings
from autobot.paper.paired_runtime import (
    RecordedPublicEventTape,
    _build_paired_promotion_decision,
    run_live_paired_paper,
    run_recorded_paired_paper,
)
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
from tests.test_paired_paper_reporting import _write_events, _write_opportunity_log, _write_summary


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


class _PairedDummyStrategy:
    def __init__(self, *, chosen_action: str) -> None:
        self._chosen_action = str(chosen_action).strip().upper()

    def on_ts(
        self,
        *,
        ts_ms: int,
        active_markets: list[str],
        latest_prices: dict[str, float],
        open_markets: set[str],
    ) -> StrategyStepResult:
        _ = active_markets
        if "KRW-BTC" in open_markets:
            return StrategyStepResult(scored_rows=1, selected_rows=0)
        price = float(latest_prices.get("KRW-BTC", 100.0))
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
            opportunities=(
                StrategyOpportunityRecord(
                    opportunity_id=f"entry:{int(ts_ms)}:KRW-BTC",
                    ts_ms=int(ts_ms),
                    market="KRW-BTC",
                    side="bid",
                    selection_score=0.9,
                    selection_score_raw=0.9,
                    feature_hash="paired-runtime-hash",
                    chosen_action=self._chosen_action,
                    reason_code="MODEL_ALPHA_ENTRY_V1",
                    candidate_actions_json=(
                        {"action_code": "PASSIVE_MAKER", "selected": self._chosen_action == "PASSIVE_MAKER"},
                        {"action_code": "CROSS_1T", "selected": self._chosen_action == "CROSS_1T"},
                    ),
                ),
            ),
            scored_rows=1,
            selected_rows=1,
        )

    def on_fill(self, event: StrategyFillEvent) -> None:
        _ = event


class _PaperEngineWithDummyModel(PaperRunEngine):
    def __init__(self, *args, dummy_strategy: _PairedDummyStrategy, **kwargs) -> None:
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


class _FakeFanoutWsClient:
    def __init__(self, events: list[TickerEvent]) -> None:
        self._events = list(events)

    async def stream_ticker(self, markets: list[str] | tuple[str, ...], *, duration_sec: float | None = None):
        _ = (markets, duration_sec)
        for event in self._events:
            await asyncio.sleep(0)
            yield event

    async def stream_trade(self, markets: list[str] | tuple[str, ...], *, duration_sec: float | None = None):
        _ = (markets, duration_sec)
        if False:
            yield None

    async def stream_orderbook(
        self,
        markets: list[str] | tuple[str, ...],
        *,
        duration_sec: float | None = None,
        level: int | str | None = 0,
    ):
        _ = (markets, duration_sec, level)
        if False:
            yield None


class _RecordingSourceWsClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, float | None]] = []

    async def stream_ticker(self, markets: list[str] | tuple[str, ...], *, duration_sec: float | None = None):
        _ = markets
        self.calls.append(("ticker", duration_sec))
        yield TickerEvent(
            market="KRW-BTC",
            ts_ms=1_000,
            trade_price=100.0,
            acc_trade_price_24h=1_000_000_000_000.0,
        )

    async def stream_trade(self, markets: list[str] | tuple[str, ...], *, duration_sec: float | None = None):
        _ = markets
        self.calls.append(("trade", duration_sec))
        if False:
            yield None

    async def stream_orderbook(
        self,
        markets: list[str] | tuple[str, ...],
        *,
        duration_sec: float | None = None,
        level: int | str | None = 0,
    ):
        _ = (markets, level)
        self.calls.append(("orderbook", duration_sec))
        if False:
            yield None


def _make_settings(*, model_ref: str, out_root: Path) -> PaperRunSettings:
    return PaperRunSettings(
        duration_sec=2,
        quote="KRW",
        top_n=1,
        tf="5m",
        strategy="model_alpha_v1",
        model_ref=model_ref,
        feature_set="v4",
        model_alpha=ModelAlphaSettings(model_ref=model_ref),
        paper_feature_provider="offline_parquet",
        paper_micro_provider="offline_parquet",
        print_every_sec=60,
        decision_interval_sec=0.1,
        universe_refresh_sec=1,
        universe_hold_sec=0,
        momentum_window_sec=60,
        min_momentum_pct=0.2,
        starting_krw=50_000.0,
        per_trade_krw=10_000.0,
        max_positions=2,
        out_root_dir=str(out_root),
    )


def test_run_recorded_paired_paper_writes_operational_artifact(tmp_path: Path) -> None:
    tape = RecordedPublicEventTape(
        markets=("KRW-BTC",),
        ticker_events=(
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
        ),
        trade_events=(),
        orderbook_events=(),
        duration_sec_requested=60,
    )
    settings = UpbitSettings(
        base_url="https://api.upbit.com",
        timeout=UpbitTimeoutSettings(),
        auth=UpbitAuthSettings(),
        ratelimit=UpbitRateLimitSettings(),
        retry=UpbitRetrySettings(),
        websocket=UpbitWebSocketSettings(),
    )
    champion_settings = _make_settings(model_ref="champion-run", out_root=tmp_path / "champion-root")
    challenger_settings = _make_settings(model_ref="candidate-run", out_root=tmp_path / "challenger-root")

    def _engine_factory(*, upbit_settings, run_settings, ws_client, market_loader, rules_provider):
        action = "PASSIVE_MAKER" if str(run_settings.model_ref) == "champion-run" else "CROSS_1T"
        return _PaperEngineWithDummyModel(
            upbit_settings=upbit_settings,
            run_settings=run_settings,
            ws_client=ws_client,
            market_loader=market_loader,
            rules_provider=rules_provider,
            dummy_strategy=_PairedDummyStrategy(chosen_action=action),
        )

    payload = asyncio.run(
        run_recorded_paired_paper(
            upbit_settings=settings,
            champion_run_settings=champion_settings,
            challenger_run_settings=challenger_settings,
            tape=tape,
            output_root=tmp_path / "paired-paper",
            min_matched_opportunities=1,
            min_challenger_hours=0.0,
            min_orders_filled=0,
            min_realized_pnl_quote=0.0,
            min_micro_quality_score=0.0,
            min_nonnegative_ratio=0.0,
            engine_factory=_engine_factory,
            rules_provider=_StaticRulesProvider(),
            market_loader=lambda quote: ["KRW-BTC"],
        )
    )

    latest_path = tmp_path / "paired-paper" / "latest.json"
    report_path = Path(payload["report_path"])
    assert latest_path.exists()
    assert report_path.exists()
    latest = json.loads(latest_path.read_text(encoding="utf-8"))
    assert latest["gate"]["pass"] is True
    assert latest["promotion_decision"]["decision"]["promote"] is True
    assert latest["paired_report"]["clock_alignment"]["pair_ready"] is True
    assert latest["paired_report"]["taxonomy_counts"]["both_trade_different_action"] >= 1


def test_run_live_paired_paper_uses_single_feed_fanout_runtime(tmp_path: Path, monkeypatch) -> None:
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
    fake_client = _FakeFanoutWsClient(events)

    def _engine_factory(*, upbit_settings, run_settings, ws_client, market_loader, rules_provider):
        action = "PASSIVE_MAKER" if str(run_settings.model_ref) == "champion-run" else "CROSS_1T"
        return _PaperEngineWithDummyModel(
            upbit_settings=upbit_settings,
            run_settings=run_settings,
            ws_client=ws_client,
            market_loader=market_loader,
            rules_provider=rules_provider,
            dummy_strategy=_PairedDummyStrategy(chosen_action=action),
        )

    monkeypatch.setattr(paired_runtime_module, "load_upbit_settings", lambda config_dir: settings)
    monkeypatch.setattr(paired_runtime_module, "_load_quote_markets", lambda upbit_settings, quote: ["KRW-BTC"])

    payload = asyncio.run(
        run_live_paired_paper(
            config_dir=Path("config"),
            duration_sec=2,
            quote="KRW",
            top_n=1,
            tf="5m",
            champion_model_ref="champion-run",
            challenger_model_ref="candidate-run",
            model_family="train_v4_crypto_cs",
            feature_set="v4",
            preset="live_v4",
            paper_feature_provider="offline_parquet",
            paper_micro_provider="offline_parquet",
            paper_micro_warmup_sec=0,
            paper_micro_warmup_min_trade_events_per_market=1,
            out_dir=tmp_path / "paired-live",
            min_matched_opportunities=1,
            min_challenger_hours=0.0,
            min_orders_filled=0,
            min_realized_pnl_quote=0.0,
            min_micro_quality_score=0.0,
            min_nonnegative_ratio=0.0,
            max_drawdown_deterioration_factor=1.10,
            micro_quality_tolerance=0.02,
            nonnegative_ratio_tolerance=0.05,
            max_time_to_fill_deterioration_factor=1.25,
            replay_time_scale=0.001,
            replay_max_sleep_sec=0.25,
            source_ws_client=fake_client,
            engine_factory=_engine_factory,
            rules_provider=_StaticRulesProvider(),
        )
    )

    latest = json.loads((tmp_path / "paired-live" / "latest.json").read_text(encoding="utf-8"))
    assert payload["mode"] == "paired_paper_live_fanout_v1"
    assert latest["capture"]["source_mode"] == "live_ws_fanout"
    assert latest["capture"]["ticker_events_captured"] == 3
    assert latest["promotion_decision"]["decision"]["promote"] is True


def test_run_live_paired_paper_supports_distinct_champion_and_challenger_families(tmp_path: Path, monkeypatch) -> None:
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
    fake_client = _FakeFanoutWsClient(events)
    seen_families: dict[str, str] = {}

    def _engine_factory(*, upbit_settings, run_settings, ws_client, market_loader, rules_provider):
        seen_families[str(run_settings.model_ref)] = str(run_settings.model_family)
        action = "PASSIVE_MAKER" if str(run_settings.model_ref) == "champion-run" else "CROSS_1T"
        return _PaperEngineWithDummyModel(
            upbit_settings=upbit_settings,
            run_settings=run_settings,
            ws_client=ws_client,
            market_loader=market_loader,
            rules_provider=rules_provider,
            dummy_strategy=_PairedDummyStrategy(chosen_action=action),
        )

    monkeypatch.setattr(paired_runtime_module, "load_upbit_settings", lambda config_dir: settings)
    monkeypatch.setattr(paired_runtime_module, "_load_quote_markets", lambda upbit_settings, quote: ["KRW-BTC"])

    payload = asyncio.run(
        run_live_paired_paper(
            config_dir=Path("config"),
            duration_sec=2,
            quote="KRW",
            top_n=1,
            tf="5m",
            champion_model_ref="champion-run",
            challenger_model_ref="candidate-run",
            model_family="train_v5_panel_ensemble",
            champion_model_family="train_v4_crypto_cs",
            challenger_model_family="train_v5_panel_ensemble",
            feature_set="v4",
            preset="live_v4",
            paper_feature_provider="offline_parquet",
            paper_micro_provider="offline_parquet",
            paper_micro_warmup_sec=0,
            paper_micro_warmup_min_trade_events_per_market=1,
            out_dir=tmp_path / "paired-live-cross-family",
            min_matched_opportunities=1,
            min_challenger_hours=0.0,
            min_orders_filled=0,
            min_realized_pnl_quote=0.0,
            min_micro_quality_score=0.0,
            min_nonnegative_ratio=0.0,
            max_drawdown_deterioration_factor=1.10,
            micro_quality_tolerance=0.02,
            nonnegative_ratio_tolerance=0.05,
            max_time_to_fill_deterioration_factor=1.25,
            replay_time_scale=0.001,
            replay_max_sleep_sec=0.25,
            source_ws_client=fake_client,
            engine_factory=_engine_factory,
            rules_provider=_StaticRulesProvider(),
        )
    )

    assert payload["champion_model_family"] == "train_v4_crypto_cs"
    assert payload["challenger_model_family"] == "train_v5_panel_ensemble"
    assert seen_families["champion-run"] == "train_v4_crypto_cs"
    assert seen_families["candidate-run"] == "train_v5_panel_ensemble"


def test_build_paired_promotion_decision_reads_decision_language_from_run_dirs(tmp_path: Path) -> None:
    champion_run = tmp_path / "paper-champion-runtime"
    challenger_run = tmp_path / "paper-challenger-runtime"
    champion_run.mkdir(parents=True, exist_ok=True)
    challenger_run.mkdir(parents=True, exist_ok=True)
    _write_summary(
        champion_run,
        role="champion",
        model_ref="champion-run",
        model_run_id="champion-run-id",
        realized_pnl=100.0,
    )
    _write_summary(
        challenger_run,
        role="challenger",
        model_ref="candidate-run",
        model_run_id="candidate-run-id",
        realized_pnl=120.0,
    )
    _write_opportunity_log(
        champion_run,
        opportunity_id="entry:1000:KRW-BTC",
        chosen_action="PASSIVE_MAKER",
        meta={
            "entry_decision": {
                "primary_reason_code": "ENTRY_ALLOWED",
                "primary_reason_family": "entry_gate",
                "reason_codes": ["ENTRY_ALLOWED"],
            }
        },
    )
    _write_opportunity_log(
        challenger_run,
        opportunity_id="entry:1000:KRW-BTC",
        chosen_action="",
        skip_reason_code="ENTRY_GATE_PORTFOLIO_BUDGET_BLOCKED",
        meta={
            "entry_decision": {
                "primary_reason_code": "ENTRY_GATE_ALPHA_LCB_NOT_POSITIVE",
                "primary_reason_family": "entry_gate",
                "reason_codes": ["ENTRY_GATE_ALPHA_LCB_NOT_POSITIVE"],
            },
            "safety_vetoes": {
                "portfolio_budget": {"reason_codes": ["ENTRY_GATE_PORTFOLIO_BUDGET_BLOCKED"]},
            },
        },
    )
    _write_events(champion_run, with_intent=True, intent_id="intent-champion")
    _write_events(challenger_run, with_intent=False, intent_id="intent-challenger")

    decision = _build_paired_promotion_decision(
        champion_run_dir=champion_run,
        challenger_run_dir=challenger_run,
        paired_report={
            "clock_alignment": {"pair_ready": True, "matched_opportunities": 1},
            "paired_deltas": {},
            "taxonomy_counts": {},
            "decision_language_counts": {"challenger_safety_veto_only": 1},
            "report_path": str(tmp_path / "paired_paper_report.json"),
        },
        paired_gate={"pass": True, "reason": "PAIRED_PAPER_READY"},
        min_challenger_hours=0.0,
        min_orders_filled=0,
        min_realized_pnl_quote=0.0,
        min_micro_quality_score=0.0,
        min_nonnegative_ratio=0.0,
        max_drawdown_deterioration_factor=1.10,
        micro_quality_tolerance=0.02,
        nonnegative_ratio_tolerance=0.05,
        max_time_to_fill_deterioration_factor=1.25,
    )

    assert decision["paired_report_excerpt"]["decision_language_counts"]["challenger_safety_veto_only"] == 1
    assert decision["decision"]["alpha_failure_summary"]["challenger_alpha_gate_fail_total"] == 1
    assert decision["decision"]["safety_failure_summary"]["challenger_safety_veto_total"] == 1


def test_build_paired_promotion_decision_blocks_failed_quality_budget_summary(tmp_path: Path) -> None:
    champion_run = tmp_path / "paper-champion-quality"
    challenger_run = tmp_path / "paper-challenger-quality"
    champion_run.mkdir(parents=True, exist_ok=True)
    challenger_run.mkdir(parents=True, exist_ok=True)
    _write_summary(
        champion_run,
        role="champion",
        model_ref="champion-run",
        model_run_id="champion-run-id",
        realized_pnl=100.0,
    )
    _write_summary(
        challenger_run,
        role="challenger",
        model_ref="candidate-run",
        model_run_id="candidate-run-id",
        realized_pnl=120.0,
    )

    cert_meta = tmp_path / "data" / "features" / "features_v4" / "_meta"
    cert_meta.mkdir(parents=True, exist_ok=True)
    (cert_meta / "feature_dataset_certification.json").write_text(
        json.dumps(
            {
                "status": "PASS",
                "pass": True,
                "quality_budget_summary": {
                    "paired_inclusion_pass": False,
                    "eligible_market_ratio": 0.0,
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    decision = _build_paired_promotion_decision(
        champion_run_dir=champion_run,
        challenger_run_dir=challenger_run,
        paired_report={
            "clock_alignment": {"pair_ready": True, "matched_opportunities": 1},
            "paired_deltas": {},
            "taxonomy_counts": {},
            "decision_language_counts": {},
            "report_path": str(tmp_path / "paired_paper_report.json"),
        },
        paired_gate={"pass": True, "reason": "PAIRED_PAPER_READY"},
        min_challenger_hours=0.0,
        min_orders_filled=0,
        min_realized_pnl_quote=0.0,
        min_micro_quality_score=0.0,
        min_nonnegative_ratio=0.0,
        max_drawdown_deterioration_factor=1.10,
        micro_quality_tolerance=0.02,
        nonnegative_ratio_tolerance=0.05,
        max_time_to_fill_deterioration_factor=1.25,
    )

    assert decision["decision"]["promote"] is False
    assert "PAIRED_FEATURE_DATA_QUALITY_BUDGET_FAILED" in decision["decision"]["hard_failures"]


def test_build_paper_run_settings_allows_unbounded_duration_for_service_mode() -> None:
    settings = paired_runtime_module._build_paper_run_settings(
        model_ref="candidate-run",
        model_family="train_v4_crypto_cs",
        feature_set="v4",
        preset="live_v4",
        quote="KRW",
        top_n=1,
        tf="5m",
        duration_sec=0,
        paper_feature_provider="live_v4",
        paper_micro_provider="live_ws",
        paper_micro_warmup_sec=60,
        paper_micro_warmup_min_trade_events_per_market=1,
        allow_unbounded_duration=True,
    )

    assert settings.duration_sec == 0


def test_fanout_public_ws_client_preserves_unbounded_duration_when_zero() -> None:
    source = _RecordingSourceWsClient()
    client = paired_runtime_module.FanoutPublicWsClient(
        source_client=source,
        source_markets=["KRW-BTC"],
        duration_sec=0,
        orderbook_level=0,
    )

    async def _consume_one() -> None:
        async for _event in client.stream_ticker(["KRW-BTC"]):
            await client.close()
            break

    asyncio.run(_consume_one())

    assert source.calls
    assert source.calls[0] == ("ticker", None)


def test_fanout_public_ws_client_bounds_in_memory_history_but_keeps_total_capture_counts() -> None:
    class _BurstSourceClient:
        def __init__(self) -> None:
            self.calls: list[tuple[str, float | None]] = []

        async def stream_ticker(self, markets: list[str] | tuple[str, ...], *, duration_sec: float | None = None):
            _ = markets
            self.calls.append(("ticker", duration_sec))
            base_ts_ms = 1_000
            for idx in range(10):
                yield TickerEvent(
                    market="KRW-BTC",
                    ts_ms=base_ts_ms + (idx * 1_000),
                    trade_price=100.0 + idx,
                    acc_trade_price_24h=1_000_000_000_000.0 + idx,
                )

        async def stream_trade(self, markets: list[str] | tuple[str, ...], *, duration_sec: float | None = None):
            _ = (markets, duration_sec)
            if False:
                yield None

        async def stream_orderbook(
            self,
            markets: list[str] | tuple[str, ...],
            *,
            duration_sec: float | None = None,
            level: int | str | None = 0,
        ):
            _ = (markets, duration_sec, level)
            if False:
                yield None

    source = _BurstSourceClient()
    client = paired_runtime_module.FanoutPublicWsClient(
        source_client=source,
        source_markets=["KRW-BTC"],
        duration_sec=0,
        orderbook_level=0,
        history_sec=3,
        max_ticker_history=3,
    )

    captured = []

    async def _consume_all() -> None:
        async for event in client.stream_ticker(["KRW-BTC"]):
            captured.append(int(event.ts_ms))
            if len(captured) >= 10:
                await client.close()
                break

    asyncio.run(_consume_all())

    assert captured == [1_000 + (idx * 1_000) for idx in range(10)]
    assert client.capture_counts["ticker_events_captured"] == 10
    assert client.capture_counts["ticker_events_buffered"] <= 3
