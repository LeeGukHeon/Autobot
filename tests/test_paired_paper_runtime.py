from __future__ import annotations

import asyncio
import json
from pathlib import Path

from autobot.backtest.strategy_adapter import StrategyFillEvent, StrategyOpportunityRecord, StrategyOrderIntent, StrategyStepResult
from autobot.paper.engine import PaperRunEngine, PaperRunSettings
from autobot.paper.paired_runtime import RecordedPublicEventTape, run_recorded_paired_paper
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
