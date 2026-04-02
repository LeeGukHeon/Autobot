from __future__ import annotations

import asyncio
import csv
import json
from pathlib import Path

from autobot.backtest.strategy_adapter import StrategyFillEvent, StrategyOpportunityRecord, StrategyOrderIntent, StrategyStepResult
from autobot.execution.order_supervisor import make_legacy_exec_profile, order_exec_profile_to_dict
from autobot.paper.engine import PaperRunEngine, PaperRunSettings
from autobot.paper.paired_engine import run_paired_paper_harness
from autobot.paper.paired_reporting import build_paired_paper_report
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
            min_total=5000.0,
            tick_size=1.0,
        )


class _PairedDummyStrategy:
    def __init__(self, *, chosen_action: str, exec_price_mode: str) -> None:
        self._chosen_action = str(chosen_action).strip().upper()
        self._exec_profile = order_exec_profile_to_dict(
            make_legacy_exec_profile(
                timeout_ms=120_000,
                replace_interval_ms=120_000,
                max_replaces=0,
                price_mode=str(exec_price_mode).strip().upper(),
                max_chase_bps=10_000,
                min_replace_interval_ms_global=1_500,
            )
        )

    def on_ts(
        self,
        *,
        ts_ms: int,
        active_markets: list[str],
        latest_prices: dict[str, float],
        open_markets: set[str],
    ) -> StrategyStepResult:
        _ = active_markets
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
                    meta={"exec_profile": dict(self._exec_profile)},
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
                    feature_hash="paired-paper-hash",
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


def _write_summary(run_dir: Path, *, role: str, model_ref: str, model_run_id: str, realized_pnl: float) -> None:
    payload = {
        "run_id": run_dir.name,
        "paper_runtime_role": role,
        "paper_runtime_model_ref": model_ref,
        "paper_runtime_model_ref_pinned": model_ref,
        "paper_runtime_model_run_id": model_run_id,
        "run_started_ts_ms": 1_000,
        "run_completed_ts_ms": 2_000,
        "realized_pnl_quote": realized_pnl,
        "orders_filled": 1,
    }
    (run_dir / "summary.json").write_text(json.dumps(payload), encoding="utf-8")


def _write_opportunity_log(
    run_dir: Path,
    *,
    opportunity_id: str,
    chosen_action: str,
    skip_reason_code: str | None = None,
    meta: dict[str, object] | None = None,
) -> None:
    rows = [
        {
            "artifact_version": 1,
            "opportunity_id": opportunity_id,
            "ts_ms": 1_000,
            "market": "KRW-BTC",
            "side": "bid",
            "feature_hash": "same-hash",
            "chosen_action": chosen_action,
            "reason_code": "MODEL_ALPHA_ENTRY_V1",
            "skip_reason_code": skip_reason_code,
            "meta": dict(meta or {}),
        }
    ]
    (run_dir / "opportunity_log.jsonl").write_text(
        "\n".join(json.dumps(item) for item in rows) + "\n",
        encoding="utf-8",
    )


def _write_events(run_dir: Path, *, with_intent: bool, intent_id: str) -> None:
    rows = []
    if with_intent:
        rows.append(
            {
                "event_type": "INTENT_CREATED",
                "ts_ms": 1_000,
                "payload": {
                    "intent_id": intent_id,
                    "market": "KRW-BTC",
                    "side": "bid",
                    "reason_code": "MODEL_ALPHA_ENTRY_V1",
                    "ts_ms": 1_000,
                },
            }
        )
    (run_dir / "events.jsonl").write_text(
        ("\n".join(json.dumps(item) for item in rows) + "\n") if rows else "",
        encoding="utf-8",
    )


def _write_trades_csv(
    run_dir: Path,
    *,
    intent_id: str,
    price_mode: str,
    slippage_bps: float,
    with_fill: bool,
    exit_fill_price: float | None = None,
) -> None:
    fields = [
        "ts_ms",
        "market",
        "side",
        "ref_price",
        "tick_bps",
        "order_price",
        "fill_price",
        "slippage_bps",
        "price_mode",
        "price",
        "volume",
        "notional_quote",
        "fee_quote",
        "order_id",
        "intent_id",
        "reason_code",
    ]
    with (run_dir / "trades.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        if with_fill:
            writer.writerow(
                {
                    "ts_ms": 1_000,
                    "market": "KRW-BTC",
                    "side": "bid",
                    "ref_price": 100.0,
                    "tick_bps": 0.0,
                    "order_price": 100.0,
                    "fill_price": 100.1,
                    "slippage_bps": slippage_bps,
                    "price_mode": price_mode,
                    "price": 100.1,
                    "volume": 0.1,
                    "notional_quote": 10.01,
                    "fee_quote": 5.0,
                    "order_id": f"order-{intent_id}",
                    "intent_id": intent_id,
                    "reason_code": "MODEL_ALPHA_ENTRY_V1",
                }
            )
            if exit_fill_price is not None:
                writer.writerow(
                    {
                        "ts_ms": 2_000,
                        "market": "KRW-BTC",
                        "side": "ask",
                        "ref_price": 101.0,
                        "tick_bps": 0.0,
                        "order_price": exit_fill_price,
                        "fill_price": exit_fill_price,
                        "slippage_bps": 1.0,
                        "price_mode": "JOIN",
                        "price": exit_fill_price,
                        "volume": 0.1,
                        "notional_quote": float(exit_fill_price) * 0.1,
                        "fee_quote": 5.0,
                        "order_id": f"order-exit-{intent_id}",
                        "intent_id": f"{intent_id}-exit",
                        "reason_code": "MODEL_ALPHA_EXIT_TP",
                    }
                )


def test_build_paired_paper_report_tracks_trade_and_no_trade_delta(tmp_path: Path) -> None:
    champion_run = tmp_path / "champion"
    challenger_run = tmp_path / "challenger"
    champion_run.mkdir(parents=True, exist_ok=True)
    challenger_run.mkdir(parents=True, exist_ok=True)

    _write_summary(champion_run, role="champion", model_ref="champion_v4", model_run_id="champion-run", realized_pnl=10.0)
    _write_summary(
        challenger_run,
        role="challenger",
        model_ref="candidate_v4",
        model_run_id="candidate-run",
        realized_pnl=25.0,
    )
    _write_opportunity_log(champion_run, opportunity_id="entry:1000:KRW-BTC", chosen_action="PASSIVE_MAKER")
    _write_opportunity_log(
        challenger_run,
        opportunity_id="entry:1000:KRW-BTC",
        chosen_action="",
        skip_reason_code="RISK_SKIP",
        meta={
            "entry_decision": {"reason_codes": ["ENTRY_GATE_PORTFOLIO_BUDGET_BLOCKED"]},
            "safety_vetoes": {"portfolio_budget": {"reason_codes": ["ENTRY_GATE_PORTFOLIO_BUDGET_BLOCKED"]}},
        },
    )
    _write_events(champion_run, with_intent=True, intent_id="intent-champion")
    _write_events(challenger_run, with_intent=False, intent_id="intent-challenger")
    _write_trades_csv(
        champion_run,
        intent_id="intent-champion",
        price_mode="PASSIVE_MAKER",
        slippage_bps=1.5,
        with_fill=True,
        exit_fill_price=101.5,
    )
    _write_trades_csv(
        challenger_run,
        intent_id="intent-challenger",
        price_mode="JOIN",
        slippage_bps=2.0,
        with_fill=False,
    )

    report = build_paired_paper_report(
        champion_run_dir=champion_run,
        challenger_run_dir=challenger_run,
    )

    assert report["clock_alignment"]["pair_ready"] is True
    assert report["clock_alignment"]["matched_opportunities"] == 1
    assert report["taxonomy_counts"]["champion_trade_challenger_no_trade"] == 1
    assert report["taxonomy_counts"]["champion_fill_only"] == 1
    assert report["paired_deltas"]["aggregate_realized_pnl_delta_quote"] == 15.0
    assert report["paired_deltas"]["matched_pnl_delta_quote"] > 0.0
    assert report["paired_deltas"]["matched_pnl_covered_opportunity_count"] == 1
    assert report["paired_deltas"]["matched_fill_delta"] == -1
    assert report["paired_deltas"]["matched_no_trade_delta"] == -1
    assert report["decision_language_counts"]["challenger_safety_veto_only"] == 1
    assert report["disagreement_examples"][0]["challenger"]["primary_decision_family"] == "safety_veto"


def test_run_paired_paper_harness_writes_report_for_same_feed_and_clock(tmp_path: Path) -> None:
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

    def _run_engine(out_root: Path, dummy_strategy: _PairedDummyStrategy) -> Path:
        engine = _PaperEngineWithDummyModel(
            upbit_settings=settings,
            run_settings=PaperRunSettings(
                duration_sec=2,
                quote="KRW",
                top_n=1,
                tf="5m",
                strategy="model_alpha_v1",
                model_ref="latest_v4",
                feature_set="v4",
                model_alpha=ModelAlphaSettings(model_ref="latest_v4"),
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
            ),
            ws_client=_FakeWsClient(events),
            market_loader=lambda quote: ["KRW-BTC"] if quote == "KRW" else [],
            rules_provider=_StaticRulesProvider(),
            dummy_strategy=dummy_strategy,
        )
        summary = asyncio.run(engine.run())
        return Path(summary.run_dir)

    champion_run = _run_engine(
        tmp_path / "champion-root",
        _PairedDummyStrategy(chosen_action="PASSIVE_MAKER", exec_price_mode="PASSIVE_MAKER"),
    )
    challenger_run = _run_engine(
        tmp_path / "challenger-root",
        _PairedDummyStrategy(chosen_action="CROSS_1T", exec_price_mode="CROSS_1T"),
    )
    output_path = tmp_path / "paired_report.json"

    report = run_paired_paper_harness(
        champion_run_dir=champion_run,
        challenger_run_dir=challenger_run,
        output_path=output_path,
    )

    assert output_path.exists()
    written = json.loads(output_path.read_text(encoding="utf-8"))
    assert written["clock_alignment"]["pair_ready"] is True
    assert written["clock_alignment"]["matched_opportunities"] >= 1
    assert written["taxonomy_counts"]["both_trade_different_action"] >= 1
    assert report["paired_deltas"]["matched_fill_delta"] is not None
