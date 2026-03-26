from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from autobot.backtest.strategy_adapter import StrategyFillEvent, StrategyOpportunityRecord, StrategyOrderIntent, StrategyStepResult
from autobot.execution.order_supervisor import make_legacy_exec_profile, order_exec_profile_to_dict
from autobot.paper.engine import PaperRunEngine, PaperRunSettings
from autobot.paper.sim_exchange import MarketRules
from autobot.models.live_execution_policy import build_live_execution_contract
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
            opportunities=(
                StrategyOpportunityRecord(
                    opportunity_id="entry:paper:KRW-BTC",
                    ts_ms=int(ts_ms),
                    market="KRW-BTC",
                    side="bid",
                    decision_outcome="intent_created",
                    selection_score=self._score,
                    selection_score_raw=self._score,
                    feature_hash="paper-test-hash",
                    chosen_action="JOIN",
                    reason_code="MODEL_ALPHA_ENTRY_V1",
                    run_id="run-paper",
                    chosen_action_propensity=1.0,
                    no_trade_action_propensity=0.0,
                    behavior_policy_name="model_alpha_execution_behavior_policy_v1",
                    behavior_policy_mode="deterministic_execution_stage",
                    behavior_policy_support="deterministic_no_exploration",
                    candidate_actions_json=(
                        {"action_code": "PASSIVE_MAKER", "selected": False, "propensity": 0.0, "predicted_utility_bps": 1.0},
                        {"action_code": "JOIN", "selected": True, "propensity": 1.0, "predicted_utility_bps": 2.0},
                        {"action_code": "NO_TRADE", "selected": False, "propensity": 0.0, "predicted_utility_bps": 0.0},
                    ),
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
    opportunity_rows = [
        json.loads(line)
        for line in (run_dir / "opportunity_log.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert opportunity_rows
    assert opportunity_rows[0]["lane"] in {"paper", "paper_champion", "paper_candidate"}
    assert opportunity_rows[0]["decision_outcome"] in {"intent_created", "skip"}
    assert opportunity_rows[0]["chosen_action"]
    assert opportunity_rows[0]["behavior_policy_name"] == "model_alpha_execution_behavior_policy_v1"
    assert opportunity_rows[0]["chosen_action_propensity"] in {0.0, 1.0}
    assert opportunity_rows[0]["no_trade_action_propensity"] in {0.0, 1.0}
    counterfactual_rows = [
        json.loads(line)
        for line in (run_dir / "counterfactual_action_log.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(counterfactual_rows) >= 2
    assert any(row["action_payload"].get("action_code") == "NO_TRADE" for row in counterfactual_rows)
    assert all("action_propensity" in row for row in counterfactual_rows)
    summary_json = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary_json["opportunity_log_path"].endswith("opportunity_log.jsonl")
    assert summary_json["counterfactual_action_log_path"].endswith("counterfactual_action_log.jsonl")
    assert summary_json["execution_ope_report_path"].endswith("execution_ope_report.json")
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
            execution=ModelAlphaExecutionSettings(
                price_mode="JOIN",
                timeout_bars=3,
                replace_max=4,
                use_learned_recommendations=False,
            ),
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
    assert int(exec_profile.get("timeout_ms", 0)) == 1_125_000
    assert int(exec_profile.get("replace_interval_ms", 0)) == 1_350_000
    assert int(exec_profile.get("max_replaces", -1)) == 1


def test_paper_engine_model_alpha_uses_strategy_exec_profile_override(tmp_path: Path) -> None:
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
    override_profile = order_exec_profile_to_dict(
        make_legacy_exec_profile(
            timeout_ms=120_000,
            replace_interval_ms=120_000,
            max_replaces=0,
            price_mode="CROSS_1T",
            max_chase_bps=10_000,
            min_replace_interval_ms_global=1_500,
        )
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
            execution=ModelAlphaExecutionSettings(
                price_mode="JOIN",
                timeout_bars=3,
                replace_max=4,
                use_learned_recommendations=False,
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
    dummy_strategy = _DummyModelStrategy(intent_meta={"exec_profile": override_profile})
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

    run_dir = Path(summary.run_dir)
    events_payloads = [
        json.loads(line)
        for line in (run_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    intent_events = [item for item in events_payloads if item.get("event_type") == "INTENT_CREATED"]
    assert intent_events
    intent_meta = ((intent_events[0].get("payload") or {}).get("meta") or {})
    exec_profile = intent_meta.get("exec_profile") or {}
    assert exec_profile.get("price_mode") == "CROSS_1T"
    assert int(exec_profile.get("timeout_ms", 0)) == 120_000
    assert int(exec_profile.get("replace_interval_ms", 0)) == 120_000
    assert int(exec_profile.get("max_replaces", -1)) == 0


def test_paper_engine_model_alpha_uses_execution_contract_to_pick_join(tmp_path: Path) -> None:
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
    contract_path = tmp_path / "live_execution_contract.json"
    attempts = [
        {
            "action_code": "LIMIT_GTC_JOIN",
            "spread_bps": 4.0,
            "depth_top5_notional_krw": 3_000_000.0,
            "snapshot_age_ms": 100.0,
            "expected_edge_bps": 20.0,
            "submitted_ts_ms": 0,
            "first_fill_ts_ms": 2_500,
            "shortfall_bps": 1.5,
        },
        {
            "action_code": "LIMIT_GTC_JOIN",
            "spread_bps": 4.0,
            "depth_top5_notional_krw": 3_000_000.0,
            "snapshot_age_ms": 100.0,
            "expected_edge_bps": 20.0,
            "submitted_ts_ms": 0,
            "first_fill_ts_ms": 2_800,
            "shortfall_bps": 1.0,
        },
        {
            "action_code": "LIMIT_GTC_PASSIVE_MAKER",
            "spread_bps": 4.0,
            "depth_top5_notional_krw": 3_000_000.0,
            "snapshot_age_ms": 100.0,
            "expected_edge_bps": 20.0,
            "submitted_ts_ms": 0,
            "final_state": "MISSED",
            "expected_net_edge_bps": 20.0,
            "shortfall_bps": 0.0,
        },
    ] * 10
    contract_path.write_text(
        json.dumps({"execution_contract": build_live_execution_contract(attempts=attempts)}, ensure_ascii=False),
        encoding="utf-8",
    )

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
        execution_contract_artifact_path=str(contract_path),
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
    dummy_strategy = _DummyModelStrategy(
        intent_meta={"trade_action": {"expected_edge": 0.0020}},
    )
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

    run_dir = Path(summary.run_dir)
    events_payloads = [
        json.loads(line)
        for line in (run_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    intent_events = [item for item in events_payloads if item.get("event_type") == "INTENT_CREATED"]
    assert intent_events
    intent_meta = ((intent_events[0].get("payload") or {}).get("meta") or {})
    exec_profile = intent_meta.get("exec_profile") or {}
    execution_policy = intent_meta.get("execution_policy") or {}
    assert exec_profile.get("price_mode") == "JOIN"
    assert execution_policy.get("selected_action_code") == "LIMIT_GTC_JOIN"


def test_paper_engine_model_alpha_skips_execution_contract_when_learned_execution_is_disabled(tmp_path: Path) -> None:
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
    contract_path = tmp_path / "live_execution_contract.json"
    attempts = [
        {
            "action_code": "LIMIT_GTC_JOIN",
            "spread_bps": 4.0,
            "depth_top5_notional_krw": 3_000_000.0,
            "snapshot_age_ms": 100.0,
            "expected_edge_bps": 20.0,
            "submitted_ts_ms": 0,
            "first_fill_ts_ms": 2_500,
            "shortfall_bps": 1.5,
        },
        {
            "action_code": "LIMIT_GTC_JOIN",
            "spread_bps": 4.0,
            "depth_top5_notional_krw": 3_000_000.0,
            "snapshot_age_ms": 100.0,
            "expected_edge_bps": 20.0,
            "submitted_ts_ms": 0,
            "first_fill_ts_ms": 2_800,
            "shortfall_bps": 1.0,
        },
        {
            "action_code": "LIMIT_GTC_PASSIVE_MAKER",
            "spread_bps": 4.0,
            "depth_top5_notional_krw": 3_000_000.0,
            "snapshot_age_ms": 100.0,
            "expected_edge_bps": 20.0,
            "submitted_ts_ms": 0,
            "final_state": "MISSED",
            "expected_net_edge_bps": 20.0,
            "shortfall_bps": 0.0,
        },
    ] * 10
    contract_path.write_text(
        json.dumps({"execution_contract": build_live_execution_contract(attempts=attempts)}, ensure_ascii=False),
        encoding="utf-8",
    )

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
        execution_contract_artifact_path=str(contract_path),
        model_alpha=ModelAlphaSettings(
            model_ref="latest_v3",
            execution=ModelAlphaExecutionSettings(
                price_mode="PASSIVE_MAKER",
                timeout_bars=3,
                replace_max=4,
                use_learned_recommendations=False,
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
    dummy_strategy = _DummyModelStrategy(
        intent_meta={"trade_action": {"expected_edge": 0.0020}},
    )
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

    run_dir = Path(summary.run_dir)
    events_payloads = [
        json.loads(line)
        for line in (run_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    abort_events = [item for item in events_payloads if item.get("event_type") == "EXECUTION_POLICY_ABORT"]
    intent_events = [item for item in events_payloads if item.get("event_type") == "INTENT_CREATED"]
    assert not abort_events
    assert intent_events
    intent_meta = ((intent_events[0].get("payload") or {}).get("meta") or {})
    assert not intent_meta.get("execution_policy")
    exec_profile = intent_meta.get("exec_profile") or {}
    assert exec_profile.get("price_mode") == "PASSIVE_MAKER"


def test_paper_engine_model_alpha_reports_execution_policy_abort_counts(tmp_path: Path) -> None:
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
    contract_path = tmp_path / "live_execution_contract.json"
    contract_payload = {
        "execution_contract": {
            "policy": "live_execution_contract_v2",
            "status": "ready",
            "rows_total": 40,
            "horizons_ms": [1_000, 3_000, 10_000, 30_000, 60_000, 180_000, 300_000],
            "fill_model": {
                "policy": "live_fill_hazard_survival_v2",
                "status": "ready",
                "rows_total": 40,
                "horizons_ms": [1_000, 3_000, 10_000, 30_000, 60_000, 180_000, 300_000],
                "action_stats": {
                    "LIMIT_GTC_PASSIVE_MAKER": {
                        "sample_count": 40,
                        "p_fill_within_300000ms": 0.25,
                        "p_fill_within_default": 0.10,
                        "mean_shortfall_bps": 0.0,
                        "mean_time_to_first_fill_ms": 120_000.0,
                    },
                    "LIMIT_GTC_JOIN": {
                        "sample_count": 40,
                        "p_fill_within_300000ms": 0.20,
                        "p_fill_within_default": 0.08,
                        "mean_shortfall_bps": 0.0,
                        "mean_time_to_first_fill_ms": 180_000.0,
                    },
                    "BEST_IOC": {
                        "sample_count": 40,
                        "p_fill_within_300000ms": 0.40,
                        "p_fill_within_default": 0.20,
                        "mean_shortfall_bps": 0.0,
                        "mean_time_to_first_fill_ms": 1_000.0,
                    },
                },
                "price_mode_stats": {
                    "PASSIVE_MAKER": {
                        "sample_count": 40,
                        "p_fill_within_300000ms": 0.25,
                        "p_fill_within_default": 0.10,
                        "mean_shortfall_bps": 0.0,
                        "mean_time_to_first_fill_ms": 120_000.0,
                    },
                    "JOIN": {
                        "sample_count": 40,
                        "p_fill_within_300000ms": 0.20,
                        "p_fill_within_default": 0.08,
                        "mean_shortfall_bps": 0.0,
                        "mean_time_to_first_fill_ms": 180_000.0,
                    },
                    "CROSS_1T": {
                        "sample_count": 40,
                        "p_fill_within_300000ms": 0.40,
                        "p_fill_within_default": 0.20,
                        "mean_shortfall_bps": 0.0,
                        "mean_time_to_first_fill_ms": 1_000.0,
                    },
                },
                "state_action_stats": {},
                "global_stats": {
                    "sample_count": 120,
                    "p_fill_within_300000ms": 0.28,
                    "p_fill_within_default": 0.12,
                    "mean_shortfall_bps": 0.0,
                    "mean_time_to_first_fill_ms": 100_000.0,
                },
            },
            "miss_cost_model": {
                "policy": "execution_miss_cost_summary_v2",
                "status": "ready",
                "action_stats": {
                    "LIMIT_GTC_PASSIVE_MAKER": {"sample_count": 40, "mean_miss_cost_bps": 80.0},
                    "LIMIT_GTC_JOIN": {"sample_count": 40, "mean_miss_cost_bps": 70.0},
                    "BEST_IOC": {"sample_count": 40, "mean_miss_cost_bps": 60.0},
                },
                "price_mode_stats": {
                    "PASSIVE_MAKER": {"sample_count": 40, "mean_miss_cost_bps": 80.0},
                    "JOIN": {"sample_count": 40, "mean_miss_cost_bps": 70.0},
                    "CROSS_1T": {"sample_count": 40, "mean_miss_cost_bps": 60.0},
                },
                "state_action_stats": {},
                "global_stats": {"sample_count": 120, "mean_miss_cost_bps": 70.0},
            },
        }
    }
    contract_path.write_text(json.dumps(contract_payload, ensure_ascii=False), encoding="utf-8")

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
        execution_contract_artifact_path=str(contract_path),
        model_alpha=ModelAlphaSettings(
            model_ref="latest_v3",
            execution=ModelAlphaExecutionSettings(
                price_mode="PASSIVE_MAKER",
                timeout_bars=3,
                replace_max=4,
                use_learned_recommendations=True,
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
    dummy_strategy = _DummyModelStrategy(
        intent_meta={"trade_action": {"expected_edge": 0.0020}},
    )
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

    run_dir = Path(summary.run_dir)
    events_payloads = [
        json.loads(line)
        for line in (run_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    abort_events = [item for item in events_payloads if item.get("event_type") == "EXECUTION_POLICY_ABORT"]
    assert abort_events
    assert summary.orders_submitted == 0
    assert summary.candidates_aborted_by_policy > 0


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


def test_paper_engine_model_alpha_best_bid_uses_target_notional_as_submit_price(
    tmp_path: Path,
    monkeypatch,
) -> None:
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
        model_alpha=ModelAlphaSettings(
            model_ref="latest_v3",
            execution=ModelAlphaExecutionSettings(use_learned_recommendations=True),
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

    monkeypatch.setattr(
        "autobot.paper.engine.select_live_execution_action",
        lambda **kwargs: {
            "status": "ok",
            "selected_ord_type": "best",
            "selected_time_in_force": "ioc",
            "selected_price_mode": "JOIN",
        },
    )
    monkeypatch.setattr(
        "autobot.paper.engine.load_live_execution_contract_artifact",
        lambda **kwargs: {"rows_total": 1},
    )

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
    assert summary.orders_filled >= 1

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

    assert intent_payload["ord_type"] == "best"
    assert intent_payload["time_in_force"] == "ioc"
    assert intent_payload["volume"] is None
    assert float(intent_payload["price"]) >= 14_000.0
    assert float(meta["target_notional_quote"]) >= 14_000.0
    assert float(intent_payload["price"]) == pytest.approx(float(meta["target_notional_quote"]))
    assert meta["submit_volume"] is None


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
