from __future__ import annotations

import asyncio
from dataclasses import replace
import json
from pathlib import Path
from types import SimpleNamespace
import time

import pytest

from autobot.backtest.strategy_adapter import StrategyOpportunityRecord, StrategyOrderIntent, StrategyStepResult
from autobot.execution.order_supervisor import make_legacy_exec_profile, order_exec_profile_to_dict
from autobot.models.live_execution_policy import build_live_execution_contract
from autobot.live.breakers import ACTION_HALT_AND_CANCEL_BOT_ORDERS, ACTION_HALT_NEW_INTENTS, arm_breaker
from autobot.live.daemon import LiveDaemonSettings
from autobot.live.model_alpha_runtime import (
    LiveModelAlphaRuntimeSettings,
    _attach_exit_order_to_risk_plan,
    _resolve_strategy_entry_ts_ms,
    run_live_model_alpha_runtime,
)
from autobot.live.rollout import build_rollout_contract, build_rollout_test_order_record
from autobot.live.state_store import IntentRecord, LiveStateStore, OrderRecord, PositionRecord, RiskPlanRecord, TradeJournalRecord
from autobot.live.trade_journal import record_entry_submission
from autobot.risk.live_risk_manager import LiveRiskManager, RiskManagerConfig
from autobot.strategy.micro_order_policy import MicroOrderPolicySettings
from autobot.strategy.model_alpha_v1 import ModelAlphaExecutionSettings, ModelAlphaSettings
from autobot.upbit.ws.models import MyOrderEvent, OrderbookEvent, OrderbookUnit, TickerEvent, TradeEvent


class _PrivateClient:
    def accounts(self):  # noqa: ANN201
        return [
            {"currency": "KRW", "balance": "50000", "locked": "0", "avg_buy_price": "0"},
            {"currency": "BTC", "balance": "0", "locked": "0", "avg_buy_price": "0"},
        ]

    def chance(self, *, market: str):  # noqa: ANN201
        _ = market
        return {
            "market": {
                "bid": {"min_total": "5000"},
                "ask": {"min_total": "5000"},
            },
            "bid_fee": "0.0005",
            "ask_fee": "0.0005",
        }

    def open_orders(self, *, states):  # noqa: ANN201
        _ = states
        return []

    def order(self, *, uuid: str | None = None, identifier: str | None = None):  # noqa: ANN201
        _ = uuid, identifier
        return {}

    def cancel_order(self, *, uuid: str | None = None, identifier: str | None = None):  # noqa: ANN201
        return {"ok": True, "uuid": uuid, "identifier": identifier}


class _ChanceFailurePrivateClient(_PrivateClient):
    def chance(self, *, market: str):  # noqa: ANN201
        _ = market
        raise RuntimeError("chance failed")


class _BidOnlyChancePrivateClient(_PrivateClient):
    def chance(self, *, market: str):  # noqa: ANN201
        _ = market
        return {
            "market": {
                "bid": {"min_total": "5000"},
            },
            "bid_fee": "0.0005",
            "ask_fee": "0.0005",
        }


class _AccountsFailurePrivateClient(_PrivateClient):
    def accounts(self):  # noqa: ANN201
        raise RuntimeError("accounts failed")


class _PublicClient:
    def markets(self, *, is_details: bool = False):  # noqa: ANN201
        _ = is_details
        return [{"market": "KRW-BTC"}]

    def orderbook_instruments(self, markets):  # noqa: ANN201
        _ = markets
        return [{"market": "KRW-BTC", "tick_size": 1000}]


class _FlowPublicClient:
    def markets(self, *, is_details: bool = False):  # noqa: ANN201
        _ = is_details
        return [{"market": "KRW-FLOW"}]

    def orderbook_instruments(self, markets):  # noqa: ANN201
        _ = markets
        return [{"market": "KRW-FLOW", "tick_size": 0.1}]


class _StaticPublicClient:
    def __init__(self, market: str, tick_size: float) -> None:
        self._market = market
        self._tick_size = tick_size

    def markets(self, *, is_details: bool = False):  # noqa: ANN201
        _ = is_details
        return [{"market": self._market}]

    def orderbook_instruments(self, markets):  # noqa: ANN201
        _ = markets
        return [{"market": self._market, "tick_size": self._tick_size}]


class _NoInstrumentPublicClient:
    def markets(self, *, is_details: bool = False):  # noqa: ANN201
        _ = is_details
        return []

    def orderbook_instruments(self, markets):  # noqa: ANN201
        _ = markets
        return []


class _PublicWsClient:
    async def stream_ticker(self, markets, duration_sec=None):  # noqa: ANN201
        _ = markets, duration_sec
        yield TickerEvent(
            market="KRW-BTC",
            ts_ms=int(time.time() * 1000),
            trade_price=50_000_000.0,
            acc_trade_price_24h=10_000_000_000.0,
        )

    @property
    def stats(self):  # noqa: ANN201
        return {
            "reconnect_count": 0,
            "received_events": 1,
            "last_event_ts_ms": 1,
            "last_event_latency_ms": 0,
            "last_malformed_payload_preview": None,
        }


class _PublicWsClientWithMicro(_PublicWsClient):
    async def stream_trade(self, markets, duration_sec=None):  # noqa: ANN201
        _ = markets, duration_sec
        yield TradeEvent(
            market="KRW-BTC",
            ts_ms=int(time.time() * 1000),
            trade_price=50_000_000.0,
            trade_volume=0.01,
            ask_bid="BID",
            sequential_id=1,
        )

    async def stream_orderbook(self, markets, duration_sec=None, level=0):  # noqa: ANN201
        _ = markets, duration_sec, level
        yield OrderbookEvent(
            market="KRW-BTC",
            ts_ms=int(time.time() * 1000),
            total_ask_size=10.0,
            total_bid_size=9.0,
            units=(
                OrderbookUnit(ask_price=50_001_000.0, ask_size=1.0, bid_price=49_999_000.0, bid_size=1.1),
                OrderbookUnit(ask_price=50_002_000.0, ask_size=1.2, bid_price=49_998_000.0, bid_size=1.3),
            ),
        )


class _FaultyPublicWsClient(_PublicWsClient):
    @property
    def stats(self):  # noqa: ANN201
        return {
            "reconnect_count": 0,
            "received_events": 0,
            "last_event_ts_ms": None,
            "last_event_latency_ms": None,
            "last_malformed_payload_preview": '{"tp":null,"atp24h":null}',
            "last_malformed_parser": "parse_ticker_event",
        }

    async def stream_ticker(self, markets, duration_sec=None):  # noqa: ANN201
        _ = markets, duration_sec
        if False:
            yield None
        raise TypeError("float() argument must be a string or a real number, not 'NoneType'")


class _CapturingMicroProvider:
    def __init__(self) -> None:
        self.trades: list[dict[str, object]] = []

    def ingest_trade(self, payload):  # noqa: ANN201
        self.trades.append(dict(payload))


class _FlowPublicWsClient:
    async def stream_ticker(self, markets, duration_sec=None):  # noqa: ANN201
        _ = markets, duration_sec
        yield TickerEvent(
            market="KRW-FLOW",
            ts_ms=int(time.time() * 1000),
            trade_price=90.6,
            acc_trade_price_24h=1_000_000_000.0,
        )


class _StaticPublicWsClient:
    def __init__(self, market: str, trade_price: float) -> None:
        self._market = market
        self._trade_price = trade_price

    async def stream_ticker(self, markets, duration_sec=None):  # noqa: ANN201
        _ = markets, duration_sec
        yield TickerEvent(
            market=self._market,
            ts_ms=int(time.time() * 1000),
            trade_price=self._trade_price,
            acc_trade_price_24h=1_000_000_000.0,
        )


class _PrivateWsClient:
    def __init__(self, events) -> None:  # noqa: ANN001
        self._events = list(events)
        self.stats = {"received_events": len(self._events)}

    async def stream_private(self, *, channels=("myOrder", "myAsset"), duration_sec=None):  # noqa: ANN201
        _ = channels, duration_sec
        for event in self._events:
            yield event


class _FeatureProvider:
    def ingest_ticker(self, event):  # noqa: ANN201
        _ = event

    def build_frame(self, *, ts_ms: int, markets):  # noqa: ANN201
        _ = ts_ms, markets
        return None

    def last_build_stats(self):  # noqa: ANN201
        return {"provider": "fake"}


class _Strategy:
    def on_ts(self, *, ts_ms: int, active_markets, latest_prices, open_markets):  # noqa: ANN201
        _ = ts_ms, active_markets, latest_prices, open_markets
        return StrategyStepResult(
            intents=(
                StrategyOrderIntent(
                    market="KRW-BTC",
                    side="bid",
                    ref_price=50_000_000.0,
                    reason_code="MODEL_ALPHA_ENTRY_V1",
                ),
            ),
            opportunities=(
                StrategyOpportunityRecord(
                    opportunity_id=f"entry:{int(ts_ms)}:KRW-BTC",
                    ts_ms=int(ts_ms),
                    market="KRW-BTC",
                    side="bid",
                    selection_score=0.0,
                    selection_score_raw=0.0,
                    feature_hash="test-hash",
                    chosen_action="intent_created",
                    reason_code="MODEL_ALPHA_ENTRY_V1",
                    run_id="run-live",
                    candidate_actions_json=(
                        {"action_code": "PASSIVE_MAKER", "selected": False, "predicted_utility_bps": 1.0},
                        {"action_code": "JOIN", "selected": True, "predicted_utility_bps": 2.0},
                    ),
                ),
            ),
            scored_rows=1,
            eligible_rows=1,
            selected_rows=1,
        )

    def on_fill(self, event):  # noqa: ANN201
        _ = event


class _ExecutorGateway:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def submit_intent(self, *, intent, identifier: str, meta_json: str | None = None):  # noqa: ANN201
        self.calls.append({"intent": intent, "identifier": identifier, "meta_json": meta_json})
        return SimpleNamespace(
            accepted=True,
            reason="",
            upbit_uuid="order-1",
            identifier=identifier,
            intent_id=intent.intent_id,
        )


class _OrderSupervisionGateway:
    def __init__(self) -> None:
        self.cancel_calls: list[dict[str, object]] = []
        self.replace_calls: list[dict[str, object]] = []

    def cancel(self, *, upbit_uuid: str | None = None, identifier: str | None = None):  # noqa: ANN201
        payload = {"upbit_uuid": upbit_uuid, "identifier": identifier}
        self.cancel_calls.append(payload)
        return SimpleNamespace(
            accepted=True,
            reason="",
            upbit_uuid=upbit_uuid,
            identifier=identifier,
            intent_id=None,
        )

    def replace_order(self, **kwargs):  # noqa: ANN003, ANN201
        self.replace_calls.append(dict(kwargs))
        return SimpleNamespace(
            accepted=True,
            reason="",
            cancelled_order_uuid=kwargs.get("prev_order_uuid"),
            new_order_uuid="replaced-order-1",
            new_identifier=kwargs.get("new_identifier"),
        )


class _NullMicroProvider:
    def get(self, market: str, ts_ms: int):  # noqa: ANN201
        _ = market, ts_ms
        return None


class _LargeNotionalStrategy:
    def on_ts(self, *, ts_ms: int, active_markets, latest_prices, open_markets):  # noqa: ANN201
        _ = ts_ms, active_markets, latest_prices, open_markets
        return StrategyStepResult(
            intents=(
                StrategyOrderIntent(
                    market="KRW-BTC",
                    side="bid",
                    ref_price=50_000_000.0,
                    reason_code="MODEL_ALPHA_ENTRY_V1",
                    meta={"notional_multiplier": 10.0},
                ),
            ),
            scored_rows=1,
            eligible_rows=1,
            selected_rows=1,
        )

    def on_fill(self, event):  # noqa: ANN201
        _ = event


class _StalePriceFlowStrategy:
    def on_ts(self, *, ts_ms: int, active_markets, latest_prices, open_markets):  # noqa: ANN201
        _ = ts_ms, active_markets, latest_prices, open_markets
        return StrategyStepResult(
            intents=(
                StrategyOrderIntent(
                    market="KRW-FLOW",
                    side="bid",
                    ref_price=78.0,
                    reason_code="MODEL_ALPHA_ENTRY_V1",
                    prob=0.9,
                    meta={"model_prob": 0.9},
                ),
            ),
            scored_rows=1,
            eligible_rows=1,
            selected_rows=1,
        )

    def on_fill(self, event):  # noqa: ANN201
        _ = event


class _NoIntentStrategy:
    def on_ts(self, *, ts_ms: int, active_markets, latest_prices, open_markets):  # noqa: ANN201
        _ = ts_ms, active_markets, latest_prices, open_markets
        return StrategyStepResult(
            intents=(),
            scored_rows=0,
            eligible_rows=0,
            selected_rows=0,
        )

    def on_fill(self, event):  # noqa: ANN201
        _ = event


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _seed_runtime_contract(tmp_path: Path, *, run_id: str) -> Path:
    registry_root = tmp_path / "models" / "registry"
    family_dir = registry_root / "train_v4_crypto_cs"
    (family_dir / run_id).mkdir(parents=True, exist_ok=True)
    _write_json(family_dir / "champion.json", {"run_id": run_id})
    now_ms = int(time.time() * 1000)
    ws_meta_dir = tmp_path / "data" / "raw_ws" / "upbit" / "_meta"
    _write_json(
        ws_meta_dir / "ws_public_health.json",
        {
            "run_id": "ws-run-1",
            "updated_at_ms": now_ms,
            "connected": True,
            "last_rx_ts_ms": {"trade": now_ms, "orderbook": now_ms},
            "subscribed_markets_count": 1,
        },
    )
    _write_json(
        ws_meta_dir / "ws_runs_summary.json",
        {"runs": [{"run_id": "ws-run-1", "rows_total": 10, "bytes_total": 100}]},
    )
    _write_json(
        tmp_path / "data" / "parquet" / "micro_v1" / "_meta" / "aggregate_report.json",
        {"run_id": "micro-run-1", "rows_written_total": 10},
    )
    return registry_root


def _runtime_settings(
    tmp_path: Path,
    *,
    rollout_mode: str,
    canary: bool = False,
    model_alpha: ModelAlphaSettings | None = None,
    micro_order_policy: MicroOrderPolicySettings | None = None,
) -> LiveModelAlphaRuntimeSettings:
    registry_root = _seed_runtime_contract(tmp_path, run_id="run-live")
    daemon_settings = LiveDaemonSettings(
        bot_id="autobot-001",
        identifier_prefix="AUTOBOT",
        unknown_open_orders_policy="ignore",
        unknown_positions_policy="import_as_unmanaged",
        allow_cancel_external_orders=False,
        poll_interval_sec=60,
        startup_reconcile=False,
        duration_sec=1,
        small_account_canary_enabled=canary,
        small_account_max_positions=1,
        small_account_max_open_orders_per_market=1,
        registry_root=str(registry_root),
        runtime_model_ref_source="champion_v4",
        runtime_model_family="train_v4_crypto_cs",
        ws_public_raw_root=str(tmp_path / "data" / "raw_ws" / "upbit" / "public"),
        ws_public_meta_dir=str(tmp_path / "data" / "raw_ws" / "upbit" / "_meta"),
        ws_public_stale_threshold_sec=180,
        micro_aggregate_report_path=str(tmp_path / "data" / "parquet" / "micro_v1" / "_meta" / "aggregate_report.json"),
        rollout_mode=rollout_mode,
        rollout_target_unit="autobot-live-alpha.service",
    )
    return LiveModelAlphaRuntimeSettings(
        daemon=daemon_settings,
        model_alpha=model_alpha or ModelAlphaSettings(),
        micro_order_policy=micro_order_policy or MicroOrderPolicySettings(),
    )


def test_live_model_alpha_runtime_shadow_records_hypothetical_intent(tmp_path: Path, monkeypatch) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    monkeypatch.setattr(runtime_module, "_load_predictor_for_runtime", lambda **_: SimpleNamespace(run_dir=Path("run-live")))
    monkeypatch.setattr(runtime_module, "_build_live_feature_provider", lambda **_: _FeatureProvider())
    monkeypatch.setattr(runtime_module, "_build_live_strategy", lambda **_: _Strategy())

    settings = _runtime_settings(tmp_path, rollout_mode="shadow")
    with LiveStateStore(tmp_path / "live_state.db") as store:
        summary = asyncio.run(
            run_live_model_alpha_runtime(
                store=store,
                client=_PrivateClient(),
                public_client=_PublicClient(),
                public_ws_client=_PublicWsClient(),
                settings=settings,
                executor_gateway=None,
            )
        )
        intents = store.list_intents()

    assert summary["shadow_intents_total"] == 1
    assert summary["submitted_intents_total"] == 0
    assert intents
    assert intents[0]["status"] == "SHADOW"
    opportunity_path = Path(str(summary["opportunity_log_path"]))
    assert opportunity_path.exists()
    records = [json.loads(line) for line in opportunity_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert records
    assert records[0]["lane"] == "live_champion"
    counterfactual_path = Path(str(summary["counterfactual_action_log_path"]))
    assert counterfactual_path.exists()
    counterfactual_rows = [json.loads(line) for line in counterfactual_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(counterfactual_rows) >= 2
    risk_budget_path = Path(str(summary["risk_budget_ledger_path"]))
    assert risk_budget_path.exists()
    risk_budget_rows = [json.loads(line) for line in risk_budget_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert risk_budget_rows
    assert risk_budget_rows[-1]["status"] == "SHADOW"
    assert risk_budget_rows[-1]["runtime_model_run_id"] == "run-live"
    assert risk_budget_rows[-1]["sizing"]["target_notional_quote"] is not None
    risk_budget_latest_path = Path(str(summary["risk_budget_latest_path"]))
    assert risk_budget_latest_path.exists()


@pytest.mark.parametrize(
    ("client_factory", "expected_reason"),
    [
        (_ChanceFailurePrivateClient, "CHANCE_LOOKUP_FAILED"),
        (_AccountsFailurePrivateClient, "ACCOUNTS_LOOKUP_FAILED"),
    ],
)
def test_live_model_alpha_runtime_skips_lookup_failures_without_halting(
    tmp_path: Path,
    monkeypatch,
    client_factory,
    expected_reason: str,
) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    monkeypatch.setattr(runtime_module, "_load_predictor_for_runtime", lambda **_: SimpleNamespace(run_dir=Path("run-live")))
    monkeypatch.setattr(runtime_module, "_build_live_feature_provider", lambda **_: _FeatureProvider())
    monkeypatch.setattr(runtime_module, "_build_live_strategy", lambda **_: _Strategy())

    settings = _runtime_settings(tmp_path, rollout_mode="shadow")
    with LiveStateStore(tmp_path / "live_state.db") as store:
        summary = asyncio.run(
            run_live_model_alpha_runtime(
                store=store,
                client=client_factory(),
                public_client=_PublicClient(),
                public_ws_client=_PublicWsClient(),
                settings=settings,
                executor_gateway=None,
            )
        )
        intents = store.list_intents()

    assert summary["halted"] is False
    assert summary["skipped_intents_total"] == 1
    assert summary["stream_stop_reason"] == "STREAM_COMPLETED"
    assert intents
    assert intents[0]["status"] == "SKIPPED"
    assert intents[0]["meta"]["skip_reason"] == expected_reason


def test_live_model_alpha_runtime_captures_public_ws_context_on_stream_failure(tmp_path: Path, monkeypatch) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    monkeypatch.setattr(runtime_module, "_load_predictor_for_runtime", lambda **_: SimpleNamespace(run_dir=Path("run-live")))
    monkeypatch.setattr(runtime_module, "_build_live_feature_provider", lambda **_: _FeatureProvider())
    monkeypatch.setattr(runtime_module, "_build_live_strategy", lambda **_: _Strategy())

    settings = _runtime_settings(tmp_path, rollout_mode="canary", canary=True)
    now_ms = int(time.time() * 1000)
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.set_live_rollout_contract(
            payload=build_rollout_contract(
                mode="canary",
                target_unit="autobot-live-alpha.service",
                arm_token="demo-token",
                ts_ms=now_ms - 1000,
            ),
            ts_ms=now_ms - 1000,
        )
        store.set_live_test_order(
            payload=build_rollout_test_order_record(
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price="5000",
                volume="1",
                ok=True,
                response_payload={"ok": True},
                ts_ms=now_ms,
            ),
            ts_ms=now_ms,
        )
        summary = asyncio.run(
            run_live_model_alpha_runtime(
                store=store,
                client=_PrivateClient(),
                public_client=_PublicClient(),
                public_ws_client=_FaultyPublicWsClient(),
                settings=settings,
                executor_gateway=None,
            )
        )

    assert summary["halted"] is True
    assert "LIVE_RUNTIME_LOOP_FAILED" in summary["halted_reasons"]
    assert "NoneType" in str(summary["stream_stop_reason"])
    assert summary["stream_stop_traceback"]
    assert summary["public_ws_stats"]["last_malformed_payload_preview"] == '{"tp":null,"atp24h":null}'


def test_ingest_live_micro_trade_event_ignores_none_price() -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    provider = _CapturingMicroProvider()
    runtime_module._ingest_live_micro_trade_event(
        provider=provider,
        event=TradeEvent(
            market="KRW-BTC",
            ts_ms=1_000,
            trade_price=None,  # type: ignore[arg-type]
            trade_volume=0.01,
            ask_bid="BID",
            sequential_id=1,
        ),
    )

    assert provider.trades == []


def test_ingest_live_micro_from_ticker_ignores_none_trade_price() -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    provider = _CapturingMicroProvider()
    runtime_module._ingest_live_micro_from_ticker(
        provider=provider,  # type: ignore[arg-type]
        ticker=TickerEvent(
            market="KRW-BTC",
            ts_ms=1_000,
            trade_price=None,  # type: ignore[arg-type]
            acc_trade_price_24h=1_000_000_000.0,
        ),
    )

    assert provider.trades == []


def test_live_model_alpha_runtime_canary_submits_when_armed(tmp_path: Path, monkeypatch) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    monkeypatch.setattr(runtime_module, "_load_predictor_for_runtime", lambda **_: SimpleNamespace(run_dir=Path("run-live")))
    monkeypatch.setattr(runtime_module, "_build_live_feature_provider", lambda **_: _FeatureProvider())
    monkeypatch.setattr(runtime_module, "_build_live_strategy", lambda **_: _Strategy())

    settings = _runtime_settings(tmp_path, rollout_mode="canary", canary=True)
    executor = _ExecutorGateway()
    now_ms = int(time.time() * 1000)
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.set_live_rollout_contract(
            payload=build_rollout_contract(
                mode="canary",
                target_unit="autobot-live-alpha.service",
                arm_token="demo-token",
                ts_ms=now_ms - 1000,
            ),
            ts_ms=now_ms - 1000,
        )
        store.set_live_test_order(
            payload=build_rollout_test_order_record(
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price="50000000",
                volume="0.0001",
                ok=True,
                response_payload={"ok": True},
                ts_ms=now_ms,
            ),
            ts_ms=now_ms,
        )
        summary = asyncio.run(
            run_live_model_alpha_runtime(
                store=store,
                client=_PrivateClient(),
                public_client=_PublicClient(),
                public_ws_client=_PublicWsClient(),
                settings=settings,
                executor_gateway=executor,
            )
        )
        intents = store.list_intents()
        orders = store.list_orders(open_only=False)

    assert summary["submitted_intents_total"] == 1
    assert len(executor.calls) == 1
    assert intents
    assert intents[0]["status"] == "SUBMITTED"
    assert orders
    assert orders[0]["uuid"] == "order-1"


def test_live_model_alpha_runtime_uses_exec_profile_timeout_for_execution_policy_deadline(tmp_path: Path, monkeypatch) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    class _StrategyWithExecProfile:
        def on_ts(self, *, ts_ms: int, active_markets, latest_prices, open_markets):  # noqa: ANN201
            _ = ts_ms, active_markets, latest_prices, open_markets
            profile = order_exec_profile_to_dict(
                make_legacy_exec_profile(
                    timeout_ms=60_000,
                    replace_interval_ms=60_000,
                    max_replaces=0,
                    price_mode="JOIN",
                    max_chase_bps=10_000,
                    min_replace_interval_ms_global=1_500,
                )
            )
            return StrategyStepResult(
                intents=(
                    StrategyOrderIntent(
                        market="KRW-BTC",
                        side="bid",
                        ref_price=50_000_000.0,
                        reason_code="MODEL_ALPHA_ENTRY_V1",
                        prob=0.91,
                        score=0.91,
                        meta={
                            "trade_action": {"expected_edge": 0.0100},
                            "exec_profile": profile,
                        },
                    ),
                ),
                scored_rows=1,
                eligible_rows=1,
                selected_rows=1,
            )

        def on_fill(self, event):  # noqa: ANN201
            _ = event

    attempts = [
        {
            "action_code": "LIMIT_GTC_JOIN",
            "spread_bps": 4.0,
            "depth_top5_notional_krw": 3_000_000.0,
            "snapshot_age_ms": 100.0,
            "expected_edge_bps": 50.0,
            "submitted_ts_ms": 0,
            "first_fill_ts_ms": 25_000,
            "shortfall_bps": 1.5,
        }
    ] * 30

    monkeypatch.setattr(runtime_module, "_load_predictor_for_runtime", lambda **_: SimpleNamespace(run_dir=Path("run-live")))
    monkeypatch.setattr(runtime_module, "_build_live_feature_provider", lambda **_: _FeatureProvider())
    monkeypatch.setattr(runtime_module, "_build_live_strategy", lambda **_: _StrategyWithExecProfile())

    settings = _runtime_settings(tmp_path, rollout_mode="canary", canary=True)
    executor = _ExecutorGateway()
    now_ms = int(time.time() * 1000)
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.set_checkpoint(
            name="live_execution_policy_model",
            payload={"execution_contract": build_live_execution_contract(attempts=attempts)},
            ts_ms=now_ms - 1000,
        )
        store.set_live_rollout_contract(
            payload=build_rollout_contract(
                mode="canary",
                target_unit="autobot-live-alpha.service",
                arm_token="demo-token",
                ts_ms=now_ms - 1000,
            ),
            ts_ms=now_ms - 1000,
        )
        store.set_live_test_order(
            payload=build_rollout_test_order_record(
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price="50000000",
                volume="0.0001",
                ok=True,
                response_payload={"ok": True},
                ts_ms=now_ms,
            ),
            ts_ms=now_ms,
        )
        summary = asyncio.run(
            run_live_model_alpha_runtime(
                store=store,
                client=_PrivateClient(),
                public_client=_PublicClient(),
                public_ws_client=_PublicWsClient(),
                settings=settings,
                executor_gateway=executor,
            )
        )

    assert summary["submitted_intents_total"] == 1
    meta_payload = json.loads(str(executor.calls[0]["meta_json"]))
    execution_policy = meta_payload.get("execution_policy") or {}
    execution_trace = meta_payload.get("execution_trace") or {}
    assert execution_policy.get("deadline_ms") == 60_000
    assert (execution_trace.get("execution_policy") or {}).get("deadline_ms") == 60_000


def test_live_model_alpha_runtime_skips_execution_policy_when_learned_execution_disabled(tmp_path: Path, monkeypatch) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    class _StrategyWithExecProfile:
        def on_ts(self, *, ts_ms: int, active_markets, latest_prices, open_markets):  # noqa: ANN201
            _ = ts_ms, active_markets, latest_prices, open_markets
            profile = order_exec_profile_to_dict(
                make_legacy_exec_profile(
                    timeout_ms=60_000,
                    replace_interval_ms=60_000,
                    max_replaces=0,
                    price_mode="PASSIVE_MAKER",
                    max_chase_bps=10_000,
                    min_replace_interval_ms_global=1_500,
                )
            )
            return StrategyStepResult(
                intents=(
                    StrategyOrderIntent(
                        market="KRW-BTC",
                        side="bid",
                        ref_price=50_000_000.0,
                        reason_code="MODEL_ALPHA_ENTRY_V1",
                        prob=0.91,
                        score=0.91,
                        meta={
                            "trade_action": {"expected_edge": 0.0100},
                            "exec_profile": profile,
                        },
                    ),
                ),
                scored_rows=1,
                eligible_rows=1,
                selected_rows=1,
            )

        def on_fill(self, event):  # noqa: ANN201
            _ = event

    attempts = [
        {
            "action_code": "LIMIT_GTC_JOIN",
            "spread_bps": 4.0,
            "depth_top5_notional_krw": 3_000_000.0,
            "snapshot_age_ms": 100.0,
            "expected_edge_bps": 50.0,
            "submitted_ts_ms": 0,
            "first_fill_ts_ms": 25_000,
            "shortfall_bps": 1.5,
        }
    ] * 30

    monkeypatch.setattr(runtime_module, "_load_predictor_for_runtime", lambda **_: SimpleNamespace(run_dir=Path("run-live")))
    monkeypatch.setattr(runtime_module, "_build_live_feature_provider", lambda **_: _FeatureProvider())
    monkeypatch.setattr(runtime_module, "_build_live_strategy", lambda **_: _StrategyWithExecProfile())

    settings = replace(
        _runtime_settings(tmp_path, rollout_mode="canary", canary=True),
        model_alpha=replace(
            _runtime_settings(tmp_path, rollout_mode="canary", canary=True).model_alpha,
            execution=ModelAlphaExecutionSettings(
                price_mode="PASSIVE_MAKER",
                timeout_bars=2,
                replace_max=0,
                use_learned_recommendations=False,
            ),
        ),
    )
    executor = _ExecutorGateway()
    now_ms = int(time.time() * 1000)
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.set_checkpoint(
            name="live_execution_policy_model",
            payload={"execution_contract": build_live_execution_contract(attempts=attempts)},
            ts_ms=now_ms - 1000,
        )
        store.set_live_rollout_contract(
            payload=build_rollout_contract(
                mode="canary",
                target_unit="autobot-live-alpha.service",
                arm_token="demo-token",
                ts_ms=now_ms - 1000,
            ),
            ts_ms=now_ms - 1000,
        )
        store.set_live_test_order(
            payload=build_rollout_test_order_record(
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price="50000000",
                volume="0.0001",
                ok=True,
                response_payload={"ok": True},
                ts_ms=now_ms,
            ),
            ts_ms=now_ms,
        )
        summary = asyncio.run(
            run_live_model_alpha_runtime(
                store=store,
                client=_PrivateClient(),
                public_client=_PublicClient(),
                public_ws_client=_PublicWsClient(),
                settings=settings,
                executor_gateway=executor,
            )
        )

    assert summary["submitted_intents_total"] == 1
    meta_payload = json.loads(str(executor.calls[0]["meta_json"]))
    assert meta_payload.get("execution_policy") == {}
    exec_profile = ((meta_payload.get("execution") or {}).get("exec_profile")) or {}
    assert exec_profile.get("price_mode") == "PASSIVE_MAKER"


def test_live_model_alpha_runtime_skips_when_execution_risk_control_blocks_entry(tmp_path: Path, monkeypatch) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    class _LowActionValueStrategy:
        def on_ts(self, *, ts_ms: int, active_markets, latest_prices, open_markets):  # noqa: ANN201
            _ = ts_ms, active_markets, latest_prices, open_markets
            return StrategyStepResult(
                intents=(
                    StrategyOrderIntent(
                        market="KRW-BTC",
                        side="bid",
                        ref_price=50_000_000.0,
                        reason_code="MODEL_ALPHA_ENTRY_V1",
                        prob=0.91,
                        score=0.91,
                        meta={
                            "model_prob": 0.91,
                            "trade_action": {
                                "recommended_action": "risk",
                                "expected_action_value": 0.4,
                            },
                        },
                    ),
                ),
                scored_rows=1,
                eligible_rows=1,
                selected_rows=1,
            )

        def on_fill(self, event):  # noqa: ANN201
            _ = event

    predictor = SimpleNamespace(
        run_dir=Path("run-live"),
        runtime_recommendations={
            "risk_control": {
                "version": 1,
                "policy": "execution_risk_control_hoeffding_v1",
                "status": "ready",
                "decision_metric_name": "expected_action_value",
                "selected_threshold": 2.0,
                "selected_coverage": 31,
                "selected_nonpositive_rate_ucb": 0.18,
                "selected_severe_loss_rate_ucb": 0.11,
                "live_gate": {
                    "enabled": True,
                    "metric_name": "expected_action_value",
                    "threshold": 2.0,
                    "skip_reason_code": "RISK_CONTROL_BELOW_THRESHOLD",
                },
                "subgroup_family": {
                    "enabled": True,
                    "feature_name": "rv_12",
                    "bucket_count_requested": 2,
                    "bucket_count_effective": 2,
                    "bounds": [0.1, 0.3, 0.6],
                    "min_coverage": 10,
                },
            }
        },
    )
    monkeypatch.setattr(runtime_module, "_load_predictor_for_runtime", lambda **_: predictor)
    monkeypatch.setattr(runtime_module, "_build_live_feature_provider", lambda **_: _FeatureProvider())
    monkeypatch.setattr(runtime_module, "_build_live_strategy", lambda **_: _LowActionValueStrategy())

    settings = _runtime_settings(tmp_path, rollout_mode="canary", canary=True)
    executor = _ExecutorGateway()
    now_ms = int(time.time() * 1000)
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.set_live_rollout_contract(
            payload=build_rollout_contract(
                mode="canary",
                target_unit="autobot-live-alpha.service",
                arm_token="demo-token",
                ts_ms=now_ms - 1000,
            ),
            ts_ms=now_ms - 1000,
        )
        store.set_live_test_order(
            payload=build_rollout_test_order_record(
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price="50000000",
                volume="0.0001",
                ok=True,
                response_payload={"ok": True},
                ts_ms=now_ms,
            ),
            ts_ms=now_ms,
        )
        summary = asyncio.run(
            run_live_model_alpha_runtime(
                store=store,
                client=_PrivateClient(),
                public_client=_PublicClient(),
                public_ws_client=_PublicWsClient(),
                settings=settings,
                executor_gateway=executor,
            )
        )
        intents = store.list_intents()

    assert summary["submitted_intents_total"] == 0
    assert summary["skipped_intents_total"] == 1
    assert executor.calls == []
    assert intents
    assert intents[0]["meta"].get("skip_reason") == "RISK_CONTROL_BELOW_THRESHOLD"
    assert intents[0]["meta"].get("risk_control", {}).get("allowed") is False


def test_live_model_alpha_runtime_allows_protective_exit_under_halt_new_intents(tmp_path: Path, monkeypatch) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    monkeypatch.setattr(runtime_module, "_load_predictor_for_runtime", lambda **_: SimpleNamespace(run_dir=Path("run-live")))
    monkeypatch.setattr(runtime_module, "_build_live_feature_provider", lambda **_: _FeatureProvider())
    monkeypatch.setattr(runtime_module, "_build_live_strategy", lambda **_: _NoIntentStrategy())

    settings = replace(
        _runtime_settings(tmp_path, rollout_mode="canary", canary=True),
        risk_enabled=True,
    )
    executor = _ExecutorGateway()
    now_ms = int(time.time() * 1000)
    with LiveStateStore(tmp_path / "live_state.db") as store:
        arm_breaker(
            store,
            reason_codes=["LIVE_TEST_ORDER_REQUIRED"],
            source="test",
            ts_ms=now_ms - 1000,
            action=ACTION_HALT_NEW_INTENTS,
        )
        store.upsert_position(
            PositionRecord(
                market="KRW-ENSO",
                base_currency="ENSO",
                base_amount=3.0,
                avg_entry_price=1872.0,
                updated_ts=now_ms - 60_000,
                tp_json="{}",
                sl_json="{}",
                trailing_json="{}",
                managed=True,
            )
        )
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="plan-enso-1",
                market="KRW-ENSO",
                side="long",
                entry_price_str="1872.0",
                qty_str="3.0",
                tp_enabled=True,
                tp_price_str=None,
                tp_pct=3.5,
                sl_enabled=False,
                sl_price_str=None,
                sl_pct=None,
                trailing_enabled=False,
                trail_pct=None,
                high_watermark_price_str=None,
                armed_ts_ms=None,
                timeout_ts_ms=now_ms - 1_000,
                state="ACTIVE",
                last_eval_ts_ms=now_ms - 60_000,
                last_action_ts_ms=0,
                current_exit_order_uuid=None,
                current_exit_order_identifier=None,
                replace_attempt=0,
                created_ts=now_ms - 60_000,
                updated_ts=now_ms - 60_000,
                plan_source="model_alpha_v1",
                source_intent_id="intent-enso-1",
        )
        )
        summary = asyncio.run(
            run_live_model_alpha_runtime(
                store=store,
                client=_PrivateClient(),
                public_client=_StaticPublicClient("KRW-ENSO", 1.0),
                public_ws_client=_StaticPublicWsClient("KRW-ENSO", 1950.0),
                settings=settings,
                executor_gateway=executor,
            )
        )
        plan = store.risk_plan_by_id(plan_id="plan-enso-1")

    assert summary["halted"] is False
    assert summary["risk_actions_total"] >= 1
    assert len(executor.calls) == 1
    assert executor.calls[0]["intent"].side == "ask"
    assert plan is not None
    assert plan["state"] == "EXITING"


def test_live_model_alpha_runtime_shadow_mode_suppresses_protective_exit_orders(tmp_path: Path, monkeypatch) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    monkeypatch.setattr(runtime_module, "_load_predictor_for_runtime", lambda **_: SimpleNamespace(run_dir=Path("run-live")))
    monkeypatch.setattr(runtime_module, "_build_live_feature_provider", lambda **_: _FeatureProvider())
    monkeypatch.setattr(runtime_module, "_build_live_strategy", lambda **_: _NoIntentStrategy())

    settings = replace(
        _runtime_settings(tmp_path, rollout_mode="shadow"),
        risk_enabled=True,
    )
    executor = _ExecutorGateway()
    now_ms = int(time.time() * 1000)
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.upsert_position(
            PositionRecord(
                market="KRW-ENSO",
                base_currency="ENSO",
                base_amount=3.0,
                avg_entry_price=1872.0,
                updated_ts=now_ms - 60_000,
                tp_json="{}",
                sl_json="{}",
                trailing_json="{}",
                managed=True,
            )
        )
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="plan-enso-shadow",
                market="KRW-ENSO",
                side="long",
                entry_price_str="1872.0",
                qty_str="3.0",
                tp_enabled=True,
                tp_price_str=None,
                tp_pct=3.5,
                sl_enabled=False,
                sl_price_str=None,
                sl_pct=None,
                trailing_enabled=False,
                trail_pct=None,
                high_watermark_price_str=None,
                armed_ts_ms=None,
                timeout_ts_ms=now_ms - 1_000,
                state="ACTIVE",
                last_eval_ts_ms=now_ms - 60_000,
                last_action_ts_ms=0,
                current_exit_order_uuid=None,
                current_exit_order_identifier=None,
                replace_attempt=0,
                created_ts=now_ms - 60_000,
                updated_ts=now_ms - 60_000,
                plan_source="model_alpha_v1",
                source_intent_id="intent-enso-1",
            )
        )
        summary = asyncio.run(
            run_live_model_alpha_runtime(
                store=store,
                client=_PrivateClient(),
                public_client=_StaticPublicClient("KRW-ENSO", 1.0),
                public_ws_client=_StaticPublicWsClient("KRW-ENSO", 1950.0),
                settings=settings,
                executor_gateway=executor,
            )
        )
        plan = store.risk_plan_by_id(plan_id="plan-enso-shadow")

    assert summary["halted"] is False
    assert summary["risk_actions_total"] == 0
    assert executor.calls == []
    assert plan is not None
    assert plan["state"] == "ACTIVE"


def test_live_model_alpha_runtime_startup_reconcile_stays_alive_under_halt_new_intents(tmp_path: Path, monkeypatch) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    monkeypatch.setattr(runtime_module, "_load_predictor_for_runtime", lambda **_: SimpleNamespace(run_dir=Path("run-live")))
    monkeypatch.setattr(runtime_module, "_build_live_feature_provider", lambda **_: _FeatureProvider())
    monkeypatch.setattr(runtime_module, "_build_live_strategy", lambda **_: _NoIntentStrategy())

    base_settings = _runtime_settings(tmp_path, rollout_mode="canary", canary=True)
    settings = replace(
        base_settings,
        daemon=replace(base_settings.daemon, startup_reconcile=True),
        risk_enabled=False,
    )
    now_ms = int(time.time() * 1000)
    with LiveStateStore(tmp_path / "live_state.db") as store:
        arm_breaker(
            store,
            reason_codes=["LIVE_TEST_ORDER_REQUIRED"],
            source="test",
            ts_ms=now_ms - 1000,
            action=ACTION_HALT_NEW_INTENTS,
        )
        summary = asyncio.run(
            run_live_model_alpha_runtime(
                store=store,
                client=_PrivateClient(),
                public_client=_PublicClient(),
                public_ws_client=_PublicWsClient(),
                settings=settings,
                executor_gateway=_ExecutorGateway(),
            )
        )

    assert summary["halted"] is False
    assert summary["cycles"] >= 1


def test_live_model_alpha_runtime_consumes_private_ws_order_events(tmp_path: Path, monkeypatch) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    monkeypatch.setattr(runtime_module, "_load_predictor_for_runtime", lambda **_: SimpleNamespace(run_dir=Path("run-live")))
    monkeypatch.setattr(runtime_module, "_build_live_feature_provider", lambda **_: _FeatureProvider())
    monkeypatch.setattr(runtime_module, "_build_live_strategy", lambda **_: _NoIntentStrategy())

    settings = _runtime_settings(tmp_path, rollout_mode="canary", canary=True)
    now_ms = int(time.time() * 1000)
    private_ws_client = _PrivateWsClient(
        [
            MyOrderEvent(
                ts_ms=now_ms,
                uuid="ws-order-1",
                identifier="AUTOBOT-autobot-001-intent-ws-1-1700000000000-a",
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                state="wait",
                price=100000000.0,
                volume=0.01,
                executed_volume=0.0,
                raw={"event_name": "ORDER_STATE", "state": "wait", "uuid": "ws-order-1"},
            )
        ]
    )
    with LiveStateStore(tmp_path / "live_state.db") as store:
        summary = asyncio.run(
            run_live_model_alpha_runtime(
                store=store,
                client=_PrivateClient(),
                public_client=_PublicClient(),
                public_ws_client=_PublicWsClient(),
                settings=settings,
                executor_gateway=None,
                private_ws_client=private_ws_client,
            )
        )
        order = store.order_by_uuid(uuid="ws-order-1")

    assert summary["private_ws_events_total"] == 1
    assert summary["private_ws_last_event_ts_ms"] == now_ms
    assert order is not None
    assert order["local_state"] == "OPEN"


def test_live_model_alpha_runtime_prefers_real_public_micro_streams_when_available(tmp_path: Path, monkeypatch) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    captured_provider: dict[str, object] = {}

    class _CapturingFeatureProvider(_FeatureProvider):
        def __init__(self, provider):  # noqa: ANN001
            super().__init__()
            captured_provider["provider"] = provider

    monkeypatch.setattr(runtime_module, "_load_predictor_for_runtime", lambda **_: SimpleNamespace(run_dir=Path("run-live")))
    monkeypatch.setattr(
        runtime_module,
        "_build_live_feature_provider",
        lambda **kwargs: _CapturingFeatureProvider(kwargs["micro_snapshot_provider"]),
    )
    monkeypatch.setattr(runtime_module, "_build_live_strategy", lambda **_: _NoIntentStrategy())

    settings = _runtime_settings(tmp_path, rollout_mode="shadow")
    with LiveStateStore(tmp_path / "live_state.db") as store:
        summary = asyncio.run(
            run_live_model_alpha_runtime(
                store=store,
                client=_PrivateClient(),
                public_client=_PublicClient(),
                public_ws_client=_PublicWsClientWithMicro(),
                settings=settings,
                executor_gateway=None,
            )
        )

    provider = captured_provider.get("provider")
    assert provider is not None
    snapshot = provider.get("KRW-BTC", int(summary["ended_ts_ms"]))  # type: ignore[union-attr]
    assert snapshot is not None
    assert snapshot.trade_events >= 1  # type: ignore[union-attr]
    assert snapshot.book_available is True  # type: ignore[union-attr]
    assert snapshot.book_events >= 1  # type: ignore[union-attr]


def test_live_model_alpha_runtime_does_not_halt_on_clean_private_ws_completion(tmp_path: Path, monkeypatch) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    class _FinitePrivateWsClient:
        stats = {"received_events": 0}

        async def stream_private(self, *, duration_sec=None):  # noqa: ANN201
            _ = duration_sec
            if False:
                yield None
            return

    monkeypatch.setattr(runtime_module, "_load_predictor_for_runtime", lambda **_: SimpleNamespace(run_dir=Path("run-live")))
    monkeypatch.setattr(runtime_module, "_build_live_feature_provider", lambda **_: _FeatureProvider())
    monkeypatch.setattr(runtime_module, "_build_live_strategy", lambda **_: _NoIntentStrategy())

    settings = _runtime_settings(tmp_path, rollout_mode="canary", canary=True)
    with LiveStateStore(tmp_path / "live_state.db") as store:
        summary = asyncio.run(
            run_live_model_alpha_runtime(
                store=store,
                client=_PrivateClient(),
                public_client=_PublicClient(),
                public_ws_client=_PublicWsClient(),
                settings=settings,
                executor_gateway=None,
                private_ws_client=_FinitePrivateWsClient(),
            )
        )

    assert summary["halted"] is False
    assert summary["stream_stop_reason"] == "STREAM_COMPLETED"
    assert "STALE_PRIVATE_WS_STREAM" not in summary["halted_reasons"]


def test_live_model_alpha_runtime_caps_bid_notional_to_canary_limit(tmp_path: Path, monkeypatch) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    monkeypatch.setattr(runtime_module, "_load_predictor_for_runtime", lambda **_: SimpleNamespace(run_dir=Path("run-live")))
    monkeypatch.setattr(runtime_module, "_build_live_feature_provider", lambda **_: _FeatureProvider())
    monkeypatch.setattr(runtime_module, "_build_live_strategy", lambda **_: _LargeNotionalStrategy())

    settings = _runtime_settings(tmp_path, rollout_mode="canary", canary=True)
    executor = _ExecutorGateway()
    now_ms = int(time.time() * 1000)
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.set_live_rollout_contract(
            payload=build_rollout_contract(
                mode="canary",
                target_unit="autobot-live-alpha.service",
                arm_token="demo-token",
                ts_ms=now_ms - 1000,
                canary_max_notional_quote=6000.0,
            ),
            ts_ms=now_ms - 1000,
        )
        store.set_live_test_order(
            payload=build_rollout_test_order_record(
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price="50000000",
                volume="0.0001",
                ok=True,
                response_payload={"ok": True},
                ts_ms=now_ms,
            ),
            ts_ms=now_ms,
        )
        summary = asyncio.run(
            run_live_model_alpha_runtime(
                store=store,
                client=_PrivateClient(),
                public_client=_PublicClient(),
                public_ws_client=_PublicWsClient(),
                settings=settings,
                executor_gateway=executor,
            )
        )

    assert summary["submitted_intents_total"] == 1
    assert len(executor.calls) == 1
    submitted_intent = executor.calls[0]["intent"]
    notional_quote = float(submitted_intent.price) * float(submitted_intent.volume)
    assert notional_quote <= 6003.0


def test_live_model_alpha_runtime_caps_canary_bid_timeout_to_three_minutes(tmp_path: Path, monkeypatch) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    monkeypatch.setattr(runtime_module, "_load_predictor_for_runtime", lambda **_: SimpleNamespace(run_dir=Path("run-live")))
    monkeypatch.setattr(runtime_module, "_build_live_feature_provider", lambda **_: _FeatureProvider())
    monkeypatch.setattr(runtime_module, "_build_live_strategy", lambda **_: _Strategy())

    settings = _runtime_settings(tmp_path, rollout_mode="canary", canary=True)
    executor = _ExecutorGateway()
    now_ms = int(time.time() * 1000)
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.set_live_rollout_contract(
            payload=build_rollout_contract(
                mode="canary",
                target_unit="autobot-live-alpha.service",
                arm_token="demo-token",
                ts_ms=now_ms - 1000,
            ),
            ts_ms=now_ms - 1000,
        )
        store.set_live_test_order(
            payload=build_rollout_test_order_record(
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price="50000000",
                volume="0.0001",
                ok=True,
                response_payload={"ok": True},
                ts_ms=now_ms,
            ),
            ts_ms=now_ms,
        )
        summary = asyncio.run(
            run_live_model_alpha_runtime(
                store=store,
                client=_PrivateClient(),
                public_client=_PublicClient(),
                public_ws_client=_PublicWsClient(),
                settings=settings,
                executor_gateway=executor,
            )
        )

    assert summary["submitted_intents_total"] == 1
    meta_payload = json.loads(str(executor.calls[0]["meta_json"]))
    exec_profile = ((meta_payload.get("execution") or {}).get("exec_profile")) or {}
    assert int(exec_profile["timeout_ms"]) == 180000
    assert int(exec_profile["replace_interval_ms"]) == 180000


def test_live_model_alpha_runtime_caps_main_live_bid_timeout_to_three_minutes(tmp_path: Path, monkeypatch) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    monkeypatch.setattr(runtime_module, "_load_predictor_for_runtime", lambda **_: SimpleNamespace(run_dir=Path("run-live")))
    monkeypatch.setattr(runtime_module, "_build_live_feature_provider", lambda **_: _FeatureProvider())
    monkeypatch.setattr(runtime_module, "_build_live_strategy", lambda **_: _Strategy())

    settings = _runtime_settings(tmp_path, rollout_mode="live", canary=False)
    executor = _ExecutorGateway()
    now_ms = int(time.time() * 1000)
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.set_live_rollout_contract(
            payload=build_rollout_contract(
                mode="live",
                target_unit="autobot-live-alpha.service",
                arm_token="demo-token",
                ts_ms=now_ms - 1000,
            ),
            ts_ms=now_ms - 1000,
        )
        store.set_live_test_order(
            payload=build_rollout_test_order_record(
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price="50000000",
                volume="0.0001",
                ok=True,
                response_payload={"ok": True},
                ts_ms=now_ms,
            ),
            ts_ms=now_ms,
        )
        summary = asyncio.run(
            run_live_model_alpha_runtime(
                store=store,
                client=_PrivateClient(),
                public_client=_PublicClient(),
                public_ws_client=_PublicWsClient(),
                settings=settings,
                executor_gateway=executor,
            )
        )

    assert summary["submitted_intents_total"] == 1
    meta_payload = json.loads(str(executor.calls[0]["meta_json"]))
    exec_profile = ((meta_payload.get("execution") or {}).get("exec_profile")) or {}
    assert int(exec_profile["timeout_ms"]) == 180000
    assert int(exec_profile["replace_interval_ms"]) == 180000


def test_live_model_alpha_runtime_uses_strategy_exec_profile_override(tmp_path: Path, monkeypatch) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    override_profile = order_exec_profile_to_dict(
        make_legacy_exec_profile(
            timeout_ms=60_000,
            replace_interval_ms=60_000,
            max_replaces=0,
            price_mode="CROSS_1T",
            max_chase_bps=10_000,
            min_replace_interval_ms_global=1_500,
        )
    )

    class _StrategyWithExecProfile:
        def on_ts(self, *, ts_ms: int, active_markets, latest_prices, open_markets):  # noqa: ANN201
            _ = ts_ms, active_markets, latest_prices, open_markets
            return StrategyStepResult(
                intents=(
                    StrategyOrderIntent(
                        market="KRW-BTC",
                        side="bid",
                        ref_price=50_000_000.0,
                        reason_code="MODEL_ALPHA_ENTRY_V1",
                        meta={"exec_profile": override_profile},
                    ),
                ),
                scored_rows=1,
                eligible_rows=1,
                selected_rows=1,
            )

        def on_fill(self, event):  # noqa: ANN201
            _ = event

    monkeypatch.setattr(runtime_module, "_load_predictor_for_runtime", lambda **_: SimpleNamespace(run_dir=Path("run-live")))
    monkeypatch.setattr(runtime_module, "_build_live_feature_provider", lambda **_: _FeatureProvider())
    monkeypatch.setattr(runtime_module, "_build_live_strategy", lambda **_: _StrategyWithExecProfile())

    settings = _runtime_settings(tmp_path, rollout_mode="canary", canary=True)
    executor = _ExecutorGateway()
    now_ms = int(time.time() * 1000)
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.set_live_rollout_contract(
            payload=build_rollout_contract(
                mode="canary",
                target_unit="autobot-live-alpha.service",
                arm_token="demo-token",
                ts_ms=now_ms - 1000,
            ),
            ts_ms=now_ms - 1000,
        )
        store.set_live_test_order(
            payload=build_rollout_test_order_record(
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price="50000000",
                volume="0.0001",
                ok=True,
                response_payload={"ok": True},
                ts_ms=now_ms,
            ),
            ts_ms=now_ms,
        )
        summary = asyncio.run(
            run_live_model_alpha_runtime(
                store=store,
                client=_PrivateClient(),
                public_client=_PublicClient(),
                public_ws_client=_PublicWsClient(),
                settings=settings,
                executor_gateway=executor,
            )
        )

    assert summary["submitted_intents_total"] == 1
    meta_payload = json.loads(str(executor.calls[0]["meta_json"]))
    exec_profile = ((meta_payload.get("execution") or {}).get("exec_profile")) or {}
    assert exec_profile["price_mode"] == "CROSS_1T"
    assert int(exec_profile["timeout_ms"]) == 60_000
    assert int(exec_profile["replace_interval_ms"]) == 60_000
    assert int(exec_profile["max_replaces"]) == 0


def test_live_model_alpha_runtime_clamps_bid_notional_with_size_ladder(tmp_path: Path, monkeypatch) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    predictor = SimpleNamespace(
        run_dir=Path("run-live"),
        runtime_recommendations={
            "risk_control": {
                "version": 1,
                "policy": "execution_risk_control_hoeffding_v1",
                "status": "ready",
                "decision_metric_name": "expected_action_value",
                "selected_threshold": 0.0,
                "selected_coverage": 31,
                "selected_nonpositive_rate_ucb": 0.18,
                "selected_severe_loss_rate_ucb": 0.11,
                "live_gate": {
                    "enabled": False,
                    "metric_name": "expected_action_value",
                    "threshold": 0.0,
                    "skip_reason_code": "RISK_CONTROL_BELOW_THRESHOLD",
                },
                "subgroup_family": {
                    "enabled": False,
                    "feature_name": "",
                    "bucket_count_requested": 0,
                    "bucket_count_effective": 0,
                    "bounds": [],
                    "min_coverage": 0,
                },
                "size_ladder": {
                    "enabled": True,
                    "status": "ready",
                    "feature_name": "",
                    "global_max_multiplier": 1.5,
                    "group_limits": [],
                },
            }
        },
    )
    monkeypatch.setattr(runtime_module, "_load_predictor_for_runtime", lambda **_: predictor)
    monkeypatch.setattr(runtime_module, "_build_live_feature_provider", lambda **_: _FeatureProvider())
    monkeypatch.setattr(runtime_module, "_build_live_strategy", lambda **_: _LargeNotionalStrategy())

    settings = _runtime_settings(tmp_path, rollout_mode="canary", canary=True)
    executor = _ExecutorGateway()
    now_ms = int(time.time() * 1000)
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.set_live_rollout_contract(
            payload=build_rollout_contract(
                mode="canary",
                target_unit="autobot-live-alpha.service",
                arm_token="demo-token",
                ts_ms=now_ms - 1000,
            ),
            ts_ms=now_ms - 1000,
        )
        store.set_live_test_order(
            payload=build_rollout_test_order_record(
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price="50000000",
                volume="0.0001",
                ok=True,
                response_payload={"ok": True},
                ts_ms=now_ms,
            ),
            ts_ms=now_ms,
        )
        summary = asyncio.run(
            run_live_model_alpha_runtime(
                store=store,
                client=_PrivateClient(),
                public_client=_PublicClient(),
                public_ws_client=_PublicWsClient(),
                settings=settings,
                executor_gateway=executor,
            )
        )

    assert summary["submitted_intents_total"] == 1
    submitted_intent = executor.calls[0]["intent"]
    notional_quote = float(submitted_intent.price) * float(submitted_intent.volume)
    meta_payload = json.loads(str(executor.calls[0]["meta_json"]))
    assert 9990.0 <= notional_quote <= 15010.0
    assert float(meta_payload["strategy"]["meta"]["notional_multiplier"]) == 1.5
    assert meta_payload["strategy"]["meta"]["notional_multiplier_source"] == "risk_control_size_ladder"
    assert float(meta_payload["size_ladder"]["resolved_multiplier"]) == 1.5


def test_live_model_alpha_runtime_clamps_bid_notional_with_portfolio_budget(tmp_path: Path, monkeypatch) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    predictor = SimpleNamespace(run_dir=Path("run-live"), runtime_recommendations={})
    monkeypatch.setattr(runtime_module, "_load_predictor_for_runtime", lambda **_: predictor)
    monkeypatch.setattr(runtime_module, "_build_live_feature_provider", lambda **_: _FeatureProvider())
    monkeypatch.setattr(runtime_module, "_build_live_strategy", lambda **_: _LargeNotionalStrategy())

    settings = _runtime_settings(tmp_path, rollout_mode="live", canary=False)
    executor = _ExecutorGateway()
    now_ms = int(time.time() * 1000)
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.upsert_position(
            PositionRecord(
                market="KRW-ETH",
                base_currency="ETH",
                base_amount=0.0029,
                avg_entry_price=5_000_000.0,
                updated_ts=now_ms - 2000,
            )
        )
        store.set_live_rollout_contract(
            payload=build_rollout_contract(
                mode="live",
                target_unit="autobot-live-alpha.service",
                arm_token="demo-token",
                ts_ms=now_ms - 1000,
            ),
            ts_ms=now_ms - 1000,
        )
        store.set_live_test_order(
            payload=build_rollout_test_order_record(
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price="50000000",
                volume="0.0001",
                ok=True,
                response_payload={"ok": True},
                ts_ms=now_ms,
            ),
            ts_ms=now_ms,
        )
        summary = asyncio.run(
            run_live_model_alpha_runtime(
                store=store,
                client=_PrivateClient(),
                public_client=_PublicClient(),
                public_ws_client=_PublicWsClient(),
                settings=settings,
                executor_gateway=executor,
            )
        )

    assert summary["submitted_intents_total"] == 1
    meta_payload = json.loads(str(executor.calls[0]["meta_json"]))
    budget_payload = dict(meta_payload.get("portfolio_budget") or {})
    assert budget_payload["enabled"] is True
    assert "PORTFOLIO_GROSS_BUDGET_CLAMP" in list(budget_payload.get("risk_reason_codes") or [])
    assert float(budget_payload["resolved_notional_quote"]) == pytest.approx(5_500.0)
    assert float(budget_payload["position_budget_fraction"]) == pytest.approx(0.55)
    assert float(((meta_payload.get("admissibility") or {}).get("decision") or {}).get("adjusted_notional")) == pytest.approx(5497.251374312844)


def test_live_model_alpha_runtime_steps_up_threshold_from_recent_losses(tmp_path: Path, monkeypatch) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    class _MediumActionValueStrategy:
        def on_ts(self, *, ts_ms: int, active_markets, latest_prices, open_markets):  # noqa: ANN201
            _ = ts_ms, active_markets, latest_prices, open_markets
            return StrategyStepResult(
                intents=(
                    StrategyOrderIntent(
                        market="KRW-BTC",
                        side="bid",
                        ref_price=50_000_000.0,
                        reason_code="MODEL_ALPHA_ENTRY_V1",
                        prob=0.91,
                        score=0.91,
                        meta={
                            "model_prob": 0.91,
                            "trade_action": {
                                "recommended_action": "risk",
                                "expected_action_value": 1.5,
                            },
                        },
                    ),
                ),
                scored_rows=1,
                eligible_rows=1,
                selected_rows=1,
            )

        def on_fill(self, event):  # noqa: ANN201
            _ = event

    predictor = SimpleNamespace(
        run_dir=Path("run-live"),
        runtime_recommendations={
            "risk_control": {
                "version": 1,
                "policy": "execution_risk_control_hoeffding_v1",
                "status": "ready",
                "decision_metric_name": "expected_action_value",
                "selected_threshold": 1.0,
                "selected_coverage": 31,
                "selected_nonpositive_rate_ucb": 0.18,
                "selected_severe_loss_rate_ucb": 0.11,
                "nonpositive_alpha": 0.30,
                "severe_loss_alpha": 0.20,
                "severe_loss_return_threshold": 0.01,
                "confidence_delta": 0.20,
                "live_gate": {
                    "enabled": True,
                    "metric_name": "expected_action_value",
                    "threshold": 1.0,
                    "skip_reason_code": "RISK_CONTROL_BELOW_THRESHOLD",
                },
                "subgroup_family": {
                    "enabled": False,
                    "feature_name": "",
                    "bucket_count_requested": 0,
                    "bucket_count_effective": 0,
                    "bounds": [],
                    "min_coverage": 0,
                },
                "weighting": {
                    "enabled": True,
                    "mode": "window_recency_exponential_v1",
                    "half_life_windows": 2.0,
                    "max_window_index": 1,
                },
                "threshold_results": [
                    {"threshold": 2.0},
                    {"threshold": 1.0},
                ],
                "online_adaptation": {
                    "enabled": True,
                    "mode": "recent_closed_trade_hoeffding_stepup_v1",
                    "lookback_trades": 2,
                    "max_step_up": 2,
                    "recovery_streak_required": 2,
                    "halt_breach_streak": 3,
                    "halt_reason_code": "RISK_CONTROL_ONLINE_BREACH_STREAK",
                    "confidence_delta": 0.20,
                    "checkpoint_name": "execution_risk_control_online_buffer",
                },
            }
        },
    )
    monkeypatch.setattr(runtime_module, "_load_predictor_for_runtime", lambda **_: predictor)
    monkeypatch.setattr(runtime_module, "_build_live_feature_provider", lambda **_: _FeatureProvider())
    monkeypatch.setattr(runtime_module, "_build_live_strategy", lambda **_: _MediumActionValueStrategy())
    monkeypatch.setattr(runtime_module, "recompute_trade_journal_records", lambda **_: {"rows_total": 2, "rows_updated": 0, "rows_compacted": 0})

    settings = _runtime_settings(tmp_path, rollout_mode="canary", canary=True)
    executor = _ExecutorGateway()
    now_ms = int(time.time() * 1000)
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.set_live_rollout_contract(
            payload=build_rollout_contract(
                mode="canary",
                target_unit="autobot-live-alpha.service",
                arm_token="demo-token",
                ts_ms=now_ms - 1000,
            ),
            ts_ms=now_ms - 1000,
        )
        store.set_live_test_order(
            payload=build_rollout_test_order_record(
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price="50000000",
                volume="0.0001",
                ok=True,
                response_payload={"ok": True},
                ts_ms=now_ms,
            ),
            ts_ms=now_ms,
        )
        for index in range(2):
            store.upsert_trade_journal(
                TradeJournalRecord(
                    journal_id=f"journal-loss-{index}",
                    market="KRW-BTC",
                    status="CLOSED",
                    entry_intent_id=f"intent-loss-{index}",
                    entry_order_uuid=f"entry-loss-{index}",
                    exit_order_uuid=f"exit-loss-{index}",
                    plan_id=f"plan-loss-{index}",
                    entry_submitted_ts_ms=now_ms - (10_000 * (index + 2)),
                    entry_filled_ts_ms=now_ms - (10_000 * (index + 2)),
                    exit_ts_ms=now_ms - (5_000 * (index + 1)),
                    entry_price=100.0,
                    exit_price=98.0,
                    qty=1.0,
                    entry_notional_quote=100.0,
                    exit_notional_quote=98.0,
                    realized_pnl_quote=-2.0,
                    realized_pnl_pct=-2.0,
                    entry_reason_code="MODEL_ALPHA_ENTRY_V1",
                    close_reason_code="STATE_MARK",
                    close_mode="done_ask_order",
                    model_prob=0.9,
                    selection_policy_mode="rank_effective_quantile",
                    trade_action="risk",
                    expected_edge_bps=50.0,
                    expected_downside_bps=20.0,
                    expected_net_edge_bps=30.0,
                    notional_multiplier=1.0,
                    entry_meta_json=json.dumps(
                        {"runtime": {"live_runtime_model_run_id": "run-live"}},
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    exit_meta_json=json.dumps({"close_verified": True}, ensure_ascii=False, sort_keys=True),
                    updated_ts=now_ms - (5_000 * (index + 1)),
                )
            )
        summary = asyncio.run(
            run_live_model_alpha_runtime(
                store=store,
                client=_PrivateClient(),
                public_client=_PublicClient(),
                public_ws_client=_PublicWsClient(),
                settings=settings,
                executor_gateway=executor,
            )
        )
        intents = store.list_intents()

    assert summary["submitted_intents_total"] == 1
    assert summary["skipped_intents_total"] == 0
    assert len(executor.calls) == 1
    assert intents[0]["status"] == "SUBMITTED"


def test_live_model_alpha_runtime_halts_new_intents_when_online_breach_streak_triggers(tmp_path: Path, monkeypatch) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    class _MediumActionValueStrategy:
        def on_ts(self, *, ts_ms: int, active_markets, latest_prices, open_markets):  # noqa: ANN201
            _ = ts_ms, active_markets, latest_prices, open_markets
            return StrategyStepResult(
                intents=(
                    StrategyOrderIntent(
                        market="KRW-BTC",
                        side="bid",
                        ref_price=50_000_000.0,
                        reason_code="MODEL_ALPHA_ENTRY_V1",
                        prob=0.91,
                        score=0.91,
                        meta={"model_prob": 0.91, "trade_action": {"recommended_action": "risk", "expected_action_value": 1.5}},
                    ),
                ),
                scored_rows=1,
                eligible_rows=1,
                selected_rows=1,
            )

        def on_fill(self, event):  # noqa: ANN201
            _ = event

    predictor = SimpleNamespace(
        run_dir=Path("run-live"),
        runtime_recommendations={
            "risk_control": {
                "version": 1,
                "policy": "execution_risk_control_hoeffding_v1",
                "status": "ready",
                "decision_metric_name": "expected_action_value",
                "selected_threshold": 1.0,
                "selected_coverage": 31,
                "selected_nonpositive_rate_ucb": 0.18,
                "selected_severe_loss_rate_ucb": 0.11,
                "nonpositive_alpha": 0.30,
                "severe_loss_alpha": 0.20,
                "severe_loss_return_threshold": 0.01,
                "confidence_delta": 0.20,
                "live_gate": {"enabled": True, "metric_name": "expected_action_value", "threshold": 1.0, "skip_reason_code": "RISK_CONTROL_BELOW_THRESHOLD"},
                "subgroup_family": {"enabled": False, "feature_name": "", "bucket_count_requested": 0, "bucket_count_effective": 0, "bounds": [], "min_coverage": 0},
                "weighting": {"enabled": True, "mode": "window_recency_exponential_v1", "half_life_windows": 2.0, "max_window_index": 1},
                "threshold_results": [{"threshold": 2.0}, {"threshold": 1.0}],
                "online_adaptation": {
                    "enabled": True,
                    "mode": "recent_closed_trade_hoeffding_stepup_v1",
                    "lookback_trades": 2,
                    "max_step_up": 2,
                    "recovery_streak_required": 2,
                    "min_halt_trade_count": 2,
                    "halt_breach_streak": 1,
                    "halt_reason_code": "RISK_CONTROL_ONLINE_BREACH_STREAK",
                    "confidence_delta": 0.20,
                    "checkpoint_name": "execution_risk_control_online_buffer",
                },
            }
        },
    )
    monkeypatch.setattr(runtime_module, "_load_predictor_for_runtime", lambda **_: predictor)
    monkeypatch.setattr(runtime_module, "_build_live_feature_provider", lambda **_: _FeatureProvider())
    monkeypatch.setattr(runtime_module, "_build_live_strategy", lambda **_: _MediumActionValueStrategy())
    monkeypatch.setattr(runtime_module, "recompute_trade_journal_records", lambda **_: {"rows_total": 2, "rows_updated": 0, "rows_compacted": 0})

    settings = _runtime_settings(tmp_path, rollout_mode="canary", canary=True)
    executor = _ExecutorGateway()
    now_ms = int(time.time() * 1000)
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.set_live_rollout_contract(
            payload=build_rollout_contract(mode="canary", target_unit="autobot-live-alpha.service", arm_token="demo-token", ts_ms=now_ms - 1000),
            ts_ms=now_ms - 1000,
        )
        store.set_live_test_order(
            payload=build_rollout_test_order_record(
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price="50000000",
                volume="0.0001",
                ok=True,
                response_payload={"ok": True},
                ts_ms=now_ms,
            ),
            ts_ms=now_ms,
        )
        for index in range(2):
            store.upsert_trade_journal(
                TradeJournalRecord(
                    journal_id=f"journal-halt-{index}",
                    market="KRW-BTC",
                    status="CLOSED",
                    entry_intent_id=f"intent-halt-{index}",
                    entry_order_uuid=f"entry-halt-{index}",
                    exit_order_uuid=f"exit-halt-{index}",
                    plan_id=f"plan-halt-{index}",
                    entry_submitted_ts_ms=now_ms - (10_000 * (index + 2)),
                    entry_filled_ts_ms=now_ms - (10_000 * (index + 2)),
                    exit_ts_ms=now_ms - (5_000 * (index + 1)),
                    entry_price=100.0,
                    exit_price=98.0,
                    qty=1.0,
                    entry_notional_quote=100.0,
                    exit_notional_quote=98.0,
                    realized_pnl_quote=-2.0,
                    realized_pnl_pct=-2.0,
                    entry_reason_code="MODEL_ALPHA_ENTRY_V1",
                    close_reason_code="STATE_MARK",
                    close_mode="done_ask_order",
                    model_prob=0.9,
                    selection_policy_mode="rank_effective_quantile",
                    trade_action="risk",
                    expected_edge_bps=50.0,
                    expected_downside_bps=20.0,
                    expected_net_edge_bps=30.0,
                    notional_multiplier=1.0,
                    entry_meta_json=json.dumps({"runtime": {"live_runtime_model_run_id": "run-live"}}, ensure_ascii=False, sort_keys=True),
                    exit_meta_json=json.dumps({"close_verified": True}, ensure_ascii=False, sort_keys=True),
                    updated_ts=now_ms - (5_000 * (index + 1)),
                )
            )
        summary = asyncio.run(
            run_live_model_alpha_runtime(
                store=store,
                client=_PrivateClient(),
                public_client=_PublicClient(),
                public_ws_client=_PublicWsClient(),
                settings=settings,
                executor_gateway=executor,
            )
        )
        intents = store.list_intents()
        breaker_state = store.breaker_state(breaker_key="live")

    assert summary["submitted_intents_total"] == 0
    assert summary["skipped_intents_total"] == 1
    assert intents[0]["meta"]["skip_reason"] == "RISK_CONTROL_ONLINE_BREACH_STREAK"
    assert intents[0]["meta"]["risk_control_online"]["halt_triggered"] is True
    assert breaker_state is not None
    assert breaker_state["active"] is True
    assert "RISK_CONTROL_ONLINE_BREACH_STREAK" in breaker_state["reason_codes"]


def test_resolve_execution_risk_control_online_threshold_isolates_checkpoint_by_run_id(tmp_path: Path) -> None:
    import autobot.live.model_alpha_runtime_execute as runtime_execute

    payload = {
        "version": 1,
        "policy": "execution_risk_control_hoeffding_v1",
        "status": "ready",
        "selected_threshold": 1.0,
        "threshold_results": [{"threshold": 2.0}, {"threshold": 1.0}],
        "nonpositive_alpha": 0.30,
        "severe_loss_alpha": 0.20,
        "online_adaptation": {
            "enabled": True,
            "mode": "recent_closed_trade_hoeffding_stepup_v1",
            "lookback_trades": 12,
            "max_step_up": 2,
            "recovery_streak_required": 2,
            "min_halt_trade_count": 1,
            "halt_breach_streak": 3,
            "halt_reason_code": "RISK_CONTROL_ONLINE_BREACH_STREAK",
            "confidence_delta": 0.20,
            "checkpoint_name": "execution_risk_control_online_buffer",
        },
    }

    with LiveStateStore(tmp_path / "live_state.db") as store:
        old_checkpoint_name = runtime_execute._execution_risk_control_checkpoint_name(
            base_name="execution_risk_control_online_buffer",
            run_id="run-old",
        )
        store.set_checkpoint(
            name=old_checkpoint_name,
            payload={
                "step_up": 2,
                "breach_streak": 0,
                "recovery_streak": 0,
                "halt_triggered": False,
            },
            ts_ms=1,
        )

        old_state = runtime_execute.resolve_execution_risk_control_online_threshold(
            store=store,
            run_id="run-old",
            risk_control_payload=payload,
        )
        new_state = runtime_execute.resolve_execution_risk_control_online_threshold(
            store=store,
            run_id="run-new",
            risk_control_payload=payload,
        )

    assert old_state["checkpoint_name"] == "execution_risk_control_online_buffer:run-old"
    assert new_state["checkpoint_name"] == "execution_risk_control_online_buffer:run-new"
    assert float(old_state["adaptive_threshold"]) == 2.0
    assert float(new_state["adaptive_threshold"]) == 1.0


def test_resolve_execution_risk_control_online_threshold_ignores_history_before_manual_clear_baseline(tmp_path: Path) -> None:
    import autobot.live.model_alpha_runtime_execute as runtime_execute

    payload = {
        "version": 1,
        "policy": "execution_risk_control_hoeffding_v1",
        "status": "ready",
        "selected_threshold": 1.0,
        "threshold_results": [{"threshold": 2.0}, {"threshold": 1.0}],
        "nonpositive_alpha": 0.30,
        "severe_loss_alpha": 0.20,
        "online_adaptation": {
            "enabled": True,
            "mode": "recent_closed_trade_hoeffding_stepup_v1",
            "lookback_trades": 12,
            "max_step_up": 2,
            "recovery_streak_required": 2,
            "min_halt_trade_count": 1,
            "halt_breach_streak": 1,
            "halt_reason_code": "RISK_CONTROL_ONLINE_BREACH_STREAK",
            "confidence_delta": 0.20,
            "checkpoint_name": "execution_risk_control_online_buffer",
        },
    }

    with LiveStateStore(tmp_path / "live_state.db") as store:
        checkpoint_name = runtime_execute._execution_risk_control_checkpoint_name(
            base_name="execution_risk_control_online_buffer",
            run_id="run-live",
        )
        store.set_checkpoint(
            name=checkpoint_name,
            payload={
                "history_reset_exit_ts_ms": 200,
                "last_processed_exit_ts_ms": 200,
                "breach_streak": 0,
                "halt_triggered": False,
            },
            ts_ms=300,
        )
        store.upsert_trade_journal(
            TradeJournalRecord(
                journal_id="journal-old",
                market="KRW-BTC",
                status="CLOSED",
                entry_intent_id="intent-old",
                entry_order_uuid="entry-old",
                exit_order_uuid="exit-old",
                plan_id="plan-old",
                entry_submitted_ts_ms=100,
                entry_filled_ts_ms=100,
                exit_ts_ms=150,
                entry_price=100.0,
                exit_price=98.0,
                qty=1.0,
                entry_notional_quote=100.0,
                exit_notional_quote=98.0,
                realized_pnl_quote=-2.0,
                realized_pnl_pct=-2.0,
                entry_reason_code="MODEL_ALPHA_ENTRY_V1",
                close_reason_code="STATE_MARK",
                close_mode="done_ask_order",
                entry_meta_json=json.dumps({"runtime": {"live_runtime_model_run_id": "run-live"}}, ensure_ascii=False, sort_keys=True),
                exit_meta_json=json.dumps({"close_verified": True}, ensure_ascii=False, sort_keys=True),
                updated_ts=150,
            )
        )
        store.upsert_trade_journal(
            TradeJournalRecord(
                journal_id="journal-new",
                market="KRW-BTC",
                status="CLOSED",
                entry_intent_id="intent-new",
                entry_order_uuid="entry-new",
                exit_order_uuid="exit-new",
                plan_id="plan-new",
                entry_submitted_ts_ms=250,
                entry_filled_ts_ms=250,
                exit_ts_ms=250,
                entry_price=100.0,
                exit_price=98.0,
                qty=1.0,
                entry_notional_quote=100.0,
                exit_notional_quote=98.0,
                realized_pnl_quote=-2.0,
                realized_pnl_pct=-2.0,
                entry_reason_code="MODEL_ALPHA_ENTRY_V1",
                close_reason_code="STATE_MARK",
                close_mode="done_ask_order",
                entry_meta_json=json.dumps({"runtime": {"live_runtime_model_run_id": "run-live"}}, ensure_ascii=False, sort_keys=True),
                exit_meta_json=json.dumps({"close_verified": True}, ensure_ascii=False, sort_keys=True),
                updated_ts=250,
            )
        )

        state = runtime_execute.resolve_execution_risk_control_online_threshold(
            store=store,
            run_id="run-live",
            risk_control_payload=payload,
        )

    assert state["recent_trade_count"] == 1
    assert state["halt_triggered"] is False


def test_live_model_alpha_runtime_does_not_halt_online_breach_before_min_trade_count(tmp_path: Path, monkeypatch) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    class _MediumActionValueStrategy:
        def on_ts(self, *, ts_ms: int, active_markets, latest_prices, open_markets):  # noqa: ANN201
            _ = ts_ms, active_markets, latest_prices, open_markets
            return StrategyStepResult(
                intents=(
                    StrategyOrderIntent(
                        market="KRW-BTC",
                        side="bid",
                        ref_price=50_000_000.0,
                        reason_code="MODEL_ALPHA_ENTRY_V1",
                        prob=0.91,
                        score=0.91,
                        meta={"model_prob": 0.91, "trade_action": {"recommended_action": "risk", "expected_action_value": 1.5}},
                    ),
                ),
                scored_rows=1,
                eligible_rows=1,
                selected_rows=1,
            )

        def on_fill(self, event):  # noqa: ANN201
            _ = event

    predictor = SimpleNamespace(
        run_dir=Path("run-live"),
        runtime_recommendations={
            "risk_control": {
                "version": 1,
                "policy": "execution_risk_control_hoeffding_v1",
                "status": "ready",
                "decision_metric_name": "expected_action_value",
                "selected_threshold": 1.0,
                "selected_coverage": 31,
                "selected_nonpositive_rate_ucb": 0.18,
                "selected_severe_loss_rate_ucb": 0.11,
                "nonpositive_alpha": 0.30,
                "severe_loss_alpha": 0.20,
                "severe_loss_return_threshold": 0.01,
                "confidence_delta": 0.20,
                "live_gate": {"enabled": True, "metric_name": "expected_action_value", "threshold": 1.0, "skip_reason_code": "RISK_CONTROL_BELOW_THRESHOLD"},
                "subgroup_family": {"enabled": False, "feature_name": "", "bucket_count_requested": 0, "bucket_count_effective": 0, "bounds": [], "min_coverage": 0},
                "weighting": {"enabled": True, "mode": "window_recency_exponential_v1", "half_life_windows": 2.0, "max_window_index": 1},
                "threshold_results": [{"threshold": 2.0}, {"threshold": 1.0}],
                "online_adaptation": {
                    "enabled": True,
                    "mode": "recent_closed_trade_hoeffding_stepup_v1",
                    "lookback_trades": 2,
                    "max_step_up": 2,
                    "recovery_streak_required": 2,
                    "min_halt_trade_count": 5,
                    "halt_breach_streak": 1,
                    "halt_reason_code": "RISK_CONTROL_ONLINE_BREACH_STREAK",
                    "confidence_delta": 0.20,
                    "checkpoint_name": "execution_risk_control_online_buffer",
                },
            }
        },
    )
    monkeypatch.setattr(runtime_module, "_load_predictor_for_runtime", lambda **_: predictor)
    monkeypatch.setattr(runtime_module, "_build_live_feature_provider", lambda **_: _FeatureProvider())
    monkeypatch.setattr(runtime_module, "_build_live_strategy", lambda **_: _MediumActionValueStrategy())
    monkeypatch.setattr(runtime_module, "recompute_trade_journal_records", lambda **_: {"rows_total": 2, "rows_updated": 0, "rows_compacted": 0})

    settings = _runtime_settings(tmp_path, rollout_mode="canary", canary=True)
    executor = _ExecutorGateway()
    now_ms = int(time.time() * 1000)
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.set_live_rollout_contract(
            payload=build_rollout_contract(mode="canary", target_unit="autobot-live-alpha.service", arm_token="demo-token", ts_ms=now_ms - 1000),
            ts_ms=now_ms - 1000,
        )
        store.set_live_test_order(
            payload=build_rollout_test_order_record(
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price="50000000",
                volume="0.0001",
                ok=True,
                response_payload={"ok": True},
                ts_ms=now_ms,
            ),
            ts_ms=now_ms,
        )
        for index in range(2):
            store.upsert_trade_journal(
                TradeJournalRecord(
                    journal_id=f"journal-halt-min-{index}",
                    market="KRW-BTC",
                    status="CLOSED",
                    entry_intent_id=f"intent-halt-min-{index}",
                    entry_order_uuid=f"entry-halt-min-{index}",
                    exit_order_uuid=f"exit-halt-min-{index}",
                    plan_id=f"plan-halt-min-{index}",
                    entry_submitted_ts_ms=now_ms - (10_000 * (index + 2)),
                    entry_filled_ts_ms=now_ms - (10_000 * (index + 2)),
                    exit_ts_ms=now_ms - (5_000 * (index + 1)),
                    entry_price=100.0,
                    exit_price=98.0,
                    qty=1.0,
                    entry_notional_quote=100.0,
                    exit_notional_quote=98.0,
                    realized_pnl_quote=-2.0,
                    realized_pnl_pct=-2.0,
                    entry_reason_code="MODEL_ALPHA_ENTRY_V1",
                    close_reason_code="STATE_MARK",
                    close_mode="done_ask_order",
                    model_prob=0.9,
                    selection_policy_mode="rank_effective_quantile",
                    trade_action="risk",
                    expected_edge_bps=50.0,
                    expected_downside_bps=20.0,
                    expected_net_edge_bps=30.0,
                    notional_multiplier=1.0,
                    entry_meta_json=json.dumps({"runtime": {"live_runtime_model_run_id": "run-live"}}, ensure_ascii=False, sort_keys=True),
                    exit_meta_json=json.dumps({"close_verified": True}, ensure_ascii=False, sort_keys=True),
                    updated_ts=now_ms - (5_000 * (index + 1)),
                )
            )
        summary = asyncio.run(
            run_live_model_alpha_runtime(
                store=store,
                client=_PrivateClient(),
                public_client=_PublicClient(),
                public_ws_client=_PublicWsClient(),
                settings=settings,
                executor_gateway=executor,
            )
        )
        intents = store.list_intents()
        breaker_state = store.breaker_state(breaker_key="live")

    assert summary["submitted_intents_total"] == 1
    assert summary["skipped_intents_total"] == 0
    assert intents[0]["status"] == "SUBMITTED"
    assert breaker_state is None or breaker_state["active"] is False


def test_live_model_alpha_runtime_clears_stale_online_halt_from_previous_run(tmp_path: Path, monkeypatch) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    class _MediumActionValueStrategy:
        def on_ts(self, *, ts_ms: int, active_markets, latest_prices, open_markets):  # noqa: ANN201
            _ = ts_ms, active_markets, latest_prices, open_markets
            return StrategyStepResult(
                intents=(
                    StrategyOrderIntent(
                        market="KRW-BTC",
                        side="bid",
                        ref_price=50_000_000.0,
                        reason_code="MODEL_ALPHA_ENTRY_V1",
                        prob=0.91,
                        score=0.91,
                        meta={"model_prob": 0.91, "trade_action": {"recommended_action": "risk", "expected_action_value": 1.5}},
                    ),
                ),
                scored_rows=1,
                eligible_rows=1,
                selected_rows=1,
            )

        def on_fill(self, event):  # noqa: ANN201
            _ = event

    predictor = SimpleNamespace(
        run_dir=Path("run-new"),
        runtime_recommendations={
            "risk_control": {
                "version": 1,
                "policy": "execution_risk_control_hoeffding_v1",
                "status": "ready",
                "decision_metric_name": "expected_action_value",
                "selected_threshold": 1.0,
                "selected_coverage": 31,
                "selected_nonpositive_rate_ucb": 0.18,
                "selected_severe_loss_rate_ucb": 0.11,
                "nonpositive_alpha": 0.30,
                "severe_loss_alpha": 0.20,
                "severe_loss_return_threshold": 0.01,
                "confidence_delta": 0.20,
                "live_gate": {"enabled": True, "metric_name": "expected_action_value", "threshold": 1.0, "skip_reason_code": "RISK_CONTROL_BELOW_THRESHOLD"},
                "subgroup_family": {"enabled": False, "feature_name": "", "bucket_count_requested": 0, "bucket_count_effective": 0, "bounds": [], "min_coverage": 0},
                "weighting": {"enabled": True, "mode": "window_recency_exponential_v1", "half_life_windows": 2.0, "max_window_index": 1},
                "threshold_results": [{"threshold": 2.0}, {"threshold": 1.0}],
                "online_adaptation": {
                    "enabled": True,
                    "mode": "recent_closed_trade_hoeffding_stepup_v1",
                    "lookback_trades": 12,
                    "max_step_up": 2,
                    "recovery_streak_required": 2,
                    "min_halt_trade_count": 5,
                    "halt_breach_streak": 3,
                    "halt_reason_code": "RISK_CONTROL_ONLINE_BREACH_STREAK",
                    "confidence_delta": 0.20,
                    "checkpoint_name": "execution_risk_control_online_buffer",
                },
            }
        },
    )
    monkeypatch.setattr(runtime_module, "_load_predictor_for_runtime", lambda **_: predictor)
    monkeypatch.setattr(runtime_module, "_build_live_feature_provider", lambda **_: _FeatureProvider())
    monkeypatch.setattr(runtime_module, "_build_live_strategy", lambda **_: _MediumActionValueStrategy())
    monkeypatch.setattr(runtime_module, "recompute_trade_journal_records", lambda **_: {"rows_total": 0, "rows_updated": 0, "rows_compacted": 0})

    settings = _runtime_settings(tmp_path, rollout_mode="canary", canary=True)
    executor = _ExecutorGateway()
    now_ms = int(time.time() * 1000)
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.set_live_rollout_contract(
            payload=build_rollout_contract(mode="canary", target_unit="autobot-live-alpha.service", arm_token="demo-token", ts_ms=now_ms - 1000),
            ts_ms=now_ms - 1000,
        )
        store.set_live_test_order(
            payload=build_rollout_test_order_record(
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price="50000000",
                volume="0.0001",
                ok=True,
                response_payload={"ok": True},
                ts_ms=now_ms,
            ),
            ts_ms=now_ms,
        )
        arm_breaker(
            store,
            reason_codes=["RISK_CONTROL_ONLINE_BREACH_STREAK"],
            source="execution_risk_control_online_halt",
            ts_ms=now_ms - 5000,
            action=ACTION_HALT_NEW_INTENTS,
            details={
                "checkpoint_base_name": "execution_risk_control_online_buffer",
                "checkpoint_name": "execution_risk_control_online_buffer:run-old",
                "halt_reason_code": "RISK_CONTROL_ONLINE_BREACH_STREAK",
            },
        )

        summary = asyncio.run(
            run_live_model_alpha_runtime(
                store=store,
                client=_PrivateClient(),
                public_client=_PublicClient(),
                public_ws_client=_PublicWsClient(),
                settings=settings,
                executor_gateway=executor,
            )
        )
        intents = store.list_intents()
        breaker_state = store.breaker_state(breaker_key="live")

    assert summary["submitted_intents_total"] == 1
    assert summary["skipped_intents_total"] == 0
    assert intents[0]["status"] == "SUBMITTED"
    assert breaker_state is None or breaker_state["active"] is False


def test_startup_sync_rechecks_current_run_online_halt_before_stopping(tmp_path: Path, monkeypatch) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    settings = _runtime_settings(tmp_path, rollout_mode="canary", canary=True).daemon
    now_ms = int(time.time() * 1000)
    registry_root = Path(settings.registry_root)
    run_dir = registry_root / "train_v4_crypto_cs" / "run-live"
    _write_json(
        run_dir / "runtime_recommendations.json",
        {
            "version": 1,
            "risk_control": {
                "version": 1,
                "policy": "execution_risk_control_hoeffding_v1",
                "status": "ready",
                "decision_metric_name": "expected_action_value",
                "selected_threshold": 1.0,
                "selected_coverage": 31,
                "selected_nonpositive_rate_ucb": 0.18,
                "selected_severe_loss_rate_ucb": 0.11,
                "nonpositive_alpha": 0.30,
                "severe_loss_alpha": 0.20,
                "severe_loss_return_threshold": 0.01,
                "confidence_delta": 0.20,
                "live_gate": {
                    "enabled": True,
                    "metric_name": "expected_action_value",
                    "threshold": 1.0,
                    "skip_reason_code": "RISK_CONTROL_BELOW_THRESHOLD",
                },
                "threshold_results": [{"threshold": 2.0}, {"threshold": 1.0}],
                "online_adaptation": {
                    "enabled": True,
                    "mode": "recent_closed_trade_hoeffding_stepup_v1",
                    "lookback_trades": 12,
                    "max_step_up": 2,
                    "recovery_streak_required": 2,
                    "min_halt_trade_count": 5,
                    "halt_breach_streak": 3,
                    "halt_reason_code": "RISK_CONTROL_ONLINE_BREACH_STREAK",
                    "confidence_delta": 0.20,
                    "checkpoint_name": "execution_risk_control_online_buffer",
                },
            },
        },
    )

    monkeypatch.setattr(
        runtime_module,
        "backfill_recent_bot_closed_orders",
        lambda **_: {"orders_upserted": 0},
    )
    monkeypatch.setattr(
        runtime_module,
        "_run_sync_cycle_with_breakers",
        lambda **_: {
            "report": {
                "halted": False,
                "halted_reasons": [],
                "counts": {
                    "exchange_bot_open_orders": 0,
                    "exchange_open_orders": 0,
                    "exchange_positions": 0,
                    "external_open_orders": 0,
                    "ignored_dust_positions": 0,
                    "local_only_open_orders": 0,
                    "local_open_orders": 0,
                    "local_positions": 0,
                    "local_positions_missing_on_exchange": 0,
                    "unknown_bot_open_orders": 0,
                    "unknown_positions": 0,
                },
            },
            "cancel_summary": None,
            "breaker_report": None,
            "small_account_report": None,
        },
    )
    monkeypatch.setattr(
        runtime_module,
        "resume_risk_plans_after_reconcile",
        lambda **_: {"halted": False, "counts": {"plans_kept_active": 0}},
    )

    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.set_live_rollout_contract(
            payload=build_rollout_contract(
                mode="canary",
                target_unit="autobot-live-alpha.service",
                arm_token="demo-token",
                ts_ms=now_ms - 1000,
            ),
            ts_ms=now_ms - 1000,
        )
        store.set_live_test_order(
            payload=build_rollout_test_order_record(
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price="50000000",
                volume="0.0001",
                ok=True,
                response_payload={"ok": True},
                ts_ms=now_ms,
            ),
            ts_ms=now_ms,
        )
        arm_breaker(
            store,
            reason_codes=["RISK_CONTROL_ONLINE_BREACH_STREAK"],
            source="execution_risk_control_online_halt",
            ts_ms=now_ms - 5000,
            action=ACTION_HALT_NEW_INTENTS,
            details={
                "checkpoint_base_name": "execution_risk_control_online_buffer",
                "checkpoint_name": "execution_risk_control_online_buffer:run-live",
                "halt_reason_code": "RISK_CONTROL_ONLINE_BREACH_STREAK",
            },
        )
        for index in range(2):
            store.upsert_trade_journal(
                TradeJournalRecord(
                    journal_id=f"journal-startup-online-clear-{index}",
                    market="KRW-BTC",
                    status="CLOSED",
                    entry_intent_id=f"intent-startup-online-clear-{index}",
                    entry_order_uuid=f"entry-startup-online-clear-{index}",
                    exit_order_uuid=f"exit-startup-online-clear-{index}",
                    plan_id=f"plan-startup-online-clear-{index}",
                    entry_submitted_ts_ms=now_ms - (10_000 * (index + 2)),
                    entry_filled_ts_ms=now_ms - (10_000 * (index + 2)),
                    exit_ts_ms=now_ms - (5_000 * (index + 1)),
                    entry_price=100.0,
                    exit_price=98.0,
                    qty=1.0,
                    entry_notional_quote=100.0,
                    exit_notional_quote=98.0,
                    realized_pnl_quote=-2.0,
                    realized_pnl_pct=-2.0,
                    entry_reason_code="MODEL_ALPHA_ENTRY_V1",
                    close_reason_code="STATE_MARK",
                    close_mode="done_ask_order",
                    model_prob=0.9,
                    selection_policy_mode="rank_effective_quantile",
                    trade_action="risk",
                    expected_edge_bps=50.0,
                    expected_downside_bps=20.0,
                    expected_net_edge_bps=30.0,
                    notional_multiplier=1.0,
                    entry_meta_json=json.dumps({"runtime": {"live_runtime_model_run_id": "run-live"}}, ensure_ascii=False, sort_keys=True),
                    exit_meta_json=json.dumps({"close_verified": True}, ensure_ascii=False, sort_keys=True),
                    updated_ts=now_ms - (5_000 * (index + 1)),
                )
            )

        summary = {
            "cycles": 0,
            "last_report": None,
            "last_cancel_summary": None,
            "breaker_report": None,
            "small_account_report": None,
            "last_breaker_cancel_summary": None,
            "halted": False,
            "halted_reasons": [],
            "closed_orders_backfill": None,
            "resume_report": None,
            "runtime_handoff": None,
            "rollout": None,
            "rollout_start_allowed": False,
            "rollout_order_emission_allowed": False,
            "rollout_reason_codes": [],
        }
        ok = runtime_module._startup_sync(
            store=store,
            client=_PrivateClient(),
            settings=settings,
            summary=summary,
        )
        breaker_state = store.breaker_state(breaker_key="live")

    assert ok is True
    assert summary["halted"] is False
    assert summary["startup_online_risk_recovery"]["attempted"] is True
    assert "RISK_CONTROL_ONLINE_BREACH_STREAK" in summary["startup_online_risk_recovery"]["cleared_reason_codes"]
    assert summary["rollout_start_allowed"] is True
    assert summary["rollout_order_emission_allowed"] is True
    assert breaker_state is None or breaker_state["active"] is False


def test_live_model_alpha_runtime_halts_new_intents_when_martingale_evidence_triggers(tmp_path: Path, monkeypatch) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    class _MediumActionValueStrategy:
        def on_ts(self, *, ts_ms: int, active_markets, latest_prices, open_markets):  # noqa: ANN201
            _ = ts_ms, active_markets, latest_prices, open_markets
            return StrategyStepResult(
                intents=(
                    StrategyOrderIntent(
                        market="KRW-BTC",
                        side="bid",
                        ref_price=50_000_000.0,
                        reason_code="MODEL_ALPHA_ENTRY_V1",
                        prob=0.91,
                        score=0.91,
                        meta={"model_prob": 0.91, "trade_action": {"recommended_action": "risk", "expected_action_value": 3.0}},
                    ),
                ),
                scored_rows=1,
                eligible_rows=1,
                selected_rows=1,
            )

        def on_fill(self, event):  # noqa: ANN201
            _ = event

    predictor = SimpleNamespace(
        run_dir=Path("run-live"),
        runtime_recommendations={
            "risk_control": {
                "version": 1,
                "policy": "execution_risk_control_hoeffding_v1",
                "status": "ready",
                "decision_metric_name": "expected_action_value",
                "selected_threshold": 1.0,
                "selected_coverage": 31,
                "selected_nonpositive_rate_ucb": 0.18,
                "selected_severe_loss_rate_ucb": 0.11,
                "nonpositive_alpha": 0.30,
                "severe_loss_alpha": 0.20,
                "severe_loss_return_threshold": 0.01,
                "confidence_delta": 0.20,
                "live_gate": {"enabled": True, "metric_name": "expected_action_value", "threshold": 1.0, "skip_reason_code": "RISK_CONTROL_BELOW_THRESHOLD"},
                "subgroup_family": {"enabled": False, "feature_name": "", "bucket_count_requested": 0, "bucket_count_effective": 0, "bounds": [], "min_coverage": 0},
                "weighting": {"enabled": True, "mode": "window_recency_exponential_v1", "half_life_windows": 2.0, "max_window_index": 1},
                "threshold_results": [{"threshold": 2.0}, {"threshold": 1.0}],
                "online_adaptation": {
                    "enabled": True,
                    "mode": "recent_closed_trade_hoeffding_stepup_v1",
                    "lookback_trades": 3,
                    "max_step_up": 0,
                    "recovery_streak_required": 2,
                    "halt_breach_streak": 99,
                    "halt_reason_code": "RISK_CONTROL_ONLINE_BREACH_STREAK",
                    "martingale_enabled": True,
                    "martingale_mode": "bernoulli_betting_eprocess_v1",
                    "martingale_bet_fraction": 1.0,
                    "martingale_halt_threshold": 2.0,
                    "martingale_clear_threshold": 1.1,
                    "martingale_halt_reason_code": "RISK_CONTROL_MARTINGALE_EVIDENCE",
                    "confidence_delta": 0.20,
                    "checkpoint_name": "execution_risk_control_online_buffer",
                },
            }
        },
    )
    monkeypatch.setattr(runtime_module, "_load_predictor_for_runtime", lambda **_: predictor)
    monkeypatch.setattr(runtime_module, "_build_live_feature_provider", lambda **_: _FeatureProvider())
    monkeypatch.setattr(runtime_module, "_build_live_strategy", lambda **_: _MediumActionValueStrategy())
    monkeypatch.setattr(runtime_module, "recompute_trade_journal_records", lambda **_: {"rows_total": 3, "rows_updated": 0, "rows_compacted": 0})

    settings = _runtime_settings(tmp_path, rollout_mode="canary", canary=True)
    executor = _ExecutorGateway()
    now_ms = int(time.time() * 1000)
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.set_live_rollout_contract(
            payload=build_rollout_contract(mode="canary", target_unit="autobot-live-alpha.service", arm_token="demo-token", ts_ms=now_ms - 1000),
            ts_ms=now_ms - 1000,
        )
        store.set_live_test_order(
            payload=build_rollout_test_order_record(
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price="50000000",
                volume="0.0001",
                ok=True,
                response_payload={"ok": True},
                ts_ms=now_ms,
            ),
            ts_ms=now_ms,
        )
        for index in range(3):
            store.upsert_trade_journal(
                TradeJournalRecord(
                    journal_id=f"journal-martingale-{index}",
                    market="KRW-BTC",
                    status="CLOSED",
                    entry_intent_id=f"intent-martingale-{index}",
                    entry_order_uuid=f"entry-martingale-{index}",
                    exit_order_uuid=f"exit-martingale-{index}",
                    plan_id=f"plan-martingale-{index}",
                    entry_submitted_ts_ms=now_ms - (10_000 * (index + 2)),
                    entry_filled_ts_ms=now_ms - (10_000 * (index + 2)),
                    exit_ts_ms=now_ms - (5_000 * (index + 1)),
                    entry_price=100.0,
                    exit_price=98.0,
                    qty=1.0,
                    entry_notional_quote=100.0,
                    exit_notional_quote=98.0,
                    realized_pnl_quote=-2.0,
                    realized_pnl_pct=-2.0,
                    entry_reason_code="MODEL_ALPHA_ENTRY_V1",
                    close_reason_code="STATE_MARK",
                    close_mode="done_ask_order",
                    model_prob=0.9,
                    selection_policy_mode="rank_effective_quantile",
                    trade_action="risk",
                    expected_edge_bps=50.0,
                    expected_downside_bps=20.0,
                    expected_net_edge_bps=30.0,
                    notional_multiplier=1.0,
                    entry_meta_json=json.dumps({"runtime": {"live_runtime_model_run_id": "run-live"}}, ensure_ascii=False, sort_keys=True),
                    exit_meta_json=json.dumps({"close_verified": True}, ensure_ascii=False, sort_keys=True),
                    updated_ts=now_ms - (5_000 * (index + 1)),
                )
            )
        summary = asyncio.run(
            run_live_model_alpha_runtime(
                store=store,
                client=_PrivateClient(),
                public_client=_PublicClient(),
                public_ws_client=_PublicWsClient(),
                settings=settings,
                executor_gateway=executor,
            )
        )
        intents = store.list_intents()
        breaker_state = store.breaker_state(breaker_key="live")

    assert summary["submitted_intents_total"] == 0
    assert summary["skipped_intents_total"] == 1
    assert intents[0]["meta"]["skip_reason"] == "RISK_CONTROL_MARTINGALE_EVIDENCE"
    assert intents[0]["meta"]["risk_control_online"]["martingale_halt_triggered"] is True
    assert breaker_state is not None
    assert breaker_state["action"] == ACTION_HALT_NEW_INTENTS
    assert "RISK_CONTROL_MARTINGALE_EVIDENCE" in breaker_state["reason_codes"]


def test_live_model_alpha_runtime_escalates_martingale_critical_evidence_to_cancel_halt(tmp_path: Path, monkeypatch) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    class _MediumActionValueStrategy:
        def on_ts(self, *, ts_ms: int, active_markets, latest_prices, open_markets):  # noqa: ANN201
            _ = ts_ms, active_markets, latest_prices, open_markets
            return StrategyStepResult(
                intents=(
                    StrategyOrderIntent(
                        market="KRW-BTC",
                        side="bid",
                        ref_price=50_000_000.0,
                        reason_code="MODEL_ALPHA_ENTRY_V1",
                        prob=0.91,
                        score=0.91,
                        meta={"model_prob": 0.91, "trade_action": {"recommended_action": "risk", "expected_action_value": 3.0}},
                    ),
                ),
                scored_rows=1,
                eligible_rows=1,
                selected_rows=1,
            )

        def on_fill(self, event):  # noqa: ANN201
            _ = event

    predictor = SimpleNamespace(
        run_dir=Path("run-live"),
        runtime_recommendations={
            "risk_control": {
                "version": 1,
                "policy": "execution_risk_control_hoeffding_v1",
                "status": "ready",
                "decision_metric_name": "expected_action_value",
                "selected_threshold": 1.0,
                "selected_coverage": 31,
                "selected_nonpositive_rate_ucb": 0.18,
                "selected_severe_loss_rate_ucb": 0.11,
                "nonpositive_alpha": 0.30,
                "severe_loss_alpha": 0.20,
                "severe_loss_return_threshold": 0.01,
                "confidence_delta": 0.20,
                "live_gate": {"enabled": True, "metric_name": "expected_action_value", "threshold": 1.0, "skip_reason_code": "RISK_CONTROL_BELOW_THRESHOLD"},
                "subgroup_family": {"enabled": False, "feature_name": "", "bucket_count_requested": 0, "bucket_count_effective": 0, "bounds": [], "min_coverage": 0},
                "weighting": {"enabled": True, "mode": "window_recency_exponential_v1", "half_life_windows": 2.0, "max_window_index": 1},
                "threshold_results": [{"threshold": 2.0}, {"threshold": 1.0}],
                "online_adaptation": {
                    "enabled": True,
                    "mode": "recent_closed_trade_hoeffding_stepup_v1",
                    "lookback_trades": 3,
                    "max_step_up": 0,
                    "recovery_streak_required": 2,
                    "halt_breach_streak": 99,
                    "halt_reason_code": "RISK_CONTROL_ONLINE_BREACH_STREAK",
                    "martingale_enabled": True,
                    "martingale_mode": "bernoulli_betting_eprocess_v1",
                    "martingale_bet_fraction": 1.0,
                    "martingale_halt_threshold": 2.0,
                    "martingale_clear_threshold": 1.1,
                    "martingale_escalation_threshold": 4.0,
                    "martingale_halt_reason_code": "RISK_CONTROL_MARTINGALE_EVIDENCE",
                    "martingale_critical_reason_code": "RISK_CONTROL_MARTINGALE_CRITICAL_EVIDENCE",
                    "confidence_delta": 0.20,
                    "checkpoint_name": "execution_risk_control_online_buffer",
                },
            }
        },
    )
    monkeypatch.setattr(runtime_module, "_load_predictor_for_runtime", lambda **_: predictor)
    monkeypatch.setattr(runtime_module, "_build_live_feature_provider", lambda **_: _FeatureProvider())
    monkeypatch.setattr(runtime_module, "_build_live_strategy", lambda **_: _MediumActionValueStrategy())
    monkeypatch.setattr(runtime_module, "recompute_trade_journal_records", lambda **_: {"rows_total": 3, "rows_updated": 0, "rows_compacted": 0})

    settings = _runtime_settings(tmp_path, rollout_mode="canary", canary=True)
    executor = _ExecutorGateway()
    now_ms = int(time.time() * 1000)
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.set_live_rollout_contract(
            payload=build_rollout_contract(mode="canary", target_unit="autobot-live-alpha.service", arm_token="demo-token", ts_ms=now_ms - 1000),
            ts_ms=now_ms - 1000,
        )
        store.set_live_test_order(
            payload=build_rollout_test_order_record(
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price="50000000",
                volume="0.0001",
                ok=True,
                response_payload={"ok": True},
                ts_ms=now_ms,
            ),
            ts_ms=now_ms,
        )
        for index in range(3):
            store.upsert_trade_journal(
                TradeJournalRecord(
                    journal_id=f"journal-martingale-critical-{index}",
                    market="KRW-BTC",
                    status="CLOSED",
                    entry_intent_id=f"intent-martingale-critical-{index}",
                    entry_order_uuid=f"entry-martingale-critical-{index}",
                    exit_order_uuid=f"exit-martingale-critical-{index}",
                    plan_id=f"plan-martingale-critical-{index}",
                    entry_submitted_ts_ms=now_ms - (10_000 * (index + 2)),
                    entry_filled_ts_ms=now_ms - (10_000 * (index + 2)),
                    exit_ts_ms=now_ms - (5_000 * (index + 1)),
                    entry_price=100.0,
                    exit_price=98.0,
                    qty=1.0,
                    entry_notional_quote=100.0,
                    exit_notional_quote=98.0,
                    realized_pnl_quote=-2.0,
                    realized_pnl_pct=-2.0,
                    entry_reason_code="MODEL_ALPHA_ENTRY_V1",
                    close_reason_code="STATE_MARK",
                    close_mode="done_ask_order",
                    model_prob=0.9,
                    selection_policy_mode="rank_effective_quantile",
                    trade_action="risk",
                    expected_edge_bps=50.0,
                    expected_downside_bps=20.0,
                    expected_net_edge_bps=30.0,
                    notional_multiplier=1.0,
                    entry_meta_json=json.dumps({"runtime": {"live_runtime_model_run_id": "run-live"}}, ensure_ascii=False, sort_keys=True),
                    exit_meta_json=json.dumps({"close_verified": True}, ensure_ascii=False, sort_keys=True),
                    updated_ts=now_ms - (5_000 * (index + 1)),
                )
            )
        summary = asyncio.run(
            run_live_model_alpha_runtime(
                store=store,
                client=_PrivateClient(),
                public_client=_PublicClient(),
                public_ws_client=_PublicWsClient(),
                settings=settings,
                executor_gateway=executor,
            )
        )
        intents = store.list_intents()
        breaker_state = store.breaker_state(breaker_key="live")

    assert summary["submitted_intents_total"] == 0
    assert summary["skipped_intents_total"] == 1
    assert intents[0]["meta"]["skip_reason"] == "RISK_CONTROL_MARTINGALE_CRITICAL_EVIDENCE"
    assert intents[0]["meta"]["risk_control_online"]["martingale_critical_triggered"] is True
    assert breaker_state is not None
    assert breaker_state["action"] == ACTION_HALT_AND_CANCEL_BOT_ORDERS
    assert "RISK_CONTROL_MARTINGALE_CRITICAL_EVIDENCE" in breaker_state["reason_codes"]


def test_live_model_alpha_runtime_uses_latest_price_and_price_mode_for_bid(tmp_path: Path, monkeypatch) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    monkeypatch.setattr(runtime_module, "_load_predictor_for_runtime", lambda **_: SimpleNamespace(run_dir=Path("run-live")))
    monkeypatch.setattr(runtime_module, "_build_live_feature_provider", lambda **_: _FeatureProvider())
    monkeypatch.setattr(runtime_module, "_build_live_strategy", lambda **_: _StalePriceFlowStrategy())

    settings = _runtime_settings(
        tmp_path,
        rollout_mode="canary",
        canary=True,
        model_alpha=ModelAlphaSettings(
            execution=ModelAlphaExecutionSettings(price_mode="PASSIVE_MAKER", timeout_bars=2, replace_max=2),
        ),
    )
    executor = _ExecutorGateway()
    now_ms = int(time.time() * 1000)
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.set_live_rollout_contract(
            payload=build_rollout_contract(
                mode="canary",
                target_unit="autobot-live-alpha.service",
                arm_token="demo-token",
                ts_ms=now_ms - 1000,
                canary_max_notional_quote=6000.0,
            ),
            ts_ms=now_ms - 1000,
        )
        store.set_live_test_order(
            payload=build_rollout_test_order_record(
                market="KRW-FLOW",
                side="bid",
                ord_type="limit",
                price="90.6",
                volume="10",
                ok=True,
                response_payload={"ok": True},
                ts_ms=now_ms,
            ),
            ts_ms=now_ms,
        )
        summary = asyncio.run(
            run_live_model_alpha_runtime(
                store=store,
                client=_PrivateClient(),
                public_client=_FlowPublicClient(),
                public_ws_client=_FlowPublicWsClient(),
                settings=settings,
                executor_gateway=executor,
            )
        )

    assert summary["submitted_intents_total"] == 1
    submitted_intent = executor.calls[0]["intent"]
    assert submitted_intent.market == "KRW-FLOW"
    assert float(submitted_intent.price) == 90.5


def test_live_model_alpha_runtime_applies_micro_order_policy_to_price_mode(tmp_path: Path, monkeypatch) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    monkeypatch.setattr(runtime_module, "_load_predictor_for_runtime", lambda **_: SimpleNamespace(run_dir=Path("run-live")))
    monkeypatch.setattr(runtime_module, "_build_live_feature_provider", lambda **_: _FeatureProvider())
    monkeypatch.setattr(runtime_module, "_build_live_strategy", lambda **_: _StalePriceFlowStrategy())

    settings = _runtime_settings(
        tmp_path,
        rollout_mode="canary",
        canary=True,
        model_alpha=ModelAlphaSettings(
            execution=ModelAlphaExecutionSettings(price_mode="JOIN", timeout_bars=2, replace_max=2),
        ),
        micro_order_policy=replace(MicroOrderPolicySettings(), enabled=True),
    )
    executor = _ExecutorGateway()
    now_ms = int(time.time() * 1000)
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.set_live_rollout_contract(
            payload=build_rollout_contract(
                mode="canary",
                target_unit="autobot-live-alpha.service",
                arm_token="demo-token",
                ts_ms=now_ms - 1000,
                canary_max_notional_quote=6000.0,
            ),
            ts_ms=now_ms - 1000,
        )
        store.set_live_test_order(
            payload=build_rollout_test_order_record(
                market="KRW-FLOW",
                side="bid",
                ord_type="limit",
                price="90.6",
                volume="10",
                ok=True,
                response_payload={"ok": True},
                ts_ms=now_ms,
            ),
            ts_ms=now_ms,
        )
        summary = asyncio.run(
            run_live_model_alpha_runtime(
                store=store,
                client=_PrivateClient(),
                public_client=_FlowPublicClient(),
                public_ws_client=_FlowPublicWsClient(),
                settings=settings,
                executor_gateway=executor,
            )
        )

    assert summary["submitted_intents_total"] == 1
    submitted_intent = executor.calls[0]["intent"]
    assert float(submitted_intent.price) == 90.6
    meta_payload = json.loads(str(executor.calls[0]["meta_json"]))
    policy_payload = meta_payload.get("micro_order_policy")
    assert isinstance(policy_payload, dict)
    assert policy_payload.get("reason_code") == "MICRO_MISSING_FALLBACK"
    execution_payload = meta_payload.get("execution")
    assert isinstance(execution_payload, dict)
    exec_profile_payload = execution_payload.get("exec_profile")
    assert isinstance(exec_profile_payload, dict)
    assert exec_profile_payload.get("price_mode") == "JOIN"


def test_live_model_alpha_runtime_applies_trade_gate_duplicate_entry_block(tmp_path: Path, monkeypatch) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    monkeypatch.setattr(runtime_module, "_load_predictor_for_runtime", lambda **_: SimpleNamespace(run_dir=Path("run-live")))
    monkeypatch.setattr(runtime_module, "_build_live_feature_provider", lambda **_: _FeatureProvider())
    monkeypatch.setattr(runtime_module, "_build_live_strategy", lambda **_: _Strategy())

    settings = _runtime_settings(tmp_path, rollout_mode="canary", canary=True)
    executor = _ExecutorGateway()
    now_ms = int(time.time() * 1000)
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.upsert_position(
            PositionRecord(
                market="KRW-BTC",
                base_currency="BTC",
                base_amount=0.0001,
                avg_entry_price=50_000_000.0,
                updated_ts=now_ms - 1000,
            )
        )
        store.set_live_rollout_contract(
            payload=build_rollout_contract(
                mode="canary",
                target_unit="autobot-live-alpha.service",
                arm_token="demo-token",
                ts_ms=now_ms - 1000,
                canary_max_notional_quote=6000.0,
            ),
            ts_ms=now_ms - 1000,
        )
        store.set_live_test_order(
            payload=build_rollout_test_order_record(
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price="50000000",
                volume="0.0001",
                ok=True,
                response_payload={"ok": True},
                ts_ms=now_ms,
            ),
            ts_ms=now_ms,
        )
        summary = asyncio.run(
            run_live_model_alpha_runtime(
                store=store,
                client=_PrivateClient(),
                public_client=_PublicClient(),
                public_ws_client=_PublicWsClient(),
                settings=settings,
                executor_gateway=executor,
            )
        )
        intents = store.list_intents()

    assert summary["submitted_intents_total"] == 0
    assert summary["skipped_intents_total"] == 1
    assert executor.calls == []
    assert len(intents) == 1
    meta_payload = intents[0]["meta"]
    assert meta_payload.get("skip_reason") == "CANARY_MARKET_ALREADY_ACTIVE"


def test_live_model_alpha_runtime_exposes_open_order_markets_to_strategy(tmp_path: Path, monkeypatch) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    seen_open_markets: list[set[str]] = []

    class _CaptureOpenMarketsStrategy:
        def on_ts(self, *, ts_ms: int, active_markets, latest_prices, open_markets):  # noqa: ANN201
            _ = ts_ms, active_markets, latest_prices
            seen_open_markets.append(set(open_markets))
            return StrategyStepResult(
                intents=(),
                scored_rows=0,
                eligible_rows=0,
                selected_rows=0,
            )

        def on_fill(self, event):  # noqa: ANN201
            _ = event

    monkeypatch.setattr(runtime_module, "_load_predictor_for_runtime", lambda **_: SimpleNamespace(run_dir=Path("run-live")))
    monkeypatch.setattr(runtime_module, "_build_live_feature_provider", lambda **_: _FeatureProvider())
    monkeypatch.setattr(runtime_module, "_build_live_strategy", lambda **_: _CaptureOpenMarketsStrategy())

    settings = _runtime_settings(tmp_path, rollout_mode="canary", canary=True)
    now_ms = int(time.time() * 1000)
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.upsert_order(
            OrderRecord(
                uuid="order-eth-open",
                identifier="AUTOBOT-open-eth",
                market="KRW-ETH",
                side="bid",
                ord_type="limit",
                price=3_000_000.0,
                volume_req=0.002,
                volume_filled=0.0,
                state="wait",
                created_ts=now_ms - 1000,
                updated_ts=now_ms - 1000,
                intent_id="intent-eth-open",
                tp_sl_link=None,
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="SUBMIT_ACCEPTED",
                event_source="test",
                replace_seq=0,
                root_order_uuid="order-eth-open",
                prev_order_uuid=None,
                prev_order_identifier=None,
            )
        )
        store.set_live_rollout_contract(
            payload=build_rollout_contract(
                mode="canary",
                target_unit="autobot-live-alpha.service",
                arm_token="demo-token",
                ts_ms=now_ms - 1000,
                canary_max_notional_quote=6000.0,
            ),
            ts_ms=now_ms - 1000,
        )
        store.set_live_test_order(
            payload=build_rollout_test_order_record(
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price="50000000",
                volume="0.0001",
                ok=True,
                response_payload={"ok": True},
                ts_ms=now_ms,
            ),
            ts_ms=now_ms,
        )
        asyncio.run(
            run_live_model_alpha_runtime(
                store=store,
                client=_PrivateClient(),
                public_client=_PublicClient(),
                public_ws_client=_PublicWsClient(),
                settings=settings,
                executor_gateway=_ExecutorGateway(),
            )
        )

    assert seen_open_markets
    assert "KRW-ETH" in seen_open_markets[-1]


def test_live_model_alpha_runtime_canary_blocks_new_bid_when_exchange_accounts_show_other_active_market(
    tmp_path: Path,
    monkeypatch,
) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    class _PrivateClientWithMePosition(_PrivateClient):
        def accounts(self):  # noqa: ANN201
            return [
                {"currency": "KRW", "balance": "50000", "locked": "0", "avg_buy_price": "0"},
                {"currency": "ME", "balance": "30.39811615", "locked": "0", "avg_buy_price": "184"},
                {"currency": "BTC", "balance": "0", "locked": "0", "avg_buy_price": "0"},
            ]

    monkeypatch.setattr(runtime_module, "_load_predictor_for_runtime", lambda **_: SimpleNamespace(run_dir=Path("run-live")))
    monkeypatch.setattr(runtime_module, "_build_live_feature_provider", lambda **_: _FeatureProvider())
    monkeypatch.setattr(runtime_module, "_build_live_strategy", lambda **_: _Strategy())

    settings = _runtime_settings(tmp_path, rollout_mode="canary", canary=True)
    executor = _ExecutorGateway()
    now_ms = int(time.time() * 1000)
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.set_live_rollout_contract(
            payload=build_rollout_contract(
                mode="canary",
                target_unit="autobot-live-alpha.service",
                arm_token="demo-token",
                ts_ms=now_ms - 1000,
                canary_max_notional_quote=6000.0,
            ),
            ts_ms=now_ms - 1000,
        )
        store.set_live_test_order(
            payload=build_rollout_test_order_record(
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price="50000000",
                volume="0.0001",
                ok=True,
                response_payload={"ok": True},
                ts_ms=now_ms,
            ),
            ts_ms=now_ms,
        )
        summary = asyncio.run(
            run_live_model_alpha_runtime(
                store=store,
                client=_PrivateClientWithMePosition(),
                public_client=_PublicClient(),
                public_ws_client=_PublicWsClient(),
                settings=settings,
                executor_gateway=executor,
            )
        )
        intents = store.list_intents()

    assert summary["submitted_intents_total"] == 0
    assert summary["skipped_intents_total"] == 1
    assert executor.calls == []
    assert len(intents) == 1
    assert intents[0]["meta"].get("skip_reason") == "CANARY_SLOT_UNAVAILABLE"


def test_live_model_alpha_runtime_canary_skips_new_bid_when_other_market_order_is_open(
    tmp_path: Path,
    monkeypatch,
) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    monkeypatch.setattr(runtime_module, "_load_predictor_for_runtime", lambda **_: SimpleNamespace(run_dir=Path("run-live")))
    monkeypatch.setattr(runtime_module, "_build_live_feature_provider", lambda **_: _FeatureProvider())
    monkeypatch.setattr(runtime_module, "_build_live_strategy", lambda **_: _Strategy())

    settings = _runtime_settings(tmp_path, rollout_mode="canary", canary=True)
    executor = _ExecutorGateway()
    now_ms = int(time.time() * 1000)
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.upsert_order(
            OrderRecord(
                uuid="order-eth-open",
                identifier="AUTOBOT-open-eth",
                market="KRW-ETH",
                side="bid",
                ord_type="limit",
                price=3_000_000.0,
                volume_req=0.002,
                volume_filled=0.0,
                state="wait",
                created_ts=now_ms - 1000,
                updated_ts=now_ms - 1000,
                intent_id="intent-eth-open",
                tp_sl_link=None,
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="SUBMIT_ACCEPTED",
                event_source="test",
                replace_seq=0,
                root_order_uuid="order-eth-open",
                prev_order_uuid=None,
                prev_order_identifier=None,
            )
        )
        store.set_live_rollout_contract(
            payload=build_rollout_contract(
                mode="canary",
                target_unit="autobot-live-alpha.service",
                arm_token="demo-token",
                ts_ms=now_ms - 1000,
                canary_max_notional_quote=6000.0,
            ),
            ts_ms=now_ms - 1000,
        )
        store.set_live_test_order(
            payload=build_rollout_test_order_record(
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price="50000000",
                volume="0.0001",
                ok=True,
                response_payload={"ok": True},
                ts_ms=now_ms,
            ),
            ts_ms=now_ms,
        )
        summary = asyncio.run(
            run_live_model_alpha_runtime(
                store=store,
                client=_PrivateClient(),
                public_client=_PublicClient(),
                public_ws_client=_PublicWsClient(),
                settings=settings,
                executor_gateway=executor,
            )
        )
        intents = store.list_intents()

    assert summary["submitted_intents_total"] == 0
    assert summary["skipped_intents_total"] == 1
    assert executor.calls == []
    assert len(intents) == 1
    assert intents[0]["meta"].get("skip_reason") == "CANARY_SLOT_UNAVAILABLE"


def test_live_model_alpha_runtime_blocks_new_bid_while_any_exit_is_pending(
    tmp_path: Path,
    monkeypatch,
) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    monkeypatch.setattr(runtime_module, "_load_predictor_for_runtime", lambda **_: SimpleNamespace(run_dir=Path("run-live")))
    monkeypatch.setattr(runtime_module, "_build_live_feature_provider", lambda **_: _FeatureProvider())
    monkeypatch.setattr(runtime_module, "_build_live_strategy", lambda **_: _Strategy())

    settings = _runtime_settings(tmp_path, rollout_mode="canary", canary=True)
    executor = _ExecutorGateway()
    now_ms = int(time.time() * 1000)
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="pending-exit-eth",
                market="KRW-ETH",
                side="long",
                entry_price_str="3000000",
                qty_str="0.002",
                tp_enabled=True,
                tp_pct=3.0,
                sl_enabled=True,
                sl_pct=2.0,
                trailing_enabled=False,
                state="EXITING",
                last_eval_ts_ms=now_ms - 1000,
                last_action_ts_ms=now_ms - 1000,
                current_exit_order_uuid="exit-eth-uuid",
                current_exit_order_identifier="AUTOBOT-RISK-ETH",
                replace_attempt=1,
                created_ts=now_ms - 5000,
                updated_ts=now_ms - 1000,
            )
        )
        store.set_live_rollout_contract(
            payload=build_rollout_contract(
                mode="canary",
                target_unit="autobot-live-alpha.service",
                arm_token="demo-token",
                ts_ms=now_ms - 1000,
                canary_max_notional_quote=6000.0,
            ),
            ts_ms=now_ms - 1000,
        )
        store.set_live_test_order(
            payload=build_rollout_test_order_record(
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                price="50000000",
                volume="0.0001",
                ok=True,
                response_payload={"ok": True},
                ts_ms=now_ms,
            ),
            ts_ms=now_ms,
        )
        summary = asyncio.run(
            run_live_model_alpha_runtime(
                store=store,
                client=_PrivateClient(),
                public_client=_PublicClient(),
                public_ws_client=_PublicWsClient(),
                settings=settings,
                executor_gateway=executor,
            )
        )
        intents = store.list_intents()

    assert summary["submitted_intents_total"] == 0
    assert summary["skipped_intents_total"] == 1
    assert executor.calls == []
    assert len(intents) == 1
    assert intents[0]["meta"].get("skip_reason") == "EXIT_PENDING_FLATTEN_IN_PROGRESS"


def test_live_model_alpha_runtime_persists_model_exit_plan_in_submit_meta(tmp_path: Path, monkeypatch) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    class _StrategyWithExitPlan:
        def on_ts(self, *, ts_ms: int, active_markets, latest_prices, open_markets):  # noqa: ANN201
            _ = ts_ms, active_markets, latest_prices, open_markets
            return StrategyStepResult(
                intents=(
                    StrategyOrderIntent(
                        market="KRW-FLOW",
                        side="bid",
                        ref_price=78.0,
                        reason_code="MODEL_ALPHA_ENTRY_V1",
                        prob=0.9,
                        meta={
                            "model_prob": 0.9,
                            "trade_action": {
                                "recommended_action": "risk",
                                "expected_edge": 0.0123,
                                "expected_downside_deviation": 0.0045,
                                "expected_es": 0.0061,
                                "expected_ctm": 0.000041,
                                "expected_ctm_order": 2,
                                "expected_action_value": 1.7,
                                "decision_source": "continuous_conditional_action_value",
                                "recommended_notional_multiplier": 1.2,
                            },
                            "model_exit_plan": {
                                "source": "model_alpha_v1",
                                "mode": "risk",
                                "hold_bars": 6,
                                "interval_ms": 300000,
                                "timeout_delta_ms": 1800000,
                                "tp_pct": 0.02,
                                "sl_pct": 0.01,
                                "trailing_pct": 0.015,
                            },
                            "exit_recommendation": {
                                "recommended_exit_mode": "risk",
                                "recommended_exit_mode_source": "execution_backtest_grid_search_compare",
                                "recommended_exit_mode_reason_code": "RISK_EXECUTION_COMPARE_EDGE",
                                "chosen_family": "risk",
                                "chosen_rule_id": "risk_h6_rv_36_tp2p5_sl1p5_tr0p75",
                                "hold_family_status": "supported",
                                "risk_family_status": "supported",
                                "family_compare_status": "supported",
                            },
                        },
                    ),
                ),
                scored_rows=1,
                eligible_rows=1,
                selected_rows=1,
            )

        def on_fill(self, event):  # noqa: ANN201
            _ = event

    monkeypatch.setattr(runtime_module, "_load_predictor_for_runtime", lambda **_: SimpleNamespace(run_dir=Path("run-live")))
    monkeypatch.setattr(runtime_module, "_build_live_feature_provider", lambda **_: _FeatureProvider())
    monkeypatch.setattr(runtime_module, "_build_live_strategy", lambda **_: _StrategyWithExitPlan())

    settings = _runtime_settings(
        tmp_path,
        rollout_mode="canary",
        canary=True,
        model_alpha=ModelAlphaSettings(
            execution=ModelAlphaExecutionSettings(price_mode="PASSIVE_MAKER", timeout_bars=2, replace_max=2),
        ),
    )
    executor = _ExecutorGateway()
    now_ms = int(time.time() * 1000)
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.set_live_rollout_contract(
            payload=build_rollout_contract(
                mode="canary",
                target_unit="autobot-live-alpha.service",
                arm_token="demo-token",
                ts_ms=now_ms - 1000,
                canary_max_notional_quote=6000.0,
            ),
            ts_ms=now_ms - 1000,
        )
        store.set_live_test_order(
            payload=build_rollout_test_order_record(
                market="KRW-FLOW",
                side="bid",
                ord_type="limit",
                price="90.6",
                volume="10",
                ok=True,
                response_payload={"ok": True},
                ts_ms=now_ms,
            ),
            ts_ms=now_ms,
        )
        summary = asyncio.run(
            run_live_model_alpha_runtime(
                store=store,
                client=_PrivateClient(),
                public_client=_FlowPublicClient(),
                public_ws_client=_FlowPublicWsClient(),
                settings=settings,
                executor_gateway=executor,
            )
        )

    assert summary["submitted_intents_total"] == 1
    meta_payload = json.loads(str(executor.calls[0]["meta_json"]))
    strategy_payload = meta_payload.get("strategy")
    assert isinstance(strategy_payload, dict)
    strategy_meta = strategy_payload.get("meta")
    assert isinstance(strategy_meta, dict)
    exit_plan = strategy_meta.get("model_exit_plan")
    assert isinstance(exit_plan, dict)
    assert exit_plan.get("source") == "model_alpha_v1"
    assert int(exit_plan.get("timeout_delta_ms", 0)) > 0
    trade_action = strategy_meta.get("trade_action")
    assert isinstance(trade_action, dict)
    assert trade_action.get("recommended_action") == "risk"
    assert trade_action.get("expected_es") == 0.0061
    assert trade_action.get("decision_source") == "continuous_conditional_action_value"
    exit_recommendation = strategy_meta.get("exit_recommendation")
    assert isinstance(exit_recommendation, dict)
    assert exit_recommendation.get("chosen_family") == "risk"
    assert exit_recommendation.get("chosen_rule_id") == "risk_h6_rv_36_tp2p5_sl1p5_tr0p75"
    admissibility = meta_payload.get("admissibility")
    assert isinstance(admissibility, dict)
    decision = admissibility.get("decision")
    assert isinstance(decision, dict)
    assert float(decision.get("expected_edge_bps", 0.0)) == 123.0


def test_live_model_alpha_runtime_backfills_existing_active_plan_from_model_intent(tmp_path: Path, monkeypatch) -> None:
    import autobot.live.model_alpha_runtime as runtime_module
    now_ms = int(time.time() * 1000)
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.upsert_position(
            PositionRecord(
                market="KRW-KITE",
                base_currency="KITE",
                base_amount=13.56787669,
                avg_entry_price=442.0,
                updated_ts=now_ms - 1000,
            )
        )
        store.upsert_intent(
            IntentRecord(
                intent_id="intent-kite-1",
                ts_ms=now_ms - 2000,
                market="KRW-KITE",
                side="bid",
                price=442.0,
                volume=13.56787669,
                reason_code="MODEL_ALPHA_ENTRY_V1",
                status="SUBMITTED",
                meta_json=json.dumps(
                    {
                        "submit_result": {"accepted": True},
                        "strategy": {
                            "meta": {
                                "model_exit_plan": {
                                    "source": "model_alpha_v1",
                                    "mode": "hold",
                                    "hold_bars": 6,
                                    "interval_ms": 300000,
                                    "timeout_delta_ms": 1800000,
                                    "tp_pct": 0.02,
                                    "sl_pct": 0.01,
                                    "trailing_pct": 0.015,
                                }
                            }
                        },
                    },
                    ensure_ascii=False,
                ),
            )
        )
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="legacy-plan-kite",
                market="KRW-KITE",
                side="long",
                entry_price_str="442",
                qty_str="13.56787669",
                tp_enabled=False,
                tp_pct=0.0,
                sl_enabled=False,
                sl_pct=0.0,
                trailing_enabled=False,
                trail_pct=0.0,
                state="ACTIVE",
                last_eval_ts_ms=now_ms - 1500,
                last_action_ts_ms=now_ms - 1500,
                replace_attempt=0,
                created_ts=now_ms - 2000,
                updated_ts=now_ms - 1500,
            )
        )
        runtime_module._ensure_live_risk_plan(
            store=store,
            risk_manager=LiveRiskManager(
                store=store,
                executor_gateway=None,
                config=RiskManagerConfig(
                    default_tp_pct=3.0,
                    default_sl_pct=2.0,
                    default_trailing_enabled=False,
                    default_trail_pct=0.01,
                ),
            ),
            market="KRW-KITE",
            position={
                "market": "KRW-KITE",
                "base_amount": 13.56787669,
                "avg_entry_price": 442.0,
            },
            ts_ms=now_ms,
        )
        plan = store.risk_plan_by_id(plan_id="legacy-plan-kite")

    assert plan is not None
    assert plan["timeout_ts_ms"] == (now_ms - 2000) + 1800000
    assert plan["plan_source"] == "model_alpha_v1"
    assert plan["source_intent_id"] == "intent-kite-1"
    assert plan["tp"]["tp_pct"] == 2.0
    assert plan["sl"]["sl_pct"] == 1.0
    assert plan["trailing"]["trail_pct"] == 0.015


def test_ensure_live_risk_plan_backfills_position_policy_json_from_model_exit_plan(tmp_path: Path) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    now_ms = int(time.time() * 1000)
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.upsert_position(
            PositionRecord(
                market="KRW-BSV",
                base_currency="BSV",
                base_amount=0.26916523,
                avg_entry_price=22260.0,
                updated_ts=now_ms - 500,
                tp_json=json.dumps({"enabled": False, "source": "model_alpha_v1"}, ensure_ascii=False),
                sl_json=json.dumps({"enabled": False, "source": "model_alpha_v1"}, ensure_ascii=False),
                trailing_json=json.dumps({"enabled": False, "source": "model_alpha_v1"}, ensure_ascii=False),
                managed=True,
            )
        )
        store.upsert_intent(
            IntentRecord(
                intent_id="intent-bsv-1",
                ts_ms=now_ms - 2000,
                market="KRW-BSV",
                side="bid",
                price=22260.0,
                volume=0.26916523,
                reason_code="MODEL_ALPHA_ENTRY_V1",
                status="SUBMITTED",
                meta_json=json.dumps(
                    {
                        "submit_result": {"accepted": True},
                        "strategy": {
                            "meta": {
                                "model_exit_plan": {
                                    "source": "model_alpha_v1",
                                    "mode": "risk",
                                    "hold_bars": 9,
                                    "interval_ms": 300000,
                                    "timeout_delta_ms": 2700000,
                                    "tp_pct": 0.050928971583880406,
                                    "sl_pct": 0.03395264772258694,
                                    "trailing_pct": 0.0,
                                }
                            }
                        },
                    },
                    ensure_ascii=False,
                ),
            )
        )
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="model-risk-bsv-1",
                market="KRW-BSV",
                side="long",
                entry_price_str="22260",
                qty_str="0.26916523",
                tp_enabled=False,
                tp_pct=None,
                sl_enabled=False,
                sl_pct=None,
                trailing_enabled=False,
                trail_pct=None,
                state="ACTIVE",
                last_eval_ts_ms=now_ms - 1000,
                last_action_ts_ms=0,
                replace_attempt=0,
                created_ts=now_ms - 2000,
                updated_ts=now_ms - 1000,
                timeout_ts_ms=(now_ms - 2000) + 2700000,
                plan_source="model_alpha_v1",
                source_intent_id="intent-bsv-1",
            )
        )

        runtime_module._ensure_live_risk_plan(
            store=store,
            risk_manager=LiveRiskManager(
                store=store,
                executor_gateway=None,
                config=RiskManagerConfig(
                    default_tp_pct=3.0,
                    default_sl_pct=2.0,
                    default_trailing_enabled=False,
                    default_trail_pct=0.01,
                ),
            ),
            market="KRW-BSV",
            position={
                "market": "KRW-BSV",
                "base_amount": 0.26916523,
                "avg_entry_price": 22260.0,
            },
            ts_ms=now_ms,
        )
        plan = store.risk_plan_by_id(plan_id="model-risk-bsv-1")
        positions = store.list_positions()

    assert plan is not None
    assert plan["tp"]["enabled"] is True
    assert plan["sl"]["enabled"] is True
    assert plan["tp"]["tp_pct"] == pytest.approx(5.0928971583880405)
    assert plan["sl"]["sl_pct"] == pytest.approx(3.3952647722586943)
    assert plan["plan_source"] == "model_alpha_v1"
    assert plan["source_intent_id"] == "intent-bsv-1"
    assert positions[0]["tp"]["enabled"] is True
    assert positions[0]["sl"]["enabled"] is True


def test_find_latest_model_entry_intent_prefers_filled_order_match_over_newer_unfilled_order(tmp_path: Path) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    with LiveStateStore(tmp_path / "live_state.db") as store:
        for intent_id, ts_ms, price, volume, hold_bars in (
            ("intent-old-filled", 1000, 100.0, 5.0, 6),
            ("intent-new-open", 2000, 120.0, 9.0, 18),
        ):
            store.upsert_intent(
                IntentRecord(
                    intent_id=intent_id,
                    ts_ms=ts_ms,
                    market="KRW-ION",
                    side="bid",
                    price=price,
                    volume=volume,
                    reason_code="MODEL_ALPHA_ENTRY_V1",
                    status="SUBMITTED",
                    meta_json=json.dumps(
                        {
                            "submit_result": {"accepted": True},
                            "strategy": {
                                "meta": {
                                    "model_exit_plan": {
                                        "source": "model_alpha_v1",
                                        "mode": "hold",
                                        "hold_bars": hold_bars,
                                        "interval_ms": 300000,
                                        "timeout_delta_ms": 1800000,
                                        "tp_pct": 0.02,
                                        "sl_pct": 0.01,
                                        "trailing_pct": 0.015,
                                    }
                                }
                            },
                        },
                        ensure_ascii=False,
                    ),
                )
            )
        store.upsert_order(
            OrderRecord(
                uuid="order-old-filled",
                identifier="AUTOBOT-autobot-001-intent-old-filled-1000-a",
                market="KRW-ION",
                side="bid",
                ord_type="limit",
                price=100.0,
                volume_req=5.0,
                volume_filled=5.0,
                state="done",
                created_ts=1000,
                updated_ts=1100,
                intent_id="intent-old-filled",
                local_state="DONE",
                raw_exchange_state="done",
                last_event_name="ORDER_STATE",
                event_source="test",
                root_order_uuid="order-old-filled",
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="order-new-open",
                identifier="AUTOBOT-autobot-001-intent-new-open-2000-a",
                market="KRW-ION",
                side="bid",
                ord_type="limit",
                price=120.0,
                volume_req=9.0,
                volume_filled=0.0,
                state="wait",
                created_ts=2000,
                updated_ts=2100,
                intent_id="intent-new-open",
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="ORDER_STATE",
                event_source="test",
                root_order_uuid="order-new-open",
            )
        )

        entry_intent = runtime_module._find_latest_model_entry_intent(
            store=store,
            market="KRW-ION",
            position={
                "market": "KRW-ION",
                "base_amount": 5.0,
                "avg_entry_price": 100.0,
            },
        )

    assert entry_intent is not None
    assert entry_intent["intent_id"] == "intent-old-filled"


def test_find_latest_model_entry_intent_accepts_closed_order_backfill_status_when_contract_survives(tmp_path: Path) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.upsert_intent(
            IntentRecord(
                intent_id="intent-awe",
                ts_ms=1_000,
                market="KRW-AWE",
                side="bid",
                price=77.0,
                volume=77.88313635,
                reason_code="MODEL_ALPHA_ENTRY_V1",
                status="UPDATED_FROM_CLOSED_ORDERS",
                meta_json=json.dumps(
                    {
                        "submit_result": {"accepted": True, "order_uuid": "entry-order-awe"},
                        "strategy": {
                            "meta": {
                                "model_exit_plan": {
                                    "source": "model_alpha_v1",
                                    "mode": "hold",
                                    "hold_bars": 9,
                                    "interval_ms": 300000,
                                    "timeout_delta_ms": 2700000,
                                    "tp_pct": 0.0,
                                    "sl_pct": 0.02,
                                    "trailing_pct": 0.0,
                                }
                            }
                        },
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="entry-order-awe",
                identifier="AUTOBOT-autobot-001-intent-awe-1000-a",
                market="KRW-AWE",
                side="bid",
                ord_type="limit",
                price=77.0,
                volume_req=77.88313635,
                volume_filled=77.88313635,
                state="done",
                created_ts=1_000,
                updated_ts=1_100,
                intent_id="intent-awe",
                local_state="DONE",
                raw_exchange_state="done",
                last_event_name="CLOSED_ORDERS_BACKFILL",
                event_source="closed_orders_backfill",
                root_order_uuid="entry-order-awe",
            )
        )

        entry_intent = runtime_module._find_latest_model_entry_intent(
            store=store,
            market="KRW-AWE",
            position={
                "market": "KRW-AWE",
                "base_amount": 77.88313635,
                "avg_entry_price": 77.0,
            },
        )

    assert entry_intent is not None
    assert entry_intent["intent_id"] == "intent-awe"
    assert entry_intent["plan_payload"]["mode"] == "hold"


def test_startup_sync_backfills_closed_orders_before_reconcile(tmp_path: Path, monkeypatch) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    call_order: list[str] = []

    monkeypatch.setattr(
        runtime_module,
        "backfill_recent_bot_closed_orders",
        lambda **_: call_order.append("backfill") or {"orders_upserted": 1},
    )
    monkeypatch.setattr(
        runtime_module,
        "_run_sync_cycle_with_breakers",
        lambda **_: call_order.append("cycle")
        or {
            "report": {"halted": True, "halted_reasons": ["TEST_HALT"]},
            "cancel_summary": None,
            "breaker_report": None,
            "small_account_report": None,
        },
    )
    monkeypatch.setattr(runtime_module, "_maybe_enforce_breaker", lambda **_: None)
    settings = _runtime_settings(tmp_path, rollout_mode="canary").daemon

    with LiveStateStore(tmp_path / "live_state.db") as store:
        summary = {
            "cycles": 0,
            "last_report": None,
            "last_cancel_summary": None,
            "breaker_report": None,
            "small_account_report": None,
            "last_breaker_cancel_summary": None,
            "halted": False,
            "halted_reasons": [],
            "closed_orders_backfill": None,
        }
        ok = runtime_module._startup_sync(
            store=store,
            client=_PrivateClient(),
            settings=settings,
            summary=summary,
        )

    assert ok is False
    assert call_order == ["backfill", "cycle"]
    assert summary["closed_orders_backfill"] == {"orders_upserted": 1}


def test_bootstrap_strategy_positions_opens_trade_journal_for_existing_position(tmp_path: Path) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    class _BootstrapStrategy:
        def __init__(self) -> None:
            self.fills = []

        def on_fill(self, event):  # noqa: ANN001, ANN201
            self.fills.append(event)

    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.upsert_intent(
            IntentRecord(
                intent_id="intent-bootstrap-1",
                ts_ms=1_000,
                market="KRW-ION",
                side="bid",
                price=100.0,
                volume=5.0,
                reason_code="MODEL_ALPHA_ENTRY_V1",
                status="SUBMITTED",
                meta_json=json.dumps(
                    {
                        "submit_result": {"accepted": True, "order_uuid": "entry-order-bootstrap-1"},
                        "strategy": {
                            "meta": {
                                "model_prob": 0.88,
                                "selection_policy_mode": "rank_effective_quantile",
                                "trade_action": {
                                    "recommended_action": "hold",
                                    "expected_edge": 0.0091,
                                    "expected_downside_deviation": 0.0034,
                                    "recommended_notional_multiplier": 1.1,
                                },
                                "model_exit_plan": {
                                    "source": "model_alpha_v1",
                                    "mode": "hold",
                                    "hold_bars": 6,
                                    "interval_ms": 300000,
                                    "timeout_delta_ms": 1800000,
                                    "tp_pct": 0.0,
                                    "sl_pct": 0.0,
                                    "trailing_pct": 0.0,
                                },
                            }
                        },
                        "admissibility": {"decision": {"expected_net_edge_bps": 70.0}},
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="entry-order-bootstrap-1",
                identifier="AUTOBOT-autobot-001-intent-bootstrap-1-1000-a",
                market="KRW-ION",
                side="bid",
                ord_type="limit",
                price=100.0,
                volume_req=5.0,
                volume_filled=5.0,
                state="done",
                created_ts=1_000,
                updated_ts=1_100,
                intent_id="intent-bootstrap-1",
                local_state="DONE",
                raw_exchange_state="done",
                last_event_name="ORDER_STATE",
                event_source="test",
                root_order_uuid="entry-order-bootstrap-1",
            )
        )
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="plan-bootstrap-1",
                market="KRW-ION",
                side="long",
                entry_price_str="100",
                qty_str="5",
                state="ACTIVE",
                created_ts=1_000,
                updated_ts=1_200,
                plan_source="model_alpha_v1",
                source_intent_id="intent-bootstrap-1",
            )
        )
        strategy = _BootstrapStrategy()
        runtime_module._bootstrap_strategy_positions(
            store=store,
            strategy=strategy,
            risk_manager=None,
            known_positions={
                "KRW-ION": {
                    "market": "KRW-ION",
                    "base_amount": 5.0,
                    "avg_entry_price": 100.0,
                    "updated_ts": 1_200,
                }
            },
            ts_ms=1_500,
        )
        journals = store.list_trade_journal()

    assert len(strategy.fills) == 1
    assert len(journals) == 1
    assert journals[0]["status"] == "OPEN"
    assert journals[0]["entry_intent_id"] == "intent-bootstrap-1"
    assert journals[0]["plan_id"] == "plan-bootstrap-1"
    assert journals[0]["expected_net_edge_bps"] == 70.0


def test_live_model_alpha_runtime_uses_default_risk_when_position_intent_match_is_ambiguous(tmp_path: Path) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    now_ms = int(time.time() * 1000)
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.upsert_position(
            PositionRecord(
                market="KRW-ION",
                base_currency="ION",
                base_amount=5.0,
                avg_entry_price=100.0,
                updated_ts=now_ms - 500,
            )
        )
        for intent_id, ts_ms, price, volume, hold_bars in (
            ("intent-old-ambiguous", now_ms - 4000, 101.0, 5.0, 6),
            ("intent-new-unfilled", now_ms - 2000, 120.0, 9.0, 18),
        ):
            store.upsert_intent(
                IntentRecord(
                    intent_id=intent_id,
                    ts_ms=ts_ms,
                    market="KRW-ION",
                    side="bid",
                    price=price,
                    volume=volume,
                    reason_code="MODEL_ALPHA_ENTRY_V1",
                    status="SUBMITTED",
                    meta_json=json.dumps(
                        {
                            "submit_result": {"accepted": True},
                            "strategy": {
                                "meta": {
                                    "model_exit_plan": {
                                        "source": "model_alpha_v1",
                                        "mode": "hold",
                                        "hold_bars": hold_bars,
                                        "interval_ms": 300000,
                                        "timeout_delta_ms": 1800000,
                                        "tp_pct": 0.02,
                                        "sl_pct": 0.01,
                                        "trailing_pct": 0.015,
                                    }
                                }
                            },
                        },
                        ensure_ascii=False,
                    ),
                )
            )
        store.upsert_order(
            OrderRecord(
                uuid="order-new-unfilled",
                identifier="AUTOBOT-autobot-001-intent-new-unfilled-2000-a",
                market="KRW-ION",
                side="bid",
                ord_type="limit",
                price=120.0,
                volume_req=9.0,
                volume_filled=0.0,
                state="wait",
                created_ts=now_ms - 2000,
                updated_ts=now_ms - 1500,
                intent_id="intent-new-unfilled",
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="ORDER_STATE",
                event_source="test",
                root_order_uuid="order-new-unfilled",
            )
        )

        runtime_module._ensure_live_risk_plan(
            store=store,
            risk_manager=LiveRiskManager(
                store=store,
                executor_gateway=None,
                config=RiskManagerConfig(
                    default_tp_pct=3.0,
                    default_sl_pct=2.0,
                    default_trailing_enabled=False,
                    default_trail_pct=0.01,
                ),
            ),
            market="KRW-ION",
            position={
                "market": "KRW-ION",
                "base_amount": 5.0,
                "avg_entry_price": 100.0,
            },
            ts_ms=now_ms,
        )
        plans = store.list_risk_plans(states=("ACTIVE", "TRIGGERED", "EXITING"))

    assert len(plans) == 1
    assert plans[0]["plan_id"] == "default-risk-KRW-ION"
    assert plans[0]["plan_source"] is None
    assert plans[0]["source_intent_id"] is None


def test_resolve_strategy_entry_ts_ms_ignores_closed_historical_plans(tmp_path: Path) -> None:
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="plan-old-closed",
                market="KRW-KITE",
                side="long",
                entry_price_str="441",
                qty_str="1",
                tp_enabled=False,
                sl_enabled=False,
                trailing_enabled=False,
                state="CLOSED",
                last_eval_ts_ms=1000,
                last_action_ts_ms=1000,
                replace_attempt=0,
                created_ts=1000,
                updated_ts=1000,
            )
        )
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="plan-new-active",
                market="KRW-KITE",
                side="long",
                entry_price_str="441",
                qty_str="1",
                tp_enabled=False,
                sl_enabled=False,
                trailing_enabled=False,
                state="ACTIVE",
                last_eval_ts_ms=10_000,
                last_action_ts_ms=0,
                replace_attempt=0,
                created_ts=10_000,
                updated_ts=10_000,
            )
        )

        resolved_ts = _resolve_strategy_entry_ts_ms(
            store=store,
            market="KRW-KITE",
            position={"market": "KRW-KITE", "updated_ts": 20_000},
            default_ts_ms=30_000,
        )

    assert resolved_ts == 10_000


def test_attach_exit_order_to_risk_plan_marks_latest_plan_exiting(tmp_path: Path) -> None:
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="plan-active",
                market="KRW-KITE",
                side="long",
                entry_price_str="441",
                qty_str="12.7",
                tp_enabled=True,
                tp_pct=2.0,
                sl_enabled=True,
                sl_pct=1.0,
                trailing_enabled=True,
                trail_pct=0.015,
                state="ACTIVE",
                last_eval_ts_ms=1_000,
                last_action_ts_ms=0,
                replace_attempt=0,
                created_ts=5_000,
                updated_ts=5_000,
                timeout_ts_ms=35_000,
                plan_source="model_alpha_v1",
                source_intent_id="intent-kite",
            )
        )

        linked_plan_id = _attach_exit_order_to_risk_plan(
            store=store,
            market="KRW-KITE",
            order_uuid="exit-uuid-1",
            order_identifier="AUTOBOT-exit-1",
            ts_ms=40_000,
        )
        plan = store.risk_plan_by_id(plan_id="plan-active")

    assert linked_plan_id == "plan-active"
    assert plan is not None
    assert plan["state"] == "EXITING"
    assert plan["current_exit_order_uuid"] == "exit-uuid-1"
    assert plan["current_exit_order_identifier"] == "AUTOBOT-exit-1"
    assert plan["tp"]["enabled"] is True
    assert plan["tp"]["tp_pct"] == pytest.approx(2.0)
    assert plan["sl"]["enabled"] is True
    assert plan["sl"]["sl_pct"] == pytest.approx(1.0)
    assert plan["trailing"]["enabled"] is True
    assert plan["trailing"]["trail_pct"] == pytest.approx(0.015)


def test_supervise_open_strategy_orders_aborts_stale_bid_order(tmp_path: Path) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    profile = make_legacy_exec_profile(
        timeout_ms=1_000,
        replace_interval_ms=1_000,
        max_replaces=0,
        price_mode="PASSIVE_MAKER",
        max_chase_bps=10,
        min_replace_interval_ms_global=1,
    )
    gateway = _OrderSupervisionGateway()
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.upsert_intent(
            IntentRecord(
                intent_id="intent-doge-1",
                ts_ms=1_000,
                market="KRW-DOGE",
                side="bid",
                price=134.0,
                volume=41.9,
                reason_code="MODEL_ALPHA_ENTRY_V1",
                status="SUBMITTED",
                meta_json=json.dumps(
                    {
                        "execution": {
                            "initial_ref_price": 135.0,
                            "effective_ref_price": 135.0,
                            "requested_price": 134.0,
                            "exec_profile": order_exec_profile_to_dict(profile),
                        },
                        "strategy": {"meta": {"model_prob": 0.79}},
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="doge-order-1",
                identifier="doge-order-1",
                market="KRW-DOGE",
                side="bid",
                ord_type="limit",
                price=134.0,
                volume_req=41.9,
                volume_filled=0.0,
                state="wait",
                created_ts=1_000,
                updated_ts=9_000,
                intent_id="intent-doge-1",
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="EXCHANGE_SNAPSHOT",
                event_source="test",
                replace_seq=0,
                root_order_uuid="doge-order-1",
            )
        )
        record_entry_submission(
            store=store,
            market="KRW-DOGE",
            intent_id="intent-doge-1",
            requested_price=134.0,
            requested_volume=41.9,
            reason_code="MODEL_ALPHA_ENTRY_V1",
            meta_payload={"strategy": {"meta": {"model_prob": 0.79}}},
            ts_ms=1_000,
            order_uuid="doge-order-1",
        )

        report = runtime_module._supervise_open_strategy_orders(
            store=store,
            client=_PrivateClient(),
            public_client=_NoInstrumentPublicClient(),
            executor_gateway=gateway,
            latest_prices={"KRW-DOGE": 135.0},
            micro_snapshot_provider=_NullMicroProvider(),
            micro_order_policy=None,
            instrument_cache={},
            ts_ms=10_000,
        )
        order = store.order_by_uuid(uuid="doge-order-1")
        intent = store.intent_by_id(intent_id="intent-doge-1")
        journal = store.list_trade_journal()[0]

    assert report["aborted"] == 1
    assert len(gateway.cancel_calls) == 1
    assert order is not None
    assert order["state"] == "cancel"
    assert order["local_state"] == "CANCELLED"
    assert intent is not None
    assert intent["status"] == "CANCELLED"
    assert journal["status"] == "CANCELLED_ENTRY"
    assert journal["close_reason_code"] == "MAX_REPLACES_REACHED"
    assert journal["close_mode"] == "entry_order_timeout"


def test_supervise_open_strategy_orders_waits_when_order_was_recently_updated(tmp_path: Path) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    profile = make_legacy_exec_profile(
        timeout_ms=1_000,
        replace_interval_ms=1_000,
        max_replaces=1,
        price_mode="JOIN",
        max_chase_bps=10_000,
        min_replace_interval_ms_global=1,
    )
    gateway = _OrderSupervisionGateway()
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.upsert_intent(
            IntentRecord(
                intent_id="intent-recent-1",
                ts_ms=1_000,
                market="KRW-DOGE",
                side="bid",
                price=134.0,
                volume=41.9,
                reason_code="MODEL_ALPHA_ENTRY_V1",
                status="SUBMITTED",
                meta_json=json.dumps(
                    {
                        "execution": {
                            "initial_ref_price": 135.0,
                            "effective_ref_price": 135.0,
                            "requested_price": 134.0,
                            "exec_profile": order_exec_profile_to_dict(profile),
                        },
                        "strategy": {"meta": {"model_prob": 0.79}},
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="recent-order-1",
                identifier="recent-order-1",
                market="KRW-DOGE",
                side="bid",
                ord_type="limit",
                price=134.0,
                volume_req=41.9,
                volume_filled=10.0,
                state="wait",
                created_ts=1_000,
                updated_ts=9_900,
                intent_id="intent-recent-1",
                local_state="PARTIAL",
                raw_exchange_state="wait",
                last_event_name="ORDER_STATE",
                event_source="private_ws",
                replace_seq=0,
                root_order_uuid="recent-order-1",
            )
        )

        report = runtime_module._supervise_open_strategy_orders(
            store=store,
            client=_PrivateClient(),
            public_client=_StaticPublicClient("KRW-DOGE", 1.0),
            executor_gateway=gateway,
            latest_prices={"KRW-DOGE": 135.0},
            micro_snapshot_provider=_NullMicroProvider(),
            micro_order_policy=None,
            instrument_cache={},
            ts_ms=10_000,
        )
        order = store.order_by_uuid(uuid="recent-order-1")

    assert report["waited"] == 1
    assert report["replaced"] == 0
    assert report["aborted"] == 0
    assert gateway.replace_calls == []
    assert gateway.cancel_calls == []
    assert order is not None
    assert order["local_state"] == "PARTIAL"


def test_supervise_open_strategy_orders_aborts_stale_bid_order_when_execution_meta_is_missing(tmp_path: Path) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    gateway = _OrderSupervisionGateway()
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.upsert_intent(
            IntentRecord(
                intent_id="intent-avnt-1",
                ts_ms=1_000,
                market="KRW-AVNT",
                side="bid",
                price=245.0,
                volume=22.0,
                reason_code="MODEL_ALPHA_ENTRY_V1",
                meta_json=json.dumps(
                    {
                        "source": "private_ws",
                        "stream_type": "myOrder",
                        "order_uuid": "avnt-order-1",
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                status="UPDATED_FROM_WS",
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="avnt-order-1",
                identifier="AUTOBOT-autobot-candidate-001-intent-avnt-1-1-run",
                market="KRW-AVNT",
                side="bid",
                ord_type="limit",
                price=245.0,
                volume_req=22.0,
                volume_filled=0.0,
                state="wait",
                created_ts=1_000,
                updated_ts=1_000,
                intent_id="intent-avnt-1",
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="EXCHANGE_SNAPSHOT",
                event_source="test",
                replace_seq=0,
                root_order_uuid="avnt-order-1",
            )
        )
        record_entry_submission(
            store=store,
            market="KRW-AVNT",
            intent_id="intent-avnt-1",
            requested_price=245.0,
            requested_volume=22.0,
            reason_code="MODEL_ALPHA_ENTRY_V1",
            meta_payload={"source": "private_ws"},
            ts_ms=1_000,
            order_uuid="avnt-order-1",
        )

        report = runtime_module._supervise_open_strategy_orders(
            store=store,
            client=_PrivateClient(),
            public_client=_NoInstrumentPublicClient(),
            executor_gateway=gateway,
            latest_prices={"KRW-AVNT": 245.0},
            micro_snapshot_provider=_NullMicroProvider(),
            micro_order_policy=None,
            instrument_cache={},
            ts_ms=360_000,
        )
        order = store.order_by_uuid(uuid="avnt-order-1")
        intent = store.intent_by_id(intent_id="intent-avnt-1")

    assert report["aborted"] == 1
    assert len(gateway.cancel_calls) == 1
    assert order is not None
    assert order["local_state"] == "CANCELLED"
    assert intent is not None
    assert intent["status"] == "CANCELLED"


def test_supervise_open_strategy_orders_aborts_partially_filled_bid_without_cancelling_entry_journal(tmp_path: Path) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    gateway = _OrderSupervisionGateway()
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.upsert_intent(
            IntentRecord(
                intent_id="intent-partial-abort-1",
                ts_ms=1_000,
                market="KRW-AVNT",
                side="bid",
                price=245.0,
                volume=22.0,
                reason_code="MODEL_ALPHA_ENTRY_V1",
                meta_json=json.dumps(
                    {
                        "source": "private_ws",
                        "stream_type": "myOrder",
                        "order_uuid": "avnt-order-partial-1",
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                status="UPDATED_FROM_WS",
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="avnt-order-partial-1",
                identifier="AUTOBOT-autobot-candidate-001-intent-avnt-partial-1-run",
                market="KRW-AVNT",
                side="bid",
                ord_type="limit",
                price=245.0,
                volume_req=22.0,
                volume_filled=5.0,
                state="wait",
                created_ts=1_000,
                updated_ts=1_000,
                intent_id="intent-partial-abort-1",
                local_state="PARTIAL",
                raw_exchange_state="wait",
                last_event_name="ORDER_STATE",
                event_source="test",
                replace_seq=0,
                root_order_uuid="avnt-order-partial-1",
            )
        )
        record_entry_submission(
            store=store,
            market="KRW-AVNT",
            intent_id="intent-partial-abort-1",
            requested_price=245.0,
            requested_volume=22.0,
            reason_code="MODEL_ALPHA_ENTRY_V1",
            meta_payload={"source": "private_ws"},
            ts_ms=1_000,
            order_uuid="avnt-order-partial-1",
        )

        report = runtime_module._supervise_open_strategy_orders(
            store=store,
            client=_PrivateClient(),
            public_client=_NoInstrumentPublicClient(),
            executor_gateway=gateway,
            latest_prices={"KRW-AVNT": 245.0},
            micro_snapshot_provider=_NullMicroProvider(),
            micro_order_policy=None,
            instrument_cache={},
            ts_ms=360_000,
        )
        order = store.order_by_uuid(uuid="avnt-order-partial-1")
        journal = store.trade_journal_by_entry_intent(entry_intent_id="intent-partial-abort-1")

    assert report["aborted"] == 1
    assert len(gateway.cancel_calls) == 1
    assert order is not None
    assert order["local_state"] == "CANCELLED"
    assert order["volume_filled"] == 5.0
    assert journal is not None
    assert journal["status"] == "PENDING_ENTRY"


def test_supervise_open_strategy_orders_replaces_stale_ask_order_and_updates_plan(tmp_path: Path) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    profile = make_legacy_exec_profile(
        timeout_ms=1_000,
        replace_interval_ms=1_000,
        max_replaces=1,
        price_mode="JOIN",
        max_chase_bps=10,
        min_replace_interval_ms_global=1,
    )
    gateway = _OrderSupervisionGateway()
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.upsert_intent(
            IntentRecord(
                intent_id="intent-kite-exit-1",
                ts_ms=1_000,
                market="KRW-KITE",
                side="ask",
                price=100.0,
                volume=50.0,
                reason_code="MODEL_ALPHA_EXIT_TIMEOUT",
                status="SUBMITTED",
                meta_json=json.dumps(
                    {
                        "execution": {
                            "initial_ref_price": 100.0,
                            "effective_ref_price": 100.0,
                            "requested_price": 100.0,
                            "exec_profile": order_exec_profile_to_dict(profile),
                        },
                        "strategy": {"meta": {"model_prob": 0.88}},
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            )
        )
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="plan-kite-1",
                market="KRW-KITE",
                side="long",
                entry_price_str="95",
                qty_str="50",
                tp_enabled=False,
                sl_enabled=False,
                trailing_enabled=False,
                state="EXITING",
                current_exit_order_uuid="kite-order-1",
                current_exit_order_identifier="kite-order-1",
                replace_attempt=0,
                created_ts=1_000,
                updated_ts=1_000,
                plan_source="model_alpha_v1",
                source_intent_id="intent-kite-entry",
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="kite-order-1",
                identifier="kite-order-1",
                market="KRW-KITE",
                side="ask",
                ord_type="limit",
                price=100.0,
                volume_req=50.0,
                volume_filled=0.0,
                state="wait",
                created_ts=1_000,
                updated_ts=9_000,
                intent_id="intent-kite-exit-1",
                tp_sl_link="plan-kite-1",
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="EXCHANGE_SNAPSHOT",
                event_source="test",
                replace_seq=0,
                root_order_uuid="kite-order-1",
            )
        )

        report = runtime_module._supervise_open_strategy_orders(
            store=store,
            client=_PrivateClient(),
            public_client=_StaticPublicClient("KRW-KITE", 1.0),
            executor_gateway=gateway,
            latest_prices={"KRW-KITE": 105.0},
            micro_snapshot_provider=_NullMicroProvider(),
            micro_order_policy=None,
            instrument_cache={},
            ts_ms=10_000,
        )
        previous = store.order_by_uuid(uuid="kite-order-1")
        replaced = store.order_by_uuid(uuid="replaced-order-1")
        plan = store.risk_plan_by_id(plan_id="plan-kite-1")
        lineage = store.list_order_lineage(intent_id="intent-kite-exit-1")

    assert report["replaced"] == 1
    assert len(gateway.replace_calls) == 1
    assert str(gateway.replace_calls[0]["new_identifier"]).startswith("AUTOBOT-autobot-001-SUPREP-")
    assert previous is not None
    assert previous["state"] == "cancel"
    assert previous["local_state"] == "CANCELLED"
    assert replaced is not None
    assert replaced["state"] == "wait"
    assert replaced["local_state"] == "REPLACING"
    assert replaced["replace_seq"] == 1
    assert replaced["tp_sl_link"] == "plan-kite-1"
    assert plan is not None
    assert plan["state"] == "EXITING"
    assert plan["current_exit_order_uuid"] == "replaced-order-1"
    assert plan["replace_attempt"] == 1
    assert len(lineage) == 1
    assert lineage[0]["prev_uuid"] == "kite-order-1"
    assert lineage[0]["new_uuid"] == "replaced-order-1"


def test_supervise_open_strategy_orders_keeps_pending_replace_binding_when_new_uuid_is_unresolved(tmp_path: Path) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    class _PendingReplaceGateway(_OrderSupervisionGateway):
        def replace_order(self, **kwargs):  # noqa: ANN003, ANN201
            self.replace_calls.append(dict(kwargs))
            return SimpleNamespace(
                accepted=True,
                reason="replace_accepted_new_order_pending_lookup",
                cancelled_order_uuid=kwargs.get("prev_order_uuid"),
                new_order_uuid=None,
                new_identifier=kwargs.get("new_identifier"),
            )

    profile = make_legacy_exec_profile(
        timeout_ms=1_000,
        replace_interval_ms=1_000,
        max_replaces=1,
        price_mode="JOIN",
        max_chase_bps=10,
        min_replace_interval_ms_global=1,
    )
    gateway = _PendingReplaceGateway()
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.upsert_intent(
            IntentRecord(
                intent_id="intent-kite-pending-replace",
                ts_ms=1_000,
                market="KRW-KITE",
                side="ask",
                price=100.0,
                volume=50.0,
                reason_code="MODEL_ALPHA_EXIT_TIMEOUT",
                status="SUBMITTED",
                meta_json=json.dumps(
                    {
                        "execution": {
                            "initial_ref_price": 100.0,
                            "effective_ref_price": 100.0,
                            "requested_price": 100.0,
                            "exec_profile": order_exec_profile_to_dict(profile),
                        },
                        "strategy": {"meta": {"model_prob": 0.88}},
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            )
        )
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="plan-kite-pending-replace",
                market="KRW-KITE",
                side="long",
                entry_price_str="95",
                qty_str="50",
                tp_enabled=False,
                sl_enabled=False,
                trailing_enabled=False,
                state="EXITING",
                current_exit_order_uuid="kite-order-pending",
                current_exit_order_identifier="kite-order-pending",
                replace_attempt=0,
                created_ts=1_000,
                updated_ts=1_000,
                plan_source="model_alpha_v1",
                source_intent_id="intent-kite-entry",
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="kite-order-pending",
                identifier="kite-order-pending",
                market="KRW-KITE",
                side="ask",
                ord_type="limit",
                price=100.0,
                volume_req=50.0,
                volume_filled=0.0,
                state="wait",
                created_ts=1_000,
                updated_ts=9_000,
                intent_id="intent-kite-pending-replace",
                tp_sl_link="plan-kite-pending-replace",
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="EXCHANGE_SNAPSHOT",
                event_source="test",
                replace_seq=0,
                root_order_uuid="kite-order-pending",
            )
        )

        report = runtime_module._supervise_open_strategy_orders(
            store=store,
            client=_PrivateClient(),
            public_client=_StaticPublicClient("KRW-KITE", 1.0),
            executor_gateway=gateway,
            latest_prices={"KRW-KITE": 105.0},
            micro_snapshot_provider=_NullMicroProvider(),
            micro_order_policy=None,
            instrument_cache={},
            ts_ms=10_000,
        )
        previous = store.order_by_uuid(uuid="kite-order-pending")
        plan = store.risk_plan_by_id(plan_id="plan-kite-pending-replace")
        lineage = store.list_order_lineage(intent_id="intent-kite-pending-replace")
        pending_uuid = str(report["results"][0]["pending_order_uuid"])
        pending_order = store.order_by_uuid(uuid=pending_uuid)

    assert report["replaced"] == 1
    assert report["results"][0]["new_order_uuid"] is None
    assert report["results"][0]["pending_order_uuid"].startswith("pending:replace:")
    assert previous is not None
    assert previous["local_state"] == "CANCELLED"
    assert pending_order is not None
    assert pending_order["identifier"] == report["results"][0]["new_identifier"]
    assert pending_order["local_state"] == "REPLACING"
    assert plan is not None
    assert plan["current_exit_order_uuid"] == pending_uuid
    assert len(lineage) == 1
    assert lineage[0]["new_uuid"] == pending_uuid


def test_supervise_open_strategy_orders_aborts_stale_ask_order_when_execution_meta_is_missing(tmp_path: Path) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    gateway = _OrderSupervisionGateway()
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.upsert_intent(
            IntentRecord(
                intent_id="intent-kite-exit-missing-meta",
                ts_ms=1_000,
                market="KRW-KITE",
                side="ask",
                price=100.0,
                volume=50.0,
                reason_code="MODEL_ALPHA_EXIT_TIMEOUT",
                meta_json=json.dumps(
                    {
                        "source": "private_ws",
                        "stream_type": "myOrder",
                        "order_uuid": "kite-order-missing-meta",
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                status="UPDATED_FROM_WS",
            )
        )
        store.upsert_risk_plan(
            RiskPlanRecord(
                plan_id="plan-kite-missing-meta",
                market="KRW-KITE",
                side="long",
                entry_price_str="95",
                qty_str="50",
                tp_enabled=False,
                sl_enabled=False,
                trailing_enabled=False,
                state="EXITING",
                current_exit_order_uuid="kite-order-missing-meta",
                current_exit_order_identifier="AUTOBOT-kite-order-missing-meta",
                replace_attempt=0,
                created_ts=1_000,
                updated_ts=1_000,
                plan_source="model_alpha_v1",
                source_intent_id="intent-kite-entry",
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="kite-order-missing-meta",
                identifier="AUTOBOT-kite-order-missing-meta",
                market="KRW-KITE",
                side="ask",
                ord_type="limit",
                price=100.0,
                volume_req=50.0,
                volume_filled=0.0,
                state="wait",
                created_ts=1_000,
                updated_ts=1_000,
                intent_id="intent-kite-exit-missing-meta",
                tp_sl_link="plan-kite-missing-meta",
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="EXCHANGE_SNAPSHOT",
                event_source="test",
                replace_seq=0,
                root_order_uuid="kite-order-missing-meta",
            )
        )

        report = runtime_module._supervise_open_strategy_orders(
            store=store,
            client=_PrivateClient(),
            public_client=_StaticPublicClient("KRW-KITE", 1.0),
            executor_gateway=gateway,
            latest_prices={"KRW-KITE": 105.0},
            micro_snapshot_provider=_NullMicroProvider(),
            micro_order_policy=None,
            instrument_cache={},
            ts_ms=360_000,
        )
        order = store.order_by_uuid(uuid="kite-order-missing-meta")
        intent = store.intent_by_id(intent_id="intent-kite-exit-missing-meta")
        plan = store.risk_plan_by_id(plan_id="plan-kite-missing-meta")

    assert report["aborted"] == 1
    assert len(gateway.cancel_calls) == 1
    assert order is not None
    assert order["local_state"] == "CANCELLED"
    assert intent is not None
    assert intent["status"] == "CANCELLED"
    assert plan is not None
    assert plan["state"] == "TRIGGERED"
    assert plan["current_exit_order_uuid"] is None


def test_supervise_open_strategy_orders_falls_back_to_other_side_min_total_when_side_missing(tmp_path: Path) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    gateway = _OrderSupervisionGateway()
    profile = make_legacy_exec_profile(
        timeout_ms=1_000,
        replace_interval_ms=1_000,
        max_replaces=2,
        price_mode="JOIN",
    )
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.upsert_intent(
            IntentRecord(
                intent_id="intent-kite-dust-fallback",
                ts_ms=1_000,
                market="KRW-KITE",
                side="ask",
                price=100.0,
                volume=40.0,
                reason_code="MODEL_ALPHA_EXIT_TIMEOUT",
                meta_json=json.dumps(
                    {
                        "execution": {
                            "initial_ref_price": 100.0,
                            "effective_ref_price": 100.0,
                            "requested_price": 100.0,
                            "exec_profile": order_exec_profile_to_dict(profile),
                        },
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                status="SUBMITTED",
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="kite-order-dust-fallback",
                identifier="kite-order-dust-fallback",
                market="KRW-KITE",
                side="ask",
                ord_type="limit",
                price=100.0,
                volume_req=40.0,
                volume_filled=0.0,
                state="wait",
                created_ts=1_000,
                updated_ts=1_000,
                intent_id="intent-kite-dust-fallback",
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="EXCHANGE_SNAPSHOT",
                event_source="test",
                replace_seq=0,
                root_order_uuid="kite-order-dust-fallback",
            )
        )

        report = runtime_module._supervise_open_strategy_orders(
            store=store,
            client=_BidOnlyChancePrivateClient(),
            public_client=_StaticPublicClient("KRW-KITE", 1.0),
            executor_gateway=gateway,
            latest_prices={"KRW-KITE": 100.0},
            micro_snapshot_provider=_NullMicroProvider(),
            micro_order_policy=None,
            instrument_cache={},
            ts_ms=10_000,
        )
        order = store.order_by_uuid(uuid="kite-order-dust-fallback")

    assert report["aborted"] == 1
    assert report["replaced"] == 0
    assert report["results"][0]["reason_code"] == "MIN_NOTIONAL_DUST_ABORT"
    assert len(gateway.cancel_calls) == 1
    assert len(gateway.replace_calls) == 0
    assert order is not None
    assert order["state"] == "cancel"


def test_supervise_open_strategy_orders_reports_market_rule_failure_instead_of_silent_skip(tmp_path: Path) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    profile = make_legacy_exec_profile(
        timeout_ms=1_000,
        replace_interval_ms=1_000,
        max_replaces=1,
        price_mode="JOIN",
        max_chase_bps=10,
        min_replace_interval_ms_global=1,
    )
    gateway = _OrderSupervisionGateway()
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.upsert_intent(
            IntentRecord(
                intent_id="intent-kite-market-rules-fail",
                ts_ms=1_000,
                market="KRW-KITE",
                side="ask",
                price=100.0,
                volume=50.0,
                reason_code="MODEL_ALPHA_EXIT_TIMEOUT",
                status="SUBMITTED",
                meta_json=json.dumps(
                    {
                        "execution": {
                            "initial_ref_price": 100.0,
                            "effective_ref_price": 100.0,
                            "requested_price": 100.0,
                            "exec_profile": order_exec_profile_to_dict(profile),
                        },
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="kite-order-market-rules-fail",
                identifier="kite-order-market-rules-fail",
                market="KRW-KITE",
                side="ask",
                ord_type="limit",
                price=100.0,
                volume_req=50.0,
                volume_filled=0.0,
                state="wait",
                created_ts=1_000,
                updated_ts=1_000,
                intent_id="intent-kite-market-rules-fail",
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="EXCHANGE_SNAPSHOT",
                event_source="test",
                replace_seq=0,
                root_order_uuid="kite-order-market-rules-fail",
            )
        )

        report = runtime_module._supervise_open_strategy_orders(
            store=store,
            client=_ChanceFailurePrivateClient(),
            public_client=_StaticPublicClient("KRW-KITE", 1.0),
            executor_gateway=gateway,
            latest_prices={"KRW-KITE": 105.0},
            micro_snapshot_provider=_NullMicroProvider(),
            micro_order_policy=None,
            instrument_cache={},
            ts_ms=10_000,
        )

    assert report["evaluated"] == 1
    assert report["failed"] == 1
    assert report["replaced"] == 0
    assert report["aborted"] == 0
    assert report["reason_counts"]["MARKET_RULES_CHANCE_FAILED"] == 1
    assert report["results"][0]["reason_code"] == "MARKET_RULES_CHANCE_FAILED"
    assert gateway.replace_calls == []
    assert gateway.cancel_calls == []


def test_supervise_open_strategy_orders_lineage_persist_failure_only_warns(tmp_path: Path, monkeypatch) -> None:
    import autobot.live.model_alpha_runtime as runtime_module

    gateway = _OrderSupervisionGateway()
    profile = make_legacy_exec_profile(
        timeout_ms=1_000,
        replace_interval_ms=1_000,
        max_replaces=2,
        price_mode="JOIN",
    )
    with LiveStateStore(tmp_path / "live_state.db") as store:
        store.upsert_intent(
            IntentRecord(
                intent_id="intent-kite-lineage-warning",
                ts_ms=1_000,
                market="KRW-KITE",
                side="ask",
                price=100.0,
                volume=50.0,
                reason_code="MODEL_ALPHA_EXIT_TIMEOUT",
                meta_json=json.dumps(
                    {
                        "execution": {
                            "initial_ref_price": 100.0,
                            "effective_ref_price": 100.0,
                            "requested_price": 100.0,
                            "exec_profile": order_exec_profile_to_dict(profile),
                        },
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                status="SUBMITTED",
            )
        )
        store.upsert_order(
            OrderRecord(
                uuid="kite-order-lineage-warning",
                identifier="kite-order-lineage-warning",
                market="KRW-KITE",
                side="ask",
                ord_type="limit",
                price=100.0,
                volume_req=50.0,
                volume_filled=0.0,
                state="wait",
                created_ts=1_000,
                updated_ts=1_000,
                intent_id="intent-kite-lineage-warning",
                tp_sl_link=None,
                local_state="OPEN",
                raw_exchange_state="wait",
                last_event_name="EXCHANGE_SNAPSHOT",
                event_source="test",
                replace_seq=0,
                root_order_uuid="kite-order-lineage-warning",
            )
        )
        monkeypatch.setattr(store, "append_order_lineage", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("lineage write failed")))

        report = runtime_module._supervise_open_strategy_orders(
            store=store,
            client=_PrivateClient(),
            public_client=_StaticPublicClient("KRW-KITE", 1.0),
            executor_gateway=gateway,
            latest_prices={"KRW-KITE": 105.0},
            micro_snapshot_provider=_NullMicroProvider(),
            micro_order_policy=None,
            instrument_cache={},
            ts_ms=360_000,
        )
        breaker = runtime_module.breaker_status(store)

    assert report["replaced"] == 1
    assert report["results"][0]["warning_reason_code"] == "SUPERVISOR_REPLACE_PERSIST_FAILED"
    assert breaker["active"] is False
    assert any(
        event["event_kind"] == "WARN" and "SUPERVISOR_REPLACE_PERSIST_FAILED" in event["reason_codes"]
        for event in breaker["recent_events"]
    )
