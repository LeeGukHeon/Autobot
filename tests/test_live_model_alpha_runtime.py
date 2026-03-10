from __future__ import annotations

import asyncio
from dataclasses import replace
import json
from pathlib import Path
from types import SimpleNamespace
import time

from autobot.backtest.strategy_adapter import StrategyOrderIntent, StrategyStepResult
from autobot.live.daemon import LiveDaemonSettings
from autobot.live.model_alpha_runtime import LiveModelAlphaRuntimeSettings, run_live_model_alpha_runtime
from autobot.live.rollout import build_rollout_contract, build_rollout_test_order_record
from autobot.live.state_store import IntentRecord, LiveStateStore, OrderRecord, PositionRecord, RiskPlanRecord
from autobot.risk.live_risk_manager import LiveRiskManager, RiskManagerConfig
from autobot.strategy.micro_order_policy import MicroOrderPolicySettings
from autobot.strategy.model_alpha_v1 import ModelAlphaExecutionSettings, ModelAlphaSettings
from autobot.upbit.ws.models import TickerEvent


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


class _PublicWsClient:
    async def stream_ticker(self, markets, duration_sec=None):  # noqa: ANN201
        _ = markets, duration_sec
        yield TickerEvent(
            market="KRW-BTC",
            ts_ms=int(time.time() * 1000),
            trade_price=50_000_000.0,
            acc_trade_price_24h=10_000_000_000.0,
        )


class _FlowPublicWsClient:
    async def stream_ticker(self, markets, duration_sec=None):  # noqa: ANN201
        _ = markets, duration_sec
        yield TickerEvent(
            market="KRW-FLOW",
            ts_ms=int(time.time() * 1000),
            trade_price=90.6,
            acc_trade_price_24h=1_000_000_000.0,
        )


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
