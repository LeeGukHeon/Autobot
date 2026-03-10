from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
import time

from autobot.backtest.strategy_adapter import StrategyOrderIntent, StrategyStepResult
from autobot.live.daemon import LiveDaemonSettings
from autobot.live.model_alpha_runtime import LiveModelAlphaRuntimeSettings, run_live_model_alpha_runtime
from autobot.live.rollout import build_rollout_contract, build_rollout_test_order_record
from autobot.live.state_store import LiveStateStore
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


class _PublicWsClient:
    async def stream_ticker(self, markets, duration_sec=None):  # noqa: ANN201
        _ = markets, duration_sec
        yield TickerEvent(
            market="KRW-BTC",
            ts_ms=int(time.time() * 1000),
            trade_price=50_000_000.0,
            acc_trade_price_24h=10_000_000_000.0,
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


def _runtime_settings(tmp_path: Path, *, rollout_mode: str, canary: bool = False) -> LiveModelAlphaRuntimeSettings:
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
    return LiveModelAlphaRuntimeSettings(daemon=daemon_settings)


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
