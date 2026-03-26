from __future__ import annotations

import asyncio
import json
from pathlib import Path

import polars as pl

from autobot.data.collect.validate_ws_candles import validate_ws_candle_dataset
from autobot.data.collect.ws_candle_collector import WsCandleCollectOptions, collect_ws_candles_from_plan


class _DummyRateLimit:
    connect_rps = 5
    message_rps = 5
    message_rpm = 100


class _DummyWebSocketSettings:
    public_url = "wss://example.invalid/websocket"
    ratelimit = _DummyRateLimit()


class _DummySettings:
    websocket = _DummyWebSocketSettings()


class _FakeWebSocket:
    def __init__(self, messages: list[dict]) -> None:
        self._messages = list(messages)
        self.sent: list[object] = []

    async def send(self, payload: object) -> None:
        self.sent.append(payload)

    async def recv(self) -> object:
        if self._messages:
            return json.dumps(self._messages.pop(0))
        await asyncio.sleep(3600)
        return None

    def ping(self):
        async def _pong() -> None:
            return None

        return _pong()


class _FakeConnect:
    def __init__(self, messages: list[dict]) -> None:
        self._messages = messages
        self.websocket = _FakeWebSocket(messages)

    async def __aenter__(self) -> _FakeWebSocket:
        return self.websocket

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


def test_collect_ws_candles_dedupes_latest_update_and_validates(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    meta_dir = tmp_path / "collect_meta"
    meta_dir.mkdir(parents=True, exist_ok=True)
    plan_path = meta_dir / "ws_candle_plan.json"
    plan = {
        "filters": {"tf_set": ["1s"], "market_source_dataset": "candles_api_v1"},
        "runtime_policy": {
            "format": "DEFAULT",
            "is_only_snapshot": False,
            "is_only_realtime": False,
        },
        "safety": {"max_subscribe_messages_per_min": 20},
        "codes": ["KRW-BTC"],
        "selected_markets": ["KRW-BTC"],
    }
    plan_path.write_text(json.dumps(plan, indent=2), encoding="utf-8")

    messages = [
        {
            "type": "candle.1s",
            "code": "KRW-BTC",
            "candle_date_time_utc": "2026-03-27T00:00:05",
            "opening_price": 100.0,
            "high_price": 101.0,
            "low_price": 99.0,
            "trade_price": 100.5,
            "candle_acc_trade_volume": 1.2,
            "candle_acc_trade_price": 120.0,
            "timestamp": 1774569605000,
            "stream_type": "SNAPSHOT",
        },
        {
            "type": "candle.1s",
            "code": "KRW-BTC",
            "candle_date_time_utc": "2026-03-27T00:00:05",
            "opening_price": 100.0,
            "high_price": 102.0,
            "low_price": 99.0,
            "trade_price": 101.5,
            "candle_acc_trade_volume": 2.2,
            "candle_acc_trade_price": 220.0,
            "timestamp": 1774569605900,
            "stream_type": "REALTIME",
        },
    ]

    summary = collect_ws_candles_from_plan(
        WsCandleCollectOptions(
            plan_path=plan_path,
            parquet_root=parquet_root,
            out_dataset="ws_candle_v1",
            meta_dir=meta_dir,
            duration_sec=1,
            keepalive_interval_sec=1,
            keepalive_stale_sec=2,
        ),
        websocket_connect=lambda _url: _FakeConnect(list(messages)),
        settings_loader=lambda _config_dir: _DummySettings(),
    )

    assert summary.failures == ()
    assert summary.received_messages == 2
    assert summary.snapshot_messages == 1
    assert summary.realtime_messages == 1
    assert summary.rows_buffered == 1
    assert summary.persisted_pairs == 1

    part_file = parquet_root / "ws_candle_v1" / "tf=1s" / "market=KRW-BTC" / "part-000.parquet"
    frame = pl.read_parquet(part_file)
    assert frame.height == 1
    assert float(frame.get_column("close")[0]) == 101.5
    assert float(frame.get_column("high")[0]) == 102.0

    validate_summary = validate_ws_candle_dataset(
        parquet_root=parquet_root,
        dataset_name="ws_candle_v1",
        report_path=meta_dir / "ws_candle_validate_report.json",
    )
    assert validate_summary.fail_files == 0
    assert validate_summary.ok_files == 1
