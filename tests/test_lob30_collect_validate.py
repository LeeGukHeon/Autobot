from __future__ import annotations

import asyncio
import json
from pathlib import Path

import polars as pl

from autobot.data.collect.lob30_collector import Lob30CollectOptions, collect_lob30_from_plan
from autobot.data.collect.validate_lob30 import validate_lob30_dataset


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
        self.websocket = _FakeWebSocket(messages)

    async def __aenter__(self) -> _FakeWebSocket:
        return self.websocket

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


def test_collect_lob30_dedupes_latest_snapshot_and_validates(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    meta_dir = tmp_path / "collect_meta"
    meta_dir.mkdir(parents=True, exist_ok=True)
    plan_path = meta_dir / "lob30_plan.json"
    plan = {
        "runtime_policy": {
            "format": "DEFAULT",
            "requested_depth": 30,
            "orderbook_level": 0,
            "is_only_snapshot": False,
            "is_only_realtime": False,
        },
        "safety": {"max_subscribe_messages_per_min": 20},
        "request_codes": ["KRW-BTC.30"],
        "selected_markets": ["KRW-BTC"],
    }
    plan_path.write_text(json.dumps(plan, indent=2), encoding="utf-8")

    base_units = [
        {
            "ask_price": 101.0 + idx,
            "ask_size": 1.0 + idx,
            "bid_price": 100.0 - idx,
            "bid_size": 2.0 + idx,
        }
        for idx in range(30)
    ]
    updated_units = [dict(item) for item in base_units]
    updated_units[0]["ask_size"] = 7.5

    messages = [
        {
            "type": "orderbook",
            "code": "KRW-BTC",
            "timestamp": 1774569605000,
            "total_ask_size": 10.0,
            "total_bid_size": 9.0,
            "orderbook_units": base_units,
            "stream_type": "SNAPSHOT",
            "level": 0,
        },
        {
            "type": "orderbook",
            "code": "KRW-BTC",
            "timestamp": 1774569605000,
            "total_ask_size": 11.0,
            "total_bid_size": 9.5,
            "orderbook_units": updated_units,
            "stream_type": "REALTIME",
            "level": 0,
        },
    ]

    summary = collect_lob30_from_plan(
        Lob30CollectOptions(
            plan_path=plan_path,
            parquet_root=parquet_root,
            out_dataset="lob30_v1",
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
    assert summary.rows_buffered == 1
    assert summary.persisted_partitions == 1

    part_file = parquet_root / "lob30_v1" / "market=KRW-BTC" / "date=2026-03-27" / "part-000.parquet"
    frame = pl.read_parquet(part_file)
    assert frame.height == 1
    assert int(frame.get_column("requested_depth")[0]) == 30
    assert int(frame.get_column("levels_present")[0]) == 30
    assert float(frame.get_column("ask1_size")[0]) == 7.5

    validate_summary = validate_lob30_dataset(
        parquet_root=parquet_root,
        dataset_name="lob30_v1",
        report_path=meta_dir / "lob30_validate_report.json",
    )
    assert validate_summary.fail_files == 0
    assert validate_summary.ok_files == 1

    build_report = json.loads(summary.build_report_file.read_text(encoding="utf-8"))
    assert build_report["run_id"] == summary.run_id
    assert build_report["source_contract_ids"] == ["raw_ws_dataset:upbit_public"]
    assert "source_run_ids" in build_report
