from __future__ import annotations

import asyncio
import json
from pathlib import Path

from autobot.data.collect.private_ws_collector import PrivateWsDaemonOptions, collect_private_ws_daemon
from autobot.upbit.ws.models import MyAssetEvent, MyOrderEvent


class _FakePrivateWsClient:
    def __init__(self) -> None:
        self._events = [
            MyOrderEvent(
                ts_ms=1_774_855_600_000,
                uuid="u-1",
                identifier="id-1",
                market="KRW-BTC",
                side="bid",
                ord_type="limit",
                state="wait",
                price=100000000.0,
                volume=0.01,
                executed_volume=0.0,
                raw={"uuid": "u-1", "code": "KRW-BTC"},
            ),
            MyAssetEvent(
                ts_ms=1_774_855_601_000,
                currency="KRW",
                balance=1000000.0,
                locked=0.0,
                avg_buy_price=0.0,
                raw={"currency": "KRW", "balance": "1000000"},
            ),
        ]
        self.stats = {"reconnect_count": 0, "last_event_ts_ms": None, "last_event_latency_ms": None}

    async def stream_private(self, *, channels=("myOrder", "myAsset"), duration_sec=None):  # noqa: ANN201
        for event in self._events:
            self.stats["last_event_ts_ms"] = int(event.ts_ms)
            self.stats["last_event_latency_ms"] = 0
            yield event
        await asyncio.sleep(0)


def test_collect_private_ws_daemon_writes_health_report_and_manifest(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw_ws" / "upbit" / "private"
    meta_dir = tmp_path / "raw_ws" / "upbit" / "_meta"

    summary = collect_private_ws_daemon(
        PrivateWsDaemonOptions(
            raw_root=raw_root,
            meta_dir=meta_dir,
            duration_sec=1,
            retention_days=3650,
            rotate_sec=1,
        ),
        ws_client=_FakePrivateWsClient(),
    )

    assert summary.received_myorder == 1
    assert summary.received_myasset == 1
    assert summary.files_written >= 2
    assert summary.collect_report_file.exists()
    assert summary.health_snapshot_file.exists()
    assert summary.manifest_file.exists()

    report = json.loads(summary.collect_report_file.read_text(encoding="utf-8"))
    assert report["received_myorder"] == 1
    assert report["received_myasset"] == 1

    health = json.loads(summary.health_snapshot_file.read_text(encoding="utf-8"))
    assert health["received_events"]["myorder"] == 1
    assert health["received_events"]["myasset"] == 1

    manifest_text = summary.manifest_file.read_bytes()
    assert manifest_text

    myorder_parts = list(raw_root.glob("myorder/date=*/hour=*/part-*.jsonl.zst"))
    myasset_parts = list(raw_root.glob("myasset/date=*/hour=*/part-*.jsonl.zst"))
    assert myorder_parts
    assert myasset_parts
