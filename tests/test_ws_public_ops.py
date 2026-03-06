from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from autobot.data.collect.ws_public_collector import (
    _flush_writer_manifest_state,
    _normalize_keepalive_mode,
    fetch_top_quote_markets,
    load_ws_public_status,
    purge_ws_public_retention,
)
from autobot.data.collect.ws_public_manifest import load_ws_manifest
from autobot.data.collect.ws_public_writer import WsRawRotatingWriter


class _DummySettings:
    websocket = None


class _DummyHttpClient:
    def __init__(self, *_args, **_kwargs) -> None:
        return

    def __enter__(self) -> "_DummyHttpClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return


class _DummyPublicClient:
    def __init__(self, _http_client: _DummyHttpClient) -> None:
        return

    def markets(self, *, is_details: bool = False):
        assert is_details is True
        return [
            {"market": "KRW-BTC"},
            {"market": "KRW-ETH"},
            {"market": "KRW-XRP"},
            {"market": "USDT-BTC"},
        ]

    def ticker(self, markets):
        table = {
            "KRW-BTC": 1000.0,
            "KRW-ETH": 500.0,
            "KRW-XRP": 900.0,
        }
        return [{"market": market, "acc_trade_price_24h": table[market]} for market in markets if market in table]


def test_keepalive_mode_normalization() -> None:
    assert _normalize_keepalive_mode("message") == "message"
    assert _normalize_keepalive_mode("frame") == "frame"
    assert _normalize_keepalive_mode("off") == "off"
    assert _normalize_keepalive_mode("AUTO") == "auto"
    assert _normalize_keepalive_mode("unexpected") == "auto"


def test_purge_ws_public_retention_writes_reports(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw_ws" / "upbit" / "public"
    meta_dir = tmp_path / "raw_ws" / "upbit" / "_meta"

    old_date = (datetime.now(timezone.utc).date() - timedelta(days=10)).isoformat()
    keep_date = datetime.now(timezone.utc).date().isoformat()

    for channel in ("trade", "orderbook"):
        old_dir = raw_root / channel / f"date={old_date}" / "hour=00"
        keep_dir = raw_root / channel / f"date={keep_date}" / "hour=00"
        old_dir.mkdir(parents=True, exist_ok=True)
        keep_dir.mkdir(parents=True, exist_ok=True)
        (old_dir / "part-old.jsonl.zst").write_text("x", encoding="utf-8")
        (keep_dir / "part-keep.jsonl.zst").write_text("x", encoding="utf-8")

    payload = purge_ws_public_retention(
        raw_root=raw_root,
        meta_dir=meta_dir,
        retention_days=3,
    )

    assert old_date in payload["removed"]["trade"]
    assert old_date in payload["removed"]["orderbook"]
    assert (raw_root / "trade" / f"date={old_date}").exists() is False
    assert (raw_root / "orderbook" / f"date={old_date}").exists() is False
    assert (meta_dir / "retention_report.json").exists()
    assert (meta_dir / "ws_purge_report.json").exists()


def test_load_ws_public_status_reads_health_and_latest_run(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw_ws" / "upbit" / "public"
    meta_dir = tmp_path / "raw_ws" / "upbit" / "_meta"
    meta_dir.mkdir(parents=True, exist_ok=True)

    (meta_dir / "ws_public_health.json").write_text(
        json.dumps({"run_id": "r1", "connected": True}),
        encoding="utf-8",
    )
    (meta_dir / "ws_collect_report.json").write_text(
        json.dumps({"run_id": "r1", "written_trade": 10}),
        encoding="utf-8",
    )
    (meta_dir / "ws_runs_summary.json").write_text(
        json.dumps({"runs": [{"run_id": "r0"}, {"run_id": "r1"}]}),
        encoding="utf-8",
    )

    payload = load_ws_public_status(meta_dir=meta_dir, raw_root=raw_root)
    assert payload["health_snapshot"]["run_id"] == "r1"
    assert payload["collect_report"]["run_id"] == "r1"
    assert payload["runs_summary_latest"]["run_id"] == "r1"


def test_fetch_top_quote_markets_uses_ticker_ranking(monkeypatch) -> None:
    import autobot.data.collect.ws_public_collector as wsops

    monkeypatch.setattr(wsops, "load_upbit_settings", lambda _config_dir: _DummySettings())
    monkeypatch.setattr(wsops, "UpbitHttpClient", _DummyHttpClient)
    monkeypatch.setattr(wsops, "UpbitPublicClient", _DummyPublicClient)

    markets = fetch_top_quote_markets(
        config_dir=Path("."),
        quote="KRW",
        top_n=2,
        max_markets=10,
    )
    assert markets == ("KRW-BTC", "KRW-XRP")


def test_flush_writer_manifest_state_updates_manifest_and_runs_summary_incrementally(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw_ws" / "upbit" / "public"
    meta_dir = tmp_path / "raw_ws" / "upbit" / "_meta"
    manifest_path = meta_dir / "ws_manifest.parquet"
    runs_summary_path = meta_dir / "ws_runs_summary.json"

    writer = WsRawRotatingWriter(
        raw_root=raw_root,
        run_id="test-run",
        rotate_sec=3600,
        max_bytes=1024,
    )
    row = {
        "channel": "trade",
        "market": "KRW-BTC",
        "trade_ts_ms": 1_700_000_000_000,
        "recv_ts_ms": 1_700_000_000_010,
        "price": 100.0,
        "volume": 0.1,
        "ask_bid": "BID",
        "source": "ws",
        "collected_at_ms": 1_700_000_000_020,
        "pad": "x" * 1500,
    }

    writer.write(channel="trade", row=row, event_ts_ms=1_700_000_000_000)
    second_row = dict(row)
    second_row["trade_ts_ms"] = 1_700_000_000_100
    writer.write(channel="trade", row=second_row, event_ts_ms=1_700_000_000_100)

    _flush_writer_manifest_state(
        writer=writer,
        manifest_path=manifest_path,
        runs_summary_path=runs_summary_path,
    )
    manifest = load_ws_manifest(manifest_path)
    assert manifest.height == 1
    assert int(manifest.get_column("rows").sum()) == 1

    writer.close()
    _flush_writer_manifest_state(
        writer=writer,
        manifest_path=manifest_path,
        runs_summary_path=runs_summary_path,
    )
    manifest = load_ws_manifest(manifest_path)
    assert manifest.height == 2
    assert int(manifest.get_column("rows").sum()) == 2

    summary = json.loads(runs_summary_path.read_text(encoding="utf-8"))
    assert summary["runs"][-1]["run_id"] == "test-run"
    assert summary["runs"][-1]["parts"] == 2
    assert summary["runs"][-1]["trade_rows"] == 2
