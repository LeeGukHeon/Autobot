"""Raw private websocket collector for Upbit myOrder/myAsset streams."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import shutil
import time
from typing import Any

from ...upbit import UpbitHttpClient, UpbitPrivateClient, load_upbit_settings, require_upbit_credentials
from ...upbit.ws import MyAssetEvent, MyOrderEvent, UpbitWebSocketPrivateClient
from .private_ws_manifest import append_private_ws_manifest_rows
from .private_ws_writer import PrivateWsRawRotatingWriter


@dataclass(frozen=True)
class PrivateWsDaemonOptions:
    raw_root: Path = Path("data/raw_ws/upbit/private")
    meta_dir: Path = Path("data/raw_ws/upbit/_meta")
    duration_sec: int | None = None
    retention_days: int = 30
    rotate_sec: int = 3600
    max_bytes: int = 64 * 1024 * 1024
    health_update_sec: int = 5
    config_dir: Path = Path("config")

    @property
    def collect_report_path(self) -> Path:
        return self.meta_dir / "private_ws_collect_report.json"

    @property
    def manifest_path(self) -> Path:
        return self.meta_dir / "private_ws_manifest.parquet"

    @property
    def runs_summary_path(self) -> Path:
        return self.meta_dir / "private_ws_runs_summary.json"

    @property
    def health_snapshot_path(self) -> Path:
        return self.meta_dir / "private_ws_health.json"

    @property
    def retention_report_path(self) -> Path:
        return self.meta_dir / "private_ws_retention_report.json"


@dataclass(frozen=True)
class PrivateWsDaemonSummary:
    run_id: str
    duration_sec: int
    received_myorder: int
    received_myasset: int
    files_written: int
    bytes_written: int
    reconnect_count: int
    health_snapshot_file: Path
    collect_report_file: Path
    manifest_file: Path


def collect_private_ws_daemon(
    options: PrivateWsDaemonOptions,
    *,
    ws_client: Any | None = None,
) -> PrivateWsDaemonSummary:
    return asyncio.run(_collect_private_ws_daemon_async(options, ws_client=ws_client))


async def _collect_private_ws_daemon_async(
    options: PrivateWsDaemonOptions,
    *,
    ws_client: Any | None = None,
) -> PrivateWsDaemonSummary:
    started_at = int(time.time())
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    bootstrap_rows: list[tuple[str, dict[str, Any], int]] = []
    if ws_client is None:
        settings = load_upbit_settings(options.config_dir)
        credentials = require_upbit_credentials(settings)
        with UpbitHttpClient(settings, credentials=credentials) as private_http:
            private_client = UpbitPrivateClient(private_http)
            bootstrap_rows = _bootstrap_private_rest_rows(private_client=private_client)
        client = UpbitWebSocketPrivateClient(settings.websocket, credentials)
    else:
        client = ws_client
    writer = PrivateWsRawRotatingWriter(
        raw_root=options.raw_root,
        run_id=run_id,
        rotate_sec=max(int(options.rotate_sec), 1),
        max_bytes=max(int(options.max_bytes), 1024),
    )

    received_myorder = 0
    received_myasset = 0
    last_event_ts_ms: int | None = None
    last_event_latency_ms: int | None = None
    connected = False
    next_health_update = time.monotonic()
    stream_duration = (int(options.duration_sec) if options.duration_sec is not None and int(options.duration_sec) > 0 else None)

    def _flush_manifest_state() -> None:
        pending_parts = writer.drain_closed_parts()
        if not pending_parts:
            return
        append_private_ws_manifest_rows(options.manifest_path, pending_parts)
        _update_runs_summary(options.manifest_path, options.runs_summary_path)

    try:
        for channel, row, event_ts_ms in bootstrap_rows:
            writer.write(channel=channel, row=row, event_ts_ms=event_ts_ms)
            if channel == "myorder":
                received_myorder += 1
            else:
                received_myasset += 1
            last_event_ts_ms = int(event_ts_ms)
            last_event_latency_ms = max(int(time.time() * 1000) - int(event_ts_ms), 0)
        if bootstrap_rows:
            writer.close()
            _flush_manifest_state()
            _write_health_snapshot(
                path=options.health_snapshot_path,
                payload=_build_health_payload(
                    run_id=run_id,
                    connected=False,
                    client=client,
                    received_myorder=received_myorder,
                    received_myasset=received_myasset,
                    last_event_ts_ms=last_event_ts_ms,
                    last_event_latency_ms=last_event_latency_ms,
                ),
            )
            _write_collect_report(
                options=options,
                run_id=run_id,
                started_at=started_at,
                finished_at=int(time.time()),
                received_myorder=received_myorder,
                received_myasset=received_myasset,
                writer=writer,
                client=client,
                running=True,
            )
        async for ws_event in client.stream_private(channels=("myOrder", "myAsset"), duration_sec=stream_duration):
            connected = True
            now_ms = int(time.time() * 1000)
            channel = _channel_for_event(ws_event)
            row = _normalize_private_event_row(event=ws_event, collected_at_ms=now_ms)
            writer.write(channel=channel, row=row, event_ts_ms=int(ws_event.ts_ms))
            if channel == "myorder":
                received_myorder += 1
            else:
                received_myasset += 1
            last_event_ts_ms = int(ws_event.ts_ms)
            last_event_latency_ms = max(now_ms - int(ws_event.ts_ms), 0)
            if time.monotonic() >= next_health_update:
                _flush_manifest_state()
                _write_health_snapshot(
                    path=options.health_snapshot_path,
                    payload=_build_health_payload(
                        run_id=run_id,
                        connected=connected,
                        client=client,
                        received_myorder=received_myorder,
                        received_myasset=received_myasset,
                        last_event_ts_ms=last_event_ts_ms,
                        last_event_latency_ms=last_event_latency_ms,
                    ),
                )
                _write_collect_report(
                    options=options,
                    run_id=run_id,
                    started_at=started_at,
                    finished_at=int(time.time()),
                    received_myorder=received_myorder,
                    received_myasset=received_myasset,
                    writer=writer,
                    client=client,
                    running=True,
                )
                next_health_update = time.monotonic() + max(int(options.health_update_sec), 1)
    finally:
        writer.close()
        _flush_manifest_state()
        retention_payload = _purge_retention(raw_root=options.raw_root, retention_days=max(int(options.retention_days), 1))
        options.retention_report_path.parent.mkdir(parents=True, exist_ok=True)
        options.retention_report_path.write_text(
            json.dumps(retention_payload, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        _write_health_snapshot(
            path=options.health_snapshot_path,
            payload=_build_health_payload(
                run_id=run_id,
                connected=connected,
                client=client,
                received_myorder=received_myorder,
                received_myasset=received_myasset,
                last_event_ts_ms=last_event_ts_ms,
                last_event_latency_ms=last_event_latency_ms,
            ),
        )
        _write_collect_report(
            options=options,
            run_id=run_id,
            started_at=started_at,
            finished_at=int(time.time()),
            received_myorder=received_myorder,
            received_myasset=received_myasset,
            writer=writer,
            client=client,
            running=False,
        )

    files_written = len(writer.closed_parts)
    bytes_written = int(sum(int(item.get("bytes", 0)) for item in writer.closed_parts))
    reconnect_count = int((getattr(client, "stats", {}) or {}).get("reconnect_count", 0))
    return PrivateWsDaemonSummary(
        run_id=run_id,
        duration_sec=(stream_duration if stream_duration is not None else 86_400 * 365),
        received_myorder=received_myorder,
        received_myasset=received_myasset,
        files_written=files_written,
        bytes_written=bytes_written,
        reconnect_count=reconnect_count,
        health_snapshot_file=options.health_snapshot_path,
        collect_report_file=options.collect_report_path,
        manifest_file=options.manifest_path,
    )


def _channel_for_event(event: MyOrderEvent | MyAssetEvent) -> str:
    if isinstance(event, MyOrderEvent):
        return "myorder"
    return "myasset"


def _normalize_private_event_row(*, event: MyOrderEvent | MyAssetEvent, collected_at_ms: int) -> dict[str, Any]:
    channel = _channel_for_event(event)
    payload: dict[str, Any] = {
        "channel": channel,
        "stream_type": str(event.stream_type),
        "ts_ms": int(event.ts_ms),
        "collected_at_ms": int(collected_at_ms),
    }
    if isinstance(event, MyOrderEvent):
        payload.update(
            {
                "uuid": event.uuid,
                "identifier": event.identifier,
                "market": event.market,
                "side": event.side,
                "ord_type": event.ord_type,
                "state": event.state,
                "price": event.price,
                "volume": event.volume,
                "executed_volume": event.executed_volume,
            }
        )
    else:
        payload.update(
            {
                "currency": event.currency,
                "balance": event.balance,
                "locked": event.locked,
                "avg_buy_price": event.avg_buy_price,
            }
        )
    if isinstance(event.raw, dict):
        payload["raw"] = dict(event.raw)
    return payload


def _bootstrap_private_rest_rows(*, private_client: UpbitPrivateClient) -> list[tuple[str, dict[str, Any], int]]:
    rows: list[tuple[str, dict[str, Any], int]] = []
    collected_at_ms = int(time.time() * 1000)
    try:
        accounts_payload = private_client.accounts()
    except Exception:
        accounts_payload = []
    if isinstance(accounts_payload, list):
        for item in accounts_payload:
            if not isinstance(item, dict):
                continue
            currency = str(item.get("currency") or "").strip().upper()
            if not currency:
                continue
            rows.append(
                (
                    "myasset",
                    {
                        "channel": "myasset",
                        "stream_type": "BOOTSTRAP",
                        "source": "accounts_rest_bootstrap",
                        "ts_ms": collected_at_ms,
                        "collected_at_ms": collected_at_ms,
                        "currency": currency,
                        "balance": item.get("balance"),
                        "locked": item.get("locked"),
                        "avg_buy_price": item.get("avg_buy_price"),
                        "raw": dict(item),
                    },
                    collected_at_ms,
                )
            )
    try:
        open_orders_payload = private_client.open_orders(states=("wait", "watch"))
    except Exception:
        open_orders_payload = []
    if isinstance(open_orders_payload, list):
        for item in open_orders_payload:
            if not isinstance(item, dict):
                continue
            market = str(item.get("market") or "").strip().upper()
            rows.append(
                (
                    "myorder",
                    {
                        "channel": "myorder",
                        "stream_type": "BOOTSTRAP",
                        "source": "open_orders_rest_bootstrap",
                        "ts_ms": collected_at_ms,
                        "collected_at_ms": collected_at_ms,
                        "uuid": item.get("uuid"),
                        "identifier": item.get("identifier"),
                        "market": market or None,
                        "side": item.get("side"),
                        "ord_type": item.get("ord_type"),
                        "state": item.get("state"),
                        "price": item.get("price"),
                        "volume": item.get("volume"),
                        "executed_volume": item.get("executed_volume"),
                        "raw": dict(item),
                    },
                    collected_at_ms,
                )
            )
    return rows


def _build_health_payload(
    *,
    run_id: str,
    connected: bool,
    client: Any,
    received_myorder: int,
    received_myasset: int,
    last_event_ts_ms: int | None,
    last_event_latency_ms: int | None,
) -> dict[str, Any]:
    stats = dict(getattr(client, "stats", {}) or {})
    return {
        "run_id": run_id,
        "connected": bool(connected),
        "received_events": {
            "myorder": int(received_myorder),
            "myasset": int(received_myasset),
            "total": int(received_myorder + received_myasset),
        },
        "last_event_ts_ms": last_event_ts_ms,
        "last_event_latency_ms": last_event_latency_ms,
        "reconnect_count": int(stats.get("reconnect_count", 0) or 0),
        "updated_at_ms": int(time.time() * 1000),
    }


def _write_collect_report(
    *,
    options: PrivateWsDaemonOptions,
    run_id: str,
    started_at: int,
    finished_at: int,
    received_myorder: int,
    received_myasset: int,
    writer: PrivateWsRawRotatingWriter,
    client: Any,
    running: bool,
) -> None:
    files_written = len(writer.closed_parts)
    bytes_written = int(sum(int(item.get("bytes", 0)) for item in writer.closed_parts))
    stats = dict(getattr(client, "stats", {}) or {})
    payload = {
        "policy": "private_ws_collect_v1",
        "run_id": run_id,
        "started_at": int(started_at),
        "finished_at": int(finished_at),
        "running": bool(running),
        "raw_root": str(options.raw_root),
        "meta_dir": str(options.meta_dir),
        "channels": ["myOrder", "myAsset"],
        "received_myorder": int(received_myorder),
        "received_myasset": int(received_myasset),
        "received_total": int(received_myorder + received_myasset),
        "files_written": int(files_written),
        "bytes_written": int(bytes_written),
        "manifest_file": str(options.manifest_path),
        "health_snapshot_file": str(options.health_snapshot_path),
        "runs_summary_file": str(options.runs_summary_path),
        "reconnect_count": int(stats.get("reconnect_count", 0) or 0),
        "last_event_ts_ms": stats.get("last_event_ts_ms"),
        "last_event_latency_ms": stats.get("last_event_latency_ms"),
    }
    options.collect_report_path.parent.mkdir(parents=True, exist_ok=True)
    options.collect_report_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _write_health_snapshot(*, path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def _update_runs_summary(manifest_path: Path, summary_path: Path) -> None:
    if not manifest_path.exists():
        payload = {"manifest_file": str(manifest_path), "runs": []}
    else:
        import polars as pl

        frame = pl.read_parquet(manifest_path)
        rows = [dict(item) for item in frame.iter_rows(named=True)]
        grouped: dict[str, dict[str, Any]] = {}
        for row in rows:
            run_id = str(row.get("run_id") or "").strip()
            if not run_id:
                continue
            item = grouped.setdefault(
                run_id,
                {
                    "run_id": run_id,
                    "channels": set(),
                    "rows": 0,
                    "bytes": 0,
                    "min_ts_ms": None,
                    "max_ts_ms": None,
                },
            )
            channel = str(row.get("channel") or "").strip().lower()
            if channel:
                item["channels"].add(channel)
            item["rows"] += int(row.get("rows") or 0)
            item["bytes"] += int(row.get("bytes") or 0)
            min_ts_ms = row.get("min_ts_ms")
            max_ts_ms = row.get("max_ts_ms")
            if min_ts_ms is not None:
                item["min_ts_ms"] = int(min(min_ts_ms, item["min_ts_ms"])) if item["min_ts_ms"] is not None else int(min_ts_ms)
            if max_ts_ms is not None:
                item["max_ts_ms"] = int(max(max_ts_ms, item["max_ts_ms"])) if item["max_ts_ms"] is not None else int(max_ts_ms)
        payload = {
            "manifest_file": str(manifest_path),
            "runs": [
                {
                    **{k: v for k, v in item.items() if k != "channels"},
                    "channels": sorted(item["channels"]),
                }
                for _, item in sorted(grouped.items())
            ],
        }
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _purge_retention(*, raw_root: Path, retention_days: int) -> dict[str, Any]:
    if not raw_root.exists():
        return {"raw_root": str(raw_root), "removed_dates": []}
    cutoff = datetime.now(timezone.utc).date() - timedelta(days=max(int(retention_days), 1))
    removed: list[str] = []
    for channel_dir in raw_root.iterdir():
        if not channel_dir.is_dir():
            continue
        for date_dir in sorted(channel_dir.glob("date=*")):
            date_text = date_dir.name.replace("date=", "", 1).strip()
            try:
                parsed = datetime.fromisoformat(date_text).date()
            except ValueError:
                continue
            if parsed < cutoff:
                shutil.rmtree(date_dir, ignore_errors=True)
                removed.append(f"{channel_dir.name}/{date_text}")
    return {"raw_root": str(raw_root), "removed_dates": removed}
