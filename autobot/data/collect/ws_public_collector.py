"""Plan-driven collector for Upbit public websocket trade/orderbook data."""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
import random
import shutil
import time
from typing import Any, Callable

import websockets

from ...upbit import UpbitHttpClient, UpbitPublicClient, load_upbit_settings
from ...upbit.ws.ws_rate_limiter import WebSocketRateLimiter
from .ws_public_checkpoint import load_ws_checkpoint, save_ws_checkpoint, update_ws_checkpoint
from .ws_public_manifest import append_ws_manifest_rows, load_ws_manifest
from .ws_public_writer import WsRawRotatingWriter


VALID_WS_PUBLIC_CHANNELS: set[str] = {"trade", "orderbook"}
VALID_KEEPALIVE_MODES: set[str] = {"message", "frame", "auto", "off"}


@dataclass(frozen=True)
class WsPublicCollectOptions:
    plan_path: Path = Path("data/raw_ws/upbit/_meta/ws_public_plan.json")
    raw_root: Path = Path("data/raw_ws/upbit/public")
    meta_dir: Path = Path("data/raw_ws/upbit/_meta")
    duration_sec: int = 120
    rotate_sec: int = 300
    max_bytes: int = 64 * 1024 * 1024
    retention_days: int = 7
    rate_limit_strict: bool = True
    reconnect_max_per_min: int = 3
    orderbook_spread_bps_threshold: float = 0.5
    orderbook_top1_size_change_threshold: float = 0.2
    keepalive_mode: str = "auto"
    keepalive_interval_sec: int = 60
    keepalive_stale_sec: int = 120
    health_update_sec: int = 5
    config_dir: Path = Path("config")

    @property
    def collect_report_path(self) -> Path:
        return self.meta_dir / "ws_collect_report.json"

    @property
    def validate_report_path(self) -> Path:
        return self.meta_dir / "ws_validate_report.json"

    @property
    def manifest_path(self) -> Path:
        return self.meta_dir / "ws_manifest.parquet"

    @property
    def checkpoint_path(self) -> Path:
        return self.meta_dir / "ws_checkpoint.json"

    @property
    def runs_summary_path(self) -> Path:
        return self.meta_dir / "ws_runs_summary.json"

    @property
    def retention_report_path(self) -> Path:
        return self.meta_dir / "retention_report.json"

    @property
    def health_snapshot_path(self) -> Path:
        return self.meta_dir / "ws_public_health.json"


@dataclass(frozen=True)
class WsPublicDaemonOptions:
    raw_root: Path = Path("data/raw_ws/upbit/public")
    meta_dir: Path = Path("data/raw_ws/upbit/_meta")
    quote: str = "KRW"
    top_n: int = 50
    refresh_sec: int = 900
    duration_sec: int | None = None
    retention_days: int = 30
    downsample_hz: float = 1.0
    max_markets: int = 60
    format: str = "DEFAULT"
    channels: tuple[str, ...] = ("trade", "orderbook")
    orderbook_topk: int = 30
    orderbook_level: int | str | None = 0
    keepalive_mode: str = "message"
    keepalive_interval_sec: int = 55
    keepalive_stale_sec: int = 120
    rotate_sec: int = 3600
    max_bytes: int = 64 * 1024 * 1024
    rate_limit_strict: bool = True
    reconnect_max_per_min: int = 3
    max_subscribe_messages_per_min: int = 100
    min_subscribe_interval_sec: int = 60
    orderbook_spread_bps_threshold: float = 0.5
    orderbook_top1_size_change_threshold: float = 0.2
    health_update_sec: int = 5
    config_dir: Path = Path("config")

    @property
    def plan_path(self) -> Path:
        return self.meta_dir / "ws_public_plan.json"

    @property
    def collect_report_path(self) -> Path:
        return self.meta_dir / "ws_collect_report.json"

    @property
    def manifest_path(self) -> Path:
        return self.meta_dir / "ws_manifest.parquet"

    @property
    def checkpoint_path(self) -> Path:
        return self.meta_dir / "ws_checkpoint.json"

    @property
    def runs_summary_path(self) -> Path:
        return self.meta_dir / "ws_runs_summary.json"

    @property
    def retention_report_path(self) -> Path:
        return self.meta_dir / "retention_report.json"

    @property
    def purge_report_path(self) -> Path:
        return self.meta_dir / "ws_purge_report.json"

    @property
    def health_snapshot_path(self) -> Path:
        return self.meta_dir / "ws_public_health.json"


@dataclass(frozen=True)
class WsPublicCollectSummary:
    run_id: str
    duration_sec: int
    codes_count: int
    received_trade: int
    received_orderbook: int
    written_trade: int
    written_orderbook: int
    dropped_orderbook_by_interval: int
    dropped_by_parse_error: int
    reconnect_count: int
    ping_sent_count: int
    pong_rx_count: int
    files_written: int
    bytes_written: int
    collect_report_file: Path
    manifest_file: Path
    checkpoint_file: Path
    runs_summary_file: Path
    details: tuple[dict[str, Any], ...]
    failures: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class WsPublicDaemonSummary:
    run_id: str
    duration_sec: int
    quote: str
    top_n: int
    refresh_sec: int
    subscribed_markets_count: int
    received_trade: int
    received_orderbook: int
    written_trade: int
    written_orderbook: int
    dropped_orderbook_by_interval: int
    dropped_by_parse_error: int
    reconnect_count: int
    ping_sent_count: int
    pong_rx_count: int
    status_up_count: int
    refresh_attempt_count: int
    refresh_applied_count: int
    refresh_noop_count: int
    files_written: int
    bytes_written: int
    collect_report_file: Path
    plan_file: Path
    manifest_file: Path
    checkpoint_file: Path
    runs_summary_file: Path
    health_snapshot_file: Path
    details: tuple[dict[str, Any], ...]
    failures: tuple[dict[str, Any], ...]


def _build_ws_public_daemon_collect_report(
    *,
    started_at: int,
    finished_at: int,
    run_id: str,
    options: WsPublicDaemonOptions,
    channels: tuple[str, ...],
    fmt: str,
    top_n: int,
    max_markets: int,
    refresh_sec: int,
    runtime_counters: _RuntimeCounters,
    files_written: int,
    bytes_written: int,
    retention_payload: dict[str, Any] | None,
    details: list[dict[str, Any]],
    failures: list[dict[str, Any]],
    running: bool,
) -> dict[str, Any]:
    return {
        "started_at": int(started_at),
        "finished_at": int(finished_at),
        "run_id": run_id,
        "mode": "daemon",
        "running": bool(running),
        "duration_sec": int(options.duration_sec) if options.duration_sec is not None else 86_400 * 365,
        "quote": str(options.quote).strip().upper() or "KRW",
        "top_n": int(top_n),
        "max_markets": int(max_markets),
        "refresh_sec": int(refresh_sec),
        "plan_file": str(options.plan_path),
        "raw_root": str(options.raw_root),
        "meta_dir": str(options.meta_dir),
        "codes_count": int(runtime_counters.subscribed_markets_count),
        "channels": list(channels),
        "format": fmt,
        "received_trade": int(runtime_counters.received_trade),
        "received_orderbook": int(runtime_counters.received_orderbook),
        "written_trade": int(runtime_counters.written_trade),
        "written_orderbook": int(runtime_counters.written_orderbook),
        "dropped_orderbook_by_interval": int(runtime_counters.dropped_orderbook_by_interval),
        "dropped_by_parse_error": int(runtime_counters.dropped_by_parse_error),
        "reconnect_count": int(runtime_counters.reconnect_count),
        "ping_sent_count": int(runtime_counters.ping_sent_count),
        "pong_rx_count": int(runtime_counters.pong_rx_count),
        "status_up_count": int(runtime_counters.status_up_count),
        "refresh_attempt_count": int(runtime_counters.refresh_attempt_count),
        "refresh_applied_count": int(runtime_counters.refresh_applied_count),
        "refresh_noop_count": int(runtime_counters.refresh_noop_count),
        "files_written": int(files_written),
        "bytes_written": int(bytes_written),
        "fatal_reason": runtime_counters.fatal_reason,
        "manifest_file": str(options.manifest_path),
        "checkpoint_file": str(options.checkpoint_path),
        "runs_summary_file": str(options.runs_summary_path),
        "health_snapshot_file": str(options.health_snapshot_path),
        "retention_report": dict(retention_payload or {}),
        "details": list(details),
        "failures": list(failures),
    }


@dataclass
class _RuntimeCounters:
    received_trade: int = 0
    received_orderbook: int = 0
    written_trade: int = 0
    written_orderbook: int = 0
    dropped_orderbook_by_interval: int = 0
    dropped_by_parse_error: int = 0
    reconnect_count: int = 0
    ping_sent_count: int = 0
    pong_rx_count: int = 0
    status_up_count: int = 0
    subscribe_messages_sent: int = 0
    connection_open_count: int = 0
    connection_failure_count: int = 0
    refresh_attempt_count: int = 0
    refresh_applied_count: int = 0
    refresh_noop_count: int = 0
    last_trade_rx_ts_ms: int | None = None
    last_orderbook_rx_ts_ms: int | None = None
    subscribed_markets_count: int = 0
    fatal_reason: str | None = None


def collect_ws_public_from_plan(options: WsPublicCollectOptions) -> WsPublicCollectSummary:
    started_at = int(time.time())
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    plan = _load_plan(options.plan_path)

    settings = load_upbit_settings(options.config_dir)
    ws_settings = settings.websocket

    runtime_policy = plan.get("runtime_policy", {}) if isinstance(plan.get("runtime_policy"), dict) else {}
    safety_policy = plan.get("safety", {}) if isinstance(plan.get("safety"), dict) else {}
    channels = _normalize_channels(plan.get("filters", {}).get("channels"))
    codes = _normalize_codes(plan.get("codes") or plan.get("selected_markets"))
    if not channels:
        raise ValueError("plan.filters.channels is required")
    if not codes:
        raise ValueError("plan.codes is required")

    fmt = str(runtime_policy.get("format", "DEFAULT")).strip().upper() or "DEFAULT"
    orderbook_topk = max(int(runtime_policy.get("orderbook_topk", 30)), 1)
    orderbook_level = runtime_policy.get("orderbook_level", 0)
    orderbook_min_write_interval_ms = max(int(runtime_policy.get("orderbook_min_write_interval_ms", 200)), 1)
    max_subscribe_messages_per_min = max(int(safety_policy.get("max_subscribe_messages_per_min", 5)), 1)

    writer = WsRawRotatingWriter(
        raw_root=options.raw_root,
        run_id=run_id,
        rotate_sec=options.rotate_sec,
        max_bytes=options.max_bytes,
    )

    def _flush_manifest_state() -> None:
        _flush_writer_manifest_state(
            writer=writer,
            manifest_path=options.manifest_path,
            runs_summary_path=options.runs_summary_path,
        )

    counters = _RuntimeCounters()
    details: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    try:
        counters = asyncio.run(
            _run_ws_collection(
                ws_url=ws_settings.public_url,
                channels=channels,
                codes=codes,
                fmt=fmt,
                duration_sec=max(int(options.duration_sec), 1),
                writer=writer,
                orderbook_topk=orderbook_topk,
                orderbook_level=orderbook_level,
                orderbook_min_write_interval_ms=orderbook_min_write_interval_ms,
                orderbook_spread_bps_threshold=float(options.orderbook_spread_bps_threshold),
                orderbook_top1_size_change_threshold=float(options.orderbook_top1_size_change_threshold),
                connect_rps=int(ws_settings.ratelimit.connect_rps),
                message_rps=int(ws_settings.ratelimit.message_rps),
                message_rpm=int(ws_settings.ratelimit.message_rpm),
                reconnect_max_per_min=max(int(options.reconnect_max_per_min), 1),
                max_subscribe_messages_per_min=max_subscribe_messages_per_min,
                rate_limit_strict=bool(options.rate_limit_strict),
                refresh_sec=None,
                market_resolver=None,
                min_subscribe_interval_sec=60,
                keepalive_mode=str(options.keepalive_mode),
                keepalive_interval_sec=max(int(options.keepalive_interval_sec), 1),
                keepalive_stale_sec=max(int(options.keepalive_stale_sec), 30),
                health_snapshot_path=options.health_snapshot_path,
                health_update_sec=max(int(options.health_update_sec), 1),
                manifest_flush_callback=_flush_manifest_state,
                run_id=run_id,
            )
        )
    except Exception as exc:
        failures.append({"reason": "RUNTIME_EXCEPTION", "error_message": str(exc)})
        counters.fatal_reason = counters.fatal_reason or "RUNTIME_EXCEPTION"

    closed_parts = writer.close()
    _flush_manifest_state()

    files_written = len(closed_parts)
    bytes_written = int(sum(int(item.get("bytes", 0)) for item in closed_parts))

    retention_payload = purge_ws_public_retention(
        raw_root=options.raw_root,
        meta_dir=options.meta_dir,
        retention_days=max(int(options.retention_days), 1),
    )

    checkpoint = load_ws_checkpoint(options.checkpoint_path)
    update_ws_checkpoint(
        checkpoint,
        run_id=run_id,
        reconnect_count=int(counters.reconnect_count),
        ping_sent_count=int(counters.ping_sent_count),
        pong_rx_count=int(counters.pong_rx_count),
        files_written=files_written,
        bytes_written=bytes_written,
        updated_at_ms=int(time.time() * 1000),
    )
    save_ws_checkpoint(options.checkpoint_path, checkpoint)

    if counters.fatal_reason:
        failures.append({"reason": counters.fatal_reason})

    details.append(
        {
            "connection_open_count": int(counters.connection_open_count),
            "connection_failure_count": int(counters.connection_failure_count),
            "subscribe_messages_sent": int(counters.subscribe_messages_sent),
            "format": fmt,
            "channels": list(channels),
            "codes_count": len(codes),
            "orderbook_topk": int(orderbook_topk),
            "orderbook_level": orderbook_level,
            "orderbook_min_write_interval_ms": int(orderbook_min_write_interval_ms),
            "keepalive_mode": _normalize_keepalive_mode(options.keepalive_mode),
            "keepalive_interval_sec": max(int(options.keepalive_interval_sec), 1),
            "keepalive_stale_sec": max(int(options.keepalive_stale_sec), 30),
        }
    )

    collect_report = {
        "started_at": started_at,
        "finished_at": int(time.time()),
        "run_id": run_id,
        "duration_sec": max(int(options.duration_sec), 1),
        "plan_file": str(options.plan_path),
        "raw_root": str(options.raw_root),
        "meta_dir": str(options.meta_dir),
        "codes_count": len(codes),
        "channels": list(channels),
        "format": fmt,
        "received_trade": int(counters.received_trade),
        "received_orderbook": int(counters.received_orderbook),
        "written_trade": int(counters.written_trade),
        "written_orderbook": int(counters.written_orderbook),
        "dropped_orderbook_by_interval": int(counters.dropped_orderbook_by_interval),
        "dropped_by_parse_error": int(counters.dropped_by_parse_error),
        "reconnect_count": int(counters.reconnect_count),
        "ping_sent_count": int(counters.ping_sent_count),
        "pong_rx_count": int(counters.pong_rx_count),
        "status_up_count": int(counters.status_up_count),
        "files_written": int(files_written),
        "bytes_written": int(bytes_written),
        "fatal_reason": counters.fatal_reason,
        "manifest_file": str(options.manifest_path),
        "checkpoint_file": str(options.checkpoint_path),
        "runs_summary_file": str(options.runs_summary_path),
        "health_snapshot_file": str(options.health_snapshot_path),
        "retention_report": retention_payload,
        "details": details,
        "failures": failures,
    }
    options.collect_report_path.parent.mkdir(parents=True, exist_ok=True)
    options.collect_report_path.write_text(
        json.dumps(collect_report, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    return WsPublicCollectSummary(
        run_id=run_id,
        duration_sec=max(int(options.duration_sec), 1),
        codes_count=len(codes),
        received_trade=int(counters.received_trade),
        received_orderbook=int(counters.received_orderbook),
        written_trade=int(counters.written_trade),
        written_orderbook=int(counters.written_orderbook),
        dropped_orderbook_by_interval=int(counters.dropped_orderbook_by_interval),
        dropped_by_parse_error=int(counters.dropped_by_parse_error),
        reconnect_count=int(counters.reconnect_count),
        ping_sent_count=int(counters.ping_sent_count),
        pong_rx_count=int(counters.pong_rx_count),
        files_written=files_written,
        bytes_written=bytes_written,
        collect_report_file=options.collect_report_path,
        manifest_file=options.manifest_path,
        checkpoint_file=options.checkpoint_path,
        runs_summary_file=options.runs_summary_path,
        details=tuple(details),
        failures=tuple(failures),
    )


def collect_ws_public_daemon(options: WsPublicDaemonOptions) -> WsPublicDaemonSummary:
    started_at = int(time.time())
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    quote = str(options.quote).strip().upper() or "KRW"
    channels = _normalize_channels(options.channels) or ("trade", "orderbook")
    fmt = str(options.format).strip().upper() or "DEFAULT"
    top_n = max(int(options.top_n), 1)
    max_markets = max(int(options.max_markets), 1)
    refresh_sec = max(int(options.refresh_sec), 30)

    settings = load_upbit_settings(options.config_dir)
    ws_settings = settings.websocket

    initial_codes = fetch_top_quote_markets(
        config_dir=options.config_dir,
        quote=quote,
        top_n=top_n,
        max_markets=max_markets,
    )
    if not initial_codes:
        raise RuntimeError(f"No markets found for quote={quote}")

    downsample_hz = max(float(options.downsample_hz), 0.1)
    orderbook_min_write_interval_ms = max(int(round(1000.0 / downsample_hz)), 1)
    plan_payload = _build_ws_runtime_plan_payload(
        quote=quote,
        top_n=top_n,
        max_markets=max_markets,
        channels=channels,
        fmt=fmt,
        orderbook_topk=max(int(options.orderbook_topk), 1),
        orderbook_level=options.orderbook_level,
        orderbook_min_write_interval_ms=orderbook_min_write_interval_ms,
        keepalive_mode=str(options.keepalive_mode),
        keepalive_interval_sec=max(int(options.keepalive_interval_sec), 1),
        keepalive_stale_sec=max(int(options.keepalive_stale_sec), 30),
        codes=initial_codes,
    )
    options.plan_path.parent.mkdir(parents=True, exist_ok=True)
    options.plan_path.write_text(
        json.dumps(plan_payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    purge_ws_public_retention(
        raw_root=options.raw_root,
        meta_dir=options.meta_dir,
        retention_days=max(int(options.retention_days), 1),
    )

    writer = WsRawRotatingWriter(
        raw_root=options.raw_root,
        run_id=run_id,
        rotate_sec=max(int(options.rotate_sec), 1),
        max_bytes=max(int(options.max_bytes), 1024),
    )

    def _flush_manifest_state() -> None:
        _flush_writer_manifest_state(
            writer=writer,
            manifest_path=options.manifest_path,
            runs_summary_path=options.runs_summary_path,
        )

    failures: list[dict[str, Any]] = []
    details: list[dict[str, Any]] = []
    runtime_counters = _RuntimeCounters(subscribed_markets_count=len(initial_codes))

    refresh_state: dict[str, Any] = {
        "codes": tuple(initial_codes),
        "last_refresh_at_ms": int(time.time() * 1000),
    }

    def _report_details(*, retention_doc: dict[str, Any] | None) -> list[dict[str, Any]]:
        return [
            {
                "quote": quote,
                "top_n": top_n,
                "max_markets": max_markets,
                "refresh_sec": refresh_sec,
                "channels": list(channels),
                "format": fmt,
                "orderbook_topk": int(options.orderbook_topk),
                "orderbook_level": options.orderbook_level,
                "downsample_hz": downsample_hz,
                "orderbook_min_write_interval_ms": orderbook_min_write_interval_ms,
                "keepalive_mode": _normalize_keepalive_mode(options.keepalive_mode),
                "keepalive_interval_sec": max(int(options.keepalive_interval_sec), 1),
                "keepalive_stale_sec": max(int(options.keepalive_stale_sec), 30),
                "retention_days": max(int(options.retention_days), 1),
                "refresh_last_codes_count": len(tuple(refresh_state.get("codes") or ())),
                "retention_report": dict(retention_doc or {}),
            }
        ]

    def _write_runtime_collect_report(*, connected: bool, retention_doc: dict[str, Any] | None = None) -> None:
        files_written_live = len(writer.closed_parts)
        bytes_written_live = int(sum(int(item.get("bytes", 0)) for item in writer.closed_parts))
        report = _build_ws_public_daemon_collect_report(
            started_at=started_at,
            finished_at=int(time.time()),
            run_id=run_id,
            options=options,
            channels=channels,
            fmt=fmt,
            top_n=top_n,
            max_markets=max_markets,
            refresh_sec=refresh_sec,
            runtime_counters=runtime_counters,
            files_written=files_written_live,
            bytes_written=bytes_written_live,
            retention_payload=retention_doc,
            details=_report_details(retention_doc=retention_doc),
            failures=failures,
            running=connected and not bool(runtime_counters.fatal_reason),
        )
        options.collect_report_path.parent.mkdir(parents=True, exist_ok=True)
        options.collect_report_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    def _resolve_codes() -> tuple[str, ...]:
        latest = fetch_top_quote_markets(
            config_dir=options.config_dir,
            quote=quote,
            top_n=top_n,
            max_markets=max_markets,
        )
        if latest:
            refresh_state["codes"] = tuple(latest)
            refresh_state["last_refresh_at_ms"] = int(time.time() * 1000)
            plan_payload["codes"] = list(latest)
            plan_payload["selected_markets"] = list(latest)
            plan_payload["generated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
            options.plan_path.write_text(
                json.dumps(plan_payload, ensure_ascii=False, indent=2, sort_keys=True),
                encoding="utf-8",
            )
            return tuple(latest)
        return tuple(refresh_state.get("codes") or ())

    duration_sec = int(options.duration_sec) if options.duration_sec is not None else 86_400 * 365
    duration_sec = max(duration_sec, 1)

    try:
        runtime_counters = asyncio.run(
            _run_ws_collection(
                ws_url=ws_settings.public_url,
                channels=channels,
                codes=tuple(initial_codes),
                fmt=fmt,
                duration_sec=duration_sec,
                writer=writer,
                orderbook_topk=max(int(options.orderbook_topk), 1),
                orderbook_level=options.orderbook_level,
                orderbook_min_write_interval_ms=orderbook_min_write_interval_ms,
                orderbook_spread_bps_threshold=float(options.orderbook_spread_bps_threshold),
                orderbook_top1_size_change_threshold=float(options.orderbook_top1_size_change_threshold),
                connect_rps=int(ws_settings.ratelimit.connect_rps),
                message_rps=int(ws_settings.ratelimit.message_rps),
                message_rpm=int(ws_settings.ratelimit.message_rpm),
                reconnect_max_per_min=max(int(options.reconnect_max_per_min), 1),
                max_subscribe_messages_per_min=max(int(options.max_subscribe_messages_per_min), 1),
                rate_limit_strict=bool(options.rate_limit_strict),
                refresh_sec=refresh_sec,
                market_resolver=_resolve_codes,
                min_subscribe_interval_sec=max(int(options.min_subscribe_interval_sec), 1),
                keepalive_mode=str(options.keepalive_mode),
                keepalive_interval_sec=max(int(options.keepalive_interval_sec), 1),
                keepalive_stale_sec=max(int(options.keepalive_stale_sec), 30),
                health_snapshot_path=options.health_snapshot_path,
                health_update_sec=max(int(options.health_update_sec), 1),
                manifest_flush_callback=_flush_manifest_state,
                progress_snapshot_callback=lambda counters, connected: _write_runtime_collect_report(
                    connected=connected,
                    retention_doc=None,
                ),
                run_id=run_id,
            )
        )
    except Exception as exc:
        failures.append({"reason": "RUNTIME_EXCEPTION", "error_message": str(exc)})
        runtime_counters.fatal_reason = runtime_counters.fatal_reason or "RUNTIME_EXCEPTION"

    closed_parts = writer.close()
    _flush_manifest_state()

    files_written = len(closed_parts)
    bytes_written = int(sum(int(item.get("bytes", 0)) for item in closed_parts))
    retention_payload = purge_ws_public_retention(
        raw_root=options.raw_root,
        meta_dir=options.meta_dir,
        retention_days=max(int(options.retention_days), 1),
    )

    checkpoint = load_ws_checkpoint(options.checkpoint_path)
    update_ws_checkpoint(
        checkpoint,
        run_id=run_id,
        reconnect_count=int(runtime_counters.reconnect_count),
        ping_sent_count=int(runtime_counters.ping_sent_count),
        pong_rx_count=int(runtime_counters.pong_rx_count),
        files_written=files_written,
        bytes_written=bytes_written,
        updated_at_ms=int(time.time() * 1000),
    )
    save_ws_checkpoint(options.checkpoint_path, checkpoint)

    if runtime_counters.fatal_reason:
        failures.append({"reason": runtime_counters.fatal_reason})

    details.extend(_report_details(retention_doc=retention_payload))
    _write_runtime_collect_report(connected=False, retention_doc=retention_payload)

    _write_health_snapshot(
        path=options.health_snapshot_path,
        payload=_health_payload(run_id=run_id, counters=runtime_counters, connected=False),
    )

    return WsPublicDaemonSummary(
        run_id=run_id,
        duration_sec=duration_sec,
        quote=quote,
        top_n=top_n,
        refresh_sec=refresh_sec,
        subscribed_markets_count=int(runtime_counters.subscribed_markets_count),
        received_trade=int(runtime_counters.received_trade),
        received_orderbook=int(runtime_counters.received_orderbook),
        written_trade=int(runtime_counters.written_trade),
        written_orderbook=int(runtime_counters.written_orderbook),
        dropped_orderbook_by_interval=int(runtime_counters.dropped_orderbook_by_interval),
        dropped_by_parse_error=int(runtime_counters.dropped_by_parse_error),
        reconnect_count=int(runtime_counters.reconnect_count),
        ping_sent_count=int(runtime_counters.ping_sent_count),
        pong_rx_count=int(runtime_counters.pong_rx_count),
        status_up_count=int(runtime_counters.status_up_count),
        refresh_attempt_count=int(runtime_counters.refresh_attempt_count),
        refresh_applied_count=int(runtime_counters.refresh_applied_count),
        refresh_noop_count=int(runtime_counters.refresh_noop_count),
        files_written=files_written,
        bytes_written=bytes_written,
        collect_report_file=options.collect_report_path,
        plan_file=options.plan_path,
        manifest_file=options.manifest_path,
        checkpoint_file=options.checkpoint_path,
        runs_summary_file=options.runs_summary_path,
        health_snapshot_file=options.health_snapshot_path,
        details=tuple(details),
        failures=tuple(failures),
    )


def fetch_top_quote_markets(
    *,
    config_dir: Path,
    quote: str,
    top_n: int,
    max_markets: int,
) -> tuple[str, ...]:
    if UpbitHttpClient is None or UpbitPublicClient is None:
        raise RuntimeError("Upbit REST runtime is not available")

    quote_value = str(quote).strip().upper() or "KRW"
    quote_prefix = f"{quote_value}-"
    selected_size = min(max(int(top_n), 1), max(int(max_markets), 1))
    settings = load_upbit_settings(config_dir)

    with UpbitHttpClient(settings) as http_client:
        payload = UpbitPublicClient(http_client).markets(is_details=True)
        market_rows = payload if isinstance(payload, list) else []
        markets: list[str] = []
        seen: set[str] = set()
        for item in market_rows:
            if not isinstance(item, dict):
                continue
            market = str(item.get("market", "")).strip().upper()
            if not market.startswith(quote_prefix):
                continue
            if market in seen:
                continue
            seen.add(market)
            markets.append(market)
        if not markets:
            return ()

        by_value: list[tuple[str, float]] = []
        for chunk in _chunk_codes(markets, size=100):
            ticker_payload = UpbitPublicClient(http_client).ticker(chunk)
            ticker_rows = ticker_payload if isinstance(ticker_payload, list) else []
            for row in ticker_rows:
                if not isinstance(row, dict):
                    continue
                market = str(row.get("market", "")).strip().upper()
                if not market.startswith(quote_prefix):
                    continue
                acc_value = _to_float(row.get("acc_trade_price_24h")) or 0.0
                by_value.append((market, float(acc_value)))

    ranked = sorted(by_value, key=lambda item: (-float(item[1]), item[0]))
    if ranked:
        picked: list[str] = []
        seen_ranked: set[str] = set()
        for market, _ in ranked:
            if market in seen_ranked:
                continue
            seen_ranked.add(market)
            picked.append(market)
            if len(picked) >= selected_size:
                break
        return tuple(picked)

    fallback = [market for market in markets if market.startswith(quote_prefix)]
    return tuple(fallback[:selected_size])


def purge_ws_public_retention(
    *,
    raw_root: Path,
    meta_dir: Path,
    retention_days: int,
) -> dict[str, Any]:
    removed = {}
    if int(retention_days) > 0:
        removed = _prune_ws_retention(raw_root=raw_root, retention_days=max(int(retention_days), 1))
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "raw_root": str(raw_root),
        "retention_days": max(int(retention_days), 1),
        "removed": removed or {"trade": [], "orderbook": []},
        "removed_counts": {
            "trade": len((removed or {}).get("trade", [])),
            "orderbook": len((removed or {}).get("orderbook", [])),
        },
    }
    retention_path = meta_dir / "retention_report.json"
    purge_path = meta_dir / "ws_purge_report.json"
    retention_path.parent.mkdir(parents=True, exist_ok=True)
    retention_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    purge_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return payload


def load_ws_public_status(*, meta_dir: Path, raw_root: Path) -> dict[str, Any]:
    health = _load_json(meta_dir / "ws_public_health.json")
    collect_report = _load_json(meta_dir / "ws_collect_report.json")
    runs_summary = _load_json(meta_dir / "ws_runs_summary.json")
    latest_run = None
    runs = runs_summary.get("runs") if isinstance(runs_summary, dict) else None
    if isinstance(runs, list) and runs:
        candidate = runs[-1]
        latest_run = candidate if isinstance(candidate, dict) else None

    return {
        "meta_dir": str(meta_dir),
        "raw_root": str(raw_root),
        "health_snapshot": health,
        "collect_report": collect_report,
        "runs_summary_latest": latest_run,
    }


async def _run_ws_collection(
    *,
    ws_url: str,
    channels: tuple[str, ...],
    codes: tuple[str, ...],
    fmt: str,
    duration_sec: int,
    writer: WsRawRotatingWriter,
    orderbook_topk: int,
    orderbook_level: Any,
    orderbook_min_write_interval_ms: int,
    orderbook_spread_bps_threshold: float,
    orderbook_top1_size_change_threshold: float,
    connect_rps: int,
    message_rps: int,
    message_rpm: int,
    reconnect_max_per_min: int,
    max_subscribe_messages_per_min: int,
    rate_limit_strict: bool,
    refresh_sec: int | None,
    market_resolver: Callable[[], tuple[str, ...]] | None,
    min_subscribe_interval_sec: int,
    keepalive_mode: str,
    keepalive_interval_sec: int,
    keepalive_stale_sec: int,
    health_snapshot_path: Path | None,
    health_update_sec: int,
    manifest_flush_callback: Callable[[], None] | None,
    progress_snapshot_callback: Callable[[_RuntimeCounters, bool], None] | None,
    run_id: str,
) -> _RuntimeCounters:
    counters = _RuntimeCounters()
    keepalive_mode_value = _normalize_keepalive_mode(keepalive_mode)
    connect_limiter = WebSocketRateLimiter(per_second=max(int(connect_rps), 1))
    send_limiter = WebSocketRateLimiter(
        per_second=max(int(message_rps), 1),
        per_minute=max(int(message_rpm), 1),
    )
    rng = random.Random()

    deadline = time.monotonic() + float(duration_sec)
    reconnect_attempt = 0
    reconnect_window: deque[float] = deque()
    subscribe_window: deque[float] = deque()
    orderbook_state: dict[str, dict[str, float]] = {}
    active_codes = tuple(codes)
    active_codes_set = set(active_codes)
    counters.subscribed_markets_count = len(active_codes)
    refresh_period_sec = max(int(refresh_sec), 1) if refresh_sec is not None else None
    next_refresh_monotonic = (time.monotonic() + float(refresh_period_sec)) if refresh_period_sec else None
    min_subscribe_gap_sec = max(int(min_subscribe_interval_sec), 1)
    keepalive_interval = max(int(keepalive_interval_sec), 1)
    keepalive_stale_limit = max(int(keepalive_stale_sec), keepalive_interval + 1)
    next_health_update_monotonic = time.monotonic()
    health_interval_sec = max(int(health_update_sec), 1)
    last_subscribe_sent_monotonic = 0.0
    connected = False

    def _emit_runtime_snapshots(*, force: bool = False) -> None:
        nonlocal next_health_update_monotonic
        now_monotonic = time.monotonic()
        if (not force) and now_monotonic < next_health_update_monotonic:
            return
        _write_health_snapshot(
            path=health_snapshot_path,
            payload=_health_payload(run_id=run_id, counters=counters, connected=connected),
        )
        if progress_snapshot_callback is not None:
            progress_snapshot_callback(counters, connected)
        next_health_update_monotonic = now_monotonic + float(health_interval_sec)

    while time.monotonic() < deadline:
        _emit_runtime_snapshots()

        await connect_limiter.acquire()
        payload = _build_public_subscribe_payload(
            channels=channels,
            codes=active_codes,
            fmt=fmt,
            orderbook_level=orderbook_level,
            ticket=f"autobot-ws-public-{int(time.time() * 1000)}",
        )

        try:
            async with _connect_public_websocket(ws_url) as websocket:
                counters.connection_open_count += 1
                reconnect_attempt = 0
                connected = True

                now_stamp = time.monotonic()
                while subscribe_window and subscribe_window[0] <= now_stamp - 60.0:
                    subscribe_window.popleft()
                if len(subscribe_window) >= max_subscribe_messages_per_min:
                    if rate_limit_strict:
                        counters.fatal_reason = "MAX_SUBSCRIBE_MESSAGES_PER_MIN_REACHED"
                        connected = False
                        if progress_snapshot_callback is not None:
                            progress_snapshot_callback(counters, connected)
                        return counters
                    await asyncio.sleep(1.0)
                await _send_json_with_limit(websocket, payload, send_limiter)
                subscribe_window.append(time.monotonic())
                counters.subscribe_messages_sent += 1
                last_subscribe_sent_monotonic = time.monotonic()
                _emit_runtime_snapshots(force=True)

                last_recv_monotonic = time.monotonic()
                last_text_ping_monotonic = 0.0
                while time.monotonic() < deadline:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    _emit_runtime_snapshots()

                    if next_refresh_monotonic is not None and time.monotonic() >= next_refresh_monotonic:
                        next_refresh_monotonic = time.monotonic() + float(refresh_period_sec or 1)
                        counters.refresh_attempt_count += 1
                        if market_resolver is not None:
                            try:
                                refreshed_codes = tuple(market_resolver())
                            except Exception:
                                refreshed_codes = active_codes
                            refreshed_codes = _normalize_codes(refreshed_codes)
                            if refreshed_codes:
                                if refreshed_codes == active_codes:
                                    counters.refresh_noop_count += 1
                                else:
                                    now_subscribe = time.monotonic()
                                    if (now_subscribe - last_subscribe_sent_monotonic) < float(min_subscribe_gap_sec):
                                        counters.refresh_noop_count += 1
                                    else:
                                        while subscribe_window and subscribe_window[0] <= now_subscribe - 60.0:
                                            subscribe_window.popleft()
                                        if len(subscribe_window) >= max_subscribe_messages_per_min:
                                            if rate_limit_strict:
                                                counters.fatal_reason = "MAX_SUBSCRIBE_MESSAGES_PER_MIN_REACHED"
                                                connected = False
                                                return counters
                                        else:
                                            refresh_payload = _build_public_subscribe_payload(
                                                channels=channels,
                                                codes=refreshed_codes,
                                                fmt=fmt,
                                                orderbook_level=orderbook_level,
                                                ticket=f"autobot-ws-public-refresh-{int(time.time() * 1000)}",
                                            )
                                            await _send_json_with_limit(websocket, refresh_payload, send_limiter)
                                            subscribe_window.append(time.monotonic())
                                            counters.subscribe_messages_sent += 1
                                            counters.refresh_applied_count += 1
                                            last_subscribe_sent_monotonic = time.monotonic()
                                            active_codes = refreshed_codes
                                            active_codes_set = set(active_codes)
                                            counters.subscribed_markets_count = len(active_codes)
                                            orderbook_state = {
                                                market: state
                                                for market, state in orderbook_state.items()
                                                if market in active_codes_set
                                            }
                                            _emit_runtime_snapshots(force=True)
                            else:
                                counters.refresh_noop_count += 1

                    recv_timeout = min(30.0, remaining)
                    try:
                        raw = await asyncio.wait_for(websocket.recv(), timeout=recv_timeout)
                    except asyncio.TimeoutError:
                        now_timeout = time.monotonic()
                        if now_timeout - last_recv_monotonic >= float(keepalive_interval):
                            if keepalive_mode_value == "frame":
                                pong_waiter = websocket.ping()
                                counters.ping_sent_count += 1
                                await asyncio.wait_for(pong_waiter, timeout=10.0)
                                counters.pong_rx_count += 1
                            elif keepalive_mode_value == "message":
                                if now_timeout - last_text_ping_monotonic >= float(keepalive_interval):
                                    await _send_text_with_limit(websocket, "PING", send_limiter)
                                    counters.ping_sent_count += 1
                                    last_text_ping_monotonic = now_timeout
                            elif keepalive_mode_value == "auto":
                                try:
                                    pong_waiter = websocket.ping()
                                    counters.ping_sent_count += 1
                                    await asyncio.wait_for(pong_waiter, timeout=10.0)
                                    counters.pong_rx_count += 1
                                except Exception:
                                    if now_timeout - last_text_ping_monotonic >= float(keepalive_interval):
                                        await _send_text_with_limit(websocket, "PING", send_limiter)
                                        counters.ping_sent_count += 1
                                        last_text_ping_monotonic = now_timeout
                        if time.monotonic() - last_recv_monotonic >= float(keepalive_stale_limit):
                            raise TimeoutError("websocket idle timeout (120s)")
                        continue

                    if raw is None:
                        continue

                    decoded = _decode_ws_message(raw)
                    if decoded is None:
                        counters.dropped_by_parse_error += 1
                        continue

                    message = _extract_mapping(decoded)
                    if message is None:
                        counters.dropped_by_parse_error += 1
                        continue

                    status = _as_str(message.get("status"), upper=True)
                    if status == "UP":
                        counters.pong_rx_count += 1
                        counters.status_up_count += 1
                        last_recv_monotonic = time.monotonic()
                        continue

                    collected_at_ms = int(time.time() * 1000)
                    normalized = _normalize_public_ws_row(
                        message=message,
                        orderbook_topk=orderbook_topk,
                        orderbook_level=orderbook_level,
                        collected_at_ms=collected_at_ms,
                    )
                    if normalized is None:
                        counters.dropped_by_parse_error += 1
                        continue

                    channel = str(normalized.get("channel"))
                    if channel == "trade":
                        counters.received_trade += 1
                        counters.last_trade_rx_ts_ms = int(collected_at_ms)
                        writer.write(
                            channel="trade",
                            row=normalized,
                            event_ts_ms=int(normalized.get("trade_ts_ms")),
                        )
                        if manifest_flush_callback is not None:
                            manifest_flush_callback()
                        counters.written_trade += 1
                    elif channel == "orderbook":
                        counters.received_orderbook += 1
                        counters.last_orderbook_rx_ts_ms = int(collected_at_ms)
                        market = str(normalized.get("market"))
                        if active_codes_set and market not in active_codes_set:
                            counters.dropped_by_parse_error += 1
                            continue
                        should_write = _should_write_orderbook(
                            record=normalized,
                            state=orderbook_state.get(market),
                            min_write_interval_ms=orderbook_min_write_interval_ms,
                            spread_bps_threshold=orderbook_spread_bps_threshold,
                            top1_size_change_threshold=orderbook_top1_size_change_threshold,
                        )
                        if not should_write:
                            counters.dropped_orderbook_by_interval += 1
                            continue
                        writer.write(
                            channel="orderbook",
                            row=normalized,
                            event_ts_ms=int(normalized.get("ts_ms")),
                        )
                        if manifest_flush_callback is not None:
                            manifest_flush_callback()
                        counters.written_orderbook += 1
                        orderbook_state[market] = _orderbook_state_snapshot(normalized)
                    else:
                        counters.dropped_by_parse_error += 1
                        continue

                    last_recv_monotonic = time.monotonic()
                    _emit_runtime_snapshots()
            connected = False
        except asyncio.CancelledError:
            raise
        except Exception:
            connected = False
            counters.connection_failure_count += 1
            counters.reconnect_count += 1
            now_reconnect = time.monotonic()
            reconnect_window.append(now_reconnect)
            while reconnect_window and reconnect_window[0] <= now_reconnect - 60.0:
                reconnect_window.popleft()
            if len(reconnect_window) > reconnect_max_per_min:
                counters.fatal_reason = "MAX_RECONNECT_PER_MIN_REACHED"
                _emit_runtime_snapshots(force=True)
                return counters

            delay = _reconnect_delay_sec(reconnect_attempt, rng=rng)
            reconnect_attempt += 1
            remaining = max(deadline - time.monotonic(), 0.0)
            if remaining <= 0:
                break
            await asyncio.sleep(min(delay, remaining))
            continue

    _emit_runtime_snapshots(force=True)
    return counters


def _build_public_subscribe_payload(
    *,
    channels: tuple[str, ...],
    codes: tuple[str, ...],
    fmt: str,
    orderbook_level: Any,
    ticket: str,
) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = [{"ticket": ticket}]
    for channel in channels:
        item: dict[str, Any] = {
            "type": channel,
            "codes": list(codes),
        }
        if channel == "orderbook" and orderbook_level is not None:
            item["level"] = orderbook_level
        payload.append(item)
    if str(fmt).strip().upper() != "DEFAULT":
        payload.append({"format": str(fmt).strip().upper()})
    return payload


def _connect_public_websocket(url: str) -> Any:
    common_kwargs = {
        "ping_interval": None,
        "ping_timeout": None,
        "close_timeout": 3,
        "max_queue": 1024,
    }
    try:
        return websockets.connect(url, origin=None, **common_kwargs)
    except TypeError:
        return websockets.connect(url, **common_kwargs)


async def _send_json_with_limit(websocket: Any, payload: Any, limiter: WebSocketRateLimiter) -> None:
    await limiter.acquire()
    await websocket.send(json.dumps(payload, separators=(",", ":"), ensure_ascii=False))


async def _send_text_with_limit(websocket: Any, text: str, limiter: WebSocketRateLimiter) -> None:
    await limiter.acquire()
    await websocket.send(text)


def _normalize_public_ws_row(
    *,
    message: dict[str, Any],
    orderbook_topk: int,
    orderbook_level: Any,
    collected_at_ms: int,
) -> dict[str, Any] | None:
    raw_type = _as_str(_coalesce(message, "type", "ty"), upper=True)
    if raw_type == "TRADE":
        market = _as_str(_coalesce(message, "code", "cd", "market"), upper=True)
        trade_ts_ms = _to_ts_ms(_coalesce(message, "trade_timestamp", "ttms", "trade_ts", "timestamp", "tms"))
        recv_ts_ms = _to_ts_ms(_coalesce(message, "timestamp", "tms"))
        price = _to_float(_coalesce(message, "trade_price", "tp"))
        volume = _to_float(_coalesce(message, "trade_volume", "tv"))
        ask_bid = _as_str(_coalesce(message, "ask_bid", "ab"), upper=True)
        sequential_id = _to_int(_coalesce(message, "sequential_id", "sid"))
        if not market or trade_ts_ms is None or price is None or volume is None:
            return None
        if ask_bid not in {"ASK", "BID"}:
            return None
        return {
            "channel": "trade",
            "market": market,
            "trade_ts_ms": int(trade_ts_ms),
            "recv_ts_ms": recv_ts_ms,
            "price": float(price),
            "volume": float(volume),
            "ask_bid": ask_bid,
            "sequential_id": sequential_id,
            "source": "ws",
            "collected_at_ms": int(collected_at_ms),
        }

    if raw_type == "ORDERBOOK":
        market = _as_str(_coalesce(message, "code", "cd", "market"), upper=True)
        ts_ms = _to_ts_ms(_coalesce(message, "timestamp", "tms"))
        units_raw = _coalesce(message, "orderbook_units", "obu")
        units = [item for item in units_raw if isinstance(item, dict)] if isinstance(units_raw, list) else []
        if not market or ts_ms is None:
            return None

        topk = max(int(orderbook_topk), 1)
        record: dict[str, Any] = {
            "channel": "orderbook",
            "market": market,
            "ts_ms": int(ts_ms),
            "total_ask_size": _to_float(_coalesce(message, "total_ask_size", "tas")),
            "total_bid_size": _to_float(_coalesce(message, "total_bid_size", "tbs")),
            "topk": int(topk),
            "level": _coalesce(message, "level", "lv", "orderbook_level", default=orderbook_level),
            "source": "ws",
            "collected_at_ms": int(collected_at_ms),
        }
        for idx in range(1, topk + 1):
            unit = units[idx - 1] if idx - 1 < len(units) else {}
            record[f"ask{idx}_price"] = _to_float(_coalesce(unit, "ask_price", "ap"))
            record[f"ask{idx}_size"] = _to_float(_coalesce(unit, "ask_size", "as"))
            record[f"bid{idx}_price"] = _to_float(_coalesce(unit, "bid_price", "bp"))
            record[f"bid{idx}_size"] = _to_float(_coalesce(unit, "bid_size", "bs"))
        return record

    return None


def _should_write_orderbook(
    *,
    record: dict[str, Any],
    state: dict[str, float] | None,
    min_write_interval_ms: int,
    spread_bps_threshold: float,
    top1_size_change_threshold: float,
) -> bool:
    if state is None:
        return True

    ts_ms = _to_int(record.get("ts_ms"))
    bid1_price = _to_float(record.get("bid1_price"))
    ask1_price = _to_float(record.get("ask1_price"))
    bid1_size = _to_float(record.get("bid1_size"))
    ask1_size = _to_float(record.get("ask1_size"))
    spread_bps = _spread_bps(bid1_price=bid1_price, ask1_price=ask1_price)

    if ts_ms is None:
        return True
    if bid1_price is not None and ask1_price is not None:
        if bid1_price != state.get("bid1_price") or ask1_price != state.get("ask1_price"):
            return True
    prev_spread = state.get("spread_bps")
    if spread_bps is not None and prev_spread is not None:
        if abs(spread_bps - prev_spread) >= float(spread_bps_threshold):
            return True

    bid_change = _relative_change(current=bid1_size, previous=state.get("bid1_size"))
    ask_change = _relative_change(current=ask1_size, previous=state.get("ask1_size"))
    if max(bid_change, ask_change) >= float(top1_size_change_threshold):
        return True

    last_write_ts = _to_int(state.get("last_write_ts_ms"))
    if last_write_ts is None:
        return True
    return (int(ts_ms) - int(last_write_ts)) >= max(int(min_write_interval_ms), 1)


def _orderbook_state_snapshot(record: dict[str, Any]) -> dict[str, float]:
    bid1_price = _to_float(record.get("bid1_price"))
    ask1_price = _to_float(record.get("ask1_price"))
    return {
        "last_write_ts_ms": float(_to_int(record.get("ts_ms")) or 0),
        "bid1_price": float(bid1_price or 0.0),
        "ask1_price": float(ask1_price or 0.0),
        "bid1_size": float(_to_float(record.get("bid1_size")) or 0.0),
        "ask1_size": float(_to_float(record.get("ask1_size")) or 0.0),
        "spread_bps": float(_spread_bps(bid1_price=bid1_price, ask1_price=ask1_price) or 0.0),
    }


def _reconnect_delay_sec(attempt: int, *, rng: random.Random) -> float:
    backoff = min(2 ** max(int(attempt), 0), 32)
    jitter = rng.uniform(0.0, 0.5)
    return max(float(backoff) + jitter, 0.05)


def _decode_ws_message(raw: bytes | str | Any) -> Any:
    if isinstance(raw, (bytes, str)):
        try:
            if isinstance(raw, bytes):
                return json.loads(raw.decode("utf-8"))
            return json.loads(raw)
        except Exception:
            return None
    return raw


def _extract_mapping(payload: Any) -> dict[str, Any] | None:
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                return item
    return None


def _coalesce(payload: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in payload:
            return payload[key]
    return default


def _load_plan(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"ws public plan file not found: {path}")
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("ws public plan file must contain JSON object")
    return raw


def _flush_writer_manifest_state(
    *,
    writer: WsRawRotatingWriter,
    manifest_path: Path,
    runs_summary_path: Path,
) -> None:
    pending_parts = writer.drain_closed_parts()
    if not pending_parts:
        return
    append_ws_manifest_rows(manifest_path, pending_parts)
    _update_ws_runs_summary(manifest_path, runs_summary_path)


def _update_ws_runs_summary(manifest_path: Path, summary_path: Path) -> None:
    if not manifest_path.exists():
        payload = {"manifest_file": str(manifest_path), "runs": []}
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        return

    manifest = load_ws_manifest(manifest_path)
    rows = [dict(item) for item in manifest.iter_rows(named=True)]
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        run_id = str(row.get("run_id") or "").strip()
        if not run_id:
            continue
        bucket = grouped.setdefault(
            run_id,
            {
                "run_id": run_id,
                "parts": 0,
                "rows_total": 0,
                "bytes_total": 0,
                "trade_rows": 0,
                "orderbook_rows": 0,
                "ok_parts": 0,
                "warn_parts": 0,
                "fail_parts": 0,
                "reasons": {},
                "min_date": None,
                "max_date": None,
                "min_ts_ms": None,
                "max_ts_ms": None,
            },
        )
        bucket["parts"] += 1
        rows_value = int(_to_int(row.get("rows")) or 0)
        bytes_value = int(_to_int(row.get("bytes")) or 0)
        bucket["rows_total"] += rows_value
        bucket["bytes_total"] += bytes_value
        channel = str(row.get("channel") or "").strip().lower()
        if channel == "trade":
            bucket["trade_rows"] += rows_value
        elif channel == "orderbook":
            bucket["orderbook_rows"] += rows_value

        status = str(row.get("status") or "").strip().upper()
        if status == "OK":
            bucket["ok_parts"] += 1
        elif status == "WARN":
            bucket["warn_parts"] += 1
        elif status == "FAIL":
            bucket["fail_parts"] += 1

        date_value = str(row.get("date") or "").strip()
        if date_value:
            if bucket["min_date"] is None or date_value < bucket["min_date"]:
                bucket["min_date"] = date_value
            if bucket["max_date"] is None or date_value > bucket["max_date"]:
                bucket["max_date"] = date_value

        min_ts = _to_int(row.get("min_ts_ms"))
        max_ts = _to_int(row.get("max_ts_ms"))
        if min_ts is not None:
            if bucket["min_ts_ms"] is None or min_ts < bucket["min_ts_ms"]:
                bucket["min_ts_ms"] = min_ts
        if max_ts is not None:
            if bucket["max_ts_ms"] is None or max_ts > bucket["max_ts_ms"]:
                bucket["max_ts_ms"] = max_ts

        reasons = _parse_reasons_json(row.get("reasons_json"))
        for reason in reasons:
            bucket["reasons"][reason] = int(bucket["reasons"].get(reason, 0) + 1)

    payload = {
        "manifest_file": str(manifest_path),
        "runs": [grouped[key] for key in sorted(grouped)],
    }
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _prune_ws_retention(*, raw_root: Path, retention_days: int) -> dict[str, list[str]]:
    if not raw_root.exists():
        return {"trade": [], "orderbook": []}
    cutoff = datetime.now(timezone.utc).date() - timedelta(days=max(int(retention_days), 1))
    removed: dict[str, list[str]] = {"trade": [], "orderbook": []}
    for channel in ("trade", "orderbook"):
        channel_root = raw_root / channel
        if not channel_root.exists():
            continue
        for date_dir in sorted(channel_root.glob("date=*")):
            if not date_dir.is_dir():
                continue
            date_text = date_dir.name.replace("date=", "", 1).strip()
            try:
                parsed = date.fromisoformat(date_text)
            except ValueError:
                continue
            if parsed < cutoff:
                shutil.rmtree(date_dir, ignore_errors=True)
                removed[channel].append(date_text)
    return removed


def _build_ws_runtime_plan_payload(
    *,
    quote: str,
    top_n: int,
    max_markets: int,
    channels: tuple[str, ...],
    fmt: str,
    orderbook_topk: int,
    orderbook_level: Any,
    orderbook_min_write_interval_ms: int,
    keepalive_mode: str,
    keepalive_interval_sec: int,
    keepalive_stale_sec: int,
    codes: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "version": "t13.1c-ws-public-ops-v1",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "filters": {
            "quote": quote,
            "market_mode": "top_n_by_rest_ticker",
            "top_n": int(top_n),
            "max_markets": int(max_markets),
            "channels": list(channels),
        },
        "runtime_policy": {
            "format": fmt,
            "orderbook_topk": int(orderbook_topk),
            "orderbook_level": orderbook_level,
            "orderbook_min_write_interval_ms": int(orderbook_min_write_interval_ms),
            "keepalive_mode": _normalize_keepalive_mode(keepalive_mode),
            "keepalive_interval_sec": int(keepalive_interval_sec),
            "keepalive_stale_sec": int(keepalive_stale_sec),
        },
        "safety": {
            "enforce_no_origin_header": True,
            "max_subscribe_messages_per_min": 100,
        },
        "selected_markets": list(codes),
        "codes": list(codes),
        "summary": {
            "selected_markets": len(codes),
            "codes_count": len(codes),
            "channels_count": len(channels),
        },
    }


def _chunk_codes(codes: list[str], *, size: int) -> list[tuple[str, ...]]:
    chunk_size = max(int(size), 1)
    return [tuple(codes[idx : idx + chunk_size]) for idx in range(0, len(codes), chunk_size)]


def _normalize_keepalive_mode(value: Any) -> str:
    mode = str(value).strip().lower()
    if mode not in VALID_KEEPALIVE_MODES:
        return "auto"
    return mode


def _health_payload(*, run_id: str, counters: _RuntimeCounters, connected: bool) -> dict[str, Any]:
    written_total = int(counters.written_trade + counters.written_orderbook)
    dropped_total = int(counters.dropped_orderbook_by_interval + counters.dropped_by_parse_error)
    return {
        "run_id": run_id,
        "updated_at_ms": int(time.time() * 1000),
        "connected": bool(connected),
        "reconnect_count": int(counters.reconnect_count),
        "last_rx_ts_ms": {
            "trade": counters.last_trade_rx_ts_ms,
            "orderbook": counters.last_orderbook_rx_ts_ms,
        },
        "written_rows": {
            "trade": int(counters.written_trade),
            "orderbook": int(counters.written_orderbook),
            "total": written_total,
        },
        "dropped_rows": {
            "orderbook_downsample": int(counters.dropped_orderbook_by_interval),
            "parse_error": int(counters.dropped_by_parse_error),
            "total": dropped_total,
        },
        "backlog_queue": 0,
        "subscribed_markets_count": int(counters.subscribed_markets_count),
        "keepalive": {
            "ping_sent_count": int(counters.ping_sent_count),
            "pong_rx_count": int(counters.pong_rx_count),
            "status_up_count": int(counters.status_up_count),
        },
        "refresh": {
            "attempt_count": int(counters.refresh_attempt_count),
            "applied_count": int(counters.refresh_applied_count),
            "noop_count": int(counters.refresh_noop_count),
        },
        "connections": {
            "opened": int(counters.connection_open_count),
            "failed": int(counters.connection_failure_count),
            "subscribe_messages_sent": int(counters.subscribe_messages_sent),
        },
        "fatal_reason": counters.fatal_reason,
    }


def _write_health_snapshot(*, path: Path | None, payload: dict[str, Any]) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    temp_path = path.with_name(f".{path.name}.tmp")
    temp_path.write_text(serialized, encoding="utf-8")
    temp_path.replace(path)


def _normalize_channels(value: Any) -> tuple[str, ...]:
    normalized: list[str] = []
    seen: set[str] = set()
    values = value if isinstance(value, (list, tuple)) else []
    for raw in values:
        channel = str(raw).strip().lower()
        if channel not in VALID_WS_PUBLIC_CHANNELS or channel in seen:
            continue
        seen.add(channel)
        normalized.append(channel)
    return tuple(normalized)


def _normalize_codes(value: Any) -> tuple[str, ...]:
    normalized: list[str] = []
    seen: set[str] = set()
    values = value if isinstance(value, (list, tuple)) else []
    for raw in values:
        code = str(raw).strip().upper()
        if not code or code in seen:
            continue
        seen.add(code)
        normalized.append(code)
    return tuple(normalized)


def _parse_reasons_json(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return [text]
    else:
        parsed = value
    if isinstance(parsed, list):
        return [str(item) for item in parsed if str(item).strip()]
    if isinstance(parsed, str) and parsed.strip():
        return [parsed.strip()]
    return []


def _load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    for attempt in range(3):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            if attempt == 2:
                return None
            time.sleep(0.05)
            continue
        if isinstance(payload, dict):
            return payload
        return None
    return None


def _spread_bps(*, bid1_price: float | None, ask1_price: float | None) -> float | None:
    if bid1_price is None or ask1_price is None:
        return None
    mid = (float(bid1_price) + float(ask1_price)) / 2.0
    if mid <= 0:
        return None
    spread = float(ask1_price) - float(bid1_price)
    return (spread / mid) * 10_000.0


def _relative_change(*, current: float | None, previous: float | None) -> float:
    if current is None or previous is None:
        return 0.0
    denominator = max(abs(float(previous)), 1e-12)
    return abs(float(current) - float(previous)) / denominator


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            return int(float(text))
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_ts_ms(value: Any) -> int | None:
    parsed = _to_int(value)
    if parsed is None:
        return None
    if abs(parsed) < 10_000_000_000:
        return int(parsed) * 1000
    return int(parsed)


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_str(value: Any, *, upper: bool) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if upper:
        return text.upper()
    return text
