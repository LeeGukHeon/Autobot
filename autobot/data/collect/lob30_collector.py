"""Plan-driven collector for 30-level orderbook parquet snapshots."""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import random
import time
from typing import Any, Callable

from ...upbit import load_upbit_settings
from ...upbit.ws.ws_rate_limiter import WebSocketRateLimiter
from .lob30_manifest import append_manifest_rows, manifest_path
from .lob30_writer import write_lob30_partition
from .ws_public_collector import (
    _as_str,
    _coalesce,
    _connect_public_websocket,
    _decode_ws_message,
    _extract_mapping,
    _normalize_keepalive_mode,
    _normalize_public_ws_row,
    _send_json_with_limit,
    _send_text_with_limit,
    _write_health_snapshot,
)


@dataclass(frozen=True)
class Lob30CollectOptions:
    plan_path: Path = Path("data/collect/_meta/lob30_plan.json")
    parquet_root: Path = Path("data/parquet")
    out_dataset: str = "lob30_v1"
    meta_dir: Path = Path("data/collect/_meta")
    duration_sec: int = 120
    rate_limit_strict: bool = True
    reconnect_max_per_min: int = 3
    keepalive_mode: str = "auto"
    keepalive_interval_sec: int = 60
    keepalive_stale_sec: int = 120
    health_update_sec: int = 5
    config_dir: Path = Path("config")

    @property
    def dataset_root(self) -> Path:
        return self.parquet_root / self.out_dataset

    @property
    def collect_report_path(self) -> Path:
        return self.meta_dir / "lob30_collect_report.json"

    @property
    def validate_report_path(self) -> Path:
        return self.meta_dir / "lob30_validate_report.json"

    @property
    def build_report_path(self) -> Path:
        return self.dataset_root / "_meta" / "build_report.json"

    @property
    def health_snapshot_path(self) -> Path:
        return self.meta_dir / "lob30_health.json"


@dataclass(frozen=True)
class Lob30CollectSummary:
    run_id: str
    duration_sec: int
    codes_count: int
    received_messages: int
    snapshot_messages: int
    realtime_messages: int
    rows_buffered: int
    rows_written: int
    persisted_partitions: int
    reconnect_count: int
    ping_sent_count: int
    pong_rx_count: int
    collect_report_file: Path
    build_report_file: Path
    manifest_file: Path
    details: tuple[dict[str, Any], ...]
    failures: tuple[dict[str, Any], ...]


@dataclass
class _Lob30RuntimeCounters:
    received_messages: int = 0
    snapshot_messages: int = 0
    realtime_messages: int = 0
    reconnect_count: int = 0
    ping_sent_count: int = 0
    pong_rx_count: int = 0
    status_up_count: int = 0
    subscribe_messages_sent: int = 0
    connection_open_count: int = 0
    connection_failure_count: int = 0
    dropped_by_parse_error: int = 0
    last_rx_ts_ms: int | None = None
    subscribed_markets_count: int = 0
    fatal_reason: str | None = None


def collect_lob30_from_plan(
    options: Lob30CollectOptions,
    *,
    websocket_connect: Callable[[str], Any] | None = None,
    settings_loader: Callable[[Path], Any] | None = None,
) -> Lob30CollectSummary:
    started_at = int(time.time())
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    plan = _load_plan(options.plan_path)

    load_settings = settings_loader or load_upbit_settings
    settings = load_settings(options.config_dir)
    ws_settings = settings.websocket

    runtime_policy = plan.get("runtime_policy", {}) if isinstance(plan.get("runtime_policy"), dict) else {}
    safety_policy = plan.get("safety", {}) if isinstance(plan.get("safety"), dict) else {}
    request_codes = _normalize_codes(plan.get("request_codes") or [])
    selected_markets = _normalize_codes(plan.get("selected_markets") or plan.get("codes") or [])
    if not request_codes:
        raise ValueError("plan.request_codes is required")

    fmt = str(runtime_policy.get("format", "DEFAULT")).strip().upper() or "DEFAULT"
    requested_depth = int(runtime_policy.get("requested_depth", 30) or 30)
    orderbook_level = runtime_policy.get("orderbook_level", 0)
    if requested_depth != 30:
        raise ValueError("lob30 collector requires requested_depth=30")
    if float(orderbook_level or 0) != 0.0:
        raise ValueError("lob30 collector requires orderbook_level=0")
    is_only_snapshot = bool(runtime_policy.get("is_only_snapshot", False))
    is_only_realtime = bool(runtime_policy.get("is_only_realtime", False))
    max_subscribe_messages_per_min = max(int(safety_policy.get("max_subscribe_messages_per_min", 20)), 1)

    dataset_root = options.dataset_root
    dataset_root.mkdir(parents=True, exist_ok=True)
    manifest_file = manifest_path(dataset_root)

    counters = _Lob30RuntimeCounters(subscribed_markets_count=len(selected_markets))
    details: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    buffers: dict[tuple[str, int], dict[str, Any]] = {}
    connect_fn = websocket_connect or _connect_public_websocket

    try:
        counters, buffers = asyncio.run(
            _run_lob30_collection(
                ws_url=ws_settings.public_url,
                request_codes=request_codes,
                duration_sec=max(int(options.duration_sec), 1),
                fmt=fmt,
                is_only_snapshot=is_only_snapshot,
                is_only_realtime=is_only_realtime,
                connect_rps=int(ws_settings.ratelimit.connect_rps),
                message_rps=int(ws_settings.ratelimit.message_rps),
                message_rpm=int(ws_settings.ratelimit.message_rpm),
                reconnect_max_per_min=max(int(options.reconnect_max_per_min), 1),
                max_subscribe_messages_per_min=max_subscribe_messages_per_min,
                rate_limit_strict=bool(options.rate_limit_strict),
                keepalive_mode=str(options.keepalive_mode),
                keepalive_interval_sec=max(int(options.keepalive_interval_sec), 1),
                keepalive_stale_sec=max(int(options.keepalive_stale_sec), 30),
                health_snapshot_path=options.health_snapshot_path,
                health_update_sec=max(int(options.health_update_sec), 1),
                run_id=run_id,
                websocket_connect=connect_fn,
            )
        )
    except Exception as exc:
        failures.append({"reason": "RUNTIME_EXCEPTION", "error_message": str(exc)})
        counters.fatal_reason = counters.fatal_reason or "RUNTIME_EXCEPTION"

    grouped_rows: dict[str, list[dict[str, Any]]] = {}
    for row in buffers.values():
        grouped_rows.setdefault(str(row["market"]), []).append(row)

    manifest_rows: list[dict[str, Any]] = []
    rows_written = 0
    window_tag = f"{run_id}__{max(int(options.duration_sec), 1)}s"
    for market, rows in sorted(grouped_rows.items()):
        write_result = write_lob30_partition(
            dataset_root=dataset_root,
            market=market,
            rows=rows,
        )
        rows_written += len(rows)
        manifest_rows.append(
            {
                "dataset_name": options.out_dataset,
                "source": "upbit_ws_orderbook_30",
                "window_tag": window_tag,
                "market": market,
                "date": str(write_result.get("date", "")),
                "rows": int(write_result.get("rows", 0)),
                "min_ts_ms": write_result.get("min_ts_ms"),
                "max_ts_ms": write_result.get("max_ts_ms"),
                "status": "OK",
                "reasons_json": json.dumps([], ensure_ascii=False),
                "error_message": None,
                "part_file": str(write_result.get("part_file", "")),
                "collected_at": int(time.time()),
            }
        )

    if manifest_rows:
        append_manifest_rows(manifest_file, manifest_rows)

    details.append(
        {
            "codes_count": len(request_codes),
            "selected_markets_count": len(selected_markets),
            "format": fmt,
            "requested_depth": requested_depth,
            "orderbook_level": orderbook_level,
            "is_only_snapshot": is_only_snapshot,
            "is_only_realtime": is_only_realtime,
            "rows_buffered": len(buffers),
            "persisted_partitions": len(grouped_rows),
            "subscribe_messages_sent": int(counters.subscribe_messages_sent),
            "dropped_by_parse_error": int(counters.dropped_by_parse_error),
        }
    )
    if counters.fatal_reason:
        failures.append({"reason": counters.fatal_reason})

    collect_report = {
        "started_at": started_at,
        "finished_at": int(time.time()),
        "run_id": run_id,
        "duration_sec": max(int(options.duration_sec), 1),
        "plan_file": str(options.plan_path),
        "dataset_root": str(dataset_root),
        "codes_count": len(request_codes),
        "selected_markets_count": len(selected_markets),
        "received_messages": int(counters.received_messages),
        "snapshot_messages": int(counters.snapshot_messages),
        "realtime_messages": int(counters.realtime_messages),
        "rows_buffered": len(buffers),
        "rows_written": int(rows_written),
        "persisted_partitions": len(grouped_rows),
        "reconnect_count": int(counters.reconnect_count),
        "ping_sent_count": int(counters.ping_sent_count),
        "pong_rx_count": int(counters.pong_rx_count),
        "dropped_by_parse_error": int(counters.dropped_by_parse_error),
        "fatal_reason": counters.fatal_reason,
        "manifest_file": str(manifest_file),
        "health_snapshot_file": str(options.health_snapshot_path),
        "details": details,
        "failures": failures,
    }
    options.collect_report_path.parent.mkdir(parents=True, exist_ok=True)
    options.collect_report_path.write_text(json.dumps(collect_report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    build_report = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "dataset_name": options.out_dataset,
        "dataset_root": str(dataset_root),
        "manifest_file": str(manifest_file),
        "collect_report_file": str(options.collect_report_path),
        "summary": {
            "received_messages": int(counters.received_messages),
            "snapshot_messages": int(counters.snapshot_messages),
            "realtime_messages": int(counters.realtime_messages),
            "rows_buffered": len(buffers),
            "rows_written": int(rows_written),
            "persisted_partitions": len(grouped_rows),
        },
    }
    options.build_report_path.parent.mkdir(parents=True, exist_ok=True)
    options.build_report_path.write_text(json.dumps(build_report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    return Lob30CollectSummary(
        run_id=run_id,
        duration_sec=max(int(options.duration_sec), 1),
        codes_count=len(request_codes),
        received_messages=int(counters.received_messages),
        snapshot_messages=int(counters.snapshot_messages),
        realtime_messages=int(counters.realtime_messages),
        rows_buffered=len(buffers),
        rows_written=int(rows_written),
        persisted_partitions=len(grouped_rows),
        reconnect_count=int(counters.reconnect_count),
        ping_sent_count=int(counters.ping_sent_count),
        pong_rx_count=int(counters.pong_rx_count),
        collect_report_file=options.collect_report_path,
        build_report_file=options.build_report_path,
        manifest_file=manifest_file,
        details=tuple(details),
        failures=tuple(failures),
    )


async def _run_lob30_collection(
    *,
    ws_url: str,
    request_codes: tuple[str, ...],
    duration_sec: int,
    fmt: str,
    is_only_snapshot: bool,
    is_only_realtime: bool,
    connect_rps: int,
    message_rps: int,
    message_rpm: int,
    reconnect_max_per_min: int,
    max_subscribe_messages_per_min: int,
    rate_limit_strict: bool,
    keepalive_mode: str,
    keepalive_interval_sec: int,
    keepalive_stale_sec: int,
    health_snapshot_path: Path | None,
    health_update_sec: int,
    run_id: str,
    websocket_connect: Callable[[str], Any],
) -> tuple[_Lob30RuntimeCounters, dict[tuple[str, int], dict[str, Any]]]:
    counters = _Lob30RuntimeCounters(subscribed_markets_count=len(request_codes))
    buffers: dict[tuple[str, int], dict[str, Any]] = {}
    keepalive_mode_value = _normalize_keepalive_mode(keepalive_mode)
    connect_limiter = WebSocketRateLimiter(per_second=max(int(connect_rps), 1))
    send_limiter = WebSocketRateLimiter(per_second=max(int(message_rps), 1), per_minute=max(int(message_rpm), 1))
    rng = random.Random()
    deadline = time.monotonic() + float(duration_sec)
    reconnect_attempt = 0
    reconnect_window: deque[float] = deque()
    subscribe_window: deque[float] = deque()
    keepalive_interval = max(int(keepalive_interval_sec), 1)
    keepalive_stale_limit = max(int(keepalive_stale_sec), keepalive_interval + 1)
    next_health_update_monotonic = time.monotonic()
    health_interval_sec = max(int(health_update_sec), 1)
    connected = False

    payload = [
        {"ticket": f"autobot-lob30-{int(time.time() * 1000)}"},
        {
            "type": "orderbook",
            "codes": list(request_codes),
            "level": 0,
            "is_only_snapshot": bool(is_only_snapshot),
            "is_only_realtime": bool(is_only_realtime),
        },
    ]
    if str(fmt).strip().upper() != "DEFAULT":
        payload.append({"format": str(fmt).strip().upper()})

    while time.monotonic() < deadline:
        if health_snapshot_path is not None and time.monotonic() >= next_health_update_monotonic:
            _write_health_snapshot(path=health_snapshot_path, payload=_health_payload(run_id=run_id, counters=counters, connected=connected))
            next_health_update_monotonic = time.monotonic() + float(health_interval_sec)

        await connect_limiter.acquire()
        try:
            async with websocket_connect(ws_url) as websocket:
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
                        return counters, buffers
                    await asyncio.sleep(1.0)
                await _send_json_with_limit(websocket, payload, send_limiter)
                subscribe_window.append(time.monotonic())
                counters.subscribe_messages_sent += 1

                last_recv_monotonic = time.monotonic()
                last_text_ping_monotonic = 0.0
                while time.monotonic() < deadline:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break

                    try:
                        raw = await asyncio.wait_for(websocket.recv(), timeout=min(30.0, remaining))
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
                            raise TimeoutError("websocket idle timeout")
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

                    normalized = _normalize_lob30_row(message=message, collected_at_ms=int(time.time() * 1000))
                    if normalized is None:
                        counters.dropped_by_parse_error += 1
                        continue

                    key = (str(normalized["market"]), int(normalized["ts_ms"]))
                    buffers[key] = normalized
                    counters.received_messages += 1
                    counters.last_rx_ts_ms = int(normalized["collected_at_ms"])
                    stream_type = str(normalized.get("stream_type", "")).strip().upper()
                    if stream_type == "SNAPSHOT":
                        counters.snapshot_messages += 1
                    else:
                        counters.realtime_messages += 1
                    last_recv_monotonic = time.monotonic()
                    _write_health_snapshot(path=health_snapshot_path, payload=_health_payload(run_id=run_id, counters=counters, connected=connected))
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
                _write_health_snapshot(path=health_snapshot_path, payload=_health_payload(run_id=run_id, counters=counters, connected=connected))
                return counters, buffers
            delay = min((2 ** max(int(reconnect_attempt), 0)) + rng.uniform(0.0, 0.5), 32.0)
            reconnect_attempt += 1
            remaining = max(deadline - time.monotonic(), 0.0)
            if remaining <= 0:
                break
            await asyncio.sleep(min(delay, remaining))
            continue

    _write_health_snapshot(path=health_snapshot_path, payload=_health_payload(run_id=run_id, counters=counters, connected=False))
    return counters, buffers


def _normalize_lob30_row(*, message: dict[str, Any], collected_at_ms: int) -> dict[str, Any] | None:
    base = _normalize_public_ws_row(
        message=message,
        orderbook_topk=30,
        orderbook_level=0,
        collected_at_ms=collected_at_ms,
    )
    if base is None or str(base.get("channel")) != "orderbook":
        return None
    row = dict(base)
    row["source"] = "ws_orderbook_lob30"
    row["requested_depth"] = 30
    row["levels_present"] = sum(1 for idx in range(1, 31) if row.get(f"ask{idx}_price") is not None and row.get(f"bid{idx}_price") is not None)
    row["stream_type"] = _as_str(_coalesce(message, "stream_type", "st"), upper=True) or "REALTIME"
    return row


def _normalize_codes(values: Any) -> tuple[str, ...]:
    normalized: list[str] = []
    seen: set[str] = set()
    raw_values = values if isinstance(values, (list, tuple)) else []
    for raw in raw_values:
        value = str(raw).strip().upper()
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return tuple(normalized)


def _health_payload(*, run_id: str, counters: _Lob30RuntimeCounters, connected: bool) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "updated_at_ms": int(time.time() * 1000),
        "connected": bool(connected),
        "received_messages": int(counters.received_messages),
        "snapshot_messages": int(counters.snapshot_messages),
        "realtime_messages": int(counters.realtime_messages),
        "dropped_by_parse_error": int(counters.dropped_by_parse_error),
        "subscribed_markets_count": int(counters.subscribed_markets_count),
        "last_rx_ts_ms": counters.last_rx_ts_ms,
        "keepalive": {
            "ping_sent_count": int(counters.ping_sent_count),
            "pong_rx_count": int(counters.pong_rx_count),
            "status_up_count": int(counters.status_up_count),
        },
        "connections": {
            "opened": int(counters.connection_open_count),
            "failed": int(counters.connection_failure_count),
            "subscribe_messages_sent": int(counters.subscribe_messages_sent),
            "reconnect_count": int(counters.reconnect_count),
        },
        "fatal_reason": counters.fatal_reason,
    }


def _load_plan(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"lob30 plan file not found: {path}")
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("lob30 plan file must contain JSON object")
    return raw
