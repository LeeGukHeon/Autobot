"""Plan-driven collector for Upbit websocket candle streams into parquet."""

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
from ...upbit.ws.models import Subscription
from ...upbit.ws.payloads import build_subscribe_payload
from ...upbit.ws.ws_rate_limiter import WebSocketRateLimiter
from ..inventory import parse_utc_ts_ms
from .candle_manifest import append_manifest_rows, manifest_path
from .candle_writer import write_candle_partition
from .ws_public_collector import (
    _as_str,
    _coalesce,
    _connect_public_websocket,
    _decode_ws_message,
    _extract_mapping,
    _normalize_keepalive_mode,
    _send_json_with_limit,
    _send_text_with_limit,
    _to_float,
    _to_int,
    _write_health_snapshot,
)


VALID_WS_CANDLE_TFS: tuple[str, ...] = ("1s", "1m", "3m", "5m", "10m", "15m", "30m", "60m", "240m")


@dataclass(frozen=True)
class WsCandleCollectOptions:
    plan_path: Path = Path("data/collect/_meta/ws_candle_plan.json")
    parquet_root: Path = Path("data/parquet")
    out_dataset: str = "ws_candle_v1"
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
        return self.meta_dir / "ws_candle_collect_report.json"

    @property
    def validate_report_path(self) -> Path:
        return self.meta_dir / "ws_candle_validate_report.json"

    @property
    def build_report_path(self) -> Path:
        return self.dataset_root / "_meta" / "build_report.json"

    @property
    def health_snapshot_path(self) -> Path:
        return self.meta_dir / "ws_candle_health.json"


@dataclass(frozen=True)
class WsCandleCollectSummary:
    run_id: str
    duration_sec: int
    codes_count: int
    tf_count: int
    received_messages: int
    snapshot_messages: int
    realtime_messages: int
    rows_buffered: int
    rows_written: int
    persisted_pairs: int
    reconnect_count: int
    ping_sent_count: int
    pong_rx_count: int
    collect_report_file: Path
    build_report_file: Path
    manifest_file: Path
    details: tuple[dict[str, Any], ...]
    failures: tuple[dict[str, Any], ...]


@dataclass
class _WsCandleRuntimeCounters:
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


def collect_ws_candles_from_plan(
    options: WsCandleCollectOptions,
    *,
    websocket_connect: Callable[[str], Any] | None = None,
    settings_loader: Callable[[Path], Any] | None = None,
) -> WsCandleCollectSummary:
    started_at = int(time.time())
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    plan = _load_plan(options.plan_path)

    load_settings = settings_loader or load_upbit_settings
    settings = load_settings(options.config_dir)
    ws_settings = settings.websocket

    runtime_policy = plan.get("runtime_policy", {}) if isinstance(plan.get("runtime_policy"), dict) else {}
    safety_policy = plan.get("safety", {}) if isinstance(plan.get("safety"), dict) else {}
    tf_set = _normalize_tf_set(plan.get("filters", {}).get("tf_set"))
    codes = _normalize_codes(plan.get("codes") or plan.get("selected_markets"))
    if not tf_set:
        raise ValueError("plan.filters.tf_set is required")
    if not codes:
        raise ValueError("plan.codes is required")

    fmt = str(runtime_policy.get("format", "DEFAULT")).strip().upper() or "DEFAULT"
    is_only_snapshot = bool(runtime_policy.get("is_only_snapshot", False))
    is_only_realtime = bool(runtime_policy.get("is_only_realtime", False))
    max_subscribe_messages_per_min = max(int(safety_policy.get("max_subscribe_messages_per_min", 20)), 1)

    dataset_root = options.dataset_root
    dataset_root.mkdir(parents=True, exist_ok=True)
    manifest_file = manifest_path(dataset_root)

    counters = _WsCandleRuntimeCounters(subscribed_markets_count=len(codes))
    details: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    buffers: dict[tuple[str, str, int], dict[str, Any]] = {}

    connect_fn = websocket_connect or _connect_public_websocket
    try:
        counters, buffers = asyncio.run(
            _run_ws_candle_collection(
                ws_url=ws_settings.public_url,
                codes=codes,
                tf_set=tf_set,
                fmt=fmt,
                duration_sec=max(int(options.duration_sec), 1),
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

    grouped_rows: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in buffers.values():
        key = (str(row["tf"]).strip().lower(), str(row["market"]).strip().upper())
        grouped_rows.setdefault(key, []).append(row)

    manifest_rows: list[dict[str, Any]] = []
    rows_written = 0
    window_tag = f"{run_id}__{max(int(options.duration_sec), 1)}s"
    for (tf, market), rows in sorted(grouped_rows.items()):
        write_result = write_candle_partition(
            dataset_root=dataset_root,
            tf=tf,
            market=market,
            candles=rows,
        )
        rows_written += len(rows)
        manifest_rows.append(
            {
                "dataset_name": options.out_dataset,
                "source": "upbit_ws_candle",
                "window_tag": window_tag,
                "market": market,
                "tf": tf,
                "rows": int(write_result.get("rows", 0)),
                "min_ts_ms": write_result.get("min_ts_ms"),
                "max_ts_ms": write_result.get("max_ts_ms"),
                "calls_made": int(counters.subscribe_messages_sent),
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
            "tf_set": list(tf_set),
            "codes_count": len(codes),
            "format": fmt,
            "is_only_snapshot": is_only_snapshot,
            "is_only_realtime": is_only_realtime,
            "rows_buffered": len(buffers),
            "persisted_pairs": len(grouped_rows),
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
        "codes_count": len(codes),
        "tf_set": list(tf_set),
        "received_messages": int(counters.received_messages),
        "snapshot_messages": int(counters.snapshot_messages),
        "realtime_messages": int(counters.realtime_messages),
        "rows_buffered": len(buffers),
        "rows_written": int(rows_written),
        "persisted_pairs": len(grouped_rows),
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
    options.collect_report_path.write_text(
        json.dumps(collect_report, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )

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
            "persisted_pairs": len(grouped_rows),
        },
    }
    options.build_report_path.parent.mkdir(parents=True, exist_ok=True)
    options.build_report_path.write_text(
        json.dumps(build_report, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    return WsCandleCollectSummary(
        run_id=run_id,
        duration_sec=max(int(options.duration_sec), 1),
        codes_count=len(codes),
        tf_count=len(tf_set),
        received_messages=int(counters.received_messages),
        snapshot_messages=int(counters.snapshot_messages),
        realtime_messages=int(counters.realtime_messages),
        rows_buffered=len(buffers),
        rows_written=int(rows_written),
        persisted_pairs=len(grouped_rows),
        reconnect_count=int(counters.reconnect_count),
        ping_sent_count=int(counters.ping_sent_count),
        pong_rx_count=int(counters.pong_rx_count),
        collect_report_file=options.collect_report_path,
        build_report_file=options.build_report_path,
        manifest_file=manifest_file,
        details=tuple(details),
        failures=tuple(failures),
    )


async def _run_ws_candle_collection(
    *,
    ws_url: str,
    codes: tuple[str, ...],
    tf_set: tuple[str, ...],
    fmt: str,
    duration_sec: int,
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
) -> tuple[_WsCandleRuntimeCounters, dict[tuple[str, str, int], dict[str, Any]]]:
    counters = _WsCandleRuntimeCounters(subscribed_markets_count=len(codes))
    buffers: dict[tuple[str, str, int], dict[str, Any]] = {}
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
    keepalive_interval = max(int(keepalive_interval_sec), 1)
    keepalive_stale_limit = max(int(keepalive_stale_sec), keepalive_interval + 1)
    next_health_update_monotonic = time.monotonic()
    health_interval_sec = max(int(health_update_sec), 1)
    connected = False

    subscriptions = [
        Subscription(
            type=f"candle.{tf}",
            codes=codes,
            is_only_snapshot=is_only_snapshot or None,
            is_only_realtime=is_only_realtime or None,
        )
        for tf in tf_set
    ]

    while time.monotonic() < deadline:
        if health_snapshot_path is not None and time.monotonic() >= next_health_update_monotonic:
            _write_health_snapshot(
                path=health_snapshot_path,
                payload=_health_payload(run_id=run_id, counters=counters, connected=connected),
            )
            next_health_update_monotonic = time.monotonic() + float(health_interval_sec)

        await connect_limiter.acquire()
        payload = build_subscribe_payload(
            ticket=f"autobot-ws-candle-{int(time.time() * 1000)}",
            subscriptions=subscriptions,
            fmt=fmt,
        )

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

                    collected_at_ms = int(time.time() * 1000)
                    normalized = _normalize_ws_candle_row(message=message, collected_at_ms=collected_at_ms)
                    if normalized is None:
                        counters.dropped_by_parse_error += 1
                        continue

                    key = (str(normalized["tf"]), str(normalized["market"]), int(normalized["ts_ms"]))
                    buffers[key] = normalized
                    counters.received_messages += 1
                    counters.last_rx_ts_ms = collected_at_ms
                    stream_type = str(normalized.get("stream_type", "")).strip().upper()
                    if stream_type == "SNAPSHOT":
                        counters.snapshot_messages += 1
                    else:
                        counters.realtime_messages += 1
                    last_recv_monotonic = time.monotonic()
                    _write_health_snapshot(
                        path=health_snapshot_path,
                        payload=_health_payload(run_id=run_id, counters=counters, connected=connected),
                    )
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
                _write_health_snapshot(
                    path=health_snapshot_path,
                    payload=_health_payload(run_id=run_id, counters=counters, connected=connected),
                )
                return counters, buffers

            delay = min((2 ** max(int(reconnect_attempt), 0)) + rng.uniform(0.0, 0.5), 32.0)
            reconnect_attempt += 1
            remaining = max(deadline - time.monotonic(), 0.0)
            if remaining <= 0:
                break
            await asyncio.sleep(min(delay, remaining))
            continue

    _write_health_snapshot(
        path=health_snapshot_path,
        payload=_health_payload(run_id=run_id, counters=counters, connected=False),
    )
    return counters, buffers


def _normalize_ws_candle_row(*, message: dict[str, Any], collected_at_ms: int) -> dict[str, Any] | None:
    raw_type = _as_str(_coalesce(message, "type", "ty"), upper=False)
    if raw_type is None:
        return None
    tf = _tf_from_ws_type(raw_type)
    if tf is None:
        return None

    market = _as_str(_coalesce(message, "code", "cd", "market"), upper=True)
    candle_date_time_utc = _as_str(_coalesce(message, "candle_date_time_utc", "cdttmu"), upper=False)
    ts_ms = parse_utc_ts_ms(candle_date_time_utc) if candle_date_time_utc else None
    opening_price = _to_float(_coalesce(message, "opening_price", "op"))
    high_price = _to_float(_coalesce(message, "high_price", "hp"))
    low_price = _to_float(_coalesce(message, "low_price", "lp"))
    trade_price = _to_float(_coalesce(message, "trade_price", "tp"))
    volume_base = _to_float(_coalesce(message, "candle_acc_trade_volume", "catv"))
    volume_quote = _to_float(_coalesce(message, "candle_acc_trade_price", "catp"))
    stream_type = _as_str(_coalesce(message, "stream_type", "st"), upper=True)
    recv_ts_ms = _to_int(_coalesce(message, "timestamp", "tms"))

    if not market or ts_ms is None:
        return None
    if (
        opening_price is None
        or high_price is None
        or low_price is None
        or trade_price is None
        or volume_base is None
    ):
        return None

    return {
        "tf": tf,
        "market": market,
        "ts_ms": int(ts_ms),
        "open": float(opening_price),
        "high": float(high_price),
        "low": float(low_price),
        "close": float(trade_price),
        "volume_base": float(volume_base),
        "volume_quote": float(volume_quote) if volume_quote is not None else None,
        "volume_quote_est": False,
        "stream_type": stream_type or "REALTIME",
        "recv_ts_ms": recv_ts_ms,
        "source": "ws_candle",
        "collected_at_ms": int(collected_at_ms),
    }


def _tf_from_ws_type(value: str) -> str | None:
    text = str(value).strip().lower()
    if not text.startswith("candle."):
        return None
    tf = text.split(".", 1)[1]
    if tf not in VALID_WS_CANDLE_TFS:
        return None
    return tf


def _normalize_tf_set(value: Any) -> tuple[str, ...]:
    values = value if isinstance(value, (list, tuple)) else []
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in values:
        tf = str(raw).strip().lower()
        if tf not in VALID_WS_CANDLE_TFS or tf in seen:
            continue
        seen.add(tf)
        normalized.append(tf)
    return tuple(normalized)


def _normalize_codes(value: Any) -> tuple[str, ...]:
    values = value if isinstance(value, (list, tuple)) else []
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in values:
        code = str(raw).strip().upper()
        if not code or code in seen:
            continue
        seen.add(code)
        normalized.append(code)
    return tuple(normalized)


def _health_payload(*, run_id: str, counters: _WsCandleRuntimeCounters, connected: bool) -> dict[str, Any]:
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
        raise FileNotFoundError(f"ws candle plan file not found: {path}")
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("ws candle plan file must contain JSON object")
    return raw
