"""Async Upbit private websocket client for myOrder/myAsset streams."""

from __future__ import annotations

import asyncio
import json
import random
import time
import uuid
from typing import Any, AsyncIterator, Sequence

import websockets

from ..auth_jwt import UpbitJwtSigner
from ..config import UpbitCredentials, UpbitWebSocketSettings
from .models import MyAssetEvent, MyOrderEvent
from .parsers import decode_ws_message, parse_private_events
from .payloads import build_private_subscribe_payload
from .ws_rate_limiter import WebSocketRateLimiter


class UpbitWebSocketPrivateClient:
    def __init__(
        self,
        settings: UpbitWebSocketSettings,
        credentials: UpbitCredentials,
    ) -> None:
        self._settings = settings
        self._signer = UpbitJwtSigner(
            access_key=credentials.access_key,
            secret_key=credentials.secret_key,
        )
        self._connect_limiter = WebSocketRateLimiter(per_second=settings.ratelimit.connect_rps)
        self._send_limiter = WebSocketRateLimiter(
            per_second=settings.ratelimit.message_rps,
            per_minute=settings.ratelimit.message_rpm,
        )
        self._stats: dict[str, Any] = {
            "reconnect_count": 0,
            "received_events": 0,
            "last_event_ts_ms": None,
            "last_event_latency_ms": None,
            "last_reconnect_ts_ms": None,
        }

    @property
    def stats(self) -> dict[str, Any]:
        return dict(self._stats)

    async def stream_private(
        self,
        *,
        channels: Sequence[str] = ("myOrder", "myAsset"),
        duration_sec: float | None = None,
    ) -> AsyncIterator[MyOrderEvent | MyAssetEvent]:
        normalized_channels = _normalize_channels(channels)
        if not normalized_channels:
            raise ValueError("channels is required")

        queue: asyncio.Queue[MyOrderEvent | MyAssetEvent] = asyncio.Queue()
        stop_event = asyncio.Event()

        worker = asyncio.create_task(
            self._run_private_connection(channels=normalized_channels, queue=queue, stop_event=stop_event)
        )
        timer_task: asyncio.Task[None] | None = None
        if duration_sec is not None and duration_sec > 0:

            async def _stop_later() -> None:
                await asyncio.sleep(duration_sec)
                stop_event.set()

            timer_task = asyncio.create_task(_stop_later())

        try:
            while True:
                if stop_event.is_set() and queue.empty() and worker.done():
                    break
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    if worker.done():
                        break
                    continue
                yield event
        finally:
            stop_event.set()
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
            if timer_task is not None:
                timer_task.cancel()
                await asyncio.gather(timer_task, return_exceptions=True)

    async def _run_private_connection(
        self,
        *,
        channels: tuple[str, ...],
        queue: asyncio.Queue[MyOrderEvent | MyAssetEvent],
        stop_event: asyncio.Event,
    ) -> None:
        attempt = 0
        while not stop_event.is_set():
            await self._connect_limiter.acquire()
            ticket = f"autobot-private-{uuid.uuid4().hex[:12]}"
            payload = build_private_subscribe_payload(ticket=ticket, types=channels, fmt=self._settings.format)
            headers = {"Authorization": self._signer.build_authorization_header(query_string="")}

            try:
                async with _connect_private_websocket(self._settings.private_url, headers=headers) as websocket:
                    attempt = 0
                    await self._send_json(websocket, payload)
                    await self._recv_private_loop(websocket=websocket, queue=queue, stop_event=stop_event)
            except asyncio.CancelledError:
                raise
            except Exception:
                if stop_event.is_set() or not self._settings.reconnect.enabled:
                    return
                self._stats["reconnect_count"] = int(self._stats["reconnect_count"]) + 1
                self._stats["last_reconnect_ts_ms"] = int(time.time() * 1000)
                await asyncio.sleep(self._reconnect_delay_sec(attempt))
                attempt += 1

    async def _recv_private_loop(
        self,
        *,
        websocket: Any,
        queue: asyncio.Queue[MyOrderEvent | MyAssetEvent],
        stop_event: asyncio.Event,
    ) -> None:
        keepalive_interval = min(max(self._settings.keepalive.ping_interval_sec, 1.0), 110.0)
        ping_timeout = max(self._settings.keepalive.ping_timeout_sec, 1.0)
        idle_limit = 120.0 + ping_timeout
        last_received = time.monotonic()

        while not stop_event.is_set():
            try:
                raw = await asyncio.wait_for(websocket.recv(), timeout=keepalive_interval)
            except asyncio.TimeoutError:
                await self._send_keepalive(websocket, ping_timeout=ping_timeout)
                if time.monotonic() - last_received > idle_limit:
                    raise TimeoutError("private websocket idle timeout")
                continue

            if raw is None:
                continue
            if isinstance(raw, str) and raw.strip().upper() == "PONG":
                continue

            try:
                payload = decode_ws_message(raw) if isinstance(raw, (bytes, str)) else raw
            except json.JSONDecodeError:
                continue

            events = parse_private_events(payload)
            if not events:
                continue
            last_received = time.monotonic()
            for event in events:
                now_ms = int(time.time() * 1000)
                self._stats["received_events"] = int(self._stats["received_events"]) + 1
                self._stats["last_event_ts_ms"] = int(event.ts_ms)
                self._stats["last_event_latency_ms"] = max(now_ms - int(event.ts_ms), 0)
                await queue.put(event)

    async def _send_json(self, websocket: Any, payload: Any) -> None:
        await self._send_limiter.acquire()
        await websocket.send(json.dumps(payload, separators=(",", ":"), ensure_ascii=False))

    async def _send_text(self, websocket: Any, text: str) -> None:
        await self._send_limiter.acquire()
        await websocket.send(text)

    async def _send_keepalive(self, websocket: Any, *, ping_timeout: float) -> None:
        if self._settings.keepalive.allow_text_ping:
            await self._send_text(websocket, "PING")
            return
        pong_waiter = websocket.ping()
        await asyncio.wait_for(pong_waiter, timeout=ping_timeout)

    def _reconnect_delay_sec(self, attempt: int) -> float:
        reconnect = self._settings.reconnect
        backoff_ms = min(reconnect.base_delay_ms * (2 ** max(attempt, 0)), reconnect.max_delay_ms)
        jitter_ms = random.randint(0, reconnect.jitter_ms) if reconnect.jitter_ms > 0 else 0
        return (backoff_ms + jitter_ms) / 1000.0


def _normalize_channels(channels: Sequence[str]) -> tuple[str, ...]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in channels:
        lowered = str(raw).strip().lower()
        if lowered == "myorder":
            value = "myOrder"
        elif lowered == "myasset":
            value = "myAsset"
        else:
            continue
        if value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return tuple(normalized)


def _connect_private_websocket(url: str, *, headers: dict[str, str]) -> Any:
    common_kwargs = {
        "ping_interval": None,
        "ping_timeout": None,
        "close_timeout": 3,
        "max_queue": 1024,
    }
    try:
        return websockets.connect(url, additional_headers=headers, **common_kwargs)
    except TypeError:
        return websockets.connect(url, extra_headers=headers, **common_kwargs)
