"""Async Upbit public websocket client for quotation streams."""

from __future__ import annotations

import asyncio
import json
import random
import time
import uuid
from typing import Any, AsyncIterator, Sequence

import websockets

from ..config import UpbitWebSocketSettings
from .models import Subscription, TickerEvent
from .parsers import decode_ws_message, parse_ticker_event
from .payloads import build_subscribe_payload
from .ws_rate_limiter import WebSocketRateLimiter


class UpbitWebSocketPublicClient:
    def __init__(self, settings: UpbitWebSocketSettings) -> None:
        self._settings = settings
        self._connect_limiter = WebSocketRateLimiter(per_second=settings.ratelimit.connect_rps)
        self._send_limiter = WebSocketRateLimiter(
            per_second=settings.ratelimit.message_rps,
            per_minute=settings.ratelimit.message_rpm,
        )

    async def stream_ticker(
        self,
        markets: Sequence[str],
        *,
        duration_sec: float | None = None,
    ) -> AsyncIterator[TickerEvent]:
        codes = _normalize_codes(markets)
        if not codes:
            raise ValueError("markets is required")

        chunks = _chunk_codes(codes, size=self._settings.codes_per_connection)
        if len(chunks) > self._settings.max_connections:
            raise ValueError(
                f"required connections={len(chunks)} exceeds max_connections={self._settings.max_connections}"
            )

        queue: asyncio.Queue[TickerEvent] = asyncio.Queue()
        stop_event = asyncio.Event()
        worker_tasks = [
            asyncio.create_task(self._run_ticker_connection(index=idx, codes=chunk, queue=queue, stop_event=stop_event))
            for idx, chunk in enumerate(chunks, start=1)
        ]

        timer_task: asyncio.Task[None] | None = None
        if duration_sec is not None and duration_sec > 0:

            async def _stop_later() -> None:
                await asyncio.sleep(duration_sec)
                stop_event.set()

            timer_task = asyncio.create_task(_stop_later())

        try:
            while True:
                if stop_event.is_set() and queue.empty() and all(task.done() for task in worker_tasks):
                    break
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    if all(task.done() for task in worker_tasks):
                        break
                    continue
                yield event
        finally:
            stop_event.set()
            for task in worker_tasks:
                task.cancel()
            await asyncio.gather(*worker_tasks, return_exceptions=True)
            if timer_task is not None:
                timer_task.cancel()
                await asyncio.gather(timer_task, return_exceptions=True)

    async def _run_ticker_connection(
        self,
        *,
        index: int,
        codes: tuple[str, ...],
        queue: asyncio.Queue[TickerEvent],
        stop_event: asyncio.Event,
    ) -> None:
        attempt = 0
        while not stop_event.is_set():
            await self._connect_limiter.acquire()
            ticket = f"autobot-{index}-{uuid.uuid4().hex[:12]}"
            payload = build_subscribe_payload(
                ticket=ticket,
                subscriptions=[
                    Subscription(
                        type="ticker",
                        codes=codes,
                        is_only_realtime=True,
                    )
                ],
                fmt=self._settings.format,
            )

            try:
                async with websockets.connect(
                    self._settings.public_url,
                    ping_interval=None,
                    ping_timeout=None,
                    close_timeout=3,
                    max_queue=1024,
                ) as websocket:
                    attempt = 0
                    await self._send_json(websocket, payload)
                    await self._recv_ticker_loop(websocket=websocket, queue=queue, stop_event=stop_event)
            except asyncio.CancelledError:
                raise
            except Exception:
                if stop_event.is_set() or not self._settings.reconnect.enabled:
                    return
                await asyncio.sleep(self._reconnect_delay_sec(attempt))
                attempt += 1

    async def _recv_ticker_loop(
        self,
        *,
        websocket: Any,
        queue: asyncio.Queue[TickerEvent],
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
                    raise TimeoutError("websocket idle timeout")
                continue

            if raw is None:
                continue
            if isinstance(raw, str) and raw.strip().upper() == "PONG":
                continue

            try:
                payload = decode_ws_message(raw) if isinstance(raw, (bytes, str)) else raw
            except json.JSONDecodeError:
                continue

            event = parse_ticker_event(payload)
            if event is None:
                continue
            last_received = time.monotonic()
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


def _normalize_codes(markets: Sequence[str]) -> tuple[str, ...]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_market in markets:
        market = str(raw_market).strip().upper()
        if not market or market in seen:
            continue
        seen.add(market)
        normalized.append(market)
    return tuple(normalized)


def _chunk_codes(codes: Sequence[str], *, size: int) -> list[tuple[str, ...]]:
    chunk_size = max(int(size), 1)
    return [tuple(codes[idx : idx + chunk_size]) for idx in range(0, len(codes), chunk_size)]

