"""Async Upbit public websocket client for quotation streams."""

from __future__ import annotations

import asyncio
import json
import random
import time
import traceback
import uuid
from typing import Any, AsyncIterator, Sequence

import websockets

from ..config import UpbitWebSocketSettings
from .models import OrderbookEvent, Subscription, TickerEvent, TradeEvent
from .parsers import decode_ws_message, parse_orderbook_event, parse_ticker_event, parse_trade_event
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
        self._stats: dict[str, Any] = {
            "reconnect_count": 0,
            "received_events": 0,
            "last_event_ts_ms": None,
            "last_event_latency_ms": None,
            "last_reconnect_ts_ms": None,
            "last_subscription_type": None,
            "last_parser_name": None,
            "last_payload_preview": None,
            "last_malformed_payload_preview": None,
            "last_malformed_parser": None,
            "last_malformed_ts_ms": None,
            "last_exception_traceback": None,
        }

    @property
    def stats(self) -> dict[str, Any]:
        return dict(self._stats)

    async def stream_ticker(
        self,
        markets: Sequence[str],
        *,
        duration_sec: float | None = None,
    ) -> AsyncIterator[TickerEvent]:
        async for event in self._stream_public_events(
            markets=markets,
            duration_sec=duration_sec,
            subscription_type="ticker",
            parser=parse_ticker_event,
            orderbook_level=None,
        ):
            yield event

    async def stream_trade(
        self,
        markets: Sequence[str],
        *,
        duration_sec: float | None = None,
    ) -> AsyncIterator[TradeEvent]:
        async for event in self._stream_public_events(
            markets=markets,
            duration_sec=duration_sec,
            subscription_type="trade",
            parser=parse_trade_event,
            orderbook_level=None,
        ):
            yield event

    async def stream_orderbook(
        self,
        markets: Sequence[str],
        *,
        duration_sec: float | None = None,
        level: int | str | None = 0,
    ) -> AsyncIterator[OrderbookEvent]:
        async for event in self._stream_public_events(
            markets=markets,
            duration_sec=duration_sec,
            subscription_type="orderbook",
            parser=parse_orderbook_event,
            orderbook_level=level,
        ):
            yield event

    async def _stream_public_events(
        self,
        *,
        markets: Sequence[str],
        duration_sec: float | None,
        subscription_type: str,
        parser: Any,
        orderbook_level: int | str | None,
    ) -> AsyncIterator[Any]:
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
            asyncio.create_task(
                self._run_public_connection(
                    index=idx,
                    codes=chunk,
                    queue=queue,
                    stop_event=stop_event,
                    subscription_type=subscription_type,
                    parser=parser,
                    orderbook_level=orderbook_level,
                )
            )
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

    async def _run_public_connection(
        self,
        *,
        index: int,
        codes: tuple[str, ...],
        queue: asyncio.Queue[Any],
        stop_event: asyncio.Event,
        subscription_type: str,
        parser: Any,
        orderbook_level: int | str | None,
    ) -> None:
        attempt = 0
        while not stop_event.is_set():
            await self._connect_limiter.acquire()
            ticket = f"autobot-{index}-{uuid.uuid4().hex[:12]}"
            payload = build_subscribe_payload(
                ticket=ticket,
                subscriptions=[
                    Subscription(
                        type=subscription_type,
                        codes=codes,
                        is_only_realtime=True,
                    )
                ],
                fmt=self._settings.format,
            )
            if subscription_type == "orderbook" and orderbook_level is not None:
                payload[1]["level"] = orderbook_level

            try:
                async with websockets.connect(
                    self._settings.public_url,
                    ping_interval=None,
                    ping_timeout=None,
                    close_timeout=3,
                    max_queue=1024,
                ) as websocket:
                    attempt = 0
                    self._stats["last_subscription_type"] = str(subscription_type)
                    await self._send_json(websocket, payload)
                    await self._recv_public_loop(
                        websocket=websocket,
                        queue=queue,
                        stop_event=stop_event,
                        parser=parser,
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                self._stats["last_exception_traceback"] = _preview_text(traceback.format_exc(), limit=3000)
                self._stats["reconnect_count"] = int(self._stats["reconnect_count"]) + 1
                self._stats["last_reconnect_ts_ms"] = int(time.time() * 1000)
                if stop_event.is_set() or not self._settings.reconnect.enabled:
                    return
                await asyncio.sleep(self._reconnect_delay_sec(attempt))
                attempt += 1

    async def _recv_public_loop(
        self,
        *,
        websocket: Any,
        queue: asyncio.Queue[Any],
        stop_event: asyncio.Event,
        parser: Any,
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

            self._stats["last_parser_name"] = getattr(parser, "__name__", parser.__class__.__name__)
            self._stats["last_payload_preview"] = _preview_text(_dump_preview(payload), limit=1200)

            event = parser(payload)
            if event is None:
                self._stats["last_malformed_payload_preview"] = _preview_text(_dump_preview(payload), limit=1200)
                self._stats["last_malformed_parser"] = getattr(parser, "__name__", parser.__class__.__name__)
                self._stats["last_malformed_ts_ms"] = int(time.time() * 1000)
                continue
            last_received = time.monotonic()
            now_ms = int(time.time() * 1000)
            event_ts_ms = getattr(event, "ts_ms", None)
            self._stats["received_events"] = int(self._stats["received_events"]) + 1
            self._stats["last_event_ts_ms"] = int(event_ts_ms) if event_ts_ms is not None else None
            self._stats["last_event_latency_ms"] = (
                max(now_ms - int(event_ts_ms), 0) if event_ts_ms is not None else None
            )
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


def _dump_preview(payload: Any) -> str:
    try:
        return json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    except Exception:
        return repr(payload)


def _preview_text(value: str, *, limit: int) -> str:
    text = str(value or "").replace("\r", " ").replace("\n", " ").strip()
    if len(text) <= max(int(limit), 1):
        return text
    return text[: max(int(limit) - 1, 1)] + "…"
