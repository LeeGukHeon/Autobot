"""Async fixed-window rate limiter for websocket connect/send limits."""

from __future__ import annotations

import asyncio
from collections import deque
import time


class WebSocketRateLimiter:
    def __init__(self, *, per_second: int, per_minute: int | None = None) -> None:
        self._per_second = max(int(per_second), 1)
        self._per_minute = max(int(per_minute), 1) if per_minute is not None else None
        self._second_window: deque[float] = deque()
        self._minute_window: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            sleep_for = 0.0
            async with self._lock:
                now = time.monotonic()
                self._trim(now)

                second_wait = 0.0
                minute_wait = 0.0
                if len(self._second_window) >= self._per_second:
                    second_wait = max(self._second_window[0] + 1.0 - now, 0.0)
                if self._per_minute is not None and len(self._minute_window) >= self._per_minute:
                    minute_wait = max(self._minute_window[0] + 60.0 - now, 0.0)

                sleep_for = max(second_wait, minute_wait)
                if sleep_for <= 0.0:
                    stamp = time.monotonic()
                    self._second_window.append(stamp)
                    if self._per_minute is not None:
                        self._minute_window.append(stamp)
                    return

            await asyncio.sleep(sleep_for)

    def _trim(self, now: float) -> None:
        second_cutoff = now - 1.0
        while self._second_window and self._second_window[0] <= second_cutoff:
            self._second_window.popleft()

        if self._per_minute is None:
            return
        minute_cutoff = now - 60.0
        while self._minute_window and self._minute_window[0] <= minute_cutoff:
            self._minute_window.popleft()

