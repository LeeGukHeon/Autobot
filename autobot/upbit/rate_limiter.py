"""Group-aware rate limiter with 429/418 cooldown handling."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Callable

from .types import RemainingReqInfo


@dataclass
class _GroupState:
    rate_per_sec: float
    capacity: float
    tokens: float
    last_refill_at: float
    cooldown_until: float = 0.0
    last_remaining_sec: int | None = None


class UpbitRateLimiter:
    """Token-bucket limiter that syncs with Upbit Remaining-Req headers."""

    def __init__(
        self,
        *,
        enabled: bool = True,
        ban_cooldown_sec: int = 60,
        group_rates: dict[str, float] | None = None,
        monotonic_time: Callable[[], float] | None = None,
        sleep_fn: Callable[[float], None] | None = None,
    ) -> None:
        self._enabled = enabled
        self._ban_cooldown_sec = max(int(ban_cooldown_sec), 1)
        self._group_rates = dict(group_rates or {})
        self._states: dict[str, _GroupState] = {}
        self._global_cooldown_until = 0.0
        self._lock = threading.Lock()
        self._time = monotonic_time or time.monotonic
        self._sleep = sleep_fn or time.sleep

    def acquire(self, group: str) -> None:
        if not self._enabled:
            return

        group_name = group or "default"
        while True:
            with self._lock:
                now = self._time()
                state = self._get_or_create_state(group_name, now)
                self._refill(state, now)

                cooldown_wait = max(self._global_cooldown_until - now, state.cooldown_until - now, 0.0)
                if cooldown_wait <= 0.0 and state.tokens >= 1.0:
                    state.tokens -= 1.0
                    return

                if cooldown_wait > 0.0:
                    wait_for = cooldown_wait
                else:
                    wait_for = (1.0 - state.tokens) / state.rate_per_sec

            self._sleep(max(wait_for, 0.01))

    def observe_remaining_req(self, info: RemainingReqInfo | None) -> None:
        if not self._enabled or info is None:
            return

        with self._lock:
            now = self._time()
            state = self._get_or_create_state(info.group, now)
            self._refill(state, now)
            state.last_remaining_sec = info.sec

            if info.sec <= 0:
                state.tokens = 0.0
                state.cooldown_until = max(state.cooldown_until, now + 1.0)
                return

            state.tokens = min(state.tokens, float(info.sec))

    def register_429(self, group: str, attempt: int) -> float:
        delay_sec = max(1.0, min(8.0, float(2 ** max(attempt - 1, 0))))
        if not self._enabled:
            return delay_sec

        with self._lock:
            now = self._time()
            state = self._get_or_create_state(group or "default", now)
            state.tokens = 0.0
            state.cooldown_until = max(state.cooldown_until, now + delay_sec)
        return delay_sec

    def register_418(self, group: str, cooldown_sec: int | None = None) -> float:
        delay_sec = float(max(cooldown_sec or self._ban_cooldown_sec, 1))
        if not self._enabled:
            return delay_sec

        with self._lock:
            now = self._time()
            until = now + delay_sec
            state = self._get_or_create_state(group or "default", now)
            state.tokens = 0.0
            state.cooldown_until = max(state.cooldown_until, until)
            self._global_cooldown_until = max(self._global_cooldown_until, until)
        return delay_sec

    def _get_or_create_state(self, group: str, now: float) -> _GroupState:
        state = self._states.get(group)
        if state is not None:
            return state

        rate = self._resolve_rate(group)
        capacity = max(rate, 1.0)
        created = _GroupState(
            rate_per_sec=rate,
            capacity=capacity,
            tokens=capacity,
            last_refill_at=now,
        )
        self._states[group] = created
        return created

    def _resolve_rate(self, group: str) -> float:
        if group in self._group_rates:
            return max(float(self._group_rates[group]), 0.1)
        if "default" in self._group_rates:
            return max(float(self._group_rates["default"]), 0.1)
        return 10.0

    @staticmethod
    def _refill(state: _GroupState, now: float) -> None:
        elapsed = max(now - state.last_refill_at, 0.0)
        if elapsed <= 0:
            return
        state.tokens = min(state.capacity, state.tokens + elapsed * state.rate_per_sec)
        state.last_refill_at = now
