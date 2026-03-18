from __future__ import annotations

import httpx
import pytest

from autobot.upbit.config import (
    UpbitAuthSettings,
    UpbitRateLimitSettings,
    UpbitRetrySettings,
    UpbitSettings,
    UpbitTimeoutSettings,
    UpbitWebSocketSettings,
)
from autobot.upbit.exceptions import AuthError, RateLimitError
from autobot.upbit.http_client import UpbitHttpClient


class _FakeRateLimiter:
    def __init__(self) -> None:
        self.acquired: list[str] = []
        self.observed: list[object] = []
        self.registered_429: list[tuple[str, int]] = []
        self.registered_418: list[tuple[str, int | None]] = []

    def acquire(self, group: str) -> None:
        self.acquired.append(group)

    def observe_remaining_req(self, info: object) -> None:
        self.observed.append(info)

    def register_429(self, group: str, attempt: int) -> float:
        self.registered_429.append((group, attempt))
        return 1.0

    def register_418(self, group: str, cooldown_sec: int | None = None) -> float:
        self.registered_418.append((group, cooldown_sec))
        return float(cooldown_sec or 60)


class _SequencedClient:
    def __init__(self, responses: list[object]) -> None:
        self._responses = list(responses)
        self.calls: list[dict[str, object]] = []

    def request(self, method: str, url: str, **kwargs: object) -> httpx.Response:
        self.calls.append({"method": method, "url": url, "kwargs": kwargs})
        if not self._responses:
            raise AssertionError("no more queued responses")
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    def close(self) -> None:
        return None


def _settings(*, retry_attempts: int = 2) -> UpbitSettings:
    return UpbitSettings(
        base_url="https://api.upbit.com",
        timeout=UpbitTimeoutSettings(connect_sec=1.0, read_sec=1.0, write_sec=1.0),
        auth=UpbitAuthSettings(access_key_env="UPBIT_ACCESS_KEY", secret_key_env="UPBIT_SECRET_KEY"),
        ratelimit=UpbitRateLimitSettings(enabled=True, ban_cooldown_sec=60, group_defaults={}),
        retry=UpbitRetrySettings(max_attempts=retry_attempts, base_backoff_ms=1, max_backoff_ms=2),
        websocket=UpbitWebSocketSettings(),
    )


def _json_response(status_code: int, payload: object, *, headers: dict[str, str] | None = None) -> httpx.Response:
    request = httpx.Request("GET", "https://api.upbit.com/test")
    return httpx.Response(status_code, json=payload, headers=headers, request=request)


def test_http_client_retries_timeout_then_returns_json() -> None:
    rate_limiter = _FakeRateLimiter()
    client = _SequencedClient(
        [
            httpx.TimeoutException("temporary timeout"),
            _json_response(200, {"ok": True}),
        ]
    )
    with UpbitHttpClient(_settings(retry_attempts=2), client=client, rate_limiter=rate_limiter) as http_client:
        payload = http_client.request_json("GET", "/test", rate_limit_group="market")

    assert payload == {"ok": True}
    assert len(client.calls) == 2
    assert rate_limiter.acquired == ["market", "market"]


def test_http_client_registers_429_then_succeeds() -> None:
    rate_limiter = _FakeRateLimiter()
    client = _SequencedClient(
        [
            _json_response(
                429,
                {"error": {"name": "too_many_requests", "message": "slow down"}},
                headers={"Remaining-Req": "group=market; min=99; sec=0"},
            ),
            _json_response(
                200,
                {"ok": True},
                headers={"Remaining-Req": "group=market; min=99; sec=7"},
            ),
        ]
    )
    with UpbitHttpClient(_settings(retry_attempts=2), client=client, rate_limiter=rate_limiter) as http_client:
        payload = http_client.request_json("GET", "/test", rate_limit_group="market")

    assert payload == {"ok": True}
    assert rate_limiter.registered_429 == [("market", 1)]
    assert len(rate_limiter.observed) == 2


def test_http_client_raises_banned_ratelimit_on_418() -> None:
    rate_limiter = _FakeRateLimiter()
    client = _SequencedClient(
        [
            _json_response(
                418,
                {"error": {"name": "banned", "message": "blocked for 30 seconds"}},
                headers={"Remaining-Req": "group=market; min=99; sec=0"},
            )
        ]
    )
    with UpbitHttpClient(_settings(retry_attempts=1), client=client, rate_limiter=rate_limiter) as http_client:
        with pytest.raises(RateLimitError) as exc_info:
            http_client.request_json("GET", "/test", rate_limit_group="market")

    assert exc_info.value.banned is True
    assert exc_info.value.cooldown_sec == 30.0
    assert rate_limiter.registered_418 == [("market", 30)]


def test_http_client_requires_credentials_for_private_auth() -> None:
    rate_limiter = _FakeRateLimiter()
    client = _SequencedClient([])
    with UpbitHttpClient(_settings(), client=client, rate_limiter=rate_limiter) as http_client:
        with pytest.raises(AuthError):
            http_client.request_json("GET", "/test", auth=True)
