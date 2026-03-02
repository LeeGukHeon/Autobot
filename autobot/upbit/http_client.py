"""HTTP client with Upbit auth, rate-limit, and retry policies."""

from __future__ import annotations

import re
import time
from typing import Any

import httpx

from .auth_jwt import UpbitJwtSigner
from .config import UpbitCredentials, UpbitSettings
from .error_policy import classify_status
from .exceptions import (
    AuthError,
    ClientRequestError,
    NetworkError,
    RateLimitError,
    ServerError,
    UpbitError,
    ValidationError,
)
from .logging import get_upbit_logger, log_rest_event
from .querystring import build_query_string, normalize_param_pairs
from .rate_limiter import UpbitRateLimiter
from .remaining_req import parse_remaining_req_header
from .types import JSONValue, ParamsInput


class UpbitHttpClient:
    """Thin sync wrapper around `httpx.Client` for Upbit REST APIs."""

    def __init__(
        self,
        settings: UpbitSettings,
        *,
        credentials: UpbitCredentials | None = None,
        client: httpx.Client | None = None,
        rate_limiter: UpbitRateLimiter | None = None,
    ) -> None:
        self._settings = settings
        self._logger = get_upbit_logger()
        self._rate_limiter = rate_limiter or UpbitRateLimiter(
            enabled=settings.ratelimit.enabled,
            ban_cooldown_sec=settings.ratelimit.ban_cooldown_sec,
            group_rates=settings.ratelimit.to_group_rates(),
        )
        self._signer = (
            UpbitJwtSigner(access_key=credentials.access_key, secret_key=credentials.secret_key)
            if credentials is not None
            else None
        )

        timeout = httpx.Timeout(
            connect=settings.timeout.connect_sec,
            read=settings.timeout.read_sec,
            write=settings.timeout.write_sec,
            pool=settings.timeout.read_sec,
        )
        self._owned_client = client is None
        self._client = client or httpx.Client(
            base_url=settings.base_url.rstrip("/"),
            timeout=timeout,
            headers={"Accept": "application/json"},
        )

    def close(self) -> None:
        if self._owned_client:
            self._client.close()

    def __enter__(self) -> UpbitHttpClient:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    def request_json(
        self,
        method: str,
        endpoint: str,
        *,
        params: ParamsInput | None = None,
        json_body: dict[str, Any] | None = None,
        auth: bool = False,
        rate_limit_group: str = "default",
    ) -> JSONValue:
        method_upper = method.upper()
        request_params = normalize_param_pairs(params) or None
        query_string = self._query_string_for_auth(method_upper, params=params, json_body=json_body)
        attempts = max(self._settings.retry.max_attempts, 1)

        for attempt in range(1, attempts + 1):
            self._rate_limiter.acquire(rate_limit_group)

            headers = self._build_headers(auth=auth, query_string=query_string, has_json=json_body is not None)
            started_at = time.perf_counter()
            try:
                response = self._client.request(
                    method=method_upper,
                    url=endpoint,
                    params=request_params,
                    json=json_body,
                    headers=headers,
                )
            except httpx.TimeoutException as exc:
                latency_ms = (time.perf_counter() - started_at) * 1000.0
                self._log_event(
                    method=method_upper,
                    endpoint=endpoint,
                    status=None,
                    latency_ms=latency_ms,
                    error_name="timeout",
                    error_message=str(exc),
                )
                if attempt < attempts:
                    self._sleep_retry_backoff(attempt)
                    continue
                raise NetworkError("Upbit request timed out", endpoint=endpoint, method=method_upper) from exc
            except httpx.RequestError as exc:
                latency_ms = (time.perf_counter() - started_at) * 1000.0
                self._log_event(
                    method=method_upper,
                    endpoint=endpoint,
                    status=None,
                    latency_ms=latency_ms,
                    error_name=exc.__class__.__name__,
                    error_message=str(exc),
                )
                if attempt < attempts:
                    self._sleep_retry_backoff(attempt)
                    continue
                raise NetworkError("Upbit network request failed", endpoint=endpoint, method=method_upper) from exc

            latency_ms = (time.perf_counter() - started_at) * 1000.0
            remaining_raw = response.headers.get("Remaining-Req")
            request_id = response.headers.get("Request-Id") or response.headers.get("X-Request-Id")
            parsed_remaining = parse_remaining_req_header(remaining_raw)
            self._rate_limiter.observe_remaining_req(parsed_remaining)
            group = parsed_remaining.group if parsed_remaining is not None else rate_limit_group

            status_code = response.status_code
            if 200 <= status_code < 300:
                self._log_event(
                    method=method_upper,
                    endpoint=endpoint,
                    status=status_code,
                    latency_ms=latency_ms,
                    remaining_req=remaining_raw,
                    request_id=request_id,
                )
                return _decode_response_json(response)

            error_name, error_message = _parse_upbit_error(response)
            error_message = error_message or f"HTTP {status_code}"

            if status_code == 429:
                cooldown = self._rate_limiter.register_429(group, attempt)
                self._log_event(
                    method=method_upper,
                    endpoint=endpoint,
                    status=status_code,
                    latency_ms=latency_ms,
                    remaining_req=remaining_raw,
                    request_id=request_id,
                    error_name=error_name,
                    error_message=error_message,
                )
                if attempt < attempts:
                    continue
                raise RateLimitError(
                    error_message,
                    status_code=status_code,
                    error_name=error_name,
                    endpoint=endpoint,
                    method=method_upper,
                    cooldown_sec=cooldown,
                )

            if status_code == 418:
                cooldown = _extract_ban_cooldown_seconds(response, self._settings.ratelimit.ban_cooldown_sec)
                self._rate_limiter.register_418(group, cooldown)
                self._log_event(
                    method=method_upper,
                    endpoint=endpoint,
                    status=status_code,
                    latency_ms=latency_ms,
                    remaining_req=remaining_raw,
                    request_id=request_id,
                    error_name=error_name,
                    error_message=error_message,
                )
                raise RateLimitError(
                    error_message,
                    status_code=status_code,
                    error_name=error_name,
                    endpoint=endpoint,
                    method=method_upper,
                    cooldown_sec=float(cooldown),
                    banned=True,
                )

            category, retriable = classify_status(status_code)
            self._log_event(
                method=method_upper,
                endpoint=endpoint,
                status=status_code,
                latency_ms=latency_ms,
                remaining_req=remaining_raw,
                request_id=request_id,
                error_name=error_name,
                error_message=error_message,
            )

            if category == "server":
                if attempt < attempts:
                    self._sleep_retry_backoff(attempt)
                    continue
                raise ServerError(
                    error_message,
                    status_code=status_code,
                    error_name=error_name,
                    endpoint=endpoint,
                    method=method_upper,
                    retriable=retriable,
                )

            raise _build_client_error(
                status_code=status_code,
                error_name=error_name,
                error_message=error_message,
                endpoint=endpoint,
                method=method_upper,
                retriable=retriable,
            )

        raise UpbitError("Upbit request failed after retries", endpoint=endpoint, method=method_upper)

    def _build_headers(self, *, auth: bool, query_string: str, has_json: bool) -> dict[str, str]:
        headers: dict[str, str] = {}
        if auth:
            if self._signer is None:
                raise AuthError("Upbit private API requires credentials")
            headers["Authorization"] = self._signer.build_authorization_header(query_string=query_string)
        if has_json:
            headers["Content-Type"] = "application/json; charset=utf-8"
        return headers

    @staticmethod
    def _query_string_for_auth(
        method: str,
        *,
        params: ParamsInput | None,
        json_body: dict[str, Any] | None,
    ) -> str:
        if method in {"GET", "DELETE"}:
            return build_query_string(params)
        if method in {"POST", "PUT", "PATCH"}:
            return build_query_string(json_body)
        return ""

    def _sleep_retry_backoff(self, attempt: int) -> None:
        base_ms = self._settings.retry.base_backoff_ms
        max_ms = self._settings.retry.max_backoff_ms
        delay_ms = min(base_ms * (2 ** max(attempt - 1, 0)), max_ms)
        time.sleep(delay_ms / 1000.0)

    def _log_event(
        self,
        *,
        method: str,
        endpoint: str,
        status: int | None,
        latency_ms: float,
        remaining_req: str | None = None,
        request_id: str | None = None,
        error_name: str | None = None,
        error_message: str | None = None,
    ) -> None:
        log_rest_event(
            self._logger,
            method=method,
            endpoint=endpoint,
            status=status,
            latency_ms=latency_ms,
            remaining_req=remaining_req,
            request_id=request_id,
            error_name=error_name,
            error_message=error_message,
        )


def _decode_response_json(response: httpx.Response) -> JSONValue:
    if not response.content:
        return {}
    content_type = response.headers.get("Content-Type", "").lower()
    if "json" in content_type:
        return response.json()
    try:
        return response.json()
    except ValueError:
        return {"text": response.text}


def _parse_upbit_error(response: httpx.Response) -> tuple[str | None, str | None]:
    try:
        payload = response.json()
    except ValueError:
        return (None, response.text or None)

    if isinstance(payload, dict):
        nested = payload.get("error")
        if isinstance(nested, dict):
            error_name = nested.get("name")
            error_message = nested.get("message")
            return (str(error_name) if error_name else None, str(error_message) if error_message else None)
        if "message" in payload:
            return (None, str(payload["message"]))
    return (None, None)


def _extract_ban_cooldown_seconds(response: httpx.Response, fallback: int) -> int:
    retry_after = response.headers.get("Retry-After")
    if retry_after:
        try:
            return max(int(float(retry_after)), 1)
        except ValueError:
            pass

    _, message = _parse_upbit_error(response)
    if message:
        # Examples: "blocked for 30 seconds", "1 minute cooldown"
        lowered = message.lower()
        match = re.search(r"(\d+)", lowered)
        if match:
            value = max(int(match.group(1)), 1)
            if any(token in lowered for token in ("minute", "minutes", "min", "\ubd84")):
                return value * 60
            return value

    return max(int(fallback), 1)


def _build_client_error(
    *,
    status_code: int,
    error_name: str | None,
    error_message: str,
    endpoint: str,
    method: str,
    retriable: bool,
) -> UpbitError:
    kwargs = {
        "message": error_message,
        "status_code": status_code,
        "error_name": error_name,
        "endpoint": endpoint,
        "method": method,
        "retriable": retriable,
    }
    if status_code == 401:
        return AuthError(**kwargs)
    if status_code in (400, 404, 422):
        return ValidationError(**kwargs)
    return ClientRequestError(**kwargs)
