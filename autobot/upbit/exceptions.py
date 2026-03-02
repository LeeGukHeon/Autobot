"""Custom exceptions for Upbit REST integration."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(eq=False)
class UpbitError(Exception):
    """Base exception with normalized Upbit error metadata."""

    message: str
    status_code: int | None = None
    error_name: str | None = None
    endpoint: str | None = None
    method: str | None = None
    retriable: bool = False

    def __str__(self) -> str:
        details: list[str] = [self.message]
        if self.status_code is not None:
            details.append(f"status={self.status_code}")
        if self.error_name:
            details.append(f"error={self.error_name}")
        if self.method and self.endpoint:
            details.append(f"{self.method} {self.endpoint}")
        return " | ".join(details)


class ConfigError(UpbitError):
    """Raised when Upbit config is invalid or incomplete."""


class AuthError(UpbitError):
    """Raised when JWT credentials are missing or invalid."""


class ValidationError(UpbitError):
    """Raised when request arguments are invalid."""


class ClientRequestError(UpbitError):
    """Raised for non-auth 4xx responses."""


class NetworkError(UpbitError):
    """Raised for network-level failures."""

    def __init__(self, message: str, **kwargs: object) -> None:
        super().__init__(message=message, retriable=True, **kwargs)


class ServerError(UpbitError):
    """Raised for 5xx responses."""

    def __init__(self, message: str, **kwargs: object) -> None:
        super().__init__(message=message, retriable=True, **kwargs)


class RateLimitError(UpbitError):
    """Raised for 429/418 rate-limit responses."""

    def __init__(
        self,
        message: str,
        *,
        cooldown_sec: float | None = None,
        banned: bool = False,
        **kwargs: object,
    ) -> None:
        super().__init__(message=message, retriable=True, **kwargs)
        self.cooldown_sec = cooldown_sec
        self.banned = banned
