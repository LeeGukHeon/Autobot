"""Upbit REST client package."""

from .auth_jwt import UpbitJwtSigner, hash_query_string
from .config import (
    UpbitCredentials,
    UpbitRateLimitSettings,
    UpbitRetrySettings,
    UpbitSettings,
    UpbitTimeoutSettings,
    UpbitWebSocketKeepaliveSettings,
    UpbitWebSocketRateLimitSettings,
    UpbitWebSocketReconnectSettings,
    UpbitWebSocketSettings,
    load_upbit_credentials,
    load_upbit_settings,
    require_upbit_credentials,
)
from .error_policy import classify_status
from .exceptions import (
    AuthError,
    ClientRequestError,
    ConfigError,
    NetworkError,
    RateLimitError,
    ServerError,
    UpbitError,
    ValidationError,
)
from .querystring import build_query_string, normalize_param_pairs
from .rate_limiter import UpbitRateLimiter
from .remaining_req import parse_remaining_req_header

try:
    from .http_client import UpbitHttpClient
    from .private import UpbitPrivateClient
    from .public import UpbitPublicClient
except ModuleNotFoundError:  # pragma: no cover - optional dependency guard
    UpbitHttpClient = None  # type: ignore[assignment]
    UpbitPrivateClient = None  # type: ignore[assignment]
    UpbitPublicClient = None  # type: ignore[assignment]

__all__ = [
    "AuthError",
    "ClientRequestError",
    "ConfigError",
    "NetworkError",
    "RateLimitError",
    "ServerError",
    "UpbitCredentials",
    "UpbitError",
    "UpbitHttpClient",
    "UpbitJwtSigner",
    "UpbitPrivateClient",
    "UpbitPublicClient",
    "UpbitRateLimitSettings",
    "UpbitRateLimiter",
    "UpbitRetrySettings",
    "UpbitSettings",
    "UpbitTimeoutSettings",
    "UpbitWebSocketKeepaliveSettings",
    "UpbitWebSocketRateLimitSettings",
    "UpbitWebSocketReconnectSettings",
    "UpbitWebSocketSettings",
    "ValidationError",
    "build_query_string",
    "classify_status",
    "hash_query_string",
    "load_upbit_credentials",
    "load_upbit_settings",
    "normalize_param_pairs",
    "parse_remaining_req_header",
    "require_upbit_credentials",
]
