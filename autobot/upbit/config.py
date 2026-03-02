"""Config loader for Upbit REST integration."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

from .exceptions import ConfigError


@dataclass(frozen=True)
class UpbitTimeoutSettings:
    connect_sec: float = 3.0
    read_sec: float = 10.0
    write_sec: float = 10.0


@dataclass(frozen=True)
class UpbitAuthSettings:
    access_key_env: str = "UPBIT_ACCESS_KEY"
    secret_key_env: str = "UPBIT_SECRET_KEY"


@dataclass(frozen=True)
class UpbitRateLimitSettings:
    enabled: bool = True
    ban_cooldown_sec: int = 60
    group_defaults: dict[str, float] | None = None

    def to_group_rates(self) -> dict[str, float]:
        raw = dict(self.group_defaults or {})
        return {
            "market": float(raw.get("market_rps", 10)),
            "candle": float(raw.get("candle_rps", 10)),
            "trade": float(raw.get("trade_rps", 10)),
            "ticker": float(raw.get("ticker_rps", 10)),
            "orderbook": float(raw.get("orderbook_rps", 10)),
            "default": float(raw.get("exchange_default_rps", 30)),
            "order": float(raw.get("order_rps", 8)),
            "order-test": float(raw.get("order_test_rps", 8)),
            "order-cancel-all": float(raw.get("order_cancel_all_rps_2s", 1)) / 2.0,
        }


@dataclass(frozen=True)
class UpbitRetrySettings:
    max_attempts: int = 3
    base_backoff_ms: int = 200
    max_backoff_ms: int = 2000


@dataclass(frozen=True)
class UpbitWebSocketKeepaliveSettings:
    ping_interval_sec: float = 30.0
    ping_timeout_sec: float = 10.0
    allow_text_ping: bool = True


@dataclass(frozen=True)
class UpbitWebSocketRateLimitSettings:
    connect_rps: int = 5
    message_rps: int = 5
    message_rpm: int = 100


@dataclass(frozen=True)
class UpbitWebSocketReconnectSettings:
    enabled: bool = True
    base_delay_ms: int = 500
    max_delay_ms: int = 15000
    jitter_ms: int = 250


@dataclass(frozen=True)
class UpbitWebSocketSettings:
    public_url: str = "wss://api.upbit.com/websocket/v1"
    private_url: str = "wss://api.upbit.com/websocket/v1/private"
    format: str = "SIMPLE_LIST"
    codes_per_connection: int = 120
    max_connections: int = 5
    keepalive: UpbitWebSocketKeepaliveSettings = field(default_factory=UpbitWebSocketKeepaliveSettings)
    ratelimit: UpbitWebSocketRateLimitSettings = field(default_factory=UpbitWebSocketRateLimitSettings)
    reconnect: UpbitWebSocketReconnectSettings = field(default_factory=UpbitWebSocketReconnectSettings)


@dataclass(frozen=True)
class UpbitCredentials:
    access_key: str
    secret_key: str


@dataclass(frozen=True)
class UpbitSettings:
    base_url: str
    timeout: UpbitTimeoutSettings
    auth: UpbitAuthSettings
    ratelimit: UpbitRateLimitSettings
    retry: UpbitRetrySettings
    websocket: UpbitWebSocketSettings


DEFAULT_UPBIT_SETTINGS: dict[str, Any] = {
    "base_url": "https://api.upbit.com",
    "timeout": {
        "connect_sec": 3,
        "read_sec": 10,
        "write_sec": 10,
    },
    "auth": {
        "access_key_env": "UPBIT_ACCESS_KEY",
        "secret_key_env": "UPBIT_SECRET_KEY",
    },
    "ratelimit": {
        "enabled": True,
        "ban_cooldown_sec": 60,
        "group_defaults": {
            "market_rps": 10,
            "candle_rps": 10,
            "trade_rps": 10,
            "ticker_rps": 10,
            "orderbook_rps": 10,
            "exchange_default_rps": 30,
            "order_rps": 8,
            "order_test_rps": 8,
            "order_cancel_all_rps_2s": 1,
        },
    },
    "retry": {
        "max_attempts": 3,
        "base_backoff_ms": 200,
        "max_backoff_ms": 2000,
    },
    "websocket": {
        "public_url": "wss://api.upbit.com/websocket/v1",
        "private_url": "wss://api.upbit.com/websocket/v1/private",
        "format": "SIMPLE_LIST",
        "codes_per_connection": 120,
        "max_connections": 5,
        "keepalive": {
            "ping_interval_sec": 30,
            "ping_timeout_sec": 10,
            "allow_text_ping": True,
        },
        "ratelimit": {
            "connect_rps": 5,
            "message_rps": 5,
            "message_rpm": 100,
        },
        "reconnect": {
            "enabled": True,
            "base_delay_ms": 500,
            "max_delay_ms": 15000,
            "jitter_ms": 250,
        },
    },
}


def load_upbit_settings(config_dir: str | Path = "config") -> UpbitSettings:
    directory = Path(config_dir)
    _autoload_dotenv(directory)
    base_doc = _load_yaml_doc(directory / "base.yaml")
    upbit_doc = _load_yaml_doc(directory / "upbit.yaml")

    merged = _deep_merge(
        _deep_merge(dict(DEFAULT_UPBIT_SETTINGS), _extract_upbit_section(base_doc, treat_root_as_upbit=False)),
        _extract_upbit_section(upbit_doc, treat_root_as_upbit=True),
    )

    timeout_cfg = merged.get("timeout", {})
    auth_cfg = merged.get("auth", {})
    rate_cfg = merged.get("ratelimit", {})
    retry_cfg = merged.get("retry", {})
    websocket_cfg = merged.get("websocket", {})
    ws_keepalive_cfg = websocket_cfg.get("keepalive", {}) if isinstance(websocket_cfg.get("keepalive"), dict) else {}
    ws_ratelimit_cfg = websocket_cfg.get("ratelimit", {}) if isinstance(websocket_cfg.get("ratelimit"), dict) else {}
    ws_reconnect_cfg = websocket_cfg.get("reconnect", {}) if isinstance(websocket_cfg.get("reconnect"), dict) else {}
    group_defaults_cfg = rate_cfg.get("group_defaults")
    if not isinstance(group_defaults_cfg, dict):
        group_defaults_cfg = {}

    settings = UpbitSettings(
        base_url=str(merged.get("base_url", DEFAULT_UPBIT_SETTINGS["base_url"])),
        timeout=UpbitTimeoutSettings(
            connect_sec=float(timeout_cfg.get("connect_sec", 3)),
            read_sec=float(timeout_cfg.get("read_sec", 10)),
            write_sec=float(timeout_cfg.get("write_sec", 10)),
        ),
        auth=UpbitAuthSettings(
            access_key_env=str(auth_cfg.get("access_key_env", "UPBIT_ACCESS_KEY")),
            secret_key_env=str(auth_cfg.get("secret_key_env", "UPBIT_SECRET_KEY")),
        ),
        ratelimit=UpbitRateLimitSettings(
            enabled=bool(rate_cfg.get("enabled", True)),
            ban_cooldown_sec=int(rate_cfg.get("ban_cooldown_sec", 60)),
            group_defaults=dict(group_defaults_cfg),
        ),
        retry=UpbitRetrySettings(
            max_attempts=max(int(retry_cfg.get("max_attempts", 3)), 1),
            base_backoff_ms=max(int(retry_cfg.get("base_backoff_ms", 200)), 1),
            max_backoff_ms=max(int(retry_cfg.get("max_backoff_ms", 2000)), 1),
        ),
        websocket=UpbitWebSocketSettings(
            public_url=str(websocket_cfg.get("public_url", "wss://api.upbit.com/websocket/v1")),
            private_url=str(websocket_cfg.get("private_url", "wss://api.upbit.com/websocket/v1/private")),
            format=str(websocket_cfg.get("format", "SIMPLE_LIST")).strip().upper(),
            codes_per_connection=max(int(websocket_cfg.get("codes_per_connection", 120)), 1),
            max_connections=max(int(websocket_cfg.get("max_connections", 5)), 1),
            keepalive=UpbitWebSocketKeepaliveSettings(
                ping_interval_sec=max(float(ws_keepalive_cfg.get("ping_interval_sec", 30.0)), 1.0),
                ping_timeout_sec=max(float(ws_keepalive_cfg.get("ping_timeout_sec", 10.0)), 1.0),
                allow_text_ping=bool(ws_keepalive_cfg.get("allow_text_ping", True)),
            ),
            ratelimit=UpbitWebSocketRateLimitSettings(
                connect_rps=max(int(ws_ratelimit_cfg.get("connect_rps", 5)), 1),
                message_rps=max(int(ws_ratelimit_cfg.get("message_rps", 5)), 1),
                message_rpm=max(int(ws_ratelimit_cfg.get("message_rpm", 100)), 1),
            ),
            reconnect=UpbitWebSocketReconnectSettings(
                enabled=bool(ws_reconnect_cfg.get("enabled", True)),
                base_delay_ms=max(int(ws_reconnect_cfg.get("base_delay_ms", 500)), 1),
                max_delay_ms=max(int(ws_reconnect_cfg.get("max_delay_ms", 15000)), 1),
                jitter_ms=max(int(ws_reconnect_cfg.get("jitter_ms", 250)), 0),
            ),
        ),
    )

    if not settings.base_url:
        raise ConfigError("upbit.base_url must be configured")
    return settings


def load_upbit_credentials(settings: UpbitSettings) -> UpbitCredentials | None:
    access_key = (os.getenv(settings.auth.access_key_env) or "").strip()
    secret_key = (os.getenv(settings.auth.secret_key_env) or "").strip()
    if not access_key or not secret_key:
        return None
    return UpbitCredentials(access_key=access_key, secret_key=secret_key)


def require_upbit_credentials(settings: UpbitSettings) -> UpbitCredentials:
    credentials = load_upbit_credentials(settings)
    if credentials is None:
        raise ConfigError(
            "Upbit API keys are not configured. "
            f"Set env vars {settings.auth.access_key_env} and {settings.auth.secret_key_env}."
        )
    return credentials


def _load_yaml_doc(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if isinstance(raw, dict):
        return raw
    return {}


def _extract_upbit_section(doc: dict[str, Any], *, treat_root_as_upbit: bool) -> dict[str, Any]:
    value = doc.get("upbit")
    if isinstance(value, dict):
        return value
    if treat_root_as_upbit:
        return doc
    return {}


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(dict(merged[key]), value)
        else:
            merged[key] = value
    return merged


def _autoload_dotenv(config_dir: Path) -> None:
    """Load .env automatically from the project root or config directory."""

    candidates = [
        config_dir / ".env",
        config_dir.parent / ".env",
        Path.cwd() / ".env",
    ]
    visited: set[Path] = set()
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved in visited:
            continue
        visited.add(resolved)
        if resolved.exists():
            load_dotenv(dotenv_path=resolved, override=False)
