"""Structured logging helpers for Upbit HTTP calls."""

from __future__ import annotations

import json
import logging
from typing import Any


def get_upbit_logger() -> logging.Logger:
    logger = logging.getLogger("autobot.upbit")
    if not logger.handlers:
        logger.addHandler(logging.NullHandler())
    return logger


def log_rest_event(
    logger: logging.Logger,
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
    record: dict[str, Any] = {
        "event": "upbit_rest",
        "method": method.upper(),
        "endpoint": endpoint,
        "status": status,
        "latency_ms": round(latency_ms, 3),
        "remaining_req": remaining_req,
        "request_id": request_id,
        "error_name": error_name,
        "error_message": error_message,
    }
    logger.info(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
