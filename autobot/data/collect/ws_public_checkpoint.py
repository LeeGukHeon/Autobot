"""Checkpoint helpers for resumable public websocket collection."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_ws_checkpoint(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    return payload


def save_ws_checkpoint(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    tmp_path.replace(path)


def update_ws_checkpoint(
    state: dict[str, Any],
    *,
    run_id: str,
    reconnect_count: int,
    ping_sent_count: int,
    pong_rx_count: int,
    files_written: int,
    bytes_written: int,
    updated_at_ms: int,
) -> dict[str, Any]:
    state["last_run_id"] = str(run_id)
    state["last_reconnect_count"] = int(reconnect_count)
    state["last_ping_sent_count"] = int(ping_sent_count)
    state["last_pong_rx_count"] = int(pong_rx_count)
    state["last_files_written"] = int(files_written)
    state["last_bytes_written"] = int(bytes_written)
    state["updated_at_ms"] = int(updated_at_ms)
    return state
