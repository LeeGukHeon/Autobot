"""Run-id helpers for backtest runtime."""

from __future__ import annotations

import hashlib
import json
import time
import uuid


def build_backtest_run_id(
    *,
    tf: str,
    markets: list[str],
    from_ts_ms: int,
    to_ts_ms: int,
    seed: int,
) -> str:
    payload = {
        "tf": str(tf).strip().lower(),
        "markets": sorted(str(item).strip().upper() for item in markets),
        "from_ts_ms": int(from_ts_ms),
        "to_ts_ms": int(to_ts_ms),
        "seed": int(seed),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    digest = hashlib.sha1(encoded).hexdigest()[:10]
    token = uuid.uuid4().hex[:8]
    return f"backtest-{time.strftime('%Y%m%d-%H%M%S')}-{digest}-{token}"
