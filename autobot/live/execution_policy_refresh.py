from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from autobot.models.live_execution_policy import build_live_execution_survival_model

from .state_store import LiveStateStore

DEFAULT_CHECKPOINT_NAME = "live_execution_policy_model"


def build_execution_policy_refresh_payload(
    *,
    store: LiveStateStore,
    lookback_days: int = 14,
    limit: int = 5000,
) -> dict[str, Any]:
    now_ts_ms = int(time.time() * 1000)
    since_ts_ms = now_ts_ms - (max(int(lookback_days), 1) * 86_400_000)
    attempts = store.list_execution_attempts(final_only=True, since_ts_ms=since_ts_ms, limit=max(int(limit), 1))
    model = build_live_execution_survival_model(attempts=attempts)
    return {
        "policy": "live_execution_policy_refresh_v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "db_path": str(store.db_path),
        "lookback_days": max(int(lookback_days), 1),
        "rows_total": int(len(attempts)),
        "model": model,
    }


def refresh_execution_policy(
    *,
    db_path: Path,
    output_path: Path | None = None,
    lookback_days: int = 14,
    limit: int = 5000,
    checkpoint_name: str = DEFAULT_CHECKPOINT_NAME,
) -> dict[str, Any]:
    with LiveStateStore(db_path) as store:
        payload = build_execution_policy_refresh_payload(
            store=store,
            lookback_days=lookback_days,
            limit=limit,
        )
        store.set_checkpoint(
            name=str(checkpoint_name).strip() or DEFAULT_CHECKPOINT_NAME,
            payload=payload,
            ts_ms=int(time.time() * 1000),
        )
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")
    return payload


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Refresh live execution policy artifact from execution_attempts.")
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--output-path")
    parser.add_argument("--lookback-days", type=int, default=14)
    parser.add_argument("--limit", type=int, default=5000)
    parser.add_argument("--checkpoint-name", default=DEFAULT_CHECKPOINT_NAME)
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    payload = refresh_execution_policy(
        db_path=Path(str(args.db_path)),
        output_path=(Path(str(args.output_path)) if args.output_path else None),
        lookback_days=max(int(args.lookback_days), 1),
        limit=max(int(args.limit), 1),
        checkpoint_name=str(args.checkpoint_name),
    )
    if args.output_path:
        print(str(args.output_path))
    else:
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
