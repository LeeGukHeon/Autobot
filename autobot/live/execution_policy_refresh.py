from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from autobot.models.live_execution_policy import build_live_execution_contract

from .state_store import LiveStateStore

DEFAULT_CHECKPOINT_NAME = "live_execution_policy_model"


def load_execution_attempts_from_db(
    *,
    db_path: Path,
    lookback_days: int = 14,
    limit: int = 5000,
) -> list[dict[str, Any]]:
    with LiveStateStore(db_path) as store:
        now_ts_ms = int(time.time() * 1000)
        since_ts_ms = now_ts_ms - (max(int(lookback_days), 1) * 86_400_000)
        attempts = store.list_execution_attempts(
            final_only=True,
            since_ts_ms=since_ts_ms,
            limit=max(int(limit), 1),
        )
    payloads: list[dict[str, Any]] = []
    for item in attempts:
        row = dict(item)
        row["source_db_path"] = str(db_path)
        payloads.append(row)
    return payloads


def build_execution_policy_refresh_payload(
    *,
    store: LiveStateStore,
    lookback_days: int = 14,
    limit: int = 5000,
) -> dict[str, Any]:
    now_ts_ms = int(time.time() * 1000)
    since_ts_ms = now_ts_ms - (max(int(lookback_days), 1) * 86_400_000)
    attempts = store.list_execution_attempts(final_only=True, since_ts_ms=since_ts_ms, limit=max(int(limit), 1))
    execution_contract = build_live_execution_contract(attempts=attempts)
    return {
        "policy": "live_execution_policy_refresh_v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "db_path": str(store.db_path),
        "lookback_days": max(int(lookback_days), 1),
        "rows_total": int(len(attempts)),
        "model": dict(execution_contract.get("fill_model") or {}),
        "execution_twin": dict(execution_contract.get("execution_twin") or {}),
        "execution_contract": execution_contract,
    }


def build_combined_execution_policy_refresh_payload(
    *,
    db_paths: list[Path],
    lookback_days: int = 14,
    limit_per_db: int = 5000,
    combined_limit: int | None = None,
) -> dict[str, Any]:
    attempts: list[dict[str, Any]] = []
    db_row_counts: list[dict[str, Any]] = []
    for db_path in db_paths:
        loaded = load_execution_attempts_from_db(
            db_path=db_path,
            lookback_days=lookback_days,
            limit=limit_per_db,
        )
        attempts.extend(loaded)
        db_row_counts.append(
            {
                "db_path": str(db_path),
                "rows_total": int(len(loaded)),
            }
        )
    attempts.sort(
        key=lambda item: (
            int(item.get("submitted_ts_ms") or 0),
            int(item.get("updated_ts") or 0),
        ),
        reverse=True,
    )
    if combined_limit is not None and int(combined_limit) > 0:
        attempts = attempts[: int(combined_limit)]
    execution_contract = build_live_execution_contract(attempts=attempts)
    return {
        "policy": "live_execution_policy_refresh_v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "db_paths": [str(path) for path in db_paths],
        "lookback_days": max(int(lookback_days), 1),
        "rows_total": int(len(attempts)),
        "db_row_counts": db_row_counts,
        "model": dict(execution_contract.get("fill_model") or {}),
        "execution_twin": dict(execution_contract.get("execution_twin") or {}),
        "execution_contract": execution_contract,
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


def refresh_combined_execution_policy(
    *,
    db_paths: list[Path],
    output_path: Path | None = None,
    lookback_days: int = 14,
    limit_per_db: int = 5000,
    combined_limit: int | None = None,
    checkpoint_name: str = DEFAULT_CHECKPOINT_NAME,
) -> dict[str, Any]:
    resolved_paths = [Path(path) for path in db_paths if str(path)]
    payload = build_combined_execution_policy_refresh_payload(
        db_paths=resolved_paths,
        lookback_days=lookback_days,
        limit_per_db=limit_per_db,
        combined_limit=combined_limit,
    )
    for db_path in resolved_paths:
        with LiveStateStore(db_path) as store:
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
    parser.add_argument("--db-path")
    parser.add_argument("--db-paths")
    parser.add_argument("--output-path")
    parser.add_argument("--lookback-days", type=int, default=14)
    parser.add_argument("--limit", type=int, default=5000)
    parser.add_argument("--combined-limit", type=int)
    parser.add_argument("--checkpoint-name", default=DEFAULT_CHECKPOINT_NAME)
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    if args.db_paths:
        raw_paths = [Path(item.strip()) for item in str(args.db_paths).split(",") if item.strip()]
        payload = refresh_combined_execution_policy(
            db_paths=raw_paths,
            output_path=(Path(str(args.output_path)) if args.output_path else None),
            lookback_days=max(int(args.lookback_days), 1),
            limit_per_db=max(int(args.limit), 1),
            combined_limit=(max(int(args.combined_limit), 1) if args.combined_limit is not None else None),
            checkpoint_name=str(args.checkpoint_name),
        )
    else:
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
