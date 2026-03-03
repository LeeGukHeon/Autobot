from __future__ import annotations

import json
from pathlib import Path

from autobot.data.collect import (
    TicksCollectOptions,
    TicksFetchResult,
    collect_ticks_from_plan,
    validate_ticks_raw_dataset,
)


def test_collect_and_validate_ticks_with_offline_fixture(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw_ticks" / "upbit" / "trades"
    meta_dir = tmp_path / "raw_ticks" / "upbit" / "_meta"
    plan_path = meta_dir / "ticks_plan.json"
    plan_path.parent.mkdir(parents=True, exist_ok=True)

    plan = {
        "targets": [
            {
                "market": "KRW-BTC",
                "days_ago": 1,
                "target_key": "KRW-BTC|1",
                "reason": "TEST",
            }
        ]
    }
    plan_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")

    class _FakeClient:
        def fetch_trades_ticks(
            self,
            *,
            market: str,
            days_ago: int,
            start_cursor: str | None = None,
            max_pages: int | None = None,
            max_requests: int | None = None,
            count: int = 200,
        ) -> TicksFetchResult:
            ticks = (
                _tick_row(1_772_377_200_000, 100.0, 10_001),
                _tick_row(1_772_377_260_000, 101.0, 10_002),
                _tick_row(1_772_377_320_000, 102.0, 10_003),
            )
            return TicksFetchResult(
                market=market,
                days_ago=days_ago,
                ticks=ticks,
                calls_made=1,
                throttled_count=0,
                backoff_count=0,
                pages_collected=1,
                loop_guard_triggered=False,
                truncated_by_budget=False,
                start_cursor=start_cursor,
                last_cursor="10003",
                raw_rows=len(ticks),
                unique_rows=len(ticks),
            )

    collect_summary = collect_ticks_from_plan(
        TicksCollectOptions(
            plan_path=plan_path,
            raw_root=raw_root,
            meta_dir=meta_dir,
            dry_run=False,
            retention_days=3650,
        ),
        client=_FakeClient(),  # type: ignore[arg-type]
    )

    assert collect_summary.fail_targets == 0
    assert collect_summary.ok_targets == 1
    assert collect_summary.rows_collected_total == 3

    validate_summary = validate_ticks_raw_dataset(
        raw_root=raw_root,
        report_path=meta_dir / "ticks_validate_report.json",
    )
    assert validate_summary.fail_files == 0
    assert validate_summary.ok_files == 1
    assert validate_summary.rows_total == 3


def _tick_row(timestamp_ms: int, trade_price: float, sequential_id: int) -> dict:
    return {
        "market": "KRW-BTC",
        "timestamp_ms": timestamp_ms,
        "trade_price": trade_price,
        "trade_volume": 0.1,
        "ask_bid": "BID",
        "sequential_id": sequential_id,
        "days_ago": 1,
        "collected_at_ms": timestamp_ms + 5_000,
    }
