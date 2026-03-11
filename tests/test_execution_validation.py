from __future__ import annotations

import csv
import json
from pathlib import Path

from autobot.models.execution_validation import build_execution_validation_summary


def test_build_execution_validation_summary_reads_run_artifacts_and_builds_sortino_folds(tmp_path: Path) -> None:
    run_dir = tmp_path / "run-001"
    run_dir.mkdir(parents=True, exist_ok=True)

    equity_rows = []
    realized = 0.0
    equity = 50_000.0
    for index in range(12):
        ts_ms = index * 1_800_000
        if index % 2 == 1:
            realized += 50.0
            equity += 50.0
        equity_rows.append(
            {
                "ts_ms": ts_ms,
                "equity_quote": equity,
                "cash_free": equity,
                "cash_locked": 0.0,
                "realized_pnl_quote": realized,
                "unrealized_pnl_quote": 0.0,
                "open_positions": 1,
            }
        )
    with (run_dir / "equity.csv").open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(
            fp,
            fieldnames=[
                "ts_ms",
                "equity_quote",
                "cash_free",
                "cash_locked",
                "realized_pnl_quote",
                "unrealized_pnl_quote",
                "open_positions",
            ],
        )
        writer.writeheader()
        for row in equity_rows:
            writer.writerow(row)

    fill_rows = [
        {"ts_ms": (index * 3_600_000) + 600_000, "market": "KRW-BTC"}
        for index in range(6)
    ]
    with (run_dir / "fills.jsonl").open("w", encoding="utf-8") as fp:
        for row in fill_rows:
            fp.write(json.dumps(row) + "\n")

    report = build_execution_validation_summary(
        {"run_dir": str(run_dir)},
        window_minutes=60,
        fold_count=3,
        min_active_windows=3,
    )

    assert report["comparable"] is True
    assert report["active_windows"] == 6
    assert report["effective_fold_count"] == 3
    assert report["comparable_fold_count"] == 3
    assert report["objective_score"] > 0.0
    assert len(report["folds"]) == 3
