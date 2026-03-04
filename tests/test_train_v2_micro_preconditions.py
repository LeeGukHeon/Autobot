from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl

from autobot.models.train_v2_micro import TrainV2MicroOptions, check_v2_micro_preconditions


def _write_feature_dates(dataset_root: Path, *, days: int, trade_source: str = "rest") -> None:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    for offset in range(days):
        day = start + timedelta(days=offset)
        day_text = day.strftime("%Y-%m-%d")
        base_ts = int(day.timestamp() * 1000)
        part_dir = dataset_root / "tf=5m" / "market=KRW-BTC" / f"date={day_text}"
        part_dir.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            {
                "ts_ms": [base_ts + 300_000 * i for i in range(3)],
                "candle_ok": [True, True, True],
                "y_reg": [0.01, 0.02, -0.01],
                "y_cls": [1, 1, 0],
                "m_micro_available": [True, True, True],
                "m_trade_source": [trade_source, trade_source, trade_source],
                "m_trade_events": [3, 2, 1],
                "m_book_events": [0, 0, 0],
                "m_trade_coverage_ms": [240_000, 240_000, 180_000],
                "m_book_coverage_ms": [0, 0, 0],
                "m_micro_trade_available": [True, True, True],
                "m_micro_book_available": [False, False, False],
            }
        ).write_parquet(part_dir / "part-000.parquet")


def _make_options(dataset_root: Path) -> TrainV2MicroOptions:
    return TrainV2MicroOptions(
        dataset_root=dataset_root,
        registry_root=dataset_root / "_registry",
        logs_root=dataset_root / "_logs",
        model_family="train_v2_micro",
        tf="5m",
        quote="KRW",
        top_n=20,
        start="2026-01-01",
        end="2026-01-31",
        feature_set="v2",
        label_set="v1",
        task="cls",
        booster_sweep_trials=1,
        seed=42,
        nthread=1,
        batch_rows=10_000,
        train_ratio=0.7,
        valid_ratio=0.15,
        test_ratio=0.15,
        embargo_bars=2,
        fee_bps_est=10.0,
        safety_bps=5.0,
        ev_scan_steps=20,
        ev_min_selected=5,
        min_rows_for_train=10,
        min_distinct_dates=7,
    )


def test_preconditions_fail_when_distinct_dates_below_min(tmp_path: Path) -> None:
    dataset_root = tmp_path / "features_v2"
    _write_feature_dates(dataset_root, days=6, trade_source="rest")
    options = _make_options(dataset_root)

    report = check_v2_micro_preconditions(options=options)

    assert report["ready"] is False
    assert "DISTINCT_DATES_BELOW_MIN" in report["fail_reasons"]


def test_preconditions_fail_when_micro_integrity_invalid(tmp_path: Path) -> None:
    dataset_root = tmp_path / "features_v2"
    _write_feature_dates(dataset_root, days=8, trade_source="invalid-source")
    options = _make_options(dataset_root)

    report = check_v2_micro_preconditions(options=options)

    assert report["ready"] is False
    assert "MICRO_INTEGRITY_FAIL" in report["fail_reasons"]
