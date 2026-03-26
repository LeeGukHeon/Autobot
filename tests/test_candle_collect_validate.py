from __future__ import annotations

import json
from pathlib import Path

from autobot.data.collect import (
    CandleCollectOptions,
    CandleFetchResult,
    collect_candles_from_plan,
    validate_candles_api_dataset,
)
from autobot.data.inventory import parse_utc_ts_ms
from autobot.upbit import ValidationError


def test_collect_and_validate_with_offline_fixture(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    collect_meta = tmp_path / "collect_meta"
    collect_meta.mkdir(parents=True, exist_ok=True)

    start_ts_ms = parse_utc_ts_ms("2026-03-01T00:00:00+00:00")
    end_ts_ms = parse_utc_ts_ms("2026-03-01T00:02:00+00:00")
    assert start_ts_ms is not None
    assert end_ts_ms is not None

    plan_path = collect_meta / "plan.json"
    plan = {
        "base_dataset": "candles_v1",
        "window": {
            "start_ts_ms": start_ts_ms,
            "end_ts_ms": end_ts_ms,
            "start_utc": "2026-03-01T00:00:00+00:00",
            "end_utc": "2026-03-01T00:02:00+00:00",
        },
        "targets": [
            {
                "market": "KRW-BTC",
                "tf": "1m",
                "need_from_ts_ms": start_ts_ms,
                "need_to_ts_ms": end_ts_ms,
                "reason": "NO_LOCAL_DATA",
            }
        ],
    }
    plan_path.write_text(json.dumps(plan, indent=2), encoding="utf-8")

    class _FakeClient:
        def fetch_minutes_range(
            self,
            *,
            market: str,
            tf: str,
            start_ts_ms: int,
            end_ts_ms: int,
            max_requests: int | None = None,
        ) -> CandleFetchResult:
            candles = (
                _candle_row(start_ts_ms, 100.0),
                _candle_row(start_ts_ms + 60_000, 101.0),
                _candle_row(end_ts_ms, 102.0),
            )
            return CandleFetchResult(
                market=market,
                tf=tf,
                start_ts_ms=start_ts_ms,
                end_ts_ms=end_ts_ms,
                candles=candles,
                calls_made=1,
                throttled_count=0,
                backoff_count=0,
                loop_guard_triggered=False,
                truncated_by_budget=False,
            )

    collect_summary = collect_candles_from_plan(
        CandleCollectOptions(
            plan_path=plan_path,
            parquet_root=parquet_root,
            out_dataset="candles_api_v1",
            collect_meta_dir=collect_meta,
            dry_run=False,
        ),
        client=_FakeClient(),
    )
    assert collect_summary.fail_targets == 0
    assert collect_summary.ok_targets == 1
    assert collect_summary.calls_made == 1

    validate_summary = validate_candles_api_dataset(
        parquet_root=parquet_root,
        dataset_name="candles_api_v1",
        plan_path=plan_path,
        report_path=collect_meta / "validate.json",
    )
    assert validate_summary.fail_files == 0
    assert validate_summary.ok_files == 1


def test_collect_treats_delisted_market_404_as_warning(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    collect_meta = tmp_path / "collect_meta"
    collect_meta.mkdir(parents=True, exist_ok=True)

    start_ts_ms = parse_utc_ts_ms("2026-03-16T00:00:00+00:00")
    end_ts_ms = parse_utc_ts_ms("2026-03-16T00:10:00+00:00")
    assert start_ts_ms is not None
    assert end_ts_ms is not None

    plan_path = collect_meta / "plan.json"
    plan = {
        "base_dataset": "candles_v1",
        "window": {
            "start_ts_ms": start_ts_ms,
            "end_ts_ms": end_ts_ms,
            "start_utc": "2026-03-16T00:00:00+00:00",
            "end_utc": "2026-03-16T00:10:00+00:00",
        },
        "targets": [
            {
                "market": "KRW-FLOW",
                "tf": "15m",
                "need_from_ts_ms": start_ts_ms,
                "need_to_ts_ms": end_ts_ms,
                "reason": "MISSING_TAIL",
            }
        ],
    }
    plan_path.write_text(json.dumps(plan, indent=2), encoding="utf-8")

    class _DelistedClient:
        def fetch_minutes_range(
            self,
            *,
            market: str,
            tf: str,
            start_ts_ms: int,
            end_ts_ms: int,
            max_requests: int | None = None,
        ) -> CandleFetchResult:
            raise ValidationError(
                "Code not found",
                status_code=404,
                endpoint=f"/v1/candles/minutes/{tf.removesuffix('m')}",
                method="GET",
            )

    collect_summary = collect_candles_from_plan(
        CandleCollectOptions(
            plan_path=plan_path,
            parquet_root=parquet_root,
            out_dataset="candles_api_v1",
            collect_meta_dir=collect_meta,
            dry_run=False,
        ),
        client=_DelistedClient(),
    )

    assert collect_summary.fail_targets == 0
    assert collect_summary.warn_targets == 1
    assert collect_summary.details[0]["status"] == "WARN"
    assert collect_summary.details[0]["reasons"] == ["DELISTED_OR_INACTIVE_MARKET"]


def test_collect_second_candles_uses_second_artifact_paths_and_sparse_validation(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    collect_meta = tmp_path / "collect_meta"
    collect_meta.mkdir(parents=True, exist_ok=True)

    start_ts_ms = parse_utc_ts_ms("2026-03-27T00:00:00+00:00")
    end_ts_ms = parse_utc_ts_ms("2026-03-27T00:00:10+00:00")
    assert start_ts_ms is not None
    assert end_ts_ms is not None

    plan_path = collect_meta / "plan_second.json"
    plan = {
        "base_dataset": "candles_second_v1",
        "window": {
            "start_ts_ms": start_ts_ms,
            "end_ts_ms": end_ts_ms,
            "start_utc": "2026-03-27T00:00:00+00:00",
            "end_utc": "2026-03-27T00:00:10+00:00",
        },
        "targets": [
            {
                "market": "KRW-BTC",
                "tf": "1s",
                "need_from_ts_ms": start_ts_ms,
                "need_to_ts_ms": end_ts_ms,
                "reason": "NO_LOCAL_DATA",
            }
        ],
    }
    plan_path.write_text(json.dumps(plan, indent=2), encoding="utf-8")

    class _FakeSecondClient:
        def fetch_candles_range(
            self,
            *,
            market: str,
            tf: str,
            start_ts_ms: int,
            end_ts_ms: int,
            max_requests: int | None = None,
        ) -> CandleFetchResult:
            assert tf == "1s"
            candles = (
                _candle_row(start_ts_ms, 100.0),
                _candle_row(start_ts_ms + 5_000, 101.0),
                _candle_row(end_ts_ms, 102.0),
            )
            return CandleFetchResult(
                market=market,
                tf=tf,
                start_ts_ms=start_ts_ms,
                end_ts_ms=end_ts_ms,
                candles=candles,
                calls_made=1,
                throttled_count=0,
                backoff_count=0,
                loop_guard_triggered=False,
                truncated_by_budget=False,
            )

    collect_summary = collect_candles_from_plan(
        CandleCollectOptions(
            plan_path=plan_path,
            parquet_root=parquet_root,
            out_dataset="candles_second_v1",
            collect_meta_dir=collect_meta,
            dry_run=False,
        ),
        client=_FakeSecondClient(),
    )
    assert collect_summary.fail_targets == 0
    assert collect_summary.ok_targets == 1
    assert collect_summary.collect_report_file.name == "candle_second_collect_report.json"

    validate_summary = validate_candles_api_dataset(
        parquet_root=parquet_root,
        dataset_name="candles_second_v1",
        plan_path=plan_path,
        report_path=collect_meta / "candle_second_validate_report.json",
    )
    assert validate_summary.fail_files == 0
    assert validate_summary.ok_files == 1


def _candle_row(ts_ms: int, price: float) -> dict:
    return {
        "ts_ms": ts_ms,
        "open": price,
        "high": price + 1.0,
        "low": price - 1.0,
        "close": price + 0.2,
        "volume_base": 10.0,
        "volume_quote": 1000.0,
        "volume_quote_est": False,
    }
