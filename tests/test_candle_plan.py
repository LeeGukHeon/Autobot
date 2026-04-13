from __future__ import annotations

from pathlib import Path

from autobot.data.collect.plan_candles import CandlePlanOptions, generate_candle_topup_plan
from autobot.data.inventory import DAY_MS, parse_utc_ts_ms
from autobot.data.manifest import append_manifest_rows


def test_plan_applies_1m_backfill_limit(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    dataset_root = parquet_root / "candles_v1"
    manifest_file = dataset_root / "_meta" / "manifest.parquet"

    end_ts_ms = parse_utc_ts_ms("2026-03-01", end_of_day=True)
    assert end_ts_ms is not None

    append_manifest_rows(
        manifest_file,
        [
            {
                "quote": "KRW",
                "symbol": "BTC",
                "market": "KRW-BTC",
                "tf": "1m",
                "rows": 100,
                "min_ts_ms": end_ts_ms - (400 * DAY_MS),
                "max_ts_ms": end_ts_ms - (300 * DAY_MS),
                "status": "OK",
                "ingested_at": 1,
                "reasons_json": "[]",
            }
        ],
    )

    options = CandlePlanOptions(
        parquet_root=parquet_root,
        base_dataset="candles_v1",
        output_path=tmp_path / "plan.json",
        lookback_months=24,
        tf_set=("1m",),
        quote="KRW",
        market_mode="fixed_list",
        fixed_markets=("KRW-BTC",),
        max_backfill_days_1m=90,
        end_ts_ms=end_ts_ms,
    )
    plan = generate_candle_topup_plan(options)
    targets = [item for item in plan["targets"] if item["market"] == "KRW-BTC" and item["tf"] == "1m"]
    assert len(targets) == 1

    target = targets[0]
    assert int(target["need_from_ts_ms"]) >= int(end_ts_ms - (90 * DAY_MS))
    assert "1M_BACKFILL_LIMIT" in str(target["reason"])


def test_plan_supports_second_dataset_with_market_source_dataset(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    source_manifest = parquet_root / "candles_api_v1" / "_meta" / "manifest.parquet"

    end_ts_ms = parse_utc_ts_ms("2026-03-27", end_of_day=True)
    assert end_ts_ms is not None

    append_manifest_rows(
        source_manifest,
        [
            {
                "quote": "KRW",
                "symbol": "BTC",
                "market": "KRW-BTC",
                "tf": "1m",
                "rows": 100,
                "min_ts_ms": end_ts_ms - (10 * DAY_MS),
                "max_ts_ms": end_ts_ms - DAY_MS,
                "status": "OK",
                "ingested_at": 1,
                "reasons_json": "[]",
            }
        ],
    )

    options = CandlePlanOptions(
        parquet_root=parquet_root,
        base_dataset="candles_second_v1",
        market_source_dataset="candles_api_v1",
        output_path=tmp_path / "plan_second.json",
        lookback_months=6,
        tf_set=("1s",),
        quote="KRW",
        market_mode="top_n_by_recent_value_est",
        top_n=1,
        max_backfill_days_1s=2,
        end_ts_ms=end_ts_ms,
        config_dir=config_dir,
    )
    plan = generate_candle_topup_plan(options)

    assert plan["filters"]["market_source_dataset"] == "candles_api_v1"
    assert plan["constraints"]["per_tf_limits"]["1s_max_backfill_days"] == 2
    assert plan["summary"]["targets"] == 1

    target = plan["targets"][0]
    assert target["market"] == "KRW-BTC"
    assert target["tf"] == "1s"
    assert int(target["need_from_ts_ms"]) >= int(end_ts_ms - (2 * DAY_MS))
    assert "1S_BACKFILL_LIMIT" in str(target["reason"])


def test_plan_filters_inactive_markets_from_fixed_list(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    source_manifest = parquet_root / "candles_api_v1" / "_meta" / "manifest.parquet"
    end_ts_ms = parse_utc_ts_ms("2026-03-27", end_of_day=True)
    assert end_ts_ms is not None

    append_manifest_rows(
        source_manifest,
        [
            {
                "quote": "KRW",
                "symbol": "BTC",
                "market": "KRW-BTC",
                "tf": "1m",
                "rows": 100,
                "min_ts_ms": end_ts_ms - (5 * DAY_MS),
                "max_ts_ms": end_ts_ms - DAY_MS,
                "status": "OK",
                "ingested_at": 1,
                "reasons_json": "[]",
            }
        ],
    )

    options = CandlePlanOptions(
        parquet_root=parquet_root,
        base_dataset="candles_api_v1",
        market_source_dataset="candles_api_v1",
        output_path=tmp_path / "plan.json",
        tf_set=("1m",),
        quote="KRW",
        market_mode="fixed_list",
        fixed_markets=("KRW-BTC", "KRW-FLOW"),
        end_ts_ms=end_ts_ms,
        active_markets_override=("KRW-BTC",),
    )
    plan = generate_candle_topup_plan(options)

    assert plan["selected_markets"] == ["KRW-BTC"]
    active_filter = dict(plan["market_selection"]["active_market_filter"])
    assert active_filter["status"] == "override"
    assert active_filter["dropped_count"] == 1


def test_plan_prioritizes_low_call_high_tf_targets_before_one_minute_targets(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    manifest_file = parquet_root / "candles_v1" / "_meta" / "manifest.parquet"
    end_ts_ms = parse_utc_ts_ms("2026-04-12", end_of_day=True)
    assert end_ts_ms is not None

    append_manifest_rows(
        manifest_file,
        [
            {
                "quote": "KRW",
                "symbol": "BTC",
                "market": "KRW-BTC",
                "tf": "1m",
                "rows": 1000,
                "min_ts_ms": end_ts_ms - (10 * DAY_MS),
                "max_ts_ms": end_ts_ms - (4 * DAY_MS),
                "status": "OK",
                "ingested_at": 1,
                "reasons_json": "[]",
            },
            {
                "quote": "KRW",
                "symbol": "BTC",
                "market": "KRW-BTC",
                "tf": "60m",
                "rows": 100,
                "min_ts_ms": end_ts_ms - (30 * DAY_MS),
                "max_ts_ms": end_ts_ms - (10 * DAY_MS),
                "status": "OK",
                "ingested_at": 1,
                "reasons_json": "[]",
            },
        ],
    )

    options = CandlePlanOptions(
        parquet_root=parquet_root,
        base_dataset="candles_v1",
        output_path=tmp_path / "plan.json",
        lookback_months=2,
        tf_set=("1m", "60m"),
        quote="KRW",
        market_mode="fixed_list",
        fixed_markets=("KRW-BTC",),
        max_backfill_days_1m=10,
        end_ts_ms=end_ts_ms,
    )
    plan = generate_candle_topup_plan(options)
    targets = [item for item in plan["targets"] if item["market"] == "KRW-BTC"]

    assert len(targets) >= 2
    first_one_m_index = next(idx for idx, item in enumerate(targets) if item["tf"] == "1m")
    first_sixty_index = next(idx for idx, item in enumerate(targets) if item["tf"] == "60m")
    assert first_sixty_index < first_one_m_index
    assert int(targets[first_sixty_index]["max_calls_budget_hint"]) <= int(targets[first_one_m_index]["max_calls_budget_hint"])
