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
