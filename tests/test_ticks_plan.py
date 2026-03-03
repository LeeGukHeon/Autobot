from __future__ import annotations

from pathlib import Path

from autobot.data.collect.plan_ticks import TicksPlanOptions, generate_ticks_collection_plan
from autobot.data.manifest import append_manifest_rows


def test_generate_ticks_plan_fixed_list(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    dataset_root = parquet_root / "candles_v1"
    manifest_file = dataset_root / "_meta" / "manifest.parquet"

    append_manifest_rows(
        manifest_file,
        [
            {
                "quote": "KRW",
                "symbol": "BTC",
                "market": "KRW-BTC",
                "tf": "1m",
                "rows": 100,
                "min_ts_ms": 1_772_300_000_000,
                "max_ts_ms": 1_772_399_000_000,
                "status": "OK",
                "ingested_at": 1,
                "reasons_json": "[]",
            }
        ],
    )

    options = TicksPlanOptions(
        parquet_root=parquet_root,
        base_dataset="candles_v1",
        output_path=tmp_path / "ticks_plan.json",
        quote="KRW",
        market_mode="fixed_list",
        fixed_markets=("KRW-BTC",),
        days_ago=(1, 2),
    )
    plan = generate_ticks_collection_plan(options)

    assert plan["summary"]["selected_markets"] == 1
    assert plan["summary"]["targets"] == 2
    keys = {item["target_key"] for item in plan["targets"]}
    assert "KRW-BTC|1" in keys
    assert "KRW-BTC|2" in keys
