from __future__ import annotations

from pathlib import Path

from autobot.data.collect.plan_lob30 import Lob30PlanOptions, generate_lob30_collection_plan
from autobot.data.manifest import append_manifest_rows


def test_generate_lob30_plan_uses_market_source_and_request_codes(tmp_path: Path) -> None:
    parquet_root = tmp_path / "parquet"
    source_manifest = parquet_root / "candles_api_v1" / "_meta" / "manifest.parquet"

    append_manifest_rows(
        source_manifest,
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

    options = Lob30PlanOptions(
        parquet_root=parquet_root,
        base_dataset="lob30_v1",
        market_source_dataset="candles_api_v1",
        output_path=tmp_path / "lob30_plan.json",
        quote="KRW",
        market_mode="top_n_by_recent_value_est",
        top_n=1,
        format="SIMPLE_LIST",
    )
    plan = generate_lob30_collection_plan(options)

    assert plan["filters"]["market_source_dataset"] == "candles_api_v1"
    assert plan["runtime_policy"]["requested_depth"] == 30
    assert plan["runtime_policy"]["orderbook_level"] == 0
    assert plan["request_codes"] == ["KRW-BTC.30"]
    assert plan["summary"]["request_codes_count"] == 1
