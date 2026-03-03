from __future__ import annotations

from pathlib import Path

from autobot.data.collect.plan_ws_public import WsPublicPlanOptions, generate_ws_public_collection_plan
from autobot.data.manifest import append_manifest_rows


def test_generate_ws_public_plan_fixed_list(tmp_path: Path) -> None:
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

    options = WsPublicPlanOptions(
        parquet_root=parquet_root,
        base_dataset="candles_v1",
        output_path=tmp_path / "ws_public_plan.json",
        quote="KRW",
        market_mode="fixed_list",
        fixed_markets=("krw-btc",),
        channels=("trade", "orderbook"),
        format="DEFAULT",
        orderbook_topk=5,
        orderbook_level=0,
        orderbook_min_write_interval_ms=200,
    )
    plan = generate_ws_public_collection_plan(options)

    assert plan["summary"]["selected_markets"] == 1
    assert plan["summary"]["codes_count"] == 1
    assert plan["summary"]["channels_count"] == 2
    assert plan["codes"] == ["KRW-BTC"]
    assert plan["runtime_policy"]["orderbook_topk"] == 5
    assert plan["runtime_policy"]["orderbook_min_write_interval_ms"] == 200
