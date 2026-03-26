from __future__ import annotations

from pathlib import Path

from autobot.data.collect.plan_ws_candles import WsCandlePlanOptions, generate_ws_candle_collection_plan
from autobot.data.manifest import append_manifest_rows


def test_generate_ws_candle_plan_uses_market_source_dataset_and_tf_set(tmp_path: Path) -> None:
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

    options = WsCandlePlanOptions(
        parquet_root=parquet_root,
        base_dataset="ws_candle_v1",
        market_source_dataset="candles_api_v1",
        output_path=tmp_path / "ws_candle_plan.json",
        quote="KRW",
        market_mode="top_n_by_recent_value_est",
        top_n=1,
        tf_set=("1s", "1m", "3m"),
        format="SIMPLE_LIST",
    )
    plan = generate_ws_candle_collection_plan(options)

    assert plan["summary"]["selected_markets"] == 1
    assert plan["summary"]["tf_count"] == 3
    assert plan["filters"]["market_source_dataset"] == "candles_api_v1"
    assert plan["runtime_policy"]["subscription_types"] == ["candle.1s", "candle.1m", "candle.3m"]
    assert plan["runtime_policy"]["format"] == "SIMPLE_LIST"
    assert plan["codes"] == ["KRW-BTC"]
