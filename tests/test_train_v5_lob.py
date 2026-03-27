from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import polars as pl

from autobot.models.registry import load_json
from autobot.models.train_v5_lob import TrainV5LobOptions, train_and_register_v5_lob


def test_train_v5_lob_writes_core_contract_artifacts(tmp_path: Path) -> None:
    dataset_root = tmp_path / "parquet" / "sequence_v1"
    meta_root = dataset_root / "_meta"
    cache_root = dataset_root / "cache" / "market=KRW-BTC" / "date=2026-03-27"
    meta_root.mkdir(parents=True, exist_ok=True)
    cache_root.mkdir(parents=True, exist_ok=True)

    anchors = [1_774_569_600_000 + (idx * 60_000) for idx in range(12)]
    manifest_rows = []
    for idx, anchor_ts_ms in enumerate(anchors):
        cache_file = cache_root / f"anchor-{anchor_ts_ms}.npz"
        np.savez_compressed(
            cache_file,
            second_tensor=np.full((4, 4), 0.1 + idx, dtype=np.float32),
            minute_tensor=np.full((1, 4), 0.2 + idx, dtype=np.float32),
            micro_tensor=np.full((1, 7), 0.3 + idx, dtype=np.float32),
            lob_tensor=np.full((4, 30, 5), 0.4 + idx, dtype=np.float32),
            lob_global_tensor=np.full((4, 5), 0.5 + idx, dtype=np.float32),
            second_mask=np.ones((4,), dtype=np.float32),
            minute_mask=np.ones((1,), dtype=np.float32),
            micro_mask=np.ones((1,), dtype=np.float32),
            lob_mask=np.ones((4,), dtype=np.float32),
        )
        manifest_rows.append(
            {
                "market": "KRW-BTC",
                "date": "2026-03-27",
                "anchor_ts_ms": anchor_ts_ms,
                "anchor_utc": "2026-03-27T00:00:00+00:00",
                "status": "OK",
                "reasons_json": "[]",
                "error_message": None,
                "cache_file": str(cache_file),
                "second_coverage_ratio": 1.0,
                "minute_coverage_ratio": 1.0,
                "micro_coverage_ratio": 1.0,
                "lob_coverage_ratio": 1.0,
                "built_at_ms": anchor_ts_ms + 1,
            }
        )
    pl.DataFrame(manifest_rows).write_parquet(meta_root / "manifest.parquet")

    second_root = tmp_path / "parquet" / "candles_second_v1" / "tf=1s" / "market=KRW-BTC"
    second_root.mkdir(parents=True, exist_ok=True)
    second_rows = []
    for minute_idx, anchor_ts_ms in enumerate(anchors):
        base_price = 100.0 + minute_idx
        for sec in range(0, 65):
            ts_ms = anchor_ts_ms + (sec * 1_000)
            second_rows.append(
                {
                    "ts_ms": ts_ms,
                    "open": base_price,
                    "high": base_price + 0.2,
                    "low": base_price - 0.2,
                    "close": base_price + (sec * 0.01),
                    "volume_base": 1.0,
                    "volume_quote": 100.0,
                    "volume_quote_est": False,
                }
            )
    pl.DataFrame(second_rows).write_parquet(second_root / "part-000.parquet")

    minute_root = tmp_path / "parquet" / "ws_candle_v1" / "tf=1m" / "market=KRW-BTC"
    minute_root.mkdir(parents=True, exist_ok=True)
    minute_rows = []
    for idx in range(20):
        ts_ms = anchors[0] + (idx * 60_000)
        close = 200.0 + idx
        minute_rows.append(
            {
                "ts_ms": ts_ms,
                "open": close - 0.5,
                "high": close + 0.5,
                "low": close - 0.7,
                "close": close,
                "volume_base": 2.0,
                "volume_quote": 200.0,
                "volume_quote_est": False,
            }
        )
    pl.DataFrame(minute_rows).write_parquet(minute_root / "part-000.parquet")

    options = TrainV5LobOptions(
        dataset_root=dataset_root,
        registry_root=tmp_path / "registry",
        logs_root=tmp_path / "logs",
        model_family="train_v5_lob",
        quote="KRW",
        top_n=1,
        start="2026-03-27",
        end="2026-03-27",
        seed=7,
        backbone_family="deeplob",
        batch_size=4,
        epochs=1,
        hidden_dim=16,
        temporal_hidden_dim=16,
    )
    result = train_and_register_v5_lob(options)

    assert result.run_dir.exists()
    assert result.lob_model_contract_path.exists()
    assert result.predictor_contract_path.exists()
    assert (result.run_dir / "expert_prediction_table.parquet").exists()
    assert result.walk_forward_report_path.exists()
    assert result.promotion_path.exists()
    assert load_json(result.run_dir / "train_config.yaml")["trainer"] == "v5_lob"
    assert load_json(result.lob_model_contract_path)["policy"] == "v5_lob_v1"
    assert load_json(result.predictor_contract_path)["micro_alpha_30s_field"] == "micro_alpha_30s"
