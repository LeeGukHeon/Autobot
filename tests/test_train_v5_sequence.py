from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import polars as pl

from autobot.models.predictor import load_predictor_from_registry
from autobot.models.registry import load_json
from autobot.models.train_v5_sequence import (
    TrainV5SequenceOptions,
    _load_minute_close_map_sources,
    resume_v5_sequence_tail,
    train_and_register_v5_sequence,
)


def test_train_v5_sequence_writes_core_contract_artifacts(tmp_path: Path) -> None:
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
            lob_tensor=np.full((1, 30, 5), 0.4 + idx, dtype=np.float32),
            lob_global_tensor=np.full((1, 5), 0.5 + idx, dtype=np.float32),
            second_mask=np.ones((4,), dtype=np.float32),
            minute_mask=np.ones((1,), dtype=np.float32),
            micro_mask=np.ones((1,), dtype=np.float32),
            lob_mask=np.ones((1,), dtype=np.float32),
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
    (meta_root / "sequence_tensor_contract.json").write_text(
        json.dumps({"policy": "sequence_tensor_contract_v1", "second_tensor": {"lookback_steps": 4}}),
        encoding="utf-8",
    )
    (meta_root / "lob_tensor_contract.json").write_text(
        json.dumps({"policy": "lob_tensor_contract_v1", "shape": {"levels": 30}}),
        encoding="utf-8",
    )

    ws_root = tmp_path / "parquet" / "ws_candle_v1" / "tf=1m" / "market=KRW-BTC"
    ws_root.mkdir(parents=True, exist_ok=True)
    ws_rows = []
    for idx in range(40):
        ts_ms = anchors[0] + (idx * 60_000)
        close = 100.0 + (idx * 0.5)
        ws_rows.append(
            {
                "ts_ms": ts_ms,
                "open": close - 0.2,
                "high": close + 0.2,
                "low": close - 0.3,
                "close": close,
                "volume_base": 1.0 + idx,
                "volume_quote": 100.0 + idx,
                "volume_quote_est": False,
            }
        )
    pl.DataFrame(ws_rows).write_parquet(ws_root / "part-000.parquet")

    options = TrainV5SequenceOptions(
        dataset_root=dataset_root,
        registry_root=tmp_path / "registry",
        logs_root=tmp_path / "logs",
        model_family="train_v5_sequence",
        quote="KRW",
        top_n=1,
        start="2026-03-27",
        end="2026-03-27",
        seed=7,
        pretrain_method="none",
        batch_size=4,
        pretrain_epochs=1,
        finetune_epochs=1,
        horizons_minutes=(3, 6, 12, 24),
        quantile_levels=(0.1, 0.5, 0.9),
        hidden_dim=16,
        regime_embedding_dim=4,
        run_scope="scheduled_daily_dependency_v5_sequence",
    )
    result = train_and_register_v5_sequence(options)

    assert result.run_dir.exists()
    assert result.sequence_model_contract_path.exists()
    assert result.predictor_contract_path.exists()
    assert (result.run_dir / "expert_prediction_table.parquet").exists()
    assert (result.run_dir / "runtime_feature_dataset" / "_meta" / "feature_spec.json").exists()
    assert result.walk_forward_report_path.exists()
    assert result.promotion_path.exists()
    assert (result.run_dir / "expert_tail_context.json").exists()
    assert load_json(result.run_dir / "train_config.yaml")["trainer"] == "v5_sequence"
    assert load_json(result.sequence_model_contract_path)["policy"] == "v5_sequence_v1"
    assert load_json(result.predictor_contract_path)["regime_embedding_dim"] == 4
    assert load_json(result.train_report_path)["resumed"] is False
    assert float(load_json(result.train_report_path)["tail_duration_sec"]) >= 0.0
    artifact_status = load_json(result.run_dir / "artifact_status.json")
    assert artifact_status["tail_context_written"] is True
    assert artifact_status["runtime_recommendations_complete"] is True
    assert artifact_status["promotion_complete"] is True
    assert artifact_status["decision_surface_complete"] is True
    assert artifact_status["expert_prediction_table_complete"] is True
    predictor = load_predictor_from_registry(
        registry_root=tmp_path / "registry",
        model_ref=result.run_id,
        model_family="train_v5_sequence",
    )
    payload = predictor.predict_score_contract(np.zeros((2, len(predictor.feature_columns)), dtype=np.float64))
    assert payload["final_rank_score"].shape == (2,)
    assert (tmp_path / "registry" / "train_v5_sequence" / "latest.json").exists()


def test_resume_v5_sequence_tail_reuses_existing_artifacts(tmp_path: Path) -> None:
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
            lob_tensor=np.full((1, 30, 5), 0.4 + idx, dtype=np.float32),
            lob_global_tensor=np.full((1, 5), 0.5 + idx, dtype=np.float32),
            second_mask=np.ones((4,), dtype=np.float32),
            minute_mask=np.ones((1,), dtype=np.float32),
            micro_mask=np.ones((1,), dtype=np.float32),
            lob_mask=np.ones((1,), dtype=np.float32),
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
    (meta_root / "sequence_tensor_contract.json").write_text(
        json.dumps({"policy": "sequence_tensor_contract_v1", "second_tensor": {"lookback_steps": 4}}),
        encoding="utf-8",
    )
    (meta_root / "lob_tensor_contract.json").write_text(
        json.dumps({"policy": "lob_tensor_contract_v1", "shape": {"levels": 30}}),
        encoding="utf-8",
    )
    ws_root = tmp_path / "parquet" / "ws_candle_v1" / "tf=1m" / "market=KRW-BTC"
    ws_root.mkdir(parents=True, exist_ok=True)
    ws_rows = []
    for idx in range(40):
        ts_ms = anchors[0] + (idx * 60_000)
        close = 100.0 + (idx * 0.5)
        ws_rows.append({"ts_ms": ts_ms, "open": close - 0.2, "high": close + 0.2, "low": close - 0.3, "close": close, "volume_base": 1.0 + idx, "volume_quote": 100.0 + idx, "volume_quote_est": False})
    pl.DataFrame(ws_rows).write_parquet(ws_root / "part-000.parquet")
    options = TrainV5SequenceOptions(
        dataset_root=dataset_root,
        registry_root=tmp_path / "registry",
        logs_root=tmp_path / "logs",
        model_family="train_v5_sequence",
        quote="KRW",
        top_n=1,
        start="2026-03-27",
        end="2026-03-27",
        seed=7,
        pretrain_method="none",
        batch_size=4,
        pretrain_epochs=1,
        finetune_epochs=1,
        horizons_minutes=(3, 6, 12, 24),
        quantile_levels=(0.1, 0.5, 0.9),
        hidden_dim=16,
        regime_embedding_dim=4,
    )
    result = train_and_register_v5_sequence(options)
    report_before = load_json(result.train_report_path)
    resumed = resume_v5_sequence_tail(run_dir=result.run_dir)
    report_after = load_json(resumed.train_report_path)
    assert resumed.run_id == result.run_id
    assert report_after["resumed"] is True
    assert report_after["data_platform_ready_snapshot_id"] == report_before["data_platform_ready_snapshot_id"]
    assert (result.run_dir / "expert_prediction_table.parquet").exists()


def test_train_v5_sequence_load_minute_close_map_sources_reads_date_partitions(tmp_path: Path) -> None:
    root = tmp_path / "parquet" / "ws_candle_v1" / "tf=1m" / "market=KRW-BTC" / "date=2026-03-27"
    root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "ts_ms": [1_774_569_548_855, 1_774_569_607_341],
            "close": [100.0, 101.0],
        }
    ).write_parquet(root / "part-000.parquet")

    close_map = _load_minute_close_map_sources(market="KRW-BTC", roots=(tmp_path / "parquet" / "ws_candle_v1" / "tf=1m",))

    assert close_map[1_774_569_600_000] == 100.0
    assert close_map[1_774_569_660_000] == 101.0
