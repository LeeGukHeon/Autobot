from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from autobot.ops.data_contract_registry import build_data_contract_registry, write_data_contract_registry


def test_build_data_contract_registry_infers_lineage(tmp_path: Path) -> None:
    project_root = tmp_path

    ws_public_root = project_root / "data" / "raw_ws" / "upbit" / "public"
    ws_meta_dir = project_root / "data" / "raw_ws" / "upbit" / "_meta"
    ws_public_root.mkdir(parents=True, exist_ok=True)
    ws_meta_dir.mkdir(parents=True, exist_ok=True)
    (ws_meta_dir / "ws_collect_report.json").write_text(
        json.dumps({"raw_root": "data/raw_ws/upbit/public"}),
        encoding="utf-8",
    )
    (ws_meta_dir / "ws_public_health.json").write_text(
        json.dumps({"connected": True, "run_id": "ws-run-1"}),
        encoding="utf-8",
    )
    pl.DataFrame({"run_id": ["ws-run-1"], "channel": ["trade"], "date": ["2026-03-25"]}).write_parquet(
        ws_meta_dir / "ws_manifest.parquet"
    )

    raw_ticks_root = project_root / "data" / "raw_ticks" / "upbit" / "trades"
    raw_ticks_meta = project_root / "data" / "raw_ticks" / "upbit" / "_meta"
    raw_ticks_root.mkdir(parents=True, exist_ok=True)
    raw_ticks_meta.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"run_id": ["ticks-run-1"], "market": ["KRW-BTC"], "date": ["2026-03-25"]}).write_parquet(
        raw_ticks_meta / "ticks_manifest.parquet"
    )

    candles_root = project_root / "data" / "parquet" / "candles_v1"
    candles_meta = candles_root / "_meta"
    candles_meta.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"dataset": ["candles_v1"], "rows": [1]}).write_parquet(candles_meta / "manifest.parquet")

    micro_root = project_root / "data" / "parquet" / "micro_v1"
    micro_meta = micro_root / "_meta"
    micro_meta.mkdir(parents=True, exist_ok=True)
    (micro_meta / "aggregate_report.json").write_text(
        json.dumps(
            {
                "raw_ws_root": "data/raw_ws/upbit/public",
                "raw_ticks_root": "data/raw_ticks/upbit/trades",
                "base_candles_root": "data/parquet/candles_v1",
                "out_root": "data/parquet/micro_v1",
            }
        ),
        encoding="utf-8",
    )
    (micro_meta / "validate_report.json").write_text(
        json.dumps({"checked_files": 1, "fail_files": 0, "status": "PASS"}),
        encoding="utf-8",
    )
    pl.DataFrame({"dataset": ["micro_v1"], "rows": [1]}).write_parquet(micro_meta / "manifest.parquet")
    (micro_meta / "spec.json").write_text(json.dumps({"version": 1}), encoding="utf-8")

    features_root = project_root / "data" / "features" / "features_v4"
    features_meta = features_root / "_meta"
    features_meta.mkdir(parents=True, exist_ok=True)
    (features_meta / "build_report.json").write_text(
        json.dumps(
            {
                "base_candles_root": "data/parquet/candles_v1",
                "micro_root": "data/parquet/micro_v1",
                "status": "PASS",
            }
        ),
        encoding="utf-8",
    )
    (features_meta / "validate_report.json").write_text(
        json.dumps({"checked_files": 1, "fail_files": 0, "status": "PASS"}),
        encoding="utf-8",
    )
    (features_meta / "feature_spec.json").write_text(
        json.dumps(
            {
                "feature_columns": ["close", "rv_12"],
                "input_dataset_root": "data/parquet/candles_v1",
            }
        ),
        encoding="utf-8",
    )
    (features_meta / "label_spec.json").write_text(
        json.dumps({"label_columns": ["y_reg_net_12", "y_cls_topq_12"]}),
        encoding="utf-8",
    )

    registry = build_data_contract_registry(project_root=project_root)

    entries = {entry["contract_id"]: entry for entry in registry["entries"]}
    assert "raw_ws_dataset:upbit_public" in entries
    assert "raw_ticks_dataset:upbit_trades" in entries
    assert "parquet_dataset:candles_v1" in entries
    assert "micro_dataset:micro_v1" in entries
    assert "feature_dataset:features_v4" in entries

    assert entries["raw_ws_dataset:upbit_public"]["status"] == "active"
    assert entries["micro_dataset:micro_v1"]["status"] == "validated"
    assert entries["feature_dataset:features_v4"]["status"] == "validated"

    assert set(entries["micro_dataset:micro_v1"]["source_contract_ids"]) == {
        "raw_ws_dataset:upbit_public",
        "raw_ticks_dataset:upbit_trades",
        "parquet_dataset:candles_v1",
    }
    assert set(entries["feature_dataset:features_v4"]["source_contract_ids"]) == {
        "micro_dataset:micro_v1",
        "parquet_dataset:candles_v1",
    }

    assert registry["summary"]["contract_count"] == 5
    assert registry["summary"]["layers"]["feature_dataset"] == 1
    assert registry["summary"]["layers"]["micro_dataset"] == 1


def test_write_data_contract_registry_uses_default_output_path(tmp_path: Path) -> None:
    project_root = tmp_path
    (project_root / "data" / "raw_ws" / "upbit" / "public").mkdir(parents=True, exist_ok=True)
    meta_dir = project_root / "data" / "raw_ws" / "upbit" / "_meta"
    meta_dir.mkdir(parents=True, exist_ok=True)
    (meta_dir / "ws_public_health.json").write_text(json.dumps({"connected": False}), encoding="utf-8")

    output_path = write_data_contract_registry(project_root=project_root)

    assert output_path == project_root / "data" / "_meta" / "data_contract_registry.json"
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["version"] == 1
    assert payload["summary"]["contract_count"] >= 1
    assert any(entry["contract_id"] == "raw_ws_dataset:upbit_public" for entry in payload["entries"])
