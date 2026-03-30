from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from autobot.ops.data_platform_snapshot import (
    load_ready_snapshot,
    publish_ready_snapshot,
    ready_snapshot_pointer_path,
    resolve_ready_snapshot_dataset_root,
)


def _write_dataset(root: Path, dataset_name: str) -> None:
    if dataset_name == "features_v4":
        dataset_root = root / "data" / "features" / dataset_name / "_meta"
        part_path = dataset_root.parent / "tf=5m" / "market=KRW-BTC" / "date=2026-03-30" / "part-000.parquet"
    else:
        dataset_root = root / "data" / "parquet" / dataset_name / "_meta"
        part_path = dataset_root.parent / "market=KRW-BTC" / "date=2026-03-30" / "part-000.parquet"
    dataset_root.mkdir(parents=True, exist_ok=True)
    part_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"value": [1]}).write_parquet(part_path)
    (dataset_root / "build_report.json").write_text(json.dumps({"dataset_name": dataset_name}), encoding="utf-8")
    (dataset_root / "validate_report.json").write_text(json.dumps({"dataset_name": dataset_name}), encoding="utf-8")
    pl.DataFrame({"market": ["KRW-BTC"], "anchor_ts_ms": [1]}).write_parquet(dataset_root / "manifest.parquet")


def test_publish_ready_snapshot_writes_pointer_and_resolves_dataset_roots(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    for name in ("candles_second_v1", "ws_candle_v1", "lob30_v1", "sequence_v1", "candles_api_v1", "features_v4"):
        _write_dataset(project_root, name)

    result = publish_ready_snapshot(project_root=project_root)

    pointer = ready_snapshot_pointer_path(project_root=project_root)
    assert pointer.exists()
    payload = load_ready_snapshot(project_root=project_root)
    assert payload["snapshot_id"] == result["snapshot_id"]
    resolved = resolve_ready_snapshot_dataset_root(project_root=project_root, dataset_name="sequence_v1")
    assert resolved is not None
    assert resolved.exists()
    assert str(resolved).startswith(str(project_root / "data" / "snapshots" / "data_platform"))
    features_root = resolve_ready_snapshot_dataset_root(project_root=project_root, dataset_name="features_v4")
    assert features_root is not None
    assert features_root.exists()
