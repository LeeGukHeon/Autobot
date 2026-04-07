from __future__ import annotations

import json
from pathlib import Path
import time

import polars as pl

from autobot.ops.data_platform_snapshot import (
    load_ready_snapshot,
    publish_ready_snapshot,
    ready_snapshot_pointer_path,
    ready_snapshot_summary_path,
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
    if dataset_name == "features_v4":
        (dataset_root / "feature_dataset_certification.json").write_text(
            json.dumps({"status": "PASS", "pass": True}, ensure_ascii=False),
            encoding="utf-8",
        )
        (dataset_root / "raw_to_feature_lineage_report.json").write_text(
            json.dumps({"policy": "raw_to_feature_lineage_report_v1"}, ensure_ascii=False),
            encoding="utf-8",
        )


def _write_collect_validate_report(root: Path, report_name: str, *, dataset_name: str) -> None:
    report_path = root / "data" / "collect" / "_meta" / report_name
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps({"dataset_name": dataset_name, "status": "PASS", "fail_files": 0}, ensure_ascii=False),
        encoding="utf-8",
    )


def test_publish_ready_snapshot_writes_pointer_and_resolves_dataset_roots(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    for name in ("candles_second_v1", "ws_candle_v1", "lob30_v1", "sequence_v1", "private_execution_v1", "candles_api_v1", "features_v4"):
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


def test_env_snapshot_id_overrides_ready_pointer(tmp_path: Path, monkeypatch) -> None:
    project_root = tmp_path / "project"
    for name in ("candles_second_v1", "ws_candle_v1", "lob30_v1", "sequence_v1", "private_execution_v1", "candles_api_v1", "features_v4"):
        _write_dataset(project_root, name)

    first = publish_ready_snapshot(project_root=project_root)
    time.sleep(1.1)
    second = publish_ready_snapshot(project_root=project_root)
    assert first["snapshot_id"] != second["snapshot_id"]

    pointer_payload = load_ready_snapshot(project_root=project_root)
    assert pointer_payload["snapshot_id"] == second["snapshot_id"]

    monkeypatch.setenv("AUTOBOT_DATA_PLATFORM_READY_SNAPSHOT_ID", first["snapshot_id"])
    env_payload = load_ready_snapshot(project_root=project_root)
    assert env_payload["snapshot_id"] == first["snapshot_id"]
    assert ready_snapshot_summary_path(project_root=project_root, snapshot_id=first["snapshot_id"]).exists()

    resolved = resolve_ready_snapshot_dataset_root(project_root=project_root, dataset_name="sequence_v1")
    assert resolved is not None
    assert str(resolved).startswith(str(project_root / "data" / "snapshots" / "data_platform" / first["snapshot_id"]))


def test_publish_ready_snapshot_accepts_collect_meta_validate_reports_for_collect_datasets(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    for name in ("sequence_v1", "private_execution_v1", "candles_api_v1", "features_v4"):
        _write_dataset(project_root, name)

    for name in ("candles_second_v1", "ws_candle_v1", "lob30_v1"):
        _write_dataset(project_root, name)
        validate_path = project_root / "data" / "parquet" / name / "_meta" / "validate_report.json"
        validate_path.unlink()

    _write_collect_validate_report(project_root, "candle_second_validate_report.json", dataset_name="candles_second_v1")
    _write_collect_validate_report(project_root, "ws_candle_validate_report.json", dataset_name="ws_candle_v1")
    _write_collect_validate_report(project_root, "lob30_validate_report.json", dataset_name="lob30_v1")

    result = publish_ready_snapshot(project_root=project_root)

    snapshot_root = Path(result["snapshot_root"])
    assert (snapshot_root / "data" / "parquet" / "candles_second_v1" / "_meta" / "validate_report.json").exists()
    assert (snapshot_root / "data" / "parquet" / "ws_candle_v1" / "_meta" / "validate_report.json").exists()
    assert (snapshot_root / "data" / "parquet" / "lob30_v1" / "_meta" / "validate_report.json").exists()


def test_publish_ready_snapshot_freezes_snapshot_contents_independent_from_source(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    for name in ("candles_second_v1", "ws_candle_v1", "lob30_v1", "sequence_v1", "private_execution_v1", "candles_api_v1", "features_v4"):
        _write_dataset(project_root, name)

    result = publish_ready_snapshot(project_root=project_root)
    snapshot_root = Path(result["snapshot_root"])

    source_cert = project_root / "data" / "features" / "features_v4" / "_meta" / "feature_dataset_certification.json"
    frozen_cert = snapshot_root / "data" / "features" / "features_v4" / "_meta" / "feature_dataset_certification.json"
    source_part = project_root / "data" / "parquet" / "sequence_v1" / "market=KRW-BTC" / "date=2026-03-30" / "part-000.parquet"
    frozen_part = snapshot_root / "data" / "parquet" / "sequence_v1" / "market=KRW-BTC" / "date=2026-03-30" / "part-000.parquet"

    source_cert.write_text(
        json.dumps({"status": "FAIL", "pass": False}, ensure_ascii=False),
        encoding="utf-8",
    )
    pl.DataFrame({"value": [999]}).write_parquet(source_part)

    frozen_cert_payload = json.loads(frozen_cert.read_text(encoding="utf-8"))
    assert frozen_cert_payload["status"] == "PASS"
    assert frozen_cert_payload["pass"] is True
    assert pl.read_parquet(frozen_part).get_column("value").to_list() == [1]
