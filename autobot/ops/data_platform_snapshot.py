"""Immutable ready-snapshot publisher for derived v5 data-platform datasets."""

from __future__ import annotations

from datetime import datetime, timezone
import argparse
import json
import os
from pathlib import Path
import shutil
from typing import Any


READY_SNAPSHOT_ID_ENV = "AUTOBOT_DATA_PLATFORM_READY_SNAPSHOT_ID"

SNAPSHOT_DATASET_LAYOUT: dict[str, tuple[Path, Path]] = {
    "candles_second_v1": (Path("data/parquet/candles_second_v1"), Path("data/parquet/candles_second_v1")),
    "ws_candle_v1": (Path("data/parquet/ws_candle_v1"), Path("data/parquet/ws_candle_v1")),
    "lob30_v1": (Path("data/parquet/lob30_v1"), Path("data/parquet/lob30_v1")),
    "sequence_v1": (Path("data/parquet/sequence_v1"), Path("data/parquet/sequence_v1")),
    "candles_api_v1": (Path("data/parquet/candles_api_v1"), Path("data/parquet/candles_api_v1")),
    "features_v4": (Path("data/features/features_v4"), Path("data/features/features_v4")),
}

CORE_DERIVED_DATASETS: tuple[str, ...] = tuple(SNAPSHOT_DATASET_LAYOUT.keys())


def ready_snapshot_pointer_path(*, project_root: Path) -> Path:
    return project_root / "data" / "_meta" / "data_platform_ready_snapshot.json"


def ready_snapshot_summary_path(*, project_root: Path, snapshot_id: str) -> Path:
    return project_root / "data" / "snapshots" / "data_platform" / str(snapshot_id).strip() / "_meta" / "summary.json"


def _resolve_requested_snapshot_id(*, snapshot_id: str | None = None) -> str:
    explicit = str(snapshot_id or "").strip()
    if explicit:
        return explicit
    return str(os.getenv(READY_SNAPSHOT_ID_ENV, "")).strip()


def _load_json_doc(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def load_ready_snapshot(*, project_root: Path, snapshot_id: str | None = None) -> dict[str, Any]:
    requested_snapshot_id = _resolve_requested_snapshot_id(snapshot_id=snapshot_id)
    if requested_snapshot_id:
        return _load_json_doc(
            ready_snapshot_summary_path(project_root=project_root, snapshot_id=requested_snapshot_id)
        )
    return _load_json_doc(ready_snapshot_pointer_path(project_root=project_root))


def resolve_ready_snapshot_dataset_root(
    *,
    project_root: Path,
    dataset_name: str,
    snapshot_id: str | None = None,
) -> Path | None:
    payload = load_ready_snapshot(project_root=project_root, snapshot_id=snapshot_id)
    datasets = dict(payload.get("datasets") or {})
    raw = datasets.get(str(dataset_name).strip())
    if not isinstance(raw, dict):
        return None
    path_value = str(raw.get("dataset_root") or "").strip()
    if not path_value:
        return None
    return Path(path_value)


def resolve_ready_snapshot_id(*, project_root: Path, snapshot_id: str | None = None) -> str | None:
    payload = load_ready_snapshot(project_root=project_root, snapshot_id=snapshot_id)
    value = str(payload.get("snapshot_id") or "").strip()
    return value or None


def publish_ready_snapshot(
    *,
    project_root: Path,
    dataset_names: tuple[str, ...] = CORE_DERIVED_DATASETS,
) -> dict[str, Any]:
    resolved_project_root = Path(project_root).resolve()
    parquet_root = resolved_project_root / "data" / "parquet"
    snapshot_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    snapshot_root = resolved_project_root / "data" / "snapshots" / "data_platform" / snapshot_id
    summary_root = snapshot_root / "_meta"
    summary_root.mkdir(parents=True, exist_ok=True)

    dataset_payload: dict[str, Any] = {}
    for dataset_name in dataset_names:
        name = str(dataset_name).strip()
        if not name:
            continue
        relative_source, relative_target = SNAPSHOT_DATASET_LAYOUT.get(name, (Path("data/parquet") / name, Path("data/parquet") / name))
        source_root = resolved_project_root / relative_source
        if not source_root.exists():
            raise FileNotFoundError(f"derived dataset root missing: {source_root}")
        requirements = _resolve_snapshot_requirements(dataset_name=name, source_root=source_root)
        target_root = snapshot_root / relative_target
        _copytree_hardlink(src=source_root, dst=target_root)
        dataset_payload[name] = {
            "dataset_root": str(target_root),
            "source_root": str(source_root),
            "build_report_path": str(target_root / "_meta" / "build_report.json"),
            "validate_report_path": str(target_root / "_meta" / "validate_report.json"),
            "manifest_path": str(target_root / "_meta" / "manifest.parquet"),
            "feature_dataset_certification_path": requirements.get("feature_dataset_certification_path"),
            "raw_to_feature_lineage_report_path": requirements.get("raw_to_feature_lineage_report_path"),
            "requirements": requirements,
        }

    summary = {
        "policy": "data_platform_ready_snapshot_v1",
        "snapshot_id": snapshot_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "project_root": str(resolved_project_root),
        "snapshot_root": str(snapshot_root),
        "datasets": dataset_payload,
    }
    summary_path = summary_root / "summary.json"
    _write_json_atomic(summary_path, summary)
    _write_json_atomic(ready_snapshot_pointer_path(project_root=resolved_project_root), summary)
    return {
        "snapshot_id": snapshot_id,
        "snapshot_root": str(snapshot_root),
        "summary_path": str(summary_path),
        "pointer_path": str(ready_snapshot_pointer_path(project_root=resolved_project_root)),
        "datasets": dataset_payload,
    }


def _copytree_hardlink(*, src: Path, dst: Path) -> None:
    if dst.exists():
        shutil.rmtree(dst)
    for root, dirs, files in os.walk(src):
        root_path = Path(root)
        rel = root_path.relative_to(src)
        target_dir = dst / rel
        target_dir.mkdir(parents=True, exist_ok=True)
        for name in dirs:
            (target_dir / name).mkdir(parents=True, exist_ok=True)
        for name in files:
            source_file = root_path / name
            target_file = target_dir / name
            try:
                os.link(source_file, target_file)
            except OSError:
                shutil.copy2(source_file, target_file)


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temp.replace(path)


def _resolve_snapshot_requirements(*, dataset_name: str, source_root: Path) -> dict[str, Any]:
    meta_root = Path(source_root) / "_meta"
    validate_report_path = meta_root / "validate_report.json"
    validate_report = _load_json_doc(validate_report_path)
    if not validate_report_path.exists():
        raise FileNotFoundError(f"snapshot publish requires validate report: {validate_report_path}")
    validate_status = str(validate_report.get("status") or "").strip().upper()
    validate_fail_files = int(validate_report.get("fail_files") or 0)
    if validate_status == "FAIL" or validate_fail_files > 0:
        raise ValueError(f"snapshot publish blocked by failed validate report: {validate_report_path}")

    requirements: dict[str, Any] = {
        "validate_report_required": True,
        "validate_report_path": str(validate_report_path),
        "validate_report_status": validate_status,
    }
    if str(dataset_name).strip().lower() == "features_v4":
        certification_path = meta_root / "feature_dataset_certification.json"
        certification = _load_json_doc(certification_path)
        if not certification_path.exists():
            raise FileNotFoundError(f"snapshot publish requires feature dataset certification: {certification_path}")
        if not bool(certification.get("pass", False)):
            raise ValueError(f"snapshot publish blocked by failed feature dataset certification: {certification_path}")
        lineage_path = meta_root / "raw_to_feature_lineage_report.json"
        if not lineage_path.exists():
            raise FileNotFoundError(f"snapshot publish requires raw-to-feature lineage report: {lineage_path}")
        requirements.update(
            {
                "feature_dataset_certification_required": True,
                "feature_dataset_certification_path": str(certification_path),
                "feature_dataset_certification_status": str(certification.get("status") or "").strip(),
                "raw_to_feature_lineage_report_path": str(lineage_path),
            }
        )
    return requirements


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish immutable ready snapshot for derived data-platform datasets.")
    parser.add_argument("command", choices=("publish", "show"))
    parser.add_argument("--project-root", default=".")
    args = parser.parse_args()

    project_root = Path(args.project_root).resolve()
    if str(args.command).strip().lower() == "publish":
        result = publish_ready_snapshot(project_root=project_root)
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        return 0

    payload = load_ready_snapshot(project_root=project_root)
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
