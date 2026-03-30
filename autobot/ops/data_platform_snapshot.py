"""Immutable ready-snapshot publisher for derived v5 data-platform datasets."""

from __future__ import annotations

from datetime import datetime, timezone
import argparse
import json
import os
from pathlib import Path
import shutil
from typing import Any


CORE_DERIVED_DATASETS: tuple[str, ...] = (
    "candles_second_v1",
    "ws_candle_v1",
    "lob30_v1",
    "sequence_v1",
)


def ready_snapshot_pointer_path(*, project_root: Path) -> Path:
    return project_root / "data" / "_meta" / "data_platform_ready_snapshot.json"


def load_ready_snapshot(*, project_root: Path) -> dict[str, Any]:
    path = ready_snapshot_pointer_path(project_root=project_root)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def resolve_ready_snapshot_dataset_root(*, project_root: Path, dataset_name: str) -> Path | None:
    payload = load_ready_snapshot(project_root=project_root)
    datasets = dict(payload.get("datasets") or {})
    raw = datasets.get(str(dataset_name).strip())
    if not isinstance(raw, dict):
        return None
    path_value = str(raw.get("dataset_root") or "").strip()
    if not path_value:
        return None
    return Path(path_value)


def publish_ready_snapshot(
    *,
    project_root: Path,
    dataset_names: tuple[str, ...] = CORE_DERIVED_DATASETS,
) -> dict[str, Any]:
    resolved_project_root = Path(project_root).resolve()
    parquet_root = resolved_project_root / "data" / "parquet"
    snapshot_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    snapshot_root = resolved_project_root / "data" / "snapshots" / "data_platform" / snapshot_id
    datasets_root = snapshot_root / "data" / "parquet"
    summary_root = snapshot_root / "_meta"
    datasets_root.mkdir(parents=True, exist_ok=True)
    summary_root.mkdir(parents=True, exist_ok=True)

    dataset_payload: dict[str, Any] = {}
    for dataset_name in dataset_names:
        name = str(dataset_name).strip()
        if not name:
            continue
        source_root = parquet_root / name
        if not source_root.exists():
            raise FileNotFoundError(f"derived dataset root missing: {source_root}")
        target_root = datasets_root / name
        _copytree_hardlink(src=source_root, dst=target_root)
        dataset_payload[name] = {
            "dataset_root": str(target_root),
            "source_root": str(source_root),
            "build_report_path": str(target_root / "_meta" / "build_report.json"),
            "validate_report_path": str(target_root / "_meta" / "validate_report.json"),
            "manifest_path": str(target_root / "_meta" / "manifest.parquet"),
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
