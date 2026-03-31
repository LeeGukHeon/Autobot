"""Helpers for certification-window expert table exports."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


_EXPORTS_DIRNAME = "_runtime_exports"
_EXPORT_FILENAME = "expert_prediction_table.parquet"
_METADATA_FILENAME = "metadata.json"


def build_expert_export_window_id(start: str, end: str) -> str:
    start_text = str(start).strip()
    end_text = str(end).strip()
    if not start_text or not end_text:
        raise ValueError("expert export window requires non-empty start/end")
    return f"{start_text}__{end_text}"


def resolve_expert_runtime_export_paths(run_dir: Path, start: str, end: str) -> dict[str, Path | str]:
    window_id = build_expert_export_window_id(start, end)
    export_root = Path(run_dir) / _EXPORTS_DIRNAME / window_id
    return {
        "window_id": window_id,
        "export_root": export_root,
        "export_path": export_root / _EXPORT_FILENAME,
        "metadata_path": export_root / _METADATA_FILENAME,
    }


def load_existing_expert_runtime_export(run_dir: Path, start: str, end: str) -> dict[str, Any]:
    paths = resolve_expert_runtime_export_paths(run_dir, start, end)
    metadata_path = Path(str(paths["metadata_path"]))
    export_path = Path(str(paths["export_path"]))
    metadata: dict[str, Any] = {}
    if metadata_path.exists():
        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        except Exception:
            metadata = {}
    return {
        "paths": paths,
        "exists": export_path.exists() and metadata_path.exists(),
        "metadata": metadata,
    }


def write_expert_runtime_export_metadata(
    *,
    run_dir: Path,
    start: str,
    end: str,
    payload: dict[str, Any],
) -> Path:
    paths = resolve_expert_runtime_export_paths(run_dir, start, end)
    export_root = Path(str(paths["export_root"]))
    export_root.mkdir(parents=True, exist_ok=True)
    metadata_path = Path(str(paths["metadata_path"]))
    doc = dict(payload or {})
    doc.setdefault("window_id", str(paths["window_id"]))
    doc.setdefault("export_path", str(paths["export_path"]))
    metadata_path.write_text(
        json.dumps(doc, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return metadata_path
