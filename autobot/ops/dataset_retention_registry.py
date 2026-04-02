"""Explicit dataset retention policy registry builders."""

from __future__ import annotations

import argparse
from fnmatch import fnmatch
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_RETENTION_REL_PATH = Path("data") / "_meta" / "dataset_retention_registry.json"


_EXPLICIT_RETENTION_POLICIES: tuple[dict[str, str], ...] = (
    {
        "policy_id": "raw_ws_hot_v1",
        "policy_scope": "dataset",
        "layer_pattern": "raw_ws_dataset",
        "contract_id_pattern": "raw_ws_dataset:*",
        "dataset_name_pattern": "*",
        "root_path_pattern": "data/raw_ws/*",
        "retention_class": "hot",
        "policy_source": "explicit_retention_policy_v1",
        "reason": "raw websocket feeds are always-on operational inputs and must stay hot",
    },
    {
        "policy_id": "raw_ticks_hot_v1",
        "policy_scope": "dataset",
        "layer_pattern": "raw_ticks_dataset",
        "contract_id_pattern": "raw_ticks_dataset:*",
        "dataset_name_pattern": "*",
        "root_path_pattern": "data/raw_ticks/*",
        "retention_class": "hot",
        "policy_source": "explicit_retention_policy_v1",
        "reason": "raw tick collection is an upstream operational dataset and must stay hot",
    },
    {
        "policy_id": "runtime_hot_v1",
        "policy_scope": "dataset",
        "layer_pattern": "runtime",
        "contract_id_pattern": "runtime:*",
        "dataset_name_pattern": "*",
        "root_path_pattern": "data/state/*",
        "retention_class": "hot",
        "policy_source": "explicit_retention_policy_v1",
        "reason": "runtime state and checkpoints are hot operational state",
    },
    {
        "policy_id": "live_hot_v1",
        "policy_scope": "dataset",
        "layer_pattern": "live",
        "contract_id_pattern": "live:*",
        "dataset_name_pattern": "*",
        "root_path_pattern": "data/raw_ws/*",
        "retention_class": "hot",
        "policy_source": "explicit_retention_policy_v1",
        "reason": "live provider contracts are operationally hot",
    },
    {
        "policy_id": "micro_warm_v1",
        "policy_scope": "dataset",
        "layer_pattern": "micro_dataset",
        "contract_id_pattern": "micro_dataset:*",
        "dataset_name_pattern": "*",
        "root_path_pattern": "data/parquet/*",
        "retention_class": "warm",
        "policy_source": "explicit_retention_policy_v1",
        "reason": "micro aggregate datasets are reusable derived data and stay warm",
    },
    {
        "policy_id": "parquet_warm_v1",
        "policy_scope": "dataset",
        "layer_pattern": "parquet_dataset",
        "contract_id_pattern": "parquet_dataset:*",
        "dataset_name_pattern": "*",
        "root_path_pattern": "data/parquet/*",
        "retention_class": "warm",
        "policy_source": "explicit_retention_policy_v1",
        "reason": "parquet dataset layers stay warm by default",
    },
    {
        "policy_id": "feature_warm_v1",
        "policy_scope": "dataset",
        "layer_pattern": "feature_dataset",
        "contract_id_pattern": "feature_dataset:*",
        "dataset_name_pattern": "*",
        "root_path_pattern": "data/features/*",
        "retention_class": "warm",
        "policy_source": "explicit_retention_policy_v1",
        "reason": "feature datasets are reusable derived inputs and stay warm",
    },
    {
        "policy_id": "meta_cold_v1",
        "policy_scope": "artifact",
        "layer_pattern": "meta_report",
        "contract_id_pattern": "artifact:*",
        "dataset_name_pattern": "*",
        "root_path_pattern": "data/**/_meta/*",
        "retention_class": "cold",
        "policy_source": "explicit_retention_policy_v1",
        "reason": "meta, lineage, validation, and certification artifacts are cold reports",
    },
    {
        "policy_id": "archive_backup_v1",
        "policy_scope": "artifact",
        "layer_pattern": "archive_backup",
        "contract_id_pattern": "artifact:*backup*",
        "dataset_name_pattern": "*",
        "root_path_pattern": "**/*backup*",
        "retention_class": "archive",
        "policy_source": "explicit_retention_policy_v1",
        "reason": "backup-like artifacts should be archived rather than treated as live datasets",
    },
    {
        "policy_id": "scratch_default_v1",
        "policy_scope": "artifact",
        "layer_pattern": "scratch",
        "contract_id_pattern": "artifact:*scratch*",
        "dataset_name_pattern": "*",
        "root_path_pattern": "**/*scratch*",
        "retention_class": "scratch",
        "policy_source": "explicit_retention_policy_v1",
        "reason": "scratch artifacts are purge candidates",
    },
    {
        "policy_id": "fallback_cold_v1",
        "policy_scope": "default",
        "layer_pattern": "*",
        "contract_id_pattern": "*",
        "dataset_name_pattern": "*",
        "root_path_pattern": "*",
        "retention_class": "cold",
        "policy_source": "explicit_retention_policy_v1",
        "reason": "unknown datasets fail closed to cold retention",
    },
)


def resolve_retention_policy(
    *,
    layer: str,
    contract_id: str = "",
    dataset_name: str = "",
    root_path: str = "",
) -> dict[str, str]:
    layer_value = _normalize_match_value(layer)
    contract_value = _normalize_match_value(contract_id)
    dataset_value = _normalize_match_value(dataset_name)
    root_value = _normalize_match_value(root_path)
    for item in _EXPLICIT_RETENTION_POLICIES:
        if _matches_policy(
            policy=item,
            layer=layer_value,
            contract_id=contract_value,
            dataset_name=dataset_value,
            root_path=root_value,
        ):
            return dict(item)
    return dict(_EXPLICIT_RETENTION_POLICIES[-1])


def resolve_retention_class(
    *,
    layer: str,
    contract_id: str = "",
    dataset_name: str = "",
    root_path: str = "",
) -> str:
    policy = resolve_retention_policy(
        layer=layer,
        contract_id=contract_id,
        dataset_name=dataset_name,
        root_path=root_path,
    )
    return str(policy.get("retention_class") or "cold").strip() or "cold"


def build_dataset_retention_registry(*, project_root: str | Path) -> dict[str, Any]:
    root = Path(project_root).resolve()
    resolved_entries = _discover_resolved_retention_entries(project_root=root)
    retention_classes = sorted(
        {
            str(item.get("retention_class") or "").strip()
            for item in resolved_entries
            if str(item.get("retention_class") or "").strip()
        }
    )
    return {
        "policy": "dataset_retention_registry_v2",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "project_root": str(root),
        "registry_path_default": str((root / DEFAULT_RETENTION_REL_PATH).resolve()),
        "policy_entries": [dict(item) for item in _EXPLICIT_RETENTION_POLICIES],
        "entries": resolved_entries,
        "resolved_entries": resolved_entries,
        "summary": {
            "policy_count": len(_EXPLICIT_RETENTION_POLICIES),
            "entry_count": len(resolved_entries),
            "retention_classes": retention_classes,
        },
    }


def write_dataset_retention_registry(
    *,
    project_root: str | Path,
    output_path: str | Path | None = None,
) -> Path:
    root = Path(project_root).resolve()
    path = Path(output_path).resolve() if output_path is not None else (root / DEFAULT_RETENTION_REL_PATH)
    payload = build_dataset_retention_registry(project_root=root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _discover_resolved_retention_entries(*, project_root: Path) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    data_root = project_root / "data"

    for contract_id, layer, dataset_name, path in (
        ("raw_ws_dataset:upbit_public", "raw_ws_dataset", "upbit_public", data_root / "raw_ws" / "upbit" / "public"),
        ("raw_ws_dataset:upbit_private", "raw_ws_dataset", "upbit_private", data_root / "raw_ws" / "upbit" / "private"),
        ("raw_ticks_dataset:upbit_trades", "raw_ticks_dataset", "upbit_trades", data_root / "raw_ticks" / "upbit" / "trades"),
    ):
        if path.exists():
            entries.append(
                _build_resolved_entry(
                    project_root=project_root,
                    contract_id=contract_id,
                    layer=layer,
                    dataset_name=dataset_name,
                    root_path=path,
                )
            )
    for meta_dir in (
        data_root / "raw_ws" / "upbit" / "_meta",
        data_root / "raw_ticks" / "upbit" / "_meta",
    ):
        _append_meta_entries(entries=entries, project_root=project_root, meta_dir=meta_dir)

    parquet_root = data_root / "parquet"
    if parquet_root.exists():
        for dataset_root in sorted(path for path in parquet_root.iterdir() if path.is_dir()):
            meta_dir = dataset_root / "_meta"
            if not meta_dir.exists():
                continue
            layer = "micro_dataset" if (meta_dir / "aggregate_report.json").exists() else "parquet_dataset"
            entries.append(
                _build_resolved_entry(
                    project_root=project_root,
                    contract_id=f"{layer}:{dataset_root.name}",
                    layer=layer,
                    dataset_name=dataset_root.name,
                    root_path=dataset_root,
                )
            )
            _append_meta_entries(entries=entries, project_root=project_root, meta_dir=meta_dir)

    features_root = data_root / "features"
    if features_root.exists():
        for dataset_root in sorted(path for path in features_root.iterdir() if path.is_dir()):
            meta_dir = dataset_root / "_meta"
            if not meta_dir.exists():
                continue
            entries.append(
                _build_resolved_entry(
                    project_root=project_root,
                    contract_id=f"feature_dataset:{dataset_root.name}",
                    layer="feature_dataset",
                    dataset_name=dataset_root.name,
                    root_path=dataset_root,
                )
            )
            for meta_file in sorted(path for path in meta_dir.iterdir() if path.is_file()):
                entries.append(
                    _build_resolved_entry(
                        project_root=project_root,
                        contract_id=f"artifact:{dataset_root.name}:{meta_file.name}",
                        layer="meta_report",
                        dataset_name=dataset_root.name,
                        root_path=meta_file,
                    )
                )

    if (data_root / "raw_ws" / "upbit" / "public").exists() and (features_root / "features_v4").exists():
        entries.append(
            _build_resolved_entry(
                project_root=project_root,
                contract_id="live:features_v4_online",
                layer="live",
                dataset_name="features_v4_online",
                root_path=data_root / "raw_ws" / "upbit" / "public",
            )
        )

    for contract_id, dataset_name, path in (
        ("runtime:live_main", "live_main", _first_existing_path(
            data_root / "state" / "live" / "live_state.db",
            data_root / "state" / "live_state.db",
        )),
        ("runtime:live_candidate", "live_candidate", _first_existing_path(
            data_root / "state" / "live_canary" / "live_state.db",
            data_root / "state" / "live_candidate" / "live_state.db",
        )),
    ):
        if path is not None:
            entries.append(
                _build_resolved_entry(
                    project_root=project_root,
                    contract_id=contract_id,
                    layer="runtime",
                    dataset_name=dataset_name,
                    root_path=path,
                )
            )

    data_meta_root = data_root / "_meta"
    if data_meta_root.exists():
        _append_meta_entries(entries=entries, project_root=project_root, meta_dir=data_meta_root, dataset_name="data_meta")

    return entries


def _build_resolved_entry(
    *,
    project_root: Path,
    contract_id: str,
    layer: str,
    dataset_name: str,
    root_path: Path,
) -> dict[str, Any]:
    rel_path = _relpath(project_root, root_path)
    policy = resolve_retention_policy(
        layer=layer,
        contract_id=contract_id,
        dataset_name=dataset_name,
        root_path=rel_path,
    )
    return {
        "contract_id": contract_id,
        "layer": layer,
        "dataset_name": dataset_name,
        "root_path": rel_path,
        "retention_class": str(policy.get("retention_class") or "").strip(),
        "policy_id": str(policy.get("policy_id") or "").strip(),
        "policy_scope": str(policy.get("policy_scope") or "").strip(),
        "policy_source": str(policy.get("policy_source") or "").strip(),
        "policy_reason": str(policy.get("reason") or "").strip(),
    }


def _append_meta_entries(
    *,
    entries: list[dict[str, Any]],
    project_root: Path,
    meta_dir: Path,
    dataset_name: str = "",
) -> None:
    if not meta_dir.exists():
        return
    dataset_label = str(dataset_name or meta_dir.parent.name).strip() or "meta"
    for meta_file in sorted(path for path in meta_dir.iterdir() if path.is_file()):
        entries.append(
            _build_resolved_entry(
                project_root=project_root,
                contract_id=f"artifact:{dataset_label}:{meta_file.name}",
                layer="meta_report",
                dataset_name=dataset_label,
                root_path=meta_file,
            )
        )


def _matches_policy(
    *,
    policy: dict[str, str],
    layer: str,
    contract_id: str,
    dataset_name: str,
    root_path: str,
) -> bool:
    return (
        fnmatch(layer, _normalize_match_value(policy.get("layer_pattern")))
        and fnmatch(contract_id, _normalize_match_value(policy.get("contract_id_pattern")))
        and fnmatch(dataset_name, _normalize_match_value(policy.get("dataset_name_pattern")))
        and fnmatch(root_path, _normalize_match_value(policy.get("root_path_pattern")))
    )


def _normalize_match_value(value: Any) -> str:
    text = str(value or "").strip().replace("\\", "/")
    return text or "*"


def _first_existing_path(*paths: Path) -> Path | None:
    for path in paths:
        if path.exists():
            return path
    return None


def _relpath(project_root: Path, path: Path) -> str:
    try:
        return str(path.resolve().relative_to(project_root.resolve())).replace("\\", "/")
    except ValueError:
        return str(path.resolve()).replace("\\", "/")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build dataset retention registry.")
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--output-path", default="")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    path = write_dataset_retention_registry(
        project_root=Path(args.project_root),
        output_path=(Path(args.output_path) if str(args.output_path).strip() else None),
    )
    print(f"[ops][dataset-retention-registry] path={path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
