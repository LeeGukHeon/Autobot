"""Machine-readable data contract registry builders."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any


DATA_CONTRACT_REGISTRY_VERSION = 1
DEFAULT_REGISTRY_REL_PATH = Path("data") / "_meta" / "data_contract_registry.json"
MAX_SHA256_BYTES = 16 * 1024 * 1024


def build_data_contract_registry(*, project_root: str | Path) -> dict[str, Any]:
    root = Path(project_root).resolve()
    data_root = root / "data"
    entries: list[dict[str, Any]] = []

    raw_ws_root = _resolve_existing_root(
        data_root / "raw_ws" / "upbit" / "public",
        data_root / "raw_ws" / "upbit",
    )
    raw_ticks_root = _resolve_existing_root(
        data_root / "raw_ticks" / "upbit" / "trades",
        data_root / "raw_ticks" / "upbit",
    )

    if raw_ws_root is not None:
        entries.append(
            _build_raw_contract_entry(
                project_root=root,
                contract_id="raw_ws_dataset:upbit_public",
                layer="raw_ws_dataset",
                dataset_name="upbit_public",
                root_path=raw_ws_root,
            )
        )
    if raw_ticks_root is not None:
        entries.append(
            _build_raw_contract_entry(
                project_root=root,
                contract_id="raw_ticks_dataset:upbit_trades",
                layer="raw_ticks_dataset",
                dataset_name="upbit_trades",
                root_path=raw_ticks_root,
            )
        )

    parquet_root = data_root / "parquet"
    if parquet_root.exists():
        for dataset_root in sorted(path for path in parquet_root.iterdir() if path.is_dir()):
            meta_dir = dataset_root / "_meta"
            if not meta_dir.exists():
                continue
            layer = "micro_dataset" if (meta_dir / "aggregate_report.json").exists() else "parquet_dataset"
            contract_id = f"{layer}:{dataset_root.name}"
            entries.append(
                _build_dataset_contract_entry(
                    project_root=root,
                    contract_id=contract_id,
                    layer=layer,
                    dataset_name=dataset_root.name,
                    dataset_root=dataset_root,
                )
            )

    features_root = data_root / "features"
    if features_root.exists():
        for dataset_root in sorted(path for path in features_root.iterdir() if path.is_dir()):
            meta_dir = dataset_root / "_meta"
            if not meta_dir.exists():
                continue
            entries.append(
                _build_dataset_contract_entry(
                    project_root=root,
                    contract_id=f"feature_dataset:{dataset_root.name}",
                    layer="feature_dataset",
                    dataset_name=dataset_root.name,
                    dataset_root=dataset_root,
                )
            )

    entries.sort(key=lambda item: (str(item.get("layer", "")), str(item.get("dataset_name", "")), str(item.get("root_path", ""))))
    contract_id_by_root = _build_contract_id_by_root(entries=entries)
    for entry in entries:
        source_roots = [str(path) for path in (entry.get("source_roots") or []) if str(path).strip()]
        entry["source_contract_ids"] = _source_contract_ids(
            source_roots=source_roots,
            contract_id_by_root=contract_id_by_root,
        )

    payload = {
        "version": DATA_CONTRACT_REGISTRY_VERSION,
        "project_root": str(root),
        "registry_path_default": str((root / DEFAULT_REGISTRY_REL_PATH).resolve()),
        "entries": entries,
        "summary": {
            "contract_count": len(entries),
            "layers": _layer_counts(entries=entries),
            "dataset_names": sorted({str(item.get("dataset_name", "")).strip() for item in entries if str(item.get("dataset_name", "")).strip()}),
        },
    }
    return payload


def write_data_contract_registry(
    *,
    project_root: str | Path,
    output_path: str | Path | None = None,
) -> Path:
    root = Path(project_root).resolve()
    path = Path(output_path).resolve() if output_path is not None else (root / DEFAULT_REGISTRY_REL_PATH)
    payload = build_data_contract_registry(project_root=root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _build_raw_contract_entry(
    *,
    project_root: Path,
    contract_id: str,
    layer: str,
    dataset_name: str,
    root_path: Path,
) -> dict[str, Any]:
    meta_dir = root_path.parent / "_meta"
    meta_files = _meta_artifacts(meta_dir=meta_dir)
    collect_report = _load_json(meta_dir / "ws_collect_report.json")
    health = _load_json(meta_dir / "ws_public_health.json")
    validate_report = _load_json(meta_dir / "ws_validate_report.json")
    source_roots = []
    raw_root_text = str((collect_report or {}).get("raw_root", "")).strip()
    if raw_root_text:
        source_roots.append(_normalize_root_reference(project_root=project_root, value=raw_root_text))
    status = "present"
    if health:
        status = "active" if bool(health.get("connected", False)) else "stale"
    elif validate_report:
        status = "validated"
    identity = _contract_identity(
        contract_id=contract_id,
        root_path=root_path,
        meta_files=meta_files,
    )
    return {
        "contract_id": contract_id,
        "layer": layer,
        "dataset_name": dataset_name,
        "root_path": _relpath(project_root, root_path),
        "meta_dir": _relpath(project_root, meta_dir) if meta_dir.exists() else None,
        "status": status,
        "identity": identity,
        "artifacts": meta_files,
        "source_roots": source_roots,
    }


def _build_dataset_contract_entry(
    *,
    project_root: Path,
    contract_id: str,
    layer: str,
    dataset_name: str,
    dataset_root: Path,
) -> dict[str, Any]:
    meta_dir = dataset_root / "_meta"
    meta_files = _meta_artifacts(meta_dir=meta_dir)
    feature_spec = _load_json(meta_dir / "feature_spec.json")
    build_report = _load_json(meta_dir / "build_report.json")
    aggregate_report = _load_json(meta_dir / "aggregate_report.json")
    validate_report = _load_json(meta_dir / "validate_report.json")

    source_roots: list[str] = []
    if layer == "micro_dataset":
        for key in ("raw_ws_root", "raw_ticks_root", "base_candles_root"):
            value = str((aggregate_report or {}).get(key, "")).strip()
            if not value:
                continue
            normalized = _normalize_root_reference(project_root=project_root, value=value)
            if normalized not in source_roots:
                source_roots.append(normalized)
    elif layer == "feature_dataset":
        for key in ("base_candles_root", "micro_root"):
            value = str((build_report or {}).get(key, "")).strip()
            if not value:
                continue
            normalized = _normalize_root_reference(project_root=project_root, value=value)
            if normalized not in source_roots:
                source_roots.append(normalized)
        for key in ("input_dataset_root", "dataset_root"):
            value = str((feature_spec or {}).get(key, "")).strip()
            if not value:
                continue
            normalized = _normalize_root_reference(project_root=project_root, value=value)
            if normalized not in source_roots:
                source_roots.append(normalized)

    status = "present"
    if validate_report:
        validate_fail_files = _safe_int(validate_report.get("fail_files"))
        validate_status = str(validate_report.get("status", "")).strip().upper()
        if validate_fail_files > 0 or validate_status == "FAIL":
            status = "invalid"
        else:
            status = "validated"
    elif build_report:
        build_status = str(build_report.get("status", "")).strip().upper()
        status = "built" if build_status in {"PASS", "WARN", "OK"} or not build_status else build_status.lower()

    identity = _contract_identity(
        contract_id=contract_id,
        root_path=dataset_root,
        meta_files=meta_files,
    )
    return {
        "contract_id": contract_id,
        "layer": layer,
        "dataset_name": dataset_name,
        "root_path": _relpath(project_root, dataset_root),
        "meta_dir": _relpath(project_root, meta_dir),
        "status": status,
        "identity": identity,
        "artifacts": meta_files,
        "source_roots": source_roots,
    }


def _meta_artifacts(*, meta_dir: Path) -> list[dict[str, Any]]:
    if not meta_dir.exists():
        return []
    artifacts: list[dict[str, Any]] = []
    for path in sorted(item for item in meta_dir.iterdir() if item.is_file()):
        artifacts.append(_artifact_descriptor(path))
    return artifacts


def _artifact_descriptor(path: Path) -> dict[str, Any]:
    size_bytes = int(path.stat().st_size)
    descriptor = {
        "name": path.name,
        "path": str(path),
        "size_bytes": size_bytes,
        "modified_ts_ms": int(path.stat().st_mtime * 1000),
        "sha256": "",
        "hash_mode": "size_mtime",
    }
    if size_bytes <= MAX_SHA256_BYTES:
        descriptor["sha256"] = _sha256_file(path)
        descriptor["hash_mode"] = "sha256"
    return descriptor


def _contract_identity(
    *,
    contract_id: str,
    root_path: Path,
    meta_files: list[dict[str, Any]],
) -> dict[str, Any]:
    tokens = [
        {
            "name": str(item.get("name", "")),
            "hash_mode": str(item.get("hash_mode", "")),
            "sha256": str(item.get("sha256", "")),
            "size_bytes": _safe_int(item.get("size_bytes")),
            "modified_ts_ms": _safe_int(item.get("modified_ts_ms")),
        }
        for item in meta_files
    ]
    digest_input = {
        "contract_id": contract_id,
        "root_path": str(root_path),
        "tokens": tokens,
    }
    digest = hashlib.sha256(json.dumps(digest_input, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()
    return {
        "mode": "meta_artifact_fingerprint_v1",
        "fingerprint": digest,
        "artifact_count": len(meta_files),
        "artifact_tokens": tokens,
    }


def _build_contract_id_by_root(*, entries: list[dict[str, Any]]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for entry in entries:
        contract_id = str(entry.get("contract_id", "")).strip()
        root_path = str(entry.get("root_path", "")).strip()
        if contract_id and root_path:
            mapping[root_path] = contract_id
    return mapping


def _source_contract_ids(*, source_roots: list[str], contract_id_by_root: dict[str, str]) -> list[str]:
    resolved: list[str] = []
    known_roots = sorted(contract_id_by_root.keys(), key=len, reverse=True)
    for root in source_roots:
        normalized = str(root).strip().replace("\\", "/")
        matched = ""
        for candidate in known_roots:
            candidate_norm = str(candidate).strip().replace("\\", "/")
            if normalized == candidate_norm or normalized.startswith(candidate_norm.rstrip("/") + "/"):
                matched = contract_id_by_root[candidate]
                break
        if matched and matched not in resolved:
            resolved.append(matched)
    return resolved


def _normalize_root_reference(*, project_root: Path, value: str) -> str:
    text = str(value).strip()
    if not text:
        return ""
    path = Path(text)
    if path.is_absolute():
        resolved = path
    else:
        resolved = (project_root / path).resolve()
    try:
        return _relpath(project_root, resolved)
    except ValueError:
        return str(resolved).replace("\\", "/")


def _resolve_existing_root(*paths: Path) -> Path | None:
    for path in paths:
        if path.exists():
            return path
    return None


def _relpath(project_root: Path, path: Path) -> str:
    try:
        return str(path.resolve().relative_to(project_root.resolve())).replace("\\", "/")
    except ValueError:
        return str(path.resolve()).replace("\\", "/")


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _layer_counts(*, entries: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for entry in entries:
        key = str(entry.get("layer", "")).strip()
        if not key:
            continue
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def _safe_int(value: Any) -> int:
    try:
        if value is None or value == "":
            return 0
        return int(value)
    except (TypeError, ValueError):
        return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build machine-readable data contract registry.")
    parser.add_argument("--project-root", default=".", help="Project root directory")
    parser.add_argument("--out", default="", help="Optional output path override")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    output_path = Path(str(args.out)).resolve() if str(args.out).strip() else None
    path = write_data_contract_registry(
        project_root=Path(str(args.project_root)),
        output_path=output_path,
    )
    print(f"[ops][data-contract-registry] path={path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
