"""Machine-readable data contract registry builders."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sqlite3
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
    raw_private_ws_root = _resolve_existing_root(
        data_root / "raw_ws" / "upbit" / "private",
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
                collect_report_name="ws_collect_report.json",
                health_snapshot_name="ws_public_health.json",
                status_mode="ws_connected",
                artifact_prefixes=("ws_",),
            )
        )
    if raw_private_ws_root is not None:
        entries.append(
            _build_raw_contract_entry(
                project_root=root,
                contract_id="raw_ws_dataset:upbit_private",
                layer="raw_ws_dataset",
                dataset_name="upbit_private",
                root_path=raw_private_ws_root,
                collect_report_name="private_ws_collect_report.json",
                health_snapshot_name="private_ws_health.json",
                status_mode="artifact_presence",
                artifact_prefixes=("private_ws_",),
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
                collect_report_name="ticks_collect_report.json",
                health_snapshot_name="ticks_daily_latest.json",
                status_mode="artifact_presence",
                artifact_prefixes=("ticks_",),
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

    feature_v4_root = features_root / "features_v4"
    micro_v1_root = parquet_root / "micro_v1"
    if raw_ws_root is not None and feature_v4_root.exists():
        entries.append(
            _build_live_feature_contract_entry(
                project_root=root,
                contract_id="live:features_v4_online",
                dataset_name="features_v4_online",
                raw_ws_root=raw_ws_root,
                micro_root=(micro_v1_root if micro_v1_root.exists() else None),
                feature_root=feature_v4_root,
            )
        )

    for contract_id, dataset_name, db_path in (
        ("runtime:live_main", "live_main", _resolve_existing_root(data_root / "state" / "live" / "live_state.db", data_root / "state" / "live_state.db")),
        ("runtime:live_candidate", "live_candidate", _resolve_existing_root(data_root / "state" / "live_candidate" / "live_state.db")),
    ):
        if db_path is None:
            continue
        entries.append(
            _build_runtime_contract_entry(
                project_root=root,
                contract_id=contract_id,
                dataset_name=dataset_name,
                db_path=db_path,
                feature_root=(feature_v4_root if feature_v4_root.exists() else None),
                micro_root=(micro_v1_root if micro_v1_root.exists() else None),
                raw_ws_root=raw_ws_root,
            )
        )

    entries.sort(key=lambda item: (str(item.get("layer", "")), str(item.get("dataset_name", "")), str(item.get("root_path", ""))))
    contract_id_by_root = _build_contract_id_by_root(entries=entries)
    for entry in entries:
        explicit_source_ids = [
            str(item).strip()
            for item in (entry.get("source_contract_ids") or [])
            if str(item).strip()
        ]
        if explicit_source_ids:
            entry["source_contract_ids"] = explicit_source_ids
            continue
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
    collect_report_name: str,
    health_snapshot_name: str,
    status_mode: str,
    artifact_prefixes: tuple[str, ...],
) -> dict[str, Any]:
    meta_dir = root_path.parent / "_meta"
    meta_files = _meta_artifacts(meta_dir=meta_dir, artifact_prefixes=artifact_prefixes)
    collect_report = _load_json(meta_dir / collect_report_name)
    health = _load_json(meta_dir / health_snapshot_name)
    validate_report = _load_json(meta_dir / "ws_validate_report.json")
    source_roots = []
    raw_root_text = str((collect_report or {}).get("raw_root", "")).strip()
    if raw_root_text:
        normalized_root = _normalize_root_reference(project_root=project_root, value=raw_root_text)
        if normalized_root and normalized_root != _relpath(project_root, root_path):
            source_roots.append(normalized_root)
    status = "present"
    if str(status_mode).strip().lower() == "ws_connected":
        if health:
            status = "active" if bool(health.get("connected", False)) else "stale"
        elif validate_report:
            status = "validated"
    elif health or collect_report:
        status = "active"
    elif validate_report:
        status = "validated"
    validation_status = _validation_status_from_status(status=status, validate_report=validate_report)
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
        "validation_status": validation_status,
        "retention_class": _retention_class_for_layer(layer),
        "coverage_window": _coverage_window_from_reports(
            ws_health=health,
            validate_report=validate_report,
        ),
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
    coverage_window = _coverage_window_from_reports(
        build_report=build_report,
        aggregate_report=aggregate_report,
        validate_report=validate_report,
    )
    return {
        "contract_id": contract_id,
        "layer": layer,
        "dataset_name": dataset_name,
        "root_path": _relpath(project_root, dataset_root),
        "meta_dir": _relpath(project_root, meta_dir),
        "status": status,
        "validation_status": _validation_status_from_status(status=status, validate_report=validate_report),
        "retention_class": _retention_class_for_layer(layer),
        "coverage_window": coverage_window,
        "identity": identity,
        "artifacts": meta_files,
        "source_roots": source_roots,
    }


def _build_live_feature_contract_entry(
    *,
    project_root: Path,
    contract_id: str,
    dataset_name: str,
    raw_ws_root: Path,
    micro_root: Path | None,
    feature_root: Path,
) -> dict[str, Any]:
    ws_meta_dir = raw_ws_root.parent / "_meta"
    feature_meta_dir = feature_root / "_meta"
    ws_health = _load_json(ws_meta_dir / "ws_public_health.json")
    ws_validate = _load_json(ws_meta_dir / "ws_validate_report.json")
    feature_validate = _load_json(feature_meta_dir / "validate_report.json")
    feature_build = _load_json(feature_meta_dir / "build_report.json")
    artifacts = _meta_artifacts(meta_dir=ws_meta_dir) + _meta_artifacts(meta_dir=feature_meta_dir)
    root_path = raw_ws_root
    live_ready = bool(ws_health.get("connected", False))
    feature_ready = str(feature_validate.get("status", "")).strip().upper() in {"PASS", "OK"} and _safe_int(feature_validate.get("fail_files")) <= 0
    status = "active" if live_ready and feature_ready else ("degraded" if live_ready else "stale")
    source_roots = [
        _relpath(project_root, raw_ws_root),
        _relpath(project_root, feature_root),
    ]
    if micro_root is not None:
        source_roots.append(_relpath(project_root, micro_root))
    return {
        "contract_id": contract_id,
        "layer": "live",
        "dataset_name": dataset_name,
        "root_path": _relpath(project_root, root_path),
        "meta_dir": None,
        "status": status,
        "validation_status": "ready" if feature_ready else _validation_status_from_status(status=status, validate_report=feature_validate or ws_validate),
        "retention_class": _retention_class_for_layer("live"),
        "coverage_window": _coverage_window_from_reports(
            ws_health=ws_health,
            build_report=feature_build,
            validate_report=feature_validate,
        ),
        "identity": _contract_identity(
            contract_id=contract_id,
            root_path=root_path,
            meta_files=artifacts,
        ),
        "artifacts": artifacts,
        "source_roots": source_roots,
        "source_contract_ids": [
            "raw_ws_dataset:upbit_public",
            *(["micro_dataset:micro_v1"] if micro_root is not None else []),
            f"feature_dataset:{feature_root.name}",
        ],
    }


def _build_runtime_contract_entry(
    *,
    project_root: Path,
    contract_id: str,
    dataset_name: str,
    db_path: Path,
    feature_root: Path | None,
    micro_root: Path | None,
    raw_ws_root: Path | None,
) -> dict[str, Any]:
    runtime_state = _runtime_state_summary(db_path=db_path)
    artifacts = [_artifact_descriptor(db_path)]
    runtime_run_id = str(runtime_state.get("live_runtime_model_run_id") or "").strip()
    status = "active" if runtime_run_id else "present"
    source_roots: list[str] = []
    source_contract_ids: list[str] = []
    if raw_ws_root is not None:
        source_roots.append(_relpath(project_root, raw_ws_root))
        source_contract_ids.append("raw_ws_dataset:upbit_public")
    if micro_root is not None:
        source_roots.append(_relpath(project_root, micro_root))
        source_contract_ids.append("micro_dataset:micro_v1")
    if feature_root is not None:
        source_roots.append(_relpath(project_root, feature_root))
        source_contract_ids.append(f"feature_dataset:{feature_root.name}")
    source_contract_ids.append("live:features_v4_online")
    return {
        "contract_id": contract_id,
        "layer": "runtime",
        "dataset_name": dataset_name,
        "root_path": _relpath(project_root, db_path),
        "meta_dir": None,
        "status": status,
        "validation_status": "runtime_contract_present" if runtime_run_id else "runtime_contract_missing",
        "retention_class": _retention_class_for_layer("runtime"),
        "coverage_window": dict(runtime_state.get("coverage_window") or {}),
        "runtime_state": runtime_state,
        "identity": _contract_identity(
            contract_id=contract_id,
            root_path=db_path,
            meta_files=artifacts,
        ),
        "artifacts": artifacts,
        "source_roots": source_roots,
        "source_contract_ids": [item for item in source_contract_ids if item],
    }


def _meta_artifacts(*, meta_dir: Path, artifact_prefixes: tuple[str, ...] | None = None) -> list[dict[str, Any]]:
    if not meta_dir.exists():
        return []
    artifacts: list[dict[str, Any]] = []
    for path in sorted(item for item in meta_dir.iterdir() if item.is_file()):
        if artifact_prefixes:
            name = str(path.name)
            if not any(name.startswith(prefix) for prefix in artifact_prefixes):
                continue
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


def _runtime_state_summary(*, db_path: Path) -> dict[str, Any]:
    if not db_path.exists():
        return {}
    checkpoint_rows: list[sqlite3.Row] = []
    conn: sqlite3.Connection | None = None
    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        checkpoint_rows = conn.execute(
            "SELECT name, payload_json, ts_ms FROM checkpoints"
        ).fetchall()
    except sqlite3.Error:
        checkpoint_rows = []
    finally:
        try:
            conn.close()
        except Exception:
            pass
    checkpoint_names = [str(row["name"]) for row in checkpoint_rows if row["name"]]
    live_runtime_contract = next((row for row in checkpoint_rows if str(row["name"]) == "live_runtime_contract"), None)
    payload = _parse_json_text(live_runtime_contract["payload_json"]) if live_runtime_contract is not None else {}
    ts_values = [_safe_int(row["ts_ms"]) for row in checkpoint_rows if _safe_int(row["ts_ms"]) > 0]
    if ts_values:
        coverage_window = {
            "start_ts_ms": min(ts_values),
            "end_ts_ms": max(ts_values),
            "source": "checkpoints",
        }
    else:
        coverage_window = {
            "start_ts_ms": int(db_path.stat().st_mtime * 1000),
            "end_ts_ms": int(db_path.stat().st_mtime * 1000),
            "source": "db_mtime",
        }
    return {
        "db_path": str(db_path),
        "checkpoint_names": checkpoint_names,
        "checkpoint_count": len(checkpoint_names),
        "live_runtime_model_run_id": str(payload.get("live_runtime_model_run_id") or "").strip() or None,
        "coverage_window": coverage_window,
    }


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _parse_json_text(value: Any) -> dict[str, Any]:
    if value in (None, ""):
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _coverage_window_from_reports(
    *,
    ws_health: dict[str, Any] | None = None,
    build_report: dict[str, Any] | None = None,
    aggregate_report: dict[str, Any] | None = None,
    validate_report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    candidates = []
    for payload in (ws_health or {}, build_report or {}, aggregate_report or {}, validate_report or {}):
        for key in ("min_ts_ms", "start_ts_ms", "from_ts_ms", "updated_at_ms"):
            value = _safe_int(payload.get(key))
            if value > 0:
                candidates.append(value)
        for key in ("max_ts_ms", "end_ts_ms", "to_ts_ms", "updated_at_ms"):
            value = _safe_int(payload.get(key))
            if value > 0:
                candidates.append(value)
    if not candidates:
        return {}
    return {
        "start_ts_ms": min(candidates),
        "end_ts_ms": max(candidates),
        "source": "artifact_reports",
    }


def _retention_class_for_layer(layer: str) -> str:
    key = str(layer).strip().lower()
    if "raw" in key:
        return "hot"
    if key in {"live", "runtime"}:
        return "hot"
    if "micro" in key or "feature" in key or "parquet" in key:
        return "warm"
    return "cold"


def _validation_status_from_status(*, status: str, validate_report: dict[str, Any] | None) -> str:
    report = dict(validate_report or {})
    explicit = str(report.get("status", "")).strip().upper()
    if explicit:
        return explicit.lower()
    status_value = str(status).strip().lower()
    if status_value in {"validated", "active"}:
        return "pass"
    if status_value in {"invalid", "stale"}:
        return "fail"
    return "unknown"


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
