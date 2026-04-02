"""Explicit raw-to-feature lineage report builders."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .data_contract_registry import build_data_contract_registry


DEFAULT_LINEAGE_REL_PATH = (
    Path("data") / "features" / "features_v4" / "_meta" / "raw_to_feature_lineage_report.json"
)


def build_raw_to_feature_lineage_report(
    *,
    project_root: str | Path,
    feature_set: str = "v4",
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    feature_set_value = str(feature_set).strip().lower() or "v4"
    dataset_name = f"features_{feature_set_value}"
    registry = build_data_contract_registry(project_root=root)
    feature_entry = _find_contract_entry(registry, contract_id=f"feature_dataset:{dataset_name}")
    live_entry = _find_contract_entry(registry, contract_id="live:features_v4_online")
    source_entries = {
        str(item.get("contract_id") or "").strip(): dict(item)
        for item in registry.get("entries") or []
        if isinstance(item, dict)
    }
    feature_sources = [source_entries.get(contract_id, {"contract_id": contract_id}) for contract_id in (feature_entry.get("source_contract_ids") or [])]
    live_sources = [source_entries.get(contract_id, {"contract_id": contract_id}) for contract_id in (live_entry.get("source_contract_ids") or [])]
    return {
        "policy": "raw_to_feature_lineage_report_v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "project_root": str(root),
        "feature_set": feature_set_value,
        "dataset_name": dataset_name,
        "report_path_default": str((root / DEFAULT_LINEAGE_REL_PATH).resolve()),
        "feature_contract": feature_entry,
        "feature_source_contracts": feature_sources,
        "live_feature_contract": live_entry,
        "live_source_contracts": live_sources,
    }


def write_raw_to_feature_lineage_report(
    *,
    project_root: str | Path,
    output_path: str | Path | None = None,
    feature_set: str = "v4",
) -> Path:
    root = Path(project_root).resolve()
    path = Path(output_path).resolve() if output_path is not None else (root / DEFAULT_LINEAGE_REL_PATH)
    payload = build_raw_to_feature_lineage_report(project_root=root, feature_set=feature_set)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _find_contract_entry(registry: dict[str, Any], *, contract_id: str) -> dict[str, Any]:
    for item in registry.get("entries") or []:
        if not isinstance(item, dict):
            continue
        if str(item.get("contract_id") or "").strip() == str(contract_id).strip():
            return dict(item)
    return {}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build raw-to-feature lineage report.")
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--feature-set", default="v4")
    parser.add_argument("--output-path", default="")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    path = write_raw_to_feature_lineage_report(
        project_root=Path(args.project_root),
        output_path=(Path(args.output_path) if str(args.output_path).strip() else None),
        feature_set=str(args.feature_set).strip().lower() or "v4",
    )
    print(f"[ops][raw-to-feature-lineage] path={path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
