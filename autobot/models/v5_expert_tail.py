"""Shared lightweight tail helpers for v5 expert trainers."""

from __future__ import annotations

import json
from pathlib import Path
import time
from typing import Any, Callable

from .registry import load_artifact_status, load_json, update_artifact_status, update_latest_pointer
from .v5_runtime_artifacts import persist_v5_runtime_governance_artifacts


EXPERT_TAIL_CONTEXT_FILENAME = "expert_tail_context.json"


def expert_tail_context_path(run_dir: Path) -> Path:
    return run_dir / EXPERT_TAIL_CONTEXT_FILENAME


def _expert_prediction_table_path(run_dir: Path) -> Path:
    return run_dir / "expert_prediction_table.parquet"


def _runtime_governance_paths(run_dir: Path) -> dict[str, Path]:
    return {
        "execution_acceptance_report_path": run_dir / "execution_acceptance_report.json",
        "runtime_recommendations_path": run_dir / "runtime_recommendations.json",
        "promotion_path": run_dir / "promotion_decision.json",
        "trainer_research_evidence_path": run_dir / "trainer_research_evidence.json",
        "economic_objective_profile_path": run_dir / "economic_objective_profile.json",
        "lane_governance_path": run_dir / "lane_governance.json",
        "decision_surface_path": run_dir / "decision_surface.json",
    }


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def build_v5_expert_tail_context(
    *,
    run_id: str,
    trainer_name: str,
    model_family: str,
    data_platform_ready_snapshot_id: str | None,
    dataset_root: Path,
    source_dataset_root: Path,
    runtime_dataset_root: Path,
    selected_markets: tuple[str, ...] | list[str],
    support_level_counts: dict[str, int],
    run_scope: str,
) -> dict[str, Any]:
    return {
        "version": 1,
        "source_of_truth": EXPERT_TAIL_CONTEXT_FILENAME,
        "run_id": str(run_id).strip(),
        "trainer": str(trainer_name).strip(),
        "model_family": str(model_family).strip(),
        "data_platform_ready_snapshot_id": str(data_platform_ready_snapshot_id or "").strip(),
        "dataset_root": str(Path(dataset_root).resolve()),
        "source_dataset_root": str(Path(source_dataset_root).resolve()),
        "runtime_dataset_root": str(Path(runtime_dataset_root).resolve()),
        "selected_markets": [str(item).strip().upper() for item in selected_markets if str(item).strip()],
        "support_level_counts": {str(key): int(value) for key, value in dict(support_level_counts or {}).items()},
        "run_scope": str(run_scope).strip(),
    }


def load_v5_expert_tail_context(*, run_dir: Path) -> dict[str, Any]:
    return load_json(expert_tail_context_path(run_dir))


def _normalize_tail_context(payload: dict[str, Any] | None) -> dict[str, Any]:
    doc = dict(payload) if isinstance(payload, dict) else {}
    return {
        "run_id": str(doc.get("run_id", "")).strip(),
        "trainer": str(doc.get("trainer", "")).strip(),
        "model_family": str(doc.get("model_family", "")).strip(),
        "data_platform_ready_snapshot_id": str(doc.get("data_platform_ready_snapshot_id") or "").strip(),
        "dataset_root": str(doc.get("dataset_root", "")).strip(),
        "source_dataset_root": str(doc.get("source_dataset_root", "")).strip(),
        "runtime_dataset_root": str(doc.get("runtime_dataset_root", "")).strip(),
        "selected_markets": [str(item).strip().upper() for item in (doc.get("selected_markets") or []) if str(item).strip()],
        "support_level_counts": {str(key): int(value) for key, value in dict(doc.get("support_level_counts") or {}).items()},
        "run_scope": str(doc.get("run_scope", "")).strip(),
    }


def v5_expert_tail_context_matches(existing: dict[str, Any] | None, expected: dict[str, Any] | None) -> bool:
    if not isinstance(existing, dict) or not isinstance(expected, dict):
        return False
    return _normalize_tail_context(existing) == _normalize_tail_context(expected)


def resolve_existing_v5_expert_tail_artifacts(*, run_dir: Path, tail_context: dict[str, Any]) -> dict[str, Any]:
    previous_context = load_v5_expert_tail_context(run_dir=run_dir)
    artifact_status = load_artifact_status(run_dir)
    artifacts: dict[str, dict[str, Any]] = {}
    for key, path in _runtime_governance_paths(run_dir).items():
        payload: dict[str, Any] | None = None
        if path.exists():
            payload = load_json(path)
        artifacts[key] = {
            "path": path,
            "exists": path.exists(),
            "payload": payload,
        }
    artifacts["expert_prediction_table_path"] = {
        "path": _expert_prediction_table_path(run_dir),
        "exists": _expert_prediction_table_path(run_dir).exists(),
        "payload": None,
    }
    return {
        "previous_context": previous_context,
        "context_matches": v5_expert_tail_context_matches(previous_context, tail_context),
        "artifact_status": artifact_status,
        "artifacts": artifacts,
    }


def v5_expert_tail_stage_reusable(*, existing_tail_artifacts: dict[str, Any], stage_name: str) -> bool:
    if not bool(existing_tail_artifacts.get("context_matches", False)):
        return False
    artifact_status = dict(existing_tail_artifacts.get("artifact_status") or {})
    artifacts = dict(existing_tail_artifacts.get("artifacts") or {})
    if stage_name == "runtime_governance":
        required = (
            "execution_acceptance_report_path",
            "runtime_recommendations_path",
            "promotion_path",
            "trainer_research_evidence_path",
            "economic_objective_profile_path",
            "lane_governance_path",
            "decision_surface_path",
        )
        return (
            bool(artifact_status.get("execution_acceptance_complete", False))
            and bool(artifact_status.get("runtime_recommendations_complete", False))
            and bool(artifact_status.get("governance_artifacts_complete", False))
            and bool(artifact_status.get("promotion_complete", False))
            and bool(artifact_status.get("decision_surface_complete", False))
            and all(bool((artifacts.get(key) or {}).get("payload")) for key in required)
        )
    if stage_name == "expert_prediction_table":
        return bool(artifact_status.get("expert_prediction_table_complete", False)) and bool(
            (artifacts.get("expert_prediction_table_path") or {}).get("exists", False)
        )
    return False


def _annotate_tail_payload(payload: dict[str, Any], *, tail_context: dict[str, Any], resumed: bool) -> dict[str, Any]:
    doc = dict(payload or {})
    doc["run_id"] = str(tail_context.get("run_id", "")).strip()
    doc["trainer"] = str(tail_context.get("trainer", "")).strip()
    doc["model_family"] = str(tail_context.get("model_family", "")).strip()
    doc["data_platform_ready_snapshot_id"] = str(tail_context.get("data_platform_ready_snapshot_id") or "").strip()
    doc["tail_context"] = dict(tail_context)
    doc["resumed"] = bool(resumed)
    return doc


def run_or_reuse_v5_runtime_governance_artifacts(
    *,
    run_dir: Path,
    trainer_name: str,
    model_family: str,
    run_scope: str,
    metrics: dict[str, Any],
    runtime_recommendations: dict[str, Any],
    promotion: dict[str, Any],
    trainer_research_reasons: list[str] | None,
    tail_context: dict[str, Any],
    existing_tail_artifacts: dict[str, Any],
    resumed: bool,
) -> dict[str, Any]:
    if v5_expert_tail_stage_reusable(existing_tail_artifacts=existing_tail_artifacts, stage_name="runtime_governance"):
        update_artifact_status(
            run_dir,
            execution_acceptance_complete=True,
            runtime_recommendations_complete=True,
            governance_artifacts_complete=True,
            promotion_complete=True,
            decision_surface_complete=True,
        )
        artifacts = dict(existing_tail_artifacts.get("artifacts") or {})
        return {
            "execution_acceptance_report_path": Path(str((artifacts.get("execution_acceptance_report_path") or {}).get("path"))),
            "runtime_recommendations_path": Path(str((artifacts.get("runtime_recommendations_path") or {}).get("path"))),
            "promotion_path": Path(str((artifacts.get("promotion_path") or {}).get("path"))),
            "trainer_research_evidence_path": Path(str((artifacts.get("trainer_research_evidence_path") or {}).get("path"))),
            "economic_objective_profile_path": Path(str((artifacts.get("economic_objective_profile_path") or {}).get("path"))),
            "lane_governance_path": Path(str((artifacts.get("lane_governance_path") or {}).get("path"))),
            "decision_surface_path": Path(str((artifacts.get("decision_surface_path") or {}).get("path"))),
            "execution_acceptance": dict((artifacts.get("execution_acceptance_report_path") or {}).get("payload") or {}),
            "runtime_recommendations": dict((artifacts.get("runtime_recommendations_path") or {}).get("payload") or {}),
            "promotion": dict((artifacts.get("promotion_path") or {}).get("payload") or {}),
            "trainer_research_evidence": dict((artifacts.get("trainer_research_evidence_path") or {}).get("payload") or {}),
            "economic_objective_profile": dict((artifacts.get("economic_objective_profile_path") or {}).get("payload") or {}),
            "lane_governance": dict((artifacts.get("lane_governance_path") or {}).get("payload") or {}),
            "decision_surface": dict((artifacts.get("decision_surface_path") or {}).get("payload") or {}),
        }

    runtime_artifacts = persist_v5_runtime_governance_artifacts(
        run_dir=run_dir,
        trainer_name=trainer_name,
        model_family=model_family,
        run_scope=run_scope,
        metrics=metrics,
        runtime_recommendations=runtime_recommendations,
        promotion=promotion,
        trainer_research_reasons=trainer_research_reasons,
    )
    annotated = {
        "execution_acceptance": _annotate_tail_payload(
            dict(runtime_artifacts.get("execution_acceptance") or {}),
            tail_context=tail_context,
            resumed=resumed,
        ),
        "runtime_recommendations": _annotate_tail_payload(
            dict(runtime_recommendations or {}),
            tail_context=tail_context,
            resumed=resumed,
        ),
        "promotion": _annotate_tail_payload(
            dict(promotion or {}),
            tail_context=tail_context,
            resumed=resumed,
        ),
        "trainer_research_evidence": _annotate_tail_payload(
            dict(runtime_artifacts.get("trainer_research_evidence") or {}),
            tail_context=tail_context,
            resumed=resumed,
        ),
        "economic_objective_profile": _annotate_tail_payload(
            dict(runtime_artifacts.get("economic_objective_profile") or {}),
            tail_context=tail_context,
            resumed=resumed,
        ),
        "lane_governance": _annotate_tail_payload(
            dict(runtime_artifacts.get("lane_governance") or {}),
            tail_context=tail_context,
            resumed=resumed,
        ),
        "decision_surface": _annotate_tail_payload(
            dict(runtime_artifacts.get("decision_surface") or {}),
            tail_context=tail_context,
            resumed=resumed,
        ),
    }
    _write_json(_runtime_governance_paths(run_dir)["execution_acceptance_report_path"], annotated["execution_acceptance"])
    _write_json(_runtime_governance_paths(run_dir)["runtime_recommendations_path"], annotated["runtime_recommendations"])
    _write_json(_runtime_governance_paths(run_dir)["promotion_path"], annotated["promotion"])
    _write_json(_runtime_governance_paths(run_dir)["trainer_research_evidence_path"], annotated["trainer_research_evidence"])
    _write_json(_runtime_governance_paths(run_dir)["economic_objective_profile_path"], annotated["economic_objective_profile"])
    _write_json(_runtime_governance_paths(run_dir)["lane_governance_path"], annotated["lane_governance"])
    _write_json(_runtime_governance_paths(run_dir)["decision_surface_path"], annotated["decision_surface"])
    update_artifact_status(
        run_dir,
        execution_acceptance_complete=True,
        runtime_recommendations_complete=True,
        governance_artifacts_complete=True,
        promotion_complete=True,
        decision_surface_complete=True,
    )
    return {
        **{key: value for key, value in runtime_artifacts.items() if key.endswith("_path")},
        **annotated,
    }


def run_or_reuse_v5_expert_prediction_table(
    *,
    run_dir: Path,
    existing_tail_artifacts: dict[str, Any],
    writer: Callable[[], Path],
) -> Path:
    path = _expert_prediction_table_path(run_dir)
    if v5_expert_tail_stage_reusable(existing_tail_artifacts=existing_tail_artifacts, stage_name="expert_prediction_table"):
        update_artifact_status(run_dir, expert_prediction_table_complete=True)
        return path
    written = writer()
    update_artifact_status(run_dir, expert_prediction_table_complete=True)
    return written


def finalize_v5_expert_family_run(
    *,
    run_dir: Path,
    run_id: str,
    registry_root: Path,
    model_family: str,
    logs_root: Path,
    report_name: str,
    report_payload: dict[str, Any],
    data_platform_ready_snapshot_id: str | None,
    resumed: bool,
    tail_started_at: float,
) -> Path:
    update_latest_pointer(registry_root, model_family, run_id)
    update_artifact_status(run_dir, status="candidate", support_artifacts_written=True)
    report = dict(report_payload)
    report["status"] = str(report.get("status", "candidate")).strip() or "candidate"
    report["data_platform_ready_snapshot_id"] = str(data_platform_ready_snapshot_id or "").strip()
    report["resumed"] = bool(resumed)
    report["tail_duration_sec"] = round(time.time() - tail_started_at, 3)
    report["artifact_status"] = load_artifact_status(run_dir)
    report_path = logs_root / report_name
    report_path.parent.mkdir(parents=True, exist_ok=True)
    _write_json(report_path, report)
    return report_path
