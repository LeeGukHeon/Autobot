"""Model registry helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any
import uuid

import joblib


@dataclass(frozen=True)
class RegistrySavePayload:
    registry_root: Path
    model_family: str
    run_id: str
    model_bundle: Any
    metrics: dict[str, Any]
    thresholds: dict[str, Any]
    feature_spec: dict[str, Any]
    label_spec: dict[str, Any]
    train_config: dict[str, Any]
    data_fingerprint: dict[str, Any]
    leaderboard_row: dict[str, Any]
    model_card_text: str
    selection_recommendations: dict[str, Any] | None = None
    selection_policy: dict[str, Any] | None = None
    selection_calibration: dict[str, Any] | None = None
    runtime_recommendations: dict[str, Any] | None = None


ARTIFACT_STATUS_FILENAME = "artifact_status.json"
_PROMOTION_REQUIRED_ARTIFACTS = (
    ARTIFACT_STATUS_FILENAME,
    "leaderboard_row.json",
    "metrics.json",
    "thresholds.json",
    "selection_recommendations.json",
    "selection_policy.json",
    "selection_calibration.json",
    "walk_forward_report.json",
    "execution_acceptance_report.json",
    "runtime_recommendations.json",
    "promotion_decision.json",
    "trainer_research_evidence.json",
    "economic_objective_profile.json",
    "lane_governance.json",
    "decision_surface.json",
    "certification_report.json",
)
_PROMOTION_REQUIRED_STATUS_FIELDS = (
    "core_saved",
    "support_artifacts_written",
    "execution_acceptance_complete",
    "runtime_recommendations_complete",
    "governance_artifacts_complete",
    "acceptance_completed",
)


def make_run_id(*, seed: int | None = None) -> str:
    now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    token = uuid.uuid4().hex[:8]
    if seed is None:
        return f"{now}-{token}"
    return f"{now}-s{int(seed)}-{token}"


def save_run(payload: RegistrySavePayload, *, publish_pointers: bool = True) -> Path:
    run_dir = payload.registry_root / payload.model_family / payload.run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    model_path = run_dir / "model.bin"
    joblib.dump(payload.model_bundle, model_path)
    _write_json(run_dir / "metrics.json", payload.metrics)
    _write_json(run_dir / "thresholds.json", payload.thresholds)
    _write_json(run_dir / "feature_spec.json", payload.feature_spec)
    _write_json(run_dir / "label_spec.json", payload.label_spec)
    _write_yaml_like_json(run_dir / "train_config.yaml", payload.train_config)
    _write_json(run_dir / "data_fingerprint.json", payload.data_fingerprint)
    _write_json(run_dir / "leaderboard_row.json", payload.leaderboard_row)
    if isinstance(payload.selection_recommendations, dict) and payload.selection_recommendations:
        _write_json(run_dir / "selection_recommendations.json", payload.selection_recommendations)
    if isinstance(payload.selection_policy, dict) and payload.selection_policy:
        _write_json(run_dir / "selection_policy.json", payload.selection_policy)
    if isinstance(payload.selection_calibration, dict) and payload.selection_calibration:
        _write_json(run_dir / "selection_calibration.json", payload.selection_calibration)
    if isinstance(payload.runtime_recommendations, dict) and payload.runtime_recommendations:
        _write_json(run_dir / "runtime_recommendations.json", payload.runtime_recommendations)
    (run_dir / "model_card.md").write_text(payload.model_card_text.rstrip() + "\n", encoding="utf-8")

    if publish_pointers:
        update_latest_pointer(payload.registry_root, payload.model_family, payload.run_id)
        update_latest_pointer(payload.registry_root, "_global", payload.run_id, family=payload.model_family)
    return run_dir


def artifact_status_path(run_dir: Path) -> Path:
    return run_dir / ARTIFACT_STATUS_FILENAME


def load_artifact_status(run_dir: Path) -> dict[str, Any]:
    return _normalize_artifact_status(run_dir.name, load_json(artifact_status_path(run_dir)))


def update_artifact_status(run_dir: Path, **changes: Any) -> Path:
    current = load_artifact_status(run_dir)
    current.update(changes)
    _write_json(artifact_status_path(run_dir), _normalize_artifact_status(run_dir.name, current))
    return artifact_status_path(run_dir)


def verify_run_completeness(
    run_dir: Path,
    *,
    require_acceptance_completed: bool = True,
) -> dict[str, Any]:
    required_artifacts = list(_PROMOTION_REQUIRED_ARTIFACTS)
    required_status_fields = list(_PROMOTION_REQUIRED_STATUS_FIELDS)
    if not require_acceptance_completed:
        required_artifacts = [name for name in required_artifacts if name != "certification_report.json"]
        required_status_fields = [name for name in required_status_fields if name != "acceptance_completed"]
    missing_artifacts = [name for name in required_artifacts if not (run_dir / name).exists()]
    artifact_status = load_artifact_status(run_dir)
    missing_status_fields = [name for name in required_status_fields if not bool(artifact_status.get(name, False))]
    return {
        "run_id": run_dir.name,
        "run_dir": str(run_dir),
        "checked_at_utc": _utc_now(),
        "ready": (not missing_artifacts) and (not missing_status_fields),
        "missing_artifacts": missing_artifacts,
        "missing_status_fields": missing_status_fields,
        "artifact_status": artifact_status,
    }


def ensure_run_completeness(
    run_dir: Path,
    *,
    require_acceptance_completed: bool = True,
) -> dict[str, Any]:
    completeness = verify_run_completeness(
        run_dir,
        require_acceptance_completed=require_acceptance_completed,
    )
    if completeness["ready"]:
        return completeness
    missing_artifacts = ", ".join(completeness["missing_artifacts"]) or "none"
    missing_status_fields = ", ".join(completeness["missing_status_fields"]) or "none"
    raise ValueError(
        "incomplete run cannot be promoted: "
        f"run_id='{run_dir.name}' "
        f"missing_artifacts=[{missing_artifacts}] "
        f"missing_status_fields=[{missing_status_fields}]"
    )


def update_pointer(
    registry_root: Path,
    model_family: str,
    run_id: str,
    *,
    pointer_name: str,
    family: str | None = None,
    extra: dict[str, Any] | None = None,
) -> Path:
    if model_family == "_global":
        path = registry_root / f"{pointer_name}.json"
        body = {
            "run_id": run_id,
            "model_family": family,
            "updated_at_utc": _utc_now(),
        }
    else:
        path = registry_root / model_family / f"{pointer_name}.json"
        body = {
            "run_id": run_id,
            "updated_at_utc": _utc_now(),
        }
    if extra:
        body.update(dict(extra))
    _write_json(path, body)
    return path


def update_latest_pointer(
    registry_root: Path,
    model_family: str,
    run_id: str,
    *,
    family: str | None = None,
) -> Path:
    return update_pointer(
        registry_root,
        model_family,
        run_id,
        pointer_name="latest",
        family=family,
    )


def update_latest_candidate_pointer(
    registry_root: Path,
    model_family: str,
    run_id: str,
    *,
    family: str | None = None,
) -> Path:
    return update_pointer(
        registry_root,
        model_family,
        run_id,
        pointer_name="latest_candidate",
        family=family,
    )


def set_champion_pointer(
    registry_root: Path,
    model_family: str,
    *,
    run_id: str,
    score: float,
    score_key: str = "test_precision_top5",
    extra: dict[str, Any] | None = None,
) -> Path:
    payload = {
        "score_key": score_key,
        "score": float(score),
    }
    if extra:
        payload.update(dict(extra))
    return update_pointer(
        registry_root,
        model_family,
        run_id,
        pointer_name="champion",
        extra=payload,
    )


def update_champion_pointer(
    registry_root: Path,
    model_family: str,
    *,
    run_id: str,
    score: float,
    score_key: str = "test_precision_top5",
) -> tuple[Path, bool]:
    family_dir = registry_root / model_family
    family_dir.mkdir(parents=True, exist_ok=True)
    champion_path = family_dir / "champion.json"
    previous = load_json(champion_path)
    previous_score = float(previous.get("score", -1e18)) if isinstance(previous, dict) else -1e18
    replaced = float(score) > previous_score
    if replaced:
        champion_path = set_champion_pointer(
            registry_root,
            model_family,
            run_id=run_id,
            score=score,
            score_key=score_key,
        )
    return champion_path, replaced


def promote_run_to_champion(
    registry_root: Path,
    *,
    model_ref: str,
    model_family: str | None = None,
    score_key: str = "test_precision_top5",
) -> dict[str, Any]:
    run_dir = resolve_run_dir(registry_root, model_ref=model_ref, model_family=model_family)
    completeness = ensure_run_completeness(run_dir, require_acceptance_completed=True)
    resolved_family = str(model_family).strip() if model_family else run_dir.parent.name
    leaderboard_row = load_json(run_dir / "leaderboard_row.json")
    if not leaderboard_row:
        raise FileNotFoundError(f"missing leaderboard_row.json at {run_dir}")
    if score_key not in leaderboard_row:
        raise ValueError(f"leaderboard_row missing score_key='{score_key}' at {run_dir}")

    score = float(leaderboard_row.get(score_key, 0.0))
    champion_path = set_champion_pointer(
        registry_root,
        resolved_family,
        run_id=run_dir.name,
        score=score,
        score_key=score_key,
        extra={
            "promotion_mode": "manual",
            "completeness_checked_at_utc": completeness["checked_at_utc"],
        },
    )

    promotion_path = run_dir / "promotion_decision.json"
    previous = load_json(promotion_path)
    payload = dict(previous) if isinstance(previous, dict) else {}
    payload.update(
        {
            "run_id": run_dir.name,
            "promote": True,
            "status": "champion",
            "reasons": [],
            "promotion_mode": "manual",
            "promoted_at_utc": _utc_now(),
            "score_key": score_key,
            "score": score,
        }
    )
    _write_json(promotion_path, payload)
    update_artifact_status(run_dir, status="champion", promoted=True)

    return {
        "run_id": run_dir.name,
        "run_dir": str(run_dir),
        "model_family": resolved_family,
        "score_key": score_key,
        "score": score,
        "champion_path": str(champion_path),
        "promotion_path": str(promotion_path),
    }


def list_runs(registry_root: Path, *, model_family: str | None = None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if model_family:
        roots = [registry_root / model_family]
    else:
        roots = [path for path in registry_root.iterdir() if path.is_dir()] if registry_root.exists() else []
    for family_dir in roots:
        for run_dir in sorted(family_dir.iterdir()) if family_dir.exists() else []:
            if not run_dir.is_dir():
                continue
            row_path = run_dir / "leaderboard_row.json"
            if not row_path.exists():
                continue
            row = load_json(row_path)
            if not isinstance(row, dict):
                continue
            row["model_family"] = row.get("model_family", family_dir.name)
            row["run_id"] = row.get("run_id", run_dir.name)
            row["run_dir"] = str(run_dir)
            rows.append(row)
    rows.sort(
        key=lambda item: (
            str(item.get("created_at_utc", "")),
            str(item.get("run_id", "")),
        ),
        reverse=True,
    )
    return rows


def resolve_run_dir(
    registry_root: Path,
    *,
    model_ref: str,
    model_family: str | None = None,
) -> Path:
    ref = str(model_ref).strip()
    if not ref:
        raise ValueError("model_ref must not be blank")
    if ref == "candidate":
        ref = "latest_candidate"

    as_path = Path(ref)
    if as_path.exists():
        return as_path

    if ref in {"latest", "champion", "latest_candidate"}:
        pointer = _load_pointer(registry_root, pointer_name=ref, model_family=model_family)
        if not pointer:
            raise FileNotFoundError(f"pointer '{ref}' not found")
        pointed_run_id = str(pointer.get("run_id", "")).strip()
        pointed_family = str(pointer.get("model_family", model_family or "")).strip()
        if not pointed_run_id:
            raise FileNotFoundError(f"pointer '{ref}' has no run_id")
        if pointed_family:
            return registry_root / pointed_family / pointed_run_id
        matches = _search_run_id(registry_root, pointed_run_id)
        if len(matches) == 1:
            return matches[0]
        raise FileNotFoundError(f"run_id '{pointed_run_id}' not uniquely found")

    if model_family:
        candidate = registry_root / model_family / ref
        if candidate.exists():
            return candidate

    matches = _search_run_id(registry_root, ref)
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise FileNotFoundError(f"run_id '{ref}' exists in multiple families; pass --model-family")
    raise FileNotFoundError(f"model ref not found: {ref}")


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def load_model_bundle(run_dir: Path) -> Any:
    model_path = run_dir / "model.bin"
    if not model_path.exists():
        raise FileNotFoundError(f"missing model artifact: {model_path}")
    return joblib.load(model_path)


def _load_pointer(
    registry_root: Path,
    *,
    pointer_name: str,
    model_family: str | None,
) -> dict[str, Any]:
    if model_family:
        return load_json(registry_root / model_family / f"{pointer_name}.json")
    return load_json(registry_root / f"{pointer_name}.json")


def _search_run_id(registry_root: Path, run_id: str) -> list[Path]:
    if not registry_root.exists():
        return []
    matches: list[Path] = []
    for family_dir in registry_root.iterdir():
        if not family_dir.is_dir():
            continue
        candidate = family_dir / run_id
        if candidate.exists() and candidate.is_dir():
            matches.append(candidate)
    return matches


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    tmp_path = path.parent / f".{path.name}.{uuid.uuid4().hex}.tmp"
    tmp_path.write_text(body, encoding="utf-8")
    tmp_path.replace(path)


def _write_yaml_like_json(path: Path, payload: Any) -> None:
    # Keep parser-free dependency surface for runtime by storing JSON content in .yaml extension.
    _write_json(path, payload)


def _normalize_artifact_status(run_id: str, payload: dict[str, Any] | None) -> dict[str, Any]:
    doc = dict(payload) if isinstance(payload, dict) else {}
    return {
        "run_id": str(doc.get("run_id", run_id)).strip() or run_id,
        "status": str(doc.get("status", "pending")).strip() or "pending",
        "core_saved": bool(doc.get("core_saved", False)),
        "support_artifacts_written": bool(doc.get("support_artifacts_written", False)),
        "execution_acceptance_complete": bool(doc.get("execution_acceptance_complete", False)),
        "runtime_recommendations_complete": bool(doc.get("runtime_recommendations_complete", False)),
        "governance_artifacts_complete": bool(doc.get("governance_artifacts_complete", False)),
        "acceptance_completed": bool(doc.get("acceptance_completed", False)),
        "candidate_adoptable": bool(doc.get("candidate_adoptable", False)),
        "candidate_adopted": bool(doc.get("candidate_adopted", False)),
        "promoted": bool(doc.get("promoted", False)),
        "updated_at_utc": str(doc.get("updated_at_utc", "")).strip() or _utc_now(),
    }


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
