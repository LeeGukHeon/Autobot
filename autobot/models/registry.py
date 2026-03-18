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
        extra={"promotion_mode": "manual"},
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


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
