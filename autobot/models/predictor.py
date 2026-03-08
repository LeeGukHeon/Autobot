"""Registry-backed model predictor helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np

from .dataset_loader import feature_columns_from_spec
from .registry import load_json, load_model_bundle, resolve_run_dir
from .train_v1 import _predict_scores


@dataclass(frozen=True)
class ModelPredictor:
    run_dir: Path
    model_bundle: dict[str, Any]
    model_ref: str
    model_family: str | None
    feature_columns: tuple[str, ...]
    train_config: dict[str, Any]
    thresholds: dict[str, Any]
    selection_recommendations: dict[str, Any]
    runtime_recommendations: dict[str, Any] = field(default_factory=dict)

    @property
    def dataset_root(self) -> Path | None:
        value = str(self.train_config.get("dataset_root", "")).strip()
        if not value:
            return None
        return Path(value)

    def predict_scores(self, x: np.ndarray) -> np.ndarray:
        return _predict_scores(self.model_bundle, x)


def load_predictor_from_registry(
    *,
    registry_root: Path,
    model_ref: str,
    model_family: str | None = None,
) -> ModelPredictor:
    run_dir = resolve_run_dir(
        registry_root=Path(registry_root),
        model_ref=str(model_ref).strip(),
        model_family=(str(model_family).strip() if model_family else None),
    )
    train_config = load_json(run_dir / "train_config.yaml")
    if not train_config:
        raise ValueError(f"invalid train_config.yaml at {run_dir}")
    model_bundle_raw = load_model_bundle(run_dir)
    if not isinstance(model_bundle_raw, dict):
        raise ValueError(f"unsupported model bundle type at {run_dir}: {type(model_bundle_raw)!r}")
    thresholds = load_json(run_dir / "thresholds.json")
    selection_recommendations = load_json(run_dir / "selection_recommendations.json")
    if not selection_recommendations:
        raw_train_recommendations = train_config.get("selection_recommendations")
        if isinstance(raw_train_recommendations, dict):
            selection_recommendations = raw_train_recommendations
    runtime_recommendations = load_json(run_dir / "runtime_recommendations.json")
    if not runtime_recommendations:
        raw_runtime_recommendations = train_config.get("runtime_recommendations")
        if isinstance(raw_runtime_recommendations, dict):
            runtime_recommendations = raw_runtime_recommendations

    feature_columns = tuple(str(item) for item in train_config.get("feature_columns", []))
    if not feature_columns:
        dataset_root_raw = str(train_config.get("dataset_root", "")).strip()
        if not dataset_root_raw:
            raise ValueError(f"train_config missing dataset_root and feature_columns: {run_dir}")
        dataset_root = Path(dataset_root_raw)
        if not dataset_root.exists():
            raise FileNotFoundError(f"dataset_root not found: {dataset_root}")
        feature_columns = feature_columns_from_spec(dataset_root)

    return ModelPredictor(
        run_dir=run_dir,
        model_bundle=model_bundle_raw,
        model_ref=str(model_ref).strip(),
        model_family=(str(model_family).strip() if model_family else None),
        feature_columns=feature_columns,
        train_config=train_config,
        thresholds=thresholds if isinstance(thresholds, dict) else {},
        selection_recommendations=selection_recommendations if isinstance(selection_recommendations, dict) else {},
        runtime_recommendations=runtime_recommendations if isinstance(runtime_recommendations, dict) else {},
    )
