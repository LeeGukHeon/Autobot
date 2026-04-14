"""Registry-backed model predictor helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np

from .dataset_loader import feature_columns_from_spec
from .registry import load_json, load_model_bundle, resolve_run_dir
from .runtime_recommendation_contract import normalize_runtime_recommendations_payload
from .selection_calibration import apply_selection_calibration, normalize_selection_calibration
from .selection_policy import build_selection_policy_from_recommendations, normalize_selection_policy
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
    selection_policy: dict[str, Any] = field(default_factory=dict)
    selection_calibration: dict[str, Any] = field(default_factory=dict)
    predictor_contract: dict[str, Any] = field(default_factory=dict)
    entry_boundary_contract: dict[str, Any] = field(default_factory=dict)

    @property
    def dataset_root(self) -> Path | None:
        value = str(self.train_config.get("dataset_root", "")).strip()
        if not value:
            return None
        return Path(value)

    def predict_scores(self, x: np.ndarray) -> np.ndarray:
        return _predict_scores(self.model_bundle, x)

    def predict_selection_scores(self, x: np.ndarray, *, score_source: str | None = None) -> np.ndarray:
        resolved_source = str(
            score_source
            or (self.selection_policy.get("score_source") if isinstance(self.selection_policy, dict) else "")
            or "score_mean"
        ).strip() or "score_mean"
        score_contract = self.predict_score_contract(x)
        if resolved_source == "score_lcb":
            base_scores = score_contract["score_lcb"]
        else:
            base_scores = score_contract["final_rank_score"]
        return apply_selection_calibration(
            base_scores,
            self.selection_calibration if isinstance(self.selection_calibration, dict) else {},
            score_source=resolved_source,
        )

    def predict_uncertainty(self, x: np.ndarray) -> np.ndarray | None:
        estimator = self.model_bundle.get("estimator") if isinstance(self.model_bundle, dict) else None
        if estimator is None or not hasattr(estimator, "predict_uncertainty"):
            return None
        values = np.asarray(estimator.predict_uncertainty(x), dtype=np.float64)
        if values.ndim != 1:
            raise ValueError("predict_uncertainty must return a 1-D array")
        return values

    def predict_distributional_contract(self, x: np.ndarray) -> dict[str, Any]:
        estimator = self.model_bundle.get("estimator") if isinstance(self.model_bundle, dict) else None
        if estimator is None or not hasattr(estimator, "predict_distributional_contract"):
            return {}
        payload = estimator.predict_distributional_contract(x)
        return dict(payload) if isinstance(payload, dict) else {}

    def predict_score_contract(self, x: np.ndarray) -> dict[str, np.ndarray]:
        estimator = self.model_bundle.get("estimator") if isinstance(self.model_bundle, dict) else None
        if estimator is not None and hasattr(estimator, "predict_edge2stage_contract"):
            payload = estimator.predict_edge2stage_contract(x)
            if isinstance(payload, dict):
                result = {key: np.asarray(value, dtype=np.float64) for key, value in payload.items() if key != "final_trade_flag"}
                if "final_trade_flag" in payload:
                    result["final_trade_flag"] = np.asarray(payload["final_trade_flag"])
                size = len(next(iter(result.values()))) if result else 0
                if "final_rank_score" not in result and "final_go_score" in result:
                    result["final_rank_score"] = np.asarray(result["final_go_score"], dtype=np.float64)
                if "score_mean" not in result and "final_go_score" in result:
                    result["score_mean"] = np.asarray(result["final_go_score"], dtype=np.float64)
                if "score_std" not in result:
                    result["score_std"] = np.full(size, np.nan, dtype=np.float64)
                if "score_lcb" not in result and "score_mean" in result:
                    result["score_lcb"] = np.asarray(result["score_mean"], dtype=np.float64)
                if "final_tradability" not in result and "final_tradeable_prob" in result:
                    result["final_tradability"] = np.asarray(result["final_tradeable_prob"], dtype=np.float64)
                if "final_expected_return" not in result and "final_expected_net_edge_bps" in result:
                    result["final_expected_return"] = np.asarray(result["final_expected_net_edge_bps"], dtype=np.float64) / 10_000.0
                if "final_expected_es" not in result:
                    result["final_expected_es"] = np.zeros(size, dtype=np.float64)
                if "final_alpha_lcb" not in result and "final_expected_return" in result:
                    result["final_alpha_lcb"] = np.asarray(result["final_expected_return"], dtype=np.float64)
                result["uncertainty_available"] = np.full(size, False, dtype=bool)
                return result
        if estimator is not None and hasattr(estimator, "predict_panel_contract"):
            payload = estimator.predict_panel_contract(x)
            if isinstance(payload, dict):
                result = {
                    key: np.asarray(value, dtype=np.float64) if key != "uncertainty_available" else np.asarray(value)
                    for key, value in payload.items()
                    if value is not None
                }
                if "uncertainty_available" not in result:
                    size = len(next(iter(result.values()))) if result else 0
                    result["uncertainty_available"] = np.full(size, True, dtype=bool)
                return result
        score_mean = self.predict_scores(x).astype(np.float64, copy=False)
        score_std = self.predict_uncertainty(x)
        distributional = self.predict_distributional_contract(x)
        mu_by_horizon = dict(distributional.get("mu_by_horizon") or {})
        es_by_horizon = dict(distributional.get("expected_shortfall_proxy_by_horizon") or {})
        primary_mu = None
        primary_es = None
        for key in ("h12", "h6", "h3", "h24"):
            if key in mu_by_horizon:
                primary_mu = np.asarray(mu_by_horizon[key], dtype=np.float64)
                break
        for key in ("h12", "h6", "h3", "h24"):
            if key in es_by_horizon:
                primary_es = np.abs(np.asarray(es_by_horizon[key], dtype=np.float64))
                break
        if score_std is None:
            score_lcb = score_mean.copy()
            uncertainty_available = False
        else:
            score_std = np.maximum(np.asarray(score_std, dtype=np.float64), 0.0)
            score_lcb = np.clip(score_mean - score_std, 0.0, 1.0)
            uncertainty_available = True
        if primary_mu is None:
            primary_mu = np.full(score_mean.shape[0], np.nan, dtype=np.float64)
        if primary_es is None:
            primary_es = np.full(score_mean.shape[0], np.nan, dtype=np.float64)
        final_tradability = np.full(score_mean.shape[0], np.nan, dtype=np.float64)
        valid_proxy = np.isfinite(primary_es) & np.isfinite(score_std if score_std is not None else np.full(score_mean.shape[0], np.nan))
        if np.any(valid_proxy):
            safe_std = score_std if score_std is not None else np.full(score_mean.shape[0], np.nan, dtype=np.float64)
            proxy_values = np.clip(1.0 / (1.0 + np.abs(primary_es) + np.abs(safe_std)), 0.0, 1.0)
            final_tradability = proxy_values.astype(np.float64, copy=False)
        final_alpha_lcb = primary_mu - primary_es - (score_std if score_std is not None else np.full(score_mean.shape[0], np.nan, dtype=np.float64))
        return {
            "final_rank_score": score_mean,
            "final_uncertainty": score_std if uncertainty_available else np.full(score_mean.shape[0], np.nan, dtype=np.float64),
            "score_mean": score_mean,
            "score_std": score_std if uncertainty_available else np.full(score_mean.shape[0], np.nan, dtype=np.float64),
            "score_lcb": score_lcb,
            "final_expected_return": primary_mu,
            "final_expected_es": primary_es,
            "final_tradability": final_tradability,
            "final_alpha_lcb": final_alpha_lcb,
            "uncertainty_available": np.full(score_mean.shape[0], uncertainty_available, dtype=bool),
        }


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
    selection_calibration = load_json(run_dir / "selection_calibration.json")
    if not selection_recommendations:
        raw_train_recommendations = train_config.get("selection_recommendations")
        if isinstance(raw_train_recommendations, dict):
            selection_recommendations = raw_train_recommendations
    if not selection_calibration:
        raw_train_calibration = train_config.get("selection_calibration")
        if isinstance(raw_train_calibration, dict):
            selection_calibration = raw_train_calibration
    runtime_recommendations = load_json(run_dir / "runtime_recommendations.json")
    if not runtime_recommendations:
        raw_runtime_recommendations = train_config.get("runtime_recommendations")
        if isinstance(raw_runtime_recommendations, dict):
            runtime_recommendations = raw_runtime_recommendations
    if runtime_recommendations:
        runtime_recommendations = normalize_runtime_recommendations_payload(runtime_recommendations)
    selection_policy = load_json(run_dir / "selection_policy.json")
    if not selection_policy:
        raw_train_policy = train_config.get("selection_policy")
        if isinstance(raw_train_policy, dict):
            selection_policy = raw_train_policy
    if not selection_policy and selection_recommendations:
        selection_policy = build_selection_policy_from_recommendations(
            selection_recommendations=selection_recommendations,
            fallback_threshold_key="top_5pct",
        )
    if selection_policy:
        selection_policy = normalize_selection_policy(selection_policy, fallback_threshold_key="top_5pct")
    if selection_calibration:
        selection_calibration = normalize_selection_calibration(selection_calibration)
    predictor_contract = load_json(run_dir / "predictor_contract.json")
    if not predictor_contract:
        raw_predictor_contract = train_config.get("predictor_contract")
        if isinstance(raw_predictor_contract, dict):
            predictor_contract = raw_predictor_contract
    entry_boundary_contract = load_json(run_dir / "entry_boundary_contract.json")
    if not entry_boundary_contract:
        raw_entry_boundary_contract = train_config.get("entry_boundary_contract")
        if isinstance(raw_entry_boundary_contract, dict):
            entry_boundary_contract = raw_entry_boundary_contract

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
        selection_policy=selection_policy if isinstance(selection_policy, dict) else {},
        selection_calibration=selection_calibration if isinstance(selection_calibration, dict) else {},
        predictor_contract=predictor_contract if isinstance(predictor_contract, dict) else {},
        entry_boundary_contract=entry_boundary_contract if isinstance(entry_boundary_contract, dict) else {},
    )
