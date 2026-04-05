"""Stacked v5 panel ensemble trainer built on the current v4 artifact backbone."""

from __future__ import annotations

from dataclasses import dataclass, replace
import json
from pathlib import Path
import time
from typing import Any

import numpy as np
import polars as pl

from autobot import __version__ as autobot_version
from autobot.features.feature_spec import parse_date_to_ts_ms
from autobot.ops.data_platform_snapshot import resolve_ready_snapshot_id
from autobot.strategy.v5_post_model_contract import annotate_v5_runtime_recommendations

from . import train_v4_crypto_cs as v4
from . import train_v4_persistence as v4_persistence
from . import train_v4_postprocess as v4_postprocess
from .dataset_loader import (
    DatasetRequest,
    build_data_fingerprint,
    feature_columns_from_spec,
    load_feature_aux_frame,
    load_feature_dataset,
    load_feature_spec,
    load_label_spec,
)
from .factor_block_selector import (
    build_factor_block_selection_report,
    resolve_selected_feature_columns_from_latest,
    v4_factor_block_registry,
)
from .model_card import render_model_card
from .registry import (
    RegistrySavePayload,
    load_artifact_status,
    load_json,
    load_model_bundle,
    save_run,
    update_artifact_status,
    update_latest_pointer,
)
from .research_acceptance import compare_balanced_pareto, summarize_walk_forward_windows
from .search_budget import resolve_v4_search_budget
from .selection_calibration import build_selection_calibration_by_score_source, build_selection_calibration_from_oos_rows
from .selection_optimizer import SelectionGridConfig, build_selection_recommendations_from_walk_forward, build_window_selection_objectives
from .selection_policy import build_selection_policy_from_recommendations
from .split import SPLIT_DROP, SPLIT_TEST, SPLIT_TRAIN, SPLIT_VALID, compute_anchored_walk_forward_splits, compute_time_splits, split_masks
from .train_v1 import _build_thresholds, _evaluate_split, _predict_scores, build_selection_recommendations
from .train_v4_artifacts import build_decision_surface_v4, build_v4_metrics_doc, train_config_snapshot_v4
from .train_v4_core import prepare_v4_training_inputs
from .v5_runtime_artifacts import persist_v5_runtime_governance_artifacts
from .v5_domain_weighting import build_v5_domain_weighting_report, write_v5_domain_weighting_report
from .v5_expert_runtime_export import (
    OPERATING_WINDOW_TIMEZONE,
    build_ts_date_coverage_payload,
    load_existing_expert_runtime_export,
    parse_operating_date_to_ts_ms,
    resolve_expert_runtime_export_paths,
    write_expert_runtime_export_metadata,
)
from ..strategy.model_alpha_v1 import (
    ModelAlphaExecutionSettings,
    ModelAlphaExitSettings,
    ModelAlphaOperationalSettings,
    ModelAlphaPositionSettings,
    ModelAlphaSelectionSettings,
    ModelAlphaSettings,
)


TrainV5PanelEnsembleOptions = v4.TrainV4CryptoCsOptions
TrainV5PanelEnsembleResult = v4.TrainV4CryptoCsResult

_STACK_COMPONENT_ORDER = ("cls_score", "rank_score", "mu_h3", "mu_h6", "mu_h12", "mu_h24")
_PANEL_TAIL_CONTEXT_FILENAME = "panel_tail_context.json"
_RUNTIME_RECOMMENDATION_SEARCH_CACHE_FILENAME = "runtime_recommendation_search_cache.json"

@dataclass(frozen=True)
class _StackMetaModel:
    intercept: float
    coefficients: tuple[float, ...]
    feature_shift: tuple[float, ...]
    feature_scale: tuple[float, ...]

    def predict_proba(self, x: np.ndarray) -> np.ndarray:
        matrix = np.asarray(x, dtype=np.float64)
        shift = np.asarray(self.feature_shift, dtype=np.float64)
        scale = np.asarray(self.feature_scale, dtype=np.float64)
        coeff = np.asarray(self.coefficients, dtype=np.float64)
        safe_scale = np.where(np.abs(scale) < 1e-12, 1.0, scale)
        normalized = (matrix - shift) / safe_scale
        logits = normalized @ coeff + float(self.intercept)
        probs = 1.0 / (1.0 + np.exp(-np.clip(logits, -40.0, 40.0)))
        return np.column_stack([1.0 - probs, probs])


@dataclass(frozen=True)
class V5PanelEnsembleEstimator:
    classifier_bundle: dict[str, Any]
    ranker_bundle: dict[str, Any]
    auxiliary_classifier_bundles: dict[str, dict[str, Any]]
    auxiliary_ranker_bundles: dict[str, dict[str, Any]]
    regressor_bundles: dict[str, dict[str, Any]]
    regression_member_bundles: dict[str, tuple[dict[str, Any], ...]]
    meta_model: _StackMetaModel
    meta_ensemble: tuple[_StackMetaModel, ...]
    regression_horizons: tuple[int, ...]
    primary_horizon: int
    uncertainty_temperature: float = 1.0

    def _predict_regression_distribution(self, x: np.ndarray) -> dict[str, dict[str, np.ndarray]]:
        distributions: dict[str, dict[str, np.ndarray]] = {}
        for horizon in self.regression_horizons:
            horizon_key = f"h{int(horizon)}"
            base_bundle = self.regressor_bundles[horizon_key]
            member_bundles = self.regression_member_bundles.get(horizon_key) or ()
            member_predictions: list[np.ndarray] = []
            for member_bundle in member_bundles:
                estimator = member_bundle.get("estimator") if isinstance(member_bundle, dict) else None
                if estimator is None:
                    continue
                member_predictions.append(np.asarray(estimator.predict(x), dtype=np.float64))
            if not member_predictions:
                member_predictions.append(np.asarray(base_bundle["estimator"].predict(x), dtype=np.float64))
            member_matrix = np.column_stack(member_predictions)
            q10 = np.quantile(member_matrix, 0.10, axis=1)
            q50 = np.quantile(member_matrix, 0.50, axis=1)
            q90 = np.quantile(member_matrix, 0.90, axis=1)
            sigma = np.std(member_matrix, axis=1, ddof=0)
            es10 = np.array(
                [
                    float(np.mean(row[row <= quantile])) if np.any(row <= quantile) else float(quantile)
                    for row, quantile in zip(member_matrix, q10, strict=False)
                ],
                dtype=np.float64,
            )
            distributions[horizon_key] = {
                "member_matrix": member_matrix,
                "mu": np.asarray(base_bundle["estimator"].predict(x), dtype=np.float64),
                "q10": q10,
                "q50": q50,
                "q90": q90,
                "sigma": sigma,
                "expected_shortfall_proxy": es10,
            }
        return distributions

    def _component_payload(self, x: np.ndarray) -> dict[str, Any]:
        cls_score = _predict_scores(self.classifier_bundle, x)
        rank_score = _predict_scores(self.ranker_bundle, x)
        aux_cls_by_horizon = {
            key: _predict_scores(bundle["bundle"] if "bundle" in bundle else bundle, x)
            for key, bundle in self.auxiliary_classifier_bundles.items()
        }
        aux_rank_by_horizon = {
            key: _predict_scores(bundle["bundle"] if "bundle" in bundle else bundle, x)
            for key, bundle in self.auxiliary_ranker_bundles.items()
        }
        mu_by_horizon: dict[int, np.ndarray] = {}
        aux_cls_parts = [aux_cls_by_horizon[key] for key in sorted(aux_cls_by_horizon.keys(), key=lambda item: int(item.replace("h", "")))]
        aux_rank_parts = [aux_rank_by_horizon[key] for key in sorted(aux_rank_by_horizon.keys(), key=lambda item: int(item.replace("h", "")))]
        reg_prob_parts: list[np.ndarray] = []
        regression_distribution = self._predict_regression_distribution(x)
        for horizon in self.regression_horizons:
            horizon_key = f"h{int(horizon)}"
            raw = np.asarray(regression_distribution[horizon_key]["mu"], dtype=np.float64)
            mu_by_horizon[int(horizon)] = raw
            reg_prob_parts.append(_sigmoid(raw))
        component_matrix = np.column_stack([cls_score, rank_score, *aux_cls_parts, *aux_rank_parts, *reg_prob_parts])
        component_std = np.std(component_matrix, axis=1, ddof=0)
        return {
            "cls_score": cls_score,
            "rank_score": rank_score,
            "aux_cls_by_horizon": aux_cls_by_horizon,
            "aux_rank_by_horizon": aux_rank_by_horizon,
            "mu_by_horizon": mu_by_horizon,
            "regression_distribution": regression_distribution,
            "component_matrix": component_matrix,
            "component_std": component_std,
        }

    def predict_proba(self, x: np.ndarray) -> np.ndarray:
        payload = self._component_payload(x)
        return self.meta_model.predict_proba(payload["component_matrix"])

    def predict(self, x: np.ndarray) -> np.ndarray:
        return self.predict_proba(x)[:, 1]

    def predict_uncertainty(self, x: np.ndarray) -> np.ndarray:
        payload = self._component_payload(x)
        if self.meta_ensemble:
            member_scores = np.column_stack(
                [member.predict_proba(payload["component_matrix"])[:, 1] for member in self.meta_ensemble]
            )
            return np.std(member_scores, axis=1, ddof=0) * max(float(self.uncertainty_temperature), 1e-6)
        return np.asarray(payload["component_std"], dtype=np.float64) * max(float(self.uncertainty_temperature), 1e-6)

    def predict_mu_horizons(self, x: np.ndarray) -> dict[str, np.ndarray]:
        payload = self._component_payload(x)
        return {f"h{horizon}": values for horizon, values in payload["mu_by_horizon"].items()}

    def predict_distributional_contract(self, x: np.ndarray) -> dict[str, dict[str, np.ndarray]]:
        payload = self._component_payload(x)
        quantiles_by_horizon: dict[str, np.ndarray] = {}
        sigma_by_horizon: dict[str, np.ndarray] = {}
        es_proxy_by_horizon: dict[str, np.ndarray] = {}
        mu_by_horizon: dict[str, np.ndarray] = {}
        for horizon in self.regression_horizons:
            horizon_key = f"h{int(horizon)}"
            distribution = payload["regression_distribution"][horizon_key]
            quantiles_by_horizon[horizon_key] = np.column_stack(
                [
                    np.asarray(distribution["q10"], dtype=np.float64),
                    np.asarray(distribution["q50"], dtype=np.float64),
                    np.asarray(distribution["q90"], dtype=np.float64),
                ]
            )
            sigma_by_horizon[horizon_key] = np.asarray(distribution["sigma"], dtype=np.float64)
            es_proxy_by_horizon[horizon_key] = np.asarray(distribution["expected_shortfall_proxy"], dtype=np.float64)
            mu_by_horizon[horizon_key] = np.asarray(distribution["mu"], dtype=np.float64)
        return {
            "mu_by_horizon": mu_by_horizon,
            "return_quantiles_by_horizon": quantiles_by_horizon,
            "sigma_by_horizon": sigma_by_horizon,
            "expected_shortfall_proxy_by_horizon": es_proxy_by_horizon,
        }

    def predict_panel_contract(self, x: np.ndarray) -> dict[str, np.ndarray]:
        score_mean = self.predict(x).astype(np.float64, copy=False)
        score_std = self.predict_uncertainty(x).astype(np.float64, copy=False)
        distribution = self.predict_distributional_contract(x)
        primary_key = f"h{int(self.primary_horizon)}"
        mu_by_horizon = dict(distribution.get("mu_by_horizon") or {})
        es_by_horizon = dict(distribution.get("expected_shortfall_proxy_by_horizon") or {})
        primary_mu = np.asarray(mu_by_horizon.get(primary_key, score_mean), dtype=np.float64)
        primary_es_raw = np.asarray(es_by_horizon.get(primary_key, np.zeros_like(primary_mu)), dtype=np.float64)
        primary_es = np.abs(primary_es_raw)
        final_alpha_lcb = primary_mu - primary_es - score_std
        final_tradability = np.clip(1.0 / (1.0 + primary_es + score_std), 0.0, 1.0)
        score_lcb = np.clip(score_mean - score_std, 0.0, 1.0)
        return {
            "final_rank_score": score_mean,
            "final_uncertainty": score_std,
            "score_mean": score_mean,
            "score_std": score_std,
            "score_lcb": score_lcb,
            "final_expected_return": primary_mu,
            "final_expected_es": primary_es,
            "final_tradability": final_tradability,
            "final_alpha_lcb": final_alpha_lcb,
        }


def _sigmoid(values: np.ndarray) -> np.ndarray:
    raw = np.asarray(values, dtype=np.float64)
    return 1.0 / (1.0 + np.exp(-np.clip(raw, -40.0, 40.0)))


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _build_split_labels(*, train_mask: np.ndarray, valid_mask: np.ndarray, test_mask: np.ndarray, size: int) -> np.ndarray:
    labels = np.full(int(size), "drop", dtype=object)
    labels[np.asarray(train_mask, dtype=bool)] = "train"
    labels[np.asarray(valid_mask, dtype=bool)] = "valid"
    labels[np.asarray(test_mask, dtype=bool)] = "test"
    return labels


def _write_expert_prediction_table(
    *,
    run_dir: Path,
    dataset: Any,
    estimator: Any,
    primary_y_reg: np.ndarray,
    split_labels: np.ndarray,
    output_path: Path | None = None,
) -> Path:
    payload = estimator.predict_panel_contract(dataset.X)
    frame = pl.DataFrame(
        {
            "market": np.asarray(dataset.markets, dtype=object),
            "ts_ms": np.asarray(dataset.ts_ms, dtype=np.int64),
            "split": np.asarray(split_labels, dtype=object),
            "y_cls": np.asarray(dataset.y_cls, dtype=np.int64),
            "y_reg": np.asarray(primary_y_reg, dtype=np.float64),
            "final_rank_score": np.asarray(payload["final_rank_score"], dtype=np.float64),
            "final_uncertainty": np.asarray(payload["final_uncertainty"], dtype=np.float64),
            "score_mean": np.asarray(payload["score_mean"], dtype=np.float64),
            "score_std": np.asarray(payload["score_std"], dtype=np.float64),
            "score_lcb": np.asarray(payload["score_lcb"], dtype=np.float64),
            "final_expected_return": np.asarray(payload["final_expected_return"], dtype=np.float64),
            "final_expected_es": np.asarray(payload["final_expected_es"], dtype=np.float64),
            "final_tradability": np.asarray(payload["final_tradability"], dtype=np.float64),
            "final_alpha_lcb": np.asarray(payload["final_alpha_lcb"], dtype=np.float64),
        }
    ).sort(["ts_ms", "market"])
    resolved_output_path = Path(output_path) if output_path is not None else (run_dir / "expert_prediction_table.parquet")
    resolved_output_path.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(resolved_output_path)
    return resolved_output_path


def _load_panel_inference_dataset_window(
    *,
    run_dir: Path,
    start: str,
    end: str,
    selected_markets_override: tuple[str, ...] | None = None,
) -> tuple[Any, TrainV5PanelEnsembleOptions, dict[str, Any]]:
    train_config = load_json(run_dir / "train_config.yaml")
    if not train_config:
        raise FileNotFoundError(f"missing train_config.yaml in {run_dir}")
    options = replace(_options_from_v5_panel_train_config(train_config), start=str(start), end=str(end))
    feature_cols = tuple(str(item).strip() for item in (train_config.get("feature_columns") or []) if str(item).strip())
    if not feature_cols:
        feature_cols = feature_columns_from_spec(options.dataset_root)
    selected_markets = (
        tuple(str(item).strip().upper() for item in selected_markets_override if str(item).strip())
        if selected_markets_override is not None
        else tuple(str(item).strip().upper() for item in (train_config.get("selected_markets") or []) if str(item).strip())
    )
    request = DatasetRequest(
        dataset_root=Path(options.dataset_root),
        tf=str(options.tf).strip().lower(),
        quote=(str(options.quote).strip().upper() if options.quote else None),
        top_n=max(int(options.top_n), 1) if options.top_n is not None else None,
        start_ts_ms=parse_operating_date_to_ts_ms(options.start, timezone_name=OPERATING_WINDOW_TIMEZONE),
        end_ts_ms=parse_operating_date_to_ts_ms(options.end, end_of_day=True, timezone_name=OPERATING_WINDOW_TIMEZONE),
        markets=selected_markets,
        batch_rows=max(int(options.batch_rows), 1),
    )
    dataset = load_feature_dataset(
        request,
        feature_columns=feature_cols,
        y_cls_column=str(train_config.get("y_cls_column") or "y_cls"),
        y_reg_column=str(train_config.get("y_reg_column") or "y_reg"),
        y_rank_column=str(train_config.get("y_rank_column") or "y_rank"),
        drop_missing_targets=False,
    )
    return dataset, options, train_config


def _resolve_panel_runtime_export_dataset(
    *,
    run_dir: Path,
    start: str,
    end: str,
    selected_markets_override: tuple[str, ...] | None = None,
) -> tuple[Any, TrainV5PanelEnsembleOptions, dict[str, Any], list[str], str, str]:
    requested_selected_markets = (
        [str(item).strip().upper() for item in selected_markets_override if str(item).strip()]
        if selected_markets_override is not None
        else []
    )
    if selected_markets_override is not None:
        dataset, options, train_config = _load_panel_inference_dataset_window(
            run_dir=run_dir,
            start=start,
            end=end,
            selected_markets_override=selected_markets_override,
        )
        return (
            dataset,
            options,
            train_config,
            requested_selected_markets,
            "acceptance_common_runtime_universe",
            "",
        )

    dataset = None
    options = None
    train_config = None
    try:
        dataset, options, train_config = _load_panel_inference_dataset_window(run_dir=run_dir, start=start, end=end)
        requested_selected_markets = [
            str(item).strip().upper()
            for item in ((train_config or {}).get("selected_markets") or [])
            if str(item).strip()
        ]
        return (
            dataset,
            options,
            train_config,
            requested_selected_markets,
            "train_selected_markets",
            "",
        )
    except ValueError as exc:
        train_config = load_json(run_dir / "train_config.yaml")
        requested_selected_markets = [
            str(item).strip().upper()
            for item in ((train_config or {}).get("selected_markets") or [])
            if str(item).strip()
        ]
        if (not requested_selected_markets) or "no feature rows found for the requested train dataset" not in str(exc):
            raise
        dataset, options, train_config = _load_panel_inference_dataset_window(
            run_dir=run_dir,
            start=start,
            end=end,
            selected_markets_override=tuple(),
        )
        return (
            dataset,
            options,
            train_config,
            requested_selected_markets,
            "window_available_markets_fallback",
            "TRAIN_SELECTED_MARKETS_EMPTY_IN_RUNTIME_WINDOW",
        )


def _export_panel_expert_prediction_table_window(
    *,
    run_dir: Path,
    start: str,
    end: str,
    selected_markets_override: tuple[str, ...] | None = None,
    resolve_markets_only: bool = False,
) -> dict[str, Any]:
    run_dir = Path(run_dir).resolve()
    dataset, options, train_config, requested_selected_markets, selected_markets_source, fallback_reason = _resolve_panel_runtime_export_dataset(
        run_dir=run_dir,
        start=start,
        end=end,
        selected_markets_override=selected_markets_override,
    )
    data_platform_ready_snapshot_id = (
        str(train_config.get("data_platform_ready_snapshot_id") or "").strip()
        or resolve_ready_snapshot_id(project_root=Path.cwd())
    )
    existing_export = load_existing_expert_runtime_export(run_dir, start, end)
    existing_metadata = dict(existing_export.get("metadata") or {})
    paths = dict(existing_export.get("paths") or {})
    export_path = Path(str(paths.get("export_path")))
    metadata_path = Path(str(paths.get("metadata_path")))
    if (
        selected_markets_override is None
        and (not resolve_markets_only)
        and
        bool(existing_export.get("exists", False))
        and str(existing_metadata.get("run_id") or "").strip() == run_dir.name
        and str(existing_metadata.get("data_platform_ready_snapshot_id") or "").strip() == data_platform_ready_snapshot_id
        and str(existing_metadata.get("start") or "").strip() == str(start).strip()
        and str(existing_metadata.get("end") or "").strip() == str(end).strip()
        and existing_metadata.get("coverage_start_ts_ms") is not None
        and existing_metadata.get("coverage_end_ts_ms") is not None
        and str(existing_metadata.get("coverage_start_date") or "").strip()
        and str(existing_metadata.get("coverage_end_date") or "").strip()
        and str(existing_metadata.get("window_timezone") or "").strip() == OPERATING_WINDOW_TIMEZONE
    ):
        return {
            "run_id": run_dir.name,
            "trainer": "v5_panel_ensemble",
            "model_family": str(train_config.get("model_family") or options.model_family).strip(),
            "data_platform_ready_snapshot_id": data_platform_ready_snapshot_id,
            "start": str(start).strip(),
            "end": str(end).strip(),
            "coverage_start_ts_ms": int(existing_metadata.get("coverage_start_ts_ms", 0) or 0),
            "coverage_end_ts_ms": int(existing_metadata.get("coverage_end_ts_ms", 0) or 0),
            "coverage_start_date": str(existing_metadata.get("coverage_start_date") or ""),
            "coverage_end_date": str(existing_metadata.get("coverage_end_date") or ""),
            "coverage_dates": list(existing_metadata.get("coverage_dates") or []),
            "window_timezone": str(existing_metadata.get("window_timezone") or ""),
            "rows": int(existing_metadata.get("rows", 0) or 0),
            "requested_selected_markets": list(existing_metadata.get("requested_selected_markets") or []),
            "selected_markets": list(existing_metadata.get("selected_markets") or []),
            "selected_markets_source": str(existing_metadata.get("selected_markets_source") or ""),
            "fallback_reason": str(existing_metadata.get("fallback_reason") or ""),
            "export_path": str(export_path),
            "metadata_path": str(metadata_path),
            "reused": True,
            "source_mode": "existing_export",
        }

    ts_values = np.asarray(getattr(dataset, "ts_ms", np.asarray([], dtype=np.int64)), dtype=np.int64)
    coverage_payload = build_ts_date_coverage_payload(ts_values, timezone_name=OPERATING_WINDOW_TIMEZONE)
    metadata = {
        "version": 1,
        "policy": "v5_expert_runtime_export_v1",
        "run_id": run_dir.name,
        "trainer": "v5_panel_ensemble",
        "model_family": str(train_config.get("model_family") or options.model_family).strip(),
        "data_platform_ready_snapshot_id": data_platform_ready_snapshot_id,
        "start": str(start).strip(),
        "end": str(end).strip(),
        "coverage_start_ts_ms": int(ts_values.min()) if ts_values.size > 0 else 0,
        "coverage_end_ts_ms": int(ts_values.max()) if ts_values.size > 0 else 0,
        **coverage_payload,
        "generation_context_window": {
            "start": str(start).strip(),
            "end": str(end).strip(),
            "source": "output_window_only",
        },
        "output_window": {
            "start": str(start).strip(),
            "end": str(end).strip(),
        },
        "rows": int(dataset.rows),
        "requested_selected_markets": requested_selected_markets,
        "selected_markets": [str(item).strip().upper() for item in getattr(dataset, "selected_markets", ())],
        "selected_markets_source": selected_markets_source,
        "fallback_reason": fallback_reason,
    }
    if resolve_markets_only:
        return {
            **metadata,
            "export_path": "",
            "metadata_path": "",
            "reused": False,
            "source_mode": "resolve_markets_only",
        }

    model_bundle = load_model_bundle(run_dir)
    estimator = model_bundle.get("estimator") if isinstance(model_bundle, dict) else None
    if estimator is None:
        raise ValueError(f"run_dir does not contain a usable panel estimator: {run_dir}")
    split_labels = np.full(int(dataset.rows), "runtime", dtype=object)
    export_path = _write_expert_prediction_table(
        run_dir=run_dir,
        dataset=dataset,
        estimator=estimator,
        primary_y_reg=np.asarray(dataset.y_reg, dtype=np.float64),
        split_labels=split_labels,
        output_path=export_path,
    )
    metadata_path = write_expert_runtime_export_metadata(
        run_dir=run_dir,
        start=start,
        end=end,
        payload=metadata,
    )
    return {
        **metadata,
        "export_path": str(export_path),
        "metadata_path": str(metadata_path),
        "reused": False,
        "source_mode": "fresh_export",
    }


def materialize_v5_panel_ensemble_runtime_export(
    *,
    run_dir: Path,
    start: str,
    end: str,
    selected_markets_override: tuple[str, ...] | None = None,
    resolve_markets_only: bool = False,
) -> dict[str, Any]:
    return _export_panel_expert_prediction_table_window(
        run_dir=run_dir,
        start=start,
        end=end,
        selected_markets_override=selected_markets_override,
        resolve_markets_only=resolve_markets_only,
    )


def _normalize_optional_path_text(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    return str(Path(raw).resolve())


def _build_execution_evaluation_window_doc(*, options: TrainV5PanelEnsembleOptions) -> dict[str, Any]:
    start_text = str(getattr(options, "execution_acceptance_eval_start", "") or "").strip()
    end_text = str(getattr(options, "execution_acceptance_eval_end", "") or "").strip()
    label = str(getattr(options, "execution_acceptance_eval_label", "") or "").strip() or "train_window"
    source = str(getattr(options, "execution_acceptance_eval_source", "") or "").strip() or "train_command_window"
    if not start_text or not end_text:
        start_text = str(getattr(options, "start", "") or "").strip()
        end_text = str(getattr(options, "end", "") or "").strip()
        label = "train_window"
        source = "train_command_window"
    return {
        "start_ts_ms": int(parse_date_to_ts_ms(start_text)),
        "end_ts_ms": int(parse_date_to_ts_ms(end_text, end_of_day=True)),
        "label": label,
        "source": source,
    }


def _runtime_recommendation_profile_from_search_budget(search_budget_decision: dict[str, Any] | None) -> str:
    return (
        str(((search_budget_decision or {}).get("applied") or {}).get("runtime_recommendation_profile", "full")).strip()
        or "full"
    )


def _should_use_dependency_expert_only_mode(options: TrainV5PanelEnsembleOptions) -> bool:
    return bool(getattr(options, "dependency_expert_only", False))


def _panel_tail_mode(options: TrainV5PanelEnsembleOptions) -> str:
    if _should_use_dependency_expert_only_mode(options):
        return "dependency_expert_only"
    return "full"


def _panel_tail_context_path(run_dir: Path) -> Path:
    return run_dir / _PANEL_TAIL_CONTEXT_FILENAME


def _runtime_recommendation_search_cache_path(run_dir: Path) -> Path:
    return run_dir / _RUNTIME_RECOMMENDATION_SEARCH_CACHE_FILENAME


def _build_panel_runtime_artifact_paths(run_dir: Path) -> dict[str, Path]:
    return {
        "execution_acceptance_report_path": run_dir / "execution_acceptance_report.json",
        "runtime_recommendations_path": run_dir / "runtime_recommendations.json",
        "promotion_path": run_dir / "promotion_decision.json",
        "trainer_research_evidence_path": run_dir / "trainer_research_evidence.json",
        "economic_objective_profile_path": run_dir / "economic_objective_profile.json",
        "lane_governance_path": run_dir / "lane_governance.json",
        "decision_surface_path": run_dir / "decision_surface.json",
        "expert_prediction_table_path": run_dir / "expert_prediction_table.parquet",
    }


def _normalize_panel_tail_context(payload: dict[str, Any] | None) -> dict[str, Any]:
    doc = dict(payload) if isinstance(payload, dict) else {}
    execution_window = dict(doc.get("execution_window") or {})
    execution_acceptance = dict(doc.get("execution_acceptance") or {})
    return {
        "run_id": str(doc.get("run_id", "")).strip(),
        "model_family": str(doc.get("model_family", "")).strip(),
        "candidate_ref": str(doc.get("candidate_ref", "")).strip(),
        "data_platform_ready_snapshot_id": str(doc.get("data_platform_ready_snapshot_id") or "").strip(),
        "duplicate_candidate": bool(doc.get("duplicate_candidate", False)),
        "dependency_expert_only": bool(doc.get("dependency_expert_only", False)),
        "tail_mode": str(doc.get("tail_mode", "")).strip() or "full",
        "runtime_recommendation_profile": str(doc.get("runtime_recommendation_profile") or "").strip() or "full",
        "execution_window": {
            "start_ts_ms": int(execution_window.get("start_ts_ms", 0) or 0),
            "end_ts_ms": int(execution_window.get("end_ts_ms", 0) or 0),
            "label": str(execution_window.get("label", "")).strip() or "train_window",
            "source": str(execution_window.get("source", "")).strip() or "train_command_window",
        },
        "execution_acceptance": {
            "dataset_name": str(execution_acceptance.get("dataset_name", "")).strip() or "candles_v1",
            "parquet_root": _normalize_optional_path_text(execution_acceptance.get("parquet_root")),
            "output_root_dir": _normalize_optional_path_text(execution_acceptance.get("output_root_dir")),
            "tf": str(execution_acceptance.get("tf", "")).strip().lower(),
            "quote": str(execution_acceptance.get("quote", "")).strip().upper(),
            "top_n": max(int(execution_acceptance.get("top_n", 0) or 0), 0),
            "feature_set": str(execution_acceptance.get("feature_set", "")).strip().lower() or "v4",
            "execution_contract_artifact_path": str(
                execution_acceptance.get("execution_contract_artifact_path", "")
            ).strip(),
        },
    }


def _build_panel_tail_context(
    *,
    run_id: str,
    options: TrainV5PanelEnsembleOptions,
    data_platform_ready_snapshot_id: str | None,
    search_budget_decision: dict[str, Any] | None,
    duplicate_candidate: bool,
    live_domain_reweighting: dict[str, Any] | None = None,
) -> dict[str, Any]:
    execution_window = _build_execution_evaluation_window_doc(options=options)
    return {
        "version": 1,
        "source_of_truth": _PANEL_TAIL_CONTEXT_FILENAME,
        "run_id": str(run_id).strip(),
        "model_family": str(options.model_family).strip(),
        "candidate_ref": str(run_id).strip(),
        "data_platform_ready_snapshot_id": str(data_platform_ready_snapshot_id or "").strip(),
        "duplicate_candidate": bool(duplicate_candidate),
        "dependency_expert_only": _should_use_dependency_expert_only_mode(options),
        "tail_mode": _panel_tail_mode(options),
        "runtime_recommendation_profile": _runtime_recommendation_profile_from_search_budget(search_budget_decision),
        "live_domain_reweighting": dict(live_domain_reweighting or {}),
        "execution_window": execution_window,
        "execution_acceptance": {
            "dataset_name": str(options.execution_acceptance_dataset_name).strip() or "candles_v1",
            "parquet_root": _normalize_optional_path_text(options.execution_acceptance_parquet_root),
            "output_root_dir": _normalize_optional_path_text(options.execution_acceptance_output_root),
            "tf": str(options.tf).strip().lower(),
            "quote": str(options.quote).strip().upper(),
            "top_n": max(
                int(options.execution_acceptance_top_n)
                if int(options.execution_acceptance_top_n) > 0
                else int(options.top_n),
                1,
            ),
            "feature_set": str(options.feature_set).strip().lower() or "v4",
            "execution_contract_artifact_path": str(
                getattr(options, "execution_contract_artifact_path", "") or ""
            ).strip(),
        },
    }


def _load_panel_tail_context(*, run_dir: Path) -> dict[str, Any]:
    return load_json(_panel_tail_context_path(run_dir))


def _panel_tail_context_matches(existing: dict[str, Any] | None, expected: dict[str, Any] | None) -> bool:
    if not isinstance(existing, dict) or not isinstance(expected, dict):
        return False
    return _normalize_panel_tail_context(existing) == _normalize_panel_tail_context(expected)


def _resolve_existing_tail_artifacts(*, run_dir: Path, tail_context: dict[str, Any]) -> dict[str, Any]:
    paths = _build_panel_runtime_artifact_paths(run_dir)
    previous_context = _load_panel_tail_context(run_dir=run_dir)
    artifact_status = load_artifact_status(run_dir)
    artifacts: dict[str, dict[str, Any]] = {}
    for key, path in paths.items():
        payload: dict[str, Any] | None = None
        if path.suffix == ".json" and path.exists():
            payload = load_json(path)
        artifacts[key] = {
            "path": path,
            "exists": path.exists(),
            "payload": payload,
        }
    return {
        "previous_context": previous_context,
        "context_matches": _panel_tail_context_matches(previous_context, tail_context),
        "artifact_status": artifact_status,
        "artifacts": artifacts,
    }


def _tail_stage_is_reusable(*, existing_tail_artifacts: dict[str, Any], stage_name: str) -> bool:
    if not bool(existing_tail_artifacts.get("context_matches", False)):
        return False
    artifact_status = dict(existing_tail_artifacts.get("artifact_status") or {})
    artifacts = dict(existing_tail_artifacts.get("artifacts") or {})
    if stage_name == "execution_acceptance":
        return bool(artifact_status.get("execution_acceptance_complete", False)) and bool(
            (artifacts.get("execution_acceptance_report_path") or {}).get("payload")
        )
    if stage_name == "runtime_recommendations":
        return bool(artifact_status.get("runtime_recommendations_complete", False)) and bool(
            (artifacts.get("runtime_recommendations_path") or {}).get("payload")
        )
    if stage_name == "promotion_bundle":
        required_keys = (
            "promotion_path",
            "trainer_research_evidence_path",
            "decision_surface_path",
            "economic_objective_profile_path",
            "lane_governance_path",
        )
        return (
            bool(artifact_status.get("governance_artifacts_complete", False))
            and all(bool((artifacts.get(key) or {}).get("payload")) for key in required_keys)
        )
    if stage_name == "expert_prediction_table":
        return bool(artifact_status.get("expert_prediction_table_complete", False)) and bool(
            (artifacts.get("expert_prediction_table_path") or {}).get("exists", False)
        )
    return False


def _load_existing_execution_acceptance(*, existing_tail_artifacts: dict[str, Any]) -> dict[str, Any]:
    if not _tail_stage_is_reusable(existing_tail_artifacts=existing_tail_artifacts, stage_name="execution_acceptance"):
        return {}
    return dict(((existing_tail_artifacts.get("artifacts") or {}).get("execution_acceptance_report_path") or {}).get("payload") or {})


def _load_existing_runtime_recommendations(*, existing_tail_artifacts: dict[str, Any]) -> dict[str, Any]:
    if not _tail_stage_is_reusable(existing_tail_artifacts=existing_tail_artifacts, stage_name="runtime_recommendations"):
        return {}
    return dict(((existing_tail_artifacts.get("artifacts") or {}).get("runtime_recommendations_path") or {}).get("payload") or {})


def _load_existing_promotion_decision(*, existing_tail_artifacts: dict[str, Any]) -> dict[str, Any]:
    if not _tail_stage_is_reusable(existing_tail_artifacts=existing_tail_artifacts, stage_name="promotion_bundle"):
        return {}
    return dict(((existing_tail_artifacts.get("artifacts") or {}).get("promotion_path") or {}).get("payload") or {})


def _load_existing_trainer_research_evidence(*, existing_tail_artifacts: dict[str, Any]) -> dict[str, Any]:
    if not _tail_stage_is_reusable(existing_tail_artifacts=existing_tail_artifacts, stage_name="promotion_bundle"):
        return {}
    return dict(((existing_tail_artifacts.get("artifacts") or {}).get("trainer_research_evidence_path") or {}).get("payload") or {})


def _load_existing_decision_surface(*, existing_tail_artifacts: dict[str, Any]) -> dict[str, Any]:
    if not _tail_stage_is_reusable(existing_tail_artifacts=existing_tail_artifacts, stage_name="promotion_bundle"):
        return {}
    return dict(((existing_tail_artifacts.get("artifacts") or {}).get("decision_surface_path") or {}).get("payload") or {})


def _annotate_panel_tail_artifact(
    payload: dict[str, Any],
    *,
    tail_context: dict[str, Any],
    resumed: bool,
) -> dict[str, Any]:
    doc = dict(payload or {})
    doc["run_id"] = str(tail_context.get("run_id", "")).strip()
    doc["candidate_ref"] = str(tail_context.get("candidate_ref", "")).strip()
    doc["model_family"] = str(tail_context.get("model_family", "")).strip()
    doc["data_platform_ready_snapshot_id"] = str(tail_context.get("data_platform_ready_snapshot_id") or "").strip()
    doc["dependency_expert_only"] = bool(tail_context.get("dependency_expert_only", False))
    doc["tail_mode"] = str(tail_context.get("tail_mode", "")).strip() or "full"
    doc["tail_context"] = dict(tail_context)
    doc["resumed"] = bool(resumed)
    return doc


def _build_runtime_artifacts_from_existing_paths(*, run_dir: Path) -> dict[str, Path]:
    paths = _build_panel_runtime_artifact_paths(run_dir)
    return {
        "execution_acceptance_report_path": paths["execution_acceptance_report_path"],
        "runtime_recommendations_path": paths["runtime_recommendations_path"],
        "promotion_path": paths["promotion_path"],
        "trainer_research_evidence_path": paths["trainer_research_evidence_path"],
        "economic_objective_profile_path": paths["economic_objective_profile_path"],
        "lane_governance_path": paths["lane_governance_path"],
        "decision_surface_path": paths["decision_surface_path"],
    }


def _run_or_reuse_execution_acceptance(
    *,
    run_dir: Path,
    options: TrainV5PanelEnsembleOptions,
    run_id: str,
    tail_context: dict[str, Any],
    existing_tail_artifacts: dict[str, Any],
    duplicate_candidate: bool,
    duplicate_artifacts: dict[str, Any],
    resumed: bool,
) -> dict[str, Any]:
    existing = _load_existing_execution_acceptance(existing_tail_artifacts=existing_tail_artifacts)
    if existing:
        update_artifact_status(run_dir, execution_acceptance_complete=True)
        return dict(existing)
    if duplicate_candidate:
        execution_acceptance = v4._build_duplicate_candidate_execution_acceptance(
            run_id=run_id,
            duplicate_artifacts=duplicate_artifacts,
        )
    else:
        execution_acceptance = v4._run_execution_acceptance_v4(
            options=options,
            run_id=run_id,
        )
    execution_acceptance = _annotate_panel_tail_artifact(
        execution_acceptance,
        tail_context=tail_context,
        resumed=resumed,
    )
    _write_json(_build_panel_runtime_artifact_paths(run_dir)["execution_acceptance_report_path"], execution_acceptance)
    update_artifact_status(run_dir, execution_acceptance_complete=True)
    return execution_acceptance


def _run_or_reuse_runtime_recommendations(
    *,
    run_dir: Path,
    options: TrainV5PanelEnsembleOptions,
    run_id: str,
    search_budget_decision: dict[str, Any],
    tail_context: dict[str, Any],
    existing_tail_artifacts: dict[str, Any],
    execution_acceptance: dict[str, Any],
    duplicate_candidate: bool,
    duplicate_artifacts: dict[str, Any],
    selection_calibration: dict[str, Any],
    trade_action_oos_rows: list[dict[str, Any]],
    resumed: bool,
) -> tuple[dict[str, Any], dict[str, Any]]:
    live_domain_reweighting = dict(tail_context.get("live_domain_reweighting") or {})
    runtime_recommendations = _load_existing_runtime_recommendations(
        existing_tail_artifacts=existing_tail_artifacts
    )
    if not runtime_recommendations:
        if duplicate_candidate:
            runtime_recommendations = v4._build_duplicate_candidate_runtime_recommendations(
                run_id=run_id,
                duplicate_artifacts=duplicate_artifacts,
            )
        else:
            runtime_recommendations = v4._build_runtime_recommendations_v4(
                options=options,
                run_id=run_id,
                search_budget_decision=search_budget_decision,
                runtime_recommendation_cache_path=_runtime_recommendation_search_cache_path(run_dir),
                cache_context={
                    "data_platform_ready_snapshot_id": str(
                        tail_context.get("data_platform_ready_snapshot_id") or ""
                    ).strip()
                },
            )
            runtime_recommendations["exit_path_risk"] = v4._build_exit_path_risk_summary_v4(
                runtime_recommendations=runtime_recommendations,
                selection_calibration=selection_calibration,
                oos_rows=trade_action_oos_rows,
            )
            if isinstance(runtime_recommendations.get("exit"), dict):
                runtime_recommendations["exit"]["path_risk"] = dict(runtime_recommendations["exit_path_risk"])
            runtime_recommendations["trade_action"] = v4._build_trade_action_policy_v4(
                options=options,
                runtime_recommendations=runtime_recommendations,
                selection_calibration=selection_calibration,
                oos_rows=trade_action_oos_rows,
            )
            runtime_recommendations["risk_control"] = v4._build_execution_risk_control_v4(
                options=options,
                runtime_recommendations=runtime_recommendations,
                selection_calibration=selection_calibration,
                oos_rows=trade_action_oos_rows,
            )
    execution_artifact_cleanup = v4._purge_execution_artifact_run_dirs(
        output_root=options.execution_acceptance_output_root,
        execution_acceptance=execution_acceptance,
        runtime_recommendations=runtime_recommendations,
    )
    execution_acceptance_doc = dict(execution_acceptance)
    runtime_recommendations_doc = dict(runtime_recommendations)
    if execution_artifact_cleanup.get("evaluated"):
        execution_acceptance_doc["artifacts_cleanup"] = execution_artifact_cleanup
        runtime_recommendations_doc["artifacts_cleanup"] = execution_artifact_cleanup
    execution_acceptance_doc = _annotate_panel_tail_artifact(
        execution_acceptance_doc,
        tail_context=tail_context,
        resumed=resumed,
    )
    runtime_recommendations_doc = _annotate_panel_tail_artifact(
        runtime_recommendations_doc,
        tail_context=tail_context,
        resumed=resumed,
    )
    runtime_recommendations_doc["domain_weighting_policy"] = "v5_domain_weighting_v1"
    runtime_recommendations_doc["domain_weighting_source_kind"] = "live_candidate_density_ratio_v1"
    runtime_recommendations_doc["domain_weighting_enabled"] = bool(live_domain_reweighting)
    runtime_recommendations_doc["domain_weighting_status"] = (
        "live_candidate_density_ratio_ready" if bool(live_domain_reweighting) else "disabled"
    )
    artifact_paths = _build_panel_runtime_artifact_paths(run_dir)
    _write_json(artifact_paths["execution_acceptance_report_path"], execution_acceptance_doc)
    _write_json(artifact_paths["runtime_recommendations_path"], runtime_recommendations_doc)
    update_artifact_status(
        run_dir,
        execution_acceptance_complete=True,
        runtime_recommendations_complete=True,
    )
    return execution_acceptance_doc, runtime_recommendations_doc


def _run_or_reuse_promotion_bundle(
    *,
    run_dir: Path,
    options: TrainV5PanelEnsembleOptions,
    run_id: str,
    tail_context: dict[str, Any],
    existing_tail_artifacts: dict[str, Any],
    walk_forward: dict[str, Any],
    execution_acceptance: dict[str, Any],
    runtime_recommendations: dict[str, Any],
    duplicate_candidate: bool,
    duplicate_artifacts: dict[str, Any],
    research_support_lane: dict[str, Any],
    metrics: dict[str, Any],
    selection_policy: dict[str, Any],
    selection_calibration: dict[str, Any],
    factor_block_selection: dict[str, Any],
    factor_block_selection_context: dict[str, Any],
    cpcv_lite_runtime: dict[str, Any],
    search_budget_decision: dict[str, Any],
    economic_objective_profile: dict[str, Any],
    lane_governance: dict[str, Any],
    resumed: bool,
) -> dict[str, Any]:
    promotion = _load_existing_promotion_decision(existing_tail_artifacts=existing_tail_artifacts)
    trainer_research_evidence = _load_existing_trainer_research_evidence(
        existing_tail_artifacts=existing_tail_artifacts
    )
    decision_surface = _load_existing_decision_surface(existing_tail_artifacts=existing_tail_artifacts)
    if (
        not promotion
        or not trainer_research_evidence
        or not decision_surface
        or not _tail_stage_is_reusable(existing_tail_artifacts=existing_tail_artifacts, stage_name="promotion_bundle")
    ):
        if duplicate_candidate:
            promotion = v4._build_duplicate_candidate_promotion_decision_v4(
                options=options,
                run_id=run_id,
                walk_forward=walk_forward,
                execution_acceptance=execution_acceptance,
                duplicate_artifacts=duplicate_artifacts,
                runtime_recommendations=runtime_recommendations,
            )
        else:
            promotion = v4._manual_promotion_decision_v4(
                options=options,
                run_id=run_id,
                walk_forward=walk_forward,
                execution_acceptance=execution_acceptance,
                runtime_recommendations=runtime_recommendations,
            )
        trainer_research_evidence = v4._build_trainer_research_evidence_from_promotion_v4(
            promotion=promotion,
            support_lane=research_support_lane,
        )
        decision_surface = build_decision_surface_v4(
            options=options,
            task="cls",
            selection_policy=selection_policy,
            selection_calibration=selection_calibration,
            factor_block_selection=factor_block_selection,
            research_support_lane=research_support_lane,
            factor_block_selection_context=factor_block_selection_context,
            cpcv_lite_runtime=cpcv_lite_runtime,
            search_budget_decision=search_budget_decision,
            execution_acceptance=execution_acceptance,
            runtime_recommendations=runtime_recommendations,
            promotion=promotion,
            economic_objective_profile=economic_objective_profile,
            lane_governance=lane_governance,
        )
        decision_surface["panel_ensemble"] = dict(metrics.get("panel_ensemble", {}))
    promotion = _annotate_panel_tail_artifact(
        promotion,
        tail_context=tail_context,
        resumed=resumed,
    )
    trainer_research_evidence = _annotate_panel_tail_artifact(
        trainer_research_evidence,
        tail_context=tail_context,
        resumed=resumed,
    )
    decision_surface = _annotate_panel_tail_artifact(
        decision_surface,
        tail_context=tail_context,
        resumed=resumed,
    )
    economic_objective_profile_doc = _annotate_panel_tail_artifact(
        dict(economic_objective_profile),
        tail_context=tail_context,
        resumed=resumed,
    )
    lane_governance_doc = _annotate_panel_tail_artifact(
        dict(lane_governance),
        tail_context=tail_context,
        resumed=resumed,
    )
    runtime_artifacts = v4_persistence.persist_v4_runtime_and_governance_artifacts(
        run_dir=run_dir,
        execution_acceptance=execution_acceptance,
        runtime_recommendations=runtime_recommendations,
        promotion=promotion,
        trainer_research_evidence=trainer_research_evidence,
        economic_objective_profile=economic_objective_profile_doc,
        lane_governance=lane_governance_doc,
        decision_surface=decision_surface,
    )
    update_artifact_status(
        run_dir,
        status="trainer_artifacts_complete",
        execution_acceptance_complete=True,
        runtime_recommendations_complete=True,
        governance_artifacts_complete=True,
        promotion_complete=True,
        decision_surface_complete=True,
    )
    return {
        "promotion": promotion,
        "trainer_research_evidence": trainer_research_evidence,
        "decision_surface": decision_surface,
        "runtime_artifacts": runtime_artifacts,
    }


def _run_or_reuse_expert_prediction_table(
    *,
    run_dir: Path,
    existing_tail_artifacts: dict[str, Any],
    dataset: Any,
    estimator: Any,
    primary_y_reg: np.ndarray,
    train_mask: np.ndarray,
    valid_mask: np.ndarray,
    test_mask: np.ndarray,
) -> Path:
    artifact_paths = _build_panel_runtime_artifact_paths(run_dir)
    expert_prediction_table_path = artifact_paths["expert_prediction_table_path"]
    if _tail_stage_is_reusable(existing_tail_artifacts=existing_tail_artifacts, stage_name="expert_prediction_table"):
        update_artifact_status(run_dir, expert_prediction_table_complete=True)
        return expert_prediction_table_path
    expert_prediction_table_path = _write_expert_prediction_table(
        run_dir=run_dir,
        dataset=dataset,
        estimator=estimator,
        primary_y_reg=primary_y_reg,
        split_labels=_build_split_labels(
            train_mask=train_mask,
            valid_mask=valid_mask,
            test_mask=test_mask,
            size=dataset.rows,
        ),
    )
    update_artifact_status(run_dir, expert_prediction_table_complete=True)
    return expert_prediction_table_path


def _build_panel_dependency_runtime_recommendations(
    *,
    options: TrainV5PanelEnsembleOptions,
    run_id: str,
    search_budget_decision: dict[str, Any],
    data_platform_ready_snapshot_id: str | None,
    domain_details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    execution_window = _build_execution_evaluation_window_doc(options=options)
    details = dict(domain_details or {})
    return annotate_v5_runtime_recommendations({
        "version": 1,
        "policy": "v5_panel_dependency_runtime_recommendations_v1",
        "status": "trainer_runtime_contract_ready",
        "reason": "DEPENDENCY_EXPERT_ONLY_MODE",
        "run_id": str(run_id).strip(),
        "model_family": str(options.model_family).strip(),
        "trainer": "v5_panel_ensemble",
        "source_family": str(options.model_family).strip(),
        "source_trainer": "v5_panel_ensemble",
        "domain_weighting_policy": str(details.get("policy") or "v5_domain_weighting_v1").strip() or "v5_domain_weighting_v1",
        "domain_weighting_source_kind": str(details.get("source_kind") or "live_candidate_density_ratio_v1").strip() or "live_candidate_density_ratio_v1",
        "domain_weighting_enabled": bool(details.get("enabled", False)),
        "dependency_expert_only": True,
        "tail_mode": "dependency_expert_only",
        "data_platform_ready_snapshot_id": str(data_platform_ready_snapshot_id or "").strip(),
        "runtime_recommendation_profile": _runtime_recommendation_profile_from_search_budget(search_budget_decision),
        "selection": {
            "top_n": max(
                int(options.execution_acceptance_top_n)
                if int(options.execution_acceptance_top_n) > 0
                else int(options.top_n),
                1,
            ),
            "quote": str(options.quote).strip().upper(),
            "tf": str(options.tf).strip().lower(),
        },
        "execution_window": execution_window,
        "entry": {"mode": "dependency_expert_only"},
        "exit": {"mode": "dependency_expert_only"},
        "execution": {"mode": "dependency_expert_only"},
        "risk_control": {
            "status": "not_required",
            "contract_status": "not_required",
            "operating_mode": "dependency_expert_only",
        },
    })


def _build_panel_dependency_promotion_payload(
    *,
    options: TrainV5PanelEnsembleOptions,
    run_id: str,
    walk_forward: dict[str, Any],
    runtime_recommendations: dict[str, Any],
) -> dict[str, Any]:
    walk_forward_summary = dict((walk_forward or {}).get("summary") or {})
    windows_run = int(walk_forward_summary.get("windows_run", 0) or 0)
    return {
        "run_id": str(run_id).strip(),
        "promote": False,
        "status": "candidate",
        "promotion_mode": "dependency_expert_only",
        "reasons": ["DEPENDENCY_EXPERT_ONLY_MODE"],
        "checks": {
            "manual_review_required": False,
            "existing_champion_present": False,
            "walk_forward_present": windows_run > 0,
            "walk_forward_windows_run": windows_run,
            "execution_acceptance_enabled": False,
            "execution_acceptance_present": False,
            "risk_control_required": False,
            "risk_control_present": bool((runtime_recommendations.get("risk_control") or {})),
            "risk_control_governance_pass": True,
            "dependency_expert_only": True,
        },
        "research_acceptance": {
            "policy": "dependency_expert_only",
            "walk_forward_summary": walk_forward_summary,
        },
        "execution_acceptance": {"status": "not_required"},
        "candidate_ref": {
            "model_ref": "latest_candidate",
            "model_family": str(options.model_family).strip(),
        },
    }


def _run_panel_dependency_expert_tail(
    *,
    run_dir: Path,
    run_id: str,
    options: TrainV5PanelEnsembleOptions,
    dataset: Any,
    estimator: Any,
    primary_y_reg: np.ndarray,
    train_mask: np.ndarray,
    valid_mask: np.ndarray,
    test_mask: np.ndarray,
    walk_forward: dict[str, Any],
    cpcv_lite: dict[str, Any],
    factor_block_selection: dict[str, Any],
    factor_block_selection_context: dict[str, Any],
    search_budget_decision: dict[str, Any],
    metrics: dict[str, Any],
    economic_objective_profile: dict[str, Any],
    lane_governance: dict[str, Any],
    data_platform_ready_snapshot_id: str | None,
    logs_root: Path,
    pipeline_started_at: float,
    resumed: bool,
) -> dict[str, Any]:
    tail_started_at = time.time()
    duplicate_artifacts = v4._detect_duplicate_candidate_artifacts(
        options=options,
        run_id=run_id,
        run_dir=run_dir,
    )
    duplicate_candidate = bool(duplicate_artifacts.get("duplicate", False))
    tail_context = _build_panel_tail_context(
        run_id=run_id,
        options=options,
        data_platform_ready_snapshot_id=data_platform_ready_snapshot_id,
        search_budget_decision=search_budget_decision,
        duplicate_candidate=duplicate_candidate,
        live_domain_reweighting={},
    )
    existing_tail_artifacts = _resolve_existing_tail_artifacts(
        run_dir=run_dir,
        tail_context=tail_context,
    )
    _write_json(_panel_tail_context_path(run_dir), tail_context)
    update_artifact_status(run_dir, tail_context_written=True)

    artifact_paths = _build_panel_runtime_artifact_paths(run_dir)
    execution_acceptance = _load_existing_execution_acceptance(existing_tail_artifacts=existing_tail_artifacts)
    runtime_recommendations = _load_existing_runtime_recommendations(existing_tail_artifacts=existing_tail_artifacts)
    promotion = _load_existing_promotion_decision(existing_tail_artifacts=existing_tail_artifacts)
    trainer_research_evidence = _load_existing_trainer_research_evidence(
        existing_tail_artifacts=existing_tail_artifacts
    )
    decision_surface = _load_existing_decision_surface(existing_tail_artifacts=existing_tail_artifacts)
    economic_objective_profile_doc = dict(
        ((existing_tail_artifacts.get("artifacts") or {}).get("economic_objective_profile_path") or {}).get("payload")
        or {}
    )
    lane_governance_doc = dict(
        ((existing_tail_artifacts.get("artifacts") or {}).get("lane_governance_path") or {}).get("payload") or {}
    )

    if not (
        execution_acceptance
        and runtime_recommendations
        and promotion
        and trainer_research_evidence
        and decision_surface
        and economic_objective_profile_doc
        and lane_governance_doc
        and _tail_stage_is_reusable(existing_tail_artifacts=existing_tail_artifacts, stage_name="promotion_bundle")
    ):
        runtime_recommendations = _build_panel_dependency_runtime_recommendations(
            options=options,
            run_id=run_id,
            search_budget_decision=search_budget_decision,
            data_platform_ready_snapshot_id=data_platform_ready_snapshot_id,
        )
        promotion = _build_panel_dependency_promotion_payload(
            options=options,
            run_id=run_id,
            walk_forward=walk_forward,
            runtime_recommendations=runtime_recommendations,
        )
        runtime_artifacts = persist_v5_runtime_governance_artifacts(
            run_dir=run_dir,
            trainer_name="v5_panel_ensemble",
            model_family=options.model_family,
            run_scope=options.run_scope,
            metrics=metrics,
            runtime_recommendations=runtime_recommendations,
            promotion=promotion,
            trainer_research_reasons=["DEPENDENCY_EXPERT_ONLY_MODE"],
        )
        execution_acceptance = _annotate_panel_tail_artifact(
            dict(runtime_artifacts.get("execution_acceptance") or {}),
            tail_context=tail_context,
            resumed=resumed,
        )
        runtime_recommendations = _annotate_panel_tail_artifact(
            dict(runtime_recommendations or {}),
            tail_context=tail_context,
            resumed=resumed,
        )
        promotion = _annotate_panel_tail_artifact(
            dict(promotion or {}),
            tail_context=tail_context,
            resumed=resumed,
        )
        trainer_research_evidence = _annotate_panel_tail_artifact(
            dict(runtime_artifacts.get("trainer_research_evidence") or {}),
            tail_context=tail_context,
            resumed=resumed,
        )
        economic_objective_profile_doc = _annotate_panel_tail_artifact(
            dict(runtime_artifacts.get("economic_objective_profile") or {}),
            tail_context=tail_context,
            resumed=resumed,
        )
        lane_governance_doc = _annotate_panel_tail_artifact(
            dict(runtime_artifacts.get("lane_governance") or {}),
            tail_context=tail_context,
            resumed=resumed,
        )
        decision_surface = _annotate_panel_tail_artifact(
            dict(runtime_artifacts.get("decision_surface") or {}),
            tail_context=tail_context,
            resumed=resumed,
        )
        _write_json(artifact_paths["execution_acceptance_report_path"], execution_acceptance)
        _write_json(artifact_paths["runtime_recommendations_path"], runtime_recommendations)
        _write_json(artifact_paths["promotion_path"], promotion)
        _write_json(artifact_paths["trainer_research_evidence_path"], trainer_research_evidence)
        _write_json(artifact_paths["economic_objective_profile_path"], economic_objective_profile_doc)
        _write_json(artifact_paths["lane_governance_path"], lane_governance_doc)
        _write_json(artifact_paths["decision_surface_path"], decision_surface)
        update_artifact_status(
            run_dir,
            execution_acceptance_complete=True,
            runtime_recommendations_complete=True,
            governance_artifacts_complete=True,
            promotion_complete=True,
            decision_surface_complete=True,
        )

    expert_prediction_table_path = _run_or_reuse_expert_prediction_table(
        run_dir=run_dir,
        existing_tail_artifacts=existing_tail_artifacts,
        dataset=dataset,
        estimator=estimator,
        primary_y_reg=primary_y_reg,
        train_mask=train_mask,
        valid_mask=valid_mask,
        test_mask=test_mask,
    )
    finalization = _finalize_panel_tail_outputs(
        run_dir=run_dir,
        run_id=run_id,
        options=options,
        walk_forward=walk_forward,
        cpcv_lite=cpcv_lite,
        factor_block_selection=factor_block_selection,
        factor_block_selection_context=factor_block_selection_context,
        search_budget_decision=search_budget_decision,
        execution_acceptance=execution_acceptance,
        runtime_recommendations=runtime_recommendations,
        promotion=promotion,
        economic_objective_profile=economic_objective_profile_doc,
        lane_governance=lane_governance_doc,
        data_platform_ready_snapshot_id=data_platform_ready_snapshot_id,
        expert_prediction_table_path=expert_prediction_table_path,
        logs_root=logs_root,
        pipeline_duration_sec=round(time.time() - pipeline_started_at, 3),
        tail_duration_sec=round(time.time() - tail_started_at, 3),
        resumed=resumed,
    )
    return {
        "execution_acceptance": execution_acceptance,
        "runtime_recommendations": runtime_recommendations,
        "promotion": promotion,
        "runtime_artifacts": {
            "execution_acceptance_report_path": artifact_paths["execution_acceptance_report_path"],
            "runtime_recommendations_path": artifact_paths["runtime_recommendations_path"],
            "promotion_path": artifact_paths["promotion_path"],
            "trainer_research_evidence_path": artifact_paths["trainer_research_evidence_path"],
            "economic_objective_profile_path": artifact_paths["economic_objective_profile_path"],
            "lane_governance_path": artifact_paths["lane_governance_path"],
            "decision_surface_path": artifact_paths["decision_surface_path"],
        },
        "expert_prediction_table_path": expert_prediction_table_path,
        **finalization,
    }


def _finalize_panel_tail_outputs(
    *,
    run_dir: Path,
    run_id: str,
    options: TrainV5PanelEnsembleOptions,
    walk_forward: dict[str, Any],
    cpcv_lite: dict[str, Any],
    factor_block_selection: dict[str, Any],
    factor_block_selection_context: dict[str, Any],
    search_budget_decision: dict[str, Any],
    execution_acceptance: dict[str, Any],
    runtime_recommendations: dict[str, Any],
    promotion: dict[str, Any],
    economic_objective_profile: dict[str, Any],
    lane_governance: dict[str, Any],
    data_platform_ready_snapshot_id: str | None,
    expert_prediction_table_path: Path,
    logs_root: Path,
    pipeline_duration_sec: float,
    tail_duration_sec: float,
    resumed: bool,
) -> dict[str, Any]:
    status = str(promotion.get("status", "candidate")).strip() or "candidate"
    if (v4.normalize_factor_block_run_scope(options.run_scope) == "scheduled_daily") and (
        not _should_use_dependency_expert_only_mode(options)
    ):
        update_latest_pointer(options.registry_root, options.model_family, run_id)
    update_artifact_status(run_dir, status=status)
    experiment_ledger_record = v4.build_experiment_ledger_record(
        run_id=run_id,
        task="cls",
        status=status,
        duration_sec=float(pipeline_duration_sec),
        run_dir=run_dir,
        search_budget_decision=search_budget_decision,
        walk_forward=walk_forward,
        cpcv_lite=cpcv_lite,
        factor_block_selection=factor_block_selection,
        factor_block_policy={},
        factor_block_selection_context=factor_block_selection_context,
        execution_acceptance=execution_acceptance,
        runtime_recommendations=runtime_recommendations,
        promotion=promotion,
        duplicate_candidate=bool((load_json(_panel_tail_context_path(run_dir)).get("duplicate_candidate"))),
        economic_objective_profile=economic_objective_profile,
        lane_governance=lane_governance,
        run_scope=options.run_scope,
    )
    experiment_ledger_record["data_platform_ready_snapshot_id"] = str(data_platform_ready_snapshot_id or "").strip()
    experiment_ledger_record["resumed"] = bool(resumed)
    experiment_ledger_record["tail_duration_sec"] = float(tail_duration_sec)
    experiment_ledger_path = v4.append_experiment_ledger_record(
        registry_root=options.registry_root,
        model_family=options.model_family,
        record=experiment_ledger_record,
        run_scope=options.run_scope,
    )
    experiment_ledger_history = v4.load_experiment_ledger(
        registry_root=options.registry_root,
        model_family=options.model_family,
        run_scope=options.run_scope,
    )
    experiment_ledger_summary = v4.build_recent_experiment_ledger_summary(
        history_records=experiment_ledger_history,
    )
    experiment_ledger_summary_path = v4.write_latest_experiment_ledger_summary(
        registry_root=options.registry_root,
        model_family=options.model_family,
        run_id=run_id,
        summary=experiment_ledger_summary,
        run_scope=options.run_scope,
    )
    artifact_status = load_artifact_status(run_dir)
    train_report_path = _write_json(
        logs_root / "train_v5_panel_ensemble_report.json",
        {
            "run_id": run_id,
            "trainer": "v5_panel_ensemble",
            "status": status,
            "duration_sec": float(pipeline_duration_sec),
            "tail_duration_sec": float(tail_duration_sec),
            "resumed": bool(resumed),
            "dependency_expert_only": _should_use_dependency_expert_only_mode(options),
            "tail_mode": _panel_tail_mode(options),
            "data_platform_ready_snapshot_id": str(data_platform_ready_snapshot_id or "").strip(),
            "walk_forward_summary": walk_forward.get("summary", {}),
            "panel_ensemble": load_json(run_dir / "metrics.json").get("panel_ensemble", {}),
            "expert_prediction_table_path": str(expert_prediction_table_path),
            "artifact_status": artifact_status,
            **({"resumed_from_run_dir": str(run_dir)} if resumed else {}),
        },
    )
    return {
        "status": status,
        "experiment_ledger_path": experiment_ledger_path,
        "experiment_ledger_summary_path": experiment_ledger_summary_path,
        "train_report_path": train_report_path,
    }


def _run_panel_tail_common(
    *,
    run_dir: Path,
    run_id: str,
    options: TrainV5PanelEnsembleOptions,
    dataset: Any,
    estimator: Any,
    primary_y_reg: np.ndarray,
    train_mask: np.ndarray,
    valid_mask: np.ndarray,
    test_mask: np.ndarray,
    walk_forward: dict[str, Any],
    cpcv_lite: dict[str, Any],
    factor_block_selection: dict[str, Any],
    factor_block_selection_context: dict[str, Any],
    search_budget_decision: dict[str, Any],
    selection_policy: dict[str, Any],
    selection_calibration: dict[str, Any],
    metrics: dict[str, Any],
    research_support_lane: dict[str, Any],
    cpcv_lite_runtime: dict[str, Any],
    live_domain_reweighting: dict[str, Any],
    economic_objective_profile: dict[str, Any],
    lane_governance: dict[str, Any],
    data_platform_ready_snapshot_id: str | None,
    logs_root: Path,
    pipeline_started_at: float,
    resumed: bool,
) -> dict[str, Any]:
    if _should_use_dependency_expert_only_mode(options):
        return _run_panel_dependency_expert_tail(
            run_dir=run_dir,
            run_id=run_id,
            options=options,
            dataset=dataset,
            estimator=estimator,
            primary_y_reg=primary_y_reg,
            train_mask=train_mask,
            valid_mask=valid_mask,
            test_mask=test_mask,
            walk_forward=walk_forward,
            cpcv_lite=cpcv_lite,
            factor_block_selection=factor_block_selection,
            factor_block_selection_context=factor_block_selection_context,
            search_budget_decision=search_budget_decision,
            metrics=metrics,
            economic_objective_profile=economic_objective_profile,
            lane_governance=lane_governance,
            data_platform_ready_snapshot_id=data_platform_ready_snapshot_id,
            logs_root=logs_root,
            pipeline_started_at=pipeline_started_at,
            resumed=resumed,
        )
    tail_started_at = time.time()
    duplicate_artifacts = v4._detect_duplicate_candidate_artifacts(
        options=options,
        run_id=run_id,
        run_dir=run_dir,
    )
    duplicate_candidate = bool(duplicate_artifacts.get("duplicate", False))
    tail_context = _build_panel_tail_context(
        run_id=run_id,
        options=options,
        data_platform_ready_snapshot_id=data_platform_ready_snapshot_id,
        search_budget_decision=search_budget_decision,
        duplicate_candidate=duplicate_candidate,
        live_domain_reweighting=live_domain_reweighting,
    )
    existing_tail_artifacts = _resolve_existing_tail_artifacts(
        run_dir=run_dir,
        tail_context=tail_context,
    )
    _write_json(_panel_tail_context_path(run_dir), tail_context)
    update_artifact_status(run_dir, tail_context_written=True)
    trade_action_oos_rows = list(walk_forward.get("_trade_action_oos_rows") or [])
    execution_acceptance = _run_or_reuse_execution_acceptance(
        run_dir=run_dir,
        options=options,
        run_id=run_id,
        tail_context=tail_context,
        existing_tail_artifacts=existing_tail_artifacts,
        duplicate_candidate=duplicate_candidate,
        duplicate_artifacts=duplicate_artifacts,
        resumed=resumed,
    )
    execution_acceptance, runtime_recommendations = _run_or_reuse_runtime_recommendations(
        run_dir=run_dir,
        options=options,
        run_id=run_id,
        search_budget_decision=search_budget_decision,
        tail_context=tail_context,
        existing_tail_artifacts=existing_tail_artifacts,
        execution_acceptance=execution_acceptance,
        duplicate_candidate=duplicate_candidate,
        duplicate_artifacts=duplicate_artifacts,
        selection_calibration=selection_calibration,
        trade_action_oos_rows=trade_action_oos_rows,
        resumed=resumed,
    )
    promotion_bundle = _run_or_reuse_promotion_bundle(
        run_dir=run_dir,
        options=options,
        run_id=run_id,
        tail_context=tail_context,
        existing_tail_artifacts=existing_tail_artifacts,
        walk_forward=walk_forward,
        execution_acceptance=execution_acceptance,
        runtime_recommendations=runtime_recommendations,
        duplicate_candidate=duplicate_candidate,
        duplicate_artifacts=duplicate_artifacts,
        research_support_lane=research_support_lane,
        metrics=metrics,
        selection_policy=selection_policy,
        selection_calibration=selection_calibration,
        factor_block_selection=factor_block_selection,
        factor_block_selection_context=factor_block_selection_context,
        cpcv_lite_runtime=cpcv_lite_runtime,
        search_budget_decision=search_budget_decision,
        economic_objective_profile=economic_objective_profile,
        lane_governance=lane_governance,
        resumed=resumed,
    )
    expert_prediction_table_path = _run_or_reuse_expert_prediction_table(
        run_dir=run_dir,
        existing_tail_artifacts=existing_tail_artifacts,
        dataset=dataset,
        estimator=estimator,
        primary_y_reg=primary_y_reg,
        train_mask=train_mask,
        valid_mask=valid_mask,
        test_mask=test_mask,
    )
    finalization = _finalize_panel_tail_outputs(
        run_dir=run_dir,
        run_id=run_id,
        options=options,
        walk_forward=walk_forward,
        cpcv_lite=cpcv_lite,
        factor_block_selection=factor_block_selection,
        factor_block_selection_context=factor_block_selection_context,
        search_budget_decision=search_budget_decision,
        execution_acceptance=execution_acceptance,
        runtime_recommendations=runtime_recommendations,
        promotion=promotion_bundle["promotion"],
        economic_objective_profile=economic_objective_profile,
        lane_governance=lane_governance,
        data_platform_ready_snapshot_id=data_platform_ready_snapshot_id,
        expert_prediction_table_path=expert_prediction_table_path,
        logs_root=logs_root,
        pipeline_duration_sec=round(time.time() - pipeline_started_at, 3),
        tail_duration_sec=round(time.time() - tail_started_at, 3),
        resumed=resumed,
    )
    return {
        "execution_acceptance": execution_acceptance,
        "runtime_recommendations": runtime_recommendations,
        "promotion": promotion_bundle["promotion"],
        "runtime_artifacts": promotion_bundle["runtime_artifacts"],
        "expert_prediction_table_path": expert_prediction_table_path,
        **finalization,
    }

def _load_v5_regression_targets(
    *,
    request: Any,
    label_spec: dict[str, Any],
    dataset: Any,
    primary_y_cls_column: str,
    primary_y_reg_column: str,
    primary_y_rank_column: str,
) -> dict[str, np.ndarray]:
    column_families = dict((label_spec.get("canonical_multi_horizon_columns") or {}))
    residual_columns = column_families.get("y_reg_resid_leader")
    if not isinstance(residual_columns, list) or not residual_columns:
        raise ValueError("label_spec missing canonical_multi_horizon_columns.y_reg_resid_leader for v5_panel_ensemble")
    aux = load_feature_aux_frame(
        request,
        columns=tuple(str(item).strip() for item in residual_columns if str(item).strip()),
        y_cls_column=primary_y_cls_column,
        y_reg_column=primary_y_reg_column,
        y_rank_column=primary_y_rank_column,
    )
    aux_ts_ms = aux.get_column("ts_ms").to_numpy().astype(np.int64, copy=False)
    aux_markets = aux.get_column("market").to_numpy()
    if not np.array_equal(aux_ts_ms, dataset.ts_ms) or not np.array_equal(aux_markets, dataset.markets):
        raise ValueError("V5_PANEL_TARGET_ALIGNMENT_FAILED")
    targets: dict[str, np.ndarray] = {}
    for name in residual_columns:
        column = str(name).strip()
        horizon_token = column.split("_h")[-1]
        targets[f"h{horizon_token}"] = aux.get_column(column).to_numpy().astype(np.float64, copy=False)
    return targets


def _load_v5_auxiliary_panel_targets(
    *,
    request: Any,
    label_spec: dict[str, Any],
    dataset: Any,
    primary_y_cls_column: str,
    primary_y_reg_column: str,
    primary_y_rank_column: str,
) -> dict[str, dict[str, np.ndarray]]:
    column_families = dict((label_spec.get("canonical_multi_horizon_columns") or {}))
    cls_columns = [str(item).strip() for item in (column_families.get("y_cls_resid_leader") or []) if str(item).strip()]
    rank_columns = [str(item).strip() for item in (column_families.get("y_rank_resid_leader") or []) if str(item).strip()]
    if not cls_columns and not rank_columns:
        return {"cls": {}, "rank": {}}
    aux = load_feature_aux_frame(
        request,
        columns=tuple(dict.fromkeys([*cls_columns, *rank_columns])),
        y_cls_column=primary_y_cls_column,
        y_reg_column=primary_y_reg_column,
        y_rank_column=primary_y_rank_column,
    )
    aux_ts_ms = aux.get_column("ts_ms").to_numpy().astype(np.int64, copy=False)
    aux_markets = aux.get_column("market").to_numpy()
    if not np.array_equal(aux_ts_ms, dataset.ts_ms) or not np.array_equal(aux_markets, dataset.markets):
        raise ValueError("V5_PANEL_AUX_TARGET_ALIGNMENT_FAILED")
    cls_targets: dict[str, np.ndarray] = {}
    rank_targets: dict[str, np.ndarray] = {}
    for name in cls_columns:
        horizon_token = name.split("_h")[-1]
        cls_targets[f"h{horizon_token}"] = aux.get_column(name).to_numpy().astype(np.float64, copy=False)
    for name in rank_columns:
        horizon_token = name.split("_h")[-1]
        rank_targets[f"h{horizon_token}"] = aux.get_column(name).to_numpy().astype(np.float64, copy=False)
    return {"cls": cls_targets, "rank": rank_targets}


def _fit_v5_regression_heads(
    *,
    options: TrainV5PanelEnsembleOptions,
    dataset: Any,
    regression_targets: dict[str, np.ndarray],
    train_mask: np.ndarray,
    valid_mask: np.ndarray,
    sweep_trials: int,
) -> dict[str, dict[str, Any]]:
    results: dict[str, dict[str, Any]] = {}
    for horizon_key, target in regression_targets.items():
        results[horizon_key] = v4._fit_booster_sweep_regression(
            x_train=dataset.X[train_mask],
            y_train=np.asarray(target[train_mask], dtype=np.float64),
            w_train=dataset.sample_weight[train_mask],
            x_valid=dataset.X[valid_mask],
            y_valid_cls=dataset.y_cls[valid_mask],
            y_valid_reg=np.asarray(target[valid_mask], dtype=np.float64),
            w_valid=dataset.sample_weight[valid_mask],
            fee_bps_est=options.fee_bps_est,
            safety_bps=options.safety_bps,
            seed=options.seed + int(horizon_key.replace("h", "")),
            nthread=options.nthread,
            trials=sweep_trials,
            eval_sample_weight=dataset.sample_weight[valid_mask],
        )
    return results


def _fit_v5_auxiliary_classifier_heads(
    *,
    options: TrainV5PanelEnsembleOptions,
    best_params: dict[str, Any],
    dataset: Any,
    auxiliary_targets: dict[str, np.ndarray],
    train_mask: np.ndarray,
    valid_mask: np.ndarray,
) -> dict[str, dict[str, Any]]:
    results: dict[str, dict[str, Any]] = {}
    for horizon_key, target in auxiliary_targets.items():
        values = np.asarray(target, dtype=np.float64)
        usable_train = np.asarray(train_mask, dtype=bool) & np.isfinite(values)
        usable_valid = np.asarray(valid_mask, dtype=bool) & np.isfinite(values)
        if int(np.sum(usable_train)) <= 1 or int(np.sum(usable_valid)) <= 0:
            continue
        target_train = values[usable_train].astype(np.int64, copy=False)
        target_valid = values[usable_valid].astype(np.int64, copy=False)
        if np.unique(target_train).size < 2:
            continue
        results[horizon_key] = v4._fit_fixed_classifier_model(
            options=options,
            best_params=best_params,
            x_train=dataset.X[usable_train],
            y_train=target_train,
            w_train=dataset.sample_weight[usable_train],
            x_valid=dataset.X[usable_valid],
            y_valid=target_valid,
            w_valid=dataset.sample_weight[usable_valid],
            fold_index=int(horizon_key.replace("h", "")),
        )
    return results


def _fit_v5_auxiliary_ranker_heads(
    *,
    options: TrainV5PanelEnsembleOptions,
    best_params: dict[str, Any],
    dataset: Any,
    auxiliary_targets: dict[str, np.ndarray],
    train_mask: np.ndarray,
    valid_mask: np.ndarray,
) -> dict[str, dict[str, Any]]:
    results: dict[str, dict[str, Any]] = {}
    for horizon_key, target in auxiliary_targets.items():
        values = np.asarray(target, dtype=np.float64)
        usable_train = np.asarray(train_mask, dtype=bool) & np.isfinite(values)
        usable_valid = np.asarray(valid_mask, dtype=bool) & np.isfinite(values)
        if int(np.sum(usable_train)) <= 1 or int(np.sum(usable_valid)) <= 0:
            continue
        results[horizon_key] = v4._fit_fixed_ranker_model(
            options=options,
            best_params=best_params,
            x_train=dataset.X[usable_train],
            y_train=values[usable_train],
            ts_train_ms=dataset.ts_ms[usable_train],
            w_train=dataset.sample_weight[usable_train],
            x_valid=dataset.X[usable_valid],
            y_valid=values[usable_valid],
            ts_valid_ms=dataset.ts_ms[usable_valid],
            fold_index=int(horizon_key.replace("h", "")),
        )
    return results


def _build_component_matrix(
    *,
    x: np.ndarray,
    classifier_bundle: dict[str, Any],
    ranker_bundle: dict[str, Any],
    auxiliary_classifier_bundles: dict[str, dict[str, Any]],
    auxiliary_ranker_bundles: dict[str, dict[str, Any]],
    regressor_bundles: dict[str, dict[str, Any]],
) -> tuple[np.ndarray, dict[str, np.ndarray]]:
    cls_score = _predict_scores(classifier_bundle["bundle"] if "bundle" in classifier_bundle else classifier_bundle, x)
    rank_score = _predict_scores(ranker_bundle["bundle"] if "bundle" in ranker_bundle else ranker_bundle, x)
    aux_cls_scores = {
        key: _predict_scores(bundle["bundle"] if "bundle" in bundle else bundle, x)
        for key, bundle in auxiliary_classifier_bundles.items()
    }
    aux_rank_scores = {
        key: _predict_scores(bundle["bundle"] if "bundle" in bundle else bundle, x)
        for key, bundle in auxiliary_ranker_bundles.items()
    }
    reg_keys = sorted(regressor_bundles.keys(), key=lambda item: int(item.replace("h", "")))
    raw_mus = {
        key: np.asarray(
            (regressor_bundles[key]["bundle"]["estimator"] if "bundle" in regressor_bundles[key] else regressor_bundles[key]["estimator"]).predict(x),
            dtype=np.float64,
        )
        for key in reg_keys
    }
    reg_probs = [_sigmoid(values) for values in raw_mus.values()]
    matrix = np.column_stack(
        [
            cls_score,
            rank_score,
            *[aux_cls_scores[key] for key in sorted(aux_cls_scores.keys(), key=lambda item: int(item.replace("h", "")))],
            *[aux_rank_scores[key] for key in sorted(aux_rank_scores.keys(), key=lambda item: int(item.replace("h", "")))],
            *reg_probs,
        ]
    )
    payload = {
        "cls_score": cls_score,
        "rank_score": rank_score,
        **{f"aux_cls_{key}": values for key, values in aux_cls_scores.items()},
        **{f"aux_rank_{key}": values for key, values in aux_rank_scores.items()},
        **{f"mu_{key}": values for key, values in raw_mus.items()},
    }
    return matrix, payload


def _fit_meta_logistic(meta_x: np.ndarray, meta_y: np.ndarray, *, sample_weight: np.ndarray | None = None) -> dict[str, Any]:
    x_values = np.asarray(meta_x, dtype=np.float64)
    y_values = np.asarray(meta_y, dtype=np.int64)
    shift = np.mean(x_values, axis=0) if x_values.size > 0 else np.zeros(x_values.shape[1], dtype=np.float64)
    scale = np.std(x_values, axis=0, ddof=0) if x_values.size > 0 else np.ones(x_values.shape[1], dtype=np.float64)
    scale = np.where(np.abs(scale) < 1e-12, 1.0, scale)
    normalized = (x_values - shift) / scale
    if np.unique(y_values).size < 2:
        probability = float(np.mean(y_values)) if y_values.size > 0 else 0.5
        intercept = float(np.log(np.clip(probability, 1e-6, 1.0 - 1e-6) / np.clip(1.0 - probability, 1e-6, 1.0)))
        model = _StackMetaModel(
            intercept=intercept,
            coefficients=tuple([0.0] * normalized.shape[1]),
            feature_shift=tuple(shift.tolist()),
            feature_scale=tuple(scale.tolist()),
        )
        return {"meta_model": model, "uncertainty_temperature": 1.0}

    try:
        from sklearn.linear_model import LogisticRegression
    except Exception as exc:
        raise RuntimeError(f"sklearn logistic regression is required for v5 panel stacking: {exc}") from exc

    estimator = LogisticRegression(max_iter=1000, solver="lbfgs")
    fit_kwargs: dict[str, Any] = {}
    if sample_weight is not None and np.asarray(sample_weight).size == y_values.size:
        fit_kwargs["sample_weight"] = np.clip(np.asarray(sample_weight, dtype=np.float64), 1e-6, None)
    estimator.fit(normalized, y_values, **fit_kwargs)
    model = _StackMetaModel(
        intercept=float(np.asarray(estimator.intercept_, dtype=np.float64)[0]),
        coefficients=tuple(np.asarray(estimator.coef_, dtype=np.float64)[0].tolist()),
        feature_shift=tuple(shift.tolist()),
        feature_scale=tuple(scale.tolist()),
    )
    return {"meta_model": model, "uncertainty_temperature": 1.0}


def _build_v5_oof_windows(
    *,
    options: TrainV5PanelEnsembleOptions,
    dataset: Any,
    primary_y_reg: np.ndarray,
    regression_targets: dict[str, np.ndarray],
    auxiliary_classifier_targets: dict[str, np.ndarray],
    auxiliary_rank_targets: dict[str, np.ndarray],
    classifier_best_params: dict[str, Any],
    ranker_best_params: dict[str, Any],
    regressor_best_params: dict[str, dict[str, Any]],
    action_aux_arrays: dict[str, np.ndarray],
) -> dict[str, Any]:
    windows: list[dict[str, Any]] = []
    skipped_windows: list[dict[str, Any]] = []
    selection_calibration_rows: list[dict[str, Any]] = []
    trade_action_rows: list[dict[str, Any]] = []
    meta_rows: list[dict[str, Any]] = []
    meta_weight_parts: list[np.ndarray] = []
    meta_models: list[_StackMetaModel] = []
    regression_member_bundles: dict[str, list[dict[str, Any]]] = {
        str(key): [] for key in regression_targets.keys()
    }
    try:
        window_specs = compute_anchored_walk_forward_splits(
            dataset.ts_ms,
            valid_ratio=options.valid_ratio,
            test_ratio=options.test_ratio,
            window_count=max(int(options.walk_forward_windows), 1),
            embargo_bars=options.embargo_bars,
            interval_ms=v4.expected_interval_ms(options.tf),
        )
    except ValueError as exc:
        return {
            "windows": [],
            "skipped_windows": [{"window_index": -1, "reason": str(exc)}],
            "_selection_calibration_rows": [],
            "_trade_action_oos_rows": [],
            "meta_rows": [],
            "sample_weight": np.empty(0, dtype=np.float64),
        }

    raw_window_rows: list[dict[str, Any]] = []
    for labels, info in window_specs:
        masks = split_masks(labels)
        train_mask = masks[SPLIT_TRAIN]
        valid_mask = masks[SPLIT_VALID]
        test_mask = masks[SPLIT_TEST]
        row_counts = {
            "train": int(np.sum(train_mask)),
            "valid": int(np.sum(valid_mask)),
            "test": int(np.sum(test_mask)),
            "drop": int(np.sum(masks[SPLIT_DROP])),
        }
        if row_counts["train"] <= 0 or row_counts["valid"] <= 0 or row_counts["test"] <= 0:
            skipped_windows.append({"window_index": int(info.window_index), "counts": row_counts, "reason": "INSUFFICIENT_ROWS"})
            continue

        cls_window = v4._fit_fixed_classifier_model(
            options=options,
            best_params=classifier_best_params,
            x_train=dataset.X[train_mask],
            y_train=dataset.y_cls[train_mask],
            w_train=dataset.sample_weight[train_mask],
            x_valid=dataset.X[valid_mask],
            y_valid=dataset.y_cls[valid_mask],
            w_valid=dataset.sample_weight[valid_mask],
            fold_index=int(info.window_index),
        )
        rank_window = v4._fit_fixed_ranker_model(
            options=options,
            best_params=ranker_best_params,
            x_train=dataset.X[train_mask],
            y_train=dataset.y_rank[train_mask],
            ts_train_ms=dataset.ts_ms[train_mask],
            w_train=dataset.sample_weight[train_mask],
            x_valid=dataset.X[valid_mask],
            y_valid=dataset.y_rank[valid_mask],
            ts_valid_ms=dataset.ts_ms[valid_mask],
            fold_index=int(info.window_index),
        )
        aux_cls_windows = _fit_v5_auxiliary_classifier_heads(
            options=options,
            best_params=classifier_best_params,
            dataset=dataset,
            auxiliary_targets=auxiliary_classifier_targets,
            train_mask=train_mask,
            valid_mask=valid_mask,
        )
        aux_rank_windows = _fit_v5_auxiliary_ranker_heads(
            options=options,
            best_params=ranker_best_params,
            dataset=dataset,
            auxiliary_targets=auxiliary_rank_targets,
            train_mask=train_mask,
            valid_mask=valid_mask,
        )
        reg_windows: dict[str, dict[str, Any]] = {}
        for horizon_key, target in regression_targets.items():
            reg_windows[horizon_key] = v4._fit_fixed_regression_model(
                options=options,
                best_params=regressor_best_params[horizon_key],
                x_train=dataset.X[train_mask],
                y_train=np.asarray(target[train_mask], dtype=np.float64),
                w_train=dataset.sample_weight[train_mask],
                x_valid=dataset.X[valid_mask],
                y_valid=np.asarray(target[valid_mask], dtype=np.float64),
                w_valid=dataset.sample_weight[valid_mask],
                fold_index=int(info.window_index),
            )
            member_bundle = reg_windows[horizon_key].get("bundle")
            if isinstance(member_bundle, dict):
                regression_member_bundles[horizon_key].append(member_bundle)

        meta_x, payload = _build_component_matrix(
            x=dataset.X[test_mask],
            classifier_bundle=cls_window,
            ranker_bundle=rank_window,
            auxiliary_classifier_bundles=aux_cls_windows,
            auxiliary_ranker_bundles=aux_rank_windows,
            regressor_bundles=reg_windows,
        )
        raw_window_rows.append(
            {
                "window_index": int(info.window_index),
                "counts": row_counts,
                "time_window": {
                    "valid_start_ts": int(info.valid_start_ts),
                    "test_start_ts": int(info.test_start_ts),
                    "test_end_ts": int(info.test_end_ts),
                },
                "meta_x": meta_x,
                "y_cls": np.asarray(dataset.y_cls[test_mask], dtype=np.int64),
                "y_reg": np.asarray(primary_y_reg[test_mask], dtype=np.float64),
                "y_rank": np.asarray(dataset.y_rank[test_mask], dtype=np.float64),
                "sample_weight": np.asarray(dataset.sample_weight[test_mask], dtype=np.float64),
                "markets": np.asarray(dataset.markets[test_mask], dtype=object),
                "ts_ms": np.asarray(dataset.ts_ms[test_mask], dtype=np.int64),
                "payload": payload,
                "mask": np.asarray(test_mask, dtype=bool),
            }
        )

    for window in raw_window_rows:
        other_windows = [item for item in raw_window_rows if item["window_index"] != window["window_index"]]
        if other_windows:
            meta_x_train = np.concatenate([item["meta_x"] for item in other_windows], axis=0)
            meta_y_train = np.concatenate([item["y_cls"] for item in other_windows], axis=0)
            meta_w_train = np.concatenate([item["sample_weight"] for item in other_windows], axis=0)
        else:
            meta_x_train = np.asarray(window["meta_x"], dtype=np.float64)
            meta_y_train = np.asarray(window["y_cls"], dtype=np.int64)
            meta_w_train = np.asarray(window["sample_weight"], dtype=np.float64)

        meta_fit = _fit_meta_logistic(meta_x_train, meta_y_train, sample_weight=meta_w_train)
        meta_models.append(meta_fit["meta_model"])
        final_scores = meta_fit["meta_model"].predict_proba(window["meta_x"])[:, 1]
        lcb_scores = np.clip(final_scores - np.std(window["meta_x"], axis=1, ddof=0), 0.0, 1.0)
        metrics = v4._attach_ranking_metrics(
            metrics=_evaluate_split(
                y_cls=window["y_cls"],
                y_reg=window["y_reg"],
                scores=final_scores,
                markets=window["markets"],
                fee_bps_est=options.fee_bps_est,
                safety_bps=options.safety_bps,
                sample_weight=window["sample_weight"],
            ),
            y_rank=window["y_rank"],
            ts_ms=window["ts_ms"],
            scores=final_scores,
        )
        windows.append(
            {
                "window_index": int(window["window_index"]),
                "time_window": dict(window["time_window"]),
                "counts": dict(window["counts"]),
                "metrics": v4._compact_eval_metrics(metrics),
                "oos_periods": v4._build_oos_period_metrics(
                    ts_ms=window["ts_ms"],
                    y_cls=window["y_cls"],
                    y_reg=window["y_reg"],
                    scores=final_scores,
                    markets=window["markets"],
                    fee_bps_est=options.fee_bps_est,
                    safety_bps=options.safety_bps,
                ),
                "oos_slices": v4._build_oos_slice_metrics(
                    ts_ms=window["ts_ms"],
                    y_cls=window["y_cls"],
                    y_reg=window["y_reg"],
                    scores=final_scores,
                    markets=window["markets"],
                    fee_bps_est=options.fee_bps_est,
                    safety_bps=options.safety_bps,
                ),
                "selection_optimization": build_window_selection_objectives(
                    scores=final_scores,
                    y_reg=window["y_reg"],
                    ts_ms=window["ts_ms"],
                    thresholds={},
                    fee_bps_est=options.fee_bps_est,
                    safety_bps=options.safety_bps,
                    config=SelectionGridConfig(),
                ),
                "selection_optimization_by_score_source": {
                    "score_mean": build_window_selection_objectives(
                        scores=final_scores,
                        y_reg=window["y_reg"],
                        ts_ms=window["ts_ms"],
                        thresholds={},
                        fee_bps_est=options.fee_bps_est,
                        safety_bps=options.safety_bps,
                        config=SelectionGridConfig(),
                    ),
                    "score_lcb": build_window_selection_objectives(
                        scores=lcb_scores,
                        y_reg=window["y_reg"],
                        ts_ms=window["ts_ms"],
                        thresholds={},
                        fee_bps_est=options.fee_bps_est,
                        safety_bps=options.safety_bps,
                        config=SelectionGridConfig(),
                    ),
                },
                "trial_records": [],
            }
        )
        selection_calibration_rows.append(
            {
                "window_index": int(window["window_index"]),
                "scores": np.asarray(final_scores, dtype=np.float64).tolist(),
                "score_lcb": np.asarray(lcb_scores, dtype=np.float64).tolist(),
                "y_cls": np.asarray(window["y_cls"], dtype=np.int64).tolist(),
            }
        )
        mask = np.asarray(window["mask"], dtype=bool)
        trade_action_rows.append(
            {
                "window_index": int(window["window_index"]),
                "raw_scores": np.asarray(final_scores, dtype=np.float64).tolist(),
                "markets": np.asarray(window["markets"], dtype=object).tolist(),
                "ts_ms": np.asarray(window["ts_ms"], dtype=np.int64).tolist(),
                "close": np.asarray(action_aux_arrays.get("close", np.array([]))[mask], dtype=np.float64).tolist(),
                "rv_12": np.asarray(action_aux_arrays.get("rv_12", np.array([]))[mask], dtype=np.float64).tolist(),
                "rv_36": np.asarray(action_aux_arrays.get("rv_36", np.array([]))[mask], dtype=np.float64).tolist(),
                "atr_14": np.asarray(action_aux_arrays.get("atr_14", np.array([]))[mask], dtype=np.float64).tolist(),
                "atr_pct_14": np.asarray(action_aux_arrays.get("atr_pct_14", np.array([]))[mask], dtype=np.float64).tolist(),
            }
        )
        meta_rows.append({"x": np.asarray(window["meta_x"], dtype=np.float64), "y": np.asarray(window["y_cls"], dtype=np.int64)})
        meta_weight_parts.append(np.asarray(window["sample_weight"], dtype=np.float64))

    return {
        "windows": windows,
        "skipped_windows": skipped_windows,
        "_selection_calibration_rows": selection_calibration_rows,
        "_trade_action_oos_rows": trade_action_rows,
        "meta_rows": meta_rows,
        "sample_weight": np.concatenate(meta_weight_parts, axis=0) if meta_weight_parts else np.empty(0, dtype=np.float64),
        "meta_models": meta_models,
        "regression_member_bundles": {key: tuple(value) for key, value in regression_member_bundles.items()},
    }


def _build_walk_forward_report_v5(
    *,
    windows: list[dict[str, Any]],
    skipped: list[dict[str, Any]],
    selection_calibration_rows: list[dict[str, Any]],
    trade_action_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "policy": "stacked_panel_oof_v1",
        "enabled": True,
        "windows_requested": len(windows) + len(skipped),
        "windows": windows,
        "skipped_windows": skipped,
        "summary": summarize_walk_forward_windows(windows),
        "compare_to_champion": compare_balanced_pareto({}, {}),
        "selected_threshold_key": "top_5pct",
        "selected_threshold_key_source": "walk_forward_objective_optimizer",
        "_factor_block_window_rows": [],
        "_selection_calibration_rows": selection_calibration_rows,
        "_trade_action_oos_rows": trade_action_rows,
        "factor_block_refit_windows": [],
        "trial_panel": [],
    }


def _build_disabled_cpcv_lite(*, trigger: str) -> dict[str, Any]:
    return {
        "policy": "cpcv_lite_research_v1",
        "enabled": False,
        "trigger": trigger,
        "estimate_label": "disabled",
        "summary": {"status": "disabled", "reasons": ["V5_PANEL_ENSEMBLE_CPCV_NOT_IMPLEMENTED"]},
        "folds": [],
        "skipped_folds": [],
    }


def _build_v5_metrics_doc(
    *,
    run_id: str,
    options: TrainV5PanelEnsembleOptions,
    split_info: Any,
    interval_ms: int,
    rows: dict[str, int],
    valid_metrics: dict[str, Any],
    test_metrics: dict[str, Any],
    walk_forward: dict[str, Any],
    cpcv_lite: dict[str, Any],
    factor_block_selection: dict[str, Any],
    search_budget_decision: dict[str, Any],
    economic_objective_profile: dict[str, Any],
    lane_governance: dict[str, Any],
    cls_bundle: dict[str, Any],
    rank_bundle: dict[str, Any],
    auxiliary_classifier_horizons: list[int],
    auxiliary_rank_horizons: list[int],
    regressor_results: dict[str, dict[str, Any]],
    meta_fit: dict[str, Any],
    meta_ensemble_count: int,
    primary_horizon: int,
) -> dict[str, Any]:
    metrics = build_v4_metrics_doc(
        run_id=run_id,
        options=options,
        task="cls",
        split_info=split_info,
        interval_ms=interval_ms,
        rows=rows,
        valid_metrics=valid_metrics,
        test_metrics=test_metrics,
        walk_forward_summary=walk_forward.get("summary", {}),
        cpcv_lite_summary=(cpcv_lite.get("summary") or {}),
        factor_block_selection_summary=(factor_block_selection.get("summary") or {}),
        best_params={"meta_model": "logistic_stack"},
        sweep_records=[],
        ranker_budget_profile={"profile": "v5_panel_ensemble"},
        cpcv_lite_runtime={"enabled": False, "trigger": "disabled"},
        search_budget_decision=search_budget_decision,
        economic_objective_profile=economic_objective_profile,
        lane_governance=lane_governance,
    )
    metrics["trainer"] = "v5_panel_ensemble"
    metrics["panel_ensemble"] = {
        "version": 1,
        "policy": "v5_panel_ensemble_v1",
        "component_order": list(_STACK_COMPONENT_ORDER),
        "auxiliary_classifier_horizons": list(auxiliary_classifier_horizons),
        "auxiliary_rank_horizons": list(auxiliary_rank_horizons),
        "regression_horizons": [int(key.replace("h", "")) for key in sorted(regressor_results.keys(), key=lambda item: int(item.replace("h", "")))],
        "classifier_backend": "xgboost",
        "ranker_backend": "xgboost_ranker",
        "regressor_backend": "xgboost_regressor",
        "stack_meta_model": "logistic_regression",
        "classifier_best_params": dict(cls_bundle.get("best_params", {})),
        "ranker_best_params": dict(rank_bundle.get("best_params", {})),
        "regressor_best_params": {key: dict(value.get("best_params", {})) for key, value in regressor_results.items()},
        "uncertainty_mode": "walk_forward_meta_ensemble_std_v1",
        "uncertainty_member_count": int(meta_ensemble_count),
        "uncertainty_temperature": float(meta_fit.get("uncertainty_temperature", 1.0)),
            "distributional_contract": {
                "version": 1,
                "quantile_levels": [0.10, 0.50, 0.90],
                "return_quantiles_field_prefix": "return_quantiles",
                "sigma_field_prefix": "return_sigma",
                "expected_shortfall_proxy_field_prefix": "return_expected_shortfall_proxy",
                "horizon_keys": [f"h{int(key.replace('h', ''))}" for key in sorted(regressor_results.keys(), key=lambda item: int(item.replace("h", "")))],
                "member_ensemble_source": "walk_forward_regression_members",
                "primary_horizon_key": f"h{int(primary_horizon)}",
            },
        "final_output_contract": {
            "score_field": "final_rank_score",
            "score_mean_field": "score_mean",
            "score_std_field": "score_std",
            "score_lcb_field": "score_lcb",
            "uncertainty_field": "score_std",
            "expected_return_field": "final_expected_return",
            "expected_es_field": "final_expected_es",
            "tradability_field": "final_tradability",
            "alpha_lcb_field": "final_alpha_lcb",
            "score_aliases": {"final_rank_score": "score_mean", "final_uncertainty": "score_std"},
            "tradability_mode": "panel_risk_confidence_proxy_v1",
            "alpha_lcb_formula": "final_alpha_lcb = final_expected_return - final_expected_es - final_uncertainty",
        },
    }
    return metrics


def _build_v5_leaderboard_row(
    *,
    run_id: str,
    options: TrainV5PanelEnsembleOptions,
    rows: dict[str, int],
    test_metrics: dict[str, Any],
) -> dict[str, Any]:
    row = v4_postprocess.build_leaderboard_row_v4(
        run_id=run_id,
        options=options,
        task="cls",
        rows=rows,
        test_metrics=test_metrics,
    )
    row["trainer"] = "v5_panel_ensemble"
    row["champion_backend"] = "v5_panel_ensemble"
    return row


def _build_v5_train_config(
    *,
    options: TrainV5PanelEnsembleOptions,
    feature_cols: tuple[str, ...],
    markets: tuple[str, ...],
    label_contract: dict[str, Any],
    selection_recommendations: dict[str, Any],
    selection_policy: dict[str, Any],
    selection_calibration: dict[str, Any],
    research_support_lane: dict[str, Any],
    cpcv_lite_summary: dict[str, Any],
    factor_block_selection: dict[str, Any],
    factor_block_selection_context: dict[str, Any],
    cpcv_lite_runtime: dict[str, Any],
    search_budget_decision: dict[str, Any],
    lane_governance: dict[str, Any],
    ensemble_contract: dict[str, Any],
) -> dict[str, Any]:
    payload = train_config_snapshot_v4(
        asdict_fn=v4.asdict,
        options=options,
        task="cls",
        feature_cols=feature_cols,
        markets=markets,
        label_contract=label_contract,
        selection_recommendations=selection_recommendations,
        selection_policy=selection_policy,
        selection_calibration=selection_calibration,
        research_support_lane=research_support_lane,
        ranker_budget_profile={"profile": "v5_panel_ensemble"},
        cpcv_lite_summary=cpcv_lite_summary,
        factor_block_selection=factor_block_selection,
        factor_block_selection_context=factor_block_selection_context,
        cpcv_lite_runtime=cpcv_lite_runtime,
        search_budget_decision=search_budget_decision,
        lane_governance=lane_governance,
    )
    payload["trainer"] = "v5_panel_ensemble"
    payload["task"] = "cls"
    payload["panel_ensemble"] = dict(ensemble_contract or {})
    payload["predictor_contract"] = {
        "version": 1,
        "score_mean_field": "score_mean",
        "score_std_field": "score_std",
        "score_lcb_field": "score_lcb",
        "final_rank_score_field": "final_rank_score",
        "final_uncertainty_field": "score_std",
        "final_expected_return_field": "final_expected_return",
        "final_expected_es_field": "final_expected_es",
        "final_tradability_field": "final_tradability",
        "final_alpha_lcb_field": "final_alpha_lcb",
        "score_aliases": {"final_rank_score": "score_mean", "final_uncertainty": "score_std"},
        "score_lcb_formula": "score_lcb = clip(score_mean - score_std, 0, 1)",
        "alpha_lcb_formula": "final_alpha_lcb = final_expected_return - final_expected_es - final_uncertainty",
        "distributional_contract": dict((ensemble_contract or {}).get("distributional_contract") or {}),
    }
    return payload


def _score_source_objective(selection_recommendations: dict[str, Any]) -> float:
    if not isinstance(selection_recommendations, dict):
        return float("-inf")
    threshold_key = str(selection_recommendations.get("recommended_threshold_key", "")).strip()
    by_key = selection_recommendations.get("by_threshold_key") if isinstance(selection_recommendations.get("by_threshold_key"), dict) else {}
    if threshold_key and isinstance(by_key.get(threshold_key), dict):
        value = by_key[threshold_key].get("objective_score")
        try:
            return float(value)
        except Exception:
            return float("-inf")
    return float("-inf")


def _apply_score_source_to_windows(windows: list[dict[str, Any]], *, score_source: str) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for window in windows:
        payload = dict(window or {})
        by_source = payload.get("selection_optimization_by_score_source") if isinstance(payload.get("selection_optimization_by_score_source"), dict) else {}
        selected_opt = by_source.get(score_source) or payload.get("selection_optimization")
        payload["selection_optimization"] = dict(selected_opt or {})
        selected.append(payload)
    return selected


def train_and_register_v5_panel_ensemble(options: TrainV5PanelEnsembleOptions) -> TrainV5PanelEnsembleResult:
    if options.feature_set != "v4":
        raise ValueError("trainer v5_panel_ensemble requires --feature-set v4")
    if options.label_set != "v3":
        raise ValueError("trainer v5_panel_ensemble requires --label-set v3")

    started_at = time.time()
    run_id = v4.make_run_id(seed=options.seed)
    economic_objective_profile = v4.build_v4_shared_economic_objective_profile()
    lane_governance = v4._build_lane_governance_v4(
        task="cls",
        run_scope=options.run_scope,
        economic_objective_profile=economic_objective_profile,
    )
    prepared = prepare_v4_training_inputs(
        options=options,
        task="cls",
        build_dataset_request_fn=v4.build_dataset_request,
        load_feature_spec_fn=load_feature_spec,
        load_label_spec_fn=load_label_spec,
        feature_columns_from_spec_fn=feature_columns_from_spec,
        resolve_selected_feature_columns_from_latest_fn=resolve_selected_feature_columns_from_latest,
        resolve_v4_search_budget_fn=resolve_v4_search_budget,
        load_feature_dataset_fn=v4.load_feature_dataset,
        load_feature_aux_frame_fn=load_feature_aux_frame,
        expected_interval_ms_fn=v4.expected_interval_ms,
        compute_time_splits_fn=compute_time_splits,
        split_masks_fn=split_masks,
        validate_split_counts_fn=v4._validate_split_counts,
        resolve_ranker_budget_profile_fn=v4._resolve_ranker_budget_profile,
        factor_block_registry_fn=v4_factor_block_registry,
        resolve_cpcv_lite_runtime_config_fn=v4._resolve_cpcv_lite_runtime_config,
    )

    dataset = prepared["dataset"]
    pre_domain_sample_weight = np.asarray(prepared.get("pre_domain_sample_weight"), dtype=np.float64)
    label_spec = prepared["label_spec"]
    label_contract = prepared["label_contract"]
    request = prepared["request"]
    train_mask = prepared["train_mask"]
    valid_mask = prepared["valid_mask"]
    test_mask = prepared["test_mask"]
    rows = prepared["rows"]
    split_info = prepared["split_info"]
    interval_ms = int(prepared["interval_ms"])
    search_budget_decision = prepared["search_budget_decision"]
    factor_block_selection_context = prepared["factor_block_selection_context"]
    factor_block_registry = prepared["factor_block_registry"]
    cpcv_lite_runtime = prepared["cpcv_lite_runtime"]
    action_aux_arrays = prepared["action_aux_arrays"]
    effective_booster_sweep_trials = int(prepared["effective_booster_sweep_trials"])
    live_domain_reweighting = dict(prepared.get("live_domain_reweighting") or {})

    regression_targets = _load_v5_regression_targets(
        request=request,
        label_spec=label_spec,
        dataset=dataset,
        primary_y_cls_column=str(label_contract["y_cls_column"]),
        primary_y_reg_column=str(label_contract["y_reg_column"]),
        primary_y_rank_column=str(label_contract["y_rank_column"]),
    )
    auxiliary_targets = _load_v5_auxiliary_panel_targets(
        request=request,
        label_spec=label_spec,
        dataset=dataset,
        primary_y_cls_column=str(label_contract["y_cls_column"]),
        primary_y_reg_column=str(label_contract["y_reg_column"]),
        primary_y_rank_column=str(label_contract["y_rank_column"]),
    )
    cls_bundle = v4._fit_booster_sweep_weighted(
        x_train=dataset.X[train_mask],
        y_train=dataset.y_cls[train_mask],
        w_train=dataset.sample_weight[train_mask],
        x_valid=dataset.X[valid_mask],
        y_valid=dataset.y_cls[valid_mask],
        w_valid=dataset.sample_weight[valid_mask],
        y_reg_valid=dataset.y_reg[valid_mask],
        fee_bps_est=options.fee_bps_est,
        safety_bps=options.safety_bps,
        seed=options.seed,
        nthread=options.nthread,
        trials=effective_booster_sweep_trials,
        eval_sample_weight=dataset.sample_weight[valid_mask],
    )
    rank_bundle = v4._fit_booster_sweep_ranker(
        x_train=dataset.X[train_mask],
        y_train_rank=dataset.y_rank[train_mask],
        ts_train_ms=dataset.ts_ms[train_mask],
        w_train=dataset.sample_weight[train_mask],
        x_valid=dataset.X[valid_mask],
        y_valid_cls=dataset.y_cls[valid_mask],
        y_valid_reg=dataset.y_reg[valid_mask],
        y_valid_rank=dataset.y_rank[valid_mask],
        ts_valid_ms=dataset.ts_ms[valid_mask],
        w_valid=dataset.sample_weight[valid_mask],
        fee_bps_est=options.fee_bps_est,
        safety_bps=options.safety_bps,
        seed=options.seed,
        nthread=options.nthread,
        trials=int(v4._resolve_ranker_budget_profile(options=options, task="rank", effective_booster_sweep_trials=effective_booster_sweep_trials)["main_trials"]),
        eval_sample_weight=dataset.sample_weight[valid_mask],
    )
    regressor_results = _fit_v5_regression_heads(
        options=options,
        dataset=dataset,
        regression_targets=regression_targets,
        train_mask=train_mask,
        valid_mask=valid_mask,
        sweep_trials=effective_booster_sweep_trials,
    )

    primary_horizon_key = f"h{int(label_contract.get('primary_horizon_bars', 12))}"
    auxiliary_classifier_targets = {
        key: value for key, value in dict(auxiliary_targets.get("cls") or {}).items() if key != primary_horizon_key
    }
    auxiliary_rank_targets = {
        key: value for key, value in dict(auxiliary_targets.get("rank") or {}).items() if key != primary_horizon_key
    }
    auxiliary_classifier_bundles = _fit_v5_auxiliary_classifier_heads(
        options=options,
        best_params=dict(cls_bundle.get("best_params", {})),
        dataset=dataset,
        auxiliary_targets=auxiliary_classifier_targets,
        train_mask=train_mask,
        valid_mask=valid_mask,
    )
    auxiliary_ranker_bundles = _fit_v5_auxiliary_ranker_heads(
        options=options,
        best_params=dict(rank_bundle.get("best_params", {})),
        dataset=dataset,
        auxiliary_targets=auxiliary_rank_targets,
        train_mask=train_mask,
        valid_mask=valid_mask,
    )
    primary_y_reg = np.asarray(regression_targets.get(primary_horizon_key, dataset.y_reg), dtype=np.float64)
    oof = _build_v5_oof_windows(
        options=options,
        dataset=dataset,
        primary_y_reg=primary_y_reg,
        regression_targets=regression_targets,
        auxiliary_classifier_targets=auxiliary_classifier_targets,
        auxiliary_rank_targets=auxiliary_rank_targets,
        classifier_best_params=dict(cls_bundle.get("best_params", {})),
        ranker_best_params=dict(rank_bundle.get("best_params", {})),
        regressor_best_params={key: dict(value.get("best_params", {})) for key, value in regressor_results.items()},
        action_aux_arrays=action_aux_arrays,
    )
    selection_calibration_rows = list(oof.get("_selection_calibration_rows", []))
    selection_calibration = build_selection_calibration_by_score_source(
        by_score_source={
            "score_mean": build_selection_calibration_from_oos_rows(oos_rows=selection_calibration_rows),
            "score_lcb": build_selection_calibration_from_oos_rows(
                oos_rows=[
                    {
                        "window_index": row.get("window_index"),
                        "scores": row.get("score_lcb") or [],
                        "y_cls": row.get("y_cls") or [],
                    }
                    for row in selection_calibration_rows
                ]
            ),
        },
        default_score_source="score_mean",
    )
    meta_x_all = np.concatenate([row["x"] for row in oof["meta_rows"]], axis=0) if oof["meta_rows"] else np.empty((0, len(_STACK_COMPONENT_ORDER)), dtype=np.float64)
    meta_y_all = np.concatenate([row["y"] for row in oof["meta_rows"]], axis=0) if oof["meta_rows"] else np.empty(0, dtype=np.int64)
    meta_fit = _fit_meta_logistic(meta_x_all, meta_y_all, sample_weight=oof.get("sample_weight"))

    estimator = V5PanelEnsembleEstimator(
        classifier_bundle=cls_bundle["bundle"],
        ranker_bundle=rank_bundle["bundle"],
        auxiliary_classifier_bundles=dict(auxiliary_classifier_bundles),
        auxiliary_ranker_bundles=dict(auxiliary_ranker_bundles),
        regressor_bundles={key: value["bundle"] for key, value in regressor_results.items()},
        regression_member_bundles={
            key: tuple(oof.get("regression_member_bundles", {}).get(key) or ())
            for key in regressor_results.keys()
        },
        meta_model=meta_fit["meta_model"],
        meta_ensemble=tuple(oof.get("meta_models", [])),
        regression_horizons=tuple(int(key.replace("h", "")) for key in sorted(regressor_results.keys(), key=lambda item: int(item.replace("h", "")))),
        primary_horizon=int(label_contract.get("primary_horizon_bars", 12)),
        uncertainty_temperature=float(meta_fit.get("uncertainty_temperature", 1.0)),
    )
    final_bundle = {"model_type": "v5_panel_ensemble", "scaler": None, "estimator": estimator}

    valid_scores = _predict_scores(final_bundle, dataset.X[valid_mask])
    test_scores = _predict_scores(final_bundle, dataset.X[test_mask])
    valid_metrics = v4._attach_ranking_metrics(
        metrics=_evaluate_split(
            y_cls=dataset.y_cls[valid_mask],
            y_reg=primary_y_reg[valid_mask],
            scores=valid_scores,
            markets=dataset.markets[valid_mask],
            fee_bps_est=options.fee_bps_est,
            safety_bps=options.safety_bps,
            sample_weight=dataset.sample_weight[valid_mask],
        ),
        y_rank=dataset.y_rank[valid_mask],
        ts_ms=dataset.ts_ms[valid_mask],
        scores=valid_scores,
    )
    test_metrics = v4._attach_ranking_metrics(
        metrics=_evaluate_split(
            y_cls=dataset.y_cls[test_mask],
            y_reg=primary_y_reg[test_mask],
            scores=test_scores,
            markets=dataset.markets[test_mask],
            fee_bps_est=options.fee_bps_est,
            safety_bps=options.safety_bps,
            sample_weight=dataset.sample_weight[test_mask],
        ),
        y_rank=dataset.y_rank[test_mask],
        ts_ms=dataset.ts_ms[test_mask],
        scores=test_scores,
    )
    thresholds = _build_thresholds(
        valid_scores=valid_scores,
        y_reg_valid=primary_y_reg[valid_mask],
        fee_bps_est=options.fee_bps_est,
        safety_bps=options.safety_bps,
        ev_scan_steps=options.ev_scan_steps,
        ev_min_selected=options.ev_min_selected,
        sample_weight=dataset.sample_weight[valid_mask],
    )

    walk_forward = _build_walk_forward_report_v5(
        windows=oof.get("windows", []),
        skipped=oof.get("skipped_windows", []),
        selection_calibration_rows=selection_calibration_rows,
        trade_action_rows=oof.get("_trade_action_oos_rows", []),
    )
    fallback_selection_recommendations = build_selection_recommendations(
        valid_scores=valid_scores,
        valid_ts_ms=dataset.ts_ms[valid_mask],
        thresholds=thresholds,
    )
    walk_forward_windows = list(walk_forward.get("windows", []))
    selection_recommendations_mean = build_selection_recommendations_from_walk_forward(
        windows=_apply_score_source_to_windows(walk_forward_windows, score_source="score_mean"),
        fallback_recommendations=fallback_selection_recommendations,
    )
    selection_recommendations_lcb = build_selection_recommendations_from_walk_forward(
        windows=_apply_score_source_to_windows(walk_forward_windows, score_source="score_lcb"),
        fallback_recommendations=fallback_recommendations_mean if (fallback_recommendations_mean := selection_recommendations_mean) else fallback_selection_recommendations,
    )
    chosen_score_source = (
        "score_lcb"
        if _score_source_objective(selection_recommendations_lcb) >= _score_source_objective(selection_recommendations_mean)
        else "score_mean"
    )
    selection_recommendations = selection_recommendations_lcb if chosen_score_source == "score_lcb" else selection_recommendations_mean
    walk_forward["windows"] = _apply_score_source_to_windows(walk_forward_windows, score_source=chosen_score_source)
    walk_forward["selection_policy_compare"] = {
        "version": 1,
        "policy": "score_source_compare_v1",
        "score_mean_objective_score": _score_source_objective(selection_recommendations_mean),
        "score_lcb_objective_score": _score_source_objective(selection_recommendations_lcb),
        "chosen_score_source": chosen_score_source,
    }
    selection_policy = build_selection_policy_from_recommendations(
        selection_recommendations=selection_recommendations,
        fallback_threshold_key="top_5pct",
        forced_threshold_key=getattr(options, "selection_threshold_key_override", None),
        score_source=chosen_score_source,
    )
    cpcv_lite = _build_disabled_cpcv_lite(trigger=str((cpcv_lite_runtime or {}).get("trigger", "disabled")).strip() or "disabled")
    research_support_lane = v4._build_research_support_lane_v4(walk_forward=walk_forward, cpcv_lite=cpcv_lite)
    factor_block_selection = build_factor_block_selection_report(
        block_registry=factor_block_registry,
        window_rows=[],
        selection_mode=options.factor_block_selection_mode,
        feature_columns=dataset.feature_names,
        run_id=run_id,
        refit_support=walk_forward.get("factor_block_refit_support"),
    )
    metrics = _build_v5_metrics_doc(
        run_id=run_id,
        options=options,
        split_info=split_info,
        interval_ms=interval_ms,
        rows=rows,
        valid_metrics=valid_metrics,
        test_metrics=test_metrics,
        walk_forward=walk_forward,
        cpcv_lite=cpcv_lite,
        factor_block_selection=factor_block_selection,
        search_budget_decision=search_budget_decision,
        economic_objective_profile=economic_objective_profile,
        lane_governance=lane_governance,
        cls_bundle=cls_bundle,
        rank_bundle=rank_bundle,
        auxiliary_classifier_horizons=[int(key.replace("h", "")) for key in sorted(auxiliary_classifier_bundles.keys(), key=lambda item: int(item.replace("h", "")))],
        auxiliary_rank_horizons=[int(key.replace("h", "")) for key in sorted(auxiliary_ranker_bundles.keys(), key=lambda item: int(item.replace("h", "")))],
        regressor_results=regressor_results,
        meta_fit=meta_fit,
        meta_ensemble_count=len(oof.get("meta_models", [])),
        primary_horizon=int(label_contract.get("primary_horizon_bars", 12)),
    )
    leaderboard_row = _build_v5_leaderboard_row(run_id=run_id, options=options, rows=rows, test_metrics=test_metrics)
    train_config = _build_v5_train_config(
        options=options,
        feature_cols=dataset.feature_names,
        markets=dataset.selected_markets,
        label_contract=label_contract,
        selection_recommendations=selection_recommendations,
        selection_policy=selection_policy,
        selection_calibration=selection_calibration,
        research_support_lane=research_support_lane,
        cpcv_lite_summary=cpcv_lite.get("summary", {}),
        factor_block_selection=factor_block_selection,
        factor_block_selection_context=factor_block_selection_context,
        cpcv_lite_runtime=cpcv_lite_runtime,
        search_budget_decision=search_budget_decision,
        lane_governance=lane_governance,
        ensemble_contract=metrics.get("panel_ensemble", {}),
    )
    data_platform_ready_snapshot_id = resolve_ready_snapshot_id(project_root=Path.cwd())
    train_config["data_platform_ready_snapshot_id"] = data_platform_ready_snapshot_id
    data_fingerprint = build_data_fingerprint(request=request, selected_markets=dataset.selected_markets, total_rows=dataset.rows)
    data_fingerprint["code_version"] = autobot_version
    data_fingerprint["data_platform_ready_snapshot_id"] = data_platform_ready_snapshot_id
    if live_domain_reweighting:
        data_fingerprint["live_domain_reweighting"] = live_domain_reweighting
    model_card = render_model_card(
        run_id=run_id,
        model_family=options.model_family,
        champion="panel_ensemble",
        metrics=metrics,
        thresholds=thresholds,
        data_fingerprint=data_fingerprint,
    )

    run_dir = save_run(
        RegistrySavePayload(
            registry_root=options.registry_root,
            model_family=options.model_family,
            run_id=run_id,
            model_bundle=final_bundle,
            metrics=metrics,
            thresholds=thresholds,
            feature_spec=prepared["feature_spec"],
            label_spec=label_spec,
            train_config=train_config,
            data_fingerprint=data_fingerprint,
            leaderboard_row=leaderboard_row,
            model_card_text=model_card,
            selection_recommendations=selection_recommendations,
            selection_policy=selection_policy,
            selection_calibration=selection_calibration,
        ),
        publish_pointers=False,
    )
    update_artifact_status(run_dir, status="core_saved", core_saved=True)
    _write_json(run_dir / "panel_ensemble_contract.json", metrics.get("panel_ensemble", {}))
    _write_json(
        run_dir / "predictor_contract.json",
        {
            "version": 1,
            "score_mean_field": "score_mean",
            "score_std_field": "score_std",
            "score_lcb_field": "score_lcb",
            "final_rank_score_field": "final_rank_score",
            "final_uncertainty_field": "score_std",
            "final_expected_return_field": "final_expected_return",
            "final_expected_es_field": "final_expected_es",
            "final_tradability_field": "final_tradability",
            "final_alpha_lcb_field": "final_alpha_lcb",
            "score_aliases": {"final_rank_score": "score_mean", "final_uncertainty": "score_std"},
            "score_lcb_formula": "score_lcb = clip(score_mean - score_std, 0, 1)",
            "alpha_lcb_formula": "final_alpha_lcb = final_expected_return - final_expected_es - final_uncertainty",
            "distributional_contract": dict((metrics.get("panel_ensemble", {}) or {}).get("distributional_contract") or {}),
        },
    )
    write_v5_domain_weighting_report(
        run_dir=run_dir,
        payload=build_v5_domain_weighting_report(
            run_id=run_id,
            trainer_name="v5_panel_ensemble",
            model_family=options.model_family,
            component_order=["base_sample_weight", "data_quality_weight", "support_level_weight", "domain_weight"],
            final_sample_weight=np.asarray(dataset.sample_weight, dtype=np.float64),
            base_sample_weight=np.ones(dataset.rows, dtype=np.float64),
            data_quality_weight=pre_domain_sample_weight,
            support_weight=np.ones(dataset.rows, dtype=np.float64),
            domain_weight=np.clip(
                np.asarray(dataset.sample_weight, dtype=np.float64) / np.maximum(pre_domain_sample_weight, 1e-12),
                1e-6,
                None,
            ),
            domain_details=(
                dict(live_domain_reweighting or {})
                | {
                    "enabled": bool(live_domain_reweighting),
                    "policy": "v5_domain_weighting_v1",
                    "source_kind": "live_candidate_density_ratio_v1",
                    "status": (
                        "live_candidate_density_ratio_ready"
                        if bool(live_domain_reweighting)
                        else "disabled"
                    ),
                }
            ),
        ),
    )
    support_artifacts = v4_persistence.persist_v4_support_artifacts(
        run_dir=run_dir,
        options=options,
        run_id=run_id,
        factor_block_registry=factor_block_registry,
        walk_forward=walk_forward,
        cpcv_lite=cpcv_lite,
        factor_block_selection=factor_block_selection,
        search_budget_decision=search_budget_decision,
    )
    update_artifact_status(run_dir, status="support_artifacts_written", support_artifacts_written=True)
    tail_outputs = _run_panel_tail_common(
        run_dir=run_dir,
        run_id=run_id,
        options=options,
        dataset=dataset,
        estimator=estimator,
        primary_y_reg=primary_y_reg,
        train_mask=train_mask,
        valid_mask=valid_mask,
        test_mask=test_mask,
        walk_forward=walk_forward,
        cpcv_lite=cpcv_lite,
        factor_block_selection=factor_block_selection,
        factor_block_selection_context=factor_block_selection_context,
        search_budget_decision=search_budget_decision,
        selection_policy=selection_policy,
        selection_calibration=selection_calibration,
        metrics=metrics,
        research_support_lane=research_support_lane,
        cpcv_lite_runtime=cpcv_lite_runtime,
        live_domain_reweighting=live_domain_reweighting,
        economic_objective_profile=economic_objective_profile,
        lane_governance=lane_governance,
        data_platform_ready_snapshot_id=data_platform_ready_snapshot_id,
        logs_root=options.logs_root,
        pipeline_started_at=started_at,
        resumed=False,
    )
    return TrainV5PanelEnsembleResult(
        run_id=run_id,
        run_dir=run_dir,
        status=tail_outputs["status"],
        leaderboard_row=leaderboard_row,
        metrics=metrics,
        thresholds=thresholds,
        train_report_path=tail_outputs["train_report_path"],
        promotion_path=tail_outputs["runtime_artifacts"]["promotion_path"],
        walk_forward_report_path=support_artifacts.get("walk_forward_report_path"),
        cpcv_lite_report_path=support_artifacts.get("cpcv_lite_report_path"),
        factor_block_selection_path=support_artifacts.get("factor_block_selection_path"),
        factor_block_history_path=support_artifacts.get("factor_block_history_path"),
        factor_block_policy_path=support_artifacts.get("factor_block_policy_path"),
        search_budget_decision_path=support_artifacts.get("search_budget_decision_path"),
        execution_acceptance_report_path=tail_outputs["runtime_artifacts"].get("execution_acceptance_report_path"),
        runtime_recommendations_path=tail_outputs["runtime_artifacts"].get("runtime_recommendations_path"),
        trainer_research_evidence_path=tail_outputs["runtime_artifacts"].get("trainer_research_evidence_path"),
        economic_objective_profile_path=tail_outputs["runtime_artifacts"].get("economic_objective_profile_path"),
        lane_governance_path=tail_outputs["runtime_artifacts"].get("lane_governance_path"),
        decision_surface_path=tail_outputs["runtime_artifacts"].get("decision_surface_path"),
        experiment_ledger_path=tail_outputs["experiment_ledger_path"],
        experiment_ledger_summary_path=tail_outputs["experiment_ledger_summary_path"],
        live_domain_reweighting_path=None,
    )


def resume_v5_panel_ensemble_tail(*, run_dir: Path) -> TrainV5PanelEnsembleResult:
    started_at = time.time()
    run_dir = Path(run_dir).resolve()
    if not run_dir.exists():
        raise FileNotFoundError(f"run_dir not found: {run_dir}")

    train_config = load_json(run_dir / "train_config.yaml")
    if not train_config:
        raise FileNotFoundError(f"missing train_config.yaml in {run_dir}")
    options = _options_from_v5_panel_train_config(train_config)

    prepared = prepare_v4_training_inputs(
        options=options,
        task="cls",
        build_dataset_request_fn=v4.build_dataset_request,
        load_feature_spec_fn=load_feature_spec,
        load_label_spec_fn=load_label_spec,
        feature_columns_from_spec_fn=feature_columns_from_spec,
        resolve_selected_feature_columns_from_latest_fn=resolve_selected_feature_columns_from_latest,
        resolve_v4_search_budget_fn=resolve_v4_search_budget,
        load_feature_dataset_fn=v4.load_feature_dataset,
        load_feature_aux_frame_fn=load_feature_aux_frame,
        expected_interval_ms_fn=v4.expected_interval_ms,
        compute_time_splits_fn=compute_time_splits,
        split_masks_fn=split_masks,
        validate_split_counts_fn=v4._validate_split_counts,
        resolve_ranker_budget_profile_fn=v4._resolve_ranker_budget_profile,
        factor_block_registry_fn=v4_factor_block_registry,
        resolve_cpcv_lite_runtime_config_fn=v4._resolve_cpcv_lite_runtime_config,
    )
    dataset = prepared["dataset"]
    label_spec = prepared["label_spec"]
    label_contract = prepared["label_contract"]
    train_mask = prepared["train_mask"]
    valid_mask = prepared["valid_mask"]
    test_mask = prepared["test_mask"]
    rows = prepared["rows"]
    split_info = prepared["split_info"]
    interval_ms = int(prepared["interval_ms"])

    regression_targets = _load_v5_regression_targets(
        request=prepared["request"],
        label_spec=label_spec,
        dataset=dataset,
        primary_y_cls_column=str(label_contract["y_cls_column"]),
        primary_y_reg_column=str(label_contract["y_reg_column"]),
        primary_y_rank_column=str(label_contract["y_rank_column"]),
    )
    primary_horizon_key = f"h{int(label_contract.get('primary_horizon_bars', 12))}"
    primary_y_reg = np.asarray(regression_targets.get(primary_horizon_key, dataset.y_reg), dtype=np.float64)

    walk_forward = load_json(run_dir / "walk_forward_report.json")
    cpcv_lite = load_json(run_dir / "cpcv_lite_report.json")
    factor_block_selection = load_json(run_dir / "factor_block_selection.json")
    search_budget_decision = load_json(run_dir / "search_budget_decision.json") or prepared["search_budget_decision"]
    selection_policy = load_json(run_dir / "selection_policy.json")
    selection_calibration = load_json(run_dir / "selection_calibration.json")
    metrics = load_json(run_dir / "metrics.json")
    thresholds = load_json(run_dir / "thresholds.json")
    leaderboard_row = load_json(run_dir / "leaderboard_row.json")
    model_bundle = load_model_bundle(run_dir)
    estimator = model_bundle.get("estimator") if isinstance(model_bundle, dict) else None
    if estimator is None:
        raise ValueError(f"run_dir does not contain a usable panel estimator: {run_dir}")

    factor_block_selection_context = dict(((train_config.get("factor_block_selection") or {}).get("resolution_context")) or {})
    cpcv_lite_runtime = dict(train_config.get("cpcv_lite") or prepared["cpcv_lite_runtime"])
    live_domain_reweighting = dict(prepared.get("live_domain_reweighting") or {})
    economic_objective_profile = v4.build_v4_shared_economic_objective_profile()
    lane_governance = v4._build_lane_governance_v4(
        task="cls",
        run_scope=options.run_scope,
        economic_objective_profile=economic_objective_profile,
    )
    research_support_lane = v4._build_research_support_lane_v4(walk_forward=walk_forward, cpcv_lite=cpcv_lite)
    data_platform_ready_snapshot_id = (
        str(train_config.get("data_platform_ready_snapshot_id") or "").strip()
        or resolve_ready_snapshot_id(project_root=Path.cwd())
    )
    tail_outputs = _run_panel_tail_common(
        run_dir=run_dir,
        run_id=run_dir.name,
        options=options,
        dataset=dataset,
        estimator=estimator,
        primary_y_reg=primary_y_reg,
        train_mask=train_mask,
        valid_mask=valid_mask,
        test_mask=test_mask,
        walk_forward=walk_forward,
        cpcv_lite=cpcv_lite,
        factor_block_selection=factor_block_selection,
        factor_block_selection_context=factor_block_selection_context,
        search_budget_decision=search_budget_decision,
        selection_policy=selection_policy,
        selection_calibration=selection_calibration,
        metrics=metrics,
        research_support_lane=research_support_lane,
        cpcv_lite_runtime=cpcv_lite_runtime,
        live_domain_reweighting=live_domain_reweighting,
        economic_objective_profile=economic_objective_profile,
        lane_governance=lane_governance,
        data_platform_ready_snapshot_id=data_platform_ready_snapshot_id,
        logs_root=options.logs_root,
        pipeline_started_at=started_at,
        resumed=True,
    )
    return TrainV5PanelEnsembleResult(
        run_id=run_dir.name,
        run_dir=run_dir,
        status=tail_outputs["status"],
        leaderboard_row=leaderboard_row,
        metrics=metrics,
        thresholds=thresholds,
        train_report_path=tail_outputs["train_report_path"],
        promotion_path=tail_outputs["runtime_artifacts"]["promotion_path"],
        walk_forward_report_path=(run_dir / "walk_forward_report.json") if (run_dir / "walk_forward_report.json").exists() else None,
        cpcv_lite_report_path=(run_dir / "cpcv_lite_report.json") if (run_dir / "cpcv_lite_report.json").exists() else None,
        factor_block_selection_path=(run_dir / "factor_block_selection.json") if (run_dir / "factor_block_selection.json").exists() else None,
        factor_block_history_path=Path(str((factor_block_selection or {}).get("history_path"))) if str((factor_block_selection or {}).get("history_path", "")).strip() else None,
        factor_block_policy_path=Path(str((factor_block_selection or {}).get("guarded_policy_path"))) if str((factor_block_selection or {}).get("guarded_policy_path", "")).strip() else None,
        search_budget_decision_path=(run_dir / "search_budget_decision.json") if (run_dir / "search_budget_decision.json").exists() else None,
        execution_acceptance_report_path=tail_outputs["runtime_artifacts"].get("execution_acceptance_report_path"),
        runtime_recommendations_path=tail_outputs["runtime_artifacts"].get("runtime_recommendations_path"),
        trainer_research_evidence_path=tail_outputs["runtime_artifacts"].get("trainer_research_evidence_path"),
        economic_objective_profile_path=tail_outputs["runtime_artifacts"].get("economic_objective_profile_path"),
        lane_governance_path=tail_outputs["runtime_artifacts"].get("lane_governance_path"),
        decision_surface_path=tail_outputs["runtime_artifacts"].get("decision_surface_path"),
        experiment_ledger_path=tail_outputs["experiment_ledger_path"],
        experiment_ledger_summary_path=tail_outputs["experiment_ledger_summary_path"],
        live_domain_reweighting_path=None,
    )


def _options_from_v5_panel_train_config(train_config: dict[str, Any]) -> TrainV5PanelEnsembleOptions:
    base = dict(train_config or {})
    alpha_doc = dict(base.get("execution_acceptance_model_alpha") or {})
    return v4.TrainV4CryptoCsOptions(
        dataset_root=Path(str(base["dataset_root"])),
        registry_root=Path(str(base["registry_root"])),
        logs_root=Path(str(base["logs_root"])),
        model_family=str(base["model_family"]),
        tf=str(base["tf"]),
        quote=str(base["quote"]),
        top_n=int(base["top_n"]),
        start=str(base["start"]),
        end=str(base["end"]),
        feature_set=str(base["feature_set"]),
        label_set=str(base["label_set"]),
        task=str(base["task"]),
        booster_sweep_trials=int(base["booster_sweep_trials"]),
        seed=int(base["seed"]),
        nthread=int(base["nthread"]),
        batch_rows=int(base["batch_rows"]),
        train_ratio=float(base["train_ratio"]),
        valid_ratio=float(base["valid_ratio"]),
        test_ratio=float(base["test_ratio"]),
        embargo_bars=int(base["embargo_bars"]),
        fee_bps_est=float(base["fee_bps_est"]),
        safety_bps=float(base["safety_bps"]),
        ev_scan_steps=int(base["ev_scan_steps"]),
        ev_min_selected=int(base["ev_min_selected"]),
        min_rows_for_train=int(base.get("min_rows_for_train", 5000)),
        walk_forward_enabled=bool(base.get("walk_forward_enabled", True)),
        walk_forward_windows=int(base.get("walk_forward_windows", 4)),
        walk_forward_sweep_trials=int(base.get("walk_forward_sweep_trials", 3)),
        walk_forward_min_train_rows=int(base.get("walk_forward_min_train_rows", 1000)),
        walk_forward_min_test_rows=int(base.get("walk_forward_min_test_rows", 200)),
        cpcv_lite_enabled=bool(base.get("cpcv_lite_enabled", False)),
        cpcv_lite_group_count=int(base.get("cpcv_lite_group_count", 6)),
        cpcv_lite_test_group_count=int(base.get("cpcv_lite_test_group_count", 2)),
        cpcv_lite_max_combinations=int(base.get("cpcv_lite_max_combinations", 6)),
        cpcv_lite_min_train_rows=int(base.get("cpcv_lite_min_train_rows", 1000)),
        cpcv_lite_min_test_rows=int(base.get("cpcv_lite_min_test_rows", 200)),
        factor_block_selection_mode=str(base.get("factor_block_selection_mode", "guarded_auto")),
        selection_threshold_key_override=base.get("selection_threshold_key_override"),
        multiple_testing_alpha=float(base.get("multiple_testing_alpha", 0.20)),
        multiple_testing_bootstrap_iters=int(base.get("multiple_testing_bootstrap_iters", 500)),
        multiple_testing_block_length=int(base.get("multiple_testing_block_length", 0)),
        execution_acceptance_enabled=bool(base.get("execution_acceptance_enabled", False)),
        execution_acceptance_dataset_name=str(base.get("execution_acceptance_dataset_name", "candles_v1")),
        execution_acceptance_parquet_root=Path(str(base.get("execution_acceptance_parquet_root", "data/parquet"))),
        execution_acceptance_output_root=Path(str(base.get("execution_acceptance_output_root", "data/backtest"))),
        execution_acceptance_eval_start=base.get("execution_acceptance_eval_start"),
        execution_acceptance_eval_end=base.get("execution_acceptance_eval_end"),
        execution_acceptance_eval_label=str(base.get("execution_acceptance_eval_label", "train_window")),
        execution_acceptance_eval_source=str(base.get("execution_acceptance_eval_source", "train_command_window")),
        execution_acceptance_top_n=int(base.get("execution_acceptance_top_n", 0)),
        execution_acceptance_dense_grid=bool(base.get("execution_acceptance_dense_grid", False)),
        execution_acceptance_starting_krw=float(base.get("execution_acceptance_starting_krw", 50000.0)),
        execution_acceptance_per_trade_krw=float(base.get("execution_acceptance_per_trade_krw", 10000.0)),
        execution_acceptance_max_positions=int(base.get("execution_acceptance_max_positions", 2)),
        execution_acceptance_min_order_krw=float(base.get("execution_acceptance_min_order_krw", 5000.0)),
        execution_acceptance_order_timeout_bars=int(base.get("execution_acceptance_order_timeout_bars", 5)),
        execution_acceptance_reprice_max_attempts=int(base.get("execution_acceptance_reprice_max_attempts", 1)),
        execution_acceptance_reprice_tick_steps=int(base.get("execution_acceptance_reprice_tick_steps", 1)),
        execution_acceptance_rules_ttl_sec=int(base.get("execution_acceptance_rules_ttl_sec", 86400)),
        run_scope=str(base.get("run_scope", "scheduled_daily")),
        execution_acceptance_model_alpha=ModelAlphaSettings(
            model_ref=str(alpha_doc.get("model_ref", "champion_v4")),
            model_family=alpha_doc.get("model_family"),
            feature_set=str(alpha_doc.get("feature_set", "v4")),
            selection=ModelAlphaSelectionSettings(**dict(alpha_doc.get("selection") or {})),
            position=ModelAlphaPositionSettings(**dict(alpha_doc.get("position") or {})),
            exit=ModelAlphaExitSettings(**dict(alpha_doc.get("exit") or {})),
            execution=ModelAlphaExecutionSettings(**dict(alpha_doc.get("execution") or {})),
            operational=ModelAlphaOperationalSettings(**dict(alpha_doc.get("operational") or {})),
        ),
        dependency_expert_only=bool(base.get("dependency_expert_only", False)),
        live_domain_reweighting_enabled=bool(base.get("live_domain_reweighting_enabled", False)),
        live_domain_reweighting_db_path=Path(str(base["live_domain_reweighting_db_path"])) if base.get("live_domain_reweighting_db_path") else None,
        live_domain_reweighting_min_target_rows=int(base.get("live_domain_reweighting_min_target_rows", 32)),
        live_domain_reweighting_max_target_rows=int(base.get("live_domain_reweighting_max_target_rows", 1024)),
        live_domain_reweighting_clip_min=float(base.get("live_domain_reweighting_clip_min", 0.5)),
        live_domain_reweighting_clip_max=float(base.get("live_domain_reweighting_clip_max", 3.0)),
    )
