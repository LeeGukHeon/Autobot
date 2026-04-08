"""Fusion meta-model trainer for panel/sequence/LOB expert predictions."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import time
from typing import Any

import numpy as np
import polars as pl

from autobot import __version__ as autobot_version
from autobot.strategy.model_alpha_evaluation_contract import load_evaluation_contract
from autobot.strategy.v5_post_model_contract import (
    V5_POST_MODEL_CONTRACT_VERSION,
    annotate_v5_runtime_recommendations,
    resolve_v5_entry_gate,
)

from .entry_boundary import build_risk_calibrated_entry_boundary
from .execution_risk_control import normalize_execution_risk_control_payload
from .metrics import classification_metrics, grouped_trading_metrics, trading_metrics
from .model_card import render_model_card
from .entry_boundary import evaluate_entry_boundary
from .registry import RegistrySavePayload, load_json, load_model_bundle, make_run_id, save_run, update_artifact_status, update_latest_pointer
from .runtime_feature_dataset import write_runtime_feature_dataset
from .runtime_recommendation_contract import normalize_runtime_exit_payload
from .selection_calibration import _identity_calibration
from .selection_policy import build_selection_policy_from_recommendations
from .selection_optimizer import (
    SelectionGridConfig,
    build_selection_recommendations_from_walk_forward,
    build_window_selection_objectives,
)
from .split import compute_time_splits, split_masks
from .trade_action_policy import normalize_trade_action_policy
from .train_v1 import _build_thresholds, build_selection_recommendations
from .train_v5_sequence import _parse_date_to_ts_ms, _sha256_file
from .v5_expert_runtime_export import OPERATING_WINDOW_TIMEZONE, build_ts_date_coverage_payload, operating_date_range
from .v5_expert_tail import (
    build_v5_expert_tail_context,
    expert_tail_context_path,
    finalize_v5_expert_family_run,
    resolve_existing_v5_expert_tail_artifacts,
    run_or_reuse_v5_runtime_governance_artifacts,
)
from .v5_domain_weighting import (
    build_v5_domain_weighting_report,
    resolve_v5_domain_weighting_components,
    write_v5_domain_weighting_report,
)
from .ood_generalization import build_ood_generalization_report, write_ood_generalization_report
from autobot.ops.data_platform_snapshot import resolve_ready_snapshot_id
from .split import compute_anchored_walk_forward_splits


VALID_FUSION_STACKERS = ("linear", "monotone_gbdt", "regime_moe")
_FUSION_INPUT_CONTRACT_FILENAME = "fusion_input_contract.json"
_FUSION_RUNTIME_INPUT_CONTRACT_FILENAME = "fusion_runtime_input_contract.json"
_FUSION_ENTRY_BOUNDARY_STATUS_KEY = "entry_boundary_complete"


@dataclass(frozen=True)
class TrainV5FusionOptions:
    panel_input_path: Path
    sequence_input_path: Path
    lob_input_path: Path
    registry_root: Path
    logs_root: Path
    model_family: str
    quote: str
    start: str
    end: str
    seed: int
    tradability_input_path: Path | None = None
    panel_runtime_input_path: Path | None = None
    sequence_runtime_input_path: Path | None = None
    lob_runtime_input_path: Path | None = None
    tradability_runtime_input_path: Path | None = None
    runtime_start: str | None = None
    runtime_end: str | None = None
    stacker_family: str = "linear"
    run_scope: str = "manual_fusion_expert"


@dataclass(frozen=True)
class TrainV5FusionResult:
    run_id: str
    run_dir: Path
    status: str
    leaderboard_row: dict[str, Any]
    metrics: dict[str, Any]
    thresholds: dict[str, Any]
    train_report_path: Path
    promotion_path: Path
    walk_forward_report_path: Path
    fusion_model_contract_path: Path
    predictor_contract_path: Path
    entry_boundary_contract_path: Path
    runtime_viability_report_path: Path


@dataclass(frozen=True)
class _FusionInputBundle:
    merged: pl.DataFrame
    input_contract: dict[str, Any]
    feature_names: tuple[str, ...]
    monotone_signs: tuple[int, ...]


@dataclass
class V5FusionEstimator:
    score_model: Any
    return_model: Any
    es_model: Any
    tradability_model: Any
    uncertainty_model: Any
    stacker_family: str
    feature_names: tuple[str, ...]
    regime_feature_columns: tuple[str, ...] = ()
    regime_cluster_count: int = 1
    gating_policy: str = "single_expert_v1"

    def _predict_score(self, x: np.ndarray) -> np.ndarray:
        if hasattr(self.score_model, "predict_proba"):
            return np.asarray(self.score_model.predict_proba(x)[:, 1], dtype=np.float64)
        return np.clip(np.asarray(self.score_model.predict(x), dtype=np.float64), 0.0, 1.0)

    def _predict_binary_prob(self, model: Any, x: np.ndarray) -> np.ndarray:
        if hasattr(model, "predict_proba"):
            return np.asarray(model.predict_proba(x)[:, 1], dtype=np.float64)
        return np.clip(np.asarray(model.predict(x), dtype=np.float64), 0.0, 1.0)

    def predict_panel_contract(self, x: np.ndarray) -> dict[str, np.ndarray]:
        score_mean = self._predict_score(x)
        expected_return = np.asarray(self.return_model.predict(x), dtype=np.float64)
        expected_es = np.abs(np.asarray(self.es_model.predict(x), dtype=np.float64))
        tradability = np.clip(self._predict_binary_prob(self.tradability_model, x), 0.0, 1.0)
        uncertainty = np.maximum(np.asarray(self.uncertainty_model.predict(x), dtype=np.float64), 1e-6)
        score_lcb = np.clip(score_mean - uncertainty, 0.0, 1.0)
        return {
            "final_rank_score": score_mean,
            "final_uncertainty": uncertainty,
            "score_mean": score_mean,
            "score_std": uncertainty,
            "score_lcb": score_lcb,
            "final_expected_return": expected_return,
            "final_expected_es": expected_es,
            "final_tradability": tradability,
            "final_alpha_lcb": expected_return - expected_es - uncertainty,
        }


@dataclass
class _RegimeMoEBinaryHead:
    centroids: np.ndarray
    feature_indices: tuple[int, ...]
    models: tuple[Any, ...]

    def _assign(self, x: np.ndarray) -> np.ndarray:
        matrix = np.asarray(x, dtype=np.float64)
        if len(self.models) <= 1 or len(self.feature_indices) <= 0:
            return np.zeros(matrix.shape[0], dtype=np.int64)
        regime = matrix[:, list(self.feature_indices)]
        distance = np.sum((regime[:, None, :] - self.centroids[None, :, :]) ** 2, axis=2)
        return np.argmin(distance, axis=1).astype(np.int64, copy=False)

    def predict_proba(self, x: np.ndarray) -> np.ndarray:
        matrix = np.asarray(x, dtype=np.float64)
        assignments = self._assign(matrix)
        payload = np.zeros((matrix.shape[0], 2), dtype=np.float64)
        for cluster_idx, model in enumerate(self.models):
            mask = assignments == int(cluster_idx)
            if not np.any(mask):
                continue
            if hasattr(model, "predict_proba"):
                payload[mask] = np.asarray(model.predict_proba(matrix[mask]), dtype=np.float64)
            else:
                prob = np.clip(np.asarray(model.predict(matrix[mask]), dtype=np.float64), 0.0, 1.0)
                payload[mask] = np.column_stack([1.0 - prob, prob])
        return payload

    def predict(self, x: np.ndarray) -> np.ndarray:
        return self.predict_proba(x)[:, 1]


@dataclass
class _RegimeMoERegressionHead:
    centroids: np.ndarray
    feature_indices: tuple[int, ...]
    models: tuple[Any, ...]

    def _assign(self, x: np.ndarray) -> np.ndarray:
        matrix = np.asarray(x, dtype=np.float64)
        if len(self.models) <= 1 or len(self.feature_indices) <= 0:
            return np.zeros(matrix.shape[0], dtype=np.int64)
        regime = matrix[:, list(self.feature_indices)]
        distance = np.sum((regime[:, None, :] - self.centroids[None, :, :]) ** 2, axis=2)
        return np.argmin(distance, axis=1).astype(np.int64, copy=False)

    def predict(self, x: np.ndarray) -> np.ndarray:
        matrix = np.asarray(x, dtype=np.float64)
        assignments = self._assign(matrix)
        payload = np.zeros(matrix.shape[0], dtype=np.float64)
        for cluster_idx, model in enumerate(self.models):
            mask = assignments == int(cluster_idx)
            if not np.any(mask):
                continue
            payload[mask] = np.asarray(model.predict(matrix[mask]), dtype=np.float64)
        return payload


def _fusion_input_contract_path(run_dir: Path) -> Path:
    return run_dir / _FUSION_INPUT_CONTRACT_FILENAME


def _fusion_runtime_input_contract_path(run_dir: Path) -> Path:
    return run_dir / _FUSION_RUNTIME_INPUT_CONTRACT_FILENAME


def _fusion_entry_boundary_path(run_dir: Path) -> Path:
    return run_dir / "entry_boundary_contract.json"


def _resolve_fusion_interval_ms(ts_ms: np.ndarray) -> int:
    values = np.unique(np.asarray(ts_ms, dtype=np.int64))
    if values.size < 2:
        return 300_000
    diffs = np.diff(values)
    positive = diffs[diffs > 0]
    if positive.size <= 0:
        return 300_000
    return max(int(np.median(positive)), 1)


def _build_fusion_selection_walk_forward(
    *,
    valid_scores: np.ndarray,
    y_reg_valid: np.ndarray,
    valid_ts_ms: np.ndarray,
    thresholds: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    fallback_selection_recommendations = build_selection_recommendations(
        valid_scores=valid_scores,
        valid_ts_ms=valid_ts_ms,
        thresholds=thresholds,
    )
    unique_ts = np.unique(np.asarray(valid_ts_ms, dtype=np.int64))
    interval_ms = _resolve_fusion_interval_ms(valid_ts_ms)
    selection_walk_forward: dict[str, Any] = {
        "policy": "fusion_selection_walk_forward_v1",
        "enabled": False,
        "window_count_requested": 4,
        "window_count_effective": 0,
        "interval_ms": int(interval_ms),
        "windows": [],
        "skip_reason": "",
    }
    if unique_ts.size < 5:
        selection_walk_forward["skip_reason"] = "INSUFFICIENT_UNIQUE_TIMESTAMPS"
        return fallback_selection_recommendations, selection_walk_forward
    try:
        window_specs = compute_anchored_walk_forward_splits(
            np.asarray(valid_ts_ms, dtype=np.int64),
            valid_ratio=0.25,
            test_ratio=0.25,
            window_count=int(selection_walk_forward["window_count_requested"]),
            embargo_bars=0,
            interval_ms=int(interval_ms),
        )
    except ValueError as exc:
        selection_walk_forward["skip_reason"] = str(exc)
        return fallback_selection_recommendations, selection_walk_forward

    windows: list[dict[str, Any]] = []
    valid_scores_array = np.asarray(valid_scores, dtype=np.float64)
    valid_returns_array = np.asarray(y_reg_valid, dtype=np.float64)
    valid_ts_array = np.asarray(valid_ts_ms, dtype=np.int64)
    for labels, info in window_specs:
        masks = split_masks(labels)
        test_mask = masks["test"]
        if not np.any(test_mask):
            continue
        window_scores = valid_scores_array[test_mask]
        window_returns = valid_returns_array[test_mask]
        window_ts = valid_ts_array[test_mask]
        windows.append(
            {
                "window_index": int(info.window_index),
                "time_window": {
                    "valid_start_ts": int(info.valid_start_ts),
                    "test_start_ts": int(info.test_start_ts),
                    "test_end_ts": int(info.test_end_ts),
                },
                "counts": dict(info.counts),
                "selection_optimization": build_window_selection_objectives(
                    scores=window_scores,
                    y_reg=window_returns,
                    ts_ms=window_ts,
                    thresholds=thresholds,
                    fee_bps_est=0.0,
                    safety_bps=0.0,
                    config=SelectionGridConfig(),
                ),
            }
        )
    if not windows:
        selection_walk_forward["skip_reason"] = "NO_USABLE_WALK_FORWARD_WINDOWS"
        return fallback_selection_recommendations, selection_walk_forward

    selection_recommendations = build_selection_recommendations_from_walk_forward(
        windows=windows,
        fallback_recommendations=fallback_selection_recommendations,
    )
    selection_walk_forward.update(
        {
            "enabled": True,
            "window_count_effective": int(len(windows)),
            "windows": windows,
            "recommended_threshold_key": str(selection_recommendations.get("recommended_threshold_key") or "").strip(),
            "recommended_threshold_key_source": str(selection_recommendations.get("recommended_threshold_key_source") or "").strip(),
            "fallback_used": bool(((selection_recommendations.get("optimizer") or {}).get("fallback_used"))),
        }
    )
    return selection_recommendations, selection_walk_forward


def _normalize_support_level_summary(frame: pl.DataFrame) -> dict[str, int]:
    if "support_level" not in frame.columns:
        return {}
    counts: dict[str, int] = {}
    for row in frame.group_by("support_level").len().iter_rows(named=True):
        key = str(row.get("support_level") or "").strip()
        if not key:
            continue
        counts[key] = int(row.get("len", 0) or 0)
    return counts


def _load_fusion_input_metadata(*, path: Path, prefix: str) -> dict[str, Any]:
    resolved_path = Path(path).resolve()
    if not resolved_path.exists():
        raise FileNotFoundError(f"{prefix} input parquet missing: {resolved_path}")
    run_dir = resolved_path.parent
    if not (run_dir / "train_config.yaml").exists():
        for parent in resolved_path.parents:
            if (parent / "train_config.yaml").exists():
                run_dir = parent
                break
    train_config = load_json(run_dir / "train_config.yaml")
    if not train_config:
        raise FileNotFoundError(f"{prefix} input missing train_config.yaml: {run_dir}")
    runtime_recommendations = load_json(run_dir / "runtime_recommendations.json")
    sequence_pretrain_contract_path = run_dir / "sequence_pretrain_contract.json"
    sequence_pretrain_report_path = run_dir / "sequence_pretrain_report.json"
    sequence_pretrain_contract = load_json(sequence_pretrain_contract_path) if sequence_pretrain_contract_path.exists() else {}
    sequence_pretrain_report = load_json(sequence_pretrain_report_path) if sequence_pretrain_report_path.exists() else {}
    export_metadata_path = resolved_path.parent / "metadata.json"
    export_metadata = load_json(export_metadata_path) if export_metadata_path.exists() else {}
    return {
        "prefix": prefix,
        "path": str(resolved_path),
        "run_dir": str(run_dir),
        "run_id": run_dir.name,
        "model_family": str(train_config.get("model_family") or "").strip() or run_dir.parent.name,
        "trainer": str(train_config.get("trainer") or "").strip(),
        "backbone_family": str(train_config.get("backbone_family") or "").strip(),
        "pretrain_method": str(train_config.get("pretrain_method") or "").strip(),
        "sequence_variant_name": str(
            train_config.get("sequence_variant_name")
            or runtime_recommendations.get("sequence_variant_name")
            or ""
        ).strip(),
        "lob_variant_name": str(
            train_config.get("lob_variant_name")
            or runtime_recommendations.get("lob_variant_name")
            or ""
        ).strip(),
        "data_platform_ready_snapshot_id": str(train_config.get("data_platform_ready_snapshot_id") or "").strip(),
        "runtime_recommendations": dict(runtime_recommendations or {}),
        "sequence_pretrain_contract_path": str(sequence_pretrain_contract_path) if sequence_pretrain_contract_path.exists() else "",
        "sequence_pretrain_report_path": str(sequence_pretrain_report_path) if sequence_pretrain_report_path.exists() else "",
        "sequence_pretrain_method": str(
            runtime_recommendations.get("sequence_pretrain_method")
            or train_config.get("pretrain_method")
            or ""
        ).strip(),
        "sequence_pretrain_status": str(
            runtime_recommendations.get("sequence_pretrain_status")
            or sequence_pretrain_report.get("status")
            or sequence_pretrain_contract.get("status")
            or ""
        ).strip(),
        "sequence_pretrain_objective": str(
            runtime_recommendations.get("sequence_pretrain_objective")
            or sequence_pretrain_report.get("objective_name")
            or sequence_pretrain_contract.get("objective_name")
            or ""
        ).strip(),
        "sequence_pretrain_ready": bool(
            runtime_recommendations.get("sequence_pretrain_ready", sequence_pretrain_contract.get("pretrain_ready", False))
        ),
        "sequence_pretrain_best_epoch": int(
            runtime_recommendations.get("sequence_pretrain_best_epoch")
            or sequence_pretrain_report.get("best_epoch")
            or sequence_pretrain_contract.get("best_epoch")
            or 0
        ),
        "sequence_pretrain_encoder_present": bool(
            runtime_recommendations.get("sequence_pretrain_encoder_present", False)
            or (
                sequence_pretrain_contract_path.exists()
                and bool(str(sequence_pretrain_contract.get("encoder_artifact_path") or "").strip())
                and Path(str(sequence_pretrain_contract.get("encoder_artifact_path") or "")).exists()
            )
        ),
        "requested_selected_markets": list(export_metadata.get("requested_selected_markets") or []),
        "selected_markets": list(export_metadata.get("selected_markets") or list(train_config.get("selected_markets") or [])),
        "selected_markets_source": str(export_metadata.get("selected_markets_source") or "").strip(),
        "fallback_reason": str(export_metadata.get("fallback_reason") or "").strip(),
        "export_window_start": str(export_metadata.get("start") or "").strip(),
        "export_window_end": str(export_metadata.get("end") or "").strip(),
        "coverage_start_ts_ms": int(export_metadata.get("coverage_start_ts_ms", 0) or 0),
        "coverage_end_ts_ms": int(export_metadata.get("coverage_end_ts_ms", 0) or 0),
        "coverage_start_date": str(export_metadata.get("coverage_start_date") or "").strip(),
        "coverage_end_date": str(export_metadata.get("coverage_end_date") or "").strip(),
        "coverage_dates": list(export_metadata.get("coverage_dates") or []),
        "window_timezone": str(export_metadata.get("window_timezone") or "").strip(),
        "generation_context_window": dict(export_metadata.get("generation_context_window") or {}),
        "output_window": dict(export_metadata.get("output_window") or {}),
        "runtime_export_metadata_path": str(export_metadata_path) if export_metadata_path.exists() else "",
    }


def _required_input_columns(prefix: str) -> tuple[str, ...]:
    if prefix == "panel":
        return (
            "market",
            "ts_ms",
            "split",
            "y_cls",
            "y_reg",
            "final_rank_score",
            "final_expected_return",
            "final_expected_es",
            "final_tradability",
            "final_uncertainty",
            "final_alpha_lcb",
        )
    if prefix == "sequence":
        return (
            "market",
            "ts_ms",
            "directional_probability_primary",
            "sequence_uncertainty_primary",
        )
    if prefix == "lob":
        return (
            "market",
            "ts_ms",
            "micro_alpha_1s",
            "micro_alpha_5s",
            "micro_alpha_30s",
            "micro_uncertainty",
        )
    if prefix == "tradability":
        return (
            "market",
            "ts_ms",
            "tradability_prob",
            "fill_within_deadline_prob",
            "expected_shortfall_bps",
            "adverse_tolerance_prob",
            "tradability_uncertainty",
        )
    raise ValueError(f"unsupported fusion input prefix: {prefix}")


def _assert_required_input_columns(frame: pl.DataFrame, *, prefix: str) -> None:
    missing = [name for name in _required_input_columns(prefix) if name not in frame.columns]
    if missing:
        raise ValueError(f"{prefix} fusion input missing required columns: {', '.join(missing)}")


def _assert_no_duplicate_input_rows(frame: pl.DataFrame, *, prefix: str) -> None:
    duplicates = frame.group_by(["market", "ts_ms"]).len().filter(pl.col("len") > 1)
    if duplicates.height > 0:
        raise ValueError(f"{prefix} fusion input contains duplicate (market, ts_ms) rows")


def _support_level_indicator_columns(frame: pl.DataFrame, *, prefix: str) -> list[pl.Expr]:
    if "support_level" not in frame.columns:
        return []
    base = pl.col("support_level").cast(pl.Utf8).fill_null("")
    return [
        base.eq("strict_full").cast(pl.Float64).alias(f"{prefix}_support_is_strict"),
        base.eq("reduced_context").cast(pl.Float64).alias(f"{prefix}_support_is_reduced"),
        (
            pl.when(base.eq("strict_full"))
            .then(2.0)
            .when(base.eq("reduced_context"))
            .then(1.0)
            .otherwise(0.0)
        ).cast(pl.Float64).alias(f"{prefix}_support_score"),
    ]


def _load_expert_table(path: Path, *, prefix: str) -> tuple[pl.DataFrame, dict[str, Any]]:
    frame = pl.read_parquet(path)
    _assert_required_input_columns(frame, prefix=prefix)
    _assert_no_duplicate_input_rows(frame, prefix=prefix)
    metadata = _load_fusion_input_metadata(path=path, prefix=prefix)
    metadata["rows"] = int(frame.height)
    frame_markets = sorted({str(item).strip().upper() for item in frame.get_column("market").to_list() if str(item).strip()})
    if not metadata.get("selected_markets"):
        metadata["selected_markets"] = frame_markets
    if not metadata.get("requested_selected_markets"):
        metadata["requested_selected_markets"] = list(metadata.get("selected_markets") or frame_markets)
    if int(metadata.get("coverage_start_ts_ms", 0) or 0) <= 0 and frame.height > 0:
        metadata["coverage_start_ts_ms"] = int(frame.get_column("ts_ms").min())
    if int(metadata.get("coverage_end_ts_ms", 0) or 0) <= 0 and frame.height > 0:
        metadata["coverage_end_ts_ms"] = int(frame.get_column("ts_ms").max())
    if not metadata.get("coverage_start_date") or not metadata.get("coverage_end_date"):
        metadata.update(
            build_ts_date_coverage_payload(
                frame.get_column("ts_ms").to_list(),
                timezone_name=OPERATING_WINDOW_TIMEZONE,
            )
        )
    metadata["support_level_counts"] = _normalize_support_level_summary(frame)
    metadata["label_columns"] = (
        {
            "split": "split",
            "y_cls": "y_cls",
            "y_reg": "y_reg",
            "source_y_cls_column": "y_cls",
            "source_y_reg_column": "y_reg",
        }
        if prefix == "panel"
        else {}
    )
    metadata["available_markets"] = frame_markets
    renamed: list[pl.Expr] = [pl.col("market"), pl.col("ts_ms")]
    if prefix == "panel":
        renamed.extend(
            [
                pl.col("split"),
                pl.col("y_cls"),
                pl.col("y_reg"),
            ]
        )
    renamed.extend(_support_level_indicator_columns(frame, prefix=prefix))
    for column in frame.columns:
        if column in {"market", "ts_ms", "split", "y_cls", "y_reg", "support_level"}:
            continue
        dtype = frame.schema.get(column)
        if dtype is None:
            continue
        if dtype.is_numeric():
            renamed.append(pl.col(column).cast(pl.Float64).alias(f"{prefix}_{column}"))
    return frame.select(renamed), metadata


def _resolve_fusion_monotone_sign(feature_name: str) -> int:
    name = str(feature_name).strip().lower()
    if not name:
        return 0
    if "present" in name or "support_" in name or "regime_embedding" in name:
        return 0
    negative_tokens = (
        "uncertainty",
        "expected_es",
        "adverse_excursion",
        "score_std",
    )
    if any(token in name for token in negative_tokens):
        return -1
    positive_tokens = (
        "final_rank_score",
        "score_mean",
        "directional_probability",
        "micro_alpha",
        "return_quantile",
        "final_expected_return",
        "final_tradability",
        "final_alpha_lcb",
    )
    if any(token in name for token in positive_tokens):
        return 1
    return 0


def _build_fusion_numeric_feature_contract(merged: pl.DataFrame) -> tuple[tuple[str, ...], tuple[int, ...], dict[str, Any]]:
    excluded = {"market", "ts_ms", "split", "y_cls", "y_reg", "y_es_proxy", "y_tradability_target"}
    feature_names: list[str] = []
    excluded_non_numeric: list[str] = []
    monotone_signs: list[int] = []
    for column, dtype in merged.schema.items():
        if column in excluded:
            continue
        if not dtype.is_numeric():
            excluded_non_numeric.append(column)
            continue
        feature_names.append(column)
        monotone_signs.append(_resolve_fusion_monotone_sign(column))
    return tuple(feature_names), tuple(monotone_signs), {
        "excluded_non_numeric_columns": excluded_non_numeric,
        "feature_columns": list(feature_names),
        "monotone_sign_map": {name: sign for name, sign in zip(feature_names, monotone_signs, strict=False)},
    }


def _build_runtime_coverage_summary(merged: pl.DataFrame) -> dict[str, Any]:
    total_rows = int(merged.height)
    experts: dict[str, Any] = {}
    for expert_name in ("panel", "sequence", "lob", "tradability"):
        present_column = f"{expert_name}_present"
        present_rows = total_rows
        if present_column in merged.columns:
            present_rows = int(
                merged.filter(pl.col(present_column).cast(pl.Float64).fill_null(0.0) >= 0.5).height
            )
        missing_rows = max(total_rows - present_rows, 0)
        experts[expert_name] = {
            "present_rows": present_rows,
            "missing_rows": missing_rows,
            "coverage_ratio": float(present_rows / total_rows) if total_rows > 0 else 0.0,
            "required_full_window": expert_name in {"sequence", "lob", "tradability"},
        }
    return {
        "policy": "auxiliary_experts_full_window_required",
        "total_panel_anchor_rows": total_rows,
        "experts": experts,
    }


def _normalize_market_list(values: list[str] | tuple[str, ...] | None) -> list[str]:
    return [str(item).strip().upper() for item in (values or []) if str(item).strip()]


def _panel_order_preserving_intersection(markets_by_expert: dict[str, list[str]]) -> list[str]:
    ordered_source = list(markets_by_expert.get("panel") or [])
    if not ordered_source:
        for values in markets_by_expert.values():
            if values:
                ordered_source = list(values)
                break
    if not ordered_source:
        return []
    intersection = set(ordered_source)
    for values in markets_by_expert.values():
        if not values:
            intersection = set()
            break
        intersection &= set(values)
    return [market for market in ordered_source if market in intersection]


def _build_common_runtime_universe_id(*, snapshot_id: str, start: str, end: str, markets: list[str]) -> str:
    seed = "|".join([snapshot_id, start, end, ",".join(markets)])
    return f"common_runtime_universe_{hashlib.sha256(seed.encode('utf-8')).hexdigest()[:12]}"


def _runtime_window_gap_error(expert_name: str) -> str:
    expert = str(expert_name).strip().lower()
    if expert == "panel":
        return "FUSION_RUNTIME_INPUT_WINDOW_GAP:PANEL_RUNTIME_WINDOW_GAP"
    if expert == "sequence":
        return "FUSION_RUNTIME_INPUT_WINDOW_GAP:SEQUENCE_RUNTIME_WINDOW_GAP"
    if expert == "lob":
        return "FUSION_RUNTIME_INPUT_WINDOW_GAP:LOB_RUNTIME_WINDOW_GAP"
    if expert == "tradability":
        return "FUSION_RUNTIME_INPUT_WINDOW_GAP:TRADABILITY_RUNTIME_WINDOW_GAP"
    return "FUSION_RUNTIME_INPUT_WINDOW_GAP"


def _runtime_dates_allow_single_trailing_gap(*, expected_dates: list[str], expert_dates: list[str]) -> bool:
    expected = [str(item).strip() for item in expected_dates if str(item).strip()]
    observed = [str(item).strip() for item in expert_dates if str(item).strip()]
    if not expected or not observed:
        return False
    missing = [day for day in expected if day not in set(observed)]
    if not missing:
        return True
    return len(missing) == 1 and missing[0] == expected[-1]


def _resolve_fusion_support_weight(merged: pl.DataFrame) -> np.ndarray:
    sequence_support = (
        merged.get_column("sequence_support_score").to_numpy().astype(np.float64, copy=False)
        if "sequence_support_score" in merged.columns
        else np.full(merged.height, 2.0, dtype=np.float64)
    )
    lob_support = (
        merged.get_column("lob_support_score").to_numpy().astype(np.float64, copy=False)
        if "lob_support_score" in merged.columns
        else np.full(merged.height, 2.0, dtype=np.float64)
    )
    support_score = np.minimum(sequence_support, lob_support)
    return np.clip(np.maximum(support_score, 1.0) / 2.0, 0.5, 1.0)


def _resolve_regime_feature_indices(feature_names: tuple[str, ...]) -> tuple[int, ...]:
    indices = [
        idx
        for idx, name in enumerate(feature_names)
        if str(name).startswith("sequence_regime_embedding_")
    ]
    if indices:
        return tuple(indices)
    return tuple(
        idx for idx, name in enumerate(feature_names) if "regime_embedding" in str(name)
    )


def _fit_regime_centroids(
    *,
    x_train: np.ndarray,
    regime_feature_indices: tuple[int, ...],
    seed: int,
) -> tuple[np.ndarray, np.ndarray]:
    if x_train.shape[0] <= 0:
        raise ValueError("regime_moe requires non-empty training rows")
    if len(regime_feature_indices) <= 0:
        return np.zeros(x_train.shape[0], dtype=np.int64), np.zeros((1, 0), dtype=np.float64)
    regime = np.asarray(x_train[:, list(regime_feature_indices)], dtype=np.float64)
    if regime.shape[0] < 24 or np.allclose(regime, regime[0]):
        return np.zeros(regime.shape[0], dtype=np.int64), np.mean(regime, axis=0, keepdims=True)
    from sklearn.cluster import KMeans

    cluster_count = min(3, max(2, min(regime.shape[0], 3)))
    kmeans = KMeans(n_clusters=cluster_count, n_init=10, random_state=int(seed))
    assignments = np.asarray(kmeans.fit_predict(regime), dtype=np.int64)
    return assignments, np.asarray(kmeans.cluster_centers_, dtype=np.float64)


def _fit_regime_moe_binary_head(
    *,
    x_train: np.ndarray,
    y_train: np.ndarray,
    sample_weight: np.ndarray,
    seed: int,
    regime_feature_indices: tuple[int, ...],
) -> _RegimeMoEBinaryHead:
    assignments, centroids = _fit_regime_centroids(
        x_train=x_train,
        regime_feature_indices=regime_feature_indices,
        seed=seed,
    )
    cluster_count = int(max(assignments.max(initial=0) + 1, 1))
    models: list[Any] = []
    for cluster_idx in range(cluster_count):
        mask = assignments == int(cluster_idx)
        local_x = x_train[mask] if int(np.sum(mask)) >= 8 else x_train
        local_y = y_train[mask] if int(np.sum(mask)) >= 8 else y_train
        local_w = sample_weight[mask] if int(np.sum(mask)) >= 8 else sample_weight
        models.append(
            _fit_binary_head(
                local_x,
                local_y,
                stacker_family="linear",
                seed=seed + cluster_idx,
                monotone_signs=tuple(0 for _ in range(x_train.shape[1])),
                sample_weight=local_w,
            )
        )
    return _RegimeMoEBinaryHead(
        centroids=np.asarray(centroids, dtype=np.float64),
        feature_indices=tuple(regime_feature_indices),
        models=tuple(models),
    )


def _fit_regime_moe_reg_head(
    *,
    x_train: np.ndarray,
    y_train: np.ndarray,
    sample_weight: np.ndarray,
    seed: int,
    regime_feature_indices: tuple[int, ...],
) -> _RegimeMoERegressionHead:
    assignments, centroids = _fit_regime_centroids(
        x_train=x_train,
        regime_feature_indices=regime_feature_indices,
        seed=seed,
    )
    cluster_count = int(max(assignments.max(initial=0) + 1, 1))
    models: list[Any] = []
    for cluster_idx in range(cluster_count):
        mask = assignments == int(cluster_idx)
        local_x = x_train[mask] if int(np.sum(mask)) >= 8 else x_train
        local_y = y_train[mask] if int(np.sum(mask)) >= 8 else y_train
        local_w = sample_weight[mask] if int(np.sum(mask)) >= 8 else sample_weight
        models.append(
            _fit_reg_head(
                local_x,
                local_y,
                stacker_family="linear",
                seed=seed + cluster_idx,
                monotone_signs=tuple(0 for _ in range(x_train.shape[1])),
                sample_weight=local_w,
            )
        )
    return _RegimeMoERegressionHead(
        centroids=np.asarray(centroids, dtype=np.float64),
        feature_indices=tuple(regime_feature_indices),
        models=tuple(models),
    )


def _load_and_merge_expert_tables(options: TrainV5FusionOptions) -> _FusionInputBundle:
    panel, panel_meta = _load_expert_table(options.panel_input_path, prefix="panel")
    sequence, sequence_meta = _load_expert_table(options.sequence_input_path, prefix="sequence")
    lob, lob_meta = _load_expert_table(options.lob_input_path, prefix="lob")
    if options.tradability_input_path is None:
        raise ValueError("v5_fusion requires explicit tradability expert input")
    tradability, tradability_meta = _load_expert_table(options.tradability_input_path, prefix="tradability")
    if panel.height <= 0:
        raise ValueError("fusion panel anchor has no rows")
    snapshot_ids = {
        str(panel_meta.get("data_platform_ready_snapshot_id") or "").strip(),
        str(sequence_meta.get("data_platform_ready_snapshot_id") or "").strip(),
        str(lob_meta.get("data_platform_ready_snapshot_id") or "").strip(),
        str(tradability_meta.get("data_platform_ready_snapshot_id") or "").strip(),
    }
    if "" in snapshot_ids or len(snapshot_ids) != 1:
        raise ValueError("fusion inputs must come from the same non-empty data_platform_ready_snapshot_id")
    merged = panel.join(sequence, on=["market", "ts_ms"], how="left", coalesce=True)
    merged = merged.join(lob, on=["market", "ts_ms"], how="left", coalesce=True)
    merged = merged.join(tradability, on=["market", "ts_ms"], how="left", coalesce=True)
    merged = merged.with_columns(
        pl.col("panel_final_rank_score").is_not_null().cast(pl.Float64).alias("panel_present"),
        pl.col("sequence_directional_probability_primary").is_not_null().cast(pl.Float64).alias("sequence_present"),
        pl.col("lob_micro_alpha_30s").is_not_null().cast(pl.Float64).alias("lob_present"),
        pl.col("tradability_tradability_prob").is_not_null().cast(pl.Float64).alias("tradability_present"),
    )
    expert_value_columns = [
        name
        for name, dtype in merged.schema.items()
        if (name.startswith("panel_") or name.startswith("sequence_") or name.startswith("lob_") or name.startswith("tradability_"))
        and dtype.is_numeric()
    ]
    if expert_value_columns:
        merged = merged.with_columns([pl.col(name).fill_null(0.0) for name in expert_value_columns])
    merged = merged.filter(pl.col("split").is_not_null() & pl.col("y_cls").is_not_null() & pl.col("y_reg").is_not_null())
    merged = merged.with_columns(
        pl.when(pl.col("y_reg") < 0.0).then(pl.col("y_reg").abs()).otherwise(0.0).alias("y_es_proxy"),
        (
            (pl.col("y_reg") > 0.0)
            & (
                pl.col("y_reg").abs()
                >= pl.when(pl.col("y_reg") < 0.0).then(pl.col("y_reg").abs()).otherwise(0.0)
            )
        )
        .cast(pl.Int64)
        .alias("y_tradability_target"),
    )
    feature_names, monotone_signs, feature_contract = _build_fusion_numeric_feature_contract(merged)
    input_contract = {
        "policy": "v5_fusion_input_contract_v1",
        "keys": ["market", "ts_ms", "split"],
        "snapshot_id": str(next(iter(snapshot_ids))),
        "label_anchor": "panel",
        "label_contract_source": "train_v5_panel_ensemble",
        "panel_label_columns": dict(panel_meta.get("label_columns") or {}),
        "auxiliary_experts": ["sequence", "lob", "tradability"],
        "target_alignment_policy": "panel_anchor_only",
        "runtime_coverage_policy": "auxiliary_experts_full_window_required",
        "runtime_coverage_summary": {},
        "inputs": {
            "panel": panel_meta,
            "sequence": sequence_meta,
            "lob": lob_meta,
            "tradability": tradability_meta,
        },
        "feature_contract": feature_contract,
        "rows_after_merge": int(merged.height),
    }
    return _FusionInputBundle(
        merged=merged.sort(["ts_ms", "market"]),
        input_contract=input_contract,
        feature_names=feature_names,
        monotone_signs=monotone_signs,
    )

def _runtime_fusion_input_options(options: TrainV5FusionOptions) -> TrainV5FusionOptions:
    return TrainV5FusionOptions(
        panel_input_path=Path(str(options.panel_runtime_input_path or options.panel_input_path)),
        sequence_input_path=Path(str(options.sequence_runtime_input_path or options.sequence_input_path)),
        lob_input_path=Path(str(options.lob_runtime_input_path or options.lob_input_path)),
        tradability_input_path=(
            Path(str(options.tradability_runtime_input_path or options.tradability_input_path))
            if (options.tradability_runtime_input_path is not None or options.tradability_input_path is not None)
            else None
        ),
        panel_runtime_input_path=options.panel_runtime_input_path,
        sequence_runtime_input_path=options.sequence_runtime_input_path,
        lob_runtime_input_path=options.lob_runtime_input_path,
        tradability_runtime_input_path=options.tradability_runtime_input_path,
        registry_root=options.registry_root,
        logs_root=options.logs_root,
        model_family=options.model_family,
        quote=options.quote,
        start=str(options.runtime_start or options.start),
        end=str(options.runtime_end or options.end),
        runtime_start=options.runtime_start,
        runtime_end=options.runtime_end,
        seed=options.seed,
        stacker_family=options.stacker_family,
        run_scope=options.run_scope,
    )


def _prepare_fusion_input_bundle(options: TrainV5FusionOptions) -> _FusionInputBundle:
    input_bundle = _load_and_merge_expert_tables(options)
    merged = input_bundle.merged
    start_ts_ms = _parse_date_to_ts_ms(options.start)
    end_ts_ms = _parse_date_to_ts_ms(options.end, end_of_day=True)
    if start_ts_ms is not None:
        merged = merged.filter(pl.col("ts_ms") >= int(start_ts_ms))
    if end_ts_ms is not None:
        merged = merged.filter(pl.col("ts_ms") <= int(end_ts_ms))
    if merged.height <= 0:
        raise ValueError("fusion inputs have no rows in the requested range")
    input_contract = dict(input_bundle.input_contract)
    input_contract["rows_after_date_filter"] = int(merged.height)
    return _FusionInputBundle(
        merged=merged,
        input_contract=input_contract,
        feature_names=input_bundle.feature_names,
        monotone_signs=input_bundle.monotone_signs,
    )


def _prepare_fusion_runtime_input_bundle(options: TrainV5FusionOptions) -> _FusionInputBundle:
    runtime_options = _runtime_fusion_input_options(options)
    input_bundle = _load_and_merge_expert_tables(runtime_options)
    merged = input_bundle.merged
    input_contract = dict(input_bundle.input_contract)
    input_metadata = {key: dict(value or {}) for key, value in (input_contract.get("inputs") or {}).items()}
    explicit_runtime_requested = any(
        path is not None
        for path in (
            options.panel_runtime_input_path,
            options.sequence_runtime_input_path,
            options.lob_runtime_input_path,
            options.tradability_runtime_input_path,
        )
    ) or (
        (options.runtime_start is not None and str(options.runtime_start).strip() != str(options.start).strip())
        or (options.runtime_end is not None and str(options.runtime_end).strip() != str(options.end).strip())
    )
    start_ts_ms = _parse_date_to_ts_ms(runtime_options.start)
    end_ts_ms = _parse_date_to_ts_ms(runtime_options.end, end_of_day=True)
    markets_by_expert = {
        key: _normalize_market_list(
            list((payload.get("selected_markets") or payload.get("requested_selected_markets") or payload.get("available_markets") or []))
        )
        for key, payload in input_metadata.items()
    }
    common_runtime_markets = _panel_order_preserving_intersection(markets_by_expert)
    common_runtime_universe_id = _build_common_runtime_universe_id(
        snapshot_id=str(input_contract.get("snapshot_id") or ""),
        start=str(runtime_options.start),
        end=str(runtime_options.end),
        markets=common_runtime_markets,
    )
    input_contract["common_runtime_universe_policy"] = "panel_order_preserving_intersection"
    input_contract["common_runtime_universe_id"] = common_runtime_universe_id
    input_contract["common_runtime_markets"] = list(common_runtime_markets)
    if explicit_runtime_requested and not common_runtime_markets:
        raise ValueError("COMMON_RUNTIME_UNIVERSE_EMPTY")
    expected_runtime_dates = operating_date_range(str(runtime_options.start), str(runtime_options.end))
    for expert_name in ("panel", "sequence", "lob", "tradability"):
        payload = dict(input_metadata.get(expert_name) or {})
        expert_start = int(payload.get("coverage_start_ts_ms", 0) or 0)
        expert_end = int(payload.get("coverage_end_ts_ms", 0) or 0)
        expert_dates = list(payload.get("coverage_dates") or [])
        expert_timezone = str(payload.get("window_timezone") or OPERATING_WINDOW_TIMEZONE).strip() or OPERATING_WINDOW_TIMEZONE
        if explicit_runtime_requested:
            if expert_timezone != OPERATING_WINDOW_TIMEZONE:
                raise ValueError(_runtime_window_gap_error(expert_name))
            if not _runtime_dates_allow_single_trailing_gap(expected_dates=expected_runtime_dates, expert_dates=expert_dates):
                raise ValueError(_runtime_window_gap_error(expert_name))
            if start_ts_ms is not None and expert_start <= 0:
                raise ValueError(_runtime_window_gap_error(expert_name))
            if end_ts_ms is not None and expert_end <= 0:
                raise ValueError(_runtime_window_gap_error(expert_name))
    if start_ts_ms is not None:
        merged = merged.filter(pl.col("ts_ms") >= int(start_ts_ms))
    if end_ts_ms is not None:
        merged = merged.filter(pl.col("ts_ms") <= int(end_ts_ms))
    if merged.height <= 0:
        raise ValueError("FUSION_RUNTIME_INPUT_WINDOW_EMPTY")
    coverage_start = int(merged.get_column("ts_ms").min()) if merged.height > 0 else None
    coverage_end = int(merged.get_column("ts_ms").max()) if merged.height > 0 else None
    input_contract["runtime_window"] = {
        "start": runtime_options.start,
        "end": runtime_options.end,
        "start_ts_ms": start_ts_ms,
        "end_ts_ms": end_ts_ms,
    }
    input_contract["coverage_start_ts_ms"] = coverage_start
    input_contract["coverage_end_ts_ms"] = coverage_end
    input_contract.update(
        build_ts_date_coverage_payload(
            merged.get_column("ts_ms").to_list(),
            timezone_name=OPERATING_WINDOW_TIMEZONE,
        )
    )
    input_contract["runtime_rows_after_date_filter"] = int(merged.height)
    runtime_coverage_summary = _build_runtime_coverage_summary(merged)
    runtime_coverage_summary["common_runtime_market_count"] = len(common_runtime_markets)
    runtime_coverage_summary["common_runtime_markets"] = list(common_runtime_markets)
    input_contract["runtime_coverage_policy"] = "auxiliary_experts_full_window_required"
    input_contract["runtime_coverage_summary"] = runtime_coverage_summary
    if explicit_runtime_requested:
        expert_coverage = dict(runtime_coverage_summary.get("experts") or {})
        if int(((expert_coverage.get("sequence") or {}).get("missing_rows") or 0)) > 0:
            raise ValueError("FUSION_RUNTIME_SEQUENCE_COVERAGE_GAP")
        if int(((expert_coverage.get("lob") or {}).get("missing_rows") or 0)) > 0:
            raise ValueError("FUSION_RUNTIME_LOB_COVERAGE_GAP")
        if int(((expert_coverage.get("tradability") or {}).get("missing_rows") or 0)) > 0:
            raise ValueError("FUSION_RUNTIME_TRADABILITY_COVERAGE_GAP")
    return _FusionInputBundle(
        merged=merged,
        input_contract=input_contract,
        feature_names=input_bundle.feature_names,
        monotone_signs=input_bundle.monotone_signs,
    )


def _build_fusion_runtime_recommendations(*, options: TrainV5FusionOptions, input_contract: dict[str, Any]) -> dict[str, Any]:
    upstream_inputs = dict(input_contract.get("inputs") or {})
    upstream_runtime_context: dict[str, Any] = {}
    for key in ("panel", "sequence", "lob", "tradability"):
        payload = dict(upstream_inputs.get(key) or {})
        upstream_runtime_context[key] = dict(payload.get("runtime_recommendations") or {})
    panel_runtime_context = dict(upstream_runtime_context.get("panel") or {})
    sequence_runtime_context = dict(upstream_runtime_context.get("sequence") or {})
    lob_runtime_context = dict(upstream_runtime_context.get("lob") or {})
    inherited_exit = dict(panel_runtime_context.get("exit") or {})
    inherited_execution = dict(panel_runtime_context.get("execution") or {})
    inherited_risk_control = dict(panel_runtime_context.get("risk_control") or {})
    inherited_trade_action = dict(panel_runtime_context.get("trade_action") or {})
    if not inherited_trade_action:
        inherited_trade_action = {
            "status": "ready",
            "policy": "fusion_advisory_trade_action_backfill_v1",
            "trade_action_role": "advisory_only_v1",
            "state_feature_names": [],
        }
    missing_docs = [
        name
        for name, payload in {
            "exit": inherited_exit,
            "execution": inherited_execution,
            "risk_control": inherited_risk_control,
            "trade_action": inherited_trade_action,
        }.items()
        if not payload
    ]
    if missing_docs:
        raise ValueError(f"FUSION_RUNTIME_RECOMMENDATION_TOP_LEVEL_MISSING:{','.join(missing_docs)}")
    fusion_owned_mode = "fusion_owned_panel_seeded"
    return annotate_v5_runtime_recommendations({
        "status": "fusion_runtime_ready",
        "policy": "v5_fusion_runtime_recommendations_v1",
        "source_family": "train_v5_fusion",
        "contract_owner_family": "train_v5_fusion",
        "contract_seed_family": "train_v5_panel_ensemble",
        "entry_boundary_enabled": True,
        "exit": {
            **inherited_exit,
            "runtime_source_family": "train_v5_fusion",
            "runtime_source_mode": fusion_owned_mode,
            "contract_owner_family": "train_v5_fusion",
            "contract_seed_family": "train_v5_panel_ensemble",
        },
        "execution": {
            **inherited_execution,
            "runtime_source_family": "train_v5_fusion",
            "runtime_source_mode": fusion_owned_mode,
            "contract_owner_family": "train_v5_fusion",
            "contract_seed_family": "train_v5_panel_ensemble",
        },
        "risk_control": {
            **inherited_risk_control,
            "runtime_source_family": "train_v5_fusion",
            "runtime_source_mode": fusion_owned_mode,
            "contract_owner_family": "train_v5_fusion",
            "contract_seed_family": "train_v5_panel_ensemble",
        },
        "trade_action": {
            **inherited_trade_action,
            "runtime_source_family": "train_v5_fusion",
            "runtime_source_mode": fusion_owned_mode,
            "contract_owner_family": "train_v5_fusion",
            "contract_seed_family": "train_v5_panel_ensemble",
        },
        "upstream_experts": {
            key: {
                "run_id": str((upstream_inputs.get(key) or {}).get("run_id") or "").strip(),
                "model_family": str((upstream_inputs.get(key) or {}).get("model_family") or "").strip(),
                "data_platform_ready_snapshot_id": str(
                    (upstream_inputs.get(key) or {}).get("data_platform_ready_snapshot_id") or ""
                ).strip(),
            }
            for key in ("panel", "sequence", "lob")
        },
        "upstream_runtime_context": upstream_runtime_context,
        "sequence_variant_name": str(
            (upstream_inputs.get("sequence") or {}).get("sequence_variant_name")
            or sequence_runtime_context.get("sequence_variant_name")
            or ""
        ).strip(),
        "lob_variant_name": str(
            (upstream_inputs.get("lob") or {}).get("lob_variant_name")
            or lob_runtime_context.get("lob_variant_name")
            or ""
        ).strip(),
        "fusion_variant_name": str(options.stacker_family or "").strip(),
        "sequence_pretrain_method": str(
            (upstream_inputs.get("sequence") or {}).get("sequence_pretrain_method")
            or sequence_runtime_context.get("sequence_pretrain_method")
            or ""
        ).strip(),
        "sequence_pretrain_ready": bool(
            (upstream_inputs.get("sequence") or {}).get("sequence_pretrain_ready")
            or sequence_runtime_context.get("sequence_pretrain_ready", False)
        ),
        "sequence_pretrain_status": str(sequence_runtime_context.get("sequence_pretrain_status") or "").strip(),
        "sequence_pretrain_objective": str(sequence_runtime_context.get("sequence_pretrain_objective") or "").strip(),
        "sequence_pretrain_best_epoch": int(
            (upstream_inputs.get("sequence") or {}).get("sequence_pretrain_best_epoch")
            or sequence_runtime_context.get("sequence_pretrain_best_epoch")
            or 0
        ),
        "sequence_pretrain_encoder_present": bool(
            (upstream_inputs.get("sequence") or {}).get("sequence_pretrain_encoder_present")
            or sequence_runtime_context.get("sequence_pretrain_encoder_present", False)
        ),
        "sequence_pretrain_contract_path": str(
            (upstream_inputs.get("sequence") or {}).get("sequence_pretrain_contract_path")
            or ""
        ).strip(),
        "sequence_pretrain_report_path": str(
            (upstream_inputs.get("sequence") or {}).get("sequence_pretrain_report_path")
            or ""
        ).strip(),
    })    


def _build_runtime_viability_report(
    *,
    run_id: str,
    model_family: str,
    predictor: V5FusionEstimator,
    runtime_x: np.ndarray,
    runtime_markets: np.ndarray,
    runtime_ts_ms: np.ndarray,
    runtime_input_contract: dict[str, Any],
    entry_boundary: dict[str, Any],
) -> dict[str, Any]:
    contract = predictor.predict_panel_contract(runtime_x)
    final_expected_return = np.asarray(contract["final_expected_return"], dtype=np.float64)
    final_expected_es = np.asarray(contract["final_expected_es"], dtype=np.float64)
    final_tradability = np.asarray(contract["final_tradability"], dtype=np.float64)
    final_uncertainty = np.asarray(contract["final_uncertainty"], dtype=np.float64)
    final_alpha_lcb = np.asarray(contract["final_alpha_lcb"], dtype=np.float64)
    alpha_lcb_floor = float(entry_boundary.get("alpha_lcb_floor") or 0.0)

    rows_total = int(final_alpha_lcb.shape[0])
    alpha_lcb_positive_count = int(np.sum(final_alpha_lcb > 0.0))
    rows_above_alpha_floor = int(np.sum(final_alpha_lcb > alpha_lcb_floor))
    expected_return_positive_count = int(np.sum(final_expected_return > 0.0))
    mean_final_expected_return = float(np.mean(final_expected_return)) if rows_total > 0 else 0.0
    mean_final_expected_es = float(np.mean(final_expected_es)) if rows_total > 0 else 0.0
    mean_final_uncertainty = float(np.mean(final_uncertainty)) if rows_total > 0 else 0.0
    mean_final_alpha_lcb = float(np.mean(final_alpha_lcb)) if rows_total > 0 else 0.0

    entry_gate_allowed_count = 0
    reason_counts: dict[str, int] = {}
    sample_rows: list[dict[str, Any]] = []
    for idx in range(rows_total):
        boundary_decision = evaluate_entry_boundary(
            row={
                "final_rank_score": float(contract["final_rank_score"][idx]),
                "final_expected_return": float(final_expected_return[idx]),
                "final_expected_es": float(final_expected_es[idx]),
                "final_tradability": float(final_tradability[idx]),
                "final_uncertainty": float(final_uncertainty[idx]),
                "final_alpha_lcb": float(final_alpha_lcb[idx]),
            },
            contract=entry_boundary,
        )
        gate = resolve_v5_entry_gate(
            market=str(runtime_markets[idx]),
            final_expected_return=float(final_expected_return[idx]),
            final_expected_es=float(final_expected_es[idx]),
            final_tradability=float(final_tradability[idx]),
            final_uncertainty=float(final_uncertainty[idx]),
            final_alpha_lcb=float(final_alpha_lcb[idx]),
            entry_boundary_decision=boundary_decision,
            expected_net_edge_bps=float(final_expected_return[idx]) * 10_000.0,
        )
        if bool(gate.get("allowed", False)):
            entry_gate_allowed_count += 1
        else:
            code = str((gate.get("reason_codes") or ["ENTRY_GATE_BLOCKED"])[0]).strip() or "ENTRY_GATE_BLOCKED"
            reason_counts[code] = int(reason_counts.get(code, 0)) + 1
        if idx < 5:
            sample_rows.append(
                {
                    "market": str(runtime_markets[idx]),
                    "ts_ms": int(runtime_ts_ms[idx]),
                    "final_expected_return": float(final_expected_return[idx]),
                    "final_expected_es": float(final_expected_es[idx]),
                    "final_tradability": float(final_tradability[idx]),
                    "final_uncertainty": float(final_uncertainty[idx]),
                    "final_alpha_lcb": float(final_alpha_lcb[idx]),
                    "expected_net_edge_bps": float(final_expected_return[idx]) * 10_000.0,
                    "alpha_lcb_floor": alpha_lcb_floor,
                    "gate_allowed": bool(gate.get("allowed", False)),
                    "gate_reason_codes": list(gate.get("reason_codes") or []),
                }
            )
    rows_total_float = float(rows_total) if rows_total > 0 else 1.0
    top_entry_gate_reason_codes = [
        {"reason_code": reason_code, "count": int(count)}
        for reason_code, count in sorted(reason_counts.items(), key=lambda item: (-int(item[1]), str(item[0])))
    ][:5]
    return {
        "policy": "v5_runtime_viability_report_v1",
        "run_id": run_id,
        "model_family": model_family,
        "generation_window": dict(runtime_input_contract.get("runtime_window") or {}),
        "common_runtime_universe_id": str(runtime_input_contract.get("common_runtime_universe_id") or "").strip(),
        "alpha_lcb_floor": alpha_lcb_floor,
        "runtime_rows_total": rows_total,
        "mean_final_expected_return": mean_final_expected_return,
        "mean_final_expected_es": mean_final_expected_es,
        "mean_final_uncertainty": mean_final_uncertainty,
        "mean_final_alpha_lcb": mean_final_alpha_lcb,
        "alpha_lcb_positive_count": alpha_lcb_positive_count,
        "rows_above_alpha_floor": rows_above_alpha_floor,
        "rows_above_alpha_floor_ratio": float(rows_above_alpha_floor / rows_total_float),
        "expected_return_positive_count": expected_return_positive_count,
        "entry_gate_allowed_count": entry_gate_allowed_count,
        "entry_gate_allowed_ratio": float(entry_gate_allowed_count / rows_total_float),
        "estimated_intent_candidate_count": entry_gate_allowed_count,
        "pass": bool(rows_above_alpha_floor > 0 and entry_gate_allowed_count > 0),
        "primary_reason_code": (
            "FUSION_RUNTIME_ALPHA_LCB_ZERO_VIABILITY"
            if rows_above_alpha_floor <= 0
            else ("FUSION_RUNTIME_ENTRY_GATE_ZERO_VIABILITY" if entry_gate_allowed_count <= 0 else "PASS")
        ),
        "entry_gate_reason_counts": reason_counts,
        "top_entry_gate_reason_codes": top_entry_gate_reason_codes,
        "sample_rows": sample_rows,
    }


def _build_runtime_viability_summary(report: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(report or {})
    return {
        "alpha_lcb_floor": payload.get("alpha_lcb_floor"),
        "runtime_rows_total": payload.get("runtime_rows_total"),
        "mean_final_expected_return": payload.get("mean_final_expected_return"),
        "mean_final_expected_es": payload.get("mean_final_expected_es"),
        "mean_final_uncertainty": payload.get("mean_final_uncertainty"),
        "mean_final_alpha_lcb": payload.get("mean_final_alpha_lcb"),
        "alpha_lcb_positive_count": payload.get("alpha_lcb_positive_count"),
        "rows_above_alpha_floor": payload.get("rows_above_alpha_floor"),
        "rows_above_alpha_floor_ratio": payload.get("rows_above_alpha_floor_ratio"),
        "expected_return_positive_count": payload.get("expected_return_positive_count"),
        "entry_gate_allowed_count": payload.get("entry_gate_allowed_count"),
        "entry_gate_allowed_ratio": payload.get("entry_gate_allowed_ratio"),
        "estimated_intent_candidate_count": payload.get("estimated_intent_candidate_count"),
        "primary_reason_code": payload.get("primary_reason_code"),
        "top_entry_gate_reason_codes": list(payload.get("top_entry_gate_reason_codes") or []),
        "sample_rows": list(payload.get("sample_rows") or [])[:5],
    }


def _is_dependency_expert_only_runtime_doc(
    payload: dict[str, Any] | None,
    *,
    parent_context: dict[str, Any] | None = None,
) -> bool:
    doc = dict(payload or {})
    parent = dict(parent_context or {})
    mode_candidates = (
        doc.get("mode"),
        doc.get("operating_mode"),
        doc.get("tail_mode"),
        parent.get("tail_mode"),
    )
    return bool(doc.get("dependency_expert_only", False)) or bool(parent.get("dependency_expert_only", False)) or any(
        str(value or "").strip().lower() == "dependency_expert_only"
        for value in mode_candidates
    )


def _build_fusion_execution_contract_readiness(
    *,
    payload: dict[str, Any] | None,
    parent_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    doc = dict(payload or {})
    reason_codes: list[str] = []
    seed_dependency_expert_only = _is_dependency_expert_only_runtime_doc(doc, parent_context=parent_context)
    stages = [
        dict(item)
        for item in (doc.get("stages") or [])
        if isinstance(item, dict) and str(item.get("stage", "")).strip()
    ]
    supported_stage_count = sum(1 for item in stages if bool(item.get("supported", False)))
    if seed_dependency_expert_only:
        reason_codes.append("PANEL_DEPENDENCY_EXPERT_ONLY_RUNTIME_SEED")
    if not doc:
        reason_codes.append("EXECUTION_DOC_MISSING")
    elif supported_stage_count <= 0:
        reason_codes.append("EXECUTION_FRONTIER_MISSING")
    return {
        "component": "execution",
        "required": True,
        "ready": len(reason_codes) == 0,
        "reason_codes": reason_codes,
        "seed_dependency_expert_only": seed_dependency_expert_only,
        "stage_order": [str(value).strip().upper() for value in (doc.get("stage_order") or []) if str(value).strip()],
        "supported_stage_count": int(supported_stage_count),
        "recommended_price_mode": str(doc.get("recommended_price_mode") or "").strip().upper(),
        "frontier_policy": str(doc.get("policy") or "").strip(),
        "frontier_summary": dict(doc.get("frontier_summary") or {}),
    }


def _build_fusion_exit_contract_readiness(
    *,
    payload: dict[str, Any] | None,
    parent_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    doc = normalize_runtime_exit_payload(dict(payload or {})) if isinstance(payload, dict) else {}
    reason_codes: list[str] = []
    seed_dependency_expert_only = _is_dependency_expert_only_runtime_doc(doc, parent_context=parent_context)
    contract_status = str(doc.get("contract_status") or "").strip().lower()
    if seed_dependency_expert_only:
        reason_codes.append("PANEL_DEPENDENCY_EXPERT_ONLY_RUNTIME_SEED")
    if not doc:
        reason_codes.append("EXIT_DOC_MISSING")
    elif contract_status == "invalid":
        reason_codes.append("EXIT_DOC_INVALID")
    elif str(doc.get("recommended_exit_mode") or "").strip().lower() not in {"hold", "risk"}:
        reason_codes.append("EXIT_MODE_MISSING")
    return {
        "component": "exit",
        "required": True,
        "ready": len(reason_codes) == 0,
        "reason_codes": reason_codes,
        "seed_dependency_expert_only": seed_dependency_expert_only,
        "contract_status": str(doc.get("contract_status") or "").strip(),
        "contract_issues": list(doc.get("contract_issues") or []),
        "recommended_exit_mode": str(doc.get("recommended_exit_mode") or "").strip().lower(),
        "recommended_hold_bars": int(doc.get("recommended_hold_bars") or 0) if doc.get("recommended_hold_bars") not in (None, "") else 0,
        "family_compare_status": str(doc.get("family_compare_status") or "").strip(),
    }


def _build_fusion_trade_action_contract_readiness(
    *,
    payload: dict[str, Any] | None,
    parent_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    doc = normalize_trade_action_policy(dict(payload or {})) if isinstance(payload, dict) else {}
    reason_codes: list[str] = []
    seed_dependency_expert_only = _is_dependency_expert_only_runtime_doc(doc, parent_context=parent_context)
    status = str(doc.get("status") or "").strip().lower()
    if seed_dependency_expert_only:
        reason_codes.append("PANEL_DEPENDENCY_EXPERT_ONLY_RUNTIME_SEED")
    if not doc:
        reason_codes.append("TRADE_ACTION_DOC_MISSING")
    elif status not in {"ready"}:
        reason_codes.append("TRADE_ACTION_NOT_READY")
    return {
        "component": "trade_action",
        "required": False,
        "ready": len(reason_codes) == 0,
        "reason_codes": reason_codes,
        "seed_dependency_expert_only": seed_dependency_expert_only,
        "status": str(doc.get("status") or "").strip(),
        "policy": str(doc.get("policy") or "").strip(),
        "runtime_decision_source": str(doc.get("runtime_decision_source") or "").strip(),
    }


def _build_fusion_risk_control_contract_readiness(
    *,
    payload: dict[str, Any] | None,
    parent_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    doc = normalize_execution_risk_control_payload(dict(payload or {})) if isinstance(payload, dict) else {}
    reason_codes: list[str] = []
    seed_dependency_expert_only = _is_dependency_expert_only_runtime_doc(doc, parent_context=parent_context)
    contract_status = str(doc.get("contract_status") or "").strip().lower()
    status = str(doc.get("status") or "").strip().lower()
    if seed_dependency_expert_only:
        reason_codes.append("PANEL_DEPENDENCY_EXPERT_ONLY_RUNTIME_SEED")
    if not doc:
        reason_codes.append("RISK_CONTROL_DOC_MISSING")
    elif contract_status == "invalid":
        reason_codes.append("RISK_CONTROL_DOC_INVALID")
    elif status in {"", "missing", "skipped"}:
        reason_codes.append("RISK_CONTROL_NOT_READY")
    return {
        "component": "risk_control",
        "required": False,
        "ready": len(reason_codes) == 0,
        "reason_codes": reason_codes,
        "seed_dependency_expert_only": seed_dependency_expert_only,
        "contract_status": str(doc.get("contract_status") or "").strip(),
        "contract_issues": list(doc.get("contract_issues") or []),
        "status": str(doc.get("status") or "").strip(),
        "operating_mode": str(doc.get("operating_mode") or "").strip(),
        "live_gate_enabled": bool((doc.get("live_gate") or {}).get("enabled", False)),
    }


def _build_runtime_deploy_contract_readiness_summary(report: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(report or {})
    component_readiness = dict(payload.get("component_readiness") or {})
    return {
        "evaluation_contract_id": str(payload.get("evaluation_contract_id") or "").strip(),
        "evaluation_contract_role": str(payload.get("evaluation_contract_role") or "").strip(),
        "decision_contract_version": str(payload.get("decision_contract_version") or "").strip(),
        "pass": bool(payload.get("pass", False)),
        "primary_reason_code": str(payload.get("primary_reason_code") or "").strip(),
        "required_components": list(payload.get("required_components") or []),
        "advisory_components": list(payload.get("advisory_components") or []),
        "component_readiness": {
            name: {
                "required": bool(dict(doc or {}).get("required", False)),
                "ready": bool(dict(doc or {}).get("ready", False)),
                "reason_codes": list(dict(doc or {}).get("reason_codes") or []),
            }
            for name, doc in component_readiness.items()
        },
    }


def _build_runtime_deploy_contract_readiness(
    *,
    runtime_recommendations: dict[str, Any],
    input_contract: dict[str, Any],
) -> dict[str, Any]:
    evaluation_contract = load_evaluation_contract(contract_id="runtime_deploy_contract_v1")
    if evaluation_contract is None:
        return {
            "policy": "v5_runtime_deploy_contract_readiness_v1",
            "evaluation_contract_id": "runtime_deploy_contract_v1",
            "evaluation_contract_role": "deploy_runtime",
            "decision_contract_version": str(runtime_recommendations.get("decision_contract_version") or "").strip(),
            "pass": False,
            "primary_reason_code": "RUNTIME_DEPLOY_EVALUATION_CONTRACT_UNRESOLVED",
            "required_components": [],
            "advisory_components": [],
            "component_readiness": {},
        }
    upstream_inputs = dict(input_contract.get("inputs") or {})
    panel_runtime_context = dict(((upstream_inputs.get("panel") or {}).get("runtime_recommendations")) or {})
    decision_contract_version = str(runtime_recommendations.get("decision_contract_version") or "").strip()
    required_components: list[str] = []
    advisory_components: list[str] = []
    if bool(evaluation_contract.use_learned_exit_mode) or bool(evaluation_contract.use_learned_hold_bars) or bool(
        evaluation_contract.use_learned_risk_recommendations
    ):
        required_components.append("exit")
    if bool(evaluation_contract.use_learned_execution_recommendations):
        required_components.append("execution")
    if bool(evaluation_contract.use_trade_level_action_policy):
        if decision_contract_version == V5_POST_MODEL_CONTRACT_VERSION:
            advisory_components.extend(["trade_action", "risk_control"])
        else:
            required_components.extend(["trade_action", "risk_control"])
    component_readiness = {
        "exit": _build_fusion_exit_contract_readiness(
            payload=runtime_recommendations.get("exit"),
            parent_context=panel_runtime_context,
        ),
        "execution": _build_fusion_execution_contract_readiness(
            payload=runtime_recommendations.get("execution"),
            parent_context=panel_runtime_context,
        ),
        "trade_action": _build_fusion_trade_action_contract_readiness(
            payload=runtime_recommendations.get("trade_action"),
            parent_context=panel_runtime_context,
        ),
        "risk_control": _build_fusion_risk_control_contract_readiness(
            payload=runtime_recommendations.get("risk_control"),
            parent_context=panel_runtime_context,
        ),
    }
    primary_reason_code = "PASS"
    for component_name in required_components:
        component = dict(component_readiness.get(component_name) or {})
        if not bool(component.get("ready", False)):
            reason_codes = [str(item).strip() for item in (component.get("reason_codes") or []) if str(item).strip()]
            primary_reason_code = (
                f"FUSION_RUNTIME_DEPLOY_CONTRACT_{str(component_name).strip().upper()}_NOT_READY"
                if not reason_codes
                else str(reason_codes[0])
            )
            break
    return {
        "policy": "v5_runtime_deploy_contract_readiness_v1",
        "evaluation_contract_id": str(evaluation_contract.contract_id).strip(),
        "evaluation_contract_role": str(evaluation_contract.contract_role).strip(),
        "decision_contract_version": decision_contract_version,
        "required_components": list(dict.fromkeys(required_components)),
        "advisory_components": list(dict.fromkeys(advisory_components)),
        "component_readiness": component_readiness,
        "pass": primary_reason_code == "PASS",
        "primary_reason_code": primary_reason_code,
    }


def _build_v5_fusion_tail_context(
    *,
    run_id: str,
    options: TrainV5FusionOptions,
    data_platform_ready_snapshot_id: str | None,
    runtime_dataset_root: Path,
    input_contract: dict[str, Any],
) -> dict[str, Any]:
    runtime_start = str(options.runtime_start or options.start).strip()
    runtime_end = str(options.runtime_end or options.end).strip()
    return build_v5_expert_tail_context(
        run_id=run_id,
        trainer_name="v5_fusion",
        model_family=options.model_family,
        data_platform_ready_snapshot_id=data_platform_ready_snapshot_id,
        dataset_root=runtime_dataset_root,
        source_dataset_root=Path("fusion_oof_tables"),
        runtime_dataset_root=runtime_dataset_root,
        selected_markets=tuple(),
        support_level_counts={},
        run_scope=options.run_scope,
    ) | {
        "panel_run_id": str(((input_contract.get("inputs") or {}).get("panel") or {}).get("run_id") or "").strip(),
        "sequence_run_id": str(((input_contract.get("inputs") or {}).get("sequence") or {}).get("run_id") or "").strip(),
        "lob_run_id": str(((input_contract.get("inputs") or {}).get("lob") or {}).get("run_id") or "").strip(),
        "tradability_run_id": str(((input_contract.get("inputs") or {}).get("tradability") or {}).get("run_id") or "").strip(),
        "panel_input_path": str(options.panel_input_path),
        "sequence_input_path": str(options.sequence_input_path),
        "lob_input_path": str(options.lob_input_path),
        "tradability_input_path": str(options.tradability_input_path),
        "panel_runtime_input_path": str(options.panel_runtime_input_path or options.panel_input_path),
        "sequence_runtime_input_path": str(options.sequence_runtime_input_path or options.sequence_input_path),
        "lob_runtime_input_path": str(options.lob_runtime_input_path or options.lob_input_path),
        "tradability_runtime_input_path": str(options.tradability_runtime_input_path or options.tradability_input_path),
        "runtime_start": runtime_start,
        "runtime_end": runtime_end,
        "runtime_window_id": f"{runtime_start}__{runtime_end}",
    }


def _resolve_existing_v5_fusion_tail_artifacts(*, run_dir: Path, tail_context: dict[str, Any]) -> dict[str, Any]:
    payload = resolve_existing_v5_expert_tail_artifacts(run_dir=run_dir, tail_context=tail_context)
    artifacts = dict(payload.get("artifacts") or {})
    artifacts["entry_boundary_contract_path"] = {
        "path": _fusion_entry_boundary_path(run_dir),
        "exists": _fusion_entry_boundary_path(run_dir).exists(),
        "payload": load_json(_fusion_entry_boundary_path(run_dir)) if _fusion_entry_boundary_path(run_dir).exists() else None,
    }
    artifacts["fusion_input_contract_path"] = {
        "path": _fusion_input_contract_path(run_dir),
        "exists": _fusion_input_contract_path(run_dir).exists(),
        "payload": load_json(_fusion_input_contract_path(run_dir)) if _fusion_input_contract_path(run_dir).exists() else None,
    }
    artifacts["fusion_runtime_input_contract_path"] = {
        "path": _fusion_runtime_input_contract_path(run_dir),
        "exists": _fusion_runtime_input_contract_path(run_dir).exists(),
        "payload": load_json(_fusion_runtime_input_contract_path(run_dir)) if _fusion_runtime_input_contract_path(run_dir).exists() else None,
    }
    payload["artifacts"] = artifacts
    return payload


def _fusion_tail_stage_reusable(*, existing_tail_artifacts: dict[str, Any], stage_name: str) -> bool:
    if stage_name == "entry_boundary":
        if not bool(existing_tail_artifacts.get("context_matches", False)):
            return False
        artifacts = dict(existing_tail_artifacts.get("artifacts") or {})
        return bool((artifacts.get("entry_boundary_contract_path") or {}).get("payload"))
    if stage_name == "fusion_input_contract":
        if not bool(existing_tail_artifacts.get("context_matches", False)):
            return False
        artifacts = dict(existing_tail_artifacts.get("artifacts") or {})
        return bool((artifacts.get("fusion_input_contract_path") or {}).get("payload"))
    if stage_name == "fusion_runtime_input_contract":
        if not bool(existing_tail_artifacts.get("context_matches", False)):
            return False
        artifacts = dict(existing_tail_artifacts.get("artifacts") or {})
        return bool((artifacts.get("fusion_runtime_input_contract_path") or {}).get("payload"))
    return False


def _run_fusion_tail(
    *,
    run_dir: Path,
    run_id: str,
    options: TrainV5FusionOptions,
    metrics: dict[str, Any],
    valid_metrics: dict[str, Any],
    test_metrics: dict[str, Any],
    data_platform_ready_snapshot_id: str | None,
    runtime_dataset_root: Path,
    input_contract: dict[str, Any],
    runtime_input_contract: dict[str, Any],
    runtime_recommendations: dict[str, Any],
    promotion_payload: dict[str, Any],
    entry_boundary: dict[str, Any],
    runtime_viability_report_path: Path,
    runtime_deploy_contract_readiness_path: Path,
    resumed: bool,
) -> tuple[dict[str, Any], Path]:
    tail_started_at = time.time()
    tail_context = _build_v5_fusion_tail_context(
        run_id=run_id,
        options=options,
        data_platform_ready_snapshot_id=data_platform_ready_snapshot_id,
        runtime_dataset_root=runtime_dataset_root,
        input_contract=input_contract,
    )
    existing_tail_artifacts = _resolve_existing_v5_fusion_tail_artifacts(
        run_dir=run_dir,
        tail_context=tail_context,
    )
    _fusion_input_contract_path(run_dir).write_text(
        json.dumps(dict(input_contract), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    _fusion_runtime_input_contract_path(run_dir).write_text(
        json.dumps(dict(runtime_input_contract), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    Path(expert_tail_context_path(run_dir)).write_text(
        json.dumps(dict(tail_context), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    update_artifact_status(run_dir, tail_context_written=True)
    if not _fusion_tail_stage_reusable(existing_tail_artifacts=existing_tail_artifacts, stage_name="entry_boundary"):
        _fusion_entry_boundary_path(run_dir).write_text(
            json.dumps(dict(entry_boundary), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    runtime_artifacts = run_or_reuse_v5_runtime_governance_artifacts(
        run_dir=run_dir,
        trainer_name="v5_fusion",
        model_family=options.model_family,
        run_scope=options.run_scope,
        metrics=metrics,
        runtime_recommendations=runtime_recommendations,
        promotion=promotion_payload,
        trainer_research_reasons=["FUSION_RUNTIME_CONTRACT_READY"],
        tail_context=tail_context,
        existing_tail_artifacts=existing_tail_artifacts,
        resumed=resumed,
    )
    report_path = finalize_v5_expert_family_run(
        run_dir=run_dir,
        run_id=run_id,
        registry_root=options.registry_root,
        model_family=options.model_family,
        logs_root=options.logs_root,
        report_name="train_v5_fusion_report.json",
        report_payload={
            "run_id": run_id,
            "status": "candidate",
            "leaderboard_row": load_json(run_dir / "leaderboard_row.json"),
            "valid_metrics": valid_metrics,
            "test_metrics": test_metrics,
            "runtime_dataset_root": str(runtime_dataset_root),
            "entry_boundary_contract_path": str(_fusion_entry_boundary_path(run_dir)),
            "fusion_input_contract_path": str(_fusion_input_contract_path(run_dir)),
            "fusion_runtime_input_contract_path": str(_fusion_runtime_input_contract_path(run_dir)),
            "runtime_viability_report_path": str(runtime_viability_report_path),
            "runtime_viability_pass": bool(runtime_recommendations.get("runtime_viability_pass", False)),
            "runtime_viability_summary": dict(runtime_recommendations.get("runtime_viability_summary") or {}),
            "runtime_deploy_contract_readiness_path": str(runtime_deploy_contract_readiness_path),
            "runtime_deploy_contract_ready": bool(runtime_recommendations.get("runtime_deploy_contract_ready", False)),
            "runtime_deploy_contract_summary": dict(runtime_recommendations.get("runtime_deploy_contract_summary") or {}),
        },
        data_platform_ready_snapshot_id=data_platform_ready_snapshot_id,
        resumed=resumed,
        tail_started_at=tail_started_at,
        publish_family_latest=bool(
            runtime_recommendations.get("runtime_viability_pass", False)
            and runtime_recommendations.get("runtime_deploy_contract_ready", False)
        ),
        publish_global_latest=(str(options.run_scope).strip().lower() == "scheduled_daily"),
    )
    return runtime_artifacts, report_path

def train_and_register_v5_fusion(options: TrainV5FusionOptions) -> TrainV5FusionResult:
    stacker_family = str(options.stacker_family).strip().lower()
    if stacker_family not in VALID_FUSION_STACKERS:
        raise ValueError(f"stacker_family must be one of: {', '.join(VALID_FUSION_STACKERS)}")

    run_id = make_run_id(seed=options.seed)
    train_input_bundle = _prepare_fusion_input_bundle(options)
    merged = train_input_bundle.merged
    if merged.height <= 0:
        raise ValueError("fusion inputs produced no aligned rows")
    start_ts_ms = _parse_date_to_ts_ms(options.start)
    end_ts_ms = _parse_date_to_ts_ms(options.end, end_of_day=True)

    feature_names = tuple(train_input_bundle.feature_names)
    if not feature_names:
        raise ValueError("fusion inputs produced no numeric feature columns")
    monotone_signs = tuple(int(item) for item in train_input_bundle.monotone_signs)
    x = merged.select(list(feature_names)).to_numpy().astype(np.float64, copy=False)
    y_cls = merged.get_column("y_cls").to_numpy().astype(np.int64, copy=False)
    y_reg = merged.get_column("y_reg").to_numpy().astype(np.float64, copy=False)
    y_es = merged.get_column("y_es_proxy").to_numpy().astype(np.float64, copy=False)
    y_tradability = merged.get_column("y_tradability_target").to_numpy().astype(np.int64, copy=False)
    ts_ms = merged.get_column("ts_ms").to_numpy().astype(np.int64, copy=False)
    markets = merged.get_column("market").to_numpy()

    if "split" in merged.columns:
        labels = merged.get_column("split").to_numpy()
        masks = split_masks(labels)
        split_info = type("SplitInfo", (), {"valid_start_ts": int(ts_ms[masks["valid"]][0]), "test_start_ts": int(ts_ms[masks["test"]][0]), "counts": {k: int(np.sum(v)) for k, v in masks.items()}})()
    else:
        labels, split_info = compute_time_splits(ts_ms, train_ratio=0.6, valid_ratio=0.2, test_ratio=0.2, embargo_bars=0, interval_ms=60_000)
        masks = split_masks(labels)

    train_mask = masks["train"]
    valid_mask = masks["valid"]
    test_mask = masks["test"]
    if not np.any(train_mask) or not np.any(valid_mask) or not np.any(test_mask):
        raise ValueError("fusion trainer requires non-empty train/valid/test rows")
    weight_components = resolve_v5_domain_weighting_components(
        markets=markets,
        ts_ms=ts_ms,
        split_labels=labels,
        base_sample_weight=np.ones(merged.height, dtype=np.float64),
        data_quality_weight=np.ones(merged.height, dtype=np.float64),
        support_weight=_resolve_fusion_support_weight(merged),
    )
    sample_weight = np.asarray(weight_components["final_sample_weight"], dtype=np.float64)
    regime_feature_indices = _resolve_regime_feature_indices(feature_names)
    if stacker_family == "regime_moe" and len(regime_feature_indices) <= 0:
        raise ValueError("regime_moe requires sequence regime embedding features")

    if stacker_family == "regime_moe":
        score_model = _fit_regime_moe_binary_head(
            x_train=x[train_mask],
            y_train=y_cls[train_mask],
            sample_weight=sample_weight[train_mask],
            seed=options.seed,
            regime_feature_indices=regime_feature_indices,
        )
        return_model = _fit_regime_moe_reg_head(
            x_train=x[train_mask],
            y_train=y_reg[train_mask],
            sample_weight=sample_weight[train_mask],
            seed=options.seed + 1,
            regime_feature_indices=regime_feature_indices,
        )
        es_model = _fit_regime_moe_reg_head(
            x_train=x[train_mask],
            y_train=y_es[train_mask],
            sample_weight=sample_weight[train_mask],
            seed=options.seed + 2,
            regime_feature_indices=regime_feature_indices,
        )
        tradability_model = _fit_regime_moe_binary_head(
            x_train=x[train_mask],
            y_train=y_tradability[train_mask],
            sample_weight=sample_weight[train_mask],
            seed=options.seed + 3,
            regime_feature_indices=regime_feature_indices,
        )
    else:
        score_model = _fit_binary_head(
            x[train_mask],
            y_cls[train_mask],
            stacker_family=stacker_family,
            seed=options.seed,
            monotone_signs=monotone_signs,
            sample_weight=sample_weight[train_mask],
        )
        return_model = _fit_reg_head(
            x[train_mask],
            y_reg[train_mask],
            stacker_family=stacker_family,
            seed=options.seed + 1,
            monotone_signs=monotone_signs,
            sample_weight=sample_weight[train_mask],
        )
        es_model = _fit_reg_head(
            x[train_mask],
            y_es[train_mask],
            stacker_family=stacker_family,
            seed=options.seed + 2,
            monotone_signs=tuple(-1 if sign == 1 else (1 if sign == -1 else 0) for sign in monotone_signs),
            sample_weight=sample_weight[train_mask],
        )
        tradability_model = _fit_binary_head(
            x[train_mask],
            y_tradability[train_mask],
            stacker_family=stacker_family,
            seed=options.seed + 3,
            monotone_signs=monotone_signs,
            sample_weight=sample_weight[train_mask],
        )

    valid_return_pred = np.asarray(return_model.predict(x[valid_mask]), dtype=np.float64)
    uncertainty_target = np.abs(y_reg[valid_mask] - valid_return_pred)
    uncertainty_model = _fit_reg_head(
        x[valid_mask],
        uncertainty_target,
        stacker_family="linear",
        seed=options.seed + 4,
        monotone_signs=tuple(0 for _ in feature_names),
        sample_weight=sample_weight[valid_mask],
    )

    estimator = V5FusionEstimator(
        score_model=score_model,
        return_model=return_model,
        es_model=es_model,
        tradability_model=tradability_model,
        uncertainty_model=uncertainty_model,
        stacker_family=stacker_family,
        feature_names=feature_names,
        regime_feature_columns=tuple(feature_names[idx] for idx in regime_feature_indices),
        regime_cluster_count=(
            int(len(getattr(score_model, "models", ()) or ()))
            if stacker_family == "regime_moe"
            else 1
        ),
        gating_policy=(
            "sequence_regime_embedding_nearest_centroid_v1"
            if stacker_family == "regime_moe"
            else "single_expert_v1"
        ),
    )
    valid_contract = estimator.predict_panel_contract(x[valid_mask])
    test_contract = estimator.predict_panel_contract(x[test_mask])
    valid_metrics = _evaluate_fusion_split(
        y_cls=y_cls[valid_mask],
        y_reg=y_reg[valid_mask],
        scores=valid_contract["final_rank_score"],
        markets=markets[valid_mask],
        sample_weight=sample_weight[valid_mask],
    )
    test_metrics = _evaluate_fusion_split(
        y_cls=y_cls[test_mask],
        y_reg=y_reg[test_mask],
        scores=test_contract["final_rank_score"],
        markets=markets[test_mask],
        sample_weight=sample_weight[test_mask],
    )
    thresholds = _build_thresholds(
        valid_scores=valid_contract["final_rank_score"],
        y_reg_valid=y_reg[valid_mask],
        fee_bps_est=0.0,
        safety_bps=0.0,
        ev_scan_steps=10,
        ev_min_selected=1,
        sample_weight=sample_weight[valid_mask],
    )
    selection_recommendations, selection_walk_forward = _build_fusion_selection_walk_forward(
        valid_scores=valid_contract["final_rank_score"],
        y_reg_valid=y_reg[valid_mask],
        valid_ts_ms=ts_ms[valid_mask],
        thresholds=thresholds,
    )
    selection_policy = build_selection_policy_from_recommendations(
        selection_recommendations=selection_recommendations,
        fallback_threshold_key="top_5pct",
        score_source="score_mean",
    )
    selection_calibration = _identity_calibration(reason="FUSION_IDENTITY_CALIBRATION")
    entry_boundary = build_risk_calibrated_entry_boundary(
        final_rank_score=valid_contract["final_rank_score"],
        final_expected_return=valid_contract["final_expected_return"],
        final_expected_es=valid_contract["final_expected_es"],
        final_tradability=valid_contract["final_tradability"],
        final_uncertainty=valid_contract["final_uncertainty"],
        final_alpha_lcb=valid_contract["final_alpha_lcb"],
        realized_return=y_reg[valid_mask],
    )

    metrics = {
        "rows": {
            "train": int(np.sum(train_mask)),
            "valid": int(np.sum(valid_mask)),
            "test": int(np.sum(test_mask)),
            "drop": int(np.sum(labels == "drop")),
        },
        "valid_metrics": valid_metrics,
        "champion_metrics": test_metrics,
        "fusion_model": {
            "policy": "v5_fusion_v1",
            "stacker_family": stacker_family,
            "gating_policy": estimator.gating_policy,
            "regime_feature_columns": list(estimator.regime_feature_columns),
            "regime_cluster_count": int(estimator.regime_cluster_count),
            "input_experts": ["panel", "sequence", "lob", "tradability"],
            "outputs": ["final_rank_score", "final_expected_return", "final_expected_es", "final_tradability", "final_uncertainty", "final_alpha_lcb"],
            "feature_columns": list(feature_names),
        },
    }
    leaderboard_row = {
        "run_id": run_id,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "model_family": options.model_family,
        "champion": "fusion_meta_model",
        "champion_backend": stacker_family,
        "test_roc_auc": float((test_metrics.get("classification", {}) or {}).get("roc_auc") or 0.0),
        "test_pr_auc": float((test_metrics.get("classification", {}) or {}).get("pr_auc") or 0.0),
        "test_log_loss": float((test_metrics.get("classification", {}) or {}).get("log_loss") or 0.0),
        "test_brier_score": float((test_metrics.get("classification", {}) or {}).get("brier_score") or 0.0),
        "test_precision_top5": float((((test_metrics.get("trading", {}) or {}).get("top_5pct", {}) or {}).get("precision") or 0.0)),
        "test_ev_net_top5": float((((test_metrics.get("trading", {}) or {}).get("top_5pct", {}) or {}).get("ev_net") or 0.0)),
        "rows_train": int(np.sum(train_mask)),
        "rows_valid": int(np.sum(valid_mask)),
        "rows_test": int(np.sum(test_mask)),
    }

    data_fingerprint = {
        "dataset_root": "fusion_oof_tables",
        "tf": "fusion_expert_oof",
        "quote": options.quote,
        "top_n": 0,
        "start_ts_ms": start_ts_ms,
        "end_ts_ms": end_ts_ms,
        "panel_input_sha256": _sha256_file(options.panel_input_path),
        "sequence_input_sha256": _sha256_file(options.sequence_input_path),
        "lob_input_sha256": _sha256_file(options.lob_input_path),
        "sample_count": int(merged.height),
        "code_version": autobot_version,
        "data_platform_ready_snapshot_id": str(train_input_bundle.input_contract.get("snapshot_id") or "").strip()
        or resolve_ready_snapshot_id(project_root=Path.cwd()),
    }
    model_card = render_model_card(
        run_id=run_id,
        model_family=options.model_family,
        champion="fusion_meta_model",
        metrics=metrics,
        thresholds=thresholds,
        data_fingerprint=data_fingerprint,
    )
    runtime_dataset_root = options.registry_root / options.model_family / run_id / "runtime_feature_dataset"
    train_config = {
        **asdict(options),
        "panel_input_path": str(options.panel_input_path),
        "sequence_input_path": str(options.sequence_input_path),
        "lob_input_path": str(options.lob_input_path),
        "tradability_input_path": str(options.tradability_input_path) if options.tradability_input_path is not None else "",
        "dataset_root": str(runtime_dataset_root),
        "source_dataset_root": "fusion_oof_tables",
        "registry_root": str(options.registry_root),
        "logs_root": str(options.logs_root),
        "trainer": "v5_fusion",
        "feature_columns": list(feature_names),
        "autobot_version": autobot_version,
        "data_platform_ready_snapshot_id": data_fingerprint.get("data_platform_ready_snapshot_id"),
        "fusion_input_contract_path": str(_fusion_input_contract_path(options.registry_root / options.model_family / run_id)),
        "fusion_runtime_input_contract_path": str(_fusion_runtime_input_contract_path(options.registry_root / options.model_family / run_id)),
        "panel_runtime_input_path": str(options.panel_runtime_input_path) if options.panel_runtime_input_path is not None else "",
        "sequence_runtime_input_path": str(options.sequence_runtime_input_path) if options.sequence_runtime_input_path is not None else "",
        "lob_runtime_input_path": str(options.lob_runtime_input_path) if options.lob_runtime_input_path is not None else "",
        "tradability_runtime_input_path": str(options.tradability_runtime_input_path) if options.tradability_runtime_input_path is not None else "",
        "runtime_start": str(options.runtime_start or options.start),
        "runtime_end": str(options.runtime_end or options.end),
        "sequence_backbone_name": str(((train_input_bundle.input_contract.get("inputs") or {}).get("sequence") or {}).get("backbone_family") or ""),
        "lob_backbone_name": str(((train_input_bundle.input_contract.get("inputs") or {}).get("lob") or {}).get("backbone_family") or ""),
        "tradability_source_run_id": str(((train_input_bundle.input_contract.get("inputs") or {}).get("tradability") or {}).get("run_id") or ""),
    }
    runtime_recommendations = _build_fusion_runtime_recommendations(
        options=options,
        input_contract=train_input_bundle.input_contract,
    )
    runtime_recommendations["sequence_backbone_name"] = str(((train_input_bundle.input_contract.get("inputs") or {}).get("sequence") or {}).get("backbone_family") or "")
    runtime_recommendations["lob_backbone_name"] = str(((train_input_bundle.input_contract.get("inputs") or {}).get("lob") or {}).get("backbone_family") or "")
    runtime_recommendations["tradability_source_run_id"] = str(((train_input_bundle.input_contract.get("inputs") or {}).get("tradability") or {}).get("run_id") or "")
    runtime_recommendations["sequence_variant_name"] = str(
        ((train_input_bundle.input_contract.get("inputs") or {}).get("sequence") or {}).get("sequence_variant_name")
        or (((train_input_bundle.input_contract.get("inputs") or {}).get("sequence") or {}).get("runtime_recommendations") or {}).get("sequence_variant_name")
        or ""
    )
    runtime_recommendations["lob_variant_name"] = str(
        ((train_input_bundle.input_contract.get("inputs") or {}).get("lob") or {}).get("lob_variant_name")
        or (((train_input_bundle.input_contract.get("inputs") or {}).get("lob") or {}).get("runtime_recommendations") or {}).get("lob_variant_name")
        or ""
    )
    runtime_recommendations["fusion_variant_name"] = stacker_family
    runtime_recommendations["fusion_offline_winner"] = stacker_family
    runtime_recommendations["fusion_default_eligible_winner"] = stacker_family
    runtime_recommendations["fusion_candidate_default_eligible"] = True
    runtime_recommendations["fusion_evidence_winner"] = stacker_family
    runtime_recommendations["fusion_evidence_reason_code"] = "OFFLINE_SELECTION_ONLY"
    runtime_recommendations["sequence_pretrain_method"] = str(
        ((train_input_bundle.input_contract.get("inputs") or {}).get("sequence") or {}).get("sequence_pretrain_method")
        or (((train_input_bundle.input_contract.get("inputs") or {}).get("sequence") or {}).get("runtime_recommendations") or {}).get("sequence_pretrain_method")
        or ""
    )
    runtime_recommendations["sequence_pretrain_ready"] = bool(
        ((train_input_bundle.input_contract.get("inputs") or {}).get("sequence") or {}).get("sequence_pretrain_ready")
        or (((train_input_bundle.input_contract.get("inputs") or {}).get("sequence") or {}).get("runtime_recommendations") or {}).get("sequence_pretrain_ready", False)
    )
    runtime_recommendations["sequence_pretrain_status"] = str(
        (((train_input_bundle.input_contract.get("inputs") or {}).get("sequence") or {}).get("runtime_recommendations") or {}).get("sequence_pretrain_status")
        or ""
    )
    runtime_recommendations["sequence_pretrain_objective"] = str(
        (((train_input_bundle.input_contract.get("inputs") or {}).get("sequence") or {}).get("runtime_recommendations") or {}).get("sequence_pretrain_objective")
        or ""
    )
    runtime_recommendations["sequence_pretrain_best_epoch"] = int(
        ((train_input_bundle.input_contract.get("inputs") or {}).get("sequence") or {}).get("sequence_pretrain_best_epoch")
        or (((train_input_bundle.input_contract.get("inputs") or {}).get("sequence") or {}).get("runtime_recommendations") or {}).get("sequence_pretrain_best_epoch")
        or 0
    )
    runtime_recommendations["sequence_pretrain_encoder_present"] = bool(
        ((train_input_bundle.input_contract.get("inputs") or {}).get("sequence") or {}).get("sequence_pretrain_encoder_present")
        or (((train_input_bundle.input_contract.get("inputs") or {}).get("sequence") or {}).get("runtime_recommendations") or {}).get("sequence_pretrain_encoder_present", False)
    )
    runtime_recommendations["sequence_pretrain_contract_path"] = str(
        ((train_input_bundle.input_contract.get("inputs") or {}).get("sequence") or {}).get("sequence_pretrain_contract_path")
        or ""
    )
    runtime_recommendations["sequence_pretrain_report_path"] = str(
        ((train_input_bundle.input_contract.get("inputs") or {}).get("sequence") or {}).get("sequence_pretrain_report_path")
        or ""
    )
    runtime_recommendations["domain_weighting_policy"] = str((weight_components.get("domain_details") or {}).get("policy") or "v5_domain_weighting_v1")
    runtime_recommendations["domain_weighting_source_kind"] = str((weight_components.get("domain_details") or {}).get("source_kind") or "regime_inverse_frequency_v1")
    runtime_recommendations["domain_weighting_enabled"] = bool((weight_components.get("domain_details") or {}).get("enabled", False))
    runtime_recommendations["fusion_stacker_family"] = stacker_family
    runtime_recommendations["fusion_gating_policy"] = estimator.gating_policy
    run_dir = save_run(
        RegistrySavePayload(
            registry_root=options.registry_root,
            model_family=options.model_family,
            run_id=run_id,
            model_bundle={"model_type": "v5_fusion", "estimator": estimator},
            metrics=metrics,
            thresholds=thresholds,
            feature_spec={"feature_columns": list(feature_names), "dataset_root": str(runtime_dataset_root)},
            label_spec={"policy": "v5_fusion_label_contract_v1", "primary_target": "y_reg", "auxiliary_targets": ["y_es_proxy", "y_tradability_target"]},
            train_config=train_config,
            data_fingerprint=data_fingerprint,
            leaderboard_row=leaderboard_row,
            model_card_text=model_card,
            selection_recommendations=selection_recommendations,
            selection_policy=selection_policy,
            selection_calibration=selection_calibration,
            runtime_recommendations=runtime_recommendations,
        ),
        publish_pointers=False,
    )
    update_artifact_status(run_dir, status="core_saved", core_saved=True)

    fusion_model_contract_path = run_dir / "fusion_model_contract.json"
    fusion_model_contract_path.write_text(
        json.dumps(
            {
                "policy": "v5_fusion_v1",
                "stacker_family": stacker_family,
                "gating_policy": estimator.gating_policy,
                "regime_feature_columns": list(estimator.regime_feature_columns),
                "regime_cluster_count": int(estimator.regime_cluster_count),
                "input_experts": {
                    "panel": dict((train_input_bundle.input_contract.get("inputs") or {}).get("panel") or {}),
                    "sequence": dict((train_input_bundle.input_contract.get("inputs") or {}).get("sequence") or {}),
                    "lob": dict((train_input_bundle.input_contract.get("inputs") or {}).get("lob") or {}),
                    "tradability": dict((train_input_bundle.input_contract.get("inputs") or {}).get("tradability") or {}),
                },
                "sequence_backbone_name": str(((train_input_bundle.input_contract.get("inputs") or {}).get("sequence") or {}).get("backbone_family") or ""),
                "lob_backbone_name": str(((train_input_bundle.input_contract.get("inputs") or {}).get("lob") or {}).get("backbone_family") or ""),
                "tradability_source_run_id": str(((train_input_bundle.input_contract.get("inputs") or {}).get("tradability") or {}).get("run_id") or ""),
                "feature_columns": list(feature_names),
                "monotone_sign_map": dict(train_input_bundle.input_contract.get("feature_contract", {}).get("monotone_sign_map") or {}),
                "outputs": {
                    "final_rank_score": "final_rank_score",
                    "final_expected_return": "final_expected_return",
                    "final_expected_es": "final_expected_es",
                    "final_tradability": "final_tradability",
                    "final_uncertainty": "final_uncertainty",
                    "final_alpha_lcb": "final_alpha_lcb",
                },
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    predictor_contract_path = run_dir / "predictor_contract.json"
    predictor_contract_path.write_text(
        json.dumps(
            {
                "version": 1,
                "score_mean_field": "score_mean",
                "score_std_field": "final_uncertainty",
                "score_lcb_field": "score_lcb",
                "final_rank_score_field": "final_rank_score",
                "final_expected_return_field": "final_expected_return",
                "final_expected_es_field": "final_expected_es",
                "final_tradability_field": "final_tradability",
                "final_alpha_lcb_field": "final_alpha_lcb",
                "feature_columns": list(feature_names),
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    walk_forward_report_path = run_dir / "walk_forward_report.json"
    walk_forward_report_path.write_text(
        json.dumps(
            {
                "policy": "fusion_holdout_v2",
                "valid_metrics": valid_metrics,
                "test_metrics": test_metrics,
                "selection_walk_forward": selection_walk_forward,
                "selection_recommendations": selection_recommendations,
                "selection_policy": selection_policy,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    promotion_payload = {
        "run_id": run_id,
        "promote": False,
        "status": "candidate",
        "reasons": ["CANDIDATE_ACCEPTANCE_REQUIRED"],
        "checks": {
            "existing_champion_present": False,
            "walk_forward_present": True,
            "walk_forward_windows_run": 1,
            "execution_acceptance_enabled": False,
            "execution_acceptance_present": False,
            "risk_control_required": False,
        },
        "research_acceptance": {
            "walk_forward_summary": {
                "valid_metrics": valid_metrics,
                "test_metrics": test_metrics,
            }
        },
        "data_platform_ready_snapshot_id": data_fingerprint.get("data_platform_ready_snapshot_id"),
    }
    runtime_input_bundle = _prepare_fusion_runtime_input_bundle(options)
    runtime_merged = runtime_input_bundle.merged
    runtime_x = runtime_merged.select(list(runtime_input_bundle.feature_names)).to_numpy().astype(np.float64, copy=False)
    runtime_y_cls = runtime_merged.get_column("y_cls").to_numpy().astype(np.int64, copy=False)
    runtime_y_reg = runtime_merged.get_column("y_reg").to_numpy().astype(np.float64, copy=False)
    runtime_markets = runtime_merged.get_column("market").to_numpy()
    runtime_ts_ms = runtime_merged.get_column("ts_ms").to_numpy().astype(np.int64, copy=False)
    runtime_split_labels = (
        runtime_merged.get_column("split").to_numpy()
        if "split" in runtime_merged.columns
        else np.full(runtime_merged.height, "runtime", dtype=object)
    )
    runtime_weight_components = resolve_v5_domain_weighting_components(
        markets=runtime_markets,
        ts_ms=runtime_ts_ms,
        split_labels=runtime_split_labels,
        base_sample_weight=np.ones(runtime_merged.height, dtype=np.float64),
        data_quality_weight=np.ones(runtime_merged.height, dtype=np.float64),
        support_weight=_resolve_fusion_support_weight(runtime_merged),
    )

    runtime_dataset_written_root = write_runtime_feature_dataset(
        output_root=runtime_dataset_root,
        tf="5m",
        feature_columns=tuple(runtime_input_bundle.feature_names),
        markets=runtime_markets,
        ts_ms=runtime_ts_ms,
        x=runtime_x,
        y_cls=runtime_y_cls,
        y_reg=runtime_y_reg,
        y_rank=runtime_y_reg,
        sample_weight=np.asarray(runtime_weight_components["final_sample_weight"], dtype=np.float64),
    )
    domain_weighting_report_path = write_v5_domain_weighting_report(
        run_dir=run_dir,
        payload=build_v5_domain_weighting_report(
            run_id=run_id,
            trainer_name="v5_fusion",
            model_family=options.model_family,
            component_order=["base_sample_weight", "data_quality_weight", "support_level_weight", "domain_weight"],
            final_sample_weight=np.asarray(weight_components["final_sample_weight"], dtype=np.float64),
            base_sample_weight=np.asarray(weight_components["base_sample_weight"], dtype=np.float64),
            data_quality_weight=np.asarray(weight_components["data_quality_weight"], dtype=np.float64),
            support_weight=np.asarray(weight_components["support_weight"], dtype=np.float64),
            domain_weight=np.asarray(weight_components["domain_weight"], dtype=np.float64),
            domain_details=dict(weight_components["domain_details"] or {}),
        ),
    )
    ood_generalization_report_path = write_ood_generalization_report(
        run_dir=run_dir,
        payload=build_ood_generalization_report(
            run_id=run_id,
            trainer_name="v5_fusion",
            model_family=options.model_family,
            source_kind=str((weight_components.get("domain_details") or {}).get("source_kind") or "regime_inverse_frequency_v1"),
            markets=markets,
            split_labels=labels,
            effective_sample_weight=np.asarray(weight_components["final_sample_weight"], dtype=np.float64),
            invariant_penalty_enabled=False,
            regime_bucket_labels=np.asarray(merged.get_column("market").to_numpy(), dtype=object),
            extra_summary={
                "gating_policy": estimator.gating_policy,
                "regime_cluster_count": int(estimator.regime_cluster_count),
            },
        ),
    )
    runtime_recommendations["ood_status"] = "informative_ready"
    runtime_recommendations["ood_source_kind"] = str((weight_components.get("domain_details") or {}).get("source_kind") or "regime_inverse_frequency_v1")
    runtime_recommendations["ood_penalty_enabled"] = True
    runtime_recommendations["ood_generalization_report_path"] = str(ood_generalization_report_path)
    runtime_input_contract = dict(runtime_input_bundle.input_contract)
    runtime_input_contract["runtime_dataset_root"] = str(runtime_dataset_written_root)
    runtime_input_contract["domain_weighting_report_path"] = str(domain_weighting_report_path)
    runtime_viability_report = _build_runtime_viability_report(
        run_id=run_id,
        model_family=options.model_family,
        predictor=estimator,
        runtime_x=runtime_x,
        runtime_markets=runtime_markets,
        runtime_ts_ms=runtime_ts_ms,
        runtime_input_contract=runtime_input_contract,
        entry_boundary=entry_boundary,
    )
    runtime_viability_report_path = run_dir / "runtime_viability_report.json"
    runtime_viability_report_path.write_text(
        json.dumps(runtime_viability_report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    runtime_deploy_contract_readiness = _build_runtime_deploy_contract_readiness(
        runtime_recommendations=runtime_recommendations,
        input_contract=train_input_bundle.input_contract,
    )
    runtime_deploy_contract_readiness_path = run_dir / "runtime_deploy_contract_readiness.json"
    runtime_deploy_contract_readiness_path.write_text(
        json.dumps(runtime_deploy_contract_readiness, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    runtime_recommendations["runtime_viability_report_path"] = str(runtime_viability_report_path)
    runtime_recommendations["runtime_viability_pass"] = bool(runtime_viability_report.get("pass", False))
    runtime_recommendations["runtime_viability_summary"] = _build_runtime_viability_summary(
        runtime_viability_report
    )
    runtime_recommendations["runtime_deploy_contract_readiness_path"] = str(runtime_deploy_contract_readiness_path)
    runtime_recommendations["runtime_deploy_contract_ready"] = bool(runtime_deploy_contract_readiness.get("pass", False))
    runtime_recommendations["runtime_deploy_contract_summary"] = _build_runtime_deploy_contract_readiness_summary(
        runtime_deploy_contract_readiness
    )
    candidate_default_eligible = bool(runtime_viability_report.get("pass", False)) and bool(
        runtime_deploy_contract_readiness.get("pass", False)
    )
    runtime_recommendations["fusion_candidate_default_eligible"] = candidate_default_eligible
    runtime_recommendations["fusion_default_eligible_winner"] = stacker_family if candidate_default_eligible else "linear"
    runtime_recommendations["fusion_evidence_winner"] = stacker_family if candidate_default_eligible else "linear"
    runtime_recommendations["fusion_evidence_reason_code"] = (
        "RUNTIME_VIABILITY_PASS"
        if candidate_default_eligible
        else (
            str(runtime_viability_report.get("primary_reason_code") or "FUSION_RUNTIME_ALPHA_LCB_ZERO_VIABILITY")
            if not bool(runtime_viability_report.get("pass", False))
            else str(
                runtime_deploy_contract_readiness.get("primary_reason_code")
                or "FUSION_RUNTIME_DEPLOY_CONTRACT_NOT_READY"
            )
        )
    )
    promotion_payload["runtime_viability_report_path"] = str(runtime_viability_report_path)
    promotion_payload["runtime_viability_pass"] = bool(runtime_viability_report.get("pass", False))
    promotion_payload["runtime_viability_summary"] = dict(runtime_recommendations["runtime_viability_summary"])
    promotion_payload["runtime_deploy_contract_readiness_path"] = str(runtime_deploy_contract_readiness_path)
    promotion_payload["runtime_deploy_contract_ready"] = bool(runtime_deploy_contract_readiness.get("pass", False))
    promotion_payload["runtime_deploy_contract_summary"] = dict(
        runtime_recommendations["runtime_deploy_contract_summary"]
    )
    runtime_artifacts, train_report_path = _run_fusion_tail(
        run_dir=run_dir,
        run_id=run_id,
        options=options,
        metrics=metrics,
        valid_metrics=valid_metrics,
        test_metrics=test_metrics,
        data_platform_ready_snapshot_id=data_fingerprint.get("data_platform_ready_snapshot_id"),
        runtime_dataset_root=runtime_dataset_written_root,
        input_contract=train_input_bundle.input_contract | {"feature_contract": train_input_bundle.input_contract.get("feature_contract", {})},
        runtime_input_contract=runtime_input_contract,
        runtime_recommendations=runtime_recommendations,
        promotion_payload=promotion_payload,
        entry_boundary=entry_boundary,
        runtime_viability_report_path=runtime_viability_report_path,
        runtime_deploy_contract_readiness_path=runtime_deploy_contract_readiness_path,
        resumed=False,
    )
    return TrainV5FusionResult(
        run_id=run_id,
        run_dir=run_dir,
        status="candidate",
        leaderboard_row=leaderboard_row,
        metrics=metrics,
        thresholds=thresholds,
        train_report_path=train_report_path,
        promotion_path=runtime_artifacts["promotion_path"],
        walk_forward_report_path=walk_forward_report_path,
        fusion_model_contract_path=fusion_model_contract_path,
        predictor_contract_path=predictor_contract_path,
        entry_boundary_contract_path=_fusion_entry_boundary_path(run_dir),
        runtime_viability_report_path=runtime_viability_report_path,
    )


def _options_from_v5_fusion_train_config(train_config: dict[str, Any]) -> TrainV5FusionOptions:
    base = dict(train_config or {})
    tradability_input_raw = str(base.get("tradability_input_path", "")).strip()
    tradability_runtime_raw = str(base.get("tradability_runtime_input_path", "")).strip()
    return TrainV5FusionOptions(
        panel_input_path=Path(str(base["panel_input_path"])),
        sequence_input_path=Path(str(base["sequence_input_path"])),
        lob_input_path=Path(str(base["lob_input_path"])),
        tradability_input_path=Path(tradability_input_raw) if tradability_input_raw and tradability_input_raw.lower() != "none" else None,
        panel_runtime_input_path=Path(str(base["panel_runtime_input_path"])) if str(base.get("panel_runtime_input_path", "")).strip() else None,
        sequence_runtime_input_path=Path(str(base["sequence_runtime_input_path"])) if str(base.get("sequence_runtime_input_path", "")).strip() else None,
        lob_runtime_input_path=Path(str(base["lob_runtime_input_path"])) if str(base.get("lob_runtime_input_path", "")).strip() else None,
        tradability_runtime_input_path=Path(tradability_runtime_raw) if tradability_runtime_raw and tradability_runtime_raw.lower() != "none" else None,
        registry_root=Path(str(base["registry_root"])),
        logs_root=Path(str(base["logs_root"])),
        model_family=str(base["model_family"]),
        quote=str(base["quote"]),
        start=str(base["start"]),
        end=str(base["end"]),
        runtime_start=(str(base.get("runtime_start", "")).strip() or None),
        runtime_end=(str(base.get("runtime_end", "")).strip() or None),
        seed=int(base["seed"]),
        stacker_family=str(base.get("stacker_family", "linear")),
        run_scope=str(base.get("run_scope", "manual_fusion_expert")),
    )


def resume_v5_fusion_tail(*, run_dir: Path) -> TrainV5FusionResult:
    run_dir = Path(run_dir).resolve()
    if not run_dir.exists():
        raise FileNotFoundError(f"run_dir not found: {run_dir}")
    train_config = load_json(run_dir / "train_config.yaml")
    if not train_config:
        raise FileNotFoundError(f"missing train_config.yaml in {run_dir}")
    options = _options_from_v5_fusion_train_config(train_config)
    model_bundle = load_model_bundle(run_dir)
    estimator = model_bundle.get("estimator") if isinstance(model_bundle, dict) else None
    if estimator is None:
        raise ValueError(f"run_dir does not contain a usable fusion estimator: {run_dir}")
    metrics = load_json(run_dir / "metrics.json")
    thresholds = load_json(run_dir / "thresholds.json")
    leaderboard_row = load_json(run_dir / "leaderboard_row.json")
    walk_forward_report_path = run_dir / "walk_forward_report.json"
    walk_forward_report = load_json(walk_forward_report_path)
    valid_metrics = dict((walk_forward_report.get("valid_metrics") or {}))
    test_metrics = dict((walk_forward_report.get("test_metrics") or {}))
    data_platform_ready_snapshot_id = (
        str(train_config.get("data_platform_ready_snapshot_id") or "").strip()
        or resolve_ready_snapshot_id(project_root=Path.cwd())
    )
    input_bundle = _prepare_fusion_input_bundle(options)
    runtime_input_bundle = _prepare_fusion_runtime_input_bundle(options)
    merged = input_bundle.merged
    x = merged.select(list(input_bundle.feature_names)).to_numpy().astype(np.float64, copy=False)
    if "split" in merged.columns:
        labels = merged.get_column("split").to_numpy()
        masks = split_masks(labels)
    else:
        labels, _ = compute_time_splits(
            merged.get_column("ts_ms").to_numpy().astype(np.int64, copy=False),
            train_ratio=0.6,
            valid_ratio=0.2,
            test_ratio=0.2,
            embargo_bars=0,
            interval_ms=60_000,
        )
        masks = split_masks(labels)
    valid_mask = masks["valid"]
    y_reg = merged.get_column("y_reg").to_numpy().astype(np.float64, copy=False)
    valid_contract = estimator.predict_panel_contract(x[valid_mask])
    entry_boundary = build_risk_calibrated_entry_boundary(
        final_rank_score=valid_contract["final_rank_score"],
        final_expected_return=valid_contract["final_expected_return"],
        final_expected_es=valid_contract["final_expected_es"],
        final_tradability=valid_contract["final_tradability"],
        final_uncertainty=valid_contract["final_uncertainty"],
        final_alpha_lcb=valid_contract["final_alpha_lcb"],
        realized_return=y_reg[valid_mask],
    )
    runtime_dataset_root = Path(str(train_config.get("dataset_root") or run_dir / "runtime_feature_dataset"))
    runtime_merged = runtime_input_bundle.merged
    runtime_x = runtime_merged.select(list(runtime_input_bundle.feature_names)).to_numpy().astype(np.float64, copy=False)
    runtime_weight_components = resolve_v5_domain_weighting_components(
        markets=runtime_merged.get_column("market").to_numpy(),
        ts_ms=runtime_merged.get_column("ts_ms").to_numpy().astype(np.int64, copy=False),
        split_labels=(
            runtime_merged.get_column("split").to_numpy()
            if "split" in runtime_merged.columns
            else np.full(runtime_merged.height, "runtime", dtype=object)
        ),
        base_sample_weight=np.ones(runtime_merged.height, dtype=np.float64),
        data_quality_weight=np.ones(runtime_merged.height, dtype=np.float64),
        support_weight=_resolve_fusion_support_weight(runtime_merged),
    )
    runtime_dataset_written_root = write_runtime_feature_dataset(
        output_root=runtime_dataset_root,
        tf="5m",
        feature_columns=tuple(runtime_input_bundle.feature_names),
        markets=runtime_merged.get_column("market").to_numpy(),
        ts_ms=runtime_merged.get_column("ts_ms").to_numpy().astype(np.int64, copy=False),
        x=runtime_x,
        y_cls=runtime_merged.get_column("y_cls").to_numpy().astype(np.int64, copy=False),
        y_reg=runtime_merged.get_column("y_reg").to_numpy().astype(np.float64, copy=False),
        y_rank=runtime_merged.get_column("y_reg").to_numpy().astype(np.float64, copy=False),
        sample_weight=np.asarray(runtime_weight_components["final_sample_weight"], dtype=np.float64),
    )
    runtime_recommendations = _build_fusion_runtime_recommendations(
        options=options,
        input_contract=input_bundle.input_contract,
    )
    promotion_payload = load_json(run_dir / "promotion_decision.json") or {
        "run_id": run_dir.name,
        "promote": False,
        "status": "candidate",
        "reasons": ["CANDIDATE_ACCEPTANCE_REQUIRED"],
    }
    runtime_input_contract = dict(runtime_input_bundle.input_contract)
    runtime_input_contract["runtime_dataset_root"] = str(runtime_dataset_written_root)
    runtime_input_contract["domain_weighting_report_path"] = str(run_dir / "domain_weighting_report.json")
    runtime_viability_report = _build_runtime_viability_report(
        run_id=run_dir.name,
        model_family=options.model_family,
        predictor=estimator,
        runtime_x=runtime_x,
        runtime_markets=runtime_merged.get_column("market").to_numpy(),
        runtime_ts_ms=runtime_merged.get_column("ts_ms").to_numpy().astype(np.int64, copy=False),
        runtime_input_contract=runtime_input_contract,
        entry_boundary=entry_boundary,
    )
    runtime_viability_report_path = run_dir / "runtime_viability_report.json"
    runtime_viability_report_path.write_text(
        json.dumps(runtime_viability_report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    runtime_deploy_contract_readiness = _build_runtime_deploy_contract_readiness(
        runtime_recommendations=runtime_recommendations,
        input_contract=input_bundle.input_contract,
    )
    runtime_deploy_contract_readiness_path = run_dir / "runtime_deploy_contract_readiness.json"
    runtime_deploy_contract_readiness_path.write_text(
        json.dumps(runtime_deploy_contract_readiness, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    runtime_recommendations["runtime_viability_report_path"] = str(runtime_viability_report_path)
    runtime_recommendations["runtime_viability_pass"] = bool(runtime_viability_report.get("pass", False))
    runtime_recommendations["runtime_viability_summary"] = _build_runtime_viability_summary(
        runtime_viability_report
    )
    runtime_recommendations["runtime_deploy_contract_readiness_path"] = str(runtime_deploy_contract_readiness_path)
    runtime_recommendations["runtime_deploy_contract_ready"] = bool(runtime_deploy_contract_readiness.get("pass", False))
    runtime_recommendations["runtime_deploy_contract_summary"] = _build_runtime_deploy_contract_readiness_summary(
        runtime_deploy_contract_readiness
    )
    candidate_default_eligible = bool(runtime_viability_report.get("pass", False)) and bool(
        runtime_deploy_contract_readiness.get("pass", False)
    )
    runtime_recommendations["fusion_candidate_default_eligible"] = candidate_default_eligible
    runtime_recommendations["fusion_default_eligible_winner"] = options.stacker_family if candidate_default_eligible else "linear"
    runtime_recommendations["fusion_evidence_winner"] = options.stacker_family if candidate_default_eligible else "linear"
    runtime_recommendations["fusion_evidence_reason_code"] = (
        "RUNTIME_VIABILITY_PASS"
        if candidate_default_eligible
        else (
            str(runtime_viability_report.get("primary_reason_code") or "FUSION_RUNTIME_ALPHA_LCB_ZERO_VIABILITY")
            if not bool(runtime_viability_report.get("pass", False))
            else str(
                runtime_deploy_contract_readiness.get("primary_reason_code")
                or "FUSION_RUNTIME_DEPLOY_CONTRACT_NOT_READY"
            )
        )
    )
    promotion_payload["runtime_viability_report_path"] = str(runtime_viability_report_path)
    promotion_payload["runtime_viability_pass"] = bool(runtime_viability_report.get("pass", False))
    promotion_payload["runtime_viability_summary"] = dict(runtime_recommendations["runtime_viability_summary"])
    promotion_payload["runtime_deploy_contract_readiness_path"] = str(runtime_deploy_contract_readiness_path)
    promotion_payload["runtime_deploy_contract_ready"] = bool(runtime_deploy_contract_readiness.get("pass", False))
    promotion_payload["runtime_deploy_contract_summary"] = dict(
        runtime_recommendations["runtime_deploy_contract_summary"]
    )
    runtime_artifacts, train_report_path = _run_fusion_tail(
        run_dir=run_dir,
        run_id=run_dir.name,
        options=options,
        metrics=metrics,
        valid_metrics=valid_metrics,
        test_metrics=test_metrics,
        data_platform_ready_snapshot_id=data_platform_ready_snapshot_id,
        runtime_dataset_root=runtime_dataset_written_root,
        input_contract=input_bundle.input_contract,
        runtime_input_contract=runtime_input_contract,
        runtime_recommendations=runtime_recommendations,
        promotion_payload=promotion_payload,
        entry_boundary=entry_boundary,
        runtime_viability_report_path=runtime_viability_report_path,
        runtime_deploy_contract_readiness_path=runtime_deploy_contract_readiness_path,
        resumed=True,
    )
    return TrainV5FusionResult(
        run_id=run_dir.name,
        run_dir=run_dir,
        status="candidate",
        leaderboard_row=leaderboard_row,
        metrics=metrics,
        thresholds=thresholds,
        train_report_path=train_report_path,
        promotion_path=runtime_artifacts["promotion_path"],
        walk_forward_report_path=walk_forward_report_path,
        fusion_model_contract_path=run_dir / "fusion_model_contract.json",
        predictor_contract_path=run_dir / "predictor_contract.json",
        entry_boundary_contract_path=_fusion_entry_boundary_path(run_dir),
        runtime_viability_report_path=runtime_viability_report_path,
    )


def _fit_binary_head(
    x: np.ndarray,
    y: np.ndarray,
    *,
    stacker_family: str,
    seed: int,
    monotone_signs: tuple[int, ...],
    sample_weight: np.ndarray | None = None,
) -> Any:
    if stacker_family == "linear":
        from sklearn.linear_model import LogisticRegression

        model = LogisticRegression(max_iter=1000, random_state=int(seed))
        model.fit(x, y, sample_weight=np.asarray(sample_weight, dtype=np.float64) if sample_weight is not None else None)
        return model
    import xgboost as xgb

    constraints = "(" + ",".join(str(int(item)) for item in monotone_signs) + ")"
    model = xgb.XGBClassifier(
        objective="binary:logistic",
        tree_method="hist",
        n_estimators=128,
        learning_rate=0.05,
        max_depth=3,
        monotone_constraints=constraints,
        random_state=int(seed),
        nthread=1,
        eval_metric="logloss",
    )
    model.fit(x, y, sample_weight=np.asarray(sample_weight, dtype=np.float64) if sample_weight is not None else None)
    return model


def _fit_reg_head(
    x: np.ndarray,
    y: np.ndarray,
    *,
    stacker_family: str,
    seed: int,
    monotone_signs: tuple[int, ...],
    sample_weight: np.ndarray | None = None,
) -> Any:
    if stacker_family == "linear":
        from sklearn.linear_model import Ridge

        model = Ridge(alpha=1.0, random_state=int(seed))
        model.fit(x, y, sample_weight=np.asarray(sample_weight, dtype=np.float64) if sample_weight is not None else None)
        return model
    import xgboost as xgb

    constraints = "(" + ",".join(str(int(item)) for item in monotone_signs) + ")"
    model = xgb.XGBRegressor(
        objective="reg:squarederror",
        tree_method="hist",
        n_estimators=128,
        learning_rate=0.05,
        max_depth=3,
        monotone_constraints=constraints,
        random_state=int(seed),
        nthread=1,
    )
    model.fit(x, y, sample_weight=np.asarray(sample_weight, dtype=np.float64) if sample_weight is not None else None)
    return model


def _evaluate_fusion_split(
    *,
    y_cls: np.ndarray,
    y_reg: np.ndarray,
    scores: np.ndarray,
    markets: np.ndarray,
    sample_weight: np.ndarray | None = None,
) -> dict[str, Any]:
    resolved_weight = np.asarray(sample_weight, dtype=np.float64) if sample_weight is not None else None
    cls = classification_metrics(y_cls, scores, sample_weight=resolved_weight)
    trading = trading_metrics(y_cls, y_reg, scores, fee_bps_est=0.0, safety_bps=0.0, sample_weight=resolved_weight)
    per_market = grouped_trading_metrics(
        markets=markets,
        y_true=y_cls,
        y_reg=y_reg,
        scores=scores,
        fee_bps_est=0.0,
        safety_bps=0.0,
        sample_weight=resolved_weight,
    )
    return {
        "rows": int(y_cls.size),
        "classification": cls,
        "trading": trading,
        "per_market": per_market,
    }
