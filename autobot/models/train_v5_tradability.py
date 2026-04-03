"""Tradability expert trainer on top of expert predictions and private execution labels."""

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
from autobot.ops.data_platform_snapshot import resolve_ready_snapshot_id
from autobot.strategy.v5_post_model_contract import annotate_v5_runtime_recommendations

from .metrics import classification_metrics
from .model_card import render_model_card
from .registry import RegistrySavePayload, load_json, load_model_bundle, save_run, update_artifact_status
from .runtime_feature_dataset import write_runtime_feature_dataset
from .selection_calibration import _identity_calibration
from .selection_policy import build_selection_policy_from_recommendations
from .split import split_masks
from .train_v1 import _build_thresholds, build_selection_recommendations
from .train_v5_fusion import _build_fusion_numeric_feature_contract, _load_expert_table
from .train_v5_sequence import _parse_date_to_ts_ms, _sha256_file
from .v5_expert_runtime_export import (
    OPERATING_WINDOW_TIMEZONE,
    build_ts_date_coverage_payload,
    load_existing_expert_runtime_export,
    write_expert_runtime_export_metadata,
)
from .v5_expert_tail import (
    build_v5_expert_tail_context,
    finalize_v5_expert_family_run,
    resolve_existing_v5_expert_tail_artifacts,
    run_or_reuse_v5_expert_prediction_table,
    run_or_reuse_v5_runtime_governance_artifacts,
)
from .v5_domain_weighting import (
    build_v5_domain_weighting_report,
    resolve_v5_domain_weighting_components,
    write_v5_domain_weighting_report,
)


@dataclass(frozen=True)
class TrainV5TradabilityOptions:
    panel_input_path: Path
    sequence_input_path: Path
    lob_input_path: Path
    private_execution_root: Path
    registry_root: Path
    logs_root: Path
    model_family: str
    quote: str
    start: str
    end: str
    seed: int
    run_scope: str = "manual_tradability_expert"


@dataclass(frozen=True)
class TrainV5TradabilityResult:
    run_id: str
    run_dir: Path
    status: str
    leaderboard_row: dict[str, Any]
    metrics: dict[str, Any]
    thresholds: dict[str, Any]
    train_report_path: Path
    promotion_path: Path
    walk_forward_report_path: Path
    tradability_model_contract_path: Path
    predictor_contract_path: Path
    domain_weighting_report_path: Path


@dataclass
class V5TradabilityEstimator:
    tradability_model: Any
    fill_model: Any
    shortfall_model: Any
    adverse_model: Any
    uncertainty_model: Any
    feature_names: tuple[str, ...]

    def _predict_binary_prob(self, model: Any, x: np.ndarray) -> np.ndarray:
        if hasattr(model, "predict_proba"):
            return np.asarray(model.predict_proba(x)[:, 1], dtype=np.float64)
        return np.clip(np.asarray(model.predict(x), dtype=np.float64), 0.0, 1.0)

    def predict_tradability_contract(self, x: np.ndarray) -> dict[str, np.ndarray]:
        matrix = np.asarray(x, dtype=np.float64)
        tradability_prob = np.clip(self._predict_binary_prob(self.tradability_model, matrix), 0.0, 1.0)
        fill_prob = np.clip(self._predict_binary_prob(self.fill_model, matrix), 0.0, 1.0)
        adverse_prob = np.clip(self._predict_binary_prob(self.adverse_model, matrix), 0.0, 1.0)
        expected_shortfall_bps = np.maximum(np.asarray(self.shortfall_model.predict(matrix), dtype=np.float64), 0.0)
        tradability_uncertainty = np.maximum(np.asarray(self.uncertainty_model.predict(matrix), dtype=np.float64), 1e-6)
        return {
            "tradability_prob": tradability_prob,
            "fill_within_deadline_prob": fill_prob,
            "expected_shortfall_bps": expected_shortfall_bps,
            "adverse_tolerance_prob": adverse_prob,
            "tradability_uncertainty": tradability_uncertainty,
        }

    def predict_proba(self, x: np.ndarray) -> np.ndarray:
        tradability_prob = np.clip(self._predict_binary_prob(self.tradability_model, np.asarray(x, dtype=np.float64)), 0.0, 1.0)
        return np.column_stack([1.0 - tradability_prob, tradability_prob])


@dataclass
class _ConstantBinaryModel:
    positive_prob: float

    def predict_proba(self, x: np.ndarray) -> np.ndarray:
        rows = np.asarray(x).shape[0]
        prob = np.clip(float(self.positive_prob), 0.0, 1.0)
        return np.column_stack([np.full(rows, 1.0 - prob), np.full(rows, prob)])

    def predict(self, x: np.ndarray) -> np.ndarray:
        rows = np.asarray(x).shape[0]
        return np.full(rows, float(self.positive_prob))


def _load_private_execution_rows(*, dataset_root: Path, start: str, end: str) -> pl.DataFrame:
    root = Path(dataset_root).resolve()
    files = sorted(root.glob("market=*/date=*/part-*.parquet"))
    if not files:
        return pl.DataFrame()
    lazy = pl.scan_parquet([str(path) for path in files])
    start_ts_ms = _parse_date_to_ts_ms(start)
    end_ts_ms = _parse_date_to_ts_ms(end, end_of_day=True)
    if start_ts_ms is not None:
        lazy = lazy.filter(pl.col("ts_ms") >= int(start_ts_ms))
    if end_ts_ms is not None:
        lazy = lazy.filter(pl.col("ts_ms") <= int(end_ts_ms))
    frame = lazy.collect().sort(["market", "ts_ms"])
    if frame.height <= 0:
        return frame
    required = {
        "market",
        "decision_bucket_ts_ms",
        "y_tradeable",
        "y_fill_within_deadline",
        "y_shortfall_bps",
        "y_adverse_tolerance",
    }
    missing = [name for name in required if name not in frame.columns]
    if missing:
        raise ValueError(f"private_execution_v1 missing required columns: {', '.join(missing)}")
    return (
        frame.group_by(["market", "decision_bucket_ts_ms"])
        .agg(
            pl.col("y_tradeable").cast(pl.Float64).mean().alias("y_tradeable"),
            pl.col("y_fill_within_deadline").cast(pl.Float64).mean().alias("y_fill_within_deadline"),
            pl.col("y_shortfall_bps").cast(pl.Float64).mean().alias("y_shortfall_bps"),
            pl.col("y_adverse_tolerance").cast(pl.Float64).mean().alias("y_adverse_tolerance"),
            pl.len().alias("label_count"),
        )
        .rename({"decision_bucket_ts_ms": "ts_ms"})
        .sort(["market", "ts_ms"])
    )


def _load_and_merge_tradability_inputs(options: TrainV5TradabilityOptions) -> tuple[pl.DataFrame, dict[str, Any], tuple[str, ...]]:
    panel, panel_meta = _load_expert_table(options.panel_input_path, prefix="panel")
    sequence, sequence_meta = _load_expert_table(options.sequence_input_path, prefix="sequence")
    lob, lob_meta = _load_expert_table(options.lob_input_path, prefix="lob")
    merged = panel.join(sequence, on=["market", "ts_ms"], how="left", coalesce=True)
    merged = merged.join(lob, on=["market", "ts_ms"], how="left", coalesce=True)
    labels = _load_private_execution_rows(
        dataset_root=options.private_execution_root,
        start=options.start,
        end=options.end,
    )
    if labels.height <= 0:
        raise ValueError("private_execution_v1 produced no label rows in requested window")
    merged = merged.join(labels, on=["market", "ts_ms"], how="inner")
    if merged.height <= 0:
        raise ValueError("tradability expert found no overlapping expert rows with private execution labels")
    feature_names, _monotone_signs, feature_contract = _build_fusion_numeric_feature_contract(merged)
    snapshot_ids = {
        str(panel_meta.get("data_platform_ready_snapshot_id") or "").strip(),
        str(sequence_meta.get("data_platform_ready_snapshot_id") or "").strip(),
        str(lob_meta.get("data_platform_ready_snapshot_id") or "").strip(),
    }
    input_contract = {
        "policy": "v5_tradability_input_contract_v1",
        "snapshot_id": next(iter(snapshot_ids)) if len(snapshot_ids) == 1 else "",
        "inputs": {
            "panel": panel_meta,
            "sequence": sequence_meta,
            "lob": lob_meta,
        },
        "private_execution_root": str(options.private_execution_root),
        "feature_contract": feature_contract,
        "rows": int(merged.height),
    }
    return merged, input_contract, feature_names


def _fit_binary_head(x: np.ndarray, y: np.ndarray, *, seed: int, sample_weight: np.ndarray | None = None) -> Any:
    target = y.astype(np.int64)
    unique = np.unique(target)
    if unique.size < 2:
        return _ConstantBinaryModel(positive_prob=float(unique[0]) if unique.size == 1 else 0.0)
    from sklearn.linear_model import LogisticRegression

    model = LogisticRegression(max_iter=1000, random_state=int(seed))
    model.fit(x, target, sample_weight=np.asarray(sample_weight, dtype=np.float64) if sample_weight is not None else None)
    return model


def _fit_reg_head(
    x: np.ndarray,
    y: np.ndarray,
    *,
    seed: int,
    clip_min: float | None = None,
    sample_weight: np.ndarray | None = None,
) -> Any:
    from sklearn.linear_model import Ridge

    target = np.asarray(y, dtype=np.float64)
    if clip_min is not None:
        target = np.maximum(target, float(clip_min))
    model = Ridge(alpha=1.0, random_state=int(seed))
    model.fit(x, target, sample_weight=np.asarray(sample_weight, dtype=np.float64) if sample_weight is not None else None)
    return model


def _write_tradability_expert_prediction_table(
    *,
    run_dir: Path,
    frame: pl.DataFrame,
    estimator: V5TradabilityEstimator,
    feature_names: tuple[str, ...],
    output_path: Path | None = None,
) -> Path:
    x = frame.select(list(feature_names)).to_numpy().astype(np.float64, copy=False)
    payload = estimator.predict_tradability_contract(x)
    export_frame = pl.DataFrame(
        {
            "market": frame.get_column("market").to_numpy(),
            "ts_ms": frame.get_column("ts_ms").to_numpy().astype(np.int64, copy=False),
            "split": frame.get_column("split").to_numpy() if "split" in frame.columns else np.full(frame.height, "runtime", dtype=object),
            "tradability_prob": payload["tradability_prob"],
            "fill_within_deadline_prob": payload["fill_within_deadline_prob"],
            "expected_shortfall_bps": payload["expected_shortfall_bps"],
            "adverse_tolerance_prob": payload["adverse_tolerance_prob"],
            "tradability_uncertainty": payload["tradability_uncertainty"],
        }
    ).sort(["ts_ms", "market"])
    resolved_path = Path(output_path) if output_path is not None else (run_dir / "expert_prediction_table.parquet")
    resolved_path.parent.mkdir(parents=True, exist_ok=True)
    export_frame.write_parquet(resolved_path)
    return resolved_path


def _build_tradability_runtime_recommendations(
    *,
    options: TrainV5TradabilityOptions,
    runtime_dataset_root: Path,
    input_contract: dict[str, Any] | None = None,
    domain_details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    inputs = dict((input_contract or {}).get("inputs") or {})
    details = dict(domain_details or {})
    return annotate_v5_runtime_recommendations(
        {
            "status": "tradability_runtime_ready",
            "source_family": options.model_family,
            "runtime_feature_dataset_root": str(runtime_dataset_root),
            "panel_source_run_id": str(((inputs.get("panel") or {}).get("run_id")) or "").strip(),
            "sequence_source_run_id": str(((inputs.get("sequence") or {}).get("run_id")) or "").strip(),
            "lob_source_run_id": str(((inputs.get("lob") or {}).get("run_id")) or "").strip(),
            "domain_weighting_policy": str(details.get("policy") or "v5_domain_weighting_v1").strip() or "v5_domain_weighting_v1",
            "domain_weighting_source_kind": str(details.get("source_kind") or "regime_inverse_frequency_v1").strip() or "regime_inverse_frequency_v1",
            "domain_weighting_enabled": bool(details.get("enabled", False)),
        }
    )


def _build_metrics(*, y_true: np.ndarray, scores: np.ndarray, sample_weight: np.ndarray | None = None) -> dict[str, Any]:
    return {
        "classification": classification_metrics(
            y_true.astype(np.int64),
            scores.astype(np.float64),
            sample_weight=np.asarray(sample_weight, dtype=np.float64) if sample_weight is not None else None,
        ),
        "rows": int(y_true.size),
    }


def _build_data_fingerprint(*, options: TrainV5TradabilityOptions, input_contract: dict[str, Any], sample_count: int) -> dict[str, Any]:
    return {
        "dataset_root": str(options.private_execution_root),
        "quote": options.quote,
        "start_ts_ms": _parse_date_to_ts_ms(options.start),
        "end_ts_ms": _parse_date_to_ts_ms(options.end, end_of_day=True),
        "panel_input_sha256": _sha256_file(options.panel_input_path),
        "sequence_input_sha256": _sha256_file(options.sequence_input_path),
        "lob_input_sha256": _sha256_file(options.lob_input_path),
        "sample_count": int(sample_count),
        "code_version": autobot_version,
        "data_platform_ready_snapshot_id": str(input_contract.get("snapshot_id") or "").strip(),
    }


def train_and_register_v5_tradability(options: TrainV5TradabilityOptions) -> TrainV5TradabilityResult:
    merged, input_contract, feature_names = _load_and_merge_tradability_inputs(options)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    labels = merged.get_column("split").to_numpy() if "split" in merged.columns else np.full(merged.height, "train", dtype=object)
    masks = split_masks(labels)
    train_mask = np.asarray(masks.get("train", np.zeros(merged.height, dtype=bool)), dtype=bool)
    valid_mask = np.asarray(masks.get("valid", np.zeros(merged.height, dtype=bool)), dtype=bool)
    test_mask = np.asarray(masks.get("test", np.zeros(merged.height, dtype=bool)), dtype=bool)
    if train_mask.sum() <= 0 or valid_mask.sum() <= 0 or test_mask.sum() <= 0:
        raise ValueError("v5_tradability requires non-empty train/valid/test splits")

    x = merged.select(list(feature_names)).to_numpy().astype(np.float64, copy=False)
    y_tradeable = (merged.get_column("y_tradeable").to_numpy().astype(np.float64, copy=False) >= 0.5).astype(np.int64)
    y_fill = (merged.get_column("y_fill_within_deadline").to_numpy().astype(np.float64, copy=False) >= 0.5).astype(np.int64)
    y_shortfall = np.maximum(merged.get_column("y_shortfall_bps").to_numpy().astype(np.float64, copy=False), 0.0)
    y_adverse = (merged.get_column("y_adverse_tolerance").to_numpy().astype(np.float64, copy=False) >= 0.5).astype(np.int64)
    label_count = np.maximum(
        merged.get_column("label_count").to_numpy().astype(np.float64, copy=False),
        1.0,
    )
    median_label_count = float(np.median(label_count)) if label_count.size > 0 else 1.0
    if not np.isfinite(median_label_count) or median_label_count <= 0.0:
        median_label_count = 1.0
    data_quality_weight = np.clip(np.sqrt(label_count / median_label_count), 0.5, 2.0)
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
    support_weight = np.clip(np.maximum(support_score, 1.0) / 2.0, 0.5, 1.0)
    weight_components = resolve_v5_domain_weighting_components(
        markets=merged.get_column("market").to_numpy(),
        ts_ms=merged.get_column("ts_ms").to_numpy().astype(np.int64, copy=False),
        split_labels=labels,
        base_sample_weight=np.ones(merged.height, dtype=np.float64),
        data_quality_weight=data_quality_weight,
        support_weight=support_weight,
    )
    sample_weight = np.asarray(weight_components["final_sample_weight"], dtype=np.float64)

    tradability_model = _fit_binary_head(
        x[train_mask],
        y_tradeable[train_mask],
        seed=options.seed,
        sample_weight=sample_weight[train_mask],
    )
    fill_model = _fit_binary_head(
        x[train_mask],
        y_fill[train_mask],
        seed=options.seed + 1,
        sample_weight=sample_weight[train_mask],
    )
    shortfall_model = _fit_reg_head(
        x[train_mask],
        y_shortfall[train_mask],
        seed=options.seed + 2,
        clip_min=0.0,
        sample_weight=sample_weight[train_mask],
    )
    adverse_model = _fit_binary_head(
        x[train_mask],
        y_adverse[train_mask],
        seed=options.seed + 3,
        sample_weight=sample_weight[train_mask],
    )
    train_scores = np.asarray(tradability_model.predict_proba(x[train_mask])[:, 1], dtype=np.float64)
    uncertainty_target = np.abs(y_tradeable[train_mask].astype(np.float64) - train_scores)
    uncertainty_model = _fit_reg_head(
        x[train_mask],
        uncertainty_target,
        seed=options.seed + 4,
        clip_min=1e-6,
        sample_weight=sample_weight[train_mask],
    )
    estimator = V5TradabilityEstimator(
        tradability_model=tradability_model,
        fill_model=fill_model,
        shortfall_model=shortfall_model,
        adverse_model=adverse_model,
        uncertainty_model=uncertainty_model,
        feature_names=feature_names,
    )

    valid_scores = estimator.predict_tradability_contract(x[valid_mask])["tradability_prob"]
    test_scores = estimator.predict_tradability_contract(x[test_mask])["tradability_prob"]
    valid_metrics = _build_metrics(
        y_true=y_tradeable[valid_mask],
        scores=valid_scores,
        sample_weight=sample_weight[valid_mask],
    )
    test_metrics = _build_metrics(
        y_true=y_tradeable[test_mask],
        scores=test_scores,
        sample_weight=sample_weight[test_mask],
    )
    thresholds = _build_thresholds(
        valid_scores=valid_scores,
        y_reg_valid=(y_tradeable[valid_mask].astype(np.float64) - 0.5),
        fee_bps_est=0.0,
        safety_bps=0.0,
        ev_scan_steps=10,
        ev_min_selected=1,
        sample_weight=sample_weight[valid_mask],
    )
    selection_recommendations = build_selection_recommendations(
        valid_scores=valid_scores,
        valid_ts_ms=merged.get_column("ts_ms").to_numpy().astype(np.int64, copy=False)[valid_mask],
        thresholds=thresholds,
    )
    selection_policy = build_selection_policy_from_recommendations(
        selection_recommendations=selection_recommendations,
        fallback_threshold_key="top_5pct",
        score_source="score_mean",
    )
    selection_calibration = _identity_calibration(reason="TRADABILITY_IDENTITY_CALIBRATION")
    metrics = {
        "rows": {
            "train": int(np.sum(train_mask)),
            "valid": int(np.sum(valid_mask)),
            "test": int(np.sum(test_mask)),
        },
        "valid_metrics": valid_metrics,
        "champion_metrics": test_metrics,
        "targets": {
            "tradeable_positive_rate": float(np.mean(y_tradeable)),
            "fill_positive_rate": float(np.mean(y_fill)),
            "adverse_tolerance_positive_rate": float(np.mean(y_adverse)),
            "shortfall_mean_bps": float(np.mean(y_shortfall)),
        },
    }
    leaderboard_row = {
        "run_id": run_id,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "model_family": options.model_family,
        "champion": "tradability_expert",
        "champion_backend": "linear_multihead",
        "test_roc_auc": float((test_metrics.get("classification", {}) or {}).get("roc_auc") or 0.0),
        "test_pr_auc": float((test_metrics.get("classification", {}) or {}).get("pr_auc") or 0.0),
        "rows_train": int(np.sum(train_mask)),
        "rows_valid": int(np.sum(valid_mask)),
        "rows_test": int(np.sum(test_mask)),
    }
    runtime_dataset_root = options.registry_root / options.model_family / run_id / "runtime_feature_dataset"
    train_config = {
        **asdict(options),
        "panel_input_path": str(options.panel_input_path),
        "sequence_input_path": str(options.sequence_input_path),
        "lob_input_path": str(options.lob_input_path),
        "private_execution_root": str(options.private_execution_root),
        "registry_root": str(options.registry_root),
        "logs_root": str(options.logs_root),
        "trainer": "v5_tradability",
        "feature_columns": list(feature_names),
        "selected_markets": sorted({str(item).strip() for item in merged.get_column("market").to_list() if str(item).strip()}),
        "dataset_root": str(runtime_dataset_root),
        "data_platform_ready_snapshot_id": str(input_contract.get("snapshot_id") or resolve_ready_snapshot_id(project_root=Path.cwd()) or ""),
    }
    runtime_recommendations = _build_tradability_runtime_recommendations(
        options=options,
        runtime_dataset_root=runtime_dataset_root,
        input_contract=input_contract,
        domain_details=dict(weight_components["domain_details"] or {}),
    )
    data_fingerprint = _build_data_fingerprint(options=options, input_contract=input_contract, sample_count=merged.height)
    model_card = render_model_card(
        run_id=run_id,
        model_family=options.model_family,
        champion="tradability_expert",
        metrics=metrics,
        thresholds=thresholds,
        data_fingerprint=data_fingerprint,
    )
    run_dir = save_run(
        RegistrySavePayload(
            registry_root=options.registry_root,
            model_family=options.model_family,
            run_id=run_id,
            model_bundle={"model_type": "v5_tradability", "estimator": estimator},
            metrics=metrics,
            thresholds=thresholds,
            feature_spec={"feature_columns": list(feature_names), "dataset_root": str(runtime_dataset_root)},
            label_spec={"policy": "v5_tradability_label_contract_v1", "targets": ["y_tradeable", "y_fill_within_deadline", "y_shortfall_bps", "y_adverse_tolerance"]},
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

    tradability_model_contract_path = run_dir / "tradability_model_contract.json"
    tradability_model_contract_path.write_text(
        json.dumps(
            {
                "policy": "v5_tradability_v1",
                "input_experts": dict(input_contract.get("inputs") or {}),
                "private_execution_root": str(options.private_execution_root),
                "outputs": {
                    "tradability_prob": "tradability_prob",
                    "fill_within_deadline_prob": "fill_within_deadline_prob",
                    "expected_shortfall_bps": "expected_shortfall_bps",
                    "adverse_tolerance_prob": "adverse_tolerance_prob",
                    "tradability_uncertainty": "tradability_uncertainty",
                },
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ) + "\n",
        encoding="utf-8",
    )
    predictor_contract_path = run_dir / "predictor_contract.json"
    predictor_contract_path.write_text(
        json.dumps(
            {
                "version": 1,
                "tradability_prob_field": "tradability_prob",
                "fill_within_deadline_prob_field": "fill_within_deadline_prob",
                "expected_shortfall_bps_field": "expected_shortfall_bps",
                "adverse_tolerance_prob_field": "adverse_tolerance_prob",
                "tradability_uncertainty_field": "tradability_uncertainty",
                "feature_columns": list(feature_names),
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ) + "\n",
        encoding="utf-8",
    )
    walk_forward_report_path = run_dir / "walk_forward_report.json"
    walk_forward_report_path.write_text(
        json.dumps({"policy": "v5_tradability_holdout_v1", "valid_metrics": valid_metrics, "test_metrics": test_metrics}, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    runtime_dataset_written_root = write_runtime_feature_dataset(
        output_root=runtime_dataset_root,
        tf="5m",
        feature_columns=feature_names,
        markets=merged.get_column("market").to_numpy(),
        ts_ms=merged.get_column("ts_ms").to_numpy().astype(np.int64, copy=False),
        x=x,
        y_cls=y_tradeable,
        y_reg=y_shortfall,
        y_rank=y_shortfall,
        sample_weight=sample_weight,
    )
    domain_weighting_report_path = write_v5_domain_weighting_report(
        run_dir=run_dir,
        payload=build_v5_domain_weighting_report(
            run_id=run_id,
            trainer_name="v5_tradability",
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
    tail_context = build_v5_expert_tail_context(
        run_id=run_id,
        trainer_name="v5_tradability",
        model_family=options.model_family,
        data_platform_ready_snapshot_id=train_config.get("data_platform_ready_snapshot_id"),
        dataset_root=runtime_dataset_root,
        source_dataset_root=options.private_execution_root,
        runtime_dataset_root=runtime_dataset_written_root,
        selected_markets=tuple(train_config["selected_markets"]),
        support_level_counts={},
        run_scope=options.run_scope,
    )
    existing_tail_artifacts = resolve_existing_v5_expert_tail_artifacts(run_dir=run_dir, tail_context=tail_context)
    export_frame = merged.select(["market", "ts_ms", "split", *feature_names])
    expert_prediction_table_path = run_or_reuse_v5_expert_prediction_table(
        run_dir=run_dir,
        existing_tail_artifacts=existing_tail_artifacts,
        writer=lambda: _write_tradability_expert_prediction_table(
            run_dir=run_dir,
            frame=export_frame,
            estimator=estimator,
            feature_names=feature_names,
        ),
    )
    promotion_payload = {
        "run_id": run_id,
        "promote": False,
        "status": "candidate",
        "reasons": ["EXPERT_FAMILY_REQUIRES_EXPLICIT_PROMOTION_PATH"],
    }
    runtime_artifacts = run_or_reuse_v5_runtime_governance_artifacts(
        run_dir=run_dir,
        trainer_name="v5_tradability",
        model_family=options.model_family,
        run_scope=options.run_scope,
        metrics=metrics,
        runtime_recommendations=runtime_recommendations,
        promotion=promotion_payload,
        trainer_research_reasons=[],
        tail_context=tail_context,
        existing_tail_artifacts=existing_tail_artifacts,
        resumed=False,
    )
    report_payload = {
        "run_id": run_id,
        "model_family": options.model_family,
        "status": "candidate",
        "metrics": metrics,
        "leaderboard_row": leaderboard_row,
        "expert_prediction_table_path": str(expert_prediction_table_path),
        "tradability_model_contract_path": str(tradability_model_contract_path),
        "predictor_contract_path": str(predictor_contract_path),
    }
    train_report_path = finalize_v5_expert_family_run(
        run_dir=run_dir,
        run_id=run_id,
        registry_root=options.registry_root,
        model_family=options.model_family,
        logs_root=options.logs_root,
        report_name="train_v5_tradability_report.json",
        report_payload=report_payload,
        data_platform_ready_snapshot_id=train_config.get("data_platform_ready_snapshot_id"),
        resumed=False,
        tail_started_at=time.time(),
    )
    return TrainV5TradabilityResult(
        run_id=run_id,
        run_dir=run_dir,
        status="candidate",
        leaderboard_row=leaderboard_row,
        metrics=metrics,
        thresholds=thresholds,
        train_report_path=train_report_path,
        promotion_path=Path(str(runtime_artifacts["promotion_path"])),
        walk_forward_report_path=walk_forward_report_path,
        tradability_model_contract_path=tradability_model_contract_path,
        predictor_contract_path=predictor_contract_path,
        domain_weighting_report_path=domain_weighting_report_path,
    )


def materialize_v5_tradability_runtime_export(
    *,
    run_dir: Path,
    start: str,
    end: str,
    panel_runtime_input_path: Path,
    sequence_runtime_input_path: Path,
    lob_runtime_input_path: Path,
    selected_markets_override: tuple[str, ...] | None = None,
    resolve_markets_only: bool = False,
) -> dict[str, Any]:
    run_dir = Path(run_dir).resolve()
    train_config = load_json(run_dir / "train_config.yaml")
    if not train_config:
        raise FileNotFoundError(f"missing train_config.yaml in {run_dir}")
    estimator_bundle = load_model_bundle(run_dir)
    estimator = estimator_bundle.get("estimator") if isinstance(estimator_bundle, dict) else None
    if estimator is None:
        raise ValueError(f"run_dir does not contain a usable tradability estimator: {run_dir}")
    panel, panel_meta = _load_expert_table(Path(panel_runtime_input_path), prefix="panel")
    sequence, sequence_meta = _load_expert_table(Path(sequence_runtime_input_path), prefix="sequence")
    lob, lob_meta = _load_expert_table(Path(lob_runtime_input_path), prefix="lob")
    merged = panel.join(sequence, on=["market", "ts_ms"], how="left", coalesce=True)
    merged = merged.join(lob, on=["market", "ts_ms"], how="left", coalesce=True)
    if selected_markets_override is not None:
        allowed = {str(item).strip().upper() for item in selected_markets_override if str(item).strip()}
        merged = merged.filter(pl.col("market").is_in(sorted(allowed)))
    if merged.height <= 0 and selected_markets_override is None:
        merged = panel.join(sequence, on=["market", "ts_ms"], how="left", coalesce=True).join(lob, on=["market", "ts_ms"], how="left", coalesce=True)
    if merged.height <= 0:
        raise ValueError("tradability runtime export produced no rows in requested certification window")
    feature_names = tuple(str(item).strip() for item in (train_config.get("feature_columns") or []) if str(item).strip())
    x = merged.select(list(feature_names)).to_numpy().astype(np.float64, copy=False)
    payload = estimator.predict_tradability_contract(x)
    metadata = {
        "version": 1,
        "policy": "v5_expert_runtime_export_v1",
        "run_id": run_dir.name,
        "trainer": "v5_tradability",
        "model_family": str(train_config.get("model_family") or "train_v5_tradability").strip(),
        "data_platform_ready_snapshot_id": str(train_config.get("data_platform_ready_snapshot_id") or "").strip(),
        "start": str(start).strip(),
        "end": str(end).strip(),
        **build_ts_date_coverage_payload(merged.get_column("ts_ms").to_list(), timezone_name=OPERATING_WINDOW_TIMEZONE),
        "coverage_start_ts_ms": int(merged.get_column("ts_ms").min()) if merged.height > 0 else 0,
        "coverage_end_ts_ms": int(merged.get_column("ts_ms").max()) if merged.height > 0 else 0,
        "requested_selected_markets": list(selected_markets_override or []),
        "selected_markets": sorted({str(item).strip().upper() for item in merged.get_column("market").to_list() if str(item).strip()}),
        "selected_markets_source": "acceptance_common_runtime_universe" if selected_markets_override is not None else "runtime_input_markets",
        "fallback_reason": "",
        "rows": int(merged.height),
    }
    existing_export = load_existing_expert_runtime_export(run_dir, start, end)
    export_path = Path(str((existing_export.get("paths") or {}).get("export_path") or (run_dir / "_runtime_exports" / f"{start}__{end}" / "expert_prediction_table.parquet")))
    if resolve_markets_only:
        return {
            **metadata,
            "export_path": "",
            "metadata_path": "",
            "reused": False,
            "source_mode": "resolve_markets_only",
        }
    export_frame = pl.DataFrame(
        {
            "market": merged.get_column("market").to_numpy(),
            "ts_ms": merged.get_column("ts_ms").to_numpy().astype(np.int64, copy=False),
            "split": np.full(merged.height, "runtime", dtype=object),
            "tradability_prob": payload["tradability_prob"],
            "fill_within_deadline_prob": payload["fill_within_deadline_prob"],
            "expected_shortfall_bps": payload["expected_shortfall_bps"],
            "adverse_tolerance_prob": payload["adverse_tolerance_prob"],
            "tradability_uncertainty": payload["tradability_uncertainty"],
        }
    ).sort(["ts_ms", "market"])
    export_path.parent.mkdir(parents=True, exist_ok=True)
    export_frame.write_parquet(export_path)
    metadata_path = write_expert_runtime_export_metadata(run_dir=run_dir, start=start, end=end, payload=metadata)
    return {
        **metadata,
        "export_path": str(export_path),
        "metadata_path": str(metadata_path),
        "reused": False,
        "source_mode": "fresh_export",
    }
