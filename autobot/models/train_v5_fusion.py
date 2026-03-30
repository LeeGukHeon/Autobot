"""Fusion meta-model trainer for panel/sequence/LOB expert predictions."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

from autobot import __version__ as autobot_version

from .entry_boundary import build_risk_calibrated_entry_boundary
from .metrics import classification_metrics, grouped_trading_metrics, trading_metrics
from .model_card import render_model_card
from .registry import RegistrySavePayload, load_json, make_run_id, save_run, update_artifact_status, update_latest_pointer
from .runtime_feature_dataset import write_runtime_feature_dataset
from .selection_calibration import _identity_calibration
from .selection_policy import build_selection_policy_from_recommendations
from .split import compute_time_splits, split_masks
from .train_v1 import _build_thresholds, build_selection_recommendations
from .train_v5_sequence import _parse_date_to_ts_ms, _sha256_file
from .v5_runtime_artifacts import persist_v5_runtime_governance_artifacts
from autobot.ops.data_platform_snapshot import resolve_ready_snapshot_id


VALID_FUSION_STACKERS = ("linear", "monotone_gbdt")


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


@dataclass
class V5FusionEstimator:
    score_model: Any
    return_model: Any
    es_model: Any
    tradability_model: Any
    uncertainty_model: Any
    stacker_family: str
    feature_names: tuple[str, ...]

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


def train_and_register_v5_fusion(options: TrainV5FusionOptions) -> TrainV5FusionResult:
    stacker_family = str(options.stacker_family).strip().lower()
    if stacker_family not in VALID_FUSION_STACKERS:
        raise ValueError(f"stacker_family must be one of: {', '.join(VALID_FUSION_STACKERS)}")

    run_id = make_run_id(seed=options.seed)
    merged = _load_and_merge_expert_tables(options)
    if merged.height <= 0:
        raise ValueError("fusion inputs produced no aligned rows")

    start_ts_ms = _parse_date_to_ts_ms(options.start)
    end_ts_ms = _parse_date_to_ts_ms(options.end, end_of_day=True)
    if start_ts_ms is not None:
        merged = merged.filter(pl.col("ts_ms") >= int(start_ts_ms))
    if end_ts_ms is not None:
        merged = merged.filter(pl.col("ts_ms") <= int(end_ts_ms))
    if merged.height <= 0:
        raise ValueError("fusion inputs have no rows in the requested range")

    feature_names = tuple(
        col for col in merged.columns if col not in {"market", "ts_ms", "split", "y_cls", "y_reg", "y_es_proxy", "y_tradability_target"}
    )
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

    score_model = _fit_binary_head(x[train_mask], y_cls[train_mask], stacker_family=stacker_family, seed=options.seed)
    return_model = _fit_reg_head(x[train_mask], y_reg[train_mask], stacker_family=stacker_family, seed=options.seed + 1)
    es_model = _fit_reg_head(x[train_mask], y_es[train_mask], stacker_family=stacker_family, seed=options.seed + 2)
    tradability_model = _fit_binary_head(x[train_mask], y_tradability[train_mask], stacker_family=stacker_family, seed=options.seed + 3)

    valid_return_pred = np.asarray(return_model.predict(x[valid_mask]), dtype=np.float64)
    uncertainty_target = np.abs(y_reg[valid_mask] - valid_return_pred)
    uncertainty_model = _fit_reg_head(x[valid_mask], uncertainty_target, stacker_family="linear", seed=options.seed + 4)

    estimator = V5FusionEstimator(
        score_model=score_model,
        return_model=return_model,
        es_model=es_model,
        tradability_model=tradability_model,
        uncertainty_model=uncertainty_model,
        stacker_family=stacker_family,
        feature_names=feature_names,
    )
    valid_contract = estimator.predict_panel_contract(x[valid_mask])
    test_contract = estimator.predict_panel_contract(x[test_mask])
    valid_metrics = _evaluate_fusion_split(y_cls=y_cls[valid_mask], y_reg=y_reg[valid_mask], scores=valid_contract["final_rank_score"], markets=markets[valid_mask])
    test_metrics = _evaluate_fusion_split(y_cls=y_cls[test_mask], y_reg=y_reg[test_mask], scores=test_contract["final_rank_score"], markets=markets[test_mask])
    thresholds = _build_thresholds(valid_scores=valid_contract["final_rank_score"], y_reg_valid=y_reg[valid_mask], fee_bps_est=0.0, safety_bps=0.0, ev_scan_steps=10, ev_min_selected=1)
    selection_recommendations = build_selection_recommendations(valid_scores=valid_contract["final_rank_score"], valid_ts_ms=ts_ms[valid_mask], thresholds=thresholds)
    selection_policy = build_selection_policy_from_recommendations(selection_recommendations=selection_recommendations, fallback_threshold_key="top_5pct", score_source="score_mean")
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
            "input_experts": ["panel", "sequence", "lob"],
            "outputs": ["final_rank_score", "final_expected_return", "final_expected_es", "final_tradability", "final_uncertainty", "final_alpha_lcb"],
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
        "data_platform_ready_snapshot_id": resolve_ready_snapshot_id(project_root=Path.cwd()),
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
        "dataset_root": str(runtime_dataset_root),
        "source_dataset_root": "fusion_oof_tables",
        "registry_root": str(options.registry_root),
        "logs_root": str(options.logs_root),
        "trainer": "v5_fusion",
        "feature_columns": list(feature_names),
        "autobot_version": autobot_version,
        "data_platform_ready_snapshot_id": data_fingerprint.get("data_platform_ready_snapshot_id"),
    }
    runtime_recommendations = _load_inherited_runtime_recommendations(options.panel_input_path)
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
                "input_experts": {
                    "panel": str(options.panel_input_path),
                    "sequence": str(options.sequence_input_path),
                    "lob": str(options.lob_input_path),
                },
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
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    walk_forward_report_path = run_dir / "walk_forward_report.json"
    walk_forward_report_path.write_text(json.dumps({"policy": "fusion_holdout_v1", "valid_metrics": valid_metrics, "test_metrics": test_metrics}, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
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
    entry_boundary_contract_path = run_dir / "entry_boundary_contract.json"
    entry_boundary_contract_path.write_text(json.dumps(entry_boundary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    runtime_dataset_written_root = write_runtime_feature_dataset(
        output_root=runtime_dataset_root,
        tf="5m",
        feature_columns=feature_names,
        markets=markets,
        ts_ms=ts_ms,
        x=x,
        y_cls=y_cls,
        y_reg=y_reg,
        y_rank=y_reg,
        sample_weight=np.ones(merged.height, dtype=np.float64),
    )
    runtime_artifacts = persist_v5_runtime_governance_artifacts(
        run_dir=run_dir,
        trainer_name="v5_fusion",
        model_family=options.model_family,
        run_scope=options.run_scope,
        metrics=metrics,
        runtime_recommendations=runtime_recommendations,
        promotion=promotion_payload,
        trainer_research_reasons=["FUSION_RUNTIME_CONTRACT_READY"],
    )
    update_artifact_status(
        run_dir,
        status="trainer_artifacts_complete",
        execution_acceptance_complete=True,
        runtime_recommendations_complete=True,
        governance_artifacts_complete=True,
    )
    train_report_path = options.logs_root / "train_v5_fusion_report.json"
    train_report_path.parent.mkdir(parents=True, exist_ok=True)
    train_report_path.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "status": "candidate",
                "leaderboard_row": leaderboard_row,
                "valid_metrics": valid_metrics,
                "test_metrics": test_metrics,
                "runtime_dataset_root": str(runtime_dataset_written_root),
                "entry_boundary_contract_path": str(entry_boundary_contract_path),
                "data_platform_ready_snapshot_id": data_fingerprint.get("data_platform_ready_snapshot_id"),
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    update_latest_pointer(options.registry_root, options.model_family, run_id)
    if str(options.run_scope).strip().lower() == "scheduled_daily":
        update_latest_pointer(options.registry_root, "_global", run_id, family=options.model_family)
    update_artifact_status(run_dir, status="candidate", support_artifacts_written=True)
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
        entry_boundary_contract_path=entry_boundary_contract_path,
    )


def _load_and_merge_expert_tables(options: TrainV5FusionOptions) -> pl.DataFrame:
    panel = _load_expert_table(options.panel_input_path, prefix="panel")
    sequence = _load_expert_table(options.sequence_input_path, prefix="sequence")
    lob = _load_expert_table(options.lob_input_path, prefix="lob")
    merged = panel.join(sequence, on=["market", "ts_ms"], how="full", coalesce=True)
    merged = merged.join(lob, on=["market", "ts_ms"], how="full", coalesce=True)
    merged = merged.with_columns(
        pl.coalesce(["split", "split_sequence", "split_lob"]).alias("split"),
        pl.coalesce(["y_cls", "y_cls_sequence", "y_cls_lob"]).cast(pl.Int64).alias("y_cls"),
        pl.coalesce(["y_reg", "y_reg_sequence", "y_reg_lob"]).cast(pl.Float64).alias("y_reg"),
        pl.col("panel_final_rank_score").is_not_null().cast(pl.Int64).alias("panel_present"),
        pl.col("sequence_directional_probability_primary").is_not_null().cast(pl.Int64).alias("sequence_present"),
        pl.col("lob_micro_alpha_30s").is_not_null().cast(pl.Int64).alias("lob_present"),
    )
    expert_value_columns = [
        name
        for name in merged.columns
        if name.startswith("panel_") or name.startswith("sequence_") or name.startswith("lob_")
    ]
    if expert_value_columns:
        merged = merged.with_columns([pl.col(name).fill_null(0.0) for name in expert_value_columns])
    merged = merged.drop([name for name in ("split_sequence", "split_lob", "y_cls_sequence", "y_cls_lob", "y_reg_sequence", "y_reg_lob") if name in merged.columns])
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
    return merged.sort(["ts_ms", "market"])


def _load_expert_table(path: Path, *, prefix: str) -> pl.DataFrame:
    frame = pl.read_parquet(path)
    keep_base = ["market", "ts_ms"]
    renamed = []
    for column in frame.columns:
        if column in keep_base:
            renamed.append(pl.col(column))
        elif column == "split":
            renamed.append(pl.col(column).alias("split" if prefix == "panel" else f"split_{prefix}"))
        elif column == "y_cls":
            renamed.append(pl.col(column).alias("y_cls" if prefix == "panel" else f"y_cls_{prefix}"))
        elif column == "y_reg":
            renamed.append(pl.col(column).alias("y_reg" if prefix == "panel" else f"y_reg_{prefix}"))
        else:
            renamed.append(pl.col(column).alias(f"{prefix}_{column}"))
    return frame.select(renamed)


def _load_inherited_runtime_recommendations(panel_input_path: Path) -> dict[str, Any]:
    panel_run_dir = Path(panel_input_path).parent
    inherited = load_json(panel_run_dir / "runtime_recommendations.json")
    payload = dict(inherited) if isinstance(inherited, dict) else {}
    payload["status"] = "fusion_runtime_ready"
    payload["source_family"] = "train_v5_fusion"
    payload["inherited_from_panel_run_id"] = panel_run_dir.name
    payload["entry_boundary_enabled"] = True
    return payload


def _fit_binary_head(x: np.ndarray, y: np.ndarray, *, stacker_family: str, seed: int) -> Any:
    if stacker_family == "linear":
        from sklearn.linear_model import LogisticRegression

        model = LogisticRegression(max_iter=1000, random_state=int(seed))
        model.fit(x, y)
        return model
    import xgboost as xgb

    constraints = "(" + ",".join(["1"] * x.shape[1]) + ")"
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
    model.fit(x, y)
    return model


def _fit_reg_head(x: np.ndarray, y: np.ndarray, *, stacker_family: str, seed: int) -> Any:
    if stacker_family == "linear":
        from sklearn.linear_model import Ridge

        model = Ridge(alpha=1.0, random_state=int(seed))
        model.fit(x, y)
        return model
    import xgboost as xgb

    constraints = "(" + ",".join(["1"] * x.shape[1]) + ")"
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
    model.fit(x, y)
    return model


def _evaluate_fusion_split(*, y_cls: np.ndarray, y_reg: np.ndarray, scores: np.ndarray, markets: np.ndarray) -> dict[str, Any]:
    cls = classification_metrics(y_cls, scores)
    trading = trading_metrics(y_cls, y_reg, scores, fee_bps_est=0.0, safety_bps=0.0)
    per_market = grouped_trading_metrics(markets=markets, y_true=y_cls, y_reg=y_reg, scores=scores, fee_bps_est=0.0, safety_bps=0.0)
    return {
        "rows": int(y_cls.size),
        "classification": cls,
        "trading": trading,
        "per_market": per_market,
    }
