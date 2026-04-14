"""Two-stage XGBoost trainer on top of market_state_training_slice_v1."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

from autobot import __version__ as autobot_version

from .metrics import classification_metrics
from .model_card import render_model_card
from .registry import RegistrySavePayload, load_json, make_run_id, save_run
from .runtime_recommendation_contract import normalize_runtime_recommendations_payload
from .selection_calibration import _identity_calibration
from .selection_policy import normalize_selection_policy
from .train_v1 import _try_import_xgboost


DEFAULT_TRADEABLE_PROB_THRESHOLD = 0.55
DEFAULT_NET_EDGE_THRESHOLD_BPS = 3.0
DEFAULT_HARD_NEGATIVE_LOW_BPS = -6.0
DEFAULT_HARD_NEGATIVE_HIGH_BPS = 3.0
DEFAULT_FLAT_NEGATIVE_DOWNSAMPLE = 0.10
DEFAULT_MIN_STANDARD_DATES = 14
DEFAULT_MIN_BOOTSTRAP_DATES = 3


@dataclass(frozen=True)
class TrainV6Edge2StageOptions:
    dataset_root: Path
    registry_root: Path
    logs_root: Path
    model_family: str
    quote: str
    start: str
    end: str
    seed: int
    nthread: int = 1
    tradeable_prob_threshold: float = DEFAULT_TRADEABLE_PROB_THRESHOLD
    net_edge_threshold_bps: float = DEFAULT_NET_EDGE_THRESHOLD_BPS
    hard_negative_low_bps: float = DEFAULT_HARD_NEGATIVE_LOW_BPS
    hard_negative_high_bps: float = DEFAULT_HARD_NEGATIVE_HIGH_BPS
    flat_negative_downsample: float = DEFAULT_FLAT_NEGATIVE_DOWNSAMPLE
    run_scope: str = "manual_edge2stage"


@dataclass(frozen=True)
class TrainV6Edge2StageResult:
    run_id: str
    run_dir: Path
    status: str
    leaderboard_row: dict[str, Any]
    metrics: dict[str, Any]
    thresholds: dict[str, Any]
    train_report_path: Path
    predictor_contract_path: Path


@dataclass
class V6Edge2StageEstimator:
    tradeable_model: Any
    edge_model: Any
    feature_names: tuple[str, ...]
    tradeable_prob_threshold: float
    net_edge_threshold_bps: float

    def _predict_tradeable_prob(self, x: np.ndarray) -> np.ndarray:
        matrix = np.asarray(x, dtype=np.float64)
        if hasattr(self.tradeable_model, "predict_proba"):
            return np.asarray(self.tradeable_model.predict_proba(matrix)[:, 1], dtype=np.float64)
        return np.clip(np.asarray(self.tradeable_model.predict(matrix), dtype=np.float64), 0.0, 1.0)

    def _predict_edge_bps(self, x: np.ndarray) -> np.ndarray:
        return np.asarray(self.edge_model.predict(np.asarray(x, dtype=np.float64)), dtype=np.float64)

    def predict_edge2stage_contract(self, x: np.ndarray) -> dict[str, np.ndarray]:
        tradeable_prob = self._predict_tradeable_prob(x)
        expected_edge_bps = self._predict_edge_bps(x)
        positive_edge_bps = np.maximum(expected_edge_bps, 0.0)
        go_score = tradeable_prob * positive_edge_bps
        trade_flag = (
            (tradeable_prob >= float(self.tradeable_prob_threshold))
            & (expected_edge_bps > float(self.net_edge_threshold_bps))
        )
        return {
            "final_tradeable_prob": tradeable_prob,
            "final_expected_net_edge_bps": expected_edge_bps,
            "final_go_score": go_score,
            "final_trade_flag": trade_flag.astype(np.int8),
        }

    def predict_panel_contract(self, x: np.ndarray) -> dict[str, np.ndarray]:
        payload = self.predict_edge2stage_contract(x)
        edge_frac = payload["final_expected_net_edge_bps"] / 10_000.0
        go_score = payload["final_go_score"]
        return {
            "final_tradeable_prob": payload["final_tradeable_prob"],
            "final_expected_net_edge_bps": payload["final_expected_net_edge_bps"],
            "final_go_score": go_score,
            "final_rank_score": go_score,
            "score_mean": go_score,
            "score_std": np.full(go_score.shape[0], np.nan, dtype=np.float64),
            "score_lcb": go_score,
            "final_expected_return": edge_frac,
            "final_expected_es": np.zeros(go_score.shape[0], dtype=np.float64),
            "final_tradability": payload["final_tradeable_prob"],
            "final_alpha_lcb": edge_frac,
            "uncertainty_available": np.zeros(go_score.shape[0], dtype=bool),
        }


def train_and_register_v6_edge2stage(options: TrainV6Edge2StageOptions) -> TrainV6Edge2StageResult:
    xgb = _try_import_xgboost()
    if xgb is None:
        raise RuntimeError("xgboost is required for trainer=v6_edge2stage")

    frame = _load_training_slice(
        dataset_root=options.dataset_root,
        start=options.start,
        end=options.end,
    )
    selected_markets = _load_selected_markets(options.dataset_root)
    operating_dates = sorted({str(item).strip() for item in frame.get_column("operating_date_kst").to_list() if str(item).strip()})
    complete_operating_dates = _resolve_complete_operating_dates(frame=frame, selected_markets=selected_markets)
    effective_operating_dates, date_selection_policy = _resolve_effective_operating_dates(
        all_operating_dates=operating_dates,
        complete_operating_dates=complete_operating_dates,
    )
    if len(effective_operating_dates) < DEFAULT_MIN_BOOTSTRAP_DATES:
        raise ValueError(
            "train_v6_edge2stage requires at least 3 effective operating dates "
            f"(found={len(effective_operating_dates)} policy={date_selection_policy})"
        )
    frame = frame.filter(pl.col("operating_date_kst").is_in(list(effective_operating_dates)))
    split = _resolve_operating_date_split(effective_operating_dates)
    train_dates = set(split["train_dates"])
    valid_dates = set(split["valid_dates"])
    test_dates = set(split["test_dates"])

    feature_columns = _load_feature_columns(options.dataset_root)
    x = frame.select(list(feature_columns)).to_numpy().astype(np.float64, copy=False)
    y_tradeable = frame.get_column("tradeable_20m").to_numpy().astype(np.int8, copy=False)
    y_edge = frame.get_column("net_edge_20m_bps").to_numpy().astype(np.float64, copy=False)
    operating_date_values = np.asarray(frame.get_column("operating_date_kst").to_list(), dtype=object)

    train_mask = np.isin(operating_date_values, list(train_dates))
    valid_mask = np.isin(operating_date_values, list(valid_dates))
    test_mask = np.isin(operating_date_values, list(test_dates))
    if not np.any(train_mask) or not np.any(valid_mask) or not np.any(test_mask):
        raise ValueError("train_v6_edge2stage requires non-empty train/valid/test splits")

    scale_pos_weight = _resolve_scale_pos_weight(y_tradeable[train_mask])
    stage_a = xgb.XGBClassifier(
        objective="binary:logistic",
        tree_method="hist",
        n_estimators=400,
        learning_rate=0.05,
        max_depth=5,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_lambda=2.0,
        reg_alpha=0.25,
        random_state=int(options.seed),
        nthread=max(int(options.nthread), 1),
        scale_pos_weight=float(scale_pos_weight),
        eval_metric="logloss",
    )
    stage_a.fit(x[train_mask], y_tradeable[train_mask])

    stage_b_train_mask = train_mask & (
        (y_tradeable == 1)
        | ((y_edge >= float(options.hard_negative_low_bps)) & (y_edge <= float(options.hard_negative_high_bps)))
        | _flat_negative_sample_mask(
            y_tradeable=y_tradeable,
            y_edge=y_edge,
            seed=int(options.seed),
            keep_prob=float(options.flat_negative_downsample),
        )
    )
    if not np.any(stage_b_train_mask):
        raise ValueError("train_v6_edge2stage stage B received no training rows")
    stage_b = xgb.XGBRegressor(
        objective="reg:squarederror",
        tree_method="hist",
        n_estimators=400,
        learning_rate=0.05,
        max_depth=5,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_lambda=2.0,
        reg_alpha=0.25,
        random_state=int(options.seed + 1),
        nthread=max(int(options.nthread), 1),
    )
    stage_b.fit(x[stage_b_train_mask], y_edge[stage_b_train_mask])

    estimator = V6Edge2StageEstimator(
        tradeable_model=stage_a,
        edge_model=stage_b,
        feature_names=feature_columns,
        tradeable_prob_threshold=float(options.tradeable_prob_threshold),
        net_edge_threshold_bps=float(options.net_edge_threshold_bps),
    )

    valid_payload = estimator.predict_edge2stage_contract(x[valid_mask])
    test_payload = estimator.predict_edge2stage_contract(x[test_mask])
    valid_metrics = _build_edge2stage_metrics(
        y_tradeable=y_tradeable[valid_mask],
        y_edge_bps=y_edge[valid_mask],
        payload=valid_payload,
    )
    test_metrics = _build_edge2stage_metrics(
        y_tradeable=y_tradeable[test_mask],
        y_edge_bps=y_edge[test_mask],
        payload=test_payload,
    )

    run_id = make_run_id(seed=options.seed)
    thresholds = {
        "tradeable_prob_threshold": float(options.tradeable_prob_threshold),
        "net_edge_bps_threshold": float(options.net_edge_threshold_bps),
        "hard_negative_low_bps": float(options.hard_negative_low_bps),
        "hard_negative_high_bps": float(options.hard_negative_high_bps),
        "flat_negative_downsample": float(options.flat_negative_downsample),
    }
    leaderboard_row = {
        "run_id": run_id,
        "created_at_utc": _utc_now(),
        "model_family": options.model_family,
        "champion": "edge2stage_xgboost",
        "champion_backend": "xgboost_two_stage",
        "test_pr_auc": float((test_metrics.get("stage_a") or {}).get("pr_auc") or 0.0),
        "test_roc_auc": float((test_metrics.get("stage_a") or {}).get("roc_auc") or 0.0),
        "test_edge_mae_bps": float((test_metrics.get("stage_b") or {}).get("mae_bps") or 0.0),
        "test_edge_rmse_bps": float((test_metrics.get("stage_b") or {}).get("rmse_bps") or 0.0),
        "test_go_top10_mean_true_edge_bps": float((test_metrics.get("joint") or {}).get("top10_mean_true_edge_bps") or 0.0),
        "test_tradeable_pass_ratio": float((test_metrics.get("joint") or {}).get("tradeable_pass_ratio") or 0.0),
        "test_no_trade_ratio": float((test_metrics.get("joint") or {}).get("no_trade_ratio") or 0.0),
        "rows_train": int(np.sum(train_mask)),
        "rows_valid": int(np.sum(valid_mask)),
        "rows_test": int(np.sum(test_mask)),
    }

    feature_spec = {
        "policy": "market_state_training_slice_v1_feature_spec",
        "dataset_root": str(options.dataset_root),
        "feature_columns": list(feature_columns),
    }
    label_spec = {
        "policy": "v6_edge2stage_label_contract_v1",
        "classification_label": "tradeable_20m",
        "regression_label": "net_edge_20m_bps",
        "auxiliary_labels": ["net_edge_10m_bps", "net_edge_40m_bps"],
    }
    train_config = {
        **asdict(options),
        "dataset_root": str(options.dataset_root),
        "registry_root": str(options.registry_root),
        "logs_root": str(options.logs_root),
        "trainer": "v6_edge2stage",
        "feature_columns": list(feature_columns),
        "operating_date_split": split,
        "date_selection_policy": str(date_selection_policy),
        "all_operating_dates": list(operating_dates),
        "complete_operating_dates": list(complete_operating_dates),
        "effective_operating_dates": list(effective_operating_dates),
        "selected_markets": list(selected_markets),
        "autobot_version": autobot_version,
    }
    data_fingerprint = _build_data_fingerprint(
        dataset_root=options.dataset_root,
        start=options.start,
        end=options.end,
        rows_total=int(frame.height),
        selected_markets=list(selected_markets),
    )
    predictor_contract = {
        "version": 1,
        "policy": "v6_edge2stage_predictor_contract_v1",
        "tradeable_prob_field": "final_tradeable_prob",
        "expected_net_edge_bps_field": "final_expected_net_edge_bps",
        "go_score_field": "final_go_score",
        "decision_rule": "trade if p_tradeable>=0.55 and expected_net_edge_bps>3.0",
        "feature_columns": list(feature_columns),
    }
    selection_policy = normalize_selection_policy(
        {
            "mode": "raw_threshold",
            "score_source": "score_mean",
            "threshold_key": "edge2stage_default",
            "tradeable_prob_min": float(options.tradeable_prob_threshold),
            "expected_net_edge_bps_min": float(options.net_edge_threshold_bps),
        },
        fallback_threshold_key="edge2stage_default",
    )
    selection_calibration = _identity_calibration(reason="EDGE2STAGE_IDENTITY_CALIBRATION")
    selection_recommendations = {
        "version": 1,
        "recommended_threshold_key": "edge2stage_default",
        "by_threshold_key": {
            "edge2stage_default": {
                "tradeable_prob_min": float(options.tradeable_prob_threshold),
                "expected_net_edge_bps_min": float(options.net_edge_threshold_bps),
                "recommended_min_candidates_per_ts": 1,
                "recommended_top_pct": 0.0,
            }
        },
    }
    runtime_recommendations = normalize_runtime_recommendations_payload(
        {
            "status": "edge2stage_train_ready",
            "source_family": options.model_family,
            "tradeable_prob_threshold": float(options.tradeable_prob_threshold),
            "net_edge_bps_threshold": float(options.net_edge_threshold_bps),
            "decision_rule": "trade if p_tradeable>=0.55 and expected_net_edge_bps>3.0",
        }
    )
    model_card = render_model_card(
        run_id=run_id,
        model_family=options.model_family,
        champion="edge2stage_xgboost",
        metrics={"valid_metrics": valid_metrics, "champion_metrics": test_metrics},
        thresholds=thresholds,
        data_fingerprint=data_fingerprint,
    )
    run_dir = save_run(
        RegistrySavePayload(
            registry_root=options.registry_root,
            model_family=options.model_family,
            run_id=run_id,
            model_bundle={"model_type": "v6_edge2stage", "estimator": estimator},
            metrics={"valid_metrics": valid_metrics, "champion_metrics": test_metrics, "joint_metrics": test_metrics.get("joint", {})},
            thresholds=thresholds,
            feature_spec=feature_spec,
            label_spec=label_spec,
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
    predictor_contract_path = run_dir / "predictor_contract.json"
    predictor_contract_path.write_text(json.dumps(predictor_contract, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    metrics_payload = {
        "stage_a": valid_metrics,
        "stage_b": test_metrics.get("stage_b", {}),
        "joint": test_metrics.get("joint", {}),
        "split": split,
    }
    train_report_path = options.logs_root / "train_v6_edge2stage_report.json"
    train_report_path.parent.mkdir(parents=True, exist_ok=True)
    train_report_path.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "model_family": options.model_family,
                "run_dir": str(run_dir),
                "metrics": {"valid": valid_metrics, "test": test_metrics},
                "leaderboard_row": leaderboard_row,
                "thresholds": thresholds,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ) + "\n",
        encoding="utf-8",
    )
    return TrainV6Edge2StageResult(
        run_id=run_id,
        run_dir=run_dir,
        status="candidate",
        leaderboard_row=leaderboard_row,
        metrics={"valid": valid_metrics, "test": test_metrics},
        thresholds=thresholds,
        train_report_path=train_report_path,
        predictor_contract_path=predictor_contract_path,
    )


def _load_training_slice(*, dataset_root: Path, start: str, end: str) -> pl.DataFrame:
    root = Path(dataset_root)
    if not root.exists():
        raise FileNotFoundError(f"dataset_root not found: {root}")
    target_dates = set(_resolve_dates(start, end))
    parts = sorted(path for path in root.glob("date=*/part-*.parquet") if path.is_file() and path.parent.name.replace("date=", "", 1) in target_dates)
    if not parts:
        raise ValueError("market_state_training_slice_v1 has no parquet parts in requested window")
    frame = pl.concat([pl.read_parquet(path) for path in parts], how="vertical").sort(["bucket_start_ts_ms", "market"])
    if frame.height <= 0:
        raise ValueError("market_state_training_slice_v1 is empty in requested window")
    return frame


def _load_feature_columns(dataset_root: Path) -> tuple[str, ...]:
    payload = load_json(Path(dataset_root) / "_meta" / "feature_spec.json")
    values = tuple(str(item).strip() for item in (payload.get("feature_columns") or []) if str(item).strip())
    if not values:
        raise ValueError("training slice feature_spec.json missing feature_columns")
    return values


def _load_selected_markets(dataset_root: Path) -> tuple[str, ...]:
    payload = load_json(Path(dataset_root) / "_meta" / "feature_spec.json")
    values = tuple(str(item).strip().upper() for item in (payload.get("selected_markets") or []) if str(item).strip())
    if not values:
        raise ValueError("training slice feature_spec.json missing selected_markets")
    return values


def _resolve_operating_date_split(operating_dates: list[str]) -> dict[str, list[str]]:
    total = len(operating_dates)
    if total >= 30:
        return {
            "train_dates": operating_dates[: total - 14],
            "valid_dates": operating_dates[total - 14 : total - 7],
            "test_dates": operating_dates[total - 7 :],
        }
    if total >= 14:
        return {
            "train_dates": operating_dates[: total - 6],
            "valid_dates": operating_dates[total - 6 : total - 3],
            "test_dates": operating_dates[total - 3 :],
        }
    if total >= DEFAULT_MIN_BOOTSTRAP_DATES:
        return {
            "train_dates": operating_dates[: total - 2],
            "valid_dates": operating_dates[total - 2 : total - 1],
            "test_dates": operating_dates[total - 1 :],
        }
    raise ValueError("train_v6_edge2stage requires at least 3 operating dates")


def _resolve_complete_operating_dates(*, frame: pl.DataFrame, selected_markets: tuple[str, ...]) -> list[str]:
    expected_market_count = len(tuple(selected_markets))
    if expected_market_count <= 0:
        return []
    counts = (
        frame.group_by("operating_date_kst")
        .agg(pl.col("market").n_unique().alias("market_count"))
        .filter(pl.col("market_count") >= int(expected_market_count))
        .sort("operating_date_kst")
    )
    return [str(item).strip() for item in counts.get_column("operating_date_kst").to_list()]


def _resolve_effective_operating_dates(*, all_operating_dates: list[str], complete_operating_dates: list[str]) -> tuple[list[str], str]:
    if len(all_operating_dates) >= DEFAULT_MIN_STANDARD_DATES:
        return list(all_operating_dates), "all_dates_standard"
    return list(complete_operating_dates), "complete_dates_only_until_adequate"


def _resolve_scale_pos_weight(y: np.ndarray) -> float:
    values = np.asarray(y, dtype=np.int64)
    positives = int(np.sum(values == 1))
    negatives = int(np.sum(values == 0))
    if positives <= 0 or negatives <= 0:
        return 1.0
    return float(negatives) / float(positives)


def _flat_negative_sample_mask(*, y_tradeable: np.ndarray, y_edge: np.ndarray, seed: int, keep_prob: float) -> np.ndarray:
    y_cls = np.asarray(y_tradeable, dtype=np.int8)
    edge = np.asarray(y_edge, dtype=np.float64)
    rng = np.random.default_rng(int(seed))
    mask = (y_cls == 0) & ((edge < DEFAULT_HARD_NEGATIVE_LOW_BPS) | ~np.isfinite(edge))
    draws = rng.random(mask.shape[0]) < float(max(min(keep_prob, 1.0), 0.0))
    return mask & draws


def _build_edge2stage_metrics(*, y_tradeable: np.ndarray, y_edge_bps: np.ndarray, payload: dict[str, np.ndarray]) -> dict[str, Any]:
    y_cls = np.asarray(y_tradeable, dtype=np.int8)
    y_edge = np.asarray(y_edge_bps, dtype=np.float64)
    p_tradeable = np.asarray(payload["final_tradeable_prob"], dtype=np.float64)
    pred_edge = np.asarray(payload["final_expected_net_edge_bps"], dtype=np.float64)
    go_score = np.asarray(payload["final_go_score"], dtype=np.float64)
    stage_a = classification_metrics(y_cls, p_tradeable)
    mae = float(np.mean(np.abs(pred_edge - y_edge)))
    rmse = float(np.sqrt(np.mean(np.square(pred_edge - y_edge))))
    positive_mask = y_edge > 0.0
    directional_hit = float(np.mean((pred_edge[positive_mask] > 0.0).astype(np.float64))) if np.any(positive_mask) else 0.0
    trade_mask = (p_tradeable >= DEFAULT_TRADEABLE_PROB_THRESHOLD) & (pred_edge > DEFAULT_NET_EDGE_THRESHOLD_BPS)
    no_trade_ratio = 1.0 - float(np.mean(trade_mask.astype(np.float64)))
    tradeable_pass_ratio = float(np.mean(trade_mask.astype(np.float64)))
    top_count = max(int(len(go_score) * 0.10), 1)
    top_idx = np.argsort(go_score)[-top_count:]
    top10_mean_true_edge = float(np.mean(y_edge[top_idx])) if top_idx.size > 0 else 0.0
    return {
        "stage_a": stage_a,
        "stage_b": {
            "mae_bps": mae,
            "rmse_bps": rmse,
            "directional_hit_positive_edge": directional_hit,
        },
        "joint": {
            "no_trade_ratio": no_trade_ratio,
            "tradeable_pass_ratio": tradeable_pass_ratio,
            "expected_edge_bps_mean": float(np.mean(pred_edge)),
            "top10_mean_true_edge_bps": top10_mean_true_edge,
        },
    }


def _resolve_dates(start: str, end: str) -> tuple[str, ...]:
    from autobot.data.micro.raw_readers import parse_date_range

    return parse_date_range(start=start, end=end)


def _build_data_fingerprint(
    *,
    dataset_root: Path,
    start: str,
    end: str,
    rows_total: int,
    selected_markets: list[str],
) -> dict[str, Any]:
    feature_spec_path = Path(dataset_root) / "_meta" / "feature_spec.json"
    label_spec_path = Path(dataset_root) / "_meta" / "label_spec.json"
    manifest_path = Path(dataset_root) / "_meta" / "manifest.parquet"
    return {
        "dataset_root": str(dataset_root),
        "start": start,
        "end": end,
        "rows_total": int(rows_total),
        "selected_markets": list(selected_markets),
        "feature_spec_sha256": _sha256_file(feature_spec_path),
        "label_spec_sha256": _sha256_file(label_spec_path),
        "manifest_sha256": _sha256_file(manifest_path),
    }


def _sha256_file(path: Path) -> str:
    if not path.exists():
        return ""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
