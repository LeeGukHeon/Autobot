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
DEFAULT_STAGE_A_LABEL = "structural_tradeable_20m"
DEFAULT_HARD_NEGATIVE_LOW_BPS = -6.0
DEFAULT_HARD_NEGATIVE_HIGH_BPS = 3.0
DEFAULT_FLAT_NEGATIVE_DOWNSAMPLE = 0.10
DEFAULT_MIN_STANDARD_DATES = 14
DEFAULT_MIN_BOOTSTRAP_DATES = 3
DEFAULT_BOOTSTRAP_MIN_TICKER_RATIO = 0.99
DEFAULT_BOOTSTRAP_MIN_TRADE_RATIO = 0.20
DEFAULT_BOOTSTRAP_MIN_BOOK_RATIO = 0.20
DEFAULT_BOOTSTRAP_MIN_LABEL_RATIO = 0.20
DEFAULT_BOOTSTRAP_MIN_PAIR_ROWS = 3_000
DEFAULT_BOOTSTRAP_MIN_USABLE_PAIRS_PER_DATE = 10


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
    stage_a_label: str = DEFAULT_STAGE_A_LABEL
    tradeable_prob_threshold: float = DEFAULT_TRADEABLE_PROB_THRESHOLD
    net_edge_threshold_bps: float = DEFAULT_NET_EDGE_THRESHOLD_BPS
    hard_negative_low_bps: float = DEFAULT_HARD_NEGATIVE_LOW_BPS
    hard_negative_high_bps: float = DEFAULT_HARD_NEGATIVE_HIGH_BPS
    flat_negative_downsample: float = DEFAULT_FLAT_NEGATIVE_DOWNSAMPLE
    bootstrap_min_ticker_ratio: float = DEFAULT_BOOTSTRAP_MIN_TICKER_RATIO
    bootstrap_min_trade_ratio: float = DEFAULT_BOOTSTRAP_MIN_TRADE_RATIO
    bootstrap_min_book_ratio: float = DEFAULT_BOOTSTRAP_MIN_BOOK_RATIO
    bootstrap_min_label_ratio: float = DEFAULT_BOOTSTRAP_MIN_LABEL_RATIO
    bootstrap_min_pair_rows: int = DEFAULT_BOOTSTRAP_MIN_PAIR_ROWS
    bootstrap_min_usable_pairs_per_date: int = DEFAULT_BOOTSTRAP_MIN_USABLE_PAIRS_PER_DATE
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


@dataclass
class _ConstantBinaryModel:
    positive_prob: float

    def predict_proba(self, x: np.ndarray) -> np.ndarray:
        rows = int(np.asarray(x).shape[0])
        positive = np.full(rows, float(self.positive_prob), dtype=np.float64)
        negative = 1.0 - positive
        return np.column_stack([negative, positive])

    def predict(self, x: np.ndarray) -> np.ndarray:
        rows = int(np.asarray(x).shape[0])
        return np.full(rows, float(self.positive_prob), dtype=np.float64)


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
    usable_pair_summary = _summarize_usable_pair_quality(frame)
    usable_pairs = _select_usable_pairs(usable_pair_summary, options=options)
    usable_operating_dates = _resolve_usable_operating_dates(
        usable_pairs=usable_pairs,
        min_pairs_per_date=int(options.bootstrap_min_usable_pairs_per_date),
    )
    effective_operating_dates, date_selection_policy = _resolve_effective_operating_dates(
        all_operating_dates=operating_dates,
        complete_operating_dates=complete_operating_dates,
        usable_operating_dates=usable_operating_dates,
    )
    if len(effective_operating_dates) < DEFAULT_MIN_BOOTSTRAP_DATES:
        raise ValueError(
            "train_v6_edge2stage requires at least 3 effective operating dates "
            f"(found={len(effective_operating_dates)} policy={date_selection_policy})"
        )
    if date_selection_policy == "usable_pairs_bootstrap_until_adequate":
        frame = frame.join(
            usable_pairs.select(["operating_date_kst", "market"]),
            on=["operating_date_kst", "market"],
            how="inner",
        )
    frame = frame.filter(pl.col("operating_date_kst").is_in(list(effective_operating_dates)))
    horizon_diagnostics = _build_horizon_diagnostics(frame)
    split = _resolve_operating_date_split(effective_operating_dates)
    train_dates = set(split["train_dates"])
    valid_dates = set(split["valid_dates"])
    test_dates = set(split["test_dates"])

    feature_columns = _load_feature_columns(options.dataset_root)
    x = frame.select(list(feature_columns)).to_numpy().astype(np.float64, copy=False)
    stage_a_label = str(options.stage_a_label).strip() or DEFAULT_STAGE_A_LABEL
    if stage_a_label not in frame.columns:
        raise ValueError(f"train_v6_edge2stage missing stage_a_label column: {stage_a_label}")
    y_stage_a = frame.get_column(stage_a_label).to_numpy().astype(np.int8, copy=False)
    y_tradeable = frame.get_column("tradeable_20m").to_numpy().astype(np.int8, copy=False)
    y_edge = frame.get_column("net_edge_20m_bps").to_numpy().astype(np.float64, copy=False)
    operating_date_values = np.asarray(frame.get_column("operating_date_kst").to_list(), dtype=object)
    label_audit = _build_label_audit(
        stage_a_label=stage_a_label,
        y_stage_a=y_stage_a,
        y_tradeable=y_tradeable,
        y_edge_bps=y_edge,
        edge_threshold_bps=float(options.net_edge_threshold_bps),
    )

    train_mask = np.isin(operating_date_values, list(train_dates))
    valid_mask = np.isin(operating_date_values, list(valid_dates))
    test_mask = np.isin(operating_date_values, list(test_dates))
    if not np.any(train_mask) or not np.any(valid_mask) or not np.any(test_mask):
        raise ValueError("train_v6_edge2stage requires non-empty train/valid/test splits")

    unique_stage_a = np.unique(y_stage_a[train_mask])
    if unique_stage_a.size < 2:
        stage_a = _ConstantBinaryModel(positive_prob=float(unique_stage_a[0]) if unique_stage_a.size == 1 else 0.0)
    else:
        scale_pos_weight = _resolve_scale_pos_weight(y_stage_a[train_mask])
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
        stage_a.fit(x[train_mask], y_stage_a[train_mask])

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

    direct_ranker = xgb.XGBRegressor(
        objective="reg:squarederror",
        tree_method="hist",
        n_estimators=400,
        learning_rate=0.05,
        max_depth=5,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_lambda=2.0,
        reg_alpha=0.25,
        random_state=int(options.seed + 2),
        nthread=max(int(options.nthread), 1),
    )
    direct_ranker.fit(x[train_mask], y_edge[train_mask])

    estimator = V6Edge2StageEstimator(
        tradeable_model=stage_a,
        edge_model=stage_b,
        feature_names=feature_columns,
        tradeable_prob_threshold=float(options.tradeable_prob_threshold),
        net_edge_threshold_bps=float(options.net_edge_threshold_bps),
    )

    valid_payload = estimator.predict_edge2stage_contract(x[valid_mask])
    test_payload = estimator.predict_edge2stage_contract(x[test_mask])
    valid_direct_ranker_payload = _build_direct_ranker_payload(
        direct_ranker.predict(x[valid_mask]),
        edge_threshold_bps=float(options.net_edge_threshold_bps),
    )
    test_direct_ranker_payload = _build_direct_ranker_payload(
        direct_ranker.predict(x[test_mask]),
        edge_threshold_bps=float(options.net_edge_threshold_bps),
    )
    valid_metrics = _build_edge2stage_metrics(
        y_stage_a=y_stage_a[valid_mask],
        y_tradeable=y_tradeable[valid_mask],
        y_edge_bps=y_edge[valid_mask],
        payload=valid_payload,
        tradeable_prob_threshold=float(options.tradeable_prob_threshold),
        edge_threshold_bps=float(options.net_edge_threshold_bps),
    )
    test_metrics = _build_edge2stage_metrics(
        y_stage_a=y_stage_a[test_mask],
        y_tradeable=y_tradeable[test_mask],
        y_edge_bps=y_edge[test_mask],
        payload=test_payload,
        tradeable_prob_threshold=float(options.tradeable_prob_threshold),
        edge_threshold_bps=float(options.net_edge_threshold_bps),
    )
    valid_challenger_metrics = _build_direct_ranker_metrics(
        y_edge_bps=y_edge[valid_mask],
        payload=valid_direct_ranker_payload,
    )
    test_challenger_metrics = _build_direct_ranker_metrics(
        y_edge_bps=y_edge[test_mask],
        payload=test_direct_ranker_payload,
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
        "test_selected_mean_true_edge_bps": float((test_metrics.get("economic") or {}).get("selected_mean_true_edge_bps") or 0.0),
        "test_false_positive_churn_ratio": float((test_metrics.get("economic") or {}).get("false_positive_churn_ratio") or 0.0),
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
        "classification_label": stage_a_label,
        "promotion_label": "tradeable_20m",
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
        "usable_operating_dates": list(usable_operating_dates),
        "effective_operating_dates": list(effective_operating_dates),
        "selected_markets": list(selected_markets),
        "usable_pair_thresholds": {
            "ticker_ratio_min": float(options.bootstrap_min_ticker_ratio),
            "trade_ratio_min": float(options.bootstrap_min_trade_ratio),
            "book_ratio_min": float(options.bootstrap_min_book_ratio),
            "label_ratio_min": float(options.bootstrap_min_label_ratio),
            "rows_min": int(options.bootstrap_min_pair_rows),
            "min_usable_pairs_per_date": int(options.bootstrap_min_usable_pairs_per_date),
        },
        "usable_pairs_by_date": (
            usable_pairs.group_by("operating_date_kst").len().sort("operating_date_kst").to_dicts()
            if usable_pairs.height > 0
            else []
        ),
        "horizon_diagnostics": horizon_diagnostics,
        "label_audit": label_audit,
        "autobot_version": autobot_version,
    }
    data_fingerprint = _build_data_fingerprint(
        dataset_root=options.dataset_root,
        start=options.start,
        end=options.end,
        rows_total=int(frame.height),
        selected_markets=list(selected_markets),
    )
    decision_rule = _build_edge2stage_decision_rule(
        stage_a_label=stage_a_label,
        tradeable_prob_threshold=float(options.tradeable_prob_threshold),
        edge_threshold_bps=float(options.net_edge_threshold_bps),
    )
    predictor_contract = {
        "version": 1,
        "policy": "v6_edge2stage_predictor_contract_v1",
        "tradeable_prob_field": "final_tradeable_prob",
        "tradeable_prob_semantics": stage_a_label,
        "expected_net_edge_bps_field": "final_expected_net_edge_bps",
        "go_score_field": "final_go_score",
        "promotion_label": "tradeable_20m",
        "decision_rule": decision_rule,
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
            "stage_a_label": stage_a_label,
            "promotion_label": "tradeable_20m",
            "tradeable_prob_threshold": float(options.tradeable_prob_threshold),
            "net_edge_bps_threshold": float(options.net_edge_threshold_bps),
            "decision_rule": decision_rule,
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
    train_report_path = options.logs_root / "train_v6_edge2stage_report.json"
    train_report_path.parent.mkdir(parents=True, exist_ok=True)
    train_report_path.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "model_family": options.model_family,
                "run_dir": str(run_dir),
                "metrics": {"valid": valid_metrics, "test": test_metrics},
                "label_audit": label_audit,
                "architecture_bakeoff": {
                    "default_candidate": {"valid": valid_metrics, "test": test_metrics},
                    "direct_ranker_challenger": {"valid": valid_challenger_metrics, "test": test_challenger_metrics},
                },
                "leaderboard_row": leaderboard_row,
                "thresholds": thresholds,
                "horizon_diagnostics": horizon_diagnostics,
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


def _summarize_usable_pair_quality(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.height <= 0:
        return pl.DataFrame(
            schema={
                "operating_date_kst": pl.Utf8,
                "market": pl.Utf8,
                "rows": pl.Int64,
                "ticker_ratio": pl.Float64,
                "trade_ratio": pl.Float64,
                "book_ratio": pl.Float64,
                "label_ratio": pl.Float64,
                "source_quality_mean": pl.Float64,
            }
        )
    return (
        frame.group_by(["operating_date_kst", "market"])
        .agg(
            [
                pl.len().cast(pl.Int64).alias("rows"),
                pl.col("ticker_available").cast(pl.Float64).mean().alias("ticker_ratio"),
                pl.col("trade_available").cast(pl.Float64).mean().alias("trade_ratio"),
                pl.col("book_available").cast(pl.Float64).mean().alias("book_ratio"),
                pl.col("label_available_20m").cast(pl.Float64).mean().alias("label_ratio"),
                pl.col("source_quality_score").mean().alias("source_quality_mean"),
            ]
        )
        .sort(["operating_date_kst", "market"])
    )


def _select_usable_pairs(summary: pl.DataFrame, *, options: TrainV6Edge2StageOptions) -> pl.DataFrame:
    if summary.height <= 0:
        return summary
    return summary.filter(
        (pl.col("ticker_ratio") >= float(options.bootstrap_min_ticker_ratio))
        & (pl.col("trade_ratio") >= float(options.bootstrap_min_trade_ratio))
        & (pl.col("book_ratio") >= float(options.bootstrap_min_book_ratio))
        & (pl.col("label_ratio") >= float(options.bootstrap_min_label_ratio))
        & (pl.col("rows") >= int(options.bootstrap_min_pair_rows))
    )


def _resolve_usable_operating_dates(*, usable_pairs: pl.DataFrame, min_pairs_per_date: int) -> list[str]:
    if usable_pairs.height <= 0:
        return []
    counts = (
        usable_pairs.group_by("operating_date_kst")
        .len()
        .filter(pl.col("len") >= int(max(min_pairs_per_date, 1)))
        .sort("operating_date_kst")
    )
    return [str(item).strip() for item in counts.get_column("operating_date_kst").to_list()]


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


def _resolve_effective_operating_dates(
    *,
    all_operating_dates: list[str],
    complete_operating_dates: list[str],
    usable_operating_dates: list[str],
) -> tuple[list[str], str]:
    if len(all_operating_dates) >= DEFAULT_MIN_STANDARD_DATES:
        return list(all_operating_dates), "all_dates_standard"
    if len(usable_operating_dates) >= DEFAULT_MIN_BOOTSTRAP_DATES:
        return list(usable_operating_dates), "usable_pairs_bootstrap_until_adequate"
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


def _build_edge2stage_metrics(
    *,
    y_stage_a: np.ndarray,
    y_tradeable: np.ndarray,
    y_edge_bps: np.ndarray,
    payload: dict[str, np.ndarray],
    tradeable_prob_threshold: float,
    edge_threshold_bps: float,
) -> dict[str, Any]:
    y_stage = np.asarray(y_stage_a, dtype=np.int8)
    y_trade = np.asarray(y_tradeable, dtype=np.int8)
    y_edge = np.asarray(y_edge_bps, dtype=np.float64)
    p_tradeable = np.asarray(payload["final_tradeable_prob"], dtype=np.float64)
    pred_edge = np.asarray(payload["final_expected_net_edge_bps"], dtype=np.float64)
    go_score = np.asarray(payload["final_go_score"], dtype=np.float64)
    stage_a = classification_metrics(y_stage, p_tradeable)
    mae = float(np.mean(np.abs(pred_edge - y_edge)))
    rmse = float(np.sqrt(np.mean(np.square(pred_edge - y_edge))))
    positive_mask = y_edge > 0.0
    directional_hit = float(np.mean((pred_edge[positive_mask] > 0.0).astype(np.float64))) if np.any(positive_mask) else 0.0
    trade_mask = (p_tradeable >= float(tradeable_prob_threshold)) & (pred_edge > float(edge_threshold_bps))
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
            "promotion_label_pass_ratio": float(np.mean(y_trade.astype(np.float64))),
        },
        "economic": _build_economic_metrics(
            y_edge_bps=y_edge,
            pred_edge_bps=pred_edge,
            score=go_score,
            trade_mask=trade_mask,
        ),
    }


def _build_direct_ranker_payload(pred_edge_bps: np.ndarray, *, edge_threshold_bps: float) -> dict[str, np.ndarray]:
    pred_edge = np.asarray(pred_edge_bps, dtype=np.float64)
    tradeable_prob = 1.0 / (1.0 + np.exp(-((pred_edge - float(edge_threshold_bps)) / 4.0)))
    go_score = np.maximum(pred_edge, 0.0)
    trade_mask = pred_edge > float(edge_threshold_bps)
    return {
        "final_tradeable_prob": tradeable_prob,
        "final_expected_net_edge_bps": pred_edge,
        "final_go_score": go_score,
        "final_trade_flag": trade_mask.astype(np.int8),
    }


def _build_direct_ranker_metrics(*, y_edge_bps: np.ndarray, payload: dict[str, np.ndarray]) -> dict[str, Any]:
    y_edge = np.asarray(y_edge_bps, dtype=np.float64)
    pred_edge = np.asarray(payload["final_expected_net_edge_bps"], dtype=np.float64)
    trade_mask = np.asarray(payload["final_trade_flag"], dtype=np.int8) == 1
    score = np.asarray(payload["final_go_score"], dtype=np.float64)
    mae = float(np.mean(np.abs(pred_edge - y_edge)))
    rmse = float(np.sqrt(np.mean(np.square(pred_edge - y_edge))))
    return {
        "regression": {
            "mae_bps": mae,
            "rmse_bps": rmse,
            "expected_edge_bps_mean": float(np.mean(pred_edge)),
        },
        "economic": _build_economic_metrics(
            y_edge_bps=y_edge,
            pred_edge_bps=pred_edge,
            score=score,
            trade_mask=trade_mask,
        ),
    }


def _build_economic_metrics(
    *,
    y_edge_bps: np.ndarray,
    pred_edge_bps: np.ndarray,
    score: np.ndarray,
    trade_mask: np.ndarray,
) -> dict[str, Any]:
    true_edge = np.asarray(y_edge_bps, dtype=np.float64)
    pred_edge = np.asarray(pred_edge_bps, dtype=np.float64)
    raw_score = np.asarray(score, dtype=np.float64)
    selected = np.asarray(trade_mask, dtype=bool)
    total_count = int(true_edge.shape[0])
    selected_count = int(np.sum(selected))
    selected_true_edge = true_edge[selected]
    selected_pred_edge = pred_edge[selected]
    top_count = max(int(total_count * 0.10), 1)
    top_idx = np.argsort(raw_score)[-top_count:] if total_count > 0 else np.asarray([], dtype=np.int64)
    selected_mean_true_edge = float(np.mean(selected_true_edge)) if selected_count > 0 else 0.0
    selected_mean_pred_edge = float(np.mean(selected_pred_edge)) if selected_count > 0 else 0.0
    false_positive_churn = float(np.mean((selected_true_edge <= 0.0).astype(np.float64))) if selected_count > 0 else 0.0
    return {
        "selected_count": selected_count,
        "selected_ratio": (float(selected_count) / float(total_count)) if total_count > 0 else 0.0,
        "no_trade_ratio": 1.0 - ((float(selected_count) / float(total_count)) if total_count > 0 else 0.0),
        "selected_mean_true_edge_bps": selected_mean_true_edge,
        "selected_total_true_edge_bps": float(np.sum(selected_true_edge)) if selected_count > 0 else 0.0,
        "selected_mean_pred_edge_bps": selected_mean_pred_edge,
        "selected_calibration_gap_bps": selected_mean_pred_edge - selected_mean_true_edge,
        "false_positive_churn_ratio": false_positive_churn,
        "top10_mean_true_edge_bps": float(np.mean(true_edge[top_idx])) if top_idx.size > 0 else 0.0,
    }


def _build_label_audit(
    *,
    stage_a_label: str,
    y_stage_a: np.ndarray,
    y_tradeable: np.ndarray,
    y_edge_bps: np.ndarray,
    edge_threshold_bps: float,
) -> dict[str, Any]:
    stage_a = np.asarray(y_stage_a, dtype=np.int8)
    tradeable = np.asarray(y_tradeable, dtype=np.int8)
    y_edge = np.asarray(y_edge_bps, dtype=np.float64)
    threshold_positive = np.where(np.isfinite(y_edge) & (y_edge > float(edge_threshold_bps)), 1, 0).astype(np.int8)
    return {
        "stage_a_label": stage_a_label,
        "promotion_label": "tradeable_20m",
        "edge_threshold_bps": float(edge_threshold_bps),
        "tradeable_vs_edge_threshold_agreement": float(np.mean((tradeable == threshold_positive).astype(np.float64))),
        "stage_a_vs_edge_threshold_agreement": float(np.mean((stage_a == threshold_positive).astype(np.float64))),
        "stage_a_vs_tradeable_agreement": float(np.mean((stage_a == tradeable).astype(np.float64))),
        "tradeable_positive_ratio": float(np.mean(tradeable.astype(np.float64))),
        "stage_a_positive_ratio": float(np.mean(stage_a.astype(np.float64))),
        "edge_threshold_positive_ratio": float(np.mean(threshold_positive.astype(np.float64))),
    }


def _build_edge2stage_decision_rule(
    *,
    stage_a_label: str,
    tradeable_prob_threshold: float,
    edge_threshold_bps: float,
) -> str:
    return (
        f"trade if p({stage_a_label})>={float(tradeable_prob_threshold):.2f} "
        f"and expected_net_edge_bps>{float(edge_threshold_bps):.1f}; "
        "promotion_label=tradeable_20m"
    )


def _build_horizon_diagnostics(frame: pl.DataFrame) -> dict[str, Any]:
    diagnostics: dict[str, Any] = {}
    for minutes in (10, 20, 40):
        column = f"net_edge_{minutes}m_bps"
        if column not in frame.columns:
            continue
        values = frame.get_column(column).cast(pl.Float64)
        finite = values.drop_nulls()
        if finite.len() <= 0:
            diagnostics[f"{minutes}m"] = {
                "rows": 0,
                "mean_bps": None,
                "median_bps": None,
                "p90_bps": None,
                "positive_ratio": None,
                "above_3bps_ratio": None,
            }
            continue
        diagnostics[f"{minutes}m"] = {
            "rows": int(finite.len()),
            "mean_bps": float(finite.mean()),
            "median_bps": float(finite.median()),
            "p90_bps": float(finite.quantile(0.90)),
            "positive_ratio": float((finite > 0.0).cast(pl.Float64).mean()),
            "above_3bps_ratio": float((finite > 3.0).cast(pl.Float64).mean()),
        }
    return diagnostics


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
