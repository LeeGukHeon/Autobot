"""Training orchestration for trainer=v3_mtf_micro."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import time
from typing import Any

import numpy as np

from autobot import __version__ as autobot_version
from autobot.data import expected_interval_ms
from autobot.features.feature_spec import parse_date_to_ts_ms

from .dataset_loader import (
    build_data_fingerprint,
    build_dataset_request,
    feature_columns_from_spec,
    load_feature_dataset,
    load_feature_spec,
    load_label_spec,
)
from .model_card import render_model_card
from .registry import (
    RegistrySavePayload,
    load_json,
    make_run_id,
    save_run,
    update_latest_candidate_pointer,
)
from .split import SPLIT_TEST, SPLIT_TRAIN, SPLIT_VALID, compute_time_splits, split_masks
from .train_v1 import (
    build_selection_recommendations,
    _build_thresholds,
    _estimate_dataset_memory_mb,
    _evaluate_split,
    _predict_scores,
    _sample_xgb_params,
    _try_import_xgboost,
    _validate_split_counts,
)


@dataclass(frozen=True)
class TrainV3MtfMicroOptions:
    dataset_root: Path
    registry_root: Path
    logs_root: Path
    model_family: str
    tf: str
    quote: str
    top_n: int
    start: str
    end: str
    feature_set: str
    label_set: str
    task: str
    booster_sweep_trials: int
    seed: int
    nthread: int
    batch_rows: int
    train_ratio: float
    valid_ratio: float
    test_ratio: float
    embargo_bars: int
    fee_bps_est: float
    safety_bps: float
    ev_scan_steps: int
    ev_min_selected: int
    min_rows_for_train: int = 5000


@dataclass(frozen=True)
class TrainV3MtfMicroResult:
    run_id: str
    run_dir: Path
    status: str
    leaderboard_row: dict[str, Any]
    metrics: dict[str, Any]
    thresholds: dict[str, Any]
    train_report_path: Path
    promotion_path: Path


def train_and_register_v3_mtf_micro(options: TrainV3MtfMicroOptions) -> TrainV3MtfMicroResult:
    if options.task != "cls":
        raise ValueError("task currently supports only 'cls'")
    if options.feature_set != "v3":
        raise ValueError("trainer v3_mtf_micro requires --feature-set v3")
    if options.label_set != "v1":
        raise ValueError("trainer v3_mtf_micro requires --label-set v1")
    if _try_import_xgboost() is None:
        raise RuntimeError("xgboost is required for trainer=v3_mtf_micro")

    started_at = time.time()
    run_id = make_run_id(seed=options.seed)
    request = build_dataset_request(
        dataset_root=options.dataset_root,
        tf=options.tf,
        quote=options.quote,
        top_n=options.top_n,
        start=options.start,
        end=options.end,
        batch_rows=options.batch_rows,
    )
    feature_spec = load_feature_spec(options.dataset_root)
    label_spec = load_label_spec(options.dataset_root)
    feature_cols = feature_columns_from_spec(options.dataset_root)
    dataset = load_feature_dataset(request, feature_columns=feature_cols)
    if dataset.rows < int(options.min_rows_for_train):
        raise ValueError(
            f"NEED_MORE_MICRO_DAYS_OR_LOOSEN_UNIVERSE: rows={dataset.rows} < min_rows_for_train={int(options.min_rows_for_train)}"
        )

    interval_ms = expected_interval_ms(options.tf)
    labels, split_info = compute_time_splits(
        dataset.ts_ms,
        train_ratio=options.train_ratio,
        valid_ratio=options.valid_ratio,
        test_ratio=options.test_ratio,
        embargo_bars=options.embargo_bars,
        interval_ms=interval_ms,
    )
    masks = split_masks(labels)
    _validate_split_counts(masks)

    train_mask = masks[SPLIT_TRAIN]
    valid_mask = masks[SPLIT_VALID]
    test_mask = masks[SPLIT_TEST]
    rows = {
        "total": dataset.rows,
        "train": int(np.sum(train_mask)),
        "valid": int(np.sum(valid_mask)),
        "test": int(np.sum(test_mask)),
        "drop": int(np.sum(masks["drop"])),
    }

    x_train = dataset.X[train_mask]
    y_train = dataset.y_cls[train_mask]
    w_train = dataset.sample_weight[train_mask]
    x_valid = dataset.X[valid_mask]
    y_valid = dataset.y_cls[valid_mask]
    w_valid = dataset.sample_weight[valid_mask]
    x_test = dataset.X[test_mask]
    y_test = dataset.y_cls[test_mask]
    y_reg_valid = dataset.y_reg[valid_mask]
    y_reg_test = dataset.y_reg[test_mask]
    market_valid = dataset.markets[valid_mask]
    market_test = dataset.markets[test_mask]

    booster = _fit_booster_sweep_weighted(
        x_train=x_train,
        y_train=y_train,
        w_train=w_train,
        x_valid=x_valid,
        y_valid=y_valid,
        w_valid=w_valid,
        y_reg_valid=y_reg_valid,
        fee_bps_est=options.fee_bps_est,
        safety_bps=options.safety_bps,
        seed=options.seed,
        nthread=options.nthread,
        trials=max(int(options.booster_sweep_trials), 1),
    )

    valid_scores = _predict_scores(booster["bundle"], x_valid)
    test_scores = _predict_scores(booster["bundle"], x_test)
    valid_metrics = _evaluate_split(
        y_cls=y_valid,
        y_reg=y_reg_valid,
        scores=valid_scores,
        markets=market_valid,
        fee_bps_est=options.fee_bps_est,
        safety_bps=options.safety_bps,
    )
    test_metrics = _evaluate_split(
        y_cls=y_test,
        y_reg=y_reg_test,
        scores=test_scores,
        markets=market_test,
        fee_bps_est=options.fee_bps_est,
        safety_bps=options.safety_bps,
    )

    thresholds = _build_thresholds(
        valid_scores=valid_scores,
        y_reg_valid=y_reg_valid,
        fee_bps_est=options.fee_bps_est,
        safety_bps=options.safety_bps,
        ev_scan_steps=options.ev_scan_steps,
        ev_min_selected=options.ev_min_selected,
    )
    selection_recommendations = build_selection_recommendations(
        valid_scores=valid_scores,
        valid_ts_ms=dataset.ts_ms[valid_mask],
        thresholds=thresholds,
    )
    metrics = _build_v3_metrics_doc(
        run_id=run_id,
        options=options,
        split_info=split_info,
        interval_ms=interval_ms,
        rows=rows,
        valid_metrics=valid_metrics,
        test_metrics=test_metrics,
        best_params=dict(booster.get("best_params", {})),
        sweep_records=list(booster.get("trials", [])),
    )
    leaderboard_row = _make_v3_leaderboard_row(
        run_id=run_id,
        options=options,
        rows=rows,
        test_metrics=test_metrics,
    )

    data_fingerprint = build_data_fingerprint(
        request=request,
        selected_markets=dataset.selected_markets,
        total_rows=dataset.rows,
    )
    data_fingerprint["code_version"] = autobot_version
    data_fingerprint["sample_weight"] = {
        "enabled": True,
        "min": float(np.nanmin(dataset.sample_weight)) if dataset.sample_weight.size > 0 else 0.0,
        "max": float(np.nanmax(dataset.sample_weight)) if dataset.sample_weight.size > 0 else 0.0,
        "mean": float(np.nanmean(dataset.sample_weight)) if dataset.sample_weight.size > 0 else 0.0,
    }

    model_card = render_model_card(
        run_id=run_id,
        model_family=options.model_family,
        champion="booster",
        metrics=metrics,
        thresholds=thresholds,
        data_fingerprint=data_fingerprint,
    )
    train_config = _train_config_snapshot_v3(
        options=options,
        feature_cols=dataset.feature_names,
        markets=dataset.selected_markets,
        selection_recommendations=selection_recommendations,
    )

    run_dir = save_run(
        RegistrySavePayload(
            registry_root=options.registry_root,
            model_family=options.model_family,
            run_id=run_id,
            model_bundle=booster["bundle"],
            metrics=metrics,
            thresholds=thresholds,
            feature_spec=feature_spec,
            label_spec=label_spec,
            train_config=train_config,
            data_fingerprint=data_fingerprint,
            leaderboard_row=leaderboard_row,
            model_card_text=model_card,
            selection_recommendations=selection_recommendations,
        )
    )
    update_latest_candidate_pointer(
        options.registry_root,
        options.model_family,
        run_id,
    )
    update_latest_candidate_pointer(
        options.registry_root,
        "_global",
        run_id,
        family=options.model_family,
    )
    promotion = _manual_promotion_decision_v3(options=options, run_id=run_id)
    promotion_path = run_dir / "promotion_decision.json"
    promotion_path.write_text(
        json.dumps(promotion, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    status = str(promotion.get("status", "candidate")).strip() or "candidate"

    finished_at = time.time()
    report_path = _write_train_report_v3(
        options.logs_root,
        {
            "run_id": run_id,
            "status": status,
            "started_at_utc": datetime.fromtimestamp(started_at, tz=timezone.utc).isoformat(),
            "finished_at_utc": datetime.fromtimestamp(finished_at, tz=timezone.utc).isoformat(),
            "duration_sec": round(finished_at - started_at, 3),
            "rows": rows,
            "memory_estimate_mb": _estimate_dataset_memory_mb(dataset),
            "sweep_trials": booster.get("trials", []),
            "candidate": leaderboard_row,
            "promotion": promotion,
            "selection_recommendations": selection_recommendations,
        },
    )

    return TrainV3MtfMicroResult(
        run_id=run_id,
        run_dir=run_dir,
        status=status,
        leaderboard_row=leaderboard_row,
        metrics=metrics,
        thresholds=thresholds,
        train_report_path=report_path,
        promotion_path=promotion_path,
    )


def _fit_booster_sweep_weighted(
    *,
    x_train: np.ndarray,
    y_train: np.ndarray,
    w_train: np.ndarray,
    x_valid: np.ndarray,
    y_valid: np.ndarray,
    w_valid: np.ndarray,
    y_reg_valid: np.ndarray,
    fee_bps_est: float,
    safety_bps: float,
    seed: int,
    nthread: int,
    trials: int,
) -> dict[str, Any]:
    xgb = _try_import_xgboost()
    if xgb is None:
        raise RuntimeError("xgboost is required for trainer=v3_mtf_micro")

    rng = np.random.default_rng(int(seed))
    trial_rows: list[dict[str, Any]] = []
    best_key: tuple[float, float, float] | None = None
    best_bundle: dict[str, Any] | None = None
    best_params: dict[str, Any] = {}

    w_train_safe = np.asarray(w_train, dtype=np.float64)
    w_valid_safe = np.asarray(w_valid, dtype=np.float64)
    if w_train_safe.size != y_train.size:
        w_train_safe = np.ones(y_train.size, dtype=np.float64)
    if w_valid_safe.size != y_valid.size:
        w_valid_safe = np.ones(y_valid.size, dtype=np.float64)
    w_train_safe = np.clip(w_train_safe, 1e-6, None)
    w_valid_safe = np.clip(w_valid_safe, 1e-6, None)

    for trial in range(max(int(trials), 1)):
        params = _sample_xgb_params(rng)
        estimator = xgb.XGBClassifier(
            objective="binary:logistic",
            tree_method="hist",
            n_estimators=1200,
            learning_rate=params["learning_rate"],
            max_depth=params["max_depth"],
            subsample=params["subsample"],
            colsample_bytree=params["colsample_bytree"],
            min_child_weight=params["min_child_weight"],
            reg_lambda=params["reg_lambda"],
            reg_alpha=params["reg_alpha"],
            max_bin=params["max_bin"],
            random_state=int(seed + trial),
            nthread=max(int(nthread), 1),
            eval_metric="logloss",
        )
        fit_kwargs = {
            "sample_weight": w_train_safe,
            "eval_set": [(x_valid, y_valid)],
            "sample_weight_eval_set": [w_valid_safe],
            "verbose": False,
        }
        try:
            estimator.fit(x_train, y_train, early_stopping_rounds=50, **fit_kwargs)
        except TypeError:
            estimator.fit(x_train, y_train, sample_weight=w_train_safe, eval_set=[(x_valid, y_valid)], verbose=False)

        valid_scores = estimator.predict_proba(x_valid)[:, 1]
        valid_metrics = _evaluate_split(
            y_cls=y_valid,
            y_reg=y_reg_valid,
            scores=valid_scores,
            markets=np.array(["_ALL_"] * int(y_valid.size), dtype=object),
            fee_bps_est=fee_bps_est,
            safety_bps=safety_bps,
        )
        cls = valid_metrics.get("classification", {}) if isinstance(valid_metrics, dict) else {}
        top5 = (valid_metrics.get("trading", {}) or {}).get("top_5pct", {})
        key = (
            float(top5.get("precision", 0.0)),
            float(cls.get("pr_auc") or 0.0),
            float(cls.get("roc_auc") or 0.0),
        )
        trial_rows.append(
            {
                "trial": trial,
                "backend": "xgboost",
                "params": params,
                "selection_key": {
                    "precision_top5": key[0],
                    "pr_auc": key[1],
                    "roc_auc": key[2],
                },
            }
        )
        if best_key is None or key > best_key:
            best_key = key
            best_params = params
            best_bundle = {"model_type": "xgboost", "scaler": None, "estimator": estimator}

    if best_bundle is None:
        raise RuntimeError("booster sweep failed to produce a model")
    return {
        "bundle": best_bundle,
        "best_params": best_params,
        "backend": "xgboost",
        "trials": trial_rows,
    }


def _build_v3_metrics_doc(
    *,
    run_id: str,
    options: TrainV3MtfMicroOptions,
    split_info: Any,
    interval_ms: int,
    rows: dict[str, int],
    valid_metrics: dict[str, Any],
    test_metrics: dict[str, Any],
    best_params: dict[str, Any],
    sweep_records: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "created_at_utc": _utc_now(),
        "trainer": "v3_mtf_micro",
        "model_family": options.model_family,
        "tf": options.tf,
        "quote": options.quote,
        "top_n": options.top_n,
        "time_range": {
            "start": options.start,
            "end": options.end,
            "start_ts_ms": parse_date_to_ts_ms(options.start),
            "end_ts_ms": parse_date_to_ts_ms(options.end, end_of_day=True),
        },
        "split_policy": {
            "train_ratio": options.train_ratio,
            "valid_ratio": options.valid_ratio,
            "test_ratio": options.test_ratio,
            "embargo_bars": options.embargo_bars,
            "interval_ms": interval_ms,
            "valid_start_ts": split_info.valid_start_ts,
            "test_start_ts": split_info.test_start_ts,
            "counts": split_info.counts,
        },
        "rows": rows,
        "fee_policy_bps": {
            "fee_bps_est": options.fee_bps_est,
            "safety_bps": options.safety_bps,
        },
        "booster": {
            "backend": "xgboost",
            "params": best_params,
            "valid": valid_metrics,
            "test": test_metrics,
        },
        "booster_sweep": {"trials": len(sweep_records), "records": sweep_records},
        "champion": {
            "name": "booster",
            "backend": "xgboost",
            "params": best_params,
        },
        "champion_metrics": test_metrics,
    }


def _make_v3_leaderboard_row(
    *,
    run_id: str,
    options: TrainV3MtfMicroOptions,
    rows: dict[str, int],
    test_metrics: dict[str, Any],
) -> dict[str, Any]:
    cls = test_metrics.get("classification", {}) if isinstance(test_metrics, dict) else {}
    top5 = (test_metrics.get("trading", {}) or {}).get("top_5pct", {})
    return {
        "run_id": run_id,
        "created_at_utc": _utc_now(),
        "model_family": options.model_family,
        "trainer": "v3_mtf_micro",
        "champion": "booster",
        "champion_backend": "xgboost",
        "test_roc_auc": _safe_float(cls.get("roc_auc")),
        "test_pr_auc": _safe_float(cls.get("pr_auc")),
        "test_log_loss": _safe_float(cls.get("log_loss")),
        "test_brier_score": _safe_float(cls.get("brier_score")),
        "test_precision_top5": _safe_float(top5.get("precision")),
        "test_ev_net_top5": _safe_float(top5.get("ev_net")),
        "rows_train": int(rows.get("train", 0)),
        "rows_valid": int(rows.get("valid", 0)),
        "rows_test": int(rows.get("test", 0)),
    }


def _train_config_snapshot_v3(
    *,
    options: TrainV3MtfMicroOptions,
    feature_cols: tuple[str, ...],
    markets: tuple[str, ...],
    selection_recommendations: dict[str, Any],
) -> dict[str, Any]:
    payload = asdict(options)
    payload["dataset_root"] = str(options.dataset_root)
    payload["registry_root"] = str(options.registry_root)
    payload["logs_root"] = str(options.logs_root)
    payload["feature_columns"] = list(feature_cols)
    payload["markets"] = list(markets)
    payload["start_ts_ms"] = parse_date_to_ts_ms(options.start)
    payload["end_ts_ms"] = parse_date_to_ts_ms(options.end, end_of_day=True)
    payload["created_at_utc"] = _utc_now()
    payload["autobot_version"] = autobot_version
    payload["trainer"] = "v3_mtf_micro"
    payload["selection_recommendations"] = selection_recommendations
    return payload


def _write_train_report_v3(logs_root: Path, payload: dict[str, Any]) -> Path:
    logs_root.mkdir(parents=True, exist_ok=True)
    path = logs_root / "train_v3_report.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _manual_promotion_decision_v3(
    *,
    options: TrainV3MtfMicroOptions,
    run_id: str,
) -> dict[str, Any]:
    champion_doc = load_json(options.registry_root / options.model_family / "champion.json")
    champion_run_id = str(champion_doc.get("run_id", "")).strip()
    reasons = ["MANUAL_PROMOTION_REQUIRED"]
    if not champion_run_id:
        reasons.append("NO_EXISTING_CHAMPION")
    return {
        "run_id": run_id,
        "promote": False,
        "status": "candidate",
        "promotion_mode": "manual_gate",
        "reasons": reasons,
        "checks": {
            "manual_review_required": True,
            "existing_champion_present": bool(champion_run_id),
        },
        "candidate_ref": {
            "model_ref": "latest_candidate",
            "model_family": options.model_family,
        },
    }


def _safe_float(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except Exception:
        return 0.0


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
