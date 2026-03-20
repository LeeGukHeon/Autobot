"""Training/evaluation orchestration for T14 model registry v1."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import time
from typing import Any

import numpy as np
import polars as pl
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import SGDClassifier
from sklearn.preprocessing import StandardScaler
import yaml

from autobot import __version__ as autobot_version
from autobot.data import expected_interval_ms
from autobot.features.feature_spec import parse_date_to_ts_ms

from .dataset_loader import (
    DatasetRequest,
    build_data_fingerprint,
    build_dataset_request,
    feature_columns_from_spec,
    load_feature_dataset,
    load_feature_spec,
    load_label_spec,
)
from .metrics import (
    classification_metrics,
    ev_optimal_threshold,
    grouped_trading_metrics,
    summarize_grouped_top5,
    top_p_threshold,
    trading_metrics,
)
from .model_card import render_model_card
from .registry import (
    RegistrySavePayload,
    list_runs,
    load_json,
    load_model_bundle,
    make_run_id,
    resolve_run_dir,
    save_run,
    update_champion_pointer,
)
from .split import SPLIT_TEST, SPLIT_TRAIN, SPLIT_VALID, compute_time_splits, split_masks


@dataclass(frozen=True)
class TrainRunOptions:
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
    run_baseline: bool
    run_booster: bool
    booster_sweep_trials: int
    seed: int
    nthread: int
    batch_rows: int
    train_ratio: float
    valid_ratio: float
    test_ratio: float
    embargo_bars: int
    baseline_alpha: float
    baseline_epochs: int
    fee_bps_est: float
    safety_bps: float
    ev_scan_steps: int
    ev_min_selected: int
    gate_min_pr_auc: float
    gate_min_precision_top5: float
    gate_max_two_market_bias: float


@dataclass(frozen=True)
class TrainRunResult:
    run_id: str
    run_dir: Path
    champion: str
    leaderboard_row: dict[str, Any]
    metrics: dict[str, Any]
    thresholds: dict[str, Any]
    train_report_path: Path


def load_train_defaults(config_dir: Path, *, base_config: dict[str, Any] | None = None) -> dict[str, Any]:
    base = base_config if isinstance(base_config, dict) else {}
    root = _load_yaml_doc(config_dir / "train.yaml")
    train_cfg = root.get("train", root) if isinstance(root, dict) else {}
    train_cfg = train_cfg if isinstance(train_cfg, dict) else {}
    storage_cfg = base.get("storage", {}) if isinstance(base.get("storage"), dict) else {}

    return {
        "registry_root": str(train_cfg.get("registry_root", "models/registry")),
        "logs_root": str(train_cfg.get("logs_root", "logs")),
        "model_family": str(train_cfg.get("model_family", "train_v1")),
        "tf": str(train_cfg.get("tf", "5m")),
        "quote": str(train_cfg.get("quote", "KRW")),
        "top_n": int(train_cfg.get("top_n", 20)),
        "start": str(train_cfg.get("start", "2024-01-01")),
        "end": str(train_cfg.get("end", "2026-03-01")),
        "task": str(train_cfg.get("task", "cls")),
        "run_baseline": bool(train_cfg.get("run_baseline", True)),
        "run_booster": bool(train_cfg.get("run_booster", True)),
        "booster_sweep_trials": int(train_cfg.get("booster_sweep_trials", 15)),
        "seed": int(train_cfg.get("seed", 42)),
        "nthread": int(train_cfg.get("nthread", 6)),
        "batch_rows": int(train_cfg.get("batch_rows", 200_000)),
        "train_ratio": float(train_cfg.get("train_ratio", 0.70)),
        "valid_ratio": float(train_cfg.get("valid_ratio", 0.15)),
        "test_ratio": float(train_cfg.get("test_ratio", 0.15)),
        "embargo_bars": int(train_cfg.get("embargo_bars", 12)),
        "baseline_alpha": float(train_cfg.get("baseline_alpha", 0.0001)),
        "baseline_epochs": int(train_cfg.get("baseline_epochs", 3)),
        "fee_bps_est": float(train_cfg.get("fee_bps_est", 10.0)),
        "safety_bps": float(train_cfg.get("safety_bps", 5.0)),
        "ev_scan_steps": int(train_cfg.get("ev_scan_steps", 200)),
        "ev_min_selected": int(train_cfg.get("ev_min_selected", 100)),
        "gate_min_pr_auc": float(train_cfg.get("gate_min_pr_auc", 0.50)),
        "gate_min_precision_top5": float(train_cfg.get("gate_min_precision_top5", 0.50)),
        "gate_max_two_market_bias": float(train_cfg.get("gate_max_two_market_bias", 0.60)),
        "storage_features_dir": str(storage_cfg.get("features_dir", "data/features")),
    }


def _load_yaml_doc(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    return raw if isinstance(raw, dict) else {}


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def train_and_register(options: TrainRunOptions) -> TrainRunResult:
    if not options.run_baseline and not options.run_booster:
        raise ValueError("at least one of run_baseline/run_booster must be true")
    if options.task != "cls":
        raise ValueError("task currently supports only 'cls'")

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
    x_valid = dataset.X[valid_mask]
    y_valid = dataset.y_cls[valid_mask]
    x_test = dataset.X[test_mask]
    y_test = dataset.y_cls[test_mask]
    y_reg_valid = dataset.y_reg[valid_mask]
    y_reg_test = dataset.y_reg[test_mask]
    market_valid = dataset.markets[valid_mask]
    market_test = dataset.markets[test_mask]

    candidates: dict[str, dict[str, Any]] = {}
    sweep_records: list[dict[str, Any]] = []

    if options.run_baseline:
        baseline = _fit_baseline(
            x_train=x_train,
            y_train=y_train,
            alpha=options.baseline_alpha,
            epochs=options.baseline_epochs,
            batch_rows=options.batch_rows,
            seed=options.seed,
        )
        valid_scores = _predict_scores(baseline, x_valid)
        test_scores = _predict_scores(baseline, x_test)
        candidates["baseline"] = {
            "bundle": baseline,
            "backend": "sgd_logistic",
            "params": {
                "alpha": float(options.baseline_alpha),
                "epochs": int(options.baseline_epochs),
            },
            "valid_scores": valid_scores,
            "test_scores": test_scores,
            "valid_metrics": _evaluate_split(
                y_cls=y_valid,
                y_reg=y_reg_valid,
                scores=valid_scores,
                markets=market_valid,
                fee_bps_est=options.fee_bps_est,
                safety_bps=options.safety_bps,
            ),
            "test_metrics": _evaluate_split(
                y_cls=y_test,
                y_reg=y_reg_test,
                scores=test_scores,
                markets=market_test,
                fee_bps_est=options.fee_bps_est,
                safety_bps=options.safety_bps,
            ),
        }

    if options.run_booster:
        booster = _fit_booster_sweep(
            x_train=x_train,
            y_train=y_train,
            x_valid=x_valid,
            y_valid=y_valid,
            y_reg_valid=y_reg_valid,
            fee_bps_est=options.fee_bps_est,
            safety_bps=options.safety_bps,
            seed=options.seed,
            nthread=options.nthread,
            trials=max(int(options.booster_sweep_trials), 1),
        )
        valid_scores = _predict_scores(booster["bundle"], x_valid)
        test_scores = _predict_scores(booster["bundle"], x_test)
        candidates["booster"] = {
            "bundle": booster["bundle"],
            "backend": booster["backend"],
            "params": booster["best_params"],
            "valid_scores": valid_scores,
            "test_scores": test_scores,
            "valid_metrics": _evaluate_split(
                y_cls=y_valid,
                y_reg=y_reg_valid,
                scores=valid_scores,
                markets=market_valid,
                fee_bps_est=options.fee_bps_est,
                safety_bps=options.safety_bps,
            ),
            "test_metrics": _evaluate_split(
                y_cls=y_test,
                y_reg=y_reg_test,
                scores=test_scores,
                markets=market_test,
                fee_bps_est=options.fee_bps_est,
                safety_bps=options.safety_bps,
            ),
        }
        sweep_records = booster["trials"]

    champion_name, champion = _select_champion(candidates)
    thresholds = _build_thresholds(
        valid_scores=champion["valid_scores"],
        y_reg_valid=y_reg_valid,
        fee_bps_est=options.fee_bps_est,
        safety_bps=options.safety_bps,
        ev_scan_steps=options.ev_scan_steps,
        ev_min_selected=options.ev_min_selected,
    )

    recommendation = _t13_1_recommendation(
        champion_metrics=champion["test_metrics"],
        gate_min_pr_auc=options.gate_min_pr_auc,
        gate_min_precision_top5=options.gate_min_precision_top5,
        gate_max_two_market_bias=options.gate_max_two_market_bias,
    )
    metrics = _build_metrics_doc(
        run_id=run_id,
        options=options,
        split_info=split_info,
        interval_ms=interval_ms,
        rows=rows,
        candidates=candidates,
        champion_name=champion_name,
        champion=champion,
        sweep_records=sweep_records,
        recommendation=recommendation,
    )
    leaderboard_row = _make_leaderboard_row(
        run_id=run_id,
        options=options,
        champion_name=champion_name,
        champion_payload=champion,
        rows=rows,
    )

    data_fingerprint = build_data_fingerprint(
        request=request,
        selected_markets=dataset.selected_markets,
        total_rows=dataset.rows,
    )
    data_fingerprint["code_version"] = autobot_version

    model_card = render_model_card(
        run_id=run_id,
        model_family=options.model_family,
        champion=champion_name,
        metrics=metrics,
        thresholds=thresholds,
        data_fingerprint=data_fingerprint,
    )
    train_config = _train_config_snapshot(
        options=options,
        feature_cols=dataset.feature_names,
        markets=dataset.selected_markets,
    )

    run_dir = save_run(
        RegistrySavePayload(
            registry_root=options.registry_root,
            model_family=options.model_family,
            run_id=run_id,
            model_bundle=champion["bundle"],
            metrics=metrics,
            thresholds=thresholds,
            feature_spec=feature_spec,
            label_spec=label_spec,
            train_config=train_config,
            data_fingerprint=data_fingerprint,
            leaderboard_row=leaderboard_row,
            model_card_text=model_card,
        )
    )
    update_champion_pointer(
        options.registry_root,
        options.model_family,
        run_id=run_id,
        score=float(leaderboard_row.get("test_precision_top5", 0.0)),
        score_key="test_precision_top5",
    )

    finished_at = time.time()
    report_path = _write_train_report(
        options.logs_root,
        {
            "run_id": run_id,
            "started_at_utc": datetime.fromtimestamp(started_at, tz=timezone.utc).isoformat(),
            "finished_at_utc": datetime.fromtimestamp(finished_at, tz=timezone.utc).isoformat(),
            "duration_sec": round(finished_at - started_at, 3),
            "rows": rows,
            "memory_estimate_mb": _estimate_dataset_memory_mb(dataset),
            "sweep_trials": sweep_records,
            "champion": leaderboard_row,
            "recommendation": recommendation,
        },
    )
    return TrainRunResult(
        run_id=run_id,
        run_dir=run_dir,
        champion=champion_name,
        leaderboard_row=leaderboard_row,
        metrics=metrics,
        thresholds=thresholds,
        train_report_path=report_path,
    )


def evaluate_registered_model(
    *,
    registry_root: Path,
    model_ref: str,
    model_family: str | None = None,
    split: str = "test",
    report_csv: Path | None = None,
) -> dict[str, Any]:
    split_name = str(split).strip().lower()
    if split_name not in {SPLIT_TRAIN, SPLIT_VALID, SPLIT_TEST}:
        raise ValueError("split must be one of train|valid|test")

    run_dir = resolve_run_dir(registry_root, model_ref=model_ref, model_family=model_family)
    train_config, dataset, label_columns = _load_registered_eval_dataset(run_dir=run_dir)

    labels, _ = compute_time_splits(
        dataset.ts_ms,
        train_ratio=float(train_config.get("train_ratio", 0.70)),
        valid_ratio=float(train_config.get("valid_ratio", 0.15)),
        test_ratio=float(train_config.get("test_ratio", 0.15)),
        embargo_bars=int(train_config.get("embargo_bars", 12)),
        interval_ms=expected_interval_ms(str(train_config.get("tf", "5m")).strip().lower()),
    )
    mask = split_masks(labels)[split_name]

    bundle = load_model_bundle(run_dir)
    scores = _predict_scores(bundle, dataset.X[mask])
    result = _evaluate_split(
        y_cls=dataset.y_cls[mask],
        y_reg=dataset.y_reg[mask],
        scores=scores,
        markets=dataset.markets[mask],
        fee_bps_est=float(train_config.get("fee_bps_est", 10.0)),
        safety_bps=float(train_config.get("safety_bps", 5.0)),
    )
    result["run_dir"] = str(run_dir)
    result["split"] = split_name
    result["label_columns"] = label_columns

    if report_csv is not None:
        frame = pl.DataFrame(
            {
                "ts_ms": dataset.ts_ms[mask],
                "market": dataset.markets[mask].astype(str, copy=False),
                "y_cls": dataset.y_cls[mask],
                "y_reg": dataset.y_reg[mask],
                "score": scores.astype(np.float64, copy=False),
            }
        )
        report_csv.parent.mkdir(parents=True, exist_ok=True)
        frame.write_csv(report_csv)
        result["report_csv"] = str(report_csv)
    return result


def _load_registered_eval_dataset(
    *,
    run_dir: Path,
    start_ts_ms: int | None = None,
    end_ts_ms: int | None = None,
) -> tuple[dict[str, Any], Any, dict[str, str]]:
    train_config = load_json(run_dir / "train_config.yaml")
    if not train_config:
        raise ValueError(f"invalid train_config.yaml at {run_dir}")

    request = DatasetRequest(
        dataset_root=Path(str(train_config.get("dataset_root", ""))),
        tf=str(train_config.get("tf", "5m")).strip().lower(),
        quote=str(train_config.get("quote", "KRW")).strip().upper(),
        top_n=int(train_config.get("top_n", 20)),
        start_ts_ms=(int(start_ts_ms) if start_ts_ms is not None else int(train_config.get("start_ts_ms"))),
        end_ts_ms=(int(end_ts_ms) if end_ts_ms is not None else int(train_config.get("end_ts_ms"))),
        markets=tuple(str(item).strip().upper() for item in train_config.get("markets", []) if str(item).strip()),
        batch_rows=max(int(train_config.get("batch_rows", 200_000)), 1),
    )
    feature_cols = tuple(str(item) for item in train_config.get("feature_columns", []))
    label_columns = _resolve_registered_label_columns(train_config)
    dataset = load_feature_dataset(
        request,
        feature_columns=feature_cols if feature_cols else None,
        y_cls_column=label_columns["y_cls"],
        y_reg_column=label_columns["y_reg"],
    )
    return train_config, dataset, label_columns


def _resolve_registered_label_columns(train_config: dict[str, Any]) -> dict[str, str]:
    y_cls_name = str(train_config.get("y_cls_column", "")).strip()
    y_reg_name = str(train_config.get("y_reg_column", "")).strip()
    label_columns = train_config.get("label_columns", [])
    if isinstance(label_columns, list):
        for value in label_columns:
            text = str(value).strip()
            if not text:
                continue
            if not y_cls_name and text.startswith("y_cls"):
                y_cls_name = text
            if not y_reg_name and text.startswith("y_reg"):
                y_reg_name = text
    return {
        "y_cls": y_cls_name or "y_cls",
        "y_reg": y_reg_name or "y_reg",
    }


def list_registered_models(*, registry_root: Path, model_family: str | None = None) -> list[dict[str, Any]]:
    return list_runs(registry_root, model_family=model_family)


def show_registered_model(*, registry_root: Path, model_ref: str, model_family: str | None = None) -> dict[str, Any]:
    run_dir = resolve_run_dir(registry_root, model_ref=model_ref, model_family=model_family)
    return {
        "run_dir": str(run_dir),
        "leaderboard_row": load_json(run_dir / "leaderboard_row.json"),
        "metrics": load_json(run_dir / "metrics.json"),
        "thresholds": load_json(run_dir / "thresholds.json"),
        "selection_recommendations": load_json(run_dir / "selection_recommendations.json"),
        "data_fingerprint": load_json(run_dir / "data_fingerprint.json"),
    }


def _build_metrics_doc(
    *,
    run_id: str,
    options: TrainRunOptions,
    split_info: Any,
    interval_ms: int,
    rows: dict[str, int],
    candidates: dict[str, dict[str, Any]],
    champion_name: str,
    champion: dict[str, Any],
    sweep_records: list[dict[str, Any]],
    recommendation: dict[str, Any],
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "created_at_utc": _utc_now(),
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
        "baseline": _extract_model_metric_block(candidates.get("baseline")),
        "booster": _extract_model_metric_block(candidates.get("booster")),
        "booster_sweep": {"trials": len(sweep_records), "records": sweep_records},
        "champion": {
            "name": champion_name,
            "backend": champion["backend"],
            "params": champion["params"],
        },
        "champion_metrics": champion["test_metrics"],
        "t13_1_recommendation": recommendation,
    }


def _fit_baseline(
    *,
    x_train: np.ndarray,
    y_train: np.ndarray,
    alpha: float,
    epochs: int,
    batch_rows: int,
    seed: int,
) -> dict[str, Any]:
    scaler = StandardScaler(copy=True)
    for start in range(0, x_train.shape[0], max(int(batch_rows), 1)):
        end = min(start + max(int(batch_rows), 1), x_train.shape[0])
        scaler.partial_fit(x_train[start:end])

    classifier = SGDClassifier(
        loss="log_loss",
        alpha=float(alpha),
        max_iter=1,
        tol=None,
        random_state=int(seed),
        learning_rate="optimal",
    )
    classes = np.array([0, 1], dtype=np.int8)
    rng = np.random.default_rng(int(seed))
    class_weights = _balanced_class_weights(y_train)
    for _ in range(max(int(epochs), 1)):
        indices = rng.permutation(x_train.shape[0])
        for start in range(0, indices.size, max(int(batch_rows), 1)):
            idx = indices[start : start + max(int(batch_rows), 1)]
            x_chunk = scaler.transform(x_train[idx])
            y_chunk = y_train[idx]
            weights = np.where(y_chunk == 1, class_weights[1], class_weights[0])
            classifier.partial_fit(x_chunk, y_chunk, classes=classes, sample_weight=weights)

    return {"model_type": "sgd_logistic", "scaler": scaler, "estimator": classifier}


def _fit_booster_sweep(
    *,
    x_train: np.ndarray,
    y_train: np.ndarray,
    x_valid: np.ndarray,
    y_valid: np.ndarray,
    y_reg_valid: np.ndarray,
    fee_bps_est: float,
    safety_bps: float,
    seed: int,
    nthread: int,
    trials: int,
) -> dict[str, Any]:
    rng = np.random.default_rng(int(seed))
    xgb = _try_import_xgboost()
    backend = "xgboost" if xgb is not None else "hgb"

    trial_rows: list[dict[str, Any]] = []
    best_key: tuple[float, float, float] | None = None
    best_bundle: dict[str, Any] | None = None
    best_params: dict[str, Any] = {}
    for trial in range(max(int(trials), 1)):
        if xgb is not None:
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
            fit_kwargs = {"eval_set": [(x_valid, y_valid)], "verbose": False}
            try:
                estimator.fit(x_train, y_train, early_stopping_rounds=50, **fit_kwargs)
            except TypeError:
                estimator.fit(x_train, y_train, **fit_kwargs)
        else:
            params = _sample_hgb_params(rng)
            estimator = HistGradientBoostingClassifier(
                learning_rate=params["learning_rate"],
                max_depth=params["max_depth"],
                max_leaf_nodes=params["max_leaf_nodes"],
                min_samples_leaf=params["min_samples_leaf"],
                l2_regularization=params["l2_regularization"],
                max_iter=params["max_iter"],
                random_state=int(seed + trial),
                early_stopping=True,
            )
            estimator.fit(x_train, y_train)

        valid_scores = estimator.predict_proba(x_valid)[:, 1]
        cls = classification_metrics(y_valid, valid_scores)
        trade = trading_metrics(
            y_valid,
            y_reg_valid,
            valid_scores,
            fee_bps_est=fee_bps_est,
            safety_bps=safety_bps,
        )
        top5 = trade.get("top_5pct", {})
        key = (
            float(top5.get("precision", 0.0)),
            float(cls.get("pr_auc") or 0.0),
            float(cls.get("roc_auc") or 0.0),
        )
        trial_rows.append(
            {
                "trial": trial,
                "backend": backend,
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
            best_bundle = {"model_type": backend, "scaler": None, "estimator": estimator}

    if best_bundle is None:
        raise RuntimeError("booster sweep failed to produce a model")
    return {
        "bundle": best_bundle,
        "best_params": best_params,
        "backend": backend,
        "trials": trial_rows,
    }


def _predict_scores(model_bundle: dict[str, Any], x: np.ndarray) -> np.ndarray:
    model_type = str(model_bundle.get("model_type", "")).strip().lower()
    estimator = model_bundle.get("estimator")
    if estimator is None:
        raise ValueError("model bundle missing estimator")

    if model_type == "sgd_logistic":
        scaler = model_bundle.get("scaler")
        if scaler is None:
            raise ValueError("sgd bundle missing scaler")
        transformed = scaler.transform(x)
        return estimator.predict_proba(transformed)[:, 1].astype(np.float64, copy=False)
    if hasattr(estimator, "predict_proba"):
        return estimator.predict_proba(x)[:, 1].astype(np.float64, copy=False)
    if hasattr(estimator, "predict"):
        raw = np.asarray(estimator.predict(x), dtype=np.float64)
        return 1.0 / (1.0 + np.exp(-raw))
    raise ValueError(f"unsupported estimator score interface for model_type='{model_type}'")


def _evaluate_split(
    *,
    y_cls: np.ndarray,
    y_reg: np.ndarray,
    scores: np.ndarray,
    markets: np.ndarray,
    fee_bps_est: float,
    safety_bps: float,
    sample_weight: np.ndarray | None = None,
) -> dict[str, Any]:
    cls = classification_metrics(y_cls, scores, sample_weight=sample_weight)
    trading = trading_metrics(
        y_cls,
        y_reg,
        scores,
        fee_bps_est=fee_bps_est,
        safety_bps=safety_bps,
        sample_weight=sample_weight,
    )
    per_market = grouped_trading_metrics(
        markets=markets,
        y_true=y_cls,
        y_reg=y_reg,
        scores=scores,
        fee_bps_est=fee_bps_est,
        safety_bps=safety_bps,
        sample_weight=sample_weight,
    )
    return {
        "rows": int(y_cls.size),
        "classification": cls,
        "trading": trading,
        "per_market": per_market,
        "per_market_summary": summarize_grouped_top5(per_market),
    }


def _build_thresholds(
    *,
    valid_scores: np.ndarray,
    y_reg_valid: np.ndarray,
    fee_bps_est: float,
    safety_bps: float,
    ev_scan_steps: int,
    ev_min_selected: int,
    sample_weight: np.ndarray | None = None,
) -> dict[str, float | int]:
    ev = ev_optimal_threshold(
        y_reg_valid,
        valid_scores,
        fee_bps_est=fee_bps_est,
        safety_bps=safety_bps,
        scan_steps=ev_scan_steps,
        min_selected=ev_min_selected,
        sample_weight=sample_weight,
    )
    return {
        "top_1pct": float(top_p_threshold(valid_scores, 0.01)),
        "top_5pct": float(top_p_threshold(valid_scores, 0.05)),
        "top_10pct": float(top_p_threshold(valid_scores, 0.10)),
        "ev_opt": float(ev.get("threshold", 1.0)),
        "ev_opt_ev_net": float(ev.get("ev_net", 0.0)),
        "ev_opt_selected_rows": int(ev.get("selected_rows", 0)),
    }


def build_selection_recommendations(
    *,
    valid_scores: np.ndarray,
    valid_ts_ms: np.ndarray,
    thresholds: dict[str, Any],
    min_ts_coverage: float = 0.85,
    min_one_pick_coverage: float = 0.75,
) -> dict[str, Any]:
    scores = np.asarray(valid_scores, dtype=np.float64)
    ts_ms = np.asarray(valid_ts_ms, dtype=np.int64)
    if scores.size <= 0 or ts_ms.size <= 0 or scores.size != ts_ms.size:
        return {}

    total_rows = int(scores.size)
    ev_selected_rows = max(int(_safe_int_metric(thresholds.get("ev_opt_selected_rows"))), 0)
    by_key: dict[str, Any] = {}
    for threshold_key in ("top_1pct", "top_5pct", "top_10pct", "ev_opt"):
        threshold_value = _safe_optional_float_metric(thresholds.get(threshold_key))
        if threshold_value is None:
            continue
        eligible_mask = scores >= float(threshold_value)
        eligible_rows = int(np.sum(eligible_mask))
        positive_counts = _group_counts_by_ts(ts_ms[eligible_mask])
        coverage_count = _quantile_int(positive_counts, float(min_one_pick_coverage))
        coverage_floor = (1.0 / float(coverage_count)) if coverage_count > 0 else 1.0
        density_ratio = 1.0
        if threshold_key != "ev_opt" and ev_selected_rows > 0 and eligible_rows > 0:
            density_ratio = max(min(float(ev_selected_rows) / float(eligible_rows), 1.0), 0.0)
        recommended_top_pct = 1.0
        if eligible_rows > 0:
            recommended_top_pct = max(density_ratio, coverage_floor, 1.0 / float(eligible_rows))
        recommended_top_pct = max(min(float(recommended_top_pct), 1.0), 0.0)
        recommended_min_candidates = _recommend_min_candidates_for_ts_counts(
            positive_counts,
            coverage_target=float(min_ts_coverage),
        )
        min_candidates_coverage = (
            float(np.mean(positive_counts >= recommended_min_candidates)) if positive_counts.size > 0 else 0.0
        )
        by_key[threshold_key] = {
            "threshold": float(threshold_value),
            "eligible_rows": eligible_rows,
            "eligible_ratio": (float(eligible_rows) / float(total_rows)) if total_rows > 0 else 0.0,
            "eligible_ts_nonzero": int(positive_counts.size),
            "eligible_ts_count_quantile_for_min_one_pick": int(coverage_count),
            "recommended_top_pct": float(recommended_top_pct),
            "recommended_min_candidates_per_ts": int(recommended_min_candidates),
            "recommended_min_candidates_coverage": float(min_candidates_coverage),
            "top_pct_source": (
                "ev_opt_relative_to_threshold_plus_ts_coverage_floor"
                if threshold_key != "ev_opt"
                else "identity_threshold_with_ts_coverage_floor"
            ),
            "min_candidates_source": "ts_coverage_target",
        }
    return {
        "version": 1,
        "created_at_utc": _utc_now(),
        "min_ts_coverage_target": float(min_ts_coverage),
        "min_one_pick_coverage_target": float(min_one_pick_coverage),
        "by_threshold_key": by_key,
    }


def _safe_optional_float_metric(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int_metric(value: Any) -> int:
    try:
        if value is None:
            return 0
        return int(value)
    except (TypeError, ValueError):
        return 0


def _select_champion(candidates: dict[str, dict[str, Any]]) -> tuple[str, dict[str, Any]]:
    if not candidates:
        raise ValueError("no model candidates available")

    best_name = ""
    best_payload: dict[str, Any] | None = None
    best_key: tuple[float, float, float] | None = None
    for name, payload in candidates.items():
        test_metrics = payload.get("test_metrics", {})
        top5 = (test_metrics.get("trading", {}) or {}).get("top_5pct", {})
        cls = test_metrics.get("classification", {})
        key = (
            float(top5.get("precision", 0.0)),
            float(cls.get("pr_auc") or 0.0),
            float(cls.get("roc_auc") or 0.0),
        )
        if best_key is None or key > best_key:
            best_key = key
            best_name = name
            best_payload = payload
    if best_payload is None:
        raise RuntimeError("champion selection failed")
    return best_name, best_payload


def _extract_model_metric_block(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if payload is None:
        return None
    return {
        "backend": payload.get("backend"),
        "params": payload.get("params"),
        "valid": payload.get("valid_metrics"),
        "test": payload.get("test_metrics"),
    }


def _group_counts_by_ts(ts_ms: np.ndarray) -> np.ndarray:
    values = np.asarray(ts_ms, dtype=np.int64)
    if values.size <= 0:
        return np.array([], dtype=np.int64)
    _, counts = np.unique(values, return_counts=True)
    return counts.astype(np.int64, copy=False)


def _quantile_int(values: np.ndarray, quantile: float) -> int:
    arr = np.asarray(values, dtype=np.int64)
    if arr.size <= 0:
        return 1
    return max(int(np.ceil(float(np.quantile(arr, min(max(quantile, 0.0), 1.0))))), 1)


def _recommend_min_candidates_for_ts_counts(values: np.ndarray, *, coverage_target: float) -> int:
    arr = np.asarray(values, dtype=np.int64)
    if arr.size <= 0:
        return 1
    target = min(max(float(coverage_target), 0.0), 1.0)
    best = 1
    max_value = int(np.max(arr))
    for candidate in range(1, max_value + 1):
        coverage = float(np.mean(arr >= candidate))
        if coverage >= target:
            best = candidate
        else:
            break
    return max(int(best), 1)


def _make_leaderboard_row(
    *,
    run_id: str,
    options: TrainRunOptions,
    champion_name: str,
    champion_payload: dict[str, Any],
    rows: dict[str, int],
) -> dict[str, Any]:
    test_metrics = champion_payload.get("test_metrics", {})
    cls = test_metrics.get("classification", {})
    top5 = (test_metrics.get("trading", {}) or {}).get("top_5pct", {})
    return {
        "run_id": run_id,
        "created_at_utc": _utc_now(),
        "model_family": options.model_family,
        "champion": champion_name,
        "champion_backend": champion_payload.get("backend"),
        "test_roc_auc": float(cls.get("roc_auc") or 0.0),
        "test_pr_auc": float(cls.get("pr_auc") or 0.0),
        "test_log_loss": float(cls.get("log_loss") or 0.0),
        "test_brier_score": float(cls.get("brier_score") or 0.0),
        "test_precision_top5": float(top5.get("precision", 0.0)),
        "test_ev_net_top5": float(top5.get("ev_net", 0.0)),
        "rows_train": int(rows.get("train", 0)),
        "rows_valid": int(rows.get("valid", 0)),
        "rows_test": int(rows.get("test", 0)),
    }


def _t13_1_recommendation(
    *,
    champion_metrics: dict[str, Any],
    gate_min_pr_auc: float,
    gate_min_precision_top5: float,
    gate_max_two_market_bias: float,
) -> dict[str, Any]:
    reasons: list[str] = []
    cls = champion_metrics.get("classification", {}) if isinstance(champion_metrics, dict) else {}
    trade = champion_metrics.get("trading", {}) if isinstance(champion_metrics, dict) else {}
    top5 = trade.get("top_5pct", {}) if isinstance(trade, dict) else {}
    summary = champion_metrics.get("per_market_summary", {}) if isinstance(champion_metrics, dict) else {}

    if float(cls.get("pr_auc") or 0.0) < float(gate_min_pr_auc):
        reasons.append("PR_AUC_BELOW_GATE")
    if float(top5.get("precision", 0.0)) < float(gate_min_precision_top5):
        reasons.append("PRECISION_TOP5_BELOW_GATE")
    if float(top5.get("ev_net", 0.0)) < 0.0:
        reasons.append("EV_TOP5_NEGATIVE")
    market_count = int(summary.get("market_count", 0))
    positive_markets = int(summary.get("positive_markets", 0))
    if market_count >= 3:
        ratio = float(positive_markets) / float(market_count)
        if ratio < (1.0 - float(gate_max_two_market_bias)):
            reasons.append("PER_MARKET_CONCENTRATION_HIGH")
    return {
        "trigger_t13_1": bool(reasons),
        "reasons": reasons,
        "next_data_targets": ["upbit_orderbook_ws", "upbit_trade_ws"] if reasons else [],
    }


def _train_config_snapshot(
    *,
    options: TrainRunOptions,
    feature_cols: tuple[str, ...],
    markets: tuple[str, ...],
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
    return payload


def _balanced_class_weights(y_train: np.ndarray) -> dict[int, float]:
    y = np.asarray(y_train, dtype=np.int8)
    positives = int(np.sum(y == 1))
    negatives = int(np.sum(y == 0))
    total = positives + negatives
    if positives <= 0 or negatives <= 0:
        return {0: 1.0, 1: 1.0}
    return {
        0: float(total) / float(2 * negatives),
        1: float(total) / float(2 * positives),
    }


def _sample_xgb_params(rng: np.random.Generator) -> dict[str, Any]:
    return {
        "learning_rate": float(rng.uniform(0.03, 0.20)),
        "max_depth": int(rng.integers(4, 9)),
        "subsample": float(rng.uniform(0.60, 1.0)),
        "colsample_bytree": float(rng.uniform(0.60, 1.0)),
        "min_child_weight": float(rng.uniform(1.0, 8.0)),
        "reg_lambda": float(rng.uniform(0.5, 8.0)),
        "reg_alpha": float(rng.uniform(0.0, 2.0)),
        "max_bin": int(rng.choice(np.array([256, 384, 512], dtype=np.int64))),
    }


def _sample_hgb_params(rng: np.random.Generator) -> dict[str, Any]:
    return {
        "learning_rate": float(rng.uniform(0.03, 0.20)),
        "max_depth": int(rng.integers(3, 9)),
        "max_leaf_nodes": int(rng.integers(15, 63)),
        "min_samples_leaf": int(rng.integers(20, 120)),
        "l2_regularization": float(rng.uniform(0.0, 2.0)),
        "max_iter": int(rng.integers(150, 500)),
    }


def _try_import_xgboost() -> Any | None:
    try:
        import xgboost as xgb  # type: ignore
    except Exception:
        return None
    return xgb


def _validate_split_counts(masks: dict[str, np.ndarray]) -> None:
    for name in (SPLIT_TRAIN, SPLIT_VALID, SPLIT_TEST):
        if int(np.sum(masks[name])) <= 0:
            raise ValueError(f"{name} split has no rows after embargo")


def _estimate_dataset_memory_mb(dataset: Any) -> float:
    total_bytes = int(dataset.X.nbytes + dataset.y_cls.nbytes + dataset.y_reg.nbytes + dataset.ts_ms.nbytes)
    return float(total_bytes) / (1024.0 * 1024.0)


def _write_train_report(logs_root: Path, payload: dict[str, Any]) -> Path:
    logs_root.mkdir(parents=True, exist_ok=True)
    path = logs_root / "train_report.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path
