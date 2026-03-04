"""Training/evaluation orchestration for T14.2 v2 micro models."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import time
from typing import Any

import numpy as np
import polars as pl

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
    select_markets,
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
from .train_v1 import (
    _build_thresholds,
    _estimate_dataset_memory_mb,
    _evaluate_split,
    _fit_booster_sweep,
    _predict_scores,
    _try_import_xgboost,
    _validate_split_counts,
)


@dataclass(frozen=True)
class TrainV2MicroOptions:
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
    min_rows_for_train: int = 5_000
    min_distinct_dates: int = 7
    integrity_min_ratio: float = 0.99
    coverage_warn_ratio: float = 0.10
    coverage_strong_warn_ratio: float = 0.03
    parquet_root: Path | None = None
    base_candles_dataset: str = "auto"
    compare_baseline_family: str = "train_v1"
    promotion_min_distinct_dates: int = 30
    promotion_min_precision_delta: float = 0.02
    promotion_min_ev_delta: float = 0.0002
    promotion_required_consecutive_runs: int = 2
    promotion_per_market_precision_drop: float = -0.05
    promotion_per_market_ev_drop: float = -0.001


@dataclass(frozen=True)
class TrainV2MicroResult:
    run_id: str
    run_dir: Path
    status: str
    leaderboard_row: dict[str, Any]
    metrics: dict[str, Any]
    thresholds: dict[str, Any]
    train_report_path: Path
    compare_report_path: Path
    promotion_path: Path


def train_and_register_v2_micro(options: TrainV2MicroOptions) -> TrainV2MicroResult:
    if options.task != "cls":
        raise ValueError("task currently supports only 'cls'")
    if options.feature_set != "v2":
        raise ValueError("trainer v2_micro requires --feature-set v2")
    if options.label_set != "v1":
        raise ValueError("trainer v2_micro requires --label-set v1")
    if _try_import_xgboost() is None:
        raise RuntimeError("xgboost is required for trainer=v2_micro")

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

    preconditions = check_v2_micro_preconditions(options=options, request=request)
    if not bool(preconditions.get("ready", False)):
        reason_text = ",".join(str(item) for item in preconditions.get("fail_reasons", []))
        raise ValueError(f"T14.2 precondition failed: {reason_text}")

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
    if str(booster.get("backend")) != "xgboost":
        raise RuntimeError("trainer=v2_micro requires xgboost backend, but fallback backend was selected")

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
    metrics = _build_v2_metrics_doc(
        run_id=run_id,
        options=options,
        split_info=split_info,
        interval_ms=interval_ms,
        rows=rows,
        valid_metrics=valid_metrics,
        test_metrics=test_metrics,
        best_params=dict(booster.get("best_params", {})),
        sweep_records=list(booster.get("trials", [])),
        preconditions=preconditions,
    )
    leaderboard_row = _make_v2_leaderboard_row(
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

    model_card = render_model_card(
        run_id=run_id,
        model_family=options.model_family,
        champion="booster",
        metrics=metrics,
        thresholds=thresholds,
        data_fingerprint=data_fingerprint,
    )
    train_config = _train_config_snapshot_v2(
        options=options,
        feature_cols=dataset.feature_names,
        markets=dataset.selected_markets,
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
        )
    )

    compare_payload = _safe_compare_to_v1(options=options, run_id=run_id)
    compare_report_path = run_dir / "compare_to_v1.json"
    compare_report_path.write_text(
        json.dumps(compare_payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    promotion = _promotion_decision(
        options=options,
        run_id=run_id,
        compare_payload=compare_payload,
        distinct_dates=int(preconditions.get("distinct_dates", 0)),
    )
    if bool(promotion.get("promote", False)):
        update_champion_pointer(
            options.registry_root,
            options.model_family,
            run_id=run_id,
            score=float(leaderboard_row.get("test_precision_top5", 0.0)),
            score_key="test_precision_top5",
        )
        status = "champion"
    else:
        status = "candidate"
    promotion_path = run_dir / "promotion_decision.json"
    promotion_path.write_text(
        json.dumps(promotion, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    finished_at = time.time()
    report_path = _write_train_report_v2(
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
            "champion": leaderboard_row,
            "preconditions": preconditions,
            "compare_to_v1": compare_payload,
            "promotion": promotion,
        },
    )

    return TrainV2MicroResult(
        run_id=run_id,
        run_dir=run_dir,
        status=status,
        leaderboard_row=leaderboard_row,
        metrics=metrics,
        thresholds=thresholds,
        train_report_path=report_path,
        compare_report_path=compare_report_path,
        promotion_path=promotion_path,
    )

def check_v2_micro_preconditions(*, options: TrainV2MicroOptions, request: DatasetRequest | None = None) -> dict[str, Any]:
    req = request or build_dataset_request(
        dataset_root=options.dataset_root,
        tf=options.tf,
        quote=options.quote,
        top_n=options.top_n,
        start=options.start,
        end=options.end,
        batch_rows=options.batch_rows,
    )
    selected = select_markets(req)
    if not selected:
        return {
            "ready": False,
            "rows_total": 0,
            "distinct_dates": 0,
            "selected_markets": [],
            "integrity": {},
            "coverage": {},
            "fail_reasons": ["NO_MARKETS_SELECTED"],
            "warnings": [],
        }

    frame = _collect_window_frame(req=req, markets=tuple(selected))
    rows_total = int(frame.height)
    distinct_dates = _distinct_date_count(frame)
    integrity = _integrity_snapshot(frame=frame, min_ratio=options.integrity_min_ratio)
    coverage = _coverage_snapshot(
        options=options,
        req=req,
        selected_markets=tuple(selected),
        rows_total=rows_total,
    )

    fail_reasons: list[str] = []
    warnings: list[str] = []
    if rows_total < int(options.min_rows_for_train):
        fail_reasons.append("ROWS_BELOW_MIN")
    if distinct_dates < int(options.min_distinct_dates):
        fail_reasons.append("DISTINCT_DATES_BELOW_MIN")
    if not bool(integrity.get("candle_integrity_ok", False)):
        fail_reasons.append("CANDLE_INTEGRITY_FAIL")
    if not bool(integrity.get("label_integrity_ok", False)):
        fail_reasons.append("LABEL_INTEGRITY_FAIL")
    if not bool(integrity.get("micro_integrity_ok", False)):
        fail_reasons.append("MICRO_INTEGRITY_FAIL")
    if coverage.get("level") == "WARN":
        warnings.append("LOW_MICRO_COVERAGE")
    if coverage.get("level") == "STRONG_WARN":
        warnings.append("VERY_LOW_MICRO_COVERAGE")

    return {
        "ready": len(fail_reasons) == 0,
        "rows_total": rows_total,
        "distinct_dates": distinct_dates,
        "selected_markets": selected,
        "integrity": integrity,
        "coverage": coverage,
        "fail_reasons": fail_reasons,
        "warnings": warnings,
    }


def evaluate_registered_model_window(
    *,
    registry_root: Path,
    model_ref: str,
    model_family: str | None = None,
    split: str = "test",
    start: str | None = None,
    end: str | None = None,
    report_csv: Path | None = None,
) -> dict[str, Any]:
    split_name = str(split).strip().lower()
    if split_name not in {SPLIT_TRAIN, SPLIT_VALID, SPLIT_TEST}:
        raise ValueError("split must be one of train|valid|test")

    run_dir = resolve_run_dir(registry_root, model_ref=model_ref, model_family=model_family)
    train_config = load_json(run_dir / "train_config.yaml")
    if not train_config:
        raise ValueError(f"invalid train_config.yaml at {run_dir}")

    start_ts_ms = parse_date_to_ts_ms(start) if start else int(train_config.get("start_ts_ms"))
    end_ts_ms = parse_date_to_ts_ms(end, end_of_day=True) if end else int(train_config.get("end_ts_ms"))
    if end_ts_ms < start_ts_ms:
        raise ValueError("compare/eval end must be >= start")

    request = DatasetRequest(
        dataset_root=Path(str(train_config.get("dataset_root", ""))),
        tf=str(train_config.get("tf", "5m")).strip().lower(),
        quote=str(train_config.get("quote", "KRW")).strip().upper(),
        top_n=int(train_config.get("top_n", 20)),
        start_ts_ms=start_ts_ms,
        end_ts_ms=end_ts_ms,
        markets=tuple(str(item).strip().upper() for item in train_config.get("markets", []) if str(item).strip()),
        batch_rows=max(int(train_config.get("batch_rows", 200_000)), 1),
    )
    feature_cols = tuple(str(item) for item in train_config.get("feature_columns", []))
    dataset = load_feature_dataset(request, feature_columns=feature_cols if feature_cols else None)

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
    result["rows_in_split"] = int(np.sum(mask))
    result["time_window"] = {
        "start_ts_ms": int(start_ts_ms),
        "end_ts_ms": int(end_ts_ms),
    }

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


def compare_registered_models(
    *,
    registry_root: Path,
    a_ref: str,
    b_ref: str,
    a_family: str | None = None,
    b_family: str | None = None,
    split: str = "test",
    start: str | None = None,
    end: str | None = None,
) -> dict[str, Any]:
    result_a = evaluate_registered_model_window(
        registry_root=registry_root,
        model_ref=a_ref,
        model_family=a_family,
        split=split,
        start=start,
        end=end,
    )
    result_b = evaluate_registered_model_window(
        registry_root=registry_root,
        model_ref=b_ref,
        model_family=b_family,
        split=split,
        start=start,
        end=end,
    )

    cls_a = result_a.get("classification", {}) if isinstance(result_a, dict) else {}
    cls_b = result_b.get("classification", {}) if isinstance(result_b, dict) else {}
    top5_a = (result_a.get("trading", {}) or {}).get("top_5pct", {})
    top5_b = (result_b.get("trading", {}) or {}).get("top_5pct", {})

    per_market = _compare_per_market(
        a=result_a.get("per_market", {}),
        b=result_b.get("per_market", {}),
    )
    return {
        "a": {
            "model_ref": a_ref,
            "model_family": a_family,
            "metrics": result_a,
        },
        "b": {
            "model_ref": b_ref,
            "model_family": b_family,
            "metrics": result_b,
        },
        "split": split,
        "time_window": {
            "start": start,
            "end": end,
        },
        "delta": {
            "roc_auc": _safe_float(cls_b.get("roc_auc")) - _safe_float(cls_a.get("roc_auc")),
            "pr_auc": _safe_float(cls_b.get("pr_auc")) - _safe_float(cls_a.get("pr_auc")),
            "log_loss_improve": _safe_float(cls_a.get("log_loss")) - _safe_float(cls_b.get("log_loss")),
            "precision_top5": _safe_float(top5_b.get("precision")) - _safe_float(top5_a.get("precision")),
            "ev_net_top5": _safe_float(top5_b.get("ev_net")) - _safe_float(top5_a.get("ev_net")),
        },
        "per_market": per_market,
    }


def _build_v2_metrics_doc(
    *,
    run_id: str,
    options: TrainV2MicroOptions,
    split_info: Any,
    interval_ms: int,
    rows: dict[str, int],
    valid_metrics: dict[str, Any],
    test_metrics: dict[str, Any],
    best_params: dict[str, Any],
    sweep_records: list[dict[str, Any]],
    preconditions: dict[str, Any],
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "created_at_utc": _utc_now(),
        "trainer": "v2_micro",
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
        "preconditions": preconditions,
    }


def _make_v2_leaderboard_row(
    *,
    run_id: str,
    options: TrainV2MicroOptions,
    rows: dict[str, int],
    test_metrics: dict[str, Any],
) -> dict[str, Any]:
    cls = test_metrics.get("classification", {}) if isinstance(test_metrics, dict) else {}
    top5 = (test_metrics.get("trading", {}) or {}).get("top_5pct", {})
    return {
        "run_id": run_id,
        "created_at_utc": _utc_now(),
        "model_family": options.model_family,
        "trainer": "v2_micro",
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

def _safe_compare_to_v1(*, options: TrainV2MicroOptions, run_id: str) -> dict[str, Any]:
    try:
        return compare_registered_models(
            registry_root=options.registry_root,
            a_ref="latest",
            a_family=options.compare_baseline_family,
            b_ref=run_id,
            b_family=options.model_family,
            split="test",
            start=options.start,
            end=options.end,
        )
    except Exception as exc:
        return {
            "error": str(exc),
            "a": {"model_ref": "latest", "model_family": options.compare_baseline_family},
            "b": {"model_ref": run_id, "model_family": options.model_family},
            "split": "test",
            "time_window": {"start": options.start, "end": options.end},
        }


def _promotion_decision(
    *,
    options: TrainV2MicroOptions,
    run_id: str,
    compare_payload: dict[str, Any],
    distinct_dates: int,
) -> dict[str, Any]:
    delta = compare_payload.get("delta", {}) if isinstance(compare_payload, dict) else {}
    precision_delta = _safe_float(delta.get("precision_top5"))
    ev_delta = _safe_float(delta.get("ev_net_top5"))
    improvement = (
        precision_delta >= float(options.promotion_min_precision_delta)
        or ev_delta >= float(options.promotion_min_ev_delta)
    )

    collapsed = _collapsed_markets(
        compare_payload=compare_payload,
        precision_drop=float(options.promotion_per_market_precision_drop),
        ev_drop=float(options.promotion_per_market_ev_drop),
    )
    no_collapse = len(collapsed) == 0
    enough_dates = int(distinct_dates) >= int(options.promotion_min_distinct_dates)
    reproducible = _has_consecutive_improvements(
        registry_root=options.registry_root,
        model_family=options.model_family,
        run_id=run_id,
        required=max(int(options.promotion_required_consecutive_runs), 1),
        this_run_improved=improvement and no_collapse,
    )

    reasons: list[str] = []
    if not improvement:
        reasons.append("IMPROVEMENT_THRESHOLD_NOT_MET")
    if not no_collapse:
        reasons.append("PER_MARKET_COLLAPSE_DETECTED")
    if not enough_dates:
        reasons.append("DISTINCT_DATES_BELOW_PROMOTION_MIN")
    if not reproducible:
        reasons.append("REPRODUCIBILITY_NOT_MET")

    return {
        "run_id": run_id,
        "promote": len(reasons) == 0,
        "status": "champion" if len(reasons) == 0 else "candidate",
        "reasons": reasons,
        "checks": {
            "improvement": improvement,
            "no_per_market_collapse": no_collapse,
            "enough_distinct_dates": enough_dates,
            "reproducible": reproducible,
        },
        "thresholds": {
            "precision_delta_min": float(options.promotion_min_precision_delta),
            "ev_delta_min": float(options.promotion_min_ev_delta),
            "distinct_dates_min": int(options.promotion_min_distinct_dates),
            "required_consecutive_runs": int(options.promotion_required_consecutive_runs),
            "per_market_precision_drop": float(options.promotion_per_market_precision_drop),
            "per_market_ev_drop": float(options.promotion_per_market_ev_drop),
        },
        "delta": {
            "precision_top5": precision_delta,
            "ev_net_top5": ev_delta,
        },
        "collapsed_markets": collapsed,
        "distinct_dates": int(distinct_dates),
    }


def _has_consecutive_improvements(
    *,
    registry_root: Path,
    model_family: str,
    run_id: str,
    required: int,
    this_run_improved: bool,
) -> bool:
    if required <= 1:
        return this_run_improved
    if not this_run_improved:
        return False

    rows = list_runs(registry_root, model_family=model_family)
    run_ids = [str(item.get("run_id", "")) for item in rows if str(item.get("run_id", "")).strip()]
    seen = run_ids if run_id in run_ids else [run_id] + run_ids

    consecutive = 0
    for rid in seen:
        if rid == run_id:
            improved = this_run_improved
        else:
            decision = load_json(registry_root / model_family / rid / "promotion_decision.json")
            checks = decision.get("checks", {}) if isinstance(decision, dict) else {}
            improved = bool(checks.get("improvement")) and bool(checks.get("no_per_market_collapse"))
        if improved:
            consecutive += 1
        else:
            break
        if consecutive >= required:
            return True
    return False


def _collapsed_markets(*, compare_payload: dict[str, Any], precision_drop: float, ev_drop: float) -> list[str]:
    per_market = compare_payload.get("per_market", {}) if isinstance(compare_payload, dict) else {}
    compared = per_market.get("compared_markets", []) if isinstance(per_market, dict) else []
    collapsed: list[str] = []
    for row in compared:
        market = str(row.get("market", "")).strip().upper()
        if not market:
            continue
        precision_delta = _safe_float(row.get("precision_top5_delta"))
        ev_delta = _safe_float(row.get("ev_net_top5_delta"))
        if precision_delta <= float(precision_drop) or ev_delta <= float(ev_drop):
            collapsed.append(market)
    return sorted(set(collapsed))


def _compare_per_market(*, a: Any, b: Any) -> dict[str, Any]:
    map_a = a if isinstance(a, dict) else {}
    map_b = b if isinstance(b, dict) else {}
    common = sorted(set(map_a.keys()) & set(map_b.keys()))
    rows: list[dict[str, Any]] = []
    for market in common:
        top5_a = ((map_a.get(market, {}) or {}).get("trading", {}) or {}).get("top_5pct", {})
        top5_b = ((map_b.get(market, {}) or {}).get("trading", {}) or {}).get("top_5pct", {})
        rows.append(
            {
                "market": market,
                "precision_top5_a": _safe_float(top5_a.get("precision")),
                "precision_top5_b": _safe_float(top5_b.get("precision")),
                "precision_top5_delta": _safe_float(top5_b.get("precision")) - _safe_float(top5_a.get("precision")),
                "ev_net_top5_a": _safe_float(top5_a.get("ev_net")),
                "ev_net_top5_b": _safe_float(top5_b.get("ev_net")),
                "ev_net_top5_delta": _safe_float(top5_b.get("ev_net")) - _safe_float(top5_a.get("ev_net")),
            }
        )
    return {
        "common_market_count": len(common),
        "compared_markets": rows,
    }


def _integrity_snapshot(*, frame: pl.DataFrame, min_ratio: float) -> dict[str, Any]:
    rows = int(frame.height)
    if rows <= 0:
        return {
            "rows_total": 0,
            "candle_non_null_ratio": 0.0,
            "candle_true_ratio": 0.0,
            "candle_integrity_ok": False,
            "label_non_null_ratio": 0.0,
            "label_integrity_ok": False,
            "micro_source_valid_ratio": 0.0,
            "micro_meta_non_null_ratio": 0.0,
            "micro_dtype_ok": False,
            "micro_integrity_ok": False,
            "micro_available_rows": 0,
        }

    candle_required = [col for col in ("open", "high", "low", "close", "volume_base") if col in frame.columns]
    if candle_required:
        candle_non_null = int(frame.filter(pl.all_horizontal([pl.col(col).is_not_null() for col in candle_required])).height)
        candle_true_ratio = 1.0
    elif "candle_ok" in frame.columns:
        candle_non_null = int(frame.filter(pl.col("candle_ok").is_not_null()).height)
        candle_true_ratio = float(frame.filter(pl.col("candle_ok") == True).height) / float(rows)  # noqa: E712
    else:
        candle_non_null = 0
        candle_true_ratio = 0.0
    candle_non_null_ratio = float(candle_non_null) / float(rows)
    candle_integrity_ok = candle_non_null_ratio >= float(min_ratio) and candle_true_ratio >= float(min_ratio)

    label_cols = [col for col in ("y_reg", "y_cls") if col in frame.columns]
    label_non_null = int(frame.filter(pl.all_horizontal([pl.col(col).is_not_null() for col in label_cols])).height) if label_cols else 0
    label_non_null_ratio = float(label_non_null) / float(rows)
    label_integrity_ok = label_non_null_ratio >= float(min_ratio)

    micro_available_rows = 0
    source_valid_ratio = 0.0
    meta_non_null_ratio = 0.0
    dtype_ok = _micro_dtype_ok(frame.schema)
    if "m_micro_available" in frame.columns:
        available = frame.filter(pl.col("m_micro_available") == True)  # noqa: E712
        micro_available_rows = int(available.height)
        if micro_available_rows > 0:
            if "m_trade_source" in available.columns:
                valid_source_rows = int(
                    available.filter(
                        pl.col("m_trade_source").cast(pl.Utf8).str.to_lowercase().is_in(["ws", "rest", "none"])
                    ).height
                )
                source_valid_ratio = float(valid_source_rows) / float(micro_available_rows)
            meta_cols = [
                col
                for col in (
                    "m_trade_events",
                    "m_book_events",
                    "m_trade_coverage_ms",
                    "m_book_coverage_ms",
                    "m_micro_trade_available",
                    "m_micro_book_available",
                )
                if col in available.columns
            ]
            if meta_cols:
                meta_non_null_rows = int(
                    available.filter(pl.all_horizontal([pl.col(col).is_not_null() for col in meta_cols])).height
                )
                meta_non_null_ratio = float(meta_non_null_rows) / float(micro_available_rows)
            else:
                meta_non_null_ratio = 0.0

    micro_integrity_ok = (
        micro_available_rows > 0
        and source_valid_ratio >= float(min_ratio)
        and meta_non_null_ratio >= float(min_ratio)
        and dtype_ok
    )

    return {
        "rows_total": rows,
        "candle_non_null_ratio": candle_non_null_ratio,
        "candle_true_ratio": candle_true_ratio,
        "candle_integrity_ok": candle_integrity_ok,
        "label_non_null_ratio": label_non_null_ratio,
        "label_integrity_ok": label_integrity_ok,
        "micro_source_valid_ratio": source_valid_ratio,
        "micro_meta_non_null_ratio": meta_non_null_ratio,
        "micro_dtype_ok": dtype_ok,
        "micro_integrity_ok": micro_integrity_ok,
        "micro_available_rows": micro_available_rows,
    }

def _micro_dtype_ok(schema: dict[str, pl.DataType]) -> bool:
    str_dtype = getattr(pl, "String", pl.Utf8)
    int_types = {
        pl.Int8,
        pl.Int16,
        pl.Int32,
        pl.Int64,
        pl.UInt8,
        pl.UInt16,
        pl.UInt32,
        pl.UInt64,
    }
    checks: list[bool] = []
    if "m_trade_source" in schema:
        checks.append(schema["m_trade_source"] in {pl.Utf8, str_dtype})
    for name in ("m_trade_events", "m_book_events", "m_trade_coverage_ms", "m_book_coverage_ms"):
        if name in schema:
            checks.append(schema[name] in int_types)
    for name in ("m_micro_trade_available", "m_micro_book_available", "m_micro_available"):
        if name in schema:
            checks.append(schema[name] == pl.Boolean)
    return all(checks) if checks else False


def _coverage_snapshot(
    *,
    options: TrainV2MicroOptions,
    req: DatasetRequest,
    selected_markets: tuple[str, ...],
    rows_total: int,
) -> dict[str, Any]:
    candles_rows = _count_candle_rows(
        parquet_root=options.parquet_root,
        base_dataset=options.base_candles_dataset,
        tf=req.tf,
        markets=selected_markets,
        start_ts_ms=int(req.start_ts_ms or 0),
        end_ts_ms=int(req.end_ts_ms or 0),
    )
    ratio = (float(rows_total) / float(candles_rows)) if candles_rows > 0 else None
    level = "OK"
    if ratio is not None and ratio < float(options.coverage_strong_warn_ratio):
        level = "STRONG_WARN"
    elif ratio is not None and ratio < float(options.coverage_warn_ratio):
        level = "WARN"
    return {
        "candles_rows": candles_rows,
        "micro_coverage_ratio": ratio,
        "level": level,
        "warn_ratio": float(options.coverage_warn_ratio),
        "strong_warn_ratio": float(options.coverage_strong_warn_ratio),
    }


def _count_candle_rows(
    *,
    parquet_root: Path | None,
    base_dataset: str,
    tf: str,
    markets: tuple[str, ...],
    start_ts_ms: int,
    end_ts_ms: int,
) -> int:
    root = _resolve_base_candles_root(parquet_root=parquet_root, base_dataset=base_dataset)
    if root is None:
        return 0
    total = 0
    for market in markets:
        files = _dataset_part_files(dataset_root=root, tf=tf, market=market)
        if not files:
            continue
        frame = _collect_lazy(
            pl.scan_parquet([str(path) for path in files])
            .filter((pl.col("ts_ms") >= int(start_ts_ms)) & (pl.col("ts_ms") <= int(end_ts_ms)))
            .select(pl.len().alias("rows"))
        )
        if frame.height > 0:
            total += int(frame.item(0, "rows") or 0)
    return total


def _resolve_base_candles_root(*, parquet_root: Path | None, base_dataset: str) -> Path | None:
    if parquet_root is None:
        return None
    value = str(base_dataset).strip() or "auto"
    if value.lower() != "auto":
        path = Path(value)
        if path.exists():
            return path
        if path.is_absolute():
            return None
        candidate = parquet_root / path
        return candidate if candidate.exists() else None
    for name in ("candles_api_v1", "candles_v1"):
        candidate = parquet_root / name
        if candidate.exists():
            return candidate
    return None


def _collect_window_frame(*, req: DatasetRequest, markets: tuple[str, ...]) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    for market in markets:
        files = _dataset_part_files(dataset_root=req.dataset_root, tf=req.tf, market=market)
        if not files:
            continue
        lazy = pl.scan_parquet([str(path) for path in files])
        if req.start_ts_ms is not None:
            lazy = lazy.filter(pl.col("ts_ms") >= int(req.start_ts_ms))
        if req.end_ts_ms is not None:
            lazy = lazy.filter(pl.col("ts_ms") <= int(req.end_ts_ms))
        frame = _collect_lazy(lazy)
        if frame.height <= 0:
            continue
        frames.append(frame.with_columns(pl.lit(market).alias("__market")))
    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="vertical_relaxed")


def _dataset_part_files(*, dataset_root: Path, tf: str, market: str) -> list[Path]:
    market_dir = dataset_root / f"tf={tf}" / f"market={market}"
    if not market_dir.exists():
        return []
    direct = sorted(path for path in market_dir.glob("part-*.parquet") if path.is_file())
    if direct:
        return direct
    legacy = market_dir / "part.parquet"
    if legacy.exists():
        return [legacy]
    nested: list[Path] = []
    for date_dir in sorted(market_dir.glob("date=*")):
        if not date_dir.is_dir():
            continue
        nested.extend(path for path in sorted(date_dir.glob("*.parquet")) if path.is_file())
    return nested


def _distinct_date_count(frame: pl.DataFrame) -> int:
    if frame.height <= 0 or "ts_ms" not in frame.columns:
        return 0
    unique_days = frame.select(pl.from_epoch(pl.col("ts_ms"), time_unit="ms").dt.strftime("%Y-%m-%d").alias("d")).unique().height
    return int(unique_days)


def _train_config_snapshot_v2(
    *,
    options: TrainV2MicroOptions,
    feature_cols: tuple[str, ...],
    markets: tuple[str, ...],
) -> dict[str, Any]:
    payload = asdict(options)
    payload["dataset_root"] = str(options.dataset_root)
    payload["registry_root"] = str(options.registry_root)
    payload["logs_root"] = str(options.logs_root)
    payload["parquet_root"] = str(options.parquet_root) if options.parquet_root is not None else None
    payload["feature_columns"] = list(feature_cols)
    payload["markets"] = list(markets)
    payload["start_ts_ms"] = parse_date_to_ts_ms(options.start)
    payload["end_ts_ms"] = parse_date_to_ts_ms(options.end, end_of_day=True)
    payload["created_at_utc"] = _utc_now()
    payload["autobot_version"] = autobot_version
    payload["trainer"] = "v2_micro"
    return payload


def _write_train_report_v2(logs_root: Path, payload: dict[str, Any]) -> Path:
    logs_root.mkdir(parents=True, exist_ok=True)
    path = logs_root / "train_v2_report.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _safe_float(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except Exception:
        return 0.0


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _collect_lazy(lazy_frame: pl.LazyFrame) -> pl.DataFrame:
    try:
        return lazy_frame.collect(engine="streaming")
    except TypeError:
        return lazy_frame.collect(streaming=True)
