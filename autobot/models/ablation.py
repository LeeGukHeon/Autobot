"""Feature ablation runner for T14.3 diagnostics."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

from autobot.data import expected_interval_ms

from .dataset_loader import build_dataset_request, feature_columns_from_spec, load_feature_dataset
from .split import SPLIT_TEST, SPLIT_TRAIN, SPLIT_VALID, compute_time_splits, split_masks
from .train_v1 import _evaluate_split, _fit_booster_sweep, _predict_scores, _try_import_xgboost, _validate_split_counts


DEFAULT_ABLATIONS: tuple[str, ...] = ("A0", "A1", "A2", "A3", "A4")
TRADE_ONLY_EXACT = {"m_trade_source", "m_trade_coverage_ms", "m_trade_events"}
BOOK_ONLY_EXACT = {"m_book_coverage_ms", "m_book_events", "m_micro_available"}


@dataclass(frozen=True)
class AblationOptions:
    dataset_root: Path
    parquet_root: Path | None
    logs_root: Path
    tf: str
    quote: str
    top_n: int
    start: str
    end: str
    feature_set: str
    label_set: str
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
    ablations: tuple[str, ...] = DEFAULT_ABLATIONS
    base_candles_dataset: str = "auto"


@dataclass(frozen=True)
class AblationResult:
    run_id: str
    rows: tuple[dict[str, Any], ...]
    summary: dict[str, Any]
    results_csv_path: Path
    summary_json_path: Path


def run_ablation(options: AblationOptions) -> AblationResult:
    if options.feature_set != "v2":
        raise ValueError("ablation requires --feature-set v2")
    if options.label_set != "v1":
        raise ValueError("ablation requires --label-set v1")
    if _try_import_xgboost() is None:
        raise RuntimeError("xgboost is required for model ablate")

    request = build_dataset_request(
        dataset_root=options.dataset_root,
        tf=options.tf,
        quote=options.quote,
        top_n=options.top_n,
        start=options.start,
        end=options.end,
        batch_rows=options.batch_rows,
    )
    all_features = feature_columns_from_spec(options.dataset_root)
    dataset = load_feature_dataset(request, feature_columns=all_features)
    feature_index = {name: idx for idx, name in enumerate(dataset.feature_names)}

    labels, _ = compute_time_splits(
        dataset.ts_ms,
        train_ratio=float(options.train_ratio),
        valid_ratio=float(options.valid_ratio),
        test_ratio=float(options.test_ratio),
        embargo_bars=max(int(options.embargo_bars), 0),
        interval_ms=expected_interval_ms(options.tf),
    )
    masks = split_masks(labels)
    _validate_split_counts(masks)

    rows_total = int(dataset.rows)
    rows_train = int(np.sum(masks[SPLIT_TRAIN]))
    rows_valid = int(np.sum(masks[SPLIT_VALID]))
    rows_test = int(np.sum(masks[SPLIT_TEST]))

    y_train = dataset.y_cls[masks[SPLIT_TRAIN]]
    y_valid = dataset.y_cls[masks[SPLIT_VALID]]
    y_test = dataset.y_cls[masks[SPLIT_TEST]]
    y_reg_valid = dataset.y_reg[masks[SPLIT_VALID]]
    y_reg_test = dataset.y_reg[masks[SPLIT_TEST]]
    markets_valid = dataset.markets[masks[SPLIT_VALID]]
    markets_test = dataset.markets[masks[SPLIT_TEST]]

    diagnostics = _compute_micro_diagnostics(
        dataset_root=options.dataset_root,
        tf=options.tf,
        markets=dataset.selected_markets,
        start_ts_ms=int(request.start_ts_ms or 0),
        end_ts_ms=int(request.end_ts_ms or 0),
        rows_total=rows_total,
        parquet_root=options.parquet_root,
        base_candles_dataset=options.base_candles_dataset,
    )

    rows: list[dict[str, Any]] = []
    for ablation_id in options.ablations:
        selected_features = select_ablation_feature_columns(dataset.feature_names, ablation_id=ablation_id)
        if not selected_features:
            raise ValueError(f"{ablation_id} selected zero columns")
        indices = np.array([feature_index[name] for name in selected_features], dtype=np.int64)

        x_train = dataset.X[masks[SPLIT_TRAIN]][:, indices]
        x_valid = dataset.X[masks[SPLIT_VALID]][:, indices]
        x_test = dataset.X[masks[SPLIT_TEST]][:, indices]

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
        _ = _evaluate_split(
            y_cls=y_valid,
            y_reg=y_reg_valid,
            scores=valid_scores,
            markets=markets_valid,
            fee_bps_est=options.fee_bps_est,
            safety_bps=options.safety_bps,
        )
        test_metrics = _evaluate_split(
            y_cls=y_test,
            y_reg=y_reg_test,
            scores=test_scores,
            markets=markets_test,
            fee_bps_est=options.fee_bps_est,
            safety_bps=options.safety_bps,
        )

        cls = test_metrics.get("classification", {})
        top1 = (test_metrics.get("trading", {}) or {}).get("top_1pct", {})
        top5 = (test_metrics.get("trading", {}) or {}).get("top_5pct", {})
        top10 = (test_metrics.get("trading", {}) or {}).get("top_10pct", {})
        rows.append(
            {
                "ablation_id": ablation_id,
                "feature_count": len(selected_features),
                "feature_columns": ",".join(selected_features),
                "rows_total": rows_total,
                "rows_train": rows_train,
                "rows_valid": rows_valid,
                "rows_test": rows_test,
                "label_pos_rate_train": float(np.mean(y_train == 1)) if y_train.size > 0 else 0.0,
                "label_pos_rate_valid": float(np.mean(y_valid == 1)) if y_valid.size > 0 else 0.0,
                "label_pos_rate_test": float(np.mean(y_test == 1)) if y_test.size > 0 else 0.0,
                "roc_auc": _safe_float(cls.get("roc_auc")),
                "pr_auc": _safe_float(cls.get("pr_auc")),
                "log_loss": _safe_float(cls.get("log_loss")),
                "brier_score": _safe_float(cls.get("brier_score")),
                "precision_top1": _safe_float(top1.get("precision")),
                "precision_top5": _safe_float(top5.get("precision")),
                "precision_top10": _safe_float(top10.get("precision")),
                "ev_net_top1": _safe_float(top1.get("ev_net")),
                "ev_net_top5": _safe_float(top5.get("ev_net")),
                "ev_net_top10": _safe_float(top10.get("ev_net")),
                "micro_coverage_ratio": diagnostics.get("micro_coverage_ratio"),
                "micro_coverage_p50_ms": diagnostics.get("micro_coverage_p50_ms"),
                "micro_coverage_p90_ms": diagnostics.get("micro_coverage_p90_ms"),
                "book_available_ratio": diagnostics.get("book_available_ratio"),
                "trade_source_ws_ratio": diagnostics.get("trade_source_ws_ratio"),
                "trade_source_rest_ratio": diagnostics.get("trade_source_rest_ratio"),
                "trade_source_none_ratio": diagnostics.get("trade_source_none_ratio"),
                "trade_source_other_ratio": diagnostics.get("trade_source_other_ratio"),
                "candles_rows": diagnostics.get("candles_rows"),
            }
        )

    summary = _build_ablation_summary(rows)
    summary["diagnostics"] = diagnostics
    summary["created_at_utc"] = _utc_now()
    summary["ablation_ids"] = list(options.ablations)
    summary["dataset_root"] = str(options.dataset_root)
    summary["time_range"] = {"start": options.start, "end": options.end}

    csv_path = options.logs_root / "t14_3_ablation_results.csv"
    json_path = options.logs_root / "t14_3_ablation_summary.json"
    write_ablation_outputs(rows=rows, summary=summary, csv_path=csv_path, summary_path=json_path)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return AblationResult(
        run_id=run_id,
        rows=tuple(rows),
        summary=summary,
        results_csv_path=csv_path,
        summary_json_path=json_path,
    )


def select_ablation_feature_columns(feature_columns: tuple[str, ...], *, ablation_id: str) -> tuple[str, ...]:
    aid = str(ablation_id).strip().upper()
    all_cols = tuple(str(item).strip() for item in feature_columns if str(item).strip())
    if aid == "A3":
        return all_cols
    if aid == "A0":
        return tuple(col for col in all_cols if not col.startswith("m_"))
    if aid == "A4":
        return tuple(col for col in all_cols if col.startswith("m_"))
    if aid == "A1":
        selected: list[str] = []
        for col in all_cols:
            if not col.startswith("m_"):
                selected.append(col)
                continue
            if col.startswith("m_trade_") or col in TRADE_ONLY_EXACT:
                selected.append(col)
        return tuple(selected)
    if aid == "A2":
        selected = []
        for col in all_cols:
            if not col.startswith("m_"):
                selected.append(col)
                continue
            if col.startswith("m_book_") or col in BOOK_ONLY_EXACT:
                selected.append(col)
        return tuple(selected)
    raise ValueError(f"unsupported ablation id: {ablation_id}")


def write_ablation_outputs(
    *,
    rows: list[dict[str, Any]],
    summary: dict[str, Any],
    csv_path: Path,
    summary_path: Path,
) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_csv(csv_path)
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _build_ablation_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_id = {str(row.get("ablation_id")): row for row in rows}
    best_precision = _best_row(rows, key="precision_top5")
    best_ev = _best_row(rows, key="ev_net_top5")
    best_pr_auc = _best_row(rows, key="pr_auc")

    decision = _decide_next_lever(by_id=by_id)
    return {
        "best_ablation_by_prec_top5": best_precision.get("ablation_id") if best_precision else None,
        "best_ablation_by_ev_top5": best_ev.get("ablation_id") if best_ev else None,
        "best_ablation_by_pr_auc": best_pr_auc.get("ablation_id") if best_pr_auc else None,
        "rule_decision": decision,
        "rows": rows,
    }


def _decide_next_lever(*, by_id: dict[str, dict[str, Any]]) -> dict[str, Any]:
    a0 = by_id.get("A0")
    a1 = by_id.get("A1")
    a2 = by_id.get("A2")
    a3 = by_id.get("A3")
    if a0 is None:
        return {
            "rule": "UNDECIDED",
            "next_ticket": None,
            "reason": "A0 result missing",
        }

    def _delta(a: dict[str, Any] | None, b: dict[str, Any] | None, key: str) -> float:
        if a is None or b is None:
            return 0.0
        return _safe_float(a.get(key)) - _safe_float(b.get(key))

    r1_precision_delta = _delta(a1, a0, "precision_top5")
    r1_ev_delta = _delta(a1, a0, "ev_net_top5")
    if r1_precision_delta >= 0.02 or r1_ev_delta >= 0.0002:
        return {
            "rule": "R1",
            "next_ticket": "T14.4",
            "reason": "A1(OHLC+trade) improved over A0 beyond threshold",
            "delta": {"precision_top5": r1_precision_delta, "ev_net_top5": r1_ev_delta},
        }

    book_available_ratio = _safe_float((a3 or a2 or a0).get("book_available_ratio"))
    a2_prec = _safe_float(a2.get("precision_top5")) if a2 else -1e18
    a3_prec = _safe_float(a3.get("precision_top5")) if a3 else -1e18
    a2_ev = _safe_float(a2.get("ev_net_top5")) if a2 else -1e18
    a3_ev = _safe_float(a3.get("ev_net_top5")) if a3 else -1e18
    a0_prec = _safe_float(a0.get("precision_top5"))
    a0_ev = _safe_float(a0.get("ev_net_top5"))
    if book_available_ratio < 0.2 and (a2_prec >= a0_prec or a3_prec >= a0_prec or a2_ev >= a0_ev or a3_ev >= a0_ev):
        return {
            "rule": "R2",
            "next_ticket": "T13.1c",
            "reason": "book signal may help but book_available_ratio is low",
            "book_available_ratio": book_available_ratio,
        }

    if a1 is not None and a3 is not None:
        better_than_a1 = a0_prec >= _safe_float(a1.get("precision_top5")) and a0_ev >= _safe_float(a1.get("ev_net_top5"))
        better_than_a3 = a0_prec >= _safe_float(a3.get("precision_top5")) and a0_ev >= _safe_float(a3.get("ev_net_top5"))
        if better_than_a1 and better_than_a3:
            return {
                "rule": "R3",
                "next_ticket": "T15",
                "reason": "A0 consistently outperformed micro variants (A1/A3)",
            }

    return {
        "rule": "UNDECIDED",
        "next_ticket": "T14.4",
        "reason": "No hard rule fired; default to label/horizon tuning",
    }


def _best_row(rows: list[dict[str, Any]], *, key: str) -> dict[str, Any] | None:
    if not rows:
        return None
    ranked = sorted(
        rows,
        key=lambda item: (_safe_float(item.get(key)), str(item.get("ablation_id", ""))),
        reverse=True,
    )
    return ranked[0]


def _compute_micro_diagnostics(
    *,
    dataset_root: Path,
    tf: str,
    markets: tuple[str, ...],
    start_ts_ms: int,
    end_ts_ms: int,
    rows_total: int,
    parquet_root: Path | None,
    base_candles_dataset: str,
) -> dict[str, Any]:
    frame = _load_diagnostic_frame(
        dataset_root=dataset_root,
        tf=tf,
        markets=markets,
        start_ts_ms=start_ts_ms,
        end_ts_ms=end_ts_ms,
    )
    rows = int(frame.height)
    if rows <= 0:
        candles_rows = _count_candle_rows(
            parquet_root=parquet_root,
            base_dataset=base_candles_dataset,
            tf=tf,
            markets=markets,
            start_ts_ms=start_ts_ms,
            end_ts_ms=end_ts_ms,
        )
        coverage_ratio = (float(rows_total) / float(candles_rows)) if candles_rows > 0 else None
        return {
            "rows_total": rows_total,
            "rows_diagnostic": 0,
            "candles_rows": candles_rows,
            "micro_coverage_ratio": coverage_ratio,
            "micro_coverage_p50_ms": None,
            "micro_coverage_p90_ms": None,
            "book_available_ratio": None,
            "trade_source_ws_ratio": None,
            "trade_source_rest_ratio": None,
            "trade_source_none_ratio": None,
            "trade_source_other_ratio": None,
        }

    available_mask = np.ones(rows, dtype=bool)
    if "m_micro_available" in frame.columns:
        available_mask = frame.get_column("m_micro_available").fill_null(False).to_numpy().astype(bool, copy=False)

    coverage_values: list[int] = []
    if "m_trade_coverage_ms" in frame.columns:
        coverage_series = frame.get_column("m_trade_coverage_ms")
        coverage_list = coverage_series.cast(pl.Int64, strict=False).to_list()
        for idx, value in enumerate(coverage_list):
            if value is None:
                continue
            if idx < available_mask.size and not available_mask[idx]:
                continue
            coverage_values.append(int(value))

    book_available_ratio: float | None = None
    if "m_book_events" in frame.columns:
        book_events = frame.get_column("m_book_events").cast(pl.Int64, strict=False).fill_null(0).to_numpy()
        book_available_ratio = float(np.mean(book_events > 0)) if book_events.size > 0 else None

    trade_counts = {"ws": 0, "rest": 0, "none": 0, "other": 0}
    if "m_trade_source" in frame.columns:
        for value in frame.get_column("m_trade_source").cast(pl.Utf8).fill_null("none").to_list():
            src = str(value).strip().lower()
            if src in {"ws", "rest", "none"}:
                trade_counts[src] += 1
            else:
                trade_counts["other"] += 1
    total_sources = sum(trade_counts.values())
    candles_rows = _count_candle_rows(
        parquet_root=parquet_root,
        base_dataset=base_candles_dataset,
        tf=tf,
        markets=markets,
        start_ts_ms=start_ts_ms,
        end_ts_ms=end_ts_ms,
    )
    micro_coverage_ratio = (float(rows_total) / float(candles_rows)) if candles_rows > 0 else None
    return {
        "rows_total": rows_total,
        "rows_diagnostic": rows,
        "candles_rows": candles_rows,
        "micro_coverage_ratio": micro_coverage_ratio,
        "micro_coverage_p50_ms": _quantile_int(coverage_values, 0.50) if coverage_values else None,
        "micro_coverage_p90_ms": _quantile_int(coverage_values, 0.90) if coverage_values else None,
        "book_available_ratio": book_available_ratio,
        "trade_source_ws_ratio": (float(trade_counts["ws"]) / float(total_sources)) if total_sources > 0 else None,
        "trade_source_rest_ratio": (float(trade_counts["rest"]) / float(total_sources)) if total_sources > 0 else None,
        "trade_source_none_ratio": (float(trade_counts["none"]) / float(total_sources)) if total_sources > 0 else None,
        "trade_source_other_ratio": (float(trade_counts["other"]) / float(total_sources)) if total_sources > 0 else None,
    }


def _load_diagnostic_frame(
    *,
    dataset_root: Path,
    tf: str,
    markets: tuple[str, ...],
    start_ts_ms: int,
    end_ts_ms: int,
) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    for market in markets:
        files = _dataset_part_files(dataset_root=dataset_root, tf=tf, market=market)
        if not files:
            continue
        lazy = pl.scan_parquet([str(path) for path in files]).filter(
            (pl.col("ts_ms") >= int(start_ts_ms)) & (pl.col("ts_ms") <= int(end_ts_ms))
        )
        schema = lazy.collect_schema()
        names = set(schema.names())
        expressions: list[pl.Expr] = []
        if "ts_ms" in names:
            expressions.append(pl.col("ts_ms").cast(pl.Int64).alias("ts_ms"))
        if "y_cls" in names:
            expressions.append(pl.col("y_cls").cast(pl.Int8).alias("y_cls"))
        if "m_micro_available" in names:
            expressions.append(pl.col("m_micro_available").cast(pl.Boolean).alias("m_micro_available"))
        if "m_trade_coverage_ms" in names:
            expressions.append(pl.col("m_trade_coverage_ms").cast(pl.Int64, strict=False).alias("m_trade_coverage_ms"))
        if "m_book_events" in names:
            expressions.append(pl.col("m_book_events").cast(pl.Int64, strict=False).alias("m_book_events"))
        if "m_trade_source" in names:
            expressions.append(pl.col("m_trade_source").cast(pl.Utf8).alias("m_trade_source"))
        if not expressions:
            continue
        frame = _collect_lazy(lazy.select(expressions))
        if frame.height <= 0:
            continue
        frames.append(frame.with_columns(pl.lit(market).alias("market")))
    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="vertical_relaxed")


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


def _safe_float(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except Exception:
        return 0.0


def _quantile_int(values: list[int], q: float) -> int:
    if not values:
        return 0
    arr = np.array(values, dtype=np.float64)
    return int(round(float(np.quantile(arr, q))))


def _collect_lazy(lazy_frame: pl.LazyFrame) -> pl.DataFrame:
    try:
        return lazy_frame.collect(engine="streaming")
    except TypeError:
        return lazy_frame.collect(streaming=True)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
