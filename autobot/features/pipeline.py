"""Feature store build/validate pipeline."""

from __future__ import annotations

from dataclasses import dataclass
import json
import math
from pathlib import Path
import time
from typing import Any

import polars as pl

from autobot.data import expected_interval_ms

from .feature_set_v1 import add_liquidity_roll_features, compute_factor_feature_frame, compute_feature_set_v1
from .feature_spec import (
    FeatureSetV1Config,
    FeaturesConfig,
    effective_threshold_bps,
    factor_prefix,
    feature_columns,
    label_columns,
    max_feature_lookback_bars,
    parse_date_to_ts_ms,
    sha256_file,
    sha256_json,
    to_serializable_config,
    write_json,
)
from .labeling_v1 import apply_labeling_v1, drop_neutral_rows, label_distribution
from .store import append_manifest_rows, load_manifest, manifest_path, save_manifest


@dataclass(frozen=True)
class FeatureBuildOptions:
    tf: str
    quote: str | None = None
    top_n: int | None = None
    start: str | None = None
    end: str | None = None
    feature_set: str = "v1"
    label_set: str = "v1"
    workers: int = 1
    fail_on_warn: bool = False


@dataclass(frozen=True)
class FeatureBuildSummary:
    discovered_markets: int
    selected_markets: tuple[str, ...]
    processed_markets: int
    ok_markets: int
    warn_markets: int
    fail_markets: int
    rows_total: int
    min_ts_ms: int | None
    max_ts_ms: int | None
    output_path: Path
    manifest_file: Path
    build_report_file: Path
    feature_spec_hash: str
    label_spec_hash: str
    details: tuple[dict[str, Any], ...]
    failures: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class FeatureValidateOptions:
    tf: str
    quote: str | None = None
    top_n: int | None = None


@dataclass(frozen=True)
class FeatureValidateSummary:
    checked_files: int
    ok_files: int
    warn_files: int
    fail_files: int
    schema_ok: bool
    null_ratio_overall: float
    worst_columns_top5: tuple[dict[str, Any], ...]
    label_distribution: dict[str, int]
    leakage_smoke: str
    validate_report_file: Path
    details: tuple[dict[str, Any], ...]


def build_features_dataset(config: FeaturesConfig, options: FeatureBuildOptions) -> FeatureBuildSummary:
    if options.feature_set != "v1":
        raise ValueError("feature_set currently supports only v1")
    if options.label_set != "v1":
        raise ValueError("label_set currently supports only v1")

    started_at = int(time.time())
    tf = str(options.tf).strip().lower()
    quote = str(options.quote or config.universe.quote).strip().upper()
    top_n = max(1, int(options.top_n if options.top_n is not None else config.universe.top_n))
    start_text = str(options.start or config.time_range.start).strip()
    end_text = str(options.end or config.time_range.end).strip()
    start_ts_ms = parse_date_to_ts_ms(start_text)
    end_ts_ms = parse_date_to_ts_ms(end_text, end_of_day=True)
    if end_ts_ms < start_ts_ms:
        raise ValueError("time_range.end must be >= time_range.start")

    input_root = config.input_dataset_root
    output_root = config.output_dataset_root
    output_root.mkdir(parents=True, exist_ok=True)
    meta_root = output_root / "_meta"
    meta_root.mkdir(parents=True, exist_ok=True)

    available_markets = _list_markets(input_root=input_root, tf=tf, quote=quote)
    selected_markets = _select_universe_markets(
        config=config,
        available_markets=available_markets,
        tf=tf,
        quote=quote,
        top_n=top_n,
        start_ts_ms=start_ts_ms,
    )

    feature_cols = feature_columns(config.feature_set_v1)
    label_cols = label_columns()
    feature_cols_hash = sha256_json(feature_cols)
    label_cols_hash = sha256_json(label_cols)

    interval_ms = expected_interval_ms(tf)
    warmup_bars = max_feature_lookback_bars(config.feature_set_v1, tf=tf) + 2
    extended_start_ts_ms = start_ts_ms - warmup_bars * interval_ms
    extended_end_ts_ms = end_ts_ms + max(1, config.label_v1.horizon_bars) * interval_ms

    factor_frames = _build_factor_frames(
        config=config,
        tf=tf,
        from_ts_ms=extended_start_ts_ms,
        to_ts_ms=extended_end_ts_ms,
    )

    manifest_rows: list[dict[str, Any]] = []
    details: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    rows_total = 0
    min_ts_ms_total: int | None = None
    max_ts_ms_total: int | None = None

    for market in selected_markets:
        try:
            result = _build_one_market(
                config=config,
                tf=tf,
                market=market,
                feature_cols=feature_cols,
                label_cols=label_cols,
                feature_cols_hash=feature_cols_hash,
                label_cols_hash=label_cols_hash,
                from_ts_ms=start_ts_ms,
                to_ts_ms=end_ts_ms,
                extended_from_ts_ms=extended_start_ts_ms,
                extended_to_ts_ms=extended_end_ts_ms,
                factor_frames=factor_frames,
                built_at=started_at,
            )
            manifest_rows.append(result["manifest_row"])
            details.append(result["detail"])
            if result["manifest_row"]["status"] == "FAIL":
                failures.append(result["detail"])
            else:
                rows_total += int(result["manifest_row"]["rows"])
                min_ts_ms_total = _safe_min(min_ts_ms_total, result["manifest_row"]["min_ts_ms"])
                max_ts_ms_total = _safe_max(max_ts_ms_total, result["manifest_row"]["max_ts_ms"])
        except Exception as exc:
            row = {
                "dataset_name": config.dataset_name,
                "tf": tf,
                "market": market,
                "rows": 0,
                "min_ts_ms": None,
                "max_ts_ms": None,
                "feature_cols_hash": feature_cols_hash,
                "label_cols_hash": label_cols_hash,
                "null_ratio_overall": 1.0,
                "null_ratio_by_col_json": json.dumps({}, ensure_ascii=False),
                "status": "FAIL",
                "reasons_json": json.dumps(["BUILD_EXCEPTION"], ensure_ascii=False),
                "error_message": str(exc),
                "built_at": int(time.time()),
            }
            detail = {"market": market, "status": "FAIL", "reasons": ["BUILD_EXCEPTION"], "error_message": str(exc)}
            manifest_rows.append(row)
            details.append(detail)
            failures.append(detail)

    if config.feature_set_v1.enable_liquidity_rank and selected_markets:
        _apply_liquidity_rank(
            config=config,
            tf=tf,
            selected_markets=selected_markets,
            feature_cols=feature_cols,
            label_cols=label_cols,
            manifest_rows=manifest_rows,
        )

    manifest_file = manifest_path(output_root)
    existing_manifest = load_manifest(manifest_file)
    if existing_manifest.height > 0:
        remaining = existing_manifest.filter(pl.col("tf") != tf)
        save_manifest(manifest_file, remaining)
    append_manifest_rows(manifest_file, manifest_rows)

    feature_spec_payload = _build_feature_spec_payload(
        config=config,
        tf=tf,
        quote=quote,
        selected_markets=selected_markets,
        feature_cols=feature_cols,
        from_ts_ms=start_ts_ms,
        to_ts_ms=end_ts_ms,
    )
    label_spec_payload = _build_label_spec_payload(
        config=config,
        tf=tf,
        label_cols=label_cols,
    )
    feature_spec_hash = sha256_json(feature_spec_payload)
    label_spec_hash = sha256_json(label_spec_payload)
    write_json(meta_root / "feature_spec.json", feature_spec_payload)
    write_json(meta_root / "label_spec.json", label_spec_payload)

    ok_markets = sum(1 for row in manifest_rows if row.get("status") == "OK")
    warn_markets = sum(1 for row in manifest_rows if row.get("status") == "WARN")
    fail_markets = sum(1 for row in manifest_rows if row.get("status") == "FAIL")

    build_report = {
        "started_at": started_at,
        "finished_at": int(time.time()),
        "dataset_name": config.dataset_name,
        "input_dataset": config.input_dataset,
        "tf": tf,
        "quote": quote,
        "requested_top_n": top_n,
        "discovered_markets": len(available_markets),
        "selected_markets": selected_markets,
        "processed_markets": len(manifest_rows),
        "ok_markets": ok_markets,
        "warn_markets": warn_markets,
        "fail_markets": fail_markets,
        "rows_total": rows_total,
        "min_ts_ms": min_ts_ms_total,
        "max_ts_ms": max_ts_ms_total,
        "output_path": str(output_root),
        "manifest_file": str(manifest_file),
        "feature_spec_hash": feature_spec_hash,
        "label_spec_hash": label_spec_hash,
        "failures": failures,
        "details": details,
    }
    build_report_file = meta_root / "build_report.json"
    write_json(build_report_file, build_report)

    return FeatureBuildSummary(
        discovered_markets=len(available_markets),
        selected_markets=tuple(selected_markets),
        processed_markets=len(manifest_rows),
        ok_markets=ok_markets,
        warn_markets=warn_markets,
        fail_markets=fail_markets,
        rows_total=rows_total,
        min_ts_ms=min_ts_ms_total,
        max_ts_ms=max_ts_ms_total,
        output_path=output_root,
        manifest_file=manifest_file,
        build_report_file=build_report_file,
        feature_spec_hash=feature_spec_hash,
        label_spec_hash=label_spec_hash,
        details=tuple(details),
        failures=tuple(failures),
    )


def validate_features_dataset(config: FeaturesConfig, options: FeatureValidateOptions) -> FeatureValidateSummary:
    tf = str(options.tf).strip().lower()
    quote = str(options.quote or config.universe.quote).strip().upper()
    top_n = int(options.top_n if options.top_n is not None else config.universe.top_n)

    dataset_root = config.output_dataset_root
    meta_root = dataset_root / "_meta"
    manifest_file = manifest_path(dataset_root)
    validate_report_file = meta_root / "validate_report.json"

    feature_spec_doc = _read_json(meta_root / "feature_spec.json")
    label_spec_doc = _read_json(meta_root / "label_spec.json")
    expected_feature_cols = feature_spec_doc.get("feature_columns") if isinstance(feature_spec_doc, dict) else None
    expected_label_cols = label_spec_doc.get("label_columns") if isinstance(label_spec_doc, dict) else None
    if not isinstance(expected_feature_cols, list):
        expected_feature_cols = feature_columns(config.feature_set_v1)
    if not isinstance(expected_label_cols, list):
        expected_label_cols = label_columns()
    expected_columns = ["ts_ms"] + [str(col) for col in expected_feature_cols] + [str(col) for col in expected_label_cols]

    manifest = load_manifest(manifest_file)
    market_rows = []
    if manifest.height > 0:
        market_rows = [
            dict(row)
            for row in manifest.filter((pl.col("tf") == tf) & (pl.col("market").str.starts_with(f"{quote}-"))).iter_rows(
                named=True
            )
        ]
    selected_markets = [str(row["market"]).upper() for row in market_rows if str(row.get("market", "")).strip()]
    if top_n > 0:
        selected_markets = selected_markets[:top_n]

    details: list[dict[str, Any]] = []
    null_counts_by_col: dict[str, int] = {col: 0 for col in expected_columns}
    total_rows = 0
    total_cells = 0
    total_null_cells = 0
    label_total = {"pos": 0, "neg": 0, "neutral": 0, "total": 0}

    ok_files = 0
    warn_files = 0
    fail_files = 0
    for market in selected_markets:
        part_file = _feature_part_path(dataset_root=dataset_root, tf=tf, market=market)
        if not part_file.exists():
            details.append(
                {
                    "market": market,
                    "file": str(part_file),
                    "status": "FAIL",
                    "reasons": ["MISSING_OUTPUT_FILE"],
                }
            )
            fail_files += 1
            continue

        frame = pl.read_parquet(part_file)
        missing_columns = [name for name in expected_columns if name not in frame.columns]
        non_monotonic = _is_non_monotonic(frame.get_column("ts_ms")) if "ts_ms" in frame.columns else True
        rows = int(frame.height)
        min_ts_ms = int(frame.get_column("ts_ms").min()) if rows > 0 and "ts_ms" in frame.columns else None
        max_ts_ms = int(frame.get_column("ts_ms").max()) if rows > 0 and "ts_ms" in frame.columns else None

        null_ratio_by_col = {}
        for col in expected_columns:
            null_count = int(frame.get_column(col).null_count()) if col in frame.columns else rows
            null_counts_by_col[col] += null_count
            null_ratio_by_col[col] = (float(null_count) / float(rows)) if rows > 0 else 0.0
            total_null_cells += null_count
        total_rows += rows
        total_cells += rows * len(expected_columns)

        distribution = label_distribution(frame=frame, config=config.label_v1)
        label_total["pos"] += int(distribution["pos"])
        label_total["neg"] += int(distribution["neg"])
        label_total["neutral"] += int(distribution["neutral"])
        label_total["total"] += int(distribution["total"])

        reasons: list[str] = []
        status = "OK"
        if missing_columns:
            reasons.append("MISSING_COLUMNS")
            status = "FAIL"
        if non_monotonic:
            reasons.append("TS_NON_MONOTONIC")
            status = "FAIL"
        if rows <= 0:
            reasons.append("NO_ROWS")
            status = "FAIL"

        null_ratio_overall = _overall_null_ratio_from_map(null_ratio_by_col)
        if status == "OK" and null_ratio_overall > 0.0:
            status = "WARN"
            reasons.append("NULLS_PRESENT")
        if status == "OK":
            ok_files += 1
        elif status == "WARN":
            warn_files += 1
        else:
            fail_files += 1

        details.append(
            {
                "market": market,
                "file": str(part_file),
                "rows": rows,
                "min_ts_ms": min_ts_ms,
                "max_ts_ms": max_ts_ms,
                "null_ratio_overall": null_ratio_overall,
                "missing_columns": missing_columns,
                "non_monotonic": non_monotonic,
                "label_distribution": distribution,
                "status": status,
                "reasons": reasons,
            }
        )

    null_ratio_overall = (float(total_null_cells) / float(total_cells)) if total_cells > 0 else 0.0
    worst_columns = []
    for col in expected_columns:
        ratio = (float(null_counts_by_col[col]) / float(total_rows)) if total_rows > 0 else 0.0
        worst_columns.append({"column": col, "null_ratio": ratio})
    worst_columns.sort(key=lambda item: (-item["null_ratio"], item["column"]))
    worst_top5 = tuple(worst_columns[:5])

    leakage_result = _run_leakage_smoke(
        config=config,
        tf=tf,
        market=(selected_markets[0] if selected_markets else None),
        from_ts_ms=parse_date_to_ts_ms(config.time_range.start),
        to_ts_ms=parse_date_to_ts_ms(config.time_range.end, end_of_day=True),
        feature_cols=[col for col in expected_feature_cols if col != "vol_rank_at_ts"],
    )

    report = {
        "dataset_name": config.dataset_name,
        "tf": tf,
        "quote": quote,
        "checked_files": len(details),
        "ok_files": ok_files,
        "warn_files": warn_files,
        "fail_files": fail_files,
        "schema_ok": fail_files == 0,
        "null_ratio_overall": null_ratio_overall,
        "worst_columns_top5": worst_top5,
        "label_distribution": label_total,
        "leakage_smoke": leakage_result["status"],
        "leakage_detail": leakage_result,
        "details": details,
    }
    write_json(validate_report_file, report)

    return FeatureValidateSummary(
        checked_files=len(details),
        ok_files=ok_files,
        warn_files=warn_files,
        fail_files=fail_files,
        schema_ok=(fail_files == 0),
        null_ratio_overall=null_ratio_overall,
        worst_columns_top5=worst_top5,
        label_distribution=label_total,
        leakage_smoke=str(leakage_result["status"]),
        validate_report_file=validate_report_file,
        details=tuple(details),
    )


def sample_features(
    config: FeaturesConfig,
    *,
    tf: str,
    market: str,
    rows: int,
) -> list[dict[str, Any]]:
    part_file = _feature_part_path(
        dataset_root=config.output_dataset_root,
        tf=str(tf).strip().lower(),
        market=str(market).strip().upper(),
    )
    if not part_file.exists():
        raise FileNotFoundError(f"feature file not found: {part_file}")
    return pl.read_parquet(part_file).head(max(int(rows), 0)).to_dicts()


def features_stats(
    config: FeaturesConfig,
    *,
    tf: str,
    quote: str | None = None,
    top_n: int | None = None,
) -> dict[str, Any]:
    tf_value = str(tf).strip().lower()
    quote_value = str(quote or config.universe.quote).strip().upper()
    top_n_value = int(top_n if top_n is not None else config.universe.top_n)

    dataset_root = config.output_dataset_root
    manifest = load_manifest(manifest_path(dataset_root))
    if manifest.height <= 0:
        return {
            "dataset_name": config.dataset_name,
            "tf": tf_value,
            "quote": quote_value,
            "markets": [],
            "rows_total": 0,
            "min_ts_ms": None,
            "max_ts_ms": None,
            "status_counts": {"OK": 0, "WARN": 0, "FAIL": 0},
            "label_distribution": {"pos": 0, "neg": 0, "neutral": 0, "total": 0},
        }

    filtered = manifest.filter((pl.col("tf") == tf_value) & (pl.col("market").str.starts_with(f"{quote_value}-")))
    if top_n_value > 0:
        filtered = filtered.head(top_n_value)

    rows = [dict(item) for item in filtered.iter_rows(named=True)]
    markets = [str(item["market"]).upper() for item in rows]
    rows_total = int(sum(int(item.get("rows") or 0) for item in rows))
    min_ts_ms = _safe_min_many([item.get("min_ts_ms") for item in rows])
    max_ts_ms = _safe_max_many([item.get("max_ts_ms") for item in rows])

    status_counts = {"OK": 0, "WARN": 0, "FAIL": 0}
    for item in rows:
        status = str(item.get("status") or "").upper()
        if status in status_counts:
            status_counts[status] += 1

    label_total = {"pos": 0, "neg": 0, "neutral": 0, "total": 0}
    for market in markets:
        part_file = _feature_part_path(dataset_root=dataset_root, tf=tf_value, market=market)
        if not part_file.exists():
            continue
        frame = pl.read_parquet(part_file, columns=["y_reg", "y_cls"])
        dist = label_distribution(frame=frame, config=config.label_v1)
        label_total["pos"] += int(dist["pos"])
        label_total["neg"] += int(dist["neg"])
        label_total["neutral"] += int(dist["neutral"])
        label_total["total"] += int(dist["total"])

    return {
        "dataset_name": config.dataset_name,
        "tf": tf_value,
        "quote": quote_value,
        "markets": markets,
        "rows_total": rows_total,
        "min_ts_ms": min_ts_ms,
        "max_ts_ms": max_ts_ms,
        "status_counts": status_counts,
        "label_distribution": label_total,
    }


def _build_one_market(
    *,
    config: FeaturesConfig,
    tf: str,
    market: str,
    feature_cols: list[str],
    label_cols: list[str],
    feature_cols_hash: str,
    label_cols_hash: str,
    from_ts_ms: int,
    to_ts_ms: int,
    extended_from_ts_ms: int,
    extended_to_ts_ms: int,
    factor_frames: dict[str, pl.DataFrame],
    built_at: int,
) -> dict[str, Any]:
    candles = _load_market_candles(
        dataset_root=config.input_dataset_root,
        tf=tf,
        market=market,
        from_ts_ms=extended_from_ts_ms,
        to_ts_ms=extended_to_ts_ms,
    )
    if candles.height <= 0:
        raise ValueError("input market frame is empty")

    featured = compute_feature_set_v1(frame=candles, tf=tf, config=config.feature_set_v1, float_dtype=config.float_dtype)
    if config.feature_set_v1.enable_factor_features:
        for factor_market in config.feature_set_v1.factor_markets:
            prefix = factor_prefix(factor_market)
            factor_frame = factor_frames.get(prefix)
            if factor_frame is None:
                continue
            featured = featured.join(factor_frame, on="ts_ms", how="left")

    if config.feature_set_v1.enable_liquidity_rank:
        interval_ms = expected_interval_ms(tf)
        bars_24h = max(1, int(86_400_000 / interval_ms))
        featured = add_liquidity_roll_features(frame=featured, window_bars=bars_24h, float_dtype=config.float_dtype)

    labeled = apply_labeling_v1(frame=featured, config=config.label_v1).filter(
        (pl.col("ts_ms") >= from_ts_ms) & (pl.col("ts_ms") <= to_ts_ms)
    )
    labeled = drop_neutral_rows(frame=labeled, config=config.label_v1)

    required_non_null = [col for col in feature_cols + label_cols if col in labeled.columns and col != "vol_rank_at_ts"]
    if required_non_null:
        labeled = labeled.filter(pl.all_horizontal([pl.col(col).is_not_null() for col in required_non_null]))

    output_columns = ["ts_ms"] + feature_cols + label_cols
    for name in output_columns:
        if name not in labeled.columns:
            labeled = labeled.with_columns(pl.lit(None).alias(name))
    output = labeled.select(output_columns).sort("ts_ms")
    output = _cast_output_dtypes(output=output, float_dtype=config.float_dtype)

    part_file = _feature_part_path(dataset_root=config.output_dataset_root, tf=tf, market=market)
    part_file.parent.mkdir(parents=True, exist_ok=True)
    output.write_parquet(part_file, compression="zstd")

    rows = int(output.height)
    min_ts_ms = int(output.get_column("ts_ms").min()) if rows > 0 else None
    max_ts_ms = int(output.get_column("ts_ms").max()) if rows > 0 else None

    null_ratio_overall, null_ratio_by_col = _null_ratio(output, output_columns)
    status = "OK"
    reasons: list[str] = []
    if rows <= 0:
        status = "FAIL"
        reasons.append("NO_ROWS_AFTER_FILTERS")
    elif null_ratio_overall > 0.0:
        status = "WARN"
        reasons.append("NULLS_PRESENT")

    manifest_row = {
        "dataset_name": config.dataset_name,
        "tf": tf,
        "market": market,
        "rows": rows,
        "min_ts_ms": min_ts_ms,
        "max_ts_ms": max_ts_ms,
        "feature_cols_hash": feature_cols_hash,
        "label_cols_hash": label_cols_hash,
        "null_ratio_overall": null_ratio_overall,
        "null_ratio_by_col_json": json.dumps(null_ratio_by_col, ensure_ascii=False),
        "status": status,
        "reasons_json": json.dumps(reasons, ensure_ascii=False),
        "error_message": None,
        "built_at": built_at,
    }
    detail = {
        "market": market,
        "status": status,
        "rows": rows,
        "min_ts_ms": min_ts_ms,
        "max_ts_ms": max_ts_ms,
        "null_ratio_overall": null_ratio_overall,
        "reasons": reasons,
        "output": str(part_file),
    }
    return {"manifest_row": manifest_row, "detail": detail}


def _build_factor_frames(
    *,
    config: FeaturesConfig,
    tf: str,
    from_ts_ms: int,
    to_ts_ms: int,
) -> dict[str, pl.DataFrame]:
    frames: dict[str, pl.DataFrame] = {}
    if not config.feature_set_v1.enable_factor_features:
        return frames

    rv_window = max(config.feature_set_v1.windows.rv)
    for market in config.feature_set_v1.factor_markets:
        prefix = factor_prefix(market)
        candles = _load_market_candles(
            dataset_root=config.input_dataset_root,
            tf=tf,
            market=market,
            from_ts_ms=from_ts_ms,
            to_ts_ms=to_ts_ms,
        )
        factor_frame = compute_factor_feature_frame(
            candles,
            market=market,
            rv_window=rv_window,
            float_dtype=config.float_dtype,
        )
        frames[prefix] = factor_frame
    return frames


def _apply_liquidity_rank(
    *,
    config: FeaturesConfig,
    tf: str,
    selected_markets: list[str],
    feature_cols: list[str],
    label_cols: list[str],
    manifest_rows: list[dict[str, Any]],
) -> None:
    if "vol_quote_24h_roll" not in feature_cols or "vol_rank_at_ts" not in feature_cols:
        return

    stacked_frames: list[pl.DataFrame] = []
    for market in selected_markets:
        part_file = _feature_part_path(dataset_root=config.output_dataset_root, tf=tf, market=market)
        if not part_file.exists():
            continue
        frame = pl.read_parquet(part_file, columns=["ts_ms", "vol_quote_24h_roll"])
        if frame.height <= 0:
            continue
        stacked_frames.append(frame.with_columns(pl.lit(market).alias("market")))

    if not stacked_frames:
        return

    stacked = pl.concat(stacked_frames, how="vertical_relaxed")
    ranked = stacked.with_columns(
        (
            pl.col("vol_quote_24h_roll").rank(method="average").over("ts_ms") / pl.len().over("ts_ms")
        ).alias("vol_rank_at_ts")
    )

    for market in selected_markets:
        part_file = _feature_part_path(dataset_root=config.output_dataset_root, tf=tf, market=market)
        if not part_file.exists():
            continue
        frame = pl.read_parquet(part_file)
        rank_frame = ranked.filter(pl.col("market") == market).select(["ts_ms", "vol_rank_at_ts"])
        merged = frame.drop("vol_rank_at_ts").join(rank_frame, on="ts_ms", how="left")
        merged = _cast_output_dtypes(merged.select(["ts_ms"] + feature_cols + label_cols), float_dtype=config.float_dtype)
        merged.write_parquet(part_file, compression="zstd")

        null_ratio_overall, null_ratio_by_col = _null_ratio(merged, ["ts_ms"] + feature_cols + label_cols)
        for row in manifest_rows:
            if row.get("market") != market or row.get("status") == "FAIL":
                continue
            row["rows"] = int(merged.height)
            row["min_ts_ms"] = int(merged.get_column("ts_ms").min()) if merged.height > 0 else None
            row["max_ts_ms"] = int(merged.get_column("ts_ms").max()) if merged.height > 0 else None
            row["null_ratio_overall"] = null_ratio_overall
            row["null_ratio_by_col_json"] = json.dumps(null_ratio_by_col, ensure_ascii=False)
            if merged.height <= 0:
                row["status"] = "FAIL"
                row["reasons_json"] = json.dumps(["NO_ROWS_AFTER_LIQUIDITY_RANK"], ensure_ascii=False)
            elif null_ratio_overall > 0.0:
                row["status"] = "WARN"
                row["reasons_json"] = json.dumps(["NULLS_PRESENT"], ensure_ascii=False)


def _build_feature_spec_payload(
    *,
    config: FeaturesConfig,
    tf: str,
    quote: str,
    selected_markets: list[str],
    feature_cols: list[str],
    from_ts_ms: int,
    to_ts_ms: int,
) -> dict[str, Any]:
    input_manifest_hash = sha256_file(config.input_dataset_root / "_meta" / "manifest.parquet")
    input_files_fingerprint = _input_files_fingerprint(
        dataset_root=config.input_dataset_root,
        tf=tf,
        markets=selected_markets,
    )
    return {
        "dataset_name": config.dataset_name,
        "input_dataset": config.input_dataset,
        "tf": tf,
        "quote": quote,
        "float_dtype": config.float_dtype,
        "feature_columns": feature_cols,
        "feature_set_version": "v1",
        "feature_set_config": _feature_set_config_to_dict(config.feature_set_v1),
        "time_range": {"from_ts_ms": from_ts_ms, "to_ts_ms": to_ts_ms},
        "selected_markets": selected_markets,
        "fingerprint": {
            "input_dataset_root": str(config.input_dataset_root),
            "input_manifest_sha256": input_manifest_hash,
            "input_files_fingerprint": input_files_fingerprint,
            "config_sha256": sha256_json(to_serializable_config(config)),
        },
    }


def _build_label_spec_payload(
    *,
    config: FeaturesConfig,
    tf: str,
    label_cols: list[str],
) -> dict[str, Any]:
    threshold_bps = effective_threshold_bps(config.label_v1)
    return {
        "dataset_name": config.dataset_name,
        "tf": tf,
        "label_columns": label_cols,
        "label_set_version": "v1",
        "horizon_bars": config.label_v1.horizon_bars,
        "thr_bps": config.label_v1.thr_bps,
        "effective_thr_bps": threshold_bps,
        "fee_bps_est": config.label_v1.fee_bps_est,
        "safety_bps": config.label_v1.safety_bps,
        "neutral_policy": config.label_v1.neutral_policy,
    }


def _run_leakage_smoke(
    *,
    config: FeaturesConfig,
    tf: str,
    market: str | None,
    from_ts_ms: int,
    to_ts_ms: int,
    feature_cols: list[str],
) -> dict[str, Any]:
    if market is None:
        return {"status": "FAIL", "reason": "NO_MARKET_TO_VALIDATE"}

    interval_ms = expected_interval_ms(tf)
    warmup = max_feature_lookback_bars(config.feature_set_v1, tf=tf) + 2
    source = _load_market_candles(
        dataset_root=config.input_dataset_root,
        tf=tf,
        market=market,
        from_ts_ms=from_ts_ms - warmup * interval_ms,
        to_ts_ms=to_ts_ms + config.label_v1.horizon_bars * interval_ms,
    )
    if source.height < 40:
        return {"status": "FAIL", "reason": "INSUFFICIENT_ROWS_FOR_SMOKE"}

    pivot_idx = max(10, min(source.height - 2, source.height // 2))
    pivot_ts = int(source.item(row=pivot_idx, column="ts_ms"))

    baseline = compute_feature_set_v1(frame=source, tf=tf, config=config.feature_set_v1, float_dtype=config.float_dtype)
    baseline = _join_factor_frames_for_smoke(
        config=config,
        tf=tf,
        frame=baseline,
        from_ts_ms=from_ts_ms - warmup * interval_ms,
        to_ts_ms=to_ts_ms + config.label_v1.horizon_bars * interval_ms,
    )

    mutated_source = (
        source.with_row_index("__idx")
        .with_columns(
            pl.when(pl.col("__idx") >= pivot_idx)
            .then(pl.col("close") * 1.1)
            .otherwise(pl.col("close"))
            .alias("close")
        )
        .drop("__idx")
    )
    mutated = compute_feature_set_v1(
        frame=mutated_source,
        tf=tf,
        config=config.feature_set_v1,
        float_dtype=config.float_dtype,
    )
    mutated = _join_factor_frames_for_smoke(
        config=config,
        tf=tf,
        frame=mutated,
        from_ts_ms=from_ts_ms - warmup * interval_ms,
        to_ts_ms=to_ts_ms + config.label_v1.horizon_bars * interval_ms,
    )

    compare_cols = [col for col in feature_cols if col in baseline.columns and col in mutated.columns]
    lhs = baseline.filter(pl.col("ts_ms") < pivot_ts).select(["ts_ms"] + compare_cols).sort("ts_ms")
    rhs = mutated.filter(pl.col("ts_ms") < pivot_ts).select(["ts_ms"] + compare_cols).sort("ts_ms")
    if lhs.height != rhs.height:
        return {"status": "FAIL", "reason": "ROW_COUNT_MISMATCH", "lhs": lhs.height, "rhs": rhs.height}

    for col in compare_cols:
        left_values = lhs.get_column(col).to_list()
        right_values = rhs.get_column(col).to_list()
        for idx, (left, right) in enumerate(zip(left_values, right_values, strict=False)):
            if left is None and right is None:
                continue
            if (left is None) != (right is None):
                return {"status": "FAIL", "reason": "NULL_MISMATCH", "column": col, "index": idx}
            if isinstance(left, bool) or isinstance(right, bool):
                if bool(left) != bool(right):
                    return {"status": "FAIL", "reason": "BOOL_MISMATCH", "column": col, "index": idx}
                continue
            if not math.isclose(float(left), float(right), rel_tol=1e-7, abs_tol=1e-9):
                return {
                    "status": "FAIL",
                    "reason": "VALUE_MISMATCH",
                    "column": col,
                    "index": idx,
                    "left": float(left),
                    "right": float(right),
                }

    return {"status": "PASS", "market": market, "checked_columns": len(compare_cols)}


def _join_factor_frames_for_smoke(
    *,
    config: FeaturesConfig,
    tf: str,
    frame: pl.DataFrame,
    from_ts_ms: int,
    to_ts_ms: int,
) -> pl.DataFrame:
    if not config.feature_set_v1.enable_factor_features:
        return frame
    rv_window = max(config.feature_set_v1.windows.rv)
    joined = frame
    for factor_market in config.feature_set_v1.factor_markets:
        factor_candles = _load_market_candles(
            dataset_root=config.input_dataset_root,
            tf=tf,
            market=factor_market,
            from_ts_ms=from_ts_ms,
            to_ts_ms=to_ts_ms,
        )
        factor_frame = compute_factor_feature_frame(
            factor_candles,
            market=factor_market,
            rv_window=rv_window,
            float_dtype=config.float_dtype,
        )
        joined = joined.join(factor_frame, on="ts_ms", how="left")
    return joined


def _select_universe_markets(
    *,
    config: FeaturesConfig,
    available_markets: list[str],
    tf: str,
    quote: str,
    top_n: int,
    start_ts_ms: int,
) -> list[str]:
    if top_n <= 0:
        return []
    mode = config.universe.mode
    if mode == "fixed_list":
        quote_prefix = f"{quote}-"
        selected = [item for item in config.universe.fixed_list if item.startswith(quote_prefix) and item in available_markets]
        return selected[:top_n]

    lookback_ms = max(1, config.universe.lookback_days) * 86_400_000
    lookback_from = start_ts_ms - lookback_ms
    lookback_to = start_ts_ms - expected_interval_ms(tf)
    ranked = _rank_markets_by_trade_value(
        dataset_root=config.input_dataset_root,
        tf=tf,
        markets=available_markets,
        from_ts_ms=lookback_from,
        to_ts_ms=lookback_to,
    )
    if not ranked:
        fallback_to = start_ts_ms + lookback_ms - expected_interval_ms(tf)
        ranked = _rank_markets_by_trade_value(
            dataset_root=config.input_dataset_root,
            tf=tf,
            markets=available_markets,
            from_ts_ms=start_ts_ms,
            to_ts_ms=fallback_to,
        )
    selected = [market for market, _ in ranked[:top_n]]
    if len(selected) < top_n:
        seen = set(selected)
        for market in available_markets:
            if market in seen:
                continue
            selected.append(market)
            seen.add(market)
            if len(selected) >= top_n:
                break
    return selected[:top_n]


def _rank_markets_by_trade_value(
    *,
    dataset_root: Path,
    tf: str,
    markets: list[str],
    from_ts_ms: int,
    to_ts_ms: int,
) -> list[tuple[str, float]]:
    ranked: list[tuple[str, float]] = []
    for market in markets:
        part_file = _input_part_file(dataset_root=dataset_root, tf=tf, market=market)
        if not part_file.exists():
            continue
        lazy = (
            pl.scan_parquet(str(part_file))
            .filter((pl.col("ts_ms") >= from_ts_ms) & (pl.col("ts_ms") <= to_ts_ms))
            .select(
                pl.when(pl.col("volume_quote").is_not_null())
                .then(pl.col("volume_quote"))
                .otherwise(pl.col("close") * pl.col("volume_base"))
                .sum()
                .alias("trade_value")
            )
        )
        frame = _collect_lazy(lazy)
        value = frame.item(row=0, column="trade_value") if frame.height > 0 else None
        trade_value = float(value) if value is not None else 0.0
        ranked.append((market, trade_value))
    ranked.sort(key=lambda item: (-item[1], item[0]))
    return ranked


def _list_markets(*, input_root: Path, tf: str, quote: str) -> list[str]:
    tf_dir = input_root / f"tf={tf}"
    if not tf_dir.exists():
        return []
    quote_prefix = f"{quote}-"
    markets: list[str] = []
    for entry in tf_dir.iterdir():
        if not entry.is_dir() or not entry.name.startswith("market="):
            continue
        market = entry.name.replace("market=", "", 1).strip().upper()
        if not market or not market.startswith(quote_prefix):
            continue
        markets.append(market)
    markets.sort()
    return markets


def _load_market_candles(
    *,
    dataset_root: Path,
    tf: str,
    market: str,
    from_ts_ms: int,
    to_ts_ms: int,
) -> pl.DataFrame:
    part_file = _input_part_file(dataset_root=dataset_root, tf=tf, market=market)
    if not part_file.exists():
        return pl.DataFrame()
    lazy = pl.scan_parquet(str(part_file))
    schema = lazy.collect_schema()
    columns = ["ts_ms", "open", "high", "low", "close", "volume_base", "volume_quote", "volume_quote_est"]
    existing = [name for name in columns if name in schema.names()]
    lazy = lazy.filter((pl.col("ts_ms") >= from_ts_ms) & (pl.col("ts_ms") <= to_ts_ms)).select(existing)
    frame = _collect_lazy(lazy).sort("ts_ms")
    interval_ms = expected_interval_ms(tf)
    frame = (
        frame.with_columns((((pl.col("ts_ms") // interval_ms) * interval_ms).cast(pl.Int64)).alias("ts_ms"))
        .sort("ts_ms")
        .unique(subset=["ts_ms"], keep="last", maintain_order=True)
    )
    if "volume_quote" not in frame.columns:
        frame = frame.with_columns(pl.lit(None, dtype=pl.Float64).alias("volume_quote"))
    if "volume_quote_est" not in frame.columns:
        frame = frame.with_columns(pl.lit(False, dtype=pl.Boolean).alias("volume_quote_est"))
    return frame.select(columns)


def _cast_output_dtypes(output: pl.DataFrame, *, float_dtype: str) -> pl.DataFrame:
    float_pl = pl.Float64 if str(float_dtype).strip().lower() == "float64" else pl.Float32
    cast_exprs: list[pl.Expr] = []
    for name in output.columns:
        if name == "ts_ms":
            cast_exprs.append(pl.col(name).cast(pl.Int64).alias(name))
        elif name in {"is_gap", "candle_ok"}:
            cast_exprs.append(pl.col(name).cast(pl.Boolean).alias(name))
        elif name == "y_cls":
            cast_exprs.append(pl.col(name).cast(pl.Int8).alias(name))
        elif output.schema[name] in {pl.Float32, pl.Float64}:
            cast_exprs.append(pl.col(name).cast(float_pl).alias(name))
        else:
            cast_exprs.append(pl.col(name).alias(name))
    return output.with_columns(cast_exprs)


def _null_ratio(frame: pl.DataFrame, columns: list[str]) -> tuple[float, dict[str, float]]:
    if frame.height <= 0 or not columns:
        return 0.0, {col: 0.0 for col in columns}
    ratios: dict[str, float] = {}
    total_null = 0
    for col in columns:
        null_count = int(frame.get_column(col).null_count()) if col in frame.columns else frame.height
        total_null += null_count
        ratios[col] = float(null_count) / float(frame.height)
    overall = float(total_null) / float(frame.height * len(columns))
    return overall, ratios


def _overall_null_ratio_from_map(ratios: dict[str, float]) -> float:
    if not ratios:
        return 0.0
    return float(sum(ratios.values())) / float(len(ratios))


def _is_non_monotonic(ts: pl.Series) -> bool:
    if ts.len() <= 1:
        return False
    diff = ts.diff().drop_nulls()
    if diff.len() <= 0:
        return False
    return bool((diff < 0).any())


def _feature_part_path(*, dataset_root: Path, tf: str, market: str) -> Path:
    return dataset_root / f"tf={tf}" / f"market={market}" / "part-000.parquet"


def _input_part_file(*, dataset_root: Path, tf: str, market: str) -> Path:
    market_dir = dataset_root / f"tf={tf}" / f"market={market}"
    legacy = market_dir / "part.parquet"
    if legacy.exists():
        return legacy
    # Support modern partition writer output names.
    canonical = market_dir / "part-000.parquet"
    if canonical.exists():
        return canonical
    parts = sorted(path for path in market_dir.glob("part-*.parquet") if path.is_file())
    if parts:
        return parts[0]
    return legacy


def _input_files_fingerprint(*, dataset_root: Path, tf: str, markets: list[str]) -> str:
    rows: list[str] = []
    for market in markets:
        part = _input_part_file(dataset_root=dataset_root, tf=tf, market=market)
        if not part.exists():
            rows.append(f"{market}|missing")
            continue
        stat = part.stat()
        rows.append(f"{market}|{stat.st_size}|{int(stat.st_mtime)}")
    return sha256_json(rows)


def _collect_lazy(lazy_frame: pl.LazyFrame) -> pl.DataFrame:
    try:
        return lazy_frame.collect(engine="streaming")
    except TypeError:
        return lazy_frame.collect(streaming=True)


def _feature_set_config_to_dict(config: FeatureSetV1Config) -> dict[str, Any]:
    return {
        "windows": {
            "ret": list(config.windows.ret),
            "rv": list(config.windows.rv),
            "ema": list(config.windows.ema),
            "rsi": config.windows.rsi,
            "atr": config.windows.atr,
            "vol_z": config.windows.vol_z,
        },
        "enable_factor_features": bool(config.enable_factor_features),
        "factor_markets": list(config.factor_markets),
        "enable_liquidity_rank": bool(config.enable_liquidity_rank),
    }


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _safe_min(current: int | None, candidate: Any) -> int | None:
    if candidate is None:
        return current
    value = int(candidate)
    if current is None:
        return value
    return min(current, value)


def _safe_max(current: int | None, candidate: Any) -> int | None:
    if candidate is None:
        return current
    value = int(candidate)
    if current is None:
        return value
    return max(current, value)


def _safe_min_many(values: list[Any]) -> int | None:
    current: int | None = None
    for value in values:
        current = _safe_min(current, value)
    return current


def _safe_max_many(values: list[Any]) -> int | None:
    current: int | None = None
    for value in values:
        current = _safe_max(current, value)
    return current
