"""Feature store v2 pipeline (OHLC v1-equivalent + micro_v1 join + label guard)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
import time
from typing import Any

import polars as pl
import yaml

from autobot.data import expected_interval_ms

from .feature_set_v1 import compute_factor_feature_frame
from .feature_set_v2 import (
    FeatureSetV2BuildResult,
    MicroFilterPolicy,
    apply_label_tail_guard,
    attach_micro_and_filter,
    build_feature_set_v2_from_candles,
)
from .feature_spec import (
    FeatureSetV1Config,
    LabelV1Config,
    TimeRangeConfig,
    UniverseConfig,
    effective_threshold_bps,
    factor_prefix,
    feature_columns,
    label_columns,
    load_features_config,
    max_feature_lookback_bars,
    parse_date_to_ts_ms,
    sha256_file,
    sha256_json,
    write_json,
)
from .labeling_v1 import drop_neutral_rows, label_distribution
from .micro_join import join_market_micro, load_market_micro_frame, prefixed_micro_columns, resolve_dataset_path
from .v2_manifest import append_manifest_rows, load_manifest, manifest_path, save_manifest


DEFAULT_FEATURES_V2_YAML = "features_v2.yaml"


@dataclass(frozen=True)
class FeaturesV2BuildConfig:
    output_dataset: str = "features_v2"
    tf: str = "5m"
    base_candles_dataset: str = "auto"
    micro_dataset: str = "micro_v1"
    alignment_mode: str = "auto"
    use_precomputed_features_v1: bool = False
    precomputed_features_v1_dataset: str = "features_v1"
    min_rows_for_train: int = 5000


@dataclass(frozen=True)
class FeaturesV2ValidateConfig:
    join_match_warn: float = 0.98
    join_match_fail: float = 0.90


@dataclass(frozen=True)
class FeaturesV2Config:
    build: FeaturesV2BuildConfig
    parquet_root: Path
    features_root: Path
    universe: UniverseConfig
    time_range: TimeRangeConfig
    feature_set_v1: FeatureSetV1Config
    label_v1: LabelV1Config
    micro_filter: MicroFilterPolicy
    validation: FeaturesV2ValidateConfig
    float_dtype: str = "float32"

    @property
    def dataset_name(self) -> str:
        return self.build.output_dataset

    @property
    def output_dataset_root(self) -> Path:
        return self.features_root / self.dataset_name

    @property
    def precomputed_features_v1_root(self) -> Path:
        return self.features_root / self.build.precomputed_features_v1_dataset


@dataclass(frozen=True)
class FeatureBuildV2Options:
    tf: str
    quote: str | None = None
    top_n: int | None = None
    start: str | None = None
    end: str | None = None
    feature_set: str = "v2"
    label_set: str = "v1"
    workers: int = 1
    fail_on_warn: bool = False
    dry_run: bool = False
    base_candles: str | None = None
    micro_dataset: str | None = None
    require_micro: bool | None = None
    min_trade_events: int | None = None
    min_trade_coverage_ms: int | None = None
    min_book_events: int | None = None
    min_book_coverage_ms: int | None = None
    use_precomputed_features_v1: bool | None = None


@dataclass(frozen=True)
class FeatureBuildV2Summary:
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
    base_candles_root: Path | None
    micro_root: Path
    details: tuple[dict[str, Any], ...]
    failures: tuple[dict[str, Any], ...]
    preflight_ok: bool


@dataclass(frozen=True)
class FeatureValidateV2Options:
    tf: str
    quote: str | None = None
    top_n: int | None = None
    join_match_warn: float | None = None
    join_match_fail: float | None = None


@dataclass(frozen=True)
class FeatureValidateV2Summary:
    checked_files: int
    ok_files: int
    warn_files: int
    fail_files: int
    schema_ok: bool
    null_ratio_overall: float
    worst_columns_top5: tuple[dict[str, Any], ...]
    label_distribution: dict[str, int]
    join_match_ratio: float | None
    micro_available_ratio: float
    validate_report_file: Path
    details: tuple[dict[str, Any], ...]


def load_features_v2_config(
    config_dir: Path,
    *,
    base_config: dict[str, Any] | None = None,
    filename: str = DEFAULT_FEATURES_V2_YAML,
) -> FeaturesV2Config:
    shared = load_features_config(config_dir, base_config=base_config, filename=filename)
    raw = _load_yaml_doc(config_dir / filename)
    root = raw.get("features_v2", {}) if isinstance(raw.get("features_v2"), dict) else {}
    filter_cfg = root.get("micro_filter", {}) if isinstance(root.get("micro_filter"), dict) else {}
    validate_cfg = root.get("validation", {}) if isinstance(root.get("validation"), dict) else {}

    dataset_name = str(root.get("output_dataset", shared.dataset_name)).strip() or "features_v2"
    alignment_mode = str(root.get("alignment_mode", "auto")).strip().lower() or "auto"
    if alignment_mode not in {"auto", "start", "end"}:
        raise ValueError("features_v2.alignment_mode must be one of: auto,start,end")

    return FeaturesV2Config(
        build=FeaturesV2BuildConfig(
            output_dataset=dataset_name,
            tf=str(root.get("tf", "5m")).strip().lower() or "5m",
            base_candles_dataset=str(root.get("base_candles_dataset", "auto")).strip() or "auto",
            micro_dataset=str(root.get("micro_dataset", "micro_v1")).strip() or "micro_v1",
            alignment_mode=alignment_mode,
            use_precomputed_features_v1=bool(root.get("use_precomputed_features_v1", False)),
            precomputed_features_v1_dataset=str(root.get("precomputed_features_v1_dataset", "features_v1")).strip()
            or "features_v1",
            min_rows_for_train=max(int(root.get("min_rows_for_train", 5000)), 1),
        ),
        parquet_root=shared.parquet_root,
        features_root=shared.features_root,
        universe=shared.universe,
        time_range=shared.time_range,
        feature_set_v1=shared.feature_set_v1,
        label_v1=shared.label_v1,
        micro_filter=MicroFilterPolicy(
            require_micro_available=bool(filter_cfg.get("require_micro_available", True)),
            min_trade_events=max(int(filter_cfg.get("min_trade_events", 1)), 0),
            min_trade_coverage_ms=max(int(filter_cfg.get("min_trade_coverage_ms", 60_000)), 0),
            min_book_events=max(int(filter_cfg.get("min_book_events", 1)), 0),
            min_book_coverage_ms=max(int(filter_cfg.get("min_book_coverage_ms", 60_000)), 0),
        ),
        validation=FeaturesV2ValidateConfig(
            join_match_warn=float(validate_cfg.get("join_match_warn", 0.98)),
            join_match_fail=float(validate_cfg.get("join_match_fail", 0.90)),
        ),
        float_dtype=shared.float_dtype,
    )


def build_features_dataset_v2(config: FeaturesV2Config, options: FeatureBuildV2Options) -> FeatureBuildV2Summary:
    if options.feature_set != "v2":
        raise ValueError("feature_set currently supports only v2 in pipeline_v2")
    if options.label_set != "v1":
        raise ValueError("label_set currently supports only v1")

    started_at = int(time.time())
    tf = str(options.tf or config.build.tf).strip().lower()
    quote = str(options.quote or config.universe.quote).strip().upper()
    top_n = max(1, int(options.top_n if options.top_n is not None else config.universe.top_n))
    start_text = str(options.start or config.time_range.start).strip()
    end_text = str(options.end or config.time_range.end).strip()
    start_ts_ms = parse_date_to_ts_ms(start_text)
    end_ts_ms = parse_date_to_ts_ms(end_text, end_of_day=True)
    if end_ts_ms < start_ts_ms:
        raise ValueError("time_range.end must be >= time_range.start")

    interval_ms = expected_interval_ms(tf)
    horizon = max(1, int(config.label_v1.horizon_bars))
    warmup_bars = max_feature_lookback_bars(config.feature_set_v1, tf=tf) + 2
    extended_start_ts_ms = start_ts_ms - warmup_bars * interval_ms
    extended_end_ts_ms = end_ts_ms + horizon * interval_ms

    output_root = config.output_dataset_root
    output_root.mkdir(parents=True, exist_ok=True)
    meta_root = output_root / "_meta"
    meta_root.mkdir(parents=True, exist_ok=True)

    micro_root = resolve_dataset_path(
        dataset=(options.micro_dataset or config.build.micro_dataset),
        parquet_root=config.parquet_root,
    )
    if not micro_root.exists():
        raise ValueError(f"micro dataset not found: {micro_root}")

    market_windows = _discover_micro_market_windows(
        micro_root=micro_root,
        tf=tf,
        quote=quote,
        start_ts_ms=start_ts_ms,
        end_ts_ms=end_ts_ms,
    )
    discovered_markets = len(market_windows)
    selected_markets = [item["market"] for item in market_windows[:top_n]]
    if not selected_markets:
        raise ValueError(
            f"no micro markets found for tf={tf} quote={quote} in range {start_text}~{end_text}; "
            "run T13.1d micro aggregate first."
        )

    preflight = _run_preflight(
        parquet_root=config.parquet_root,
        base_candles_value=str(options.base_candles or config.build.base_candles_dataset).strip() or "auto",
        tf=tf,
        selected_markets=selected_markets,
        market_windows=market_windows,
        interval_ms=interval_ms,
        horizon=horizon,
        start_text=start_text,
        end_text=end_text,
    )
    if not preflight["ok"]:
        build_report_file = meta_root / "build_report.json"
        write_json(
            build_report_file,
            {
                "started_at": started_at,
                "finished_at": int(time.time()),
                "dataset_name": config.dataset_name,
                "tf": tf,
                "quote": quote,
                "requested_top_n": top_n,
                "selected_markets": selected_markets,
                "preflight": preflight,
                "status": "FAIL",
                "error_message": preflight["message"],
            },
        )
        raise ValueError(str(preflight["message"]))

    base_candles_root = Path(str(preflight["selected_base_candles_root"]))
    target_dates = _date_strings_between(start_text, end_text)
    for market in selected_markets:
        _cleanup_market_target_dates(dataset_root=output_root, tf=tf, market=market, target_dates=target_dates)

    feature_cols = _dedupe_preserve(feature_columns(config.feature_set_v1) + prefixed_micro_columns())
    label_cols = label_columns()
    feature_cols_hash = sha256_json(feature_cols)
    label_cols_hash = sha256_json(label_cols)
    micro_filter = _resolved_micro_filter(config=config, options=options)
    use_precomputed = (
        bool(options.use_precomputed_features_v1)
        if options.use_precomputed_features_v1 is not None
        else bool(config.build.use_precomputed_features_v1)
    )
    factor_frames = _build_factor_frames(
        config=config,
        base_candles_root=base_candles_root,
        tf=tf,
        from_ts_ms=extended_start_ts_ms,
        to_ts_ms=extended_end_ts_ms,
    )

    if options.dry_run:
        build_report_file = meta_root / "build_report.json"
        write_json(
            build_report_file,
            {
                "started_at": started_at,
                "finished_at": int(time.time()),
                "dataset_name": config.dataset_name,
                "tf": tf,
                "quote": quote,
                "requested_top_n": top_n,
                "selected_markets": selected_markets,
                "base_candles_root": str(base_candles_root),
                "micro_root": str(micro_root),
                "preflight": preflight,
                "micro_filter": _micro_filter_to_dict(micro_filter),
                "dry_run": True,
                "status": "PASS",
            },
        )
        return FeatureBuildV2Summary(
            discovered_markets=discovered_markets,
            selected_markets=tuple(selected_markets),
            processed_markets=0,
            ok_markets=0,
            warn_markets=0,
            fail_markets=0,
            rows_total=0,
            min_ts_ms=None,
            max_ts_ms=None,
            output_path=output_root,
            manifest_file=manifest_path(output_root),
            build_report_file=build_report_file,
            feature_spec_hash=feature_cols_hash,
            label_spec_hash=label_cols_hash,
            base_candles_root=base_candles_root,
            micro_root=micro_root,
            details=tuple(),
            failures=tuple(),
            preflight_ok=True,
        )

    details: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    market_payloads: list[dict[str, Any]] = []
    manifest_rows: list[dict[str, Any]] = []
    rows_total = 0
    min_ts_ms_total: int | None = None
    max_ts_ms_total: int | None = None

    for market in selected_markets:
        try:
            micro_frame = load_market_micro_frame(
                micro_root=micro_root,
                tf=tf,
                market=market,
                from_ts_ms=start_ts_ms,
                to_ts_ms=end_ts_ms,
            )
            mode = "recompute_v1_equivalent"
            result: FeatureSetV2BuildResult
            if use_precomputed:
                precomputed = _load_precomputed_market_frame(
                    dataset_root=config.precomputed_features_v1_root,
                    tf=tf,
                    market=market,
                    from_ts_ms=start_ts_ms,
                    to_ts_ms=end_ts_ms,
                )
                if precomputed.height > 0:
                    mode = "precomputed_features_v1"
                    pre_guarded, tail_dropped = apply_label_tail_guard(
                        precomputed.sort("ts_ms"),
                        horizon_bars=horizon,
                    )
                    pre_guarded = drop_neutral_rows(frame=pre_guarded, config=config.label_v1)
                    joined, join_stats = join_market_micro(base_frame=pre_guarded, micro_frame=micro_frame)
                    output_frame, pre_rows, post_rows = attach_micro_and_filter(
                        base_labeled_frame=joined,
                        feature_columns_base=feature_columns(config.feature_set_v1),
                        policy=micro_filter,
                        float_dtype=config.float_dtype,
                    )
                    result = FeatureSetV2BuildResult(
                        frame=output_frame,
                        feature_columns=tuple(feature_cols),
                        label_columns=tuple(label_cols),
                        join_stats=join_stats,
                        prefilter_rows=pre_rows,
                        postfilter_rows=post_rows,
                        tail_dropped_rows=tail_dropped,
                    )
                else:
                    candles = _load_market_candles(
                        dataset_root=base_candles_root,
                        tf=tf,
                        market=market,
                        from_ts_ms=extended_start_ts_ms,
                        to_ts_ms=extended_end_ts_ms,
                    )
                    result = build_feature_set_v2_from_candles(
                        candles_frame=candles,
                        micro_frame=micro_frame,
                        tf=tf,
                        from_ts_ms=start_ts_ms,
                        to_ts_ms=end_ts_ms,
                        feature_config=config.feature_set_v1,
                        label_config=config.label_v1,
                        micro_filter=micro_filter,
                        factor_frames=factor_frames,
                        float_dtype=config.float_dtype,
                    )
            else:
                candles = _load_market_candles(
                    dataset_root=base_candles_root,
                    tf=tf,
                    market=market,
                    from_ts_ms=extended_start_ts_ms,
                    to_ts_ms=extended_end_ts_ms,
                )
                result = build_feature_set_v2_from_candles(
                    candles_frame=candles,
                    micro_frame=micro_frame,
                    tf=tf,
                    from_ts_ms=start_ts_ms,
                    to_ts_ms=end_ts_ms,
                    feature_config=config.feature_set_v1,
                    label_config=config.label_v1,
                    micro_filter=micro_filter,
                    factor_frames=factor_frames,
                    float_dtype=config.float_dtype,
                )

            market_payloads.append(
                {
                    "market": market,
                    "frame": result.frame,
                    "join": result.join_stats,
                    "prefilter_rows": result.prefilter_rows,
                    "postfilter_rows": result.postfilter_rows,
                    "tail_dropped_rows": result.tail_dropped_rows,
                    "mode": mode,
                }
            )
        except Exception as exc:
            detail = {"market": market, "status": "FAIL", "reasons": ["BUILD_EXCEPTION"], "error_message": str(exc)}
            details.append(detail)
            failures.append(detail)
            manifest_rows.append(
                _manifest_fail_row(
                    dataset_name=config.dataset_name,
                    tf=tf,
                    market=market,
                    feature_cols_hash=feature_cols_hash,
                    label_cols_hash=label_cols_hash,
                    built_at=int(time.time()),
                    error_message=str(exc),
                )
            )

    if config.feature_set_v1.enable_liquidity_rank:
        _apply_liquidity_rank_to_payloads(market_payloads=market_payloads, float_dtype=config.float_dtype)

    for payload in market_payloads:
        market = str(payload["market"])
        frame = payload["frame"]
        join_stats = payload["join"]
        prefilter_rows = int(payload["prefilter_rows"])
        postfilter_rows = int(payload["postfilter_rows"])
        tail_dropped_rows = int(payload["tail_dropped_rows"])
        mode = str(payload["mode"])

        part_files = _write_market_date_partitions(
            frame=frame,
            dataset_root=output_root,
            tf=tf,
            market=market,
        )
        output_cols = ["ts_ms"] + feature_cols + label_cols
        output = _ensure_columns(frame, output_cols).select(output_cols).sort("ts_ms")
        rows = int(output.height)
        min_ts_ms = int(output.get_column("ts_ms").min()) if rows > 0 else None
        max_ts_ms = int(output.get_column("ts_ms").max()) if rows > 0 else None
        null_ratio_overall, null_ratio_by_col = _null_ratio(output, output_cols)
        micro_available_ratio = (
            float(output.get_column("m_micro_available").cast(pl.Int64).sum()) / float(rows)
            if rows > 0 and "m_micro_available" in output.columns
            else 0.0
        )
        trade_p50, trade_p90 = _coverage_quantiles(output, flag_col="m_micro_trade_available", coverage_col="m_trade_coverage_ms")
        book_p50, book_p90 = _coverage_quantiles(output, flag_col="m_micro_book_available", coverage_col="m_book_coverage_ms")

        status = "OK"
        reasons: list[str] = []
        join_ratio = join_stats.join_match_ratio
        if rows <= 0:
            status = "WARN"
            reasons.append("NO_ROWS_AFTER_FILTERS")
        if join_stats.compared_rows <= 0 or join_ratio is None:
            status = "FAIL"
            reasons.append("NO_OVERLAP_WITH_MICRO")
        elif float(join_ratio) < float(config.validation.join_match_fail) and status != "FAIL":
            status = "WARN"
            reasons.append("LOW_JOIN_MATCH_RATIO")
        elif float(join_ratio) < float(config.validation.join_match_warn) and status == "OK":
            status = "WARN"
            reasons.append("JOIN_MATCH_RATIO_WARN")
        if null_ratio_overall > 0.0 and status == "OK":
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
            "built_at": int(time.time()),
            "join_compared_rows": int(join_stats.compared_rows),
            "join_matched_rows": int(join_stats.matched_rows),
            "join_match_ratio": join_ratio,
            "micro_available_ratio": micro_available_ratio,
            "trade_coverage_p50_ms": trade_p50,
            "trade_coverage_p90_ms": trade_p90,
            "book_coverage_p50_ms": book_p50,
            "book_coverage_p90_ms": book_p90,
            "tail_dropped_rows": tail_dropped_rows,
            "prefilter_rows": prefilter_rows,
            "postfilter_rows": postfilter_rows,
        }
        detail = {
            "market": market,
            "status": status,
            "rows": rows,
            "min_ts_ms": min_ts_ms,
            "max_ts_ms": max_ts_ms,
            "join_match_ratio": join_ratio,
            "join_compared_rows": int(join_stats.compared_rows),
            "join_matched_rows": int(join_stats.matched_rows),
            "micro_available_ratio": micro_available_ratio,
            "trade_coverage_ms": {"p50": trade_p50, "p90": trade_p90},
            "book_coverage_ms": {"p50": book_p50, "p90": book_p90},
            "tail_dropped_rows": tail_dropped_rows,
            "prefilter_rows": prefilter_rows,
            "postfilter_rows": postfilter_rows,
            "mode": mode,
            "part_files": [str(path) for path in part_files],
            "reasons": reasons,
        }
        manifest_rows.append(manifest_row)
        details.append(detail)
        if status == "FAIL":
            failures.append(detail)
        else:
            rows_total += rows
            min_ts_ms_total = _safe_min(min_ts_ms_total, min_ts_ms)
            max_ts_ms_total = _safe_max(max_ts_ms_total, max_ts_ms)

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
        base_candles_root=base_candles_root,
        micro_root=micro_root,
        preflight=preflight,
    )
    label_spec_payload = _build_label_spec_payload(config=config, tf=tf, label_cols=label_cols)
    feature_spec_hash = sha256_json(feature_spec_payload)
    label_spec_hash = sha256_json(label_spec_payload)
    write_json(meta_root / "feature_spec.json", feature_spec_payload)
    write_json(meta_root / "label_spec.json", label_spec_payload)

    ok_markets = sum(1 for row in manifest_rows if str(row.get("status", "")).upper() == "OK")
    warn_markets = sum(1 for row in manifest_rows if str(row.get("status", "")).upper() == "WARN")
    fail_markets = sum(1 for row in manifest_rows if str(row.get("status", "")).upper() == "FAIL")

    build_report = {
        "started_at": started_at,
        "finished_at": int(time.time()),
        "dataset_name": config.dataset_name,
        "tf": tf,
        "quote": quote,
        "requested_top_n": top_n,
        "discovered_markets": discovered_markets,
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
        "base_candles_root": str(base_candles_root),
        "micro_root": str(micro_root),
        "preflight": preflight,
        "micro_filter": _micro_filter_to_dict(micro_filter),
        "use_precomputed_features_v1": use_precomputed,
        "details": details,
        "failures": failures,
    }
    build_report_file = meta_root / "build_report.json"
    write_json(build_report_file, build_report)

    return FeatureBuildV2Summary(
        discovered_markets=discovered_markets,
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
        base_candles_root=base_candles_root,
        micro_root=micro_root,
        details=tuple(details),
        failures=tuple(failures),
        preflight_ok=True,
    )


def validate_features_dataset_v2(config: FeaturesV2Config, options: FeatureValidateV2Options) -> FeatureValidateV2Summary:
    tf = str(options.tf or config.build.tf).strip().lower()
    quote = str(options.quote or config.universe.quote).strip().upper()
    top_n = max(1, int(options.top_n if options.top_n is not None else config.universe.top_n))
    warn_threshold = (
        float(options.join_match_warn) if options.join_match_warn is not None else float(config.validation.join_match_warn)
    )
    fail_threshold = (
        float(options.join_match_fail) if options.join_match_fail is not None else float(config.validation.join_match_fail)
    )

    dataset_root = config.output_dataset_root
    meta_root = dataset_root / "_meta"
    validate_report_file = meta_root / "validate_report.json"
    manifest = load_manifest(manifest_path(dataset_root))
    manifest_rows: list[dict[str, Any]] = []
    if manifest.height > 0:
        manifest_rows = [
            dict(row)
            for row in manifest.filter((pl.col("tf") == tf) & (pl.col("market").str.starts_with(f"{quote}-"))).iter_rows(
                named=True
            )
        ]
    selected_markets = [str(item["market"]).upper() for item in manifest_rows][:top_n]

    feature_spec = _read_json(meta_root / "feature_spec.json")
    label_spec = _read_json(meta_root / "label_spec.json")
    expected_feature_cols = feature_spec.get("feature_columns") if isinstance(feature_spec, dict) else None
    expected_label_cols = label_spec.get("label_columns") if isinstance(label_spec, dict) else None
    if not isinstance(expected_feature_cols, list):
        expected_feature_cols = _dedupe_preserve(feature_columns(config.feature_set_v1) + prefixed_micro_columns())
    if not isinstance(expected_label_cols, list):
        expected_label_cols = label_columns()
    expected_columns = ["ts_ms"] + [str(item) for item in expected_feature_cols] + [str(item) for item in expected_label_cols]

    details: list[dict[str, Any]] = []
    null_counts_by_col: dict[str, int] = {name: 0 for name in expected_columns}
    total_rows = 0
    total_cells = 0
    total_null_cells = 0
    label_total = {"pos": 0, "neg": 0, "neutral": 0, "total": 0}
    trade_coverages_all: list[int] = []
    book_coverages_all: list[int] = []
    micro_available_rows = 0
    total_join_compared = 0
    total_join_matched = 0

    ok_files = 0
    warn_files = 0
    fail_files = 0
    for market in selected_markets:
        files = _feature_v2_part_files(dataset_root=dataset_root, tf=tf, market=market)
        if not files:
            details.append({"market": market, "status": "WARN", "reasons": ["MISSING_OUTPUT_FILE"], "files": []})
            warn_files += 1
            continue

        frame = _load_feature_v2_market(dataset_root=dataset_root, tf=tf, market=market)
        rows = int(frame.height)
        missing_columns = [name for name in expected_columns if name not in frame.columns]
        non_monotonic = _is_non_monotonic(frame.get_column("ts_ms")) if "ts_ms" in frame.columns else True
        min_ts_ms = int(frame.get_column("ts_ms").min()) if rows > 0 and "ts_ms" in frame.columns else None
        max_ts_ms = int(frame.get_column("ts_ms").max()) if rows > 0 and "ts_ms" in frame.columns else None

        null_ratio_by_col: dict[str, float] = {}
        for col in expected_columns:
            null_count = int(frame.get_column(col).null_count()) if col in frame.columns else rows
            null_counts_by_col[col] += null_count
            null_ratio_by_col[col] = (float(null_count) / float(rows)) if rows > 0 else 0.0
            total_null_cells += null_count
        total_rows += rows
        total_cells += rows * len(expected_columns)

        dist = label_distribution(frame=frame, config=config.label_v1)
        label_total["pos"] += int(dist["pos"])
        label_total["neg"] += int(dist["neg"])
        label_total["neutral"] += int(dist["neutral"])
        label_total["total"] += int(dist["total"])

        market_manifest = next((row for row in manifest_rows if str(row.get("market")) == market), {})
        join_compared = int(market_manifest.get("join_compared_rows") or 0)
        join_matched = int(market_manifest.get("join_matched_rows") or 0)
        join_ratio = market_manifest.get("join_match_ratio")
        join_ratio = float(join_ratio) if join_ratio is not None else None
        total_join_compared += join_compared
        total_join_matched += join_matched

        status = "OK"
        reasons: list[str] = []
        if missing_columns:
            status = "FAIL"
            reasons.append("MISSING_COLUMNS")
        if non_monotonic:
            status = "FAIL"
            reasons.append("TS_NON_MONOTONIC")
        if rows <= 0 and status != "FAIL":
            status = "WARN"
            reasons.append("NO_ROWS")
        if join_compared <= 0:
            status = "FAIL"
            reasons.append("NO_JOIN_OVERLAP")
        elif join_ratio is None:
            status = "FAIL"
            reasons.append("JOIN_MATCH_RATIO_MISSING")
        elif float(join_ratio) < fail_threshold and status != "FAIL":
            status = "WARN"
            reasons.append("LOW_JOIN_MATCH_RATIO")
        elif float(join_ratio) < warn_threshold and status == "OK":
            status = "WARN"
            reasons.append("JOIN_MATCH_RATIO_WARN")

        null_ratio_overall = _overall_null_ratio_from_map(null_ratio_by_col)
        if null_ratio_overall > 0.0 and status == "OK":
            status = "WARN"
            reasons.append("NULLS_PRESENT")

        if "m_micro_available" in frame.columns and rows > 0:
            micro_available_rows += int(frame.get_column("m_micro_available").cast(pl.Int64).sum())
        trade_p50, trade_p90, trade_values = _coverage_quantiles_with_values(
            frame,
            flag_col="m_micro_trade_available",
            coverage_col="m_trade_coverage_ms",
        )
        book_p50, book_p90, book_values = _coverage_quantiles_with_values(
            frame,
            flag_col="m_micro_book_available",
            coverage_col="m_book_coverage_ms",
        )
        trade_coverages_all.extend(trade_values)
        book_coverages_all.extend(book_values)

        if status == "OK":
            ok_files += 1
        elif status == "WARN":
            warn_files += 1
        else:
            fail_files += 1

        details.append(
            {
                "market": market,
                "files": [str(path) for path in files],
                "rows": rows,
                "min_ts_ms": min_ts_ms,
                "max_ts_ms": max_ts_ms,
                "missing_columns": missing_columns,
                "non_monotonic": non_monotonic,
                "null_ratio_overall": null_ratio_overall,
                "join_match_ratio": join_ratio,
                "join_compared_rows": join_compared,
                "join_matched_rows": join_matched,
                "micro_available_ratio": (
                    float(frame.get_column("m_micro_available").cast(pl.Int64).sum()) / float(rows)
                    if rows > 0 and "m_micro_available" in frame.columns
                    else 0.0
                ),
                "trade_coverage_ms": {"p50": trade_p50, "p90": trade_p90},
                "book_coverage_ms": {"p50": book_p50, "p90": book_p90},
                "label_distribution": dist,
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

    join_match_ratio_total = (
        float(total_join_matched) / float(total_join_compared) if total_join_compared > 0 else None
    )
    micro_available_ratio_total = (float(micro_available_rows) / float(total_rows)) if total_rows > 0 else 0.0
    trade_p50_all = _quantile(trade_coverages_all, 0.5) if trade_coverages_all else None
    trade_p90_all = _quantile(trade_coverages_all, 0.9) if trade_coverages_all else None
    book_p50_all = _quantile(book_coverages_all, 0.5) if book_coverages_all else None
    book_p90_all = _quantile(book_coverages_all, 0.9) if book_coverages_all else None

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
        "micro_join_quality": {
            "join_match_ratio": join_match_ratio_total,
            "join_compared_rows": total_join_compared,
            "join_matched_rows": total_join_matched,
            "micro_available_ratio": micro_available_ratio_total,
            "trade_coverage_ms": {"p50": trade_p50_all, "p90": trade_p90_all},
            "book_coverage_ms": {"p50": book_p50_all, "p90": book_p90_all},
        },
        "thresholds": {"join_match_warn": warn_threshold, "join_match_fail": fail_threshold},
        "details": details,
    }
    write_json(validate_report_file, report)

    return FeatureValidateV2Summary(
        checked_files=len(details),
        ok_files=ok_files,
        warn_files=warn_files,
        fail_files=fail_files,
        schema_ok=(fail_files == 0),
        null_ratio_overall=null_ratio_overall,
        worst_columns_top5=worst_top5,
        label_distribution=label_total,
        join_match_ratio=join_match_ratio_total,
        micro_available_ratio=micro_available_ratio_total,
        validate_report_file=validate_report_file,
        details=tuple(details),
    )


def features_stats_v2(
    config: FeaturesV2Config,
    *,
    tf: str,
    quote: str | None = None,
    top_n: int | None = None,
) -> dict[str, Any]:
    tf_value = str(tf or config.build.tf).strip().lower()
    quote_value = str(quote or config.universe.quote).strip().upper()
    top_n_value = max(1, int(top_n if top_n is not None else config.universe.top_n))

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
            "join_match_ratio": None,
            "micro_available_ratio": 0.0,
            "ready_for_t14_2": False,
            "min_rows_for_train": config.build.min_rows_for_train,
        }

    filtered = manifest.filter((pl.col("tf") == tf_value) & (pl.col("market").str.starts_with(f"{quote_value}-")))
    filtered = filtered.head(top_n_value)
    rows = [dict(item) for item in filtered.iter_rows(named=True)]
    markets = [str(item.get("market")).upper() for item in rows]
    rows_total = int(sum(int(item.get("rows") or 0) for item in rows))
    min_ts_ms = _safe_min_many([item.get("min_ts_ms") for item in rows])
    max_ts_ms = _safe_max_many([item.get("max_ts_ms") for item in rows])

    status_counts = {"OK": 0, "WARN": 0, "FAIL": 0}
    join_compared = 0
    join_matched = 0
    micro_available_weighted = 0.0
    for item in rows:
        status = str(item.get("status") or "").upper()
        if status in status_counts:
            status_counts[status] += 1
        row_count = int(item.get("rows") or 0)
        join_compared += int(item.get("join_compared_rows") or 0)
        join_matched += int(item.get("join_matched_rows") or 0)
        micro_available_weighted += float(item.get("micro_available_ratio") or 0.0) * float(row_count)

    join_ratio = (float(join_matched) / float(join_compared)) if join_compared > 0 else None
    micro_available_ratio = (micro_available_weighted / float(rows_total)) if rows_total > 0 else 0.0
    ready_for_t14_2 = (
        rows_total >= int(config.build.min_rows_for_train)
        and status_counts["FAIL"] == 0
        and join_compared > 0
        and (join_ratio is not None and join_ratio >= float(config.validation.join_match_fail))
    )

    return {
        "dataset_name": config.dataset_name,
        "tf": tf_value,
        "quote": quote_value,
        "markets": markets,
        "rows_total": rows_total,
        "min_ts_ms": min_ts_ms,
        "max_ts_ms": max_ts_ms,
        "status_counts": status_counts,
        "join_match_ratio": join_ratio,
        "micro_available_ratio": micro_available_ratio,
        "ready_for_t14_2": ready_for_t14_2,
        "min_rows_for_train": int(config.build.min_rows_for_train),
    }


def _resolved_micro_filter(config: FeaturesV2Config, options: FeatureBuildV2Options) -> MicroFilterPolicy:
    return MicroFilterPolicy(
        require_micro_available=(
            bool(options.require_micro) if options.require_micro is not None else bool(config.micro_filter.require_micro_available)
        ),
        min_trade_events=(
            int(options.min_trade_events)
            if options.min_trade_events is not None
            else int(config.micro_filter.min_trade_events)
        ),
        min_trade_coverage_ms=(
            int(options.min_trade_coverage_ms)
            if options.min_trade_coverage_ms is not None
            else int(config.micro_filter.min_trade_coverage_ms)
        ),
        min_book_events=(
            int(options.min_book_events) if options.min_book_events is not None else int(config.micro_filter.min_book_events)
        ),
        min_book_coverage_ms=(
            int(options.min_book_coverage_ms)
            if options.min_book_coverage_ms is not None
            else int(config.micro_filter.min_book_coverage_ms)
        ),
    )


def _run_preflight(
    *,
    parquet_root: Path,
    base_candles_value: str,
    tf: str,
    selected_markets: list[str],
    market_windows: list[dict[str, Any]],
    interval_ms: int,
    horizon: int,
    start_text: str,
    end_text: str,
) -> dict[str, Any]:
    market_window_map = {str(item["market"]): item for item in market_windows}
    candidates = _base_candles_candidates(parquet_root=parquet_root, base_candles_value=base_candles_value)
    candidate_reports: list[dict[str, Any]] = []
    selected_root: Path | None = None

    for candidate in candidates:
        market_details: list[dict[str, Any]] = []
        fail_count = 0
        for market in selected_markets:
            window = market_window_map.get(market, {})
            micro_min = int(window.get("min_ts_ms") or 0)
            micro_max = int(window.get("max_ts_ms") or 0)
            candle_min, candle_max = _candle_bounds(dataset_root=candidate, tf=tf, market=market)
            overlap = (
                candle_min is not None
                and candle_max is not None
                and int(candle_max) >= int(micro_min)
                and int(candle_min) <= int(micro_max)
            )
            required_label_max = (int(micro_max) + int(horizon) * int(interval_ms)) if micro_max else None
            label_horizon_ok = (
                candle_max is not None and required_label_max is not None and int(candle_max) >= int(required_label_max)
            )
            ok = bool(overlap and label_horizon_ok)
            if not ok:
                fail_count += 1
            market_details.append(
                {
                    "market": market,
                    "micro_min_ts_ms": micro_min,
                    "micro_max_ts_ms": micro_max,
                    "candle_min_ts_ms": candle_min,
                    "candle_max_ts_ms": candle_max,
                    "overlap_ok": overlap,
                    "label_horizon_ok": label_horizon_ok,
                    "required_label_max_ts_ms": required_label_max,
                    "status": "OK" if ok else "FAIL",
                }
            )

        candidate_reports.append(
            {
                "base_candles_root": str(candidate),
                "checked_markets": len(selected_markets),
                "fail_markets": fail_count,
                "status": "PASS" if fail_count == 0 else "FAIL",
                "markets": market_details,
            }
        )
        if fail_count == 0 and selected_root is None:
            selected_root = candidate
            if str(base_candles_value).strip().lower() != "auto":
                break

    ok = selected_root is not None
    message = None
    if not ok:
        message = (
            "PRECONDITION_FAILED: candles coverage is insufficient for micro range. "
            f"micro={start_text}~{end_text}, tf={tf}. "
            "Run T13.1a to top-up candles_api_v1 for this window, or shrink features build range to available candles."
        )
    return {
        "ok": ok,
        "selected_base_candles_root": (str(selected_root) if selected_root is not None else None),
        "requested_base_candles": base_candles_value,
        "candidates": candidate_reports,
        "message": message,
    }


def _base_candles_candidates(*, parquet_root: Path, base_candles_value: str) -> list[Path]:
    value = str(base_candles_value).strip() or "auto"
    if value.lower() != "auto":
        return [resolve_dataset_path(dataset=value, parquet_root=parquet_root)]
    candidates = [
        resolve_dataset_path(dataset="candles_api_v1", parquet_root=parquet_root),
        resolve_dataset_path(dataset="candles_v1", parquet_root=parquet_root),
    ]
    deduped: list[Path] = []
    seen: set[str] = set()
    for path in candidates:
        key = str(path.resolve()) if path.exists() else str(path)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(path)
    return deduped


def _discover_micro_market_windows(
    *,
    micro_root: Path,
    tf: str,
    quote: str,
    start_ts_ms: int,
    end_ts_ms: int,
) -> list[dict[str, Any]]:
    tf_dir = micro_root / f"tf={tf}"
    if not tf_dir.exists():
        return []
    quote_prefix = f"{quote}-"
    start_day = datetime.fromtimestamp(int(start_ts_ms) / 1000.0, tz=timezone.utc).date()
    end_day = datetime.fromtimestamp(int(end_ts_ms) / 1000.0, tz=timezone.utc).date()
    days = {item.isoformat() for item in _date_range(start_day, end_day)}

    windows: list[dict[str, Any]] = []
    for market_dir in sorted(tf_dir.glob("market=*")):
        if not market_dir.is_dir():
            continue
        market = market_dir.name.replace("market=", "", 1).strip().upper()
        if not market.startswith(quote_prefix):
            continue
        files: list[Path] = []
        selected_dates: list[str] = []
        for date_dir in sorted(market_dir.glob("date=*")):
            if not date_dir.is_dir():
                continue
            date_value = date_dir.name.replace("date=", "", 1)
            if date_value not in days:
                continue
            selected_dates.append(date_value)
            files.extend(path for path in sorted(date_dir.glob("*.parquet")) if path.is_file())
        if not files:
            continue

        lazy = pl.scan_parquet([str(path) for path in files]).filter(
            (pl.col("ts_ms") >= int(start_ts_ms)) & (pl.col("ts_ms") <= int(end_ts_ms))
        )
        frame = _collect_lazy(
            lazy.select(
                [
                    pl.len().alias("rows"),
                    pl.col("ts_ms").min().alias("min_ts_ms"),
                    pl.col("ts_ms").max().alias("max_ts_ms"),
                ]
            )
        )
        rows = int(frame.item(row=0, column="rows")) if frame.height > 0 else 0
        if rows <= 0:
            continue
        windows.append(
            {
                "market": market,
                "rows": rows,
                "min_ts_ms": int(frame.item(row=0, column="min_ts_ms")),
                "max_ts_ms": int(frame.item(row=0, column="max_ts_ms")),
                "dates": sorted(set(selected_dates)),
            }
        )
    windows.sort(key=lambda item: (-int(item["rows"]), str(item["market"])))
    return windows


def _build_factor_frames(
    *,
    config: FeaturesV2Config,
    base_candles_root: Path,
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
            dataset_root=base_candles_root,
            tf=tf,
            market=market,
            from_ts_ms=from_ts_ms,
            to_ts_ms=to_ts_ms,
        )
        frames[prefix] = compute_factor_feature_frame(
            candles,
            market=market,
            rv_window=rv_window,
            float_dtype=config.float_dtype,
        )
    return frames


def _load_market_candles(
    *,
    dataset_root: Path,
    tf: str,
    market: str,
    from_ts_ms: int,
    to_ts_ms: int,
) -> pl.DataFrame:
    files = _candle_part_files(dataset_root=dataset_root, tf=tf, market=market)
    if not files:
        return pl.DataFrame()
    lazy = pl.scan_parquet([str(path) for path in files]).filter(
        (pl.col("ts_ms") >= int(from_ts_ms)) & (pl.col("ts_ms") <= int(to_ts_ms))
    )
    schema = lazy.collect_schema()
    cols = ["ts_ms", "open", "high", "low", "close", "volume_base", "volume_quote", "volume_quote_est"]
    existing = [name for name in cols if name in schema.names()]
    frame = _collect_lazy(lazy.select(existing)).sort("ts_ms")
    if frame.height <= 0:
        return frame
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
    return frame.select(cols)


def _candle_part_files(*, dataset_root: Path, tf: str, market: str) -> list[Path]:
    market_dir = dataset_root / f"tf={tf}" / f"market={market}"
    if not market_dir.exists():
        return []
    legacy = market_dir / "part.parquet"
    if legacy.exists():
        return [legacy]
    return sorted(path for path in market_dir.glob("part-*.parquet") if path.is_file())


def _candle_bounds(*, dataset_root: Path, tf: str, market: str) -> tuple[int | None, int | None]:
    files = _candle_part_files(dataset_root=dataset_root, tf=tf, market=market)
    if not files:
        return None, None
    frame = _collect_lazy(
        pl.scan_parquet([str(path) for path in files]).select(
            [pl.col("ts_ms").min().alias("min_ts_ms"), pl.col("ts_ms").max().alias("max_ts_ms")]
        )
    )
    if frame.height <= 0:
        return None, None
    min_ts = frame.item(row=0, column="min_ts_ms")
    max_ts = frame.item(row=0, column="max_ts_ms")
    if min_ts is None or max_ts is None:
        return None, None
    return int(min_ts), int(max_ts)


def _load_precomputed_market_frame(
    *,
    dataset_root: Path,
    tf: str,
    market: str,
    from_ts_ms: int,
    to_ts_ms: int,
) -> pl.DataFrame:
    files = _feature_market_files_any(dataset_root=dataset_root, tf=tf, market=market)
    if not files:
        return pl.DataFrame()
    lazy = pl.scan_parquet([str(path) for path in files]).filter(
        (pl.col("ts_ms") >= int(from_ts_ms)) & (pl.col("ts_ms") <= int(to_ts_ms))
    )
    frame = _collect_lazy(lazy).sort("ts_ms")
    if frame.height <= 0:
        return frame
    required = ["ts_ms", "y_reg", "y_cls"]
    missing = [name for name in required if name not in frame.columns]
    if missing:
        return pl.DataFrame()
    return frame


def _feature_market_files_any(*, dataset_root: Path, tf: str, market: str) -> list[Path]:
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


def _cleanup_market_target_dates(*, dataset_root: Path, tf: str, market: str, target_dates: list[str]) -> None:
    market_dir = dataset_root / f"tf={tf}" / f"market={market}"
    if not market_dir.exists():
        return
    for day in target_dates:
        date_dir = market_dir / f"date={day}"
        if not date_dir.exists():
            continue
        for file in date_dir.glob("*.parquet"):
            try:
                file.unlink()
            except Exception:
                pass
        try:
            date_dir.rmdir()
        except Exception:
            pass


def _write_market_date_partitions(
    *,
    frame: pl.DataFrame,
    dataset_root: Path,
    tf: str,
    market: str,
) -> list[Path]:
    if frame.height <= 0:
        return []
    prepared = (
        frame.sort("ts_ms")
        .with_columns(pl.from_epoch(pl.col("ts_ms"), time_unit="ms").dt.strftime("%Y-%m-%d").alias("__date"))
        .select(frame.columns + ["__date"])
    )
    files: list[Path] = []
    for part in prepared.partition_by("__date", as_dict=False, maintain_order=True):
        day = str(part.item(0, "__date"))
        out_dir = dataset_root / f"tf={tf}" / f"market={market}" / f"date={day}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / "part-000.parquet"
        part.drop("__date").write_parquet(out_file, compression="zstd")
        files.append(out_file)
    return files


def _load_feature_v2_market(*, dataset_root: Path, tf: str, market: str) -> pl.DataFrame:
    files = _feature_v2_part_files(dataset_root=dataset_root, tf=tf, market=market)
    if not files:
        return pl.DataFrame()
    frame = _collect_lazy(pl.scan_parquet([str(path) for path in files])).sort("ts_ms")
    if frame.height <= 0:
        return frame
    return frame.unique(subset=["ts_ms"], keep="last", maintain_order=True).sort("ts_ms")


def _feature_v2_part_files(*, dataset_root: Path, tf: str, market: str) -> list[Path]:
    market_dir = dataset_root / f"tf={tf}" / f"market={market}"
    if not market_dir.exists():
        return []
    files: list[Path] = []
    for date_dir in sorted(market_dir.glob("date=*")):
        if not date_dir.is_dir():
            continue
        files.extend(path for path in sorted(date_dir.glob("*.parquet")) if path.is_file())
    return files


def _apply_liquidity_rank_to_payloads(*, market_payloads: list[dict[str, Any]], float_dtype: str) -> None:
    if not market_payloads:
        return
    first_frame = market_payloads[0].get("frame")
    if not isinstance(first_frame, pl.DataFrame):
        return
    if "vol_quote_24h_roll" not in first_frame.columns or "vol_rank_at_ts" not in first_frame.columns:
        return

    stacked: list[pl.DataFrame] = []
    for payload in market_payloads:
        market = str(payload["market"])
        frame = payload["frame"]
        if frame.height <= 0:
            continue
        stacked.append(frame.select(["ts_ms", "vol_quote_24h_roll"]).with_columns(pl.lit(market).alias("market")))
    if not stacked:
        return

    ranked = pl.concat(stacked, how="vertical_relaxed").with_columns(
        (pl.col("vol_quote_24h_roll").rank(method="average").over("ts_ms") / pl.len().over("ts_ms")).alias("vol_rank_at_ts")
    )
    cast_dtype = pl.Float64 if str(float_dtype).strip().lower() == "float64" else pl.Float32
    for payload in market_payloads:
        market = str(payload["market"])
        frame = payload["frame"]
        if frame.height <= 0:
            continue
        rank_frame = ranked.filter(pl.col("market") == market).select(["ts_ms", "vol_rank_at_ts"])
        payload["frame"] = (
            frame.drop("vol_rank_at_ts")
            .join(rank_frame, on="ts_ms", how="left")
            .with_columns(pl.col("vol_rank_at_ts").cast(cast_dtype).alias("vol_rank_at_ts"))
        )


def _build_feature_spec_payload(
    *,
    config: FeaturesV2Config,
    tf: str,
    quote: str,
    selected_markets: list[str],
    feature_cols: list[str],
    from_ts_ms: int,
    to_ts_ms: int,
    base_candles_root: Path,
    micro_root: Path,
    preflight: dict[str, Any],
) -> dict[str, Any]:
    return {
        "dataset_name": config.dataset_name,
        "tf": tf,
        "quote": quote,
        "float_dtype": config.float_dtype,
        "feature_columns": feature_cols,
        "feature_set_version": "v2",
        "feature_set_v1_config": _feature_set_config_to_dict(config.feature_set_v1),
        "micro_prefix": "m_",
        "micro_filter": _micro_filter_to_dict(config.micro_filter),
        "time_range": {"from_ts_ms": from_ts_ms, "to_ts_ms": to_ts_ms},
        "selected_markets": selected_markets,
        "base_candles_root": str(base_candles_root),
        "micro_root": str(micro_root),
        "preflight": preflight,
        "fingerprint": {
            "base_candles_manifest_sha256": sha256_file(base_candles_root / "_meta" / "manifest.parquet"),
            "micro_manifest_sha256": sha256_file(micro_root / "_meta" / "manifest.parquet"),
            "config_sha256": sha256_json(_config_snapshot(config)),
        },
    }


def _build_label_spec_payload(*, config: FeaturesV2Config, tf: str, label_cols: list[str]) -> dict[str, Any]:
    return {
        "dataset_name": config.dataset_name,
        "tf": tf,
        "label_columns": label_cols,
        "label_set_version": "v1",
        "horizon_bars": config.label_v1.horizon_bars,
        "thr_bps": config.label_v1.thr_bps,
        "effective_thr_bps": effective_threshold_bps(config.label_v1),
        "fee_bps_est": config.label_v1.fee_bps_est,
        "safety_bps": config.label_v1.safety_bps,
        "neutral_policy": config.label_v1.neutral_policy,
        "tail_guard": {"enabled": True, "drop_last_h_bars_per_market": True},
    }


def _manifest_fail_row(
    *,
    dataset_name: str,
    tf: str,
    market: str,
    feature_cols_hash: str,
    label_cols_hash: str,
    built_at: int,
    error_message: str,
) -> dict[str, Any]:
    return {
        "dataset_name": dataset_name,
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
        "error_message": error_message,
        "built_at": built_at,
        "join_compared_rows": 0,
        "join_matched_rows": 0,
        "join_match_ratio": None,
        "micro_available_ratio": 0.0,
        "trade_coverage_p50_ms": None,
        "trade_coverage_p90_ms": None,
        "book_coverage_p50_ms": None,
        "book_coverage_p90_ms": None,
        "tail_dropped_rows": 0,
        "prefilter_rows": 0,
        "postfilter_rows": 0,
    }


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


def _coverage_quantiles(frame: pl.DataFrame, *, flag_col: str, coverage_col: str) -> tuple[int | None, int | None]:
    p50, p90, _ = _coverage_quantiles_with_values(frame, flag_col=flag_col, coverage_col=coverage_col)
    return p50, p90


def _coverage_quantiles_with_values(
    frame: pl.DataFrame,
    *,
    flag_col: str,
    coverage_col: str,
) -> tuple[int | None, int | None, list[int]]:
    if frame.height <= 0 or flag_col not in frame.columns or coverage_col not in frame.columns:
        return None, None, []
    subset = frame.filter(pl.col(flag_col) == True)  # noqa: E712
    if subset.height <= 0:
        return None, None, []
    values = [int(item) for item in subset.get_column(coverage_col).drop_nulls().to_list()]
    if not values:
        return None, None, []
    return _quantile(values, 0.5), _quantile(values, 0.9), values


def _quantile(values: list[int], q: float) -> int:
    sorted_values = sorted(int(v) for v in values)
    if len(sorted_values) == 1:
        return sorted_values[0]
    position = max(min(float(q), 1.0), 0.0) * float(len(sorted_values) - 1)
    lower = int(position)
    upper = min(lower + 1, len(sorted_values) - 1)
    if lower == upper:
        return sorted_values[lower]
    weight = position - float(lower)
    return int(round((1.0 - weight) * float(sorted_values[lower]) + weight * float(sorted_values[upper])))


def _is_non_monotonic(ts: pl.Series) -> bool:
    if ts.len() <= 1:
        return False
    diff = ts.diff().drop_nulls()
    if diff.len() <= 0:
        return False
    return bool((diff < 0).any())


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
    result: int | None = None
    for value in values:
        result = _safe_min(result, value)
    return result


def _safe_max_many(values: list[Any]) -> int | None:
    result: int | None = None
    for value in values:
        result = _safe_max(result, value)
    return result


def _ensure_columns(frame: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    working = frame
    for name in columns:
        if name in working.columns:
            continue
        working = working.with_columns(pl.lit(None).alias(name))
    return working


def _dedupe_preserve(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for raw in values:
        item = str(raw).strip()
        if not item or item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def _date_strings_between(start_text: str, end_text: str) -> list[str]:
    start_day = date.fromisoformat(str(start_text))
    end_day = date.fromisoformat(str(end_text))
    return [item.isoformat() for item in _date_range(start_day, end_day)]


def _date_range(start_day: date, end_day: date) -> list[date]:
    days: list[date] = []
    cursor = start_day
    while cursor <= end_day:
        days.append(cursor)
        cursor = cursor + timedelta(days=1)
    return days


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


def _micro_filter_to_dict(policy: MicroFilterPolicy) -> dict[str, Any]:
    return {
        "require_micro_available": bool(policy.require_micro_available),
        "min_trade_events": int(policy.min_trade_events),
        "min_trade_coverage_ms": int(policy.min_trade_coverage_ms),
        "min_book_events": int(policy.min_book_events),
        "min_book_coverage_ms": int(policy.min_book_coverage_ms),
    }


def _config_snapshot(config: FeaturesV2Config) -> dict[str, Any]:
    return {
        "build": {
            "output_dataset": config.build.output_dataset,
            "tf": config.build.tf,
            "base_candles_dataset": config.build.base_candles_dataset,
            "micro_dataset": config.build.micro_dataset,
            "alignment_mode": config.build.alignment_mode,
            "use_precomputed_features_v1": config.build.use_precomputed_features_v1,
            "precomputed_features_v1_dataset": config.build.precomputed_features_v1_dataset,
            "min_rows_for_train": config.build.min_rows_for_train,
        },
        "parquet_root": str(config.parquet_root),
        "features_root": str(config.features_root),
        "universe": {
            "quote": config.universe.quote,
            "mode": config.universe.mode,
            "top_n": config.universe.top_n,
            "lookback_days": config.universe.lookback_days,
            "fixed_list": list(config.universe.fixed_list),
        },
        "time_range": {"start": config.time_range.start, "end": config.time_range.end},
        "feature_set_v1": _feature_set_config_to_dict(config.feature_set_v1),
        "label_v1": {
            "horizon_bars": config.label_v1.horizon_bars,
            "thr_bps": config.label_v1.thr_bps,
            "neutral_policy": config.label_v1.neutral_policy,
            "fee_bps_est": config.label_v1.fee_bps_est,
            "safety_bps": config.label_v1.safety_bps,
        },
        "micro_filter": _micro_filter_to_dict(config.micro_filter),
        "validation": {
            "join_match_warn": config.validation.join_match_warn,
            "join_match_fail": config.validation.join_match_fail,
        },
        "float_dtype": config.float_dtype,
    }


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _load_yaml_doc(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    return raw if isinstance(raw, dict) else {}


def _collect_lazy(lazy_frame: pl.LazyFrame) -> pl.DataFrame:
    try:
        return lazy_frame.collect(engine="streaming")
    except TypeError:
        return lazy_frame.collect(streaming=True)
