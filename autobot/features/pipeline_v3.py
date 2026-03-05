"""Feature store v3 pipeline: multi-tf + mandatory micro + sample weights."""

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

from .feature_set_v3 import build_feature_set_v3_from_candles, feature_columns_v3
from .feature_spec import (
    FeatureSetV1Config,
    LabelV1Config,
    TimeRangeConfig,
    UniverseConfig,
    effective_threshold_bps,
    parse_date_to_ts_ms,
    sha256_file,
    sha256_json,
    write_json,
)
from .micro_required_join_v1 import load_market_micro_for_base, resolve_micro_dataset_root


DEFAULT_FEATURES_V3_YAML = "features_v3.yaml"


FEATURES_V3_MANIFEST_SCHEMA: dict[str, pl.DataType] = {
    "dataset_name": pl.Utf8,
    "tf": pl.Utf8,
    "market": pl.Utf8,
    "rows_base_total": pl.Int64,
    "rows_after_multitf": pl.Int64,
    "rows_after_label": pl.Int64,
    "rows_final": pl.Int64,
    "rows_dropped_no_micro": pl.Int64,
    "rows_dropped_stale": pl.Int64,
    "rows_dropped_one_m_before_densify": pl.Int64,
    "rows_dropped_one_m": pl.Int64,
    "rows_rescued_by_one_m_densify": pl.Int64,
    "one_m_synth_ratio_mean": pl.Float64,
    "one_m_synth_ratio_p50": pl.Float64,
    "one_m_synth_ratio_p90": pl.Float64,
    "tail_dropped_rows": pl.Int64,
    "min_ts_ms": pl.Int64,
    "max_ts_ms": pl.Int64,
    "effective_start_ts_ms": pl.Int64,
    "effective_end_ts_ms": pl.Int64,
    "micro_tf_used": pl.Utf8,
    "status": pl.Utf8,
    "reasons_json": pl.Utf8,
    "error_message": pl.Utf8,
    "built_at": pl.Int64,
}


@dataclass(frozen=True)
class FeaturesV3BuildConfig:
    output_dataset: str = "features_v3"
    tf: str = "5m"
    base_candles_dataset: str = "auto"
    micro_dataset: str = "micro_v1"
    high_tfs: tuple[str, ...] = ("15m", "60m", "240m")
    high_tf_staleness_multiplier: float = 2.0
    one_m_required_bars: int = 5
    one_m_max_missing_ratio: float = 0.2
    one_m_drop_if_real_count_zero: bool = True
    one_m_synth_weight_floor: float = 0.2
    one_m_synth_weight_power: float = 2.0
    sample_weight_half_life_days: float = 60.0
    universe_quality_enabled: bool = True
    universe_quality_lookback_days: int = 3
    universe_quality_beta: float = 2.0
    universe_quality_q_floor: float = 0.2
    universe_quality_oversample_factor: int = 3
    min_rows_for_train: int = 5000
    require_micro_validate_pass: bool = True


@dataclass(frozen=True)
class FeaturesV3ValidateConfig:
    leakage_fail_on_future_ts: bool = True


@dataclass(frozen=True)
class FeaturesV3Config:
    build: FeaturesV3BuildConfig
    parquet_root: Path
    features_root: Path
    universe: UniverseConfig
    time_range: TimeRangeConfig
    feature_set_v1: FeatureSetV1Config
    label_v1: LabelV1Config
    validation: FeaturesV3ValidateConfig
    float_dtype: str = "float32"

    @property
    def dataset_name(self) -> str:
        return self.build.output_dataset

    @property
    def output_dataset_root(self) -> Path:
        return self.features_root / self.dataset_name


@dataclass(frozen=True)
class FeatureBuildV3Options:
    tf: str
    quote: str | None = None
    top_n: int | None = None
    start: str | None = None
    end: str | None = None
    feature_set: str = "v3"
    label_set: str = "v1"
    workers: int = 1
    fail_on_warn: bool = False
    dry_run: bool = False
    base_candles: str | None = None
    micro_dataset: str | None = None


@dataclass(frozen=True)
class FeatureBuildV3Summary:
    discovered_markets: int
    selected_markets: tuple[str, ...]
    processed_markets: int
    ok_markets: int
    warn_markets: int
    fail_markets: int
    rows_base_total: int
    rows_dropped_no_micro: int
    rows_dropped_one_m_before_densify: int
    rows_dropped_one_m: int
    rows_rescued_by_one_m_densify: int
    rows_final: int
    one_m_synth_ratio_p50: float | None
    one_m_synth_ratio_p90: float | None
    min_ts_ms: int | None
    max_ts_ms: int | None
    effective_start: str | None
    effective_end: str | None
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
class FeatureValidateV3Options:
    tf: str
    quote: str | None = None
    top_n: int | None = None
    start: str | None = None
    end: str | None = None


@dataclass(frozen=True)
class FeatureValidateV3Summary:
    checked_files: int
    ok_files: int
    warn_files: int
    fail_files: int
    schema_ok: bool
    null_ratio_overall: float
    leakage_smoke: str
    staleness_fail_rows: int
    dropped_rows_no_micro: int
    validate_report_file: Path
    details: tuple[dict[str, Any], ...]


def load_features_v3_config(
    config_dir: Path,
    *,
    base_config: dict[str, Any] | None = None,
    filename: str = DEFAULT_FEATURES_V3_YAML,
) -> FeaturesV3Config:
    from .feature_spec import load_features_config

    shared = load_features_config(config_dir, base_config=base_config, filename=filename)
    raw = _load_yaml_doc(config_dir / filename)
    root = raw.get("features_v3", {}) if isinstance(raw.get("features_v3"), dict) else {}
    validation_cfg = root.get("validation", {}) if isinstance(root.get("validation"), dict) else {}

    high_tfs_raw = root.get("high_tfs", ["15m", "60m", "240m"])
    high_tfs = tuple(
        item
        for item in (str(v).strip().lower() for v in (high_tfs_raw if isinstance(high_tfs_raw, list) else []))
        if item in {"15m", "60m", "240m"}
    )
    if not high_tfs:
        high_tfs = ("15m", "60m", "240m")

    tf_value = str(root.get("tf", "5m")).strip().lower() or "5m"
    if tf_value != "5m":
        raise ValueError("features_v3.tf currently supports only 5m")

    return FeaturesV3Config(
        build=FeaturesV3BuildConfig(
            output_dataset=str(root.get("output_dataset", "features_v3")).strip() or "features_v3",
            tf=tf_value,
            base_candles_dataset=str(root.get("base_candles_dataset", "auto")).strip() or "auto",
            micro_dataset=str(root.get("micro_dataset", "micro_v1")).strip() or "micro_v1",
            high_tfs=high_tfs,
            high_tf_staleness_multiplier=max(float(root.get("high_tf_staleness_multiplier", 2.0)), 0.0),
            one_m_required_bars=max(int(root.get("one_m_required_bars", 5)), 1),
            one_m_max_missing_ratio=max(float(root.get("one_m_max_missing_ratio", 0.2)), 0.0),
            one_m_drop_if_real_count_zero=bool(root.get("one_m_drop_if_real_count_zero", True)),
            one_m_synth_weight_floor=min(max(float(root.get("one_m_synth_weight_floor", 0.2)), 0.0), 1.0),
            one_m_synth_weight_power=max(float(root.get("one_m_synth_weight_power", 2.0)), 0.0),
            sample_weight_half_life_days=max(float(root.get("sample_weight_half_life_days", 60.0)), 1e-6),
            universe_quality_enabled=bool(root.get("universe_quality_enabled", True)),
            universe_quality_lookback_days=max(int(root.get("universe_quality_lookback_days", 3)), 1),
            universe_quality_beta=max(float(root.get("universe_quality_beta", 2.0)), 0.0),
            universe_quality_q_floor=min(max(float(root.get("universe_quality_q_floor", 0.2)), 0.0), 1.0),
            universe_quality_oversample_factor=max(int(root.get("universe_quality_oversample_factor", 3)), 1),
            min_rows_for_train=max(int(root.get("min_rows_for_train", 5000)), 1),
            require_micro_validate_pass=bool(root.get("require_micro_validate_pass", True)),
        ),
        parquet_root=shared.parquet_root,
        features_root=shared.features_root,
        universe=shared.universe,
        time_range=shared.time_range,
        feature_set_v1=shared.feature_set_v1,
        label_v1=shared.label_v1,
        validation=FeaturesV3ValidateConfig(
            leakage_fail_on_future_ts=bool(validation_cfg.get("leakage_fail_on_future_ts", True))
        ),
        float_dtype=shared.float_dtype,
    )


def build_features_dataset_v3(config: FeaturesV3Config, options: FeatureBuildV3Options) -> FeatureBuildV3Summary:
    if options.feature_set != "v3":
        raise ValueError("feature_set currently supports only v3 in pipeline_v3")
    if options.label_set != "v1":
        raise ValueError("label_set currently supports only v1")

    tf = str(options.tf or config.build.tf).strip().lower()
    if tf != "5m":
        raise ValueError("features_v3 currently supports only --tf 5m")
    quote = str(options.quote or config.universe.quote).strip().upper()
    top_n = max(1, int(options.top_n if options.top_n is not None else config.universe.top_n))
    start_text = str(options.start or config.time_range.start).strip()
    end_text = str(options.end or config.time_range.end).strip()
    start_ts_ms = parse_date_to_ts_ms(start_text)
    end_ts_ms = parse_date_to_ts_ms(end_text, end_of_day=True)
    if end_ts_ms < start_ts_ms:
        raise ValueError("time_range.end must be >= time_range.start")

    output_root = config.output_dataset_root
    output_root.mkdir(parents=True, exist_ok=True)
    meta_root = output_root / "_meta"
    meta_root.mkdir(parents=True, exist_ok=True)
    build_report_file = meta_root / "build_report.json"
    manifest_file = meta_root / "manifest.parquet"
    universe_quality_report_file = meta_root / "universe_quality_report.json"

    micro_root = resolve_micro_dataset_root(
        dataset=(options.micro_dataset or config.build.micro_dataset),
        parquet_root=config.parquet_root,
    )
    if not micro_root.exists():
        raise ValueError(f"micro dataset not found: {micro_root}")

    if config.build.require_micro_validate_pass:
        _assert_micro_validate_ok(micro_root)

    base_root = _resolve_base_candles_root(
        parquet_root=config.parquet_root,
        base_candles_value=(str(options.base_candles).strip() if options.base_candles else config.build.base_candles_dataset),
    )
    if base_root is None:
        raise ValueError("unable to resolve base candles dataset for v3")

    discovered = _discover_micro_market_windows(
        micro_root=micro_root,
        quote=quote,
        start_ts_ms=start_ts_ms,
        end_ts_ms=end_ts_ms,
    )
    discovered_markets = len(discovered)
    selected_markets, universe_quality_report = _select_v3_universe_markets(
        config=config,
        tf=tf,
        top_n=top_n,
        quote=quote,
        start_ts_ms=start_ts_ms,
        base_candles_root=base_root,
        discovered_windows=discovered,
    )
    write_json(universe_quality_report_file, universe_quality_report)
    if not selected_markets:
        raise ValueError(
            f"no micro markets found in range {start_text}~{end_text}; "
            "run micro aggregate and keep collecting ws/ticks data."
        )

    feature_cols = list(feature_columns_v3(high_tfs=config.build.high_tfs))
    label_cols = ["y_reg", "y_cls"]
    feature_spec_hash = sha256_json(feature_cols)
    label_spec_hash = sha256_json(label_cols)

    if options.dry_run:
        write_json(
            build_report_file,
            {
                "dataset_name": config.dataset_name,
                "tf": tf,
                "quote": quote,
                "requested_start": start_text,
                "requested_end": end_text,
                "selected_markets": selected_markets,
                "base_candles_root": str(base_root),
                "micro_root": str(micro_root),
                "universe_quality_report_file": str(universe_quality_report_file),
                "universe_selection": universe_quality_report,
                "status": "PASS",
                "dry_run": True,
            },
        )
        return FeatureBuildV3Summary(
            discovered_markets=discovered_markets,
            selected_markets=tuple(selected_markets),
            processed_markets=0,
            ok_markets=0,
            warn_markets=0,
            fail_markets=0,
            rows_base_total=0,
            rows_dropped_no_micro=0,
            rows_dropped_one_m_before_densify=0,
            rows_dropped_one_m=0,
            rows_rescued_by_one_m_densify=0,
            rows_final=0,
            one_m_synth_ratio_p50=None,
            one_m_synth_ratio_p90=None,
            min_ts_ms=None,
            max_ts_ms=None,
            effective_start=None,
            effective_end=None,
            output_path=output_root,
            manifest_file=manifest_file,
            build_report_file=build_report_file,
            feature_spec_hash=feature_spec_hash,
            label_spec_hash=label_spec_hash,
            base_candles_root=base_root,
            micro_root=micro_root,
            details=tuple(),
            failures=tuple(),
            preflight_ok=True,
        )

    interval_ms = expected_interval_ms(tf)
    extended_start_ts_ms = start_ts_ms - _warmup_ms(config=config, base_tf=tf)
    extended_end_ts_ms = end_ts_ms + int(config.label_v1.horizon_bars) * interval_ms

    target_dates = _date_strings_between(start_text, end_text)
    for market in selected_markets:
        _cleanup_market_target_dates(dataset_root=output_root, tf=tf, market=market, target_dates=target_dates)

    rows: list[dict[str, Any]] = []
    details: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    rows_base_total = 0
    rows_dropped_no_micro = 0
    rows_dropped_one_m_before_densify = 0
    rows_dropped_one_m = 0
    rows_rescued_by_one_m_densify = 0
    rows_final = 0
    min_ts_total: int | None = None
    max_ts_total: int | None = None
    ok_markets = 0
    warn_markets = 0
    fail_markets = 0
    one_m_synth_ratio_values: list[float] = []
    market_synth_means: list[dict[str, Any]] = []

    for market in selected_markets:
        try:
            base = _load_market_candles(
                dataset_root=base_root,
                tf=tf,
                market=market,
                from_ts_ms=extended_start_ts_ms,
                to_ts_ms=extended_end_ts_ms,
            )
            one_m = _load_market_candles(
                dataset_root=base_root,
                tf="1m",
                market=market,
                from_ts_ms=extended_start_ts_ms,
                to_ts_ms=extended_end_ts_ms,
            )
            high_frames = {
                high_tf: _load_market_candles(
                    dataset_root=base_root,
                    tf=high_tf,
                    market=market,
                    from_ts_ms=extended_start_ts_ms,
                    to_ts_ms=extended_end_ts_ms,
                )
                for high_tf in config.build.high_tfs
            }
            micro, micro_tf_used = load_market_micro_for_base(
                micro_root=micro_root,
                market=market,
                base_tf=tf,
                from_ts_ms=start_ts_ms,
                to_ts_ms=end_ts_ms,
            )
            result = build_feature_set_v3_from_candles(
                base_candles_frame=base,
                one_m_candles_frame=one_m,
                high_tf_candles=high_frames,
                micro_frame=micro,
                micro_tf_used=micro_tf_used,
                tf=tf,
                from_ts_ms=start_ts_ms,
                to_ts_ms=end_ts_ms,
                label_config=config.label_v1,
                high_tfs=config.build.high_tfs,
                high_tf_staleness_multiplier=config.build.high_tf_staleness_multiplier,
                one_m_required_bars=config.build.one_m_required_bars,
                one_m_max_missing_ratio=config.build.one_m_max_missing_ratio,
                one_m_drop_if_real_count_zero=config.build.one_m_drop_if_real_count_zero,
                sample_weight_half_life_days=config.build.sample_weight_half_life_days,
                one_m_synth_weight_floor=config.build.one_m_synth_weight_floor,
                one_m_synth_weight_power=config.build.one_m_synth_weight_power,
                float_dtype=config.float_dtype,
            )

            frame = result.frame
            _write_market_date_partitions(frame=frame, dataset_root=output_root, tf=tf, market=market)
            market_rows_final = int(frame.height)
            market_min_ts = int(frame.get_column("ts_ms").min()) if market_rows_final > 0 else None
            market_max_ts = int(frame.get_column("ts_ms").max()) if market_rows_final > 0 else None
            reasons: list[str] = []
            status = "OK"
            if market_rows_final <= 0:
                status = "WARN"
                reasons.append("NO_ROWS_AFTER_FILTERS")
            if result.rows_dropped_no_micro > 0:
                if status == "OK":
                    status = "WARN"
                reasons.append("MICRO_MANDATORY_DROPS_PRESENT")

            if status == "OK":
                ok_markets += 1
            elif status == "WARN":
                warn_markets += 1
            else:
                fail_markets += 1

            rows_base_total += int(result.rows_base_total)
            rows_dropped_no_micro += int(result.rows_dropped_no_micro)
            rows_dropped_one_m_before_densify += int(result.rows_dropped_one_m_before_densify)
            rows_dropped_one_m += int(result.rows_dropped_one_m)
            rows_rescued_by_one_m_densify += int(result.rows_rescued_by_one_m_densify)
            rows_final += market_rows_final
            min_ts_total = _safe_min(min_ts_total, market_min_ts)
            max_ts_total = _safe_max(max_ts_total, market_max_ts)

            market_synth_mean = _safe_float(result.one_m_synth_ratio_mean)
            market_synth_p50 = _safe_float(result.one_m_synth_ratio_p50)
            market_synth_p90 = _safe_float(result.one_m_synth_ratio_p90)
            if market_synth_mean is not None:
                market_synth_means.append(
                    {
                        "market": market,
                        "one_m_synth_ratio_mean": market_synth_mean,
                    }
                )
            if market_rows_final > 0 and "one_m_synth_ratio" in frame.columns:
                one_m_synth_ratio_values.extend(
                    float(v)
                    for v in frame.get_column("one_m_synth_ratio").drop_nulls().to_list()
                    if v is not None
                )

            details.append(
                {
                    "market": market,
                    "status": status,
                    "rows_base_total": int(result.rows_base_total),
                    "rows_after_multitf": int(result.rows_after_multitf),
                    "rows_after_label": int(result.rows_after_label),
                    "rows_final": market_rows_final,
                    "rows_dropped_no_micro": int(result.rows_dropped_no_micro),
                    "rows_dropped_stale": int(result.rows_dropped_stale),
                    "rows_dropped_one_m_before_densify": int(result.rows_dropped_one_m_before_densify),
                    "rows_dropped_one_m": int(result.rows_dropped_one_m),
                    "rows_rescued_by_one_m_densify": int(result.rows_rescued_by_one_m_densify),
                    "one_m_synth_ratio_mean": market_synth_mean,
                    "one_m_synth_ratio_p50": market_synth_p50,
                    "one_m_synth_ratio_p90": market_synth_p90,
                    "tail_dropped_rows": int(result.tail_dropped_rows),
                    "min_ts_ms": market_min_ts,
                    "max_ts_ms": market_max_ts,
                    "effective_start": _ts_to_date(market_min_ts),
                    "effective_end": _ts_to_date(market_max_ts),
                    "micro_tf_used": result.micro_tf_used,
                    "reasons": reasons,
                }
            )
            rows.append(
                {
                    "dataset_name": config.dataset_name,
                    "tf": tf,
                    "market": market,
                    "rows_base_total": int(result.rows_base_total),
                    "rows_after_multitf": int(result.rows_after_multitf),
                    "rows_after_label": int(result.rows_after_label),
                    "rows_final": market_rows_final,
                    "rows_dropped_no_micro": int(result.rows_dropped_no_micro),
                    "rows_dropped_stale": int(result.rows_dropped_stale),
                    "rows_dropped_one_m_before_densify": int(result.rows_dropped_one_m_before_densify),
                    "rows_dropped_one_m": int(result.rows_dropped_one_m),
                    "rows_rescued_by_one_m_densify": int(result.rows_rescued_by_one_m_densify),
                    "one_m_synth_ratio_mean": market_synth_mean,
                    "one_m_synth_ratio_p50": market_synth_p50,
                    "one_m_synth_ratio_p90": market_synth_p90,
                    "tail_dropped_rows": int(result.tail_dropped_rows),
                    "min_ts_ms": market_min_ts,
                    "max_ts_ms": market_max_ts,
                    "effective_start_ts_ms": market_min_ts,
                    "effective_end_ts_ms": market_max_ts,
                    "micro_tf_used": result.micro_tf_used,
                    "status": status,
                    "reasons_json": json.dumps(reasons, ensure_ascii=False),
                    "error_message": None,
                    "built_at": int(time.time()),
                }
            )
        except Exception as exc:
            fail_markets += 1
            detail = {"market": market, "status": "FAIL", "reasons": ["BUILD_EXCEPTION"], "error_message": str(exc)}
            details.append(detail)
            failures.append(detail)
            rows.append(
                {
                    "dataset_name": config.dataset_name,
                    "tf": tf,
                    "market": market,
                    "rows_base_total": 0,
                    "rows_after_multitf": 0,
                    "rows_after_label": 0,
                    "rows_final": 0,
                    "rows_dropped_no_micro": 0,
                    "rows_dropped_stale": 0,
                    "rows_dropped_one_m_before_densify": 0,
                    "rows_dropped_one_m": 0,
                    "rows_rescued_by_one_m_densify": 0,
                    "one_m_synth_ratio_mean": None,
                    "one_m_synth_ratio_p50": None,
                    "one_m_synth_ratio_p90": None,
                    "tail_dropped_rows": 0,
                    "min_ts_ms": None,
                    "max_ts_ms": None,
                    "effective_start_ts_ms": None,
                    "effective_end_ts_ms": None,
                    "micro_tf_used": None,
                    "status": "FAIL",
                    "reasons_json": json.dumps(["BUILD_EXCEPTION"], ensure_ascii=False),
                    "error_message": str(exc),
                    "built_at": int(time.time()),
                }
            )

    manifest = _normalize_manifest_rows(rows)
    manifest_file.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_parquet(manifest_file, compression="zstd")

    feature_spec = _build_feature_spec_payload(
        config=config,
        tf=tf,
        quote=quote,
        selected_markets=selected_markets,
        feature_cols=feature_cols,
        base_candles_root=base_root,
        micro_root=micro_root,
        start_ts_ms=start_ts_ms,
        end_ts_ms=end_ts_ms,
    )
    label_spec = _build_label_spec_payload(config=config, tf=tf, label_cols=label_cols)
    write_json(meta_root / "feature_spec.json", feature_spec)
    write_json(meta_root / "label_spec.json", label_spec)

    effective_start = _ts_to_date(min_ts_total)
    effective_end = _ts_to_date(max_ts_total)
    one_m_synth_ratio_p50 = _quantile(one_m_synth_ratio_values, 0.50)
    one_m_synth_ratio_p90 = _quantile(one_m_synth_ratio_values, 0.90)
    market_synth_desc = sorted(
        market_synth_means,
        key=lambda item: float(item.get("one_m_synth_ratio_mean", 0.0)),
        reverse=True,
    )
    market_synth_asc = sorted(
        market_synth_means,
        key=lambda item: float(item.get("one_m_synth_ratio_mean", 0.0)),
    )
    report = {
        "dataset_name": config.dataset_name,
        "tf": tf,
        "quote": quote,
        "requested_start": start_text,
        "requested_end": end_text,
        "effective_start": effective_start,
        "effective_end": effective_end,
        "rows_base_total": rows_base_total,
        "rows_dropped_no_micro": rows_dropped_no_micro,
        "rows_dropped_one_m_before_densify": rows_dropped_one_m_before_densify,
        "rows_dropped_one_m": rows_dropped_one_m,
        "rows_rescued_by_one_m_densify": rows_rescued_by_one_m_densify,
        "rows_final": rows_final,
        "one_m_synth_ratio_p50": one_m_synth_ratio_p50,
        "one_m_synth_ratio_p90": one_m_synth_ratio_p90,
        "one_m_synth_ratio_market_top10": market_synth_desc[:10],
        "one_m_synth_ratio_market_bottom10": market_synth_asc[:10],
        "selected_markets": selected_markets,
        "discovered_markets": discovered_markets,
        "universe_quality_report_file": str(universe_quality_report_file),
        "universe_selection": universe_quality_report,
        "status_counts": {
            "ok_markets": ok_markets,
            "warn_markets": warn_markets,
            "fail_markets": fail_markets,
        },
        "base_candles_root": str(base_root),
        "micro_root": str(micro_root),
        "details": details,
        "status": "PASS",
    }
    if rows_final < int(config.build.min_rows_for_train):
        report["status"] = "FAIL"
        report["error_message"] = (
            "NEED_MORE_MICRO_DAYS_OR_LOOSEN_UNIVERSE: "
            f"rows_final={rows_final} < min_rows_for_train={int(config.build.min_rows_for_train)}"
        )
    write_json(build_report_file, report)

    if rows_final < int(config.build.min_rows_for_train):
        raise ValueError(report["error_message"])

    return FeatureBuildV3Summary(
        discovered_markets=discovered_markets,
        selected_markets=tuple(selected_markets),
        processed_markets=len(rows),
        ok_markets=ok_markets,
        warn_markets=warn_markets,
        fail_markets=fail_markets,
        rows_base_total=rows_base_total,
        rows_dropped_no_micro=rows_dropped_no_micro,
        rows_dropped_one_m_before_densify=rows_dropped_one_m_before_densify,
        rows_dropped_one_m=rows_dropped_one_m,
        rows_rescued_by_one_m_densify=rows_rescued_by_one_m_densify,
        rows_final=rows_final,
        one_m_synth_ratio_p50=one_m_synth_ratio_p50,
        one_m_synth_ratio_p90=one_m_synth_ratio_p90,
        min_ts_ms=min_ts_total,
        max_ts_ms=max_ts_total,
        effective_start=effective_start,
        effective_end=effective_end,
        output_path=output_root,
        manifest_file=manifest_file,
        build_report_file=build_report_file,
        feature_spec_hash=feature_spec_hash,
        label_spec_hash=label_spec_hash,
        base_candles_root=base_root,
        micro_root=micro_root,
        details=tuple(details),
        failures=tuple(failures),
        preflight_ok=True,
    )


def validate_features_dataset_v3(config: FeaturesV3Config, options: FeatureValidateV3Options) -> FeatureValidateV3Summary:
    tf = str(options.tf or config.build.tf).strip().lower()
    quote = str(options.quote or config.universe.quote).strip().upper()
    top_n = max(1, int(options.top_n if options.top_n is not None else config.universe.top_n))
    start_ts_ms = parse_date_to_ts_ms(str(options.start).strip()) if options.start else None
    end_ts_ms = parse_date_to_ts_ms(str(options.end).strip(), end_of_day=True) if options.end else None
    if start_ts_ms is not None and end_ts_ms is not None and end_ts_ms < start_ts_ms:
        raise ValueError("validate end must be >= start")

    dataset_root = config.output_dataset_root
    manifest = _load_manifest(dataset_root)
    selected = (
        manifest.filter((pl.col("tf") == tf) & (pl.col("market").str.starts_with(f"{quote}-")))
        .head(top_n)
        .to_dicts()
        if manifest.height > 0
        else []
    )
    selected_markets = [str(item.get("market", "")).strip().upper() for item in selected if str(item.get("market", "")).strip()]
    meta_root = dataset_root / "_meta"
    report_file = meta_root / "validate_report.json"

    feature_spec = _read_json(meta_root / "feature_spec.json")
    feature_cols = feature_spec.get("feature_columns") if isinstance(feature_spec.get("feature_columns"), list) else list(feature_columns_v3(high_tfs=config.build.high_tfs))
    required = ["ts_ms"] + [str(item) for item in feature_cols] + ["sample_weight", "y_reg", "y_cls"]

    details: list[dict[str, Any]] = []
    ok_files = 0
    warn_files = 0
    fail_files = 0
    total_cells = 0
    total_null = 0
    leakage_fail_rows = 0
    staleness_fail_rows = 0
    dropped_no_micro = int(sum(int(item.get("rows_dropped_no_micro") or 0) for item in selected))

    for market in selected_markets:
        frame = _load_feature_market(dataset_root=dataset_root, tf=tf, market=market)
        if start_ts_ms is not None:
            frame = frame.filter(pl.col("ts_ms") >= int(start_ts_ms))
        if end_ts_ms is not None:
            frame = frame.filter(pl.col("ts_ms") <= int(end_ts_ms))
        rows = int(frame.height)
        missing_columns = [name for name in required if name not in frame.columns]
        non_monotonic = _is_non_monotonic(frame.get_column("ts_ms")) if rows > 1 and "ts_ms" in frame.columns else False

        null_count = 0
        for col in required:
            if col in frame.columns:
                null_count += int(frame.get_column(col).null_count())
            else:
                null_count += rows
        total_cells += rows * len(required)
        total_null += null_count

        market_leakage_rows = _count_future_source_rows(frame)
        leakage_fail_rows += market_leakage_rows
        market_stale_rows = _count_true_flags(frame, suffix="_stale")
        staleness_fail_rows += market_stale_rows

        status = "OK"
        reasons: list[str] = []
        if rows <= 0:
            status = "WARN"
            reasons.append("NO_ROWS")
        else:
            if missing_columns:
                status = "FAIL"
                reasons.append("MISSING_COLUMNS")
            if non_monotonic:
                status = "FAIL"
                reasons.append("TS_NON_MONOTONIC")
            if market_leakage_rows > 0:
                status = "FAIL"
                reasons.append("LEAKAGE_SMOKE_FAIL")
            if market_stale_rows > 0:
                status = "FAIL"
                reasons.append("STALE_ROWS_PRESENT")
        if "m_micro_available" in frame.columns and rows > 0:
            micro_ok = int(frame.filter(pl.col("m_micro_available") == True).height)  # noqa: E712
            if micro_ok < rows and status == "OK":
                status = "WARN"
                reasons.append("NON_MANDATORY_ROWS_FOUND")

        if status == "OK":
            ok_files += 1
        elif status == "WARN":
            warn_files += 1
        else:
            fail_files += 1

        details.append(
            {
                "market": market,
                "rows": rows,
                "missing_columns": missing_columns,
                "non_monotonic": non_monotonic,
                "null_ratio_overall": (float(null_count) / float(rows * len(required))) if rows > 0 else 0.0,
                "leakage_fail_rows": market_leakage_rows,
                "stale_rows": market_stale_rows,
                "status": status,
                "reasons": reasons,
            }
        )

    null_ratio_overall = (float(total_null) / float(total_cells)) if total_cells > 0 else 0.0
    leakage_smoke = "PASS" if leakage_fail_rows == 0 else "FAIL"
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
        "leakage_smoke": leakage_smoke,
        "staleness_fail_rows": staleness_fail_rows,
        "dropped_rows_no_micro": dropped_no_micro,
        "details": details,
    }
    write_json(report_file, report)

    return FeatureValidateV3Summary(
        checked_files=len(details),
        ok_files=ok_files,
        warn_files=warn_files,
        fail_files=fail_files,
        schema_ok=(fail_files == 0),
        null_ratio_overall=null_ratio_overall,
        leakage_smoke=leakage_smoke,
        staleness_fail_rows=staleness_fail_rows,
        dropped_rows_no_micro=dropped_no_micro,
        validate_report_file=report_file,
        details=tuple(details),
    )


def features_stats_v3(
    config: FeaturesV3Config,
    *,
    tf: str,
    quote: str | None = None,
    top_n: int | None = None,
) -> dict[str, Any]:
    tf_value = str(tf or config.build.tf).strip().lower()
    quote_value = str(quote or config.universe.quote).strip().upper()
    top_n_value = max(1, int(top_n if top_n is not None else config.universe.top_n))

    manifest = _load_manifest(config.output_dataset_root)
    if manifest.height <= 0:
        return {
            "dataset_name": config.dataset_name,
            "tf": tf_value,
            "quote": quote_value,
            "markets": [],
            "rows_base_total": 0,
            "rows_dropped_no_micro": 0,
            "rows_dropped_one_m_before_densify": 0,
            "rows_dropped_one_m": 0,
            "rows_rescued_by_one_m_densify": 0,
            "rows_final": 0,
            "one_m_synth_ratio_p50": None,
            "one_m_synth_ratio_p90": None,
            "effective_start": None,
            "effective_end": None,
            "ready_for_train": False,
            "min_rows_for_train": int(config.build.min_rows_for_train),
            "status_counts": {"OK": 0, "WARN": 0, "FAIL": 0},
        }

    filtered = manifest.filter((pl.col("tf") == tf_value) & (pl.col("market").str.starts_with(f"{quote_value}-"))).head(top_n_value)
    rows = filtered.to_dicts()
    status_counts = {"OK": 0, "WARN": 0, "FAIL": 0}
    for row in rows:
        status = str(row.get("status") or "").upper()
        if status in status_counts:
            status_counts[status] += 1

    rows_base_total = int(sum(int(item.get("rows_base_total") or 0) for item in rows))
    rows_dropped_no_micro = int(sum(int(item.get("rows_dropped_no_micro") or 0) for item in rows))
    rows_dropped_one_m_before_densify = int(sum(int(item.get("rows_dropped_one_m_before_densify") or 0) for item in rows))
    rows_dropped_one_m = int(sum(int(item.get("rows_dropped_one_m") or 0) for item in rows))
    rows_rescued_by_one_m_densify = int(sum(int(item.get("rows_rescued_by_one_m_densify") or 0) for item in rows))
    rows_final = int(sum(int(item.get("rows_final") or 0) for item in rows))
    synth_values = [float(item["one_m_synth_ratio_mean"]) for item in rows if item.get("one_m_synth_ratio_mean") is not None]
    min_ts = _safe_min_many([item.get("effective_start_ts_ms") for item in rows])
    max_ts = _safe_max_many([item.get("effective_end_ts_ms") for item in rows])

    return {
        "dataset_name": config.dataset_name,
        "tf": tf_value,
        "quote": quote_value,
        "markets": [str(item.get("market")) for item in rows],
        "rows_base_total": rows_base_total,
        "rows_dropped_no_micro": rows_dropped_no_micro,
        "rows_dropped_one_m_before_densify": rows_dropped_one_m_before_densify,
        "rows_dropped_one_m": rows_dropped_one_m,
        "rows_rescued_by_one_m_densify": rows_rescued_by_one_m_densify,
        "rows_final": rows_final,
        "one_m_synth_ratio_p50": _quantile(synth_values, 0.50),
        "one_m_synth_ratio_p90": _quantile(synth_values, 0.90),
        "effective_start": _ts_to_date(min_ts),
        "effective_end": _ts_to_date(max_ts),
        "ready_for_train": rows_final >= int(config.build.min_rows_for_train) and status_counts["FAIL"] == 0,
        "min_rows_for_train": int(config.build.min_rows_for_train),
        "status_counts": status_counts,
    }


def _load_manifest(dataset_root: Path) -> pl.DataFrame:
    path = dataset_root / "_meta" / "manifest.parquet"
    if not path.exists():
        return pl.DataFrame([], schema=FEATURES_V3_MANIFEST_SCHEMA, orient="row")
    return _normalize_manifest_rows(pl.read_parquet(path).to_dicts())


def _normalize_manifest_rows(rows: list[dict[str, Any]]) -> pl.DataFrame:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        item = row if isinstance(row, dict) else {}
        normalized.append(
            {
                "dataset_name": _coerce_str(item.get("dataset_name")),
                "tf": _coerce_str(item.get("tf")),
                "market": _coerce_str(item.get("market")),
                "rows_base_total": _coerce_int(item.get("rows_base_total")),
                "rows_after_multitf": _coerce_int(item.get("rows_after_multitf")),
                "rows_after_label": _coerce_int(item.get("rows_after_label")),
                "rows_final": _coerce_int(item.get("rows_final")),
                "rows_dropped_no_micro": _coerce_int(item.get("rows_dropped_no_micro")),
                "rows_dropped_stale": _coerce_int(item.get("rows_dropped_stale")),
                "rows_dropped_one_m_before_densify": _coerce_int(item.get("rows_dropped_one_m_before_densify")),
                "rows_dropped_one_m": _coerce_int(item.get("rows_dropped_one_m")),
                "rows_rescued_by_one_m_densify": _coerce_int(item.get("rows_rescued_by_one_m_densify")),
                "one_m_synth_ratio_mean": _coerce_float(item.get("one_m_synth_ratio_mean")),
                "one_m_synth_ratio_p50": _coerce_float(item.get("one_m_synth_ratio_p50")),
                "one_m_synth_ratio_p90": _coerce_float(item.get("one_m_synth_ratio_p90")),
                "tail_dropped_rows": _coerce_int(item.get("tail_dropped_rows")),
                "min_ts_ms": _coerce_int(item.get("min_ts_ms")),
                "max_ts_ms": _coerce_int(item.get("max_ts_ms")),
                "effective_start_ts_ms": _coerce_int(item.get("effective_start_ts_ms")),
                "effective_end_ts_ms": _coerce_int(item.get("effective_end_ts_ms")),
                "micro_tf_used": _coerce_str(item.get("micro_tf_used")),
                "status": _coerce_str(item.get("status")),
                "reasons_json": _coerce_json_text(item.get("reasons_json")),
                "error_message": _coerce_str(item.get("error_message")),
                "built_at": _coerce_int(item.get("built_at")),
            }
        )
    return pl.DataFrame(normalized, schema=FEATURES_V3_MANIFEST_SCHEMA, orient="row")


def _select_v3_universe_markets(
    *,
    config: FeaturesV3Config,
    tf: str,
    top_n: int,
    quote: str,
    start_ts_ms: int,
    base_candles_root: Path,
    discovered_windows: list[dict[str, Any]],
) -> tuple[list[str], dict[str, Any]]:
    discovered_markets = [str(item.get("market", "")).strip().upper() for item in discovered_windows]
    discovered_markets = [item for item in discovered_markets if item]
    discovered_set = set(discovered_markets)
    warnings: list[str] = []
    quote_prefix = f"{quote}-"
    mode = str(config.universe.mode).strip().lower()

    report: dict[str, Any] = {
        "mode": mode,
        "quality_enabled": bool(config.build.universe_quality_enabled),
        "parameters": {
            "quote": quote,
            "top_n": int(top_n),
            "value_lookback_days": int(config.universe.lookback_days),
            "quality_lookback_days": int(config.build.universe_quality_lookback_days),
            "beta": float(config.build.universe_quality_beta),
            "q_floor": float(config.build.universe_quality_q_floor),
            "oversample_factor": int(config.build.universe_quality_oversample_factor),
        },
        "lookback_window": {
            "value_from_ts_ms": int(start_ts_ms - max(1, int(config.universe.lookback_days)) * 86_400_000),
            "quality_from_ts_ms": int(start_ts_ms - max(1, int(config.build.universe_quality_lookback_days)) * 86_400_000),
            "to_ts_ms_exclusive": int(start_ts_ms),
        },
        "discovered_markets": int(len(discovered_markets)),
        "selected_markets": [],
        "warnings": [],
        "candidates": [],
        "fallback": {"enabled": True, "filled_count": 0, "reasons": []},
    }

    if not discovered_markets or top_n <= 0:
        report["warnings"] = warnings
        return [], report

    if mode == "fixed_list":
        fixed = [
            item
            for item in config.universe.fixed_list
            if item.startswith(quote_prefix) and item in discovered_set
        ]
        selected = fixed[:top_n]
        fallback_filled = 0
        if len(selected) < top_n:
            seen = set(selected)
            for market in discovered_markets:
                if market in seen:
                    continue
                selected.append(market)
                seen.add(market)
                fallback_filled += 1
                if len(selected) >= top_n:
                    break
            if fallback_filled > 0:
                warnings.append("FIXED_LIST_INSUFFICIENT_FILLED_FROM_DISCOVERED")

        report["candidates"] = [
            {
                "market": market,
                "source": "fixed_list" if market in fixed else "discovered_fallback",
                "selected": market in set(selected),
            }
            for market in selected
        ]
        report["selected_markets"] = selected[:top_n]
        report["warnings"] = warnings
        report["fallback"] = {
            "enabled": True,
            "filled_count": int(fallback_filled),
            "reasons": ["FIXED_LIST_INSUFFICIENT_FILLED_FROM_DISCOVERED"] if fallback_filled > 0 else [],
        }
        return selected[:top_n], report

    value_lookback_days = max(1, int(config.universe.lookback_days))
    quality_lookback_days = max(1, int(config.build.universe_quality_lookback_days))
    value_from_ts_ms = int(start_ts_ms - value_lookback_days * 86_400_000)
    quality_from_ts_ms = int(start_ts_ms - quality_lookback_days * 86_400_000)
    to_ts_ms_exclusive = int(start_ts_ms)

    ranked_by_value = _rank_markets_by_trade_value_v3(
        dataset_root=base_candles_root,
        tf=tf,
        markets=discovered_markets,
        from_ts_ms=value_from_ts_ms,
        to_ts_ms_exclusive=to_ts_ms_exclusive,
    )
    if not ranked_by_value:
        warnings.append("VALUE_LOOKBACK_EMPTY_FALLBACK_TO_MICRO_ROWS")
        ranked_by_value = [
            (str(item.get("market", "")).strip().upper(), float(item.get("rows") or 0.0))
            for item in discovered_windows
            if str(item.get("market", "")).strip().upper()
        ]
    elif max((float(item[1]) for item in ranked_by_value), default=0.0) <= 0.0:
        warnings.append("VALUE_LOOKBACK_ALL_ZERO")

    oversample_factor = max(1, int(config.build.universe_quality_oversample_factor))
    oversample_size = min(len(ranked_by_value), max(int(top_n), int(top_n) * oversample_factor))
    candidate_pool = ranked_by_value[:oversample_size]
    quality_enabled = bool(config.build.universe_quality_enabled)
    q_floor = min(max(float(config.build.universe_quality_q_floor), 0.0), 1.0)
    beta = max(float(config.build.universe_quality_beta), 0.0)

    candidates: list[dict[str, Any]] = []
    for rank, (market, value_est) in enumerate(candidate_pool, start=1):
        trade_value_est = float(value_est)
        synth_ratio = None
        real_ratio = None
        real_count = None
        expected_minutes = None
        q_value = 1.0
        quality_weight = 1.0
        score = trade_value_est
        source = "value_rank"
        if quality_enabled:
            synth_ratio, real_ratio, real_count, expected_minutes = _market_one_m_synth_ratio_lookback(
                dataset_root=base_candles_root,
                market=market,
                from_ts_ms=quality_from_ts_ms,
                to_ts_ms_exclusive=to_ts_ms_exclusive,
            )
            q_value = min(max(1.0 - float(synth_ratio), q_floor), 1.0)
            quality_weight = q_value ** beta
            score = trade_value_est * quality_weight

        candidates.append(
            {
                "rank_by_value": int(rank),
                "market": market,
                "source": source,
                "value_est": trade_value_est,
                "one_m_synth_ratio_lookback": synth_ratio,
                "one_m_real_ratio_lookback": real_ratio,
                "one_m_real_count_lookback": real_count,
                "one_m_expected_minutes_lookback": expected_minutes,
                "q": float(q_value),
                "quality_weight": float(quality_weight),
                "score": float(score),
                "selected": False,
            }
        )

    ranked_candidates = sorted(
        candidates,
        key=lambda item: (-float(item["score"]), -float(item["value_est"]), str(item["market"])),
    )
    selected = [str(item["market"]) for item in ranked_candidates[:top_n]]
    fallback_filled = 0
    fallback_reasons: list[str] = []
    if len(selected) < top_n:
        seen = set(selected)
        for market, _ in ranked_by_value:
            if market in seen:
                continue
            selected.append(market)
            seen.add(market)
            fallback_filled += 1
            if len(selected) >= top_n:
                break
        if fallback_filled > 0:
            fallback_reasons.append("UNIVERSE_FILL_FALLBACK_TO_VALUE_RANK")
            warnings.append("UNIVERSE_FILL_FALLBACK_TO_VALUE_RANK")

    selected_set = set(selected[:top_n])
    for item in ranked_candidates:
        item["selected"] = bool(item["market"] in selected_set)

    report["selected_markets"] = selected[:top_n]
    report["warnings"] = warnings
    report["candidates"] = ranked_candidates
    report["fallback"] = {
        "enabled": True,
        "filled_count": int(fallback_filled),
        "reasons": fallback_reasons,
    }
    return selected[:top_n], report


def _rank_markets_by_trade_value_v3(
    *,
    dataset_root: Path,
    tf: str,
    markets: list[str],
    from_ts_ms: int,
    to_ts_ms_exclusive: int,
) -> list[tuple[str, float]]:
    ranked: list[tuple[str, float]] = []
    for market in markets:
        value_est = _market_trade_value_est(
            dataset_root=dataset_root,
            tf=tf,
            market=market,
            from_ts_ms=from_ts_ms,
            to_ts_ms_exclusive=to_ts_ms_exclusive,
        )
        ranked.append((market, float(value_est)))
    ranked.sort(key=lambda item: (-float(item[1]), str(item[0])))
    return ranked


def _market_trade_value_est(
    *,
    dataset_root: Path,
    tf: str,
    market: str,
    from_ts_ms: int,
    to_ts_ms_exclusive: int,
) -> float:
    if to_ts_ms_exclusive <= from_ts_ms:
        return 0.0
    files = _candle_part_files(dataset_root=dataset_root, tf=tf, market=market)
    if not files:
        return 0.0
    lazy = pl.scan_parquet([str(path) for path in files]).filter(
        (pl.col("ts_ms") >= int(from_ts_ms)) & (pl.col("ts_ms") < int(to_ts_ms_exclusive))
    )
    schema = lazy.collect_schema()
    names = set(schema.names())
    if "volume_quote" in names and "close" in names and "volume_base" in names:
        trade_value_expr = (
            pl.when(pl.col("volume_quote").is_not_null())
            .then(pl.col("volume_quote").cast(pl.Float64))
            .otherwise((pl.col("close").cast(pl.Float64) * pl.col("volume_base").cast(pl.Float64)))
            .sum()
            .alias("trade_value")
        )
    elif "volume_quote" in names:
        trade_value_expr = pl.col("volume_quote").cast(pl.Float64).sum().alias("trade_value")
    elif "close" in names and "volume_base" in names:
        trade_value_expr = (pl.col("close").cast(pl.Float64) * pl.col("volume_base").cast(pl.Float64)).sum().alias(
            "trade_value"
        )
    else:
        return 0.0
    frame = _collect_lazy(lazy.select(trade_value_expr))
    value = frame.item(row=0, column="trade_value") if frame.height > 0 else None
    out = _safe_float(value)
    return float(out) if out is not None else 0.0


def _market_one_m_synth_ratio_lookback(
    *,
    dataset_root: Path,
    market: str,
    from_ts_ms: int,
    to_ts_ms_exclusive: int,
) -> tuple[float, float, float, int]:
    if to_ts_ms_exclusive <= from_ts_ms:
        return 1.0, 0.0, 0.0, 0
    expected_minutes = max(0, int((int(to_ts_ms_exclusive) - int(from_ts_ms)) // 60_000))
    if expected_minutes <= 0:
        return 1.0, 0.0, 0.0, 0

    files = _candle_part_files(dataset_root=dataset_root, tf="1m", market=market)
    if not files:
        return 1.0, 0.0, 0.0, expected_minutes
    lazy = pl.scan_parquet([str(path) for path in files]).filter(
        (pl.col("ts_ms") >= int(from_ts_ms)) & (pl.col("ts_ms") < int(to_ts_ms_exclusive))
    )
    schema = lazy.collect_schema()
    if "volume_base" not in schema.names():
        return 1.0, 0.0, 0.0, expected_minutes
    frame = _collect_lazy(
        lazy.select(
            pl.col("volume_base")
            .cast(pl.Float64)
            .fill_null(0.0)
            .gt(0.0)
            .cast(pl.Int64)
            .sum()
            .alias("real_count")
        )
    )
    real_count_value = frame.item(row=0, column="real_count") if frame.height > 0 else 0
    real_count = float(real_count_value or 0.0)
    real_ratio = min(max(real_count / float(expected_minutes), 0.0), 1.0)
    synth_ratio = min(max(1.0 - real_ratio, 0.0), 1.0)
    return float(synth_ratio), float(real_ratio), float(real_count), int(expected_minutes)


def _discover_micro_market_windows(
    *,
    micro_root: Path,
    quote: str,
    start_ts_ms: int,
    end_ts_ms: int,
) -> list[dict[str, Any]]:
    quote_prefix = f"{quote}-"
    windows: dict[str, dict[str, Any]] = {}
    for tf in ("5m", "1m"):
        tf_dir = micro_root / f"tf={tf}"
        if not tf_dir.exists():
            continue
        for market_dir in sorted(tf_dir.glob("market=*")):
            if not market_dir.is_dir():
                continue
            market = market_dir.name.replace("market=", "", 1).strip().upper()
            if not market.startswith(quote_prefix):
                continue
            files = _micro_part_files(market_dir=market_dir, start_ts_ms=start_ts_ms, end_ts_ms=end_ts_ms)
            if not files:
                continue
            frame = _collect_lazy(
                pl.scan_parquet([str(path) for path in files]).filter(
                    (pl.col("ts_ms") >= int(start_ts_ms)) & (pl.col("ts_ms") <= int(end_ts_ms))
                ).select(
                    [
                        pl.len().alias("rows"),
                        pl.col("ts_ms").min().alias("min_ts_ms"),
                        pl.col("ts_ms").max().alias("max_ts_ms"),
                    ]
                )
            )
            if frame.height <= 0:
                continue
            rows = int(frame.item(0, "rows") or 0)
            if rows <= 0:
                continue
            current = windows.get(market)
            if current is None or rows > int(current["rows"]):
                windows[market] = {
                    "market": market,
                    "rows": rows,
                    "min_ts_ms": int(frame.item(0, "min_ts_ms")),
                    "max_ts_ms": int(frame.item(0, "max_ts_ms")),
                    "source_tf": tf,
                }
    return sorted(windows.values(), key=lambda item: (-int(item["rows"]), str(item["market"])))


def _micro_part_files(*, market_dir: Path, start_ts_ms: int, end_ts_ms: int) -> list[Path]:
    start_day = datetime.fromtimestamp(int(start_ts_ms) / 1000.0, tz=timezone.utc).date()
    end_day = datetime.fromtimestamp(int(end_ts_ms) / 1000.0, tz=timezone.utc).date()
    files: list[Path] = []
    cursor = start_day
    while cursor <= end_day:
        day_dir = market_dir / f"date={cursor.isoformat()}"
        if day_dir.exists():
            files.extend(path for path in sorted(day_dir.glob("*.parquet")) if path.is_file())
        cursor += timedelta(days=1)
    return files


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


def _load_feature_market(*, dataset_root: Path, tf: str, market: str) -> pl.DataFrame:
    files = _feature_part_files(dataset_root=dataset_root, tf=tf, market=market)
    if not files:
        return pl.DataFrame(schema={"ts_ms": pl.Int64}, orient="row")
    return _collect_lazy(pl.scan_parquet([str(path) for path in files])).sort("ts_ms")


def _feature_part_files(*, dataset_root: Path, tf: str, market: str) -> list[Path]:
    market_dir = dataset_root / f"tf={tf}" / f"market={market}"
    if not market_dir.exists():
        return []
    nested: list[Path] = []
    for date_dir in sorted(market_dir.glob("date=*")):
        if not date_dir.is_dir():
            continue
        nested.extend(path for path in sorted(date_dir.glob("*.parquet")) if path.is_file())
    if nested:
        return nested
    direct = sorted(path for path in market_dir.glob("part-*.parquet") if path.is_file())
    if direct:
        return direct
    legacy = market_dir / "part.parquet"
    if legacy.exists():
        return [legacy]
    return []


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


def _build_feature_spec_payload(
    *,
    config: FeaturesV3Config,
    tf: str,
    quote: str,
    selected_markets: list[str],
    feature_cols: list[str],
    base_candles_root: Path,
    micro_root: Path,
    start_ts_ms: int,
    end_ts_ms: int,
) -> dict[str, Any]:
    return {
        "dataset_name": config.dataset_name,
        "tf": tf,
        "quote": quote,
        "feature_set_version": "v3",
        "feature_columns": feature_cols,
        "high_tfs": list(config.build.high_tfs),
        "sample_weight": {
            "column": "sample_weight",
            "half_life_days": float(config.build.sample_weight_half_life_days),
            "formula": "exp(-ln(2)*age_days/half_life_days) * clip((1-one_m_synth_ratio), floor, 1)^power",
            "one_m_synth_quality_weight": {
                "enabled": True,
                "floor": float(config.build.one_m_synth_weight_floor),
                "power": float(config.build.one_m_synth_weight_power),
            },
        },
        "micro_mandatory": True,
        "one_m_densify": {
            "enabled": True,
            "drop_if_real_count_zero": bool(config.build.one_m_drop_if_real_count_zero),
        },
        "time_range": {"from_ts_ms": int(start_ts_ms), "to_ts_ms": int(end_ts_ms)},
        "universe_selection": {
            "mode": config.universe.mode,
            "lookback_days": int(config.universe.lookback_days),
            "quality_enabled": bool(config.build.universe_quality_enabled),
            "quality_lookback_days": int(config.build.universe_quality_lookback_days),
            "quality_q_floor": float(config.build.universe_quality_q_floor),
            "quality_beta": float(config.build.universe_quality_beta),
            "quality_oversample_factor": int(config.build.universe_quality_oversample_factor),
            "score_formula": "value_est * clip((1-one_m_synth_ratio_lookback), q_floor, 1)^beta",
        },
        "selected_markets": selected_markets,
        "base_candles_root": str(base_candles_root),
        "micro_root": str(micro_root),
        "fingerprint": {
            "base_candles_manifest_sha256": sha256_file(base_candles_root / "_meta" / "manifest.parquet"),
            "micro_manifest_sha256": sha256_file(micro_root / "_meta" / "manifest.parquet"),
            "config_sha256": sha256_json(_config_snapshot(config)),
        },
    }


def _build_label_spec_payload(*, config: FeaturesV3Config, tf: str, label_cols: list[str]) -> dict[str, Any]:
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


def _config_snapshot(config: FeaturesV3Config) -> dict[str, Any]:
    return {
        "build": {
            "output_dataset": config.build.output_dataset,
            "tf": config.build.tf,
            "base_candles_dataset": config.build.base_candles_dataset,
            "micro_dataset": config.build.micro_dataset,
            "high_tfs": list(config.build.high_tfs),
            "high_tf_staleness_multiplier": config.build.high_tf_staleness_multiplier,
            "one_m_required_bars": config.build.one_m_required_bars,
            "one_m_max_missing_ratio": config.build.one_m_max_missing_ratio,
            "one_m_drop_if_real_count_zero": config.build.one_m_drop_if_real_count_zero,
            "one_m_synth_weight_floor": config.build.one_m_synth_weight_floor,
            "one_m_synth_weight_power": config.build.one_m_synth_weight_power,
            "sample_weight_half_life_days": config.build.sample_weight_half_life_days,
            "universe_quality_enabled": config.build.universe_quality_enabled,
            "universe_quality_lookback_days": config.build.universe_quality_lookback_days,
            "universe_quality_beta": config.build.universe_quality_beta,
            "universe_quality_q_floor": config.build.universe_quality_q_floor,
            "universe_quality_oversample_factor": config.build.universe_quality_oversample_factor,
            "min_rows_for_train": config.build.min_rows_for_train,
            "require_micro_validate_pass": config.build.require_micro_validate_pass,
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
        "label_v1": {
            "horizon_bars": config.label_v1.horizon_bars,
            "thr_bps": config.label_v1.thr_bps,
            "neutral_policy": config.label_v1.neutral_policy,
            "fee_bps_est": config.label_v1.fee_bps_est,
            "safety_bps": config.label_v1.safety_bps,
        },
        "float_dtype": config.float_dtype,
    }


def _resolve_base_candles_root(*, parquet_root: Path, base_candles_value: str) -> Path | None:
    value = str(base_candles_value).strip() or "auto"
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


def _assert_micro_validate_ok(micro_root: Path) -> None:
    report = _read_json(micro_root / "_meta" / "validate_report.json")
    if not report:
        return
    fail_files = int(report.get("fail_files") or 0)
    if fail_files > 0:
        raise ValueError(f"micro validate report has fail_files={fail_files}; fix micro dataset before features_v3 build")


def _warmup_ms(*, config: FeaturesV3Config, base_tf: str) -> int:
    base = expected_interval_ms(base_tf) * 64
    one_m = expected_interval_ms("1m") * max(config.build.one_m_required_bars + 2, 8)
    high = max(expected_interval_ms(tf) * 12 for tf in config.build.high_tfs)
    return max(base, one_m, high)


def _date_strings_between(start_text: str, end_text: str) -> list[str]:
    start_day = date.fromisoformat(str(start_text))
    end_day = date.fromisoformat(str(end_text))
    out: list[str] = []
    cursor = start_day
    while cursor <= end_day:
        out.append(cursor.isoformat())
        cursor += timedelta(days=1)
    return out


def _count_future_source_rows(frame: pl.DataFrame) -> int:
    if frame.height <= 0 or "ts_ms" not in frame.columns:
        return 0
    checks: list[pl.Expr] = []
    for name in ("one_m_last_ts", "src_ts_micro", "src_ts_15m", "src_ts_60m", "src_ts_240m"):
        if name in frame.columns:
            checks.append(pl.col(name) > pl.col("ts_ms"))
    if not checks:
        return 0
    return int(frame.filter(pl.any_horizontal(checks)).height)


def _count_true_flags(frame: pl.DataFrame, *, suffix: str) -> int:
    if frame.height <= 0:
        return 0
    flags = [name for name in frame.columns if name.endswith(suffix)]
    if not flags:
        return 0
    return int(frame.filter(pl.any_horizontal([pl.col(name) == True for name in flags])).height)  # noqa: E712


def _is_non_monotonic(ts: pl.Series) -> bool:
    if ts.len() <= 1:
        return False
    diff = ts.diff().drop_nulls()
    if diff.len() <= 0:
        return False
    return bool((diff < 0).any())


def _ts_to_date(value: int | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(int(value) / 1000.0, tz=timezone.utc).date().isoformat()


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
    out: int | None = None
    for value in values:
        out = _safe_min(out, value)
    return out


def _safe_max_many(values: list[Any]) -> int | None:
    out: int | None = None
    for value in values:
        out = _safe_max(out, value)
    return out


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            return int(float(text))
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            return float(text)
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _coerce_json_text(value: Any) -> str:
    if value is None:
        return "[]"
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return "[]"
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return json.dumps([text], ensure_ascii=False)
        return json.dumps(parsed, ensure_ascii=False)
    return json.dumps(value, ensure_ascii=False)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return raw if isinstance(raw, dict) else {}


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


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if out != out:  # NaN
        return None
    return out


def _quantile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    clean = sorted(float(v) for v in values if v is not None)
    if not clean:
        return None
    if len(clean) == 1:
        return float(clean[0])
    q_clamped = min(max(float(q), 0.0), 1.0)
    idx = int(round((len(clean) - 1) * q_clamped))
    return float(clean[idx])
