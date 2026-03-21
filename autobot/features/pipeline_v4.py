"""Feature store v4 research lane: v3 feature contract + label_v2 crypto cross-section."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import time
from typing import Any

import polars as pl

from autobot.data import expected_interval_ms

from .feature_set_v4_live_base import build_feature_set_v4_live_base_from_candles
from .feature_set_v4 import (
    attach_interaction_features_v4,
    attach_periodicity_features_v4,
    attach_spillover_breadth_features_v4,
    attach_trend_volume_features_v4,
    ctrend_feature_columns_v4,
    feature_columns_v4,
    required_feature_columns_v4,
)
from .ctrend_v1 import attach_ctrend_v1_features, ctrend_v1_factor_contract, ctrend_v1_history_lookback_days
from .feature_spec import (
    FeatureSetV1Config,
    LabelV1Config,
    TimeRangeConfig,
    UniverseConfig,
    parse_date_to_ts_ms,
    sha256_file,
    sha256_json,
    load_features_config,
    write_json,
)
from .labeling_v2_crypto_cs import (
    LabelV2CryptoCsConfig,
    apply_labeling_v2_crypto_cs,
    drop_neutral_rows_v2_crypto_cs,
    label_distribution_v2_crypto_cs,
)
from .micro_required_join_v1 import load_market_micro_for_base, resolve_micro_dataset_root
from .order_flow_panel_v1 import (
    attach_order_flow_panel_v1,
    order_flow_panel_v1_contract,
    order_flow_panel_v1_diagnostics,
)
from .pipeline_v3 import (
    _assert_micro_validate_ok,
    _cleanup_market_target_dates,
    _count_future_source_rows,
    _count_true_flags,
    _date_strings_between,
    _discover_micro_market_windows,
    _is_non_monotonic,
    _load_feature_market,
    _load_manifest,
    _load_market_candles,
    _load_yaml_doc,
    _normalize_manifest_rows,
    _quantile,
    _read_json,
    _resolve_base_candles_root,
    _safe_float,
    _safe_max,
    _safe_max_many,
    _safe_min,
    _safe_min_many,
    _select_v3_universe_markets,
    _ts_to_date,
    _warmup_ms,
)


DEFAULT_FEATURES_V4_YAML = "features_v4.yaml"


@dataclass(frozen=True)
class FeaturesV4BuildConfig:
    output_dataset: str = "features_v4"
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
class FeaturesV4ValidateConfig:
    leakage_fail_on_future_ts: bool = True


@dataclass(frozen=True)
class FeaturesV4Config:
    build: FeaturesV4BuildConfig
    parquet_root: Path
    features_root: Path
    universe: UniverseConfig
    time_range: TimeRangeConfig
    feature_set_v1: FeatureSetV1Config
    label_v2: LabelV2CryptoCsConfig
    validation: FeaturesV4ValidateConfig
    float_dtype: str = "float32"

    @property
    def dataset_name(self) -> str:
        return self.build.output_dataset

    @property
    def output_dataset_root(self) -> Path:
        return self.features_root / self.dataset_name


@dataclass(frozen=True)
class FeatureBuildV4Options:
    tf: str
    quote: str | None = None
    top_n: int | None = None
    start: str | None = None
    end: str | None = None
    feature_set: str = "v4"
    label_set: str = "v2"
    workers: int = 1
    fail_on_warn: bool = False
    dry_run: bool = False
    base_candles: str | None = None
    micro_dataset: str | None = None


@dataclass(frozen=True)
class FeatureBuildV4Summary:
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
class FeatureValidateV4Options:
    tf: str
    quote: str | None = None
    top_n: int | None = None
    start: str | None = None
    end: str | None = None


@dataclass(frozen=True)
class FeatureValidateV4Summary:
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


def load_features_v4_config(
    config_dir: Path,
    *,
    base_config: dict[str, Any] | None = None,
    filename: str = DEFAULT_FEATURES_V4_YAML,
) -> FeaturesV4Config:
    shared = load_features_config(config_dir, base_config=base_config, filename=filename)
    raw = _load_yaml_doc(config_dir / filename)
    root = raw.get("features_v4", {}) if isinstance(raw.get("features_v4"), dict) else {}
    validation_cfg = root.get("validation", {}) if isinstance(root.get("validation"), dict) else {}
    label_cfg = raw.get("label_v2", {}) if isinstance(raw.get("label_v2"), dict) else {}

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
        raise ValueError("features_v4.tf currently supports only 5m")

    label_v2 = LabelV2CryptoCsConfig(
        horizon_bars=max(int(label_cfg.get("horizon_bars", 12)), 1),
        fee_bps_est=float(label_cfg.get("fee_bps_est", 10.0)),
        safety_bps=float(label_cfg.get("safety_bps", 5.0)),
        top_quantile=float(label_cfg.get("top_quantile", 0.2)),
        bottom_quantile=float(label_cfg.get("bottom_quantile", 0.2)),
        neutral_policy=str(label_cfg.get("neutral_policy", "drop")).strip().lower() or "drop",
    )
    _validate_v2_label_config(label_v2)

    return FeaturesV4Config(
        build=FeaturesV4BuildConfig(
            output_dataset=str(root.get("output_dataset", "features_v4")).strip() or "features_v4",
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
        label_v2=label_v2,
        validation=FeaturesV4ValidateConfig(
            leakage_fail_on_future_ts=bool(validation_cfg.get("leakage_fail_on_future_ts", True))
        ),
        float_dtype=shared.float_dtype,
    )


def build_features_dataset_v4(config: FeaturesV4Config, options: FeatureBuildV4Options) -> FeatureBuildV4Summary:
    if options.feature_set != "v4":
        raise ValueError("feature_set currently supports only v4 in pipeline_v4")
    if options.label_set != "v2":
        raise ValueError("label_set currently supports only v2 in pipeline_v4")

    tf = str(options.tf or config.build.tf).strip().lower()
    if tf != "5m":
        raise ValueError("features_v4 currently supports only --tf 5m")
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
        _assert_micro_validate_ok(
            micro_root,
            from_ts_ms=start_ts_ms,
            to_ts_ms=end_ts_ms,
        )

    base_root = _resolve_base_candles_root(
        parquet_root=config.parquet_root,
        base_candles_value=(str(options.base_candles).strip() if options.base_candles else config.build.base_candles_dataset),
    )
    if base_root is None:
        raise ValueError("unable to resolve base candles dataset for v4")

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

    feature_cols = list(feature_columns_v4(high_tfs=config.build.high_tfs))
    label_cols = ["y_reg_net_12", "y_rank_cs_12", "y_cls_topq_12"]
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
        return FeatureBuildV4Summary(
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
    extended_end_ts_ms = end_ts_ms + int(config.label_v2.horizon_bars) * interval_ms
    target_dates = _date_strings_between(start_text, end_text)
    for market in selected_markets:
        _cleanup_market_target_dates(dataset_root=output_root, tf=tf, market=market, target_dates=target_dates)

    bootstrap_label = _bootstrap_label_v1_from_v2(config.label_v2)
    market_frames: list[pl.DataFrame] = []
    rows: list[dict[str, Any]] = []
    details: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    per_market_bootstrap: dict[str, dict[str, Any]] = {}
    rows_base_total = 0
    rows_dropped_no_micro = 0
    rows_dropped_one_m_before_densify = 0
    rows_dropped_one_m = 0
    rows_rescued_by_one_m_densify = 0
    ok_markets = 0
    warn_markets = 0
    fail_markets = 0
    one_m_synth_ratio_values: list[float] = []
    active_ctrend_columns = ctrend_feature_columns_v4()
    ctrend_enabled = bool(active_ctrend_columns)
    ctrend_history_roots = (
        _resolve_ctrend_history_roots(parquet_root=config.parquet_root, primary_root=base_root)
        if ctrend_enabled
        else tuple()
    )
    ctrend_history_from_ts_ms = (
        start_ts_ms - int(ctrend_v1_history_lookback_days()) * 86_400_000
        if ctrend_enabled
        else start_ts_ms
    )

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
            result = build_feature_set_v4_live_base_from_candles(
                base_candles_frame=base,
                one_m_candles_frame=one_m,
                high_tf_candles=high_frames,
                micro_frame=micro,
                micro_tf_used=micro_tf_used,
                tf=tf,
                from_ts_ms=start_ts_ms,
                to_ts_ms=end_ts_ms,
                label_config=bootstrap_label,
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
            if frame.height > 0:
                frame = frame.drop([name for name in ("y_reg", "y_cls") if name in frame.columns]).with_columns(
                    pl.lit(market, dtype=pl.Utf8).alias("market")
                )
                if ctrend_enabled:
                    ctrend_history = _load_market_candles_merged(
                        dataset_roots=ctrend_history_roots,
                        tf=tf,
                        market=market,
                        from_ts_ms=ctrend_history_from_ts_ms,
                        to_ts_ms=extended_end_ts_ms,
                    )
                    if ctrend_history.height > 0:
                        ctrend_history = ctrend_history.with_columns(pl.lit(market, dtype=pl.Utf8).alias("market"))
                        frame = attach_ctrend_v1_features(
                            frame,
                            history_frame=ctrend_history,
                            float_dtype=config.float_dtype,
                        )
                market_frames.append(frame)
                if "one_m_synth_ratio" in frame.columns:
                    one_m_synth_ratio_values.extend(
                        float(v)
                        for v in frame.get_column("one_m_synth_ratio").drop_nulls().to_list()
                        if v is not None
                    )

            rows_base_total += int(result.rows_base_total)
            rows_dropped_no_micro += int(result.rows_dropped_no_micro)
            rows_dropped_one_m_before_densify += int(result.rows_dropped_one_m_before_densify)
            rows_dropped_one_m += int(result.rows_dropped_one_m)
            rows_rescued_by_one_m_densify += int(result.rows_rescued_by_one_m_densify)

            per_market_bootstrap[market] = {
                "market": market,
                "status": "PENDING_V2",
                "rows_base_total": int(result.rows_base_total),
                "rows_after_multitf": int(result.rows_after_multitf),
                "rows_after_bootstrap_label": int(result.rows_after_label),
                "rows_dropped_no_micro": int(result.rows_dropped_no_micro),
                "rows_dropped_stale": int(result.rows_dropped_stale),
                "rows_dropped_one_m_before_densify": int(result.rows_dropped_one_m_before_densify),
                "rows_dropped_one_m": int(result.rows_dropped_one_m),
                "rows_rescued_by_one_m_densify": int(result.rows_rescued_by_one_m_densify),
                "one_m_synth_ratio_mean": _safe_float(result.one_m_synth_ratio_mean),
                "one_m_synth_ratio_p50": _safe_float(result.one_m_synth_ratio_p50),
                "one_m_synth_ratio_p90": _safe_float(result.one_m_synth_ratio_p90),
                "tail_dropped_rows": int(result.tail_dropped_rows),
                "micro_tf_used": result.micro_tf_used,
            }
        except Exception as exc:
            fail_markets += 1
            detail = {"market": market, "status": "FAIL", "reasons": ["BUILD_EXCEPTION"], "error_message": str(exc)}
            details.append(detail)
            failures.append(detail)
            rows.append(_failure_manifest_row(config=config, tf=tf, market=market, error_message=str(exc)))

    combined = pl.concat(market_frames, how="vertical_relaxed") if market_frames else pl.DataFrame()
    order_flow_diagnostics: dict[str, Any] | None = None
    if combined.height > 0:
        enriched = attach_spillover_breadth_features_v4(
            combined.sort(["ts_ms", "market"]),
            quote=quote,
            float_dtype=config.float_dtype,
        )
        enriched = attach_periodicity_features_v4(
            enriched,
            float_dtype=config.float_dtype,
        )
        enriched = attach_trend_volume_features_v4(
            enriched,
            float_dtype=config.float_dtype,
        )
        enriched = attach_order_flow_panel_v1(
            enriched,
            float_dtype=config.float_dtype,
        )
        if ctrend_enabled:
            enriched = attach_ctrend_v1_features(
                enriched,
                float_dtype=config.float_dtype,
            )
        enriched = attach_interaction_features_v4(
            enriched,
            float_dtype=config.float_dtype,
        )
        order_flow_diagnostics = order_flow_panel_v1_diagnostics(enriched)
        labeled = apply_labeling_v2_crypto_cs(
            enriched,
            config=config.label_v2,
            ts_col="ts_ms",
            market_col="market",
            close_col="close",
        )
        label_dist_before_drop = label_distribution_v2_crypto_cs(labeled, config=config.label_v2)
        labeled = drop_neutral_rows_v2_crypto_cs(labeled, config=config.label_v2)
        required_non_null = [
            "ts_ms",
            "market",
            "sample_weight",
            *required_feature_columns_v4(high_tfs=config.build.high_tfs),
            *label_cols,
        ]
        labeled = labeled.filter(
            pl.all_horizontal([pl.col(name).is_not_null() for name in required_non_null if name in labeled.columns])
        )
    else:
        labeled = pl.DataFrame()
        label_dist_before_drop = {"pos": 0, "neg": 0, "neutral": 0, "total": 0}

    rows_final = 0
    min_ts_total: int | None = None
    max_ts_total: int | None = None
    for market in selected_markets:
        if any(item.get("market") == market and item.get("status") == "FAIL" for item in failures):
            continue
        market_frame = (
            labeled.filter(pl.col("market") == market).sort("ts_ms")
            if labeled.height > 0 and "market" in labeled.columns
            else pl.DataFrame()
        )
        _write_market_date_partitions_v4(frame=market_frame, dataset_root=output_root, tf=tf, market=market)
        market_rows_final = int(market_frame.height)
        market_min_ts = int(market_frame.get_column("ts_ms").min()) if market_rows_final > 0 else None
        market_max_ts = int(market_frame.get_column("ts_ms").max()) if market_rows_final > 0 else None
        rows_final += market_rows_final
        min_ts_total = _safe_min(min_ts_total, market_min_ts)
        max_ts_total = _safe_max(max_ts_total, market_max_ts)

        pos_rows = int(market_frame.filter(pl.col("y_cls_topq_12") == 1).height) if market_rows_final > 0 else 0
        neg_rows = int(market_frame.filter(pl.col("y_cls_topq_12") == 0).height) if market_rows_final > 0 else 0
        reasons: list[str] = []
        status = "OK"
        if market_rows_final <= 0:
            status = "WARN"
            reasons.append("NO_ROWS_AFTER_LABEL_V2")
        elif pos_rows <= 0 or neg_rows <= 0:
            status = "WARN"
            reasons.append("IMBALANCED_LABELS")
        if per_market_bootstrap.get(market, {}).get("rows_dropped_no_micro", 0) > 0:
            if status == "OK":
                status = "WARN"
            reasons.append("MICRO_MANDATORY_DROPS_PRESENT")

        if status == "OK":
            ok_markets += 1
        elif status == "WARN":
            warn_markets += 1
        else:
            fail_markets += 1

        bootstrap = per_market_bootstrap.get(market, {})
        detail = {
            "market": market,
            "status": status,
            "rows_base_total": int(bootstrap.get("rows_base_total", 0)),
            "rows_after_multitf": int(bootstrap.get("rows_after_multitf", 0)),
            "rows_after_bootstrap_label": int(bootstrap.get("rows_after_bootstrap_label", 0)),
            "rows_after_label_v2": market_rows_final,
            "rows_final": market_rows_final,
            "rows_dropped_no_micro": int(bootstrap.get("rows_dropped_no_micro", 0)),
            "rows_dropped_stale": int(bootstrap.get("rows_dropped_stale", 0)),
            "rows_dropped_one_m_before_densify": int(bootstrap.get("rows_dropped_one_m_before_densify", 0)),
            "rows_dropped_one_m": int(bootstrap.get("rows_dropped_one_m", 0)),
            "rows_rescued_by_one_m_densify": int(bootstrap.get("rows_rescued_by_one_m_densify", 0)),
            "one_m_synth_ratio_mean": bootstrap.get("one_m_synth_ratio_mean"),
            "one_m_synth_ratio_p50": bootstrap.get("one_m_synth_ratio_p50"),
            "one_m_synth_ratio_p90": bootstrap.get("one_m_synth_ratio_p90"),
            "tail_dropped_rows": int(bootstrap.get("tail_dropped_rows", 0)),
            "label_pos_rows": pos_rows,
            "label_neg_rows": neg_rows,
            "min_ts_ms": market_min_ts,
            "max_ts_ms": market_max_ts,
            "effective_start": _ts_to_date(market_min_ts),
            "effective_end": _ts_to_date(market_max_ts),
            "micro_tf_used": bootstrap.get("micro_tf_used"),
            "reasons": reasons,
        }
        details.append(detail)
        rows.append(
            {
                "dataset_name": config.dataset_name,
                "tf": tf,
                "market": market,
                "rows_base_total": int(bootstrap.get("rows_base_total", 0)),
                "rows_after_multitf": int(bootstrap.get("rows_after_multitf", 0)),
                "rows_after_label": market_rows_final,
                "rows_final": market_rows_final,
                "rows_dropped_no_micro": int(bootstrap.get("rows_dropped_no_micro", 0)),
                "rows_dropped_stale": int(bootstrap.get("rows_dropped_stale", 0)),
                "rows_dropped_one_m_before_densify": int(bootstrap.get("rows_dropped_one_m_before_densify", 0)),
                "rows_dropped_one_m": int(bootstrap.get("rows_dropped_one_m", 0)),
                "rows_rescued_by_one_m_densify": int(bootstrap.get("rows_rescued_by_one_m_densify", 0)),
                "one_m_synth_ratio_mean": bootstrap.get("one_m_synth_ratio_mean"),
                "one_m_synth_ratio_p50": bootstrap.get("one_m_synth_ratio_p50"),
                "one_m_synth_ratio_p90": bootstrap.get("one_m_synth_ratio_p90"),
                "tail_dropped_rows": int(bootstrap.get("tail_dropped_rows", 0)),
                "min_ts_ms": market_min_ts,
                "max_ts_ms": market_max_ts,
                "effective_start_ts_ms": market_min_ts,
                "effective_end_ts_ms": market_max_ts,
                "micro_tf_used": bootstrap.get("micro_tf_used"),
                "status": status,
                "reasons_json": json.dumps(reasons, ensure_ascii=False),
                "error_message": None,
                "built_at": int(time.time()),
            }
        )

    manifest = _normalize_manifest_rows(rows)
    manifest_file.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_parquet(manifest_file, compression="zstd")

    write_json(
        meta_root / "feature_spec.json",
        _build_feature_spec_payload_v4(
            config=config,
            tf=tf,
            quote=quote,
            selected_markets=selected_markets,
            feature_cols=feature_cols,
            base_candles_root=base_root,
            micro_root=micro_root,
            start_ts_ms=start_ts_ms,
            end_ts_ms=end_ts_ms,
            order_flow_diagnostics=order_flow_diagnostics,
        ),
    )
    write_json(meta_root / "label_spec.json", _build_label_spec_payload_v4(config=config, tf=tf, label_cols=label_cols))

    effective_start = _ts_to_date(min_ts_total)
    effective_end = _ts_to_date(max_ts_total)
    one_m_synth_ratio_p50 = _quantile(one_m_synth_ratio_values, 0.50)
    one_m_synth_ratio_p90 = _quantile(one_m_synth_ratio_values, 0.90)
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
        "min_rows_for_train": int(config.build.min_rows_for_train),
        "one_m_synth_ratio_p50": one_m_synth_ratio_p50,
        "one_m_synth_ratio_p90": one_m_synth_ratio_p90,
        "label_distribution_before_drop": label_dist_before_drop,
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
        "order_flow_panel_v1_diagnostics": order_flow_diagnostics,
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

    return FeatureBuildV4Summary(
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


def validate_features_dataset_v4(config: FeaturesV4Config, options: FeatureValidateV4Options) -> FeatureValidateV4Summary:
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
        manifest.filter((pl.col("tf") == tf) & (pl.col("market").str.starts_with(f"{quote}-"))).head(top_n).to_dicts()
        if manifest.height > 0
        else []
    )
    selected_markets = [str(item.get("market", "")).strip().upper() for item in selected if str(item.get("market", "")).strip()]
    meta_root = dataset_root / "_meta"
    report_file = meta_root / "validate_report.json"

    feature_spec = _read_json(meta_root / "feature_spec.json")
    feature_cols = (
        feature_spec.get("feature_columns")
        if isinstance(feature_spec.get("feature_columns"), list)
        else list(feature_columns_v4(high_tfs=config.build.high_tfs))
    )
    required = ["ts_ms", "market"] + [str(item) for item in feature_cols] + [
        "sample_weight",
        "y_reg_net_12",
        "y_rank_cs_12",
        "y_cls_topq_12",
    ]

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
        rows_count = int(frame.height)
        missing_columns = [name for name in required if name not in frame.columns]
        non_monotonic = _is_non_monotonic(frame.get_column("ts_ms")) if rows_count > 1 and "ts_ms" in frame.columns else False

        null_count = 0
        for col in required:
            if col in frame.columns:
                null_count += int(frame.get_column(col).null_count())
            else:
                null_count += rows_count
        total_cells += rows_count * len(required)
        total_null += null_count

        market_leakage_rows = _count_future_source_rows(frame)
        leakage_fail_rows += market_leakage_rows
        market_stale_rows = _count_true_flags(frame, suffix="_stale")
        staleness_fail_rows += market_stale_rows

        status = "OK"
        reasons: list[str] = []
        if rows_count <= 0:
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

        if status == "OK":
            ok_files += 1
        elif status == "WARN":
            warn_files += 1
        else:
            fail_files += 1

        details.append(
            {
                "market": market,
                "rows": rows_count,
                "missing_columns": missing_columns,
                "non_monotonic": non_monotonic,
                "null_ratio_overall": (float(null_count) / float(rows_count * len(required))) if rows_count > 0 else 0.0,
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

    return FeatureValidateV4Summary(
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


def features_stats_v4(
    config: FeaturesV4Config,
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
            "label_pos_rows": 0,
            "label_neg_rows": 0,
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
    label_pos_rows = 0
    label_neg_rows = 0
    for row in rows:
        status = str(row.get("status") or "").upper()
        if status in status_counts:
            status_counts[status] += 1
        market = str(row.get("market") or "").strip().upper()
        if market:
            frame = _load_feature_market(dataset_root=config.output_dataset_root, tf=tf_value, market=market)
            if frame.height > 0 and "y_cls_topq_12" in frame.columns:
                label_pos_rows += int(frame.filter(pl.col("y_cls_topq_12") == 1).height)
                label_neg_rows += int(frame.filter(pl.col("y_cls_topq_12") == 0).height)

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
        "label_pos_rows": label_pos_rows,
        "label_neg_rows": label_neg_rows,
        "one_m_synth_ratio_p50": _quantile(synth_values, 0.50),
        "one_m_synth_ratio_p90": _quantile(synth_values, 0.90),
        "effective_start": _ts_to_date(min_ts),
        "effective_end": _ts_to_date(max_ts),
        "ready_for_train": rows_final >= int(config.build.min_rows_for_train) and status_counts["FAIL"] == 0,
        "min_rows_for_train": int(config.build.min_rows_for_train),
        "status_counts": status_counts,
    }


def _build_feature_spec_payload_v4(
    *,
    config: FeaturesV4Config,
    tf: str,
    quote: str,
    selected_markets: list[str],
    feature_cols: list[str],
    base_candles_root: Path,
    micro_root: Path,
    start_ts_ms: int,
    end_ts_ms: int,
    order_flow_diagnostics: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        "dataset_name": config.dataset_name,
        "tf": tf,
        "quote": quote,
        "feature_set_version": "v4",
        "feature_columns": feature_cols,
        "base_feature_contract_version": "v4_live_base",
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
        "active_factor_contracts": ["ctrend_v1"] if ctrend_feature_columns_v4() else [],
        "factor_contracts": {"ctrend_v1": ctrend_v1_factor_contract()} if ctrend_feature_columns_v4() else {},
        "active_micro_panel_contracts": ["order_flow_panel_v1"],
        "micro_panel_contracts": {"order_flow_panel_v1": order_flow_panel_v1_contract()},
        "one_m_densify": {
            "enabled": True,
            "drop_if_real_count_zero": bool(config.build.one_m_drop_if_real_count_zero),
        },
        "order_flow_diagnostics": order_flow_diagnostics,
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
            "config_sha256": sha256_json(_config_snapshot_v4(config)),
        },
    }


def _resolve_ctrend_history_roots(*, parquet_root: Path, primary_root: Path) -> tuple[Path, ...]:
    ordered: list[Path] = []
    legacy_root = parquet_root / "candles_v1"
    if legacy_root.exists():
        ordered.append(legacy_root)
    if primary_root.exists():
        ordered.append(primary_root)
    seen: set[str] = set()
    result: list[Path] = []
    for path in ordered:
        resolved = str(path.resolve())
        if resolved in seen:
            continue
        seen.add(resolved)
        result.append(path)
    return tuple(result)


def _load_market_candles_merged(
    *,
    dataset_roots: tuple[Path, ...],
    tf: str,
    market: str,
    from_ts_ms: int,
    to_ts_ms: int,
) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    for root in dataset_roots:
        frame = _load_market_candles(
            dataset_root=root,
            tf=tf,
            market=market,
            from_ts_ms=from_ts_ms,
            to_ts_ms=to_ts_ms,
        )
        if frame.height > 0:
            frames.append(frame)
    if not frames:
        return pl.DataFrame()
    return (
        pl.concat(frames, how="vertical_relaxed")
        .sort("ts_ms")
        .unique(subset=["ts_ms"], keep="last", maintain_order=True)
    )


def _build_label_spec_payload_v4(*, config: FeaturesV4Config, tf: str, label_cols: list[str]) -> dict[str, Any]:
    return {
        "dataset_name": config.dataset_name,
        "tf": tf,
        "label_columns": label_cols,
        "label_set_version": "v2_crypto_cs",
        "horizon_bars": int(config.label_v2.horizon_bars),
        "fee_bps_est": float(config.label_v2.fee_bps_est),
        "safety_bps": float(config.label_v2.safety_bps),
        "top_quantile": float(config.label_v2.top_quantile),
        "bottom_quantile": float(config.label_v2.bottom_quantile),
        "neutral_policy": str(config.label_v2.neutral_policy),
        "definition": "future log return net of estimated costs, ranked cross-sectionally per ts_ms",
    }


def _config_snapshot_v4(config: FeaturesV4Config) -> dict[str, Any]:
    return {
        "dataset_name": config.dataset_name,
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
        "universe": {
            "quote": config.universe.quote,
            "mode": config.universe.mode,
            "top_n": config.universe.top_n,
            "lookback_days": config.universe.lookback_days,
            "fixed_list": list(config.universe.fixed_list),
        },
        "time_range": {"start": config.time_range.start, "end": config.time_range.end},
        "label_v2": {
            "horizon_bars": config.label_v2.horizon_bars,
            "fee_bps_est": config.label_v2.fee_bps_est,
            "safety_bps": config.label_v2.safety_bps,
            "top_quantile": config.label_v2.top_quantile,
            "bottom_quantile": config.label_v2.bottom_quantile,
            "neutral_policy": config.label_v2.neutral_policy,
        },
        "float_dtype": config.float_dtype,
    }


def _bootstrap_label_v1_from_v2(label_v2: LabelV2CryptoCsConfig) -> LabelV1Config:
    # Temporary bootstrap only: reuse the stable v3 builder for warmup/tail-guard before cross-sectional relabeling.
    return LabelV1Config(
        horizon_bars=max(int(label_v2.horizon_bars), 1),
        thr_bps=1.0,
        neutral_policy="keep_as_class",
        fee_bps_est=float(label_v2.fee_bps_est),
        safety_bps=float(label_v2.safety_bps),
    )


def _write_market_date_partitions_v4(*, frame: pl.DataFrame, dataset_root: Path, tf: str, market: str) -> None:
    target_dir = dataset_root / f"tf={tf}" / f"market={market}"
    target_dir.mkdir(parents=True, exist_ok=True)
    if frame.height <= 0 or "ts_ms" not in frame.columns:
        return
    working = frame.with_columns(pl.from_epoch("ts_ms", time_unit="ms").dt.date().cast(pl.Utf8).alias("__date"))
    for date_value, part in working.partition_by("__date", as_dict=True).items():
        label = date_value[0] if isinstance(date_value, tuple) else date_value
        if label is None:
            continue
        date_dir = target_dir / f"date={label}"
        date_dir.mkdir(parents=True, exist_ok=True)
        output = part.drop("__date").sort("ts_ms")
        output.write_parquet(date_dir / "part-000.parquet", compression="zstd")


def _validate_v2_label_config(config: LabelV2CryptoCsConfig) -> None:
    neutral_policy = str(config.neutral_policy).strip().lower()
    if neutral_policy not in {"drop", "keep_as_class"}:
        raise ValueError("label_v2.neutral_policy must be one of: drop, keep_as_class")
    if not 0.0 < float(config.top_quantile) < 0.5:
        raise ValueError("label_v2.top_quantile must be between 0 and 0.5")
    if not 0.0 < float(config.bottom_quantile) < 0.5:
        raise ValueError("label_v2.bottom_quantile must be between 0 and 0.5")


def _failure_manifest_row(*, config: FeaturesV4Config, tf: str, market: str, error_message: str) -> dict[str, Any]:
    return {
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
        "error_message": error_message,
        "built_at": int(time.time()),
    }
