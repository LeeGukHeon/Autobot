"""Internal CLI helpers for v4 trainer option wiring."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from autobot.models import TrainV4CryptoCsOptions
from autobot.strategy.model_alpha_v1 import (
    ModelAlphaExecutionSettings,
    ModelAlphaExitSettings,
    ModelAlphaOperationalSettings,
    ModelAlphaPositionSettings,
    ModelAlphaSelectionSettings,
    ModelAlphaSettings,
)


def build_v4_train_options(
    *,
    args: Any,
    defaults: dict[str, Any],
    backtest_defaults: dict[str, Any],
    features_v4_config: Any,
    registry_root: Path,
    logs_root: Path,
    top_n: int,
    dataset_root_override: Path | None = None,
    execution_acceptance_parquet_root_override: Path | None = None,
    resolve_backtest_dataset_name_for_model_features: Callable[..., str],
    clamp_prob_value: Callable[[float | None], float | None],
    optional_float_value: Callable[[Any], float | None],
) -> TrainV4CryptoCsOptions:
    model_family = (
        str(getattr(args, "model_family", None) or "train_v4_crypto_cs").strip() or "train_v4_crypto_cs"
    )
    backtest_dataset_name_v4 = resolve_backtest_dataset_name_for_model_features(
        parquet_root=Path(str(backtest_defaults["parquet_root"])),
        base_candles_dataset=str(features_v4_config.build.base_candles_dataset),
        fallback=str(backtest_defaults["dataset_name"]).strip() or "candles_v1",
    )
    model_alpha_backtest_defaults = backtest_defaults.get("model_alpha", {})
    model_alpha_selection_defaults = (
        model_alpha_backtest_defaults.get("selection", {})
        if isinstance(model_alpha_backtest_defaults.get("selection"), dict)
        else {}
    )
    model_alpha_position_defaults = (
        model_alpha_backtest_defaults.get("position", {})
        if isinstance(model_alpha_backtest_defaults.get("position"), dict)
        else {}
    )
    model_alpha_exit_defaults = (
        model_alpha_backtest_defaults.get("exit", {})
        if isinstance(model_alpha_backtest_defaults.get("exit"), dict)
        else {}
    )
    model_alpha_execution_defaults = (
        model_alpha_backtest_defaults.get("execution", {})
        if isinstance(model_alpha_backtest_defaults.get("execution"), dict)
        else {}
    )
    model_alpha_operational_defaults = (
        model_alpha_backtest_defaults.get("operational", {})
        if isinstance(model_alpha_backtest_defaults.get("operational"), dict)
        else {}
    )
    exec_acceptance_top_n = max(
        int(getattr(args, "execution_acceptance_top_n", None))
        if getattr(args, "execution_acceptance_top_n", None) is not None
        else top_n,
        1,
    )
    exec_acceptance_top_pct = max(
        min(
            float(getattr(args, "execution_acceptance_top_pct", None))
            if getattr(args, "execution_acceptance_top_pct", None) is not None
            else float(model_alpha_selection_defaults.get("top_pct", 0.05)),
            1.0,
        ),
        0.0,
    )
    exec_acceptance_min_prob = clamp_prob_value(
        optional_float_value(
            getattr(args, "execution_acceptance_min_prob", None)
            if getattr(args, "execution_acceptance_min_prob", None) is not None
            else model_alpha_selection_defaults.get("min_prob")
        )
    )
    exec_acceptance_min_cands = max(
        int(getattr(args, "execution_acceptance_min_cands_per_ts", None))
        if getattr(args, "execution_acceptance_min_cands_per_ts", None) is not None
        else int(model_alpha_selection_defaults.get("min_candidates_per_ts", 10)),
        0,
    )
    exec_acceptance_hold_bars = max(
        int(getattr(args, "execution_acceptance_hold_bars", None))
        if getattr(args, "execution_acceptance_hold_bars", None) is not None
        else int(model_alpha_exit_defaults.get("hold_bars", 6)),
        0,
    )
    execution_eval_start = (
        str(getattr(args, "execution_eval_start", None)).strip()
        if getattr(args, "execution_eval_start", None)
        else None
    )
    execution_eval_end = (
        str(getattr(args, "execution_eval_end", None)).strip()
        if getattr(args, "execution_eval_end", None)
        else None
    )
    execution_eval_overridden = bool(execution_eval_start and execution_eval_end)
    return TrainV4CryptoCsOptions(
        dataset_root=(Path(dataset_root_override) if dataset_root_override is not None else features_v4_config.output_dataset_root),
        registry_root=registry_root,
        logs_root=logs_root,
        model_family=model_family,
        tf=str(args.tf or defaults["tf"]).strip().lower(),
        quote=str(args.quote or defaults["quote"]).strip().upper(),
        top_n=max(top_n, 1),
        start=str(args.start or defaults["start"]).strip(),
        end=str(args.end or defaults["end"]).strip(),
        feature_set=str(args.feature_set).strip().lower(),
        label_set=str(args.label_set).strip().lower(),
        task=str(args.task or defaults["task"]).strip().lower(),
        booster_sweep_trials=int(
            args.booster_sweep_trials
            if args.booster_sweep_trials is not None
            else defaults["booster_sweep_trials"]
        ),
        seed=int(args.seed if args.seed is not None else defaults["seed"]),
        nthread=int(args.nthread if args.nthread is not None else defaults["nthread"]),
        batch_rows=max(int(defaults["batch_rows"]), 1),
        train_ratio=float(defaults["train_ratio"]),
        valid_ratio=float(defaults["valid_ratio"]),
        test_ratio=float(defaults["test_ratio"]),
        embargo_bars=max(
            int(getattr(args, "embargo_bars", None))
            if getattr(args, "embargo_bars", None) is not None
            else int(defaults["embargo_bars"]),
            0,
        ),
        fee_bps_est=float(defaults["fee_bps_est"]),
        safety_bps=float(defaults["safety_bps"]),
        ev_scan_steps=max(int(defaults["ev_scan_steps"]), 10),
        ev_min_selected=max(int(defaults["ev_min_selected"]), 1),
        min_rows_for_train=max(
            int(
                getattr(args, "min_rows_for_train", None)
                if getattr(args, "min_rows_for_train", None) is not None
                else features_v4_config.build.min_rows_for_train
            ),
            1,
        ),
        cpcv_lite_enabled=bool(getattr(args, "cpcv_lite", False)),
        cpcv_lite_group_count=max(int(getattr(args, "cpcv_lite_group_count", None) or 6), 3),
        cpcv_lite_test_group_count=max(int(getattr(args, "cpcv_lite_test_groups", None) or 2), 1),
        cpcv_lite_max_combinations=max(int(getattr(args, "cpcv_lite_max_combinations", None) or 6), 1),
        factor_block_selection_mode=str(
            getattr(args, "factor_block_selection_mode", None) or "guarded_auto"
        ).strip().lower(),
        selection_threshold_key_override=(
            str(getattr(args, "selection_threshold_key_override", None)).strip()
            if getattr(args, "selection_threshold_key_override", None)
            else None
        ),
        dependency_expert_only=bool(getattr(args, "dependency_expert_only", False)),
        live_domain_reweighting_enabled=bool(getattr(args, "live_domain_reweighting", False)),
        live_domain_reweighting_db_path=(
            Path(str(getattr(args, "live_domain_reweighting_db_path")).strip())
            if getattr(args, "live_domain_reweighting_db_path", None)
            else None
        ),
        run_scope=str(getattr(args, "run_scope", None) or "scheduled_daily").strip().lower(),
        execution_acceptance_enabled=True,
        execution_acceptance_dataset_name=backtest_dataset_name_v4,
        execution_acceptance_parquet_root=(
            Path(execution_acceptance_parquet_root_override)
            if execution_acceptance_parquet_root_override is not None
            else Path(str(backtest_defaults["parquet_root"]))
        ),
        execution_acceptance_output_root=logs_root / "train_v4_execution_backtest",
        execution_acceptance_eval_start=execution_eval_start,
        execution_acceptance_eval_end=execution_eval_end,
        execution_acceptance_eval_label=("certification" if execution_eval_overridden else "train_window"),
        execution_acceptance_eval_source=(
            "candidate_acceptance_certification_window"
            if execution_eval_overridden
            else "train_command_window"
        ),
        execution_acceptance_top_n=exec_acceptance_top_n,
        execution_acceptance_dense_grid=bool(backtest_defaults["dense_grid"]),
        execution_acceptance_starting_krw=float(backtest_defaults["starting_krw"]),
        execution_acceptance_per_trade_krw=float(backtest_defaults["per_trade_krw"]),
        execution_acceptance_max_positions=max(int(backtest_defaults["max_positions"]), 1),
        execution_acceptance_min_order_krw=max(float(backtest_defaults["min_order_krw"]), 0.0),
        execution_acceptance_order_timeout_bars=max(int(backtest_defaults["order_timeout_bars"]), 1),
        execution_acceptance_reprice_max_attempts=max(int(backtest_defaults["reprice_max_attempts"]), 0),
        execution_acceptance_reprice_tick_steps=max(int(backtest_defaults["reprice_tick_steps"]), 1),
        execution_acceptance_rules_ttl_sec=max(int(backtest_defaults["rules_ttl_sec"]), 1),
        execution_acceptance_model_alpha=ModelAlphaSettings(
            model_ref="candidate_v4",
            model_family=model_family,
            feature_set="v4",
            selection=ModelAlphaSelectionSettings(
                top_pct=exec_acceptance_top_pct,
                min_prob=exec_acceptance_min_prob,
                min_candidates_per_ts=exec_acceptance_min_cands,
                registry_threshold_key=(
                    str(model_alpha_selection_defaults.get("registry_threshold_key", "top_5pct")).strip()
                    or "top_5pct"
                ),
                use_learned_recommendations=False,
            ),
            position=ModelAlphaPositionSettings(
                max_positions_total=max(
                    int(model_alpha_position_defaults.get("max_positions_total", 3)),
                    1,
                ),
                cooldown_bars=max(int(model_alpha_position_defaults.get("cooldown_bars", 6)), 0),
                entry_min_notional_buffer_bps=max(
                    float(model_alpha_position_defaults.get("entry_min_notional_buffer_bps", 25.0)),
                    0.0,
                ),
                sizing_mode=(
                    str(model_alpha_position_defaults.get("sizing_mode", "prob_ramp")).strip().lower()
                    or "prob_ramp"
                ),
                size_multiplier_min=max(
                    float(model_alpha_position_defaults.get("size_multiplier_min", 0.5)),
                    0.0,
                ),
                size_multiplier_max=max(
                    float(model_alpha_position_defaults.get("size_multiplier_max", 1.5)),
                    max(float(model_alpha_position_defaults.get("size_multiplier_min", 0.5)), 0.0),
                ),
            ),
            exit=ModelAlphaExitSettings(
                mode=str(model_alpha_exit_defaults.get("mode", "hold")).strip().lower() or "hold",
                hold_bars=exec_acceptance_hold_bars,
                use_learned_exit_mode=False,
                use_learned_hold_bars=False,
                use_learned_risk_recommendations=False,
                use_trade_level_action_policy=False,
                risk_scaling_mode="fixed",
                risk_vol_feature=str(model_alpha_exit_defaults.get("risk_vol_feature", "rv_12")).strip()
                or "rv_12",
                tp_vol_multiplier=None,
                sl_vol_multiplier=None,
                trailing_vol_multiplier=None,
                tp_pct=max(float(model_alpha_exit_defaults.get("tp_pct", 0.02)), 0.0),
                sl_pct=max(float(model_alpha_exit_defaults.get("sl_pct", 0.01)), 0.0),
                trailing_pct=max(float(model_alpha_exit_defaults.get("trailing_pct", 0.0)), 0.0),
                expected_exit_slippage_bps=optional_float_value(
                    model_alpha_exit_defaults.get("expected_exit_slippage_bps")
                ),
                expected_exit_fee_bps=optional_float_value(
                    model_alpha_exit_defaults.get("expected_exit_fee_bps")
                ),
            ),
            execution=ModelAlphaExecutionSettings(
                price_mode=(
                    str(model_alpha_execution_defaults.get("price_mode", "JOIN")).strip().upper()
                    or "JOIN"
                ),
                timeout_bars=max(int(model_alpha_execution_defaults.get("timeout_bars", 2)), 1),
                replace_max=max(int(model_alpha_execution_defaults.get("replace_max", 2)), 0),
                use_learned_recommendations=False,
            ),
            operational=ModelAlphaOperationalSettings(
                enabled=bool(model_alpha_operational_defaults.get("enabled", True)),
                risk_multiplier_min=max(
                    float(model_alpha_operational_defaults.get("risk_multiplier_min", 0.80)),
                    0.0,
                ),
                risk_multiplier_max=max(
                    float(model_alpha_operational_defaults.get("risk_multiplier_max", 1.20)),
                    max(float(model_alpha_operational_defaults.get("risk_multiplier_min", 0.80)), 0.0),
                ),
                max_positions_scale_min=max(
                    float(model_alpha_operational_defaults.get("max_positions_scale_min", 0.50)),
                    0.10,
                ),
                max_positions_scale_max=max(
                    float(model_alpha_operational_defaults.get("max_positions_scale_max", 1.50)),
                    max(float(model_alpha_operational_defaults.get("max_positions_scale_min", 0.50)), 0.10),
                ),
                session_overlap_boost=max(
                    float(model_alpha_operational_defaults.get("session_overlap_boost", 0.10)),
                    0.0,
                ),
                session_offpeak_penalty=max(
                    float(model_alpha_operational_defaults.get("session_offpeak_penalty", 0.05)),
                    0.0,
                ),
                micro_quality_block_threshold=max(
                    float(model_alpha_operational_defaults.get("micro_quality_block_threshold", 0.15)),
                    0.0,
                ),
                micro_quality_conservative_threshold=max(
                    float(model_alpha_operational_defaults.get("micro_quality_conservative_threshold", 0.35)),
                    0.0,
                ),
                micro_quality_aggressive_threshold=max(
                    float(model_alpha_operational_defaults.get("micro_quality_aggressive_threshold", 0.75)),
                    0.0,
                ),
                max_execution_spread_bps_for_join=max(
                    float(model_alpha_operational_defaults.get("max_execution_spread_bps_for_join", 20.0)),
                    0.0,
                ),
                max_execution_spread_bps_for_cross=max(
                    float(model_alpha_operational_defaults.get("max_execution_spread_bps_for_cross", 6.0)),
                    0.0,
                ),
                min_execution_depth_krw_for_cross=max(
                    float(model_alpha_operational_defaults.get("min_execution_depth_krw_for_cross", 1_500_000.0)),
                    0.0,
                ),
                snapshot_stale_ms=max(
                    int(model_alpha_operational_defaults.get("snapshot_stale_ms", 15_000)),
                    0,
                ),
                conservative_timeout_scale=max(
                    float(model_alpha_operational_defaults.get("conservative_timeout_scale", 1.25)),
                    0.10,
                ),
                aggressive_timeout_scale=max(
                    float(model_alpha_operational_defaults.get("aggressive_timeout_scale", 0.75)),
                    0.10,
                ),
                conservative_replace_interval_scale=max(
                    float(model_alpha_operational_defaults.get("conservative_replace_interval_scale", 1.50)),
                    0.10,
                ),
                aggressive_replace_interval_scale=max(
                    float(model_alpha_operational_defaults.get("aggressive_replace_interval_scale", 0.50)),
                    0.10,
                ),
                conservative_max_replaces_scale=max(
                    float(model_alpha_operational_defaults.get("conservative_max_replaces_scale", 0.50)),
                    0.0,
                ),
                aggressive_max_replaces_bonus=max(
                    int(model_alpha_operational_defaults.get("aggressive_max_replaces_bonus", 1)),
                    0,
                ),
                conservative_max_chase_bps_scale=max(
                    float(model_alpha_operational_defaults.get("conservative_max_chase_bps_scale", 0.75)),
                    0.0,
                ),
                aggressive_max_chase_bps_bonus=max(
                    int(model_alpha_operational_defaults.get("aggressive_max_chase_bps_bonus", 5)),
                    0,
                ),
                runtime_timeout_ms_floor=max(
                    int(model_alpha_operational_defaults.get("runtime_timeout_ms_floor", 5_000)),
                    1_000,
                ),
                runtime_replace_interval_ms_floor=max(
                    int(model_alpha_operational_defaults.get("runtime_replace_interval_ms_floor", 1_500)),
                    1,
                ),
            ),
        ),
    )
