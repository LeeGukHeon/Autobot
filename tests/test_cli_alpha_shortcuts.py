from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import autobot.cli as cli_mod
from autobot.backtest import BacktestRunSummary
from autobot.cli import (
    _handle_backtest_command,
    _normalize_backtest_alpha_args,
    _handle_model_command,
    _normalize_paper_alpha_args,
    _resolve_paper_runtime_env_model_overrides,
    _resolve_model_ref_alias,
    _resolve_v4_runtime_model_ref_fallback,
    build_parser,
)


def test_build_parser_supports_paper_alpha_shortcut() -> None:
    parser = build_parser()
    args = parser.parse_args(["paper", "alpha", "--duration-sec", "900", "--model-ref", "champion_v3"])
    assert args.command == "paper"
    assert args.paper_command == "alpha"
    assert args.duration_sec == 900
    assert args.model_ref == "champion_v3"
    assert args.preset == "live_v5"


def test_build_parser_accepts_live_v5_paper_feature_provider_for_paper_run() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "paper",
            "run",
            "--duration-sec",
            "60",
            "--strategy",
            "model_alpha_v1",
            "--paper-feature-provider",
            "live_v5",
        ]
    )

    assert args.command == "paper"
    assert args.paper_command == "run"
    assert args.paper_feature_provider == "live_v5"


def test_normalize_paper_alpha_args_defaults_to_live_v5() -> None:
    args = argparse.Namespace(
        paper_command="alpha",
        preset="live_v5",
        duration_sec=600,
        quote=None,
        top_n=None,
        tf=None,
        model_ref=None,
        model_family=None,
        feature_set=None,
        top_pct=None,
        min_prob=None,
        min_cands_per_ts=None,
        max_positions_total=None,
        cooldown_bars=None,
        exit_mode=None,
        hold_bars=None,
        tp_pct=None,
        sl_pct=None,
        trailing_pct=None,
        execution_price_mode=None,
        execution_timeout_bars=None,
        execution_replace_max=None,
        print_every_sec=None,
        starting_krw=None,
        per_trade_krw=None,
        max_positions=None,
        paper_micro_provider=None,
        paper_micro_warmup_sec=None,
        paper_micro_warmup_min_trade_events_per_market=None,
        paper_feature_provider=None,
    )
    normalized = _normalize_paper_alpha_args(args)
    assert normalized.model_ref == "champion"
    assert normalized.model_family == "train_v5_fusion"
    assert normalized.feature_set == "v4"
    assert normalized.evaluation_contract_id == "runtime_deploy_contract_v1"
    assert normalized.selection_policy_mode == "auto"


def test_normalize_paper_alpha_args_uses_live_v3_preset_defaults() -> None:
    args = argparse.Namespace(
        paper_command="alpha",
        preset="live_v3",
        duration_sec=600,
        quote=None,
        top_n=None,
        tf=None,
        model_ref=None,
        model_family=None,
        feature_set=None,
        top_pct=None,
        min_prob=None,
        min_cands_per_ts=None,
        max_positions_total=None,
        cooldown_bars=None,
        exit_mode=None,
        hold_bars=None,
        tp_pct=None,
        sl_pct=None,
        trailing_pct=None,
        execution_price_mode=None,
        execution_timeout_bars=None,
        execution_replace_max=None,
        print_every_sec=None,
        starting_krw=None,
        per_trade_krw=None,
        max_positions=None,
        paper_micro_provider=None,
        paper_micro_warmup_sec=None,
        paper_micro_warmup_min_trade_events_per_market=None,
        paper_feature_provider=None,
    )
    normalized = _normalize_paper_alpha_args(args)
    assert normalized.paper_command == "run"
    assert normalized.strategy == "model_alpha_v1"
    assert normalized.feature_set == "v3"
    assert normalized.model_ref == "champion_v3"
    assert normalized.model_family == "train_v3_mtf_micro"
    assert normalized.top_pct == 0.10
    assert normalized.min_prob is None
    assert normalized.min_cands_per_ts == 3
    assert normalized.use_learned_selection_recommendations is True
    assert normalized.paper_feature_provider == "live_v3"
    assert normalized.paper_micro_provider == "live_ws"
    assert normalized.micro_order_policy == "on"
    assert normalized.micro_order_policy_mode == "trade_only"
    assert normalized.micro_order_policy_on_missing == "static_fallback"


def test_normalize_paper_alpha_args_uses_live_v4_preset_defaults() -> None:
    args = argparse.Namespace(
        paper_command="alpha",
        preset="live_v4",
        duration_sec=600,
        quote=None,
        top_n=None,
        tf=None,
        model_ref=None,
        model_family=None,
        feature_set=None,
        top_pct=None,
        min_prob=None,
        min_cands_per_ts=None,
        max_positions_total=None,
        cooldown_bars=None,
        exit_mode=None,
        hold_bars=None,
        tp_pct=None,
        sl_pct=None,
        trailing_pct=None,
        execution_price_mode=None,
        execution_timeout_bars=None,
        execution_replace_max=None,
        print_every_sec=None,
        starting_krw=None,
        per_trade_krw=None,
        max_positions=None,
        paper_micro_provider=None,
        paper_micro_warmup_sec=None,
        paper_micro_warmup_min_trade_events_per_market=None,
        paper_feature_provider=None,
    )
    normalized = _normalize_paper_alpha_args(args)
    assert normalized.paper_command == "run"
    assert normalized.strategy == "model_alpha_v1"
    assert normalized.feature_set == "v4"
    assert normalized.model_ref == "champion_v4"
    assert normalized.model_family == "train_v4_crypto_cs"
    assert normalized.top_pct == 0.50
    assert normalized.min_prob is None
    assert normalized.min_cands_per_ts == 1
    assert normalized.use_learned_selection_recommendations is True
    assert normalized.paper_feature_provider == "live_v4"
    assert normalized.paper_micro_provider == "live_ws"


def test_normalize_paper_alpha_args_uses_live_v4_native_preset_defaults() -> None:
    args = argparse.Namespace(
        paper_command="alpha",
        preset="live_v4_native",
        duration_sec=600,
        quote=None,
        top_n=None,
        tf=None,
        model_ref=None,
        model_family=None,
        feature_set=None,
        top_pct=None,
        min_prob=None,
        min_cands_per_ts=None,
        max_positions_total=None,
        cooldown_bars=None,
        exit_mode=None,
        hold_bars=None,
        tp_pct=None,
        sl_pct=None,
        trailing_pct=None,
        execution_price_mode=None,
        execution_timeout_bars=None,
        execution_replace_max=None,
        print_every_sec=None,
        starting_krw=None,
        per_trade_krw=None,
        max_positions=None,
        paper_micro_provider=None,
        paper_micro_warmup_sec=None,
        paper_micro_warmup_min_trade_events_per_market=None,
        paper_feature_provider=None,
    )
    normalized = _normalize_paper_alpha_args(args)
    assert normalized.paper_command == "run"
    assert normalized.strategy == "model_alpha_v1"
    assert normalized.feature_set == "v4"
    assert normalized.model_ref == "champion_v4"
    assert normalized.model_family == "train_v4_crypto_cs"
    assert normalized.paper_feature_provider == "live_v4_native"
    assert normalized.paper_micro_provider == "live_ws"


def test_resolve_paper_runtime_env_model_overrides_uses_pinned_ref_from_env(monkeypatch) -> None:
    monkeypatch.setenv("AUTOBOT_PAPER_MODEL_REF_PINNED", "20260320T180256Z-s42-8b956b2f")
    monkeypatch.setenv("AUTOBOT_RUNTIME_MODEL_FAMILY", "train_v4_crypto_cs")

    model_ref, model_family = _resolve_paper_runtime_env_model_overrides("", None)

    assert model_ref == "20260320T180256Z-s42-8b956b2f"
    assert model_family == "train_v4_crypto_cs"


def test_resolve_paper_runtime_env_model_overrides_overrides_preset_ref(monkeypatch) -> None:
    monkeypatch.setenv("AUTOBOT_PAPER_MODEL_REF_PINNED", "20260320T180256Z-s42-8b956b2f")
    monkeypatch.setenv("AUTOBOT_RUNTIME_MODEL_FAMILY", "train_v4_crypto_cs")

    model_ref, model_family = _resolve_paper_runtime_env_model_overrides("champion_v4", "train_v4_crypto_cs")

    assert model_ref == "20260320T180256Z-s42-8b956b2f"
    assert model_family == "train_v4_crypto_cs"


def test_resolve_v4_runtime_model_ref_fallback_uses_latest_candidate_when_champion_missing(tmp_path: Path) -> None:
    registry_root = tmp_path / "registry"
    family_dir = registry_root / "train_v4_crypto_cs"
    family_dir.mkdir(parents=True)
    (family_dir / "latest_candidate.json").write_text(
        '{"run_id":"run-v4-candidate","model_family":"train_v4_crypto_cs"}\n',
        encoding="utf-8",
    )

    resolved_ref, resolved_family, warning = _resolve_v4_runtime_model_ref_fallback(
        "champion",
        "train_v4_crypto_cs",
        registry_root,
    )

    assert resolved_ref == "latest_candidate"
    assert resolved_family == "train_v4_crypto_cs"
    assert warning is not None
    assert "latest_candidate_v4" in warning


def test_resolve_v4_runtime_model_ref_fallback_preserves_existing_champion(tmp_path: Path) -> None:
    registry_root = tmp_path / "registry"
    family_dir = registry_root / "train_v4_crypto_cs"
    family_dir.mkdir(parents=True)
    (family_dir / "champion.json").write_text(
        '{"run_id":"run-v4-champion","model_family":"train_v4_crypto_cs"}\n',
        encoding="utf-8",
    )

    resolved_ref, resolved_family, warning = _resolve_v4_runtime_model_ref_fallback(
        "champion",
        "train_v4_crypto_cs",
        registry_root,
    )

    assert resolved_ref == "champion"
    assert resolved_family == "train_v4_crypto_cs"
    assert warning is None


def test_resolve_model_ref_alias_keeps_frozen_v4_pointer_contracts() -> None:
    assert _resolve_model_ref_alias("champion_v4") == ("champion", "train_v4_crypto_cs")
    assert _resolve_model_ref_alias("latest_v4") == ("latest", "train_v4_crypto_cs")
    assert _resolve_model_ref_alias("candidate_v4") == ("latest_candidate", "train_v4_crypto_cs")


def test_build_parser_supports_backtest_alpha_shortcut() -> None:
    parser = build_parser()
    args = parser.parse_args(["backtest", "alpha", "--start", "2026-03-01", "--end", "2026-03-05"])
    assert args.command == "backtest"
    assert args.backtest_command == "alpha"
    assert args.start == "2026-03-01"
    assert args.end == "2026-03-05"
    assert args.preset == "default"


def test_build_parser_supports_v4_selection_threshold_override() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "model",
            "train",
            "--trainer",
            "v4_crypto_cs",
            "--feature-set",
            "v4",
            "--label-set",
            "v2",
            "--selection-threshold-key-override",
            "ev_opt",
        ]
    )

    assert args.command == "model"
    assert args.model_command == "train"
    assert args.selection_threshold_key_override == "ev_opt"


def test_build_parser_supports_v4_label_v3_choice() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "model",
            "train",
            "--trainer",
            "v4_crypto_cs",
            "--feature-set",
            "v4",
            "--label-set",
            "v3",
        ]
    )

    assert args.command == "model"
    assert args.model_command == "train"
    assert args.label_set == "v3"


def test_build_parser_supports_v5_panel_ensemble_trainer_choice() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "model",
            "train",
            "--trainer",
            "v5_panel_ensemble",
            "--feature-set",
            "v4",
            "--label-set",
            "v3",
        ]
    )

    assert args.command == "model"
    assert args.model_command == "train"
    assert args.trainer == "v5_panel_ensemble"
    assert args.label_set == "v3"


def test_build_parser_supports_v5_sequence_trainer_choice() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "model",
            "train",
            "--trainer",
            "v5_sequence",
            "--sequence-backbone",
            "patchtst_v1",
            "--sequence-pretrain-method",
            "ts2vec_v1",
        ]
    )

    assert args.command == "model"
    assert args.model_command == "train"
    assert args.trainer == "v5_sequence"
    assert args.sequence_backbone == "patchtst_v1"


def test_build_parser_supports_v5_lob_trainer_choice() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "model",
            "train",
            "--trainer",
            "v5_lob",
            "--lob-backbone",
            "deeplob_v1",
        ]
    )

    assert args.command == "model"
    assert args.model_command == "train"
    assert args.trainer == "v5_lob"
    assert args.lob_backbone == "deeplob_v1"


def test_build_parser_supports_v5_fusion_trainer_choice() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "model",
            "train",
            "--trainer",
            "v5_fusion",
            "--fusion-panel-input",
            "panel.parquet",
            "--fusion-sequence-input",
            "sequence.parquet",
            "--fusion-lob-input",
            "lob.parquet",
        ]
    )

    assert args.command == "model"
    assert args.model_command == "train"
    assert args.trainer == "v5_fusion"
    assert args.fusion_panel_input == "panel.parquet"


def test_normalize_backtest_alpha_args_acceptance_disables_micro_policy() -> None:
    args = argparse.Namespace(
        backtest_command="alpha",
        preset="acceptance",
        dataset_name=None,
        parquet_root=None,
        tf=None,
        market=None,
        markets=None,
        quote=None,
        top_n=None,
        universe_mode=None,
        model_ref=None,
        model_family=None,
        feature_set=None,
        top_pct=None,
        min_prob=None,
        min_cands_per_ts=None,
        exit_mode=None,
        hold_bars=None,
        tp_pct=None,
        sl_pct=None,
        trailing_pct=None,
        cooldown_bars=None,
        max_positions_total=None,
        execution_price_mode=None,
        execution_timeout_bars=None,
        execution_replace_max=None,
        start=None,
        end=None,
        from_ts_ms=None,
        to_ts_ms=None,
        days=7,
        dense_grid=False,
        starting_krw=None,
        per_trade_krw=None,
        max_positions=None,
    )
    normalized = _normalize_backtest_alpha_args(args)
    assert normalized.backtest_command == "run"
    assert normalized.strategy == "model_alpha_v1"
    assert normalized.entry == "top_pct"
    assert normalized.feature_set == "v3"
    assert normalized.duration_days == 7
    assert normalized.micro_order_policy == "off"
    assert normalized.use_learned_selection_recommendations is False
    assert normalized.use_trade_level_action_policy is False
    assert normalized.evaluation_contract_id == "acceptance_frozen_compare_v1"
    assert normalized.selection_policy_mode == "raw_threshold"


def test_normalize_backtest_alpha_args_runtime_parity_enables_learned_runtime_contract() -> None:
    args = argparse.Namespace(
        backtest_command="alpha",
        preset="runtime_parity",
        dataset_name=None,
        parquet_root=None,
        tf=None,
        market=None,
        markets=None,
        quote=None,
        top_n=None,
        universe_mode=None,
        model_ref=None,
        model_family=None,
        feature_set=None,
        top_pct=None,
        min_prob=None,
        min_cands_per_ts=None,
        exit_mode=None,
        hold_bars=None,
        tp_pct=None,
        sl_pct=None,
        trailing_pct=None,
        cooldown_bars=None,
        max_positions_total=None,
        execution_price_mode=None,
        execution_timeout_bars=None,
        execution_replace_max=None,
        start=None,
        end=None,
        from_ts_ms=None,
        to_ts_ms=None,
        days=7,
        dense_grid=False,
        starting_krw=None,
        per_trade_krw=None,
        max_positions=None,
    )
    normalized = _normalize_backtest_alpha_args(args)
    assert normalized.backtest_command == "run"
    assert normalized.strategy == "model_alpha_v1"
    assert normalized.preset == "runtime_parity"
    assert normalized.use_learned_selection_recommendations is True
    assert normalized.use_learned_exit_mode is True
    assert normalized.use_learned_hold_bars is True
    assert normalized.use_learned_risk_recommendations is True
    assert normalized.use_trade_level_action_policy is True
    assert normalized.use_learned_execution_recommendations is True
    assert normalized.evaluation_contract_id == "runtime_deploy_contract_v1"
    assert normalized.selection_policy_mode == "auto"


def test_handle_model_command_v4_train_uses_yaml_doc_loader(monkeypatch, tmp_path: Path) -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "model",
            "train",
            "--trainer",
            "v4_crypto_cs",
            "--feature-set",
            "v4",
            "--label-set",
            "v2",
            "--task",
            "cls",
            "--tf",
            "5m",
            "--quote",
            "KRW",
            "--top-n",
            "50",
            "--execution-acceptance-top-n",
            "20",
            "--execution-acceptance-top-pct",
            "0.5",
            "--execution-acceptance-min-prob",
            "0.0",
            "--execution-acceptance-min-cands-per-ts",
            "1",
            "--execution-acceptance-hold-bars",
            "6",
            "--start",
            "2026-03-03",
            "--end",
            "2026-03-06",
        ]
    )

    monkeypatch.setattr(
        cli_mod,
        "load_train_defaults",
        lambda config_dir, base_config: {
            "registry_root": str(tmp_path / "registry"),
            "logs_root": str(tmp_path / "logs"),
            "top_n": 50,
            "tf": "5m",
            "quote": "KRW",
            "start": "2026-03-03",
            "end": "2026-03-06",
            "task": "cls",
            "booster_sweep_trials": 1,
            "seed": 42,
            "nthread": 1,
            "batch_rows": 256,
            "train_ratio": 0.6,
            "valid_ratio": 0.2,
            "test_ratio": 0.2,
            "embargo_bars": 0,
            "fee_bps_est": 10.0,
            "safety_bps": 5.0,
            "ev_scan_steps": 10,
            "ev_min_selected": 1,
        },
    )
    monkeypatch.setattr(cli_mod, "_load_yaml_doc", lambda path: {})
    monkeypatch.setattr(
        cli_mod,
        "_backtest_defaults",
        lambda **kwargs: {
            "parquet_root": "data/parquet",
            "dataset_name": "candles_v1",
            "dense_grid": False,
            "starting_krw": 50000.0,
            "per_trade_krw": 10000.0,
            "max_positions": 2,
            "min_order_krw": 5000.0,
            "order_timeout_bars": 5,
            "reprice_max_attempts": 1,
            "reprice_tick_steps": 1,
            "rules_ttl_sec": 86400,
            "model_alpha": {},
        },
    )
    monkeypatch.setattr(cli_mod, "load_features_config", lambda *args, **kwargs: SimpleNamespace())
    monkeypatch.setattr(cli_mod, "load_features_v2_config", lambda *args, **kwargs: SimpleNamespace())
    monkeypatch.setattr(cli_mod, "load_features_v3_config", lambda *args, **kwargs: SimpleNamespace())
    monkeypatch.setattr(
        cli_mod,
        "load_features_v4_config",
        lambda *args, **kwargs: SimpleNamespace(
            build=SimpleNamespace(base_candles_dataset="candles_api_v1", min_rows_for_train=1),
            output_dataset_root=tmp_path / "features_v4",
        ),
    )
    monkeypatch.setattr(cli_mod, "_resolve_backtest_dataset_name_for_model_features", lambda **kwargs: "candles_v1")
    captured: dict[str, object] = {}

    def _fake_train_and_register_v4(options):
        captured["options"] = options
        return SimpleNamespace(
            run_id="run-v4-test",
            status="candidate",
            leaderboard_row={"test_precision_top5": 0.12, "test_pr_auc": 0.34},
            run_dir=tmp_path / "registry" / "train_v4_crypto_cs" / "run-v4-test",
            train_report_path=tmp_path / "logs" / "train_report.json",
            walk_forward_report_path=tmp_path / "logs" / "walk_forward.json",
            cpcv_lite_report_path=None,
            factor_block_selection_path=None,
            factor_block_policy_path=None,
            search_budget_decision_path=None,
            live_domain_reweighting_path=None,
            execution_acceptance_report_path=tmp_path / "logs" / "execution_acceptance.json",
            promotion_path=tmp_path / "logs" / "promotion.json",
        )

    monkeypatch.setattr(cli_mod, "train_and_register_v4_crypto_cs", _fake_train_and_register_v4)

    assert _handle_model_command(args, tmp_path / "config", {}) == 0
    options = captured["options"]
    assert options.execution_acceptance_top_n == 20
    assert options.execution_acceptance_model_alpha.selection.top_pct == 0.5
    assert options.execution_acceptance_model_alpha.selection.min_prob == 0.0
    assert options.execution_acceptance_model_alpha.selection.min_candidates_per_ts == 1
    assert options.execution_acceptance_model_alpha.selection.use_learned_recommendations is False
    assert options.execution_acceptance_model_alpha.exit.hold_bars == 6
    assert options.execution_acceptance_model_alpha.exit.use_learned_exit_mode is False
    assert options.execution_acceptance_model_alpha.exit.use_learned_hold_bars is False
    assert options.execution_acceptance_model_alpha.exit.use_trade_level_action_policy is False
    assert options.execution_acceptance_model_alpha.execution.use_learned_recommendations is False


def test_manual_v4_daily_pipeline_treats_bootstrap_only_rejection_as_success(monkeypatch, tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    config_dir = project_root / "config"
    wrapper_script = project_root / "scripts" / "v4_scout_candidate_acceptance.ps1"
    wrapper_script.parent.mkdir(parents=True, exist_ok=True)
    wrapper_script.write_text("# noop\n", encoding="utf-8")
    latest_dir = project_root / "logs" / "model_v4_acceptance_manual"

    def _fake_run(command, cwd=None, text=None):  # noqa: ANN001
        out_dir = latest_dir
        out_dir.mkdir(parents=True, exist_ok=True)
        report_path = out_dir / "run-v4-report.json"
        report_path.write_text(
            json.dumps(
                {
                    "reasons": ["BOOTSTRAP_ONLY_POLICY"],
                    "candidate": {
                        "lane_mode": "bootstrap_latest_inclusive",
                        "promotion_eligible": False,
                    },
                    "split_policy": {
                        "lane_mode": "bootstrap_latest_inclusive",
                        "promotion_eligible": False,
                    },
                    "gates": {"overall_pass": False},
                }
            ),
            encoding="utf-8",
        )
        (out_dir / "latest.json").write_text(report_path.read_text(encoding="utf-8"), encoding="utf-8")
        return subprocess.CompletedProcess(command, 2, stdout=f"[v4-accept] report={report_path}\n", stderr="")

    monkeypatch.setattr(cli_mod, "_resolve_powershell_exe", lambda: "pwsh")
    monkeypatch.setattr(cli_mod.subprocess, "run", _fake_run)

    exit_code = cli_mod._run_manual_v4_daily_pipeline(
        argparse.Namespace(
            mode="spawn_only",
            lane="cls_scout",
            batch_date="2026-03-08",
            run_paper_soak=False,
            paper_soak_duration_sec=0,
            dry_run=False,
        ),
        config_dir,
    )

    assert exit_code == 0


def test_handle_backtest_command_v4_resolves_base_candles_dataset(monkeypatch, tmp_path: Path) -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "backtest",
            "alpha",
            "--preset",
            "acceptance",
            "--model-ref",
            "latest_candidate_v4",
            "--model-family",
            "train_v4_crypto_cs",
            "--feature-set",
            "v4",
            "--tf",
            "5m",
            "--quote",
            "KRW",
            "--top-n",
            "20",
            "--start",
            "2026-03-03",
            "--end",
            "2026-03-06",
        ]
    )

    captured: dict[str, object] = {}

    monkeypatch.setattr(cli_mod, "_load_yaml_doc", lambda path: {})
    monkeypatch.setattr(
        cli_mod,
        "_backtest_defaults",
        lambda **kwargs: {
            "dataset_name": "candles_v1",
            "parquet_root": "data/parquet",
            "tf": "5m",
            "from_ts_ms": None,
            "to_ts_ms": None,
            "duration_days": None,
            "strategy": "model_alpha_v1",
            "model_ref": "latest_candidate_v4",
            "model_family": "train_v4_crypto_cs",
            "feature_set": "v4",
            "max_positions": 2,
            "order_timeout_bars": 5,
            "reprice_max_attempts": 1,
            "universe_mode": "static_start",
            "quote": "KRW",
            "top_n": 20,
            "dense_grid": False,
            "starting_krw": 50000.0,
            "per_trade_krw": 10000.0,
            "min_order_krw": 5000.0,
            "reprice_tick_steps": 1,
            "rules_ttl_sec": 86400,
            "momentum_window_sec": 60,
            "min_momentum_pct": 0.2,
            "backtest_out_dir": str(tmp_path / "runs"),
            "seed": 0,
            "micro_gate": {},
            "micro_order_policy": {},
            "model_alpha": {},
            "model_registry_root": "models/registry",
            "model_feature_dataset_root": None,
        },
    )
    monkeypatch.setattr(
        cli_mod,
        "load_features_v4_config",
        lambda *args, **kwargs: SimpleNamespace(
            build=SimpleNamespace(base_candles_dataset="candles_api_v1"),
        ),
    )
    monkeypatch.setattr(cli_mod, "_resolve_backtest_dataset_name_for_model_features", lambda **kwargs: "candles_api_v1")
    monkeypatch.setattr(cli_mod, "load_upbit_settings", lambda config_dir: object())
    monkeypatch.setattr(cli_mod, "_ensure_upbit_runtime_available", lambda: None)
    monkeypatch.setattr(cli_mod, "_print_json", lambda payload: None)
    monkeypatch.setattr(
        cli_mod,
        "run_backtest_sync",
        lambda run_settings, upbit_settings=None: (
            captured.setdefault("run_settings", run_settings),
            BacktestRunSummary(
                run_id="bt-v4",
                run_dir=str(tmp_path / "runs" / "bt-v4"),
                tf="5m",
                from_ts_ms=0,
                to_ts_ms=0,
                bars_processed=0,
                markets=["KRW-BTC"],
                orders_submitted=0,
                orders_filled=0,
                orders_canceled=0,
                intents_failed=0,
                candidates_total=0,
                candidates_blocked_by_micro=0,
                candidates_aborted_by_policy=0,
                micro_blocked_ratio=0.0,
                micro_blocked_reasons={},
                replaces_total=0,
                cancels_total=0,
                aborted_timeout_total=0,
                dust_abort_total=0,
                avg_time_to_fill_ms=0.0,
                p50_time_to_fill_ms=0.0,
                p90_time_to_fill_ms=0.0,
                slippage_bps_mean=0.0,
                slippage_bps_p50=0.0,
                slippage_bps_p90=0.0,
                fill_ratio=0.0,
                fill_rate=0.0,
                realized_pnl_quote=0.0,
                unrealized_pnl_quote=0.0,
                max_drawdown_pct=0.0,
                win_rate=0.0,
            ),
        )[1],
    )

    assert _handle_backtest_command(args, tmp_path / "config", {}) == 0
    assert str(captured["run_settings"].dataset_name) == "candles_api_v1"
    assert captured["run_settings"].model_alpha.selection.use_learned_recommendations is False
    assert captured["run_settings"].model_alpha.exit.use_trade_level_action_policy is False
