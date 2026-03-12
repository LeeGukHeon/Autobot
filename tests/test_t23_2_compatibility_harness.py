from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

import autobot.cli as cli_mod
from autobot.cli import _handle_model_command, _resolve_model_ref_alias, build_parser
from autobot.dashboard_server import _load_dashboard_asset
from autobot.models.predictor import load_predictor_from_registry
from autobot.models.registry import (
    RegistrySavePayload,
    resolve_run_dir,
    save_run,
    set_champion_pointer,
    update_latest_candidate_pointer,
)


def _write_v4_registry_run(tmp_path: Path) -> tuple[Path, Path]:
    registry_root = tmp_path / "models" / "registry"
    run_id = "run-v4-compat"
    run_dir = save_run(
        RegistrySavePayload(
            registry_root=registry_root,
            model_family="train_v4_crypto_cs",
            run_id=run_id,
            model_bundle={"model_type": "dummy", "estimator": {"coef": [1.0]}},
            metrics={"rows": {"train": 10, "valid": 5, "test": 5}},
            thresholds={"top_5pct": 0.61},
            feature_spec={"feature_columns": ["f1", "f2"]},
            label_spec={"label_columns": ["y_cls"]},
            train_config={"feature_columns": ["f1", "f2"]},
            data_fingerprint={"manifest_sha256": "abc"},
            leaderboard_row={"run_id": run_id, "test_precision_top5": 0.73},
            model_card_text="# compatibility",
            selection_recommendations={
                "recommended_threshold_key": "top_5pct",
                "by_threshold_key": {
                    "top_5pct": {
                        "recommended_top_pct": 0.5,
                        "recommended_min_candidates_per_ts": 1,
                        "eligible_ratio": 0.05,
                        "recommendation_source": "optimizer",
                    }
                },
            },
            selection_policy={
                "version": 1,
                "mode": "rank_effective_quantile",
                "selection_fraction": 0.025,
                "min_candidates_per_ts": 1,
                "threshold_key": "top_5pct",
                "recommended_top_pct": 0.5,
                "eligible_ratio": 0.05,
                "selection_recommendation_source": "optimizer",
            },
            selection_calibration={
                "version": 1,
                "mode": "identity_v1",
                "reason": "OK",
            },
            runtime_recommendations={
                "exit": {
                    "mode": "hold",
                    "recommended_hold_bars": 6,
                    "recommendation_source": "execution_backtest_grid_search",
                    "objective_score": 0.72,
                    "risk_objective_score": 0.0,
                    "summary": {
                        "orders_filled": 10,
                        "realized_pnl_quote": 120.0,
                        "fill_rate": 0.95,
                        "max_drawdown_pct": 1.1,
                        "slippage_bps_mean": 2.0,
                    },
                    "risk_summary": {
                        "orders_filled": 10,
                        "realized_pnl_quote": 90.0,
                        "fill_rate": 0.90,
                        "max_drawdown_pct": 1.8,
                        "slippage_bps_mean": 3.5,
                    },
                    "grid_point": {"hold_bars": 6},
                    "risk_grid_point": {
                        "risk_scaling_mode": "volatility_scaled",
                        "risk_vol_feature": "rv_12",
                        "tp_vol_multiplier": 1.5,
                        "sl_vol_multiplier": 1.0,
                        "trailing_vol_multiplier": 0.0,
                    },
                }
            },
        )
    )
    set_champion_pointer(
        registry_root,
        "train_v4_crypto_cs",
        run_id=run_id,
        score=0.73,
        score_key="test_precision_top5",
    )
    update_latest_candidate_pointer(registry_root, "train_v4_crypto_cs", run_id)
    return registry_root, run_dir


@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        (["data", "ingest"], {"command": "data", "data_command": "ingest"}),
        (["collect", "plan-candles"], {"command": "collect", "collect_command": "plan-candles"}),
        (
            ["micro", "aggregate", "--start", "2026-03-01", "--end", "2026-03-02"],
            {"command": "micro", "micro_command": "aggregate"},
        ),
        (["features", "build", "--tf", "5m"], {"command": "features", "features_command": "build"}),
        (
            ["model", "train", "--trainer", "v4_crypto_cs"],
            {"command": "model", "model_command": "train", "trainer": "v4_crypto_cs"},
        ),
        (
            ["model", "daily-v4"],
            {"command": "model", "model_command": "daily-v4", "mode": "spawn_only"},
        ),
        (
            ["paper", "alpha", "--duration-sec", "60"],
            {"command": "paper", "paper_command": "alpha", "preset": "live_v4"},
        ),
        (
            ["backtest", "run", "--start", "2026-03-01", "--end", "2026-03-02"],
            {"command": "backtest", "backtest_command": "run"},
        ),
        (["live", "run"], {"command": "live", "live_command": "run", "duration_sec": 0}),
        (["exec", "ping"], {"command": "exec", "exec_command": "ping"}),
    ],
)
def test_t23_2_protected_cli_surface_still_parses(argv: list[str], expected: dict[str, object]) -> None:
    parser = build_parser()

    args = parser.parse_args(argv)

    for key, value in expected.items():
        assert getattr(args, key) == value


def test_t23_2_v4_aliases_resolve_to_frozen_pointer_meanings(tmp_path: Path) -> None:
    registry_root, run_dir = _write_v4_registry_run(tmp_path)

    for alias in ("latest_v4", "champion_v4", "latest_candidate_v4"):
        resolved_ref, resolved_family = _resolve_model_ref_alias(alias)
        assert resolved_family == "train_v4_crypto_cs"
        assert resolve_run_dir(
            registry_root=registry_root,
            model_ref=resolved_ref,
            model_family=resolved_family,
        ) == run_dir


def test_t23_2_predictor_loads_frozen_runtime_artifact_filenames(tmp_path: Path) -> None:
    registry_root, run_dir = _write_v4_registry_run(tmp_path)
    resolved_ref, resolved_family = _resolve_model_ref_alias("champion_v4")

    predictor = load_predictor_from_registry(
        registry_root=registry_root,
        model_ref=resolved_ref,
        model_family=resolved_family,
    )

    for filename in (
        "train_config.yaml",
        "thresholds.json",
        "selection_recommendations.json",
        "selection_policy.json",
        "selection_calibration.json",
        "runtime_recommendations.json",
    ):
        assert (run_dir / filename).exists()
    assert predictor.run_dir == run_dir
    assert predictor.feature_columns == ("f1", "f2")
    assert predictor.thresholds["top_5pct"] == 0.61
    assert predictor.selection_recommendations["recommended_threshold_key"] == "top_5pct"
    assert predictor.selection_policy["mode"] == "rank_effective_quantile"
    assert predictor.selection_calibration["mode"] == "identity_v1"
    assert predictor.runtime_recommendations["exit"]["recommended_exit_mode"] == "hold"
    assert predictor.runtime_recommendations["exit"]["contract_status"] == "backfilled"


def test_t23_2_model_daily_v4_non_spawn_only_returns_exit_code_2(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    parser = build_parser()
    args = parser.parse_args(["model", "daily-v4", "--mode", "combined"])

    monkeypatch.setattr(
        cli_mod,
        "load_train_defaults",
        lambda config_dir, base_config: {
            "registry_root": str(tmp_path / "registry"),
            "logs_root": str(tmp_path / "logs"),
            "top_n": 50,
        },
    )
    monkeypatch.setattr(cli_mod, "_load_yaml_doc", lambda path: {})
    monkeypatch.setattr(cli_mod, "_backtest_defaults", lambda **kwargs: {})
    monkeypatch.setattr(cli_mod, "load_features_config", lambda *args, **kwargs: SimpleNamespace())
    monkeypatch.setattr(cli_mod, "load_features_v2_config", lambda *args, **kwargs: SimpleNamespace())
    monkeypatch.setattr(cli_mod, "load_features_v3_config", lambda *args, **kwargs: SimpleNamespace())
    monkeypatch.setattr(cli_mod, "load_features_v4_config", lambda *args, **kwargs: SimpleNamespace())

    exit_code = _handle_model_command(args, tmp_path / "config", {})

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "spawn_only" in captured.out


def test_t23_2_dashboard_risk_plan_percent_units_stay_unscaled() -> None:
    js = str(_load_dashboard_asset("dashboard.js"))

    assert 'fmtPct(Number(plan.tp_pct))' in js
    assert 'fmtPct(Number(plan.sl_pct))' in js
    assert 'fmtPct(Number(plan.trail_pct))' in js
    assert 'fmtPct(Number(plan.tp_pct) * 100)' not in js
    assert 'fmtPct(Number(plan.sl_pct) * 100)' not in js
    assert 'fmtPct(Number(plan.trail_pct) * 100)' not in js
