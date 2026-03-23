from __future__ import annotations

from types import SimpleNamespace
from pathlib import Path

from autobot.features.feature_spec import parse_date_to_ts_ms
from autobot.models.train_v4_execution import (
    build_runtime_recommendations_v4,
    run_execution_acceptance_v4,
)
from autobot.strategy.model_alpha_v1 import (
    ModelAlphaExecutionSettings,
    ModelAlphaExitSettings,
    ModelAlphaSelectionSettings,
    ModelAlphaSettings,
)


def _options(tmp_path: Path) -> SimpleNamespace:
    return SimpleNamespace(
        registry_root=tmp_path / "registry",
        model_family="train_v4_crypto_cs",
        execution_acceptance_enabled=True,
        execution_acceptance_model_alpha=ModelAlphaSettings(
            model_ref="candidate_v4",
            model_family="train_v4_crypto_cs",
            feature_set="v4",
            selection=ModelAlphaSelectionSettings(use_learned_recommendations=False),
            exit=ModelAlphaExitSettings(use_learned_exit_mode=False, use_learned_hold_bars=False),
            execution=ModelAlphaExecutionSettings(use_learned_recommendations=False),
        ),
        execution_acceptance_parquet_root=tmp_path / "parquet",
        execution_acceptance_dataset_name="candles_v1",
        execution_acceptance_output_root=tmp_path / "backtest",
        execution_acceptance_top_n=20,
        execution_acceptance_dense_grid=False,
        execution_acceptance_starting_krw=50_000.0,
        execution_acceptance_per_trade_krw=10_000.0,
        execution_acceptance_max_positions=2,
        execution_acceptance_min_order_krw=5_000.0,
        execution_acceptance_order_timeout_bars=5,
        execution_acceptance_reprice_max_attempts=1,
        execution_acceptance_reprice_tick_steps=1,
        execution_acceptance_rules_ttl_sec=86_400,
        execution_acceptance_eval_start="2026-03-21",
        execution_acceptance_eval_end="2026-03-22",
        execution_acceptance_eval_label="certification",
        execution_acceptance_eval_source="candidate_acceptance_certification_window",
        top_n=20,
        tf="5m",
        quote="KRW",
        start="2026-03-04",
        end="2026-03-20",
        feature_set="v4",
    )


def test_run_execution_acceptance_v4_uses_explicit_evaluation_window(tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    def _fake_run_execution_acceptance_fn(execution_options):
        captured["start_ts_ms"] = execution_options.start_ts_ms
        captured["end_ts_ms"] = execution_options.end_ts_ms
        captured["label"] = execution_options.evaluation_window_label
        captured["source"] = execution_options.evaluation_window_source
        return {"status": "compared"}

    report = run_execution_acceptance_v4(
        options=_options(tmp_path),
        run_id="run-001",
        resolve_v4_execution_compare_contract_fn=lambda: {"policy": "paired_sortino_lpm_execution_v1"},
        run_execution_acceptance_fn=_fake_run_execution_acceptance_fn,
    )

    assert report == {"status": "compared"}
    assert captured["start_ts_ms"] == parse_date_to_ts_ms("2026-03-21")
    assert captured["end_ts_ms"] == parse_date_to_ts_ms("2026-03-22", end_of_day=True)
    assert captured["label"] == "certification"
    assert captured["source"] == "candidate_acceptance_certification_window"


def test_build_runtime_recommendations_v4_uses_explicit_evaluation_window(tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    def _fake_optimize_runtime_recommendations_fn(*, options, candidate_ref, grid):
        captured["start_ts_ms"] = options.start_ts_ms
        captured["end_ts_ms"] = options.end_ts_ms
        captured["label"] = options.evaluation_window_label
        captured["source"] = options.evaluation_window_source
        captured["candidate_ref"] = candidate_ref
        captured["grid"] = grid
        return {
            "version": 1,
            "status": "ready",
            "evaluation_window": {
                "start_ts_ms": options.start_ts_ms,
                "end_ts_ms": options.end_ts_ms,
                "label": options.evaluation_window_label,
                "source": options.evaluation_window_source,
            },
        }

    report = build_runtime_recommendations_v4(
        options=_options(tmp_path),
        run_id="run-001",
        search_budget_decision={"applied": {"runtime_recommendation_profile": "compact"}},
        optimize_runtime_recommendations_fn=_fake_optimize_runtime_recommendations_fn,
        runtime_recommendation_grid_for_profile_fn=lambda profile: {"profile": profile},
    )

    assert report["status"] == "ready"
    assert report["evaluation_window"] == {
        "start_ts_ms": parse_date_to_ts_ms("2026-03-21"),
        "end_ts_ms": parse_date_to_ts_ms("2026-03-22", end_of_day=True),
        "label": "certification",
        "source": "candidate_acceptance_certification_window",
    }
    assert captured["start_ts_ms"] == parse_date_to_ts_ms("2026-03-21")
    assert captured["end_ts_ms"] == parse_date_to_ts_ms("2026-03-22", end_of_day=True)
    assert captured["label"] == "certification"
    assert captured["source"] == "candidate_acceptance_certification_window"
    assert captured["candidate_ref"] == "run-001"
    assert captured["grid"] == {"profile": "compact"}
