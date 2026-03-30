from __future__ import annotations

from pathlib import Path

from autobot.models.execution_acceptance import ExecutionAcceptanceOptions
from autobot.models.runtime_recommendations import (
    optimize_runtime_recommendations,
    runtime_recommendation_grid_for_profile,
)
from autobot.strategy.model_alpha_v1 import ModelAlphaSettings


def test_optimize_runtime_recommendations_reuses_search_cache(tmp_path: Path, monkeypatch) -> None:
    calls = {"count": 0}

    def _fake_run_model_execution_backtest(*, options, model_ref, model_alpha_settings=None):
        _ = options
        _ = model_ref
        _ = model_alpha_settings
        calls["count"] += 1
        return {
            "realized_pnl_quote": 100.0,
            "fill_rate": 0.9,
            "avg_time_to_fill_ms": 100.0,
            "p90_time_to_fill_ms": 200.0,
            "max_drawdown_pct": 0.1,
            "slippage_bps_mean": 1.0,
            "execution_validation": {
                "comparable": True,
                "comparable_fold_count": 3,
                "objective_score": 1.0,
                "objective_std": 0.1,
                "nonnegative_ratio_mean": 0.8,
                "max_window_drawdown_pct": 0.2,
                "worst_window_return": -0.05,
            },
        }

    monkeypatch.setattr(
        "autobot.models.runtime_recommendations.run_model_execution_backtest",
        _fake_run_model_execution_backtest,
    )

    options = ExecutionAcceptanceOptions(
        registry_root=tmp_path / "registry",
        model_family="train_v5_panel_ensemble",
        candidate_ref="run-001",
        parquet_root=tmp_path / "parquet",
        dataset_name="candles_v1",
        output_root_dir=tmp_path / "backtest",
        tf="5m",
        quote="KRW",
        top_n=20,
        start_ts_ms=1_000,
        end_ts_ms=2_000,
        feature_set="v4",
        model_alpha_settings=ModelAlphaSettings(model_ref="run-001", model_family="train_v5_panel_ensemble"),
    )
    cache_path = tmp_path / "runtime_recommendation_search_cache.json"
    grid = runtime_recommendation_grid_for_profile("tiny")

    first = optimize_runtime_recommendations(
        options=options,
        candidate_ref="run-001",
        grid=grid,
        cache_path=cache_path,
        cache_context={
            "data_platform_ready_snapshot_id": "snapshot-001",
            "profile": "tiny",
        },
    )
    calls_after_first = calls["count"]
    second = optimize_runtime_recommendations(
        options=options,
        candidate_ref="run-001",
        grid=grid,
        cache_path=cache_path,
        cache_context={
            "data_platform_ready_snapshot_id": "snapshot-001",
            "profile": "tiny",
        },
    )

    assert first["status"] in {"ready", "fallback"}
    assert second["status"] == first["status"]
    assert calls_after_first > 0
    assert calls["count"] == calls_after_first
    assert cache_path.exists()
