from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from autobot.models.execution_acceptance import ExecutionAcceptanceOptions, run_execution_acceptance
from autobot.models.registry import set_champion_pointer
from autobot.strategy.model_alpha_v1 import ModelAlphaSettings


def test_run_execution_acceptance_compares_candidate_to_champion(tmp_path: Path, monkeypatch) -> None:
    registry_root = tmp_path / "registry"
    family_dir = registry_root / "train_v4_crypto_cs"
    (family_dir / "run_candidate").mkdir(parents=True, exist_ok=True)
    (family_dir / "run_champion").mkdir(parents=True, exist_ok=True)
    set_champion_pointer(
        registry_root,
        "train_v4_crypto_cs",
        run_id="run_champion",
        score=0.5,
    )

    def _fake_run_model_backtest(*, options, model_ref):
        _ = options
        if model_ref == "run_candidate":
            return _summary_payload(run_id="cand", pnl=980.0, fill_rate=0.94, mdd=0.72, slip=3.2, fills=12)
        if model_ref == "run_champion":
            return _summary_payload(run_id="champ", pnl=1000.0, fill_rate=0.91, mdd=0.81, slip=3.8, fills=11)
        raise AssertionError(model_ref)

    monkeypatch.setattr("autobot.models.execution_acceptance._run_model_backtest", _fake_run_model_backtest)

    report = run_execution_acceptance(
        ExecutionAcceptanceOptions(
            registry_root=registry_root,
            model_family="train_v4_crypto_cs",
            candidate_ref="run_candidate",
            parquet_root=tmp_path / "parquet",
            dataset_name="candles_v1",
            output_root_dir=tmp_path / "backtest_logs",
            tf="5m",
            quote="KRW",
            top_n=20,
            start_ts_ms=1_000,
            end_ts_ms=2_000,
            feature_set="v4",
            model_alpha_settings=replace(ModelAlphaSettings(), feature_set="v4"),
        )
    )

    assert report["status"] == "compared"
    assert report["champion_ref"] == "run_champion"
    assert report["compare_to_champion"]["decision"] == "candidate_edge"


def test_run_execution_acceptance_returns_candidate_only_when_no_champion(tmp_path: Path, monkeypatch) -> None:
    registry_root = tmp_path / "registry"
    family_dir = registry_root / "train_v4_crypto_cs"
    (family_dir / "run_candidate").mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        "autobot.models.execution_acceptance._run_model_backtest",
        lambda *, options, model_ref: _summary_payload(
            run_id=str(model_ref),
            pnl=100.0,
            fill_rate=1.0,
            mdd=0.2,
            slip=1.5,
            fills=3,
        ),
    )

    report = run_execution_acceptance(
        ExecutionAcceptanceOptions(
            registry_root=registry_root,
            model_family="train_v4_crypto_cs",
            candidate_ref="run_candidate",
            parquet_root=tmp_path / "parquet",
            dataset_name="candles_v1",
            output_root_dir=tmp_path / "backtest_logs",
            tf="5m",
            quote="KRW",
            top_n=20,
            start_ts_ms=1_000,
            end_ts_ms=2_000,
            feature_set="v4",
            model_alpha_settings=replace(ModelAlphaSettings(), feature_set="v4"),
        )
    )

    assert report["status"] == "candidate_only"
    assert report["skip_reason"] == "NO_EXISTING_CHAMPION"
    assert report["candidate_summary"]["orders_filled"] == 3


def _summary_payload(
    *,
    run_id: str,
    pnl: float,
    fill_rate: float,
    mdd: float,
    slip: float,
    fills: int,
):
    from autobot.backtest.engine import BacktestRunSummary
    from autobot.models.execution_acceptance import _summary_to_doc

    return _summary_to_doc(
        BacktestRunSummary(
        run_id=run_id,
        run_dir="unused",
        tf="5m",
        from_ts_ms=1_000,
        to_ts_ms=2_000,
        bars_processed=10,
        markets=["KRW-BTC"],
        orders_submitted=fills,
        orders_filled=fills,
        orders_canceled=0,
        intents_failed=0,
        candidates_total=10,
        candidates_blocked_by_micro=0,
        candidates_aborted_by_policy=0,
        micro_blocked_ratio=0.0,
        micro_blocked_reasons={},
        replaces_total=0,
        cancels_total=0,
        aborted_timeout_total=0,
        dust_abort_total=0,
        avg_time_to_fill_ms=100.0,
        p50_time_to_fill_ms=100.0,
        p90_time_to_fill_ms=100.0,
        slippage_bps_mean=slip,
        slippage_bps_p50=slip,
        slippage_bps_p90=slip,
        fill_ratio=fill_rate,
        fill_rate=fill_rate,
        realized_pnl_quote=pnl,
        unrealized_pnl_quote=0.0,
        max_drawdown_pct=mdd,
        win_rate=0.6,
        strategy="model_alpha_v1",
        scored_rows=20,
        selected_rows=fills,
        skipped_missing_features_rows=0,
        selection_ratio=0.1,
        exposure_avg_open_positions=1.0,
        exposure_max_open_positions=1,
        )
    )
