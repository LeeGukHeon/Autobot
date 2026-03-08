from __future__ import annotations

from pathlib import Path
import json

from autobot.models.stat_validation import build_backtest_stat_validation


def _write_equity_csv(path: Path, values: list[float]) -> None:
    lines = ["ts,equity_quote"]
    for index, value in enumerate(values):
        lines.append(f"{index},{value}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_model_metrics(path: Path, records: list[dict]) -> None:
    payload = {
        "booster_sweep": {
            "trials": len(records),
            "records": records,
        }
    }
    path.mkdir(parents=True, exist_ok=True)
    (path / "metrics.json").write_text(json.dumps(payload), encoding="utf-8")


def test_build_backtest_stat_validation_reports_missing_equity_csv(tmp_path: Path) -> None:
    report = build_backtest_stat_validation(run_dir=tmp_path, trial_count=5)

    assert report.comparable is False
    assert "MISSING_EQUITY_CSV" in report.reasons
    assert report.raw_trial_count == 5
    assert report.effective_trials == 5


def test_build_backtest_stat_validation_computes_probabilistic_metrics(tmp_path: Path) -> None:
    _write_equity_csv(
        tmp_path / "equity.csv",
        [100_000.0, 100_800.0, 101_500.0, 101_200.0, 102_600.0, 103_200.0],
    )

    report = build_backtest_stat_validation(run_dir=tmp_path, trial_count=8)

    assert report.comparable is True
    assert report.returns_count == 5
    assert report.std_return > 0.0
    assert report.sharpe_ratio > 0.0
    assert 0.0 <= report.probabilistic_sharpe_ratio <= 1.0
    assert 0.0 <= report.deflated_sharpe_ratio_est <= 1.0
    assert report.raw_trial_count == 8
    assert report.effective_trials == 8


def test_build_backtest_stat_validation_handles_flat_equity_curve(tmp_path: Path) -> None:
    _write_equity_csv(tmp_path / "equity.csv", [100_000.0, 100_000.0, 100_000.0])

    report = build_backtest_stat_validation(run_dir=tmp_path, trial_count=3)

    assert report.comparable is False
    assert "ZERO_RETURN_VOLATILITY" in report.reasons
    assert report.deflated_sharpe_ratio_est == 0.0


def test_build_backtest_stat_validation_uses_correlation_adjusted_effective_trials(tmp_path: Path) -> None:
    _write_equity_csv(
        tmp_path / "equity.csv",
        [100_000.0, 100_900.0, 101_200.0, 101_600.0, 102_300.0, 102_900.0],
    )
    model_run_dir = tmp_path / "model-run"
    _write_model_metrics(
        model_run_dir,
        [
            {"trial": 0, "selection_key": {"precision_top5": 0.51, "pr_auc": 0.61, "roc_auc": 0.71}},
            {"trial": 1, "selection_key": {"precision_top5": 0.52, "pr_auc": 0.62, "roc_auc": 0.72}},
            {"trial": 2, "selection_key": {"precision_top5": 0.53, "pr_auc": 0.63, "roc_auc": 0.73}},
            {"trial": 3, "selection_key": {"precision_top5": 0.54, "pr_auc": 0.64, "roc_auc": 0.74}},
        ],
    )

    report = build_backtest_stat_validation(run_dir=tmp_path, trial_count=4, model_run_dir=model_run_dir)

    assert report.comparable is True
    assert report.raw_trial_count == 4
    assert report.effective_trials < report.raw_trial_count
    assert report.effective_trials_source == "avg_pairwise_outcome_correlation"
    assert report.trial_dependence_avg_correlation is not None
    assert report.trial_dependence_metric_count == 3
