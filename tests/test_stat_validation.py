from __future__ import annotations

from pathlib import Path

from autobot.models.stat_validation import build_backtest_stat_validation


def _write_equity_csv(path: Path, values: list[float]) -> None:
    lines = ["ts,equity_quote"]
    for index, value in enumerate(values):
        lines.append(f"{index},{value}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_build_backtest_stat_validation_reports_missing_equity_csv(tmp_path: Path) -> None:
    report = build_backtest_stat_validation(run_dir=tmp_path, trial_count=5)

    assert report.comparable is False
    assert "MISSING_EQUITY_CSV" in report.reasons
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
    assert report.effective_trials == 8


def test_build_backtest_stat_validation_handles_flat_equity_curve(tmp_path: Path) -> None:
    _write_equity_csv(tmp_path / "equity.csv", [100_000.0, 100_000.0, 100_000.0])

    report = build_backtest_stat_validation(run_dir=tmp_path, trial_count=3)

    assert report.comparable is False
    assert "ZERO_RETURN_VOLATILITY" in report.reasons
    assert report.deflated_sharpe_ratio_est == 0.0
