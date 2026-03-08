"""Statistical validation helpers for backtest sanity checks.

This module focuses on lightweight, reusable post-run validation metrics
that can be computed from existing backtest artifacts such as ``equity.csv``.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import argparse
import csv
import json
import math
from pathlib import Path
from statistics import NormalDist
from typing import Sequence

from .registry import load_json
from .trial_dependence import estimate_effective_trials_from_trial_records


_STANDARD_NORMAL = NormalDist()
_EULER_GAMMA = 0.5772156649015329


@dataclass(frozen=True)
class BacktestStatValidation:
    run_dir: str
    equity_csv: str
    model_run_dir: str
    returns_count: int
    mean_return: float
    std_return: float
    sharpe_ratio: float
    skewness: float
    kurtosis: float
    probabilistic_sharpe_ratio: float
    deflated_sharpe_ratio_est: float
    benchmark_sharpe: float
    raw_trial_count: int
    effective_trials: int
    effective_trials_estimate: float
    effective_trials_source: str
    trial_dependence_avg_correlation: float | None
    trial_dependence_metric_count: int
    comparable: bool
    reasons: list[str]


def build_backtest_stat_validation(
    *,
    run_dir: Path,
    trial_count: int = 1,
    model_run_dir: Path | None = None,
) -> BacktestStatValidation:
    run_root = Path(run_dir)
    equity_csv = run_root / "equity.csv"
    trial_dependence = _resolve_trial_dependence(
        trial_count=max(int(trial_count), 1),
        model_run_dir=model_run_dir,
    )
    if not equity_csv.exists():
        return BacktestStatValidation(
            run_dir=str(run_root),
            equity_csv=str(equity_csv),
            model_run_dir=str(model_run_dir or ""),
            returns_count=0,
            mean_return=0.0,
            std_return=0.0,
            sharpe_ratio=0.0,
            skewness=0.0,
            kurtosis=3.0,
            probabilistic_sharpe_ratio=0.0,
            deflated_sharpe_ratio_est=0.0,
            benchmark_sharpe=0.0,
            raw_trial_count=int(trial_dependence["raw_trial_count"]),
            effective_trials=int(trial_dependence["effective_trials"]),
            effective_trials_estimate=float(trial_dependence["effective_trials_estimate"]),
            effective_trials_source=str(trial_dependence["source"]),
            trial_dependence_avg_correlation=_nullable_float(trial_dependence.get("avg_pairwise_correlation")),
            trial_dependence_metric_count=int(trial_dependence.get("metric_count", 0) or 0),
            comparable=False,
            reasons=["MISSING_EQUITY_CSV"],
        )

    equity_curve = _load_equity_curve(equity_csv)
    returns = _simple_returns(equity_curve)
    if len(returns) < 2:
        return BacktestStatValidation(
            run_dir=str(run_root),
            equity_csv=str(equity_csv),
            model_run_dir=str(model_run_dir or ""),
            returns_count=len(returns),
            mean_return=0.0,
            std_return=0.0,
            sharpe_ratio=0.0,
            skewness=0.0,
            kurtosis=3.0,
            probabilistic_sharpe_ratio=0.0,
            deflated_sharpe_ratio_est=0.0,
            benchmark_sharpe=0.0,
            raw_trial_count=int(trial_dependence["raw_trial_count"]),
            effective_trials=int(trial_dependence["effective_trials"]),
            effective_trials_estimate=float(trial_dependence["effective_trials_estimate"]),
            effective_trials_source=str(trial_dependence["source"]),
            trial_dependence_avg_correlation=_nullable_float(trial_dependence.get("avg_pairwise_correlation")),
            trial_dependence_metric_count=int(trial_dependence.get("metric_count", 0) or 0),
            comparable=False,
            reasons=["INSUFFICIENT_RETURNS"],
        )

    mean_return = sum(returns) / float(len(returns))
    std_return = _sample_std(returns)
    if std_return <= 0.0:
        sharpe_ratio = 0.0
        reasons = ["ZERO_RETURN_VOLATILITY"]
        comparable = False
    else:
        sharpe_ratio = mean_return / std_return
        reasons = []
        comparable = True

    skewness = _sample_skewness(returns)
    kurtosis = _sample_kurtosis(returns)
    if comparable:
        psr = probabilistic_sharpe_ratio(
            observed_sharpe=sharpe_ratio,
            benchmark_sharpe=0.0,
            observations=len(returns),
            skewness=skewness,
            kurtosis=kurtosis,
        )
        dsr_report = deflated_sharpe_ratio_estimate(
            observed_sharpe=sharpe_ratio,
            observations=len(returns),
            skewness=skewness,
            kurtosis=kurtosis,
            trial_count=int(trial_dependence["effective_trials"]),
        )
    else:
        psr = 0.0
        dsr_report = {
            "deflated_sharpe_ratio_est": 0.0,
            "benchmark_sharpe": 0.0,
            "effective_trials": float(int(trial_dependence["effective_trials"])),
        }
    return BacktestStatValidation(
        run_dir=str(run_root),
        equity_csv=str(equity_csv),
        model_run_dir=str(model_run_dir or ""),
        returns_count=len(returns),
        mean_return=float(mean_return),
        std_return=float(std_return),
        sharpe_ratio=float(sharpe_ratio),
        skewness=float(skewness),
        kurtosis=float(kurtosis),
        probabilistic_sharpe_ratio=float(psr),
        deflated_sharpe_ratio_est=float(dsr_report["deflated_sharpe_ratio_est"]),
        benchmark_sharpe=float(dsr_report["benchmark_sharpe"]),
        raw_trial_count=int(trial_dependence["raw_trial_count"]),
        effective_trials=int(trial_dependence["effective_trials"]),
        effective_trials_estimate=float(trial_dependence["effective_trials_estimate"]),
        effective_trials_source=str(trial_dependence["source"]),
        trial_dependence_avg_correlation=_nullable_float(trial_dependence.get("avg_pairwise_correlation")),
        trial_dependence_metric_count=int(trial_dependence.get("metric_count", 0) or 0),
        comparable=bool(comparable),
        reasons=reasons,
    )


def probabilistic_sharpe_ratio(
    *,
    observed_sharpe: float,
    benchmark_sharpe: float,
    observations: int,
    skewness: float = 0.0,
    kurtosis: float = 3.0,
) -> float:
    n = max(int(observations), 0)
    if n < 2:
        return 0.0
    denom_factor = 1.0 - (float(skewness) * float(observed_sharpe))
    denom_factor += ((float(kurtosis) - 1.0) / 4.0) * float(observed_sharpe) * float(observed_sharpe)
    if denom_factor <= 0.0:
        return 0.0
    z_score = ((float(observed_sharpe) - float(benchmark_sharpe)) * math.sqrt(float(n - 1))) / math.sqrt(denom_factor)
    return float(_STANDARD_NORMAL.cdf(z_score))


def deflated_sharpe_ratio_estimate(
    *,
    observed_sharpe: float,
    observations: int,
    skewness: float = 0.0,
    kurtosis: float = 3.0,
    trial_count: int = 1,
) -> dict[str, float]:
    n = max(int(observations), 0)
    trials = max(int(trial_count), 1)
    if n < 2:
        return {
            "deflated_sharpe_ratio_est": 0.0,
            "benchmark_sharpe": 0.0,
            "effective_trials": float(trials),
        }
    denom_factor = 1.0 - (float(skewness) * float(observed_sharpe))
    denom_factor += ((float(kurtosis) - 1.0) / 4.0) * float(observed_sharpe) * float(observed_sharpe)
    sr_std = math.sqrt(max(denom_factor, 1e-12)) / math.sqrt(float(n - 1))
    if trials <= 1:
        benchmark_sharpe = 0.0
    else:
        benchmark_sharpe = _expected_max_sharpe_ratio(trial_count=trials, sharpe_std=sr_std)
    dsr = probabilistic_sharpe_ratio(
        observed_sharpe=observed_sharpe,
        benchmark_sharpe=benchmark_sharpe,
        observations=n,
        skewness=skewness,
        kurtosis=kurtosis,
    )
    return {
        "deflated_sharpe_ratio_est": float(dsr),
        "benchmark_sharpe": float(benchmark_sharpe),
        "effective_trials": float(trials),
    }


def _expected_max_sharpe_ratio(*, trial_count: int, sharpe_std: float) -> float:
    trials = max(int(trial_count), 1)
    std_value = max(float(sharpe_std), 0.0)
    if trials <= 1 or std_value <= 0.0:
        return 0.0
    z_first = _STANDARD_NORMAL.inv_cdf(1.0 - (1.0 / float(trials)))
    z_second = _STANDARD_NORMAL.inv_cdf(1.0 - (1.0 / (float(trials) * math.e)))
    return std_value * (((1.0 - _EULER_GAMMA) * z_first) + (_EULER_GAMMA * z_second))


def _load_equity_curve(path: Path) -> list[float]:
    values: list[float] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            try:
                equity_value = float(row.get("equity_quote", 0.0) or 0.0)
            except Exception:
                continue
            if math.isfinite(equity_value) and equity_value > 0.0:
                values.append(equity_value)
    return values


def _simple_returns(equity_curve: Sequence[float]) -> list[float]:
    returns: list[float] = []
    previous: float | None = None
    for value in equity_curve:
        equity_value = float(value)
        if not math.isfinite(equity_value) or equity_value <= 0.0:
            previous = None
            continue
        if previous is not None and previous > 0.0:
            returns.append((equity_value / previous) - 1.0)
        previous = equity_value
    return returns


def _sample_std(values: Sequence[float]) -> float:
    n = len(values)
    if n <= 1:
        return 0.0
    mean_value = sum(values) / float(n)
    variance = sum((float(value) - mean_value) ** 2 for value in values) / float(n - 1)
    return math.sqrt(max(variance, 0.0))


def _sample_skewness(values: Sequence[float]) -> float:
    n = len(values)
    if n < 3:
        return 0.0
    mean_value = sum(values) / float(n)
    std_value = _sample_std(values)
    if std_value <= 0.0:
        return 0.0
    centered = [(float(value) - mean_value) / std_value for value in values]
    return float(sum(value ** 3 for value in centered) / float(n))


def _sample_kurtosis(values: Sequence[float]) -> float:
    n = len(values)
    if n < 4:
        return 3.0
    mean_value = sum(values) / float(n)
    std_value = _sample_std(values)
    if std_value <= 0.0:
        return 3.0
    centered = [(float(value) - mean_value) / std_value for value in values]
    return float(sum(value ** 4 for value in centered) / float(n))


def _resolve_trial_dependence(*, trial_count: int, model_run_dir: Path | None) -> dict[str, object]:
    raw_count = max(int(trial_count), 1)
    if model_run_dir is None:
        return {
            "raw_trial_count": raw_count,
            "effective_trials": raw_count,
            "effective_trials_estimate": float(raw_count),
            "source": "raw_trial_count_fallback",
            "avg_pairwise_correlation": None,
            "metric_count": 0,
        }
    metrics = load_json(Path(model_run_dir) / "metrics.json")
    booster = metrics.get("booster_sweep", {}) if isinstance(metrics, dict) else {}
    records = booster.get("records", []) if isinstance(booster, dict) else []
    dependence = estimate_effective_trials_from_trial_records(
        records if isinstance(records, list) else [],
        fallback_trial_count=raw_count,
    )
    return {
        "raw_trial_count": int(dependence.get("raw_trial_count", raw_count) or raw_count),
        "effective_trials": int(dependence.get("effective_trials", raw_count) or raw_count),
        "effective_trials_estimate": float(dependence.get("effective_trials_estimate", float(raw_count)) or float(raw_count)),
        "source": str(dependence.get("source", "raw_trial_count_fallback")),
        "avg_pairwise_correlation": dependence.get("avg_pairwise_correlation"),
        "metric_count": int(dependence.get("metric_count", 0) or 0),
    }


def _nullable_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _main() -> int:
    parser = argparse.ArgumentParser(description="Compute statistical validation from a backtest run directory.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--trial-count", type=int, default=1)
    parser.add_argument("--model-run-dir", default="")
    args = parser.parse_args()
    report = build_backtest_stat_validation(
        run_dir=Path(str(args.run_dir)),
        trial_count=max(int(args.trial_count), 1),
        model_run_dir=(Path(str(args.model_run_dir)) if str(args.model_run_dir).strip() else None),
    )
    print(json.dumps(asdict(report), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
