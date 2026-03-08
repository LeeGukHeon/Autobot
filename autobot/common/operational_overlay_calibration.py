"""Build and persist operational overlay calibration artifacts from paper runs."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
from pathlib import Path
from statistics import median
from typing import Any, Sequence

import numpy as np


_DEFAULT_BASE_SETTINGS: dict[str, Any] = {
    "enabled": True,
    "use_calibration_artifact": True,
    "calibration_artifact_path": "logs/operational_overlay/latest.json",
    "risk_multiplier_min": 0.80,
    "risk_multiplier_max": 1.20,
    "max_positions_scale_min": 0.50,
    "max_positions_scale_max": 1.50,
    "session_overlap_boost": 0.10,
    "session_offpeak_penalty": 0.05,
    "micro_quality_block_threshold": 0.15,
    "micro_quality_conservative_threshold": 0.35,
    "micro_quality_aggressive_threshold": 0.75,
    "max_execution_spread_bps_for_join": 20.0,
    "max_execution_spread_bps_for_cross": 6.0,
    "min_execution_depth_krw_for_cross": 1_500_000.0,
    "snapshot_stale_ms": 15_000,
    "conservative_timeout_scale": 1.25,
    "aggressive_timeout_scale": 0.75,
    "conservative_replace_interval_scale": 1.50,
    "aggressive_replace_interval_scale": 0.50,
    "conservative_max_replaces_scale": 0.50,
    "aggressive_max_replaces_bonus": 1,
    "conservative_max_chase_bps_scale": 0.75,
    "aggressive_max_chase_bps_bonus": 5,
    "runtime_timeout_ms_floor": 5_000,
    "runtime_replace_interval_ms_floor": 1_500,
    "empirical_state_score_model_enabled": False,
    "empirical_state_score_intercept": 0.0,
    "empirical_state_score_regime_coef": 0.0,
    "empirical_state_score_breadth_coef": 0.0,
    "empirical_state_score_micro_coef": 0.0,
    "empirical_state_score_output_scale": 1.0,
}


@dataclass(frozen=True)
class OperationalOverlayCalibrationArtifact:
    generated_at: str
    lane: str
    report_count: int
    sufficient_reports: bool
    calibration_method: str
    source_report_dir: str
    applied_fields: list[str]
    stats: dict[str, Any]
    calibrated_settings: dict[str, Any]
    base_settings: dict[str, Any]


def build_operational_overlay_calibration(
    *,
    reports: Sequence[dict[str, Any]],
    lane: str,
    base_settings: dict[str, Any] | None = None,
    source_report_dir: str = "",
    min_reports: int = 5,
) -> dict[str, Any]:
    base = dict(_DEFAULT_BASE_SETTINGS)
    if isinstance(base_settings, dict):
        base.update(base_settings)
    valid_reports = [dict(item) for item in reports if isinstance(item, dict)]
    count = len(valid_reports)
    sufficient = count >= max(int(min_reports), 1)

    quality_values = _collect_float(valid_reports, "micro_quality_score_mean")
    runtime_risk_values = _collect_float(valid_reports, "runtime_risk_multiplier_mean")
    fill_concentration_values = _collect_float(valid_reports, "rolling_max_fill_concentration_ratio")
    slippage_values = _collect_float(valid_reports, "slippage_bps_mean")
    nonnegative_window_values = _collect_float(valid_reports, "rolling_nonnegative_active_window_ratio")
    positive_window_values = _collect_float(valid_reports, "rolling_positive_active_window_ratio")
    drawdown_values = _collect_float(valid_reports, "max_drawdown_pct")
    fallback_values = _collect_float(valid_reports, "micro_missing_fallback_ratio")
    positive_reports = [
        item
        for item in valid_reports
        if float(item.get("realized_pnl_quote", 0.0) or 0.0) >= 0.0
    ]

    calibration_reports = positive_reports if len(positive_reports) >= 3 else valid_reports
    calibration_quality = _collect_float(calibration_reports, "micro_quality_score_mean")
    calibration_risk = _collect_float(calibration_reports, "runtime_risk_multiplier_mean")
    calibration_fill_conc = _collect_float(calibration_reports, "rolling_max_fill_concentration_ratio")
    calibration_slippage = _collect_float(calibration_reports, "slippage_bps_mean")
    calibration_nonnegative = _collect_float(calibration_reports, "rolling_nonnegative_active_window_ratio")
    calibration_positive = _collect_float(calibration_reports, "rolling_positive_active_window_ratio")
    calibration_drawdown = _collect_float(calibration_reports, "max_drawdown_pct")
    calibration_fallback = _collect_float(calibration_reports, "micro_missing_fallback_ratio")
    calibration_regime = _collect_float(calibration_reports, "operational_regime_score_mean")
    calibration_breadth = _collect_float(calibration_reports, "operational_breadth_ratio_mean")
    calibration_max_positions = _collect_float(calibration_reports, "operational_max_positions_mean")

    stats = {
        "median_micro_quality_score_mean": _safe_median(quality_values),
        "median_runtime_risk_multiplier_mean": _safe_median(runtime_risk_values),
        "median_fill_concentration_ratio": _safe_median(fill_concentration_values),
        "median_slippage_bps_mean": _safe_median(slippage_values),
        "median_nonnegative_window_ratio": _safe_median(nonnegative_window_values),
        "median_positive_window_ratio": _safe_median(positive_window_values),
        "median_max_drawdown_pct": _safe_median(drawdown_values),
        "median_fallback_ratio": _safe_median(fallback_values),
        "median_operational_regime_score_mean": _safe_median(calibration_regime),
        "median_operational_breadth_ratio_mean": _safe_median(calibration_breadth),
        "median_operational_max_positions_mean": _safe_median(calibration_max_positions),
        "positive_report_count": len(positive_reports),
        "calibration_report_count": len(calibration_reports),
    }

    calibrated = dict(base)
    applied_fields: list[str] = []
    if sufficient:
        empirical_model = _fit_empirical_state_score_model(calibration_reports)
        if empirical_model is not None:
            calibrated["empirical_state_score_model_enabled"] = True
            calibrated["empirical_state_score_intercept"] = float(empirical_model["intercept"])
            calibrated["empirical_state_score_regime_coef"] = float(empirical_model["regime_coef"])
            calibrated["empirical_state_score_breadth_coef"] = float(empirical_model["breadth_coef"])
            calibrated["empirical_state_score_micro_coef"] = float(empirical_model["micro_coef"])
            calibrated["empirical_state_score_output_scale"] = float(empirical_model["output_scale"])
            stats["empirical_state_score_model"] = dict(empirical_model)
            applied_fields.extend(
                [
                    "empirical_state_score_model_enabled",
                    "empirical_state_score_intercept",
                    "empirical_state_score_regime_coef",
                    "empirical_state_score_breadth_coef",
                    "empirical_state_score_micro_coef",
                    "empirical_state_score_output_scale",
                ]
            )

    artifact = OperationalOverlayCalibrationArtifact(
        generated_at=datetime.now(timezone.utc).isoformat(),
        lane=str(lane).strip() or "v4",
        report_count=count,
        sufficient_reports=bool(sufficient),
        calibration_method="paper_run_empirical_state_score_v2",
        source_report_dir=str(source_report_dir),
        applied_fields=applied_fields,
        stats=stats,
        calibrated_settings=calibrated,
        base_settings=base,
    )
    return artifact.__dict__


def load_paper_smoke_reports(*, directory: Path, window_runs: int = 20) -> list[dict[str, Any]]:
    if not directory.exists():
        return []
    reports: list[dict[str, Any]] = []
    files = sorted(
        directory.glob("paper_micro_smoke_*.json"),
        key=lambda item: item.stat().st_mtime,
        reverse=True,
    )[: max(int(window_runs), 1)]
    for path in files:
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(raw, dict):
            reports.append(raw)
    return reports


def write_operational_overlay_calibration(
    *,
    report_dir: Path,
    output_path: Path,
    lane: str = "v4",
    window_runs: int = 20,
    min_reports: int = 5,
) -> dict[str, Any]:
    reports = load_paper_smoke_reports(directory=report_dir, window_runs=window_runs)
    artifact = build_operational_overlay_calibration(
        reports=reports,
        lane=lane,
        source_report_dir=str(report_dir),
        min_reports=min_reports,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(artifact, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return artifact


def _collect_float(reports: Sequence[dict[str, Any]], key: str) -> list[float]:
    values: list[float] = []
    for item in reports:
        try:
            raw = item.get(key)
            if raw is None:
                continue
            values.append(float(raw))
        except (TypeError, ValueError):
            continue
    return values


def _safe_median(values: Sequence[float], default: float = 0.0) -> float:
    filtered = [float(item) for item in values]
    if not filtered:
        return float(default)
    return float(median(filtered))


def _quantile(values: Sequence[float], q: float, *, default: float) -> float:
    filtered = sorted(float(item) for item in values)
    if not filtered:
        return float(default)
    if len(filtered) == 1:
        return filtered[0]
    qv = min(max(float(q), 0.0), 1.0)
    pos = qv * (len(filtered) - 1)
    lower = int(pos)
    upper = min(lower + 1, len(filtered) - 1)
    fraction = pos - lower
    return filtered[lower] + ((filtered[upper] - filtered[lower]) * fraction)


def _bounded(value: float, lower: float, upper: float, *, default: float) -> float:
    try:
        candidate = float(value)
    except (TypeError, ValueError):
        candidate = float(default)
    lo = float(lower)
    hi = float(upper)
    if hi < lo:
        hi = lo
    return max(lo, min(candidate, hi))


def _fit_empirical_state_score_model(reports: Sequence[dict[str, Any]]) -> dict[str, Any] | None:
    rows: list[list[float]] = []
    targets: list[float] = []
    weights: list[float] = []
    for item in reports:
        try:
            regime = float(item.get("operational_regime_score_mean"))
            breadth = float(item.get("operational_breadth_ratio_mean"))
            micro = float(item.get("micro_quality_score_mean"))
            pnl = float(item.get("realized_pnl_quote", 0.0) or 0.0)
            max_dd = max(float(item.get("max_drawdown_pct", 0.0) or 0.0), 0.25)
            fills = max(int(item.get("orders_filled", 0) or 0), 1)
        except (TypeError, ValueError):
            continue
        calmar_like = pnl / max_dd
        target = math.copysign(math.log1p(abs(calmar_like)), calmar_like)
        rows.append([regime, breadth, micro])
        targets.append(target)
        weights.append(float(max(fills, 1)))
    if len(rows) < 5:
        return None

    x = np.asarray(rows, dtype=np.float64)
    y = np.asarray(targets, dtype=np.float64)
    w = np.sqrt(np.asarray(weights, dtype=np.float64))
    x_mean = x.mean(axis=0)
    x_std = x.std(axis=0, ddof=0)
    x_std = np.where(x_std <= 1e-8, 1.0, x_std)
    y_mean = float(np.mean(y))
    y_std = float(np.std(y, ddof=0))
    if not np.isfinite(y_std) or y_std <= 1e-8:
        y_std = 1.0

    x_z = (x - x_mean) / x_std
    y_z = (y - y_mean) / y_std
    x_w = x_z * w[:, None]
    y_w = y_z * w
    ridge = 0.25
    beta = np.linalg.solve(x_w.T @ x_w + (ridge * np.eye(x_z.shape[1])), x_w.T @ y_w)
    intercept = -float(np.sum(beta * (x_mean / x_std)))
    coef_regime = float(beta[0] / x_std[0])
    coef_breadth = float(beta[1] / x_std[1])
    coef_micro = float(beta[2] / x_std[2])
    preds = intercept + (coef_regime * x[:, 0]) + (coef_breadth * x[:, 1]) + (coef_micro * x[:, 2])
    residual = y_z - preds
    output_scale = float(np.std(residual, ddof=0))
    if not np.isfinite(output_scale) or output_scale <= 1e-6:
        output_scale = 1.0
    r2 = 1.0 - (float(np.var(residual, ddof=0)) / max(float(np.var(y_z, ddof=0)), 1e-12))
    return {
        "feature_columns": ["operational_regime_score_mean", "operational_breadth_ratio_mean", "micro_quality_score_mean"],
        "target": "signed_log_calmar_like_realized_pnl",
        "report_count": len(rows),
        "intercept": float(intercept),
        "regime_coef": coef_regime,
        "breadth_coef": coef_breadth,
        "micro_coef": coef_micro,
        "output_scale": float(max(output_scale, 1e-6)),
        "ridge_alpha": float(ridge),
        "r2": float(r2),
        "target_mean": float(y_mean),
        "target_std": float(y_std),
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Calibrate runtime operational overlay from paper smoke history.")
    parser.add_argument("--report-dir", required=True)
    parser.add_argument("--output-path", required=True)
    parser.add_argument("--lane", default="v4")
    parser.add_argument("--window-runs", type=int, default=20)
    parser.add_argument("--min-reports", type=int, default=5)
    args = parser.parse_args(argv)

    artifact = write_operational_overlay_calibration(
        report_dir=Path(args.report_dir),
        output_path=Path(args.output_path),
        lane=str(args.lane),
        window_runs=int(args.window_runs),
        min_reports=int(args.min_reports),
    )
    print(json.dumps(artifact, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
