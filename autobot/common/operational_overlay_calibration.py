"""Build and persist operational overlay calibration artifacts from paper runs."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from statistics import median
from typing import Any, Sequence


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

    stats = {
        "median_micro_quality_score_mean": _safe_median(quality_values),
        "median_runtime_risk_multiplier_mean": _safe_median(runtime_risk_values),
        "median_fill_concentration_ratio": _safe_median(fill_concentration_values),
        "median_slippage_bps_mean": _safe_median(slippage_values),
        "median_nonnegative_window_ratio": _safe_median(nonnegative_window_values),
        "median_positive_window_ratio": _safe_median(positive_window_values),
        "median_max_drawdown_pct": _safe_median(drawdown_values),
        "median_fallback_ratio": _safe_median(fallback_values),
        "positive_report_count": len(positive_reports),
        "calibration_report_count": len(calibration_reports),
    }

    calibrated = dict(base)
    applied_fields: list[str] = []
    if sufficient:
        q15 = _quantile(calibration_quality, 0.15, default=base["micro_quality_block_threshold"])
        q40 = _quantile(calibration_quality, 0.40, default=base["micro_quality_conservative_threshold"])
        q80 = _quantile(calibration_quality, 0.80, default=base["micro_quality_aggressive_threshold"])
        calibrated["micro_quality_block_threshold"] = _bounded(
            min(q15 * 0.80, q40 - 0.05),
            0.05,
            0.25,
            default=base["micro_quality_block_threshold"],
        )
        calibrated["micro_quality_conservative_threshold"] = _bounded(
            q40,
            calibrated["micro_quality_block_threshold"] + 0.05,
            0.65,
            default=base["micro_quality_conservative_threshold"],
        )
        calibrated["micro_quality_aggressive_threshold"] = _bounded(
            q80,
            calibrated["micro_quality_conservative_threshold"] + 0.05,
            0.95,
            default=base["micro_quality_aggressive_threshold"],
        )
        applied_fields.extend(
            [
                "micro_quality_block_threshold",
                "micro_quality_conservative_threshold",
                "micro_quality_aggressive_threshold",
            ]
        )

        calibrated["risk_multiplier_min"] = _bounded(
            _quantile(calibration_risk, 0.20, default=base["risk_multiplier_min"]),
            0.70,
            1.00,
            default=base["risk_multiplier_min"],
        )
        calibrated["risk_multiplier_max"] = _bounded(
            _quantile(calibration_risk, 0.80, default=base["risk_multiplier_max"]),
            1.00,
            1.35,
            default=base["risk_multiplier_max"],
        )
        if calibrated["risk_multiplier_max"] < calibrated["risk_multiplier_min"]:
            calibrated["risk_multiplier_max"] = calibrated["risk_multiplier_min"]
        applied_fields.extend(["risk_multiplier_min", "risk_multiplier_max"])

        median_nonnegative = _safe_median(calibration_nonnegative, default=0.50)
        median_fill_conc = _safe_median(calibration_fill_conc, default=0.50)
        median_drawdown = _safe_median(calibration_drawdown, default=1.0)
        median_fallback = _safe_median(calibration_fallback, default=0.10)
        calibrated["max_positions_scale_min"] = _bounded(
            0.40 + (median_nonnegative * 0.30) - (median_fallback * 0.20),
            0.35,
            0.90,
            default=base["max_positions_scale_min"],
        )
        calibrated["max_positions_scale_max"] = _bounded(
            0.95 + (median_nonnegative * 0.40) + ((1.0 - median_fill_conc) * 0.25) - min(median_drawdown / 20.0, 0.10),
            1.00,
            1.80,
            default=base["max_positions_scale_max"],
        )
        if calibrated["max_positions_scale_max"] < calibrated["max_positions_scale_min"]:
            calibrated["max_positions_scale_max"] = calibrated["max_positions_scale_min"]
        applied_fields.extend(["max_positions_scale_min", "max_positions_scale_max"])

        median_slippage = _safe_median(calibration_slippage, default=base["max_execution_spread_bps_for_join"] / 4.0)
        slippage_anchor = _bounded(median_slippage / 8.0, 0.0, 1.0, default=0.5)
        calibrated["max_execution_spread_bps_for_join"] = _bounded(
            24.0 - (10.0 * slippage_anchor),
            10.0,
            30.0,
            default=base["max_execution_spread_bps_for_join"],
        )
        calibrated["max_execution_spread_bps_for_cross"] = _bounded(
            8.0 - (4.0 * slippage_anchor),
            3.0,
            max(calibrated["max_execution_spread_bps_for_join"] - 2.0, 3.0),
            default=base["max_execution_spread_bps_for_cross"],
        )
        calibrated["min_execution_depth_krw_for_cross"] = _bounded(
            1_000_000.0 + (3_000_000.0 * slippage_anchor),
            1_000_000.0,
            6_000_000.0,
            default=base["min_execution_depth_krw_for_cross"],
        )
        applied_fields.extend(
            [
                "max_execution_spread_bps_for_join",
                "max_execution_spread_bps_for_cross",
                "min_execution_depth_krw_for_cross",
            ]
        )

        median_positive = _safe_median(calibration_positive, default=0.50)
        calibrated["conservative_timeout_scale"] = _bounded(
            1.15 + (median_fallback * 0.40),
            1.10,
            1.60,
            default=base["conservative_timeout_scale"],
        )
        calibrated["aggressive_timeout_scale"] = _bounded(
            0.90 - (median_positive * 0.20),
            0.60,
            0.90,
            default=base["aggressive_timeout_scale"],
        )
        calibrated["conservative_replace_interval_scale"] = _bounded(
            1.25 + (median_fallback * 0.50),
            1.20,
            1.80,
            default=base["conservative_replace_interval_scale"],
        )
        calibrated["aggressive_replace_interval_scale"] = _bounded(
            0.65 - (median_positive * 0.15),
            0.35,
            0.75,
            default=base["aggressive_replace_interval_scale"],
        )
        calibrated["aggressive_max_replaces_bonus"] = int(
            round(_bounded(1.0 + ((1.0 - median_fill_conc) * 2.0), 1.0, 3.0, default=float(base["aggressive_max_replaces_bonus"])))
        )
        calibrated["conservative_max_chase_bps_scale"] = _bounded(
            0.80 - (median_positive * 0.10) + (median_fallback * 0.10),
            0.60,
            0.90,
            default=base["conservative_max_chase_bps_scale"],
        )
        calibrated["aggressive_max_chase_bps_bonus"] = int(
            round(_bounded(4.0 + ((1.0 - slippage_anchor) * 4.0), 3.0, 8.0, default=float(base["aggressive_max_chase_bps_bonus"])))
        )
        applied_fields.extend(
            [
                "conservative_timeout_scale",
                "aggressive_timeout_scale",
                "conservative_replace_interval_scale",
                "aggressive_replace_interval_scale",
                "aggressive_max_replaces_bonus",
                "conservative_max_chase_bps_scale",
                "aggressive_max_chase_bps_bonus",
            ]
        )

    artifact = OperationalOverlayCalibrationArtifact(
        generated_at=datetime.now(timezone.utc).isoformat(),
        lane=str(lane).strip() or "v4",
        report_count=count,
        sufficient_reports=bool(sufficient),
        calibration_method="paper_run_quantile_reestimation_v1",
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
