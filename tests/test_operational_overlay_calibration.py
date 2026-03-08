from __future__ import annotations

import json
from pathlib import Path

from autobot.common.operational_overlay_calibration import (
    build_operational_overlay_calibration,
    load_paper_smoke_reports,
    write_operational_overlay_calibration,
)


def _report(*, pnl: float, micro_quality: float, risk: float, fill_conc: float, slippage: float) -> dict[str, float]:
    return {
        "realized_pnl_quote": pnl,
        "micro_quality_score_mean": micro_quality,
        "runtime_risk_multiplier_mean": risk,
        "rolling_max_fill_concentration_ratio": fill_conc,
        "slippage_bps_mean": slippage,
        "rolling_nonnegative_active_window_ratio": 0.75,
        "rolling_positive_active_window_ratio": 0.50,
        "max_drawdown_pct": 0.8,
        "micro_missing_fallback_ratio": 0.05,
    }


def test_build_operational_overlay_calibration_reestimates_coefficients() -> None:
    artifact = build_operational_overlay_calibration(
        reports=[
            _report(pnl=10.0, micro_quality=0.70, risk=0.95, fill_conc=0.40, slippage=2.0),
            _report(pnl=15.0, micro_quality=0.80, risk=1.00, fill_conc=0.35, slippage=1.5),
            _report(pnl=12.0, micro_quality=0.75, risk=1.05, fill_conc=0.45, slippage=2.5),
            _report(pnl=9.0, micro_quality=0.68, risk=0.92, fill_conc=0.50, slippage=3.0),
            _report(pnl=11.0, micro_quality=0.72, risk=0.98, fill_conc=0.38, slippage=2.0),
        ],
        lane="v4",
    )

    assert artifact["sufficient_reports"] is True
    assert "risk_multiplier_min" in artifact["calibrated_settings"]
    assert "max_positions_scale_max" in artifact["calibrated_settings"]
    assert "micro_quality_block_threshold" in artifact["calibrated_settings"]
    assert artifact["applied_fields"]


def test_write_operational_overlay_calibration_reads_recent_reports(tmp_path: Path) -> None:
    report_dir = tmp_path / "reports"
    report_dir.mkdir(parents=True)
    for idx in range(3):
        (report_dir / f"paper_micro_smoke_20260308-0{idx}.json").write_text(
            json.dumps(_report(pnl=5.0 + idx, micro_quality=0.6 + (idx * 0.05), risk=0.9 + (idx * 0.02), fill_conc=0.4, slippage=2.0)),
            encoding="utf-8",
        )
    output_path = tmp_path / "overlay" / "latest.json"

    artifact = write_operational_overlay_calibration(
        report_dir=report_dir,
        output_path=output_path,
        lane="v4",
        window_runs=2,
        min_reports=2,
    )

    assert output_path.exists()
    loaded_reports = load_paper_smoke_reports(directory=report_dir, window_runs=2)
    assert len(loaded_reports) == 2
    assert artifact["report_count"] == 2
