from __future__ import annotations

import json
from pathlib import Path

from autobot.common.paper_lane_evidence import build_lane_comparison_report


def _write_summary(
    root: Path,
    run_id: str,
    *,
    role: str,
    model_ref: str,
    started: int,
    completed: int,
    realized_pnl: float,
    drawdown: float,
    micro_quality: float,
    nonnegative_ratio: float,
    orders_filled: int,
    fill_rate: float,
    duration_sec: float = 43_200.0,
) -> None:
    run_dir = root / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "run_id": run_id,
        "paper_lane": "v4",
        "paper_runtime_role": role,
        "paper_runtime_model_ref": model_ref,
        "paper_runtime_model_ref_pinned": model_ref,
        "run_started_ts_ms": started,
        "run_completed_ts_ms": completed,
        "orders_submitted": max(orders_filled, 1),
        "orders_filled": orders_filled,
        "fill_rate": fill_rate,
        "realized_pnl_quote": realized_pnl,
        "max_drawdown_pct": drawdown,
        "micro_quality_score_mean": micro_quality,
        "rolling_nonnegative_active_window_ratio": nonnegative_ratio,
        "rolling_positive_active_window_ratio": nonnegative_ratio,
        "rolling_active_windows": 12,
        "runtime_risk_multiplier_mean": 1.0,
        "operational_regime_score_mean": 0.2,
        "operational_breadth_ratio_mean": 0.6,
        "operational_max_positions_mean": 2.0,
        "duration_sec": duration_sec,
    }
    (run_dir / "summary.json").write_text(json.dumps(payload), encoding="utf-8")


def test_build_lane_comparison_report_promotes_stronger_challenger(tmp_path: Path) -> None:
    paper_root = tmp_path / "paper"
    _write_summary(
        paper_root,
        "paper-champion-1",
        role="champion",
        model_ref="champion_v4",
        started=1_000,
        completed=2_000,
        realized_pnl=100.0,
        drawdown=1.2,
        micro_quality=0.40,
        nonnegative_ratio=0.55,
        orders_filled=10,
        fill_rate=0.90,
    )
    _write_summary(
        paper_root,
        "paper-challenger-1",
        role="challenger",
        model_ref="candidate-123",
        started=1_500,
        completed=2_500,
        realized_pnl=180.0,
        drawdown=1.0,
        micro_quality=0.43,
        nonnegative_ratio=0.60,
        orders_filled=12,
        fill_rate=0.93,
    )

    report = build_lane_comparison_report(
        paper_root=paper_root,
        lane="v4",
        challenger_model_ref="candidate-123",
        since_ts_ms=1_400,
        until_ts_ms=None,
        min_challenger_hours=1.0,
        min_orders_filled=2,
        min_realized_pnl_quote=0.0,
        min_micro_quality_score=0.25,
        min_nonnegative_ratio=0.34,
        max_drawdown_deterioration_factor=1.10,
        micro_quality_tolerance=0.02,
        nonnegative_ratio_tolerance=0.05,
    )

    assert report["decision"]["promote"] is True
    assert report["decision"]["decision"] == "promote_challenger"


def test_build_lane_comparison_report_blocks_weak_challenger(tmp_path: Path) -> None:
    paper_root = tmp_path / "paper"
    _write_summary(
        paper_root,
        "paper-champion-1",
        role="champion",
        model_ref="champion_v4",
        started=1_000,
        completed=2_000,
        realized_pnl=100.0,
        drawdown=1.0,
        micro_quality=0.45,
        nonnegative_ratio=0.60,
        orders_filled=10,
        fill_rate=0.92,
    )
    _write_summary(
        paper_root,
        "paper-challenger-1",
        role="challenger",
        model_ref="candidate-456",
        started=1_500,
        completed=2_500,
        realized_pnl=-5.0,
        drawdown=1.5,
        micro_quality=0.10,
        nonnegative_ratio=0.10,
        orders_filled=1,
        fill_rate=0.50,
        duration_sec=600.0,
    )

    report = build_lane_comparison_report(
        paper_root=paper_root,
        lane="v4",
        challenger_model_ref="candidate-456",
        since_ts_ms=1_400,
        until_ts_ms=None,
        min_challenger_hours=1.0,
        min_orders_filled=2,
        min_realized_pnl_quote=0.0,
        min_micro_quality_score=0.25,
        min_nonnegative_ratio=0.34,
        max_drawdown_deterioration_factor=1.10,
        micro_quality_tolerance=0.02,
        nonnegative_ratio_tolerance=0.05,
    )

    assert report["decision"]["promote"] is False
    assert "NEGATIVE_REALIZED_PNL" in report["decision"]["hard_failures"]
