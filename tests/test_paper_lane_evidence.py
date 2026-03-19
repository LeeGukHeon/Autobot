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
    model_run_id: str | None = None,
    execution_structure: dict[str, object] | None = None,
) -> None:
    run_dir = root / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "run_id": run_id,
        "paper_lane": "v4",
        "paper_runtime_role": role,
        "paper_runtime_model_ref": model_ref,
        "paper_runtime_model_ref_pinned": model_ref,
        "paper_runtime_model_run_id": model_run_id or model_ref,
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
        "execution_structure": dict(execution_structure or {}),
    }
    (run_dir / "summary.json").write_text(json.dumps(payload), encoding="utf-8")


def test_build_lane_comparison_report_promotes_stronger_challenger(tmp_path: Path) -> None:
    paper_root = tmp_path / "paper"
    _write_summary(
        paper_root,
        "paper-champion-1",
        role="champion",
        model_ref="champion_v4",
        model_run_id="champion-run-a",
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
        champion_model_run_id="champion-run-a",
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
        model_run_id="champion-run-a",
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
        champion_model_run_id="champion-run-a",
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


def test_build_lane_comparison_report_filters_champion_generation(tmp_path: Path) -> None:
    paper_root = tmp_path / "paper"
    _write_summary(
        paper_root,
        "paper-champion-old",
        role="champion",
        model_ref="champion_v4",
        model_run_id="champion-run-old",
        started=1_000,
        completed=2_000,
        realized_pnl=999.0,
        drawdown=0.1,
        micro_quality=0.90,
        nonnegative_ratio=0.90,
        orders_filled=50,
        fill_rate=0.99,
    )
    _write_summary(
        paper_root,
        "paper-champion-current",
        role="champion",
        model_ref="champion_v4",
        model_run_id="champion-run-a",
        started=1_100,
        completed=2_100,
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
        model_ref="candidate-789",
        model_run_id="candidate-789",
        started=1_500,
        completed=2_500,
        realized_pnl=120.0,
        drawdown=0.95,
        micro_quality=0.47,
        nonnegative_ratio=0.65,
        orders_filled=12,
        fill_rate=0.94,
    )

    report = build_lane_comparison_report(
        paper_root=paper_root,
        lane="v4",
        challenger_model_ref="candidate-789",
        champion_model_run_id="champion-run-a",
        since_ts_ms=1_000,
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

    assert report["champion"]["realized_pnl_quote_total"] == 100.0


def test_build_lane_comparison_report_blocks_bad_payoff_structure(tmp_path: Path) -> None:
    paper_root = tmp_path / "paper"
    _write_summary(
        paper_root,
        "paper-champion-1",
        role="champion",
        model_ref="champion_v4",
        model_run_id="champion-run-a",
        started=1_000,
        completed=2_000,
        realized_pnl=100.0,
        drawdown=1.0,
        micro_quality=0.45,
        nonnegative_ratio=0.60,
        orders_filled=10,
        fill_rate=0.92,
        execution_structure={
            "closed_trade_count": 6,
            "win_pnl_quote_total": 180.0,
            "loss_pnl_quote_total_abs": 120.0,
            "payoff_ratio": 1.5,
            "tp_exit_count": 2,
            "sl_exit_count": 2,
            "timeout_exit_count": 2,
            "market_loss_concentration": 0.40,
        },
    )
    _write_summary(
        paper_root,
        "paper-challenger-1",
        role="challenger",
        model_ref="candidate-structure-bad",
        model_run_id="candidate-structure-bad",
        started=1_500,
        completed=2_500,
        realized_pnl=90.0,
        drawdown=0.95,
        micro_quality=0.47,
        nonnegative_ratio=0.65,
        orders_filled=12,
        fill_rate=0.94,
        execution_structure={
            "closed_trade_count": 6,
            "win_pnl_quote_total": 60.0,
            "loss_pnl_quote_total_abs": 180.0,
            "payoff_ratio": 0.33,
            "tp_exit_count": 0,
            "sl_exit_count": 4,
            "timeout_exit_count": 2,
            "market_loss_concentration": 0.92,
        },
    )

    report = build_lane_comparison_report(
        paper_root=paper_root,
        lane="v4",
        challenger_model_ref="candidate-structure-bad",
        champion_model_run_id="champion-run-a",
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
    assert "PAYOFF_RATIO_TOO_LOW" in report["decision"]["hard_failures"]
    assert "LOSS_CONCENTRATION_TOO_HIGH" in report["decision"]["hard_failures"]
