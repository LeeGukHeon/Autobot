from __future__ import annotations

from autobot.paper.rolling_evidence import compute_rolling_paper_evidence


def test_compute_rolling_paper_evidence_tracks_window_statistics() -> None:
    samples = [
        {"ts_ms": 0, "equity_quote": 50_000.0, "realized_pnl_quote": 0.0},
        {"ts_ms": 1_800_000, "equity_quote": 50_050.0, "realized_pnl_quote": 50.0},
        {"ts_ms": 3_600_000, "equity_quote": 50_020.0, "realized_pnl_quote": 20.0},
        {"ts_ms": 5_400_000, "equity_quote": 49_990.0, "realized_pnl_quote": -10.0},
    ]
    fills = [
        {"ts_ms": 600_000, "market": "KRW-BTC"},
        {"ts_ms": 1_200_000, "market": "KRW-ETH"},
        {"ts_ms": 4_200_000, "market": "KRW-BTC"},
    ]

    report = compute_rolling_paper_evidence(
        equity_samples=samples,
        fill_records=fills,
        window_ms=3_600_000,
    )

    assert report["window_minutes"] == 60
    assert report["windows_total"] == 2
    assert report["active_windows"] == 2
    assert report["nonnegative_active_window_ratio"] == 0.5
    assert report["positive_active_window_ratio"] == 0.5
    assert report["max_fill_concentration_ratio"] == 2.0 / 3.0
    assert report["max_window_drawdown_pct"] >= 0.0
    assert report["worst_window_realized_pnl_quote"] == -30.0
