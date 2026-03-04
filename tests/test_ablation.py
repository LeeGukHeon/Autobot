from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from autobot.models.ablation import select_ablation_feature_columns, write_ablation_outputs


def test_select_ablation_feature_columns() -> None:
    features = (
        "ret_1",
        "rv_12",
        "m_trade_source",
        "m_trade_events",
        "m_trade_coverage_ms",
        "m_book_events",
        "m_book_spread_bps",
        "m_micro_available",
    )

    a0 = select_ablation_feature_columns(features, ablation_id="A0")
    a1 = select_ablation_feature_columns(features, ablation_id="A1")
    a2 = select_ablation_feature_columns(features, ablation_id="A2")
    a3 = select_ablation_feature_columns(features, ablation_id="A3")
    a4 = select_ablation_feature_columns(features, ablation_id="A4")

    assert a0 == ("ret_1", "rv_12")
    assert "m_trade_source" in a1
    assert "m_book_events" not in a1
    assert "m_book_events" in a2
    assert "m_trade_source" not in a2
    assert a3 == features
    assert a4 == (
        "m_trade_source",
        "m_trade_events",
        "m_trade_coverage_ms",
        "m_book_events",
        "m_book_spread_bps",
        "m_micro_available",
    )


def test_write_ablation_outputs_schema(tmp_path: Path) -> None:
    rows = [
        {
            "ablation_id": "A0",
            "feature_count": 2,
            "feature_columns": "ret_1,rv_12",
            "rows_total": 100,
            "rows_train": 70,
            "rows_valid": 15,
            "rows_test": 15,
            "label_pos_rate_train": 0.4,
            "label_pos_rate_valid": 0.45,
            "label_pos_rate_test": 0.5,
            "roc_auc": 0.55,
            "pr_auc": 0.51,
            "log_loss": 0.69,
            "brier_score": 0.24,
            "precision_top1": 0.6,
            "precision_top5": 0.55,
            "precision_top10": 0.53,
            "ev_net_top1": 0.001,
            "ev_net_top5": 0.0008,
            "ev_net_top10": 0.0005,
            "micro_coverage_ratio": 0.27,
            "micro_coverage_p50_ms": 240000,
            "micro_coverage_p90_ms": 300000,
            "book_available_ratio": 0.1,
            "trade_source_ws_ratio": 0.2,
            "trade_source_rest_ratio": 0.7,
            "trade_source_none_ratio": 0.1,
            "trade_source_other_ratio": 0.0,
            "candles_rows": 370,
        }
    ]
    summary = {
        "best_ablation_by_prec_top5": "A0",
        "best_ablation_by_ev_top5": "A0",
        "rule_decision": {"rule": "UNDECIDED", "next_ticket": "T14.4"},
    }

    csv_path = tmp_path / "logs" / "t14_3_ablation_results.csv"
    json_path = tmp_path / "logs" / "t14_3_ablation_summary.json"
    write_ablation_outputs(rows=rows, summary=summary, csv_path=csv_path, summary_path=json_path)

    frame = pl.read_csv(csv_path)
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert frame.height == 1
    assert {"ablation_id", "rows_train", "rows_valid", "rows_test", "precision_top5", "ev_net_top5"}.issubset(
        set(frame.columns)
    )
    assert "best_ablation_by_prec_top5" in payload
    assert "rule_decision" in payload
