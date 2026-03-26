from __future__ import annotations

import json
from pathlib import Path

from autobot.common.execution_structure import build_intent_outcomes_from_trade_rows
from autobot.common.opportunity_log import backfill_realized_outcomes


def test_build_intent_outcomes_from_trade_rows_tracks_round_trip_pnl_by_entry_intent() -> None:
    outcomes = build_intent_outcomes_from_trade_rows(
        [
            {
                "ts_ms": 1_000,
                "market": "KRW-BTC",
                "side": "bid",
                "fill_price": 100.0,
                "price": 100.0,
                "volume": 1.0,
                "fee_quote": 1.0,
                "intent_id": "intent-entry-1",
                "price_mode": "PASSIVE_MAKER",
            },
            {
                "ts_ms": 2_000,
                "market": "KRW-BTC",
                "side": "ask",
                "fill_price": 105.0,
                "price": 105.0,
                "volume": 1.0,
                "fee_quote": 1.0,
                "intent_id": "intent-exit-1",
                "reason_code": "MODEL_ALPHA_EXIT_TP",
            },
        ]
    )

    outcome = outcomes["intent-entry-1"]
    assert outcome["closed"] is True
    assert outcome["entry_fill_count"] == 1
    assert outcome["exit_fill_count"] == 1
    assert outcome["realized_pnl_quote"] == 3.0
    assert outcome["exit_reason_counts"]["MODEL_ALPHA_EXIT_TP"] == 1


def test_backfill_realized_outcomes_updates_opportunity_and_counterfactual_logs(tmp_path: Path) -> None:
    opportunity_path = tmp_path / "opportunity_log.jsonl"
    counterfactual_path = tmp_path / "counterfactual_action_log.jsonl"
    opportunity_path.write_text(
        json.dumps(
            {
                "opportunity_id": "entry:1000:KRW-BTC",
                "intent_id": "intent-entry-1",
                "chosen_action": "JOIN",
                "realized_outcome_json": None,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    counterfactual_path.write_text(
        json.dumps(
            {
                "opportunity_id": "entry:1000:KRW-BTC",
                "intent_id": "intent-entry-1",
                "action_payload": {"action_code": "JOIN"},
                "realized_outcome_json": None,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    report = backfill_realized_outcomes(
        opportunity_log_path=opportunity_path,
        counterfactual_log_path=counterfactual_path,
        outcome_by_intent={
            "intent-entry-1": {
                "intent_id": "intent-entry-1",
                "realized_pnl_quote": 3.0,
                "closed": True,
            }
        },
    )

    assert report["updated_opportunity_rows"] == 1
    assert report["updated_counterfactual_rows"] == 1
    opportunity_row = json.loads(opportunity_path.read_text(encoding="utf-8").strip())
    counterfactual_row = json.loads(counterfactual_path.read_text(encoding="utf-8").strip())
    assert opportunity_row["realized_outcome_json"]["realized_pnl_quote"] == 3.0
    assert counterfactual_row["realized_outcome_json"]["closed"] is True
