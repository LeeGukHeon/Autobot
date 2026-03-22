import json
import sqlite3
from pathlib import Path

import numpy as np

from autobot.models.live_domain_reweighting import build_live_candidate_domain_reweighting
from autobot.strategy.model_alpha_v1 import _build_live_state_feature_snapshot


def test_build_live_state_feature_snapshot_captures_risk_and_micro_columns() -> None:
    snapshot = _build_live_state_feature_snapshot(
        row={
            "vol_12": 0.12,
            "vol_36": 0.34,
            "atr_14": 10.0,
            "close": 1_000.0,
            "m_trade_coverage_ms": 60_000,
            "m_book_coverage_ms": 55_000,
            "m_spread_proxy": 3.5,
            "m_depth_bid_top5_mean": 1_500_000.0,
            "m_depth_ask_top5_mean": 2_000_000.0,
        }
    )

    assert snapshot["rv_12"] == 0.12
    assert snapshot["rv_36"] == 0.34
    assert snapshot["atr_pct_14"] == 0.01
    assert snapshot["m_trade_coverage_ms"] == 60_000.0
    assert snapshot["m_book_coverage_ms"] == 55_000.0
    assert snapshot["m_spread_proxy"] == 3.5
    assert snapshot["m_depth_top5_notional_krw"] == 3_500_000.0


def test_live_candidate_domain_reweighting_prefers_target_like_micro_states(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    _write_candidate_intents_db(db_path)

    stale_row = np.array([5_000.0, 4_000.0, 18.0, 120_000.0], dtype=np.float64)
    live_like_row = np.array([60_000.0, 58_000.0, 2.0, 5_000_000.0], dtype=np.float64)
    source_matrix = np.vstack(
        [
            np.repeat(stale_row[None, :], 6, axis=0),
            np.repeat(live_like_row[None, :], 6, axis=0),
        ]
    )

    weight, diagnostics = build_live_candidate_domain_reweighting(
        enabled=True,
        project_root=tmp_path,
        candidate_db_path=db_path,
        source_matrix=source_matrix,
        source_feature_names=(
            "m_trade_coverage_ms",
            "m_book_coverage_ms",
            "m_spread_proxy",
            "m_depth_top5_notional_krw",
        ),
        source_aux_features={},
        base_sample_weight=np.ones(source_matrix.shape[0], dtype=np.float64),
        seed=7,
        min_target_rows=8,
        max_target_rows=64,
        max_source_fit_rows=32,
    )

    assert diagnostics["status"] == "ready"
    assert "m_trade_coverage_ms" in diagnostics["feature_names"]
    assert float(np.mean(weight[6:])) > float(np.mean(weight[:6]))
    assert diagnostics["final_weight_summary"]["max"] >= diagnostics["final_weight_summary"]["mean"]


def test_live_candidate_domain_reweighting_skips_incomplete_target_rows_without_crashing(tmp_path: Path) -> None:
    db_path = tmp_path / "live_state.db"
    _write_candidate_intents_db(db_path, include_incomplete_rows=True)

    stale_row = np.array([5_000.0, 4_000.0, 18.0, 120_000.0], dtype=np.float64)
    live_like_row = np.array([60_000.0, 58_000.0, 2.0, 5_000_000.0], dtype=np.float64)
    source_matrix = np.vstack(
        [
            np.repeat(stale_row[None, :], 6, axis=0),
            np.repeat(live_like_row[None, :], 6, axis=0),
        ]
    )

    weight, diagnostics = build_live_candidate_domain_reweighting(
        enabled=True,
        project_root=tmp_path,
        candidate_db_path=db_path,
        source_matrix=source_matrix,
        source_feature_names=(
            "m_trade_coverage_ms",
            "m_book_coverage_ms",
            "m_spread_proxy",
            "m_depth_top5_notional_krw",
        ),
        source_aux_features={},
        base_sample_weight=np.ones(source_matrix.shape[0], dtype=np.float64),
        seed=7,
        min_target_rows=8,
        max_target_rows=64,
        max_source_fit_rows=32,
    )

    assert diagnostics["status"] == "ready"
    assert int(diagnostics["target_rows_dropped_incomplete"]) > 0
    assert int(diagnostics["target_rows_complete"]) < int(diagnostics["target_rows_total"])
    assert weight.shape[0] == source_matrix.shape[0]


def _write_candidate_intents_db(path: Path, *, include_incomplete_rows: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE intents (
                intent_id TEXT PRIMARY KEY,
                ts_ms INTEGER NOT NULL,
                market TEXT NOT NULL,
                side TEXT NOT NULL,
                price REAL,
                volume REAL,
                reason_code TEXT,
                meta_json TEXT NOT NULL DEFAULT '{}',
                status TEXT NOT NULL
            )
            """
        )
        payload = {
            "strategy": {
                "meta": {
                    "state_features": {
                        "rv_12": 0.01,
                        "rv_36": 0.02,
                        "atr_pct_14": 0.01,
                        "m_trade_coverage_ms": 60_000.0,
                        "m_book_coverage_ms": 58_000.0,
                        "m_spread_proxy": 2.0,
                        "m_depth_top5_notional_krw": 5_000_000.0,
                    }
                }
            },
            "micro_state": {
                "trade_coverage_ms": 60_000,
                "book_coverage_ms": 58_000,
                "spread_bps": 2.0,
                "depth_top5_notional_krw": 5_000_000.0,
            },
        }
        for idx in range(48):
            payload_row = dict(payload)
            if include_incomplete_rows and idx % 7 == 0:
                payload_row = json.loads(json.dumps(payload))
                payload_row["strategy"]["meta"]["state_features"]["m_book_coverage_ms"] = None
                payload_row["micro_state"]["book_coverage_ms"] = None
            conn.execute(
                "INSERT INTO intents VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    f"intent-{idx}",
                    1_710_000_000_000 + idx,
                    "KRW-BTC",
                    "bid",
                    None,
                    None,
                    "MODEL_ALPHA_ENTRY_V1",
                    json.dumps(payload_row, sort_keys=True),
                    "SUBMITTED",
                ),
            )
        conn.commit()
    finally:
        conn.close()
