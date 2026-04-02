from __future__ import annotations

import json
from pathlib import Path

from autobot.live.paper_live_divergence import (
    build_paper_live_divergence_report,
    paper_live_divergence_latest_path,
    write_paper_live_divergence_report,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(json.dumps(row, ensure_ascii=False, sort_keys=True) for row in rows) + ("\n" if rows else ""),
        encoding="utf-8",
    )


def test_build_paper_live_divergence_report_from_scanned_paper_run(tmp_path: Path) -> None:
    project_root = tmp_path
    unit_name = "autobot-live-alpha-candidate.service"
    live_log_path = project_root / "logs" / "opportunity_log" / "autobot_live_alpha_candidate_service" / "latest.jsonl"
    _write_jsonl(
        live_log_path,
        [
            {
                "opportunity_id": "opp-1",
                "ts_ms": 1_000,
                "market": "KRW-BTC",
                "side": "bid",
                "feature_hash": "same-hash",
                "chosen_action": "JOIN",
                "skip_reason_code": "",
                "reason_code": "MODEL_ALPHA_ENTRY_V1",
            },
            {
                "opportunity_id": "opp-2",
                "ts_ms": 2_000,
                "market": "KRW-ETH",
                "side": "bid",
                "feature_hash": "live-hash",
                "chosen_action": "NO_TRADE",
                "skip_reason_code": "ENTRY_GATE_PORTFOLIO_BUDGET_BLOCKED",
                "reason_code": "MODEL_ALPHA_ENTRY_V1",
            },
        ],
    )
    paper_run_dir = project_root / "data" / "paper" / "runs" / "paper-20260402-000000"
    _write_json(
        paper_run_dir / "summary.json",
        {
            "run_id": "paper-20260402-000000",
            "paper_runtime_role": "candidate",
            "paper_lane": "paper_candidate",
            "paper_runtime_model_run_id": "run-live",
        },
    )
    _write_jsonl(
        paper_run_dir / "opportunity_log.jsonl",
        [
            {
                "opportunity_id": "opp-1",
                "ts_ms": 1_000,
                "market": "KRW-BTC",
                "side": "bid",
                "feature_hash": "same-hash",
                "chosen_action": "JOIN",
                "skip_reason_code": "",
                "reason_code": "MODEL_ALPHA_ENTRY_V1",
            },
            {
                "opportunity_id": "opp-2",
                "ts_ms": 2_000,
                "market": "KRW-ETH",
                "side": "bid",
                "feature_hash": "paper-hash",
                "chosen_action": "JOIN",
                "skip_reason_code": "",
                "reason_code": "MODEL_ALPHA_ENTRY_V1",
            },
        ],
    )

    report = build_paper_live_divergence_report(
        project_root=project_root,
        unit_name=unit_name,
        lane="live_candidate",
        run_id="run-live",
        ts_ms=3_000,
    )

    assert report["status"] == "ready"
    assert report["paper_source"]["source_kind"] == "paper_run_scan"
    assert report["matching"]["matched_opportunities"] == 2
    assert report["feature_divergence"]["feature_hash_match_ratio"] == 0.5
    assert report["feature_divergence"]["feature_divergence_rate"] == 0.5
    assert report["decision_divergence"]["decision_divergence_rate"] == 0.5
    assert len(report["matched_records"]) == 2


def test_build_paper_live_divergence_report_prefers_paired_paper_mapping(tmp_path: Path) -> None:
    project_root = tmp_path
    unit_name = "autobot-live-alpha-canary.service"
    live_log_path = project_root / "logs" / "opportunity_log" / "autobot_live_alpha_canary_service" / "latest.jsonl"
    _write_jsonl(
        live_log_path,
        [
            {
                "opportunity_id": "opp-1",
                "ts_ms": 1_000,
                "market": "KRW-BTC",
                "side": "bid",
                "feature_hash": "same-hash",
                "chosen_action": "JOIN",
                "skip_reason_code": "",
                "reason_code": "MODEL_ALPHA_ENTRY_V1",
            }
        ],
    )
    run_dir = project_root / "data" / "paper" / "runs" / "paper-20260402-010000"
    _write_jsonl(
        run_dir / "opportunity_log.jsonl",
        [
            {
                "opportunity_id": "opp-1",
                "ts_ms": 1_000,
                "market": "KRW-BTC",
                "side": "bid",
                "feature_hash": "same-hash",
                "chosen_action": "JOIN",
                "skip_reason_code": "",
                "reason_code": "MODEL_ALPHA_ENTRY_V1",
            }
        ],
    )
    _write_json(
        project_root / "logs" / "paired_paper" / "latest.json",
        {
            "challenger": {
                "run_dir": str(run_dir),
                "run_id": run_dir.name,
                "paper_runtime_role": "candidate",
                "paper_lane": "paper_candidate",
                "paper_runtime_model_run_id": "run-canary",
            }
        },
    )

    report = build_paper_live_divergence_report(
        project_root=project_root,
        unit_name=unit_name,
        lane="live_candidate",
        run_id="run-canary",
        ts_ms=2_000,
    )

    assert report["status"] == "ready"
    assert report["paper_source"]["source_kind"] == "paired_paper"
    assert report["matching"]["matched_opportunities"] == 1


def test_build_paper_live_divergence_report_returns_insufficient_evidence_when_logs_missing(tmp_path: Path) -> None:
    report = build_paper_live_divergence_report(
        project_root=tmp_path,
        unit_name="autobot-live-alpha.service",
        lane="live_champion",
        run_id="run-live",
        ts_ms=1_000,
    )

    assert report["status"] == "insufficient_evidence"
    assert "LIVE_OPPORTUNITY_LOG_MISSING" in report["reason_codes"]
    assert report["feature_divergence"]["available"] is False


def test_repo_live_feature_parity_fixture_is_exposed_as_diagnostic() -> None:
    project_root = Path.cwd()
    report = build_paper_live_divergence_report(
        project_root=project_root,
        unit_name="autobot-live-alpha.service",
        lane="live_champion",
        run_id="run-live",
        ts_ms=1_000,
    )

    assert report["infra_parity_diagnostic"]["available"] is True
    assert report["infra_parity_diagnostic"]["acceptable"] is False


def test_write_paper_live_divergence_report_persists_latest(tmp_path: Path) -> None:
    latest_path = paper_live_divergence_latest_path(
        project_root=tmp_path,
        unit_name="autobot-live-alpha.service",
    )
    write_paper_live_divergence_report(
        latest_path=latest_path,
        payload={"artifact_version": 1, "status": "ready"},
    )

    assert latest_path.exists()
    assert json.loads(latest_path.read_text(encoding="utf-8"))["status"] == "ready"
