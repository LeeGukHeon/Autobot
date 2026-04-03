from __future__ import annotations

import json
from pathlib import Path

from autobot.ops.data_contract_registry import build_data_contract_registry
from autobot.ops.dataset_retention_registry import build_dataset_retention_registry
from autobot.ops.feature_dataset_certification import build_feature_dataset_certification
from autobot.ops.raw_to_feature_lineage_report import build_raw_to_feature_lineage_report


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def test_feature_dataset_certification_and_lineage_build_from_existing_artifacts(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    ws_meta = project_root / "data" / "raw_ws" / "upbit" / "_meta"
    _write_json(ws_meta / "ws_public_health.json", {"connected": True, "run_id": "ws-run-1"})
    _write_json(ws_meta / "ws_validate_report.json", {"status": "PASS", "fail_files": 0, "run_id": "ws-validate-1"})

    micro_meta = project_root / "data" / "parquet" / "micro_v1" / "_meta"
    _write_json(
        micro_meta / "aggregate_report.json",
        {
            "run_id": "micro-run-1",
            "raw_ws_root": "data/raw_ws/upbit",
            "rows_written_total": 10,
        },
    )
    _write_json(micro_meta / "validate_report.json", {"status": "PASS", "fail_files": 0, "run_id": "micro-validate-1"})

    features_meta = project_root / "data" / "features" / "features_v4" / "_meta"
    _write_json(
        features_meta / "build_report.json",
        {
            "status": "PASS",
            "run_id": "feature-build-1",
            "requested_start": "2026-03-01",
            "requested_end": "2026-03-31",
            "effective_start": "2026-03-01",
            "effective_end": "2026-03-31",
            "base_candles_root": "data/parquet/candles_api_v1",
            "micro_root": "data/parquet/micro_v1",
            "rows_dropped_no_micro": 2,
            "details": [
                {
                    "market": "KRW-BTC",
                    "rows_dropped_no_micro": 2,
                    "one_m_synth_ratio_mean": 0.4,
                }
            ],
            "universe_selection": {
                "candidates": [
                    {
                        "market": "KRW-BTC",
                        "quality_weight": 0.6,
                        "score": 123.0,
                        "selected": True,
                    }
                ]
            },
        },
    )
    _write_json(
        features_meta / "validate_report.json",
        {
            "status": "PASS",
            "fail_files": 0,
            "run_id": "feature-validate-1",
            "details": [
                {
                    "market": "KRW-BTC",
                    "status": "OK",
                    "leakage_fail_rows": 0,
                    "stale_rows": 0,
                }
            ],
        },
    )
    _write_json(
        features_meta / "live_feature_parity_report.json",
        {"status": "PASS", "acceptable": True, "sampled_pairs": 5},
    )

    registry = build_data_contract_registry(project_root=project_root)
    assert any(item.get("contract_id") == "feature_dataset:features_v4" for item in registry["entries"])

    certification = build_feature_dataset_certification(project_root=project_root, feature_set="v4")
    assert certification["pass"] is True
    assert certification["lineage"]["feature_source_contract_ids"] == [
        "micro_dataset:micro_v1",
    ]
    assert "micro_validate_parse_ok_ratio" in certification["quality_budget"]
    assert certification["market_quality_budget"][0]["market"] == "KRW-BTC"
    assert "quality_budget_summary" in certification
    assert certification["quality_budget_summary"]["selected_market_count"] >= 1

    retention = build_dataset_retention_registry(project_root=project_root)
    assert any(item.get("dataset_name") == "features_v4" for item in retention["entries"])
    assert any(item.get("policy_id") == "feature_warm_v1" for item in retention["entries"] if item.get("dataset_name") == "features_v4")
    assert any(item.get("policy_id") == "meta_cold_v1" for item in retention["policy_entries"])

    lineage = build_raw_to_feature_lineage_report(project_root=project_root, feature_set="v4")
    assert lineage["feature_contract"]["contract_id"] == "feature_dataset:features_v4"


def test_feature_dataset_certification_accepts_validate_reports_without_status_when_fail_files_are_zero(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    ws_meta = project_root / "data" / "raw_ws" / "upbit" / "_meta"
    _write_json(ws_meta / "ws_public_health.json", {"connected": True, "run_id": "ws-run-1"})
    _write_json(ws_meta / "ws_validate_report.json", {"status": "PASS", "fail_files": 0, "run_id": "ws-validate-1"})

    micro_meta = project_root / "data" / "parquet" / "micro_v1" / "_meta"
    _write_json(micro_meta / "aggregate_report.json", {"run_id": "micro-run-1", "raw_ws_root": "data/raw_ws/upbit", "rows_written_total": 10})
    _write_json(micro_meta / "validate_report.json", {"status": "PASS", "fail_files": 0, "run_id": "micro-validate-1"})

    features_meta = project_root / "data" / "features" / "features_v4" / "_meta"
    _write_json(
        features_meta / "build_report.json",
        {
            "status": "PASS",
            "run_id": "feature-build-1",
            "requested_start": "2026-03-20",
            "requested_end": "2026-04-02",
            "effective_start": "2026-03-20",
            "effective_end": "2026-03-31",
            "base_candles_root": "data/parquet/candles_api_v1",
            "micro_root": "data/parquet/micro_v1",
        },
    )
    _write_json(
        features_meta / "validate_report.json",
        {
            "checked_files": 50,
            "ok_files": 30,
            "warn_files": 20,
            "fail_files": 0,
            "leakage_smoke": "PASS",
        },
    )
    _write_json(
        features_meta / "live_feature_parity_report.json",
        {"status": "PASS", "acceptable": True, "sampled_pairs": 5},
    )

    certification = build_feature_dataset_certification(project_root=project_root, feature_set="v4")
    assert certification["pass"] is True
    assert certification["checks"]["validate_report_pass"] is True
