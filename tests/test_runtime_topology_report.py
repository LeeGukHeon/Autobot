from __future__ import annotations

import json
from pathlib import Path

from autobot.live.state_store import LiveStateStore
import autobot.ops.runtime_topology_report as topology_module
from autobot.ops.runtime_topology_report import build_runtime_topology_report, write_runtime_topology_report


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def test_build_runtime_topology_report_summarizes_current_state(tmp_path: Path) -> None:
    project_root = tmp_path
    registry_root = project_root / "models" / "registry" / "train_v4_crypto_cs"
    registry_root.mkdir(parents=True, exist_ok=True)
    _write_json(registry_root / "champion.json", {"run_id": "run-1"})
    _write_json(registry_root / "latest.json", {"run_id": "run-2"})
    _write_json(registry_root / "latest_candidate.json", {"run_id": "run-3"})
    _write_json(project_root / "models" / "registry" / "latest.json", {"run_id": "run-2", "model_family": "train_v4_crypto_cs"})
    _write_json(project_root / "models" / "registry" / "latest_candidate.json", {"run_id": "run-3", "model_family": "train_v4_crypto_cs"})
    (registry_root / "run-1").mkdir(parents=True, exist_ok=True)

    ws_meta_dir = project_root / "data" / "raw_ws" / "upbit" / "_meta"
    ws_meta_dir.mkdir(parents=True, exist_ok=True)
    _write_json(
        ws_meta_dir / "ws_public_health.json",
        {
            "run_id": "ws-run-1",
            "connected": True,
            "updated_at_ms": 9_900,
            "last_rx_ts_ms": {"trade": 9_850, "orderbook": 9_900},
            "subscribed_markets_count": 20,
        },
    )
    _write_json(ws_meta_dir / "ws_collect_report.json", {"run_id": "collect-1", "generated_at": "2026-03-25T00:00:00Z"})
    _write_json(ws_meta_dir / "ws_validate_report.json", {"run_id": "validate-1", "generated_at": "2026-03-25T00:00:00Z", "checked_files": 4, "fail_files": 0})
    _write_json(ws_meta_dir / "ws_runs_summary.json", {"runs": [{"run_id": "ws-run-1", "rows_total": 100, "bytes_total": 1000}]})

    raw_ws_public = project_root / "data" / "raw_ws" / "upbit" / "public"
    raw_ws_public.mkdir(parents=True, exist_ok=True)

    micro_meta = project_root / "data" / "parquet" / "micro_v1" / "_meta"
    micro_meta.mkdir(parents=True, exist_ok=True)
    _write_json(micro_meta / "aggregate_report.json", {"run_id": "micro-run-1", "rows_written_total": 123})

    rollout_root = project_root / "logs" / "live_rollout"
    rollout_root.mkdir(parents=True, exist_ok=True)
    _write_json(
        rollout_root / "latest.json",
        {
            "event_kind": "STATUS",
            "target_unit": "autobot-live-alpha-candidate.service",
            "status": {"mode": "canary", "order_emission_allowed": True},
            "contract": {"armed": True, "mode": "canary", "target_unit": "autobot-live-alpha-candidate.service"},
            "test_order": {"ok": True, "ts_ms": 9_950},
        },
    )

    candidate_db = project_root / "data" / "state" / "live_candidate" / "live_state.db"
    candidate_db.parent.mkdir(parents=True, exist_ok=True)
    with LiveStateStore(candidate_db) as store:
        store.set_runtime_contract(payload={"live_runtime_model_run_id": "run-3"}, ts_ms=10_000)
        store.set_live_runtime_health(payload={"model_pointer_divergence": True}, ts_ms=10_000)
        store.set_live_rollout_status(payload={"mode": "canary", "order_emission_allowed": True}, ts_ms=10_000)
        store.set_live_rollout_contract(payload={"armed": True, "mode": "canary"}, ts_ms=10_000)

    live_db = project_root / "data" / "state" / "live" / "live_state.db"
    live_db.parent.mkdir(parents=True, exist_ok=True)
    with LiveStateStore(live_db) as store:
        store.set_runtime_contract(payload={"live_runtime_model_run_id": "run-1"}, ts_ms=10_000)
        store.set_live_runtime_health(payload={"model_pointer_divergence": False}, ts_ms=10_000)

    original_systemd = topology_module._systemd_topology_snapshot
    original_git = topology_module._git_topology_snapshot
    original_project = topology_module._project_topology_snapshot
    topology_module._systemd_topology_snapshot = lambda: {
        "available": True,
        "services": [
            {"unit": "autobot-paper-v4.service", "active": "active", "sub": "running", "load": "loaded", "description": "Champion paper"},
            {"unit": "autobot-live-alpha-candidate.service", "active": "active", "sub": "running", "load": "loaded", "description": "Candidate live"},
        ],
        "timers": [
            {"unit": "autobot-v4-challenger-promote.timer", "active": "active", "sub": "waiting", "load": "loaded", "description": "Promote timer"},
        ],
        "unit_files": [
            {"unit_file": "autobot-paper-v4.service", "state": "enabled", "preset": "enabled"},
        ],
        "errors": {},
    }
    topology_module._git_topology_snapshot = lambda *, root: {
        "available": True,
        "head": "abc123",
        "branch": "main",
        "remote_origin": "git@github.com:example/repo.git",
        "status_short": ["?? docs/report.md"],
        "dirty": True,
        "errors": {},
    }
    topology_module._project_topology_snapshot = lambda *, root: {
        "project_root_parent": str(root.parent),
        "sibling_directories": ["Autobot", "Autobot_replay_123"],
        "replay_like_paths": ["Autobot_replay_123"],
        "replay_path_present": True,
    }
    try:
        report = build_runtime_topology_report(project_root=project_root, target_unit="autobot-live-alpha-candidate.service", ts_ms=10_000)
    finally:
        topology_module._systemd_topology_snapshot = original_systemd
        topology_module._git_topology_snapshot = original_git
        topology_module._project_topology_snapshot = original_project

    assert report["pointers"]["champion"]["run_id"] == "run-1"
    assert report["pointers"]["latest_candidate"]["run_id"] == "run-3"
    assert report["candidate_lane"]["exists"] is True
    assert report["candidate_lane"]["runtime_contract"]["live_runtime_model_run_id"] == "run-3"
    assert report["live_lane"]["runtime_contract"]["live_runtime_model_run_id"] == "run-1"
    assert report["ws_public_contract"]["ws_public_stale"] is False
    assert report["runtime_sync_status"]["live_runtime_model_run_id"] == "run-3"
    assert report["runtime_sync_status"]["champion_pointer_run_id"] == "run-1"
    assert report["runtime_sync_status"]["model_pointer_divergence"] is True
    assert report["rollout_latest"]["target_unit"] == "autobot-live-alpha-candidate.service"
    assert report["summary"]["all_primary_pointers_equal"] is False
    assert report["systemd"]["available"] is True
    assert report["systemd"]["services"][0]["unit"] == "autobot-paper-v4.service"
    assert report["git"]["dirty"] is True
    assert report["project_topology"]["replay_path_present"] is True
    assert report["summary"]["systemd_available"] is True
    assert report["summary"]["git_dirty"] is True
    assert report["summary"]["replay_path_present"] is True


def test_write_runtime_topology_report_uses_default_output_path(tmp_path: Path) -> None:
    project_root = tmp_path
    (project_root / "models" / "registry" / "train_v4_crypto_cs").mkdir(parents=True, exist_ok=True)
    _write_json(project_root / "models" / "registry" / "train_v4_crypto_cs" / "champion.json", {"run_id": "run-1"})

    output_path = write_runtime_topology_report(project_root=project_root, ts_ms=10_000)

    assert output_path == project_root / "logs" / "runtime_topology" / "latest.json"
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["version"] == 1
    assert payload["summary"]["champion_run_id"] == "run-1"
