from __future__ import annotations

import json
import sys
from pathlib import Path

from autobot import cli as cli_module


def test_cli_model_export_expert_table_dispatches_to_sequence_export(monkeypatch, tmp_path, capsys) -> None:
    run_dir = tmp_path / "registry" / "train_v5_sequence" / "run-001"
    run_dir.mkdir(parents=True, exist_ok=True)
    captured: dict[str, object] = {}

    def _fake_export(
        *,
        run_dir: Path,
        start: str,
        end: str,
        selected_markets_override: tuple[str, ...] | None = None,
        anchor_export_path: Path | None = None,
        resolve_markets_only: bool = False,
    ) -> dict[str, object]:
        captured["run_dir"] = run_dir
        captured["start"] = start
        captured["end"] = end
        captured["selected_markets_override"] = selected_markets_override
        captured["anchor_export_path"] = anchor_export_path
        captured["resolve_markets_only"] = resolve_markets_only
        return {
            "run_id": "run-001",
            "trainer": "v5_sequence",
            "model_family": "train_v5_sequence",
            "data_platform_ready_snapshot_id": "snapshot-cli-001",
            "start": start,
            "end": end,
            "rows": 12,
            "selected_markets": ["KRW-BTC"],
            "export_path": str(run_dir / "_runtime_exports" / f"{start}__{end}" / "expert_prediction_table.parquet"),
            "metadata_path": str(run_dir / "_runtime_exports" / f"{start}__{end}" / "metadata.json"),
            "reused": False,
            "source_mode": "fresh_export",
        }

    monkeypatch.setattr(cli_module, "materialize_v5_sequence_runtime_export", _fake_export)
    monkeypatch.chdir(Path(__file__).resolve().parents[1])
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "autobot.cli",
            "model",
            "export-expert-table",
            "--trainer",
            "v5_sequence",
            "--run-dir",
            str(run_dir),
            "--start",
            "2026-03-23",
            "--end",
            "2026-03-30",
        ],
    )

    exit_code = cli_module.main()

    assert exit_code == 0
    assert captured == {
        "run_dir": run_dir.resolve(),
        "start": "2026-03-23",
        "end": "2026-03-30",
        "selected_markets_override": None,
        "anchor_export_path": None,
        "resolve_markets_only": False,
    }
    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["trainer"] == "v5_sequence"
    assert payload["start"] == "2026-03-23"
    assert payload["end"] == "2026-03-30"
