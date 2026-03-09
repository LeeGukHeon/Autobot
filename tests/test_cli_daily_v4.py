from __future__ import annotations

import argparse
from pathlib import Path
from types import SimpleNamespace

import autobot.cli as cli_mod


def test_run_manual_v4_daily_pipeline_defaults_to_skip_paper_soak(tmp_path, monkeypatch) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    wrapper_script = tmp_path / "scripts" / "v4_candidate_acceptance.ps1"
    wrapper_script.parent.mkdir(parents=True, exist_ok=True)
    wrapper_script.write_text("# noop\n", encoding="utf-8")

    captured: dict[str, object] = {}

    def _fake_run(command, cwd=None, text=None):
        captured["command"] = list(command)
        captured["cwd"] = cwd
        captured["text"] = text
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(cli_mod, "_resolve_powershell_exe", lambda: "pwsh")
    monkeypatch.setattr(cli_mod.subprocess, "run", _fake_run)

    args = argparse.Namespace(
        mode="spawn_only",
        batch_date="2026-03-08",
        run_paper_soak=False,
        paper_soak_duration_sec=None,
        dry_run=True,
    )

    exit_code = cli_mod._run_manual_v4_daily_pipeline(args, config_dir)

    assert exit_code == 0
    assert captured["cwd"] == tmp_path.resolve()
    command = captured["command"]
    assert command[:6] == ["pwsh", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(wrapper_script)]
    assert "-RunScope" in command
    assert command[command.index("-RunScope") + 1] == "manual_daily"
    assert "-OutDir" in command
    assert command[command.index("-OutDir") + 1] == "logs/model_v4_acceptance_manual"
    assert "-SkipPromote" in command
    assert "-SkipPaperSoak" in command
    assert "-DryRun" in command
    assert "-BatchDate" in command
    assert command[command.index("-BatchDate") + 1] == "2026-03-08"


def test_run_manual_v4_daily_pipeline_enables_paper_soak_when_duration_set(tmp_path, monkeypatch) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    wrapper_script = tmp_path / "scripts" / "v4_candidate_acceptance.ps1"
    wrapper_script.parent.mkdir(parents=True, exist_ok=True)
    wrapper_script.write_text("# noop\n", encoding="utf-8")

    captured: dict[str, object] = {}

    def _fake_run(command, cwd=None, text=None):
        captured["command"] = list(command)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(cli_mod, "_resolve_powershell_exe", lambda: "pwsh")
    monkeypatch.setattr(cli_mod.subprocess, "run", _fake_run)

    args = argparse.Namespace(
        mode="spawn_only",
        batch_date=None,
        run_paper_soak=False,
        paper_soak_duration_sec=120,
        dry_run=False,
    )

    exit_code = cli_mod._run_manual_v4_daily_pipeline(args, config_dir)

    assert exit_code == 0
    command = captured["command"]
    assert "-SkipPaperSoak" not in command
    assert "-PaperSoakDurationSec" in command
    assert command[command.index("-PaperSoakDurationSec") + 1] == "120"


def test_run_manual_v4_daily_pipeline_rejects_non_spawn_mode(tmp_path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)

    args = argparse.Namespace(
        mode="combined",
        batch_date=None,
        run_paper_soak=False,
        paper_soak_duration_sec=None,
        dry_run=False,
    )

    try:
        cli_mod._run_manual_v4_daily_pipeline(args, config_dir)
    except ValueError as exc:
        assert "spawn_only" in str(exc)
    else:
        raise AssertionError("expected ValueError for non-spawn manual mode")
