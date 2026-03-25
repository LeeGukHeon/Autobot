from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]


def _powershell_exe() -> str:
    for name in ("powershell.exe", "pwsh"):
        resolved = shutil.which(name)
        if resolved:
            return resolved
    pytest.skip("PowerShell executable is required for installer dry-run test")


def _run_script_dry_run(script_name: str, *extra_args: str) -> str:
    completed = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(REPO_ROOT / "scripts" / script_name),
            "-ProjectRoot",
            str(REPO_ROOT),
            "-PythonExe",
            "python",
            *extra_args,
            "-DryRun",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return completed.stdout


def test_t23_2_dashboard_installer_dry_run_keeps_protected_unit_contract() -> None:
    stdout = _run_script_dry_run("install_server_dashboard_service.ps1")

    assert "[dashboard-install][dry-run] unit=autobot-dashboard.service" in stdout
    assert "autobot.dashboard_server" in stdout
    assert "--project-root" in stdout
    assert "--host" in stdout
    assert "--port" in stdout
    assert "ExecStart=/bin/bash -lc " in stdout


def test_t23_2_daily_acceptance_installer_dry_run_keeps_wrapper_and_runtime_units() -> None:
    stdout = _run_script_dry_run("install_server_daily_acceptance_service.ps1")

    assert "[daily-accept-install][dry-run] service=autobot-daily-v4-accept.service" in stdout
    assert "[daily-accept-install][dry-run] timer=autobot-daily-v4-accept.timer" in stdout
    assert "daily_champion_challenger_v4_for_server.ps1" in stdout
    assert "v4_governed_candidate_acceptance.ps1" in stdout
    assert "autobot-paper-v4.service" in stdout
    assert "autobot-paper-v4-challenger.service" in stdout


def test_t23_2_rank_shadow_installer_dry_run_keeps_protected_units() -> None:
    stdout = _run_script_dry_run("install_server_rank_shadow_service.ps1")

    assert "[rank-shadow-install][dry-run] service=autobot-v4-rank-shadow.service" in stdout
    assert "[rank-shadow-install][dry-run] timer=autobot-v4-rank-shadow.timer" in stdout
    assert "daily_rank_shadow_cycle_for_server.ps1" in stdout
    assert "v4_rank_shadow_candidate_acceptance.ps1" in stdout
    assert "autobot-v4-challenger-spawn.service" in stdout
    assert "autobot-v4-challenger-promote.service" in stdout
    assert "autobot-v4-challenger-spawn.service,autobot-v4-challenger-promote.service" in stdout
    assert "-AcceptanceArgs" in stdout
    assert "-SkipPaperSoak" in stdout


def test_t23_2_live_execution_policy_installer_dry_run_keeps_timer_contract() -> None:
    stdout = _run_script_dry_run("install_server_live_execution_policy_service.ps1")

    assert "[live-exec-install][dry-run] service=autobot-live-execution-policy.service" in stdout
    assert "[live-exec-install][dry-run] timer=autobot-live-execution-policy.timer" in stdout
    assert "refresh_live_execution_policy.ps1" in stdout
    assert "data/state/live_state.db,data/state/live_candidate/live_state.db" in stdout


def test_t23_2_daily_acceptance_installer_serializes_nested_array_args_safely() -> None:
    completed = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            (
                "& { "
                f"& '{REPO_ROOT / 'scripts' / 'install_server_daily_acceptance_service.ps1'}' "
                f"-ProjectRoot '{REPO_ROOT}' "
                "-PythonExe 'python' "
                "-PromotionTargetUnits @('autobot-live-alpha.service','autobot-live-alpha-candidate.service') "
                "-BlockOnActiveUnits @('autobot-v4-challenger-spawn.service','autobot-v4-challenger-promote.service') "
                "-AcceptanceArgs @('-SkipPaperSoak','-SkipPromote') "
                "-DryRun "
                "}"
            ),
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    stdout = completed.stdout

    assert "autobot-live-alpha.service,autobot-live-alpha-candidate.service" in stdout
    assert "autobot-v4-challenger-spawn.service,autobot-v4-challenger-promote.service" in stdout
    assert "-SkipPaperSoak,-SkipPromote" in stdout


def test_t23_2_daily_orchestrator_param_surface_keeps_protected_names() -> None:
    source = (REPO_ROOT / "scripts" / "daily_champion_challenger_v4_for_server.ps1").read_text(encoding="utf-8")

    for snippet in (
        '[string]$AcceptanceScript = ""',
        '[string]$RuntimeInstallScript = ""',
        '[string]$BatchDate = ""',
        '[string]$ChampionUnitName = "autobot-paper-v4.service"',
        '[string]$ChallengerUnitName = "autobot-paper-v4-challenger.service"',
        '[string[]]$PromotionTargetUnits = @()',
        '[string[]]$CandidateTargetUnits = @()',
        '[string[]]$BlockOnActiveUnits = @()',
        '[string[]]$AcceptanceArgs = @()',
        '[ValidateSet("combined", "promote_only", "spawn_only")]',
        '[string]$Mode = "combined"',
        '[switch]$SkipDailyPipeline',
        '[switch]$SkipReportRefresh',
        '[switch]$DryRun',
    ):
        assert snippet in source


def test_t23_2_acceptance_scripts_keep_frozen_pointer_aliases_and_runtime_units() -> None:
    protected_scripts = (
        "v4_promotable_candidate_acceptance.ps1",
        "v4_scout_candidate_acceptance.ps1",
        "v4_rank_shadow_candidate_acceptance.ps1",
        "v4_rank_governed_candidate_acceptance.ps1",
    )

    for script_name in protected_scripts:
        source = (REPO_ROOT / "scripts" / script_name).read_text(encoding="utf-8")
        assert '. (Join-Path $PSScriptRoot "v4_acceptance_contract.ps1")' in source
        assert '$knownRuntimeUnits = @("autobot-paper-v4.service", "autobot-live-alpha.service")' in source
        assert '$trainDataQualityFloorDate = Get-V4TrainDataQualityFloorDate' in source
        assert '-CandidateModelRef "latest_candidate_v4"' in source
        assert '-ChampionModelRef "champion_v4"' in source
        assert '-TrainDataQualityFloorDate $trainDataQualityFloorDate' in source
        assert '-KnownRuntimeUnits $knownRuntimeUnits' in source
        assert '-TrainStartFloorDate "2026-03-04"' not in source


def test_t23_2_v4_acceptance_contract_keeps_explicit_data_quality_floor() -> None:
    source = (REPO_ROOT / "scripts" / "v4_acceptance_contract.ps1").read_text(encoding="utf-8")

    assert "Get-V4TrainDataQualityFloorDate" in source
    assert '"2026-03-04"' in source


def test_t23_2_governed_acceptance_script_keeps_promotable_fallback() -> None:
    source = (REPO_ROOT / "scripts" / "v4_governed_candidate_acceptance.ps1").read_text(encoding="utf-8")

    assert 'selected_acceptance_script' in source
    assert 'v4_promotable_candidate_acceptance.ps1' in source
    assert re.search(r'& \$selectedScriptPath @args', source) is not None


def test_t23_2_runtime_installer_accepts_serialized_paper_cli_args() -> None:
    stdout = _run_script_dry_run(
        "install_server_runtime_services.ps1",
        "-PaperCliArgs",
        "--model-ref,run-123",
    )

    assert "--model-ref" in stdout
    assert "run-123" in stdout
    assert "bootstrap_champion=False" in stdout


def test_t23_2_runtime_installer_supports_paired_paper_preset() -> None:
    stdout = _run_script_dry_run(
        "install_server_runtime_services.ps1",
        "-PaperPreset",
        "paired_v4",
        "-PaperUnitName",
        "autobot-paper-v4-paired.service",
    )

    assert "autobot.paper.paired_runtime" in stdout
    assert "run-service" in stdout
    assert "ConditionPathExists=" in stdout
    assert "autobot-paper-v4-paired.service" in stdout


def test_t23_2_runtime_installer_keeps_explicit_bootstrap_switch() -> None:
    source = (REPO_ROOT / "scripts" / "install_server_runtime_services.ps1").read_text(encoding="utf-8")

    assert '[switch]$BootstrapChampion' in source
    assert 'install no longer auto-bootstraps' in source


def test_t23_2_v4_candidate_state_helper_is_shared_by_scripts() -> None:
    helper_snippet = '. (Join-Path $PSScriptRoot "v4_candidate_state_helpers.ps1")'

    for script_name in (
        "candidate_acceptance.ps1",
        "adopt_v4_candidate_for_server.ps1",
        "daily_champion_challenger_v4_for_server.ps1",
    ):
        source = (REPO_ROOT / "scripts" / script_name).read_text(encoding="utf-8")
        assert helper_snippet in source
