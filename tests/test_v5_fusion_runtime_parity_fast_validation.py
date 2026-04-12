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
    pytest.skip("PowerShell executable is required for this test")


def test_v5_fusion_runtime_parity_fast_validation_targets_runtime_parity_contract() -> None:
    source = (REPO_ROOT / "scripts" / "v5_fusion_runtime_parity_fast_validation.ps1").read_text(encoding="utf-8")

    assert '-ModelFamily "train_v5_fusion"' in source
    assert '-Trainer "v5_fusion"' in source
    assert '-DependencyTrainers @("v5_panel_ensemble", "v5_sequence", "v5_lob", "v5_tradability")' in source
    assert '-Tf $Tf' in source
    assert '-HoldBars $HoldBars' in source
    assert '-BacktestRuntimeParityEnabled:$true' in source
    assert '-SkipDailyPipeline' in source
    assert '-SkipPaperSoak' in source
    assert '-SkipPromote' in source
    assert '-SkipReportRefresh' in source
    assert '-ReportPrefix "v5_candidate_runtime_parity_fast"' in source


def test_v5_fusion_runtime_parity_fast_validation_delegates_to_candidate_acceptance(tmp_path: Path) -> None:
    scripts_dir = tmp_path / "scripts"
    scripts_dir.mkdir(parents=True, exist_ok=True)
    wrapper_script = scripts_dir / "v5_fusion_runtime_parity_fast_validation.ps1"
    wrapper_script.write_text(
        (REPO_ROOT / "scripts" / "v5_fusion_runtime_parity_fast_validation.ps1").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    fake_candidate_acceptance = scripts_dir / "candidate_acceptance.ps1"
    fake_candidate_acceptance.write_text(
        "param([string]$ModelFamily = '', [string]$Trainer = '', [string]$Tf = '', [int]$HoldBars = -1, [string]$OutDir = '', [bool]$BacktestRuntimeParityEnabled = $false, [string[]]$DependencyTrainers = @(), [switch]$SkipDailyPipeline, [switch]$SkipPaperSoak, [switch]$SkipPromote, [switch]$SkipReportRefresh)\n"
        "Write-Host ('[fast-parity] family=' + $ModelFamily)\n"
        "Write-Host ('[fast-parity] trainer=' + $Trainer)\n"
        "Write-Host ('[fast-parity] tf=' + $Tf)\n"
        "Write-Host ('[fast-parity] hold_bars=' + [string]$HoldBars)\n"
        "Write-Host ('[fast-parity] outdir=' + $OutDir)\n"
        "Write-Host ('[fast-parity] parity=' + [string]$BacktestRuntimeParityEnabled)\n"
        "Write-Host ('[fast-parity] deps=' + (($DependencyTrainers | ForEach-Object { [string]$_ }) -join ','))\n"
        "Write-Host ('[fast-parity] skip_daily=' + [string]$SkipDailyPipeline.IsPresent)\n"
        "Write-Host ('[fast-parity] skip_paper=' + [string]$SkipPaperSoak.IsPresent)\n"
        "Write-Host ('[fast-parity] skip_promote=' + [string]$SkipPromote.IsPresent)\n"
        "Write-Host ('[fast-parity] skip_report=' + [string]$SkipReportRefresh.IsPresent)\n"
        "exit 0\n",
        encoding="utf-8",
    )
    contract_helper = scripts_dir / "v4_acceptance_contract.ps1"
    contract_helper.write_text(
        "function Resolve-DefaultProjectRoot { return (Get-Location).Path }\n"
        "function Resolve-DefaultPythonExe { param([string]$Root) return 'python' }\n"
        "function Get-V4TrainDataQualityFloorDate { return '2026-03-04' }\n",
        encoding="utf-8",
    )

    completed = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(wrapper_script),
            "-ProjectRoot",
            str(tmp_path),
            "-PythonExe",
            "python",
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    assert "[fast-parity] family=train_v5_fusion" in completed.stdout
    assert "[fast-parity] trainer=v5_fusion" in completed.stdout
    assert "[fast-parity] tf=1m" in completed.stdout
    assert "[fast-parity] hold_bars=30" in completed.stdout
    assert "[fast-parity] outdir=logs/model_v5_runtime_parity_fast" in completed.stdout
    assert "[fast-parity] parity=True" in completed.stdout
    assert "[fast-parity] deps=v5_panel_ensemble,v5_sequence,v5_lob,v5_tradability" in completed.stdout
    assert "[fast-parity] skip_daily=True" in completed.stdout
    assert "[fast-parity] skip_paper=True" in completed.stdout
    assert "[fast-parity] skip_promote=True" in completed.stdout
    assert "[fast-parity] skip_report=True" in completed.stdout
