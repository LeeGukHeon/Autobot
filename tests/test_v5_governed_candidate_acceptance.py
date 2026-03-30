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


def test_v5_governed_candidate_acceptance_targets_v5_fusion_contract() -> None:
    source = (REPO_ROOT / "scripts" / "v5_governed_candidate_acceptance.ps1").read_text(encoding="utf-8")

    assert '-ModelFamily "train_v5_fusion"' in source
    assert '-Trainer "v5_fusion"' in source
    assert '-DependencyTrainers @("v5_panel_ensemble", "v5_sequence", "v5_lob")' in source
    assert '-FeatureSet "v4"' in source
    assert '-LabelSet "v3"' in source
    assert '-CandidateModelRef "latest_candidate"' in source
    assert '-ChampionModelRef "champion"' in source
    assert '-ChampionModelFamily "train_v4_crypto_cs"' not in source
    assert '-OutDir "logs/model_v5_acceptance"' in source


def test_v5_governed_candidate_acceptance_delegates_to_candidate_acceptance(tmp_path: Path) -> None:
    scripts_dir = tmp_path / "scripts"
    scripts_dir.mkdir(parents=True, exist_ok=True)
    governed_script = scripts_dir / "v5_governed_candidate_acceptance.ps1"
    governed_script.write_text(
        (REPO_ROOT / "scripts" / "v5_governed_candidate_acceptance.ps1").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    fake_candidate_acceptance = scripts_dir / "candidate_acceptance.ps1"
    fake_candidate_acceptance.write_text(
        "param([string]$ModelFamily = '', [string]$Trainer = '', [string]$FeatureSet = '', [string]$LabelSet = '', [string]$ChampionModelFamily = '', [string[]]$DependencyTrainers = @())\n"
        "Write-Host ('[fake-v5] family=' + $ModelFamily)\n"
        "Write-Host ('[fake-v5] trainer=' + $Trainer)\n"
        "Write-Host ('[fake-v5] feature=' + $FeatureSet)\n"
        "Write-Host ('[fake-v5] label=' + $LabelSet)\n"
        "Write-Host ('[fake-v5] champion_family=' + $ChampionModelFamily)\n"
        "Write-Host ('[fake-v5] deps=' + (($DependencyTrainers | ForEach-Object { [string]$_ }) -join ','))\n"
        "exit 0\n",
        encoding="utf-8",
    )
    contract_helper = scripts_dir / "v4_acceptance_contract.ps1"
    contract_helper.write_text(
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
            str(governed_script),
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
    assert "[fake-v5] family=train_v5_fusion" in completed.stdout
    assert "[fake-v5] trainer=v5_fusion" in completed.stdout
    assert "[fake-v5] feature=v4" in completed.stdout
    assert "[fake-v5] label=v3" in completed.stdout
    assert "[fake-v5] champion_family=" in completed.stdout
    assert "[fake-v5] deps=v5_panel_ensemble,v5_sequence,v5_lob" in completed.stdout
