import json
import shutil
import subprocess
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "candidate_acceptance.ps1"


def _powershell_exe() -> str:
    for name in ("powershell.exe", "pwsh"):
        resolved = shutil.which(name)
        if resolved:
            return resolved
    pytest.skip("PowerShell executable is required for this test")


def _extract_function_block(source: str, start_marker: str, end_marker: str) -> str:
    start = source.index(start_marker)
    end = source.index(end_marker, start)
    return source[start:end].rstrip()


def test_resolve_split_policy_candidate_holdout_days_keeps_empty_override_as_array(tmp_path: Path) -> None:
    source = SCRIPT_PATH.read_text(encoding="utf-8")
    to_int64_block = _extract_function_block(
        source,
        "function To-Int64 {",
        "function To-Bool {",
    )
    holdout_block = _extract_function_block(
        source,
        "function Resolve-SplitPolicyCandidateHoldoutDays {",
        "function Resolve-SplitPolicyHoldoutWindows {",
    )
    merge_block = _extract_function_block(
        source,
        "function Merge-UniqueStringArray {",
        "function Build-ReportMarkdown {",
    )
    script_path = tmp_path / "holdout_probe.ps1"
    script_path.write_text(
        "\n\n".join(
            [
                to_int64_block,
                holdout_block,
                merge_block,
                "$payload = [ordered]@{",
                "    default = @(Resolve-SplitPolicyCandidateHoldoutDays -RequestedBacktestLookbackDays 3 -OverrideText '')",
                "    override = @(Resolve-SplitPolicyCandidateHoldoutDays -RequestedBacktestLookbackDays 5 -OverrideText '2, 4 2')",
                "    merged_empty = @(Merge-UniqueStringArray -First @() -Second @())",
                "}",
                "$payload | ConvertTo-Json -Compress -Depth 4",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    completed = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(script_path),
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )

    payload = json.loads(completed.stdout.strip())

    assert payload["default"] == [1, 2, 3]
    assert payload["override"] == [2, 4]
    assert payload["merged_empty"] == []
