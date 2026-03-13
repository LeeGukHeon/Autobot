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


def test_candidate_acceptance_array_helpers_keep_empty_results_as_arrays(tmp_path: Path) -> None:
    source = SCRIPT_PATH.read_text(encoding="utf-8")
    convert_block = _extract_function_block(
        source,
        "function Convert-ToStringArray {",
        "function To-Double {",
    )
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
    date_range_block = _extract_function_block(
        source,
        "function Get-DateRangeAscending {",
        "function Invoke-FeaturesBuildAndLoadReport {",
    )

    script_path = tmp_path / "candidate_acceptance_array_probe.ps1"
    script_path.write_text(
        "\n\n".join(
            [
                convert_block,
                to_int64_block,
                holdout_block,
                merge_block,
                date_range_block,
                "$convertEmpty = Convert-ToStringArray $null",
                "$holdoutDefault = Resolve-SplitPolicyCandidateHoldoutDays -RequestedBacktestLookbackDays 3 -OverrideText ''",
                "$holdoutOverride = Resolve-SplitPolicyCandidateHoldoutDays -RequestedBacktestLookbackDays 5 -OverrideText '2, 4 2'",
                "$mergeEmpty = Merge-UniqueStringArray -First @() -Second @()",
                "$mergeNonempty = Merge-UniqueStringArray -First @('A', 'A') -Second @('B')",
                "$invalidRange = Get-DateRangeAscending -StartDate '2026-03-10' -EndDate '2026-03-09'",
                "$payload = [ordered]@{",
                "    convert_empty = $convertEmpty",
                "    convert_empty_count = $convertEmpty.Count",
                "    holdout_default = $holdoutDefault",
                "    holdout_default_count = $holdoutDefault.Count",
                "    holdout_override = $holdoutOverride",
                "    holdout_override_count = $holdoutOverride.Count",
                "    merge_empty = $mergeEmpty",
                "    merge_empty_count = $mergeEmpty.Count",
                "    merge_nonempty = $mergeNonempty",
                "    merge_nonempty_count = $mergeNonempty.Count",
                "    invalid_range = $invalidRange",
                "    invalid_range_count = $invalidRange.Count",
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

    assert payload["convert_empty"] == []
    assert payload["convert_empty_count"] == 0
    assert payload["holdout_default"] == [1, 2, 3]
    assert payload["holdout_default_count"] == 3
    assert payload["holdout_override"] == [2, 4]
    assert payload["holdout_override_count"] == 2
    assert payload["merge_empty"] == []
    assert payload["merge_empty_count"] == 0
    assert payload["merge_nonempty"] == ["A", "B"]
    assert payload["merge_nonempty_count"] == 2
    assert payload["invalid_range"] == []
    assert payload["invalid_range_count"] == 0
