from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _powershell_exe() -> str:
    for name in ("powershell.exe", "pwsh"):
        resolved = shutil.which(name)
        if resolved:
            return resolved
    raise RuntimeError("PowerShell executable is required")


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def test_refresh_current_features_v4_contract_artifacts_defaults_tf_to_one_minute(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    feature_meta_root = project_root / "data" / "features" / "features_v4" / "_meta"
    summary_path = feature_meta_root / "contract_refresh_report.json"

    _write_json(
        feature_meta_root / "build_report.json",
        {
            "effective_start": "2026-04-01",
            "effective_end": "2026-04-12",
            "quote": "KRW",
            "selected_markets": ["KRW-BTC", "KRW-ETH"],
        },
    )
    _write_json(feature_meta_root / "feature_spec.json", {"quote": "KRW"})
    _write_json(feature_meta_root / "label_spec.json", {"label_columns": ["y_cls_h3"]})

    completed = subprocess.run(
        [
            _powershell_exe(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(REPO_ROOT / "scripts" / "refresh_current_features_v4_contract_artifacts.ps1"),
            "-ProjectRoot",
            str(project_root),
            "-PythonExe",
            sys.executable,
            "-SummaryPath",
            str(summary_path),
            "-DryRun",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    summary = json.loads(summary_path.read_text(encoding="utf-8-sig"))
    assert summary["tf"] == "1m"
    assert summary["start"] == "2026-04-01"
    assert summary["end"] == "2026-04-12"
